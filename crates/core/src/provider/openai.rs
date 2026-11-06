//! OpenAI model provider: drives either Chat Completions (default) or the
//! stateful Responses API over a hand-rolled Server-Sent-Events transport.
//!
//! Requests are assembled as JSON, POSTed with [`reqwest`], and the streaming
//! response body is parsed as SSE by hand. Each provider event is mapped onto the
//! engine's normalized [`ModelStreamEvent`]. The
//! request-assembly and the SSE-to-event mapping are factored into pure functions
//! so they can be unit-tested with canned bytes/JSON.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use rand::RngCore;
use serde_json::json;

use crate::provider::anthropic::estimate_tokens_fallback;
use crate::provider::auth::{validate_provider_extra_headers, AuthSource};
use crate::provider::behavior::{
    render_provider_text_envelope, resolve_provider_behavior_profile, ProviderBehaviorProfile,
    ProviderPromptStyle, ProviderTextEnvelopeKind, ProviderTextEnvelopeOptions,
};
use crate::provider::classify::{classify_error, RawProviderError};
use crate::provider::interceptor::{
    admit_attempt, report_attempt, ProviderRequestInfo, SharedRequestInterceptor,
};
use crate::provider::output_reservation::reduce_output_reservation;
use crate::provider::prewarm::{endpoint_origin, probe, PrewarmOutcome};
use crate::provider::retry::{
    establishment_steps, observe_retries, with_retry, EstablishmentStep, RetryError, RetryOptions,
};
use crate::provider::util::{
    diagnostic_string, objectified_tool_input, raw_from_provider_error, tool_result_user_blocks,
};
use crate::runtime_values::{
    runtime_json, RuntimeJson as Value, RuntimeObject as Map, RuntimeString, WhatwgUtf8,
};
use crate::schemas::content::{Citation, ContentBlock, HarnessEnvelope, OutputBlock};
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::schemas::message::{Message, Usage};
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, ProviderToolCallKind, SystemPromptPart, ThinkingConfig,
    ToolChoice, ToolInputFormat, ToolSpec, ToolSpecFormat, ToolSpecGrammarSyntax, ToolSpecKind,
    WebSearchAction, WebSearchContextSize,
};

mod approval_classifier;
mod remote_compaction;

/// Fallback per-model input context window when nothing more specific matches.
const DEFAULT_CONTEXT_WINDOW: u64 = 128_000;
/// Fallback cap on output tokens for the active model.
const DEFAULT_MAX_OUTPUT: u64 = 16_384;
const RESPONSES_FAILED_WITHOUT_MESSAGE: &str =
    "Responses stream reported a failed response without a message";
/// Schema name for native structured output (`json_schema`).
const STRUCTURED_OUTPUT_NAME: &str = "result";
const STRUCTURED_OUTPUT_TOOL: &str = "klaus_emit_structured_result";

/// Strict schemas require every declared object property to be required.
/// Preserve optional fields by requesting the provider's ordinary JSON schema mode.
fn supports_strict_output_schema(schema: &Value) -> bool {
    match schema {
        Value::Object(object) => {
            if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                let required = object.get("required").and_then(Value::as_array);
                if object.get("additionalProperties") != Some(&Value::Bool(false))
                    || properties.keys().any(|name| {
                        !required.is_some_and(|required| {
                            required.iter().any(|value| value.as_str() == Some(&name))
                        })
                    })
                {
                    return false;
                }
            }
            object.values().all(supports_strict_output_schema)
        }
        Value::Array(values) => values.iter().all(supports_strict_output_schema),
        _ => true,
    }
}
const MAX_STRUCTURED_OUTPUT_UNITS: usize = 4 * 1024 * 1024;
/// The public API root when no override is supplied.
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// The default model when the caller omits one.
const DEFAULT_MODEL: &str = "gpt-4o";

fn vs(value: impl Into<RuntimeString>) -> Value {
    Value::String(value.into())
}

fn vobj(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut object = Map::new();
    for (key, value) in pairs {
        object.insert(key, value);
    }
    Value::Object(object)
}

fn invalid_openai_tool_choice(provider: &str, reason: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: format!("{provider} tool choice is invalid: {}.", reason.into()).into(),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn invalid_openai_compatibility(reason: impl Into<String>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: format!(
            "OpenAI compatibility configuration is invalid: {}",
            reason.into()
        )
        .into(),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn validate_exact_tool_choice(
    provider: &str,
    choice: Option<&ToolChoice>,
    tools: &[ToolSpec],
    has_anonymous_tools: bool,
) -> Option<ProviderError> {
    let choice = match choice {
        None | Some(ToolChoice::Auto | ToolChoice::None) => return None,
        Some(choice) => choice,
    };
    match choice {
        ToolChoice::Any if tools.is_empty() && !has_anonymous_tools => {
            Some(invalid_openai_tool_choice(
                provider,
                "`any` requires at least one provider-visible tool",
            ))
        }
        ToolChoice::Tool { name } if !tools.iter().any(|candidate| candidate.name == *name) => {
            let available = if tools.is_empty() {
                "(none)".to_owned()
            } else {
                tools
                    .iter()
                    .map(|tool| diagnostic_string(&tool.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Some(invalid_openai_tool_choice(
                provider,
                format!(
                    "forced tool \"{}\" is not provider-visible; available tools: {available}",
                    diagnostic_string(name)
                ),
            ))
        }
        ToolChoice::Auto | ToolChoice::Any | ToolChoice::Tool { .. } | ToolChoice::None => None,
    }
}

fn responses_forced_choice(
    name: &RuntimeString,
    tools: &[ToolSpec],
) -> Result<Value, ProviderError> {
    if tools
        .iter()
        .any(|tool| &tool.name == name && tool.kind == ToolSpecKind::ToolSearch)
    {
        return Err(invalid_openai_tool_choice(
            "OpenAI Responses",
            "forced native tool_search is not supported; use `auto` or `any`",
        ));
    }

    match tools.iter().find(|tool| &tool.name == name) {
        Some(tool) if tool.kind == ToolSpecKind::Freeform => {
            Ok(vobj([("type", vs("custom")), ("name", vs(name.clone()))]))
        }
        Some(tool) if tool.kind == ToolSpecKind::Function => {
            Ok(vobj([("type", vs("function")), ("name", vs(name.clone()))]))
        }
        Some(_) => Err(invalid_openai_tool_choice(
            "OpenAI Responses",
            "forced native tool_search is not supported; use `auto` or `any`",
        )),
        None => Err(invalid_openai_tool_choice(
            "OpenAI Responses",
            format!(
                "forced tool \"{}\" is not provider-visible; available tools: {}",
                diagnostic_string(name),
                if tools.is_empty() {
                    "(none)".to_owned()
                } else {
                    tools
                        .iter()
                        .map(|tool| diagnostic_string(&tool.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ),
        )),
    }
}

fn validate_responses_request_choice(request: &ModelRequest) -> Option<ProviderError> {
    if let Some(error) = validate_exact_tool_choice(
        "OpenAI Responses",
        request.tool_choice.as_ref(),
        &request.tools,
        request.web_search.is_some(),
    ) {
        return Some(error);
    }

    let forced_name = match request.tool_choice.as_ref() {
        Some(ToolChoice::Tool { name }) => name,
        _ => return None,
    };
    responses_forced_choice(forced_name, &request.tools).err()
}

fn validate_chat_request_choice(request: &ModelRequest) -> Option<ProviderError> {
    validate_exact_tool_choice(
        "OpenAI Chat",
        request.tool_choice.as_ref(),
        &request.tools,
        false,
    )
}

/// Which API surface to drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenAiApi {
    /// Chat Completions (`/chat/completions`).
    #[default]
    Chat,
    /// The stateful Responses API (`/responses`).
    Responses,
}

/// Chat Completions streaming shape. Standard OpenAI-compatible endpoints send
/// incremental `choices[].delta`; some gateways send cumulative
/// `choices[].message` snapshots instead.
///
/// A chunk states which one it is, so the shape is read off the stream rather
/// than declared by the caller: the first chunk carrying either field settles it
/// for the rest of the response. Requiring a declaration meant every endpoint
/// nobody had hand-configured a mode for — every custom model the settings form
/// can create — was parsed as deltas, and a snapshot gateway's entire reply was
/// dropped while its usage was still booked, i.e. a turn that completed empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ChatChunkShape {
    #[default]
    Delta,
    Snapshot,
}

/// Stable session identity included in explicitly enabled gateway request
/// bodies. The per-logical-request id is generated by the provider.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAIRequestIdentity {
    session_id: String,
}

impl OpenAIRequestIdentity {
    pub fn try_new(session_id: impl Into<String>) -> Result<Self, String> {
        let session_id = session_id.into();
        if session_id.is_empty() {
            return Err("OpenAI request identity sessionId must not be empty".to_string());
        }
        if session_id.len() > 256 {
            return Err("OpenAI request identity sessionId must be at most 256 bytes".to_string());
        }
        if session_id.chars().any(char::is_control) {
            return Err(
                "OpenAI request identity sessionId must not contain control characters".to_string(),
            );
        }
        Ok(Self { session_id })
    }
}

impl fmt::Debug for OpenAIRequestIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAIRequestIdentity")
            .field("session_id", &"<redacted>")
            .finish()
    }
}

/// Explicit OpenAI-compatible gateway behavior. This is opt-in so ordinary
/// OpenAI-compatible endpoints retain their standard request shape.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAICompatibilityOptions {
    extra_headers: HashMap<String, String>,
    request_identity: Option<OpenAIRequestIdentity>,
    responses_function_tools: bool,
    instruction_role: Option<OpenAIInstructionRole>,
    use_model_default_effort: bool,
}

impl OpenAICompatibilityOptions {
    pub fn try_new(
        extra_headers: HashMap<String, String>,
        request_identity: Option<OpenAIRequestIdentity>,
    ) -> Result<Self, String> {
        validate_provider_extra_headers(&extra_headers)?;
        Ok(Self {
            extra_headers,
            request_identity,
            responses_function_tools: false,
            instruction_role: None,
            use_model_default_effort: false,
        })
    }

    /// Use each tool's JSON input schema on Responses gateways that accept
    /// function tools only. Native freeform/search forms remain the default.
    pub fn with_responses_function_tools(mut self, enabled: bool) -> Self {
        self.responses_function_tools = enabled;
        self
    }

    /// Prefer the endpoint's declared trusted-instruction role before its first request.
    /// Explicit per-model validation can still negotiate a different supported role.
    pub fn with_instruction_role(mut self, role: Option<OpenAIInstructionRole>) -> Self {
        self.instruction_role = role;
        self
    }

    /// Send the current model's declared default when the request leaves effort unset.
    pub fn with_model_default_effort(mut self, enabled: bool) -> Self {
        self.use_model_default_effort = enabled;
        self
    }

    fn instruction_role(&self) -> Option<OpenAIInstructionRole> {
        self.instruction_role
    }

    fn responses_function_tools(&self) -> bool {
        self.responses_function_tools
    }

    fn extra_headers(&self) -> &HashMap<String, String> {
        &self.extra_headers
    }

    fn request_identity(&self) -> Option<&OpenAIRequestIdentity> {
        self.request_identity.as_ref()
    }
}

impl fmt::Debug for OpenAICompatibilityOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names = self.extra_headers.keys().collect::<Vec<_>>();
        names.sort();
        f.debug_struct("OpenAICompatibilityOptions")
            .field("extra_header_names", &names)
            .field("responses_function_tools", &self.responses_function_tools)
            .field("instruction_role", &self.instruction_role)
            .field("use_model_default_effort", &self.use_model_default_effort)
            .field(
                "request_identity",
                &self.request_identity.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Construction options for [`OpenAIProvider`].
/// Carry the request's speed intent only to supported local subscription bridges.
pub fn local_bridge_fast_mode(
    source: Option<&str>,
    api: OpenAiApi,
    dialect: OpenAiReasoningDialect,
    base_url: Option<&str>,
    enabled: bool,
) -> Option<bool> {
    (api == OpenAiApi::Chat
        && matches!(source, Some("qoder" | "qoderwork"))
        && dialect == OpenAiReasoningDialect::Qoder
        && base_url.is_some_and(|base| {
            url::Url::parse(base).ok().is_some_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some_and(|host| {
                        host == "localhost"
                            || host
                                .trim_matches(['[', ']'])
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    })
            })
        }))
    .then_some(enabled)
}

#[derive(Default)]
pub struct OpenAIProviderOptions {
    /// Explicit native compaction protocol advertised by this endpoint.
    pub remote_compaction: crate::types::provider::RemoteCompactionSupport,
    /// Static API key. When an `auth_source` is supplied it takes precedence.
    pub api_key: Option<String>,
    /// Override the API root.
    pub base_url: Option<String>,
    /// The model name to advertise as the default.
    pub default_model: Option<String>,
    /// Which surface to drive (`Chat` by default).
    pub api: Option<OpenAiApi>,
    /// Override the request-establishment retry policy.
    pub retry: Option<RetryOptions>,
    /// Subscription/OAuth auth — overrides `api_key`, supplies per-request headers.
    pub auth_source: Option<Arc<dyn AuthSource>>,
    /// Inject a pre-built HTTP client (tests / custom transport config).
    pub http_client: Option<reqwest::Client>,
    /// Rebuild classifier clients with the same custom trust and proxy policy.
    pub classifier_http_client_factory:
        Option<crate::provider::approval_classifier_transport::ClassifierHttpClientFactory>,
    /// Explicit, validated behavior for non-standard OpenAI-compatible gateways.
    pub compatibility: Option<OpenAICompatibilityOptions>,
    /// Transport-level request interception (per-attempt headers / veto /
    /// settled-outcome observation).
    pub interceptor: Option<SharedRequestInterceptor>,
    /// Client-declared model capabilities. The provider reads THIS instead of
    /// inferring anything from the model id; absent = the minimal set.
    pub capabilities: Option<ModelCapabilities>,
    /// Per-model capability overrides keyed by exact model id, for sessions
    /// whose tier map runs auxiliary work on models other than the main one. A
    /// model absent here reads the session-level `capabilities`.
    pub capabilities_by_model: Option<HashMap<String, ModelCapabilities>>,
    /// The endpoint's reasoning wire dialect. Selected by the client per source
    /// (OpenAI/Codex vs DeepSeek vs a managed Qwen gateway); never inferred from
    /// the model id.
    pub reasoning_dialect: Option<OpenAiReasoningDialect>,
    /// Request priority processing on the Codex subscription Responses endpoint.
    pub fast_mode: bool,
    /// Explicit per-request intent for an authenticated local subscription bridge.
    /// None never sends a bridge-only parameter to an external Chat endpoint.
    pub bridge_fast_mode: Option<bool>,
}

/// How this endpoint spells the reasoning knob on the wire. This is a provider
/// DIALECT (the vocabulary a given OpenAI-compatible backend accepts), selected
/// by the client at init — never inferred from the model id. WHICH levels a
/// model supports is a separate, client-declared capability ([`ModelCapabilities::effort`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenAiReasoningDialect {
    /// Standard OpenAI: `reasoning_effort` in {low, medium, high, xhigh}; `max`
    /// clamps to `xhigh`; `max_completion_tokens` is always sent.
    #[default]
    OpenAi,
    /// Qwen-compatible Chat Completions: a declared thinking capability enables
    /// the top-level `enable_thinking` boolean on each request.
    Qwen,
    /// DeepSeek-compatible: `reasoning_effort` in {low, high, max}; thinking is
    /// toggled with a `{"thinking": {"type": ...}}` block.
    DeepSeek,
    /// Managed gateways (e.g. the hosted Qwen reasoning models) whose backend
    /// sizes the reasoning budget itself, so `max_completion_tokens` is omitted
    /// once an effort is set. Vocabulary matches OpenAI.
    ManagedQwen,
    /// The local Qoder bridge accepts an explicit `none` for detached requests
    /// that disable thinking. Unspecified intent retains its composer default.
    Qoder,
    /// llama.cpp chat templates consume standard effort names and accept `none`
    /// to disable reasoning explicitly.
    LlamaCpp,
    /// A gateway that still reads the pre-2024 OpenAI output cap. Vocabulary
    /// matches OpenAI, but the cap goes out as `max_tokens`: such a gateway
    /// silently IGNORES `max_completion_tokens`, which is worse than a rejection
    /// — the request succeeds, the cap does not apply, and a provider that bills
    /// per output token meters an unbounded turn.
    LegacyTokens,
}

impl OpenAiReasoningDialect {
    /// Spell a concrete effort level for this endpoint's wire vocabulary.
    fn spell(self, level: EffortLevel) -> &'static str {
        match self {
            OpenAiReasoningDialect::DeepSeek => match level {
                EffortLevel::None => "none",
                EffortLevel::Minimal => "minimal",
                EffortLevel::Persistent => "persistent",
                EffortLevel::Low => "low",
                EffortLevel::Medium | EffortLevel::High => "high",
                EffortLevel::Xhigh | EffortLevel::Max | EffortLevel::Ultra => "max",
            },
            // OpenAI's own ladder tops out at `max` on the current generation. A
            // model that does not answer to it simply does not declare the rung,
            // and the declared set is what a request is clamped into first.
            OpenAiReasoningDialect::OpenAi
            | OpenAiReasoningDialect::Qwen
            | OpenAiReasoningDialect::Qoder
            | OpenAiReasoningDialect::LlamaCpp
            | OpenAiReasoningDialect::LegacyTokens => match level {
                EffortLevel::None => "none",
                EffortLevel::Minimal => "minimal",
                EffortLevel::Persistent => "persistent",
                EffortLevel::Low => "low",
                EffortLevel::Medium => "medium",
                EffortLevel::High => "high",
                EffortLevel::Xhigh => "xhigh",
                EffortLevel::Max => "max",
                EffortLevel::Ultra => "xhigh",
            },
            // A managed gateway's vocabulary stops at `xhigh`; the top rung folds
            // into it rather than putting a word the gateway never named on the wire.
            OpenAiReasoningDialect::ManagedQwen => match level {
                EffortLevel::None => "none",
                EffortLevel::Minimal => "minimal",
                EffortLevel::Persistent => "persistent",
                EffortLevel::Low => "low",
                EffortLevel::Medium => "medium",
                EffortLevel::High => "high",
                EffortLevel::Xhigh | EffortLevel::Max | EffortLevel::Ultra => "xhigh",
            },
        }
    }
    /// DeepSeek toggles thinking with an explicit request block.
    fn uses_thinking_block(self) -> bool {
        matches!(self, OpenAiReasoningDialect::DeepSeek)
    }
    fn uses_qwen_thinking_switch(self) -> bool {
        matches!(self, Self::Qwen | Self::ManagedQwen)
    }
    /// A managed gateway sizes its own reasoning budget, so the client must not
    /// also cap `max_completion_tokens` when an effort is in play.
    fn manages_output_budget(self) -> bool {
        matches!(self, OpenAiReasoningDialect::ManagedQwen)
    }
    /// The wire name this endpoint reads the output cap from.
    fn output_cap_field(self) -> &'static str {
        match self {
            OpenAiReasoningDialect::LegacyTokens => "max_tokens",
            _ => "max_completion_tokens",
        }
    }
}

/// Some gateways require an explicit value even when their directory declares a
/// default. Apply that preference only when the source opted in; explicit level
/// and numeric requests retain their original meaning.
fn apply_model_default_effort(
    request: &mut ModelRequest,
    capabilities: &ModelCapabilities,
    compatibility: Option<&OpenAICompatibilityOptions>,
) {
    if request.effort.is_none()
        && compatibility.is_some_and(|options| options.use_model_default_effort)
    {
        request.effort = capabilities
            .effort
            .as_ref()
            .and_then(|effort| effort.default)
            .map(EffortValue::Level);
    }
}

/// The reasoning-effort wire value for THIS request: `None` when the client
/// declared no effort knob for the model, or when a numeric effort was supplied
/// (the endpoint then uses its own default). The requested level is clamped to
/// the declared ladder, then spelled for the endpoint dialect.
fn effort_for_request(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
) -> Option<&'static str> {
    let cap = caps.effort.as_ref()?;
    let requested = match req.effort.as_ref()? {
        EffortValue::Number(_) | EffortValue::Custom(_) => return None,
        EffortValue::Level(level) => *level,
    };
    cap.clamp(requested).map(|level| dialect.spell(level))
}

fn raw_effort_for_request(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
) -> Option<RuntimeString> {
    if let Some(EffortValue::Custom(value)) = &req.effort {
        return Some(value.clone());
    }
    effort_for_request(req, caps, dialect).map(Into::into)
}

/// Normalize usage into the engine's canonical [`Usage`]. The server's
/// `total_tokens` is the authoritative context size (it already bundles input +
/// cached + output + reasoning), carried in `total_tokens` so the counter trusts
/// it directly. The per-field breakdown is kept DISJOINT for cost: the reported
/// prompt tokens are the TOTAL prompt with the cached count a SUBSET, so the
/// cached portion is subtracted out of `input_tokens`. `server_total` falls back
/// to input+output for endpoints that omit `total_tokens`.
fn make_usage(prompt_tokens: u64, output_tokens: u64, cache_read: u64, server_total: u64) -> Usage {
    make_usage_with_cache_creation(
        prompt_tokens,
        output_tokens,
        cache_read,
        0,
        (server_total > 0).then_some(server_total),
    )
}

fn make_usage_with_cache_creation(
    prompt_tokens: u64,
    output_tokens: u64,
    cache_read: u64,
    cache_creation: u64,
    server_total: Option<u64>,
) -> Usage {
    let cached = cache_read.min(prompt_tokens);
    let created = cache_creation.min(prompt_tokens.saturating_sub(cached));
    let input_tokens = prompt_tokens.saturating_sub(cached).saturating_sub(created);
    Usage {
        input_tokens,
        output_tokens,
        cache_read_input_tokens: cached,
        cache_creation_input_tokens: created,
        total_tokens: Some(server_total.unwrap_or_else(|| {
            input_tokens
                .saturating_add(cached)
                .saturating_add(created)
                .saturating_add(output_tokens)
        })),
    }
}

fn structured_output_error(reason: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Unknown,
        message: format!("Structured output could not be completed: {reason}.").into(),
        status: None,
        code: Some("invalid_structured_output".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn structured_output_buffer_error(reason: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::RequestTooLarge,
        ..structured_output_error(reason)
    }
}

struct StructuredOutputState {
    tool_name: RuntimeString,
    formatter_available: bool,
    call_id: Option<RuntimeString>,
    call_complete: bool,
    json: RuntimeString,
    text: RuntimeString,
    original_tool_choice: Option<ToolChoice>,
    allowed_tools: HashSet<RuntimeString>,
    user_calls: HashSet<RuntimeString>,
    completed_user_calls: HashSet<RuntimeString>,
    hosted_search_allowed: bool,
    hosted_search_seen: bool,
    pending: Vec<ModelStreamEvent>,
    pending_units: usize,
}

impl StructuredOutputState {
    fn prepare(
        req: &mut ModelRequest,
        caps: &ModelCapabilities,
    ) -> Result<Option<Self>, ProviderError> {
        if caps.structured_output || req.output_schema.is_none() {
            return Ok(None);
        }
        if caps.tools == Some(false) {
            return Err(invalid_openai_compatibility(
                "structured output requires native schema support or function tools",
            ));
        }
        let mut tool_name = RuntimeString::from(STRUCTURED_OUTPUT_TOOL);
        let mut suffix = 0;
        while req.tools.iter().any(|tool| tool.name == tool_name) {
            suffix += 1;
            tool_name = format!("{STRUCTURED_OUTPUT_TOOL}_{suffix}").into();
        }
        let schema = req.output_schema.take().expect("schema presence checked");
        let original_tool_choice = req.tool_choice.clone();
        let allowed_tools = req.tools.iter().map(|tool| tool.name.clone()).collect();
        let hosted_search_allowed = req.web_search.is_some()
            && !matches!(
                original_tool_choice,
                Some(ToolChoice::None | ToolChoice::Tool { .. })
            );
        // A requested user tool remains authoritative for this round. The next
        // round can format its final answer after those results are available.
        let formatter_available = !matches!(
            req.tool_choice,
            Some(ToolChoice::Any | ToolChoice::Tool { .. })
        );
        if formatter_available {
            let force = req.tools.is_empty() || req.tool_choice == Some(ToolChoice::None);
            if req.tool_choice == Some(ToolChoice::None) {
                req.tools.clear();
            }
            req.tools.push(ToolSpec {
                name: tool_name.clone(),
                description: "Emit the final result as JSON matching the complete declared schema."
                    .into(),
                input_schema: schema,
                kind: ToolSpecKind::Function,
                format: None,
            });
            if force {
                req.tool_choice = Some(ToolChoice::Tool {
                    name: tool_name.clone(),
                });
            }
        }
        Ok(Some(Self {
            tool_name,
            formatter_available,
            call_id: None,
            call_complete: false,
            json: RuntimeString::default(),
            text: RuntimeString::default(),
            original_tool_choice,
            allowed_tools,
            user_calls: HashSet::new(),
            completed_user_calls: HashSet::new(),
            hosted_search_allowed,
            hosted_search_seen: false,
            pending: Vec::new(),
            pending_units: 0,
        }))
    }

    fn append(target: &mut RuntimeString, text: &RuntimeString) -> Result<(), ProviderError> {
        if target
            .as_utf16()
            .len()
            .saturating_add(text.as_utf16().len())
            > MAX_STRUCTURED_OUTPUT_UNITS
        {
            return Err(structured_output_buffer_error(
                "the JSON result exceeded its buffer limit",
            ));
        }
        *target = target.concat(text);
        Ok(())
    }

    fn buffer(&mut self, event: ModelStreamEvent, units: usize) -> Result<(), ProviderError> {
        self.pending_units = self.pending_units.saturating_add(units);
        if self.pending.len() >= 4096 || self.pending_units > MAX_STRUCTURED_OUTPUT_UNITS {
            return Err(structured_output_buffer_error(
                "the pending output exceeded its buffer limit",
            ));
        }
        if let ModelStreamEvent::TextDelta { text } = &event {
            if let Some(ModelStreamEvent::TextDelta { text: previous }) = self.pending.last_mut() {
                *previous = previous.concat(text);
                return Ok(());
            }
        }
        self.pending.push(event);
        Ok(())
    }

    fn take_pending(&mut self, preserve_replay: bool) -> Vec<ModelStreamEvent> {
        std::mem::take(&mut self.pending)
            .into_iter()
            .filter_map(|event| match event {
                ModelStreamEvent::ThinkingBlock {
                    text, is_summary, ..
                } if !preserve_replay => {
                    (!text.is_empty()).then_some(ModelStreamEvent::ThinkingBlock {
                        text,
                        is_summary,
                        signature: None,
                        encrypted_content: None,
                    })
                }
                ModelStreamEvent::RedactedThinkingBlock { .. }
                | ModelStreamEvent::TextDelta { .. }
                | ModelStreamEvent::TextMessage { .. }
                | ModelStreamEvent::TextMessageCompleted { .. }
                | ModelStreamEvent::ToolUseStart { .. }
                | ModelStreamEvent::ToolUseInputDelta { .. }
                | ModelStreamEvent::ToolUseStop { .. }
                    if !preserve_replay =>
                {
                    None
                }
                _ => Some(event),
            })
            .collect()
    }

    fn project(&mut self, event: ModelStreamEvent) -> Result<Vec<ModelStreamEvent>, ProviderError> {
        match event {
            ModelStreamEvent::ToolUseStart {
                ref id, ref name, ..
            } if self.formatter_available && *name == self.tool_name => {
                if !self.user_calls.is_empty() {
                    return Err(structured_output_error(
                        "a formatting call was mixed with executable tools",
                    ));
                }
                if self.call_id.as_ref().is_some_and(|existing| existing != id) {
                    return Err(structured_output_error(
                        "multiple formatting calls were returned",
                    ));
                }
                self.call_id = Some(id.clone());
                Ok(Vec::new())
            }
            ModelStreamEvent::ToolUseInputDelta {
                ref id,
                ref partial_json,
            } if self.call_id.as_ref() == Some(id) => {
                Self::append(&mut self.json, partial_json)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ToolUseStop { ref id } if self.call_id.as_ref() == Some(id) => {
                self.call_complete = true;
                Ok(Vec::new())
            }
            ModelStreamEvent::ToolUseStart {
                ref id,
                ref name,
                ref thought_signature,
                ..
            } => {
                if self.call_id.is_some() {
                    return Err(structured_output_error(
                        "an executable tool was mixed with a formatting call",
                    ));
                }
                if !self.allowed_tools.contains(name)
                    || self.original_tool_choice == Some(ToolChoice::None)
                    || matches!(&self.original_tool_choice, Some(ToolChoice::Tool { name: required }) if required != name)
                {
                    return Err(structured_output_error(
                        "the provider returned a tool forbidden by the request",
                    ));
                }
                if !self.user_calls.insert(id.clone()) {
                    return Err(structured_output_error("a tool call identity was repeated"));
                }
                let units = id
                    .as_utf16()
                    .len()
                    .saturating_add(name.as_utf16().len())
                    .saturating_add(
                        thought_signature
                            .as_ref()
                            .map_or(0, |value| value.as_utf16().len()),
                    );
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ToolUseInputDelta {
                ref id,
                ref partial_json,
            } => {
                if !self.user_calls.contains(id) || self.completed_user_calls.contains(id) {
                    return Err(structured_output_error(
                        "tool arguments did not belong to an active declared call",
                    ));
                }
                let units = id
                    .as_utf16()
                    .len()
                    .saturating_add(partial_json.as_utf16().len());
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ToolUseStop { ref id } => {
                if !self.user_calls.contains(id) || !self.completed_user_calls.insert(id.clone()) {
                    return Err(structured_output_error(
                        "a tool completion did not belong to an active declared call",
                    ));
                }
                let units = id.as_utf16().len();
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ThinkingBlock {
                ref text,
                ref signature,
                ref encrypted_content,
                ..
            } => {
                let units = text
                    .as_utf16()
                    .len()
                    .saturating_add(signature.as_ref().map_or(0, |value| value.as_utf16().len()))
                    .saturating_add(
                        encrypted_content
                            .as_ref()
                            .map_or(0, |value| value.as_utf16().len()),
                    );
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::RedactedThinkingBlock { ref data } => {
                let units = data.as_utf16().len();
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::TextMessageCompleted {
                ref id,
                ref phase,
                ref text,
            } => {
                let units = id
                    .len_utf16()
                    .saturating_add(text.len_utf16())
                    .saturating_add(phase.as_ref().map_or(0, RuntimeString::len_utf16));
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::TextMessage { ref id, ref phase } => {
                let units = id
                    .as_utf16()
                    .len()
                    .saturating_add(phase.as_ref().map_or(0, |value| value.as_utf16().len()));
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::TextDelta { ref text } => {
                Self::append(&mut self.text, text)?;
                let units = text.as_utf16().len();
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ThinkingDelta { ref text } => {
                let units = text.as_utf16().len();
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::ServerSearchQuery {
                ref id,
                ref query,
                ref action,
            } => {
                if !self.hosted_search_allowed {
                    return Err(structured_output_error(
                        "the provider returned a hosted search forbidden by the request",
                    ));
                }
                let optional_units = |value: &Option<RuntimeString>| {
                    value.as_ref().map_or(0, |value| value.as_utf16().len())
                };
                let action_units = match action {
                    Some(WebSearchAction::Search { query, queries }) => queries
                        .as_ref()
                        .map_or(0, |queries| {
                            queries.iter().fold(0_usize, |units, query| {
                                units.saturating_add(query.as_utf16().len())
                            })
                        })
                        .saturating_add(optional_units(query)),
                    Some(WebSearchAction::OpenPage { url }) => optional_units(url),
                    Some(WebSearchAction::FindInPage { url, pattern }) => {
                        optional_units(url).saturating_add(optional_units(pattern))
                    }
                    Some(WebSearchAction::Other) | None => 0,
                };
                let units = id
                    .as_utf16()
                    .len()
                    .saturating_add(query.as_utf16().len())
                    .saturating_add(action_units);
                self.hosted_search_seen = true;
                self.buffer(event, units)?;
                Ok(Vec::new())
            }
            ModelStreamEvent::MessageStop { stop_reason, usage } => {
                let mut events = Vec::new();
                if !matches!(
                    stop_reason,
                    ModelStopReason::EndTurn | ModelStopReason::ToolUse
                ) {
                    events.extend(self.take_pending(false));
                    events.push(ModelStreamEvent::MessageStop { stop_reason, usage });
                    return Ok(events);
                }
                if !self.user_calls.is_empty() {
                    if self.user_calls != self.completed_user_calls {
                        return Err(structured_output_error(
                            "the response ended before its tool calls completed",
                        ));
                    }
                    // Calls and replay state remain provisional until the whole
                    // response has been accepted, including any late errors.
                    events.extend(self.take_pending(true));
                    events.push(ModelStreamEvent::MessageStop { stop_reason, usage });
                    return Ok(events);
                }
                if matches!(self.original_tool_choice, Some(ToolChoice::Tool { .. }))
                    || self.original_tool_choice == Some(ToolChoice::Any)
                        && !self.hosted_search_seen
                {
                    return Err(structured_output_error(
                        "the provider omitted a required user tool call",
                    ));
                }
                let text = if self.call_id.is_some() {
                    if !self.call_complete {
                        return Err(structured_output_error(
                            "the formatting call did not finish",
                        ));
                    }
                    &self.json
                } else {
                    &self.text
                };
                if runtime_json::parse(text).is_err() {
                    return Err(structured_output_error(
                        "the final result was not valid JSON",
                    ));
                }
                let text = text.clone();
                // Formatting calls are replaced by JSON text, so their signed
                // provider history no longer describes the persisted answer.
                if self.call_id.is_some() {
                    events.extend(self.take_pending(false));
                    events.push(ModelStreamEvent::TextDelta { text });
                } else {
                    events.extend(self.take_pending(true));
                }
                events.push(ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage,
                });
                Ok(events)
            }
            _ => Ok(vec![event]),
        }
    }
}

fn emulate_structured_output(
    mut stream: ModelStream,
    mut state: StructuredOutputState,
    signal: AbortSignal,
) -> ModelStream {
    Box::pin(async_stream::stream! {
        while let Some(event) = stream.next().await {
            if signal.is_cancelled() { return; }
            let terminal = matches!(event, ModelStreamEvent::Error { .. } | ModelStreamEvent::MessageStop { .. });
            match state.project(event) {
                Ok(events) => {
                    for event in events {
                        if signal.is_cancelled() { return; }
                        yield event;
                    }
                }
                Err(error) => {
                    yield ModelStreamEvent::Error { error };
                    return;
                }
            }
            if terminal { return; }
        }
    })
}

// ---- Chat Completions request building ----

#[cfg(test)]
fn build_chat_params(req: &ModelRequest) -> Value {
    build_chat_params_with_identity(
        req,
        None,
        None,
        &ModelCapabilities::default(),
        OpenAiReasoningDialect::OpenAi,
    )
}

/// Reasoning-aware chat params for tests that exercise the effort/dialect path.
#[cfg(test)]
fn build_chat_params_caps(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
) -> Value {
    build_chat_params_with_identity(req, None, None, caps, dialect)
}

fn build_chat_params_with_identity(
    req: &ModelRequest,
    identity: Option<&OpenAIRequestIdentity>,
    request_id: Option<&str>,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
) -> Value {
    let structured = req.output_schema.is_some();
    let qwen_thinking =
        (dialect.uses_qwen_thinking_switch() && caps.thinking.is_some()).then_some(matches!(
            req.thinking,
            Some(ThinkingConfig::Adaptive | ThinkingConfig::Enabled { .. })
        ));
    // Explicitly disabling thinking must win over a retained/default effort.
    let effort = if qwen_thinking == Some(false) {
        None
    } else {
        raw_effort_for_request(req, caps, dialect)
    };
    let mut obj = Map::new();
    obj.insert("model", vs(req.model.clone()));
    obj.insert("stream", Value::Bool(true));
    obj.insert(
        "stream_options",
        vobj([("include_usage", Value::Bool(true))]),
    );
    // A managed gateway sizes its own reasoning budget once an effort is set, so
    // the client must not also cap output tokens; every other endpoint sends it.
    if !(dialect.manages_output_budget() && effort.is_some()) {
        obj.insert(
            dialect.output_cap_field().to_string(),
            Value::from(req.max_output_tokens),
        );
    }
    if let Some(e) = effort {
        obj.insert("reasoning_effort", vs(e));
    }
    if let Some(enabled) = qwen_thinking {
        obj.insert("enable_thinking", Value::Bool(enabled));
    }
    if dialect == OpenAiReasoningDialect::Qoder
        && matches!(req.thinking, Some(ThinkingConfig::Disabled))
    {
        obj.insert("reasoning_effort", vs("none"));
    }
    if dialect == OpenAiReasoningDialect::LlamaCpp
        && !matches!(
            req.thinking,
            Some(ThinkingConfig::Adaptive | ThinkingConfig::Enabled { .. })
        )
    {
        obj.insert("reasoning_effort", vs("none"));
    }
    // DeepSeek-style thinking: inject {"thinking": {"type": "enabled/disabled"}}.
    // DeepSeek defaults to thinking ON, so an explicit "disabled" is sent when the
    // user turns thinking off (req.thinking is None then, because session_run only
    // sets it when config.thinking=true). Gated by the endpoint dialect, not the id.
    if dialect.uses_thinking_block() {
        let thinking_type = match &req.thinking {
            Some(t) if !matches!(t, ThinkingConfig::Disabled) => "enabled",
            _ => "disabled",
        };
        obj.insert("thinking", vobj([("type", vs(thinking_type))]));
    }
    obj.insert(
        "messages",
        to_chat_messages_with_profile(
            &req.system,
            &req.messages,
            &openai_behavior_profile(&req.model),
            &req.tools,
            Some(&req.model),
        ),
    );
    if !req.tools.is_empty() {
        let tools: Vec<Value> = req.tools.iter().map(to_chat_tool).collect();
        obj.insert("tools", Value::Array(tools));
    }
    if let Some(tc) = &req.tool_choice {
        obj.insert("tool_choice", to_chat_tool_choice(tc));
    }
    if let Some(schema) = req.output_schema.as_ref().filter(|_| structured) {
        obj.insert(
            "response_format",
            vobj([
                ("type", vs("json_schema")),
                (
                    "json_schema",
                    vobj([
                        ("name", vs(STRUCTURED_OUTPUT_NAME)),
                        ("schema", schema.clone()),
                        ("strict", Value::Bool(supports_strict_output_schema(schema))),
                    ]),
                ),
            ]),
        );
    }
    if let Some(ck) = &req.cache_key {
        obj.insert("prompt_cache_key", vs(ck.clone()));
    }
    if let Some(identity) = identity {
        obj.insert("session_id", vs(identity.session_id.clone()));
        if let Some(request_id) = request_id {
            obj.insert("request_id", vs(request_id));
        }
    }
    Value::Object(obj)
}

fn to_chat_tool(t: &ToolSpec) -> Value {
    vobj([
        ("type", vs("function")),
        (
            "function",
            vobj([
                ("name", vs(t.name.clone())),
                ("description", vs(t.description.clone())),
                ("parameters", t.input_schema.clone()),
            ]),
        ),
    ])
}

fn map_chat_stop_reason(sr: &RuntimeString) -> ModelStopReason {
    if sr == "tool_calls" || sr == "function_call" {
        ModelStopReason::ToolUse
    } else if sr == "length" {
        ModelStopReason::MaxTokens
    } else if sr == "content_filter" {
        ModelStopReason::Refusal
    } else {
        ModelStopReason::EndTurn
    }
}

fn to_chat_tool_choice(tc: &ToolChoice) -> Value {
    match tc {
        ToolChoice::Auto => vs("auto"),
        ToolChoice::Any => vs("required"),
        ToolChoice::None => vs("none"),
        ToolChoice::Tool { name } => vobj([
            ("type", vs("function")),
            ("function", vobj([("name", vs(name.clone()))])),
        ]),
    }
}

#[cfg(test)]
fn to_chat_messages(system: &[SystemPromptPart], messages: &[Message]) -> Value {
    to_chat_messages_with_profile(
        system,
        messages,
        &resolve_provider_behavior_profile(None, &RuntimeString::from("model-x")),
        &[],
        None,
    )
}

fn to_chat_messages_with_profile(
    system: &[SystemPromptPart],
    messages: &[Message],
    profile: &ProviderBehaviorProfile,
    tools: &[ToolSpec],
    current_model: Option<&RuntimeString>,
) -> Value {
    let mut out: Vec<Value> = Vec::new();

    // Keep changing guidance outside the stable system prefix. It remains a
    // system message: cache placement must not turn harness instructions into
    // a new user request. The Chat wire retains its existing system role.
    let (stable, volatile) = split_system_for_cache(system);
    let system_text = join_system_parts(stable);
    if !system_text.is_empty() {
        out.push(vobj([("role", vs("system")), ("content", vs(system_text))]));
    }

    // Media attached to a tool_result must NOT interleave between the tool
    // messages of a parallel batch — every `tool` message must follow the
    // assistant tool_calls contiguously. So media from a consecutive RUN of
    // tool_results is accumulated and flushed as ONE trailing user message AFTER
    // the whole run (before the next non-tool message, or at end).
    let mut pending_media: Vec<Value> = Vec::new();
    // A tool batch is OPEN from an assistant message carrying tool_calls until
    // every one of those calls has its `tool` message. Strict Chat Completions
    // validators reject any other role inside that window, yet a session record
    // can legitimately carry an interloper: a queued user steer or an
    // elicitation answer lands between the calls and their results. Such user
    // messages are held back and emitted right after the batch closes, keeping
    // their relative order and the batch contiguous.
    let mut pending_users: Vec<Value> = Vec::new();
    // Ids of the open batch's not-yet-answered calls, in call order.
    let mut open_calls: Vec<RuntimeString> = Vec::new();
    // Every call id an emitted assistant turn actually declared. A tool result
    // whose call never reached the wire (its assistant turn was dropped as
    // empty, or a rewind/truncation cut the pair apart) has nothing to pair
    // with and strict validators reject the orphan, so it is dropped below.
    let mut answered: HashSet<RuntimeString> = HashSet::new();
    for msg in messages {
        match msg {
            Message::User(u) => {
                for (is_guidance, blocks) in harness_user_segments(&u.content) {
                    let rendered = vobj([
                        ("role", vs(if is_guidance { "system" } else { "user" })),
                        ("content", chat_user_content(blocks)),
                    ]);
                    if open_calls.is_empty() {
                        flush_deferred(&mut out, &mut pending_media, &mut pending_users);
                        out.push(rendered);
                    } else {
                        pending_users.push(rendered);
                    }
                }
            }
            Message::Assistant(a) => {
                let text = text_of_blocks(&a.content);
                let tool_calls: Vec<Value> = a
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::ToolUse(tu) => {
                            // Chat Completions requires `function.arguments` to parse as
                            // a JSON object. Freeform history carries raw text, so wrap
                            // it into the object the tool's own schema declares.
                            let arguments = objectified_tool_input(&tu.input, tools);
                            Some(vobj([
                                ("id", vs(tu.id.clone())),
                                ("type", vs("function")),
                                (
                                    "function",
                                    vobj([
                                        ("name", vs(tu.name.clone())),
                                        ("arguments", vs(runtime_json::stringify(&arguments))),
                                    ]),
                                ),
                            ]))
                        }
                        _ => None,
                    })
                    .collect();
                // An assistant turn with NEITHER text NOR tool_calls is dropped
                // rather than emitted as a bare `{"role":"assistant"}`. Chat
                // Completions validators reject an assistant message missing both
                // keys ("must contain either 'content' or 'tool_calls'"), and
                // because the rollout is append-only such a message — produced
                // when a model spends its whole output budget on reasoning and
                // returns nothing visible — would otherwise poison EVERY later
                // request in the session and wedge it on a 400. Anthropic
                // (`map_blocks` + the `!content.is_empty()` guard) and Gemini
                // (`!parts.is_empty()`) already skip it; this is the Chat
                // Completions parity fix. No tool_result can reference a dropped
                // message: results pair with tool_use ids, which live in the
                // `tool_calls` this branch keeps.
                if text.is_empty() && tool_calls.is_empty() {
                    continue;
                }
                // A second assistant message while a batch is still open means
                // the open calls never got results; settle them synthetically so
                // the new assistant message starts a clean window.
                settle_open_batch(
                    &mut out,
                    &mut pending_media,
                    &mut pending_users,
                    &mut open_calls,
                );
                let mut m = Map::new();
                m.insert("role", vs("assistant"));
                if !text.is_empty() {
                    m.insert("content", vs(text));
                }
                // Preserve returned reasoning across tool continuations on its
                // native channel. Signed or encrypted reasoning belongs to other
                // wire formats and must not be projected as plaintext.
                if current_model == Some(&a.model) {
                    let mut reasoning = Vec::new();
                    for block in &a.content {
                        if let ContentBlock::Thinking(thinking) = block {
                            if thinking.signature.is_some() || thinking.encrypted_content.is_some()
                            {
                                reasoning.clear();
                                break;
                            }
                            reasoning.extend_from_slice(thinking.text.as_utf16());
                        }
                    }
                    if !reasoning.is_empty() {
                        m.insert(
                            "reasoning_content",
                            vs(RuntimeString::from_utf16(reasoning)),
                        );
                    }
                }
                if !tool_calls.is_empty() {
                    open_calls = a
                        .content
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::ToolUse(tu) => Some(tu.id.clone()),
                            _ => None,
                        })
                        .collect();
                    answered.extend(open_calls.iter().cloned());
                    m.insert("tool_calls", Value::Array(tool_calls));
                }
                out.push(Value::Object(m));
            }
            Message::ToolResult(tr) => {
                if !answered.contains(&tr.tool_use_id) {
                    continue;
                }
                out.push(vobj([
                    ("role", vs("tool")),
                    ("tool_call_id", vs(tr.tool_use_id.clone())),
                    ("content", vs(text_of_output_blocks(&tr.content))),
                ]));
                // Tool messages accept text only. Defer output images and native
                // attachments until every result in this tool batch is emitted.
                pending_media.extend(media_to_chat_parts(&tool_result_user_blocks(tr)));
                open_calls.retain(|id| *id != tr.tool_use_id);
                if open_calls.is_empty() {
                    flush_deferred(&mut out, &mut pending_media, &mut pending_users);
                }
            }
            Message::CompactBoundary(cb) => {
                settle_open_batch(
                    &mut out,
                    &mut pending_media,
                    &mut pending_users,
                    &mut open_calls,
                );
                out.push(vobj([
                    ("role", vs("user")),
                    (
                        "content",
                        vs(render_openai_compact_boundary_exact(&cb.summary, profile)),
                    ),
                ]));
            }
            Message::System(_) => {}
        }
    }
    settle_open_batch(
        &mut out,
        &mut pending_media,
        &mut pending_users,
        &mut open_calls,
    );
    flush_deferred(&mut out, &mut pending_media, &mut pending_users);
    // Appended only after any open tool batch has been settled, so the reminder
    // can never land between an assistant turn's calls and their results.
    let collaboration: Vec<_> = volatile
        .iter()
        .copied()
        .filter(|part| part.name == "collaboration-mode")
        .collect();
    let runtime_guidance: Vec<_> = volatile
        .iter()
        .copied()
        .filter(|part| part.name != "collaboration-mode")
        .collect();
    let reminder = volatile_system_reminder(&runtime_guidance, profile);
    if !reminder.is_empty() {
        out.push(vobj([("role", vs("system")), ("content", vs(reminder))]));
    }
    for part in collaboration {
        out.push(vobj([
            ("role", vs("developer")),
            ("content", vs(part.text.clone())),
        ]));
    }
    Value::Array(out)
}

/// Close an open tool batch whose calls never received a result (an aborted or
/// dropped execution) with one synthetic `tool` message per call, then release
/// whatever was held back for the batch window.
fn settle_open_batch(
    out: &mut Vec<Value>,
    pending_media: &mut Vec<Value>,
    pending_users: &mut Vec<Value>,
    open_calls: &mut Vec<RuntimeString>,
) {
    for id in open_calls.drain(..) {
        out.push(vobj([
            ("role", vs("tool")),
            ("tool_call_id", vs(id)),
            ("content", vs("No result was recorded for this tool call.")),
        ]));
    }
    flush_deferred(out, pending_media, pending_users);
}

fn flush_deferred(
    out: &mut Vec<Value>,
    pending_media: &mut Vec<Value>,
    pending_users: &mut Vec<Value>,
) {
    flush_media(out, pending_media);
    out.append(pending_users);
}

fn flush_media(out: &mut Vec<Value>, pending: &mut Vec<Value>) {
    if !pending.is_empty() {
        out.push(vobj([
            ("role", vs("user")),
            ("content", Value::Array(std::mem::take(pending))),
        ]));
    }
}

// ---- Responses API request building ----

#[cfg(test)]
fn build_responses_params(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
) -> Result<Value, ProviderError> {
    build_responses_params_with_tool_mode(req, caps, dialect, false)
}

fn build_responses_params_with_tool_mode(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    dialect: OpenAiReasoningDialect,
    function_tools: bool,
) -> Result<Value, ProviderError> {
    let structured = req.output_schema.is_some();
    // Leave stable instructions and history unchanged when guidance changes.
    // Responses supports developer input messages, so the trailing update keeps
    // its instruction authority without pretending to be human input.
    let (stable_parts, volatile_parts) = split_system_for_cache(&req.system);
    let instructions = join_system_parts(stable_parts);
    let profile = openai_behavior_profile(&req.model);
    let mut tools: Vec<Value> = req
        .tools
        .iter()
        .map(|tool| {
            if function_tools {
                to_responses_function_tool(tool)
            } else {
                to_responses_tool(tool)
            }
        })
        .collect();

    // Native server web search: the hosted `web_search` built-in tool. OpenAI
    // accepts allowed-domain filters, user_location, and search_context_size;
    // blocked-domain filters are not supported by this API.
    if let Some(ws_req) = req
        .web_search
        .as_ref()
        .filter(|search| search.access != crate::schemas::config::WebSearchAccess::Disabled)
    {
        let mut ws = Map::new();
        ws.insert("type", vs("web_search"));
        ws.insert(
            "external_web_access",
            Value::Bool(ws_req.access != crate::schemas::config::WebSearchAccess::Cached),
        );
        if ws_req.access == crate::schemas::config::WebSearchAccess::Indexed {
            ws.insert("indexed_web_access", Value::Bool(true));
        }
        if let Some(allowed) = ws_req.allowed_domains.as_ref().filter(|d| !d.is_empty()) {
            ws.insert(
                "filters",
                vobj([(
                    "allowed_domains",
                    Value::Array(allowed.iter().cloned().map(vs).collect()),
                )]),
            );
        }
        if let Some(loc) = &ws_req.user_location {
            let mut ul = Map::new();
            ul.insert("type", vs("approximate"));
            if let Some(city) = &loc.city {
                ul.insert("city", vs(city.clone()));
            }
            if let Some(region) = &loc.region {
                ul.insert("region", vs(region.clone()));
            }
            if let Some(country) = &loc.country {
                ul.insert("country", vs(country.clone()));
            }
            if let Some(tz) = &loc.timezone {
                ul.insert("timezone", vs(tz.clone()));
            }
            ws.insert("user_location", Value::Object(ul));
        }
        if let Some(size) = ws_req.context_size {
            let size = match size {
                WebSearchContextSize::Low => "low",
                WebSearchContextSize::Medium => "medium",
                WebSearchContextSize::High => "high",
            };
            ws.insert("search_context_size", vs(size));
        }
        tools.push(Value::Object(ws));
    }

    let effort = raw_effort_for_request(req, caps, dialect);
    // On the Responses path a declared effort knob is exactly what marks a
    // reasoning model: it gets the reasoning block (summary + encrypted content
    // for stateless replay), whether or not an effort value is pinned this turn.
    let reasoning_model = caps.effort.is_some() || !caps.supported_reasoning_efforts.is_empty();
    let mut obj = Map::new();
    obj.insert("model", vs(req.model.clone()));
    obj.insert("stream", Value::Bool(true));
    // Responses is stateful server-side; we drive it stateless to mirror the chat
    // path, so never persist and always send full input each call.
    obj.insert("store", Value::Bool(false));
    obj.insert("max_output_tokens", Value::from(req.max_output_tokens));
    // Reasoning models: request a summary (visible reasoning) + the encrypted
    // reasoning blob so it can be replayed across the stateless tool loop.
    if reasoning_model {
        let mut reasoning = Map::new();
        if let Some(e) = effort {
            reasoning.insert("effort", vs(e));
        }
        if caps.supports_reasoning_summary_parameter != Some(false)
            && req.reasoning_summary != crate::types::provider::ReasoningSummary::None
        {
            reasoning.insert("summary", vs(req.reasoning_summary.as_str()));
        }
        obj.insert("reasoning", Value::Object(reasoning));
        obj.insert(
            "include",
            Value::Array(vec![vs("reasoning.encrypted_content")]),
        );
    }
    if !instructions.is_empty() {
        obj.insert("instructions", vs(instructions));
    }
    let (mut input, unresolved_guidance) = project_responses_input(
        &req.messages,
        &req.tools,
        &profile,
        Some(&req.model),
        function_tools,
    )?;
    if unresolved_guidance {
        return Err(ProviderError {
            kind: ProviderErrorKind::InvalidRequest,
            message: "Runtime guidance cannot follow tool calls that have no recorded outputs."
                .into(),
            status: Some(400),
            code: Some("runtime_guidance_unresolved_tool_calls".into()),
            error_type: None,
            request_id: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms: None,
        });
    }
    // Appended last: a reasoning item must be followed by the message it
    // produced, so nothing may be inserted into the middle of the transcript.
    let collaboration: Vec<_> = volatile_parts
        .iter()
        .copied()
        .filter(|part| part.name == "collaboration-mode")
        .collect();
    let runtime_guidance: Vec<_> = volatile_parts
        .iter()
        .copied()
        .filter(|part| part.name != "collaboration-mode")
        .collect();
    let reminder = volatile_system_reminder(&runtime_guidance, &profile);
    if !reminder.is_empty() {
        if let Value::Array(items) = &mut input {
            items.push(vobj([("role", vs("developer")), ("content", vs(reminder))]));
        }
    }
    if let Value::Array(items) = &mut input {
        for part in collaboration {
            items.push(vobj([
                ("role", vs("developer")),
                ("content", vs(part.text.clone())),
            ]));
        }
    }
    obj.insert("input", input);
    if !tools.is_empty() {
        obj.insert("tools", Value::Array(tools));
    }
    if let Some(tc) = &req.tool_choice {
        obj.insert(
            "tool_choice",
            to_responses_tool_choice(tc, &req.tools, function_tools)?,
        );
    }
    let mut text = Map::new();
    if let Some(schema) = req.output_schema.as_ref().filter(|_| structured) {
        text.insert(
            "format",
            vobj([
                ("type", vs("json_schema")),
                ("name", vs(STRUCTURED_OUTPUT_NAME)),
                ("schema", schema.clone()),
                ("strict", Value::Bool(supports_strict_output_schema(schema))),
            ]),
        );
    }
    if let (OpenAiReasoningDialect::OpenAi, Some(verbosity)) =
        (dialect, req.model_verbosity.or(caps.output_verbosity))
    {
        text.insert("verbosity", vs(verbosity.as_str()));
    }
    if !text.is_empty() {
        obj.insert("text", Value::Object(text));
    }
    if let Some(ck) = &req.cache_key {
        obj.insert("prompt_cache_key", vs(ck.clone()));
    }
    Ok(Value::Object(obj))
}

/// Engine-owned adapter replay context is scoped independently of a model id.
/// Ordinary provider encrypted reasoning remains opaque and model-scoped.
fn retain_scoped_responses_context(
    body: &mut Value,
    compatibility: Option<&OpenAICompatibilityOptions>,
    model: &RuntimeString,
) {
    let scope = compatibility.and_then(|options| {
        options
            .extra_headers()
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-klaus-opaque-replay-scope"))
            .map(|(_, value)| value.as_str())
    });
    let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };
    items.retain(|item| {
        if item
            .get("type")
            .and_then(Value::as_str)
            .is_none_or(|value| value != "reasoning")
        {
            return true;
        }
        let Some(encoded) = item.get("encrypted_content").and_then(Value::as_str) else {
            return true;
        };
        let Ok(context) = runtime_json::parse(encoded) else {
            return true;
        };
        if context
            .get("kind")
            .and_then(Value::as_str)
            .is_none_or(|value| value != "klaus.provider-context.v1")
        {
            return true;
        }
        context.get("model").and_then(Value::as_str) == Some(model)
            && scope.is_some_and(|scope| {
                context
                    .get("scope")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == scope)
            })
    });
}

/// Adapt a Responses request body for the codex-subscription backend
/// (chatgpt.com/backend-api/codex). Mirrors exactly what the desktop
/// bridge's `adaptCodexResponsesRequest` did before forwarding:
/// - drop `max_output_tokens`: the backend rejects it with
///   `{"detail":"Unsupported parameter: max_output_tokens"}`.
///
/// The standard OpenAI Responses path never goes through this function.
fn adapt_codex_responses_params(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.remove("max_output_tokens");
}

fn to_responses_function_tool(t: &ToolSpec) -> Value {
    vobj([
        ("type", vs("function")),
        ("name", vs(t.name.clone())),
        ("description", vs(t.description.clone())),
        ("parameters", t.input_schema.clone()),
        ("strict", Value::Bool(false)),
    ])
}

fn to_responses_tool(t: &ToolSpec) -> Value {
    match t.kind {
        ToolSpecKind::Function => to_responses_function_tool(t),
        ToolSpecKind::Freeform => {
            let mut tool = Map::new();
            tool.insert("type", vs("custom"));
            tool.insert("name", vs(t.name.clone()));
            tool.insert("description", vs(t.description.clone()));
            if let Some(format) = &t.format {
                tool.insert("format", responses_custom_format(format));
            }
            Value::Object(tool)
        }
        ToolSpecKind::ToolSearch => vobj([
            ("type", vs("tool_search")),
            // OpenAI's Responses API only accepts `server` or `client` here.
            // The engine runs the search itself and answers `tool_search_call`
            // items with `tool_search_output`, which is the client-execution
            // protocol, so the internal `sync` dialect value must never reach
            // the wire.
            ("execution", vs("client")),
            ("description", vs(t.description.clone())),
            ("parameters", t.input_schema.clone()),
        ]),
    }
}

fn responses_custom_format(format: &ToolSpecFormat) -> Value {
    match format {
        ToolSpecFormat::Text => vobj([("type", vs("text"))]),
        ToolSpecFormat::Grammar { syntax, definition } => vobj([
            ("type", vs("grammar")),
            (
                "syntax",
                vs(match syntax {
                    ToolSpecGrammarSyntax::Lark => "lark",
                    ToolSpecGrammarSyntax::Regex => "regex",
                }),
            ),
            ("definition", vs(definition.clone())),
        ]),
    }
}

fn map_review_responses_stop_reason(response: &Value) -> ModelStopReason {
    if response.get("end_turn").and_then(Value::as_bool) == Some(false)
        || response
            .get("incomplete_details")
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str)
            .is_some_and(|reason| reason == "interrupted")
    {
        return ModelStopReason::Continue;
    }
    map_responses_stop_reason(response)
}

fn map_responses_stop_reason(response: &Value) -> ModelStopReason {
    // A function call left in the output means the model wants tools executed.
    if let Some(output) = response.get("output").and_then(|o| o.as_array()) {
        if output.iter().any(|o| {
            o.get("type").and_then(|t| t.as_str()).is_some_and(|kind| {
                kind == "function_call" || kind == "custom_tool_call" || kind == "tool_search_call"
            })
        }) {
            return ModelStopReason::ToolUse;
        }
    }
    ModelStopReason::EndTurn
}

fn incomplete_responses_error(reason: Option<&RuntimeString>) -> ProviderError {
    let reason = reason.cloned().unwrap_or_else(|| "unknown".into());
    ProviderError {
        kind: ProviderErrorKind::Network,
        message: RuntimeString::from("Incomplete response returned, reason: ").concat(&reason),
        status: None,
        code: Some("response_incomplete".into()),
        error_type: Some(reason),
        request_id: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
    }
}

fn persisted_call_kind(
    call: &crate::schemas::content::ToolUseBlock,
    tools: &[ToolSpec],
) -> ProviderToolCallKind {
    call.tool_kind.unwrap_or_else(|| {
        tools
            .iter()
            .find(|tool| tool.name == call.name)
            .map(|tool| match tool.kind {
                ToolSpecKind::Function => ProviderToolCallKind::Function,
                ToolSpecKind::Freeform => ProviderToolCallKind::Custom,
                ToolSpecKind::ToolSearch => ProviderToolCallKind::ToolSearch,
            })
            .unwrap_or(ProviderToolCallKind::Function)
    })
}

fn custom_tool_input_string(input: &Value) -> RuntimeString {
    if let Some(s) = input.as_str() {
        return s.clone();
    }
    runtime_json::stringify(input)
}

fn responses_tool_search_output(native_tool_output: Value) -> Value {
    let tools = native_tool_output
        .as_object()
        .and_then(|output| output.get("tools"))
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .filter_map(to_responses_loadable_tool)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    vobj([("tools", Value::Array(tools))])
}

fn to_responses_loadable_tool(tool: &Value) -> Option<Value> {
    let object = tool.as_object()?;
    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "namespace")
    {
        return to_responses_loadable_namespace(object);
    }
    if object
        .get("kind")
        .is_some_and(|kind| !kind.is_null() && kind.as_str().is_none_or(|kind| kind != "function"))
    {
        return None;
    }
    to_responses_loadable_function(tool)
}

fn to_responses_loadable_namespace(object: &Map) -> Option<Value> {
    let name = object.get("name")?.as_str()?;
    let child_tools = object
        .get("tools")?
        .as_array()?
        .iter()
        .filter_map(to_responses_loadable_function)
        .collect::<Vec<_>>();
    if child_tools.is_empty() {
        return None;
    }
    let description = object
        .get("description")
        .and_then(Value::as_str)
        .cloned()
        .unwrap_or_else(|| {
            RuntimeString::from("Tools in the ")
                .concat(name)
                .concat(&RuntimeString::from(" namespace."))
        });
    Some(vobj([
        ("type", vs("namespace")),
        ("name", vs(name.clone())),
        ("description", vs(description)),
        ("tools", Value::Array(child_tools)),
    ]))
}

fn to_responses_loadable_function(tool: &Value) -> Option<Value> {
    let object = tool.as_object()?;
    let name = object.get("name")?.as_str()?;
    let description = object.get("description")?.as_str()?;
    let parameters = match object.get("inputSchema") {
        Some(Value::Null) | None => object.get("parameters")?,
        Some(schema) => schema,
    };
    if !parameters.is_object() {
        return None;
    }
    Some(vobj([
        ("type", vs("function")),
        ("name", vs(name.clone())),
        ("description", vs(description.clone())),
        ("strict", Value::Bool(false)),
        ("defer_loading", Value::Bool(true)),
        ("parameters", parameters.clone()),
    ]))
}

fn to_responses_tool_choice(
    tc: &ToolChoice,
    tools: &[ToolSpec],
    function_tools: bool,
) -> Result<Value, ProviderError> {
    match tc {
        ToolChoice::Auto => Ok(vs("auto")),
        ToolChoice::Any => Ok(vs("required")),
        ToolChoice::None => Ok(vs("none")),
        ToolChoice::Tool { name } if function_tools => {
            if let Some(error) =
                validate_exact_tool_choice("OpenAI Responses", Some(tc), tools, false)
            {
                return Err(error);
            }
            Ok(vobj([("type", vs("function")), ("name", vs(name.clone()))]))
        }
        ToolChoice::Tool { name } => responses_forced_choice(name, tools),
    }
}

#[cfg(test)]
fn to_responses_input(messages: &[Message], tools: &[ToolSpec]) -> Value {
    to_responses_input_with_profile(
        messages,
        tools,
        &resolve_provider_behavior_profile(None, &RuntimeString::from("model-x")),
    )
}

#[cfg(test)]
pub(crate) fn to_responses_input_with_profile(
    messages: &[Message],
    tools: &[ToolSpec],
    profile: &ProviderBehaviorProfile,
) -> Value {
    project_responses_input(messages, tools, profile, None, false)
        .expect("native tool projection is infallible")
        .0
}

fn responses_function_tool_input(
    input: &Value,
    name: &RuntimeString,
    tools: &[ToolSpec],
) -> Result<Value, ProviderError> {
    if input.is_object() {
        return Ok(input.clone());
    }
    let property = responses_raw_string_property(name, tools);
    if let (Some(property), Some(_)) = (property, input.as_str()) {
        let mut object = Map::new();
        object.insert(property, input.clone());
        return Ok(Value::Object(object));
    }
    Err(ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: "Responses function tool history requires the matching tool's unambiguous string input schema.".into(),
        status: Some(400),
        code: Some("responses_tool_input_unrepresentable".into()),
        error_type: None,
        request_id: None,
        media_origins: None,
        media_repair_complete: false, credential_recovery: None,
        retry_after_ms: None,
    })
}

fn responses_raw_string_property(
    name: &RuntimeString,
    tools: &[ToolSpec],
) -> Option<RuntimeString> {
    tools
        .iter()
        .find(|tool| &tool.name == name)
        .and_then(|tool| {
            let required = tool.input_schema.get("required")?.as_array()?;
            if required.len() != 1 {
                return None;
            }
            let mut strings = tool
                .input_schema
                .get("properties")?
                .as_object()?
                .entries()
                .filter_map(|(key, schema)| {
                    (schema
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|ty| ty == "string")
                        && required.iter().any(|field| field.as_str() == Some(&key)))
                    .then_some(key)
                });
            let property = strings.next()?;
            strings.next().is_none().then_some(property)
        })
}

fn inferred_custom_tool_input(
    call: &crate::schemas::content::ToolUseBlock,
    tools: &[ToolSpec],
) -> RuntimeString {
    // Function-schema history can return to a native custom-tool endpoint.
    // Unwrap only the exact single property declared by that same tool.
    if call.tool_kind.is_none() {
        if let (Some(object), Some(property)) = (
            call.input.as_object(),
            responses_raw_string_property(&call.name, tools),
        ) {
            if object.len() == 1 {
                if let Some(text) = object.get(&property).and_then(Value::as_str) {
                    return text.clone();
                }
            }
        }
    }
    custom_tool_input_string(&call.input)
}

fn project_responses_input(
    messages: &[Message],
    tools: &[ToolSpec],
    profile: &ProviderBehaviorProfile,
    current_model: Option<&RuntimeString>,
    function_tools: bool,
) -> Result<(Value, bool), ProviderError> {
    let mut out: Vec<Value> = Vec::new();
    let mut tool_use_kinds: HashMap<RuntimeString, ProviderToolCallKind> = HashMap::new();
    let mut open_calls: HashSet<RuntimeString> = HashSet::new();
    let mut pending_guidance = Vec::new();
    for msg in messages {
        match msg {
            Message::User(u) => {
                for (is_guidance, blocks) in harness_user_segments(&u.content) {
                    let rendered = vobj([
                        ("role", vs(if is_guidance { "developer" } else { "user" })),
                        ("content", responses_user_content(blocks)),
                    ]);
                    if is_guidance && !open_calls.is_empty() {
                        pending_guidance.push(rendered);
                    } else {
                        out.push(rendered);
                    }
                }
            }
            Message::Assistant(a) => {
                // Replay items in CONTENT (stream) order: a reasoning item is
                // immediately followed by the message/function_call it produced.
                // DON'T hoist the text — a hoisted-text-first layout leaves a
                // tool-less turn's reasoning trailing → orphaned. Contiguous text
                // coalesces into one assistant message item, flushed at its position.
                let mut text_buf = RuntimeString::default();
                let mut text_phase: Option<RuntimeString> = None;
                let mut text_message_id: Option<RuntimeString> = None;
                let blocks = &a.content;
                for (i, b) in blocks.iter().enumerate() {
                    match b {
                        ContentBlock::Text(t) => {
                            if text_phase != t.phase || text_message_id != t.message_id {
                                flush_text(&mut out, &mut text_buf, text_phase.as_ref());
                                text_phase = t.phase.clone();
                                text_message_id = t.message_id.clone();
                            }
                            text_buf = text_buf.concat(&t.text);
                        }
                        ContentBlock::Thinking(th) => {
                            let enc = th
                                .encrypted_content
                                .as_ref()
                                .filter(|s| !s.is_empty())
                                .filter(|_| current_model.is_none_or(|model| model == &a.model));
                            if let Some(enc) = enc {
                                // Only replay a reasoning item FOLLOWED (in this turn) by the
                                // message/function_call it produced — a trailing orphan would 400.
                                let followed = blocks[i + 1..].iter().any(|x| match x {
                                    ContentBlock::ToolUse(_) => true,
                                    ContentBlock::Text(t) => !t.text.is_empty(),
                                    _ => false,
                                });
                                if !followed {
                                    continue;
                                }
                                flush_text(&mut out, &mut text_buf, text_phase.as_ref());
                                let summary = if th.text.is_empty() {
                                    Value::Array(Vec::new())
                                } else {
                                    Value::Array(vec![vobj([
                                        ("type", vs("summary_text")),
                                        ("text", vs(th.text.clone())),
                                    ])])
                                };
                                out.push(vobj([
                                    ("type", vs("reasoning")),
                                    ("summary", summary),
                                    ("encrypted_content", vs(enc.clone())),
                                ]));
                            }
                        }
                        ContentBlock::ToolUse(tu) => {
                            flush_text(&mut out, &mut text_buf, text_phase.as_ref());
                            open_calls.insert(tu.id.clone());
                            let kind = if function_tools {
                                ProviderToolCallKind::Function
                            } else {
                                persisted_call_kind(tu, tools)
                            };
                            tool_use_kinds.insert(tu.id.clone(), kind);
                            match kind {
                                ProviderToolCallKind::Function => out.push(vobj([
                                    ("type", vs("function_call")),
                                    ("call_id", vs(tu.id.clone())),
                                    ("name", vs(tu.name.clone())),
                                    (
                                        "arguments",
                                        vs(runtime_json::stringify(&if function_tools {
                                            responses_function_tool_input(
                                                &tu.input, &tu.name, tools,
                                            )?
                                        } else {
                                            tu.input.clone()
                                        })),
                                    ),
                                ])),
                                ProviderToolCallKind::Custom => out.push(vobj([
                                    ("type", vs("custom_tool_call")),
                                    ("call_id", vs(tu.id.clone())),
                                    ("name", vs(tu.name.clone())),
                                    ("input", vs(inferred_custom_tool_input(tu, tools))),
                                ])),
                                ProviderToolCallKind::ToolSearch => out.push(vobj([
                                    ("type", vs("tool_search_call")),
                                    ("call_id", vs(tu.id.clone())),
                                    ("execution", vs("client")),
                                    ("arguments", tu.input.clone()),
                                ])),
                            }
                        }
                        _ => {}
                    }
                }
                flush_text(&mut out, &mut text_buf, text_phase.as_ref());
            }
            Message::ToolResult(tr) => {
                let kind = if function_tools {
                    ProviderToolCallKind::Function
                } else {
                    tr.tool_kind
                        .or_else(|| tool_use_kinds.get(&tr.tool_use_id).copied())
                        .unwrap_or(ProviderToolCallKind::Function)
                };
                match kind {
                    ProviderToolCallKind::Function => out.push(vobj([
                        ("type", vs("function_call_output")),
                        ("call_id", vs(tr.tool_use_id.clone())),
                        (
                            "output",
                            vs(if function_tools {
                                let text = text_of_output_blocks(&tr.content);
                                if let Some(native) = &tr.native_tool_output {
                                    text.concat(&RuntimeString::from("\n\nNative tool output:\n"))
                                        .concat(&runtime_json::stringify(native))
                                } else {
                                    text
                                }
                            } else {
                                text_of_output_blocks(&tr.content)
                            }),
                        ),
                    ])),
                    ProviderToolCallKind::Custom => out.push(vobj([
                        ("type", vs("custom_tool_call_output")),
                        ("call_id", vs(tr.tool_use_id.clone())),
                        ("output", vs(text_of_output_blocks(&tr.content))),
                    ])),
                    ProviderToolCallKind::ToolSearch => {
                        let filtered = responses_tool_search_output(
                            tr.native_tool_output.clone().unwrap_or(Value::Null),
                        );
                        out.push(vobj([
                            ("type", vs("tool_search_output")),
                            ("call_id", vs(tr.tool_use_id.clone())),
                            ("status", vs("completed")),
                            ("execution", vs("client")),
                            ("tools", filtered["tools"].clone()),
                        ]));
                    }
                }
                // A native PDF (or image) rides a SEPARATE user input item after the
                // function_call_output — `input_file`/`input_image` belong on a user
                // message, not a tool output.
                let media = media_to_responses_parts(&tool_result_user_blocks(tr));
                if !media.is_empty() {
                    out.push(vobj([
                        ("role", vs("user")),
                        ("content", Value::Array(media)),
                    ]));
                }
                open_calls.remove(&tr.tool_use_id);
                if open_calls.is_empty() {
                    out.append(&mut pending_guidance);
                }
            }
            Message::CompactBoundary(cb) => {
                if let Some(checkpoint) = &cb.native_compaction {
                    remote_compaction::validate_checkpoint(checkpoint)?;
                    out.extend(checkpoint.retained_input.clone());
                    out.push(vobj([
                        ("type", vs("compaction")),
                        (
                            "encrypted_content",
                            vs(checkpoint.encrypted_content.clone()),
                        ),
                    ]));
                    continue;
                }
                out.push(vobj([
                    ("role", vs("user")),
                    (
                        "content",
                        vs(render_openai_compact_boundary_exact(&cb.summary, profile)),
                    ),
                ]));
            }
            Message::System(_) => {}
        }
    }
    // The fallible request builder rejects an instruction trapped behind
    // unanswered calls; never fabricate outputs or lower it to a user role.
    let unresolved_guidance = !pending_guidance.is_empty();
    Ok((Value::Array(out), unresolved_guidance))
}

fn openai_behavior_profile(model: &RuntimeString) -> ProviderBehaviorProfile {
    resolve_provider_behavior_profile(
        Some(&ProviderInfo {
            name: "openai".into(),
            base_url: None,
            env_key: None,
            default_model: model.clone(),
        }),
        model,
    )
}

#[cfg(test)]
pub(crate) fn render_openai_compact_boundary(
    summary: &str,
    profile: &ProviderBehaviorProfile,
) -> String {
    render_provider_text_envelope(
        ProviderTextEnvelopeKind::CompactBoundary,
        summary.into(),
        profile,
        Default::default(),
    )
    .try_to_utf8()
    .expect("scalar compact-boundary input renders scalar text")
}

fn render_openai_compact_boundary_exact(
    summary: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let (prefix, suffix) = match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => (
            "<system-reminder>\nEarlier conversation summary:\n",
            "\n</system-reminder>",
        ),
        ProviderPromptStyle::OpenAiDeveloper => ("# Earlier Conversation Summary\n\n", ""),
        ProviderPromptStyle::Generic => ("[Earlier conversation summarized]\n", ""),
    };
    RuntimeString::from(prefix)
        .concat(summary)
        .concat(&RuntimeString::from(suffix))
}

fn flush_text(out: &mut Vec<Value>, text_buf: &mut RuntimeString, phase: Option<&RuntimeString>) {
    if !text_buf.is_empty() {
        let mut message = vobj([
            ("role", vs("assistant")),
            ("content", vs(std::mem::take(text_buf))),
        ]);
        if let Some(phase) = phase {
            message["phase"] = vs(phase.clone());
        }
        out.push(message);
    }
}

// ---- shared content helpers ----

/// Keep trusted runtime text at its historical position without promoting its
/// user/media neighbours. Only structural provenance identifies guidance;
/// meta-message flags, sub-agent provenance, and literal tags do not.
fn harness_user_segments(blocks: &[ContentBlock]) -> Vec<(bool, &[ContentBlock])> {
    let is_guidance = |block: &ContentBlock| matches!(block, ContentBlock::Text(text) if text.harness_envelope == Some(HarnessEnvelope::SystemReminder));
    let mut segments = Vec::new();
    let mut start = 0;
    let mut guidance = blocks.first().is_some_and(is_guidance);
    for (index, block) in blocks.iter().enumerate().skip(1) {
        let next = is_guidance(block);
        if next != guidance {
            segments.push((guidance, &blocks[start..index]));
            start = index;
            guidance = next;
        }
    }
    // Preserve even an empty original user message rather than changing the
    // established empty-content projection as a side effect of segmentation.
    segments.push((guidance, &blocks[start..]));
    segments
}

/// Split a request's system parts into the region a provider prefix cache can
/// hold and the volatile remainder that must not sit inside it.
///
/// A part is volatile only when it DECLARES itself so, with `cacheable: Some(false)`.
/// That is the marker the engine's per-request reminders carry — the tracked
/// plan, the external-edit scan, the loop-recovery nudge, the effort policy —
/// and it is the only marker this split acts on. An unmarked part
/// (`cacheable: None`) is left where it sits: one-shot auxiliary requests build
/// their whole system prompt from unmarked parts, and a provider-behavior rules
/// block is unmarked but stable, so demoting either would change what those
/// models read for no cache benefit.
///
/// Keep the initial instructions and existing conversation byte-stable by
/// delivering declared-volatile guidance after them. Cacheability controls
/// placement only: callers retain a protocol-native instruction role for the
/// update, independently of the model's text-envelope style.
fn split_system_for_cache(
    system: &[SystemPromptPart],
) -> (Vec<&SystemPromptPart>, Vec<&SystemPromptPart>) {
    let mut stable = Vec::new();
    let mut volatile = Vec::new();
    for part in system {
        if part.cacheable == Some(false) || part.name == "collaboration-mode" {
            volatile.push(part);
        } else {
            stable.push(part);
        }
    }
    (stable, volatile)
}

fn join_system_parts<'a>(parts: impl IntoIterator<Item = &'a SystemPromptPart>) -> RuntimeString {
    let mut joined = RuntimeString::default();
    let mut first = true;
    for part in parts {
        if !first {
            joined = joined.concat(&RuntimeString::from("\n\n"));
        }
        first = false;
        joined = joined.concat(&part.text);
    }
    joined
}

/// Render changing guidance for the active task without replacing its goal.
/// Callers send this in a system/developer message, never as human input.
/// Empty input stays empty so ordinary turns gain no synthetic message.
fn volatile_system_reminder(
    volatile: &[&SystemPromptPart],
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let text = join_system_parts(volatile.iter().copied());
    if text.is_empty() {
        return text;
    }
    let text = RuntimeString::from(
        "Harness runtime guidance for the active task, not a new user request. \
         Continue working toward the user's goal under these instructions. \
         Guidance about final replies applies when you are ready to finish the task.\n\n",
    )
    .concat(&text);
    render_provider_text_envelope(
        ProviderTextEnvelopeKind::SystemReminder,
        text,
        profile,
        ProviderTextEnvelopeOptions { tool_name: None },
    )
}

fn text_of_blocks(blocks: &[ContentBlock]) -> RuntimeString {
    let mut text = RuntimeString::default();
    for b in blocks {
        if let ContentBlock::Text(t) = b {
            text = text.concat(&t.text);
        }
    }
    text
}

fn text_of_output_blocks(blocks: &[OutputBlock]) -> RuntimeString {
    let mut text = RuntimeString::default();
    for b in blocks {
        if let OutputBlock::Text(t) = b {
            text = text.concat(&t.text);
        }
    }
    text
}

fn chat_user_content(blocks: &[ContentBlock]) -> Value {
    if !blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Image(_) | ContentBlock::Document(_)))
    {
        return Value::String(text_of_blocks(blocks));
    }

    let mut parts = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::Text(text) if !text.text.is_empty() => {
                parts.push(vobj([
                    ("type", vs("text")),
                    ("text", vs(text.text.clone())),
                ]));
            }
            ContentBlock::Image(_) | ContentBlock::Document(_) => {
                parts.extend(media_to_chat_parts(std::slice::from_ref(block)));
            }
            _ => {}
        }
    }
    Value::Array(parts)
}

fn responses_user_content(blocks: &[ContentBlock]) -> Value {
    if !blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Image(_) | ContentBlock::Document(_)))
    {
        return Value::String(text_of_blocks(blocks));
    }

    let mut parts = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::Text(text) if !text.text.is_empty() => {
                parts.push(vobj([
                    ("type", vs("input_text")),
                    ("text", vs(text.text.clone())),
                ]));
            }
            ContentBlock::Image(_) | ContentBlock::Document(_) => {
                parts.extend(media_to_responses_parts(std::slice::from_ref(block)));
            }
            _ => {}
        }
    }
    Value::Array(parts)
}

/// base64 → the data URL the API expects for inline image/file content.
fn data_url(mime: &RuntimeString, base64: &RuntimeString) -> RuntimeString {
    RuntimeString::from("data:")
        .concat(mime)
        .concat(&RuntimeString::from(";base64,"))
        .concat(base64)
}

/// Map top-level media blocks (a native PDF `document`, or an image) to Chat
/// user-content parts. A PDF is accepted only as an `input file` on a USER
/// message. Non-media blocks are skipped.
fn media_to_chat_parts(blocks: &[ContentBlock]) -> Vec<Value> {
    let mut parts = Vec::new();
    for b in blocks {
        match b {
            ContentBlock::Image(img) => {
                let mut image = Map::new();
                image.insert("url", vs(data_url(&img.mime_type, &img.base64)));
                if let Some(detail) = img.detail {
                    image.insert("detail", vs(detail.as_str()));
                }
                parts.push(vobj([
                    ("type", vs("image_url")),
                    ("image_url", Value::Object(image)),
                ]));
            }
            ContentBlock::Document(doc) => {
                let mut file = Map::new();
                file.insert("file_data", vs(data_url(&doc.mime_type, &doc.base64)));
                if let Some(fname) = &doc.filename {
                    file.insert("filename", vs(fname.clone()));
                }
                parts.push(vobj([("type", vs("file")), ("file", Value::Object(file))]));
            }
            _ => {}
        }
    }
    parts
}

/// Responses-surface counterpart of [`media_to_chat_parts`]
/// (`input_image` / `input_file`).
fn media_to_responses_parts(blocks: &[ContentBlock]) -> Vec<Value> {
    let mut parts = Vec::new();
    for b in blocks {
        match b {
            ContentBlock::Image(img) => parts.push(vobj([
                ("type", vs("input_image")),
                ("image_url", vs(data_url(&img.mime_type, &img.base64))),
                (
                    "detail",
                    vs(img.detail.map_or("auto", |detail| detail.as_str())),
                ),
            ])),
            ContentBlock::Document(doc) => {
                let mut m = Map::new();
                m.insert("type", vs("input_file"));
                m.insert("file_data", vs(data_url(&doc.mime_type, &doc.base64)));
                if let Some(fname) = &doc.filename {
                    m.insert("filename", vs(fname.clone()));
                }
                parts.push(Value::Object(m));
            }
            _ => {}
        }
    }
    parts
}

// ---- SSE framing (pure, testable) ----

/// One decoded Server-Sent-Event: the optional `event:` name and the joined
/// `data:` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SseEvent {
    #[allow(dead_code)]
    event: Option<RuntimeString>,
    pub(super) data: RuntimeString,
}

pub(super) struct SseDecoder {
    bytes: Vec<u8>,
    decoder: WhatwgUtf8,
}

impl SseDecoder {
    pub(super) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            decoder: WhatwgUtf8::new(),
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(super) fn next_event(&mut self) -> Option<SseEvent> {
        let (block_end, consume) = find_event_boundary(&self.bytes)?;
        let framed = self.bytes.drain(..consume).collect::<Vec<_>>();
        let decoded = self.decoder.feed(&framed);
        let separator_len = consume - block_end;
        let block = decoded.slice_utf16(..decoded.len_utf16().saturating_sub(separator_len));
        Some(parse_sse_block(&block))
    }

    pub(super) fn buffered_bytes(&self) -> usize {
        self.bytes.len()
    }
}

/// Find the first event boundary (a blank line) in `buf`, returning the index at
/// which the event block ends and the index past the boundary to consume to.
/// Handles LF LF, CRLF CRLF, and CR CR terminators. Returns `None` when no
/// complete boundary is buffered yet (wait for more bytes).
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let n = buf.len();
    let mut i = 0;
    while i < n {
        if i + 3 < n
            && buf[i] == b'\r'
            && buf[i + 1] == b'\n'
            && buf[i + 2] == b'\r'
            && buf[i + 3] == b'\n'
        {
            return Some((i, i + 4));
        }
        if i + 1 < n && buf[i] == b'\n' && buf[i + 1] == b'\n' {
            return Some((i, i + 2));
        }
        if i + 1 < n && buf[i] == b'\r' && buf[i + 1] == b'\r' {
            return Some((i, i + 2));
        }
        i += 1;
    }
    None
}

/// Decode one SSE event block (the bytes before a boundary) into its fields.
/// `data:` lines are joined with `\n`; a single leading space after the colon is
/// stripped; comment lines (`:`) and unknown fields are ignored.
fn parse_sse_block(block: &RuntimeString) -> SseEvent {
    const EVENT_PREFIX: [u16; 6] = [
        b'e' as u16,
        b'v' as u16,
        b'e' as u16,
        b'n' as u16,
        b't' as u16,
        b':' as u16,
    ];
    const DATA_PREFIX: [u16; 5] = [
        b'd' as u16,
        b'a' as u16,
        b't' as u16,
        b'a' as u16,
        b':' as u16,
    ];
    let units = block.as_utf16();
    let mut event: Option<RuntimeString> = None;
    let mut data = RuntimeString::default();
    let mut has_data = false;
    let mut start = 0;
    let mut index = 0;
    while index <= units.len() {
        let at_end = index == units.len();
        let is_break =
            !at_end && (units[index] == u16::from(b'\n') || units[index] == u16::from(b'\r'));
        if !at_end && !is_break {
            index += 1;
            continue;
        }
        let line = &units[start..index];
        if !line.is_empty() && line.first() != Some(&u16::from(b':')) {
            if let Some(mut rest) = line.strip_prefix(&EVENT_PREFIX) {
                if rest.first() == Some(&u16::from(b' ')) {
                    rest = &rest[1..];
                }
                event = Some(RuntimeString::from_utf16(rest.to_vec()));
            } else if let Some(mut rest) = line.strip_prefix(&DATA_PREFIX) {
                if rest.first() == Some(&u16::from(b' ')) {
                    rest = &rest[1..];
                }
                if has_data {
                    data = data.concat(&RuntimeString::from("\n"));
                }
                data = data.concat(&RuntimeString::from_utf16(rest.to_vec()));
                has_data = true;
            }
        }
        if at_end {
            break;
        }
        if units[index] == u16::from(b'\r') && units.get(index + 1) == Some(&u16::from(b'\n')) {
            index += 1;
        }
        index += 1;
        start = index;
    }
    SseEvent { event, data }
}

// ---- SSE → ModelStreamEvent mapping (pure, testable) ----

struct ChatToolCall {
    index: u64,
    id: RuntimeString,
    started: bool,
}

struct CumulativeChatToolCall {
    index: u64,
    id: RuntimeString,
    name: Option<RuntimeString>,
    arguments: RuntimeString,
    started: bool,
    stopped: bool,
}

/// Running state accumulated across Chat Completions chunks.
struct ChatState {
    // In-flight tool calls keyed by streaming index; insertion-ordered so
    // tool_use_stop is emitted in first-seen order.
    tool_calls: Vec<ChatToolCall>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read: u64,
    total_tokens: u64,
    stop_reason: ModelStopReason,
    started: bool,
    /// Settled by the first chunk that carries `delta` or `message`; the rest of
    /// the stream is read the same way.
    shape: Option<ChatChunkShape>,
    cumulative_text: RuntimeString,
    cumulative_visible_text: RuntimeString,
    cumulative_thinking: RuntimeString,
    cumulative_tool_calls: Vec<CumulativeChatToolCall>,
    cumulative_finished: bool,
    reasoning_field_seen: bool,
    visible_text_started: bool,
    pending_think_close: RuntimeString,
    trim_after_think_close: bool,
    /// The wire's finish marker verbatim, kept for the developer trace log.
    /// The mapped [`ModelStopReason`] above loses the distinction between
    /// "the endpoint said stop" and "the stream simply ended".
    raw_finish_reason: Option<RuntimeString>,
}

// `ModelStopReason` has no `Default`, so seed it to `end_turn` by hand.
impl Default for ChatState {
    fn default() -> Self {
        ChatState {
            tool_calls: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            cache_read: 0,
            total_tokens: 0,
            stop_reason: ModelStopReason::EndTurn,
            started: false,
            shape: None,
            cumulative_text: RuntimeString::default(),
            cumulative_visible_text: RuntimeString::default(),
            cumulative_thinking: RuntimeString::default(),
            cumulative_tool_calls: Vec::new(),
            cumulative_finished: false,
            reasoning_field_seen: false,
            visible_text_started: false,
            pending_think_close: RuntimeString::default(),
            trim_after_think_close: false,
            raw_finish_reason: None,
        }
    }
}

fn invalid_cumulative_stream(reason: &'static str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Unknown,
        message: format!("OpenAI cumulative message stream is invalid: {reason}").into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn cumulative_suffix(
    previous: &RuntimeString,
    current: &RuntimeString,
    reason: &'static str,
) -> Result<RuntimeString, ProviderError> {
    let previous = previous.as_utf16();
    let current = current.as_utf16();
    if !current.starts_with(previous) {
        return Err(invalid_cumulative_stream(reason));
    }
    Ok(RuntimeString::from_utf16(
        current[previous.len()..].to_vec(),
    ))
}

fn cumulative_finish_reason(choice: &Value) -> Option<&RuntimeString> {
    choice
        .get("finish_reason")
        .and_then(|value| value.as_str())
        .filter(|reason| !reason.is_empty() && *reason != "null")
}

struct CumulativeChatText {
    visible: RuntimeString,
    thinking: RuntimeString,
}

const THINK_CLOSE: &[u16] = &[60, 47, 116, 104, 105, 110, 107, 62];

fn strip_orphan_think_close(content: &RuntimeString) -> Option<RuntimeString> {
    let units = content.as_utf16();
    if THINK_CLOSE.starts_with(units) {
        return Some(RuntimeString::default());
    }
    if !units.starts_with(THINK_CLOSE) {
        return None;
    }
    let visible = content.slice_utf16(THINK_CLOSE.len()..);
    let start = visible
        .as_utf16()
        .iter()
        .position(|unit| !matches!(*unit, 0x0009..=0x000d | 0x0020))
        .unwrap_or(visible.len_utf16());
    Some(visible.slice_utf16(start..))
}

fn visible_chat_content(content: &RuntimeString, state: &mut ChatState) -> RuntimeString {
    if state.visible_text_started {
        return content.clone();
    }
    if !state.reasoning_field_seen {
        state.visible_text_started = true;
        return content.clone();
    }
    let buffered = state.pending_think_close.concat(content);
    if THINK_CLOSE.starts_with(buffered.as_utf16()) && buffered.len_utf16() < THINK_CLOSE.len() {
        state.pending_think_close = buffered;
        return RuntimeString::default();
    }
    state.pending_think_close = RuntimeString::default();
    let visible = if let Some(stripped) = strip_orphan_think_close(&buffered) {
        state.trim_after_think_close = true;
        stripped
    } else {
        buffered
    };
    let visible = if state.trim_after_think_close {
        let start = visible
            .as_utf16()
            .iter()
            .position(|unit| !matches!(*unit, 0x0009..=0x000d | 0x0020))
            .unwrap_or(visible.len_utf16());
        visible.slice_utf16(start..)
    } else {
        visible
    };
    if !visible.is_empty() {
        state.visible_text_started = true;
        state.trim_after_think_close = false;
    }
    visible
}

fn split_cumulative_thinking_text(content: &RuntimeString) -> Option<CumulativeChatText> {
    const OPEN: &[u16] = &[60, 116, 104, 105, 110, 107, 62];
    const CLOSE: &[u16] = &[60, 47, 116, 104, 105, 110, 107, 62];

    let units = content.as_utf16();
    if OPEN.starts_with(units) {
        return Some(CumulativeChatText {
            visible: RuntimeString::default(),
            thinking: RuntimeString::default(),
        });
    }
    if !units.starts_with(OPEN) {
        return None;
    }

    let mut thinking_start = OPEN.len();
    if units.get(thinking_start..thinking_start + 2) == Some(&[b'\r' as u16, b'\n' as u16]) {
        thinking_start += 2;
    } else if matches!(units.get(thinking_start), Some(unit) if *unit == b'\r' as u16 || *unit == b'\n' as u16)
    {
        thinking_start += 1;
    }

    let remainder = &units[thinking_start..];
    if let Some(relative_close) = remainder
        .windows(CLOSE.len())
        .position(|window| window == CLOSE)
    {
        let close_start = thinking_start + relative_close;
        let visible_start = close_start + CLOSE.len();
        let visible = content.slice_utf16(visible_start..);
        let visible_trimmed_start = visible
            .as_utf16()
            .iter()
            .position(|unit| !matches!(*unit, 0x0009..=0x000d | 0x0020))
            .unwrap_or(visible.len_utf16());
        return Some(CumulativeChatText {
            visible: visible.slice_utf16(visible_trimmed_start..),
            thinking: content.slice_utf16(thinking_start..close_start),
        });
    }

    let partial_close_len = (1..CLOSE.len())
        .rev()
        .find(|length| remainder.ends_with(&CLOSE[..*length]))
        .unwrap_or(0);
    Some(CumulativeChatText {
        visible: RuntimeString::default(),
        thinking: content.slice_utf16(thinking_start..units.len() - partial_close_len),
    })
}

fn update_chat_usage(chunk: &Value, state: &mut ChatState) -> bool {
    let Some(usage) = chunk.get("usage").filter(|usage| !usage.is_null()) else {
        return false;
    };
    merge_reported_usage(
        usage,
        [
            ("prompt_tokens", &mut state.input_tokens),
            ("completion_tokens", &mut state.output_tokens),
            ("total_tokens", &mut state.total_tokens),
        ],
        "prompt_tokens_details",
        &mut state.cache_read,
    )
}

/// Usage fields are cumulative within one request. Preserve known fields when a
/// partial report omits them instead of replacing those counters with zero.
fn merge_reported_usage(
    usage: &Value,
    fields: [(&str, &mut u64); 3],
    input_details_field: &str,
    cache_read: &mut u64,
) -> bool {
    let mut reported = false;
    for (field, target) in fields {
        if let Some(value) = usage.get(field).and_then(Value::as_u64) {
            *target = value;
            reported = true;
        }
    }
    if let Some(value) = usage
        .get(input_details_field)
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
    {
        *cache_read = value;
        reported = true;
    } else if let Some(value) = usage.get("prompt_cache_hit_tokens").and_then(Value::as_u64) {
        // Flat cache accounting (DeepSeek-style OpenAI-compatible routes): the
        // cached prefix size rides on top-level `prompt_cache_hit_tokens`, with
        // `prompt_cache_miss_tokens` as its complement, instead of a nested
        // details object. Without this fallback the ledger records zero cache
        // reads for those routes, indistinguishable from a true miss.
        *cache_read = value;
        reported = true;
    }
    reported
}

fn update_responses_usage(event: &Value, state: &mut ResponsesState) -> bool {
    let Some(usage) = event
        .get("response")
        .and_then(|response| response.get("usage"))
        .or_else(|| event.get("usage"))
        .filter(|usage| !usage.is_null())
    else {
        return false;
    };
    let mut reported = merge_reported_usage(
        usage,
        [
            ("input_tokens", &mut state.input_tokens),
            ("output_tokens", &mut state.output_tokens),
            ("total_tokens", &mut state.total_tokens),
        ],
        "input_tokens_details",
        &mut state.cache_read,
    );
    state.total_tokens_reported |= usage.get("total_tokens").and_then(Value::as_u64).is_some();
    if let Some(value) = usage
        .get("input_tokens_details")
        .and_then(|details| details.get("cache_creation_tokens"))
        .and_then(Value::as_u64)
    {
        state.cache_creation = value;
        reported = true;
    }
    state.usage_reported |= reported;
    reported
}

fn map_cumulative_chat_chunk(
    chunk: &Value,
    state: &mut ChatState,
) -> Result<Vec<ModelStreamEvent>, ProviderError> {
    let mut events = Vec::new();
    if !state.started {
        state.started = true;
        let model = chunk
            .get("model")
            .and_then(|value| value.as_str())
            .cloned()
            .unwrap_or_default();
        events.push(ModelStreamEvent::MessageStart { model });
    }

    if let Some(choice) = chunk
        .get("choices")
        .and_then(|choices| choices.as_array())
        .and_then(|choices| choices.first())
    {
        if let Some(message) = choice.get("message") {
            state.reasoning_field_seen |= message.get("reasoning_content").is_some();
            if let Some(content) = message.get("content").and_then(|value| value.as_str()) {
                if state.cumulative_finished && *content != state.cumulative_text {
                    return Err(invalid_cumulative_stream(
                        "content changed after the terminal snapshot",
                    ));
                }
                cumulative_suffix(
                    &state.cumulative_text,
                    content,
                    "content snapshot is a non-prefix replacement",
                )?;
                state.cumulative_text = content.clone();
                if let Some(split) = split_cumulative_thinking_text(content) {
                    let thinking_suffix = cumulative_suffix(
                        &state.cumulative_thinking,
                        &split.thinking,
                        "thinking snapshot is a non-prefix replacement",
                    )?;
                    let visible_suffix = cumulative_suffix(
                        &state.cumulative_visible_text,
                        &split.visible,
                        "visible content snapshot is a non-prefix replacement",
                    )?;
                    state.cumulative_thinking = split.thinking;
                    state.cumulative_visible_text = split.visible;
                    if !thinking_suffix.is_empty() {
                        events.push(ModelStreamEvent::ThinkingDelta {
                            text: thinking_suffix,
                        });
                    }
                    if !visible_suffix.is_empty() {
                        events.push(ModelStreamEvent::TextDelta {
                            text: visible_suffix,
                        });
                    }
                } else {
                    let visible_content = if state.reasoning_field_seen {
                        strip_orphan_think_close(content).unwrap_or_else(|| content.clone())
                    } else {
                        content.clone()
                    };
                    let visible_suffix = cumulative_suffix(
                        &state.cumulative_visible_text,
                        &visible_content,
                        "visible content snapshot is a non-prefix replacement",
                    )?;
                    state.cumulative_visible_text = visible_content;
                    if let Some(thinking) = message
                        .get("reasoning_content")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                    {
                        events.push(ModelStreamEvent::ThinkingDelta {
                            text: thinking.clone(),
                        });
                    }
                    if !visible_suffix.is_empty() {
                        events.push(ModelStreamEvent::TextDelta {
                            text: visible_suffix,
                        });
                    }
                }
            }

            if let Some(tool_calls) = message.get("tool_calls").and_then(|value| value.as_array()) {
                let mut indexes = HashSet::new();
                for (position, tool_call) in tool_calls.iter().enumerate() {
                    let index = tool_call
                        .get("index")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(position as u64);
                    if !indexes.insert(index) {
                        return Err(invalid_cumulative_stream(
                            "tool call snapshot contains a duplicate index",
                        ));
                    }
                    let incoming_id = tool_call
                        .get("id")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty());
                    let existing = state
                        .cumulative_tool_calls
                        .iter()
                        .position(|entry| entry.index == index);
                    let entry_index = match existing {
                        Some(entry_index) => {
                            if incoming_id.is_some_and(|id| {
                                id != &state.cumulative_tool_calls[entry_index].id
                            }) {
                                return Err(invalid_cumulative_stream(
                                    "tool call id changed for an existing index",
                                ));
                            }
                            entry_index
                        }
                        None => {
                            if state.cumulative_finished {
                                return Err(invalid_cumulative_stream(
                                    "a new tool call appeared after the terminal snapshot",
                                ));
                            }
                            let Some(id) = incoming_id else {
                                return Err(invalid_cumulative_stream(
                                    "new tool call snapshot is missing an id",
                                ));
                            };
                            if state
                                .cumulative_tool_calls
                                .iter()
                                .any(|entry| entry.id == *id)
                            {
                                return Err(invalid_cumulative_stream(
                                    "tool call id is reused by another index",
                                ));
                            }
                            state.cumulative_tool_calls.push(CumulativeChatToolCall {
                                index,
                                id: id.clone(),
                                name: None,
                                arguments: RuntimeString::default(),
                                started: false,
                                stopped: false,
                            });
                            state.cumulative_tool_calls.len() - 1
                        }
                    };

                    let incoming_name = tool_call
                        .get("function")
                        .and_then(|function| function.get("name"))
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty());
                    let incoming_arguments = tool_call
                        .get("function")
                        .and_then(|function| function.get("arguments"))
                        .and_then(|value| value.as_str());
                    let entry = &mut state.cumulative_tool_calls[entry_index];
                    if let (Some(existing), Some(incoming)) = (&entry.name, incoming_name) {
                        if existing != incoming {
                            return Err(invalid_cumulative_stream(
                                "tool call name changed for an existing index",
                            ));
                        }
                    } else if entry.name.is_none() {
                        entry.name = incoming_name.cloned();
                    }

                    let previous_arguments = entry.arguments.clone();
                    let current_arguments = incoming_arguments
                        .cloned()
                        .unwrap_or_else(|| previous_arguments.clone());
                    if entry.stopped && current_arguments != previous_arguments {
                        return Err(invalid_cumulative_stream(
                            "tool arguments changed after the tool call stopped",
                        ));
                    }
                    let argument_suffix = cumulative_suffix(
                        &previous_arguments,
                        &current_arguments,
                        "tool argument snapshot is a non-prefix replacement",
                    )?;
                    entry.arguments = current_arguments.clone();

                    if !entry.started {
                        if let Some(name) = &entry.name {
                            entry.started = true;
                            events.push(ModelStreamEvent::ToolUseStart {
                                id: entry.id.clone(),
                                name: name.clone(),
                                thought_signature: None,
                                tool_kind: None,
                                input_format: None,
                            });
                            if !current_arguments.is_empty() {
                                events.push(ModelStreamEvent::ToolUseInputDelta {
                                    id: entry.id.clone(),
                                    partial_json: current_arguments,
                                });
                            }
                        }
                    } else if !argument_suffix.is_empty() {
                        events.push(ModelStreamEvent::ToolUseInputDelta {
                            id: entry.id.clone(),
                            partial_json: argument_suffix,
                        });
                    }
                }
            }
        }

        if let Some(reason) = cumulative_finish_reason(choice) {
            let stop_reason = map_chat_stop_reason(reason);
            state.raw_finish_reason = Some(reason.clone());
            if state.cumulative_finished && stop_reason != state.stop_reason {
                return Err(invalid_cumulative_stream(
                    "finish reason changed after the terminal snapshot",
                ));
            }
            if state
                .cumulative_tool_calls
                .iter()
                .any(|entry| !entry.started)
            {
                return Err(invalid_cumulative_stream(
                    "terminal tool call snapshot is missing a function name",
                ));
            }
            state.stop_reason = stop_reason;
            state.cumulative_finished = true;
            for entry in &mut state.cumulative_tool_calls {
                if entry.started && !entry.stopped {
                    entry.stopped = true;
                    events.push(ModelStreamEvent::ToolUseStop {
                        id: entry.id.clone(),
                    });
                }
            }
        }
    }

    update_chat_usage(chunk, state);
    Ok(events)
}

fn map_chat_stream_chunk(
    chunk: &Value,
    state: &mut ChatState,
) -> Result<Vec<ModelStreamEvent>, ProviderError> {
    if let Some(error) = chat_stream_error(chunk) {
        return Err(error);
    }
    match chat_chunk_shape(chunk, state) {
        ChatChunkShape::Delta => Ok(map_chat_chunk(chunk, state)),
        ChatChunkShape::Snapshot => map_cumulative_chat_chunk(chunk, state),
    }
}

/// How to read this chunk: the shape the stream already settled on, or — for the
/// first chunk that names one — the shape its own `choices[0]` states. Chunks
/// that name neither (a usage-only trailer, a keep-alive) settle nothing and are
/// read as deltas, which is what an empty choice list means either way.
fn chat_chunk_shape(chunk: &Value, state: &mut ChatState) -> ChatChunkShape {
    let stated = chunk
        .get("choices")
        .and_then(|choices| choices.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| {
            let present = |field: &str| choice.get(field).is_some_and(|value| !value.is_null());
            if present("delta") {
                Some(ChatChunkShape::Delta)
            } else if present("message") {
                Some(ChatChunkShape::Snapshot)
            } else {
                None
            }
        });
    match stated {
        Some(stated) => *state.shape.get_or_insert(stated),
        None => state.shape.unwrap_or_default(),
    }
}

fn chat_stream_error(chunk: &Value) -> Option<ProviderError> {
    let error = chunk.get("error").filter(|error| !error.is_null())?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .or_else(|| chunk.get("error_msg").and_then(Value::as_str))
        .map(diagnostic_string)
        .unwrap_or_else(|| "OpenAI Chat stream failed".to_string());
    let status = chunk
        .get("error_code")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok());
    let request_id = error
        .get("request_id")
        .and_then(Value::as_str)
        .or_else(|| chunk.get("request_id").and_then(Value::as_str))
        .map(diagnostic_string);
    Some(classify_error(&RawProviderError {
        status,
        message,
        code: error
            .get("code")
            .and_then(Value::as_str)
            .map(diagnostic_string),
        body_type: error
            .get("type")
            .and_then(Value::as_str)
            .map(diagnostic_string),
        error_request_id: request_id,
        ..Default::default()
    }))
}

/// How a Chat Completions response is consumed.
enum ChatResponseBody {
    /// It said it is an event stream: read it as it arrives.
    Stream(OpenAiByteStream),
    /// It said it is something else: the whole body, in hand.
    Buffered(bytes::Bytes),
}

type OpenAiByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;

impl ChatResponseBody {
    fn into_stream(self) -> OpenAiByteStream {
        match self {
            Self::Stream(stream) => stream,
            Self::Buffered(bytes) => Box::pin(futures::stream::once(async move { Ok(bytes) })),
        }
    }
}

/// Decide by what the response says it is. A conforming endpoint answers a
/// streaming request with `text/event-stream`; a gateway that fell back to a
/// single completion (or to an error envelope) names its real content type
/// instead, and that body has to be read whole because one JSON document
/// contains no event boundary to frame on. An endpoint that declares nothing is
/// streamed, which is what it was already treated as.
async fn chat_response_body(resp: reqwest::Response) -> Result<ChatResponseBody, ProviderError> {
    let declares_event_stream = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .is_none_or(|value| {
            value
                .to_str()
                .is_ok_and(|value| value.to_ascii_lowercase().contains("text/event-stream"))
        });
    if declares_event_stream {
        return Ok(ChatResponseBody::Stream(Box::pin(resp.bytes_stream())));
    }
    resp.bytes()
        .await
        .map(ChatResponseBody::Buffered)
        .map_err(|e| {
            classify_error(&RawProviderError {
                message: error_with_source_chain(&e),
                ..Default::default()
            })
        })
}

/// The body of a non-stream response that is not JSON either — a proxy's HTML
/// error page, a truncated payload, a plain-text refusal. Only a bounded excerpt
/// is carried: the point is to name what arrived, not to paste a whole page into
/// the transcript.
fn non_stream_body_error(body: &str) -> ProviderError {
    const EXCERPT_LIMIT: usize = 200;
    let excerpt = body.trim();
    let excerpt = match excerpt.char_indices().nth(EXCERPT_LIMIT) {
        Some((cut, _)) => format!("{}\u{2026}", &excerpt[..cut]),
        None => excerpt.to_string(),
    };
    classify_error(&RawProviderError {
        message: format!(
            "the endpoint answered the streaming request with a body that is neither an event stream nor JSON: {}",
            diagnostic_string(&RuntimeString::from(excerpt.as_str()))
        ),
        ..Default::default()
    })
}

/// The response was accepted and closed without carrying a single completion
/// chunk. Transport-kind on purpose: that is what a proxy dropping a response
/// mid-flight looks like, so the turn's retry gate gets to re-ask, and nothing
/// was emitted, so a retry cannot duplicate output. When the retries run out the
/// turn fails with this message instead of settling as an empty success.
fn empty_chat_stream_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Network,
        message: "the endpoint closed the response without sending any completion chunk".into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

/// Map one Chat Completions chunk onto normalized events, advancing `state`.
fn map_chat_chunk(chunk: &Value, state: &mut ChatState) -> Vec<ModelStreamEvent> {
    let mut events = Vec::new();
    if !state.started {
        state.started = true;
        let model = chunk
            .get("model")
            .and_then(|v| v.as_str())
            .cloned()
            .unwrap_or_default();
        events.push(ModelStreamEvent::MessageStart { model });
    }

    // Edge case: usage-only final chunk has an empty `choices` array.
    if let Some(choice) = chunk
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
    {
        let delta = choice.get("delta");
        if let Some(reasoning) = delta.and_then(|d| d.get("reasoning_content")) {
            state.reasoning_field_seen = true;
            if let Some(text) = reasoning.as_str().filter(|text| !text.is_empty()) {
                events.push(ModelStreamEvent::ThinkingDelta { text: text.clone() });
            }
        }
        if let Some(content) = delta
            .and_then(|d| d.get("content"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            let visible = visible_chat_content(content, state);
            if !visible.is_empty() {
                events.push(ModelStreamEvent::TextDelta { text: visible });
            }
        }
        if let Some(tcs) = delta
            .and_then(|d| d.get("tool_calls"))
            .and_then(|v| v.as_array())
        {
            for tc in tcs {
                let idx = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let id = tc
                    .get("id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let name = tc
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let args = tc
                    .get("function")
                    .and_then(|f| f.get("arguments"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let pos = match state.tool_calls.iter().position(|e| e.index == idx) {
                    Some(p) => Some(p),
                    None => id.map(|id| {
                        state.tool_calls.push(ChatToolCall {
                            index: idx,
                            id: id.clone(),
                            started: false,
                        });
                        state.tool_calls.len() - 1
                    }),
                };
                if let Some(p) = pos {
                    if !state.tool_calls[p].started {
                        if let Some(name) = name {
                            state.tool_calls[p].started = true;
                            events.push(ModelStreamEvent::ToolUseStart {
                                id: state.tool_calls[p].id.clone(),
                                name: name.clone(),
                                thought_signature: None,
                                tool_kind: None,
                                input_format: None,
                            });
                        }
                    }
                    if let Some(args) = args {
                        events.push(ModelStreamEvent::ToolUseInputDelta {
                            id: state.tool_calls[p].id.clone(),
                            partial_json: args.clone(),
                        });
                    }
                }
            }
        }
        if let Some(fr) = choice
            .get("finish_reason")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && *s != "null")
        {
            if !state.pending_think_close.is_empty() {
                events.push(ModelStreamEvent::TextDelta {
                    text: std::mem::take(&mut state.pending_think_close),
                });
            }
            state.stop_reason = map_chat_stop_reason(fr);
            state.raw_finish_reason = Some(fr.clone());
            for entry in &state.tool_calls {
                if entry.started {
                    events.push(ModelStreamEvent::ToolUseStop {
                        id: entry.id.clone(),
                    });
                }
            }
        }
    }

    update_chat_usage(chunk, state);

    events
}

struct ResponsesCall {
    call_id: RuntimeString,
    name: RuntimeString,
    kind: ProviderToolCallKind,
    started: bool,
    input: RuntimeString,
}

/// Running state accumulated across Responses stream events.
struct ResponsesState {
    // Function-call items keyed by output item id (or call_id): name announced on
    // output_item.added, arguments fragmented across deltas.
    calls: HashMap<RuntimeString, ResponsesCall>,
    // A completed output item may be repeated by an SDK/transport. A call must
    // close at most once even when the same done event is delivered twice.
    stopped_calls: HashSet<RuntimeString>,
    text_messages: HashMap<RuntimeString, Option<RuntimeString>>,
    text_contents: HashMap<RuntimeString, RuntimeString>,
    completed_text_messages: HashSet<RuntimeString>,
    active_text_message: Option<RuntimeString>,
    // Reasoning summary text accumulated per reasoning item id.
    reasoning_summaries: HashMap<RuntimeString, RuntimeString>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read: u64,
    cache_creation: u64,
    total_tokens: u64,
    total_tokens_reported: bool,
    usage_reported: bool,
    stop_reason: ModelStopReason,
    started: bool,
    /// `incomplete_details.reason` verbatim, for the developer trace log.
    raw_stop_reason: Option<RuntimeString>,
    /// Accumulated visible text length, for the developer trace log.
    visible_chars: u64,
    /// Only an accepted terminal event authorizes tool execution and settlement.
    terminal_received: bool,
    review_continuations: bool,
}

// `ModelStopReason` has no `Default`, so seed it to `end_turn` by hand.
impl Default for ResponsesState {
    fn default() -> Self {
        ResponsesState {
            calls: HashMap::new(),
            stopped_calls: HashSet::new(),
            text_messages: HashMap::new(),
            text_contents: HashMap::new(),
            completed_text_messages: HashSet::new(),
            active_text_message: None,
            reasoning_summaries: HashMap::new(),
            input_tokens: 0,
            output_tokens: 0,
            cache_read: 0,
            cache_creation: 0,
            total_tokens: 0,
            total_tokens_reported: false,
            usage_reported: false,
            stop_reason: ModelStopReason::EndTurn,
            started: false,
            raw_stop_reason: None,
            visible_chars: 0,
            terminal_received: false,
            review_continuations: false,
        }
    }
}

impl ResponsesState {
    fn usage(&self) -> Usage {
        let mut usage = make_usage_with_cache_creation(
            self.input_tokens,
            self.output_tokens,
            self.cache_read,
            self.cache_creation,
            self.total_tokens_reported.then_some(self.total_tokens),
        );
        if !self.usage_reported {
            // A bounded tool response may complete before billing is reported.
            // An absent total must not claim an authoritative zero context size.
            usage.total_tokens = None;
        }
        usage
    }
}

fn response_call_id(item: &Value) -> RuntimeString {
    item.get("call_id")
        .or_else(|| item.get("id"))
        .and_then(|v| v.as_str())
        .cloned()
        .unwrap_or_default()
}

fn response_function_call_id(item: &Value) -> RuntimeString {
    item.get("call_id")
        .and_then(Value::as_str)
        .cloned()
        .unwrap_or_default()
}

fn response_item_key(item: &Value, call_id: &RuntimeString) -> RuntimeString {
    item.get("id")
        .and_then(|v| v.as_str())
        .cloned()
        .unwrap_or_else(|| call_id.clone())
}

fn response_tool_start(call: &ResponsesCall) -> ModelStreamEvent {
    let (tool_kind, input_format) = match call.kind {
        ProviderToolCallKind::Function => (None, None),
        ProviderToolCallKind::Custom => (
            Some(ProviderToolCallKind::Custom),
            Some(ToolInputFormat::Raw),
        ),
        ProviderToolCallKind::ToolSearch => (Some(ProviderToolCallKind::ToolSearch), None),
    };
    ModelStreamEvent::ToolUseStart {
        id: call.call_id.clone(),
        name: call.name.clone(),
        thought_signature: None,
        tool_kind,
        input_format,
    }
}

/// Completion events carry the full arguments, including any deltas already sent.
/// Emit only the missing suffix and reject conflicting snapshots before execution.
fn response_function_arguments(
    call: &mut ResponsesCall,
    arguments: &Value,
    events: &mut Vec<ModelStreamEvent>,
) -> bool {
    let Some(arguments) = arguments.as_str() else {
        return false;
    };
    if !arguments.as_utf16().starts_with(call.input.as_utf16()) {
        events.push(ModelStreamEvent::Error {
            error: ProviderError {
                kind: ProviderErrorKind::Network,
                message: "Response tool arguments changed after being streamed.".into(),
                status: None,
                code: Some("response_tool_arguments_mismatch".into()),
                error_type: None,
                request_id: None,
                media_origins: None,
                media_repair_complete: false,
                credential_recovery: None,
                retry_after_ms: None,
            },
        });
        return true;
    }
    let suffix = arguments.slice_utf16(call.input.len_utf16()..);
    if !suffix.is_empty() {
        call.input = arguments.clone();
        events.push(ModelStreamEvent::ToolUseInputDelta {
            id: call.call_id.clone(),
            partial_json: suffix,
        });
    }
    false
}

fn response_tool_search_input(item: &Value) -> RuntimeString {
    let value = runtime_json::stringify(item.get("arguments").unwrap_or(&Value::Null));
    if value == "null" {
        "{}".into()
    } else {
        value
    }
}

fn response_custom_input(item: &Value) -> RuntimeString {
    item.get("input")
        .or_else(|| item.get("raw_input"))
        .or_else(|| item.get("rawInput"))
        .and_then(Value::as_str)
        .cloned()
        .unwrap_or_default()
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut joined = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            joined = joined.concat(&RuntimeString::from(separator));
        }
        joined = joined.concat(value);
    }
    joined
}

fn responses_web_search_action(action: Option<&Value>) -> (RuntimeString, Option<WebSearchAction>) {
    let Some(action) = action else {
        return (RuntimeString::default(), Some(WebSearchAction::Other));
    };
    let action_type = action
        .get("type")
        .and_then(Value::as_str)
        .cloned()
        .unwrap_or_default();
    match action_type {
        action_type if action_type == "search" => {
            let query = action
                .get("query")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .cloned();
            let queries = action
                .get("queries")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .filter(|q| !q.is_empty());
            (
                query
                    .clone()
                    .or_else(|| queries.as_ref().map(|q| join_runtime_strings(q, ", ")))
                    .unwrap_or_default(),
                Some(WebSearchAction::Search { query, queries }),
            )
        }
        action_type if action_type == "open_page" => {
            let url = action
                .get("url")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .cloned();
            (
                url.clone().unwrap_or_default(),
                Some(WebSearchAction::OpenPage { url }),
            )
        }
        action_type if action_type == "find_in_page" => {
            let url = action
                .get("url")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .cloned();
            let pattern = action
                .get("pattern")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .cloned();
            let detail = match (pattern.as_ref(), url.as_ref()) {
                (Some(pattern), Some(url)) => {
                    pattern.concat(&RuntimeString::from(" @ ")).concat(url)
                }
                (Some(pattern), None) => pattern.clone(),
                (None, Some(url)) => url.clone(),
                (None, None) => RuntimeString::default(),
            };
            (detail, Some(WebSearchAction::FindInPage { url, pattern }))
        }
        _ => (RuntimeString::default(), Some(WebSearchAction::Other)),
    }
}

/// Map one Responses stream event onto normalized events, advancing `state`. The
/// returned bool is `true` when the stream must terminate (an error was surfaced).
fn response_text_message(
    item: &Value,
    state: &mut ResponsesState,
    events: &mut Vec<ModelStreamEvent>,
) {
    let Some(id) = item
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return;
    };
    let phase = item
        .get("phase")
        .and_then(Value::as_str)
        .filter(|phase| *phase == "commentary" || *phase == "final_answer")
        .cloned();
    let previous = state.text_messages.get(id);
    let resolved = phase.or_else(|| previous.cloned().flatten());
    if previous != Some(&resolved) || state.active_text_message.as_ref() != Some(id) {
        events.push(ModelStreamEvent::TextMessage {
            id: id.clone(),
            phase: resolved.clone(),
        });
    }
    state.text_messages.insert(id.clone(), resolved);
    state.active_text_message = Some(id.clone());
}

fn map_responses_event(ev: &Value, state: &mut ResponsesState) -> (Vec<ModelStreamEvent>, bool) {
    let mut events = Vec::new();
    let ty = ev
        .get("type")
        .and_then(|v| v.as_str())
        .cloned()
        .unwrap_or_default();
    match ty {
        ty if ty == "response.created" || ty == "response.in_progress" => {
            if !state.started {
                state.started = true;
                let model = ev
                    .get("response")
                    .and_then(|r| r.get("model"))
                    .and_then(|m| m.as_str())
                    .cloned()
                    .unwrap_or_default();
                events.push(ModelStreamEvent::MessageStart { model });
            }
        }
        ty if ty == "response.output_text.delta" => {
            if let Some(id) = ev.get("item_id").and_then(Value::as_str) {
                if state.active_text_message.as_ref() != Some(id) {
                    let phase = state.text_messages.get(id).cloned().flatten();
                    events.push(ModelStreamEvent::TextMessage {
                        id: id.clone(),
                        phase,
                    });
                    state.active_text_message = Some(id.clone());
                }
            }
            let delta = ev
                .get("delta")
                .and_then(|v| v.as_str())
                .cloned()
                .unwrap_or_default();
            if let Some(id) = ev
                .get("item_id")
                .and_then(Value::as_str)
                .or(state.active_text_message.as_ref())
            {
                let text = state.text_contents.entry(id.clone()).or_default();
                *text = text.concat(&delta);
            }
            events.push(ModelStreamEvent::TextDelta { text: delta });
        }
        ty if ty == "response.output_item.added" => {
            if let Some(item) = ev.get("item") {
                let item_type = item
                    .get("type")
                    .and_then(|v| v.as_str())
                    .cloned()
                    .unwrap_or_default();
                if item_type == "message" {
                    response_text_message(item, state, &mut events);
                    return (events, false);
                }
                let kind = if item_type == "function_call" {
                    Some(ProviderToolCallKind::Function)
                } else if item_type == "custom_tool_call" {
                    Some(ProviderToolCallKind::Custom)
                } else if item_type == "tool_search_call"
                    && item
                        .get("execution")
                        .and_then(Value::as_str)
                        .is_some_and(|execution| execution == "client")
                {
                    Some(ProviderToolCallKind::ToolSearch)
                } else {
                    None
                };
                if let Some(kind) = kind {
                    let candidate_call_id = if kind == ProviderToolCallKind::Function {
                        response_function_call_id(item)
                    } else {
                        response_call_id(item)
                    };
                    if candidate_call_id.is_empty() && kind != ProviderToolCallKind::Function {
                        return (events, false);
                    }
                    let key = response_item_key(item, &candidate_call_id);
                    let call_id = state
                        .calls
                        .get(&key)
                        .map(|entry| entry.call_id.clone())
                        .unwrap_or(candidate_call_id);
                    if state.stopped_calls.contains(&call_id) {
                        return (events, false);
                    }
                    let name = if kind == ProviderToolCallKind::ToolSearch {
                        "tool_search".into()
                    } else {
                        item.get("name")
                            .and_then(Value::as_str)
                            .cloned()
                            .unwrap_or_default()
                    };
                    let entry = state.calls.entry(key).or_insert(ResponsesCall {
                        call_id,
                        name,
                        kind,
                        started: false,
                        input: RuntimeString::default(),
                    });
                    if !entry.started {
                        entry.started = true;
                        events.push(response_tool_start(entry));
                        if kind == ProviderToolCallKind::Function
                            && response_function_arguments(
                                entry,
                                item.get("arguments").unwrap_or(&Value::Null),
                                &mut events,
                            )
                        {
                            return (events, true);
                        }
                    }
                    if kind == ProviderToolCallKind::ToolSearch && entry.input.is_empty() {
                        let input = response_tool_search_input(item);
                        if input != "{}" {
                            entry.input = input.clone();
                            events.push(ModelStreamEvent::ToolUseInputDelta {
                                id: entry.call_id.clone(),
                                partial_json: input,
                            });
                        }
                    }
                } else if item_type == "reasoning" {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        state
                            .reasoning_summaries
                            .insert(id.clone(), RuntimeString::default());
                    }
                }
            }
        }
        ty if ty == "response.function_call_arguments.delta" => {
            let item_id = ev
                .get("item_id")
                .and_then(|v| v.as_str())
                .cloned()
                .unwrap_or_default();
            let stopped = state
                .calls
                .get(&item_id)
                .is_some_and(|entry| state.stopped_calls.contains(&entry.call_id));
            if stopped {
                return (events, false);
            }
            if let Some(entry) = state.calls.get_mut(&item_id) {
                let delta = ev
                    .get("delta")
                    .and_then(|v| v.as_str())
                    .cloned()
                    .unwrap_or_default();
                entry.input = entry.input.concat(&delta);
                events.push(ModelStreamEvent::ToolUseInputDelta {
                    id: entry.call_id.clone(),
                    partial_json: delta,
                });
            }
        }
        ty if ty == "response.function_call_arguments.done" => {
            if let Some(item_id) = ev.get("item_id").and_then(Value::as_str) {
                if let Some(entry) = state.calls.get_mut(item_id) {
                    if entry.kind == ProviderToolCallKind::Function
                        && !state.stopped_calls.contains(&entry.call_id)
                        && response_function_arguments(
                            entry,
                            ev.get("arguments").unwrap_or(&Value::Null),
                            &mut events,
                        )
                    {
                        return (events, true);
                    }
                }
            }
        }
        ty if ty == "response.custom_tool_call_input.delta" => {
            let item_id = ev
                .get("item_id")
                .and_then(|v| v.as_str())
                .cloned()
                .unwrap_or_default();
            let stopped = state
                .calls
                .get(&item_id)
                .is_some_and(|entry| state.stopped_calls.contains(&entry.call_id));
            if stopped {
                return (events, false);
            }
            if let Some(entry) = state.calls.get_mut(&item_id) {
                if entry.kind == ProviderToolCallKind::Custom {
                    let delta = ev
                        .get("delta")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_default();
                    entry.input = entry.input.concat(&delta);
                    events.push(ModelStreamEvent::ToolUseInputDelta {
                        id: entry.call_id.clone(),
                        partial_json: delta,
                    });
                }
            }
        }
        ty if ty == "response.custom_tool_call_input.done" => {
            let item_id = ev
                .get("item_id")
                .and_then(Value::as_str)
                .cloned()
                .unwrap_or_default();
            let stopped = state
                .calls
                .get(&item_id)
                .is_some_and(|entry| state.stopped_calls.contains(&entry.call_id));
            if stopped {
                return (events, false);
            }
            if let Some(entry) = state.calls.get_mut(&item_id) {
                if entry.kind == ProviderToolCallKind::Custom && entry.input.is_empty() {
                    let input = ev
                        .get("input")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_default();
                    if !input.is_empty() {
                        entry.input = input.clone();
                        events.push(ModelStreamEvent::ToolUseInputDelta {
                            id: entry.call_id.clone(),
                            partial_json: input,
                        });
                    }
                }
            }
        }
        ty if ty == "response.reasoning_summary_text.delta" => {
            let item_id = ev
                .get("item_id")
                .and_then(|v| v.as_str())
                .cloned()
                .unwrap_or_default();
            let delta = ev
                .get("delta")
                .and_then(|v| v.as_str())
                .cloned()
                .unwrap_or_default();
            let current = state.reasoning_summaries.entry(item_id).or_default();
            *current = current.concat(&delta);
            events.push(ModelStreamEvent::ThinkingDelta { text: delta });
        }
        ty if ty == "response.output_item.done" => {
            if let Some(item) = ev.get("item") {
                let item_type = item
                    .get("type")
                    .and_then(|v| v.as_str())
                    .cloned()
                    .unwrap_or_default();
                if item_type == "message" {
                    response_text_message(item, state, &mut events);
                    if let Some(id) = item.get("id").and_then(Value::as_str) {
                        if state.completed_text_messages.insert(id.clone()) {
                            let text = item
                                .get("content")
                                .and_then(Value::as_array)
                                .map(|content| {
                                    content
                                        .iter()
                                        .filter_map(|block| {
                                            block.get("text").and_then(Value::as_str)
                                        })
                                        .fold(RuntimeString::default(), |text, part| {
                                            text.concat(part)
                                        })
                                })
                                .filter(|text| !text.is_empty())
                                .or_else(|| state.text_contents.remove(id))
                                .unwrap_or_default();
                            events.push(ModelStreamEvent::TextMessageCompleted {
                                id: id.clone(),
                                phase: state.text_messages.get(id).cloned().flatten(),
                                text,
                            });
                        }
                    }
                    return (events, false);
                }
                if item_type == "function_call" {
                    let call_id = response_function_call_id(item);
                    let key = response_item_key(item, &call_id);
                    if let Some(entry) = state.calls.get_mut(&key) {
                        if state.stopped_calls.contains(&entry.call_id) {
                            return (events, false);
                        }
                        if response_function_arguments(
                            entry,
                            item.get("arguments").unwrap_or(&Value::Null),
                            &mut events,
                        ) {
                            return (events, true);
                        }
                        if state.stopped_calls.insert(entry.call_id.clone()) {
                            events.push(ModelStreamEvent::ToolUseStop {
                                id: entry.call_id.clone(),
                            });
                        }
                    }
                    return (events, false);
                }
                let kind = if item_type == "custom_tool_call" {
                    Some(ProviderToolCallKind::Custom)
                } else if item_type == "tool_search_call"
                    && item
                        .get("execution")
                        .and_then(Value::as_str)
                        .is_some_and(|execution| execution == "client")
                {
                    Some(ProviderToolCallKind::ToolSearch)
                } else {
                    None
                };
                if let Some(kind) = kind {
                    let candidate_call_id = response_call_id(item);
                    if candidate_call_id.is_empty() {
                        return (events, false);
                    }
                    let key = response_item_key(item, &candidate_call_id);
                    let call_id = state
                        .calls
                        .get(&key)
                        .map(|entry| entry.call_id.clone())
                        .unwrap_or(candidate_call_id);
                    if state.stopped_calls.contains(&call_id) {
                        return (events, false);
                    }
                    let name = if kind == ProviderToolCallKind::ToolSearch {
                        "tool_search".into()
                    } else {
                        item.get("name")
                            .and_then(Value::as_str)
                            .cloned()
                            .unwrap_or_default()
                    };
                    let entry = state.calls.entry(key).or_insert(ResponsesCall {
                        call_id: call_id.clone(),
                        name,
                        kind,
                        started: false,
                        input: RuntimeString::default(),
                    });
                    if !entry.started {
                        entry.started = true;
                        events.push(response_tool_start(entry));
                    }
                    if entry.input.is_empty() {
                        let input = match kind {
                            ProviderToolCallKind::Function => item
                                .get("arguments")
                                .and_then(Value::as_str)
                                .cloned()
                                .unwrap_or_default(),
                            ProviderToolCallKind::Custom => response_custom_input(item),
                            ProviderToolCallKind::ToolSearch => response_tool_search_input(item),
                        };
                        if !(input.is_empty()
                            || kind == ProviderToolCallKind::ToolSearch && input == "{}")
                        {
                            entry.input = input.clone();
                            events.push(ModelStreamEvent::ToolUseInputDelta {
                                id: entry.call_id.clone(),
                                partial_json: input,
                            });
                        }
                    }
                    if state.stopped_calls.insert(call_id.clone()) {
                        events.push(ModelStreamEvent::ToolUseStop { id: call_id });
                    }
                } else if item_type == "web_search_call" {
                    // A native web search completed. Surface the provider action
                    // detail; url_citation annotations carry the sources.
                    let action = item.get("action");
                    let (query, action) = responses_web_search_action(action);
                    let id = item
                        .get("id")
                        .and_then(|v| v.as_str())
                        .cloned()
                        .unwrap_or_default();
                    events.push(ModelStreamEvent::ServerSearchQuery { id, query, action });
                } else if item_type == "reasoning" {
                    // Reasoning complete: emit the whole block in stream order. `text` is
                    // the visible summary (authoritative `summary` if present, else the
                    // accumulated deltas); `encrypted_content` is the opaque replay blob.
                    let item_id = item
                        .get("id")
                        .and_then(|v| v.as_str())
                        .cloned()
                        .unwrap_or_default();
                    let summary_from_item =
                        item.get("summary").and_then(|s| s.as_array()).map(|arr| {
                            arr.iter().fold(RuntimeString::default(), |summary, item| {
                                match item.get("text").and_then(|text| text.as_str()) {
                                    Some(text) => summary.concat(text),
                                    None => summary,
                                }
                            })
                        });
                    let acc = state.reasoning_summaries.get(&item_id).cloned();
                    let summary = summary_from_item
                        .filter(|s| !s.is_empty())
                        .or_else(|| acc.filter(|s| !s.is_empty()))
                        .unwrap_or_default();
                    let encrypted = item
                        .get("encrypted_content")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .cloned();
                    state.reasoning_summaries.remove(&item_id);
                    if !summary.is_empty() || encrypted.is_some() {
                        events.push(ModelStreamEvent::ThinkingBlock {
                            text: summary,
                            signature: None,
                            encrypted_content: encrypted,
                            is_summary: true,
                        });
                    }
                }
            }
        }
        ty if ty == "response.output_text.annotation.added" => {
            if let Some(a) = ev.get("annotation") {
                if a.get("type")
                    .and_then(|v| v.as_str())
                    .is_some_and(|kind| kind == "url_citation")
                {
                    let url = a
                        .get("url")
                        .and_then(|v| v.as_str())
                        .cloned()
                        .unwrap_or_default();
                    events.push(ModelStreamEvent::Citation {
                        citation: Citation {
                            url,
                            title: a.get("title").and_then(|v| v.as_str()).cloned(),
                            cited_text: None,
                            start_index: a.get("start_index").and_then(|v| v.as_u64()),
                            end_index: a.get("end_index").and_then(|v| v.as_u64()),
                            encrypted_index: None,
                        },
                    });
                }
            }
        }
        ty if ty == "response.completed" || ty == "response.incomplete" => {
            if ty == "response.incomplete" {
                let reason = ev
                    .get("response")
                    .and_then(|response| response.get("incomplete_details"))
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str);
                if reason.is_none_or(|reason| reason != "interrupted") {
                    events.push(ModelStreamEvent::Error {
                        error: incomplete_responses_error(reason),
                    });
                    return (events, true);
                }
            }
            state.terminal_received = true;
            if let Some(response) = ev.get("response") {
                update_responses_usage(ev, state);
                state.stop_reason = if state.review_continuations {
                    map_review_responses_stop_reason(response)
                } else {
                    map_responses_stop_reason(response)
                };
                state.raw_stop_reason = response
                    .get("incomplete_details")
                    .and_then(|details| details.get("reason"))
                    .and_then(|reason| reason.as_str())
                    .map(RuntimeString::from);
            }
        }
        ty if ty == "response.failed" => {
            // Forward the machine-readable code so the status-less in-stream error
            // still classifies, instead of collapsing to unknown.
            let e = ev.get("response").and_then(|r| r.get("error"));
            let message = e
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
                .map(diagnostic_string)
                .filter(|message| !message.trim().is_empty())
                .unwrap_or_else(|| RESPONSES_FAILED_WITHOUT_MESSAGE.into());
            let code = e
                .and_then(|error| error.get("code"))
                .and_then(|code| code.as_str())
                .filter(|code| !code.is_empty());
            let error_type = e
                .and_then(|error| error.get("type"))
                .and_then(|kind| kind.as_str());
            events.push(ModelStreamEvent::Error {
                error: classify_error(&RawProviderError {
                    message,
                    code: code.map(diagnostic_string),
                    body_type: error_type.map(diagnostic_string),
                    ..Default::default()
                }),
            });
            return (events, true);
        }
        ty if ty == "error" => {
            let nested = ev.get("error");
            let message = ev
                .get("message")
                .and_then(|message| message.as_str())
                .map(diagnostic_string)
                .filter(|message| !message.trim().is_empty())
                .or_else(|| {
                    nested
                        .and_then(|error| error.get("message"))
                        .and_then(|message| message.as_str())
                        .map(diagnostic_string)
                        .filter(|message| !message.trim().is_empty())
                })
                .unwrap_or_else(|| "Responses stream returned an error without a message".into());
            let code = ev
                .get("code")
                .and_then(|code| code.as_str())
                .filter(|code| !code.is_empty())
                .or_else(|| {
                    nested
                        .and_then(|error| error.get("code"))
                        .and_then(|code| code.as_str())
                        .filter(|code| !code.is_empty())
                });
            let error_type = nested
                .and_then(|error| error.get("type"))
                .and_then(|kind| kind.as_str());
            events.push(ModelStreamEvent::Error {
                error: classify_error(&RawProviderError {
                    message,
                    code: code.map(diagnostic_string),
                    body_type: error_type.map(diagnostic_string),
                    ..Default::default()
                }),
            });
            return (events, true);
        }
        _ => {}
    }
    (events, false)
}

fn finish_responses_state(state: &ResponsesState, model: &RuntimeString) -> Vec<ModelStreamEvent> {
    let mut events = Vec::with_capacity(2);
    if !state.started {
        events.push(ModelStreamEvent::MessageStart {
            model: model.clone(),
        });
    }
    events.push(ModelStreamEvent::MessageStop {
        stop_reason: state.stop_reason,
        usage: state.usage(),
    });
    events
}

// ---- HTTP establishment / error mapping ----

fn aborted_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Unknown,
        message: "aborted".into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

fn collect_headers(headers: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (k, v) in headers {
        if let Ok(s) = v.to_str() {
            out.insert(k.as_str().to_string(), s.to_string());
        }
    }
    out
}

/// Render an error together with its full `source()` chain, joined by ": ".
/// `reqwest::Error::to_string()` alone yields opaque messages like
/// "error sending request for url (...)" while the actual root cause
/// (TLS handshake failure, DNS, connect refused, ...) lives further down
/// the source chain and would otherwise be swallowed.
fn error_with_source_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Count the actual model input and structured output/tool framing.
fn estimate_approval_body_tokens(body: &Value, responses: bool) -> usize {
    let mut input = body
        .get(if responses { "input" } else { "messages" })
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(crate::engine::session::estimate_approval_wire_input_tokens)
                .fold(0usize, usize::saturating_add)
        })
        .unwrap_or_default();
    if let Some(instructions) = body.get("instructions") {
        input = input.saturating_add(
            runtime_json::stringify(instructions)
                .to_node_utf8()
                .len()
                .div_ceil(4),
        );
    }
    let metadata = Value::Array(vec![
        body.get("tools").cloned().unwrap_or(Value::Null),
        body.get(if responses { "text" } else { "response_format" })
            .cloned()
            .unwrap_or(Value::Null),
    ]);
    input.saturating_add(
        runtime_json::stringify(&metadata)
            .to_node_utf8()
            .len()
            .div_ceil(4),
    )
}

fn encode_openai_request_body(body: &Value) -> Vec<u8> {
    let mut encoded = Vec::new();
    runtime_json::write_to(body, &mut encoded).expect("writing exact JSON to memory cannot fail");
    encoded
}

/// One settled model request, as the developer trace log sees it.
///
/// The fields are exactly the evidence that disappeared in the incident this
/// log exists for: WHAT the wire said when the stream ended (raw finish
/// reason, whether a `[DONE]` frame arrived, whether any chunk parsed at all)
/// versus what the engine concluded from it (mapped stop reason), plus the
/// usage and the visible-text length that tell a truncated answer from a
/// short one. Deliberately absent: request bodies, response bodies and
/// credentials — the trace answers "how did it end", not "what was said".
struct ProviderTraceRecord<'a> {
    api: &'a str,
    model: &'a str,
    request_id: Option<&'a str>,
    /// The wire's own stop marker, verbatim; `None` when the stream ended
    /// without one (clean EOF), which is the shape that reads as a normal stop
    /// once mapped.
    raw_finish_reason: Option<&'a str>,
    /// Whether the Chat stream saw its `[DONE]` sentinel. Responses has none.
    saw_done: bool,
    /// Whether at least one chunk parsed into stream events.
    mapped_any: bool,
    /// Whether the body arrived as one buffered JSON document instead of an
    /// event stream (a gateway that gave up on streaming).
    buffered: bool,
    stop_reason: &'a str,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    visible_chars: u64,
    elapsed_ms: u128,
}

fn stop_reason_name(reason: ModelStopReason) -> &'static str {
    match reason {
        ModelStopReason::EndTurn => "end_turn",
        ModelStopReason::Continue => "continue",
        ModelStopReason::ToolUse => "tool_use",
        ModelStopReason::MaxTokens => "max_tokens",
        ModelStopReason::StopSequence => "stop_sequence",
        ModelStopReason::Refusal => "refusal",
    }
}

/// Append one trace line when the developer switch is on. Best-effort like
/// every diagnostic: a full disk must not fail a request that succeeded. The
/// switch is read by the caller so tests can drive the record itself.
fn record_provider_trace(enabled: bool, record: &ProviderTraceRecord<'_>) {
    if !enabled {
        return;
    }
    let mut fields = json!({
        "api": record.api,
        "model": record.model,
        "sawDone": record.saw_done,
        "mappedAny": record.mapped_any,
        "buffered": record.buffered,
        "stopReason": record.stop_reason,
        "inputTokens": record.input_tokens,
        "outputTokens": record.output_tokens,
        "cacheReadTokens": record.cache_read_tokens,
        "totalTokens": record.total_tokens,
        "visibleChars": record.visible_chars,
        "elapsedMs": record.elapsed_ms,
    });
    let object = fields.as_object_mut().expect("object literal");
    if let Some(raw) = record.raw_finish_reason {
        object.insert("rawFinishReason".to_string(), json!(raw));
    }
    if let Some(request_id) = record.request_id {
        object.insert("requestId".to_string(), json!(request_id));
    }
    crate::observability::provider_diagnostics::record_named(
        crate::observability::provider_diagnostics::TRACE_LOG_FILE_NAME,
        "provider_request_settled",
        fields,
    );
}

fn random_request_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

/// Reconstruct the raw signals the shared classifier reads (status / code /
/// type / nested error / headers) from a non-2xx HTTP response.
pub(super) fn raw_http_error(
    status: u16,
    headers: &HashMap<String, String>,
    body: &str,
) -> RawProviderError {
    let parsed = runtime_json::parse(&RuntimeString::from(body)).unwrap_or(Value::Null);
    let err_obj = parsed.get("error");
    let message = err_obj
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .or_else(|| parsed.get("message").and_then(|m| m.as_str()))
        .map(diagnostic_string)
        .unwrap_or_else(|| body.to_string());
    RawProviderError {
        status: Some(status),
        message,
        code: err_obj
            .and_then(|e| e.get("code"))
            .or_else(|| parsed.get("code"))
            .and_then(|c| c.as_str())
            .map(diagnostic_string),
        body_type: err_obj
            .and_then(|e| e.get("type"))
            .and_then(|t| t.as_str())
            .map(diagnostic_string),
        inner_type: err_obj
            .and_then(|e| e.get("error"))
            .and_then(|e| e.get("type"))
            .and_then(|t| t.as_str())
            .map(diagnostic_string),
        error_request_id: err_obj
            .and_then(|e| e.get("request_id"))
            .and_then(|r| r.as_str())
            .map(diagnostic_string),
        headers: headers.clone(),
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn establish_stream(
    client: reqwest::Client,
    url: String,
    body: Value,
    auth: Option<Arc<dyn AuthSource>>,
    api_key: Option<String>,
    extra_headers: HashMap<String, String>,
    retry: RetryOptions,
    signal: AbortSignal,
    interception: (Option<SharedRequestInterceptor>, String, String),
) -> Result<reqwest::Response, ProviderError> {
    let (interceptor, provider_id, model) = interception;
    let body = Arc::new(Mutex::new(body));
    let request_attempt = Arc::new(AtomicU32::new(0));
    with_retry(retry, signal.clone(), move |_attempt: u32| {
        let client = client.clone();
        let url = url.clone();
        let body = body.clone();
        let request_attempt = request_attempt.clone();
        let auth = auth.clone();
        let api_key = api_key.clone();
        let extra_headers = extra_headers.clone();
        let signal = signal.clone();
        let interceptor = interceptor.clone();
        let provider_id = provider_id.clone();
        let model = model.clone();
        async move {
            loop {
                if signal.is_cancelled() {
                    return Err(RawProviderError {
                        message: "aborted".to_owned(),
                        ..Default::default()
                    });
                }
                // Interception runs per attempt: headers refresh on every retry
                // and a veto settles the attempt before the network.
                let request = ProviderRequestInfo {
                    provider_id: &provider_id,
                    model: &model,
                    url: &url,
                    attempt: request_attempt.fetch_add(1, Ordering::Relaxed) + 1,
                };
                let started = std::time::Instant::now();
                let intercepted = match admit_attempt(interceptor.as_ref(), &request) {
                    Ok(intercepted) => intercepted,
                    Err(denied) => {
                        report_attempt(
                            interceptor.as_ref(),
                            &request,
                            started,
                            false,
                            None,
                            Some(&denied),
                        );
                        return Err(RawProviderError {
                            message: denied,
                            ..Default::default()
                        });
                    }
                };
                let mut builder = client
                    .post(&url)
                    .header("Accept", "text/event-stream")
                    .header("Content-Type", "application/json");
                for (name, value) in &extra_headers {
                    builder = builder.header(name.as_str(), value.as_str());
                }
                for (name, value) in &intercepted {
                    builder = builder.header(name.as_str(), value.as_str());
                }
                if let Some(auth) = &auth {
                    let headers = auth
                        .get_headers(&signal)
                        .await
                        .map_err(raw_from_provider_error)?;
                    for (k, v) in &headers {
                        builder = builder.header(k.as_str(), v.as_str());
                    }
                } else if let Some(key) = &api_key {
                    builder = builder.bearer_auth(key);
                }
                let encoded = encode_openai_request_body(&body.lock_recover());
                let send = builder.body(encoded).send();
                let resp = tokio::select! {
                    _ = signal.cancelled() => return Err(RawProviderError {
                        message: "aborted".to_string(),
                        ..Default::default()
                    }),
                    // Include the full source chain: the bare reqwest Display is
                    // just "error sending request for url (...)" and the real
                    // TLS/connect root cause was being swallowed.
                    r = send => r.map_err(|e| RawProviderError {
                        message: error_with_source_chain(&e),
                        ..Default::default()
                    })?,
                };
                if !resp.status().is_success() {
                    let status = resp.status().as_u16();
                    let headers = collect_headers(resp.headers());
                    let text = resp.text().await.unwrap_or_default();
                    let error = raw_http_error(status, &headers, &text);
                    report_attempt(
                        interceptor.as_ref(),
                        &request,
                        started,
                        false,
                        Some(status),
                        Some(&error.message),
                    );
                    if !signal.is_cancelled() {
                        let adjusted = reduce_output_reservation(&mut body.lock_recover(), &error);
                        if let Some(output_tokens) = adjusted {
                            tracing::debug!(
                                output_tokens,
                                "Retrying with a smaller output reservation"
                            );
                            continue;
                        }
                    }
                    return Err(error);
                }
                report_attempt(
                    interceptor.as_ref(),
                    &request,
                    started,
                    true,
                    Some(resp.status().as_u16()),
                    None,
                );
                return Ok(resp);
            }
        }
    })
    .await
    .map_err(|e| match e {
        RetryError::Provider(pe) => pe,
        RetryError::Aborted => aborted_error(),
    })
}

fn apply_review_effort_body(
    body: &mut Value,
    effort: Option<&RuntimeString>,
    dialect: OpenAiReasoningDialect,
    responses: bool,
) -> Result<(), ProviderError> {
    let Some(effort) = effort else {
        return Ok(());
    };
    if dialect != OpenAiReasoningDialect::OpenAi {
        return Err(crate::provider::review_target::unsupported_review_effort());
    }
    if responses {
        if body.get("reasoning").is_none_or(|value| !value.is_object()) {
            body["reasoning"] = Value::Object(Map::new());
        }
        body["reasoning"]["effort"] = Value::String(effort.clone());
    } else {
        body["reasoning_effort"] = Value::String(effort.clone());
    }
    Ok(())
}

struct OpenAiManagedReviewCloudAuthSource {
    auth: Arc<dyn AuthSource>,
    client: reqwest::Client,
    backend_base_url: String,
}

#[async_trait::async_trait]
impl crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource
    for OpenAiManagedReviewCloudAuthSource
{
    async fn current(
        &self,
    ) -> Result<Option<crate::engine::approval_requirements_cloud::ManagedReviewCloudAuth>, String>
    {
        use crate::engine::approval_requirements_cloud::{
            ManagedReviewCloudAuth, ManagedReviewCloudIdentity,
        };
        let Some(credentials) = self
            .auth
            .managed_review_credentials(&AbortSignal::new())
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let identity = credentials.identity;
        Ok(Some(ManagedReviewCloudAuth {
            identity: ManagedReviewCloudIdentity {
                chatgpt_user_id: identity.chatgpt_user_id,
                account_id: identity.account_id,
                plan_type: identity.plan_type,
                backend_base_url: self.backend_base_url.clone(),
                owner_generation: identity.owner_generation,
            },
            headers: credentials.headers,
            client: self.client.clone(),
        }))
    }

    async fn recover_unauthorized(
        &self,
        recovery_step: usize,
        expected: &crate::engine::approval_requirements_cloud::ManagedReviewCloudIdentity,
    ) -> crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthRecovery {
        use crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthRecovery as CloudRecovery;
        use crate::provider::auth::{
            ManagedReviewCredentialIdentity, ManagedReviewCredentialRecovery,
        };
        if expected.backend_base_url != self.backend_base_url {
            return CloudRecovery::PermanentFailure(
                "Managed review backend changed during authentication recovery".into(),
            );
        }
        let expected = ManagedReviewCredentialIdentity {
            chatgpt_user_id: expected.chatgpt_user_id.clone(),
            account_id: expected.account_id.clone(),
            plan_type: expected.plan_type.clone(),
            owner_generation: expected.owner_generation,
        };
        match self
            .auth
            .recover_managed_review_unauthorized(recovery_step, &expected)
            .await
        {
            ManagedReviewCredentialRecovery::Refreshed => CloudRecovery::Refreshed,
            ManagedReviewCredentialRecovery::TransientFailure(error) => {
                CloudRecovery::TransientFailure(error)
            }
            ManagedReviewCredentialRecovery::PermanentFailure(error) => {
                CloudRecovery::PermanentFailure(error)
            }
            ManagedReviewCredentialRecovery::Unavailable => CloudRecovery::Unavailable,
        }
    }
}

/// The wire role used for trusted instructions at an OpenAI-compatible endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenAIInstructionRole {
    System,
    Developer,
}

impl OpenAIInstructionRole {
    fn name(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Developer => "developer",
        }
    }
}

type InstructionRoleCache = Arc<Mutex<HashMap<RuntimeString, OpenAIInstructionRole>>>;

/// Change only trusted instruction roles. Content, ordering, user input and
/// tool batches retain their original wire representation.
fn project_instruction_role(body: &mut Value, role: OpenAIInstructionRole) -> bool {
    let mut changed = false;
    for field in ["messages", "input"] {
        let Some(messages) = body.get_mut(field).and_then(Value::as_array_mut) else {
            continue;
        };
        for message in messages {
            if matches!(message["role"].as_str(), Some(name) if name == "system" || name == "developer")
                && message["role"] != vs(role.name())
            {
                message["role"] = vs(role.name());
                changed = true;
            }
        }
    }
    changed
}

/// Negotiate only an explicit role validation refusal before streaming starts.
/// A generic bad request, auth failure or error after output must never replay work.
fn refused_instruction_role(error: &ProviderError) -> Option<OpenAIInstructionRole> {
    let validation_envelope = error.status.is_none()
        && (error.error_type.as_ref().is_some_and(|kind| {
            kind == "invalid_request_error" || kind == "invalid_parameter_error"
        }) || error
            .code
            .as_ref()
            .is_some_and(|code| code == "invalid_parameter_error" || code == "unsupported_value"));
    if !matches!(error.status, Some(400 | 422)) && !validation_envelope {
        return None;
    }
    let message = diagnostic_string(&error.message).to_ascii_lowercase();
    for (name, alternate) in [
        ("developer", OpenAIInstructionRole::System),
        ("system", OpenAIInstructionRole::Developer),
    ] {
        let spellings = [name.to_string(), format!("'{name}'"), format!("\"{name}\"")];
        let enum_refusal = spellings
            .iter()
            .any(|value| message.contains(&format!("{value} is not one of [")))
            && ["'user'", "\"user\""]
                .iter()
                .any(|value| message.contains(value))
            && ["'assistant'", "\"assistant\""]
                .iter()
                .any(|value| message.contains(value));
        let role_refusal = message.contains("role")
            && spellings.iter().any(|value| {
                [
                    format!("unsupported value: {value}"),
                    format!("unsupported role: {value}"),
                    format!("unsupported message role: {value}"),
                    format!("does not support {value}"),
                    format!("role {value} is not supported"),
                    format!("role {value} not supported"),
                    format!("{value} role is not supported"),
                ]
                .iter()
                .any(|text| message.contains(text))
            });
        if enum_refusal || role_refusal {
            return Some(alternate);
        }
    }
    None
}

/// Inspect only the response prelude. The first ordinary event ends inspection;
/// its bytes and all subsequent bytes pass through untouched. In particular an
/// error after text, reasoning or a tool delta can never trigger negotiation.
async fn inspect_instruction_role_refusal(
    response: reqwest::Response,
    signal: &AbortSignal,
) -> Result<ChatResponseBody, ProviderError> {
    let body = tokio::select! {
        _ = signal.cancelled() => return Err(aborted_error()),
        body = chat_response_body(response) => body?,
    };
    let check = |bytes: &[u8]| -> Option<ProviderError> {
        let text = RuntimeString::from(String::from_utf8_lossy(bytes).as_ref());
        let chunk = runtime_json::parse(&text).ok()?;
        let generated = [&chunk, chunk.get("response").unwrap_or(&Value::Null)]
            .iter()
            .any(|value| {
                value.get("delta").is_some_and(|delta| !delta.is_null())
                    || ["choices", "output"].iter().any(|field| {
                        value
                            .get(*field)
                            .and_then(Value::as_array)
                            .is_some_and(|items| !items.is_empty())
                    })
                    || value.get("usage").is_some_and(|usage| {
                        ["completion_tokens", "output_tokens"].iter().any(|field| {
                            usage
                                .get(*field)
                                .and_then(Value::as_u64)
                                .is_some_and(|tokens| tokens > 0)
                        })
                    })
            });
        if generated {
            return None;
        }
        let mut error = chat_stream_error(&chunk)
            .or_else(|| chunk.get("response").and_then(chat_stream_error))?;
        if chunk
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == "response.failed")
            && diagnostic_string(&error.message).trim().is_empty()
        {
            error.message = RESPONSES_FAILED_WITHOUT_MESSAGE.into();
        }
        Some(error)
    };
    let ChatResponseBody::Stream(mut stream) = body else {
        if let ChatResponseBody::Buffered(bytes) = &body {
            if let Some(error) = check(bytes) {
                return Err(error);
            }
        }
        return Ok(body);
    };
    let mut prefix = Vec::new();
    let mut decoder = SseDecoder::new();
    loop {
        let next = tokio::select! {
            _ = signal.cancelled() => return Err(aborted_error()),
            next = stream.next() => next,
        };
        match next {
            Some(Ok(bytes)) => {
                prefix.extend_from_slice(&bytes);
                decoder.push(&bytes);
                while let Some(event) = decoder.next_event() {
                    if event.data.is_empty() {
                        continue;
                    }
                    if let Some(error) = check(&event.data.to_node_utf8()) {
                        return Err(error);
                    }
                    let first = bytes::Bytes::from(prefix);
                    return Ok(ChatResponseBody::Stream(Box::pin(
                        futures::stream::once(async move { Ok(first) }).chain(stream),
                    )));
                }
                if prefix.len() < 64 * 1024 {
                    continue;
                }
            }
            Some(Err(error)) => {
                let first = bytes::Bytes::from(prefix);
                return Ok(ChatResponseBody::Stream(Box::pin(
                    futures::stream::iter([Ok(first), Err(error)]).chain(stream),
                )));
            }
            None => {
                if let Some(error) = check(&prefix) {
                    return Err(error);
                }
            }
        }
        let first = bytes::Bytes::from(prefix);
        return Ok(ChatResponseBody::Stream(Box::pin(
            futures::stream::once(async move { Ok(first) }).chain(stream),
        )));
    }
}

#[allow(clippy::too_many_arguments)]
async fn establish_stream_with_instruction_role(
    client: reqwest::Client,
    url: String,
    mut body: Value,
    auth: Option<Arc<dyn AuthSource>>,
    api_key: Option<String>,
    extra_headers: HashMap<String, String>,
    retry: RetryOptions,
    signal: AbortSignal,
    interception: (Option<SharedRequestInterceptor>, String, String),
    role_state: (
        InstructionRoleCache,
        RuntimeString,
        Option<OpenAIInstructionRole>,
    ),
) -> Result<ChatResponseBody, ProviderError> {
    let (roles, model, declared_role) = role_state;
    let learned = roles
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&model)
        .copied()
        .or(declared_role);
    if let Some(role) = learned {
        project_instruction_role(&mut body, role);
    }
    let result = match establish_stream(
        client.clone(),
        url.clone(),
        body.clone(),
        auth.clone(),
        api_key.clone(),
        extra_headers.clone(),
        retry.clone(),
        signal.clone(),
        interception.clone(),
    )
    .await
    {
        Ok(response) => inspect_instruction_role_refusal(response, &signal).await,
        Err(error) => Err(error),
    };
    let error = match result {
        Ok(response) => return Ok(response),
        Err(error) => error,
    };
    let Some(role) = refused_instruction_role(&error) else {
        return Err(error);
    };
    if signal.is_cancelled() || !project_instruction_role(&mut body, role) {
        return Err(error);
    }
    let response = establish_stream(
        client,
        url,
        body,
        auth,
        api_key,
        extra_headers,
        retry,
        signal.clone(),
        interception,
    )
    .await?;
    let response = inspect_instruction_role_refusal(response, &signal).await?;
    // Each provider owns one effective endpoint and wire. Exact model keys keep
    // auxiliary routes from inheriting another model's learned capability.
    roles
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(model, role);
    Ok(response)
}

/// The OpenAI model provider.
#[derive(Clone)]
pub struct OpenAIProvider {
    remote_compaction: crate::types::provider::RemoteCompactionSupport,
    instruction_roles: InstructionRoleCache,
    info: ProviderInfo,
    client: reqwest::Client,
    classifier_http_client_factory:
        Option<crate::provider::approval_classifier_transport::ClassifierHttpClientFactory>,
    classifier_requires_client_factory: bool,
    api: OpenAiApi,
    retry: RetryOptions,
    auth_source: Option<Arc<dyn AuthSource>>,
    api_key: Option<String>,
    compatibility: Option<OpenAICompatibilityOptions>,
    /// The root used to build request URLs (auth-source override wins).
    request_base_url: String,
    /// Transport-level request interception, when configured.
    interceptor: Option<SharedRequestInterceptor>,
    /// Client-declared model capabilities; the single source of capability truth.
    capabilities: ModelCapabilities,
    /// Per-model overrides consulted before `capabilities` (see the options).
    capabilities_by_model: HashMap<String, ModelCapabilities>,
    /// The endpoint's reasoning wire dialect.
    reasoning_dialect: OpenAiReasoningDialect,
    fast_mode: bool,
    bridge_fast_mode: Option<bool>,
    review_effort: Option<RuntimeString>,
    reviewer_header_model: Option<RuntimeString>,
}

impl OpenAIProvider {
    /// Build a provider from options.
    pub fn new(opts: OpenAIProviderOptions) -> Self {
        let request_base_url = opts
            .auth_source
            .as_ref()
            .and_then(|a| a.base_url().map(str::to_string))
            .or_else(|| opts.base_url.clone())
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let classifier_requires_client_factory =
            opts.http_client.is_some() && opts.classifier_http_client_factory.is_none();
        let client = opts.http_client.unwrap_or_default();
        let api_key = opts.api_key.clone();
        let api = opts.api.unwrap_or_default();
        let retry = opts.retry.unwrap_or_default();
        let info = ProviderInfo {
            name: "openai".into(),
            env_key: Some("OPENAI_API_KEY".into()),
            default_model: opts
                .default_model
                .unwrap_or_else(|| DEFAULT_MODEL.to_string())
                .into(),
            base_url: opts.base_url.map(RuntimeString::from),
        };
        OpenAIProvider {
            remote_compaction: opts.remote_compaction,
            instruction_roles: Arc::new(Mutex::new(HashMap::new())),
            info,
            client,
            classifier_http_client_factory: opts.classifier_http_client_factory,
            classifier_requires_client_factory,
            api,
            retry,
            auth_source: opts.auth_source,
            api_key,
            compatibility: opts.compatibility,
            request_base_url,
            interceptor: opts.interceptor,
            capabilities: opts.capabilities.unwrap_or_default(),
            capabilities_by_model: opts.capabilities_by_model.unwrap_or_default(),
            reasoning_dialect: opts.reasoning_dialect.unwrap_or_default(),
            fast_mode: opts.fast_mode,
            bridge_fast_mode: opts.bridge_fast_mode,
            review_effort: None,
            reviewer_header_model: None,
        }
    }

    /// The capability set for ONE request's model: the per-model override when
    /// the host declared one, else the legacy session-level set. Media limits
    /// belong only to the model the declaration named.
    fn capabilities_for(&self, model: &RuntimeString) -> ModelCapabilities {
        model
            .try_to_utf8()
            .ok()
            .and_then(|id| self.capabilities_by_model.get(&id))
            .cloned()
            .unwrap_or_else(|| {
                let mut capabilities = self.capabilities.clone();
                if *model != self.info.default_model {
                    capabilities.image_limits = None;
                    capabilities.supports_image_detail_original = false;
                    capabilities.max_media_blocks = None;
                    capabilities.output_verbosity = None;
                }
                capabilities
            })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.request_base_url.trim_end_matches('/'), path)
    }

    /// Native subscription auth on a declared backend route admits review metadata.
    fn is_codex_subscription_backend(&self) -> bool {
        self.auth_source
            .as_ref()
            .is_some_and(|auth| auth.is_subscription_oauth())
            && self.api_key.is_none()
            && self.info.base_url.as_ref().is_none_or(|base_url| {
                base_url
                    .try_to_utf8()
                    .ok()
                    .is_some_and(|url| url.trim_end_matches('/').ends_with("/backend-api/codex"))
            })
    }

    fn uses_subscription_wire_format(&self) -> bool {
        self.request_base_url
            .contains("chatgpt.com/backend-api/codex")
    }

    fn uses_responses_backend_format(&self) -> bool {
        self.uses_subscription_wire_format()
            || (self.retry.retry_class_policy
                == Some(crate::provider::retry::RetryClassPolicy::ApprovalReview)
                && self.is_codex_subscription_backend())
    }

    fn reviewer_attribution_model(&self) -> Option<RuntimeString> {
        self.is_codex_subscription_backend()
            .then(|| self.approval_review_preferred_model())
    }

    /// Chat Completions path (default).
    fn stream_chat(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream {
        let instruction_roles = self.instruction_roles.clone();
        let client = self.client.clone();
        let url = self.endpoint("/chat/completions");
        let bridge_fast_mode = self.bridge_fast_mode;
        let auth = self.auth_source.clone();
        let api_key = self.api_key.clone();
        let retry = self.retry.clone();
        let compatibility = self.compatibility.clone();
        let interceptor = self.interceptor.clone();
        let provider_id = diagnostic_string(&self.info.name);
        let capabilities = self.capabilities_for(&req.model);
        let reasoning_dialect = self.reasoning_dialect;
        let review_effort = self.review_effort.clone();
        let review_stream = self.retry.retry_class_policy
            == Some(crate::provider::retry::RetryClassPolicy::ApprovalReview);
        let mut req = req;
        req.max_output_tokens = capabilities.clamp_output_tokens(req.max_output_tokens);
        let s = async_stream::stream! {
            let started = std::time::Instant::now();
            if compatibility
                .as_ref()
                .is_some_and(OpenAICompatibilityOptions::responses_function_tools)
            {
                yield ModelStreamEvent::Error {
                    error: invalid_openai_compatibility(
                        "responsesFunctionTools requires api 'responses'",
                    ),
                };
                return;
            }
            if let Some(error) = validate_chat_request_choice(&req) {
                yield ModelStreamEvent::Error { error };
                return;
            }
            let identity = compatibility
                .as_ref()
                .and_then(OpenAICompatibilityOptions::request_identity);
            let request_id = identity.map(|_| random_request_id());
            let mut body = build_chat_params_with_identity(&req, identity, request_id.as_deref(), &capabilities, reasoning_dialect);
            if let Err(error) = apply_review_effort_body(&mut body, review_effort.as_ref(), reasoning_dialect, false) { yield ModelStreamEvent::Error { error }; return; }
            if let Some(enabled) = bridge_fast_mode {
                body["fast_mode"] = Value::Bool(enabled);
            }
            let extra_headers = compatibility
                .as_ref()
                .map(|options| options.extra_headers().clone())
                .unwrap_or_default();
            let mut state = ChatState::default();
            // Surface every establishment retry while its backoff sleeps.
            let (retry, retry_rx) = observe_retries(retry);
            let establish = establish_stream_with_instruction_role(client, url, body, auth, api_key, extra_headers, retry, signal.clone(), (interceptor, provider_id, diagnostic_string(&req.model)), (instruction_roles, req.model.clone(), compatibility.as_ref().and_then(OpenAICompatibilityOptions::instruction_role)));
            let mut establish = std::pin::pin!(establishment_steps(establish, retry_rx));
            let mut established = None;
            while let Some(step) = establish.next().await {
                match step {
                    EstablishmentStep::Retry(event) => yield event,
                    EstablishmentStep::Done(result) => {
                        established = Some(result);
                        break;
                    }
                }
            }
            let resp = match established {
                Some(Ok(r)) => r,
                Some(Err(err)) => {
                    if signal.is_cancelled() { return; }
                    yield ModelStreamEvent::Error { error: err };
                    return;
                }
                // The step stream always terminates with `Done`.
                None => return,
            };
            // How the response is consumed, decided by what it says it is. A
            // gateway that gave up on streaming answers the very same request with
            // ONE JSON completion (often an error envelope riding a 200), and a
            // single JSON document has no event boundary in it: framing that as SSE
            // dropped the whole reply and left the turn empty with the gateway's
            // usage booked. Such a body is buffered instead — still handed to the
            // framer first, which is what a mislabeled but real event stream needs,
            // then read as one chunk below when the framer finds nothing in it.
            let body = resp;
            let buffered = match &body {
                ChatResponseBody::Buffered(bytes) => Some(bytes.clone()),
                ChatResponseBody::Stream(_) => None,
            };
            let mut byte_stream = body.into_stream();
            let mut mapped_any = false;
            let mut visible_chars: u64 = 0;
            let mut decoder = SseDecoder::new();
            let mut done = false;
            loop {
                while let Some(evt) = decoder.next_event() {
                    if evt.data.is_empty() { continue; }
                    if evt.data == "[DONE]" { done = true; break; }
                    if let Ok(chunk) = runtime_json::parse(&evt.data) {
                        if update_chat_usage(&chunk, &mut state) {
                            yield ModelStreamEvent::UsageSnapshot {
                                usage: make_usage(state.input_tokens, state.output_tokens, state.cache_read, state.total_tokens),
                            };
                        }
                        match map_chat_stream_chunk(&chunk, &mut state) {
                            Ok(events) => {
                                mapped_any = true;
                                for event in events {
                                    if let ModelStreamEvent::TextDelta { text } = &event {
                                        visible_chars += text.as_utf16().len() as u64;
                                    }
                                    yield event;
                                }
                            }
                            Err(error) => {
                                yield ModelStreamEvent::Error { error };
                                return;
                            }
                        }
                    }
                }
                if done { break; }
                let next = tokio::select! {
                    _ = signal.cancelled() => return,
                    n = byte_stream.next() => n,
                };
                match next {
                    Some(Ok(chunk)) => decoder.push(chunk.as_ref()),
                    Some(Err(e)) => {
                        if signal.is_cancelled() { return; }
                        yield ModelStreamEvent::Error {
                            error: {
                                let error = classify_error(&RawProviderError { message: e.to_string(), ..Default::default() });
                                if review_stream { error.mark_stream_interrupted() } else { error }
                            },
                        };
                        return;
                    }
                    None => break,
                }
            }
            // Nothing framed. A buffered body is the completion itself, so read it
            // as the single chunk it is; `choices[].message` is what a non-stream
            // body carries, which the snapshot path already understands. Otherwise
            // the endpoint accepted the request, closed the response cleanly and
            // sent no completion at all — reporting that as a finished, empty
            // message hid the failure behind a turn that looked successful.
            if !mapped_any {
                let text = buffered
                    .as_ref()
                    .map(|bytes| String::from_utf8_lossy(bytes).trim().to_string())
                    .filter(|text| !text.is_empty());
                let Some(text) = text else {
                    yield ModelStreamEvent::Error { error: empty_chat_stream_error() };
                    return;
                };
                let Ok(chunk) = runtime_json::parse(&RuntimeString::from(text.as_str())) else {
                    yield ModelStreamEvent::Error { error: non_stream_body_error(&text) };
                    return;
                };
                if update_chat_usage(&chunk, &mut state) {
                    yield ModelStreamEvent::UsageSnapshot {
                        usage: make_usage(state.input_tokens, state.output_tokens, state.cache_read, state.total_tokens),
                    };
                }
                match map_chat_stream_chunk(&chunk, &mut state) {
                    Ok(events) => {
                        for event in events {
                            yield event;
                        }
                    }
                    Err(error) => {
                        yield ModelStreamEvent::Error { error };
                        return;
                    }
                }
            }
            if review_stream && buffered.is_none() && !done && state.raw_finish_reason.is_none() {
                let error = classify_error(&RawProviderError { message: "Network stream ended before its terminal event".into(), ..Default::default() }).mark_stream_interrupted();
                yield ModelStreamEvent::Error { error };
                return;
            }
            if !state.started {
                yield ModelStreamEvent::MessageStart { model: req.model.clone() };
            }
            if !state.pending_think_close.is_empty() {
                let close = std::mem::take(&mut state.pending_think_close);
                visible_chars += close.as_utf16().len() as u64;
                yield ModelStreamEvent::TextDelta { text: close };
            }
            let chat_model = req.model.try_to_utf8().unwrap_or_default();
            let chat_raw_finish = state
                .raw_finish_reason
                .as_ref()
                .and_then(|reason| reason.try_to_utf8().ok());
            record_provider_trace(crate::observability::provider_diagnostics::provider_trace_enabled(), &ProviderTraceRecord {
                api: "chat",
                model: &chat_model,
                request_id: request_id.as_deref(),
                raw_finish_reason: chat_raw_finish.as_deref(),
                saw_done: done,
                mapped_any,
                buffered: buffered.is_some(),
                stop_reason: stop_reason_name(state.stop_reason),
                input_tokens: state.input_tokens,
                output_tokens: state.output_tokens,
                cache_read_tokens: state.cache_read,
                total_tokens: state.total_tokens,
                visible_chars,
                elapsed_ms: started.elapsed().as_millis(),
            });
            yield ModelStreamEvent::MessageStop {
                stop_reason: state.stop_reason,
                usage: make_usage(state.input_tokens, state.output_tokens, state.cache_read, state.total_tokens),
            };
        };
        Box::pin(s)
    }

    /// Responses API path (`api: Responses`).
    fn stream_responses(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream {
        let instruction_roles = self.instruction_roles.clone();
        let client = self.client.clone();
        let url = self.endpoint("/responses");
        let auth = self.auth_source.clone();
        let native_scope = self.native_replay_scope(&req);
        let api_key = self.api_key.clone();
        let retry = self.retry.clone();
        let compatibility = self.compatibility.clone();
        let interceptor = self.interceptor.clone();
        let provider_id = diagnostic_string(&self.info.name);
        let codex_backend = self.uses_responses_backend_format();
        let fast_mode = self.fast_mode;
        let capabilities = self.capabilities_for(&req.model);
        let reasoning_dialect = self.reasoning_dialect;
        let review_effort = self.review_effort.clone();
        let review_stream = self.retry.retry_class_policy
            == Some(crate::provider::retry::RetryClassPolicy::ApprovalReview);
        let reviewer_header_model = self.reviewer_header_model.clone();
        let mut req = req;
        req.max_output_tokens = capabilities.clamp_output_tokens(req.max_output_tokens);
        let s = async_stream::stream! {
            let auth = match remote_compaction::scope_auth(auth, native_scope, api_key.as_deref(), &signal).await {
                Ok(auth) => auth,
                Err(error) => { yield ModelStreamEvent::Error { error }; return; }
            };
            let started = std::time::Instant::now();
            let function_tools = compatibility
                .as_ref()
                .is_some_and(OpenAICompatibilityOptions::responses_function_tools);
            let choice_error = if function_tools {
                validate_exact_tool_choice(
                    "OpenAI Responses",
                    req.tool_choice.as_ref(),
                    &req.tools,
                    req.web_search.is_some(),
                )
            } else {
                validate_responses_request_choice(&req)
            };
            if let Some(error) = choice_error {
                yield ModelStreamEvent::Error { error };
                return;
            }
            if compatibility
                .as_ref()
                .is_some_and(|options| options.request_identity().is_some())
            {
                yield ModelStreamEvent::Error {
                    error: invalid_openai_compatibility(
                        "requestIdentity requires api 'chat'",
                    ),
                };
                return;
            }
            let mut body = match build_responses_params_with_tool_mode(&req, &capabilities, reasoning_dialect, function_tools) {
                Ok(mut body) => {
                    retain_scoped_responses_context(&mut body, compatibility.as_ref(), &req.model);
                    // The codex-subscription backend rejects some standard
                    // Responses parameters; strip or rewrite only transport fields.
                    if codex_backend {
                        adapt_codex_responses_params(&mut body);
                        if fast_mode {
                            body["service_tier"] = vs("priority");
                        }
                    }
                    body
                }
                Err(error) => {
                    yield ModelStreamEvent::Error { error };
                    return;
                }
            };
            let mut state = ResponsesState { review_continuations: review_stream, ..Default::default() };
            if let Err(error) = apply_review_effort_body(&mut body, review_effort.as_ref(), reasoning_dialect, true) { yield ModelStreamEvent::Error { error }; return; }
            let mut extra_headers = compatibility
                .as_ref()
                .map(|options| options.extra_headers().clone())
                .unwrap_or_default();
            if reviewer_header_model.as_ref() == Some(&req.model) {
                extra_headers.insert("x-codex-guardian".to_string(), "reviewer".to_string());
            }
            // Surface every establishment retry while its backoff sleeps.
            let (retry, retry_rx) = observe_retries(retry);
            let establish = establish_stream_with_instruction_role(client, url, body, auth, api_key, extra_headers, retry, signal.clone(), (interceptor, provider_id, diagnostic_string(&req.model)), (instruction_roles, req.model.clone(), compatibility.as_ref().and_then(OpenAICompatibilityOptions::instruction_role)));
            let mut establish = std::pin::pin!(establishment_steps(establish, retry_rx));
            let mut established = None;
            while let Some(step) = establish.next().await {
                match step {
                    EstablishmentStep::Retry(event) => yield event,
                    EstablishmentStep::Done(result) => {
                        established = Some(result);
                        break;
                    }
                }
            }
            let resp = match established {
                Some(Ok(r)) => r,
                Some(Err(err)) => {
                    if signal.is_cancelled() { return; }
                    yield ModelStreamEvent::Error { error: err };
                    return;
                }
                // The step stream always terminates with `Done`.
                None => return,
            };
            let mut byte_stream = resp.into_stream();
            let mut decoder = SseDecoder::new();
            let mut mapped_any = false;
            // A completed tool item can precede a rejected response terminal.
            // Hold its execution boundary until this attempt is accepted so a
            // stream retry cannot execute the same call twice.
            let mut pending_tool_stops = Vec::new();
            'responses: loop {
                while let Some(evt) = decoder.next_event() {
                    if evt.data.is_empty() { continue; }
                    if let Ok(evj) = runtime_json::parse(&evt.data) {
                        if update_responses_usage(&evj, &mut state) {
                            yield ModelStreamEvent::UsageSnapshot {
                                usage: state.usage(),
                            };
                        }
                        let (evs, terminate) = map_responses_event(&evj, &mut state);
                        if !evs.is_empty() { mapped_any = true; }
                        for e in evs {
                            if let ModelStreamEvent::TextDelta { text } = &e {
                                state.visible_chars += text.as_utf16().len() as u64;
                            }
                            if matches!(e, ModelStreamEvent::ToolUseStop { .. }) {
                                pending_tool_stops.push(e);
                            } else {
                                yield e;
                            }
                        }
                        if terminate { return; }
                        if state.terminal_received { break 'responses; }
                    }
                }
                let next = tokio::select! {
                    _ = signal.cancelled() => return,
                    n = byte_stream.next() => n,
                };
                match next {
                    Some(Ok(chunk)) => decoder.push(chunk.as_ref()),
                    Some(Err(e)) => {
                        if signal.is_cancelled() { return; }
                        yield ModelStreamEvent::Error {
                            error: {
                                let error = classify_error(&RawProviderError { message: e.to_string(), ..Default::default() });
                                if review_stream { error.mark_stream_interrupted() } else { error }
                            },
                        };
                        return;
                    }
                    None => break,
                }
            }
            if !state.terminal_received {
                yield ModelStreamEvent::Error {
                    error: {
                        let mut error = incomplete_responses_error(Some(&RuntimeString::from("missing_terminal_event")));
                        if review_stream { error.code = None; error = error.mark_stream_interrupted(); }
                        error
                    },
                };
                return;
            }
            for event in pending_tool_stops {
                yield event;
            }
            let responses_model = req.model.try_to_utf8().unwrap_or_default();
            let responses_raw_stop = state
                .raw_stop_reason
                .as_ref()
                .and_then(|reason| reason.try_to_utf8().ok());
            record_provider_trace(crate::observability::provider_diagnostics::provider_trace_enabled(), &ProviderTraceRecord {
                api: "responses",
                model: &responses_model,
                request_id: None,
                raw_finish_reason: responses_raw_stop.as_deref(),
                saw_done: false,
                mapped_any,
                buffered: false,
                stop_reason: stop_reason_name(state.stop_reason),
                input_tokens: state.input_tokens,
                output_tokens: state.output_tokens,
                cache_read_tokens: state.cache_read,
                total_tokens: state.total_tokens,
                visible_chars: state.visible_chars,
                elapsed_ms: started.elapsed().as_millis(),
            });
            for event in finish_responses_state(&state, &req.model) {
                yield event;
            }
        };
        Box::pin(s)
    }
}

#[async_trait::async_trait]
impl ModelProvider for OpenAIProvider {
    fn remote_compaction_support(
        &self,
        _model: &RuntimeString,
    ) -> crate::types::provider::RemoteCompactionSupport {
        if self.api != OpenAiApi::Responses {
            return crate::types::provider::RemoteCompactionSupport::Unsupported;
        }
        if self.remote_compaction != crate::types::provider::RemoteCompactionSupport::Unsupported {
            return self.remote_compaction;
        }
        if self.is_codex_subscription_backend() && self.uses_subscription_wire_format() {
            crate::types::provider::RemoteCompactionSupport::ResponsesV2
        } else {
            crate::types::provider::RemoteCompactionSupport::Unsupported
        }
    }
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        self.native_checkpoint_matches(checkpoint, model, signal)
            .await
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        self.retry
            .max_attempts
            .map(|attempts| attempts.saturating_sub(1))
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.uses_official_api_catalog()
            || (self.is_codex_subscription_backend() && self.uses_subscription_wire_format())
    }
    async fn compact_remote(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        self.native_compact(request, signal, None).await
    }

    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        self.auth_source
            .as_ref()
            .and_then(|auth| auth.credential_changes())
    }
    fn create_approval_classifier_transport(
        &self,
        attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        Some(approval_classifier::create(
            self.clone(),
            attribution,
            target,
        ))
    }
    async fn compact_remote_with_idle_timeout(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        idle_ms: u64,
    ) -> Result<crate::types::provider::RemoteCompactionResult, ProviderError> {
        self.native_compact(request, signal, Some(idle_ms)).await
    }
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        self.capabilities_for(model).clone()
    }

    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        let mut models: Vec<_> = self
            .capabilities_by_model
            .iter()
            .map(|(model, metadata)| (RuntimeString::from(model.as_str()), metadata.clone()))
            .collect();
        if !models
            .iter()
            .any(|(model, _)| model == &self.info.default_model)
        {
            models.push((self.info.default_model.clone(), self.capabilities.clone()));
        }
        models.sort_by(|left, right| left.0.cmp(&right.0));
        models
    }

    fn stream_with_model_metadata(
        &self,
        req: ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
        total_attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        let mut selected = self.clone();
        selected.reviewer_header_model = self.reviewer_attribution_model();

        let model = match req.model.try_to_utf8() {
            Ok(model) => model,
            Err(_) => return crate::provider::model_catalog::uncaptured_model_stream(&req.model),
        };
        selected.capabilities_by_model.insert(model, metadata);
        if selected.reasoning_dialect == OpenAiReasoningDialect::OpenAi {
            selected.review_effort = raw_effort;
        } else {
            let mut req = req;
            if let Err(error) = crate::provider::review_target::apply_typed_review_effort(
                &mut req,
                raw_effort.as_ref(),
            ) {
                return Box::pin(futures::stream::once(async move {
                    ModelStreamEvent::Error { error }
                }));
            }
            selected.retry = selected.retry.for_approval_review(total_attempts);
            return selected.stream(req, signal);
        }
        selected.retry = selected.retry.for_approval_review(total_attempts);
        selected.stream(req, signal)
    }

    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        if self.api != OpenAiApi::Responses || self.reviewer_attribution_model().is_none() {
            return None;
        }
        Some(Arc::new(OpenAiManagedReviewCloudAuthSource {
            auth: self.auth_source.as_ref()?.clone(),
            client: self.client.clone(),
            backend_base_url: self.request_base_url.clone(),
        }))
    }

    fn approval_review_preferred_model(&self) -> RuntimeString {
        if self.api_key.is_some() {
            "gpt-5.6-luna".into()
        } else {
            "codex-auto-review".into()
        }
    }
    fn uses_official_api_catalog(&self) -> bool {
        self.api_key.is_some()
            && self.auth_source.is_none()
            && url::Url::parse(&self.request_base_url)
                .ok()
                .is_some_and(|url| {
                    url.scheme() == "https"
                        && url.host_str() == Some("api.openai.com")
                        && url.port().is_none()
                })
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        // Capability comes from the client-declared set (per-model override
        // first); only the API-surface truths (Chat has no hosted web search,
        // caching is server-managed) stay as constants. Nothing is inferred
        // from the model id.
        let caps = self.capabilities_for(model);
        ProviderCapabilities {
            supports_image_detail_original: caps.supports_image_detail_original,
            streaming: true,
            tools: caps.tools.unwrap_or(true),
            thinking: caps.thinking.is_some(),
            structured_output: caps.structured_output,
            // Native web search is a Responses-API built-in; Chat has no hosted
            // search, so it is gated by the surface AND the declared capability.
            web_search: self.api == OpenAiApi::Responses && caps.web_search,
            // Prompt caching is automatic and server-managed: no request-side control.
            prompt_cache_control: false,
            context_window: caps.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW),
            max_output_tokens: caps.max_output_tokens.unwrap_or(DEFAULT_MAX_OUTPUT),
            native_pdf: caps.native_pdf,
            vision: caps.vision,
            image_limits: caps.image_limits,
            max_media_blocks: caps.max_media_blocks,
        }
    }

    fn stream(&self, mut req: ModelRequest, signal: AbortSignal) -> ModelStream {
        if self.api != OpenAiApi::Responses
            && crate::provider::native_compaction::contains_native_checkpoint(&req.messages)
        {
            return crate::provider::native_compaction::rejected_checkpoint_stream();
        }
        if let Err(error) = crate::provider::native_media::validate_request(&req, &[]) {
            return crate::provider::native_media::rejected_stream(error);
        }
        if req.web_search.as_ref().is_some_and(|search| {
            search.access == crate::schemas::config::WebSearchAccess::Disabled
        }) {
            req.web_search = None;
        }

        let capabilities = self.capabilities_for(&req.model);
        apply_model_default_effort(&mut req, &capabilities, self.compatibility.as_ref());
        if crate::context::inspection_images::request_uses_image_detail(&req) {
            req.supports_image_detail_original &= capabilities.supports_image_detail_original;
            crate::engine::query::gate_request_images(&mut req);
        }
        let native_controls = self.api == OpenAiApi::Responses
            && self.reasoning_dialect == OpenAiReasoningDialect::OpenAi;
        if let Some(error) = crate::provider::unsupported_inference_control(
            &req,
            "OpenAI",
            native_controls && capabilities.output_verbosity.is_some(),
            native_controls
                && capabilities.effort.is_some()
                && capabilities.supports_reasoning_summary_parameter != Some(false),
            native_controls,
        ) {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error { error }
            }));
        }

        let structured = match StructuredOutputState::prepare(&mut req, &capabilities) {
            Ok(state) => state,
            Err(error) => {
                return Box::pin(futures::stream::once(async move {
                    ModelStreamEvent::Error { error }
                }))
            }
        };
        let stream = match self.api {
            OpenAiApi::Responses => self.stream_responses(req, signal.clone()),
            OpenAiApi::Chat => self.stream_chat(req, signal.clone()),
        };
        match structured {
            Some(state) => emulate_structured_output(stream, state, signal),
            None => stream,
        }
    }

    fn approval_request_tokens_with_metadata(
        &self,
        request: &ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        let mut selected = self.clone();
        let model = request.model.try_to_utf8().map_err(|_| {
            RuntimeString::from("Cannot account for a model identifier that is not valid Unicode.")
        })?;
        selected.capabilities_by_model.insert(model, metadata);
        selected.retry.retry_class_policy =
            Some(crate::provider::retry::RetryClassPolicy::ApprovalReview);
        let mut request = request.clone();
        if selected.reasoning_dialect == OpenAiReasoningDialect::OpenAi {
            selected.review_effort = raw_effort;
        } else {
            crate::provider::review_target::apply_typed_review_effort(
                &mut request,
                raw_effort.as_ref(),
            )
            .map_err(|error| error.message)?;
        }
        selected.approval_request_tokens(&request)
    }

    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        let mut request = request.clone();
        let capabilities = self.capabilities_for(&request.model);
        apply_model_default_effort(&mut request, &capabilities, self.compatibility.as_ref());
        request.max_output_tokens = capabilities.clamp_output_tokens(request.max_output_tokens);
        StructuredOutputState::prepare(&mut request, &capabilities)
            .map_err(|error| error.message)?;
        let responses = self.api == OpenAiApi::Responses;
        let mut body = if responses {
            let function_tools = self
                .compatibility
                .as_ref()
                .is_some_and(OpenAICompatibilityOptions::responses_function_tools);
            let mut body = build_responses_params_with_tool_mode(
                &request,
                &capabilities,
                self.reasoning_dialect,
                function_tools,
            )
            .map_err(|error| error.message)?;
            retain_scoped_responses_context(&mut body, self.compatibility.as_ref(), &request.model);
            if self.uses_responses_backend_format() {
                adapt_codex_responses_params(&mut body);
            }
            body
        } else {
            let identity = self
                .compatibility
                .as_ref()
                .and_then(OpenAICompatibilityOptions::request_identity);
            build_chat_params_with_identity(
                &request,
                identity,
                None,
                &capabilities,
                self.reasoning_dialect,
            )
        };
        apply_review_effort_body(
            &mut body,
            self.review_effort.as_ref(),
            self.reasoning_dialect,
            responses,
        )
        .map_err(|error| error.message)?;
        let role = self
            .instruction_roles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&request.model)
            .copied()
            .or_else(|| {
                self.compatibility
                    .as_ref()
                    .and_then(OpenAICompatibilityOptions::instruction_role)
            });
        if let Some(role) = role {
            project_instruction_role(&mut body, role);
        }
        Ok(estimate_approval_body_tokens(&body, responses))
    }

    fn estimate_tokens(&self, req: &ModelRequest) -> Option<u64> {
        Some(estimate_tokens_fallback(req))
    }

    fn has_prewarm_endpoints(&self) -> bool {
        true
    }

    fn prewarm_transport(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<PrewarmOutcome>> + Send>> {
        let client = self.client.clone();
        let endpoint = endpoint_origin(&self.request_base_url);
        Box::pin(async move { vec![probe(&client, &endpoint).await] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_codec;
    use crate::schemas::content::{
        DocumentBlock, ImageBlock, TextBlock, ThinkingBlock, ToolUseBlock,
    };
    use crate::schemas::message::{
        AssistantMessage, CompactBoundaryMessage, ToolResultMessage, UserMessage,
    };
    use crate::types::provider::{
        open_fallback_tool_schema, ToolSpec, ToolSpecFormat, ToolSpecGrammarSyntax, ToolSpecKind,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    // Exact-runtime fixture literals. `stringify!` keeps these tests entirely
    // on the RuntimeJson parser/writer path; no Serde bridge is involved.
    macro_rules! json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    #[derive(Default)]
    struct CountingAuth {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl AuthSource for CountingAuth {
        fn is_subscription_oauth(&self) -> bool {
            true
        }
        async fn get_headers(
            &self,
            _signal: &AbortSignal,
        ) -> Result<crate::types::provider::AuthHeaders, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Default::default())
        }

        fn base_url(&self) -> Option<&str> {
            Some("http://127.0.0.1:9")
        }
    }

    #[test]
    fn reviewer_attribution_requires_subscription_backend_auth() {
        let api = OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("fixture".into()),
            ..Default::default()
        });
        assert!(api.reviewer_attribution_model().is_none());
        let mut subscription = OpenAIProvider::new(OpenAIProviderOptions {
            auth_source: Some(Arc::new(CountingAuth::default())),
            ..Default::default()
        });
        assert_eq!(
            subscription.reviewer_attribution_model(),
            Some("codex-auto-review".into())
        );
        subscription.info.base_url = Some("https://chatgpt.com/backend-api/codex".into());
        assert_eq!(
            subscription.reviewer_attribution_model(),
            Some("codex-auto-review".into())
        );
        subscription.info.base_url = Some("http://127.0.0.1:1234/proxy/backend-api/codex/".into());
        assert_eq!(
            subscription.reviewer_attribution_model(),
            Some("codex-auto-review".into())
        );
        subscription.info.base_url =
            Some("https://gateway.example/backend-api/codex-compat".into());
        assert!(subscription.reviewer_attribution_model().is_none());
    }

    #[tokio::test]
    async fn reviewer_headers_are_scoped_to_the_selected_model_and_clone() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            api_key: Some("fixture".into()),
            ..Default::default()
        });
        let mut selected = root.clone();
        selected.reviewer_header_model = Some("review".into());
        let server = tokio::spawn(async move {
            let mut headers = Vec::new();
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                headers.push(
                    capture_http_request(&mut socket)
                        .await
                        .0
                        .to_ascii_lowercase(),
                );
                let body =
                    "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n";
                socket
                    .write_all(http_response("200 OK", "text/event-stream", body).as_bytes())
                    .await
                    .unwrap();
            }
            headers
        });
        for (provider, model) in [
            (&selected, "review"),
            (&selected, "explicit-override"),
            (&root, "review"),
        ] {
            let mut request = base_req();
            request.model = model.into();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                provider
                    .stream(request, AbortSignal::new())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        }
        let headers = server.await.unwrap();
        assert!(headers[0].contains("x-codex-guardian: reviewer\r\n"));
        assert!(!headers[1].contains("x-codex-guardian"));
        assert!(!headers[2].contains("x-codex-guardian"));
    }

    async fn capture_http_request(socket: &mut TcpStream) -> (String, String) {
        let mut request = Vec::new();
        let mut header_end = None;
        let mut content_length = None;
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before completing the request");
            assert!(
                request.len() + read <= 16 * 1024 * 1024,
                "fixture request exceeded its bound"
            );
            request.extend_from_slice(&chunk[..read]);

            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    content_length = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    });
                }
            }

            if let (Some(end), Some(length)) = (header_end, content_length) {
                if request.len() >= end + length {
                    break;
                }
            }
        }

        let end = header_end.unwrap();
        let length = content_length.unwrap();
        (
            String::from_utf8(request[..end].to_vec()).unwrap(),
            String::from_utf8(request[end..end + length].to_vec()).unwrap(),
        )
    }

    fn http_response(status: &str, content_type: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    async fn capture_gateway_requests(listener: TcpListener) -> Vec<(String, String)> {
        let cumulative_body = concat!(
            "data: {\"model\":\"qwen3.7-max\",\"choices\":[{\"message\":{\"content\":\"你\"},\"finish_reason\":\"null\"}]}\n\n",
            "data: {\"model\":\"qwen3.7-max\",\"choices\":[{\"message\":{\"content\":\"你好\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let mut captured = Vec::new();
        for attempt in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(capture_http_request(&mut socket).await);
            let response = if attempt == 0 {
                http_response(
                    "500 Internal Server Error",
                    "application/json",
                    r#"{"error":{"message":"retry fixture"}}"#,
                )
            } else {
                http_response("200 OK", "text/event-stream", cumulative_body)
            };
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        captured
    }

    #[tokio::test]
    async fn gateway_fixture_reuses_request_identity_across_retry_and_rotates_between_calls() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(capture_gateway_requests(listener));
        let compatibility = OpenAICompatibilityOptions::try_new(
            HashMap::from([
                ("X-Endpoint".to_string(), "klaus-desktop".to_string()),
                ("X-Caller-App".to_string(), "klaus-desktop".to_string()),
            ]),
            Some(OpenAIRequestIdentity::try_new("session-fixture").unwrap()),
        )
        .unwrap();
        let provider = OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("fixture-token".to_string()),
            base_url: Some(format!("http://{address}/v1")),
            default_model: Some("qwen3.7-max".to_string()),
            api: Some(OpenAiApi::Chat),
            retry: Some(RetryOptions {
                max_attempts: Some(2),
                base_delay_ms: Some(0),
                jitter: Some(0.0),
                ..Default::default()
            }),
            compatibility: Some(compatibility),
            ..Default::default()
        });

        let first_events = provider
            .stream(base_req(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let second_events = provider
            .stream(base_req(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        let captured = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("gateway fixture received all requests")
            .unwrap();

        assert_eq!(captured.len(), 3);
        for (headers, _) in &captured {
            assert!(headers
                .lines()
                .any(|line| line.eq_ignore_ascii_case("authorization: Bearer fixture-token")));
            assert!(headers
                .lines()
                .any(|line| line.eq_ignore_ascii_case("x-endpoint: klaus-desktop")));
            assert!(headers
                .lines()
                .any(|line| line.eq_ignore_ascii_case("x-caller-app: klaus-desktop")));
        }

        let bodies = captured
            .iter()
            .map(|(_, body)| runtime_json::parse(&RuntimeString::from(body.as_str())).unwrap())
            .collect::<Vec<_>>();
        let session_ids = bodies
            .iter()
            .map(|body| body["session_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        let request_ids = bodies
            .iter()
            .map(|body| body["request_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(session_ids
            .iter()
            .all(|session_id| **session_id == "session-fixture"));
        assert_eq!(request_ids[0], request_ids[1]);
        assert_ne!(request_ids[1], request_ids[2]);
        assert_eq!(request_ids[0].as_utf16().len(), 36);

        let first_text = first_events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::TextDelta { text } => Some(text.clone()),
                _ => None,
            })
            .fold(RuntimeString::default(), |text, delta| text.concat(&delta));
        assert_eq!(first_text, "你好");
        assert!(!first_events
            .iter()
            .chain(second_events.iter())
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }

    /// A Chat Completions gateway that answers with ONE cumulative
    /// `choices[].message` snapshot instead of incremental deltas — the shape
    /// internal model proxies use, `event:` name and all. Nothing in a
    /// user-configured custom model can declare that shape, so the reply has to
    /// be read off the payload; when it was not, the whole answer was dropped and
    /// the turn completed empty while still booking the gateway's usage. The
    /// fixture provider carries no compatibility options at all, which is exactly
    /// what a BYO `[modelProviders]` entry resolves to.
    #[tokio::test]
    async fn chat_stream_reads_a_cumulative_snapshot_gateway_with_nothing_declared() {
        const SNAPSHOT_BODY: &str = concat!(
            "event:complete\n",
            "data:{\"choices\":[{\"finish_reason\":\"stop\",\"index\":0,\"message\":{\"content\":\"你好\",\"role\":\"Assistant\"}}],",
            "\"created\":1,\"id\":\"snapshot-fixture\",",
            "\"usage\":{\"completion_tokens\":10,\"prompt_tokens\":7,\"total_tokens\":17}}\n\n"
        );
        let events = serve_one_chat_response("text/event-stream", SNAPSHOT_BODY).await;

        assert_eq!(collect_text(&events), "你好");
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        let Some(ModelStreamEvent::MessageStop { stop_reason, usage }) = events.last() else {
            panic!("the stream ends with message_stop");
        };
        assert_eq!(*stop_reason, ModelStopReason::EndTurn);
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 10);
    }

    /// A gateway that ignores `Accept: text/event-stream` and answers with one
    /// plain JSON completion. The SSE framer never finds an event boundary in
    /// that body, so it used to be dropped whole and the turn ended empty; the
    /// content type says it is not a stream, so it is read as the single chunk
    /// it is.
    #[tokio::test]
    async fn a_non_stream_json_body_is_read_instead_of_framed_as_sse() {
        const BODY: &str = concat!(
            "{\"choices\":[{\"finish_reason\":\"stop\",\"index\":0,",
            "\"message\":{\"content\":\"你好\",\"role\":\"assistant\"}}],",
            "\"usage\":{\"completion_tokens\":3,\"prompt_tokens\":5,\"total_tokens\":8}}"
        );
        let events = serve_one_chat_response("application/json", BODY).await;

        assert_eq!(collect_text(&events), "你好");
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        let Some(ModelStreamEvent::MessageStop { usage, .. }) = events.last() else {
            panic!("the stream ends with message_stop");
        };
        assert_eq!(usage.output_tokens, 3);
    }

    /// The same fallback carrying a 401 inside a 200 body. Silently completing an
    /// empty turn on this is how a wrong API key looked like "the app does
    /// nothing": the envelope has to fail the turn.
    #[tokio::test]
    async fn a_non_stream_error_envelope_on_a_200_fails_instead_of_completing_empty() {
        const BODY: &str = concat!(
            "{\"choices\":[{\"finish_reason\":\"stop\",\"index\":0,\"message\":{\"content\":\" \"}}],",
            "\"error\":{\"code\":\"model_proxy_access_control\",\"message\":\"Incorrect API key provided!\"},",
            "\"error_code\":401,",
            "\"usage\":{\"completion_tokens\":10,\"prompt_tokens\":0,\"total_tokens\":10}}"
        );
        let events = serve_one_chat_response("application/json", BODY).await;

        let [ModelStreamEvent::UsageSnapshot { usage }, ModelStreamEvent::Error { error }] =
            events.as_slice()
        else {
            panic!("an error envelope retains reported usage and has no success event: {events:?}");
        };
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 10);
        assert_eq!(usage.cache_read_input_tokens, 0);
        assert_eq!(usage.cache_creation_input_tokens, 0);
        assert_eq!(usage.total_tokens, Some(10));
        assert_eq!(error.kind, ProviderErrorKind::Auth);
        assert_eq!(error.status, Some(401));
        assert_eq!(error.code, Some("model_proxy_access_control".into()));
        assert_eq!(error.message, "Incorrect API key provided!");
        assert!(String::from_utf16_lossy(error.message.as_utf16()).contains("Incorrect API key"));
    }

    /// An event stream that closes without a single chunk answered nothing. It
    /// used to settle as a finished, empty message, which reads as a successful
    /// turn that produced no reply.
    #[tokio::test]
    async fn an_event_stream_with_no_chunk_is_an_error_not_an_empty_message() {
        let events = serve_one_chat_response("text/event-stream", "data: [DONE]\n\n").await;

        let [ModelStreamEvent::Error { error }] = events.as_slice() else {
            panic!("a chunkless stream is one error event: {events:?}");
        };
        assert_eq!(error.kind, ProviderErrorKind::Network);
        assert!(String::from_utf16_lossy(error.message.as_utf16())
            .contains("without sending any completion chunk"));
    }

    /// An endpoint that streams SSE while declaring something else. Its body is
    /// buffered on the strength of that declaration, so it still has to go
    /// through the framer: deciding by the header alone would refuse a response
    /// that is perfectly readable.
    #[tokio::test]
    async fn a_mislabeled_event_stream_is_still_framed() {
        const BODY: &str = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"你\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"好\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let events = serve_one_chat_response("application/json", BODY).await;

        assert_eq!(collect_text(&events), "你好");
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }

    /// A body that is neither an event stream nor JSON (a proxy's error page)
    /// names itself in the failure, bounded to an excerpt.
    #[tokio::test]
    async fn a_non_stream_non_json_body_reports_a_bounded_excerpt() {
        let events =
            serve_one_chat_response("text/html", "<html><body>502 Bad Gateway</body></html>").await;

        let [ModelStreamEvent::Error { error }] = events.as_slice() else {
            panic!("an unreadable body is one error event: {events:?}");
        };
        assert!(String::from_utf16_lossy(error.message.as_utf16()).contains("502 Bad Gateway"));
    }

    /// One request, one canned response, then close: the shape of a gateway
    /// answering a streaming request however it pleases.
    async fn serve_one_chat_response(
        content_type: &'static str,
        body: &'static str,
    ) -> Vec<ModelStreamEvent> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            capture_http_request(&mut socket).await;
            socket
                .write_all(http_response("200 OK", content_type, body).as_bytes())
                .await
                .unwrap();
        });
        let provider = OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("fixture-token".to_string()),
            base_url: Some(format!("http://{address}/v1")),
            default_model: Some("qwen3.8".to_string()),
            api: Some(OpenAiApi::Chat),
            ..Default::default()
        });
        let events = provider
            .stream(base_req(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("the fixture served its request")
            .unwrap();
        events
    }

    #[test]
    fn provider_trace_records_settlement_evidence_only_when_enabled() {
        let path = crate::observability::provider_diagnostics::log_path_named(
            crate::observability::provider_diagnostics::TRACE_LOG_FILE_NAME,
        );
        let marker = format!("trace-fixture-{}", std::process::id());
        record_provider_trace(
            true,
            &ProviderTraceRecord {
                api: "chat",
                model: &marker,
                request_id: Some("req-1"),
                raw_finish_reason: None,
                saw_done: false,
                mapped_any: true,
                buffered: false,
                stop_reason: "end_turn",
                input_tokens: 1,
                output_tokens: 2,
                cache_read_tokens: 3,
                total_tokens: 6,
                visible_chars: 4,
                elapsed_ms: 7,
            },
        );
        record_provider_trace(
            false,
            &ProviderTraceRecord {
                api: "chat",
                model: "trace-disabled-fixture",
                request_id: None,
                raw_finish_reason: Some("stop"),
                saw_done: true,
                mapped_any: true,
                buffered: false,
                stop_reason: "end_turn",
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                total_tokens: 0,
                visible_chars: 0,
                elapsed_ms: 0,
            },
        );
        let log = std::fs::read_to_string(&path).unwrap_or_default();
        let line = log
            .lines()
            .rfind(|line| line.contains(&marker))
            .unwrap_or_else(|| panic!("the enabled record landed: {log}"))
            .to_string();
        assert!(
            line.contains("\"kind\":\"provider_request_settled\""),
            "{line}"
        );
        assert!(line.contains("\"sawDone\":false"), "{line}");
        assert!(line.contains("\"mappedAny\":true"), "{line}");
        assert!(line.contains("\"visibleChars\":4"), "{line}");
        assert!(line.contains("\"requestId\":\"req-1\""), "{line}");
        // No wire marker arrived, so the record must not claim one.
        assert!(!line.contains("rawFinishReason"), "{line}");
        assert!(!log.contains("trace-disabled-fixture"), "{log}");
    }

    fn collect_text(events: &[ModelStreamEvent]) -> RuntimeString {
        events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::TextDelta { text } => Some(text.clone()),
                _ => None,
            })
            .fold(RuntimeString::default(), |text, delta| text.concat(&delta))
    }

    #[test]
    fn openai_compact_boundary_history() {
        let profile = openai_behavior_profile(&RuntimeString::from("gpt-5"));
        for summary in ["resume summary", "compacted summary"] {
            let messages = vec![Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: RuntimeString::from(format!("compact-{summary}")),
                summary: RuntimeString::from(summary),
            })];
            let expected = Value::Array(vec![vobj([
                ("role", vs("user")),
                (
                    "content",
                    vs(RuntimeString::from(format!(
                        "# Earlier Conversation Summary\n\n{summary}"
                    ))),
                ),
            ])]);
            assert_eq!(
                to_chat_messages_with_profile(&[], &messages, &profile, &[], None),
                expected
            );
            assert_eq!(
                to_responses_input_with_profile(&messages, &[], &profile),
                expected
            );
        }
    }

    fn function_spec(name: &str, description: &str, input_schema: Value) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema,
            kind: ToolSpecKind::Function,
            format: None,
        }
    }

    fn freeform_spec(
        name: &str,
        description: &str,
        input_schema: Value,
        format: Option<ToolSpecFormat>,
    ) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema,
            kind: ToolSpecKind::Freeform,
            format,
        }
    }

    fn grammar_spec(
        name: &str,
        description: &str,
        input_schema: Value,
        syntax: ToolSpecGrammarSyntax,
        definition: &str,
    ) -> ToolSpec {
        freeform_spec(
            name,
            description,
            input_schema,
            Some(ToolSpecFormat::Grammar {
                syntax,
                definition: definition.into(),
            }),
        )
    }

    fn lark_spec(name: &str, description: &str, input_schema: Value, definition: &str) -> ToolSpec {
        grammar_spec(
            name,
            description,
            input_schema,
            ToolSpecGrammarSyntax::Lark,
            definition,
        )
    }

    fn regex_spec(
        name: &str,
        description: &str,
        input_schema: Value,
        definition: &str,
    ) -> ToolSpec {
        grammar_spec(
            name,
            description,
            input_schema,
            ToolSpecGrammarSyntax::Regex,
            definition,
        )
    }

    fn tool_search_spec(name: &str, description: &str, input_schema: Value) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema,
            kind: ToolSpecKind::ToolSearch,
            format: None,
        }
    }

    fn required_string_schema(field: &str) -> Value {
        let mut properties = Map::new();
        properties.insert(field, json!({ "type": "string" }));
        vobj([
            ("type", vs("object")),
            ("properties", Value::Object(properties)),
            (
                "required",
                Value::Array(vec![Value::String(RuntimeString::from(field))]),
            ),
            ("additionalProperties", Value::Bool(false)),
        ])
    }

    fn text_spec(name: &str, description: &str) -> ToolSpec {
        freeform_spec(
            name,
            description,
            open_fallback_tool_schema(),
            Some(ToolSpecFormat::Text),
        )
    }

    /// Responses params with the minimal capability set and OpenAI dialect, for
    /// the many tests that don't exercise the reasoning path.
    fn responses_params_default(req: &ModelRequest) -> Result<Value, ProviderError> {
        build_responses_params(
            req,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
        )
    }

    fn base_req() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: "gpt-4o".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![],
            messages: vec![],
            tools: vec![],
            deferred_tools: Vec::new(),
            tool_choice: None,
            max_output_tokens: 1000,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_key: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[test]
    fn collaboration_modes_use_complete_developer_messages_on_both_wires() {
        let mut req = base_req();
        let config = crate::engine::session::resolve_config_default();
        for mode in [
            crate::planning::CollaborationMode::Default,
            crate::planning::CollaborationMode::Plan,
        ] {
            let part = crate::planning::instructions::instruction_part(&config, mode);
            req.system = vec![
                SystemPromptPart {
                    name: "base".into(),
                    text: "Stable instructions".into(),
                    cacheable: Some(true),
                    cache_scope: None,
                },
                part.clone(),
            ];
            req.messages = vec![Message::User(UserMessage {
                question_answer: None,
                uuid: "user-mode".into(),
                content: vec![text_block("Inspect this workspace")],
                is_meta: None,
            })];
            let developer_message = vobj([
                ("role", vs("developer")),
                ("content", Value::String(part.text.clone())),
            ]);
            let chat = to_chat_messages(&req.system, &req.messages);
            assert_eq!(
                chat,
                Value::Array(vec![
                    json!({"role":"system","content":"Stable instructions"}),
                    json!({"role":"user","content":"Inspect this workspace"}),
                    developer_message.clone()
                ])
            );
            let responses = responses_params_default(&req).unwrap();
            assert_eq!(responses["instructions"], json!("Stable instructions"));
            let items = responses["input"].as_array().unwrap();
            assert_eq!(items.last().unwrap(), &developer_message);
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/planning-regression-snapshots");
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(
                path.join(format!("{}-provider-wire.json", mode.wire_name())),
                crate::runtime_values::runtime_json::stringify(&vobj([
                    ("chat", chat),
                    ("responses", responses),
                ]))
                .to_node_utf8(),
            )
            .unwrap();
        }
    }

    /// Changing guidance leaves the stable prefix intact and uses the native
    /// developer role, so it cannot become the latest human request.
    #[test]
    fn bridge_fast_mode_scope_requires_a_supported_source_chat_dialect_and_loopback() {
        for source in ["qoder", "qoderwork"] {
            for enabled in [true, false] {
                assert_eq!(
                    local_bridge_fast_mode(
                        Some(source),
                        OpenAiApi::Chat,
                        OpenAiReasoningDialect::Qoder,
                        Some("http://127.0.0.1:5000"),
                        enabled
                    ),
                    Some(enabled)
                );
            }
        }
        for source in ["qwenwork", "codex", "custom"] {
            assert_eq!(
                local_bridge_fast_mode(
                    Some(source),
                    OpenAiApi::Chat,
                    OpenAiReasoningDialect::Qoder,
                    Some("http://localhost:5000"),
                    true
                ),
                None
            );
        }
        assert_eq!(
            local_bridge_fast_mode(
                Some("qoder"),
                OpenAiApi::Responses,
                OpenAiReasoningDialect::Qoder,
                Some("http://localhost:5000"),
                true
            ),
            None
        );
        assert_eq!(
            local_bridge_fast_mode(
                Some("qoder"),
                OpenAiApi::Chat,
                OpenAiReasoningDialect::OpenAi,
                Some("http://localhost:5000"),
                true
            ),
            None
        );
        assert_eq!(
            local_bridge_fast_mode(
                Some("qoder"),
                OpenAiApi::Chat,
                OpenAiReasoningDialect::Qoder,
                Some("https://api.example.test"),
                true
            ),
            None
        );
    }

    #[tokio::test]
    async fn bridge_fast_mode_is_explicit_per_request_and_absent_for_other_chat_providers() {
        for expected in [Some(true), Some(false), None] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (_, body) = capture_http_request(&mut socket).await;
                socket.write_all(http_response("200 OK", "application/json", r#"{"id":"fixture","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#).as_bytes()).await.unwrap();
                body
            });
            let provider = OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture".into()),
                base_url: Some(format!("http://{address}/v1")),
                api: Some(OpenAiApi::Chat),
                bridge_fast_mode: expected,
                ..Default::default()
            });
            let events = provider
                .stream(base_req(), AbortSignal::new())
                .collect::<Vec<_>>()
                .await;
            assert!(!events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
            let body = server.await.unwrap();
            let value = runtime_json::parse(&RuntimeString::from(body)).unwrap();
            assert_eq!(value.get("fast_mode").and_then(Value::as_bool), expected);
        }
    }

    #[test]
    fn responses_instructions_exclude_a_declared_volatile_part() {
        let mut req = base_req();
        req.system = vec![
            SystemPromptPart {
                name: "identity".into(),
                text: "stable identity".into(),
                cacheable: Some(true),
                cache_scope: None,
            },
            SystemPromptPart {
                name: "task-plan-reminder".into(),
                text: "2 tasks still open".into(),
                cacheable: Some(false),
                cache_scope: None,
            },
        ];
        req.messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![text_block("hello")],
            is_meta: None,
        })];

        let body = responses_params_default(&req).unwrap();

        assert_eq!(body["instructions"], json!("stable identity"));
        // `gpt-4o` resolves to the OpenAI developer style, whose reminder
        // envelope carries a heading; role selection belongs to the wire.
        let input = body["input"].as_array().unwrap();
        assert_eq!(input.len(), 2, "the reminder is appended as one input item");
        assert_eq!(input[0]["role"], json!("user"));
        assert_eq!(input[0]["content"], json!("hello"));
        assert_eq!(input[1]["role"], json!("developer"));
        assert!(
            input[1]["content"]
                .as_str()
                .is_some_and(|text| text.contains("2 tasks still open")),
            "{input:?}"
        );

        // A changed reminder leaves `instructions` untouched.
        let mut changed = req.clone();
        changed.system[1].text = "1 task still open".into();
        assert_eq!(
            responses_params_default(&changed).unwrap()["instructions"],
            body["instructions"],
            "the cached instructions must not move when a reminder changes"
        );
    }

    #[test]
    fn model_media_metadata_is_propagated_without_borrowing_main_limits_for_unknown_models() {
        use crate::types::provider::ImageLimits;
        use std::num::NonZeroU32;
        let main = ModelCapabilities {
            image_limits: Some(ImageLimits {
                max_width: NonZeroU32::new(2_000),
                max_base64_size: NonZeroU32::new(5_242_880),
                ..Default::default()
            }),
            max_media_blocks: NonZeroU32::new(600),
            context_window: Some(200_000),
            ..Default::default()
        };
        let summary = ModelCapabilities {
            image_limits: Some(ImageLimits {
                max_height: NonZeroU32::new(1_200),
                max_base64_size: NonZeroU32::new(10_485_760),
                target_raw_size: NonZeroU32::new(512_000),
                ..Default::default()
            }),
            max_media_blocks: NonZeroU32::new(42),
            ..Default::default()
        };
        let provider = OpenAIProvider::new(OpenAIProviderOptions {
            default_model: Some("main".into()),
            capabilities: Some(main.clone()),
            capabilities_by_model: Some(HashMap::from([("summary".into(), summary.clone())])),
            ..Default::default()
        });
        let main_caps = provider.capabilities(&"main".into());
        assert_eq!(main_caps.image_limits, main.image_limits);
        assert_eq!(main_caps.max_media_blocks, main.max_media_blocks);
        let summary_caps = provider.capabilities(&"summary".into());
        assert_eq!(summary_caps.image_limits, summary.image_limits);
        assert_eq!(summary_caps.max_media_blocks, summary.max_media_blocks);
        let unknown = provider.capabilities(&"undeclared".into());
        assert_eq!(unknown.context_window, 200_000);
        assert_eq!(unknown.image_limits, None);
        assert_eq!(unknown.max_media_blocks, None);
        let mut request = base_req();
        request.model = "undeclared".into();
        request.bind_media_capabilities(&unknown);
        assert_eq!(request.image_limits, Some(ImageLimits::default()));
        assert_eq!(request.max_media_blocks.unwrap().get(), 100);
    }

    fn request_with_tools(tools: Vec<ToolSpec>) -> ModelRequest {
        ModelRequest {
            tools,
            ..base_req()
        }
    }

    #[test]
    fn responses_forced_choice_maps_final_kind_and_rejects_tool_search() {
        let function = function_spec("read", "Read a file.", required_string_schema("path"));
        let freeform = freeform_spec(
            "apply_patch",
            "Apply a patch.",
            required_string_schema("patch"),
            Some(ToolSpecFormat::Text),
        );
        let native_search = tool_search_spec(
            "tool_search",
            "Search deferred tools.",
            required_string_schema("query"),
        );
        let tools = vec![function.clone(), freeform.clone(), native_search.clone()];

        assert_eq!(
            responses_forced_choice(&RuntimeString::from("read"), &tools).unwrap(),
            json!({ "type": "function", "name": "read" })
        );
        assert_eq!(
            responses_forced_choice(&RuntimeString::from("apply_patch"), &tools).unwrap(),
            json!({ "type": "custom", "name": "apply_patch" })
        );

        let native_search_error =
            responses_forced_choice(&RuntimeString::from("tool_search"), &tools).unwrap_err();
        assert_eq!(native_search_error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(native_search_error.status, Some(400));
        assert_eq!(native_search_error.retry_after_ms, None);
        assert_eq!(native_search_error.request_id, None);
        assert_eq!(
            native_search_error.message,
            "OpenAI Responses tool choice is invalid: forced native tool_search is not supported; use `auto` or `any`."
        );

        let missing_error =
            responses_forced_choice(&RuntimeString::from("missing"), &tools).unwrap_err();
        assert_eq!(missing_error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(missing_error.status, Some(400));
        assert_eq!(
            missing_error.message,
            "OpenAI Responses tool choice is invalid: forced tool \"missing\" is not provider-visible; available tools: read, apply_patch, tool_search."
        );

        for duplicate_tools in [
            vec![
                function_spec("shared", "Function.", required_string_schema("value")),
                tool_search_spec(
                    "shared",
                    "Native tool search.",
                    required_string_schema("query"),
                ),
            ],
            vec![
                tool_search_spec(
                    "shared",
                    "Native tool search.",
                    required_string_schema("query"),
                ),
                function_spec("shared", "Function.", required_string_schema("value")),
            ],
        ] {
            let duplicate_error =
                responses_forced_choice(&RuntimeString::from("shared"), &duplicate_tools)
                    .unwrap_err();
            assert_eq!(
                duplicate_error.message,
                "OpenAI Responses tool choice is invalid: forced native tool_search is not supported; use `auto` or `any`."
            );
        }

        let shared_function = function_spec("shared", "Function.", required_string_schema("value"));
        let shared_freeform = freeform_spec(
            "shared",
            "Custom.",
            required_string_schema("source"),
            Some(ToolSpecFormat::Text),
        );
        assert_eq!(
            responses_forced_choice(
                &RuntimeString::from("shared"),
                &[shared_function.clone(), shared_freeform.clone()]
            )
            .unwrap(),
            json!({ "type": "function", "name": "shared" })
        );
        assert_eq!(
            responses_forced_choice(
                &RuntimeString::from("shared"),
                &[shared_freeform, shared_function],
            )
            .unwrap(),
            json!({ "type": "custom", "name": "shared" })
        );
    }

    #[test]
    fn responses_rejects_forced_native_tool_search_locally() {
        let mut request = base_req();
        request.tools = vec![ToolSpec {
            name: "tool_search".into(),
            description: "Search deferred tools.".into(),
            input_schema: schema_obj(),
            kind: ToolSpecKind::ToolSearch,
            format: None,
        }];
        request.tool_choice = Some(ToolChoice::Tool {
            name: "tool_search".into(),
        });
        let native_search_error = validate_responses_request_choice(&request)
            .expect("forced native tool_search must fail locally");
        assert_eq!(native_search_error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(native_search_error.status, Some(400));
        assert_eq!(native_search_error.retry_after_ms, None);
        assert_eq!(native_search_error.request_id, None);
        assert_eq!(
            native_search_error.message,
            "OpenAI Responses tool choice is invalid: forced native tool_search is not supported; use `auto` or `any`."
        );

        for tools in [
            vec![
                function_spec("shared", "Function.", schema_obj()),
                ToolSpec {
                    name: "shared".into(),
                    description: "Native tool search.".into(),
                    input_schema: schema_obj(),
                    kind: ToolSpecKind::ToolSearch,
                    format: None,
                },
            ],
            vec![
                ToolSpec {
                    name: "shared".into(),
                    description: "Native tool search.".into(),
                    input_schema: schema_obj(),
                    kind: ToolSpecKind::ToolSearch,
                    format: None,
                },
                function_spec("shared", "Function.", schema_obj()),
            ],
        ] {
            request.tools = tools;
            request.tool_choice = Some(ToolChoice::Tool {
                name: "shared".into(),
            });
            let duplicate_error = validate_responses_request_choice(&request)
                .expect("any matching native tool_search fails closed regardless order");
            assert_eq!(
                duplicate_error.message,
                "OpenAI Responses tool choice is invalid: forced native tool_search is not supported; use `auto` or `any`."
            );
        }

        request.tools.clear();
        request.web_search = Some(Default::default());
        request.tool_choice = Some(ToolChoice::Any);
        assert_eq!(validate_responses_request_choice(&request), None);

        request.tool_choice = Some(ToolChoice::Tool {
            name: "web_search".into(),
        });
        let anonymous_search_error = validate_responses_request_choice(&request)
            .expect("anonymous native web search is not forceable by name");
        assert_eq!(
            anonymous_search_error.message,
            "OpenAI Responses tool choice is invalid: forced tool \"web_search\" is not provider-visible; available tools: (none)."
        );

        request.web_search = None;
        request.tools = vec![
            function_spec("read", "Read a file.", schema_obj()),
            text_spec("apply_patch", "Apply a patch."),
        ];
        for name in ["read", "apply_patch"] {
            request.tool_choice = Some(ToolChoice::Tool { name: name.into() });
            assert_eq!(validate_responses_request_choice(&request), None, "{name}");
        }

        request.tool_choice = Some(ToolChoice::Tool {
            name: "Read".into(),
        });
        let case_error =
            validate_responses_request_choice(&request).expect("forced matching is case-sensitive");
        assert_eq!(
            case_error.message,
            "OpenAI Responses tool choice is invalid: forced tool \"Read\" is not provider-visible; available tools: read, apply_patch."
        );
        request.tool_choice = Some(ToolChoice::Tool {
            name: "missing".into(),
        });
        assert!(validate_responses_request_choice(&request).is_some());
    }

    #[test]
    fn chat_validates_the_function_fallback_set() {
        let mut request = base_req();
        request.tools = vec![text_spec("apply_patch", "Apply a patch.")];
        request.tool_choice = Some(ToolChoice::Tool {
            name: "apply_patch".into(),
        });
        assert_eq!(validate_chat_request_choice(&request), None);

        request.tool_choice = Some(ToolChoice::Tool {
            name: "missing".into(),
        });
        let missing = validate_chat_request_choice(&request)
            .expect("missing Chat fallback function must fail closed");
        assert_eq!(
            missing.message,
            "OpenAI Chat tool choice is invalid: forced tool \"missing\" is not provider-visible; available tools: apply_patch."
        );

        request.tools.clear();
        request.web_search = Some(Default::default());
        request.tool_choice = Some(ToolChoice::Any);
        let any_error = validate_chat_request_choice(&request)
            .expect("Chat ignores native web search, so any must still fail closed");
        assert_eq!(
            any_error.message,
            "OpenAI Chat tool choice is invalid: `any` requires at least one provider-visible tool."
        );

        request.tool_choice = Some(ToolChoice::Auto);
        assert_eq!(validate_chat_request_choice(&request), None);
        request.tool_choice = Some(ToolChoice::None);
        assert_eq!(validate_chat_request_choice(&request), None);
    }

    #[tokio::test]
    async fn openai_stream_invalid_choices_yield_one_error_before_auth() {
        let chat_auth = Arc::new(CountingAuth::default());
        let chat = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Chat),
            auth_source: Some(chat_auth.clone()),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut chat_request = base_req();
        chat_request.web_search = Some(Default::default());
        chat_request.tool_choice = Some(ToolChoice::Any);
        let chat_events: Vec<ModelStreamEvent> = chat
            .stream(chat_request, AbortSignal::new())
            .collect()
            .await;
        assert_eq!(chat_auth.calls.load(Ordering::SeqCst), 0);
        assert_eq!(chat_events.len(), 1);
        let ModelStreamEvent::Error { error } = &chat_events[0] else {
            panic!("expected one Chat validation error");
        };
        assert_eq!(
            error.message,
            "OpenAI Chat tool choice is invalid: `any` requires at least one provider-visible tool."
        );

        let responses_auth = Arc::new(CountingAuth::default());
        let responses = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            auth_source: Some(responses_auth.clone()),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut responses_request = base_req();
        responses_request.tools = vec![ToolSpec {
            name: "tool_search".into(),
            description: "Search deferred tools.".into(),
            input_schema: schema_obj(),
            kind: ToolSpecKind::ToolSearch,
            format: None,
        }];
        responses_request.tool_choice = Some(ToolChoice::Tool {
            name: "tool_search".into(),
        });
        let responses_events: Vec<ModelStreamEvent> = responses
            .stream(responses_request, AbortSignal::new())
            .collect()
            .await;
        assert_eq!(responses_auth.calls.load(Ordering::SeqCst), 0);
        assert_eq!(responses_events.len(), 1);
        let ModelStreamEvent::Error { error } = &responses_events[0] else {
            panic!("expected one Responses validation error");
        };
        assert_eq!(
            error.message,
            "OpenAI Responses tool choice is invalid: forced native tool_search is not supported; use `auto` or `any`."
        );
    }

    fn schema_obj() -> Value {
        json!({ "type": "object" })
    }

    fn text_block(t: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: t.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn user_msg(text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: "user".into(),
            content: vec![text_block(text)],
            is_meta: None,
        })
    }

    // ---- classifier helpers ----

    #[test]
    fn http_error_body_preserves_openai_fields_with_lone_surrogate_sibling() {
        let body = r#"{"error":{"message":"request exceeds model capacity","code":"context_length_exceeded","type":"invalid_request_error","request_id":"req_error_123"},"diagnostic":"\uD800"}"#;
        let raw = raw_http_error(400, &HashMap::new(), body);

        assert_eq!(raw.message, "request exceeds model capacity");
        assert_eq!(raw.code.as_deref(), Some("context_length_exceeded"));
        assert_eq!(raw.body_type.as_deref(), Some("invalid_request_error"));
        assert_eq!(raw.error_request_id.as_deref(), Some("req_error_123"));

        let classified = classify_error(&raw);
        assert_eq!(classified.kind, ProviderErrorKind::PromptTooLong);
        assert_eq!(classified.status, Some(400));
        assert!(classified
            .request_id
            .as_ref()
            .is_some_and(|request_id| request_id == "req_error_123"));
        assert_eq!(classified.message, "request exceeds model capacity");
    }

    fn caps_with_levels(levels: &[EffortLevel]) -> ModelCapabilities {
        use crate::types::provider::EffortCapability;
        ModelCapabilities {
            effort: Some(EffortCapability {
                levels: levels.to_vec(),
                default: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn qoder_serialized_model_requests_keep_per_turn_and_worker_effort() {
        let caps = caps_with_levels(&[
            EffortLevel::Low,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ]);
        let mut request = base_req();
        // Session low -> one Ultra turn -> explicit low worker -> ordinary max.
        for (selected, expected) in [
            (EffortLevel::Low, "low"),
            (EffortLevel::Ultra, "xhigh"),
            (EffortLevel::Low, "low"),
            (EffortLevel::Max, "max"),
        ] {
            request.effort = Some(EffortValue::Level(selected));
            let body = build_chat_params_caps(&request, &caps, OpenAiReasoningDialect::Qoder);
            assert_eq!(body["reasoning_effort"], vs(expected));
        }
        request.effort = Some(EffortValue::Level(EffortLevel::Ultra));
        let no_effort = build_chat_params_caps(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::Qoder,
        );
        assert!(no_effort.get("reasoning_effort").is_none());
    }

    #[test]
    fn ultra_is_local_and_subscription_wire_uses_supported_inference_effort() {
        let mut request = base_req();
        request.effort = Some(EffortValue::Level(EffortLevel::Ultra));
        for (levels, expected) in [
            (
                vec![
                    EffortLevel::Low,
                    EffortLevel::High,
                    EffortLevel::Xhigh,
                    EffortLevel::Max,
                ],
                Some("xhigh"),
            ),
            (vec![EffortLevel::High, EffortLevel::Max], Some("high")),
            (vec![EffortLevel::Low, EffortLevel::Medium], Some("medium")),
            (vec![EffortLevel::Low], Some("low")),
            (vec![EffortLevel::Max], Some("max")),
            (vec![EffortLevel::Ultra], None),
            (vec![], None),
        ] {
            let caps = caps_with_levels(&levels);
            let chat = build_chat_params_caps(&request, &caps, OpenAiReasoningDialect::OpenAi);
            assert_eq!(
                chat.get("reasoning_effort")
                    .and_then(Value::as_str)
                    .cloned(),
                expected.map(RuntimeString::from)
            );
            let mut response =
                build_responses_params(&request, &caps, OpenAiReasoningDialect::OpenAi).unwrap();
            adapt_codex_responses_params(&mut response);
            assert_eq!(
                response
                    .get("reasoning")
                    .and_then(|reasoning| reasoning.get("effort"))
                    .and_then(Value::as_str)
                    .cloned(),
                expected.map(RuntimeString::from)
            );
        }
        let absent = build_chat_params_caps(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
        );
        assert!(absent.get("reasoning_effort").is_none());
    }

    #[test]
    fn effort_wire_dialects_and_clamping() {
        use OpenAiReasoningDialect::{DeepSeek, ManagedQwen, OpenAi};
        let all = caps_with_levels(&[
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ]);
        let mut r = base_req();
        // OpenAI vocabulary: every declared rung is spelled verbatim, `max`
        // included. Folding max into xhigh made the top rung unreachable on the
        // models that answer to it.
        r.effort = Some(EffortValue::Level(EffortLevel::Low));
        assert_eq!(effort_for_request(&r, &all, OpenAi), Some("low"));
        r.effort = Some(EffortValue::Level(EffortLevel::Xhigh));
        assert_eq!(effort_for_request(&r, &all, OpenAi), Some("xhigh"));
        r.effort = Some(EffortValue::Level(EffortLevel::Max));
        assert_eq!(effort_for_request(&r, &all, OpenAi), Some("max"));
        // A model that stops at xhigh never resolves to max: the request is
        // clamped into the DECLARED ladder before it is spelled.
        let to_xhigh = caps_with_levels(&[EffortLevel::Low, EffortLevel::High, EffortLevel::Xhigh]);
        assert_eq!(effort_for_request(&r, &to_xhigh, OpenAi), Some("xhigh"));
        // A managed gateway never gets a word it does not know.
        assert_eq!(effort_for_request(&r, &all, ManagedQwen), Some("xhigh"));
        // DeepSeek vocabulary: medium->high, xhigh/max->max.
        r.effort = Some(EffortValue::Level(EffortLevel::Medium));
        assert_eq!(effort_for_request(&r, &all, DeepSeek), Some("high"));
        r.effort = Some(EffortValue::Level(EffortLevel::Max));
        assert_eq!(effort_for_request(&r, &all, DeepSeek), Some("max"));
        // Numeric effort omits the field (endpoint uses its own default).
        r.effort = Some(EffortValue::Number(80));
        assert_eq!(effort_for_request(&r, &all, OpenAi), None);
        // No effort value at all → nothing to send.
        r.effort = None;
        assert_eq!(effort_for_request(&r, &all, OpenAi), None);
    }

    #[test]
    fn no_effort_capability_means_no_reasoning_knob() {
        let mut r = base_req();
        r.effort = Some(EffortValue::Level(EffortLevel::High));
        assert_eq!(
            effort_for_request(
                &r,
                &ModelCapabilities::default(),
                OpenAiReasoningDialect::OpenAi
            ),
            None
        );
    }

    #[test]
    fn effort_clamps_to_declared_ladder() {
        // A ladder without xhigh clamps an xhigh request down to high.
        let no_xhigh =
            caps_with_levels(&[EffortLevel::Low, EffortLevel::Medium, EffortLevel::High]);
        let mut r = base_req();
        r.effort = Some(EffortValue::Level(EffortLevel::Xhigh));
        assert_eq!(
            effort_for_request(&r, &no_xhigh, OpenAiReasoningDialect::OpenAi),
            Some("high")
        );
    }

    #[test]
    fn chat_params_managed_qwen_omit_output_budget_when_effort_set() {
        let levels = caps_with_levels(&[EffortLevel::Low, EffortLevel::Medium, EffortLevel::High]);
        for (level, expected) in [
            (EffortLevel::Low, "low"),
            (EffortLevel::Medium, "medium"),
            (EffortLevel::High, "high"),
        ] {
            let mut r = base_req();
            r.effort = Some(EffortValue::Level(level));
            let params = build_chat_params_caps(&r, &levels, OpenAiReasoningDialect::ManagedQwen);
            assert_eq!(params["reasoning_effort"], vs(expected));
            assert!(
                params.get("max_completion_tokens").is_none(),
                "managed gateway sizes the reasoning budget"
            );
        }
        // No effort set → the output budget IS sent.
        let without_effort = base_req();
        let params = build_chat_params_caps(
            &without_effort,
            &levels,
            OpenAiReasoningDialect::ManagedQwen,
        );
        assert_eq!(params["max_completion_tokens"], json!(1000));
        assert!(params.get("reasoning_effort").is_none());
    }

    #[test]
    fn legacy_tokens_dialect_sends_the_cap_as_max_tokens() {
        // A gateway that reads only the pre-2024 field name IGNORES
        // `max_completion_tokens` outright — measured on WorkBuddy, where a cap of
        // 20 produced 404 completion tokens. Sending the modern name there is
        // worse than an error: the request succeeds, the cap silently does not
        // apply, and a provider that bills per output token meters an unbounded
        // turn. The effort vocabulary is unchanged from OpenAI.
        let levels = caps_with_levels(&[EffortLevel::Low, EffortLevel::High]);
        let mut r = base_req();
        r.effort = Some(EffortValue::Level(EffortLevel::High));
        let params = build_chat_params_caps(&r, &levels, OpenAiReasoningDialect::LegacyTokens);
        assert_eq!(params["max_tokens"], json!(1000));
        assert!(
            params.get("max_completion_tokens").is_none(),
            "the modern name must not be sent: this gateway would ignore it"
        );
        assert_eq!(params["reasoning_effort"], vs("high"));
    }

    #[test]
    fn every_other_dialect_keeps_the_modern_cap_field() {
        // Guard the default: only LegacyTokens opts into the old spelling, so the
        // other four dialects must still emit `max_completion_tokens`.
        for dialect in [
            OpenAiReasoningDialect::OpenAi,
            OpenAiReasoningDialect::DeepSeek,
            OpenAiReasoningDialect::Qoder,
            OpenAiReasoningDialect::LlamaCpp,
        ] {
            let params = build_chat_params_caps(
                &base_req(),
                &caps_with_levels(&[EffortLevel::Low]),
                dialect,
            );
            assert_eq!(
                params["max_completion_tokens"],
                json!(1000),
                "{dialect:?} must keep the modern cap field"
            );
            assert!(params.get("max_tokens").is_none(), "{dialect:?}");
        }
    }

    #[test]
    fn chat_stream_error_chunk_is_not_treated_as_an_empty_message() {
        let mut state = ChatState::default();
        let chunk = json!({
            "error": {
                "code": "invalid_parameter_error",
                "message": "max_completion_tokens must be greater than thinking_budget",
                "type": "invalid_request_error"
            },
            "error_code": 400,
            "request_id": "req-qwen-budget"
        });

        let error = map_chat_stream_chunk(&chunk, &mut state)
            .expect_err("an in-stream gateway error must fail the turn");

        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.status, Some(400));
        assert_eq!(
            error.message,
            "max_completion_tokens must be greater than thinking_budget"
        );
        assert!(error
            .request_id
            .as_ref()
            .is_some_and(|request_id| request_id == "req-qwen-budget"));
        assert!(!state.started);
    }

    #[test]
    fn make_usage_subtracts_cache_and_falls_back_total() {
        // cached is a subset of the prompt total; it is subtracted out of input.
        let u = make_usage(100, 20, 30, 150);
        assert_eq!(u.input_tokens, 70);
        assert_eq!(u.cache_read_input_tokens, 30);
        assert_eq!(u.cache_creation_input_tokens, 0);
        assert_eq!(u.output_tokens, 20);
        assert_eq!(u.total_tokens, Some(150));
        // no server total -> falls back to input+cached+output.
        let u2 = make_usage(100, 20, 30, 0);
        assert_eq!(u2.total_tokens, Some(120));
        // cache larger than prompt clamps.
        let u3 = make_usage(10, 5, 50, 0);
        assert_eq!(u3.input_tokens, 0);
        assert_eq!(u3.cache_read_input_tokens, 10);
    }

    // ---- chat request building ----

    #[test]
    fn chat_falls_back_all_tool_kinds_to_functions_with_real_schemas() {
        let path_schema = required_string_schema("path");
        let patch_schema = required_string_schema("patch");
        let query_schema = required_string_schema("query");
        let request = request_with_tools(vec![
            function_spec("read", "Read a file.", path_schema.clone()),
            freeform_spec(
                "apply_patch",
                "Apply a patch.",
                patch_schema.clone(),
                Some(ToolSpecFormat::Text),
            ),
            tool_search_spec(
                "tool_search",
                "Search deferred tools.",
                query_schema.clone(),
            ),
        ]);

        let body = build_chat_params(&request);

        assert_eq!(
            body["tools"],
            Value::Array(vec![
                vobj([
                    ("type", vs("function")),
                    (
                        "function",
                        vobj([
                            ("name", vs("read")),
                            ("description", vs("Read a file.")),
                            ("parameters", path_schema),
                        ]),
                    ),
                ]),
                vobj([
                    ("type", vs("function")),
                    (
                        "function",
                        vobj([
                            ("name", vs("apply_patch")),
                            ("description", vs("Apply a patch.")),
                            ("parameters", patch_schema),
                        ]),
                    ),
                ]),
                vobj([
                    ("type", vs("function")),
                    (
                        "function",
                        vobj([
                            ("name", vs("tool_search")),
                            ("description", vs("Search deferred tools.")),
                            ("parameters", query_schema),
                        ]),
                    ),
                ]),
            ])
        );
    }

    #[test]
    fn approval_wire_budget_includes_final_schema_and_freeform_grammar() {
        for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
            let provider = OpenAIProvider::new(OpenAIProviderOptions {
                api: Some(api),
                capabilities: Some(ModelCapabilities {
                    structured_output: true,
                    ..Default::default()
                }),
                ..Default::default()
            });
            let mut request = base_req();
            let baseline = provider.approval_request_tokens(&request).unwrap();
            request.output_schema = Some(
                json!({"type":"object", "properties":{"outcome":{"type":"string"}}, "required":["outcome"], "additionalProperties":false}),
            );
            request
                .output_schema
                .as_mut()
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(
                    "description",
                    Value::String(RuntimeString::from("x".repeat(16_384))),
                );
            let with_schema = provider.approval_request_tokens(&request).unwrap();
            assert!(with_schema >= baseline.saturating_add(4096));
            request.tools.push(lark_spec(
                "inspect",
                "Inspect an expression.",
                required_string_schema("expression"),
                &"g".repeat(16_384),
            ));
            let with_tool = provider.approval_request_tokens(&request).unwrap();
            if api == OpenAiApi::Responses {
                assert!(with_tool >= with_schema.saturating_add(4096));
            } else {
                assert!(with_tool > with_schema);
            }
        }
    }

    #[test]
    fn optional_output_fields_preserve_their_schema_in_both_transports() {
        let schema = json!({
            "type": "object",
            "properties": {
                "outcome": {"type": "string", "enum": ["allow", "deny"]},
                "rationale": {"type": "string"}
            },
            "required": ["outcome"],
            "additionalProperties": false
        });
        let mut request = base_req();
        request.output_schema = Some(schema.clone());
        let caps = ModelCapabilities {
            structured_output: true,
            ..Default::default()
        };
        let chat = build_chat_params_caps(&request, &caps, OpenAiReasoningDialect::OpenAi);
        assert_eq!(chat["response_format"]["json_schema"]["schema"], schema);
        assert_eq!(
            chat["response_format"]["json_schema"]["strict"],
            json!(false)
        );
        let responses =
            build_responses_params(&request, &caps, OpenAiReasoningDialect::OpenAi).unwrap();
        assert_eq!(responses["text"]["format"]["schema"], schema);
        assert_eq!(responses["text"]["format"]["strict"], json!(false));
    }

    #[test]
    fn nested_optional_output_fields_disable_strict_mode() {
        assert!(!supports_strict_output_schema(&json!({
            "type": "object", "properties": {"assessment": {
                "type": "object", "properties": {"outcome": {"type":"string"}, "rationale":{"type":"string"}},
                "required": ["outcome"], "additionalProperties": false
            }}, "required":["assessment"], "additionalProperties":false
        })));
        assert!(supports_strict_output_schema(&json!({
            "type":"object", "properties":{"outcome":{"type":"string"}},
            "required":["outcome"], "additionalProperties":false
        })));
    }

    #[test]
    fn chat_params_minimal() {
        let v = build_chat_params(&base_req());
        assert_eq!(v["model"], json!("gpt-4o"));
        assert_eq!(v["stream"], json!(true));
        assert_eq!(v["stream_options"], json!({ "include_usage": true }));
        assert_eq!(v["max_completion_tokens"], json!(1000));
        assert_eq!(v["messages"], json!([]));
        assert!(v.get("reasoning_effort").is_none());
        assert!(v.get("tools").is_none());
        assert!(v.get("response_format").is_none());
        assert!(v.get("prompt_cache_key").is_none());
    }

    #[test]
    fn chat_params_structured_effort_tools_choice_cache() {
        let mut r = base_req();
        r.model = "gpt-5".into();
        r.effort = Some(EffortValue::Level(EffortLevel::High));
        r.output_schema = Some(schema_obj());
        r.tools = vec![function_spec("bash", "run", schema_obj())];
        r.tool_choice = Some(ToolChoice::Tool {
            name: "bash".into(),
        });
        r.cache_key = Some("conv-1".into());
        let v = build_chat_params_caps(
            &r,
            &caps_with_levels(&[EffortLevel::Low, EffortLevel::Medium, EffortLevel::High]),
            OpenAiReasoningDialect::OpenAi,
        );
        assert_eq!(v["reasoning_effort"], json!("high"));
        assert_eq!(
            v["tools"],
            json!([{ "type": "function", "function": { "name": "bash", "description": "run", "parameters": { "type": "object" } } }])
        );
        assert_eq!(
            v["tool_choice"],
            json!({ "type": "function", "function": { "name": "bash" } })
        );
        assert_eq!(
            v["response_format"],
            json!({ "type": "json_schema", "json_schema": { "name": "result", "schema": { "type": "object" }, "strict": true } })
        );
        assert_eq!(v["prompt_cache_key"], json!("conv-1"));
    }

    #[test]
    fn chat_params_freeform_exec_falls_back_to_source_schema_without_renaming() {
        let mut r = base_req();
        r.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec("wait", "wait for exec completion", schema_obj()),
        ];

        let v = build_chat_params(&r);
        assert_eq!(v["tools"][0]["function"]["name"], json!("exec"));
        assert_eq!(v["tools"][1]["function"]["name"], json!("wait"));
        assert!(v["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .all(|tool| tool["function"]["name"] != json!("code_exec")));
        assert_eq!(v["tools"][0]["type"], json!("function"));
        assert_eq!(v["tools"][0]["function"]["name"], json!("exec"));
        assert_eq!(
            v["tools"][0]["function"]["parameters"],
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true
            })
        );
    }

    #[test]
    fn chat_tool_choice_variants() {
        assert_eq!(to_chat_tool_choice(&ToolChoice::Auto), json!("auto"));
        assert_eq!(to_chat_tool_choice(&ToolChoice::Any), json!("required"));
        assert_eq!(to_chat_tool_choice(&ToolChoice::None), json!("none"));
    }

    #[test]
    fn chat_stop_reason_mapping() {
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("tool_calls")),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("function_call")),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("length")),
            ModelStopReason::MaxTokens
        );
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("content_filter")),
            ModelStopReason::Refusal
        );
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("stop")),
            ModelStopReason::EndTurn
        );
        assert_eq!(
            map_chat_stop_reason(&RuntimeString::from("weird")),
            ModelStopReason::EndTurn
        );
    }

    #[test]
    fn chat_messages_system_assistant_tool_and_media() {
        let system = vec![
            SystemPromptPart {
                name: "a".into(),
                text: "sys1".into(),
                cacheable: None,
                cache_scope: None,
            },
            SystemPromptPart {
                name: "b".into(),
                text: "sys2".into(),
                cacheable: None,
                cache_scope: None,
            },
        ];
        let messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![text_block("hello")],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![
                    text_block("sure"),
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "call_1".into(),
                        name: "read".into(),
                        input: json!({ "path": "/x" }),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                ],
                model: "gpt-4o".into(),
                usage: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "t1".into(),
                tool_use_id: "call_1".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "file body".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: Some(vec![ContentBlock::Document(DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "QQ==".into(),
                    filename: Some("d.pdf".into()),
                })]),
            }),
            Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: "c1".into(),
                summary: "sum".into(),
            }),
        ];
        let v = to_chat_messages(&system, &messages);
        let arr = v.as_array().unwrap();
        // system, user, assistant, tool, media(user), compact(user)
        assert_eq!(arr.len(), 6);
        assert_eq!(
            arr[0],
            json!({ "role": "system", "content": "sys1\n\nsys2" })
        );
        assert_eq!(arr[1], json!({ "role": "user", "content": "hello" }));
        assert_eq!(
            arr[2],
            json!({ "role": "assistant", "content": "sure", "tool_calls": [ { "id": "call_1", "type": "function", "function": { "name": "read", "arguments": "{\"path\":\"/x\"}" } } ] })
        );
        assert_eq!(
            arr[3],
            json!({ "role": "tool", "tool_call_id": "call_1", "content": "file body" })
        );
        // Media flushed as its own user message AFTER the tool run, before compact.
        assert_eq!(
            arr[4],
            json!({ "role": "user", "content": [ { "type": "file", "file": { "file_data": "data:application/pdf;base64,QQ==", "filename": "d.pdf" } } ] })
        );
        assert_eq!(
            arr[5],
            json!({ "role": "user", "content": "[Earlier conversation summarized]\nsum" })
        );
    }

    fn part(name: &str, text: &str, cacheable: Option<bool>) -> SystemPromptPart {
        SystemPromptPart {
            name: RuntimeString::from(name),
            text: RuntimeString::from(text),
            cacheable,
            cache_scope: None,
        }
    }

    /// A changing control update stays after the reusable prefix while retaining
    /// the Chat protocol's system role.
    #[test]
    fn a_declared_volatile_part_leaves_the_system_string_for_a_trailing_message() {
        let system = vec![
            part("identity", "stable identity", Some(true)),
            part("env", "cwd /x", Some(true)),
            part("task-plan-reminder", "2 tasks still open", Some(false)),
        ];
        let messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![text_block("hello")],
            is_meta: None,
        })];

        let arr = to_chat_messages(&system, &messages);
        let arr = arr.as_array().unwrap();

        // system, user, then the reminder as its own trailing message.
        assert_eq!(arr.len(), 3);
        assert_eq!(
            arr[0],
            json!({ "role": "system", "content": "stable identity\n\ncwd /x" }),
            "the cached string must be byte-stable across reminder changes"
        );
        assert_eq!(arr[1], json!({ "role": "user", "content": "hello" }));
        assert_eq!(arr[2]["role"], json!("system"));
        assert!(
            arr[2]["content"]
                .as_str()
                .is_some_and(|text| text.contains("not a new user request")
                    && text.ends_with("2 tasks still open")),
            "{arr:?}"
        );

        // A changed reminder now touches only the last message.
        let changed = vec![
            part("identity", "stable identity", Some(true)),
            part("env", "cwd /x", Some(true)),
            part("task-plan-reminder", "1 task still open", Some(false)),
        ];
        let after = to_chat_messages(&changed, &messages);
        let after = after.as_array().unwrap();
        assert_eq!(after[0], arr[0], "the system string did not move");
        assert_eq!(after[1], arr[1], "the conversation did not move");
        assert_ne!(after[2], arr[2], "only the trailing reminder changed");
    }

    /// One-shot auxiliary requests build their whole system prompt from unmarked
    /// parts: a compaction summary, a memory recall. Nothing there is meant to be
    /// reused, so demoting it all to a trailing message would change what those
    /// models read for no cache benefit.
    #[test]
    fn unmarked_parts_stay_in_the_system_string() {
        let system = vec![
            part("compact", "summarize", None),
            part("extra", "keep", None),
        ];
        let messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![text_block("hello")],
            is_meta: None,
        })];

        let arr = to_chat_messages(&system, &messages);
        let arr = arr.as_array().unwrap();

        assert_eq!(arr.len(), 2, "no trailing reminder is invented");
        assert_eq!(
            arr[0],
            json!({ "role": "system", "content": "summarize\n\nkeep" })
        );
    }

    /// The reminder must never land inside an open tool batch: strict validators
    /// reject an instruction message between a turn's calls and their results.
    #[test]
    fn the_trailing_reminder_comes_after_the_tool_batch() {
        let system = vec![
            part("identity", "stable identity", Some(true)),
            part("task-plan-reminder", "open plan", Some(false)),
        ];
        let messages = vec![
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: "call_1".into(),
                    name: "read".into(),
                    input: json!({ "path": "/x" }),
                    tool_kind: None,
                    thought_signature: None,
                })],
                model: "model-x".into(),
                usage: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "t1".into(),
                tool_use_id: "call_1".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "file body".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: None,
            }),
        ];

        let arr = to_chat_messages(&system, &messages);
        let arr = arr.as_array().unwrap();

        // system, assistant, tool, reminder.
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[1].get("role"), Some(&json!("assistant")));
        assert_eq!(arr[2].get("role"), Some(&json!("tool")));
        assert_eq!(
            arr[3]["role"],
            json!("system"),
            "the reminder follows the settled batch, never inside it"
        );
        assert!(arr[3]["content"]
            .as_str()
            .is_some_and(|text| text.ends_with("open plan")));
    }

    #[test]
    fn chat_wire_wraps_replayed_freeform_text_into_a_json_object() {
        let patch = RuntimeString::from("*** Begin Patch\n*** End Patch\n");
        let freeform_history = vec![Message::Assistant(AssistantMessage {
            uuid: "a-freeform".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "call_patch".into(),
                name: "apply_patch".into(),
                input: Value::String(patch.clone()),
                tool_kind: Some(ProviderToolCallKind::Custom),
                thought_signature: None,
            })],
            model: "gpt-5".into(),
            usage: None,
        })];
        let tools = vec![freeform_spec(
            "apply_patch",
            "Apply a patch.",
            required_string_schema("patch"),
            Some(ToolSpecFormat::Text),
        )];
        let qwen = resolve_provider_behavior_profile(None, &RuntimeString::from("qwen3-max"));

        let arguments_of = |chat: &Value| -> RuntimeString {
            chat.as_array().unwrap()[0]
                .get("tool_calls")
                .unwrap()
                .as_array()
                .unwrap()[0]
                .get("function")
                .unwrap()
                .get("arguments")
                .unwrap()
                .as_string()
                .unwrap()
                .clone()
        };

        // The gateway contract: `function.arguments` must PARSE as a JSON
        // object, holding the raw patch under the key the schema declares.
        let with_spec = to_chat_messages_with_profile(&[], &freeform_history, &qwen, &tools, None);
        let parsed = runtime_json::parse(&arguments_of(&with_spec)).expect("arguments must parse");
        assert_eq!(parsed, vobj([("patch", vs(patch.clone()))]));

        // No matching spec: the fallback key still yields a parseable object.
        let bare = to_chat_messages_with_profile(&[], &freeform_history, &qwen, &[], None);
        let parsed_bare = runtime_json::parse(&arguments_of(&bare)).expect("arguments must parse");
        assert_eq!(parsed_bare, vobj([("value", vs(patch.clone()))]));

        // Object inputs (function tools) serialize exactly as before.
        let function_history = vec![Message::Assistant(AssistantMessage {
            uuid: "a-fn".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "call_read".into(),
                name: "read".into(),
                input: json!({ "path": "/x" }),
                tool_kind: None,
                thought_signature: None,
            })],
            model: "gpt-5".into(),
            usage: None,
        })];
        let function_wire =
            to_chat_messages_with_profile(&[], &function_history, &qwen, &tools, None);
        assert_eq!(
            arguments_of(&function_wire),
            RuntimeString::from("{\"path\":\"/x\"}")
        );

        // The Responses wire keeps the raw text verbatim on custom calls.
        let responses = to_responses_input_with_profile(&freeform_history, &tools, &qwen);
        assert_eq!(
            responses.as_array().unwrap()[0]
                .get("input")
                .unwrap()
                .as_string()
                .unwrap(),
            &patch
        );
    }

    #[tokio::test]
    async fn image_detail_reaches_the_actual_chat_and_responses_request() {
        use crate::schemas::content::ImageDetail;
        use base64::Engine as _;
        for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (_, body) = capture_http_request(&mut socket).await;
                let response_body = match api {
                    OpenAiApi::Chat => "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                    OpenAiApi::Responses => "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
                };
                socket
                    .write_all(
                        http_response("200 OK", "text/event-stream", response_body).as_bytes(),
                    )
                    .await
                    .unwrap();
                runtime_json::parse(&RuntimeString::from(body)).unwrap()
            });
            let provider = OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture-token".into()),
                base_url: Some(format!("http://{address}/v1")),
                api: Some(api),
                capabilities: Some(ModelCapabilities {
                    vision: true,
                    supports_image_detail_original: true,
                    ..Default::default()
                }),
                ..Default::default()
            });
            let mut encoded = Vec::new();
            image::RgbImage::from_pixel(2600, 16, image::Rgb([17, 29, 43]))
                .write_to(
                    &mut std::io::Cursor::new(&mut encoded),
                    image::ImageFormat::Png,
                )
                .unwrap();
            let mut request = base_req();
            request.messages = vec![Message::User(UserMessage {
                question_answer: None,
                uuid: "native-image-input".into(),
                is_meta: None,
                content: vec![ContentBlock::Image(ImageBlock {
                    mime_type: "image/png".into(),
                    base64: base64::engine::general_purpose::STANDARD
                        .encode(encoded)
                        .into(),
                    source_path: None,
                    detail: Some(ImageDetail::Original),
                })],
            })];
            request.bind_media_capabilities(&provider.capabilities(&request.model));
            crate::engine::query::gate_request_images(&mut request);
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                provider
                    .stream(request, AbortSignal::new())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            let body = tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            let part = match api {
                OpenAiApi::Chat => &body["messages"][0]["content"][0]["image_url"],
                OpenAiApi::Responses => &body["input"][0]["content"][0],
            };
            assert_eq!(part["detail"].as_str().unwrap(), "original");
            let url = match api {
                OpenAiApi::Chat => part["url"].as_str().unwrap(),
                OpenAiApi::Responses => part["image_url"].as_str().unwrap(),
            };
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(
                    url.to_node_utf8()
                        .split(|byte| *byte == b',')
                        .nth(1)
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(image::load_from_memory(&bytes).unwrap().width(), 2600);
        }
    }

    #[test]
    fn direct_user_pdf_maps_to_native_chat_and_responses_file_inputs() {
        let messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "u-pdf".into(),
            content: vec![
                text_block("before"),
                ContentBlock::Image(ImageBlock {
                    mime_type: "image/png".into(),
                    base64: "AAAA".into(),
                    detail: None,
                    source_path: None,
                }),
                text_block("between"),
                ContentBlock::Document(DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "JVBERi0xLjcK".into(),
                    filename: Some("report.pdf".into()),
                }),
                text_block("after"),
            ],
            is_meta: None,
        })];

        assert_eq!(
            to_chat_messages(&[], &messages),
            json!([{
                "role": "user",
                "content": [
                    { "type": "text", "text": "before" },
                    {
                        "type": "image_url",
                        "image_url": { "url": "data:image/png;base64,AAAA" }
                    },
                    { "type": "text", "text": "between" },
                    {
                        "type": "file",
                        "file": {
                            "file_data": "data:application/pdf;base64,JVBERi0xLjcK",
                            "filename": "report.pdf"
                        }
                    },
                    { "type": "text", "text": "after" }
                ]
            }])
        );
        assert_eq!(
            to_responses_input(&messages, &[]),
            json!([{
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "before" },
                    {
                        "type": "input_image",
                        "image_url": "data:image/png;base64,AAAA",
                        "detail": "auto"
                    },
                    { "type": "input_text", "text": "between" },
                    {
                        "type": "input_file",
                        "file_data": "data:application/pdf;base64,JVBERi0xLjcK",
                        "filename": "report.pdf"
                    },
                    { "type": "input_text", "text": "after" }
                ]
            }])
        );
        assert_eq!(
            to_chat_messages(&[], &[user_msg("plain text")]),
            json!([{ "role": "user", "content": "plain text" }])
        );
        assert_eq!(
            to_responses_input(&[user_msg("plain text")], &[]),
            json!([{ "role": "user", "content": "plain text" }])
        );
    }

    #[test]
    fn chat_assistant_omits_empty_content() {
        let messages = vec![Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "c".into(),
                name: "n".into(),
                input: json!({}),
                tool_kind: None,
                thought_signature: None,
            })],
            model: "m".into(),
            usage: None,
        })];
        let v = to_chat_messages(&[], &messages);
        let arr = v.as_array().unwrap();
        // The trailing open batch is settled with a synthetic result: a strict
        // Chat Completions validator also rejects a tool_calls message whose
        // calls never get a tool message, trailing or not.
        assert_eq!(arr.len(), 2);
        assert!(arr[0].get("content").is_none());
        assert!(arr[0].get("tool_calls").is_some());
        assert_eq!(arr[1]["role"], json!("tool"));
        assert_eq!(arr[1]["tool_call_id"], json!("c"));
    }

    // ---- responses request building ----

    #[test]
    fn responses_keeps_function_custom_and_native_search_shapes() {
        let path_schema = required_string_schema("path");
        let patch_schema = required_string_schema("patch");
        let query_schema = required_string_schema("query");
        let lark_definition = include_str!("apply_patch.lark")
            .strip_suffix('\n')
            .expect("grammar file has one repository newline");
        let request = request_with_tools(vec![
            function_spec("read", "Read a file.", path_schema.clone()),
            lark_spec(
                "apply_patch",
                "Apply a patch.",
                patch_schema,
                lark_definition,
            ),
            tool_search_spec(
                "tool_search",
                "Search deferred tools.",
                query_schema.clone(),
            ),
        ]);

        let body = responses_params_default(&request).unwrap();

        assert_eq!(
            body["tools"],
            Value::Array(vec![
                vobj([
                    ("type", vs("function")),
                    ("name", vs("read")),
                    ("description", vs("Read a file.")),
                    ("parameters", path_schema),
                    ("strict", Value::Bool(false)),
                ]),
                vobj([
                    ("type", vs("custom")),
                    ("name", vs("apply_patch")),
                    ("description", vs("Apply a patch.")),
                    (
                        "format",
                        vobj([
                            ("type", vs("grammar")),
                            ("syntax", vs("lark")),
                            ("definition", vs(lark_definition)),
                        ]),
                    ),
                ]),
                vobj([
                    ("type", vs("tool_search")),
                    ("execution", vs("client")),
                    ("description", vs("Search deferred tools.")),
                    ("parameters", query_schema),
                ]),
            ])
        );
    }

    #[test]
    fn responses_custom_formats_preserve_exact_text_grammar_and_absence() {
        let lark_definition = "  start: \"x\"\r\n       | \"y\"  \n";
        let regex_definition = "  ^(?P<word>foo\\s+bar)$  \r\n# preserve trailing space \n";
        let request = request_with_tools(vec![
            freeform_spec(
                "text_tool",
                "Text input.",
                required_string_schema("source"),
                Some(ToolSpecFormat::Text),
            ),
            lark_spec(
                "lark_tool",
                "Lark input.",
                required_string_schema("source"),
                lark_definition,
            ),
            regex_spec(
                "regex_tool",
                "Regex input.",
                required_string_schema("source"),
                regex_definition,
            ),
            freeform_spec(
                "unformatted_tool",
                "Unformatted input.",
                required_string_schema("source"),
                None,
            ),
        ]);

        let body = responses_params_default(&request).unwrap();

        assert_eq!(
            body["tools"],
            Value::Array(vec![
                vobj([
                    ("type", vs("custom")),
                    ("name", vs("text_tool")),
                    ("description", vs("Text input.")),
                    ("format", vobj([("type", vs("text"))])),
                ]),
                vobj([
                    ("type", vs("custom")),
                    ("name", vs("lark_tool")),
                    ("description", vs("Lark input.")),
                    (
                        "format",
                        vobj([
                            ("type", vs("grammar")),
                            ("syntax", vs("lark")),
                            ("definition", vs(lark_definition)),
                        ]),
                    ),
                ]),
                vobj([
                    ("type", vs("custom")),
                    ("name", vs("regex_tool")),
                    ("description", vs("Regex input.")),
                    (
                        "format",
                        vobj([
                            ("type", vs("grammar")),
                            ("syntax", vs("regex")),
                            ("definition", vs(regex_definition)),
                        ]),
                    ),
                ]),
                vobj([
                    ("type", vs("custom")),
                    ("name", vs("unformatted_tool")),
                    ("description", vs("Unformatted input.")),
                ]),
            ])
        );
        assert!(body["tools"][1]["format"]["definition"]
            .as_str()
            .is_some_and(|definition| definition == lark_definition));
        assert!(body["tools"][2]["format"]["definition"]
            .as_str()
            .is_some_and(|definition| definition == regex_definition));
    }

    #[test]
    fn api_specific_encoding_ignores_irrelevant_formats_and_appends_native_search() {
        let mut function = function_spec("read", "Read a file.", required_string_schema("path"));
        function.format = Some(ToolSpecFormat::Text);
        let mut native_search = tool_search_spec(
            "tool_search",
            "Search deferred tools.",
            required_string_schema("query"),
        );
        native_search.format = Some(ToolSpecFormat::Grammar {
            syntax: ToolSpecGrammarSyntax::Regex,
            definition: "ignored".into(),
        });
        let mut request = request_with_tools(vec![function, native_search]);
        request.web_search = Some(Default::default());

        let chat = build_chat_params(&request);
        assert_eq!(chat["tools"].as_array().unwrap().len(), 2);
        assert_eq!(chat["tools"][0]["type"], json!("function"));
        assert_eq!(chat["tools"][1]["type"], json!("function"));
        assert!(chat["tools"][0].get("format").is_none());
        assert!(chat["tools"][1].get("format").is_none());

        let responses = responses_params_default(&request).unwrap();
        assert_eq!(responses["tools"].as_array().unwrap().len(), 3);
        assert_eq!(responses["tools"][0]["type"], json!("function"));
        assert_eq!(responses["tools"][1]["type"], json!("tool_search"));
        assert_eq!(
            responses["tools"][2],
            json!({ "type": "web_search", "external_web_access": true })
        );
        assert!(responses["tools"][0].get("format").is_none());
        assert!(responses["tools"][1].get("format").is_none());
        assert!(responses["tools"][1].get("name").is_none());
    }

    #[test]
    fn responses_params_reasoning_and_search() {
        let mut r = base_req();
        r.model = "o3".into();
        r.effort = Some(EffortValue::Level(EffortLevel::Medium));
        r.system = vec![SystemPromptPart {
            name: "s".into(),
            text: "instr".into(),
            cacheable: None,
            cache_scope: None,
        }];
        r.web_search = Some(crate::types::provider::WebSearchRequest {
            access: Default::default(),
            max_uses: None,
            allowed_domains: Some(vec!["example.com".into()]),
            blocked_domains: None,
            user_location: Some(crate::types::provider::UserLocation {
                city: Some("SF".into()),
                region: None,
                country: Some("US".into()),
                timezone: None,
            }),
            context_size: Some(crate::types::provider::WebSearchContextSize::High),
        });
        let v = build_responses_params(
            &r,
            &caps_with_levels(&[
                EffortLevel::Low,
                EffortLevel::Medium,
                EffortLevel::High,
                EffortLevel::Xhigh,
                EffortLevel::Max,
            ]),
            OpenAiReasoningDialect::OpenAi,
        )
        .unwrap();
        assert_eq!(v["model"], json!("o3"));
        assert_eq!(v["stream"], json!(true));
        assert_eq!(v["store"], json!(false));
        assert_eq!(v["max_output_tokens"], json!(1000));
        assert_eq!(
            v["reasoning"],
            json!({ "effort": "medium", "summary": "auto" })
        );
        assert_eq!(v["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(v["instructions"], json!("instr"));
        assert_eq!(
            v["tools"],
            json!([{ "type": "web_search", "external_web_access": true, "filters": { "allowed_domains": ["example.com"] }, "user_location": { "type": "approximate", "city": "SF", "country": "US" }, "search_context_size": "high" }])
        );
    }

    #[test]
    fn responses_params_non_reasoning_omits_reasoning() {
        let v = responses_params_default(&base_req()).unwrap();
        assert!(v.get("reasoning").is_none());
        assert!(v.get("include").is_none());
        assert!(v.get("instructions").is_none());
        assert!(v.get("tools").is_none());
    }

    /// A transport failure's own Display is often a useless wrapper ("error
    /// sending request for url (...)"), and the root cause that tells you WHY —
    /// the TLS or connect error — hangs off `source()`. The whole chain has to
    /// reach the message, or a network diagnosis stalls on the wrapper.
    #[test]
    fn error_chain_rendering_reaches_the_root_cause() {
        #[derive(Debug)]
        struct Layer {
            text: &'static str,
            cause: Option<Box<Layer>>,
        }
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.text)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.cause
                    .as_ref()
                    .map(|cause| cause.as_ref() as &(dyn std::error::Error + 'static))
            }
        }

        // Three layers deep: every link is joined, deepest cause included.
        let nested = Layer {
            text: "error sending request for url",
            cause: Some(Box::new(Layer {
                text: "tls handshake eof",
                cause: Some(Box::new(Layer {
                    text: "unexpected eof",
                    cause: None,
                })),
            })),
        };
        assert_eq!(
            error_with_source_chain(&nested),
            "error sending request for url: tls handshake eof: unexpected eof"
        );

        // A cause-less error renders exactly its own Display, with no trailing
        // separator — the message must not grow a dangling ": ".
        let bare = Layer {
            text: "aborted",
            cause: None,
        };
        assert_eq!(error_with_source_chain(&bare), "aborted");
    }

    #[test]
    fn codex_adapter_strips_max_output_tokens_and_leaves_tools_untouched() {
        let mut body = vobj([
            ("model", vs("gpt-5")),
            ("max_output_tokens", Value::from(1000u64)),
            (
                "tools",
                Value::Array(vec![
                    vobj([("type", vs("tool_search")), ("execution", vs("client"))]),
                    vobj([("type", vs("function")), ("name", vs("f"))]),
                ]),
            ),
        ]);
        adapt_codex_responses_params(&mut body);
        assert!(body.get("max_output_tokens").is_none());
        assert_eq!(body["tools"][0]["execution"], json!("client"));
        assert_eq!(body["tools"][1], json!({ "type": "function", "name": "f" }));
        // Bodies without tools only lose max_output_tokens.
        let mut plain = vobj([
            ("model", vs("gpt-5")),
            ("max_output_tokens", Value::from(5u64)),
        ]);
        adapt_codex_responses_params(&mut plain);
        assert_eq!(plain, json!({ "model": "gpt-5" }));
    }

    #[test]
    fn subscription_adapter_preserves_the_capability_clamped_effort() {
        let mut request = base_req();
        request.effort = Some(EffortValue::Level(EffortLevel::Max));
        let caps = caps_with_levels(&[EffortLevel::Low, EffortLevel::High]);
        let mut body = build_responses_params_with_tool_mode(
            &request,
            &caps,
            OpenAiReasoningDialect::OpenAi,
            false,
        )
        .unwrap();
        assert_eq!(body["reasoning"]["effort"], json!("high"));
        adapt_codex_responses_params(&mut body);
        assert_eq!(body["reasoning"]["effort"], json!("high"));
    }

    #[test]
    fn responses_params_freeform_exec_renders_custom_and_preserves_wait_name() {
        let mut r = base_req();
        r.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec("wait", "wait for exec completion", schema_obj()),
        ];

        let v = responses_params_default(&r).unwrap();
        assert_eq!(v["tools"][0]["name"], json!("exec"));
        assert_eq!(v["tools"][1]["name"], json!("wait"));
        assert!(v["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .all(|tool| tool["name"] != json!("code_exec")));
        assert_eq!(
            v["tools"][0],
            json!({
                "type": "custom",
                "name": "exec",
                "description": "execute javascript source",
                "format": { "type": "text" }
            })
        );
        assert_eq!(v["tools"][1]["type"], json!("function"));
        assert_eq!(v["tools"][1]["name"], json!("wait"));
    }

    #[test]
    fn responses_tool_choice_uses_custom_discriminator_for_freeform_tools() {
        let mut r = base_req();
        r.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec("wait", "wait for exec completion", schema_obj()),
        ];

        r.tool_choice = Some(ToolChoice::Tool {
            name: "exec".into(),
        });
        let v = responses_params_default(&r).unwrap();
        assert_eq!(
            v["tool_choice"],
            json!({ "type": "custom", "name": "exec" })
        );

        r.tool_choice = Some(ToolChoice::Tool {
            name: "wait".into(),
        });
        let v = responses_params_default(&r).unwrap();
        assert_eq!(
            v["tool_choice"],
            json!({ "type": "function", "name": "wait" })
        );
    }

    #[test]
    fn responses_input_reasoning_replay_and_orphan() {
        // A thinking block FOLLOWED by a tool_use replays as a reasoning item.
        let followed = Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![
                ContentBlock::Thinking(ThinkingBlock {
                    message_id: None,
                    text: "reasoned".into(),
                    signature: None,
                    encrypted_content: Some("enc123".into()),
                }),
                ContentBlock::ToolUse(ToolUseBlock {
                    id: "call_9".into(),
                    name: "grep".into(),
                    input: json!({ "q": "x" }),
                    tool_kind: None,
                    thought_signature: None,
                }),
            ],
            model: "o3".into(),
            usage: None,
        });
        let v = to_responses_input(&[followed], &[]);
        assert_eq!(
            v,
            json!([
                { "type": "reasoning", "summary": [ { "type": "summary_text", "text": "reasoned" } ], "encrypted_content": "enc123" },
                { "type": "function_call", "call_id": "call_9", "name": "grep", "arguments": "{\"q\":\"x\"}" }
            ])
        );

        // A TRAILING thinking orphan (nothing after it) is dropped.
        let orphan = Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::Thinking(ThinkingBlock {
                message_id: None,
                text: "dangling".into(),
                signature: None,
                encrypted_content: Some("enc".into()),
            })],
            model: "o3".into(),
            usage: None,
        });
        assert_eq!(to_responses_input(&[orphan], &[]), json!([]));
    }

    #[test]
    fn responses_input_text_and_tool_result_media() {
        let messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u".into(),
                content: vec![text_block("hi")],
                is_meta: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "t".into(),
                tool_use_id: "call_1".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "out".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: Some(vec![ContentBlock::Image(ImageBlock {
                    mime_type: "image/png".into(),
                    base64: "QQ==".into(),
                    detail: None,
                    source_path: None,
                })]),
            }),
        ];
        let v = to_responses_input(&messages, &[]);
        assert_eq!(
            v,
            json!([
                { "role": "user", "content": "hi" },
                { "type": "function_call_output", "call_id": "call_1", "output": "out" },
                { "role": "user", "content": [ { "type": "input_image", "image_url": "data:image/png;base64,QQ==", "detail": "auto" } ] }
            ])
        );
    }

    #[test]
    fn responses_input_legacy_kind_inference_stringifies_whole_custom_value() {
        let mut r = base_req();
        r.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec("wait", "wait for exec completion", schema_obj()),
        ];
        r.messages = vec![
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: "call_exec".into(),
                    name: "exec".into(),
                    input: json!({ "source": "console.log(1)" }),
                    tool_kind: None,
                    thought_signature: None,
                })],
                model: "gpt-4o".into(),
                usage: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "t1".into(),
                tool_use_id: "call_exec".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "done".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: Some(vec![ContentBlock::Image(ImageBlock {
                    mime_type: "image/png".into(),
                    base64: "QQ==".into(),
                    detail: None,
                    source_path: None,
                })]),
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a2".into(),
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: "call_wait".into(),
                    name: "wait".into(),
                    input: json!({ "seconds": 1 }),
                    tool_kind: None,
                    thought_signature: None,
                })],
                model: "gpt-4o".into(),
                usage: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "t2".into(),
                tool_use_id: "call_wait".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "waited".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: None,
            }),
        ];

        let v = responses_params_default(&r).unwrap()["input"].clone();
        assert_eq!(
            v,
            json!([
                { "type": "custom_tool_call", "call_id": "call_exec", "name": "exec", "input": "{\"source\":\"console.log(1)\"}" },
                { "type": "custom_tool_call_output", "call_id": "call_exec", "output": "done" },
                { "role": "user", "content": [ { "type": "input_image", "image_url": "data:image/png;base64,QQ==", "detail": "auto" } ] },
                { "type": "function_call", "call_id": "call_wait", "name": "wait", "arguments": "{\"seconds\":1}" },
                { "type": "function_call_output", "call_id": "call_wait", "output": "waited" }
            ])
        );
    }

    fn assistant_provider_call(
        id: &str,
        name: &str,
        input: Value,
        tool_kind: ProviderToolCallKind,
    ) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: format!("assistant-{id}").into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: name.into(),
                input,
                thought_signature: None,
                tool_kind: Some(tool_kind),
            })],
            model: "gpt-5".into(),
            usage: None,
        })
    }

    fn provider_result(
        id: &str,
        text: &str,
        tool_kind: ProviderToolCallKind,
        native_tool_output: Option<Value>,
    ) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("result-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            output: None,
            extra_user_blocks: None,
            tool_kind: Some(tool_kind),
            native_tool_output,
        })
    }

    fn assistant_custom(id: &str, name: &str, input: &str) -> Message {
        assistant_provider_call(
            id,
            name,
            Value::String(input.into()),
            ProviderToolCallKind::Custom,
        )
    }

    fn custom_result(id: &str, text: &str) -> Message {
        provider_result(id, text, ProviderToolCallKind::Custom, None)
    }

    fn assistant_function(id: &str, name: &str, input: Value) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: format!("assistant-{id}").into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: name.into(),
                input,
                thought_signature: None,
                tool_kind: None,
            })],
            model: "gpt-5".into(),
            usage: None,
        })
    }

    fn function_result(id: &str, text: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("result-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            output: None,
            extra_user_blocks: None,
            tool_kind: None,
            native_tool_output: None,
        })
    }

    fn assistant_tool_search(id: &str, input: Value) -> Message {
        assistant_provider_call(id, "tool_search", input, ProviderToolCallKind::ToolSearch)
    }

    fn tool_search_result(id: &str, output: Value) -> Message {
        provider_result(
            id,
            "search complete",
            ProviderToolCallKind::ToolSearch,
            Some(output),
        )
    }

    fn native_search_output_fixture() -> Value {
        json!({"tools":[
          {"type":"function","name":"read","description":"Read","inputSchema":{"type":"object"}},
          {"type":"namespace","name":"calendar","tools":[
            {"type":"function","name":"events","description":"Events","parameters":{"type":"object"}}
          ]},
          {"type":"custom","kind":"custom","name":"ignored","description":"Ignored","inputSchema":{"type":"object"}},
          {"type":"namespace","name":"empty","tools":[{"type":"custom","name":"ignored"}]}
        ]})
    }

    #[test]
    fn non_default_history_metadata_survives_jsonl_round_trip_and_replay() {
        let original = vec![
            assistant_custom("c1", "apply_patch", "*** Begin Patch"),
            custom_result("c1", "done"),
            assistant_tool_search("s1", json!({"query":"calendar"})),
            tool_search_result("s1", native_search_output_fixture()),
        ];
        let encoded = original
            .iter()
            .map(|message| {
                runtime_codec::encode_value(message)
                    .map(|value| runtime_json::stringify(&value))
                    .expect("message encodes to exact runtime JSON")
            })
            .collect::<Vec<_>>();
        let decoded = encoded
            .iter()
            .map(|line| {
                let value = runtime_json::parse(line).expect("exact runtime JSON line parses");
                runtime_codec::decode_value::<Message>(&value)
                    .expect("exact runtime JSON line decodes")
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, original);

        let replay = to_responses_input(&decoded, &[]);
        assert_eq!(
            replay
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["type"].as_str().unwrap().clone())
                .collect::<Vec<_>>(),
            [
                RuntimeString::from("custom_tool_call"),
                RuntimeString::from("custom_tool_call_output"),
                RuntimeString::from("tool_search_call"),
                RuntimeString::from("tool_search_output"),
            ]
        );
        assert_eq!(replay[0]["input"], json!("*** Begin Patch"));
        assert_eq!(replay[3]["status"], json!("completed"));
        assert_eq!(replay[3]["execution"], json!("client"));
        assert_eq!(
            replay[3]["tools"],
            json!([
              {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
              {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
              ]}
            ])
        );
    }

    #[test]
    fn default_function_history_keeps_the_legacy_wire_shape() {
        let value = runtime_codec::encode_value(&assistant_function("f1", "read", json!({})))
            .expect("message encodes");
        assert!(value.pointer("/content/0/toolKind").is_none());
        assert!(value.pointer("/content/0/inputFormat").is_none());

        let result =
            runtime_codec::encode_value(&function_result("f1", "ok")).expect("message encodes");
        assert!(result.get("toolKind").is_none());
        assert!(result.get("nativeToolOutput").is_none());
    }

    #[test]
    fn persisted_call_kind_uses_explicit_metadata_then_only_legacy_specs() {
        let explicit_custom = ToolUseBlock {
            id: "c1".into(),
            name: "apply_patch".into(),
            input: json!("raw"),
            tool_kind: Some(ProviderToolCallKind::Custom),
            thought_signature: None,
        };
        let explicit_search = ToolUseBlock {
            id: "s1".into(),
            name: "tool_search".into(),
            input: json!({"query":"calendar"}),
            tool_kind: Some(ProviderToolCallKind::ToolSearch),
            thought_signature: None,
        };
        let conflicting_functions = vec![
            function_spec("apply_patch", "current function", schema_obj()),
            function_spec("tool_search", "current function", schema_obj()),
        ];
        assert_eq!(
            persisted_call_kind(&explicit_custom, &conflicting_functions),
            ProviderToolCallKind::Custom
        );
        assert_eq!(
            persisted_call_kind(&explicit_search, &conflicting_functions),
            ProviderToolCallKind::ToolSearch
        );

        let legacy_custom = ToolUseBlock {
            tool_kind: None,
            ..explicit_custom.clone()
        };
        let legacy_search = ToolUseBlock {
            tool_kind: None,
            ..explicit_search.clone()
        };
        assert_eq!(
            persisted_call_kind(
                &legacy_custom,
                &[text_spec("apply_patch", "legacy freeform")]
            ),
            ProviderToolCallKind::Custom
        );
        assert_eq!(
            persisted_call_kind(
                &legacy_search,
                &[tool_search_spec(
                    "tool_search",
                    "legacy search",
                    schema_obj()
                )]
            ),
            ProviderToolCallKind::ToolSearch
        );
        assert_eq!(
            persisted_call_kind(&legacy_custom, &[]),
            ProviderToolCallKind::Function
        );
    }

    #[test]
    fn responses_history_replays_self_describing_call_kinds_after_tool_removal() {
        let value = to_responses_input(
            &[
                assistant_custom("c1", "apply_patch", "*** Begin Patch"),
                custom_result("c1", "done"),
                assistant_tool_search("s1", json!({"query":"calendar"})),
                tool_search_result("s1", native_search_output_fixture()),
            ],
            &[],
        );
        let items = value.as_array().unwrap();
        assert_eq!(
            items,
            &[
                json!({"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"}),
                json!({"type":"custom_tool_call_output","call_id":"c1","output":"done"}),
                json!({"type":"tool_search_call","call_id":"s1","execution":"client","arguments":{"query":"calendar"}}),
                json!({
                    "type":"tool_search_output","call_id":"s1","status":"completed","execution":"client",
                    "tools":[
                        {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
                        {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                            {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
                        ]}
                    ]
                }),
            ]
        );
    }

    #[test]
    fn responses_result_only_explicit_kind_survives_missing_call_history() {
        let value = to_responses_input(
            &[
                custom_result("c1", "done"),
                tool_search_result("s1", native_search_output_fixture()),
            ],
            &[],
        );
        assert_eq!(
            value,
            json!([
                {"type":"custom_tool_call_output","call_id":"c1","output":"done"},
                {
                    "type":"tool_search_output",
                    "call_id":"s1",
                    "status":"completed",
                    "execution":"client",
                    "tools":[
                        {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
                        {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                            {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
                        ]}
                    ]
                }
            ])
        );
    }

    #[test]
    fn responses_persisted_kinds_precede_legacy_specs_and_call_map() {
        let messages = vec![
            assistant_provider_call(
                "c1",
                "apply_patch",
                json!("raw"),
                ProviderToolCallKind::Custom,
            ),
            provider_result(
                "c1",
                "explicit function result",
                ProviderToolCallKind::Function,
                None,
            ),
        ];
        let tools = vec![function_spec(
            "apply_patch",
            "conflicting current function spec",
            schema_obj(),
        )];
        assert_eq!(
            to_responses_input(&messages, &tools),
            json!([
                {"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"raw"},
                {"type":"function_call_output","call_id":"c1","output":"explicit function result"}
            ])
        );

        let legacy_messages = vec![
            Message::Assistant(AssistantMessage {
                uuid: "legacy-assistant".into(),
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: "s1".into(),
                    name: "legacy_search".into(),
                    input: json!({"query":"calendar"}),
                    thought_signature: None,
                    tool_kind: None,
                })],
                model: "gpt-5".into(),
                usage: None,
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "legacy-result".into(),
                tool_use_id: "s1".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "search complete".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_error: false,
                output: None,
                extra_user_blocks: None,
                tool_kind: None,
                native_tool_output: Some(native_search_output_fixture()),
            }),
        ];
        let legacy_replay = to_responses_input(
            &legacy_messages,
            &[tool_search_spec(
                "legacy_search",
                "legacy visible native search",
                schema_obj(),
            )],
        );
        assert_eq!(legacy_replay[0]["type"], json!("tool_search_call"));
        assert_eq!(legacy_replay[1]["type"], json!("tool_search_output"));
    }

    #[test]
    fn tool_search_output_rebuilds_only_loadable_function_tools() {
        let output = responses_tool_search_output(native_search_output_fixture());
        assert_eq!(
            output["tools"],
            json!([
              {"type":"function","name":"read","description":"Read","strict":false,"defer_loading":true,"parameters":{"type":"object"}},
              {"type":"namespace","name":"calendar","description":"Tools in the calendar namespace.","tools":[
                {"type":"function","name":"events","description":"Events","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
              ]}
            ])
        );
    }

    #[test]
    fn tool_search_output_uses_nullish_schema_and_validates_namespace_children_by_fields() {
        let output = responses_tool_search_output(json!({"tools":[
            {"kind":"custom","name":"drop-top-level","description":"Drop","inputSchema":{"type":"object"}},
            {"name":"malformed","description":"Malformed","inputSchema":"bad","parameters":{"type":"object"}},
            {"name":"fallback","description":"Fallback","inputSchema":null,"parameters":{"type":"object","properties":{"q":{"type":"string"}}}},
            {"type":"namespace","name":"mixed","description":42,"tools":[
                {"kind":"custom","name":"accepted_child","description":"Accepted child","inputSchema":{"type":"object"}},
                {"kind":"function","name":"missing_schema","description":"Missing schema"}
            ]}
        ]}));
        assert_eq!(
            output["tools"],
            json!([
                {"type":"function","name":"fallback","description":"Fallback","strict":false,"defer_loading":true,"parameters":{"type":"object","properties":{"q":{"type":"string"}}}},
                {"type":"namespace","name":"mixed","description":"Tools in the mixed namespace.","tools":[
                    {"type":"function","name":"accepted_child","description":"Accepted child","strict":false,"defer_loading":true,"parameters":{"type":"object"}}
                ]}
            ])
        );
        assert_eq!(
            responses_tool_search_output(json!("invalid"))["tools"],
            json!([])
        );
    }

    #[test]
    fn responses_stop_reason_mapping() {
        // Incomplete outcomes are rejected before this successful-terminal map.
        assert_eq!(
            map_responses_stop_reason(
                &json!({ "incomplete_details": { "reason": "max_output_tokens" } })
            ),
            ModelStopReason::EndTurn
        );
        assert_eq!(
            map_responses_stop_reason(
                &json!({ "incomplete_details": { "reason": "content_filter" } })
            ),
            ModelStopReason::EndTurn
        );
        assert_eq!(
            map_responses_stop_reason(&json!({ "output": [ { "type": "function_call" } ] })),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_responses_stop_reason(&json!({ "output": [ { "type": "custom_tool_call" } ] })),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_responses_stop_reason(&json!({ "output": [ { "type": "tool_search_call" } ] })),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_responses_stop_reason(&json!({ "output": [] })),
            ModelStopReason::EndTurn
        );
        // A string literal "null" reason (DashScope-style sentinel) must not match
        // any early-stop branch: exact matching falls through to the output scan.
        assert_eq!(
            map_responses_stop_reason(&json!({
                "incomplete_details": { "reason": "null" },
                "output": [ { "type": "function_call" } ]
            })),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_responses_stop_reason(
                &json!({ "incomplete_details": { "reason": "null" }, "output": [] })
            ),
            ModelStopReason::EndTurn
        );
    }

    // ---- SSE framing ----

    #[test]
    fn sse_boundary_and_block_parsing() {
        // LF LF
        assert_eq!(find_event_boundary(b"data: x\n\n"), Some((7, 9)));
        // CRLF CRLF
        assert_eq!(find_event_boundary(b"data: x\r\n\r\n"), Some((7, 11)));
        // incomplete
        assert_eq!(find_event_boundary(b"data: x\n"), None);
        let evt = parse_sse_block(&RuntimeString::from(
            "event: response.output_text.delta\ndata: {\"a\":1}",
        ));
        assert!(evt
            .event
            .as_ref()
            .is_some_and(|event| event == "response.output_text.delta"));
        assert_eq!(evt.data, "{\"a\":1}");
        // comment line is ignored
        let c = parse_sse_block(&RuntimeString::from(": keep-alive"));
        assert!(c.data.is_empty());
    }

    /// Drive a whole canned SSE byte buffer through the framer, mirroring the
    /// reader loop, for end-to-end mapping tests.
    fn parse_all_events(buf: &[u8]) -> Vec<SseEvent> {
        let mut decoder = SseDecoder::new();
        decoder.push(buf);
        let mut out = Vec::new();
        while let Some(evt) = decoder.next_event() {
            if !evt.data.is_empty() {
                out.push(evt);
            }
        }
        out
    }

    // ---- chat chunk mapping ----

    #[test]
    fn chat_usage_reads_flat_prompt_cache_hit_tokens() {
        let mut state = ChatState::default();
        let reported = update_chat_usage(
            &json!({ "usage": { "prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110, "prompt_cache_hit_tokens": 80, "prompt_cache_miss_tokens": 20 } }),
            &mut state,
        );
        assert!(reported);
        assert_eq!(state.cache_read, 80);
        assert_eq!(state.input_tokens, 100);
        assert_eq!(state.output_tokens, 10);
        assert_eq!(state.total_tokens, 110);
    }

    #[test]
    fn nested_cached_tokens_take_precedence_over_flat_hit_tokens() {
        let mut state = ChatState::default();
        update_chat_usage(
            &json!({ "usage": { "prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110, "prompt_tokens_details": { "cached_tokens": 30 }, "prompt_cache_hit_tokens": 80 } }),
            &mut state,
        );
        assert_eq!(state.cache_read, 30);
    }

    #[test]
    fn cumulative_chat_message_emits_only_new_text_and_ignores_string_null_finish() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "你" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "你好" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "你好" }, "finish_reason": null }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "你好" }, "finish_reason": "stop" }] }),
        ] {
            events.extend(
                map_chat_stream_chunk(&chunk, &mut state).expect("valid cumulative snapshot"),
            );
        }

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into(),
                },
                ModelStreamEvent::TextDelta { text: "你".into() },
                ModelStreamEvent::TextDelta { text: "好".into() },
            ]
        );
        assert_eq!(state.stop_reason, ModelStopReason::EndTurn);
    }

    #[test]
    fn cumulative_chat_message_separates_qwen_thinking_from_visible_text() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "", "reasoning_content": "" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "<think>\nThinking", "reasoning_content": "Thinking" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "<think>\nThinking Process", "reasoning_content": " Process" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "<think>\nThinking Process\n</think>\n\n{\"title\":", "reasoning_content": "\n" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "<think>\nThinking Process\n</think>\n\n{\"title\":\"Greeting\"}", "reasoning_content": "" }, "finish_reason": "stop" }] }),
        ] {
            events.extend(
                map_chat_stream_chunk(&chunk, &mut state).expect("valid Qwen cumulative snapshot"),
            );
        }

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: "Thinking".into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: " Process".into(),
                },
                ModelStreamEvent::ThinkingDelta { text: "\n".into() },
                ModelStreamEvent::TextDelta {
                    text: "{\"title\":".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "\"Greeting\"}".into(),
                },
            ]
        );
    }

    #[test]
    fn cumulative_chat_removes_orphan_think_close_after_reasoning_field() {
        let mut state = ChatState::default();
        let mut events = Vec::new();
        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "</", "reasoning_content": "reasoning" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "</think>\n" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "content": "</think>\nAnswer" }, "finish_reason": "stop" }] }),
        ] {
            events.extend(map_chat_stream_chunk(&chunk, &mut state).unwrap());
        }
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into()
                },
                ModelStreamEvent::ThinkingDelta {
                    text: "reasoning".into()
                },
                ModelStreamEvent::TextDelta {
                    text: "Answer".into()
                },
            ]
        );
    }

    #[test]
    fn cumulative_chat_tool_calls_emit_argument_suffixes_and_stop_once() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{\"path\":" } }] }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{\"path\":\"/tmp\"}" } }] }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{\"path\":\"/tmp\"}" } }] }, "finish_reason": "tool_calls" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{\"path\":\"/tmp\"}" } }] }, "finish_reason": "tool_calls" }] }),
        ] {
            events.extend(
                map_chat_stream_chunk(&chunk, &mut state).expect("valid cumulative tool snapshot"),
            );
        }

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "call-1".into(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call-1".into(),
                    partial_json: "{\"path\":".into(),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call-1".into(),
                    partial_json: "\"/tmp\"}".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "call-1".into(),
                },
            ]
        );
        assert_eq!(state.stop_reason, ModelStopReason::ToolUse);
    }

    #[test]
    fn cumulative_chat_rejects_non_prefix_snapshots_without_echoing_content() {
        let mut state = ChatState::default();
        map_chat_stream_chunk(
            &json!({ "choices": [{ "message": { "content": "sensitive-prefix" }, "finish_reason": "null" }] }),
            &mut state,
        )
        .unwrap();

        let error = map_chat_stream_chunk(
            &json!({ "choices": [{ "message": { "content": "diverged-secret" }, "finish_reason": "null" }] }),
            &mut state,
        )
        .expect_err("a cumulative stream cannot retract emitted output");
        let message = String::from_utf16_lossy(error.message.as_utf16());
        assert!(message.contains("non-prefix"));
        assert!(!message.contains("sensitive-prefix"));
        assert!(!message.contains("diverged-secret"));
    }

    #[test]
    fn cumulative_chat_rejects_new_tool_calls_after_terminal_snapshot() {
        let mut state = ChatState::default();
        map_chat_stream_chunk(
            &json!({ "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{}" } }] }, "finish_reason": "tool_calls" }] }),
            &mut state,
        )
        .unwrap();

        let error = map_chat_stream_chunk(
            &json!({ "choices": [{ "message": { "tool_calls": [{ "index": 0, "id": "call-1", "function": { "name": "read", "arguments": "{}" } }, { "index": 1, "id": "call-2", "function": { "name": "write", "arguments": "{}" } }] }, "finish_reason": "tool_calls" }] }),
            &mut state,
        )
        .expect_err("terminal cumulative snapshots are immutable");
        assert!(String::from_utf16_lossy(error.message.as_utf16()).contains("terminal"));
    }

    #[test]
    fn openai_compatibility_rejects_protected_headers_without_echoing_values() {
        let error = OpenAICompatibilityOptions::try_new(
            HashMap::from([("aUtHoRiZaTiOn".to_string(), "secret-token".to_string())]),
            Some(OpenAIRequestIdentity::try_new("session-1").unwrap()),
        )
        .expect_err("Authorization must remain owned by provider auth");

        assert!(error.contains("authorization"));
        assert!(!error.contains("secret-token"));
    }

    #[test]
    fn chat_request_identity_is_opt_in_and_does_not_change_standard_requests() {
        let request = base_req();
        let standard = build_chat_params(&request);
        assert!(standard.get("session_id").is_none());
        assert!(standard.get("request_id").is_none());

        let identity = OpenAIRequestIdentity::try_new("klaus-session-1").unwrap();
        let compatible = build_chat_params_with_identity(
            &request,
            Some(&identity),
            Some("req-1"),
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
        );
        assert_eq!(compatible["session_id"], json!("klaus-session-1"));
        assert_eq!(compatible["request_id"], json!("req-1"));
    }

    #[test]
    fn chat_chunks_full_tool_turn() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        // First chunk: role only + model.
        events.extend(map_chat_chunk(
            &json!({ "model": "gpt-4o", "choices": [ { "delta": { "role": "assistant", "content": "" } } ] }),
            &mut state,
        ));
        // Text delta.
        events.extend(map_chat_chunk(
            &json!({ "model": "gpt-4o", "choices": [ { "delta": { "content": "Hi" } } ] }),
            &mut state,
        ));
        // Tool call start (id + name) then args fragments.
        events.extend(map_chat_chunk(
            &json!({ "choices": [ { "delta": { "tool_calls": [ { "index": 0, "id": "call_1", "function": { "name": "read", "arguments": "" } } ] } } ] }),
            &mut state,
        ));
        events.extend(map_chat_chunk(
            &json!({ "choices": [ { "delta": { "tool_calls": [ { "index": 0, "function": { "arguments": "{\"p\":" } } ] } } ] }),
            &mut state,
        ));
        events.extend(map_chat_chunk(
            &json!({ "choices": [ { "delta": { "tool_calls": [ { "index": 0, "function": { "arguments": "1}" } } ] } } ] }),
            &mut state,
        ));
        // Finish.
        events.extend(map_chat_chunk(
            &json!({ "choices": [ { "delta": {}, "finish_reason": "tool_calls" } ] }),
            &mut state,
        ));
        // Usage-only final chunk.
        events.extend(map_chat_chunk(
            &json!({ "choices": [], "usage": { "prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 150, "prompt_tokens_details": { "cached_tokens": 30 } } }),
            &mut state,
        ));

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "gpt-4o".into()
                },
                ModelStreamEvent::TextDelta { text: "Hi".into() },
                ModelStreamEvent::ToolUseStart {
                    id: "call_1".into(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call_1".into(),
                    partial_json: "{\"p\":".into()
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call_1".into(),
                    partial_json: "1}".into()
                },
                ModelStreamEvent::ToolUseStop {
                    id: "call_1".into()
                },
            ]
        );
        assert_eq!(state.stop_reason, ModelStopReason::ToolUse);
        let usage = make_usage(
            state.input_tokens,
            state.output_tokens,
            state.cache_read,
            state.total_tokens,
        );
        assert_eq!(usage.input_tokens, 70);
        assert_eq!(usage.cache_read_input_tokens, 30);
        assert_eq!(usage.total_tokens, Some(150));
    }

    #[test]
    fn standard_delta_chat_emits_qwen_reasoning_content() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "role": "assistant", "content": "", "reasoning_content": "" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "content": "", "reasoning_content": "Think" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "content": "", "reasoning_content": " carefully" }, "finish_reason": "null" }] }),
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "content": "Answer", "reasoning_content": "" }, "finish_reason": "stop" }] }),
        ] {
            events.extend(
                map_chat_stream_chunk(&chunk, &mut state).expect("valid standard delta chunk"),
            );
        }

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: "Think".into(),
                },
                ModelStreamEvent::ThinkingDelta {
                    text: " carefully".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "Answer".into(),
                },
            ]
        );
        assert_eq!(state.stop_reason, ModelStopReason::EndTurn);
    }

    #[test]
    fn standard_delta_chat_removes_only_the_leading_orphan_think_close() {
        let mut state = ChatState::default();
        let mut events = Vec::new();
        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "reasoning_content": "reasoning" }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "content": "</th" }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "content": "ink>\n" }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "content": "Answer" }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "content": " contains </think> literally" }, "finish_reason": "stop" }] }),
        ] {
            events.extend(map_chat_stream_chunk(&chunk, &mut state).unwrap());
        }
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into()
                },
                ModelStreamEvent::ThinkingDelta {
                    text: "reasoning".into()
                },
                ModelStreamEvent::TextDelta {
                    text: "Answer".into()
                },
                ModelStreamEvent::TextDelta {
                    text: " contains </think> literally".into()
                },
            ]
        );
    }

    #[test]
    fn standard_delta_chat_keeps_literal_think_text_without_a_reasoning_field() {
        let mut state = ChatState::default();
        let events = map_chat_stream_chunk(
            &json!({ "model": "plain", "choices": [{ "delta": { "content": "</think> is a literal example" }, "finish_reason": "stop" }] }),
            &mut state,
        )
        .unwrap();
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "plain".into()
                },
                ModelStreamEvent::TextDelta {
                    text: "</think> is a literal example".into()
                },
            ]
        );
    }

    #[test]
    fn standard_delta_chat_preserves_an_incomplete_delimiter() {
        let mut state = ChatState::default();
        let events = map_chat_stream_chunk(
            &json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "reasoning_content": "reasoning", "content": "</th" }, "finish_reason": "stop" }] }),
            &mut state,
        )
        .unwrap();
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "qwen3.7-max".into()
                },
                ModelStreamEvent::ThinkingDelta {
                    text: "reasoning".into()
                },
                ModelStreamEvent::TextDelta {
                    text: "</th".into()
                },
            ]
        );
    }

    #[test]
    fn standard_delta_chat_ignores_string_null_finish_reason_for_tool_calls() {
        let mut state = ChatState::default();
        let mut events = Vec::new();

        // DashScope/Qwen gateways emit finish_reason as the string literal "null"
        // (not JSON null) on every non-terminal chunk. Tool call arguments arrive
        // fragmented across those chunks; only the last chunk carries the real
        // finish_reason.
        for chunk in [
            json!({ "model": "qwen3.7-max", "choices": [{ "delta": { "role": "assistant", "content": "", "tool_calls": [{ "index": 0, "id": "call_1", "function": { "name": "read", "arguments": "" } }] }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": "{\"path\":" } }] }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": "\"a.txt\"" } }] }, "finish_reason": "null" }] }),
            json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": "}" } }] }, "finish_reason": "tool_calls" }] }),
        ] {
            events.extend(
                map_chat_stream_chunk(&chunk, &mut state).expect("valid standard delta chunk"),
            );
        }

        // All argument fragments must survive until the real finish_reason.
        let joined_args: String = events
            .iter()
            .filter_map(|e| match e {
                ModelStreamEvent::ToolUseInputDelta { partial_json, .. } => {
                    Some(partial_json.try_to_utf8().expect("utf8 fragment"))
                }
                _ => None,
            })
            .collect();
        assert_eq!(joined_args, "{\"path\":\"a.txt\"}");

        // ToolUseStop must fire exactly once, and only after the terminal chunk.
        let stop_positions: Vec<usize> = events
            .iter()
            .enumerate()
            .filter_map(|(i, e)| matches!(e, ModelStreamEvent::ToolUseStop { .. }).then_some(i))
            .collect();
        assert_eq!(stop_positions.len(), 1, "events: {events:?}");
        assert_eq!(stop_positions[0], events.len() - 1, "events: {events:?}");
        assert_eq!(state.stop_reason, ModelStopReason::ToolUse);
    }

    #[test]
    fn chat_message_start_emitted_once() {
        let mut state = ChatState::default();
        let first = map_chat_chunk(
            &json!({ "model": "m", "choices": [ { "delta": { "content": "a" } } ] }),
            &mut state,
        );
        let second = map_chat_chunk(
            &json!({ "model": "m", "choices": [ { "delta": { "content": "b" } } ] }),
            &mut state,
        );
        assert!(matches!(first[0], ModelStreamEvent::MessageStart { .. }));
        assert!(!second
            .iter()
            .any(|e| matches!(e, ModelStreamEvent::MessageStart { .. })));
    }

    // ---- responses event mapping ----

    fn map_responses_events(state: &mut ResponsesState, values: &[Value]) -> Vec<ModelStreamEvent> {
        let mut output = Vec::new();
        for value in values {
            let (events, terminate) = map_responses_event(value, state);
            output.extend(events);
            if terminate {
                break;
            }
        }
        output
    }

    fn map_responses_sequence(values: &[Value]) -> Vec<ModelStreamEvent> {
        let mut state = ResponsesState::default();
        let mut output = Vec::new();
        for value in values {
            let (events, terminate) = map_responses_event(value, &mut state);
            output.extend(events);
            if terminate {
                return output;
            }
        }
        output.extend(finish_responses_state(
            &state,
            &RuntimeString::from("gpt-5"),
        ));
        output
    }

    fn custom_done_event(call_id: Option<&str>) -> Value {
        let mut item = json!({
            "type":"custom_tool_call","id":"item-1",
            "name":"apply_patch","input":"raw"
        });
        if let Some(call_id) = call_id {
            item["call_id"] = vs(call_id);
        }
        vobj([("type", vs("response.output_item.done")), ("item", item)])
    }

    fn tool_search_done_event(call_id: Option<&str>, arguments: Value) -> Value {
        let mut item = vobj([
            ("type", vs("tool_search_call")),
            ("id", vs("item-1")),
            ("execution", vs("client")),
            ("arguments", arguments),
        ]);
        if let Some(call_id) = call_id {
            item["call_id"] = vs(call_id);
        }
        vobj([("type", vs("response.output_item.done")), ("item", item)])
    }

    #[test]
    fn tool_search_arguments_use_javascript_number_ingress_and_wire_semantics() {
        let event = runtime_json::parse(&RuntimeString::from(
            r#"{"type":"response.output_item.done","item":{"type":"tool_search_call","execution":"client","arguments":{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}}}"#,
        ))
        .expect("valid Responses event");
        assert_eq!(
            response_tool_search_input(&event["item"]),
            r#"{"unsafe":9007199254740992,"negativeZero":0,"overflow":null}"#
        );
    }

    #[test]
    fn replayed_function_arguments_use_javascript_json_stringify_number_projection() {
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}"#,
        ))
        .expect("valid runtime tool input");
        let message = assistant_function("numbers", "numbers", input);
        let expected = r#"{"unsafe":9007199254740992,"negativeZero":0,"overflow":null}"#;

        let chat = to_chat_messages(&[], std::slice::from_ref(&message));
        assert_eq!(
            chat[0]["tool_calls"][0]["function"]["arguments"],
            vs(expected)
        );

        let responses = to_responses_input(&[message], &[]);
        assert_eq!(responses[0]["arguments"], vs(expected));
    }

    #[test]
    fn http_request_body_uses_javascript_json_stringify_number_projection() {
        let runtime = runtime_json::parse(&RuntimeString::from(
            r#"{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}"#,
        ))
        .expect("valid runtime request value");

        assert_eq!(
            encode_openai_request_body(&vobj([("input", runtime)])),
            br#"{"input":{"unsafe":9007199254740992,"negativeZero":0,"overflow":null}}"#
        );
    }

    #[test]
    fn custom_done_without_call_id_keeps_the_added_call_identity() {
        let mut state = ResponsesState::default();
        let events = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{
                    "type":"custom_tool_call","id":"item-1","call_id":"c1",
                    "name":"apply_patch","input":""
                }}),
                json!({"type":"response.custom_tool_call_input.delta",
                    "item_id":"item-1","delta":"raw"}),
                custom_done_event(None),
            ],
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "apply_patch".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::Custom),
                    input_format: Some(ToolInputFormat::Raw),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: "raw".into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
        assert!(map_responses_events(
            &mut state,
            &[
                json!({"type":"response.custom_tool_call_input.delta",
                    "item_id":"item-1","delta":"late"}),
                custom_done_event(None),
            ],
        )
        .is_empty());
    }

    #[test]
    fn custom_done_with_changed_call_id_keeps_the_added_call_identity() {
        let mut state = ResponsesState::default();
        let events = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{
                    "type":"custom_tool_call","id":"item-1","call_id":"c1",
                    "name":"apply_patch","input":""
                }}),
                json!({"type":"response.custom_tool_call_input.delta",
                    "item_id":"item-1","delta":"raw"}),
                custom_done_event(Some("c2")),
            ],
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "apply_patch".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::Custom),
                    input_format: Some(ToolInputFormat::Raw),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: "raw".into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
        assert!(map_responses_events(
            &mut state,
            &[
                json!({"type":"response.custom_tool_call_input.delta",
                    "item_id":"item-1","delta":"late"}),
                custom_done_event(Some("c2")),
            ],
        )
        .is_empty());
    }

    #[test]
    fn tool_search_done_without_call_id_keeps_the_added_call_identity() {
        let mut state = ResponsesState::default();
        let events = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{
                    "type":"tool_search_call","id":"item-1","call_id":"c1",
                    "execution":"client","arguments":{"query":"calendar"}
                }}),
                tool_search_done_event(None, json!({"query":"calendar"})),
            ],
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "tool_search".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::ToolSearch),
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: r#"{"query":"calendar"}"#.into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
        assert!(map_responses_events(
            &mut state,
            &[tool_search_done_event(None, json!({"query":"duplicate"}),)],
        )
        .is_empty());
    }

    #[test]
    fn tool_search_done_with_changed_call_id_keeps_the_added_call_identity() {
        let mut state = ResponsesState::default();
        let events = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{
                    "type":"tool_search_call","id":"item-1","call_id":"c1",
                    "execution":"client","arguments":{"query":"calendar"}
                }}),
                tool_search_done_event(Some("c2"), json!({"query":"calendar"})),
            ],
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "tool_search".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::ToolSearch),
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: r#"{"query":"calendar"}"#.into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
        assert!(map_responses_events(
            &mut state,
            &[tool_search_done_event(
                Some("c2"),
                json!({"query":"duplicate"}),
            )],
        )
        .is_empty());
    }

    #[test]
    fn late_tool_search_added_uses_the_existing_canonical_stopped_identity() {
        let mut state = ResponsesState::default();
        let initial = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{
                    "type":"tool_search_call","id":"item-1","call_id":"c1",
                    "execution":"client","arguments":{}
                }}),
                tool_search_done_event(Some("c1"), json!({})),
            ],
        );
        assert_eq!(
            initial,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "tool_search".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::ToolSearch),
                    input_format: None,
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
        assert!(map_responses_events(
            &mut state,
            &[json!({"type":"response.output_item.added","item":{
                "type":"tool_search_call","id":"item-1","call_id":"c2",
                "execution":"client","arguments":{"query":"late"}
            }})],
        )
        .is_empty());
    }

    #[test]
    fn custom_deltas_are_raw_and_stop_once() {
        let events = map_responses_sequence(&[
            json!({"type":"response.created","response":{"model":"gpt-5"}}),
            json!({"type":"response.output_item.added","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":""
            }}),
            json!({"type":"response.custom_tool_call_input.delta",
                "item_id":"item-1","delta":"*** Begin"}),
            json!({"type":"response.custom_tool_call_input.delta",
                "item_id":"item-1","delta":" Patch"}),
            json!({"type":"response.custom_tool_call_input.done",
                "item_id":"item-1","input":"*** Begin Patch"}),
            json!({"type":"response.output_item.done","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":"*** Begin Patch"
            }}),
            json!({"type":"response.completed","response":{
                "output":[{"type":"custom_tool_call"}],
                "usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}
            }}),
        ]);
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "gpt-5".into()
                },
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "apply_patch".into(),
                    thought_signature: None,
                    tool_kind: Some(crate::types::provider::ProviderToolCallKind::Custom),
                    input_format: Some(crate::types::provider::ToolInputFormat::Raw),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: "*** Begin".into(),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: " Patch".into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: Usage {
                        input_tokens: 0,
                        output_tokens: 0,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: Some(0),
                    },
                },
            ]
        );
    }

    #[test]
    fn custom_input_done_never_stops_before_output_item_done() {
        let mut state = ResponsesState::default();
        map_responses_event(
            &json!({"type":"response.output_item.added","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":""
            }}),
            &mut state,
        );
        let (done_events, _) = map_responses_event(
            &json!({"type":"response.custom_tool_call_input.done",
                "item_id":"item-1","input":"raw"}),
            &mut state,
        );
        assert_eq!(
            done_events,
            vec![ModelStreamEvent::ToolUseInputDelta {
                id: "c1".into(),
                partial_json: "raw".into(),
            }]
        );
        assert!(!done_events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ToolUseStop { .. })));
    }

    #[test]
    fn unknown_custom_delta_is_ignored_because_it_has_no_tool_name() {
        let mut state = ResponsesState::default();
        let (events, _) = map_responses_event(
            &json!({"type":"response.custom_tool_call_input.delta",
                "item_id":"missing","delta":"raw"}),
            &mut state,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn output_item_done_without_start_synthesizes_exact_missing_custom_events() {
        let mut state = ResponsesState::default();
        let (events, _) = map_responses_event(
            &json!({"type":"response.output_item.done","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":"raw"
            }}),
            &mut state,
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "c1".into(),
                    name: "apply_patch".into(),
                    thought_signature: None,
                    tool_kind: Some(crate::types::provider::ProviderToolCallKind::Custom),
                    input_format: Some(crate::types::provider::ToolInputFormat::Raw),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "c1".into(),
                    partial_json: "raw".into(),
                },
                ModelStreamEvent::ToolUseStop { id: "c1".into() },
            ]
        );
    }

    #[test]
    fn tool_search_stream_is_json_default_and_sets_tool_use_stop_reason() {
        let events = map_responses_sequence(&[
            json!({"type":"response.created","response":{"model":"gpt-5"}}),
            json!({"type":"response.output_item.added","item":{
                "type":"tool_search_call","id":"item-s1","call_id":"s1",
                "execution":"client","arguments":{"query":"calendar"}
            }}),
            json!({"type":"response.output_item.done","item":{
                "type":"tool_search_call","id":"item-s1","call_id":"s1",
                "execution":"client","arguments":{"query":"calendar"}
            }}),
            json!({"type":"response.completed","response":{
                "output":[{"type":"tool_search_call"}],
                "usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}
            }}),
        ]);
        assert!(matches!(
            &events[0],
            ModelStreamEvent::MessageStart { model } if model == "gpt-5"
        ));
        assert!(matches!(
            &events[1],
            ModelStreamEvent::ToolUseStart {
                id,
                name,
                tool_kind: Some(crate::types::provider::ProviderToolCallKind::ToolSearch),
                input_format: None,
                ..
            } if id == "s1" && name == "tool_search"
        ));
        assert!(matches!(
            &events[2],
            ModelStreamEvent::ToolUseInputDelta { id, partial_json }
                if id == "s1" && partial_json == r#"{"query":"calendar"}"#
        ));
        assert_eq!(events[3], ModelStreamEvent::ToolUseStop { id: "s1".into() });
        assert!(matches!(
            events[4],
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                ..
            }
        ));
    }

    #[test]
    fn tool_search_defaults_empty_arguments_and_ignores_non_client_items() {
        let events = map_responses_sequence(&[
            json!({"type":"response.output_item.added","item":{
                "type":"tool_search_call","id":"server-item","call_id":"server-call",
                "execution":"server","arguments":{"query":"ignored"}
            }}),
            json!({"type":"response.output_item.done","item":{
                "type":"tool_search_call","id":"server-item","call_id":"server-call",
                "execution":"server","arguments":{"query":"ignored"}
            }}),
            json!({"type":"response.output_item.added","item":{
                "type":"tool_search_call","id":"client-item","call_id":"client-call",
                "execution":"client"
            }}),
            json!({"type":"response.output_item.done","item":{
                "type":"tool_search_call","id":"client-item","call_id":"client-call",
                "execution":"client"
            }}),
        ]);
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "client-call".into(),
                    name: "tool_search".into(),
                    thought_signature: None,
                    tool_kind: Some(ProviderToolCallKind::ToolSearch),
                    input_format: None,
                },
                ModelStreamEvent::ToolUseStop {
                    id: "client-call".into(),
                },
                ModelStreamEvent::MessageStart {
                    model: "gpt-5".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 0,
                        output_tokens: 0,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]
        );
    }

    #[test]
    fn function_done_without_added_emits_no_events() {
        let mut state = ResponsesState::default();
        let (events, _) = map_responses_event(
            &json!({"type":"response.output_item.done","item":{
                "type":"function_call","id":"item-f1","call_id":"f1",
                "name":"read","arguments":"{\"path\":\"/tmp/x\"}"
            }}),
            &mut state,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn function_added_without_call_id_preserves_legacy_empty_call_id() {
        let mut state = ResponsesState::default();
        let (events, _) = map_responses_event(
            &json!({"type":"response.output_item.added","item":{
                "type":"function_call","id":"item-f1","name":"read"
            }}),
            &mut state,
        );
        assert_eq!(
            events,
            vec![ModelStreamEvent::ToolUseStart {
                id: RuntimeString::default(),
                name: "read".into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            }]
        );
    }

    #[test]
    fn duplicate_done_and_late_custom_events_are_ignored() {
        let mut state = ResponsesState::default();
        map_responses_event(
            &json!({"type":"response.output_item.added","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":""
            }}),
            &mut state,
        );
        let done = json!({"type":"response.output_item.done","item":{
            "type":"custom_tool_call","id":"item-1","call_id":"c1",
            "name":"apply_patch","input":""
        }});
        let (first_done, _) = map_responses_event(&done, &mut state);
        assert_eq!(
            first_done,
            vec![ModelStreamEvent::ToolUseStop { id: "c1".into() }]
        );

        let (duplicate_done, _) = map_responses_event(
            &json!({"type":"response.output_item.done","item":{
                "type":"custom_tool_call","id":"item-1","call_id":"c1",
                "name":"apply_patch","input":"late duplicate input"
            }}),
            &mut state,
        );
        let (late_delta, _) = map_responses_event(
            &json!({"type":"response.custom_tool_call_input.delta",
                "item_id":"item-1","delta":"late"}),
            &mut state,
        );
        let (late_input_done, _) = map_responses_event(
            &json!({"type":"response.custom_tool_call_input.done",
                "item_id":"item-1","input":"late full input"}),
            &mut state,
        );
        assert!(duplicate_done.is_empty());
        assert!(late_delta.is_empty());
        assert!(late_input_done.is_empty());
    }

    #[test]
    fn responses_message_phase_precedes_text_and_preserves_message_boundaries() {
        let events = map_responses_events(
            &mut ResponsesState::default(),
            &[
                json!({"type":"response.output_item.added","item":{"type":"message","id":"m1","phase":"commentary"}}),
                json!({"type":"response.output_text.delta","item_id":"m1","delta":"Checking"}),
                json!({"type":"response.output_item.added","item":{"type":"message","id":"m2","phase":"final_answer"}}),
                json!({"type":"response.output_text.delta","item_id":"m2","delta":"Answer"}),
                json!({"type":"response.output_item.done","item":{"type":"message","id":"m2","phase":"final_answer"}}),
            ],
        );
        assert_eq!(
            events,
            vec![
                ModelStreamEvent::TextMessage {
                    id: "m1".into(),
                    phase: Some("commentary".into())
                },
                ModelStreamEvent::TextDelta {
                    text: "Checking".into()
                },
                ModelStreamEvent::TextMessage {
                    id: "m2".into(),
                    phase: Some("final_answer".into())
                },
                ModelStreamEvent::TextDelta {
                    text: "Answer".into()
                },
                ModelStreamEvent::TextMessageCompleted {
                    id: "m2".into(),
                    phase: Some("final_answer".into()),
                    text: "Answer".into()
                },
            ]
        );
        let mut state = ResponsesState::default();
        let unknown = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.added","item":{"type":"message","id":"m1"}}),
                json!({"type":"response.output_text.delta","item_id":"m1","delta":"Text"}),
            ],
        );
        assert!(matches!(
            &unknown[0],
            ModelStreamEvent::TextMessage { phase: None, .. }
        ));
        let late = map_responses_events(
            &mut state,
            &[
                json!({"type":"response.output_item.done","item":{"type":"message","id":"m1","phase":"final_answer"}}),
            ],
        );
        assert_eq!(
            late,
            vec![
                ModelStreamEvent::TextMessage {
                    id: "m1".into(),
                    phase: Some("final_answer".into())
                },
                ModelStreamEvent::TextMessageCompleted {
                    id: "m1".into(),
                    phase: Some("final_answer".into()),
                    text: "Text".into()
                }
            ]
        );
    }

    #[test]
    fn responses_history_preserves_commentary_and_final_answer_as_separate_messages() {
        let content = [
            ("c", "commentary", "Checking"),
            ("f", "final_answer", "Answer"),
        ]
        .into_iter()
        .map(|(id, phase, text)| {
            ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: Some(id.into()),
                phase: Some(phase.into()),
            })
        })
        .collect();
        let input = to_responses_input(
            &[Message::Assistant(AssistantMessage {
                uuid: "a".into(),
                content,
                model: "gpt-5".into(),
                usage: None,
            })],
            &[],
        );
        assert_eq!(
            input,
            json!([
                {"role":"assistant","content":"Checking","phase":"commentary"},
                {"role":"assistant","content":"Answer","phase":"final_answer"}
            ])
        );
    }

    #[test]
    fn responses_events_full_turn() {
        let mut state = ResponsesState::default();
        let mut events = Vec::new();
        let mut push = |ev: Value, state: &mut ResponsesState| {
            let (evs, _t) = map_responses_event(&ev, state);
            events.extend(evs);
        };

        push(
            json!({ "type": "response.created", "response": { "model": "o3" } }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_item.added", "item": { "type": "reasoning", "id": "rs_1" } }),
            &mut state,
        );
        push(
            json!({ "type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "think" }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs_1", "encrypted_content": "ENC" } }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_text.delta", "delta": "Answer" }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_item.added", "item": { "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "grep" } }),
            &mut state,
        );
        push(
            json!({ "type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": "{\"q\":1}" }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_item.done", "item": { "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "grep" } }),
            &mut state,
        );
        push(
            json!({ "type": "response.output_text.annotation.added", "annotation": { "type": "url_citation", "url": "http://x", "title": "X", "start_index": 0, "end_index": 3 } }),
            &mut state,
        );
        push(
            json!({ "type": "response.completed", "response": { "usage": { "input_tokens": 100, "output_tokens": 20, "total_tokens": 150, "input_tokens_details": { "cached_tokens": 30 } }, "output": [ { "type": "function_call" } ] } }),
            &mut state,
        );

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::MessageStart { model: "o3".into() },
                ModelStreamEvent::ThinkingDelta {
                    text: "think".into()
                },
                ModelStreamEvent::ThinkingBlock {
                    text: "think".into(),
                    signature: None,
                    encrypted_content: Some("ENC".into()),
                    is_summary: true,
                },
                ModelStreamEvent::TextDelta {
                    text: "Answer".into()
                },
                ModelStreamEvent::ToolUseStart {
                    id: "call_1".into(),
                    name: "grep".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call_1".into(),
                    partial_json: "{\"q\":1}".into()
                },
                ModelStreamEvent::ToolUseStop {
                    id: "call_1".into()
                },
                ModelStreamEvent::Citation {
                    citation: Citation {
                        url: "http://x".into(),
                        title: Some("X".into()),
                        cited_text: None,
                        start_index: Some(0),
                        end_index: Some(3),
                        encrypted_index: None
                    }
                },
            ]
        );
        assert_eq!(state.stop_reason, ModelStopReason::ToolUse);
        assert_eq!(state.input_tokens, 100);
        assert_eq!(state.cache_read, 30);
        assert_eq!(state.total_tokens, 150);
    }

    #[test]
    fn responses_custom_tool_call_done_only_emits_raw_tool_input() {
        let mut state = ResponsesState::default();
        let (events, _terminate) = map_responses_event(
            &json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "custom_tool_call",
                    "id": "ctc_1",
                    "call_id": "call_exec",
                    "name": "exec",
                    "input": "console.log(1)"
                }
            }),
            &mut state,
        );

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "call_exec".into(),
                    name: "exec".into(),
                    thought_signature: None,
                    tool_kind: Some(crate::types::provider::ProviderToolCallKind::Custom,),
                    input_format: Some(crate::types::provider::ToolInputFormat::Raw),
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "call_exec".into(),
                    partial_json: "console.log(1)".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "call_exec".into(),
                },
            ]
        );
    }

    #[test]
    fn responses_web_search_query() {
        let mut state = ResponsesState::default();
        let (evs, _t) = map_responses_event(
            &json!({ "type": "response.output_item.done", "item": { "type": "web_search_call", "id": "ws_1", "action": { "type": "search", "query": "rust sse" } } }),
            &mut state,
        );
        assert_eq!(
            evs,
            vec![ModelStreamEvent::ServerSearchQuery {
                id: "ws_1".into(),
                query: "rust sse".into(),
                action: Some(crate::types::provider::WebSearchAction::Search {
                    query: Some("rust sse".into()),
                    queries: None,
                })
            }]
        );
    }

    #[test]
    fn responses_web_search_open_and_find_actions() {
        let mut state = ResponsesState::default();
        let (open, _t) = map_responses_event(
            &json!({ "type": "response.output_item.done", "item": { "type": "web_search_call", "id": "ws_open", "action": { "type": "open_page", "url": "https://example.com/doc" } } }),
            &mut state,
        );
        assert_eq!(
            open,
            vec![ModelStreamEvent::ServerSearchQuery {
                id: "ws_open".into(),
                query: "https://example.com/doc".into(),
                action: Some(crate::types::provider::WebSearchAction::OpenPage {
                    url: Some("https://example.com/doc".into()),
                }),
            }]
        );

        let (find, _t) = map_responses_event(
            &json!({ "type": "response.output_item.done", "item": { "type": "web_search_call", "id": "ws_find", "action": { "type": "find_in_page", "url": "https://example.com/doc", "pattern": "needle" } } }),
            &mut state,
        );
        assert_eq!(
            find,
            vec![ModelStreamEvent::ServerSearchQuery {
                id: "ws_find".into(),
                query: "needle @ https://example.com/doc".into(),
                action: Some(crate::types::provider::WebSearchAction::FindInPage {
                    url: Some("https://example.com/doc".into()),
                    pattern: Some("needle".into()),
                }),
            }]
        );
    }

    #[test]
    fn responses_failed_and_error_terminate() {
        let mut state = ResponsesState::default();
        let (evs, term) = map_responses_event(
            &json!({ "type": "response.failed", "response": { "error": { "message": "boom", "code": "rate_limit_exceeded" } } }),
            &mut state,
        );
        assert!(term);
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], ModelStreamEvent::Error { .. }));

        let (evs2, term2) =
            map_responses_event(&json!({ "type": "error", "message": "bad" }), &mut state);
        assert!(term2);
        assert!(matches!(evs2[0], ModelStreamEvent::Error { .. }));
    }

    #[test]
    fn responses_reasoning_prefers_item_summary() {
        let mut state = ResponsesState::default();
        state
            .reasoning_summaries
            .insert("rs".into(), "accumulated".into());
        let (evs, _t) = map_responses_event(
            &json!({ "type": "response.output_item.done", "item": { "type": "reasoning", "id": "rs", "summary": [ { "type": "summary_text", "text": "final" } ] } }),
            &mut state,
        );
        assert_eq!(
            evs,
            vec![ModelStreamEvent::ThinkingBlock {
                text: "final".into(),
                signature: None,
                encrypted_content: None,
                is_summary: true,
            }]
        );
    }

    #[test]
    fn sse_bytes_to_events_end_to_end() {
        let raw = b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"model\":\"o3\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n";
        let sse = parse_all_events(raw);
        assert_eq!(sse.len(), 2);
        let mut state = ResponsesState::default();
        let mut out = Vec::new();
        for e in &sse {
            let evj = runtime_json::parse(&e.data).expect("SSE data is exact runtime JSON");
            let (evs, _t) = map_responses_event(&evj, &mut state);
            out.extend(evs);
        }
        assert_eq!(
            out,
            vec![
                ModelStreamEvent::MessageStart { model: "o3".into() },
                ModelStreamEvent::TextDelta { text: "Hi".into() },
            ]
        );
    }

    // ---- capabilities ----

    #[test]
    fn structured_output_buffers_reject_excess_without_retaining_it() {
        let mut buffered = RuntimeString::from("x".repeat(MAX_STRUCTURED_OUTPUT_UNITS));
        assert!(StructuredOutputState::append(&mut buffered, &"x".into()).is_err());
        assert_eq!(buffered.as_utf16().len(), MAX_STRUCTURED_OUTPUT_UNITS);

        let mut req = base_req();
        req.output_schema = Some(schema_obj());
        let mut state = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        for _ in 0..4096 {
            state
                .buffer(
                    ModelStreamEvent::RedactedThinkingBlock { data: "".into() },
                    0,
                )
                .unwrap();
        }
        assert!(state
            .buffer(
                ModelStreamEvent::RedactedThinkingBlock { data: "".into() },
                0
            )
            .is_err());
        assert_eq!(state.pending.len(), 4096);
    }

    #[test]
    fn structured_output_releases_tool_events_in_original_order_only_after_acceptance() {
        let mut req = base_req();
        req.output_schema = Some(schema_obj());
        req.tools.push(function_spec("read", "Read.", schema_obj()));
        let mut state = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        let prefix = vec![
            ModelStreamEvent::TextMessage {
                id: "commentary".into(),
                phase: Some("commentary".into()),
            },
            ModelStreamEvent::TextDelta {
                text: "Checking.".into(),
            },
            ModelStreamEvent::ThinkingBlock {
                text: "A visible thought".into(),
                is_summary: true,
                signature: Some("signature".into()),
                encrypted_content: Some("opaque-history".into()),
            },
        ];
        for event in &prefix {
            assert!(state.project(event.clone()).unwrap().is_empty());
        }
        let start = ModelStreamEvent::ToolUseStart {
            id: "user-call".into(),
            name: "read".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        };
        let mut expected = prefix;
        expected.push(start.clone());
        assert!(state.project(start).unwrap().is_empty());
        let later = ModelStreamEvent::TextDelta {
            text: "More commentary.".into(),
        };
        expected.push(later.clone());
        assert!(state.project(later).unwrap().is_empty());
        let stop = ModelStreamEvent::ToolUseStop {
            id: "user-call".into(),
        };
        expected.push(stop.clone());
        assert!(state.project(stop).unwrap().is_empty());
        let terminal = ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: make_usage(0, 0, 0, 0),
        };
        expected.push(terminal.clone());
        assert_eq!(state.project(terminal).unwrap(), expected);
    }

    #[test]
    fn structured_output_rejects_multiple_formatting_call_identities() {
        let mut req = base_req();
        req.output_schema = Some(schema_obj());
        let mut state = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        for (id, accepted) in [("first", true), ("second", false)] {
            let result = state.project(ModelStreamEvent::ToolUseStart {
                id: id.into(),
                name: state.tool_name.clone(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            });
            assert_eq!(result.is_ok(), accepted);
        }
    }

    #[test]
    fn structured_output_required_choice_accepts_an_enabled_hosted_search_action() {
        let mut req = base_req();
        req.output_schema = Some(schema_obj());
        req.tool_choice = Some(ToolChoice::Any);
        req.web_search = Some(crate::types::provider::WebSearchRequest::default());
        let mut state = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        let search = ModelStreamEvent::ServerSearchQuery {
            id: "hosted-search".into(),
            query: "Klaus".into(),
            action: Some(WebSearchAction::Search {
                query: Some("Klaus".into()),
                queries: None,
            }),
        };
        let text = ModelStreamEvent::TextDelta { text: "{}".into() };
        assert!(state.project(search.clone()).unwrap().is_empty());
        assert!(state.project(text.clone()).unwrap().is_empty());
        let terminal = ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: make_usage(0, 0, 0, 0),
        };
        assert_eq!(
            state.project(terminal.clone()).unwrap(),
            vec![search.clone(), text, terminal]
        );

        req.web_search = None;
        req.output_schema = Some(schema_obj());
        let mut disabled = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        assert!(disabled.project(search).is_err());
    }

    #[tokio::test]
    async fn cancelled_structured_output_never_releases_a_buffered_result() {
        let mut req = base_req();
        req.output_schema = Some(schema_obj());
        let state = StructuredOutputState::prepare(&mut req, &ModelCapabilities::default())
            .unwrap()
            .unwrap();
        let events = vec![
            ModelStreamEvent::TextDelta { text: "{}".into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: make_usage(0, 0, 0, 0),
            },
        ];
        let signal = AbortSignal::new();
        signal.cancel();
        assert!(
            emulate_structured_output(Box::pin(futures::stream::iter(events)), state, signal)
                .collect::<Vec<_>>()
                .await
                .is_empty()
        );
    }

    #[test]
    fn responses_cache_creation_can_arrive_before_its_prompt_total() {
        let mut state = ResponsesState::default();
        assert!(update_responses_usage(
            &json!({"usage":{"input_tokens_details":{"cache_creation_tokens":30}}}),
            &mut state
        ));
        assert_eq!(state.usage().cache_creation_input_tokens, 0);
        assert!(update_responses_usage(
            &json!({"usage":{"input_tokens":100,"output_tokens":10,"input_tokens_details":{"cached_tokens":20}}}),
            &mut state
        ));
        assert_eq!(state.usage().cache_creation_input_tokens, 30);
        assert_eq!(state.usage().input_tokens, 50);
        assert!(!update_responses_usage(
            &json!({"usage":{"input_tokens_details":{"cache_creation_tokens":-1}}}),
            &mut state
        ));
        assert!(!update_responses_usage(
            &json!({"usage":{"input_tokens_details":{"cache_creation_tokens":0.5}}}),
            &mut state
        ));
        assert_eq!(state.usage().cache_creation_input_tokens, 30);
    }

    #[test]
    fn capabilities_vary_by_api_and_model() {
        // Chat surface, minimal declared capabilities: no thinking, no web search,
        // provider-default window, no native pdf.
        let chat = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Chat),
            ..Default::default()
        });
        let c = chat.capabilities(&"any-model".into());
        assert!(!c.thinking);
        assert!(!c.web_search);
        assert!(!c.structured_output);
        assert!(!c.prompt_cache_control);
        assert_eq!(c.context_window, DEFAULT_CONTEXT_WINDOW);
        assert_eq!(c.max_output_tokens, DEFAULT_MAX_OUTPUT);
        assert!(!c.native_pdf);

        // Declared capabilities flow straight through, regardless of model id.
        let chat_caps = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Chat),
            capabilities: Some(ModelCapabilities {
                thinking: Some(crate::types::provider::ThinkingCapability::default()),
                native_pdf: true,
                context_window: Some(1_047_576),
                ..Default::default()
            }),
            ..Default::default()
        });
        let c = chat_caps.capabilities(&"any-model".into());
        assert!(c.thinking);
        assert!(c.native_pdf);
        assert_eq!(c.context_window, 1_047_576);
        // Web search stays off on Chat even when declared: it is a Responses-only surface.
        let chat_ws = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Chat),
            capabilities: Some(ModelCapabilities {
                web_search: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(!chat_ws.capabilities(&"any-model".into()).web_search);

        // Responses surface honors a declared web-search capability.
        let resp = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            capabilities: Some(ModelCapabilities {
                web_search: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(resp.capabilities(&"any-model".into()).web_search);
        assert_eq!(resp.info().name, "openai");
        assert_eq!(resp.info().default_model, "gpt-4o");
    }

    #[test]
    fn chat_params_include_deepseek_thinking() {
        let mut r = base_req();
        r.thinking = Some(ThinkingConfig::Adaptive);
        r.effort = Some(EffortValue::Level(EffortLevel::High));
        let v = build_chat_params_caps(
            &r,
            &caps_with_levels(&[EffortLevel::Low, EffortLevel::High, EffortLevel::Max]),
            OpenAiReasoningDialect::DeepSeek,
        );
        assert_eq!(v["thinking"], json!({ "type": "enabled" }));
        assert_eq!(v["reasoning_effort"], json!("high"));
    }

    #[test]
    fn qoder_summary_disables_thinking_without_changing_main_request_defaults() {
        let mut request = base_req();
        request.max_output_tokens = 20_000;
        let capabilities = ModelCapabilities::default();
        for thinking in [None, Some(ThinkingConfig::Adaptive)] {
            request.thinking = thinking;
            let wire =
                build_chat_params_caps(&request, &capabilities, OpenAiReasoningDialect::Qoder);
            assert!(wire.get("reasoning_effort").is_none());
            assert!(wire.get("thinking").is_none());
        }
        request.thinking = Some(ThinkingConfig::Disabled);
        let wire = build_chat_params_caps(&request, &capabilities, OpenAiReasoningDialect::Qoder);
        assert_eq!(wire["reasoning_effort"], vs("none"));
        assert_eq!(wire["max_completion_tokens"], Value::from(20_000));

        request.thinking = None;
        request.effort = Some(EffortValue::Level(EffortLevel::High));
        let wire = build_chat_params_caps(
            &request,
            &caps_with_levels(&[EffortLevel::Low, EffortLevel::High]),
            OpenAiReasoningDialect::Qoder,
        );
        assert_eq!(wire["reasoning_effort"], vs("high"));
        assert_eq!(wire["max_completion_tokens"], Value::from(20_000));
    }

    #[test]
    fn llama_cpp_explicitly_disables_or_spells_declared_reasoning() {
        let mut request = base_req();
        let caps = caps_with_levels(&[EffortLevel::Low, EffortLevel::Medium, EffortLevel::Xhigh]);
        request.thinking = None;
        request.effort = Some(EffortValue::Level(EffortLevel::High));
        let disabled = build_chat_params_caps(&request, &caps, OpenAiReasoningDialect::LlamaCpp);
        assert_eq!(disabled["reasoning_effort"], json!("none"));

        request.thinking = Some(ThinkingConfig::Adaptive);
        let enabled = build_chat_params_caps(&request, &caps, OpenAiReasoningDialect::LlamaCpp);
        assert_eq!(enabled["reasoning_effort"], json!("medium"));
    }

    #[test]
    fn chat_params_deepseek_thinking_disabled() {
        let mut r = base_req();
        r.thinking = Some(ThinkingConfig::Disabled);
        let v = build_chat_params_caps(
            &r,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::DeepSeek,
        );
        assert_eq!(v["thinking"], json!({ "type": "disabled" }));
    }

    #[test]
    fn chat_params_deepseek_thinking_none_sends_disabled() {
        // When the user turns thinking OFF, session_run sets req.thinking = None.
        // DeepSeek defaults to thinking ON, so the dialect must send "disabled".
        let mut r = base_req();
        r.thinking = None;
        let v = build_chat_params_caps(
            &r,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::DeepSeek,
        );
        assert_eq!(v["thinking"], json!({ "type": "disabled" }));
    }

    #[test]
    fn effort_mapping_deepseek() {
        use OpenAiReasoningDialect::DeepSeek;
        let caps = caps_with_levels(&[
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ]);
        let mut r = base_req();
        r.effort = Some(EffortValue::Level(EffortLevel::Low));
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), Some("low"));
        r.effort = Some(EffortValue::Level(EffortLevel::Medium));
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), Some("high"));
        r.effort = Some(EffortValue::Level(EffortLevel::High));
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), Some("high"));
        r.effort = Some(EffortValue::Level(EffortLevel::Xhigh));
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), Some("max"));
        r.effort = Some(EffortValue::Level(EffortLevel::Max));
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), Some("max"));
        r.effort = None;
        assert_eq!(effort_for_request(&r, &caps, DeepSeek), None);
    }
    #[test]
    fn responses_model_switch_keeps_visible_history_without_foreign_reasoning() {
        let mut request = base_req();
        request.model = "model-b".into();
        request.messages = vec![Message::Assistant(AssistantMessage {
            uuid: "assistant-a".into(),
            model: "model-a".into(),
            usage: None,
            content: vec![
                ContentBlock::Thinking(ThinkingBlock {
                    message_id: None,
                    text: "Reason".into(),
                    signature: None,
                    encrypted_content: Some("opaque-a".into()),
                }),
                text_block("Visible"),
                ContentBlock::ToolUse(ToolUseBlock {
                    id: "call-a".into(),
                    name: "echo".into(),
                    input: json!({"value":"ok"}),
                    tool_kind: None,
                    thought_signature: None,
                }),
            ],
        })];
        let switched = responses_params_default(&request).expect("request");
        let items = switched["input"].as_array().expect("input");
        assert!(items
            .iter()
            .all(|item| item.get("type") != Some(&json!("reasoning"))));
        assert!(items
            .iter()
            .any(|item| item.get("role") == Some(&json!("assistant"))));
        assert!(items
            .iter()
            .any(|item| item.get("type") == Some(&json!("function_call"))));
        request.model = "model-a".into();
        let same = responses_params_default(&request).expect("request");
        assert!(same["input"]
            .as_array()
            .expect("input")
            .iter()
            .any(|item| item.get("type") == Some(&json!("reasoning"))));
    }

    #[test]
    fn responses_adapter_context_requires_matching_source_scope_even_for_same_model_id() {
        let context = runtime_json::stringify(
            &json!({"kind":"klaus.provider-context.v1","scope":"adapter-a","model":"same-model","message":{}}),
        );
        let mut body = json!({"input":[{"type":"reasoning","encrypted_content":""},{"type":"message","role":"assistant","content":"Visible"},{"type":"function_call","name":"echo","call_id":"call-a","arguments":"{}"}]});
        body["input"][0]["encrypted_content"] = vs(context);
        let mut foreign = body.clone();
        retain_scoped_responses_context(&mut foreign, None, &"same-model".into());
        assert_eq!(foreign["input"].as_array().expect("input").len(), 2);
        assert_eq!(foreign["input"][0]["type"], json!("message"));
        assert_eq!(foreign["input"][1]["type"], json!("function_call"));
        let owned = OpenAICompatibilityOptions::try_new(
            HashMap::from([("X-Klaus-Opaque-Replay-Scope".into(), "adapter-a".into())]),
            None,
        )
        .expect("scope");
        retain_scoped_responses_context(&mut body, Some(&owned), &"same-model".into());
        assert_eq!(body["input"].as_array().expect("input").len(), 3);
        let mut switched = body.clone();
        retain_scoped_responses_context(&mut switched, Some(&owned), &"other-model".into());
        assert_eq!(switched["input"].as_array().expect("input").len(), 2);
    }

    #[test]
    fn responses_function_tools_preserve_schemas_choices_and_bidirectional_history() {
        let patch = "*** Begin Patch\n*** End Patch";
        let unrelated = function_spec("remember", "Remember", required_string_schema("thought"));
        let freeform = grammar_spec(
            "apply_patch",
            "Apply patch",
            required_string_schema("patch"),
            ToolSpecGrammarSyntax::Lark,
            "start: PATCH",
        );
        let search = tool_search_spec(
            "tool_search",
            "Discover tools",
            required_string_schema("query"),
        );
        let mut request = base_req();
        request.tools = vec![unrelated, freeform.clone(), search];
        request.messages = vec![
            assistant_custom("patch-call", "apply_patch", patch),
            custom_result("patch-call", "applied"),
            assistant_tool_search("search-call", json!({"query":"read"})),
            tool_search_result("search-call", native_search_output_fixture()),
        ];
        let body = build_responses_params_with_tool_mode(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
            true,
        )
        .expect("function mode");
        assert!(body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool["type"] == json!("function")));
        assert_eq!(body["tools"][1]["parameters"], freeform.input_schema);
        assert_eq!(body["input"][0]["type"], json!("function_call"));
        let args = runtime_json::parse(body["input"][0]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(args, vobj([("patch", vs(patch))]));
        assert_eq!(body["input"][1]["type"], json!("function_call_output"));
        assert_eq!(body["input"][2]["name"], json!("tool_search"));
        assert_eq!(body["input"][3]["type"], json!("function_call_output"));
        let search_output = body["input"][3]["output"]
            .as_str()
            .expect("search output")
            .try_to_utf8()
            .expect("scalar fixture");
        assert!(search_output.contains("search complete"));
        assert!(search_output.contains(
            &runtime_json::stringify(&native_search_output_fixture())
                .try_to_utf8()
                .expect("scalar native fixture")
        ));
        let native = responses_params_default(&request).expect("native default");
        assert_eq!(native["tools"][1]["type"], json!("custom"));
        assert_eq!(native["input"][0]["input"], vs(patch));
        assert_eq!(native["input"][2]["type"], json!("tool_search_call"));
        for (choice, expected) in [
            (ToolChoice::Auto, json!("auto")),
            (ToolChoice::Any, json!("required")),
            (ToolChoice::None, json!("none")),
            (
                ToolChoice::Tool {
                    name: "apply_patch".into(),
                },
                json!({"type":"function","name":"apply_patch"}),
            ),
            (
                ToolChoice::Tool {
                    name: "tool_search".into(),
                },
                json!({"type":"function","name":"tool_search"}),
            ),
        ] {
            request.tool_choice = Some(choice);
            let body = build_responses_params_with_tool_mode(
                &request,
                &ModelCapabilities::default(),
                OpenAiReasoningDialect::OpenAi,
                true,
            )
            .expect("choice");
            assert_eq!(body["tool_choice"], expected);
        }
        request.tool_choice = None;
        request.messages = vec![
            assistant_function("patch-call", "apply_patch", vobj([("patch", vs(patch))])),
            function_result("patch-call", "applied"),
        ];
        let native = responses_params_default(&request).expect("switch to native custom");
        assert_eq!(native["input"][0]["type"], json!("custom_tool_call"));
        assert_eq!(native["input"][0]["input"], vs(patch));
        let functions = build_responses_params_with_tool_mode(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
            true,
        )
        .expect("switch to functions");
        assert_eq!(
            runtime_json::parse(functions["input"][0]["arguments"].as_str().unwrap()).unwrap(),
            vobj([("patch", vs(patch))])
        );
        request.messages = vec![assistant_custom("patch-call", "apply_patch", patch)];
        request.tools.retain(|tool| tool.name != "apply_patch");
        let error = build_responses_params_with_tool_mode(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
            true,
        )
        .expect_err("removed raw schema");
        assert_eq!(
            error.code,
            Some("responses_tool_input_unrepresentable".into())
        );
        let mut ambiguous = freeform;
        ambiguous.input_schema = json!({"type":"object","properties":{"patch":{"type":"string"},"confirm":{"type":"boolean"}},"required":["patch","confirm"]});
        request.tools.push(ambiguous);
        let error = build_responses_params_with_tool_mode(
            &request,
            &ModelCapabilities::default(),
            OpenAiReasoningDialect::OpenAi,
            true,
        )
        .expect_err("raw history cannot invent another required field");
        assert_eq!(
            error.code,
            Some("responses_tool_input_unrepresentable".into())
        );
    }
}
