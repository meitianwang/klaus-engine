//! Google Gemini provider adapter.
//!
//! Maps the engine's provider-agnostic [`ModelRequest`] onto Gemini's
//! `generateContent` wire shape and its streamed `GenerateContentResponse`
//! chunks back onto [`ModelStreamEvent`]. The transport is a raw `reqwest` POST
//! to the `streamGenerateContent` endpoint with `alt=sse`, and Server-Sent
//! Events are decoded by hand.
//!
//! The chunk source is abstracted behind [`GeminiClient`] so tests can inject
//! canned responses; the live implementation ([`RealGeminiClient`]) performs the
//! HTTP call. The pure chunk mapping lives in [`ChunkMapper`] and is unit-tested
//! directly against canned JSON.
//!
//! Prompt caching is implicit (automatic, server-side): there is no request-side
//! TTL/scope knob, so `prompt_cache_control` is false and the adapter only READS
//! `cachedContentTokenCount` for cost/telemetry.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::Stream;
use futures::StreamExt;

use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString, WhatwgUtf8,
};
use crate::schemas::content::{Citation, ContentBlock, NativeSearchResult, OutputBlock};
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::schemas::message::{Message, Usage};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::{
    AuthHeaders, ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream,
    ModelStreamEvent, ProviderCapabilities, ProviderInfo, ThinkingConfig, ToolChoice, ToolSpec,
};

// Sibling modules produced in the same batch (see module-level notes for their
// assumed API surface).
use crate::provider::anthropic::estimate_tokens_fallback;
use crate::provider::auth::AuthSource;
#[cfg(test)]
use crate::provider::behavior::{render_provider_text_envelope, ProviderTextEnvelopeKind};
use crate::provider::behavior::{
    resolve_provider_behavior_profile, ProviderBehaviorProfile, ProviderPromptStyle,
};
use crate::provider::classify::{classify_error, RawProviderError};
use crate::provider::interceptor::{
    admit_attempt, report_attempt, ProviderRequestInfo, SharedRequestInterceptor,
};
use crate::provider::retry::{
    establishment_steps, observe_retries, with_retry, EstablishmentStep, RetryOptions,
};
use crate::provider::util::{
    ascii_lowercase, objectified_tool_input, raw_from_provider_error, retry_err_to_provider,
    tool_result_user_blocks,
};

// ---- constants ----

const DEFAULT_CONTEXT_WINDOW: u64 = 1_048_576;
const DEFAULT_MAX_OUTPUT: u64 = 8_192;
/// Gemini 2.5 default thinking budget when `enabled` carries no explicit budget.
const DEFAULT_THINKING_BUDGET: i64 = 8_192;
/// Validator-skip sentinel for a functionCall's `thoughtSignature`. Gemini 3
/// rejects (400) a replayed model turn whose first functionCall part lacks a
/// thought_signature; the engine does not persist the real one, so we stamp this
/// documented sentinel on the first call.
const SYNTHETIC_THOUGHT_SIGNATURE: &str = "skip_thought_signature_validator";
const THOUGHT_SIGNATURE_PREFIX: &str = "gemini-thought:";

/// Code Assist (OAuth subscription) transport endpoint.
pub const CODE_ASSIST_ENDPOINT: &str = "https://cloudcode-pa.googleapis.com";
const CODE_ASSIST_API_VERSION: &str = "v1internal";

fn vs(value: impl Into<RuntimeString>) -> RuntimeJson {
    RuntimeJson::String(value.into())
}

fn vobj(pairs: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in pairs {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn encoded_runtime<T: RuntimeEncode + ?Sized>(value: &T) -> RuntimeJson {
    runtime_codec::encode_value(value).expect("Gemini runtime DTO encoding is infallible")
}

fn join_runtime_strings<'a>(
    values: impl IntoIterator<Item = &'a RuntimeString>,
    separator: &str,
) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    let mut first = true;
    for value in values {
        if !first {
            output = output.concat(&separator);
        }
        output = output.concat(value);
        first = false;
    }
    output
}

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
    fn is_trim_unit(unit: u16) -> bool {
        matches!(
            unit,
            0x0009..=0x000d
                | 0x0020
                | 0x00a0
                | 0x1680
                | 0x2000..=0x200a
                | 0x2028
                | 0x2029
                | 0x202f
                | 0x205f
                | 0x3000
                | 0xfeff
        )
    }

    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_trim_unit(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_trim_unit(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn invalid_gemini_tool_choice(reason: impl Into<RuntimeString>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: RuntimeString::from("Gemini tool choice is invalid: ")
            .concat(&reason.into())
            .concat(&RuntimeString::from(".")),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

fn stream_ownership_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Auth,
        message: "Provider credentials changed while streaming.".into(),
        status: Some(401),
        code: Some("provider_credential_changed".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

fn validate_gemini_request_choice(request: &ModelRequest) -> Option<ProviderError> {
    match request.tool_choice.as_ref() {
        None | Some(ToolChoice::Auto | ToolChoice::None) => None,
        Some(ToolChoice::Any) if request.tools.is_empty() => Some(invalid_gemini_tool_choice(
            "`any` requires at least one provider-visible tool",
        )),
        Some(ToolChoice::Tool { name }) if !request.tools.iter().any(|tool| tool.name == *name) => {
            let available = if request.tools.is_empty() {
                RuntimeString::from("(none)")
            } else {
                join_runtime_strings(request.tools.iter().map(|tool| &tool.name), ", ")
            };
            let reason = RuntimeString::from("forced tool \"")
                .concat(name)
                .concat(&RuntimeString::from(
                    "\" is not provider-visible; available tools: ",
                ))
                .concat(&available);
            Some(invalid_gemini_tool_choice(reason))
        }
        Some(ToolChoice::Any | ToolChoice::Tool { .. }) => None,
    }
}

fn grounding_retry_tool_choice(
    choice: Option<&ToolChoice>,
) -> Result<Option<ToolChoice>, ProviderError> {
    match choice {
        Some(ToolChoice::Any | ToolChoice::Tool { .. }) => Err(invalid_gemini_tool_choice(
            "native Google Search isolation cannot preserve a function-tool requirement",
        )),
        Some(ToolChoice::Auto) | None => Ok(None),
        Some(ToolChoice::None) => Ok(Some(ToolChoice::None)),
    }
}

/// How this Gemini endpoint expresses thinking on the wire, declared by the
/// client per model family — never inferred from the id. WHETHER the model
/// thinks at all is the separate client-declared [`ModelCapabilities::thinking`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GeminiThinkingDialect {
    /// Gemini 3+: the `thinkingLevel` LOW/HIGH enum, no numeric budget.
    #[default]
    Level,
    /// Gemini 2.5: a numeric `thinkingBudget` within a per-family range; some
    /// families can fully disable thinking with budget 0.
    Budget {
        min: u64,
        max: u64,
        can_disable: bool,
    },
}

// ---- provider options + struct ----

/// Route through Vertex AI instead of the public Gemini API.
#[derive(Debug, Clone)]
pub struct VertexConfig {
    pub project: String,
    pub location: String,
}

/// Route through the Code Assist (OAuth subscription) backend: a different
/// endpoint with a `{model, project, request}` envelope + SSE.
#[derive(Debug, Clone, Default)]
pub struct CodeAssistOptions {
    pub endpoint: Option<String>,
    pub project: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Clone)]
struct CodeAssistConfig {
    endpoint: String,
    project: Option<String>,
    session_id: Option<String>,
}

/// Runtime-only behavior selected by an owning transport adapter, never a user setting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeminiAdapterPolicy {
    /// Replay retained signed parts only on outgoing requests; stream metadata remains canonical.
    pub replay_signed_parts: bool,
    pub honor_tools_capability: bool,
    pub model_tool_budget: bool,
}

/// Construction options for [`GeminiProvider`].
#[derive(Default)]
pub struct GeminiProviderOptions {
    pub api_key: Option<String>,
    /// Override the Gemini API base URL.
    pub base_url: Option<String>,
    pub default_model: Option<String>,
    /// Inject a pre-built chunk source (tests).
    pub client: Option<Arc<dyn GeminiClient>>,
    /// Override request-establishment retry policy.
    pub retry: Option<RetryOptions>,
    /// Subscription/OAuth auth — supplies per-request headers.
    pub auth_source: Option<Arc<dyn AuthSource>>,
    pub vertex: Option<VertexConfig>,
    pub code_assist: Option<CodeAssistOptions>,
    /// Transport-level request interception (per-attempt headers / veto /
    /// settled-outcome observation).
    pub interceptor: Option<SharedRequestInterceptor>,
    /// Client-declared model capabilities. The provider reads THIS instead of
    /// inferring anything from the model id; absent = the minimal set.
    pub capabilities: Option<ModelCapabilities>,
    /// Exact per-model declarations for auxiliary and tier-selected models.
    pub capabilities_by_model: Option<HashMap<String, ModelCapabilities>>,
    /// The endpoint's thinking wire dialect (level vs budget), declared by the
    /// client per model family.
    pub thinking_dialect: Option<GeminiThinkingDialect>,
    /// Exact model-specific thinking wire dialects for mixed catalogs.
    pub thinking_dialects_by_model: Option<HashMap<String, GeminiThinkingDialect>>,
    /// Revalidate credential ownership before publishing each mapped event.
    pub stream_admission: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub adapter_policy: GeminiAdapterPolicy,
}

/// Provider adapter for Google Gemini.
#[derive(Clone)]
pub struct GeminiProvider {
    info: ProviderInfo,
    client: Arc<dyn GeminiClient>,
    retry: RetryOptions,
    auth_source: Option<Arc<dyn AuthSource>>,
    code_assist: Option<CodeAssistConfig>,
    /// HTTP client for the Code Assist transport (the SDK path uses `client`).
    http: reqwest::Client,
    /// Transport-level request interception, when configured.
    interceptor: Option<SharedRequestInterceptor>,
    /// Client-declared model capabilities; the single source of capability truth.
    capabilities: ModelCapabilities,
    capabilities_by_model: HashMap<String, ModelCapabilities>,
    /// The endpoint's thinking wire dialect.
    thinking_dialect: GeminiThinkingDialect,
    thinking_dialects_by_model: HashMap<String, GeminiThinkingDialect>,
    stream_admission: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    adapter_policy: GeminiAdapterPolicy,
}

impl GeminiProvider {
    pub fn new(opts: GeminiProviderOptions) -> Self {
        // The client base URL prefers a subscription backend override; `info`
        // reports only the explicit `base_url` option.
        let client_base = opts
            .auth_source
            .as_ref()
            .and_then(|a| a.base_url().map(String::from))
            .or_else(|| opts.base_url.clone());

        let client: Arc<dyn GeminiClient> = match opts.client {
            Some(c) => c,
            None => {
                // Subscription mode supplies the real bearer per request; a
                // placeholder key stands in at construction.
                let key = opts.api_key.clone().unwrap_or_else(|| {
                    if opts.auth_source.is_some() {
                        "oauth".to_string()
                    } else {
                        String::new()
                    }
                });
                Arc::new(RealGeminiClient::new(key, client_base, opts.vertex.clone()))
            }
        };

        let code_assist = opts.code_assist.map(|ca| CodeAssistConfig {
            endpoint: ca
                .endpoint
                .unwrap_or_else(|| CODE_ASSIST_ENDPOINT.to_string()),
            project: ca.project,
            session_id: ca.session_id,
        });

        let info = ProviderInfo {
            name: "gemini".into(),
            base_url: opts.base_url.clone().map(Into::into),
            env_key: Some("GEMINI_API_KEY".into()),
            default_model: opts
                .default_model
                .unwrap_or_else(|| "gemini-2.5-pro".to_string())
                .into(),
        };

        GeminiProvider {
            info,
            client,
            retry: opts.retry.unwrap_or_default(),
            auth_source: opts.auth_source,
            code_assist,
            http: reqwest::Client::new(),
            interceptor: opts.interceptor,
            capabilities: opts.capabilities.unwrap_or_default(),
            capabilities_by_model: opts.capabilities_by_model.unwrap_or_default(),
            thinking_dialect: opts.thinking_dialect.unwrap_or_default(),
            thinking_dialects_by_model: opts.thinking_dialects_by_model.unwrap_or_default(),
            stream_admission: opts.stream_admission,
            adapter_policy: opts.adapter_policy,
        }
    }

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
                    capabilities.max_media_blocks = None;
                }
                capabilities
            })
    }
}

#[async_trait::async_trait]
impl ModelProvider for GeminiProvider {
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
        let mut req = req;
        if let Err(error) =
            crate::provider::review_target::apply_typed_review_effort(&mut req, raw_effort.as_ref())
        {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error { error }
            }));
        }
        let mut selected = self.clone();
        let model = match req.model.try_to_utf8() {
            Ok(model) => model,
            Err(_) => return crate::provider::model_catalog::uncaptured_model_stream(&req.model),
        };
        selected.capabilities_by_model.insert(model, metadata);
        selected.retry = selected.retry.for_approval_review(total_attempts);
        selected.stream(req, signal)
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        // Capability comes from the client-declared set; the constants are
        // API-surface truths (Gemini always streams, offers tools, can emit
        // native structured output, caches implicitly). Nothing is inferred from
        // the model id.
        let caps = self.capabilities_for(model);
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: if self.adapter_policy.honor_tools_capability {
                caps.tools.unwrap_or(true)
            } else {
                true
            },
            thinking: caps.thinking.is_some(),
            structured_output: true,
            web_search: caps.web_search,
            prompt_cache_control: false,
            context_window: caps.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW),
            max_output_tokens: caps.max_output_tokens.unwrap_or(DEFAULT_MAX_OUTPUT),
            native_pdf: caps.native_pdf,
            vision: caps.vision,
            image_limits: caps.image_limits,
            max_media_blocks: caps.max_media_blocks,
        }
    }

    fn uses_model_tool_budget(&self, _model: &RuntimeString) -> bool {
        self.adapter_policy.model_tool_budget
    }

    fn estimate_tokens(&self, req: &ModelRequest) -> Option<u64> {
        Some(estimate_tokens_fallback(req))
    }

    fn has_prewarm_endpoints(&self) -> bool {
        // Only the Code Assist transport rides this provider's own HTTP
        // client; the subscription client owns its transport internally.
        self.code_assist.is_some()
    }

    fn prewarm_transport(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Vec<crate::provider::prewarm::PrewarmOutcome>> + Send>,
    > {
        let Some(code_assist) = &self.code_assist else {
            return Box::pin(async { Vec::new() });
        };
        let client = self.http.clone();
        let endpoint = crate::provider::prewarm::endpoint_origin(&code_assist.endpoint);
        Box::pin(async move { vec![crate::provider::prewarm::probe(&client, &endpoint).await] })
    }

    fn stream(&self, mut req: ModelRequest, signal: AbortSignal) -> ModelStream {
        if crate::provider::native_compaction::contains_native_checkpoint(&req.messages) {
            return crate::provider::native_compaction::rejected_checkpoint_stream();
        }

        if let Err(error) = crate::provider::native_media::validate_request(
            &req,
            &self.capabilities_for(&req.model).media_input_mime_types,
        ) {
            return crate::provider::native_media::rejected_stream(error);
        }
        if req.web_search.as_ref().is_some_and(|search| {
            search.access == crate::schemas::config::WebSearchAccess::Disabled
        }) {
            req.web_search = None;
        }

        if let Some(error) =
            crate::provider::unsupported_inference_control(&req, "Gemini", false, false, false)
        {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error { error }
            }));
        }
        let client = self.client.clone();
        let http = self.http.clone();
        let retry = self.retry.clone();
        let auth_source = self.auth_source.clone();
        let code_assist = self.code_assist.clone();
        let interceptor = self.interceptor.clone();
        let stream_admission = self.stream_admission.clone();
        let replay_signed_parts = self.adapter_policy.replay_signed_parts;
        let capabilities = self.capabilities_for(&req.model);
        let thinking_dialect = req
            .model
            .try_to_utf8()
            .ok()
            .and_then(|model| self.thinking_dialects_by_model.get(&model).copied())
            .unwrap_or(self.thinking_dialect);

        Box::pin(async_stream::stream! {
            if let Some(error) = validate_gemini_request_choice(&req) {
                yield ModelStreamEvent::Error { error };
                return;
            }
            let model_fallback = req.model.clone();
            let params = build_gemini_request_with_policy(&req, &capabilities, thinking_dialect, replay_signed_parts);

            let s = establish_and_map(GeminiStreamSetup {
                client: client.clone(),
                code_assist: code_assist.clone(),
                http: http.clone(),
                params,
                model_fallback: model_fallback.clone(),
                auth_source: auth_source.clone(),
                retry: retry.clone(),
                signal: signal.clone(),
                interceptor: interceptor.clone(),
            });
            futures::pin_mut!(s);

            let mut established_error: Option<ProviderError> = None;
            let mut emitted_any = false;
            while let Some(item) = s.next().await {
                match item {
                    Ok(ev) => {
                        if stream_admission.as_ref().is_some_and(|admit|!admit()) {
                            yield ModelStreamEvent::Error {error:stream_ownership_error()};
                            return;
                        }
                        emitted_any = true;
                        yield ev;
                    }
                    Err(pe) => {
                        established_error = Some(pe);
                        break;
                    }
                }
            }

            let pe = match established_error {
                Some(pe) => pe,
                None => return,
            };
            if signal.is_cancelled() {
                return;
            }

            // Runtime fallback (user-selected): some Gemini models reject Google
            // Search grounding co-sent with functionDeclarations. Retry once with
            // an ISOLATED grounding call — search only, no function tools.
            if !emitted_any
                && req.web_search.is_some()
                && !req.tools.is_empty()
                && is_grounding_tool_conflict(&pe.message)
            {
                let retry_tool_choice = match grounding_retry_tool_choice(req.tool_choice.as_ref()) {
                    Ok(choice) => choice,
                    Err(error) => {
                        yield ModelStreamEvent::Error { error };
                        return;
                    }
                };
                let mut req2 = req.clone();
                req2.tools = Vec::new();
                req2.tool_choice = retry_tool_choice;
                let params2 = build_gemini_request_with_policy(&req2, &capabilities, thinking_dialect, replay_signed_parts);
                let s2 = establish_and_map(GeminiStreamSetup {
                    client,
                    code_assist,
                    http,
                    params: params2,
                    model_fallback,
                    auth_source,
                    retry,
                    signal: signal.clone(),
                    interceptor,
                });
                futures::pin_mut!(s2);
                let mut err2: Option<ProviderError> = None;
                while let Some(item) = s2.next().await {
                    match item {
                        Ok(ev) => {
                            if stream_admission.as_ref().is_some_and(|admit|!admit()) {
                                yield ModelStreamEvent::Error {error:stream_ownership_error()};
                                return;
                            }
                            yield ev;
                        },
                        Err(pe2) => {
                            err2 = Some(pe2);
                            break;
                        }
                    }
                }
                if let Some(pe2) = err2 {
                    if signal.is_cancelled() {
                        return;
                    }
                    yield ModelStreamEvent::Error { error: pe2 };
                }
                return;
            }

            yield ModelStreamEvent::Error { error: pe };
        })
    }
}

// ---- chunk source seam ----

/// A normalized stream of decoded response chunks. Each item is a fully-formed
/// `GenerateContentResponse` (success) or an establishment/transport error.
pub type GeminiChunkStream =
    Pin<Box<dyn Stream<Item = Result<GenerateContentResponse, GeminiApiError>> + Send>>;

/// A raw Gemini transport error, carrying the signals the shared classifier reads.
#[derive(Debug, Clone)]
pub struct GeminiApiError {
    pub status: Option<u16>,
    pub message: String,
    pub headers: Option<HashMap<String, String>>,
}

/// Opens a `streamGenerateContent` stream. Errors surfaced here count as request
/// establishment failures (retryable); mid-stream failures arrive as `Err` items.
#[async_trait::async_trait]
pub trait GeminiClient: Send + Sync {
    /// Preserve source-specific refusal details before retry classification.
    fn error_details(&self, error: &GeminiApiError) -> RawProviderError {
        raw_from_gemini(error)
    }

    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError>;
}

/// The live HTTP chunk source: POST to `streamGenerateContent?alt=sse`.
struct RealGeminiClient {
    api_key: String,
    base_url: Option<String>,
    vertex: Option<VertexConfig>,
    http: reqwest::Client,
}

impl RealGeminiClient {
    fn new(api_key: String, base_url: Option<String>, vertex: Option<VertexConfig>) -> Self {
        RealGeminiClient {
            api_key,
            base_url,
            vertex,
            http: reqwest::Client::new(),
        }
    }

    fn endpoint_url(&self, model: &RuntimeString) -> String {
        let model = String::from_utf8(model.to_node_utf8())
            .expect("Node UTF-8 projection always produces valid UTF-8");
        if let Some(v) = &self.vertex {
            format!(
                "https://{loc}-aiplatform.googleapis.com/v1/projects/{proj}/locations/{loc}/publishers/google/models/{model}:streamGenerateContent?alt=sse",
                loc = v.location,
                proj = v.project,
                model = model,
            )
        } else {
            let base = self
                .base_url
                .as_deref()
                .unwrap_or("https://generativelanguage.googleapis.com");
            format!(
                "{}/v1beta/models/{}:streamGenerateContent?alt=sse",
                base.trim_end_matches('/'),
                model
            )
        }
    }
}

#[async_trait::async_trait]
impl GeminiClient for RealGeminiClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        let url = self.endpoint_url(&params.model);
        let body = to_generate_content_wire(&params);
        let mut rb = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body);
        // Intercepted headers ride along in every auth mode; the auth branch
        // below keeps precedence on key conflicts.
        for (k, v) in &extra_headers {
            rb = rb.header(k, v);
        }
        match &headers {
            Some(h) => {
                for (k, v) in h {
                    rb = rb.header(k, v);
                }
            }
            None => {
                if !self.api_key.is_empty() {
                    rb = rb.header("x-goog-api-key", &self.api_key);
                }
            }
        }

        let resp = rb.send().await.map_err(|e| GeminiApiError {
            status: None,
            message: e.to_string(),
            headers: None,
        })?;
        let status = resp.status();
        if !status.is_success() {
            let hdrs = header_map(resp.headers());
            let text = resp.text().await.unwrap_or_default();
            let message = if text.is_empty() {
                format!("Gemini request failed ({})", status.as_u16())
            } else {
                text
            };
            return Err(GeminiApiError {
                status: Some(status.as_u16()),
                message,
                headers: Some(hdrs),
            });
        }

        Ok(sse_chunk_stream(resp, signal, decode_generate_content))
    }
}

/// Collect a reqwest header map into a plain string map.
fn header_map(h: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    h.iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_string(), s.to_string()))
        })
        .collect()
}

/// Decode a raw SSE `data:` payload as a `GenerateContentResponse` (public path).
fn decode_generate_content(payload: &RuntimeString) -> Option<GenerateContentResponse> {
    let value = runtime_json::parse(payload).ok()?;
    runtime_codec::decode_value(&value).ok()
}

/// Turn an established SSE response body into a chunk stream: parse SSE events,
/// decode each payload with `decode`, skip malformed chunks, stop on cancellation.
fn sse_chunk_stream(
    resp: reqwest::Response,
    signal: AbortSignal,
    decode: fn(&RuntimeString) -> Option<GenerateContentResponse>,
) -> GeminiChunkStream {
    let byte_stream = resp.bytes_stream();
    Box::pin(async_stream::stream! {
        let mut decoder = SseDecoder::new();
        futures::pin_mut!(byte_stream);
        loop {
            let chunk = tokio::select! {
                _ = signal.cancelled() => return,
                chunk = byte_stream.next() => chunk,
            };
            let Some(chunk) = chunk else { break; };
            if signal.is_cancelled() {
                return;
            }
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    yield Err(GeminiApiError {
                        status: None,
                        message: e.to_string(),
                        headers: None,
                    });
                    return;
                }
            };
            for payload in decoder.push(bytes.as_ref()) {
                if let Some(resp) = decode(&payload) {
                    yield Ok(resp);
                }
            }
        }
        if signal.is_cancelled() { return; }
        for payload in decoder.finish() {
            if let Some(resp) = decode(&payload) {
                yield Ok(resp);
            }
        }
    })
}

// ---- SSE decoding ----

/// A byte-oriented Server-Sent-Events line accumulator. Complete lines are split
/// on `\n`; `data:` lines accumulate, a blank line flushes the buffered payload
/// (multi-line `data:` joined with `\n`). Buffering bytes (not chars) keeps a
/// multibyte code point that straddles a network chunk boundary intact.
struct SseDecoder {
    buffer: Vec<u8>,
    decoder: WhatwgUtf8,
    data_lines: Vec<RuntimeString>,
}

impl SseDecoder {
    fn new() -> Self {
        SseDecoder {
            buffer: Vec::new(),
            decoder: WhatwgUtf8::new(),
            data_lines: Vec::new(),
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Vec<RuntimeString> {
        self.buffer.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(i) = self.buffer.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=i).collect();
            let decoded = self.decoder.feed(&line);
            let text = decoded.slice_utf16(..decoded.len_utf16().saturating_sub(1));
            if let Some(p) = self.consume(&text) {
                out.push(p);
            }
        }
        out
    }

    fn consume(&mut self, raw: &RuntimeString) -> Option<RuntimeString> {
        let line = if raw.ends_with("\r") {
            raw.slice_utf16(..raw.len_utf16().saturating_sub(1))
        } else {
            raw.clone()
        };
        const DATA_PREFIX: [u16; 5] = [
            b'd' as u16,
            b'a' as u16,
            b't' as u16,
            b'a' as u16,
            b':' as u16,
        ];
        if let Some(rest) = line.as_utf16().strip_prefix(&DATA_PREFIX) {
            self.data_lines
                .push(trim_ecmascript(&RuntimeString::from_utf16(rest.to_vec())));
            None
        } else if line.is_empty() {
            self.flush()
        } else {
            None
        }
    }

    fn flush(&mut self) -> Option<RuntimeString> {
        if self.data_lines.is_empty() {
            return None;
        }
        let payload = join_runtime_strings(self.data_lines.iter(), "\n");
        self.data_lines.clear();
        Some(payload)
    }

    fn finish(&mut self) -> Vec<RuntimeString> {
        let mut out = Vec::new();
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            let text = self
                .decoder
                .feed(&line)
                .concat(&std::mem::take(&mut self.decoder).finish());
            if let Some(p) = self.consume(&text) {
                out.push(p);
            }
        } else {
            let _ = std::mem::take(&mut self.decoder).finish();
        }
        if let Some(p) = self.flush() {
            out.push(p);
        }
        out
    }
}

// ---- establishment + mapping ----

/// Owned inputs for one Gemini stream-establishment attempt.
struct GeminiStreamSetup {
    client: Arc<dyn GeminiClient>,
    code_assist: Option<CodeAssistConfig>,
    http: reqwest::Client,
    params: GenerateContentParameters,
    model_fallback: RuntimeString,
    auth_source: Option<Arc<dyn AuthSource>>,
    retry: RetryOptions,
    signal: AbortSignal,
    interceptor: Option<SharedRequestInterceptor>,
}

/// Establish the stream (SDK or Code Assist source) and map its chunks. Yields
/// `Ok(event)` for each mapped stream event, or a single `Err(ProviderError)`
/// when establishment or a mid-stream read fails.
fn establish_and_map(
    setup: GeminiStreamSetup,
) -> impl Stream<Item = Result<ModelStreamEvent, ProviderError>> + Send {
    let GeminiStreamSetup {
        client,
        code_assist,
        http,
        params,
        model_fallback,
        auth_source,
        retry,
        signal,
        interceptor,
    } = setup;
    let review_stream =
        retry.retry_class_policy == Some(crate::provider::retry::RetryClassPolicy::ApprovalReview);
    async_stream::stream! {
        // Surface every establishment retry while its backoff sleeps.
        let (retry, retry_rx) = observe_retries(retry);
        let establish = async {
            if let Some(ca) = code_assist {
                open_code_assist_stream(params, ca, auth_source, retry, signal.clone(), http, interceptor).await
            } else {
                open_sdk_stream(client.clone(), params, auth_source, retry, signal.clone(), interceptor).await
            }
        };
        let mut establish = std::pin::pin!(establishment_steps(establish, retry_rx));
        let mut established = None;
        while let Some(step) = establish.next().await {
            match step {
                EstablishmentStep::Retry(event) => yield Ok(event),
                EstablishmentStep::Done(result) => {
                    established = Some(result);
                    break;
                }
            }
        }
        let chunk_stream = match established {
            Some(Ok(s)) => s,
            Some(Err(pe)) => {
                yield Err(pe);
                return;
            }
            // The step stream always terminates with `Done`.
            None => return,
        };

        let mut mapper = ChunkMapper::new(model_fallback);
        futures::pin_mut!(chunk_stream);
        while let Some(item) = chunk_stream.next().await {
            match item {
                Ok(chunk) => {
                    for ev in mapper.handle_chunk(&chunk) {
                        yield Ok(ev);
                    }
                }
                Err(e) => {
                    let error = classify_error(&client.error_details(&e));
                    yield Err(if review_stream && error.kind == crate::types::errors::ProviderErrorKind::Network { error.mark_stream_interrupted() } else { error });
                    return;
                }
            }
        }
        if signal.is_cancelled() { return; }
        if review_stream && !mapper.terminal_received {
            let error = crate::provider::classify::classify_error(&crate::provider::classify::RawProviderError { message: "Network stream ended before its terminal event".into(), ..Default::default() }).mark_stream_interrupted();
            yield Err(error);
            return;
        }
        for ev in mapper.finish() {
            yield Ok(ev);
        }
    }
}

/// SDK (API-key / Vertex) source: establish the stream, retrying establishment.
async fn open_sdk_stream(
    client: Arc<dyn GeminiClient>,
    params: GenerateContentParameters,
    auth_source: Option<Arc<dyn AuthSource>>,
    retry: RetryOptions,
    signal: AbortSignal,
    interceptor: Option<SharedRequestInterceptor>,
) -> Result<GeminiChunkStream, ProviderError> {
    // The inner client owns URL construction; report the logical endpoint.
    let model = String::from_utf8(params.model.to_node_utf8())
        .expect("Node UTF-8 projection always produces valid UTF-8");
    let attempt_signal = signal.clone();
    with_retry(retry, signal, move |attempt: u32| {
        let client = client.clone();
        let params = params.clone();
        let auth_source = auth_source.clone();
        let signal = attempt_signal.clone();
        let interceptor = interceptor.clone();
        let model = model.clone();
        async move {
            // Interception runs per attempt: headers refresh on every retry
            // and a veto settles the attempt before the network.
            let request = ProviderRequestInfo {
                provider_id: "gemini",
                model: &model,
                url: "streamGenerateContent",
                attempt: attempt + 1,
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
            let headers = match &auth_source {
                Some(a) => Some(
                    a.get_headers(&signal)
                        .await
                        .map_err(raw_from_provider_error)?,
                ),
                None => None,
            };
            let result = client
                .generate_content_stream(params, headers, intercepted, signal.clone())
                .await;
            match &result {
                Ok(_) => report_attempt(interceptor.as_ref(), &request, started, true, None, None),
                Err(error) => report_attempt(
                    interceptor.as_ref(),
                    &request,
                    started,
                    false,
                    error.status,
                    Some(&error.message),
                ),
            }
            result.map_err(|e| client.error_details(&e))
        }
    })
    .await
    .map_err(retry_err_to_provider)
}

/// Whether an error looks like Gemini rejecting Google Search grounding co-sent
/// with function declarations (a mutual-exclusivity 400 on some models).
/// Conservative: requires a grounding term AND a tool term AND an incompatibility
/// phrase, so an unrelated 400 is never mistaken for the conflict.
fn is_grounding_tool_conflict(message: &RuntimeString) -> bool {
    let m = ascii_lowercase(message);
    let grounding =
        m.contains("google_search") || m.contains("googlesearch") || m.contains("grounding");
    let tooling = m.contains("function") || m.contains("tool");
    let conflict = m.contains("not supported")
        || m.contains("unsupported")
        || m.contains("cannot")
        || m.contains("combined")
        || m.contains("only one tool")
        || m.contains("at the same time");
    grounding && tooling && conflict
}

/// Fold a raw Gemini transport error into the shared classifier's raw signals.
/// Gemini only ever surfaces `status` / `message` / `headers`.
fn raw_from_gemini(err: &GeminiApiError) -> RawProviderError {
    RawProviderError {
        status: err.status,
        message: err.message.clone(),
        headers: err.headers.clone().unwrap_or_default(),
        ..Default::default()
    }
}

// ---- chunk -> event mapping ----

/// Running state for mapping a Gemini chunk stream onto [`ModelStreamEvent`]s.
/// Guarantees exactly one `message_start` (synthesized for an empty stream) and
/// one terminal `message_stop`. A thought run is flushed as a `thinking_block` in
/// order (before the text/tool_use it preceded). A function-call part forces stop
/// reason `tool_use` even when Gemini reports STOP alongside it.
pub struct ChunkMapper {
    started: bool,
    stop_reason: ModelStopReason,
    terminal_received: bool,
    saw_function_call: bool,
    thinking_text: RuntimeString,
    thinking_parts: Vec<Part>,
    call_counter: u64,
    last_usage: Option<UsageMetadata>,
    // Grounding arrives on the final chunk AFTER the grounded answer text. We
    // accumulate the answer text (for byte->char citation offsets) and surface
    // the search + citations after the text.
    full_text: RuntimeString,
    grounding: Option<GroundingMetadata>,
    model_fallback: RuntimeString,
}

impl ChunkMapper {
    pub fn new(model_fallback: RuntimeString) -> Self {
        ChunkMapper {
            started: false,
            stop_reason: ModelStopReason::EndTurn,
            terminal_received: false,
            saw_function_call: false,
            thinking_text: RuntimeString::default(),
            thinking_parts: Vec::new(),
            call_counter: 0,
            last_usage: None,
            full_text: RuntimeString::default(),
            grounding: None,
            model_fallback,
        }
    }

    pub fn handle_chunk(&mut self, chunk: &GenerateContentResponse) -> Vec<ModelStreamEvent> {
        let mut out = Vec::new();
        if let Some(usage) = &chunk.usage_metadata {
            let known = self.last_usage.get_or_insert_with(UsageMetadata::default);
            for (reported, target) in [
                (usage.prompt_token_count, &mut known.prompt_token_count),
                (
                    usage.cached_content_token_count,
                    &mut known.cached_content_token_count,
                ),
                (
                    usage.candidates_token_count,
                    &mut known.candidates_token_count,
                ),
                (usage.thoughts_token_count, &mut known.thoughts_token_count),
                (usage.total_token_count, &mut known.total_token_count),
            ] {
                if reported.is_some() {
                    *target = reported;
                }
            }
            out.push(ModelStreamEvent::UsageSnapshot {
                usage: make_usage(self.last_usage.as_ref()),
            });
        }
        if !self.started {
            self.started = true;
            out.push(ModelStreamEvent::MessageStart {
                model: chunk
                    .model_version
                    .clone()
                    .unwrap_or_else(|| self.model_fallback.clone()),
            });
        }
        let candidate = chunk.candidates.as_ref().and_then(|c| c.first());
        if let Some(cand) = candidate {
            if let Some(g) = &cand.grounding_metadata {
                self.grounding = Some(g.clone());
            }
            if let Some(content) = &cand.content {
                for part in &content.parts {
                    if part.thought == Some(true) {
                        self.thinking_parts.push(part.clone());
                        let t = part.text.clone().unwrap_or_default();
                        if !t.is_empty() {
                            self.thinking_text = self.thinking_text.concat(&t);
                            out.push(ModelStreamEvent::ThinkingDelta { text: t });
                        }
                        continue;
                    }
                    // A visible answer or tool call closes the thought-summary
                    // run. Empty and signature-only parts cannot complete it.
                    // Keep the completed block before that text/tool use.
                    let text_nonempty = part.text.as_ref().filter(|text| !text.is_empty());
                    if (!self.thinking_text.is_empty()
                        || self
                            .thinking_parts
                            .iter()
                            .any(|part| part.thought_signature.is_some()))
                        && (text_nonempty.is_some() || part.function_call.is_some())
                    {
                        out.push(self.complete_thinking(true));
                    }
                    let text_offset = self.full_text.len_utf16();
                    if let Some(text) = text_nonempty {
                        self.full_text = self.full_text.concat(text);
                        out.push(ModelStreamEvent::TextDelta { text: text.clone() });
                    } else if let Some(fc) = &part.function_call {
                        // Gemini sends a COMPLETE FunctionCall per part and may
                        // omit the id — synthesize a stable one and emit the
                        // start/delta/stop triple. The model's `thoughtSignature`
                        // rides on this part; capture it for replay next turn.
                        self.saw_function_call = true;
                        let id = match &fc.id {
                            Some(i) => i.clone(),
                            None => {
                                let i = RuntimeString::from(format!("call_{}", self.call_counter));
                                self.call_counter += 1;
                                i
                            }
                        };
                        let name = fc.name.clone().unwrap_or_default();
                        out.push(ModelStreamEvent::ToolUseStart {
                            id: id.clone(),
                            name,
                            thought_signature: part.thought_signature.clone(),
                            tool_kind: None,
                            input_format: None,
                        });
                        let args = fc
                            .args
                            .clone()
                            .unwrap_or_else(|| RuntimeJson::Object(RuntimeObject::new()));
                        out.push(ModelStreamEvent::ToolUseInputDelta {
                            id: id.clone(),
                            partial_json: runtime_json::stringify(&args),
                        });
                        out.push(ModelStreamEvent::ToolUseStop { id });
                    }
                    if part.function_call.is_none() && part.thought_signature.is_some() {
                        out.push(retained_provider_parts(
                            RuntimeString::default(),
                            vec![part.clone()],
                            text_offset,
                        ));
                    }
                }
            }
            if let Some(fr) = &cand.finish_reason {
                self.stop_reason = map_finish_reason(fr);
                self.terminal_received |= !fr.is_empty() && fr != "FINISH_REASON_UNSPECIFIED";
            }
        }
        out
    }

    pub fn finish(&mut self) -> Vec<ModelStreamEvent> {
        let mut out = Vec::new();
        // Empty stream: synthesize the start so a start/stop pair is always seen.
        if !self.started {
            self.started = true;
            out.push(ModelStreamEvent::MessageStart {
                model: self.model_fallback.clone(),
            });
        }
        // Trailing flush: a thinking-only turn (no following text/tool_use part).
        if !self.thinking_text.is_empty()
            || self
                .thinking_parts
                .iter()
                .any(|part| part.thought_signature.is_some())
        {
            out.push(self.complete_thinking(self.terminal_received));
        }
        // Surface the grounding (search + citations) after the text it grounded.
        if let Some(g) = self.grounding.take() {
            out.extend(grounding_events(&g, &self.full_text));
        }
        if self.saw_function_call && self.stop_reason == ModelStopReason::EndTurn {
            self.stop_reason = ModelStopReason::ToolUse;
        }
        out.push(ModelStreamEvent::MessageStop {
            stop_reason: self.stop_reason,
            usage: make_usage(self.last_usage.as_ref()),
        });
        out
    }

    fn complete_thinking(&mut self, completed: bool) -> ModelStreamEvent {
        let text = std::mem::take(&mut self.thinking_text);
        let parts = std::mem::take(&mut self.thinking_parts);
        if parts.iter().any(|part| part.thought_signature.is_some()) {
            retained_provider_parts(text, parts, self.full_text.len_utf16())
        } else {
            completed_thinking_block(text, completed)
        }
    }
}

fn retained_provider_parts(
    text: RuntimeString,
    parts: Vec<Part>,
    text_offset: usize,
) -> ModelStreamEvent {
    let retained = vobj([
        ("parts", encoded_runtime(&parts)),
        ("textOffset", RuntimeJson::from(text_offset as u64)),
    ]);
    ModelStreamEvent::ThinkingBlock {
        text,
        is_summary: false,
        signature: Some(
            RuntimeString::from(THOUGHT_SIGNATURE_PREFIX)
                .concat(&runtime_json::stringify(&retained)),
        ),
        encrypted_content: None,
    }
}

fn completed_thinking_block(text: RuntimeString, completed: bool) -> ModelStreamEvent {
    let is_summary = completed && text.try_to_utf8().is_ok_and(|text| !text.trim().is_empty());
    ModelStreamEvent::ThinkingBlock {
        text,
        signature: None,
        encrypted_content: None,
        is_summary,
    }
}

/// Convert Google Search grounding metadata into neutral search-result + citation events.
fn grounding_events(g: &GroundingMetadata, full_text: &RuntimeString) -> Vec<ModelStreamEvent> {
    let mut out = Vec::new();
    let empty: Vec<GroundingChunk> = Vec::new();
    let chunks = g.grounding_chunks.as_ref().unwrap_or(&empty);
    let id = RuntimeString::from("gemini-search");
    let query = g
        .web_search_queries
        .as_ref()
        .map(|queries| join_runtime_strings(queries.iter(), " "))
        .unwrap_or_default();
    out.push(ModelStreamEvent::ServerSearchQuery {
        id: id.clone(),
        query,
        action: None,
    });
    let results: Vec<NativeSearchResult> = chunks
        .iter()
        .filter_map(|c| {
            let web = c.web.as_ref()?;
            let url = web.uri.clone()?;
            Some(NativeSearchResult {
                title: web.title.clone().unwrap_or_default(),
                url,
                encrypted_token: None,
                page_age: None,
            })
        })
        .collect();
    out.push(ModelStreamEvent::WebSearchResult {
        id: id.clone(),
        results,
        error: None,
    });
    // Each support maps a text segment (byte offsets) to the source chunks it cites.
    if let Some(supports) = &g.grounding_supports {
        for support in supports {
            let seg = support.segment.as_ref();
            let start_index = seg
                .and_then(|s| s.start_index)
                .map(|b| byte_to_char_index(full_text, b));
            let end_index = seg
                .and_then(|s| s.end_index)
                .map(|b| byte_to_char_index(full_text, b));
            if let Some(indices) = &support.grounding_chunk_indices {
                for idx in indices {
                    let web = match chunks.get(*idx).and_then(|c| c.web.as_ref()) {
                        Some(w) => w,
                        None => continue,
                    };
                    let url = match &web.uri {
                        Some(u) => u.clone(),
                        None => continue,
                    };
                    out.push(ModelStreamEvent::Citation {
                        citation: Citation {
                            url,
                            title: web.title.clone(),
                            cited_text: None,
                            start_index,
                            end_index,
                            encrypted_index: None,
                        },
                    });
                }
            }
        }
    }
    out
}

/// Gemini grounding segment offsets are UTF-8 BYTE indices; convert to a
/// character index (UTF-16 code-unit count of the decoded prefix, matching the
/// engine's string-offset convention).
fn byte_to_char_index(text: &RuntimeString, byte_index: u64) -> u64 {
    let bytes = text.to_node_utf8();
    let bi = usize::try_from(byte_index).unwrap_or(usize::MAX);
    if bi >= bytes.len() {
        return NodeBufferUtf8::decode(&bytes).len_utf16() as u64;
    }
    NodeBufferUtf8::decode(&bytes[..bi]).len_utf16() as u64
}

/// Normalize Gemini `usageMetadata` into the engine's [`Usage`]:
/// `promptTokenCount` is the FULL prompt INCLUDING the cached subset, so subtract
/// `cachedContentTokenCount` out of `inputTokens`. There is no cache-CREATION
/// count (implicit caching), and `thoughtsTokenCount` is billed as output, so it
/// is folded into `outputTokens`. `totalTokenCount` is the authoritative total.
fn make_usage(usage: Option<&UsageMetadata>) -> Usage {
    let prompt = usage.and_then(|u| u.prompt_token_count).unwrap_or(0);
    let cached = usage
        .and_then(|u| u.cached_content_token_count)
        .unwrap_or(0)
        .min(prompt);
    let input_tokens = prompt.saturating_sub(cached);
    let output_tokens = usage
        .and_then(|u| u.candidates_token_count)
        .unwrap_or(0)
        .saturating_add(usage.and_then(|u| u.thoughts_token_count).unwrap_or(0));
    let total = usage.and_then(|u| u.total_token_count).unwrap_or(0);
    Usage {
        input_tokens,
        output_tokens,
        cache_read_input_tokens: cached,
        cache_creation_input_tokens: 0,
        total_tokens: Some(if total > 0 {
            total
        } else {
            input_tokens
                .saturating_add(cached)
                .saturating_add(output_tokens)
        }),
    }
}

fn map_finish_reason(reason: &RuntimeString) -> ModelStopReason {
    if reason == "STOP" {
        ModelStopReason::EndTurn
    } else if reason == "MAX_TOKENS" {
        ModelStopReason::MaxTokens
    } else if [
        "SAFETY",
        "RECITATION",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "IMAGE_SAFETY",
    ]
    .iter()
    .any(|candidate| reason == *candidate)
    {
        ModelStopReason::Refusal
    } else {
        ModelStopReason::EndTurn
    }
}

// ---- request building ----

#[cfg(test)]
fn build_gemini_request(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    thinking_dialect: GeminiThinkingDialect,
) -> GenerateContentParameters {
    build_gemini_request_with_policy(req, caps, thinking_dialect, false)
}

fn build_gemini_request_with_policy(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    thinking_dialect: GeminiThinkingDialect,
    replay_signed_parts: bool,
) -> GenerateContentParameters {
    let system_text = join_runtime_strings(req.system.iter().map(|part| &part.text), "\n\n");
    let thinking_config = map_thinking(
        caps,
        thinking_dialect,
        req.thinking,
        req.task_budget,
        req.effort.clone(),
    );

    let mut tools: Vec<Tool> = Vec::new();
    if !req.tools.is_empty() {
        tools.push(Tool {
            function_declarations: Some(req.tools.iter().map(to_function_declaration).collect()),
            google_search: None,
        });
    }
    if req.web_search.is_some() {
        // Google Search grounding. The `googleSearch` tool carries no parameters;
        // allowed/blocked domains, user location, and freshness have no knob.
        tools.push(Tool {
            function_declarations: None,
            google_search: Some(GoogleSearch {}),
        });
    }

    let has_output_schema = req.output_schema.is_some();
    let config = GenerateContentConfig {
        max_output_tokens: req.max_output_tokens,
        system_instruction: if system_text.is_empty() {
            None
        } else {
            Some(system_text)
        },
        tools: if tools.is_empty() { None } else { Some(tools) },
        tool_config: req.tool_choice.as_ref().map(to_tool_config),
        response_mime_type: if has_output_schema {
            Some("application/json".into())
        } else {
            None
        },
        response_json_schema: req.output_schema.as_ref().map(sanitize_schema),
        thinking_config,
    };
    GenerateContentParameters {
        model: req.model.clone(),
        contents: to_contents_with_profile(
            &req.messages,
            &gemini_behavior_profile(&req.model),
            &req.tools,
            replay_signed_parts,
        ),
        config,
    }
}

fn to_function_declaration(t: &ToolSpec) -> FunctionDeclaration {
    FunctionDeclaration {
        name: t.name.clone(),
        description: t.description.clone(),
        parameters_json_schema: sanitize_schema(&t.input_schema),
    }
}

/// Convert the engine's 3-state thinking onto Gemini's family-specific knob,
/// chosen by the client-declared dialect rather than the model id. The `Level`
/// dialect (Gemini 3+) uses `thinkingLevel` LOW/HIGH; the `Budget` dialect
/// (Gemini 2.5) uses a numeric `thinkingBudget` within its declared range. A
/// model with no declared thinking capability gets no thinkingConfig.
fn map_thinking(
    caps: &ModelCapabilities,
    dialect: GeminiThinkingDialect,
    thinking: Option<ThinkingConfig>,
    task_budget: Option<u64>,
    effort: Option<EffortValue>,
) -> Option<ThinkingConfigWire> {
    // No declared thinking capability → no thinkingConfig at all.
    caps.thinking.as_ref()?;
    let native_effort = effort.and_then(|value| match value {
        EffortValue::Level(level) => caps.effort.as_ref()?.clamp(level),
        EffortValue::Number(_) | EffortValue::Custom(_) => None,
    });
    let (min, max, can_disable) = match dialect {
        GeminiThinkingDialect::Level => {
            // The level dialect has no numeric budget knob, so a cross-provider
            // `task_budget` has no analog. Declared native effort or an explicit
            // thinking configuration drives the level.
            if !matches!(thinking, Some(ThinkingConfig::Disabled)) {
                if let Some(level) = native_effort {
                    let level = match level {
                        EffortLevel::Low | EffortLevel::Medium => "LOW",
                        _ => "HIGH",
                    };
                    return Some(ThinkingConfigWire {
                        include_thoughts: Some(true),
                        thinking_budget: None,
                        thinking_level: Some(level.into()),
                    });
                }
            }
            let thinking = thinking?;
            return Some(match thinking {
                ThinkingConfig::Disabled => ThinkingConfigWire {
                    include_thoughts: Some(false),
                    thinking_budget: None,
                    thinking_level: Some("LOW".into()),
                },
                ThinkingConfig::Enabled { .. } => ThinkingConfigWire {
                    include_thoughts: Some(true),
                    thinking_budget: None,
                    thinking_level: Some("HIGH".into()),
                },
                // Omit the level -> the model's own default depth, thoughts surfaced.
                ThinkingConfig::Adaptive => ThinkingConfigWire {
                    include_thoughts: Some(true),
                    thinking_budget: None,
                    thinking_level: None,
                },
            });
        }
        GeminiThinkingDialect::Budget {
            min,
            max,
            can_disable,
        } => (min, max, can_disable),
    };
    // thinkingBudget family (Gemini 2.5): budget precedence is explicit
    // `budget_tokens` -> the cross-provider `task_budget` -> the type default. A
    // budget from either source is clamped to the declared range.
    let explicit_budget = match thinking {
        Some(ThinkingConfig::Enabled { budget_tokens }) => budget_tokens,
        _ => None,
    };
    let budget_source = explicit_budget.or(task_budget);
    if let Some(bs) = budget_source {
        let clamped = bs.max(min).min(max) as i64;
        return Some(ThinkingConfigWire {
            include_thoughts: Some(true),
            thinking_budget: Some(clamped),
            thinking_level: None,
        });
    }
    if !matches!(thinking, Some(ThinkingConfig::Disabled)) {
        if let Some(level) = native_effort {
            // Divide only the endpoint's declared range; explicit token budgets win.
            let quarters = match level {
                EffortLevel::None | EffortLevel::Minimal => 0,
                EffortLevel::Low => 0,
                EffortLevel::Medium => 1,
                EffortLevel::High => 2,
                EffortLevel::Xhigh => 3,
                EffortLevel::Max | EffortLevel::Ultra | EffortLevel::Persistent => 4,
            };
            let budget = min
                .saturating_add(max.saturating_sub(min) / 4 * quarters)
                .max(1)
                .min(max);
            return Some(ThinkingConfigWire {
                include_thoughts: Some(true),
                thinking_budget: Some(budget as i64),
                thinking_level: None,
            });
        }
    }
    let thinking = thinking?;
    Some(match thinking {
        ThinkingConfig::Disabled => {
            // Families that can't disable thinking would 400 on budget 0 -> omit
            // the budget (default depth) rather than send an invalid value.
            if can_disable {
                ThinkingConfigWire {
                    include_thoughts: Some(false),
                    thinking_budget: Some(0),
                    thinking_level: None,
                }
            } else {
                ThinkingConfigWire {
                    include_thoughts: Some(false),
                    thinking_budget: None,
                    thinking_level: None,
                }
            }
        }
        ThinkingConfig::Adaptive => ThinkingConfigWire {
            include_thoughts: Some(true),
            thinking_budget: Some(-1),
            thinking_level: None,
        },
        ThinkingConfig::Enabled { .. } => ThinkingConfigWire {
            include_thoughts: Some(true),
            thinking_budget: Some(DEFAULT_THINKING_BUDGET),
            thinking_level: None,
        },
    })
}

fn to_tool_config(tc: &ToolChoice) -> ToolConfig {
    let fcc = match tc {
        ToolChoice::Auto => FunctionCallingConfig {
            mode: "AUTO".into(),
            allowed_function_names: None,
        },
        ToolChoice::Any => FunctionCallingConfig {
            mode: "ANY".into(),
            allowed_function_names: None,
        },
        ToolChoice::None => FunctionCallingConfig {
            mode: "NONE".into(),
            allowed_function_names: None,
        },
        ToolChoice::Tool { name } => FunctionCallingConfig {
            mode: "ANY".into(),
            allowed_function_names: Some(vec![name.clone()]),
        },
    };
    ToolConfig {
        function_calling_config: fcc,
    }
}

/// Convert the engine's flat message list onto Gemini `Content[]`. Roles: user->'user',
/// assistant->'model', tool_result->'user' (functionResponse part). A
/// functionResponse is matched to its functionCall by id+name, so first index
/// every assistant tool_use block (id->name), then look the name up. Signed
/// thinking parts are replayed only when the owning adapter enables retention.
/// Adjacent same-role Contents are merged so the turn sequence stays well-formed.
#[cfg(test)]
fn to_contents(messages: &[Message]) -> Vec<Content> {
    to_contents_with_profile(
        messages,
        &resolve_provider_behavior_profile(None, &RuntimeString::from("model-x")),
        &[],
        false,
    )
}

fn to_contents_with_profile(
    messages: &[Message],
    profile: &ProviderBehaviorProfile,
    tools: &[ToolSpec],
    replay_signed_parts: bool,
) -> Vec<Content> {
    let mut name_by_tool_use_id: HashMap<RuntimeString, RuntimeString> = HashMap::new();
    for msg in messages {
        if let Message::Assistant(a) = msg {
            for b in &a.content {
                if let ContentBlock::ToolUse(tu) = b {
                    name_by_tool_use_id.insert(tu.id.clone(), tu.name.clone());
                }
            }
        }
    }

    let mut out: Vec<Content> = Vec::new();
    for msg in messages {
        match msg {
            Message::User(u) => out.push(Content {
                role: "user".into(),
                parts: blocks_to_parts(&u.content, tools, replay_signed_parts),
            }),
            Message::Assistant(a) => {
                let parts = blocks_to_parts(&a.content, tools, replay_signed_parts);
                if !parts.is_empty() {
                    out.push(Content {
                        role: "model".into(),
                        parts,
                    });
                }
            }
            Message::ToolResult(tr) => {
                // Echo the call id (= tool_use_id) and recover the name — name
                // alone mis-pairs parallel same-name calls and 400s on Gemini 3.
                let mut parts = vec![Part {
                    function_response: Some(FunctionResponse {
                        id: tr.tool_use_id.clone(),
                        name: name_by_tool_use_id
                            .get(&tr.tool_use_id)
                            .cloned()
                            .unwrap_or_else(|| tr.tool_use_id.clone()),
                        response: vobj([("output", vs(text_of_output_blocks(&tr.content)))]),
                    }),
                    ..Part::default()
                }];
                // Output images and native attachments accompany the function
                // response as parts of the same user Content.
                parts.extend(blocks_to_parts(
                    &tool_result_user_blocks(tr),
                    tools,
                    replay_signed_parts,
                ));
                out.push(Content {
                    role: "user".into(),
                    parts,
                });
            }
            Message::CompactBoundary(cb) => out.push(Content {
                role: "user".into(),
                parts: vec![Part {
                    text: Some(render_gemini_compact_boundary_exact(&cb.summary, profile)),
                    ..Part::default()
                }],
            }),
            Message::System(_) => {}
        }
    }
    merge_adjacent_same_role(out)
}

fn gemini_behavior_profile(model: &RuntimeString) -> ProviderBehaviorProfile {
    resolve_provider_behavior_profile(
        Some(&ProviderInfo {
            name: "gemini".into(),
            base_url: None,
            env_key: None,
            default_model: model.clone(),
        }),
        model,
    )
}

#[cfg(test)]
pub(crate) fn render_gemini_compact_boundary(
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

fn render_gemini_compact_boundary_exact(
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

/// Convert engine content blocks to Gemini parts, retaining signed thinking only
/// when the owning adapter explicitly enables replay.
fn blocks_to_parts(
    blocks: &[ContentBlock],
    tools: &[ToolSpec],
    replay_signed_parts: bool,
) -> Vec<Part> {
    let mut parts = Vec::new();
    // Whether the first functionCall of this model turn has been emitted — the
    // sentinel fallback applies to that first call only.
    let mut saw_first_call = false;
    for b in blocks {
        match b {
            ContentBlock::Text(t) => {
                if !t.text.is_empty() {
                    parts.push(Part {
                        text: Some(t.text.clone()),
                        ..Part::default()
                    });
                }
            }
            ContentBlock::Image(img) => parts.push(Part {
                inline_data: Some(InlineData {
                    mime_type: img.mime_type.clone(),
                    data: img.base64.clone(),
                }),
                ..Part::default()
            }),
            // A PDF ingests as inlineData with its application/pdf mimeType — the
            // same channel as an image. No separate file primitive here.
            ContentBlock::Document(d) => parts.push(Part {
                inline_data: Some(InlineData {
                    mime_type: d.mime_type.clone(),
                    data: d.base64.clone(),
                }),
                ..Part::default()
            }),
            ContentBlock::Media(media) => parts.push(Part {
                inline_data: Some(InlineData {
                    mime_type: media.mime_type.clone(),
                    data: media.base64.clone(),
                }),
                ..Part::default()
            }),
            ContentBlock::ToolUse(tu) => {
                // Echo the call id so the replayed functionCall pairs with its
                // functionResponse. Reasoning continuity rides `thoughtSignature`:
                // prefer the REAL captured one; when absent, Gemini 3 still 400s a
                // replayed turn whose FIRST functionCall lacks a signature, so fall
                // back to the validator-skip sentinel on the first call ONLY.
                let mut part = Part {
                    function_call: Some(FunctionCall {
                        id: Some(tu.id.clone()),
                        name: Some(tu.name.clone()),
                        args: Some(objectified_tool_input(&tu.input, tools)),
                    }),
                    ..Part::default()
                };
                if let Some(sig) = &tu.thought_signature {
                    part.thought_signature = Some(sig.clone());
                } else if !saw_first_call {
                    part.thought_signature = Some(SYNTHETIC_THOUGHT_SIGNATURE.into());
                }
                saw_first_call = true;
                parts.push(part);
            }
            ContentBlock::Thinking(thinking)
                if replay_signed_parts
                    && thinking.signature.as_ref().is_some_and(|signature| {
                        signature.starts_with(THOUGHT_SIGNATURE_PREFIX)
                    }) =>
            {
                let signature = thinking.signature.as_ref().expect("signature checked");
                replay_retained_parts(&mut parts, signature);
            }
            // Unsigned thought text and foreign signatures cannot be replayed.
            // Other block kinds
            // (tool_result / web_search_call) have no Gemini part representation.
            ContentBlock::Thinking(_)
            | ContentBlock::RedactedThinking(_)
            | ContentBlock::ToolResult(_)
            | ContentBlock::WebSearchCall(_) => {}
        }
    }
    parts
}

fn replay_retained_parts(parts: &mut Vec<Part>, signature: &RuntimeString) {
    let Ok(value) = runtime_json::parse(&signature.slice_utf16(THOUGHT_SIGNATURE_PREFIX.len()..))
    else {
        return;
    };
    let Some(offset) = value
        .get("textOffset")
        .and_then(RuntimeJson::as_u64)
        .and_then(|offset| usize::try_from(offset).ok())
    else {
        return;
    };
    let Some(raw) = value.get("parts") else {
        return;
    };
    let Ok(retained) = runtime_codec::decode_value::<Vec<Part>>(raw) else {
        return;
    };
    let replacement = join_runtime_strings(
        retained
            .iter()
            .filter(|part| part.thought != Some(true))
            .filter_map(|part| part.text.as_ref()),
        "",
    );
    let total: usize = parts
        .iter()
        .filter(|part| part.thought != Some(true))
        .filter_map(|part| part.text.as_ref())
        .map(RuntimeString::len_utf16)
        .sum();
    if replacement.is_empty() && offset == total {
        parts.extend(retained);
        return;
    }
    let mut consumed = 0usize;
    for index in 0..parts.len() {
        if parts[index].thought == Some(true) {
            continue;
        }
        let Some(text) = parts[index].text.as_ref() else {
            continue;
        };
        let length = text.len_utf16();
        if offset < consumed || offset > consumed + length {
            consumed += length;
            continue;
        }
        let start = offset - consumed;
        let end = start.saturating_add(replacement.len_utf16());
        if end > length || text.slice_utf16(start..end) != replacement {
            consumed += length;
            continue;
        }
        if parts[index].thought_signature.is_some() && (start != 0 || end != length) {
            if start == length && replacement.is_empty() {
                parts.splice(index + 1..index + 1, retained);
            }
            return;
        }
        let mut expanded = Vec::new();
        if start > 0 {
            let mut before = parts[index].clone();
            before.text = Some(text.slice_utf16(..start));
            expanded.push(before);
        }
        expanded.extend(retained);
        if end < length {
            let mut after = parts[index].clone();
            after.text = Some(text.slice_utf16(end..));
            expanded.push(after);
        }
        parts.splice(index..=index, expanded);
        return;
    }
}

/// A tool_use input is a JSON value; Gemini's functionCall.args wants an object.
fn merge_adjacent_same_role(contents: Vec<Content>) -> Vec<Content> {
    let mut out: Vec<Content> = Vec::new();
    for c in contents {
        if let Some(prev) = out.last_mut() {
            if prev.role == c.role {
                prev.parts.extend(c.parts);
                continue;
            }
        }
        out.push(c);
    }
    out
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

// ---- schema sanitization ----

/// The JSON-Schema keywords Gemini's `responseJsonSchema` / `parametersJsonSchema`
/// accept; everything else (`$schema`, `const`, `pattern`, `default`, `nullable`,
/// `multipleOf`, `minLength`/`maxLength`, ...) is dropped to avoid a 400.
const SCHEMA_KEYWORDS: &[&str] = &[
    "$id",
    "$defs",
    "$ref",
    "$anchor",
    "type",
    "format",
    "title",
    "description",
    "enum",
    "items",
    "prefixItems",
    "minItems",
    "maxItems",
    "minimum",
    "maximum",
    "anyOf",
    "oneOf",
    "properties",
    "additionalProperties",
    "required",
    "propertyOrdering",
];
const SCHEMA_MAP_KEYWORDS: &[&str] = &["properties", "$defs"];
const SCHEMA_ARRAY_KEYWORDS: &[&str] = &["anyOf", "oneOf", "prefixItems"];

/// Keep only the accepted schema keywords, recursing into nested schemas while
/// preserving `properties`/`$defs` map keys (names, not schema keywords).
fn sanitize_schema(node: &RuntimeJson) -> RuntimeJson {
    let obj = match node {
        RuntimeJson::Object(m) => m,
        _ => return node.clone(),
    };
    let mut out = RuntimeObject::new();
    for (key, value) in obj {
        if !SCHEMA_KEYWORDS.iter().any(|candidate| key == *candidate) {
            continue;
        }
        if SCHEMA_MAP_KEYWORDS
            .iter()
            .any(|candidate| key == *candidate)
            && value.is_object()
        {
            // name -> subschema map: keep names, sanitize each subschema.
            let mut m = RuntimeObject::new();
            if let RuntimeJson::Object(vm) = value {
                for (name, sub) in vm {
                    m.insert(name, sanitize_schema(sub));
                }
            }
            out.insert(key, RuntimeJson::Object(m));
        } else if SCHEMA_ARRAY_KEYWORDS
            .iter()
            .any(|candidate| key == *candidate)
            && value.is_array()
        {
            let arr = value
                .as_array()
                .unwrap()
                .iter()
                .map(sanitize_schema)
                .collect();
            out.insert(key, RuntimeJson::Array(arr));
        } else if (key == "items" || key == "additionalProperties")
            && (value.is_object() || value.is_array())
        {
            // `items` may be a single schema OR (tuple form) an array of schemas.
            if value.is_array() {
                let arr = value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(sanitize_schema)
                    .collect();
                out.insert(key, RuntimeJson::Array(arr));
            } else {
                out.insert(key, sanitize_schema(value));
            }
        } else {
            out.insert(key, value.clone());
        }
    }
    RuntimeJson::Object(out)
}

// ---- Code Assist transport ----

/// Establish the Code Assist SSE stream (retrying establishment) and return its
/// chunks, each unwrapped to a `GenerateContentResponse` so the shared mapper can
/// consume them identically to the SDK path.
async fn open_code_assist_stream(
    params: GenerateContentParameters,
    config: CodeAssistConfig,
    auth_source: Option<Arc<dyn AuthSource>>,
    retry: RetryOptions,
    signal: AbortSignal,
    http: reqwest::Client,
    interceptor: Option<SharedRequestInterceptor>,
) -> Result<GeminiChunkStream, ProviderError> {
    let url = format!(
        "{}/{}:streamGenerateContent?alt=sse",
        config.endpoint.trim_end_matches('/'),
        CODE_ASSIST_API_VERSION
    );
    let model = String::from_utf8(params.model.to_node_utf8())
        .expect("Node UTF-8 projection always produces valid UTF-8");
    let envelope = to_code_assist_envelope(
        &params,
        config.project.as_deref(),
        config.session_id.as_deref(),
        &random_prompt_id(),
    );
    let mut body = Vec::new();
    runtime_json::write_to(&envelope, &mut body).expect("writing exact Gemini JSON cannot fail");
    let attempt_signal = signal.clone();
    with_retry(retry, signal, move |attempt: u32| {
        let http = http.clone();
        let url = url.clone();
        let body = body.clone();
        let auth_source = auth_source.clone();
        let signal = attempt_signal.clone();
        let interceptor = interceptor.clone();
        let model = model.clone();
        async move {
            // Interception runs per attempt: headers refresh on every retry
            // and a veto settles the attempt before the network.
            let request = ProviderRequestInfo {
                provider_id: "gemini",
                model: &model,
                url: &url,
                attempt: attempt + 1,
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
            let headers = match &auth_source {
                Some(a) => a
                    .get_headers(&signal)
                    .await
                    .map_err(raw_from_provider_error)?,
                None => HashMap::new(),
            };
            let mut rb = http
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body);
            for (k, v) in &intercepted {
                rb = rb.header(k, v);
            }
            for (k, v) in headers {
                rb = rb.header(k, v);
            }
            let resp = rb.send().await.map_err(|e| {
                report_attempt(
                    interceptor.as_ref(),
                    &request,
                    started,
                    false,
                    None,
                    Some(&e.to_string()),
                );
                raw_from_gemini(&GeminiApiError {
                    status: None,
                    message: e.to_string(),
                    headers: None,
                })
            })?;
            let status = resp.status();
            if !status.is_success() {
                let message = format!("Code Assist request failed ({})", status.as_u16());
                report_attempt(
                    interceptor.as_ref(),
                    &request,
                    started,
                    false,
                    Some(status.as_u16()),
                    Some(&message),
                );
                return Err(raw_from_gemini(&GeminiApiError {
                    status: Some(status.as_u16()),
                    message,
                    headers: Some(header_map(resp.headers())),
                }));
            }
            report_attempt(
                interceptor.as_ref(),
                &request,
                started,
                true,
                Some(status.as_u16()),
                None,
            );
            Ok(sse_chunk_stream(resp, signal.clone(), decode_code_assist))
        }
    })
    .await
    .map_err(retry_err_to_provider)
}

/// Build the Code Assist envelope: `contents`, `systemInstruction`, `tools`,
/// `toolConfig` are hoisted into `request`; the remaining generation params go
/// under `generationConfig`. systemInstruction is rendered as a `Content`.
fn to_code_assist_envelope(
    params: &GenerateContentParameters,
    project: Option<&str>,
    session_id: Option<&str>,
    user_prompt_id: &RuntimeString,
) -> RuntimeJson {
    let cfg = &params.config;
    let mut generation_config = RuntimeObject::new();
    generation_config.insert("maxOutputTokens", RuntimeJson::from(cfg.max_output_tokens));
    if let Some(mt) = &cfg.response_mime_type {
        generation_config.insert("responseMimeType", vs(mt.clone()));
    }
    if let Some(js) = &cfg.response_json_schema {
        generation_config.insert("responseJsonSchema", js.clone());
    }
    if let Some(tc) = &cfg.thinking_config {
        generation_config.insert("thinkingConfig", encoded_runtime(tc));
    }

    let mut request = RuntimeObject::new();
    request.insert("contents", encoded_runtime(&params.contents));
    if let Some(si) = &cfg.system_instruction {
        request.insert(
            "systemInstruction",
            vobj([(
                "parts",
                RuntimeJson::Array(vec![vobj([("text", vs(si.clone()))])]),
            )]),
        );
    }
    if let Some(tools) = &cfg.tools {
        request.insert("tools", encoded_runtime(tools));
    }
    if let Some(tcfg) = &cfg.tool_config {
        request.insert("toolConfig", encoded_runtime(tcfg));
    }
    request.insert("generationConfig", RuntimeJson::Object(generation_config));
    if let Some(sid) = session_id {
        request.insert("session_id", vs(sid));
    }

    let mut env = RuntimeObject::new();
    env.insert("model", vs(params.model.clone()));
    if let Some(p) = project {
        env.insert("project", vs(p));
    }
    env.insert("user_prompt_id", vs(user_prompt_id.clone()));
    env.insert("request", RuntimeJson::Object(request));
    RuntimeJson::Object(env)
}

/// Decode a Code Assist SSE payload, unwrapping `response` into the chunk shape.
fn decode_code_assist(payload: &RuntimeString) -> Option<GenerateContentResponse> {
    let value = runtime_json::parse(payload).ok()?;
    let v: CodeAssistResponse = runtime_codec::decode_value(&value).ok()?;
    let inner = v.response.unwrap_or_default();
    Some(GenerateContentResponse {
        model_version: inner.model_version,
        candidates: Some(inner.candidates.unwrap_or_default()),
        usage_metadata: inner.usage_metadata,
    })
}

/// A per-prompt id stamped on every Code Assist request.
fn random_prompt_id() -> RuntimeString {
    RuntimeString::from(format!("{:032x}", rand::random::<u128>()))
}

// ---- request-side wire types ----

/// Build the public REST body from the assembled parameters.
/// Encode the exact Vertex request shared by cloud transports.
pub fn generate_content_request(params: &GenerateContentParameters) -> RuntimeJson {
    let env = to_code_assist_envelope(params, None, None, &RuntimeString::default());
    env.get("request").cloned().unwrap_or(RuntimeJson::Null)
}

fn to_generate_content_wire(params: &GenerateContentParameters) -> Vec<u8> {
    // The public path reuses the Code Assist envelope's request sub-object shape.
    let request = generate_content_request(params);
    let mut body = Vec::new();
    runtime_json::write_to(&request, &mut body).expect("writing exact Gemini JSON cannot fail");
    body
}

/// The top-level `generateContent` request (mirrors the SDK parameter shape that
/// tests inspect).
#[derive(Debug, Clone, PartialEq)]
pub struct GenerateContentParameters {
    pub model: RuntimeString,
    pub contents: Vec<Content>,
    pub config: GenerateContentConfig,
}

impl RuntimeEncode for GenerateContentParameters {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("model", &self.model)?;
        object.field("contents", &self.contents)?;
        object.field("config", &self.config)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GenerateContentParameters {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model: object.required("model", &[])?,
                contents: object.required("contents", &[])?,
                config: object.required("config", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenerateContentConfig {
    pub max_output_tokens: u64,
    pub system_instruction: Option<RuntimeString>,
    pub tools: Option<Vec<Tool>>,
    pub tool_config: Option<ToolConfig>,
    pub response_mime_type: Option<RuntimeString>,
    pub response_json_schema: Option<RuntimeJson>,
    pub thinking_config: Option<ThinkingConfigWire>,
}

impl RuntimeEncode for GenerateContentConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("maxOutputTokens", &self.max_output_tokens)?;
        if let Some(value) = &self.system_instruction {
            object.field("systemInstruction", value)?;
        }
        if let Some(value) = &self.tools {
            object.field("tools", value)?;
        }
        if let Some(value) = &self.tool_config {
            object.field("toolConfig", value)?;
        }
        if let Some(value) = &self.response_mime_type {
            object.field("responseMimeType", value)?;
        }
        if let Some(value) = &self.response_json_schema {
            object.field("responseJsonSchema", value)?;
        }
        if let Some(value) = &self.thinking_config {
            object.field("thinkingConfig", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GenerateContentConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                max_output_tokens: object.required("maxOutputTokens", &[])?,
                system_instruction: object.optional("systemInstruction", &[])?,
                tools: object.optional("tools", &[])?,
                tool_config: object.optional("toolConfig", &[])?,
                response_mime_type: object.optional("responseMimeType", &[])?,
                response_json_schema: object.optional("responseJsonSchema", &[])?,
                thinking_config: object.optional("thinkingConfig", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Tool {
    pub function_declarations: Option<Vec<FunctionDeclaration>>,
    pub google_search: Option<GoogleSearch>,
}

impl RuntimeEncode for Tool {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.function_declarations {
            object.field("functionDeclarations", value)?;
        }
        if let Some(value) = &self.google_search {
            object.field("googleSearch", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Tool {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                function_declarations: object.optional("functionDeclarations", &[])?,
                google_search: object.optional("googleSearch", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GoogleSearch {}

impl RuntimeEncode for GoogleSearch {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {} = self;
        let object = RuntimeObjectEncoder::new();
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GoogleSearch {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |_object| Ok(Self {}))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionDeclaration {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub parameters_json_schema: RuntimeJson,
}

impl RuntimeEncode for FunctionDeclaration {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        object.field("parametersJsonSchema", &self.parameters_json_schema)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FunctionDeclaration {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                parameters_json_schema: object.required("parametersJsonSchema", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolConfig {
    pub function_calling_config: FunctionCallingConfig,
}

impl RuntimeEncode for ToolConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("functionCallingConfig", &self.function_calling_config)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                function_calling_config: object.required("functionCallingConfig", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCallingConfig {
    pub mode: RuntimeString,
    pub allowed_function_names: Option<Vec<RuntimeString>>,
}

impl RuntimeEncode for FunctionCallingConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mode", &self.mode)?;
        if let Some(value) = &self.allowed_function_names {
            object.field("allowedFunctionNames", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FunctionCallingConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mode: object.required("mode", &[])?,
                allowed_function_names: object.optional("allowedFunctionNames", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThinkingConfigWire {
    pub include_thoughts: Option<bool>,
    pub thinking_budget: Option<i64>,
    pub thinking_level: Option<RuntimeString>,
}

impl RuntimeEncode for ThinkingConfigWire {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.include_thoughts {
            object.field("includeThoughts", value)?;
        }
        if let Some(value) = &self.thinking_budget {
            object.field("thinkingBudget", value)?;
        }
        if let Some(value) = &self.thinking_level {
            object.field("thinkingLevel", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ThinkingConfigWire {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                include_thoughts: object.optional("includeThoughts", &[])?,
                thinking_budget: object.optional("thinkingBudget", &[])?,
                thinking_level: object.optional("thinkingLevel", &[])?,
            })
        })
    }
}

/// A conversation content unit. `Part` is a unified shape carrying every field a
/// request or response part may set; unused fields are omitted on serialize.
#[derive(Debug, Clone, PartialEq)]
pub struct Content {
    pub role: RuntimeString,
    pub parts: Vec<Part>,
}

impl RuntimeEncode for Content {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("role", &self.role)?;
        object.field("parts", &self.parts)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Content {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                role: object.defaulted("role", &[], RuntimeString::default)?,
                parts: object.defaulted("parts", &[], Vec::new)?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Part {
    pub text: Option<RuntimeString>,
    pub thought: Option<bool>,
    pub thought_signature: Option<RuntimeString>,
    pub inline_data: Option<InlineData>,
    pub function_call: Option<FunctionCall>,
    pub function_response: Option<FunctionResponse>,
}

impl RuntimeEncode for Part {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.text {
            object.field("text", value)?;
        }
        if let Some(value) = &self.thought {
            object.field("thought", value)?;
        }
        if let Some(value) = &self.thought_signature {
            object.field("thoughtSignature", value)?;
        }
        if let Some(value) = &self.inline_data {
            object.field("inlineData", value)?;
        }
        if let Some(value) = &self.function_call {
            object.field("functionCall", value)?;
        }
        if let Some(value) = &self.function_response {
            object.field("functionResponse", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Part {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                text: object.optional("text", &[])?,
                thought: object.optional("thought", &[])?,
                thought_signature: object.optional("thoughtSignature", &[])?,
                inline_data: object.optional("inlineData", &[])?,
                function_call: object.optional("functionCall", &[])?,
                function_response: object.optional("functionResponse", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InlineData {
    pub mime_type: RuntimeString,
    pub data: RuntimeString,
}

impl RuntimeEncode for InlineData {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mimeType", &self.mime_type)?;
        object.field("data", &self.data)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for InlineData {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mime_type: object.required("mimeType", &[])?,
                data: object.required("data", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub id: Option<RuntimeString>,
    pub name: Option<RuntimeString>,
    pub args: Option<RuntimeJson>,
}

impl RuntimeEncode for FunctionCall {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.id {
            object.field("id", value)?;
        }
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.args {
            object.field("args", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FunctionCall {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.optional("id", &[])?,
                name: object.optional("name", &[])?,
                args: object.optional("args", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionResponse {
    pub id: RuntimeString,
    pub name: RuntimeString,
    pub response: RuntimeJson,
}

impl RuntimeEncode for FunctionResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("name", &self.name)?;
        object.field("response", &self.response)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for FunctionResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                name: object.required("name", &[])?,
                response: object.required("response", &[])?,
            })
        })
    }
}

// ---- response-side wire types ----

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GenerateContentResponse {
    pub model_version: Option<RuntimeString>,
    pub candidates: Option<Vec<Candidate>>,
    pub usage_metadata: Option<UsageMetadata>,
}

impl RuntimeDecode for GenerateContentResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model_version: object.optional("modelVersion", &[])?,
                candidates: object.optional("candidates", &[])?,
                usage_metadata: object.optional("usageMetadata", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Candidate {
    pub content: Option<Content>,
    pub finish_reason: Option<RuntimeString>,
    pub grounding_metadata: Option<GroundingMetadata>,
}

impl RuntimeDecode for Candidate {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                content: object.optional("content", &[])?,
                finish_reason: object.optional("finishReason", &[])?,
                grounding_metadata: object.optional("groundingMetadata", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct UsageMetadata {
    pub prompt_token_count: Option<u64>,
    pub cached_content_token_count: Option<u64>,
    pub candidates_token_count: Option<u64>,
    pub thoughts_token_count: Option<u64>,
    pub total_token_count: Option<u64>,
}

impl RuntimeDecode for UsageMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                prompt_token_count: object.optional("promptTokenCount", &[])?,
                cached_content_token_count: object.optional("cachedContentTokenCount", &[])?,
                candidates_token_count: object.optional("candidatesTokenCount", &[])?,
                thoughts_token_count: object.optional("thoughtsTokenCount", &[])?,
                total_token_count: object.optional("totalTokenCount", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GroundingMetadata {
    pub web_search_queries: Option<Vec<RuntimeString>>,
    pub grounding_chunks: Option<Vec<GroundingChunk>>,
    pub grounding_supports: Option<Vec<GroundingSupport>>,
}

impl RuntimeDecode for GroundingMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                web_search_queries: object.optional("webSearchQueries", &[])?,
                grounding_chunks: object.optional("groundingChunks", &[])?,
                grounding_supports: object.optional("groundingSupports", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GroundingChunk {
    pub web: Option<Web>,
}

impl RuntimeDecode for GroundingChunk {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                web: object.optional("web", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Web {
    pub uri: Option<RuntimeString>,
    pub title: Option<RuntimeString>,
}

impl RuntimeDecode for Web {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri: object.optional("uri", &[])?,
                title: object.optional("title", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GroundingSupport {
    pub segment: Option<Segment>,
    pub grounding_chunk_indices: Option<Vec<usize>>,
}

impl RuntimeDecode for GroundingSupport {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                segment: object.optional("segment", &[])?,
                grounding_chunk_indices: object.optional("groundingChunkIndices", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Segment {
    pub start_index: Option<u64>,
    pub end_index: Option<u64>,
}

impl RuntimeDecode for Segment {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                start_index: object.optional("startIndex", &[])?,
                end_index: object.optional("endIndex", &[])?,
            })
        })
    }
}

// ---- Code Assist response envelope ----

#[derive(Debug, Clone, Default)]
struct CodeAssistResponse {
    response: Option<CodeAssistInner>,
}

impl RuntimeDecode for CodeAssistResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                response: object.optional("response", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Default)]
struct CodeAssistInner {
    candidates: Option<Vec<Candidate>>,
    usage_metadata: Option<UsageMetadata>,
    model_version: Option<RuntimeString>,
}

impl RuntimeDecode for CodeAssistInner {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                candidates: object.optional("candidates", &[])?,
                usage_metadata: object.optional("usageMetadata", &[])?,
                model_version: object.optional("modelVersion", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{
        DocumentBlock, TextBlock, ThinkingBlock as CThinkingBlock, ToolUseBlock,
    };
    use crate::schemas::message::{
        AssistantMessage, CompactBoundaryMessage, ToolResultMessage, UserMessage,
    };
    use crate::types::provider::{
        open_fallback_tool_schema, SystemPromptPart, ToolSpec, ToolSpecFormat, ToolSpecKind,
        WebSearchRequest,
    };
    use std::sync::Mutex;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    #[test]
    fn gemini_compact_boundary_history() {
        let profile = gemini_behavior_profile(&RuntimeString::from("gemini-2.5-pro"));
        for summary in ["resume summary", "compacted summary"] {
            let messages = vec![Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: format!("compact-{summary}").into(),
                summary: summary.into(),
            })];
            let out = to_contents_with_profile(&messages, &profile, &[], false);
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].role, "user");
            let expected =
                RuntimeString::from(format!("[Earlier conversation summarized]\n{summary}"));
            assert_eq!(out[0].parts[0].text.as_ref(), Some(&expected));
        }
    }

    // ---- test seam: a scriptable chunk source ----

    struct FakeGeminiClient {
        calls: Arc<Mutex<Vec<GenerateContentParameters>>>,
        #[allow(clippy::type_complexity)]
        responder: Box<
            dyn Fn(usize) -> Result<Vec<GenerateContentResponse>, GeminiApiError> + Send + Sync,
        >,
    }

    #[async_trait::async_trait]
    impl GeminiClient for FakeGeminiClient {
        async fn generate_content_stream(
            &self,
            params: GenerateContentParameters,
            _headers: Option<AuthHeaders>,
            _extra_headers: Vec<(String, String)>,
            _signal: AbortSignal,
        ) -> Result<GeminiChunkStream, GeminiApiError> {
            let idx = {
                let mut c = self.calls.lock().unwrap();
                c.push(params);
                c.len() - 1
            };
            match (self.responder)(idx) {
                Ok(chunks) => Ok(Box::pin(futures::stream::iter(
                    chunks.into_iter().map(Ok::<_, GeminiApiError>),
                ))),
                Err(e) => Err(e),
            }
        }
    }

    struct MidStreamConflictClient {
        calls: Arc<Mutex<Vec<GenerateContentParameters>>>,
        first: GenerateContentResponse,
        error: GeminiApiError,
    }

    #[async_trait::async_trait]
    impl GeminiClient for MidStreamConflictClient {
        async fn generate_content_stream(
            &self,
            params: GenerateContentParameters,
            _headers: Option<AuthHeaders>,
            _extra_headers: Vec<(String, String)>,
            _signal: AbortSignal,
        ) -> Result<GeminiChunkStream, GeminiApiError> {
            self.calls.lock().unwrap().push(params);
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(self.first.clone()),
                Err(self.error.clone()),
            ])))
        }
    }

    fn chunk(v: RuntimeJson) -> GenerateContentResponse {
        runtime_codec::decode_value(&v).expect("test response fixture decodes")
    }

    #[test]
    fn direct_user_pdf_maps_to_native_inline_data() {
        let contents = to_contents(&[Message::User(UserMessage {
            question_answer: None,
            uuid: "u-pdf".into(),
            content: vec![ContentBlock::Document(DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("report.pdf".into()),
            })],
            is_meta: None,
        })]);

        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].role, "user");
        assert_eq!(
            contents[0].parts[0].inline_data,
            Some(InlineData {
                mime_type: "application/pdf".into(),
                data: "JVBERi0xLjcK".into(),
            })
        );
    }

    #[test]
    fn adapter_policy_is_opt_in_for_public_vertex_and_code_assist_sources() {
        for (vertex, code_assist) in [
            (None, None),
            (
                Some(VertexConfig {
                    project: "fixture-project".into(),
                    location: "fixture-location".into(),
                }),
                None,
            ),
            (None, Some(CodeAssistOptions::default())),
        ] {
            let provider = GeminiProvider::new(GeminiProviderOptions {
                client: Some(fake_chunks(Vec::new())),
                vertex,
                code_assist,
                capabilities: Some(ModelCapabilities {
                    tools: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            });
            assert!(provider.capabilities(&"fixture".into()).tools);
            assert!(!provider.uses_model_tool_budget(&"fixture".into()));
        }
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(fake_chunks(Vec::new())),
            capabilities: Some(ModelCapabilities {
                tools: Some(false),
                ..Default::default()
            }),
            adapter_policy: GeminiAdapterPolicy {
                honor_tools_capability: true,
                model_tool_budget: true,
                ..Default::default()
            },
            ..Default::default()
        });
        assert!(!provider.capabilities(&"fixture".into()).tools);
        assert!(provider.uses_model_tool_budget(&"fixture".into()));
    }

    #[tokio::test]
    async fn adapter_policy_preserves_media_wire_and_canonical_signed_stream_parts() {
        use crate::schemas::content::ImageBlock;
        for replay_signed_parts in [false, true] {
            let fake = fake_chunks(vec![chunk(exact_json!({
                "candidates":[{"content":{"parts":[
                    {"thought":true,"text":"reason","thoughtSignature":"signed-reason"},
                    {"text":"answer","thoughtSignature":"signed-answer"}
                ]},"finishReason":"STOP"}]
            }))]);
            let provider = GeminiProvider::new(GeminiProviderOptions {
                client: Some(fake.clone()),
                capabilities: Some(ModelCapabilities {
                    vision: true,
                    native_pdf: true,
                    ..Default::default()
                }),
                adapter_policy: GeminiAdapterPolicy {
                    replay_signed_parts,
                    ..Default::default()
                },
                ..Default::default()
            });
            let mut request = base_req();
            let Message::User(user) = &mut request.messages[0] else {
                panic!("ordinary user fixture");
            };
            user.content.extend([
                ContentBlock::Image(ImageBlock {
                    detail: None,
                    mime_type: "image/png".into(),
                    base64: "aW1hZ2U=".into(),
                    source_path: None,
                }),
                ContentBlock::Document(DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "JVBERi0xLjcK".into(),
                    filename: Some("fixture.pdf".into()),
                }),
            ]);
            let events = collect(&provider, request).await;
            assert!(!events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
            assert!(events.iter().any(
                |event| matches!(event, ModelStreamEvent::TextDelta { text } if text == "answer")
            ));
            let signed: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    ModelStreamEvent::ThinkingBlock {
                        signature: Some(signature),
                        ..
                    } => Some(signature),
                    _ => None,
                })
                .collect();
            assert_eq!(signed.len(), 2);
            assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::ThinkingDelta { text } if text == "reason")));
            let calls = fake.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            let parts = &calls[0].contents[0].parts;
            assert_eq!(parts[0].text.as_ref().unwrap(), "hi");
            assert_eq!(
                parts[1].inline_data.as_ref().unwrap().mime_type,
                "image/png"
            );
            assert_eq!(parts[1].inline_data.as_ref().unwrap().data, "aW1hZ2U=");
            assert_eq!(
                parts[2].inline_data.as_ref().unwrap().mime_type,
                "application/pdf"
            );
            assert_eq!(parts[2].inline_data.as_ref().unwrap().data, "JVBERi0xLjcK");
        }
    }

    #[tokio::test]
    async fn native_audio_video_use_inline_data_and_undeclared_mime_never_calls_transport() {
        use crate::schemas::content::MediaBlock;
        let fake = fake_chunks(Vec::new());
        let mut provider = provider_with(fake.clone());
        provider.capabilities.media_input_mime_types = vec!["audio/wav".into(), "video/mp4".into()];
        let mut request = base_req();
        request.messages = vec![Message::User(UserMessage {
            uuid: "native-media".into(),
            is_meta: None,
            question_answer: None,
            content: vec![
                ContentBlock::Media(MediaBlock {
                    mime_type: "audio/wav".into(),
                    base64: "UklGRg==".into(),
                    filename: Some("voice.wav".into()),
                }),
                ContentBlock::Media(MediaBlock {
                    mime_type: "video/mp4".into(),
                    base64: "AAAAHGZ0eXA=".into(),
                    filename: None,
                }),
            ],
        })];
        let events: Vec<_> = provider
            .stream(request.clone(), AbortSignal::new())
            .collect()
            .await;
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        {
            let calls = fake.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            let parts = &calls[0].contents[0].parts;
            assert_eq!(
                parts[0].inline_data,
                Some(InlineData {
                    mime_type: "audio/wav".into(),
                    data: "UklGRg==".into()
                })
            );
            assert_eq!(
                parts[1].inline_data,
                Some(InlineData {
                    mime_type: "video/mp4".into(),
                    data: "AAAAHGZ0eXA=".into()
                })
            );
        }
        if let Message::User(user) = &mut request.messages[0] {
            if let ContentBlock::Media(media) = &mut user.content[0] {
                media.mime_type = "audio/mpeg".into();
            }
        }
        let events: Vec<_> = provider.stream(request, AbortSignal::new()).collect().await;
        assert!(
            matches!(&events[0], ModelStreamEvent::Error { error } if error.kind == ProviderErrorKind::InvalidRequest)
        );
        assert_eq!(fake.calls.lock().unwrap().len(), 1);
    }

    fn fake_chunks(chunks: Vec<GenerateContentResponse>) -> Arc<FakeGeminiClient> {
        Arc::new(FakeGeminiClient {
            calls: Arc::new(Mutex::new(Vec::new())),
            responder: Box::new(move |_| Ok(chunks.clone())),
        })
    }

    fn fake_throwing(err: GeminiApiError) -> Arc<FakeGeminiClient> {
        Arc::new(FakeGeminiClient {
            calls: Arc::new(Mutex::new(Vec::new())),
            responder: Box::new(move |_| Err(err.clone())),
        })
    }

    fn provider_with(fake: Arc<FakeGeminiClient>) -> GeminiProvider {
        let client: Arc<dyn GeminiClient> = fake;
        GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            ..Default::default()
        })
    }
    /// A provider that declares thinking support with a chosen wire dialect, so
    /// the thinking tests exercise the dialect branches without model-id lookup.
    fn provider_with_thinking(
        fake: Arc<FakeGeminiClient>,
        dialect: GeminiThinkingDialect,
    ) -> GeminiProvider {
        let client: Arc<dyn GeminiClient> = fake;
        GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            capabilities: Some(ModelCapabilities {
                thinking: Some(crate::types::provider::ThinkingCapability::default()),
                ..Default::default()
            }),
            thinking_dialect: Some(dialect),
            ..Default::default()
        })
    }

    fn provider_with_retry(fake: Arc<FakeGeminiClient>, max_attempts: u32) -> GeminiProvider {
        let client: Arc<dyn GeminiClient> = fake;
        GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            retry: Some(RetryOptions {
                max_attempts: Some(max_attempts),
                // A rate-limited or overloaded establishment deliberately keeps
                // retrying past the attempt cap for the whole congestion hold,
                // which defaults to five minutes of real time. These cases are
                // about error classification, not about the hold, and they used
                // the real clock: `classifies_establishment_error` sat there
                // sleeping for five minutes with nothing reporting it. Disable
                // the hold and the sleeps, as `retry.rs`'s own tests do.
                congestion_hold_ms: Some(0),
                sleep: Some(Arc::new(|_ms, _sig| {
                    Box::pin(async { Ok::<(), crate::provider::retry::Aborted>(()) })
                })),
                random: Some(Arc::new(|| 0.0)),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn base_req() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: "gemini-2.5-pro".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![SystemPromptPart {
                name: "core".into(),
                text: "You are helpful.".into(),
                cacheable: None,
                cache_scope: None,
            }],
            messages: vec![Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "hi".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            })],
            tools: vec![],
            tool_choice: None,
            max_output_tokens: 1024,
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
    fn gemini_initial_choice_uses_only_final_function_names() {
        let mut request = base_req();
        request.tools = vec![function_spec(
            "read",
            "Read a file.",
            schema(exact_json!({ "type": "object" })),
        )];
        request.tool_choice = Some(ToolChoice::Tool {
            name: "read".into(),
        });
        assert_eq!(validate_gemini_request_choice(&request), None);

        request.tool_choice = Some(ToolChoice::Tool {
            name: "Read".into(),
        });
        let case_error = validate_gemini_request_choice(&request)
            .expect("Gemini forced names are case-sensitive");
        assert_eq!(
            case_error.message,
            "Gemini tool choice is invalid: forced tool \"Read\" is not provider-visible; available tools: read."
        );

        request.tools.clear();
        request.web_search = Some(WebSearchRequest::default());
        request.tool_choice = Some(ToolChoice::Any);
        let any_error = validate_gemini_request_choice(&request)
            .expect("Gemini grounding is anonymous and does not satisfy any");
        assert_eq!(
            any_error.message,
            "Gemini tool choice is invalid: `any` requires at least one provider-visible tool."
        );

        request.tool_choice = Some(ToolChoice::Tool {
            name: "web_search".into(),
        });
        let forced_search_error = validate_gemini_request_choice(&request)
            .expect("Gemini native grounding has no forceable function name");
        assert_eq!(
            forced_search_error.message,
            "Gemini tool choice is invalid: forced tool \"web_search\" is not provider-visible; available tools: (none)."
        );

        request.tool_choice = Some(ToolChoice::Auto);
        assert_eq!(validate_gemini_request_choice(&request), None);
        request.tool_choice = Some(ToolChoice::None);
        assert_eq!(validate_gemini_request_choice(&request), None);
        request.tool_choice = None;
        assert_eq!(validate_gemini_request_choice(&request), None);
    }

    #[test]
    fn gemini_grounding_retry_applies_the_dedicated_choice_matrix() {
        for choice in [
            ToolChoice::Any,
            ToolChoice::Tool {
                name: "read".into(),
            },
        ] {
            let error = grounding_retry_tool_choice(Some(&choice))
                .expect_err("function-tool requirements cannot survive isolation");
            assert_eq!(
                error.kind,
                crate::types::errors::ProviderErrorKind::InvalidRequest
            );
            assert_eq!(error.status, Some(400));
            assert_eq!(error.retry_after_ms, None);
            assert_eq!(error.request_id, None);
            assert_eq!(
                error.message,
                "Gemini tool choice is invalid: native Google Search isolation cannot preserve a function-tool requirement."
            );
        }

        assert_eq!(
            grounding_retry_tool_choice(Some(&ToolChoice::Auto)),
            Ok(None),
            "auto is stripped for the isolated retry"
        );
        assert_eq!(
            grounding_retry_tool_choice(Some(&ToolChoice::None)),
            Ok(Some(ToolChoice::None)),
            "none is retained for the isolated retry"
        );
        assert_eq!(grounding_retry_tool_choice(None), Ok(None));
    }

    #[tokio::test]
    async fn gemini_invalid_choice_yields_one_error_without_transport() {
        let fake = fake_chunks(vec![]);
        let provider = provider_with(fake.clone());
        let mut request = base_req();
        request.web_search = Some(WebSearchRequest::default());
        request.tool_choice = Some(ToolChoice::Any);

        let events = collect(&provider, request).await;
        assert_eq!(fake.calls.lock().unwrap().len(), 0);
        assert_eq!(events.len(), 1);
        let ModelStreamEvent::Error { error } = &events[0] else {
            panic!("expected one Gemini validation error");
        };
        assert_eq!(
            error.kind,
            crate::types::errors::ProviderErrorKind::InvalidRequest
        );
        assert_eq!(error.status, Some(400));
        assert_eq!(
            error.message,
            "Gemini tool choice is invalid: `any` requires at least one provider-visible tool."
        );
    }

    fn schema(v: RuntimeJson) -> RuntimeJson {
        v
    }

    fn function_spec(name: &str, description: &str, input_schema: RuntimeJson) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema,
            kind: ToolSpecKind::Function,
            format: None,
        }
    }

    fn text_spec(name: &str, description: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema: open_fallback_tool_schema(),
            kind: ToolSpecKind::Freeform,
            format: Some(ToolSpecFormat::Text),
        }
    }

    #[test]
    fn estimate_tokens_fallback_matches_javascript_astral_utf16_oracle() {
        let mut request = base_req();
        request.system[0].text = "😀😀😀😀😀".into();
        request.messages.clear();

        assert_eq!(estimate_tokens_fallback(&request), 1_003);
    }

    #[test]
    fn estimate_tokens_fallback_ignores_tool_format() {
        let mut text = base_req();
        text.system.clear();
        text.messages.clear();
        text.tools = vec![text_spec("exec", "execute javascript source")];

        let mut grammar = text.clone();
        grammar.tools[0].format = Some(ToolSpecFormat::Grammar {
            syntax: crate::types::provider::ToolSpecGrammarSyntax::Regex,
            definition: ".+".into(),
        });

        assert_eq!(
            estimate_tokens_fallback(&text),
            estimate_tokens_fallback(&grammar)
        );
    }

    async fn collect(p: &GeminiProvider, req: ModelRequest) -> Vec<ModelStreamEvent> {
        let signal = AbortSignal::new();
        p.stream(req, signal).collect().await
    }

    fn cfg_of(fake: &Arc<FakeGeminiClient>) -> RuntimeJson {
        let calls = fake.calls.lock().unwrap();
        encoded_runtime(&calls[0].config)
    }

    fn contents_of(fake: &Arc<FakeGeminiClient>) -> RuntimeJson {
        let calls = fake.calls.lock().unwrap();
        encoded_runtime(&calls[0].contents)
    }

    // ---- capabilities ----

    #[test]
    fn capabilities_reflect_declared_set() {
        // Minimal declared capabilities: provider-default window, no thinking/pdf/search.
        let p = GeminiProvider::new(GeminiProviderOptions {
            api_key: Some("x".to_string()),
            ..Default::default()
        });
        let c = p.capabilities(&"any-model".into());
        assert_eq!(c.context_window, DEFAULT_CONTEXT_WINDOW);
        assert!(!c.prompt_cache_control);
        assert!(!c.thinking);
        assert!(c.structured_output);
        assert!(!c.native_pdf);
        assert!(!c.web_search);

        // Declared capabilities flow through regardless of the model id.
        let p2 = GeminiProvider::new(GeminiProviderOptions {
            api_key: Some("x".to_string()),
            capabilities: Some(ModelCapabilities {
                thinking: Some(crate::types::provider::ThinkingCapability::default()),
                web_search: true,
                native_pdf: true,
                context_window: Some(256_000),
                ..Default::default()
            }),
            ..Default::default()
        });
        let c = p2.capabilities(&"any-model".into());
        assert_eq!(c.context_window, 256_000);
        assert!(c.thinking);
        assert!(c.web_search);
        assert!(c.native_pdf);
    }

    #[tokio::test]
    async fn per_model_media_and_thinking_metadata_reach_the_selected_model_and_wire_builder() {
        use crate::types::provider::{ImageLimits, ThinkingCapability};
        use std::num::NonZeroU32;
        let fake = fake_chunks(vec![chunk(exact_json!({
            "candidates": [{"content": {"parts": [{"text": "done"}]}, "finishReason": "STOP"}]
        }))]);
        let main = ModelCapabilities {
            thinking: Some(ThinkingCapability::default()),
            image_limits: Some(ImageLimits {
                max_width: NonZeroU32::new(2_000),
                ..Default::default()
            }),
            max_media_blocks: NonZeroU32::new(600),
            ..Default::default()
        };
        let summary = ModelCapabilities {
            image_limits: Some(ImageLimits {
                max_height: NonZeroU32::new(1_200),
                target_raw_size: NonZeroU32::new(512_000),
                ..Default::default()
            }),
            max_media_blocks: NonZeroU32::new(42),
            context_window: Some(17_000),
            ..Default::default()
        };
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(fake.clone()),
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
        assert_eq!(summary_caps.context_window, 17_000);
        assert!(!summary_caps.thinking);
        let unknown = provider.capabilities(&"undeclared".into());
        assert_eq!(unknown.image_limits, None);
        assert_eq!(unknown.max_media_blocks, None);
        let mut request = base_req();
        request.model = "summary".into();
        request.thinking = Some(ThinkingConfig::Adaptive);
        request.bind_media_capabilities(&summary_caps);
        let _ = collect(&provider, request).await;
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].model, "summary");
        assert_eq!(calls[0].config.thinking_config, None);
        let wire = runtime_codec::encode_text(&calls[0]).unwrap();
        assert!(!wire.contains("imageLimits"));
        assert!(!wire.contains("maxMediaBlocks"));
    }

    #[test]
    fn web_search_follows_declared_capability() {
        let off = GeminiProvider::new(GeminiProviderOptions {
            api_key: Some("x".to_string()),
            ..Default::default()
        });
        assert!(!off.capabilities(&"any-model".into()).web_search);
        let on = GeminiProvider::new(GeminiProviderOptions {
            api_key: Some("x".to_string()),
            capabilities: Some(ModelCapabilities {
                web_search: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(on.capabilities(&"any-model".into()).web_search);
    }

    // ---- text + usage ----

    #[tokio::test]
    async fn maps_text_and_subtracts_cached_tokens() {
        let fake = fake_chunks(vec![
            chunk(exact_json!({
                "modelVersion": "gemini-2.5-pro",
                "candidates": [{ "content": { "parts": [{ "text": "Hello" }] } }]
            })),
            chunk(exact_json!({
                "candidates": [{ "content": { "parts": [{ "text": " world" }] }, "finishReason": "STOP" }],
                "usageMetadata": {
                    "promptTokenCount": 100,
                    "cachedContentTokenCount": 40,
                    "candidatesTokenCount": 20,
                    "thoughtsTokenCount": 5,
                    "totalTokenCount": 125
                }
            })),
        ]);
        let p = provider_with(fake);
        let events = collect(&p, base_req()).await;

        assert_eq!(
            events[0],
            ModelStreamEvent::MessageStart {
                model: "gemini-2.5-pro".into()
            }
        );
        let texts: Vec<RuntimeString> = events
            .iter()
            .filter_map(|e| match e {
                ModelStreamEvent::TextDelta { text } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            texts,
            vec![RuntimeString::from("Hello"), RuntimeString::from(" world")]
        );

        match events.last().unwrap() {
            ModelStreamEvent::MessageStop { stop_reason, usage } => {
                assert_eq!(*stop_reason, ModelStopReason::EndTurn);
                assert_eq!(usage.input_tokens, 60);
                assert_eq!(usage.cache_read_input_tokens, 40);
                assert_eq!(usage.cache_creation_input_tokens, 0);
                assert_eq!(usage.output_tokens, 25);
                assert_eq!(usage.total_tokens, Some(125));
            }
            _ => panic!("expected message_stop"),
        }
    }

    #[tokio::test]
    async fn synthesizes_message_start_on_empty_stream() {
        let p = provider_with(fake_chunks(vec![]));
        let events = collect(&p, base_req()).await;
        assert_eq!(
            events[0],
            ModelStreamEvent::MessageStart {
                model: "gemini-2.5-pro".into()
            }
        );
        assert!(matches!(
            events.last().unwrap(),
            ModelStreamEvent::MessageStop { .. }
        ));
    }

    // ---- thinking + tools ----

    #[tokio::test]
    async fn maps_thought_to_delta_then_block() {
        let fake = fake_chunks(vec![
            chunk(
                exact_json!({ "candidates": [{ "content": { "parts": [{ "text": "pondering", "thought": true }] } }] }),
            ),
            chunk(
                exact_json!({ "candidates": [{ "content": { "parts": [{ "text": "answer" }] }, "finishReason": "STOP" }] }),
            ),
        ]);
        let events = collect(&provider_with(fake), base_req()).await;
        assert!(events.contains(&ModelStreamEvent::ThinkingDelta {
            text: "pondering".into()
        }));
        let block = events
            .iter()
            .find(|e| matches!(e, ModelStreamEvent::ThinkingBlock { .. }))
            .unwrap();
        assert_eq!(
            block,
            &ModelStreamEvent::ThinkingBlock {
                text: "pondering".into(),
                signature: None,
                encrypted_content: None,
                is_summary: true,
            }
        );
    }

    #[tokio::test]
    async fn thinking_block_precedes_tool_use() {
        let fake = fake_chunks(vec![chunk(exact_json!({
            "candidates": [{
                "content": { "parts": [
                    { "text": "reason", "thought": true },
                    { "functionCall": { "name": "do_thing", "args": {} } }
                ] },
                "finishReason": "STOP"
            }]
        }))]);
        let events = collect(&provider_with(fake), base_req()).await;
        let block_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::ThinkingBlock { .. }))
            .unwrap();
        let tool_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::ToolUseStart { .. }))
            .unwrap();
        assert!(block_idx < tool_idx);
    }

    #[tokio::test]
    async fn tool_call_triple_and_tool_use_stop_reason() {
        let fake = fake_chunks(vec![chunk(exact_json!({
            "candidates": [{
                "content": { "parts": [{ "functionCall": { "name": "do_thing", "args": { "a": 1 } } }] },
                "finishReason": "STOP"
            }]
        }))]);
        let events = collect(&provider_with(fake), base_req()).await;
        let (id, name) = events
            .iter()
            .find_map(|e| match e {
                ModelStreamEvent::ToolUseStart { id, name, .. } => Some((id.clone(), name.clone())),
                _ => None,
            })
            .unwrap();
        assert_eq!(name, "do_thing");
        assert!(events.contains(&ModelStreamEvent::ToolUseInputDelta {
            id: id.clone(),
            partial_json: "{\"a\":1}".into(),
        }));
        assert!(events.contains(&ModelStreamEvent::ToolUseStop { id }));
        assert!(matches!(
            events.last().unwrap(),
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                ..
            }
        ));
    }

    #[test]
    fn function_call_args_use_javascript_number_ingress_and_wire_semantics() {
        let chunk = decode_generate_content(
            &RuntimeString::from(
                r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"numbers","args":{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}}}]}}]}"#,
            ),
        )
        .expect("valid Gemini payload");
        let events = ChunkMapper::new("gemini-test".into()).handle_chunk(&chunk);
        let actual = events
            .iter()
            .find_map(|event| match event {
                ModelStreamEvent::ToolUseInputDelta { id, partial_json } if id == "call_0" => {
                    Some(partial_json)
                }
                _ => None,
            })
            .expect("function call input delta");
        assert_eq!(
            actual,
            r#"{"unsafe":9007199254740992,"negativeZero":0,"overflow":null}"#
        );
    }

    #[test]
    fn code_assist_wrapper_uses_javascript_number_ingress_semantics() {
        let chunk = decode_code_assist(
            &RuntimeString::from(
                r#"{"response":{"candidates":[{"content":{"parts":[{"functionCall":{"name":"numbers","args":{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}}}]}}]}}"#,
            ),
        )
        .expect("valid Code Assist response envelope");
        let candidates = chunk.candidates.unwrap();
        let args = candidates[0].content.as_ref().unwrap().parts[0]
            .function_call
            .as_ref()
            .unwrap()
            .args
            .as_ref()
            .unwrap();

        assert_eq!(args["unsafe"], exact_json!(9007199254740992));
        let negative_zero = args["negativeZero"].as_f64().unwrap();
        assert_eq!(negative_zero, 0.0);
        assert!(negative_zero.is_sign_negative());
        assert!(args["overflow"].is_number());
        assert!(args["overflow"].as_f64().is_some_and(f64::is_infinite));
    }

    #[test]
    fn generate_content_wire_uses_javascript_json_stringify_number_projection() {
        let runtime = runtime_json::parse(&RuntimeString::from(
            r#"{"unsafe":9007199254740993,"negativeZero":-0,"overflow":1e400}"#,
        ))
        .expect("valid runtime function call input");
        let params = GenerateContentParameters {
            model: "gemini-test".into(),
            contents: vec![Content {
                role: "model".into(),
                parts: vec![Part {
                    function_call: Some(FunctionCall {
                        id: Some("call-1".into()),
                        name: Some("numbers".into()),
                        args: Some(runtime),
                    }),
                    ..Part::default()
                }],
            }],
            config: GenerateContentConfig {
                max_output_tokens: 128,
                system_instruction: None,
                tools: None,
                tool_config: None,
                response_mime_type: None,
                response_json_schema: None,
                thinking_config: None,
            },
        };

        let wire_text = NodeBufferUtf8::decode(&to_generate_content_wire(&params));
        let wire = runtime_json::parse(&wire_text).expect("exact request body parses");
        assert_eq!(
            wire["contents"][0]["parts"][0]["functionCall"]["args"],
            exact_json!({
                "unsafe": 9007199254740992,
                "negativeZero": 0,
                "overflow": null
            })
        );
    }

    #[tokio::test]
    async fn safety_finish_maps_to_refusal() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "SAFETY" }] }),
        )]);
        let events = collect(&provider_with(fake), base_req()).await;
        assert!(matches!(
            events.last().unwrap(),
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::Refusal,
                ..
            }
        ));
    }

    // ---- request mapping ----

    #[tokio::test]
    async fn maps_system_tools_toolchoice_outputschema_and_sanitizes() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.tools = vec![function_spec(
            "do_thing",
            "does",
            schema(exact_json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": false,
                "properties": { "name": { "type": "string", "pattern": "^x" } },
                "required": ["name"]
            })),
        )];
        req.tool_choice = Some(ToolChoice::Tool {
            name: "do_thing".into(),
        });
        req.output_schema = Some(schema(
            exact_json!({ "type": "object", "properties": { "ok": { "type": "boolean" } } }),
        ));
        collect(&p, req).await;

        let cfg = cfg_of(&fake);
        assert_eq!(cfg["systemInstruction"], exact_json!("You are helpful."));
        let s = &cfg["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"];
        assert!(s.get("$schema").is_none());
        assert!(s["properties"]["name"].get("pattern").is_none());
        assert_eq!(s["properties"]["name"]["type"], exact_json!("string"));
        assert_eq!(s["required"], exact_json!(["name"]));
        assert_eq!(s["additionalProperties"], exact_json!(false));
        assert_eq!(cfg["responseMimeType"], exact_json!("application/json"));
        assert_eq!(
            cfg["toolConfig"]["functionCallingConfig"]["mode"],
            exact_json!("ANY")
        );
        assert_eq!(
            cfg["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"],
            exact_json!(["do_thing"])
        );
    }

    #[tokio::test]
    async fn freeform_exec_falls_back_to_source_schema_without_renaming() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec(
                "wait",
                "wait for exec completion",
                schema(exact_json!({ "type": "object" })),
            ),
        ];

        collect(&p, req).await;
        let declarations = &cfg_of(&fake)["tools"][0]["functionDeclarations"];
        assert_eq!(declarations[0]["name"], exact_json!("exec"));
        assert_eq!(declarations[1]["name"], exact_json!("wait"));
        assert!(declarations
            .as_array()
            .expect("declarations array")
            .iter()
            .all(|declaration| declaration["name"] != exact_json!("code_exec")));
        assert_eq!(declarations[0]["name"], exact_json!("exec"));
        assert_eq!(
            declarations[0]["parametersJsonSchema"],
            exact_json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true
            })
        );
    }

    #[tokio::test]
    async fn sanitizes_tuple_items() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.tools = vec![function_spec(
            "t",
            "d",
            schema(exact_json!({
                "type": "array",
                "items": [
                    { "type": "string", "pattern": "^x" },
                    { "type": "number", "minimum": 0 }
                ]
            })),
        )];
        collect(&p, req).await;
        let items =
            &cfg_of(&fake)["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["items"];
        assert!(items[0].get("pattern").is_none());
        assert_eq!(items[0]["type"], exact_json!("string"));
        assert_eq!(items[1]["minimum"], exact_json!(0));
    }

    async fn thinking_of(
        dialect: GeminiThinkingDialect,
        thinking: Option<ThinkingConfig>,
    ) -> RuntimeJson {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with_thinking(fake.clone(), dialect);
        let mut req = base_req();
        req.thinking = thinking;
        collect(&p, req).await;
        cfg_of(&fake)
            .get("thinkingConfig")
            .cloned()
            .unwrap_or(RuntimeJson::Null)
    }

    #[tokio::test]
    async fn thinking_branches_by_dialect() {
        // Budget dialect (Gemini 2.5-pro range), no disable: adaptive -> auto
        // budget, disabled -> thoughts off with no budget field.
        const BUDGET_PRO: GeminiThinkingDialect = GeminiThinkingDialect::Budget {
            min: 128,
            max: 32_768,
            can_disable: false,
        };
        // Budget dialect that CAN disable (2.5-flash): disabled -> budget 0.
        const BUDGET_FLASH: GeminiThinkingDialect = GeminiThinkingDialect::Budget {
            min: 0,
            max: 24_576,
            can_disable: true,
        };
        assert_eq!(
            thinking_of(BUDGET_PRO, Some(ThinkingConfig::Adaptive)).await,
            exact_json!({ "includeThoughts": true, "thinkingBudget": -1 })
        );
        assert_eq!(
            thinking_of(BUDGET_PRO, Some(ThinkingConfig::Disabled)).await,
            exact_json!({ "includeThoughts": false })
        );
        let flash = thinking_of(BUDGET_FLASH, Some(ThinkingConfig::Disabled)).await;
        assert_eq!(flash["thinkingBudget"], exact_json!(0));
        let g3 = thinking_of(
            GeminiThinkingDialect::Level,
            Some(ThinkingConfig::Enabled {
                budget_tokens: None,
            }),
        )
        .await;
        assert_eq!(g3["thinkingLevel"], exact_json!("HIGH"));
        // No declared thinking capability -> no thinkingConfig at all.
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.thinking = Some(ThinkingConfig::Adaptive);
        collect(&p, req).await;
        assert_eq!(
            cfg_of(&fake)
                .get("thinkingConfig")
                .cloned()
                .unwrap_or(RuntimeJson::Null),
            RuntimeJson::Null
        );
    }

    #[tokio::test]
    async fn clamps_budget_to_range_not_maxoutput() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with_thinking(
            fake.clone(),
            GeminiThinkingDialect::Budget {
                min: 128,
                max: 32_768,
                can_disable: false,
            },
        );
        let mut req = base_req();
        req.max_output_tokens = 1024;
        req.thinking = Some(ThinkingConfig::Enabled {
            budget_tokens: Some(20_000),
        });
        collect(&p, req).await;
        assert_eq!(
            cfg_of(&fake)["thinkingConfig"]["thinkingBudget"],
            exact_json!(20000)
        );
    }

    #[tokio::test]
    async fn folds_task_budget_into_thinking_budget() {
        async fn cap(
            dialect: GeminiThinkingDialect,
            over: impl FnOnce(&mut ModelRequest),
        ) -> RuntimeJson {
            let fake = fake_chunks(vec![chunk(
                exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
            )]);
            let p = provider_with_thinking(fake.clone(), dialect);
            let mut req = base_req();
            over(&mut req);
            collect(&p, req).await;
            cfg_of(&fake)["thinkingConfig"].clone()
        }
        const BUDGET_PRO: GeminiThinkingDialect = GeminiThinkingDialect::Budget {
            min: 128,
            max: 32_768,
            can_disable: false,
        };
        let a = cap(BUDGET_PRO, |r| {
            r.thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: None,
            });
            r.task_budget = Some(5_000);
        })
        .await;
        assert_eq!(a["thinkingBudget"], exact_json!(5000));
        assert_eq!(a["includeThoughts"], exact_json!(true));

        let b = cap(BUDGET_PRO, |r| {
            r.thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: Some(10_000),
            });
            r.task_budget = Some(5_000);
        })
        .await;
        assert_eq!(b["thinkingBudget"], exact_json!(10000));

        let c = cap(BUDGET_PRO, |r| {
            r.thinking = Some(ThinkingConfig::Adaptive);
            r.task_budget = Some(5_000);
        })
        .await;
        assert_eq!(c["thinkingBudget"], exact_json!(5000));

        let d = cap(BUDGET_PRO, |r| {
            r.thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: None,
            });
            r.task_budget = Some(999_999);
        })
        .await;
        assert_eq!(d["thinkingBudget"], exact_json!(32768));

        // The level dialect has no numeric budget knob, so a cross-provider
        // task_budget has no analog: only the explicit thinking config drives it.
        let e = cap(GeminiThinkingDialect::Level, |r| {
            r.thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: None,
            });
            r.task_budget = Some(5_000);
        })
        .await;
        assert_eq!(e["thinkingLevel"], exact_json!("HIGH"));
    }

    // ---- message history mapping ----

    #[tokio::test]
    async fn maps_tool_use_and_tool_result_by_name() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "go".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                model: "gemini-2.5-pro".into(),
                usage: None,
                content: vec![
                    ContentBlock::Thinking(CThinkingBlock {
                        message_id: None,
                        text: "secret reasoning".into(),
                        signature: None,
                        encrypted_content: None,
                    }),
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "tu_1".into(),
                        name: "lookup".into(),
                        input: exact_json!({ "q": "x" }),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                ],
            }),
            Message::ToolResult(ToolResultMessage {
                uuid: "r1".into(),
                tool_use_id: "tu_1".into(),
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                content: vec![OutputBlock::Text(TextBlock {
                    text: "result-text".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                output: None,
                extra_user_blocks: None,
            }),
        ];
        collect(&p, req).await;

        let contents = contents_of(&fake);
        let model = contents
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["role"] == exact_json!("model"))
            .unwrap();
        assert_eq!(
            model["parts"],
            exact_json!([{
                "thoughtSignature": "skip_thought_signature_validator",
                "functionCall": { "id": "tu_1", "name": "lookup", "args": { "q": "x" } }
            }])
        );
        let fn_resp = contents
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|c| c["parts"].as_array().unwrap().clone())
            .find(|p| p.get("functionResponse").is_some())
            .unwrap();
        let fr = &fn_resp["functionResponse"];
        assert_eq!(fr["id"], exact_json!("tu_1"));
        assert_eq!(fr["name"], exact_json!("lookup"));
        assert_eq!(fr["response"], exact_json!({ "output": "result-text" }));
    }

    #[tokio::test]
    async fn merges_adjacent_same_role() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "a".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u2".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "b".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
        ];
        collect(&p, req).await;
        let contents = contents_of(&fake);
        assert_eq!(contents.as_array().unwrap().len(), 1);
        assert_eq!(
            contents[0]["parts"],
            exact_json!([{ "text": "a" }, { "text": "b" }])
        );
    }

    // ---- thoughtSignature capture + replay ----

    #[tokio::test]
    async fn captures_thought_signature_onto_tool_use_start() {
        let fake = fake_chunks(vec![chunk(exact_json!({
            "candidates": [{
                "content": { "parts": [
                    { "functionCall": { "name": "lookup", "args": { "q": "x" } }, "thoughtSignature": "SIG_R" }
                ] },
                "finishReason": "STOP"
            }]
        }))]);
        let events = collect(&provider_with(fake), base_req()).await;
        let start = events
            .iter()
            .find(|e| matches!(e, ModelStreamEvent::ToolUseStart { .. }))
            .unwrap();
        match start {
            ModelStreamEvent::ToolUseStart {
                name,
                thought_signature,
                ..
            } => {
                assert_eq!(name, "lookup");
                assert!(thought_signature
                    .as_ref()
                    .is_some_and(|signature| signature == "SIG_R"));
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn replays_real_signature() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "go".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                model: "gemini-2.5-pro".into(),
                usage: None,
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: "tu_1".into(),
                    name: "lookup".into(),
                    input: exact_json!({ "q": "x" }),
                    tool_kind: None,
                    thought_signature: Some("SIG_R".into()),
                })],
            }),
        ];
        collect(&p, req).await;
        let contents = contents_of(&fake);
        let model = contents
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["role"] == exact_json!("model"))
            .unwrap();
        assert_eq!(
            model["parts"],
            exact_json!([{
                "thoughtSignature": "SIG_R",
                "functionCall": { "id": "tu_1", "name": "lookup", "args": { "q": "x" } }
            }])
        );
    }

    #[tokio::test]
    async fn sentinel_on_first_unsigned_call_only() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "go".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                model: "gemini-2.5-pro".into(),
                usage: None,
                content: vec![
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "tu_1".into(),
                        name: "a".into(),
                        input: exact_json!({}),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "tu_2".into(),
                        name: "b".into(),
                        input: exact_json!({}),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                ],
            }),
        ];
        collect(&p, req).await;
        let contents = contents_of(&fake);
        let model = contents
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["role"] == exact_json!("model"))
            .unwrap();
        assert_eq!(
            model["parts"],
            exact_json!([
                {
                    "thoughtSignature": "skip_thought_signature_validator",
                    "functionCall": { "id": "tu_1", "name": "a", "args": {} }
                },
                { "functionCall": { "id": "tu_2", "name": "b", "args": {} } }
            ])
        );
    }

    // ---- grounding ----

    #[tokio::test]
    async fn cosends_google_search_with_function_declarations() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        let mut req = base_req();
        req.web_search = Some(WebSearchRequest::default());
        req.tools = vec![function_spec(
            "t",
            "d",
            schema(exact_json!({ "type": "object" })),
        )];
        collect(&p, req).await;
        let tools = cfg_of(&fake)["tools"].as_array().unwrap().clone();
        assert!(tools.iter().any(|t| t.get("googleSearch").is_some()));
        assert!(tools
            .iter()
            .any(|t| t.get("functionDeclarations").is_some()));
    }

    #[tokio::test]
    async fn omits_tools_when_no_search_no_functions() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({ "candidates": [{ "finishReason": "STOP" }] }),
        )]);
        let p = provider_with(fake.clone());
        collect(&p, base_req()).await;
        assert!(cfg_of(&fake).get("tools").is_none());
    }

    #[tokio::test]
    async fn maps_grounding_metadata_to_citations() {
        // 'cafe\u{0301}' path: 'café' is 5 UTF-8 bytes but 4 chars; byte 5 -> char 4.
        let fake = fake_chunks(vec![
            chunk(
                exact_json!({ "candidates": [{ "content": { "parts": [{ "text": "café" }] } }] }),
            ),
            chunk(exact_json!({
                "candidates": [{
                    "content": { "parts": [] },
                    "finishReason": "STOP",
                    "groundingMetadata": {
                        "webSearchQueries": ["paris weather"],
                        "groundingChunks": [{ "web": { "uri": "https://a.com", "title": "A" } }],
                        "groundingSupports": [
                            { "segment": { "startIndex": 0, "endIndex": 5 }, "groundingChunkIndices": [0] }
                        ]
                    }
                }]
            })),
        ]);
        let events = collect(&provider_with(fake), base_req()).await;
        assert!(events.contains(&ModelStreamEvent::ServerSearchQuery {
            id: "gemini-search".into(),
            query: "paris weather".into(),
            action: None,
        }));
        assert!(events.contains(&ModelStreamEvent::WebSearchResult {
            id: "gemini-search".into(),
            results: vec![NativeSearchResult {
                title: "A".into(),
                url: "https://a.com".into(),
                encrypted_token: None,
                page_age: None,
            }],
            error: None,
        }));
        assert!(events.contains(&ModelStreamEvent::Citation {
            citation: Citation {
                url: "https://a.com".into(),
                title: Some("A".into()),
                cited_text: None,
                start_index: Some(0),
                end_index: Some(4),
                encrypted_index: None,
            },
        }));
        let citation_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::Citation { .. }))
            .unwrap();
        let stop_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::MessageStop { .. }))
            .unwrap();
        assert!(citation_idx < stop_idx);
    }

    #[tokio::test]
    async fn skips_grounding_support_without_uri() {
        let fake = fake_chunks(vec![chunk(exact_json!({
            "candidates": [{
                "content": { "parts": [{ "text": "hi" }] },
                "finishReason": "STOP",
                "groundingMetadata": {
                    "webSearchQueries": ["q"],
                    "groundingChunks": [{ "web": { "title": "no-uri" } }],
                    "groundingSupports": [
                        { "segment": { "startIndex": 0, "endIndex": 2 }, "groundingChunkIndices": [0] }
                    ]
                }
            }]
        }))]);
        let events = collect(&provider_with(fake), base_req()).await;
        assert!(!events
            .iter()
            .any(|e| matches!(e, ModelStreamEvent::Citation { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, ModelStreamEvent::ServerSearchQuery { .. })));
        assert!(events.contains(&ModelStreamEvent::WebSearchResult {
            id: "gemini-search".into(),
            results: vec![],
            error: None,
        }));
    }

    // ---- runtime fallback ----

    #[tokio::test]
    async fn falls_back_to_isolated_google_search_on_conflict() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let ok = chunk(exact_json!({
            "candidates": [{ "content": { "parts": [{ "text": "ok" }] }, "finishReason": "STOP" }]
        }));
        let fake = Arc::new(FakeGeminiClient {
            calls: calls.clone(),
            responder: Box::new(move |i| {
                if i == 0 {
                    Err(GeminiApiError {
                        status: None,
                        message:
                            "google_search is not supported together with function declarations at the same time"
                                .to_string(),
                        headers: None,
                    })
                } else {
                    Ok(vec![ok.clone()])
                }
            }),
        });
        let p = provider_with(fake);
        let mut req = base_req();
        req.web_search = Some(WebSearchRequest::default());
        req.tools = vec![function_spec(
            "t",
            "d",
            schema(exact_json!({ "type": "object" })),
        )];
        let events = collect(&p, req).await;

        let captured = calls.lock().unwrap();
        assert_eq!(captured.len(), 2);
        let tools0 = encoded_runtime(&captured[0].config)["tools"].clone();
        let tools1 = encoded_runtime(&captured[1].config)["tools"].clone();
        assert!(tools0
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t.get("functionDeclarations").is_some()));
        assert!(!tools1
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t.get("functionDeclarations").is_some()));
        assert!(tools1
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t.get("googleSearch").is_some()));
        assert!(events
            .iter()
            .any(|e| matches!(e, ModelStreamEvent::TextDelta { text } if text == "ok")));
    }

    #[tokio::test]
    async fn grounding_conflict_retry_preserves_choice_matrix_on_wire() {
        for (choice, expected_mode) in [
            (None, None),
            (Some(ToolChoice::Auto), None),
            (Some(ToolChoice::None), Some("NONE")),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let ok = chunk(exact_json!({
                "candidates": [{ "content": { "parts": [{ "text": "ok" }] }, "finishReason": "STOP" }]
            }));
            let fake = Arc::new(FakeGeminiClient {
                calls: calls.clone(),
                responder: Box::new(move |index| {
                    if index == 0 {
                        Err(GeminiApiError {
                            status: Some(400),
                            message: "google_search is not supported together with function declarations at the same time".to_owned(),
                            headers: None,
                        })
                    } else {
                        Ok(vec![ok.clone()])
                    }
                }),
            });
            let provider = provider_with_retry(fake, 1);
            let mut request = base_req();
            request.web_search = Some(WebSearchRequest::default());
            request.tools = vec![function_spec(
                "read",
                "Read a file.",
                schema(exact_json!({ "type": "object" })),
            )];
            request.tool_choice = choice;

            let events = collect(&provider, request).await;
            let captured = calls.lock().unwrap();
            assert_eq!(captured.len(), 2);
            let second = &captured[1].config;
            assert!(second
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .all(|tool| tool.function_declarations.is_none()));
            assert!(second
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .any(|tool| tool.google_search.is_some()));
            assert_eq!(
                second
                    .tool_config
                    .as_ref()
                    .map(|config| config.function_calling_config.mode.clone()),
                expected_mode.map(RuntimeString::from)
            );
            assert!(events.iter().any(
                |event| matches!(event, ModelStreamEvent::TextDelta { text } if text == "ok")
            ));
        }

        for choice in [
            ToolChoice::Any,
            ToolChoice::Tool {
                name: "read".into(),
            },
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let fake = Arc::new(FakeGeminiClient {
                calls: calls.clone(),
                responder: Box::new(|_| {
                    Err(GeminiApiError {
                        status: Some(400),
                        message: "google_search is not supported together with function declarations at the same time".to_owned(),
                        headers: None,
                    })
                }),
            });
            let provider = provider_with_retry(fake, 1);
            let mut request = base_req();
            request.web_search = Some(WebSearchRequest::default());
            request.tools = vec![function_spec(
                "read",
                "Read a file.",
                schema(exact_json!({ "type": "object" })),
            )];
            request.tool_choice = Some(choice);

            let events = collect(&provider, request).await;
            assert_eq!(calls.lock().unwrap().len(), 1);
            assert_eq!(events.len(), 1);
            assert!(matches!(
                &events[0],
                ModelStreamEvent::Error { error }
                    if error.message == "Gemini tool choice is invalid: native Google Search isolation cannot preserve a function-tool requirement."
            ));
        }
    }

    #[tokio::test]
    async fn does_not_retry_grounding_conflict_after_stream_output() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let conflict_message =
            "google_search is not supported together with function declarations at the same time";
        let fake = Arc::new(MidStreamConflictClient {
            calls: calls.clone(),
            first: chunk(exact_json!({
                "modelVersion": "gemini-2.5-pro",
                "candidates": [{ "content": { "parts": [{ "text": "before conflict" }] } }]
            })),
            error: GeminiApiError {
                status: Some(400),
                message: conflict_message.to_owned(),
                headers: None,
            },
        });
        let client: Arc<dyn GeminiClient> = fake;
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut request = base_req();
        request.web_search = Some(WebSearchRequest::default());
        request.tools = vec![function_spec(
            "read",
            "Read a file.",
            schema(exact_json!({ "type": "object" })),
        )];

        let events = collect(&provider, request).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelStreamEvent::TextDelta { text } if text == "before conflict"))
                .count(),
            1
        );
        assert!(matches!(
            events.last(),
            Some(ModelStreamEvent::Error { error }) if error.message == conflict_message
        ));
    }

    #[tokio::test]
    async fn establishment_retry_surfaces_a_retry_scheduled_event() {
        // First establishment attempt fails with a retryable 503; the stream
        // must lead with a RetryScheduled event and then the retried content.
        let calls = Arc::new(Mutex::new(Vec::new()));
        let ok = chunk(exact_json!({
            "modelVersion": "gemini-2.5-pro",
            "candidates": [{ "content": { "parts": [{ "text": "after retry" }] } }]
        }));
        let fake = Arc::new(FakeGeminiClient {
            calls: calls.clone(),
            responder: Box::new(move |idx| {
                if idx == 0 {
                    Err(GeminiApiError {
                        status: Some(503),
                        message: "overloaded".to_owned(),
                        headers: None,
                    })
                } else {
                    Ok(vec![ok.clone()])
                }
            }),
        });
        let client: Arc<dyn GeminiClient> = fake;
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            retry: Some(RetryOptions {
                max_attempts: Some(2),
                sleep: Some(Arc::new(|_ms, _sig| {
                    Box::pin(async { Ok::<(), crate::provider::retry::Aborted>(()) })
                })),
                random: Some(Arc::new(|| 0.0)),
                ..Default::default()
            }),
            ..Default::default()
        });

        let events = collect(&provider, base_req()).await;

        assert_eq!(calls.lock().unwrap().len(), 2);
        assert!(matches!(
            &events[0],
            ModelStreamEvent::RetryScheduled {
                attempt: 1,
                max_attempts: 2,
                ..
            }
        ));
        assert!(events.iter().any(
            |event| matches!(event, ModelStreamEvent::TextDelta { text } if text == "after retry")
        ));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }

    #[tokio::test]
    async fn interceptor_veto_fails_fast_without_reaching_the_client_or_retrying() {
        use crate::provider::interceptor::{
            ProviderRequestDirectives, ProviderRequestInterceptor, DENIED_REQUEST_PREFIX,
        };

        struct Veto;
        impl ProviderRequestInterceptor for Veto {
            fn before_request(
                &self,
                _request: &ProviderRequestInfo<'_>,
            ) -> ProviderRequestDirectives {
                ProviderRequestDirectives {
                    extra_headers: Vec::new(),
                    deny: Some("budget exhausted".to_string()),
                }
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let fake = Arc::new(FakeGeminiClient {
            calls: calls.clone(),
            responder: Box::new(|_| Ok(Vec::new())),
        });
        let client: Arc<dyn GeminiClient> = fake;
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            retry: Some(RetryOptions {
                max_attempts: Some(3),
                sleep: Some(Arc::new(|_ms, _sig| {
                    Box::pin(async { Ok::<(), crate::provider::retry::Aborted>(()) })
                })),
                random: Some(Arc::new(|| 0.0)),
                ..Default::default()
            }),
            interceptor: Some(Arc::new(Veto)),
            ..Default::default()
        });

        let events = collect(&provider, base_req()).await;

        // The veto settled before the network and was never retried.
        assert!(calls.lock().unwrap().is_empty());
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::RetryScheduled { .. })));
        assert!(matches!(
            events.last(),
            Some(ModelStreamEvent::Error { error })
                if error.message.contains(DENIED_REQUEST_PREFIX)
                    && error.message.contains("budget exhausted")
        ));
    }

    #[tokio::test]
    async fn interceptor_headers_reach_the_gemini_client() {
        use crate::provider::interceptor::{ProviderRequestDirectives, ProviderRequestInterceptor};

        /// The headers of one request, in arrival order.
        type SeenHeaders = Vec<(String, String)>;
        struct HeaderFake {
            seen: Arc<Mutex<Vec<SeenHeaders>>>,
        }
        #[async_trait::async_trait]
        impl GeminiClient for HeaderFake {
            async fn generate_content_stream(
                &self,
                _params: GenerateContentParameters,
                _headers: Option<AuthHeaders>,
                extra_headers: Vec<(String, String)>,
                _signal: AbortSignal,
            ) -> Result<GeminiChunkStream, GeminiApiError> {
                self.seen.lock().unwrap().push(extra_headers);
                Ok(Box::pin(futures::stream::iter(Vec::<
                    Result<GenerateContentResponse, GeminiApiError>,
                >::new())))
            }
        }

        struct Tag;
        impl ProviderRequestInterceptor for Tag {
            fn before_request(
                &self,
                _request: &ProviderRequestInfo<'_>,
            ) -> ProviderRequestDirectives {
                ProviderRequestDirectives {
                    extra_headers: vec![("x-trace".to_string(), "t-1".to_string())],
                    deny: None,
                }
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let client: Arc<dyn GeminiClient> = Arc::new(HeaderFake { seen: seen.clone() });
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(client),
            interceptor: Some(Arc::new(Tag)),
            ..Default::default()
        });

        let _ = collect(&provider, base_req()).await;

        assert_eq!(
            *seen.lock().unwrap(),
            vec![vec![("x-trace".to_string(), "t-1".to_string())]]
        );
    }

    #[tokio::test]
    async fn no_fallback_on_unrelated_error() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fake = Arc::new(FakeGeminiClient {
            calls: calls.clone(),
            responder: Box::new(|_| {
                Err(GeminiApiError {
                    status: None,
                    message: "quota exceeded".to_string(),
                    headers: None,
                })
            }),
        });
        let p = provider_with_retry(fake, 1);
        let mut req = base_req();
        req.web_search = Some(WebSearchRequest::default());
        req.tools = vec![function_spec(
            "t",
            "d",
            schema(exact_json!({ "type": "object" })),
        )];
        let events = collect(&p, req).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert!(matches!(
            events.last().unwrap(),
            ModelStreamEvent::Error { .. }
        ));
    }

    // ---- errors + abort ----

    #[tokio::test]
    async fn classifies_establishment_error() {
        let fake = fake_throwing(GeminiApiError {
            status: Some(429),
            message: "rate limited".to_string(),
            headers: None,
        });
        let p = provider_with_retry(fake, 1);
        let events = collect(&p, base_req()).await;
        match events.last().unwrap() {
            ModelStreamEvent::Error { error } => {
                assert_eq!(
                    error.kind,
                    crate::types::errors::ProviderErrorKind::RateLimit
                );
            }
            _ => panic!("expected error event"),
        }
    }

    #[tokio::test]
    async fn emits_nothing_when_signal_aborted() {
        let fake = fake_throwing(GeminiApiError {
            status: None,
            message: "boom".to_string(),
            headers: None,
        });
        let p = provider_with(fake);
        let signal = AbortSignal::new();
        signal.cancel();
        let events: Vec<ModelStreamEvent> = p.stream(base_req(), signal).collect().await;
        assert!(events.is_empty());
    }

    // ---- pure-function unit tests ----

    #[test]
    fn sse_decoder_splits_and_flushes() {
        let mut d = SseDecoder::new();
        let mut out = d.push(b"data: {\"a\":1}\n\ndata: {\"b\":2}\n\n");
        out.extend(d.finish());
        assert_eq!(
            out,
            vec![
                RuntimeString::from("{\"a\":1}"),
                RuntimeString::from("{\"b\":2}"),
            ]
        );
    }

    #[test]
    fn sse_decoder_handles_split_chunks() {
        let mut d = SseDecoder::new();
        assert!(d.push(b"data: {\"a\"").is_empty());
        assert!(d.push(b":1}").is_empty());
        let out = d.push(b"\n\n");
        assert_eq!(out, vec![RuntimeString::from("{\"a\":1}")]);
    }

    #[test]
    fn sse_decoder_matches_streaming_text_decoder_and_js_trim() {
        let mut d = SseDecoder::new();
        assert!(d
            .push(b"\xef\xbb\xbfdata: \xc2\xa0{\"text\":\"caf\xc3")
            .is_empty());
        let out = d.push(b"\xa9\"}\xc2\xa0\n\n");
        assert_eq!(out, vec![RuntimeString::from("{\"text\":\"caf\u{e9}\"}")]);

        let out = d.push(b"data: {\"text\":\"a\xffb\"}\n\n");
        assert_eq!(out, vec![RuntimeString::from("{\"text\":\"a\u{fffd}b\"}")]);
    }

    #[test]
    fn byte_to_char_index_maps_utf8_bytes() {
        let cafe = RuntimeString::from("café");
        assert_eq!(byte_to_char_index(&cafe, 5), 4);
        assert_eq!(byte_to_char_index(&cafe, 0), 0);
        assert_eq!(byte_to_char_index(&RuntimeString::from("hello"), 3), 3);
    }

    #[test]
    fn byte_to_char_index_projects_lone_surrogates_through_node_utf8() {
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800😀""#)).unwrap();
        let text = parsed.as_str().unwrap();

        // Buffer.from("\ud800😀", "utf8") is 3 + 4 bytes. JavaScript
        // then reports three UTF-16 code units for the decoded full prefix.
        assert_eq!(byte_to_char_index(text, 7), 3);
        // Cutting one byte into an astral UTF-8 sequence decodes that prefix as
        // U+FFFD, so the JS `.length` is the lone-surrogate replacement plus one.
        assert_eq!(byte_to_char_index(text, 4), 2);
    }

    #[test]
    fn chunk_mapper_orders_thinking_before_tool_use() {
        let mut m = ChunkMapper::new("fallback".into());
        let mut events = m.handle_chunk(&chunk(exact_json!({
            "candidates": [{
                "content": { "parts": [
                    { "text": "reason", "thought": true },
                    { "functionCall": { "name": "go", "args": {} } }
                ] },
                "finishReason": "STOP"
            }]
        })));
        events.extend(m.finish());
        let block_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::ThinkingBlock { .. }))
            .unwrap();
        let tool_idx = events
            .iter()
            .position(|e| matches!(e, ModelStreamEvent::ToolUseStart { .. }))
            .unwrap();
        assert!(block_idx < tool_idx);
        assert!(matches!(
            events.last().unwrap(),
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                ..
            }
        ));
    }

    #[test]
    fn signature_only_terminal_parts_survive_storage_and_replay() {
        let mut mapper = ChunkMapper::new("fixture".into());
        let events = mapper.handle_chunk(&chunk(exact_json!({
            "candidates":[{"finishReason":"STOP","content":{"role":"model","parts":[
                {"text":"","thoughtSignature":"opaque-signature"}
            ]}}]
        })));
        let signature = events
            .iter()
            .find_map(|event| match event {
                ModelStreamEvent::ThinkingBlock { signature, .. } => signature.clone(),
                _ => None,
            })
            .expect("empty signature part must be retained");
        let parts = blocks_to_parts(
            &[ContentBlock::Thinking(
                crate::schemas::content::ThinkingBlock {
                    text: RuntimeString::default(),
                    message_id: None,
                    signature: Some(signature),
                    encrypted_content: None,
                },
            )],
            &[],
            true,
        );
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0].thought_signature.as_ref().unwrap(),
            "opaque-signature"
        );
        assert_eq!(parts[0].text.as_ref().unwrap(), "");
        assert_eq!(parts[0].thought, None);
    }

    #[test]
    fn signed_visible_and_thought_parts_keep_their_original_association() {
        let mut mapper = ChunkMapper::new("fixture".into());
        let events=mapper.handle_chunk(&chunk(exact_json!({"candidates":[{"content":{"parts":[
            {"text":"prefix "},{"text":"answer","thoughtSignature":"text-signature"},{"text":" tail"}
        ]},"finishReason":"STOP"}]})));
        let marker = events
            .into_iter()
            .find_map(|event| match event {
                ModelStreamEvent::ThinkingBlock {
                    signature: Some(signature),
                    ..
                } => Some(signature),
                _ => None,
            })
            .unwrap();
        let mut replay = vec![Part {
            text: Some("prefix answer tail".into()),
            ..Default::default()
        }];
        replay_retained_parts(&mut replay, &marker);
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0].text.as_ref().unwrap(), "prefix ");
        assert_eq!(replay[1].text.as_ref().unwrap(), "answer");
        assert_eq!(
            replay[1].thought_signature.as_ref().unwrap(),
            "text-signature"
        );
        assert_eq!(replay[2].text.as_ref().unwrap(), " tail");
        let mut mapper = ChunkMapper::new("fixture".into());
        let events=mapper.handle_chunk(&chunk(exact_json!({"candidates":[{"content":{"parts":[
            {"thought":true,"text":"first thought"},{"thought":true,"text":"signed thought","thoughtSignature":"thought-signature"},{"text":"answer"}
        ]},"finishReason":"STOP"}]})));
        let marker = events
            .into_iter()
            .find_map(|event| match event {
                ModelStreamEvent::ThinkingBlock {
                    signature: Some(signature),
                    ..
                } => Some(signature),
                _ => None,
            })
            .unwrap();
        let mut replay = Vec::new();
        replay_retained_parts(&mut replay, &marker);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].thought, Some(true));
        assert_eq!(replay[0].text.as_ref().unwrap(), "first thought");
        assert_eq!(replay[1].thought, Some(true));
        assert_eq!(replay[1].text.as_ref().unwrap(), "signed thought");
        assert_eq!(
            replay[1].thought_signature.as_ref().unwrap(),
            "thought-signature"
        );
        let mut mapper = ChunkMapper::new("fixture".into());
        let events = mapper.handle_chunk(&chunk(exact_json!({"candidates":[{"content":{"parts":[
            {"thought":true,"text":"","thoughtSignature":"empty-thought"},{"text":"answer"}
        ]},"finishReason":"STOP"}]})));
        let thought_index = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ModelStreamEvent::ThinkingBlock {
                        signature: Some(_),
                        ..
                    }
                )
            })
            .unwrap();
        let text_index = events
            .iter()
            .position(|event| matches!(event, ModelStreamEvent::TextDelta { .. }))
            .unwrap();
        assert!(thought_index < text_index);
    }

    #[tokio::test]
    async fn credential_admission_rechecks_events_buffered_from_one_chunk() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let admitted = Arc::new(AtomicBool::new(true));
        let check = admitted.clone();
        let fake = fake_chunks(vec![chunk(
            exact_json!({"candidates":[{"content":{"parts":[{"text":"buffered answer"}]},"finishReason":"STOP"}]}),
        )]);
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(fake),
            stream_admission: Some(Arc::new(move || check.load(Ordering::SeqCst))),
            ..Default::default()
        });
        let mut stream = provider.stream(base_req(), AbortSignal::new());
        assert!(matches!(
            stream.next().await,
            Some(ModelStreamEvent::MessageStart { .. })
        ));
        admitted.store(false, Ordering::SeqCst);
        assert!(
            matches!(stream.next().await,Some(ModelStreamEvent::Error {error}) if error.code.as_ref().is_some_and(|code|code=="provider_credential_changed"))
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn per_model_thinking_dialects_keep_mixed_cloud_models_independent() {
        let fake = fake_chunks(vec![chunk(
            exact_json!({"candidates":[{"finishReason":"STOP"}]}),
        )]);
        let thinking = ModelCapabilities {
            thinking: Some(crate::types::provider::ThinkingCapability::default()),
            ..Default::default()
        };
        let provider = GeminiProvider::new(GeminiProviderOptions {
            client: Some(fake.clone()),
            capabilities: Some(thinking.clone()),
            capabilities_by_model: Some(HashMap::from([("budget-model".into(), thinking)])),
            thinking_dialect: Some(GeminiThinkingDialect::Level),
            thinking_dialects_by_model: Some(HashMap::from([(
                "budget-model".into(),
                GeminiThinkingDialect::Budget {
                    min: 32,
                    max: 32768,
                    can_disable: false,
                },
            )])),
            ..Default::default()
        });
        let mut request = base_req();
        request.thinking = Some(ThinkingConfig::Adaptive);
        request.model = "budget-model".into();
        collect(&provider, request).await;
        let calls = fake.calls.lock().unwrap();
        assert_eq!(
            calls[0]
                .config
                .thinking_config
                .as_ref()
                .unwrap()
                .thinking_budget,
            Some(-1)
        );
        assert_eq!(
            calls[0]
                .config
                .thinking_config
                .as_ref()
                .unwrap()
                .thinking_level,
            None
        );
    }

    #[test]
    fn code_assist_envelope_shapes_request() {
        let params = build_gemini_request(
            &base_req(),
            &ModelCapabilities::default(),
            GeminiThinkingDialect::default(),
        );
        let env = to_code_assist_envelope(
            &params,
            Some("proj"),
            Some("sess"),
            &RuntimeString::from("pid"),
        );
        assert_eq!(env["model"], exact_json!("gemini-2.5-pro"));
        assert_eq!(env["project"], exact_json!("proj"));
        assert_eq!(env["request"]["session_id"], exact_json!("sess"));
        assert_eq!(
            env["request"]["systemInstruction"],
            exact_json!({ "parts": [{ "text": "You are helpful." }] })
        );
        assert_eq!(
            env["request"]["generationConfig"]["maxOutputTokens"],
            exact_json!(1024)
        );
    }
    #[tokio::test]
    async fn native_effort_reaches_declared_gemini_level_and_budget_transports() {
        use crate::types::provider::{EffortCapability, ThinkingCapability};
        let levels = [
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ];
        for (index, level) in levels.into_iter().enumerate() {
            for dialect in [
                GeminiThinkingDialect::Level,
                GeminiThinkingDialect::Budget {
                    min: 128,
                    max: 32896,
                    can_disable: false,
                },
            ] {
                let fake = fake_chunks(vec![chunk(
                    exact_json!({"candidates": [{"finishReason": "STOP"}]}),
                )]);
                let provider = GeminiProvider::new(GeminiProviderOptions {
                    client: Some(fake.clone()),
                    thinking_dialect: Some(dialect),
                    capabilities: Some(ModelCapabilities {
                        thinking: Some(ThinkingCapability::default()),
                        effort: Some(EffortCapability {
                            levels: levels.to_vec(),
                            default: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                let mut request = base_req();
                request.effort = Some(EffortValue::Level(level));
                collect(&provider, request).await;
                let config = cfg_of(&fake);
                let thinking = &config["thinkingConfig"];
                match dialect {
                    GeminiThinkingDialect::Level => assert_eq!(
                        thinking["thinkingLevel"],
                        RuntimeJson::String(RuntimeString::from(if index < 2 {
                            "LOW"
                        } else {
                            "HIGH"
                        }))
                    ),
                    GeminiThinkingDialect::Budget { .. } => assert_eq!(
                        thinking["thinkingBudget"],
                        RuntimeJson::from(128_u64 + 8192 * index as u64)
                    ),
                }
            }
        }
    }

    #[test]
    fn native_gemini_effort_requires_a_declaration_and_preserves_explicit_budgets() {
        use crate::types::provider::{EffortCapability, ThinkingCapability};
        let mut caps = ModelCapabilities {
            thinking: Some(ThinkingCapability::default()),
            ..Default::default()
        };
        let mut request = base_req();
        request.effort = Some(EffortValue::Level(EffortLevel::Max));
        assert!(
            build_gemini_request(&request, &caps, GeminiThinkingDialect::Level)
                .config
                .thinking_config
                .is_none()
        );
        caps.effort = Some(EffortCapability {
            levels: vec![EffortLevel::Low],
            default: None,
        });
        let config = build_gemini_request(&request, &caps, GeminiThinkingDialect::Level);
        assert_eq!(
            config.config.thinking_config.unwrap().thinking_level,
            Some("LOW".into())
        );
        request.thinking = Some(ThinkingConfig::Enabled {
            budget_tokens: Some(2000),
        });
        let config = build_gemini_request(
            &request,
            &caps,
            GeminiThinkingDialect::Budget {
                min: 128,
                max: 32000,
                can_disable: false,
            },
        );
        assert_eq!(
            config.config.thinking_config.unwrap().thinking_budget,
            Some(2000)
        );
    }
}
