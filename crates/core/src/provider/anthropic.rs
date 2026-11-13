//! Anthropic Messages provider: streams a turn over HTTPS + Server-Sent Events.
//!
//! The request is a plain `POST` to `{base}/v1/messages` with `stream:true`, and
//! the SSE response is decoded by hand: bytes are buffered, complete events are
//! split on a blank line, and each event's `data:` payload is JSON-parsed and
//! mapped onto [`ModelStreamEvent`].
//!
//! The raw-event → normalized-event mapping is factored into the pure
//! [`handle_event`] function (threading a small per-block bookkeeping map), so it
//! can be unit-tested with canned bytes without any network.

use crate::context::context_hints::{
    invalidate_cleared_reads, project_hint_edits, ContextHintController, ContextHintProjection,
    HintErrorAction, CONTEXT_HINT_BETA, CONTEXT_HINT_STREAM_FALLBACK_CODE,
};
use crate::context::images::ImageLimits;
use crate::context::media_budget::{
    parse_media_rejection, trim_media, validate_final_images, ImagePayloadError, MediaBudget,
    MediaRepairState,
};
use crate::provider::auth::{ApiKeyScheme, AuthSource};
use crate::provider::aws_eventstream::AwsEventStreamDecoder;
use crate::provider::behavior::{
    project_anthropic_tool_input_schema, resolve_provider_behavior_profile,
    ProviderBehaviorProfile, ProviderPromptStyle,
};
#[cfg(test)]
use crate::provider::behavior::{render_provider_text_envelope, ProviderTextEnvelopeKind};
pub use crate::provider::classify::classify_error;
use crate::provider::classify::RawProviderError;
use crate::provider::interceptor::{
    admit_attempt, report_attempt, ProviderRequestInfo, SharedRequestInterceptor,
};
use crate::provider::output_reservation::reduce_output_reservation;
use crate::provider::prewarm::{endpoint_origin, probe, PrewarmOutcome};
use crate::provider::retry::{
    establishment_steps, observe_retries, with_retry, EstablishmentStep, RetryError, RetryOptions,
};
use crate::provider::sigv4::{AwsCredentials, SigV4Request};
use crate::provider::util::{
    diagnostic_string, objectified_tool_input, raw_from_provider_error, retry_err_to_provider,
};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson as Value, RuntimeObject as Map, RuntimeString,
    WhatwgUtf8,
};
use crate::schemas::config::ClearToolInputs;
use crate::schemas::config::ContextManagementConfig;
use crate::schemas::content::{
    Citation, ContentBlock, DocumentBlock, NativeSearchResult, OutputBlock,
};
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::schemas::message::{Message, ToolResultMessage, Usage};
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::{
    CacheScope, CacheTtl, ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason,
    ModelStream, ModelStreamEvent, ProviderCapabilities, ProviderInfo, SystemPromptPart,
    ThinkingConfig, ThinkingContextPolicy, ToolChoice, ToolSpec, WebSearchRequest,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

const DEFAULT_CONTEXT_WINDOW: u64 = 200_000;
const DEFAULT_MAX_OUTPUT: u64 = 8_192;
/// Name of the synthetic tool used to emulate native structured output.
const STRUCTURED_OUTPUT_TOOL: &str = "emit_result";

/// claude.ai OAuth subscription gates the premium models (opus/sonnet) behind an
/// anti-abuse spoof check: the request's FIRST system block must be exactly this
/// string, verbatim, as its own block (extra content goes in later blocks).
/// Without it those models return a misleading `rate_limit_error` 429; haiku is
/// exempt. This is the contract claude.ai enforces so subscription tokens are only
/// spent by a genuine Claude Code client, so the OAuth path must lead with it
/// (api-key / cloud-provider requests must NOT).
const CLAUDE_CODE_SYSTEM_IDENTITY: &str =
    "You are Claude Code, Anthropic's official CLI for Claude.";

/// The API beta identifier that activates the reasoning-effort feature (a
/// wire-protocol identifier the API requires — an interop contract).
const EFFORT_BETA: &str = "effort-2025-11-24";
const FAST_MODE_BETA: &str = "fast-mode-2026-02-01";
/// Interleaved-thinking beta: lets the model interleave thinking with tool calls
/// and replay signed thinking across turns. Auto-attached for capable models.
const INTERLEAVED_THINKING_BETA: &str = "interleaved-thinking-2025-05-14";
/// Cross-session prompt-cache scope beta: lets a cache breakpoint carry
/// `scope:'global'`, sharing the cached prefix across this org's sessions.
/// First-party only; a no-op unless a breakpoint sets the `scope` field.
const CACHE_SCOPE_BETA: &str = "prompt-caching-scope-2026-01-05";
/// Native structured-outputs beta: enables `output_config.format`, constraining
/// the model's direct output to a schema via grammar sampling (composes with
/// thinking). First-party + supported-model only.
const STRUCTURED_OUTPUTS_BETA: &str = "structured-outputs-2025-12-15";
/// 1M-context beta: unlocks the 1,000,000-token window on models that support it.
const CONTEXT_1M_BETA: &str = "context-1m-2025-08-07";
/// Task-budgets beta: `output_config.task_budget` tells the model how many output
/// tokens it has for the whole agentic loop. First-party + supported-model only.
const TASK_BUDGETS_BETA: &str = "task-budgets-2026-03-13";
const TASK_BUDGET_MIN: u64 = 20_000;
/// Context-management beta: `context_management.edits` asks the server to clear
/// aged tool blocks in-flight (API-level context editing). First-party only.
const CONTEXT_MANAGEMENT_BETA: &str = "context-management-2025-06-27";
/// The single edit type carried by the context-management beta.
const CLEAR_TOOL_USES_EDIT: &str = "clear_tool_uses_20250919";
/// The context-management edit type governing PRIOR-turn thinking blocks.
const CLEAR_THINKING_EDIT: &str = "clear_thinking_20251015";
/// The API allows at most 4 cache_control breakpoints per request.
const MAX_CACHE_BREAKPOINTS: usize = 4;
/// The pinned wire API version sent on every request.
const ANTHROPIC_VERSION: &str = "2023-06-01";
/// The API version pinned in the body when routing through Vertex AI.
const VERTEX_ANTHROPIC_VERSION: &str = "vertex-2023-10-16";
/// The API version pinned in the body when routing through AWS Bedrock.
const BEDROCK_ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";
/// Default endpoint when no base URL / gateway is configured.
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

fn encode_anthropic_request_body(body: &Value) -> Vec<u8> {
    let mut encoded = Vec::new();
    runtime_json::write_to(body, &mut encoded).expect("writing exact JSON to memory cannot fail");
    encoded
}

fn with_anthropic_json_body(
    builder: reqwest::RequestBuilder,
    body: &Value,
) -> reqwest::RequestBuilder {
    builder
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(encode_anthropic_request_body(body))
}

fn decode_anthropic_response(body: &str) -> Option<Value> {
    runtime_json::parse(&RuntimeString::from(body)).ok()
}

fn validate_anthropic_request_choice(
    request: &ModelRequest,
    native_web_search_enabled: bool,
    forced_output_fallback: bool,
) -> Option<ProviderError> {
    if forced_output_fallback {
        return None;
    }
    let mut names: Vec<RuntimeString> =
        request.tools.iter().map(|tool| tool.name.clone()).collect();
    if native_web_search_enabled {
        names.push(RuntimeString::from("web_search"));
    }
    let choice = match request.tool_choice.as_ref() {
        None | Some(ToolChoice::Auto | ToolChoice::None) => return None,
        Some(choice) => choice,
    };
    let reason = match choice {
        ToolChoice::Any if names.is_empty() => {
            "`any` requires at least one provider-visible tool".to_owned()
        }
        ToolChoice::Tool { name } if !names.iter().any(|candidate| candidate == name) => {
            let available = if names.is_empty() {
                "(none)".to_owned()
            } else {
                names
                    .iter()
                    .map(diagnostic_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!(
                "forced tool \"{}\" is not provider-visible; available tools: {available}",
                diagnostic_string(name)
            )
        }
        ToolChoice::Auto | ToolChoice::Any | ToolChoice::Tool { .. } | ToolChoice::None => {
            return None;
        }
    };
    Some(ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: format!("Anthropic tool choice is invalid: {reason}.").into(),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    })
}

/// Construction options for [`AnthropicProvider`].
#[derive(Default)]
pub struct AnthropicProviderOptions {
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub default_model: Option<String>,
    /// Explicit subscription Fast intent. Applied only to first-party requests
    /// whose exact model declares Fast; auxiliary models retain standard service.
    pub fast_mode: bool,
    /// Override request-establishment retry policy.
    pub retry: Option<RetryOptions>,
    /// Subscription/OAuth auth — overrides `api_key`, supplies per-request headers.
    pub auth_source: Option<Arc<dyn AuthSource>>,
    /// How `api_key` is spent on the wire: `x-api-key` (the Anthropic-native
    /// default) or an `Authorization: Bearer` carrier. Gateways that front the
    /// Anthropic Messages surface behind an OAuth-style key accept only the
    /// bearer scheme, and reject `x-api-key` outright. Auth-source traffic
    /// (subscriptions) never reads this field.
    pub api_key_scheme: ApiKeyScheme,
    /// Additional static HTTP headers included in every request as
    /// (header name, value) pairs. Auth and beta headers take precedence on
    /// key conflicts.
    pub http_headers: Option<HashMap<String, String>>,
    /// Additional HTTP headers whose values are resolved from environment
    /// variables at request time: (header name, environment variable name).
    /// A header is skipped when its variable is unset or empty.
    pub env_http_headers: Option<HashMap<String, String>>,
    /// Ignore environment endpoint/kill-switch overrides and freeze the
    /// first-party contract. Protected Eval runs use this so asynchronous
    /// identity preflight cannot change provider request behavior.
    pub protected_first_party_contract: bool,
    /// Transport-level request interception (per-attempt headers / veto /
    /// settled-outcome observation).
    pub interceptor: Option<SharedRequestInterceptor>,
    /// Route Claude requests through Google Vertex AI instead of the first-party
    /// Messages API. When set, requests target the Vertex publisher endpoint,
    /// carry `anthropic_version: "vertex-2023-10-16"` in the body (the model is
    /// in the URL), and authenticate via `auth_source` (a GCP bearer) rather than
    /// an `x-api-key` header.
    pub vertex: Option<VertexConfig>,
    /// Route Claude requests through AWS Bedrock. Requests are SigV4-signed to
    /// the `bedrock-runtime` endpoint, carry `anthropic_version:
    /// "bedrock-2023-05-31"` in the body (the model is in the URL), and the
    /// response is decoded from the AWS event-stream framing.
    pub bedrock: Option<BedrockConfig>,
    /// Request-body `metadata.user_id`. claude.ai uses it for abuse / rate-limit
    /// bucketing on OAuth subscription traffic; the host supplies the same opaque
    /// id the official Claude Code client sends. Emitted on the first-party
    /// Messages request only (never for api-key / Vertex / Bedrock).
    pub metadata_user_id: Option<String>,
    /// The model's capability set, declared by the client at session init. The
    /// provider reads THIS instead of inferring anything from the model id; an
    /// absent set (the default) means the model supports nothing beyond plain
    /// streaming/tools.
    pub capabilities: Option<ModelCapabilities>,
    /// Per-model capability overrides keyed by exact model id, for sessions
    /// whose tier map runs auxiliary work on models other than the main one
    /// (e.g. low-tier background calls on a model with no effort knob). A model
    /// absent here reads the session-level `capabilities`.
    pub capabilities_by_model: Option<HashMap<String, ModelCapabilities>>,
    /// Source-default per-image constraints, resolved by the host's model source.
    pub image_limits: Option<ImageLimits>,
    /// Exact-model image constraints; fall back to the source default above.
    pub image_limits_by_model: Option<HashMap<String, ImageLimits>>,
    /// Explicit source media-count declaration; absence is conservative (100).
    pub max_media_blocks: Option<usize>,
    /// Exact-model media-count declarations, independent from token windows.
    pub max_media_blocks_by_model: Option<HashMap<String, usize>>,
}

/// AWS Bedrock routing for Claude models. Credentials are resolved per request
/// from the standard AWS environment variables.
#[derive(Debug, Clone)]
pub struct BedrockConfig {
    /// AWS region, e.g. `us-east-1`. Composes the `bedrock-runtime` host.
    pub region: String,
}

impl BedrockConfig {
    /// Resolve Bedrock routing from `AWS_REGION` / `AWS_DEFAULT_REGION`. Returns
    /// `None` when no region is configured; credentials are resolved separately
    /// per request so rotated/temporary credentials are picked up.
    pub fn from_env() -> Option<Self> {
        let region = std::env::var("AWS_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        Some(Self { region })
    }
}

/// Google Vertex AI routing for Claude models.
#[derive(Debug, Clone)]
pub struct VertexConfig {
    pub project: String,
    pub location: String,
}

impl VertexConfig {
    /// Build a Vertex routing config from the environment, mirroring the standard
    /// Claude-on-Vertex variables: `ANTHROPIC_VERTEX_PROJECT_ID` (required) and
    /// `CLOUD_ML_REGION` (the location; defaults to `global` when unset).
    /// Returns `None` when no project id is configured.
    pub fn from_env() -> Option<Self> {
        let project = std::env::var("ANTHROPIC_VERTEX_PROJECT_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let location = std::env::var("CLOUD_ML_REGION")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "global".to_string());
        Some(Self { project, location })
    }

    /// The Vertex publisher host: the `global` location uses the un-prefixed
    /// host; every other region prefixes the host with the region name.
    fn host(&self) -> String {
        if self.location == "global" {
            "aiplatform.googleapis.com".to_string()
        } else {
            format!("{}-aiplatform.googleapis.com", self.location)
        }
    }

    /// The Vertex `:streamRawPredict` endpoint for a given publisher model.
    /// The `global` location uses the un-prefixed host; every other region
    /// prefixes the host with the region name.
    fn stream_url(&self, model: &str) -> String {
        let host = self.host();
        format!(
            "https://{host}/v1/projects/{}/locations/{}/publishers/anthropic/models/{model}:streamRawPredict",
            self.project, self.location
        )
    }
}

/// Azure AI Foundry routing for Claude. Foundry exposes the STANDARD Anthropic
/// Messages surface on a custom host (`{resource}.services.ai.azure.com/anthropic`),
/// so it needs no wire changes beyond a base URL and API key — this only resolves
/// them from the environment for the server entry points.
pub struct FoundryRouting {
    pub base_url: String,
    pub api_key: Option<String>,
}

/// Compose the Foundry base URL from its two configuration sources.
///
/// `ANTHROPIC_FOUNDRY_BASE_URL` wins when present; a trailing `/v1` is trimmed
/// because the provider re-appends `/v1/messages`. Otherwise the host is composed
/// from `ANTHROPIC_FOUNDRY_RESOURCE`. Returns `None` when neither is set.
fn foundry_base_url(base_url_env: Option<&str>, resource_env: Option<&str>) -> Option<String> {
    let normalized_base = base_url_env
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/'))
        .map(|s| {
            s.strip_suffix("/v1")
                .unwrap_or(s)
                .trim_end_matches('/')
                .to_string()
        });
    if let Some(base) = normalized_base {
        return Some(base);
    }
    resource_env
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|resource| format!("https://{resource}.services.ai.azure.com/anthropic"))
}

impl FoundryRouting {
    /// Resolve Foundry routing from `ANTHROPIC_FOUNDRY_BASE_URL` /
    /// `ANTHROPIC_FOUNDRY_RESOURCE` (+ `ANTHROPIC_FOUNDRY_API_KEY`). `None` when
    /// no Foundry host is configured.
    pub fn from_env() -> Option<Self> {
        let base_url = foundry_base_url(
            std::env::var("ANTHROPIC_FOUNDRY_BASE_URL").ok().as_deref(),
            std::env::var("ANTHROPIC_FOUNDRY_RESOURCE").ok().as_deref(),
        )?;
        let api_key = std::env::var("ANTHROPIC_FOUNDRY_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Some(Self { base_url, api_key })
    }
}

#[derive(Clone, Copy)]
struct AnthropicRuntimeEnvironment {
    native_structured_output_disabled: bool,
    global_cache_scope_disabled: bool,
    interleaved_thinking_disabled: bool,
    context_1m_disabled: bool,
    context_management_disabled: bool,
}

impl AnthropicRuntimeEnvironment {
    fn protected_first_party_contract() -> Self {
        Self {
            native_structured_output_disabled: false,
            global_cache_scope_disabled: false,
            interleaved_thinking_disabled: false,
            context_1m_disabled: false,
            context_management_disabled: false,
        }
    }
}

#[derive(Clone, Copy)]
enum AnthropicFeatureFlag {
    NativeStructuredOutput,
    GlobalCacheScope,
    InterleavedThinking,
    Context1m,
    ContextManagement,
}

/// The Anthropic Messages provider.
#[derive(Clone)]
pub struct AnthropicProvider {
    info: ProviderInfo,
    client: reqwest::Client,
    api_key: Option<String>,
    auth_source: Option<Arc<dyn AuthSource>>,
    api_key_scheme: ApiKeyScheme,
    http_headers: HashMap<String, String>,
    env_http_headers: HashMap<String, String>,
    retry: RetryOptions,
    review_tool_loop: bool,
    /// True when talking to the first-party endpoint (no custom base URL/gateway).
    /// Cross-session cache `scope:'global'` and its beta are first-party only.
    is_first_party: bool,
    direct_media_endpoint: bool,
    image_limits: ImageLimits,
    image_limits_by_model: HashMap<String, ImageLimits>,
    max_media_blocks: Option<usize>,
    max_media_blocks_by_model: HashMap<String, usize>,
    messages_url: String,
    count_tokens_url: String,
    frozen_runtime_environment: Option<AnthropicRuntimeEnvironment>,
    /// Vertex AI routing, when configured. Requests build a per-model URL and
    /// omit the top-level `model`/`anthropic-version` header in favor of the
    /// Vertex body field.
    vertex: Option<VertexConfig>,
    /// AWS Bedrock routing, when configured. Requests are SigV4-signed and the
    /// response is decoded from the AWS event-stream framing.
    bedrock: Option<BedrockConfig>,
    /// Request-body `metadata.user_id` for OAuth subscription abuse/rate-limit
    /// bucketing; emitted only on the first-party Messages request.
    metadata_user_id: Option<String>,
    fast_mode: bool,
    /// Transport-level request interception, when configured.
    interceptor: Option<SharedRequestInterceptor>,
    /// Client-declared model capabilities. The provider never infers capability
    /// from the model id; this is the single source of truth.
    capabilities: ModelCapabilities,
    /// Per-model overrides consulted before `capabilities` (see the options).
    capabilities_by_model: HashMap<String, ModelCapabilities>,
    native_tool_support: super::native_tools::NativeToolSupport,
}

impl AnthropicProvider {
    pub fn new(opts: AnthropicProviderOptions) -> Self {
        // Fold the env var into the resolved base so the first-party gate and the
        // request target cannot diverge (an env-configured gateway must not be
        // misclassified as first-party and leak the scope:'global' beta).
        let env_base = if opts.protected_first_party_contract {
            None
        } else {
            std::env::var("ANTHROPIC_BASE_URL")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        let resolved_base = if opts.protected_first_party_contract {
            None
        } else {
            opts.auth_source
                .as_ref()
                .and_then(|a| a.base_url().map(|s| s.to_string()))
                .or_else(|| opts.base_url.clone())
                .or(env_base)
        };
        let is_first_party =
            resolved_base.is_none() && opts.vertex.is_none() && opts.bedrock.is_none();
        let base = resolved_base.unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let base = base.trim_end_matches('/').to_string();
        let messages_url = format!("{base}/v1/messages");
        let count_tokens_url = format!("{base}/v1/messages/count_tokens");
        let direct_media_endpoint = opts.vertex.is_none()
            && opts.bedrock.is_none()
            && url::Url::parse(&base).ok().is_some_and(|url| {
                url.host_str() == Some("api.anthropic.com") && url.port().is_none()
            });
        let frozen_runtime_environment = opts
            .protected_first_party_contract
            .then(AnthropicRuntimeEnvironment::protected_first_party_contract);
        let info = ProviderInfo {
            name: "anthropic".into(),
            base_url: if opts.protected_first_party_contract {
                None
            } else {
                opts.base_url.clone().map(Into::into)
            },
            env_key: Some("ANTHROPIC_API_KEY".into()),
            default_model: opts
                .default_model
                .clone()
                .unwrap_or_else(|| "claude-sonnet-5".to_string())
                .into(),
        };
        AnthropicProvider {
            info,
            client: reqwest::Client::new(),
            api_key: opts.api_key,
            auth_source: opts.auth_source,
            api_key_scheme: opts.api_key_scheme,
            http_headers: opts.http_headers.unwrap_or_default(),
            env_http_headers: opts.env_http_headers.unwrap_or_default(),
            retry: opts.retry.unwrap_or_default(),
            review_tool_loop: false,
            is_first_party,
            direct_media_endpoint,
            image_limits: opts.image_limits.unwrap_or_default(),
            image_limits_by_model: opts.image_limits_by_model.unwrap_or_default(),
            max_media_blocks: opts.max_media_blocks,
            max_media_blocks_by_model: opts.max_media_blocks_by_model.unwrap_or_default(),
            messages_url,
            count_tokens_url,
            frozen_runtime_environment,
            vertex: opts.vertex,
            bedrock: opts.bedrock,
            metadata_user_id: opts.metadata_user_id,
            fast_mode: opts.fast_mode,
            interceptor: opts.interceptor,
            capabilities: opts.capabilities.unwrap_or_default(),
            capabilities_by_model: opts.capabilities_by_model.unwrap_or_default(),
            native_tool_support: Default::default(),
        }
    }

    /// The capability set for ONE request's model: the per-model override when
    /// the host declared one, else the session-level set.
    fn capabilities_for(&self, model: &RuntimeString) -> ModelCapabilities {
        let declared = model
            .try_to_utf8()
            .ok()
            .and_then(|id| self.capabilities_by_model.get(&id));
        let mut caps = declared
            .cloned()
            .unwrap_or_else(|| self.capabilities.clone());
        if declared.is_none() && model != &self.info.default_model {
            caps.fast_mode = false;
            caps.image_limits = None;
            caps.max_media_blocks = None;
            caps.native_deferred_tools = None;
        }
        caps.native_deferred_tools = Some(caps.native_deferred_tools.unwrap_or_else(|| {
            self.is_first_party
                && super::model_catalog::native_deferred_tools_default("anthropic", model)
        }));
        caps
    }

    fn native_deferred_tools_enabled(&self, model: &RuntimeString, bound: Option<bool>) -> bool {
        self.vertex.is_none()
            && self.bedrock.is_none()
            && bound.or(self.capabilities_for(model).native_deferred_tools) == Some(true)
            && !self.native_tool_support.disabled(&self.messages_url, model)
    }

    /// The resolved model/source limits used by admission and final validation.
    pub fn image_limits_for(&self, model: &RuntimeString) -> ImageLimits {
        let native = model
            .try_to_utf8()
            .ok()
            .and_then(|id| self.image_limits_by_model.get(&id))
            .copied()
            .unwrap_or(self.image_limits);
        native.with_declaration(self.capabilities_for(model).image_limits.as_ref())
    }

    pub fn media_budget_for(&self, model: &RuntimeString) -> MediaBudget {
        let count = self
            .capabilities_for(model)
            .max_media_blocks
            .and_then(|value| usize::try_from(value.get()).ok())
            .or_else(|| {
                model
                    .try_to_utf8()
                    .ok()
                    .and_then(|id| self.max_media_blocks_by_model.get(&id))
                    .copied()
            })
            .or(self.max_media_blocks);
        MediaBudget::for_anthropic(self.direct_media_endpoint, count)
    }

    /// Captured request policy outranks native/source declarations. Neither a
    /// model name nor a configured token window is a media-count declaration.
    pub fn resolve_request_media_policy(
        &self,
        model: &RuntimeString,
        image_limits_snapshot: Option<ImageLimits>,
        max_media_blocks_snapshot: Option<usize>,
    ) -> (ImageLimits, MediaBudget) {
        let image_limits = image_limits_snapshot.unwrap_or_else(|| self.image_limits_for(model));
        let mut budget = self.media_budget_for(model);
        if let Some(limit) = max_media_blocks_snapshot.filter(|limit| *limit > 0) {
            budget.max_blocks = limit;
        }
        (image_limits, budget)
    }

    /// The endpoint whose pooled connection the first real request reuses:
    /// Bedrock's regional runtime host, Vertex's (region-prefixed) publisher
    /// host, else the resolved base origin (first-party, custom base URL, or
    /// Foundry).
    fn prewarm_endpoint(&self) -> String {
        if let Some(bedrock) = &self.bedrock {
            return format!("https://bedrock-runtime.{}.amazonaws.com/", bedrock.region);
        }
        if let Some(vertex) = &self.vertex {
            return format!("https://{}/", vertex.host());
        }
        endpoint_origin(&self.messages_url)
    }

    /// Whether to use NATIVE structured outputs (`output_config.format`) instead
    /// of result-tool emulation. Native support is a client-declared capability;
    /// result-tool selection also follows the declared thinking compatibility.
    fn use_native_structured_output(&self, req: &ModelRequest) -> bool {
        req.output_schema.is_some()
            && self.is_first_party
            && self.capabilities_for(&req.model).structured_output
            && !feature_disabled(
                self.frozen_runtime_environment,
                AnthropicFeatureFlag::NativeStructuredOutput,
            )
    }

    fn structured_output_mode(&self, req: &ModelRequest) -> StructuredOutputMode {
        let capabilities = self.capabilities_for(&req.model);
        StructuredOutputMode {
            native: self.use_native_structured_output(req),
            review_tool_loop: self.review_tool_loop,
            // Mandatory reasoning alone says nothing about tool-choice support.
            auto_result_tool: capabilities.forced_tool_choice_with_thinking == Some(false)
                && capabilities
                    .thinking
                    .is_some_and(|thinking| !thinking.can_disable_thinking),
        }
    }

    /// Merge static custom headers with env-resolved ones. Env values are
    /// read at request time; unset or empty variables skip the header.
    fn resolved_custom_headers(&self) -> Vec<(String, String)> {
        let mut headers: Vec<(String, String)> = self
            .http_headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (name, env_var) in &self.env_http_headers {
            if let Ok(value) = std::env::var(env_var) {
                let value = value.trim().to_string();
                if !value.is_empty() {
                    headers.push((name.clone(), value));
                }
            }
        }
        headers
    }

    /// Whether THIS request must lead with the verbatim Claude Code identity
    /// system block. True only for claude.ai OAuth subscription auth (an
    /// `auth_source` with no api key); api-key and Vertex/Bedrock cloud paths
    /// must never send it. Leading with the verbatim identity block is what
    /// the subscription anti-abuse gate requires to unlock premium models.
    fn requires_claude_code_identity(&self) -> bool {
        self.api_key.is_none()
            && self.auth_source.is_some()
            && self.vertex.is_none()
            && self.bedrock.is_none()
    }

    /// Spend a stored API key on the configured carrier header. Gateways in
    /// front of the Anthropic surface are split on which scheme they validate:
    /// the native `x-api-key`, or an `Authorization: Bearer` carrier. The
    /// default stays `x-api-key` so first-party traffic is byte-identical to
    /// before this knob existed.
    fn attach_api_key_header(
        &self,
        rb: reqwest::RequestBuilder,
        key: &str,
    ) -> reqwest::RequestBuilder {
        match self.api_key_scheme {
            ApiKeyScheme::XApiKey => rb.header("x-api-key", key),
            ApiKeyScheme::Bearer => {
                rb.header(reqwest::header::AUTHORIZATION, format!("Bearer {key}"))
            }
        }
    }

    /// Exact pre-turn token count via the documented `count_tokens` endpoint.
    /// Optional/opt-in (a network round-trip); falls back to the chars/4 estimate
    /// on any failure.
    pub async fn count_tokens(&self, req: &ModelRequest) -> u64 {
        match self.count_tokens_request(req).await {
            Some(n) => n,
            None => estimate_tokens_fallback(req),
        }
    }

    async fn count_tokens_request(&self, req: &ModelRequest) -> Option<u64> {
        // Token counting never writes cache; mirror the native-vs-forced dispatch
        // so the tool array we count matches what we'll send.
        let (image_limits, media_budget) = self.resolve_request_media_policy(
            &req.model,
            req.image_limits.as_ref().map(|declaration| {
                self.image_limits_for(&req.model)
                    .with_declaration(Some(declaration))
            }),
            req.max_media_blocks
                .and_then(|value| usize::try_from(value.get()).ok()),
        );
        let params = build_params_with_media_limits(
            req,
            req.output_schema.is_some(),
            false,
            self.native_deferred_tools_enabled(&req.model, req.native_deferred_tools),
            self.structured_output_mode(req),
            self.requires_claude_code_identity(),
            image_limits,
            media_budget,
        )
        .ok()?;
        let mut body = Map::new();
        body.insert("model", params.get("model").cloned()?);
        body.insert(
            "messages",
            params
                .get("messages")
                .cloned()
                .unwrap_or_else(|| Value::Array(vec![])),
        );
        if let Some(s) = params.get("system") {
            body.insert("system", s.clone());
        }
        if let Some(t) = params.get("tools") {
            body.insert("tools", t.clone());
        }
        if let Some(tc) = params.get("tool_choice") {
            body.insert("tool_choice", tc.clone());
        }
        let mut rb = self
            .client
            .post(&self.count_tokens_url)
            .header("anthropic-version", ANTHROPIC_VERSION);
        for (k, v) in &self.resolved_custom_headers() {
            rb = rb.header(k.as_str(), v.as_str());
        }
        if let Some(k) = &self.api_key {
            rb = self.attach_api_key_header(rb, k);
        } else if self.auth_source.is_some() {
            rb = rb.header("authorization", "Bearer oauth");
        }
        let resp = with_anthropic_json_body(rb, &Value::Object(body))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let response_body = resp.text().await.ok()?;
        let v: Value = decode_anthropic_response(&response_body)?;
        v.get("input_tokens").and_then(Value::as_u64)
    }
}

#[async_trait::async_trait]
impl ModelProvider for AnthropicProvider {
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
        req.native_deferred_tools = metadata.native_deferred_tools;
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
        selected.review_tool_loop = true;
        selected.stream(req, signal)
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
        selected.review_tool_loop = true;
        let mut request = request.clone();
        crate::provider::review_target::apply_typed_review_effort(
            &mut request,
            raw_effort.as_ref(),
        )
        .map_err(|error| error.message)?;
        selected.approval_request_tokens(&request)
    }

    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        let mut request = request.clone();
        let capabilities = self.capabilities_for(&request.model);
        request.max_output_tokens = capabilities.clamp_output_tokens(request.max_output_tokens);
        let native = self.use_native_structured_output(&request);
        let (image_limits, media_budget) = self.resolve_request_media_policy(
            &request.model,
            request.image_limits.as_ref().map(|declaration| {
                self.image_limits_for(&request.model)
                    .with_declaration(Some(declaration))
            }),
            request
                .max_media_blocks
                .and_then(|value| usize::try_from(value.get()).ok()),
        );
        let cache_scope_enabled = self.is_first_party
            && !feature_disabled(
                self.frozen_runtime_environment,
                AnthropicFeatureFlag::GlobalCacheScope,
            );
        let output_mode = self.structured_output_mode(&request);
        let mut params = build_params_with_media_limits(
            &request,
            request.output_schema.is_some(),
            cache_scope_enabled,
            self.native_deferred_tools_enabled(&request.model, request.native_deferred_tools),
            output_mode,
            self.requires_claude_code_identity(),
            image_limits,
            media_budget,
        )
        .map_err(|error| RuntimeString::from(error.to_string()))?;
        let output_config =
            build_output_config(&request, &capabilities, native, self.is_first_party);
        if !output_config.is_empty() {
            params.insert("output_config", Value::Object(output_config));
        }
        let forced = request.output_schema.is_some()
            && !native
            && !self.review_tool_loop
            && !output_mode.auto_result_tool;
        if let Some(thinking) =
            resolve_wire_thinking(&request, &capabilities, self.is_first_party, forced)
        {
            params.insert("thinking", thinking);
        }
        let mut input = Map::new();
        for field in [
            "system",
            "messages",
            "tools",
            "tool_choice",
            "output_config",
            "thinking",
        ] {
            if let Some(value) = params.remove(field) {
                input.insert(field, value);
            }
        }
        Ok(crate::engine::session::estimate_approval_wire_input_tokens(
            &Value::Object(input),
        ))
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        // Every field the engine gates on comes from the client-declared
        // capability set (per-model override first); nothing is inferred from
        // the model id. The remaining constants are provider-level truths
        // (Anthropic always streams, offers tools, can emulate structured
        // output, and honors request-side cache control), not model-specific
        // claims.
        let caps = self.capabilities_for(model);
        let effective = self.image_limits_for(model);
        let declared = caps.image_limits.as_ref();
        let image_limits = crate::types::provider::ImageLimits {
            max_width: std::num::NonZeroU32::new(effective.max_width),
            max_height: std::num::NonZeroU32::new(effective.max_height),
            max_base64_size: u32::try_from(effective.max_base64_chars)
                .ok()
                .and_then(std::num::NonZeroU32::new),
            target_raw_size: declared
                .and_then(|limits| limits.target_raw_size)
                .or_else(|| {
                    (effective.target_raw_bytes != effective.max_base64_chars / 4 * 3)
                        .then(|| {
                            u32::try_from(effective.target_raw_bytes)
                                .ok()
                                .and_then(std::num::NonZeroU32::new)
                        })
                        .flatten()
                }),
        };
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: true,
            web_search: self.is_first_party && caps.web_search,
            prompt_cache_control: true,
            context_window: caps.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW),
            max_output_tokens: caps.max_output_tokens.unwrap_or(DEFAULT_MAX_OUTPUT),
            native_pdf: caps.native_pdf,
            vision: caps.vision,
            image_limits: Some(image_limits),
            max_media_blocks: u32::try_from(self.media_budget_for(model).max_blocks)
                .ok()
                .and_then(std::num::NonZeroU32::new),
        }
    }

    fn estimate_tokens(&self, req: &ModelRequest) -> Option<u64> {
        Some(estimate_tokens_fallback(req))
    }

    fn has_prewarm_endpoints(&self) -> bool {
        true
    }

    fn credential_account_id(&self) -> Option<String> {
        self.auth_source
            .as_ref()
            .and_then(|auth| auth.credential_account_id())
    }

    async fn rotate_credential(
        &self,
        failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        self.auth_source
            .as_ref()?
            .rotate_credential(failed_id)
            .await
    }

    fn prewarm_transport(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Vec<PrewarmOutcome>> + Send>> {
        let client = self.client.clone();
        let endpoint = self.prewarm_endpoint();
        Box::pin(async move { vec![probe(&client, &endpoint).await] })
    }

    fn stream(&self, mut req: ModelRequest, signal: AbortSignal) -> ModelStream {
        if crate::provider::native_compaction::contains_native_checkpoint(&req.messages) {
            return crate::provider::native_compaction::rejected_checkpoint_stream();
        }

        if let Some(hints) = req
            .context_hints
            .as_ref()
            .filter(|hints| self.is_first_party && hints.enabled && hints.main_thread)
        {
            hints.state.apply_frozen(&req.model, &mut req.messages);
        }
        if let Err(error) = crate::provider::native_media::validate_request(&req, &[]) {
            return crate::provider::native_media::rejected_stream(error);
        }
        if req.web_search.as_ref().is_some_and(|search| {
            search.access == crate::schemas::config::WebSearchAccess::Disabled
        }) {
            req.web_search = None;
        }

        if let Some(error) =
            crate::provider::unsupported_inference_control(&req, "Anthropic", false, false, false)
        {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error { error }
            }));
        }
        let client = self.client.clone();
        let api_key = self.api_key.clone();
        let auth_source = self.auth_source.clone();
        if let Some(auth) = &auth_source {
            auth.credential_request_started();
        }
        let api_key_scheme = self.api_key_scheme;
        let custom_headers = self.resolved_custom_headers();
        let mut retry = self.retry.clone();
        let review_stream = retry.retry_class_policy
            == Some(crate::provider::retry::RetryClassPolicy::ApprovalReview);
        retry.yield_to_credential_rotation = self.credential_account_id().is_some();
        let is_first_party = self.is_first_party;
        let native_thinking_summaries = req.reasoning_summary
            != crate::types::provider::ReasoningSummary::None
            && self.vertex.is_none()
            && self.bedrock.is_none()
            && is_native_summary_endpoint(&self.messages_url);
        let fast_mode = self.fast_mode;
        let messages_url = self.messages_url.clone();
        let vertex = self.vertex.clone();
        let bedrock = self.bedrock.clone();
        let frozen_runtime_environment = self.frozen_runtime_environment;
        let require_claude_code_identity = self.requires_claude_code_identity();
        let metadata_user_id = self.metadata_user_id.clone();
        let interceptor = self.interceptor.clone();
        let provider_id = diagnostic_string(&self.info.name);
        // The single source of capability truth for this request: what the client
        // declared at init (per-model override first). Nothing below looks at
        // the model id to decide what the model can do.
        let capabilities = self.capabilities_for(&req.model);
        let (image_limits, media_budget) = self.resolve_request_media_policy(
            &req.model,
            req.image_limits.as_ref().map(|declaration| {
                self.image_limits_for(&req.model)
                    .with_declaration(Some(declaration))
            }),
            req.max_media_blocks
                .and_then(|value| usize::try_from(value.get()).ok()),
        );
        // A declared output ceiling is the API's own limit, so asking past it is a
        // refused request, not a longer answer. The turn's budget can climb above
        // it on its own (the max-token recovery ladder doubles the budget), so the
        // clamp lives here where the per-model ceiling is known.
        let mut req = req;
        req.max_output_tokens = capabilities.clamp_output_tokens(req.max_output_tokens);
        let output_mode = self.structured_output_mode(&req);
        let defer_loading_supported =
            self.native_deferred_tools_enabled(&req.model, req.native_deferred_tools);
        let native_tool_support = self.native_tool_support.clone();

        let s = async_stream::stream! {
            let structured = req.output_schema.is_some();
            // Native schemas and synthetic result tools share the same assembly
            // policy as token counting and approval request accounting.
            let use_native = output_mode.native;
            let structured_tool_fallback = structured && !use_native;
            let result_tool_only = structured_tool_fallback && !output_mode.review_tool_loop;
            let forced_structured = result_tool_only && !output_mode.auto_result_tool;
            let buffer_result_text = result_tool_only && output_mode.auto_result_tool;
            if let Some(error) = validate_anthropic_request_choice(
                &req,
                req.web_search.is_some(),
                result_tool_only,
            ) {
                yield ModelStreamEvent::Error { error };
                return;
            }
            let cache_scope_enabled = is_first_party
                && !feature_disabled(
                    frozen_runtime_environment,
                    AnthropicFeatureFlag::GlobalCacheScope,
                );
            // A scope:'global' breakpoint is emitted only when the request carries
            // a leading run of global parts; the beta rides along only then.
            let emits_global_scope = cache_scope_enabled
                && req
                    .system
                    .first()
                    .is_some_and(|p| p.cache_scope == Some(CacheScope::Global));
            let params = match build_params_with_media_limits(
                &req, structured, cache_scope_enabled, defer_loading_supported,
                output_mode,
                require_claude_code_identity, image_limits, media_budget,
            ) {
                Ok(params) => params,
                Err(error) => {
                    yield ModelStreamEvent::Error { error: ProviderError {
                        kind: ProviderErrorKind::InvalidRequest,
                        message: error.to_string().into(),
                        status: None,
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false, credential_recovery: None,
                        retry_after_ms: None,
                        request_id: None,
                    }};
                    return;
                }
            };

            let effort_level = req
                .effort
                .clone()
                .and_then(|e| resolve_effort_level(e, &capabilities));
            let wire_thinking = resolve_wire_thinking(
                &req,
                &capabilities,
                is_first_party,
                forced_structured,
            );
            let interleaved = capabilities.thinking.is_some_and(|t| t.interleaved)
                && !feature_disabled(
                    frozen_runtime_environment,
                    AnthropicFeatureFlag::InterleavedThinking,
                );
            let uses_1m = is_first_party
                && capabilities.one_million_context
                && !feature_disabled(
                    frozen_runtime_environment,
                    AnthropicFeatureFlag::Context1m,
                );
            let task_budget_active = req.task_budget.is_some()
                && is_first_party
                && capabilities.task_budgets;
            // API-level context editing: first-party only, and suppressible by
            // env/frozen contract. Complements client-side micro-compaction.
            let (tool_uses_editing, thinking_editing) = resolve_context_editing(
                &req,
                is_first_party,
                feature_disabled(
                    frozen_runtime_environment,
                    AnthropicFeatureFlag::ContextManagement,
                ),
                wire_thinking.is_some(),
            );
            let context_management = build_context_management(tool_uses_editing, thinking_editing);
            let context_editing_active = context_management.is_some();

            let output_config = build_output_config(&req, &capabilities, use_native, is_first_party);

            let mut request_params = params;
            let fast_active = fast_mode && is_first_party && capabilities.fast_mode;
            if fast_active {
                request_params.insert("speed", vs("fast"));
            }
            if !output_config.is_empty() {
                request_params.insert("output_config", Value::Object(output_config));
            }
            if let Some(cm) = context_management {
                request_params.insert("context_management", cm);
            }
            if let Some(t) = &wire_thinking {
                request_params.insert("thinking", t.clone());
            }
            let mut context_hints = ContextHintController::new(req.context_hints.as_ref(), is_first_party);
            if let Some(hint) = context_hints.build_request(&req.messages) {
                if hint.enabled {
                    request_params.insert("context_hint", vobj(vec![
                        ("enabled", Value::Bool(true)),
                        ("target_tokens_saved", Value::from(hint.target_tokens_saved)),
                    ]));
                }
            }
            // Request-body metadata.user_id: claude.ai's abuse / rate-limit bucketing
            // for OAuth subscription traffic, matching the official client. First-party
            // only — Vertex/Bedrock route bodies must not carry it.
            if bedrock.is_none() && vertex.is_none() {
                if let Some(uid) = &metadata_user_id {
                    request_params
                        .insert("metadata", vobj(vec![("user_id", vs(uid.clone()))]));
                }
            }
            // Route selection. Bedrock and Vertex both carry the model in the URL
            // and pin their own body `anthropic_version`; Bedrock additionally
            // signs with SigV4 and decodes an event-stream response.
            let (request_url, uses_vertex, bedrock_attempt) = if let Some(cfg) = &bedrock {
                let Some(credentials) = AwsCredentials::from_env() else {
                    yield ModelStreamEvent::Error {
                        error: ProviderError {
                            kind: ProviderErrorKind::Auth,
                            message: RuntimeString::from(
                                "AWS credentials not found: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for Bedrock",
                            ),
                            status: None,
                            code: None,
                            error_type: None,
                            media_origins: None,
                            media_repair_complete: false, credential_recovery: None,
                            retry_after_ms: None,
                            request_id: None,
                        },
                    };
                    return;
                };
                apply_bedrock_request_body(&mut request_params);
                let host = format!("bedrock-runtime.{}.amazonaws.com", cfg.region);
                let encoded_model =
                    urlencoding::encode(&diagnostic_string(&req.model)).into_owned();
                let canonical_uri =
                    format!("/model/{encoded_model}/invoke-with-response-stream");
                let url = format!("https://{host}{canonical_uri}");
                (
                    url,
                    false,
                    Some(BedrockAttempt {
                        region: cfg.region.clone(),
                        host,
                        canonical_uri,
                        credentials,
                    }),
                )
            } else if let Some(cfg) = &vertex {
                apply_vertex_request_body(&mut request_params);
                (cfg.stream_url(&diagnostic_string(&req.model)), true, None)
            } else {
                (messages_url, false, None)
            };
            let body = Value::Object(request_params);

            let credential_request_id = uuid::Uuid::new_v4().to_string();
            let ctx = Arc::new(AttemptCtx {
                client,
                url: request_url,
                api_key,
                auth_source: auth_source.clone(),
                api_key_scheme,
                custom_headers,
                body: Mutex::new(body),
                media_repair: Mutex::new(MediaRepairState::default()),
                media_repair_observer: crate::context::media_budget::media_repair_observer(),
                request_attempt: AtomicU32::new(0),
                effort_beta: effort_level.is_some(),
                fast_mode: fast_active,
                interleaved,
                uses_1m,
                task_budget: task_budget_active,
                context_management: context_editing_active,
                context_hints: Mutex::new(context_hints),
                hint_messages: req.messages.clone(),
                hint_model: req.model.clone(),
                native_tool_support,
                emits_global: emits_global_scope,
                use_native,
                vertex: uses_vertex,
                bedrock: bedrock_attempt,
                provider_id,
                model: diagnostic_string(&req.model),
                interceptor,
                credential_recovery: Mutex::new(crate::types::errors::CredentialRecovery {
                    diagnostic_request_id: Some(credential_request_id.clone()),
                    ..Default::default()
                }),
                credential_request_id,
                body_bytes: AtomicU32::new(0),
                output_tokens: AtomicU32::new(0),
                upstream_request_id: Mutex::new(None),
            });

            // One establishment attempt: refresh + attach the token, open the SSE,
            // advance to the FIRST event. Connect/HTTP errors surface here so
            // `with_retry` can retry them; once the first event lands we never retry.
            // Every scheduled retry is yielded as a `RetryScheduled` stream event
            // WHILE its backoff sleeps, so consumers see establishment progress.
            let (retry_opts, retry_rx) = observe_retries(retry);
            let make_attempt = || {
                let ctx = ctx.clone();
                let signal = signal.clone();
                move |_attempt: u32| {
                    let ctx = ctx.clone();
                    let signal = signal.clone();
                    async move { attempt_with_media_remediation(&ctx, &signal).await }
                }
            };

            let establish = async {
                loop {
                    let mut established =
                        with_retry(retry_opts.clone(), signal.clone(), make_attempt()).await;
                    let failed_account = ctx.credential_recovery.lock_recover().failed_account_id.clone();
                    // A revoked token may need one refresh even before its local expiry.
                    let is_auth_err =
                        matches!(&established, Err(RetryError::Provider(pe)) if pe.kind == ProviderErrorKind::Auth);
                    if is_auth_err && !signal.is_cancelled() {
                        if let Some(auth) = &auth_source {
                            let refreshed = match failed_account.as_deref() {
                                Some(id) => auth.force_refresh_for_account(id).await,
                                None => auth.force_refresh().await,
                            };
                            if refreshed == Some(true) {
                                established =
                                    with_retry(retry_opts.clone(), signal.clone(), make_attempt()).await;
                            }
                        }
                    }
                    // An account that still cannot authenticate after renewal
                    // must not prevent the remaining subscription accounts from
                    // serving this request. Refresh happens before rotation.
                    let failed_account = ctx.credential_recovery.lock_recover().failed_account_id.clone();
                    let can_rotate = matches!(&established, Err(RetryError::Provider(error))
                        if crate::provider::rotation::is_quota_rotation_error(error)
                            || error.kind == ProviderErrorKind::Auth);
                    if can_rotate && !signal.is_cancelled() {
                        if let (Some(auth), Some(failed)) = (&auth_source, failed_account) {
                            let repeated = {
                                let mut recovery = ctx.credential_recovery.lock_recover();
                                if recovery.attempted_account_ids.contains(&failed) {
                                    true
                                } else {
                                    recovery.attempted_account_ids.push(failed.clone());
                                    false
                                }
                            };
                            if repeated {
                                record_credential_exhaustion(&ctx);
                                return established;
                            }
                            let rotation = auth.rotate_credential(&failed).await;
                            crate::observability::provider_diagnostics::record_named(
                                crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                                "credential_rotation",
                                serde_json::json!({
                                    "credentialRequestId": ctx.credential_request_id,
                                    "model": ctx.model,
                                    "failedAccountId": failed,
                                    "nextAccountId": rotation.as_ref().and_then(|outcome| outcome.current()),
                                    "outcome": format!("{rotation:?}"),
                                }),
                            );
                            if rotation.as_ref().is_some_and(|outcome| outcome.retriable()) {
                                continue;
                            }
                            record_credential_exhaustion(&ctx);
                        }
                    }
                    return established;
                }
            };
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
            let Some(established) = established else {
                // The step stream always terminates with `Done`.
                return;
            };

            let Established {
                mut body_stream,
                mut decoder,
                first_events,
                native_response_endpoint,
            } = match established {
                Ok(AttemptOutcome::Stream(established)) => established,
                Ok(AttemptOutcome::RejectedMedia(raw)) => {
                    let mut error = classify_error(&raw);
                    error.media_repair_complete = true;
                    yield ModelStreamEvent::Error { error };
                    return;
                }
                Err(err) => {
                    // Tokens may have started before a break — the engine loop owns
                    // recovery, so we surface a single error event (silent on abort).
                    if signal.is_cancelled() {
                        return;
                    }
                    let mut error = retry_err_to_provider(err);
                    error.credential_recovery = Some(Box::new(ctx.credential_recovery.lock_recover().clone()));
                    error.media_repair_complete = ctx.media_repair.lock_recover().has_repairs();
                    yield ModelStreamEvent::Error { error };
                    return;
                }
            };

            let mut blocks: HashMap<usize, BlockInfo> = HashMap::new();
            let mut thinking_summary = ThinkingSummaryState::new(native_thinking_summaries && native_response_endpoint);
            let mut usage = Usage { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0, cache_creation_input_tokens: 0, total_tokens: None };
            let mut stop_reason = ModelStopReason::EndTurn;
            // An automatic result tool may follow ordinary prose. Hold that prose
            // until completion so only the structured result becomes final text.
            let mut structured_text = RuntimeString::default();
            let mut structured_json = RuntimeString::default();
            let mut saw_wire_stop = false;

            // Replay the probed first event(s) (never an error — checked at probe).
            for data in first_events {
                saw_wire_stop |= data.get("type").and_then(Value::as_str).is_some_and(|tag| tag == "message_stop");
                let out = handle_event(&data, &mut blocks, structured_tool_fallback, &mut structured_json, &mut thinking_summary);
                if update_stream_usage(&data, &mut usage) {
                    yield ModelStreamEvent::UsageSnapshot { usage: usage.clone() };
                }
                if let Some((cleared_tool_uses, cleared_input_tokens, cleared_thinking_turns)) =
                    parse_applied_context_edits(&data)
                {
                    yield ModelStreamEvent::ContextEditsApplied {
                        cleared_tool_uses,
                        cleared_input_tokens,
                        cleared_thinking_turns,
                    };
                }
                for e in out.events {
                    if buffer_result_text {
                        if let ModelStreamEvent::TextDelta { text } = &e {
                            structured_text = structured_text.concat(text);
                            continue;
                        }
                    }
                    yield e;
                }
                if let Some(sr) = out.stop_reason {
                    stop_reason = sr;
                }
            }

            let mut errored = false;
            loop {
                let mut got_error = false;
                while let Some(block) = decoder.next_block() {
                    let Some(data) = parse_sse_data(&block) else {
                        continue;
                    };
                    if data
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|tag| tag == "error")
                    {
                        if !signal.is_cancelled() {
                            yield ModelStreamEvent::Error {
                                error: {
                                    let mut error = classify_error(&raw_from_sse(&data));
                                    error.credential_recovery = Some(Box::new(ctx.credential_recovery.lock_recover().clone()));
                                    if error.request_id.is_none() {
                                        error.request_id = ctx.upstream_request_id.lock_recover().clone().map(Into::into);
                                    }
                                    crate::observability::provider_diagnostics::record_named(
                                        crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
                                        "credential_stream_error",
                                        serde_json::json!({
                                            "credentialRequestId": ctx.credential_request_id,
                                            "accountId": ctx.credential_recovery.lock_recover().failed_account_id,
                                            "upstreamRequestId": error.request_id.as_ref().and_then(|id| id.try_to_utf8().ok()),
                                            "status": error.status,
                                            "errorKind": error.kind.as_str(),
                                            "retryAfterMs": error.retry_after_ms,
                                        }),
                                    );
                                    if ctx.context_hints.lock_recover().classify_stream_error(&error) {
                                        error.code = Some(CONTEXT_HINT_STREAM_FALLBACK_CODE.into());
                                    }
                                    error.media_repair_complete = ctx.media_repair.lock_recover().has_repairs();
                                    error
                                },
                            };
                        }
                        got_error = true;
                        break;
                    }
                    let out =
                        handle_event(&data, &mut blocks, structured_tool_fallback, &mut structured_json, &mut thinking_summary);
                    saw_wire_stop |= data.get("type").and_then(Value::as_str).is_some_and(|tag| tag == "message_stop");
                    if update_stream_usage(&data, &mut usage) {
                        yield ModelStreamEvent::UsageSnapshot { usage: usage.clone() };
                    }
                    if let Some((cleared_tool_uses, cleared_input_tokens, cleared_thinking_turns)) =
                        parse_applied_context_edits(&data)
                    {
                        yield ModelStreamEvent::ContextEditsApplied {
                            cleared_tool_uses,
                            cleared_input_tokens,
                            cleared_thinking_turns,
                        };
                    }
                    for e in out.events {
                        if buffer_result_text {
                            if let ModelStreamEvent::TextDelta { text } = &e {
                                structured_text = structured_text.concat(text);
                                continue;
                            }
                        }
                        yield e;
                    }
                    if let Some(sr) = out.stop_reason {
                        stop_reason = sr;
                    }
                }
                if got_error {
                    errored = true;
                    break;
                }
                if signal.is_cancelled() {
                    return;
                }
                tokio::select! {
                    _ = signal.cancelled() => { return; }
                    next = body_stream.next() => {
                        match next {
                            Some(Ok(b)) => decoder.push(&b),
                            Some(Err(e)) => {
                                if !signal.is_cancelled() {
                                    yield ModelStreamEvent::Error {
                                        error: {
                                            let mut error = classify_error(&raw_from_transport(&e));
                                            if review_stream { error = error.mark_stream_interrupted(); }
                                            error.media_repair_complete = ctx.media_repair.lock_recover().has_repairs();
                                            error
                                        },
                                    };
                                }
                                errored = true;
                                break;
                            }
                            None => {
                                if !saw_wire_stop {
                                    let mut error = classify_error(&premature_eof());
                                    if review_stream { error = error.mark_stream_interrupted(); }
                                    error.media_repair_complete = ctx.media_repair.lock_recover().has_repairs();
                                    yield ModelStreamEvent::Error { error };
                                    errored = true;
                                }
                                break;
                            },
                        }
                    }
                }
            }
            if errored {
                return;
            }

            // The synthetic result tool is captured as the final text rather than
            // dispatched to the host, including between inspection rounds.
            if structured_tool_fallback && !structured_json.is_empty() {
                yield ModelStreamEvent::TextDelta {
                    text: structured_json.clone(),
                };
                if stop_reason == ModelStopReason::ToolUse {
                    stop_reason = ModelStopReason::EndTurn;
                }
            } else if buffer_result_text && !structured_text.is_empty() {
                // Text-only JSON still passes through the engine's schema validator.
                yield ModelStreamEvent::TextDelta { text: structured_text };
            }

            if let Some(auth) = &auth_source {
                auth.credential_request_succeeded();
            }
            yield ModelStreamEvent::MessageStop { stop_reason, usage };
        };
        Box::pin(s)
    }
}

// ---------------------------------------------------------------------------
// Establishment attempt
// ---------------------------------------------------------------------------

/// Per-request transport state. Request-local budget and media repairs update
/// only the normalized body across establishment attempts.
struct AttemptCtx {
    client: reqwest::Client,
    url: String,
    api_key: Option<String>,
    auth_source: Option<Arc<dyn AuthSource>>,
    api_key_scheme: ApiKeyScheme,
    custom_headers: Vec<(String, String)>,
    body: Mutex<Value>,
    media_repair: Mutex<MediaRepairState>,
    media_repair_observer: Option<Arc<std::sync::atomic::AtomicBool>>,
    request_attempt: AtomicU32,
    effort_beta: bool,
    fast_mode: bool,
    interleaved: bool,
    uses_1m: bool,
    task_budget: bool,
    context_management: bool,
    context_hints: Mutex<ContextHintController>,
    hint_messages: Vec<Message>,
    hint_model: RuntimeString,
    native_tool_support: super::native_tools::NativeToolSupport,
    emits_global: bool,
    use_native: bool,
    /// Vertex routing: the API version travels in the body, so suppress the
    /// `anthropic-version` header and the first-party `x-api-key` (GCP bearer
    /// auth arrives through `auth_source`).
    vertex: bool,
    /// AWS Bedrock routing: when set, the request is SigV4-signed and the
    /// response body is decoded from the AWS event-stream framing.
    bedrock: Option<BedrockAttempt>,
    /// Provider identity reported to the request interceptor.
    provider_id: String,
    /// Model reported to the request interceptor.
    model: String,
    /// Transport-level request interception, when configured.
    interceptor: Option<SharedRequestInterceptor>,
    credential_recovery: Mutex<crate::types::errors::CredentialRecovery>,
    credential_request_id: String,
    body_bytes: AtomicU32,
    output_tokens: AtomicU32,
    upstream_request_id: Mutex<Option<String>>,
}

fn record_credential_exhaustion(ctx: &AttemptCtx) {
    let recovery = {
        let mut recovery = ctx.credential_recovery.lock_recover();
        recovery.exhausted = true;
        recovery.clone()
    };
    crate::observability::provider_diagnostics::record_named(
        crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
        "credential_exhausted",
        serde_json::json!({
            "credentialRequestId": ctx.credential_request_id,
            "model": ctx.model,
            "attemptedAccountIds": recovery.attempted_account_ids,
            "attemptedAccountCount": recovery.attempted_account_ids.len(),
            "outcome": "exhausted",
        }),
    );
}

/// Per-request AWS Bedrock signing inputs.
struct BedrockAttempt {
    region: String,
    host: String,
    /// The signed/request path, e.g. `/model/{encoded-model}/invoke-with-response-stream`.
    canonical_uri: String,
    credentials: AwsCredentials,
}

enum AttemptOutcome {
    Stream(Established),
    RejectedMedia(RawProviderError),
}

/// The opened stream, its residual decode buffer, and the probed first event.
struct Established {
    body_stream: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    decoder: SseDecoder,
    first_events: Vec<Value>,
    native_response_endpoint: bool,
}

fn apply_hint_projection_to_body(body: &mut Value, projection: &ContextHintProjection) {
    if projection.replacements.is_empty() {
        return;
    }
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    for message in messages {
        let Some(blocks) = message.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        for block in blocks {
            if block
                .get("type")
                .and_then(Value::as_str)
                .is_none_or(|kind| kind != "tool_result")
            {
                continue;
            }
            let replacement = block
                .get("tool_use_id")
                .and_then(Value::as_str)
                .and_then(|id| projection.replacements.get(id))
                .cloned();
            if let Some(replacement) = replacement {
                if let Some(object) = block.as_object_mut() {
                    object.insert("content", vs(replacement));
                }
            }
        }
    }
}

async fn attempt_with_media_remediation(
    ctx: &AttemptCtx,
    signal: &AbortSignal,
) -> Result<AttemptOutcome, RawProviderError> {
    loop {
        if signal.is_cancelled() {
            return Err(RawProviderError {
                message: "aborted".to_owned(),
                ..Default::default()
            });
        }
        let request = ProviderRequestInfo {
            provider_id: &ctx.provider_id,
            model: &ctx.model,
            url: &ctx.url,
            attempt: ctx.request_attempt.fetch_add(1, Ordering::Relaxed) + 1,
        };
        let started = std::time::Instant::now();
        // Every physical resend refreshes interception and authentication.
        let extra = match admit_attempt(ctx.interceptor.as_ref(), &request) {
            Ok(extra) => extra,
            Err(denied) => {
                report_attempt(
                    ctx.interceptor.as_ref(),
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
        let result = attempt_once(ctx, signal, &extra).await;
        let classified = result.as_ref().err().map(classify_error);
        crate::observability::provider_diagnostics::record_named(
            crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
            "credential_attempt",
            serde_json::json!({
                "providerId": ctx.provider_id,
                "phase": "establishment",
                "model": ctx.model,
                "credentialRequestId": ctx.credential_request_id,
                "accountId": ctx.credential_recovery.lock_recover().failed_account_id,
                "bodyBytes": ctx.body_bytes.load(Ordering::Relaxed),
                "maxOutputTokens": ctx.output_tokens.load(Ordering::Relaxed),
                "attempt": request.attempt,
                "elapsedMs": started.elapsed().as_millis(),
                "success": result.is_ok(),
                "status": classified.as_ref().and_then(|error| error.status).or_else(|| result.is_ok().then_some(200)),
                "retryAfterMs": classified.as_ref().and_then(|error| error.retry_after_ms),
                "upstreamRequestId": ctx.upstream_request_id.lock_recover().clone(),
            }),
        );
        match &result {
            Ok(_) => report_attempt(
                ctx.interceptor.as_ref(),
                &request,
                started,
                true,
                None,
                None,
            ),
            Err(raw) => report_attempt(
                ctx.interceptor.as_ref(),
                &request,
                started,
                false,
                raw.status,
                Some(&raw.message),
            ),
        }
        if let Err(raw) = &result {
            if !signal.is_cancelled()
                && super::native_tools::unsupported_native_tools(raw.status, &raw.message)
            {
                let downgraded = {
                    let mut body = ctx.body.lock_recover();
                    let mut changed = false;
                    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
                        tools.retain(|tool| {
                            let deferred =
                                tool.get("defer_loading").and_then(Value::as_bool) == Some(true);
                            changed |= deferred;
                            !deferred
                        });
                    }
                    changed
                };
                if downgraded {
                    ctx.native_tool_support.disable(&ctx.url, &ctx.hint_model);
                    continue;
                }
            }
            if !signal.is_cancelled() {
                let adjusted = reduce_output_reservation(&mut ctx.body.lock_recover(), raw);
                if let Some(output_tokens) = adjusted {
                    tracing::debug!(output_tokens, "Retrying with a smaller output reservation");
                    continue;
                }
            }
            let kind = classify_error(raw).kind;
            let media_rejection = parse_media_rejection(raw.status, &raw.message).is_some()
                && !matches!(
                    kind,
                    ProviderErrorKind::PromptTooLong | ProviderErrorKind::RequestTooLarge
                );
            if media_rejection {
                if !signal.is_cancelled() {
                    let repaired = {
                        let mut state = ctx.media_repair.lock_recover();
                        let mut body = ctx.body.lock_recover();
                        state
                            .repair_request(&body, raw.status, &raw.message)
                            .map(|repair| {
                                *body = repair.body;
                                repair.reason
                            })
                    };
                    if let Some(reason) = repaired {
                        if let Some(observer) = &ctx.media_repair_observer {
                            observer.store(true, std::sync::atomic::Ordering::Release);
                        }
                        tracing::warn!(%reason, "Retrying request after replacing rejected media");
                        continue;
                    }
                }
                return Ok(AttemptOutcome::RejectedMedia(raw.clone()));
            }
            let error = classify_error(raw);
            let (hint_action, hint_stream_fallback) = {
                let mut hints = ctx.context_hints.lock_recover();
                let action = hints.on_request_error(&error);
                let stream_fallback =
                    action == HintErrorAction::None && hints.classify_stream_error(&error);
                (action, stream_fallback)
            };
            if hint_stream_fallback {
                let mut marked = raw.clone();
                marked.code = Some(CONTEXT_HINT_STREAM_FALLBACK_CODE.to_owned());
                return Err(marked);
            }
            if hint_action != HintErrorAction::None && !signal.is_cancelled() {
                let projection = if hint_action == HintErrorAction::ClearAndRetry {
                    let intent = ctx.context_hints.lock_recover().request_intent().cloned();
                    let directory = intent
                        .as_ref()
                        .and_then(|intent| intent.artifact_dir.as_ref());
                    let projection = project_hint_edits(&ctx.hint_messages, directory).await;
                    if let Some(intent) = intent {
                        intent.state.record_projection(&ctx.hint_model, &projection);
                        invalidate_cleared_reads(&ctx.hint_messages, &projection, &intent);
                    }
                    projection
                } else {
                    ContextHintProjection::default()
                };
                {
                    let mut body = ctx.body.lock_recover();
                    if let Some(object) = body.as_object_mut() {
                        object.remove("context_hint");
                    }
                    apply_hint_projection_to_body(&mut body, &projection);
                }
                if matches!(
                    hint_action,
                    HintErrorAction::ClearAndRetry | HintErrorAction::StripAndRetry
                ) {
                    continue;
                }
            }
        }
        return result.map(AttemptOutcome::Stream);
    }
}

/// Open the SSE and advance to the first event. Refreshes + attaches the bearer
/// per attempt (subscription mode), attaches beta headers, POSTs, and probes the
/// first event so connect/HTTP/first-event errors are retryable here.
/// `intercepted_headers` come from the request interceptor and are appended
/// after the static custom headers, before auth/protocol headers.
async fn attempt_once(
    ctx: &AttemptCtx,
    signal: &AbortSignal,
    intercepted_headers: &[(String, String)],
) -> Result<Established, RawProviderError> {
    let mut headers = match &ctx.auth_source {
        Some(a) => {
            let current = a.credential_account_id();
            let attempted = ctx
                .credential_recovery
                .lock_recover()
                .attempted_account_ids
                .clone();
            // Other requests can move the shared head back to an account this
            // request already failed on. Membership, not queue position, tells
            // whether there is still an untried credential.
            let id = current.as_ref().and_then(|current| {
                if attempted.contains(current) {
                    a.credential_account_ids()
                        .into_iter()
                        .find(|id| !attempted.contains(id))
                        .or_else(|| Some(current.clone()))
                } else {
                    Some(current.clone())
                }
            });
            ctx.credential_recovery.lock_recover().failed_account_id = id.clone();
            let result = match id.as_deref() {
                Some(id) => a.get_headers_for_account(id, signal).await,
                None => a.get_headers(signal).await,
            };
            result.map_err(|error| {
                // Credential preparation can fail before there is an HTTP
                // status. Preserve its auth classification through retry.
                let authentication_failed = error.kind == ProviderErrorKind::Auth;
                let mut raw = raw_from_provider_error(error);
                if authentication_failed {
                    raw.error_type = Some("authentication_error".to_string());
                }
                raw
            })?
        }
        None => HashMap::new(),
    };
    // Bedrock carries versions/betas via the body and signs with SigV4, so it
    // never receives the first-party `anthropic-beta` headers.
    if ctx.bedrock.is_none() {
        if ctx.fast_mode {
            merge_beta_header(&mut headers, FAST_MODE_BETA);
        }
        if ctx.effort_beta {
            merge_beta_header(&mut headers, EFFORT_BETA);
        }
        if ctx.interleaved {
            merge_beta_header(&mut headers, INTERLEAVED_THINKING_BETA);
        }
        if ctx.uses_1m {
            merge_beta_header(&mut headers, CONTEXT_1M_BETA);
        }
        if ctx.task_budget {
            merge_beta_header(&mut headers, TASK_BUDGETS_BETA);
        }
        if ctx.context_management {
            merge_beta_header(&mut headers, CONTEXT_MANAGEMENT_BETA);
        }
        if ctx.context_hints.lock_recover().beta_active() {
            merge_beta_header(&mut headers, CONTEXT_HINT_BETA);
        }
        if ctx.emits_global {
            merge_beta_header(&mut headers, CACHE_SCOPE_BETA);
        }
        if ctx.use_native {
            merge_beta_header(&mut headers, STRUCTURED_OUTPUTS_BETA);
        }
    }

    // Encode the body ONCE: Bedrock's SigV4 signature is over the exact bytes we
    // send, so the hashed payload and the request body must be identical.
    let payload = {
        let body = ctx.body.lock_recover();
        ctx.output_tokens.store(
            body.get("max_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                .min(u64::from(u32::MAX)) as u32,
            Ordering::Relaxed,
        );
        encode_anthropic_request_body(&body)
    };
    ctx.body_bytes.store(
        payload.len().min(u32::MAX as usize) as u32,
        Ordering::Relaxed,
    );
    let mut rb = ctx
        .client
        .post(&ctx.url)
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    match &ctx.bedrock {
        Some(bedrock) => {
            let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            let signed = super::sigv4::sign(
                &SigV4Request {
                    method: "POST",
                    service: "bedrock",
                    region: &bedrock.region,
                    host: &bedrock.host,
                    canonical_uri: &bedrock.canonical_uri,
                    canonical_query: "",
                    payload: &payload,
                    extra_headers: &[("content-type".to_string(), "application/json".to_string())],
                    sign_content_sha256: true,
                },
                &bedrock.credentials,
                &amz_date,
            );
            rb = rb
                .header(reqwest::header::AUTHORIZATION, signed.authorization)
                .header("x-amz-date", signed.amz_date)
                .header("x-amz-content-sha256", signed.content_sha256);
            if let Some(token) = signed.session_token {
                rb = rb.header("x-amz-security-token", token);
            }
            // Interceptor headers ride along unsigned: SigV4 only verifies the
            // signed set, so appended gateway/trace headers cannot break it.
            for (k, v) in intercepted_headers {
                rb = rb.header(k.as_str(), v.as_str());
            }
        }
        None => {
            if !ctx.vertex {
                rb = rb.header("anthropic-version", ANTHROPIC_VERSION);
            }
            for (k, v) in &ctx.custom_headers {
                rb = rb.header(k.as_str(), v.as_str());
            }
            for (k, v) in intercepted_headers {
                rb = rb.header(k.as_str(), v.as_str());
            }
            if let Some(k) = &ctx.api_key {
                if !ctx.vertex {
                    rb = match ctx.api_key_scheme {
                        ApiKeyScheme::XApiKey => rb.header("x-api-key", k.as_str()),
                        ApiKeyScheme::Bearer => {
                            rb.header(reqwest::header::AUTHORIZATION, format!("Bearer {k}"))
                        }
                    };
                }
            }
            for (k, v) in &headers {
                rb = rb.header(k.as_str(), v.as_str());
            }
        }
    }
    let resp = rb
        .body(payload)
        .send()
        .await
        .map_err(|e| raw_from_transport(&e))?;

    let status = resp.status();
    *ctx.upstream_request_id.lock_recover() = resp
        .headers()
        .get("request-id")
        .or_else(|| resp.headers().get("x-request-id"))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !status.is_success() {
        let hdrs = header_hashmap(resp.headers());
        let body = resp.text().await.unwrap_or_default();
        return Err(raw_from_http(status.as_u16(), &body, hdrs));
    }

    // A redirect cannot confer native summary provenance on a different source.
    let native_response_endpoint = is_native_summary_endpoint(resp.url().as_str());

    // Bedrock returns AWS event-stream framing; adapt it into SSE-shaped bytes so
    // the SSE decoder and the whole downstream event loop are reused unchanged.
    let mut body_stream: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>> =
        if ctx.bedrock.is_some() {
            Box::pin(bedrock_event_stream_to_sse(resp.bytes_stream()))
        } else {
            Box::pin(resp.bytes_stream())
        };
    let mut decoder = SseDecoder::new();
    loop {
        if let Some(block) = decoder.next_block() {
            if let Some(data) = parse_sse_data(&block) {
                if data
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|tag| tag == "error")
                {
                    return Err(raw_from_sse(&data));
                }
                return Ok(Established {
                    body_stream,
                    decoder,
                    first_events: vec![data],
                    native_response_endpoint,
                });
            }
            continue;
        }
        match body_stream.next().await {
            Some(Ok(b)) => decoder.push(&b),
            Some(Err(e)) => return Err(raw_from_transport(&e)),
            None => {
                return Err(premature_eof());
            }
        }
    }
}

fn premature_eof() -> RawProviderError {
    RawProviderError {
        message: "Network stream ended before the provider's message_stop event.".into(),
        ..Default::default()
    }
}

/// Convert Bedrock's AWS event-stream response into SSE-shaped byte chunks so
/// the SSE decoder and the downstream event loop are reused unchanged. Each
/// event frame's payload is `{"bytes": base64(<anthropic event json>)}`; the
/// decoded JSON is emitted as `data: <json>\n\n`. Exception frames and decode
/// failures become Anthropic-shaped error events.
fn bedrock_event_stream_to_sse(
    inner: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
) -> impl Stream<Item = reqwest::Result<Bytes>> + Send {
    async_stream::stream! {
        let mut decoder = AwsEventStreamDecoder::new();
        let mut inner = Box::pin(inner);
        loop {
            loop {
                match decoder.next_message() {
                    Ok(Some(message)) => {
                        if let Some(sse) = bedrock_message_to_sse(&message) {
                            yield Ok(Bytes::from(sse));
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        yield Ok(Bytes::from(bedrock_error_event_bytes(&error)));
                        return;
                    }
                }
            }
            match inner.next().await {
                Some(Ok(bytes)) => decoder.push(&bytes),
                Some(Err(error)) => {
                    yield Err(error);
                    return;
                }
                None => return,
            }
        }
    }
}

/// Build an Anthropic-shaped SSE error event (`data: {"type":"error",...}`).
fn bedrock_error_event_bytes(message: &str) -> Vec<u8> {
    let event = vobj(vec![
        ("type", vs("error")),
        (
            "error",
            vobj(vec![("type", vs("api_error")), ("message", vs(message))]),
        ),
    ]);
    let mut out = b"data: ".to_vec();
    out.extend_from_slice(&encode_anthropic_request_body(&event));
    out.extend_from_slice(b"\n\n");
    out
}

/// Translate one Bedrock event-stream message into SSE `data:` bytes, or `None`
/// to skip a frame we do not surface.
fn bedrock_message_to_sse(
    message: &crate::provider::aws_eventstream::EventStreamMessage,
) -> Option<Vec<u8>> {
    if message.is_exception() {
        let text = String::from_utf8_lossy(&message.payload).into_owned();
        return Some(bedrock_error_event_bytes(&text));
    }
    let payload_text = String::from_utf8_lossy(&message.payload);
    let payload_json = runtime_json::parse(&RuntimeString::from(payload_text.as_ref())).ok()?;
    let encoded = payload_json.get("bytes").and_then(Value::as_string)?;
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.to_node_utf8())
        .ok()?;
    let mut out = b"data: ".to_vec();
    out.extend_from_slice(&decoded);
    out.extend_from_slice(b"\n\n");
    Some(out)
}

fn header_hashmap(h: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (k, v) in h.iter() {
        if let Ok(s) = v.to_str() {
            m.insert(k.as_str().to_string(), s.to_string());
        }
    }
    m
}

fn raw_from_transport(e: &reqwest::Error) -> RawProviderError {
    RawProviderError {
        status: e.status().map(|s| s.as_u16()),
        // Tag transport failures so they classify as retryable network errors.
        message: format!("network error: {e}"),
        ..Default::default()
    }
}

fn raw_from_http(status: u16, body: &str, headers: HashMap<String, String>) -> RawProviderError {
    let v: Value = decode_anthropic_response(body).unwrap_or(Value::Null);
    let err = v.get("error");
    RawProviderError {
        status: Some(status),
        code: err
            .and_then(|e| e.get("code"))
            .or_else(|| v.get("code"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        message: err
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
            .map(diagnostic_string)
            .or_else(|| {
                if body.is_empty() {
                    None
                } else {
                    Some(body.to_string())
                }
            })
            .unwrap_or_default(),
        error_type: v.get("type").and_then(Value::as_str).map(diagnostic_string),
        body_type: err
            .and_then(|e| e.get("type"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        request_id: v
            .get("request_id")
            .and_then(Value::as_str)
            .map(diagnostic_string),
        error_request_id: err
            .and_then(|e| e.get("request_id"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        headers,
        ..Default::default()
    }
}

fn raw_from_sse(json: &Value) -> RawProviderError {
    let err = json.get("error");
    RawProviderError {
        code: err
            .and_then(|e| e.get("code"))
            .or_else(|| json.get("code"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        message: err
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
            .map(diagnostic_string)
            .unwrap_or_default(),
        error_type: json
            .get("type")
            .and_then(Value::as_str)
            .map(diagnostic_string),
        body_type: err
            .and_then(|e| e.get("type"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        request_id: json
            .get("request_id")
            .and_then(Value::as_str)
            .map(diagnostic_string),
        error_request_id: err
            .and_then(|e| e.get("request_id"))
            .and_then(Value::as_str)
            .map(diagnostic_string),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// SSE decoding
// ---------------------------------------------------------------------------

/// Accumulates response bytes and splits complete SSE events on a blank line.
struct SseDecoder {
    buf: Vec<u8>,
    decoder: WhatwgUtf8,
}

impl SseDecoder {
    fn new() -> Self {
        SseDecoder {
            buf: Vec::new(),
            decoder: WhatwgUtf8::new(),
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Pop the next complete event block (the bytes up to the next blank line),
    /// or `None` if no complete block is buffered yet.
    fn next_block(&mut self) -> Option<RuntimeString> {
        let (idx, sep_len) = find_separator(&self.buf)?;
        let framed: Vec<u8> = self.buf.drain(..idx + sep_len).collect();
        let decoded = self.decoder.feed(&framed);
        Some(decoded.slice_utf16(..decoded.len_utf16().saturating_sub(sep_len)))
    }
}

/// Find the earliest blank-line separator (`\r\n\r\n` or `\n\n`), returning its
/// start offset and length.
fn find_separator(buf: &[u8]) -> Option<(usize, usize)> {
    let n = buf.len();
    let mut i = 0;
    while i < n {
        if i + 4 <= n && &buf[i..i + 4] == b"\r\n\r\n" {
            return Some((i, 4));
        }
        if i + 2 <= n && &buf[i..i + 2] == b"\n\n" {
            return Some((i, 2));
        }
        i += 1;
    }
    None
}

/// Extract and JSON-parse the concatenated `data:` payload of one event block.
/// Returns `None` when the block has no data or the payload isn't JSON.
fn parse_sse_data(block: &RuntimeString) -> Option<Value> {
    let mut data = Vec::new();
    let mut has = false;
    for mut line in block.as_utf16().split(|unit| *unit == u16::from(b'\n')) {
        if line.last() == Some(&u16::from(b'\r')) {
            line = &line[..line.len() - 1];
        }
        const DATA_PREFIX: [u16; 5] = [
            b'd' as u16,
            b'a' as u16,
            b't' as u16,
            b'a' as u16,
            b':' as u16,
        ];
        if let Some(mut rest) = line.strip_prefix(&DATA_PREFIX) {
            if rest.first() == Some(&u16::from(b' ')) {
                rest = &rest[1..];
            }
            if has {
                data.push(u16::from(b'\n'));
            }
            data.extend_from_slice(rest);
            has = true;
        }
    }
    if !has {
        return None;
    }
    runtime_json::parse(&RuntimeString::from_utf16(data)).ok()
}

// ---------------------------------------------------------------------------
// Raw-event → normalized-event mapping (pure)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
enum BlockKind {
    ToolUse,
    #[default]
    Text,
    Thinking,
    RedactedThinking,
    StructuredTool,
    ServerSearch,
}

#[derive(Default)]
struct BlockInfo {
    kind: BlockKind,
    id: Option<RuntimeString>,
    /// Accumulated thinking text (kind == Thinking), delivered as a complete
    /// `thinking_block` at content_block_stop.
    thinking_text: Option<RuntimeString>,
    signature: Option<RuntimeString>,
    /// Opaque encrypted payload (kind == RedactedThinking).
    data: Option<RuntimeString>,
    /// Accumulated input JSON for a server-search block (the `{query}`).
    json: Option<RuntimeString>,
}

/// Extract the server's applied context-edit report from a stream event, when
/// one is present. Streaming responses carry `context_management` on the final
/// `message_delta`; `message_start`'s message is checked as well so a shape
/// change cannot silently drop the accounting. Returns
/// `(cleared_tool_uses, cleared_input_tokens, cleared_thinking_turns)`.
fn parse_applied_context_edits(event: &Value) -> Option<(u64, u64, u64)> {
    let management = event.get("context_management").or_else(|| {
        event
            .get("message")
            .and_then(|m| m.get("context_management"))
    })?;
    let edits = management.get("applied_edits")?.as_array()?;
    let (mut uses, mut tokens, mut turns) = (0u64, 0u64, 0u64);
    let mut any = false;
    let empty = RuntimeString::default();
    for edit in edits {
        let kind = edit.get("type").and_then(Value::as_str).unwrap_or(&empty);
        let cleared_tokens = edit
            .get("cleared_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if kind == CLEAR_TOOL_USES_EDIT {
            uses += edit
                .get("cleared_tool_uses")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            tokens += cleared_tokens;
            any = true;
        } else if kind == CLEAR_THINKING_EDIT {
            turns += edit
                .get("cleared_thinking_turns")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            tokens += cleared_tokens;
            any = true;
        }
    }
    any.then_some((uses, tokens, turns))
}

/// Merge only fields the provider actually reported; later events can extend
/// earlier input/cache counts without erasing fields absent from a delta.
fn update_stream_usage(event: &Value, usage: &mut Usage) -> bool {
    let source = match event.get("type").and_then(Value::as_str) {
        Some(kind) if kind == "message_start" => event
            .get("message")
            .and_then(|message| message.get("usage")),
        Some(kind) if kind == "message_delta" => event.get("usage"),
        _ => None,
    };
    let Some(source) = source else {
        return false;
    };
    let mut reported = false;
    for (field, target) in [
        ("input_tokens", &mut usage.input_tokens),
        ("output_tokens", &mut usage.output_tokens),
        (
            "cache_read_input_tokens",
            &mut usage.cache_read_input_tokens,
        ),
        (
            "cache_creation_input_tokens",
            &mut usage.cache_creation_input_tokens,
        ),
    ] {
        if let Some(value) = source.get(field).and_then(Value::as_u64) {
            *target = value;
            reported = true;
        }
    }
    reported
}

#[derive(Default)]
struct HandledEvent {
    events: Vec<ModelStreamEvent>,
    stop_reason: Option<ModelStopReason>,
}

/// Summary provenance belongs to a known direct provider endpoint and the
/// actual model announced by that request, never a user-selected alias alone.
fn is_native_summary_endpoint(endpoint: &str) -> bool {
    url::Url::parse(endpoint).ok().is_some_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some("api.anthropic.com")
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/v1/messages"
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn model_returns_thinking_summaries(model: &RuntimeString) -> bool {
    if model.len_utf16() > 128 {
        return false;
    }
    let Ok(model) = model.try_to_utf8() else {
        return false;
    };
    let normalized = model.strip_suffix("[1m]").unwrap_or(&model);
    let Some(rest) = normalized.strip_prefix("claude-") else {
        return false;
    };
    let fields = rest.split('-').collect::<Vec<_>>();
    if !(2..=6).contains(&fields.len()) {
        return false;
    }
    let family = |name: &str| matches!(name, "opus" | "sonnet" | "haiku");
    let (major, suffix) = if family(fields[0]) {
        (fields[1], &fields[2..])
    } else if family(fields[1]) {
        (fields[0], &fields[2..])
    } else {
        return false;
    };
    if major.len() > 2
        || major.starts_with('0')
        || !major.bytes().all(|byte| byte.is_ascii_digit())
        || !major.parse::<u8>().is_ok_and(|major| major >= 4)
    {
        return false;
    }
    suffix.iter().enumerate().all(|(index, field)| {
        (*field == "latest" && index + 1 == suffix.len())
            || (!field.is_empty()
                && field.len() <= 8
                && field.bytes().all(|byte| byte.is_ascii_digit()))
    })
}

#[cfg(test)]
fn model_supports_deferred_tool_loading(model: &RuntimeString) -> bool {
    super::model_catalog::native_deferred_tools_default("anthropic", model)
}

struct ThinkingSummaryState {
    native_source: bool,
    model_summary: bool,
}
impl ThinkingSummaryState {
    fn new(native_source: bool) -> Self {
        Self {
            native_source,
            model_summary: false,
        }
    }
    fn observe(&mut self, event: &Value) {
        if event
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == "message_start")
        {
            self.model_summary = self.native_source
                && event
                    .get("message")
                    .and_then(|message| message.get("model"))
                    .and_then(Value::as_str)
                    .is_some_and(model_returns_thinking_summaries);
        }
    }
    fn completed_summary(&self, text: &RuntimeString) -> bool {
        self.model_summary && text.try_to_utf8().is_ok_and(|text| !text.trim().is_empty())
    }
}

/// Map one raw stream event onto zero or more normalized events, threading block
/// bookkeeping, actual-model provenance and structured-output capture.
fn handle_event(
    ev: &Value,
    blocks: &mut HashMap<usize, BlockInfo>,
    structured: bool,
    structured_json: &mut RuntimeString,
    thinking_summary: &mut ThinkingSummaryState,
) -> HandledEvent {
    thinking_summary.observe(ev);
    let empty = RuntimeString::default();
    let ty = ev.get("type").and_then(Value::as_str).unwrap_or(&empty);
    match ty {
        ty if ty == "message_start" => {
            let model = ev
                .get("message")
                .and_then(|m| m.get("model"))
                .and_then(Value::as_str)
                .cloned()
                .unwrap_or_default();
            HandledEvent {
                events: vec![ModelStreamEvent::MessageStart { model }],
                ..Default::default()
            }
        }
        ty if ty == "content_block_start" => {
            let index = ev.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
            let cb = ev.get("content_block");
            let cbt = cb
                .and_then(|c| c.get("type"))
                .and_then(Value::as_str)
                .unwrap_or(&empty);
            let str_field = |key: &str| {
                cb.and_then(|c| c.get(key))
                    .and_then(Value::as_str)
                    .cloned()
                    .unwrap_or_default()
            };
            match cbt {
                cbt if cbt == "tool_use" => {
                    let id = str_field("id");
                    let name = str_field("name");
                    // The structured-output tool is internal — capture its input
                    // but do not surface a tool call to the engine.
                    if structured && name == STRUCTURED_OUTPUT_TOOL {
                        blocks.insert(
                            index,
                            BlockInfo {
                                kind: BlockKind::StructuredTool,
                                id: Some(id),
                                ..Default::default()
                            },
                        );
                        return HandledEvent::default();
                    }
                    blocks.insert(
                        index,
                        BlockInfo {
                            kind: BlockKind::ToolUse,
                            id: Some(id.clone()),
                            ..Default::default()
                        },
                    );
                    HandledEvent {
                        events: vec![ModelStreamEvent::ToolUseStart {
                            id,
                            name,
                            thought_signature: None,
                            tool_kind: None,
                            input_format: None,
                        }],
                        ..Default::default()
                    }
                }
                cbt if cbt == "thinking" => {
                    blocks.insert(
                        index,
                        BlockInfo {
                            kind: BlockKind::Thinking,
                            thinking_text: Some(RuntimeString::default()),
                            ..Default::default()
                        },
                    );
                    HandledEvent::default()
                }
                cbt if cbt == "redacted_thinking" => {
                    blocks.insert(
                        index,
                        BlockInfo {
                            kind: BlockKind::RedactedThinking,
                            data: Some(str_field("data")),
                            ..Default::default()
                        },
                    );
                    HandledEvent::default()
                }
                cbt if cbt == "server_tool_use" => {
                    blocks.insert(
                        index,
                        BlockInfo {
                            kind: BlockKind::ServerSearch,
                            id: Some(str_field("id")),
                            json: Some(RuntimeString::default()),
                            ..Default::default()
                        },
                    );
                    HandledEvent::default()
                }
                cbt if cbt == "web_search_tool_result" => {
                    let id = str_field("tool_use_id");
                    let (results, error) = map_web_search_result(cb.and_then(|c| c.get("content")));
                    HandledEvent {
                        events: vec![ModelStreamEvent::WebSearchResult { id, results, error }],
                        ..Default::default()
                    }
                }
                _ => {
                    blocks.insert(
                        index,
                        BlockInfo {
                            kind: BlockKind::Text,
                            ..Default::default()
                        },
                    );
                    HandledEvent::default()
                }
            }
        }
        ty if ty == "content_block_delta" => {
            let index = ev.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
            let d = ev.get("delta");
            let dt = d
                .and_then(|d| d.get("type"))
                .and_then(Value::as_str)
                .unwrap_or(&empty);
            let str_field = |key: &str| {
                d.and_then(|d| d.get(key))
                    .and_then(Value::as_str)
                    .cloned()
                    .unwrap_or_default()
            };
            match dt {
                dt if dt == "text_delta" => HandledEvent {
                    events: vec![ModelStreamEvent::TextDelta {
                        text: str_field("text"),
                    }],
                    ..Default::default()
                },
                dt if dt == "thinking_delta" => {
                    let think = str_field("thinking");
                    if let Some(info) = blocks.get_mut(&index) {
                        let current = info
                            .thinking_text
                            .get_or_insert_with(RuntimeString::default);
                        *current = current.concat(&think);
                    }
                    HandledEvent {
                        events: vec![ModelStreamEvent::ThinkingDelta { text: think }],
                        ..Default::default()
                    }
                }
                dt if dt == "signature_delta" => {
                    let sig = str_field("signature");
                    if let Some(info) = blocks.get_mut(&index) {
                        let current = info.signature.get_or_insert_with(RuntimeString::default);
                        *current = current.concat(&sig);
                    }
                    HandledEvent::default()
                }
                dt if dt == "citations_delta" => {
                    let c = d.and_then(|d| d.get("citation"));
                    let ct = c
                        .and_then(|c| c.get("type"))
                        .and_then(Value::as_str)
                        .unwrap_or(&empty);
                    if ct == "web_search_result_location" {
                        let s =
                            |key: &str| c.and_then(|c| c.get(key)).and_then(Value::as_str).cloned();
                        let citation = Citation {
                            url: s("url").unwrap_or_default(),
                            title: s("title"),
                            cited_text: s("cited_text"),
                            start_index: None,
                            end_index: None,
                            encrypted_index: s("encrypted_index"),
                        };
                        HandledEvent {
                            events: vec![ModelStreamEvent::Citation { citation }],
                            ..Default::default()
                        }
                    } else {
                        HandledEvent::default()
                    }
                }
                dt if dt == "input_json_delta" => {
                    let pj = str_field("partial_json");
                    match blocks.get_mut(&index) {
                        Some(info) if matches!(info.kind, BlockKind::StructuredTool) => {
                            *structured_json = structured_json.concat(&pj);
                            HandledEvent::default()
                        }
                        Some(info) if matches!(info.kind, BlockKind::ServerSearch) => {
                            let current = info.json.get_or_insert_with(RuntimeString::default);
                            *current = current.concat(&pj);
                            HandledEvent::default()
                        }
                        Some(info) if info.id.is_some() => HandledEvent {
                            events: vec![ModelStreamEvent::ToolUseInputDelta {
                                id: info.id.clone().unwrap(),
                                partial_json: pj,
                            }],
                            ..Default::default()
                        },
                        _ => HandledEvent::default(),
                    }
                }
                _ => HandledEvent::default(),
            }
        }
        ty if ty == "content_block_stop" => {
            let index = ev.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
            match blocks.get(&index) {
                Some(info) if matches!(info.kind, BlockKind::ToolUse) && info.id.is_some() => {
                    HandledEvent {
                        events: vec![ModelStreamEvent::ToolUseStop {
                            id: info.id.clone().unwrap(),
                        }],
                        ..Default::default()
                    }
                }
                Some(info) if matches!(info.kind, BlockKind::Thinking) => {
                    let text = info.thinking_text.clone().unwrap_or_default();
                    HandledEvent {
                        events: vec![ModelStreamEvent::ThinkingBlock {
                            is_summary: thinking_summary.completed_summary(&text),
                            text,
                            signature: info.signature.clone().filter(|s| !s.is_empty()),
                            encrypted_content: None,
                        }],
                        ..Default::default()
                    }
                }
                Some(info) if matches!(info.kind, BlockKind::RedactedThinking) => HandledEvent {
                    events: vec![ModelStreamEvent::RedactedThinkingBlock {
                        data: info.data.clone().unwrap_or_default(),
                    }],
                    ..Default::default()
                },
                Some(info) if matches!(info.kind, BlockKind::ServerSearch) => {
                    let mut query = RuntimeString::default();
                    let raw = info.json.clone().unwrap_or_else(|| "{}".into());
                    if let Ok(v) = runtime_json::parse(&raw) {
                        if let Some(q) = v.get("query").and_then(Value::as_str) {
                            query = q.clone();
                        }
                    }
                    HandledEvent {
                        events: vec![ModelStreamEvent::ServerSearchQuery {
                            id: info.id.clone().unwrap_or_default(),
                            query,
                            action: None,
                        }],
                        ..Default::default()
                    }
                }
                _ => HandledEvent::default(),
            }
        }
        ty if ty == "message_delta" => {
            let mut out = HandledEvent::default();
            if let Some(sr) = ev
                .get("delta")
                .and_then(|d| d.get("stop_reason"))
                .and_then(Value::as_str)
            {
                out.stop_reason = Some(map_stop_reason(sr));
            }
            out
        }
        // message_stop and any future event types: nothing to surface.
        _ => HandledEvent::default(),
    }
}

/// Map a `web_search_tool_result` content into neutral results + an optional error.
fn map_web_search_result(
    content: Option<&Value>,
) -> (Vec<NativeSearchResult>, Option<RuntimeString>) {
    match content {
        Some(Value::Array(arr)) => {
            let results = arr
                .iter()
                .map(|r| NativeSearchResult {
                    title: r
                        .get("title")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_default(),
                    url: r
                        .get("url")
                        .and_then(Value::as_str)
                        .cloned()
                        .unwrap_or_default(),
                    encrypted_token: r.get("encrypted_content").and_then(Value::as_str).cloned(),
                    page_age: r.get("page_age").and_then(Value::as_str).cloned(),
                })
                .collect();
            (results, None)
        }
        Some(Value::Object(o)) => (vec![], o.get("error_code").and_then(Value::as_str).cloned()),
        _ => (vec![], None),
    }
}

/// All stop reasons mapped precisely; a server-tool `pause_turn` is a natural
/// turn boundary (the engine re-sends to resume).
fn map_stop_reason(sr: &RuntimeString) -> ModelStopReason {
    match sr {
        sr if sr == "tool_use" => ModelStopReason::ToolUse,
        sr if sr == "max_tokens" => ModelStopReason::MaxTokens,
        sr if sr == "stop_sequence" => ModelStopReason::StopSequence,
        sr if sr == "refusal" => ModelStopReason::Refusal,
        sr if sr == "pause_turn" => ModelStopReason::EndTurn,
        _ => ModelStopReason::EndTurn,
    }
}

// ---------------------------------------------------------------------------
// Request assembly (pure)
// ---------------------------------------------------------------------------

/// Build the streaming request body: system parts, converted messages, tools +
/// tool_choice (or the forced structured-output tool), and cache breakpoints.
#[cfg(test)]
fn build_params(
    req: &ModelRequest,
    structured: bool,
    cache_scope_enabled: bool,
    use_native_structured: bool,
    require_claude_code_identity: bool,
) -> Map {
    build_params_with_media_limits(
        req,
        structured,
        cache_scope_enabled,
        false,
        StructuredOutputMode {
            native: use_native_structured,
            review_tool_loop: false,
            auto_result_tool: false,
        },
        require_claude_code_identity,
        ImageLimits::default(),
        MediaBudget::for_anthropic(true, None),
    )
    .expect("request assembly fixture must contain valid image payloads")
}

#[derive(Clone, Copy)]
struct StructuredOutputMode {
    native: bool,
    review_tool_loop: bool,
    auto_result_tool: bool,
}

#[allow(clippy::too_many_arguments)]
fn build_params_with_media_limits(
    req: &ModelRequest,
    structured: bool,
    cache_scope_enabled: bool,
    defer_loading_supported: bool,
    output_mode: StructuredOutputMode,
    require_claude_code_identity: bool,
    image_limits: ImageLimits,
    media_budget: MediaBudget,
) -> Result<Map, ImagePayloadError> {
    let mut system: Vec<Value> = req
        .system
        .iter()
        .map(|p| vobj(vec![("type", vs("text")), ("text", vs(p.text.clone()))]))
        .collect();
    let mut messages = to_api_messages(&req.messages, &diagnostic_string(&req.model), &req.tools);
    // Modern Claude models reject assistant message prefill (400 when the final
    // message has role="assistant"). Ensure empty-input continuations after a
    // partial assistant turn always end on a user message.
    if messages
        .last()
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        .is_some_and(|role| role == "assistant")
    {
        messages.push(vobj(vec![
            ("role", vs("user")),
            (
                "content",
                Value::Array(vec![vobj(vec![
                    ("type", vs("text")),
                    (
                        "text",
                        vs("<system-reminder>\nContinue from where you left off without repeating content already shown to the user.\n</system-reminder>"),
                    ),
                ])]),
            ),
        ]));
    }
    // The final per-image ceiling precedes aggregate shedding. Otherwise an
    // invalid historical image could be silently discarded instead of refused.
    validate_final_images(&messages, image_limits.max_base64_chars)?;
    if let std::borrow::Cow::Owned(trimmed) = trim_media(&messages, media_budget) {
        messages = trimmed;
    }

    let mut tools: Vec<Value> = req.tools.iter().map(to_api_tool).collect();
    let mut tool_choice: Option<Value> = req.tool_choice.as_ref().map(to_api_tool_choice);
    let mut structured_replacement = false;

    if let Some(schema) = req
        .output_schema
        .as_ref()
        .filter(|_| structured && !output_mode.native)
    {
        let result_tool = vobj(vec![
            ("name", vs(STRUCTURED_OUTPUT_TOOL)),
            (
                "description",
                vs("Emit the final result as a structured object matching the schema."),
            ),
            ("input_schema", project_anthropic_tool_input_schema(schema)),
        ]);
        if output_mode.review_tool_loop {
            tools.push(result_tool);
            tool_choice = Some(vobj(vec![("type", vs("auto"))]));
        } else {
            structured_replacement = true;
            tools = vec![result_tool];
            tool_choice = Some(if output_mode.auto_result_tool {
                vobj(vec![("type", vs("auto"))])
            } else {
                vobj(vec![
                    ("type", vs("tool")),
                    ("name", vs(STRUCTURED_OUTPUT_TOOL)),
                ])
            });
        }
    } else if let Some(ws) = &req.web_search {
        // Native server web search: the hosted tool rides in the tools array,
        // appended after the client tools so it stays in the cacheable prefix.
        tools.push(build_web_search_tool(ws));
    }

    let ttl = req.cache_ttl == Some(CacheTtl::OneHour);
    apply_cache_breakpoints(
        &req.system,
        &mut system,
        &mut tools,
        &mut messages,
        req.skip_cache_write.unwrap_or(false),
        ttl,
        cache_scope_enabled,
    );

    // Native deferred tool loading: not-yet-surfaced definitions ride along
    // marked `defer_loading` so a supporting model can pull them in without
    // paying their context cost up front. Appended AFTER cache breakpoints so
    // the breakpoint stays on the last immediate tool and the cached prefix
    // does not shift. Skipped when a forced structured-output tool replaced
    // the whole inventory.
    if defer_loading_supported && !req.deferred_tools.is_empty() && !structured_replacement {
        if tools.is_empty() {
            // Nothing immediate to bootstrap from: promote the deferred
            // definitions instead of sending an all-deferred inventory.
            tools.extend(req.deferred_tools.iter().map(to_api_tool));
        } else {
            tools.extend(req.deferred_tools.iter().map(|spec| {
                let mut tool = to_api_tool(spec);
                if let Some(object) = tool.as_object_mut() {
                    object.insert("defer_loading", Value::Bool(true));
                }
                tool
            }));
        }
    }

    // OAuth subscription premium-model spoof gate: lead with the verbatim Claude
    // Code identity as its own first block. Prepended AFTER cache breakpoints so
    // the static, uncached identity does not shift the cacheable prefix that
    // follows. Skipped when the request already leads with it (idempotent).
    if require_claude_code_identity {
        let already_present = matches!(
            system.first(),
            Some(Value::Object(m))
                if matches!(
                    m.get("text"),
                    Some(Value::String(s)) if s == CLAUDE_CODE_SYSTEM_IDENTITY
                )
        );
        if !already_present {
            system.insert(
                0,
                vobj(vec![
                    ("type", vs("text")),
                    ("text", vs(CLAUDE_CODE_SYSTEM_IDENTITY)),
                ]),
            );
        }
    }

    let mut map = Map::new();
    map.insert("model", vs(req.model.clone()));
    map.insert("max_tokens", Value::from(req.max_output_tokens));
    map.insert("stream", Value::Bool(true));
    map.insert("system", Value::Array(system));
    map.insert("messages", Value::Array(messages));
    if !tools.is_empty() {
        map.insert("tools", Value::Array(tools));
    }
    if let Some(tc) = tool_choice {
        map.insert("tool_choice", tc);
    }
    Ok(map)
}

fn build_output_config(
    req: &ModelRequest,
    capabilities: &ModelCapabilities,
    use_native: bool,
    is_first_party: bool,
) -> Map {
    let mut output_config = Map::new();
    if let Some(level) = req
        .effort
        .clone()
        .and_then(|effort| resolve_effort_level(effort, capabilities))
    {
        output_config.insert("effort", vs(level.as_str()));
    }
    if use_native {
        if let Some(schema) = &req.output_schema {
            output_config.insert(
                "format",
                vobj(vec![
                    ("type", vs("json_schema")),
                    ("schema", schema.clone()),
                ]),
            );
        }
    }
    if is_first_party && capabilities.task_budgets {
        if let Some(total) = req.task_budget {
            output_config.insert(
                "task_budget",
                vobj(vec![
                    ("type", vs("tokens")),
                    ("total", Value::from(total.max(TASK_BUDGET_MIN))),
                ]),
            );
        }
    }
    output_config
}

fn to_api_tool(t: &ToolSpec) -> Value {
    vobj(vec![
        ("name", vs(t.name.clone())),
        ("description", vs(t.description.clone())),
        (
            "input_schema",
            project_anthropic_tool_input_schema(&t.input_schema),
        ),
    ])
}

/// Build the native web-search server tool from the neutral request.
fn build_web_search_tool(ws: &WebSearchRequest) -> Value {
    let mut m = Map::new();
    m.insert("type", vs("web_search_20250305"));
    m.insert("name", vs("web_search"));
    m.insert("max_uses", Value::from(ws.max_uses.unwrap_or(8)));
    // allowed and blocked domains are mutually exclusive (allow wins).
    if ws.allowed_domains.as_ref().is_some_and(|d| !d.is_empty()) {
        m.insert(
            "allowed_domains",
            Value::Array(
                ws.allowed_domains
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|d| vs(d.clone()))
                    .collect(),
            ),
        );
    } else if ws.blocked_domains.as_ref().is_some_and(|d| !d.is_empty()) {
        m.insert(
            "blocked_domains",
            Value::Array(
                ws.blocked_domains
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|d| vs(d.clone()))
                    .collect(),
            ),
        );
    }
    if let Some(loc) = &ws.user_location {
        let mut l = Map::new();
        l.insert("type", vs("approximate"));
        if let Some(c) = &loc.city {
            l.insert("city", vs(c.clone()));
        }
        if let Some(r) = &loc.region {
            l.insert("region", vs(r.clone()));
        }
        if let Some(c) = &loc.country {
            l.insert("country", vs(c.clone()));
        }
        if let Some(t) = &loc.timezone {
            l.insert("timezone", vs(t.clone()));
        }
        m.insert("user_location", Value::Object(l));
    }
    Value::Object(m)
}

fn to_api_tool_choice(tc: &ToolChoice) -> Value {
    match tc {
        ToolChoice::Auto => vobj(vec![("type", vs("auto"))]),
        ToolChoice::Any => vobj(vec![("type", vs("any"))]),
        ToolChoice::Tool { name } => vobj(vec![("type", vs("tool")), ("name", vs(name.clone()))]),
        ToolChoice::None => vobj(vec![("type", vs("none"))]),
    }
}

/// Build a cache_control breakpoint with an optional 1h TTL + global scope.
fn cache_control_for(ttl: bool, scope: bool) -> Value {
    let mut m = Map::new();
    m.insert("type", vs("ephemeral"));
    if ttl {
        m.insert("ttl", vs("1h"));
    }
    if scope {
        m.insert("scope", vs("global"));
    }
    Value::Object(m)
}

fn set_cache_control(block: &mut Value, cache_control: Value) {
    if let Value::Object(m) = block {
        m.insert("cache_control", cache_control);
    }
}

/// Where the message-level breakpoint goes: the last markable block of the final
/// message (or the second-to-last message on a fire-and-forget fork). Walks back
/// past trailing thinking / redacted_thinking blocks (which can't carry it).
fn message_cache_target(messages: &[Value], skip: bool) -> Option<(usize, usize)> {
    let len = messages.len() as isize;
    let target_index = if skip { len - 2 } else { len - 1 };
    if target_index < 0 {
        return None;
    }
    let ti = target_index as usize;
    let content = messages.get(ti)?.get("content")?.as_array()?;
    if content.is_empty() {
        return None;
    }
    let mut bi = content.len() as isize - 1;
    while bi >= 0 {
        let t = content[bi as usize].get("type").and_then(Value::as_str);
        if !t.is_some_and(|tag| tag == "thinking" || tag == "redacted_thinking") {
            break;
        }
        bi -= 1;
    }
    if bi < 0 || !content[bi as usize].is_object() {
        return None;
    }
    Some((ti, bi as usize))
}

/// Setter targets in low→high priority order (we keep the last N).
enum CacheTarget {
    System(usize),
    Tool(usize),
    Message(usize, usize),
}

/// Set cache breakpoints across the stable prefix (system + tools) and the
/// conversation up to the final message, budgeted to ≤4 total; a leading run of
/// `scope:'global'` parts reserves one always-kept breakpoint.
fn apply_cache_breakpoints(
    parts: &[SystemPromptPart],
    system: &mut [Value],
    tools: &mut [Value],
    messages: &mut [Value],
    skip_cache_write: bool,
    ttl: bool,
    cache_scope_enabled: bool,
) {
    // The leading run of global parts: one breakpoint on the LAST of them, always
    // kept (the stable prefix is the most valuable thing to pin).
    let mut last_global_idx: isize = -1;
    for (i, p) in parts.iter().enumerate() {
        if p.cache_scope == Some(CacheScope::Global) && i < system.len() {
            last_global_idx = i as isize;
        } else {
            break;
        }
    }
    let has_global = last_global_idx >= 0;

    let mut targets: Vec<CacheTarget> = Vec::new();

    // Caller `cacheable` hints (a part in the global prefix is handled below).
    for (idx, p) in parts.iter().enumerate() {
        let in_global_prefix =
            (idx as isize) <= last_global_idx && p.cache_scope == Some(CacheScope::Global);
        if p.cacheable == Some(true) && idx < system.len() && !in_global_prefix {
            targets.push(CacheTarget::System(idx));
        }
    }

    // Fallback: if NO part is flagged cacheable and there's no global prefix,
    // cache the last system block.
    let any_cacheable = parts
        .iter()
        .enumerate()
        .any(|(idx, p)| p.cacheable == Some(true) && idx < system.len());
    if !any_cacheable && last_global_idx < 0 && !system.is_empty() {
        targets.push(CacheTarget::System(system.len() - 1));
    }

    // After the tools array (on the last tool definition).
    if !tools.is_empty() {
        targets.push(CacheTarget::Tool(tools.len() - 1));
    }

    // Last content block of the final message (highest priority).
    if let Some((mi, bi)) = message_cache_target(messages, skip_cache_write) {
        targets.push(CacheTarget::Message(mi, bi));
    }

    // The reserved global breakpoint takes one budget slot; the rest split the rest.
    let budget = MAX_CACHE_BREAKPOINTS.saturating_sub(if has_global { 1 } else { 0 });
    let start = targets.len().saturating_sub(budget);
    for t in &targets[start..] {
        match *t {
            CacheTarget::System(i) => {
                set_cache_control(&mut system[i], cache_control_for(ttl, false))
            }
            CacheTarget::Tool(i) => set_cache_control(&mut tools[i], cache_control_for(ttl, false)),
            CacheTarget::Message(mi, bi) => {
                if let Some(content) = messages[mi]
                    .get_mut("content")
                    .and_then(Value::as_array_mut)
                {
                    if let Some(block) = content.get_mut(bi) {
                        set_cache_control(block, cache_control_for(ttl, false));
                    }
                }
            }
        }
    }
    if has_global {
        let gi = last_global_idx as usize;
        set_cache_control(&mut system[gi], cache_control_for(ttl, cache_scope_enabled));
    }
}

// ---------------------------------------------------------------------------
// Message conversion (pure)
// ---------------------------------------------------------------------------

/// Strip the context-window beta suffix (e.g. `claude-opus-4-8[1m]`) so a model
/// produced under one beta and re-sent under another compares equal.
#[cfg(test)]
fn normalize_model_id(model: &str) -> String {
    use regex::Regex;
    use std::sync::OnceLock;
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[[^\]]*\]$").unwrap())
        .replace(model, "")
        .into_owned()
}

fn normalize_runtime_model_id(model: &RuntimeString) -> RuntimeString {
    let units = model.as_utf16();
    if units.last() != Some(&u16::from(b']')) {
        return model.clone();
    }
    match units.iter().rposition(|unit| *unit == u16::from(b'[')) {
        Some(start) => model.slice_utf16(..start),
        None => model.clone(),
    }
}

/// Drop signed thinking / redacted_thinking blocks from an assistant message
/// produced by a DIFFERENT model (signatures are model-bound). Thinking-only
/// messages become empty so `to_api_messages` omits them instead of fabricating
/// a synthetic assistant reply that triggers prefill rejection on continuation.
fn strip_stale_thinking(content: &[ContentBlock]) -> Vec<ContentBlock> {
    content
        .iter()
        .filter(|b| {
            !matches!(
                b,
                ContentBlock::Thinking(_) | ContentBlock::RedactedThinking(_)
            )
        })
        .cloned()
        .collect()
}

fn to_api_messages(messages: &[Message], target_model: &str, tools: &[ToolSpec]) -> Vec<Value> {
    let behavior_profile = resolve_provider_behavior_profile(
        Some(&ProviderInfo {
            name: "anthropic".into(),
            base_url: None,
            env_key: None,
            default_model: target_model.into(),
        }),
        &RuntimeString::from(target_model),
    );
    let mut out: Vec<Value> = Vec::new();
    let mut i = 0;
    while i < messages.len() {
        match &messages[i] {
            Message::ToolResult(_) => {
                // Coalesce a run of tool_result messages into ONE user message.
                let mut content: Vec<Value> = Vec::new();
                // Native media attached to a result is a SIBLING of the tool_result
                // in the same user turn — deferred to AFTER the whole coalesced run
                // (results must be contiguous at the front of the user message).
                let mut deferred: Vec<Value> = Vec::new();
                while i < messages.len() {
                    let Message::ToolResult(tr) = &messages[i] else {
                        break;
                    };
                    content.push(tool_result_msg_block(tr));
                    if let Some(extra) = &tr.extra_user_blocks {
                        for eb in extra {
                            deferred.extend(map_block(eb, tools));
                        }
                    }
                    i += 1;
                }
                content.extend(deferred);
                if !content.is_empty() {
                    out.push(vobj(vec![
                        ("role", vs("user")),
                        ("content", Value::Array(content)),
                    ]));
                }
                continue;
            }
            Message::User(u) => {
                let content = map_blocks(&u.content, tools);
                push_user_content(&mut out, content);
            }
            Message::Assistant(a) => {
                // A model switch leaves prior assistant turns carrying thinking
                // signatures bound to their producing model; drop those first.
                let stripped;
                let blocks: &[ContentBlock] = if normalize_runtime_model_id(&a.model)
                    != normalize_runtime_model_id(&RuntimeString::from(target_model))
                {
                    stripped = strip_stale_thinking(&a.content);
                    &stripped
                } else {
                    &a.content
                };
                let content = map_blocks(blocks, tools);
                // Coalesce consecutive assistant messages so roles alternate.
                let merged = out.last_mut().is_some_and(|last| {
                    if last
                        .get("role")
                        .and_then(Value::as_str)
                        .is_some_and(|role| role == "assistant")
                    {
                        if let Some(prev) = last.get_mut("content").and_then(Value::as_array_mut) {
                            prev.extend(content.clone());
                            return true;
                        }
                    }
                    false
                });
                if !merged && !content.is_empty() {
                    out.push(vobj(vec![
                        ("role", vs("assistant")),
                        ("content", Value::Array(content)),
                    ]));
                }
            }
            Message::CompactBoundary(cb) => {
                push_user_content(
                    &mut out,
                    vec![vobj(vec![
                        ("type", vs("text")),
                        (
                            "text",
                            render_anthropic_compact_boundary_exact(&cb.summary, &behavior_profile)
                                .into(),
                        ),
                    ])],
                );
            }
            Message::System(_) => {}
        }
        i += 1;
    }

    // The API rejects a request whose FINAL assistant message ends in a thinking /
    // redacted_thinking block — strip the trailing run from the last assistant
    // message, dropping the message completely if nothing visible remains.
    let mut pop_empty_last = false;
    if let Some(last) = out.last_mut() {
        if last
            .get("role")
            .and_then(Value::as_str)
            .is_some_and(|role| role == "assistant")
        {
            if let Some(content) = last.get_mut("content").and_then(Value::as_array_mut) {
                let mut end = content.len();
                while end > 0 {
                    let t = content[end - 1].get("type").and_then(Value::as_str);
                    if !t.is_some_and(|tag| tag == "thinking" || tag == "redacted_thinking") {
                        break;
                    }
                    end -= 1;
                }
                if end < content.len() {
                    if end > 0 {
                        content.truncate(end);
                    } else {
                        pop_empty_last = true;
                    }
                }
            }
        }
    }
    if pop_empty_last {
        out.pop();
    }
    out
}

#[cfg(test)]
pub(crate) fn render_anthropic_compact_boundary(
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

fn render_anthropic_compact_boundary_exact(
    summary: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    let (prefix, suffix) = match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => (
            format!(
                "<system-reminder>\n{}\n\nEarlier conversation summary:\n",
                crate::provider::behavior::COMPACT_BOUNDARY_HANDOFF
            ),
            "\n</system-reminder>".to_string(),
        ),
        ProviderPromptStyle::OpenAiDeveloper => (
            format!(
                "# Earlier Conversation Summary\n\n{}\n\n",
                crate::provider::behavior::COMPACT_BOUNDARY_HANDOFF
            ),
            String::new(),
        ),
        ProviderPromptStyle::Generic => (
            crate::provider::behavior::COMPACT_BOUNDARY_GENERIC_PREFIX.to_string(),
            String::new(),
        ),
    };
    RuntimeString::from(prefix)
        .concat(summary)
        .concat(&RuntimeString::from(suffix))
}

/// Append user content, coalescing into a preceding user message so roles
/// always alternate while keeping any `tool_result` blocks contiguous at the front.
fn push_user_content(out: &mut Vec<Value>, content: Vec<Value>) {
    if content.is_empty() {
        return;
    }
    let last_is_user = out.last().is_some_and(|last| {
        last.get("role")
            .and_then(Value::as_str)
            .is_some_and(|role| role == "user")
    });
    if last_is_user {
        if let Some(prev) = out
            .last_mut()
            .and_then(|l| l.get_mut("content"))
            .and_then(Value::as_array_mut)
        {
            let incoming_has_tool_result = content.iter().any(|b| {
                b.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|tag| tag == "tool_result")
            });
            if incoming_has_tool_result {
                let insert_at = prev
                    .iter()
                    .position(|b| {
                        b.get("type")
                            .and_then(Value::as_str)
                            .is_none_or(|tag| tag != "tool_result")
                    })
                    .unwrap_or(prev.len());
                let (results, others): (Vec<_>, Vec<_>) = content.into_iter().partition(|b| {
                    b.get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|tag| tag == "tool_result")
                });
                prev.splice(insert_at..insert_at, results);
                prev.extend(others);
            } else {
                prev.extend(content);
            }
            return;
        }
    }
    out.push(vobj(vec![
        ("role", vs("user")),
        ("content", Value::Array(content)),
    ]));
}

fn map_blocks(blocks: &[ContentBlock], tools: &[ToolSpec]) -> Vec<Value> {
    let mut out = Vec::new();
    for b in blocks {
        out.extend(map_block(b, tools));
    }
    out
}

/// Map an engine content block to zero or more API content blocks.
fn map_block(b: &ContentBlock, tools: &[ToolSpec]) -> Vec<Value> {
    match b {
        ContentBlock::Text(t) => {
            // Drop an empty text block on replay (the API 400s on a contentless
            // text block; a citations-only orphan anchor has no span anyway).
            if t.text.is_empty() {
                return vec![];
            }
            // Replay web-search citations (only those with the encrypted_index).
            let mut citations: Vec<Value> = Vec::new();
            if let Some(cs) = &t.citations {
                for c in cs {
                    if c.encrypted_index.is_none() {
                        continue;
                    }
                    citations.push(vobj(vec![
                        ("type", vs("web_search_result_location")),
                        ("url", vs(c.url.clone())),
                        ("title", c.title.clone().map(vs).unwrap_or(Value::Null)),
                        ("cited_text", vs(c.cited_text.clone().unwrap_or_default())),
                        ("encrypted_index", vs(c.encrypted_index.clone().unwrap())),
                    ]));
                }
            }
            let mut o = Map::new();
            o.insert("type", vs("text"));
            o.insert("text", vs(t.text.clone()));
            if !citations.is_empty() {
                o.insert("citations", Value::Array(citations));
            }
            vec![Value::Object(o)]
        }
        ContentBlock::Image(im) => vec![vobj(vec![
            ("type", vs("image")),
            (
                "source",
                vobj(vec![
                    ("type", vs("base64")),
                    ("media_type", vs(im.mime_type.clone())),
                    ("data", vs(im.base64.clone())),
                ]),
            ),
        ])],
        ContentBlock::Document(d) => vec![document_param(d)],
        // The stream admission rejects these before wire construction.
        ContentBlock::Media(_) => Vec::new(),
        ContentBlock::ToolUse(tu) => vec![vobj(vec![
            ("type", vs("tool_use")),
            ("id", vs(tu.id.clone())),
            ("name", vs(tu.name.clone())),
            ("input", objectified_tool_input(&tu.input, tools)),
        ])],
        ContentBlock::ToolResult(tr) => {
            let content: Vec<Value> = tr.content.iter().map(map_output_block).collect();
            let mut o = Map::new();
            o.insert("type", vs("tool_result"));
            o.insert("tool_use_id", vs(tr.tool_use_id.clone()));
            o.insert("content", Value::Array(content));
            if let Some(e) = tr.is_error {
                o.insert("is_error", Value::Bool(e));
            }
            vec![Value::Object(o)]
        }
        ContentBlock::Thinking(t) => {
            // Replay thinking ONLY when it carries BOTH its original text and a
            // signature (required for interleaved-thinking continuation). The
            // provider validates the signature against the thinking content, and
            // streams that never carried visible reasoning persist signed blocks
            // with empty text; replaying those makes the strict endpoint reject
            // the whole request with `Invalid 'signature' in 'thinking' block`.
            // Unsigned thinking is dropped as before.
            if !t.text.is_empty() {
                if let Some(sig) = &t.signature {
                    if !sig.is_empty() {
                        return vec![vobj(vec![
                            ("type", vs("thinking")),
                            ("thinking", vs(t.text.clone())),
                            ("signature", vs(sig.clone())),
                        ])];
                    }
                }
            }
            vec![]
        }
        ContentBlock::RedactedThinking(r) => vec![vobj(vec![
            ("type", vs("redacted_thinking")),
            ("data", vs(r.data.clone())),
        ])],
        ContentBlock::WebSearchCall(w) => {
            // Echo a native search as the server_tool_use + web_search_tool_result
            // pair so a follow-up turn can re-cite it. Replay needs every result's
            // encrypted_content; if any is missing (or it errored / was empty), drop.
            if w.error.is_some()
                || w.results.is_empty()
                || !w.results.iter().all(|r| r.encrypted_token.is_some())
            {
                return vec![];
            }
            let results: Vec<Value> = w
                .results
                .iter()
                .map(|r| {
                    let mut o = Map::new();
                    o.insert("type", vs("web_search_result"));
                    o.insert("title", vs(r.title.clone()));
                    o.insert("url", vs(r.url.clone()));
                    o.insert("encrypted_content", vs(r.encrypted_token.clone().unwrap()));
                    if let Some(pa) = &r.page_age {
                        o.insert("page_age", vs(pa.clone()));
                    }
                    Value::Object(o)
                })
                .collect();
            vec![
                vobj(vec![
                    ("type", vs("server_tool_use")),
                    ("id", vs(w.id.clone())),
                    ("name", vs("web_search")),
                    ("input", vobj(vec![("query", vs(w.query.clone()))])),
                ]),
                vobj(vec![
                    ("type", vs("web_search_tool_result")),
                    ("tool_use_id", vs(w.id.clone())),
                    ("content", Value::Array(results)),
                ]),
            ]
        }
    }
}

fn map_output_block(b: &OutputBlock) -> Value {
    match b {
        OutputBlock::Text(t) => vobj(vec![("type", vs("text")), ("text", vs(t.text.clone()))]),
        OutputBlock::Image(im) => vobj(vec![
            ("type", vs("image")),
            (
                "source",
                vobj(vec![
                    ("type", vs("base64")),
                    ("media_type", vs(im.mime_type.clone())),
                    ("data", vs(im.base64.clone())),
                ]),
            ),
        ]),
    }
}

fn tool_result_msg_block(tr: &ToolResultMessage) -> Value {
    let content: Vec<Value> = tr.content.iter().map(map_output_block).collect();
    vobj(vec![
        ("type", vs("tool_result")),
        ("tool_use_id", vs(tr.tool_use_id.clone())),
        ("content", Value::Array(content)),
        ("is_error", Value::Bool(tr.is_error)),
    ])
}

/// Map an engine document block to a native `document` content block. Only
/// base64 PDFs are produced today, so `media_type` is fixed to `application/pdf`.
fn document_param(d: &DocumentBlock) -> Value {
    let mut o = Map::new();
    o.insert("type", vs("document"));
    o.insert(
        "source",
        vobj(vec![
            ("type", vs("base64")),
            ("media_type", vs("application/pdf")),
            ("data", vs(d.base64.clone())),
        ]),
    );
    if let Some(f) = &d.filename {
        o.insert("title", vs(f.clone()));
    }
    Value::Object(o)
}

// ---------------------------------------------------------------------------
// Capability-driven request resolution (pure)
//
// The engine no longer keeps a model-id table: what a model supports is
// declared by the client and carried in `ModelCapabilities`. These helpers turn
// the request's effort/thinking intent into wire params, gated only by the
// declared capability.
// ---------------------------------------------------------------------------

/// Resolve a normalized effort value to a concrete API level, clamped to the
/// levels the client declared for this model. `None` when the model has no
/// effort knob (absent or empty effort capability).
fn resolve_effort_level(effort: EffortValue, caps: &ModelCapabilities) -> Option<EffortLevel> {
    let cap = caps.effort.as_ref()?;
    let requested = match effort {
        EffortValue::Number(n) => {
            if n <= 33 {
                EffortLevel::Low
            } else if n <= 66 {
                EffortLevel::Medium
            } else {
                EffortLevel::High
            }
        }
        EffortValue::Level(l) => l,
        EffortValue::Custom(_) => return None,
    };
    cap.clamp(requested)
}

/// Resolve thinking after selecting native or forced-tool structured output.
fn resolve_wire_thinking(
    req: &ModelRequest,
    caps: &ModelCapabilities,
    is_first_party: bool,
    forced_structured: bool,
) -> Option<Value> {
    if forced_structured {
        // Compatible endpoints may enable thinking by default. Turn it off only
        // when the selected model declares that shape. A model that requires
        // thinking must keep its own thinking mode; otherwise the forced tool
        // request is rejected before the title can be produced.
        if is_first_party {
            return None;
        }
        let thinking = caps.thinking.as_ref()?;
        if thinking.can_disable_thinking {
            return Some(vobj(vec![("type", vs("disabled"))]));
        }
        return thinking
            .adaptive
            .then(|| vobj(vec![("type", vs("adaptive"))]));
    }
    req.thinking
        .and_then(|thinking| resolve_thinking(thinking, caps, req.max_output_tokens))
}

/// Resolve enabled thinking only when the client declared the capability.
fn resolve_thinking(
    cfg: ThinkingConfig,
    caps: &ModelCapabilities,
    max_output_tokens: u64,
) -> Option<Value> {
    if matches!(cfg, ThinkingConfig::Disabled) {
        return None;
    }
    let think = caps.thinking.as_ref()?;
    if matches!(cfg, ThinkingConfig::Adaptive) && think.adaptive {
        return Some(vobj(vec![("type", vs("adaptive"))]));
    }
    let ceiling = std::cmp::max(1, max_output_tokens.saturating_sub(1));
    let requested = match cfg {
        ThinkingConfig::Enabled {
            budget_tokens: Some(b),
        } => b,
        _ => ceiling,
    };
    Some(vobj(vec![
        ("type", vs("enabled")),
        (
            "budget_tokens",
            Value::from(std::cmp::min(ceiling, requested)),
        ),
    ]))
}

/// Merge a beta flag into the `anthropic-beta` header without clobbering an
/// existing value (a comma-separated list; the lookup is case-insensitive).
fn merge_beta_header(headers: &mut HashMap<String, String>, beta: &str) {
    let key = headers
        .keys()
        .find(|k| k.to_lowercase() == "anthropic-beta")
        .cloned()
        .unwrap_or_else(|| "anthropic-beta".to_string());
    let existing = headers.get(&key).cloned().unwrap_or_default();
    let mut betas: Vec<String> = existing
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if !betas.iter().any(|b| b == beta) {
        betas.push(beta.to_string());
    }
    headers.insert(key, betas.join(","));
}

/// A boolean env kill-switch, truthy only for "1" / "true".
fn env_flag(name: &str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("1") | Ok("true"))
}

fn feature_disabled(
    frozen: Option<AnthropicRuntimeEnvironment>,
    flag: AnthropicFeatureFlag,
) -> bool {
    if let Some(frozen) = frozen {
        return match flag {
            AnthropicFeatureFlag::NativeStructuredOutput => {
                frozen.native_structured_output_disabled
            }
            AnthropicFeatureFlag::GlobalCacheScope => frozen.global_cache_scope_disabled,
            AnthropicFeatureFlag::InterleavedThinking => frozen.interleaved_thinking_disabled,
            AnthropicFeatureFlag::Context1m => frozen.context_1m_disabled,
            AnthropicFeatureFlag::ContextManagement => frozen.context_management_disabled,
        };
    }
    env_flag(match flag {
        AnthropicFeatureFlag::NativeStructuredOutput => "KLAUS_DISABLE_NATIVE_STRUCTURED_OUTPUT",
        AnthropicFeatureFlag::GlobalCacheScope => "KLAUS_DISABLE_GLOBAL_CACHE_SCOPE",
        AnthropicFeatureFlag::InterleavedThinking => "KLAUS_DISABLE_INTERLEAVED_THINKING",
        AnthropicFeatureFlag::Context1m => "KLAUS_DISABLE_CONTEXT_1M",
        AnthropicFeatureFlag::ContextManagement => "KLAUS_DISABLE_CONTEXT_MANAGEMENT",
    })
}

/// JavaScript UTF-16 code units/4 + safety buffer. There is no pre-request tokenizer.
pub fn estimate_tokens_fallback(req: &ModelRequest) -> u64 {
    let mut chars: usize = 0;
    for p in &req.system {
        chars += p.text.len_utf16();
    }
    for m in &req.messages {
        chars += runtime_codec::encode_value(m)
            .map(|value| runtime_json::stringify(&value).len_utf16())
            .unwrap_or(0);
    }
    for t in &req.tools {
        chars += t.name.len_utf16() + t.description.len_utf16();
        chars += runtime_json::stringify(&t.input_schema).len_utf16();
    }
    const SAFETY_BUFFER: u64 = 1_000;
    chars.div_ceil(4) as u64 + SAFETY_BUFFER
}

// ---------------------------------------------------------------------------
// Small JSON builders (unambiguous; no macro interpolation surprises)
// ---------------------------------------------------------------------------

fn vs(s: impl Into<RuntimeString>) -> Value {
    Value::String(s.into())
}

fn vobj(pairs: Vec<(&str, Value)>) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k, v);
    }
    Value::Object(m)
}

/// Which context-management edits this request is allowed to declare.
///
/// Context editing is a first-party capability and is suppressible by the frozen
/// runtime contract, so both gates apply to either edit. The thinking edit
/// additionally requires that this request actually carries thinking: a request
/// without it has no prior-turn reasoning for the server to keep or shed, and
/// declaring a policy anyway would only churn the cache key.
fn resolve_context_editing(
    req: &ModelRequest,
    first_party: bool,
    suppressed: bool,
    thinking_on_the_wire: bool,
) -> (
    Option<&ContextManagementConfig>,
    Option<ThinkingContextPolicy>,
) {
    if !first_party || suppressed {
        return (None, None);
    }
    (
        req.context_management.as_ref(),
        req.thinking_context.filter(|_| thinking_on_the_wire),
    )
}

/// Build the Anthropic `context_management` request object from the neutral
/// per-request declarations.
///
/// Emits up to two edits, the thinking edit first: the API requires
/// `clear_thinking_20251015` to lead the `edits` array whenever strategies
/// combine. The tool-use edit is driven by config: every threshold/limit is
/// optional and only serialized when set, so an unset field lets the server
/// apply its own default, and tool-name exclusions are forwarded verbatim. The
/// thinking edit is resolved per request: `KeepAll` pins every prior-turn
/// thinking block so no server-side default sheds reasoning the model may still
/// need, while `KeepMostRecentTurn` keeps exactly one — the wire schema
/// requires a positive count, and omitting the edit entirely would fall back to
/// the server default (usually keep-everything) and clear nothing.
///
/// Returns `None` when neither edit applies, so the caller can skip both the
/// request field and the beta header.
fn build_context_management(
    tool_uses: Option<&ContextManagementConfig>,
    thinking: Option<ThinkingContextPolicy>,
) -> Option<Value> {
    let mut edits: Vec<Value> = Vec::new();
    if let Some(policy) = thinking {
        let keep = match policy {
            ThinkingContextPolicy::KeepAll => vs("all"),
            ThinkingContextPolicy::KeepMostRecentTurn => vobj(vec![
                ("type", vs("thinking_turns")),
                ("value", Value::from(1u64)),
            ]),
        };
        edits.push(vobj(vec![
            ("type", vs(CLEAR_THINKING_EDIT)),
            ("keep", keep),
        ]));
    }
    if let Some(config) = tool_uses {
        edits.push(build_clear_tool_uses_edit(config));
    }
    if edits.is_empty() {
        return None;
    }
    Some(vobj(vec![("edits", Value::Array(edits))]))
}

/// The `clear_tool_uses_20250919` edit built from neutral config.
fn build_clear_tool_uses_edit(config: &ContextManagementConfig) -> Value {
    let mut edit = vec![("type", vs(CLEAR_TOOL_USES_EDIT))];
    // The count-based trigger wins when both forms are configured: it batches
    // clears against the tool-call count the client can observe exactly.
    if let Some(trigger) = config.trigger_tool_uses {
        edit.push((
            "trigger",
            vobj(vec![
                ("type", vs("tool_uses")),
                ("value", Value::from(trigger)),
            ]),
        ));
    } else if let Some(trigger) = config.trigger_input_tokens {
        edit.push((
            "trigger",
            vobj(vec![
                ("type", vs("input_tokens")),
                ("value", Value::from(trigger)),
            ]),
        ));
    }
    if let Some(keep) = config.keep_tool_uses {
        edit.push((
            "keep",
            vobj(vec![
                ("type", vs("tool_uses")),
                ("value", Value::from(keep)),
            ]),
        ));
    }
    if let Some(at_least) = config.clear_at_least_input_tokens {
        edit.push((
            "clear_at_least",
            vobj(vec![
                ("type", vs("input_tokens")),
                ("value", Value::from(at_least)),
            ]),
        ));
    }
    match &config.clear_tool_inputs {
        Some(ClearToolInputs::All(all)) => {
            edit.push(("clear_tool_inputs", Value::Bool(*all)));
        }
        Some(ClearToolInputs::Tools(names)) => {
            let names = names.iter().map(vs).collect::<Vec<_>>();
            edit.push(("clear_tool_inputs", Value::Array(names)));
        }
        None => {}
    }
    if !config.exclude_tools.is_empty() {
        let names = config.exclude_tools.iter().map(vs).collect::<Vec<_>>();
        edit.push(("exclude_tools", Value::Array(names)));
    }
    vobj(edit)
}

/// Rewrite a Messages request body for Vertex AI: the model travels in the URL,
/// so drop the top-level `model`, and pin the Vertex-specific `anthropic_version`.
fn apply_vertex_request_body(params: &mut Map) {
    params.remove("model");
    params.insert("anthropic_version", vs(VERTEX_ANTHROPIC_VERSION));
}

/// Rewrite a Messages request body for AWS Bedrock: the model travels in the
/// URL and the `invoke-with-response-stream` endpoint implies streaming, so drop
/// both `model` and `stream`, and pin the Bedrock-specific `anthropic_version`.
fn apply_bedrock_request_body(params: &mut Map) {
    params.remove("model");
    params.remove("stream");
    params.insert("anthropic_version", vs(BEDROCK_ANTHROPIC_VERSION));
}

#[cfg(test)]
#[path = "../../tests/regressions/r20261010_image_media_stream_acceptance.rs"]
mod image_media_stream_acceptance;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{DocumentBlock, TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, UserMessage};
    use crate::types::provider::{
        open_fallback_tool_schema, ToolSpecFormat, ToolSpecGrammarSyntax, ToolSpecKind,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    macro_rules! exact_json {
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

    fn function_spec(name: &str, description: &str, input_schema: Value) -> ToolSpec {
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

    fn schema_of(v: Value) -> Value {
        v
    }

    fn sys(text: impl Into<RuntimeString>) -> SystemPromptPart {
        SystemPromptPart {
            name: "core".into(),
            text: text.into(),
            cacheable: None,
            cache_scope: None,
        }
    }

    fn user_msg(text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    #[test]
    fn anthropic_compact_boundary_history() {
        for summary in ["resume summary", "compacted summary"] {
            let messages = vec![Message::CompactBoundary(
                crate::schemas::message::CompactBoundaryMessage {
                    native_compaction: None,
                    uuid: RuntimeString::from(format!("compact-{summary}")),
                    summary: RuntimeString::from(summary),
                },
            )];
            let out = to_api_messages(&messages, "claude-sonnet-4-6", &[]);
            let expected_text = RuntimeString::from(format!(
                "<system-reminder>\nThe conversation before this point was compacted. You also have access to the current state of the files and tools used so far. Build on the work already done and avoid duplicating it.\n\nEarlier conversation summary:\n{summary}\n</system-reminder>"
            ));
            assert_eq!(
                out,
                vec![vobj(vec![
                    ("role", vs("user")),
                    (
                        "content",
                        Value::Array(vec![vobj(vec![
                            ("type", vs("text")),
                            ("text", Value::String(expected_text)),
                        ])]),
                    ),
                ])]
            );
        }
    }

    #[test]
    fn direct_user_pdf_maps_to_native_document_input() {
        let messages = vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "u-pdf".into(),
            content: vec![ContentBlock::Document(DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("report.pdf".into()),
            })],
            is_meta: None,
        })];

        assert_eq!(
            to_api_messages(&messages, "claude-opus-4-8", &[]),
            vec![exact_json!({
                "role": "user",
                "content": [{
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": "JVBERi0xLjcK"
                    },
                    "title": "report.pdf"
                }]
            })]
        );
    }

    fn base_req() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            model: "claude-opus-4-8".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![sys("You are helpful.")],
            messages: vec![user_msg("hi")],
            tools: vec![],
            native_deferred_tools: None,
            deferred_tools: vec![],
            tool_choice: None,
            max_output_tokens: 1024,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            skip_cache_write: None,
            cache_ttl: None,
            cache_domain: None,
            cache_key: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[test]
    fn build_context_management_emits_faithful_clear_tool_uses_edit() {
        let config = ContextManagementConfig {
            trigger_input_tokens: Some(30_000),
            trigger_tool_uses: None,
            keep_tool_uses: Some(3),
            clear_at_least_input_tokens: Some(5_000),
            clear_tool_inputs: Some(ClearToolInputs::All(false)),
            exclude_tools: vec!["web_search".into()],
        };
        assert_eq!(
            build_context_management(Some(&config), None),
            Some(exact_json!({
                "edits": [{
                    "type": "clear_tool_uses_20250919",
                    "trigger": { "type": "input_tokens", "value": 30000 },
                    "keep": { "type": "tool_uses", "value": 3 },
                    "clear_at_least": { "type": "input_tokens", "value": 5000 },
                    "clear_tool_inputs": false,
                    "exclude_tools": ["web_search"]
                }]
            }))
        );
    }

    #[test]
    fn build_context_management_omits_unset_thresholds() {
        // An all-default config still emits a valid single edit; unset fields are
        // left off so the server applies its own defaults.
        let config = ContextManagementConfig::default();
        assert_eq!(
            build_context_management(Some(&config), None),
            Some(exact_json!({ "edits": [{ "type": "clear_tool_uses_20250919" }] }))
        );
    }

    #[test]
    fn build_context_management_forwards_a_clear_tool_inputs_name_list() {
        // The wire field takes either a flag or an explicit tool-name list; the
        // list form must survive as an array, not collapse to a boolean.
        let config = ContextManagementConfig {
            clear_tool_inputs: Some(ClearToolInputs::Tools(vec!["bash".into(), "grep".into()])),
            ..Default::default()
        };
        assert_eq!(
            build_context_management(Some(&config), None),
            Some(exact_json!({
                "edits": [{
                    "type": "clear_tool_uses_20250919",
                    "clear_tool_inputs": ["bash", "grep"]
                }]
            }))
        );
    }

    #[test]
    fn build_context_management_lists_the_thinking_edit_first_when_combined() {
        // The API requires `clear_thinking_20251015` to lead the edits array
        // whenever strategies combine.
        let config = ContextManagementConfig {
            trigger_input_tokens: Some(30_000),
            ..Default::default()
        };
        assert_eq!(
            build_context_management(Some(&config), Some(ThinkingContextPolicy::KeepAll)),
            Some(exact_json!({
                "edits": [
                    { "type": "clear_thinking_20251015", "keep": "all" },
                    {
                        "type": "clear_tool_uses_20250919",
                        "trigger": { "type": "input_tokens", "value": 30000 }
                    }
                ]
            }))
        );
    }

    #[test]
    fn build_clear_tool_uses_edit_prefers_the_count_based_trigger() {
        // `tool_uses` is the newer trigger form and wins when both are set; it
        // counts tool calls instead of estimated tokens.
        let config = ContextManagementConfig {
            trigger_tool_uses: Some(24),
            trigger_input_tokens: Some(30_000),
            keep_tool_uses: Some(5),
            ..Default::default()
        };
        assert_eq!(
            build_context_management(Some(&config), None),
            Some(exact_json!({
                "edits": [{
                    "type": "clear_tool_uses_20250919",
                    "trigger": { "type": "tool_uses", "value": 24 },
                    "keep": { "type": "tool_uses", "value": 5 }
                }]
            }))
        );
    }

    #[test]
    fn parse_applied_context_edits_reads_the_message_delta_report() {
        let event = exact_json!({
            "type": "message_delta",
            "context_management": {
                "applied_edits": [
                    {
                        "type": "clear_tool_uses_20250919",
                        "cleared_tool_uses": 8,
                        "cleared_input_tokens": 50000
                    },
                    {
                        "type": "clear_thinking_20251015",
                        "cleared_thinking_turns": 2,
                        "cleared_input_tokens": 1500
                    }
                ]
            }
        });
        assert_eq!(parse_applied_context_edits(&event), Some((8, 51_500, 2)));
    }

    #[test]
    fn parse_applied_context_edits_ignores_events_without_reports() {
        let plain = exact_json!({ "type": "message_delta", "usage": { "output_tokens": 3 } });
        assert_eq!(parse_applied_context_edits(&plain), None);
        let empty = exact_json!({
            "type": "message_delta",
            "context_management": { "applied_edits": [] }
        });
        assert_eq!(parse_applied_context_edits(&empty), None);
    }

    #[test]
    fn build_context_management_pins_every_thinking_turn_while_the_cache_is_warm() {
        assert_eq!(
            build_context_management(None, Some(ThinkingContextPolicy::KeepAll)),
            Some(exact_json!({
                "edits": [{ "type": "clear_thinking_20251015", "keep": "all" }]
            }))
        );
    }

    #[test]
    fn build_context_management_keeps_one_thinking_turn_once_the_cache_is_cold() {
        // The wire schema requires a positive count, so the cold case names 1
        // explicitly; omitting the edit would fall back to the server default and
        // clear nothing at all.
        assert_eq!(
            build_context_management(None, Some(ThinkingContextPolicy::KeepMostRecentTurn)),
            Some(exact_json!({
                "edits": [{
                    "type": "clear_thinking_20251015",
                    "keep": { "type": "thinking_turns", "value": 1 }
                }]
            }))
        );
    }

    #[test]
    fn build_context_management_carries_both_edits_together() {
        let config = ContextManagementConfig {
            keep_tool_uses: Some(2),
            ..Default::default()
        };
        // The API requires the thinking edit to lead the array when strategies
        // combine, so it comes first even though the tool config is passed first.
        assert_eq!(
            build_context_management(Some(&config), Some(ThinkingContextPolicy::KeepAll)),
            Some(exact_json!({
                "edits": [
                    { "type": "clear_thinking_20251015", "keep": "all" },
                    {
                        "type": "clear_tool_uses_20250919",
                        "keep": { "type": "tool_uses", "value": 2 }
                    }
                ]
            }))
        );
    }

    #[test]
    fn build_context_management_is_absent_when_neither_edit_applies() {
        // Nothing to declare must mean no request field and no beta header.
        assert_eq!(build_context_management(None, None), None);
    }

    /// A request declaring both a tool-use config and a thinking policy.
    fn context_editing_req() -> ModelRequest {
        ModelRequest {
            context_management: Some(ContextManagementConfig {
                keep_tool_uses: Some(2),
                ..Default::default()
            }),
            thinking_context: Some(ThinkingContextPolicy::KeepMostRecentTurn),
            ..base_req()
        }
    }

    #[test]
    fn context_editing_passes_both_declarations_through_for_a_first_party_request() {
        let req = context_editing_req();
        let (tool_uses, thinking) = resolve_context_editing(&req, true, false, true);
        assert!(tool_uses.is_some());
        assert_eq!(thinking, Some(ThinkingContextPolicy::KeepMostRecentTurn));
    }

    #[test]
    fn context_editing_is_withheld_from_third_party_endpoints() {
        let req = context_editing_req();
        assert_eq!(
            resolve_context_editing(&req, false, false, true),
            (None, None)
        );
    }

    #[test]
    fn context_editing_is_withheld_when_the_frozen_contract_suppresses_it() {
        let req = context_editing_req();
        assert_eq!(
            resolve_context_editing(&req, true, true, true),
            (None, None)
        );
    }

    #[test]
    fn thinking_editing_is_dropped_when_the_request_carries_no_thinking() {
        // No thinking on the wire means no prior-turn reasoning to govern; the
        // tool-use declaration is unaffected.
        let req = context_editing_req();
        let (tool_uses, thinking) = resolve_context_editing(&req, true, false, false);
        assert!(tool_uses.is_some());
        assert_eq!(thinking, None);
    }

    #[test]
    fn vertex_stream_url_uses_region_host_and_publisher_path() {
        let cfg = VertexConfig {
            project: "my-proj".to_string(),
            location: "us-east5".to_string(),
        };
        assert_eq!(
            cfg.stream_url("claude-opus-4-8"),
            "https://us-east5-aiplatform.googleapis.com/v1/projects/my-proj/locations/us-east5/publishers/anthropic/models/claude-opus-4-8:streamRawPredict"
        );
    }

    #[test]
    fn vertex_stream_url_global_region_uses_unprefixed_host() {
        let cfg = VertexConfig {
            project: "p".to_string(),
            location: "global".to_string(),
        };
        assert_eq!(
            cfg.stream_url("claude-sonnet-4-6"),
            "https://aiplatform.googleapis.com/v1/projects/p/locations/global/publishers/anthropic/models/claude-sonnet-4-6:streamRawPredict"
        );
    }

    #[test]
    fn vertex_request_body_moves_model_to_url_and_pins_version() {
        let mut params = build_params(&base_req(), false, false, false, false);
        assert_eq!(
            params.get("model").unwrap(),
            &exact_json!("claude-opus-4-8")
        );
        apply_vertex_request_body(&mut params);
        // The model is dropped (it rides in the URL) and the Vertex API version is
        // pinned in the body; unrelated request fields are preserved.
        assert!(params.get("model").is_none());
        assert_eq!(
            params.get("anthropic_version").unwrap(),
            &exact_json!("vertex-2023-10-16")
        );
        assert_eq!(params.get("max_tokens").unwrap(), &exact_json!(1024));
        assert_eq!(params.get("stream").unwrap(), &exact_json!(true));
    }

    fn deferred_spec(name: &str) -> ToolSpec {
        function_spec(
            name,
            "Deferred tool.",
            schema_of(exact_json!({ "type": "object", "properties": {} })),
        )
    }

    fn deferred_req() -> ModelRequest {
        ModelRequest {
            tools: vec![deferred_spec("read")],
            tool_choice: Some(ToolChoice::Auto),
            deferred_tools: vec![
                deferred_spec("mcp__alpha__one"),
                deferred_spec("mcp__alpha__two"),
            ],
            ..base_req()
        }
    }

    fn deferred_wire_tools(req: &ModelRequest, structured: bool, supported: bool) -> Vec<Value> {
        let params = build_params_with_media_limits(
            req,
            structured,
            false,
            supported,
            StructuredOutputMode {
                native: false,
                review_tool_loop: false,
                auto_result_tool: false,
            },
            false,
            ImageLimits::default(),
            MediaBudget::for_anthropic(true, None),
        )
        .expect("request assembly fixture must contain valid image payloads");
        params
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    #[test]
    fn deferred_tool_loading_gate_matches_family_and_version() {
        for model in [
            "claude-sonnet-4-5",
            "claude-opus-4-8",
            "claude-sonnet-4-5[1m]",
            "claude-sonnet-4-5-20250929",
            "claude-opus-4-5-latest",
        ] {
            assert!(
                model_supports_deferred_tool_loading(&RuntimeString::from(model)),
                "{model} should support deferred tool loading"
            );
        }
        for model in [
            "claude-haiku-4-5",
            "claude-haiku-4-5-20251001",
            "claude-sonnet-4-0",
            "claude-opus-4-1-20250805",
            "claude-3-5-sonnet",
            "claude-sonnet-4-20250514",
            "claude-sonnet-latest",
            "gpt-5",
        ] {
            assert!(
                !model_supports_deferred_tool_loading(&RuntimeString::from(model)),
                "{model} should not support deferred tool loading"
            );
        }
    }

    #[test]
    fn deferred_tools_ride_after_the_cache_breakpoint_when_supported() {
        let tools = deferred_wire_tools(&deferred_req(), false, true);
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0].get("name").unwrap(), &exact_json!("read"));
        assert!(tools[0].get("defer_loading").is_none());
        // The cache breakpoint stays on the last immediate tool, never on a
        // deferred definition appended behind it.
        assert!(tools[0].get("cache_control").is_some());
        for tool in &tools[1..] {
            assert_eq!(tool.get("defer_loading").unwrap(), &exact_json!(true));
            assert!(tool.get("cache_control").is_none());
        }
        assert_eq!(
            tools[1].get("name").unwrap(),
            &exact_json!("mcp__alpha__one")
        );
        assert_eq!(
            tools[2].get("name").unwrap(),
            &exact_json!("mcp__alpha__two")
        );
    }

    #[test]
    fn deferred_tools_are_omitted_without_native_support() {
        let tools = deferred_wire_tools(&deferred_req(), false, false);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].get("name").unwrap(), &exact_json!("read"));
        assert!(tools[0].get("defer_loading").is_none());
    }

    #[test]
    fn forced_structured_output_replacement_drops_deferred_tools() {
        let req = ModelRequest {
            output_schema: Some(schema_of(
                exact_json!({ "type": "object", "properties": {} }),
            )),
            ..deferred_req()
        };
        let tools = deferred_wire_tools(&req, true, true);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].get("name").unwrap(), &exact_json!("emit_result"));
    }

    #[test]
    fn an_all_deferred_inventory_is_promoted_to_immediate_tools() {
        let req = ModelRequest {
            tools: vec![],
            tool_choice: None,
            ..deferred_req()
        };
        let tools = deferred_wire_tools(&req, false, true);
        assert_eq!(tools.len(), 2);
        for tool in &tools {
            assert!(tool.get("defer_loading").is_none());
        }
    }

    #[test]
    fn foundry_base_url_composes_from_resource_and_trims_env_v1_suffix() {
        // Resource form composes the Azure AI Foundry host; appending the
        // provider's `/v1/messages` yields the standard Anthropic surface.
        let from_resource = foundry_base_url(None, Some("my-res")).unwrap();
        assert_eq!(
            from_resource,
            "https://my-res.services.ai.azure.com/anthropic"
        );
        assert_eq!(
            format!("{from_resource}/v1/messages"),
            "https://my-res.services.ai.azure.com/anthropic/v1/messages"
        );

        // An explicit base URL wins and its trailing `/v1` is trimmed so the
        // provider does not double it.
        assert_eq!(
            foundry_base_url(
                Some("https://x.services.ai.azure.com/anthropic/v1"),
                Some("ignored")
            )
            .unwrap(),
            "https://x.services.ai.azure.com/anthropic"
        );
        assert_eq!(
            foundry_base_url(Some("https://x.services.ai.azure.com/anthropic/"), None).unwrap(),
            "https://x.services.ai.azure.com/anthropic"
        );
        assert!(foundry_base_url(None, None).is_none());
        assert!(foundry_base_url(Some("  "), Some("")).is_none());
    }

    #[test]
    fn bedrock_request_body_drops_model_and_stream_and_pins_version() {
        let mut params = build_params(&base_req(), false, false, false, false);
        assert_eq!(
            params.get("model").unwrap(),
            &exact_json!("claude-opus-4-8")
        );
        assert_eq!(params.get("stream").unwrap(), &exact_json!(true));
        apply_bedrock_request_body(&mut params);
        // Model + stream ride in the URL/endpoint; the Bedrock API version is pinned.
        assert!(params.get("model").is_none());
        assert!(params.get("stream").is_none());
        assert_eq!(
            params.get("anthropic_version").unwrap(),
            &exact_json!("bedrock-2023-05-31")
        );
        assert_eq!(params.get("max_tokens").unwrap(), &exact_json!(1024));
    }

    #[test]
    fn bedrock_config_forces_non_first_party_gating() {
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            bedrock: Some(BedrockConfig {
                region: "us-east-1".to_string(),
            }),
            ..Default::default()
        });
        assert!(!provider.is_first_party);
    }

    #[test]
    fn bedrock_model_id_url_percent_encodes_the_version_colon() {
        // The `:0` version suffix must be percent-encoded consistently in both
        // the request URL and the SigV4 canonical URI.
        let encoded = urlencoding::encode("anthropic.claude-3-5-sonnet-20241022-v2:0").into_owned();
        assert_eq!(encoded, "anthropic.claude-3-5-sonnet-20241022-v2%3A0");
        let host = format!("bedrock-runtime.{}.amazonaws.com", "us-east-1");
        assert_eq!(host, "bedrock-runtime.us-east-1.amazonaws.com");
        let canonical_uri = format!("/model/{encoded}/invoke-with-response-stream");
        assert_eq!(
            canonical_uri,
            "/model/anthropic.claude-3-5-sonnet-20241022-v2%3A0/invoke-with-response-stream"
        );
    }

    #[test]
    fn bedrock_event_frame_unwraps_bytes_into_sse_data() {
        use crate::provider::aws_eventstream::EventStreamMessage;
        use base64::Engine;
        // A Bedrock chunk wraps the model event JSON as base64 under `bytes`.
        let event_json = br#"{"type":"content_block_delta","index":0}"#;
        let b64 = base64::engine::general_purpose::STANDARD.encode(event_json);
        let mut headers = std::collections::HashMap::new();
        headers.insert(":message-type".to_string(), "event".to_string());
        let message = EventStreamMessage {
            headers,
            payload: format!(r#"{{"bytes":"{b64}"}}"#).into_bytes(),
        };
        let sse = super::bedrock_message_to_sse(&message).expect("event maps to SSE");
        assert_eq!(
            sse,
            b"data: {\"type\":\"content_block_delta\",\"index\":0}\n\n"
        );
    }

    #[test]
    fn bedrock_exception_frame_becomes_an_error_event() {
        use crate::provider::aws_eventstream::EventStreamMessage;
        let mut headers = std::collections::HashMap::new();
        headers.insert(":message-type".to_string(), "exception".to_string());
        let message = EventStreamMessage {
            headers,
            payload: br#"{"message":"throttled"}"#.to_vec(),
        };
        let sse = super::bedrock_message_to_sse(&message).expect("exception maps to SSE");
        let text = String::from_utf8(sse).unwrap();
        assert!(text.starts_with("data: "));
        // Parses back to an Anthropic-shaped error event the SSE loop understands.
        let json = &text["data: ".len()..text.len() - 2];
        let value = runtime_json::parse(&RuntimeString::from(json)).unwrap();
        assert_eq!(value["type"], exact_json!("error"));
    }

    #[test]
    fn vertex_config_forces_non_first_party_gating() {
        // Vertex is not the first-party endpoint, so first-party-only betas
        // (global cache scope, 1M, structured outputs, task budgets, context
        // management) must be gated off automatically.
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            vertex: Some(VertexConfig {
                project: "p".to_string(),
                location: "global".to_string(),
            }),
            ..Default::default()
        });
        assert!(!provider.is_first_party);
    }

    async fn capture_one_http_request(listener: TcpListener) -> (String, String) {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut header_end = None;
        let mut content_length = None;
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before completing the request");
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
        let headers = String::from_utf8(request[..end].to_vec()).unwrap();
        let body = String::from_utf8(request[end..end + length].to_vec()).unwrap();

        let event = "data: {\"type\":\"message_stop\"}\n\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{event}",
            event.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        (headers, body)
    }

    #[tokio::test]
    async fn anthropic_request_raw_body_escapes_lone_surrogate_tool_input() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(capture_one_http_request(listener));

        let input =
            runtime_json::parse(&RuntimeString::from(r#"{"\uD800":"high","low":"\uDC00"}"#))
                .unwrap();
        let mut request = base_req();
        request.messages = vec![Message::Assistant(AssistantMessage {
            uuid: "assistant-surrogate".into(),
            model: request.model.clone(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "tool-surrogate".into(),
                name: "echo".into(),
                input,
                thought_signature: None,
                tool_kind: None,
            })],
        })];

        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("test-key".into()),
            base_url: Some(format!("http://{address}")),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
        let _: Vec<ModelStreamEvent> = provider.stream(request, AbortSignal::new()).collect().await;
        let (headers, body) = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("Anthropic request reached the local server")
            .unwrap();

        assert!(headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("content-type: application/json")));
        assert!(
            body.contains(r#""input":{"\ud800":"high","low":"\udc00"}"#),
            "raw request body did not carry canonical surrogate escapes: {body}"
        );
        assert!(
            !body.contains('\u{fdd0}'),
            "the internal surrogate sentinel leaked onto the HTTP wire: {body}"
        );
    }

    #[test]
    fn subscription_fast_service_never_inherits_eligibility_for_an_unknown_request_model() {
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            default_model: Some("eligible-main".into()),
            capabilities: Some(ModelCapabilities {
                fast_mode: true,
                ..Default::default()
            }),
            capabilities_by_model: Some(HashMap::from([
                (
                    "eligible-tier".into(),
                    ModelCapabilities {
                        fast_mode: true,
                        ..Default::default()
                    },
                ),
                ("ordinary-tier".into(), ModelCapabilities::default()),
            ])),
            ..Default::default()
        });
        assert!(provider.capabilities_for(&"eligible-main".into()).fast_mode);
        assert!(provider.capabilities_for(&"eligible-tier".into()).fast_mode);
        assert!(!provider.capabilities_for(&"ordinary-tier".into()).fast_mode);
        assert!(!provider.capabilities_for(&"unknown-model".into()).fast_mode);
    }

    #[tokio::test]
    async fn subscription_fast_service_preserves_thinking_and_effort_and_is_exact_model_gated() {
        for (enabled, eligible, first_party) in [
            (true, true, true),
            (false, true, true),
            (true, false, true),
            (true, true, false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(capture_one_http_request(listener));
            let mut provider = AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("fixture-key".into()),
                base_url: Some(format!("http://{address}")),
                fast_mode: enabled,
                default_model: Some("claude-opus-4-8".into()),
                capabilities: Some(ModelCapabilities {
                    fast_mode: eligible,
                    thinking: Some(crate::types::provider::ThinkingCapability {
                        adaptive: true,
                        interleaved: true,
                        can_disable_thinking: true,
                    }),
                    effort: Some(crate::types::provider::EffortCapability {
                        levels: vec![EffortLevel::High],
                        default: None,
                    }),
                    ..Default::default()
                }),
                retry: Some(RetryOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            });
            // The loopback fixture exercises the first-party wire without remote IO.
            provider.is_first_party = first_party;
            let mut request = base_req();
            request.thinking = Some(ThinkingConfig::Adaptive);
            request.effort = Some(EffortValue::Level(EffortLevel::High));
            let _: Vec<_> = provider.stream(request, AbortSignal::new()).collect().await;
            let (headers, body) = server.await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&body).unwrap();
            let active = enabled && eligible && first_party;
            assert_eq!(
                body.get("speed").and_then(serde_json::Value::as_str),
                active.then_some("fast")
            );
            assert_eq!(headers.contains(FAST_MODE_BETA), active);
            assert_eq!(body["output_config"]["effort"], serde_json::json!("high"));
            assert!(body["thinking"].is_object());
        }
    }

    #[tokio::test]
    async fn interceptor_headers_ride_the_request_and_outcomes_are_reported() {
        use crate::provider::interceptor::{
            ProviderRequestDirectives, ProviderRequestInterceptor, ProviderResponseInfo,
        };

        /// One `after_response` report: (ok, status, attempt).
        type InterceptorOutcome = (bool, Option<u16>, u32);
        struct Gateway {
            outcomes: Arc<std::sync::Mutex<Vec<InterceptorOutcome>>>,
        }
        impl ProviderRequestInterceptor for Gateway {
            fn before_request(
                &self,
                request: &crate::provider::interceptor::ProviderRequestInfo<'_>,
            ) -> ProviderRequestDirectives {
                ProviderRequestDirectives {
                    extra_headers: vec![(
                        "x-gateway-tag".to_string(),
                        format!("audit-{}", request.attempt),
                    )],
                    deny: None,
                }
            }
            fn after_response(&self, response: &ProviderResponseInfo<'_>) {
                self.outcomes.lock().unwrap().push((
                    response.ok,
                    response.status,
                    response.attempt,
                ));
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(capture_one_http_request(listener));
        let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("test-key".into()),
            base_url: Some(format!("http://{address}")),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            interceptor: Some(Arc::new(Gateway {
                outcomes: outcomes.clone(),
            })),
            ..Default::default()
        });
        let _: Vec<ModelStreamEvent> = provider
            .stream(base_req(), AbortSignal::new())
            .collect()
            .await;
        let (headers, _body) = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("Anthropic request reached the local server")
            .unwrap();

        // The per-attempt header rode the wire alongside auth headers.
        assert!(
            headers
                .lines()
                .any(|line| line.eq_ignore_ascii_case("x-gateway-tag: audit-1")),
            "intercepted header missing from the request: {headers}"
        );
        assert!(headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("x-api-key: test-key")));
        // The settled attempt was reported exactly once, as a success.
        assert_eq!(*outcomes.lock().unwrap(), vec![(true, None, 1)]);
    }

    #[test]
    fn anthropic_validates_final_facade_and_named_web_search_set() {
        let mut request = base_req();
        request.tools = vec![function_spec(
            "Read",
            "Read a file.",
            schema_of(exact_json!({ "type": "object" })),
        )];

        request.tool_choice = Some(ToolChoice::Tool {
            name: RuntimeString::from("Read"),
        });
        assert_eq!(
            validate_anthropic_request_choice(&request, false, false),
            None
        );

        request.tool_choice = Some(ToolChoice::Tool {
            name: RuntimeString::from("read"),
        });
        let case_error = validate_anthropic_request_choice(&request, false, false)
            .expect("forced names are case-sensitive");
        assert_eq!(
            case_error.message,
            "Anthropic tool choice is invalid: forced tool \"read\" is not provider-visible; available tools: Read."
        );

        request.tool_choice = Some(ToolChoice::Tool {
            name: RuntimeString::from("web_search"),
        });
        assert_eq!(
            validate_anthropic_request_choice(&request, true, false),
            None,
            "Anthropic native search is named and forceable"
        );
        let missing_search = validate_anthropic_request_choice(&request, false, false)
            .expect("search is unavailable when it will not be on wire");
        assert_eq!(
            missing_search.message,
            "Anthropic tool choice is invalid: forced tool \"web_search\" is not provider-visible; available tools: Read."
        );

        request.tool_choice = Some(ToolChoice::Tool {
            name: RuntimeString::from("missing"),
        });
        assert_eq!(
            validate_anthropic_request_choice(&request, false, true),
            None,
            "the private emit_result fallback skips caller choice validation"
        );

        request.tools.clear();
        request.tool_choice = Some(ToolChoice::Any);
        assert_eq!(
            validate_anthropic_request_choice(&request, true, false),
            None
        );
        let any_error = validate_anthropic_request_choice(&request, false, false)
            .expect("any without a final tool must fail closed");
        assert_eq!(
            any_error.message,
            "Anthropic tool choice is invalid: `any` requires at least one provider-visible tool."
        );
    }

    #[tokio::test]
    async fn anthropic_stream_invalid_choice_yields_one_error_before_transport() {
        let auth = Arc::new(CountingAuth::default());
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            auth_source: Some(auth.clone()),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut request = base_req();
        request.tool_choice = Some(ToolChoice::Any);

        let events: Vec<ModelStreamEvent> =
            provider.stream(request, AbortSignal::new()).collect().await;
        assert_eq!(auth.calls.load(Ordering::SeqCst), 0);
        assert_eq!(events.len(), 1);
        let ModelStreamEvent::Error { error } = &events[0] else {
            panic!("expected one local error event");
        };
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.status, Some(400));
        assert_eq!(
            error.message,
            "Anthropic tool choice is invalid: `any` requires at least one provider-visible tool."
        );
    }

    /// Drive a canned SSE byte string through the decoder + event mapper.
    fn drive(
        raw: &str,
        forced: bool,
    ) -> (Vec<ModelStreamEvent>, (u64, u64, u64, u64), ModelStopReason) {
        drive_with_summary_source(raw, forced, "https://compatible.example/v1/messages")
    }

    fn drive_with_summary_source(
        raw: &str,
        forced: bool,
        endpoint: &str,
    ) -> (Vec<ModelStreamEvent>, (u64, u64, u64, u64), ModelStopReason) {
        let mut thinking_summary = ThinkingSummaryState::new(is_native_summary_endpoint(endpoint));
        let mut decoder = SseDecoder::new();
        decoder.push(raw.as_bytes());
        let mut blocks = HashMap::new();
        let mut structured = RuntimeString::default();
        let mut evs = Vec::new();
        let mut usage = Usage {
            input_tokens: 0,
            output_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        };
        let mut stop = ModelStopReason::EndTurn;
        while let Some(block) = decoder.next_block() {
            if let Some(data) = parse_sse_data(&block) {
                let out = handle_event(
                    &data,
                    &mut blocks,
                    forced,
                    &mut structured,
                    &mut thinking_summary,
                );
                evs.extend(out.events);
                update_stream_usage(&data, &mut usage);
                if let Some(sr) = out.stop_reason {
                    stop = sr;
                }
            }
        }
        (
            evs,
            (
                usage.input_tokens,
                usage.cache_read_input_tokens,
                usage.cache_creation_input_tokens,
                usage.output_tokens,
            ),
            stop,
        )
    }

    #[test]
    fn build_params_basic_shape() {
        let p = build_params(&base_req(), false, false, false, false);
        assert_eq!(p.get("model").unwrap(), &exact_json!("claude-opus-4-8"));
        assert_eq!(p.get("max_tokens").unwrap(), &exact_json!(1024));
        assert_eq!(p.get("stream").unwrap(), &exact_json!(true));
        let system = p.get("system").unwrap().as_array().unwrap();
        assert_eq!(system[0].get("type").unwrap(), &exact_json!("text"));
        assert_eq!(
            system[0].get("text").unwrap(),
            &exact_json!("You are helpful.")
        );
        assert!(p.get("tools").is_none());
        // Fallback: last system block gets a breakpoint; so does the last message.
        assert!(system.last().unwrap().get("cache_control").is_some());
        let messages = p.get("messages").unwrap().as_array().unwrap();
        let last_msg_content = messages
            .last()
            .unwrap()
            .get("content")
            .unwrap()
            .as_array()
            .unwrap();
        assert!(last_msg_content
            .last()
            .unwrap()
            .get("cache_control")
            .is_some());
    }

    #[test]
    fn build_params_forced_structured_tool() {
        let mut r = base_req();
        r.output_schema = Some(schema_of(
            exact_json!({ "type": "object", "properties": {} }),
        ));
        let p = build_params(&r, true, false, false, false);
        let tools = p.get("tools").unwrap().as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].get("name").unwrap(), &exact_json!("emit_result"));
        assert_eq!(
            p.get("tool_choice").unwrap(),
            &exact_json!({ "type": "tool", "name": "emit_result" })
        );
    }

    #[test]
    fn build_params_native_structured_leaves_tools_untouched() {
        let mut r = base_req();
        r.output_schema = Some(schema_of(exact_json!({ "type": "object" })));
        // use_native = true → no forced tool, no tool_choice.
        let p = build_params(&r, true, false, true, false);
        assert!(p.get("tools").is_none());
        assert!(p.get("tool_choice").is_none());
    }

    #[test]
    fn build_params_prepends_claude_code_identity_for_oauth() {
        // require_claude_code_identity=true leads with the verbatim identity as its
        // own first block; the original system content is shifted down by one.
        let p = build_params(&base_req(), false, false, false, true);
        let system = p.get("system").unwrap().as_array().unwrap();
        assert_eq!(system[0].get("type").unwrap(), &exact_json!("text"));
        assert_eq!(
            system[0].get("text").unwrap(),
            &exact_json!("You are Claude Code, Anthropic's official CLI for Claude.")
        );
        assert_eq!(
            system[1].get("text").unwrap(),
            &exact_json!("You are helpful.")
        );
    }

    #[test]
    fn build_params_identity_not_duplicated_when_already_leading() {
        let mut r = base_req();
        r.system = vec![
            sys("You are Claude Code, Anthropic's official CLI for Claude."),
            sys("rest"),
        ];
        let p = build_params(&r, false, false, false, true);
        let system = p.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 2);
        assert_eq!(
            system[0].get("text").unwrap(),
            &exact_json!("You are Claude Code, Anthropic's official CLI for Claude.")
        );
    }

    #[test]
    fn build_params_no_identity_for_api_key() {
        // require_claude_code_identity=false (api-key / cloud paths) must never inject it.
        let p = build_params(&base_req(), false, false, false, false);
        let system = p.get("system").unwrap().as_array().unwrap();
        assert_eq!(
            system[0].get("text").unwrap(),
            &exact_json!("You are helpful.")
        );
    }

    #[test]
    fn build_params_freeform_exec_falls_back_to_source_schema_without_renaming() {
        let mut r = base_req();
        r.tools = vec![
            text_spec("exec", "execute javascript source"),
            function_spec(
                "wait",
                "wait for exec completion",
                schema_of(exact_json!({ "type": "object" })),
            ),
        ];

        let p = build_params(&r, false, false, false, false);
        let tools = p.get("tools").unwrap().as_array().unwrap();
        let rendered = runtime_json::stringify(&Value::Array(tools.clone()));
        assert!(rendered.contains("\"name\":\"exec\""));
        assert!(rendered.contains("\"name\":\"wait\""));
        assert!(!rendered.contains("code_exec"));
        assert_eq!(tools[0]["name"], exact_json!("exec"));
        assert_eq!(
            tools[0]["input_schema"],
            exact_json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true
            })
        );
    }

    #[test]
    fn tool_result_messages_coalesce_into_one_user_turn() {
        use crate::schemas::content::OutputBlock;
        let tr = |id: &str| {
            Message::ToolResult(crate::schemas::message::ToolResultMessage {
                uuid: id.into(),
                tool_use_id: id.into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "ok".into(),
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
            })
        };
        let msgs = vec![tr("a"), tr("b")];
        let out = to_api_messages(&msgs, "claude-opus-4-8", &[]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("role").unwrap(), &exact_json!("user"));
        let content = out[0].get("content").unwrap().as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0].get("type").unwrap(), &exact_json!("tool_result"));
    }

    #[test]
    fn trailing_thinking_stripped_from_last_assistant() {
        use crate::schemas::content::ThinkingBlock;
        let msgs = vec![Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            model: "claude-opus-4-8".into(),
            usage: None,
            content: vec![
                ContentBlock::Text(TextBlock {
                    text: "answer".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                }),
                ContentBlock::Thinking(ThinkingBlock {
                    message_id: None,
                    text: "hmm".into(),
                    signature: Some("sig".into()),
                    encrypted_content: None,
                }),
            ],
        })];
        let out = to_api_messages(&msgs, "claude-opus-4-8", &[]);
        let content = out[0].get("content").unwrap().as_array().unwrap();
        // The trailing thinking block is stripped; only the text remains.
        assert_eq!(content.len(), 1);
        assert_eq!(content[0].get("type").unwrap(), &exact_json!("text"));
    }

    #[test]
    fn sse_pipeline_text_and_usage() {
        let raw = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-x\",\"usage\":{\"input_tokens\":10,\"output_tokens\":0,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":3}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\",\"citations\":[]}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":5}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let (evs, usage, stop) = drive(raw, false);
        assert_eq!(
            evs,
            vec![
                ModelStreamEvent::MessageStart {
                    model: "claude-x".into()
                },
                ModelStreamEvent::TextDelta { text: "Hi".into() },
            ]
        );
        assert_eq!(usage, (10, 2, 3, 5));
        assert_eq!(stop, ModelStopReason::EndTurn);
    }

    #[test]
    fn sse_pipeline_tool_use() {
        let raw = concat!(
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu1\",\"name\":\"read\",\"input\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"p\\\":1}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        );
        let (evs, _, _) = drive(raw, false);
        assert_eq!(
            evs,
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "tu1".into(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "tu1".into(),
                    partial_json: "{\"p\":1}".into()
                },
                ModelStreamEvent::ToolUseStop { id: "tu1".into() },
            ]
        );
    }

    #[test]
    fn sse_tool_input_lone_surrogate_round_trips_without_marker_leak() {
        let runtime_surrogate = runtime_json::parse(&RuntimeString::from(r#""\uD800""#)).unwrap();
        let start = exact_json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": "tu-surrogate",
                "name": "echo",
                "input": {}
            }
        });
        let mut delta = exact_json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": ""
            }
        });
        delta["delta"]["partial_json"] =
            Value::String(RuntimeString::from(r#"{"\uD800":"high","low":"\uDC00"}"#));
        delta
            .as_object_mut()
            .unwrap()
            .insert("ignored_runtime_string", runtime_surrogate);
        let stop = exact_json!({ "type": "content_block_stop", "index": 0 });
        let raw = [start, delta, stop]
            .into_iter()
            .map(|event| {
                let event = runtime_json::stringify(&event)
                    .try_to_utf8()
                    .expect("JSON writer escapes lone surrogates");
                format!("data: {event}\n\n")
            })
            .collect::<String>();

        let (events, _, _) = drive(&raw, false);
        let partial_json = events
            .iter()
            .find_map(|event| match event {
                ModelStreamEvent::ToolUseInputDelta { partial_json, .. } => Some(partial_json),
                _ => None,
            })
            .expect("the surrogate-bearing input delta survives SSE decoding");
        assert_eq!(partial_json, r#"{"\uD800":"high","low":"\uDC00"}"#);
        assert!(
            !partial_json.contains("\u{fdd0}"),
            "the SSE mapper leaked the internal surrogate sentinel: {partial_json:?}"
        );

        let input = runtime_json::parse(partial_json).unwrap();
        let replay_wire = runtime_json::stringify(&input);
        assert_eq!(replay_wire, r#"{"\ud800":"high","low":"\udc00"}"#);
        assert!(
            !replay_wire.contains("\u{fdd0}"),
            "the internal surrogate sentinel leaked onto replay wire: {replay_wire:?}"
        );
    }

    #[test]
    fn sse_pipeline_thinking_block_and_signature() {
        let raw = concat!(
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reason\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"SIG\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        );
        let (evs, _, _) = drive(raw, false);
        assert_eq!(
            evs,
            vec![
                ModelStreamEvent::ThinkingDelta {
                    text: "reason".into()
                },
                ModelStreamEvent::ThinkingBlock {
                    text: "reason".into(),
                    signature: Some("SIG".into()),
                    encrypted_content: None,
                    is_summary: false,
                },
            ]
        );
    }

    #[test]
    fn thinking_summary_identity_requires_a_direct_native_endpoint_and_bounded_model() {
        for endpoint in [
            "https://api.anthropic.com/v1/messages",
            "https://api.anthropic.com:443/v1/messages",
        ] {
            assert!(is_native_summary_endpoint(endpoint), "{endpoint}");
        }
        for endpoint in [
            "http://api.anthropic.com/v1/messages",
            "https://api.anthropic.com:444/v1/messages",
            "https://api.anthropic.com.example/v1/messages",
            "https://user@api.anthropic.com/v1/messages",
            "https://api.anthropic.com/v1/messages?proxy=true",
            "https://api.anthropic.com/v1/messages#proxy",
            "https://api.anthropic.com/proxy/v1/messages",
            "https://us-east5-aiplatform.googleapis.com/v1/messages",
            "https://bedrock-runtime.us-east-1.amazonaws.com/v1/messages",
            "http://127.0.0.1/v1/messages",
        ] {
            assert!(!is_native_summary_endpoint(endpoint), "{endpoint}");
        }
        for model in [
            "claude-sonnet-4-20250514",
            "claude-opus-4-8",
            "claude-haiku-4-5-20251001",
            "claude-4-sonnet-latest",
            "claude-sonnet-5",
            "claude-sonnet-5[1m]",
        ] {
            assert!(model_returns_thinking_summaries(&model.into()), "{model}");
        }
        for model in [
            "claude-3-7-sonnet-20250219",
            "claude-sonnet-3-7",
            "claude-sonnet-latest",
            "claude-sonnet-04",
            "claude-sonnet-40000000000000000000000",
            "claude-sonnet-4-preview",
            "claude-sonnet-4-latest-1",
            "claude-sonnet-4-123456789",
            "claude-sonnet-4-",
            "claude-sonnet-4[unknown]",
            "claude-other-4",
            "requested-model-alias",
        ] {
            assert!(!model_returns_thinking_summaries(&model.into()), "{model}");
        }
        assert!(!model_returns_thinking_summaries(&RuntimeString::from(
            "claude-sonnet-4-".repeat(20)
        )));
        assert!(!model_returns_thinking_summaries(
            &RuntimeString::from_utf16(vec![0xd800])
        ));
    }

    fn summary_wire(model: Option<&str>, text: &str, completed: bool) -> String {
        let mut start = exact_json!({"type":"message_start","message":{}});
        if let Some(model) = model {
            start["message"]["model"] = vs(model);
        }
        let mut delta = exact_json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":""}});
        delta["delta"]["thinking"] = vs(text);
        let mut wire = vec![
            start,
            exact_json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            delta,
            exact_json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque-signature"}}),
        ];
        if completed {
            wire.push(exact_json!({"type":"content_block_stop","index":0}));
            wire.push(exact_json!({"type":"message_stop"}));
        }
        wire.into_iter()
            .map(|event| {
                format!(
                    "data: {}\n\n",
                    runtime_json::stringify(&event).try_to_utf8().unwrap()
                )
            })
            .collect()
    }

    #[test]
    fn completed_native_summary_uses_the_actual_announced_model() {
        for (model, endpoint, expected) in [
            (
                Some("claude-sonnet-4-20250514"),
                "https://api.anthropic.com/v1/messages",
                true,
            ),
            (
                Some("claude-3-7-sonnet-20250219"),
                "https://api.anthropic.com/v1/messages",
                false,
            ),
            (None, "https://api.anthropic.com/v1/messages", false),
            (
                Some("requested-model-alias"),
                "https://api.anthropic.com/v1/messages",
                false,
            ),
            (
                Some("claude-sonnet-4-20250514"),
                "https://compatible.example/v1/messages",
                false,
            ),
        ] {
            let raw = summary_wire(model, "A completed summary.", true);
            let (events, _, _) = drive_with_summary_source(&raw, false, endpoint);
            let block = events.iter().find_map(|event| match event {
                ModelStreamEvent::ThinkingBlock {
                    text,
                    signature,
                    is_summary,
                    ..
                } => Some((text, signature, is_summary)),
                _ => None,
            });
            let (text, signature, is_summary) = block.unwrap();
            assert_eq!(text, "A completed summary.");
            assert_eq!(signature.as_ref(), Some(&"opaque-signature".into()));
            assert_eq!(*is_summary, expected, "{model:?} {endpoint}");
        }
    }

    #[test]
    fn partial_empty_signed_and_redacted_reasoning_never_become_summary_activity() {
        let endpoint = "https://api.anthropic.com/v1/messages";
        let raw = summary_wire(Some("claude-sonnet-4-20250514"), "partial", false);
        let (events, _, _) = drive_with_summary_source(&raw, false, endpoint);
        assert!(events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ThinkingDelta { text } if text == "partial"
        )));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ThinkingBlock { .. })));
        for text in ["", " \t\n"] {
            let raw = summary_wire(Some("claude-sonnet-4-20250514"), text, true);
            let (events, _, _) = drive_with_summary_source(&raw, false, endpoint);
            assert!(events.iter().any(|event| matches!(
                event,
                ModelStreamEvent::ThinkingBlock {
                    signature: Some(_),
                    is_summary: false,
                    ..
                }
            )));
        }
        let raw = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-4-20250514\"}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"encrypted\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let (events, _, _) = drive_with_summary_source(raw, false, endpoint);
        assert!(events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::RedactedThinkingBlock { data } if data == "encrypted"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ThinkingBlock {
                is_summary: true,
                ..
            }
        )));
    }

    #[test]
    fn sse_pipeline_web_search_and_citation() {
        let raw = concat!(
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"server_tool_use\",\"id\":\"srv1\",\"name\":\"web_search\",\"input\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"rust async\\\"}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"web_search_tool_result\",\"tool_use_id\":\"srv1\",\"content\":[{\"type\":\"web_search_result\",\"title\":\"Tokio\",\"url\":\"https://tokio.rs\",\"encrypted_content\":\"ENC\",\"page_age\":\"2d\"}]}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":3,\"content_block\":{\"type\":\"text\",\"text\":\"\",\"citations\":[]}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":3,\"delta\":{\"type\":\"citations_delta\",\"citation\":{\"type\":\"web_search_result_location\",\"url\":\"https://tokio.rs\",\"title\":\"Tokio\",\"cited_text\":\"async\",\"encrypted_index\":\"EIDX\"}}}\n\n",
        );
        let (evs, _, _) = drive(raw, false);
        assert!(evs.contains(&ModelStreamEvent::ServerSearchQuery {
            id: "srv1".into(),
            query: "rust async".into(),
            action: None,
        }));
        assert!(evs.contains(&ModelStreamEvent::WebSearchResult {
            id: "srv1".into(),
            results: vec![NativeSearchResult {
                title: "Tokio".into(),
                url: "https://tokio.rs".into(),
                encrypted_token: Some("ENC".into()),
                page_age: Some("2d".into()),
            }],
            error: None,
        }));
        assert!(evs.contains(&ModelStreamEvent::Citation {
            citation: Citation {
                url: "https://tokio.rs".into(),
                title: Some("Tokio".into()),
                cited_text: Some("async".into()),
                start_index: None,
                end_index: None,
                encrypted_index: Some("EIDX".into()),
            }
        }));
    }

    #[test]
    fn sse_web_search_error_result() {
        let raw = "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"web_search_tool_result\",\"tool_use_id\":\"srv2\",\"content\":{\"type\":\"web_search_tool_result_error\",\"error_code\":\"max_uses_exceeded\"}}}\n\n";
        let (evs, _, _) = drive(raw, false);
        assert_eq!(
            evs,
            vec![ModelStreamEvent::WebSearchResult {
                id: "srv2".into(),
                results: vec![],
                error: Some("max_uses_exceeded".into()),
            }]
        );
    }

    #[test]
    fn structured_tool_is_captured_not_surfaced() {
        let raw = concat!(
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu\",\"name\":\"emit_result\",\"input\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":1}\"}}\n\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        );
        // With `forced` true, the synthetic tool surfaces no events.
        let (evs, _, _) = drive(raw, true);
        assert!(evs.is_empty());
    }

    #[test]
    fn merge_beta_header_dedups_and_appends() {
        let mut h = HashMap::new();
        merge_beta_header(&mut h, "alpha");
        merge_beta_header(&mut h, "beta");
        merge_beta_header(&mut h, "alpha");
        assert_eq!(h.get("anthropic-beta").unwrap(), "alpha,beta");
    }

    #[test]
    fn merge_beta_header_case_insensitive_existing() {
        let mut h = HashMap::new();
        h.insert("Anthropic-Beta".to_string(), "oauth".to_string());
        merge_beta_header(&mut h, "alpha");
        assert_eq!(h.get("Anthropic-Beta").unwrap(), "oauth,alpha");
    }

    #[test]
    fn effort_downgrade_rules() {
        use crate::types::provider::{EffortCapability, ModelCapabilities};
        let caps = |levels: &[EffortLevel]| ModelCapabilities {
            effort: Some(EffortCapability {
                levels: levels.to_vec(),
                default: None,
            }),
            ..Default::default()
        };
        // A ladder without xhigh clamps an xhigh request down to the highest
        // supported rung at or below it (high).
        let no_xhigh = caps(&[
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Max,
        ]);
        assert_eq!(
            resolve_effort_level(EffortValue::Level(EffortLevel::Xhigh), &no_xhigh),
            Some(EffortLevel::High)
        );
        // A ladder that declares xhigh keeps it.
        let with_xhigh = caps(&[
            EffortLevel::Low,
            EffortLevel::Medium,
            EffortLevel::High,
            EffortLevel::Xhigh,
            EffortLevel::Max,
        ]);
        assert_eq!(
            resolve_effort_level(EffortValue::Level(EffortLevel::Xhigh), &with_xhigh),
            Some(EffortLevel::Xhigh)
        );
        // Numeric efforts bucket to low/medium/high then clamp to the ladder.
        assert_eq!(
            resolve_effort_level(EffortValue::Number(20), &with_xhigh),
            Some(EffortLevel::Low)
        );
        assert_eq!(
            resolve_effort_level(EffortValue::Number(50), &with_xhigh),
            Some(EffortLevel::Medium)
        );
        // No declared effort capability → no effort knob.
        assert_eq!(
            resolve_effort_level(
                EffortValue::Level(EffortLevel::High),
                &ModelCapabilities::default()
            ),
            None
        );
    }

    #[test]
    fn ultra_resolves_for_anthropic_without_a_provider_ultra_value() {
        use crate::types::provider::EffortCapability;
        let effort = EffortValue::Level(EffortLevel::Ultra);
        for (levels, expected) in [
            (
                vec![EffortLevel::High, EffortLevel::Xhigh, EffortLevel::Max],
                Some(EffortLevel::Xhigh),
            ),
            (
                vec![EffortLevel::Medium, EffortLevel::High, EffortLevel::Max],
                Some(EffortLevel::High),
            ),
            (vec![], None),
        ] {
            let caps = ModelCapabilities {
                effort: Some(EffortCapability {
                    levels,
                    default: None,
                }),
                ..Default::default()
            };
            assert_eq!(resolve_effort_level(effort.clone(), &caps), expected);
        }
        assert_eq!(
            resolve_effort_level(effort, &ModelCapabilities::default()),
            None
        );
    }

    #[test]
    fn forced_structured_thinking_preserves_first_party_and_native_requests() {
        let caps = ModelCapabilities {
            thinking: Some(crate::types::provider::ThinkingCapability {
                adaptive: true,
                interleaved: false,
                can_disable_thinking: true,
            }),
            ..Default::default()
        };
        let mut request = base_req();
        request.thinking = Some(ThinkingConfig::Adaptive);
        assert_eq!(resolve_wire_thinking(&request, &caps, true, true), None);
        assert_eq!(
            resolve_wire_thinking(&request, &caps, false, true),
            Some(exact_json!({ "type": "disabled" }))
        );
        for first_party in [true, false] {
            assert_eq!(
                resolve_wire_thinking(&request, &caps, first_party, false),
                Some(exact_json!({ "type": "adaptive" }))
            );
        }
    }

    #[test]
    fn thinking_resolution() {
        use crate::types::provider::{ModelCapabilities, ThinkingCapability};
        let adaptive = ModelCapabilities {
            thinking: Some(ThinkingCapability {
                adaptive: true,
                interleaved: false,
                can_disable_thinking: true,
            }),
            ..Default::default()
        };
        let non_adaptive = ModelCapabilities {
            thinking: Some(ThinkingCapability::default()),
            ..Default::default()
        };
        assert_eq!(
            resolve_thinking(ThinkingConfig::Adaptive, &adaptive, 1000),
            Some(exact_json!({ "type": "adaptive" }))
        );
        assert_eq!(
            resolve_thinking(
                ThinkingConfig::Enabled {
                    budget_tokens: Some(5000)
                },
                &adaptive,
                1000
            ),
            Some(exact_json!({ "type": "enabled", "budget_tokens": 999 }))
        );
        assert_eq!(
            resolve_thinking(ThinkingConfig::Disabled, &adaptive, 1000),
            None
        );
        // No declared thinking capability → no thinking block, even for adaptive.
        assert_eq!(
            resolve_thinking(
                ThinkingConfig::Adaptive,
                &ModelCapabilities::default(),
                1000
            ),
            None
        );
        // Thinking declared but not adaptive → an adaptive request falls to a
        // budgeted block instead.
        assert_eq!(
            resolve_thinking(ThinkingConfig::Adaptive, &non_adaptive, 1000),
            Some(exact_json!({ "type": "enabled", "budget_tokens": 999 }))
        );
    }

    #[test]
    fn stop_reason_mapping() {
        assert_eq!(
            map_stop_reason(&RuntimeString::from("tool_use")),
            ModelStopReason::ToolUse
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("max_tokens")),
            ModelStopReason::MaxTokens
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("stop_sequence")),
            ModelStopReason::StopSequence
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("refusal")),
            ModelStopReason::Refusal
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("pause_turn")),
            ModelStopReason::EndTurn
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("end_turn")),
            ModelStopReason::EndTurn
        );
        assert_eq!(
            map_stop_reason(&RuntimeString::from("whatever")),
            ModelStopReason::EndTurn
        );
    }

    #[test]
    fn normalize_model_id_strips_window_suffix() {
        assert_eq!(normalize_model_id("claude-opus-4-8[1m]"), "claude-opus-4-8");
        assert_eq!(normalize_model_id("claude-opus-4-8"), "claude-opus-4-8");
    }

    #[test]
    fn sse_decoder_splits_on_blank_lines() {
        let mut d = SseDecoder::new();
        d.push(b"data: {\"a\":1}\n\ndata: {\"b\"");
        let first = d.next_block().unwrap();
        assert_eq!(parse_sse_data(&first).unwrap(), exact_json!({ "a": 1 }));
        // The second event is incomplete until its blank line arrives.
        assert!(d.next_block().is_none());
        d.push(b":2}\n\n");
        let second = d.next_block().unwrap();
        assert_eq!(parse_sse_data(&second).unwrap(), exact_json!({ "b": 2 }));
    }

    #[test]
    fn estimate_tokens_fallback_formula() {
        let r = base_req();
        let msg_json = runtime_codec::encode_text(&r.messages[0]).unwrap();
        let chars = RuntimeString::from("You are helpful.").len_utf16() + msg_json.len_utf16();
        let expected = chars.div_ceil(4) as u64 + 1000;
        assert_eq!(estimate_tokens_fallback(&r), expected);
    }

    #[test]
    fn estimate_tokens_fallback_matches_javascript_message_utf16_oracle() {
        let mut r = base_req();
        r.system.clear();
        r.messages = vec![user_msg("😀😀")];

        assert_eq!(estimate_tokens_fallback(&r), 1_018);
    }

    #[test]
    fn estimate_tokens_fallback_matches_javascript_lone_surrogate_utf16_oracle() {
        let value = runtime_json::parse(&RuntimeString::from(r#""😀😀\ud800""#))
            .expect("JavaScript JSON parser must preserve lone surrogates");
        let text = value.as_str().expect("fixture is a JSON string");
        let mut r = base_req();
        r.system = vec![sys(text.clone())];
        r.messages.clear();

        assert_eq!(estimate_tokens_fallback(&r), 1_002);
    }

    #[test]
    fn estimate_tokens_fallback_ignores_tool_format() {
        let mut text = base_req();
        text.system.clear();
        text.messages.clear();
        text.tools = vec![text_spec("exec", "execute javascript source")];

        let mut grammar = text.clone();
        grammar.tools[0].format = Some(ToolSpecFormat::Grammar {
            syntax: ToolSpecGrammarSyntax::Lark,
            definition: RuntimeString::from("start: /.+/"),
        });

        assert_eq!(
            estimate_tokens_fallback(&text),
            estimate_tokens_fallback(&grammar)
        );
    }

    #[test]
    fn estimate_tokens_fallback_includes_freeform_tool_fields() {
        let mut r = base_req();
        let without = estimate_tokens_fallback(&r);
        r.tools = vec![text_spec("exec", "execute javascript source")];
        assert!(estimate_tokens_fallback(&r) > without);
    }

    #[test]
    fn capabilities_non_first_party_gate() {
        use crate::types::provider::ModelCapabilities;
        // A custom base URL forces non-first-party regardless of environment, so
        // native web search stays off even when the client DECLARES it. Native
        // PDF is a plain declared capability and flows through.
        let p = AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some("https://gateway.example".into()),
            capabilities: Some(ModelCapabilities {
                web_search: true,
                native_pdf: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        let c = p.capabilities(&"any-model".into());
        assert!(
            !c.web_search,
            "non-first-party gate wins over the declared capability"
        );
        assert_eq!(c.context_window, DEFAULT_CONTEXT_WINDOW);
        assert!(c.native_pdf);
        assert!(c.streaming && c.tools && c.thinking && c.structured_output);
        assert_eq!(c.max_output_tokens, DEFAULT_MAX_OUTPUT);
        // No declared capabilities → minimal (no native pdf), whatever the id.
        let minimal = AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some("https://gateway.example".into()),
            ..Default::default()
        });
        assert!(!minimal.capabilities(&"any-model".into()).native_pdf);
    }

    #[test]
    fn protected_first_party_contract_ignores_all_endpoint_overrides() {
        use crate::types::provider::ModelCapabilities;
        let p = AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some("https://explicit-gateway.invalid".into()),
            auth_source: Some(Arc::new(CountingAuth::default())),
            protected_first_party_contract: true,
            capabilities: Some(ModelCapabilities {
                web_search: true,
                context_window: Some(1_000_000),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(p.info().base_url, None);
        let capabilities = p.capabilities(&"any-model".into());
        // Protected contract forces first-party, so the declared web search is honored.
        assert!(capabilities.web_search);
        assert_eq!(capabilities.context_window, 1_000_000);
    }

    #[test]
    fn info_defaults() {
        let p = AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some("https://gw".into()),
            ..Default::default()
        });
        assert_eq!(p.info().name, "anthropic");
        assert_eq!(p.info().default_model, "claude-sonnet-5");
        assert!(p
            .info()
            .env_key
            .as_ref()
            .is_some_and(|value| value == "ANTHROPIC_API_KEY"));
        assert!(p
            .info()
            .base_url
            .as_ref()
            .is_some_and(|value| value == "https://gw"));
    }

    #[test]
    fn cache_breakpoint_budget_capped_at_four() {
        // Five cacheable system parts + tools + message would exceed 4; only the
        // last 4 breakpoints survive.
        let mut parts: Vec<SystemPromptPart> = (0..5)
            .map(|i| SystemPromptPart {
                name: RuntimeString::from(format!("p{i}")),
                text: RuntimeString::from(format!("part {i}")),
                cacheable: Some(true),
                cache_scope: None,
            })
            .collect();
        parts.push(sys("tail"));
        let mut r = base_req();
        r.system = parts;
        let p = build_params(&r, false, false, false, false);
        let system = p.get("system").unwrap().as_array().unwrap();
        let sys_marks = system
            .iter()
            .filter(|b| b.get("cache_control").is_some())
            .count();
        let messages = p.get("messages").unwrap().as_array().unwrap();
        let msg_marks: usize = messages
            .iter()
            .map(|m| {
                m.get("content")
                    .and_then(Value::as_array)
                    .map(|c| {
                        c.iter()
                            .filter(|b| b.get("cache_control").is_some())
                            .count()
                    })
                    .unwrap_or(0)
            })
            .sum();
        assert_eq!(sys_marks + msg_marks, MAX_CACHE_BREAKPOINTS);
    }

    #[test]
    fn skip_cache_write_keeps_the_ephemeral_tail_out_of_the_cache() {
        // A summary-style request reuses the live cached prefix but must not
        // create a cache entry for its one-off tail: the message breakpoint
        // steps back to the penultimate message, which the prefix already
        // cached, so the request reads without writing new cache.
        let mut r = base_req();
        r.messages = vec![
            user_msg("history"),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                model: r.model.clone(),
                usage: None,
                content: vec![ContentBlock::Text(TextBlock {
                    text: "draft".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
            }),
            user_msg("summary prompt"),
        ];
        let writing = build_params(&r, false, false, false, false);
        let msgs = writing.get("messages").unwrap().as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert!(msgs[2]["content"][0].get("cache_control").is_some());
        assert!(msgs[1]["content"][0].get("cache_control").is_none());

        r.skip_cache_write = Some(true);
        let skipping = build_params(&r, false, false, false, false);
        let msgs = skipping.get("messages").unwrap().as_array().unwrap();
        assert!(
            msgs[2]["content"][0].get("cache_control").is_none(),
            "the ephemeral tail must not carry a cache breakpoint"
        );
        assert!(
            msgs[1]["content"][0].get("cache_control").is_some(),
            "the breakpoint steps back to the penultimate message"
        );
    }
}

#[cfg(test)]
mod context_hint_transport_tests {
    use super::*;
    use crate::context::context_hints::{ContextHintRequest, CONTEXT_HINT_CLEARED_MARKER};
    use crate::schemas::content::{TextBlock, ToolUseBlock};
    use crate::schemas::message::AssistantMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const DONE: &str = "data: {\"type\":\"message_stop\"}\n\n";

    fn hint_request() -> ModelRequest {
        let value = runtime_json::parse(&RuntimeString::from(
            r#"{"model":"hint-model","system":[],"messages":[],"tools":[],"maxOutputTokens":128}"#,
        ))
        .unwrap();
        let mut request: ModelRequest = runtime_codec::decode_value(&value).unwrap();
        request.context_hints = Some(ContextHintRequest {
            enabled: true,
            main_thread: true,
            artifact_dir: None,
            cwd: crate::runtime_values::RuntimePath::default(),
            read_state: None,
            state: Arc::new(crate::context::context_hints::ContextHintState::new()),
        });
        for index in 0..6 {
            let id = RuntimeString::from(format!("hint-call-{index}"));
            request.messages.push(Message::Assistant(AssistantMessage {
                uuid: format!("hint-assistant-{index}").into(),
                model: "hint-model".into(),
                usage: None,
                content: vec![ContentBlock::ToolUse(ToolUseBlock {
                    id: id.clone(),
                    name: "read".into(),
                    input: runtime_json::parse(&"{}".into()).unwrap(),
                    tool_kind: None,
                    thought_signature: None,
                })],
            }));
            request
                .messages
                .push(Message::ToolResult(ToolResultMessage {
                    uuid: format!("hint-result-{index}").into(),
                    tool_use_id: id,
                    content: vec![OutputBlock::Text(TextBlock {
                        text: if index == 0 {
                            "x".repeat(80_000)
                        } else {
                            format!("recent-{index}")
                        }
                        .into(),
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
                }));
        }
        request
    }

    async fn wire_fixture(
        responses: Vec<(u16, &'static str)>,
    ) -> (Vec<(String, serde_json::Value)>, Vec<ModelStreamEvent>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut provider = AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("fixture-only".into()),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            retry: Some(RetryOptions {
                max_attempts: Some(2),
                congestion_hold_ms: Some(0),
                unbounded_network_retries: Some(false),
                sleep: Some(Arc::new(|_, _| Box::pin(async { Ok(()) }))),
                ..Default::default()
            }),
            ..Default::default()
        });
        // Only this local fixture substitutes for the trusted route; production
        // construction continues to derive identity from endpoint/auth routing.
        provider.is_first_party = true;
        let server = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (status, response_body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0u8; 8192];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if bytes.len() < end + 4 + length {
                            continue;
                        }
                        let body =
                            serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                        captured.push((headers, body));
                        break;
                    }
                }
                let mime = if status == 200 {
                    "text/event-stream"
                } else {
                    "application/json"
                };
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        });
        let original = hint_request();
        let canonical = original.messages.clone();
        let events = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider
                .stream(original.clone(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        let captured = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.messages, canonical);
        (captured, events)
    }

    #[tokio::test]
    async fn rejected_context_hint_clears_old_output_once_then_strips_the_beta() {
        for status in [422, 424] {
            let (requests, events) = wire_fixture(vec![
                (
                    status,
                    r#"{"error":{"type":"invalid_request_error","message":"cannot apply hint"}}"#,
                ),
                (200, DONE),
            ])
            .await;
            assert_eq!(requests.len(), 2);
            assert!(requests[0].0.contains(CONTEXT_HINT_BETA));
            assert_eq!(
                requests[0].1["context_hint"],
                serde_json::json!({"enabled":true,"target_tokens_saved":75000})
            );
            assert!(!requests[1].0.contains(CONTEXT_HINT_BETA));
            assert!(requests[1].1.get("context_hint").is_none());
            assert!(requests[1]
                .1
                .to_string()
                .contains(CONTEXT_HINT_CLEARED_MARKER));
            assert!(!requests[1].1.to_string().contains(&"x".repeat(80_000)));
            assert!(events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
        }
    }

    #[tokio::test]
    async fn unsupported_hint_beta_and_busy_retry_never_clear_old_output() {
        for (status, response) in [
            (
                400,
                r#"{"error":{"type":"invalid_request_error","message":"Unexpected value in anthropic-beta"}}"#,
            ),
            (
                529,
                r#"{"error":{"type":"overloaded_error","message":"busy"}}"#,
            ),
        ] {
            let (requests, _) = wire_fixture(vec![(status, response), (200, DONE)]).await;
            assert_eq!(requests.len(), 2);
            assert!(!requests[1].0.contains(CONTEXT_HINT_BETA));
            assert!(requests[1].1.get("context_hint").is_none());
            assert!(requests[1].1.to_string().contains(&"x".repeat(80_000)));
            assert!(!requests[1]
                .1
                .to_string()
                .contains(CONTEXT_HINT_CLEARED_MARKER));
        }
    }

    #[tokio::test]
    async fn repeated_hint_rejection_and_unrelated_failures_remain_terminal() {
        let rejection =
            r#"{"error":{"type":"invalid_request_error","message":"cannot apply hint"}}"#;
        let (requests, events) = wire_fixture(vec![(422, rejection), (422, rejection)]).await;
        assert_eq!(requests.len(), 2);
        assert!(events.iter().any(
            |event| matches!(event, ModelStreamEvent::Error { error } if error.status == Some(422))
        ));
        for status in [400, 409] {
            let (requests, events) = wire_fixture(vec![(status, rejection)]).await;
            assert_eq!(requests.len(), 1);
            assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::Error { error } if error.status == Some(status))));
        }
    }

    #[tokio::test]
    async fn statusless_hint_stream_rejection_requests_query_owned_fallback() {
        for body in [
            "data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"hint stream failed\"}}\n\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"hint-model\"}}\n\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"hint stream failed\"}}\n\n",
        ] {
            let (requests, events) = wire_fixture(vec![(200, body)]).await;
            assert_eq!(requests.len(), 1);
            assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::Error { error }
                if error.code.as_ref().is_some_and(|code| code == CONTEXT_HINT_STREAM_FALLBACK_CODE))));
        }
    }
}

#[cfg(test)]
mod empty_text_thinking_replay {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock, ThinkingBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, ToolResultMessage, UserMessage};

    fn think(sig: &str, text: &str) -> ContentBlock {
        ContentBlock::Thinking(ThinkingBlock {
            text: text.into(),
            message_id: None,
            signature: Some(sig.into()),
            encrypted_content: None,
        })
    }
    fn tool_use(id: &str) -> ContentBlock {
        ContentBlock::ToolUse(ToolUseBlock {
            id: id.into(),
            name: "Grep".into(),
            input: crate::runtime_values::RuntimeJson::Object(Default::default()),
            tool_kind: None,
            thought_signature: None,
        })
    }
    fn tr(id: &str, n: u32) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("tr{n}").into(),
            tool_use_id: id.into(),
            content: vec![],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }
    fn assistant(uuid: &str, model: &str, content: Vec<ContentBlock>) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            model: model.into(),
            usage: None,
            content,
        })
    }

    /// Streams that never carried visible reasoning persist signed thinking
    /// blocks with empty text. Replaying them makes the strict endpoint reject
    /// the whole request, so the wire view must drop them even when the
    /// producing model matches the request target.
    #[test]
    fn signed_empty_text_thinking_is_not_replayed() {
        let messages = vec![
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u0".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "hi".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            assistant(
                "a1",
                "claude-opus-5-5",
                vec![think("SIG", ""), tool_use("t1")],
            ),
            tr("t1", 2),
        ];
        let out = to_api_messages(&messages, "claude-opus-5-5", &[]);
        let blocks = out[1].get("content").unwrap().as_array().unwrap();
        let kind = |v: &Value| {
            v.get("type")
                .and_then(Value::as_str)
                .and_then(|s| s.try_to_utf8().ok())
        };
        assert!(
            blocks
                .iter()
                .all(|b| kind(b).as_deref() != Some("thinking")),
            "empty-text signed thinking must not reach the wire"
        );
        assert_eq!(kind(&blocks[0]).as_deref(), Some("tool_use"));
    }

    /// Signed thinking that DOES carry its text replays unchanged: the
    /// signature stays paired with the content it was issued for.
    #[test]
    fn signed_thinking_with_text_still_replays() {
        let messages = vec![assistant(
            "a1",
            "claude-opus-5-5",
            vec![think("SIG", "reasoning text"), tool_use("t1")],
        )];
        let out = to_api_messages(&messages, "claude-opus-5-5", &[]);
        let blocks = out[0].get("content").unwrap().as_array().unwrap();
        let kind = |v: &Value| {
            v.get("type")
                .and_then(Value::as_str)
                .and_then(|s| s.try_to_utf8().ok())
        };
        assert_eq!(kind(&blocks[0]).as_deref(), Some("thinking"));
        assert_eq!(
            blocks[0]
                .get("thinking")
                .and_then(Value::as_str)
                .map(|s| s.len_utf16()),
            Some("reasoning text".len())
        );
        assert_eq!(
            blocks[0]
                .get("signature")
                .and_then(Value::as_str)
                .map(|s| s.len_utf16()),
            Some(3)
        );
    }
}
