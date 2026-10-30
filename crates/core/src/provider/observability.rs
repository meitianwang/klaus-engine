//! Structured provider decision summaries for host debug and telemetry sinks.

use crate::provider::model_fallback::{classify_model_fallback_error, ModelFallbackKind};
use crate::provider::retry::is_retryable;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Usage;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use serde::{Deserialize, Serialize};

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDecisionEventKind {
    RequestStarted,
    RetryScheduled,
    ModelFallback,
    RequestFailed,
    RequestCompleted,
}

impl RuntimeEncode for ProviderDecisionEventKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::RequestStarted => "request_started",
            Self::RetryScheduled => "retry_scheduled",
            Self::ModelFallback => "model_fallback",
            Self::RequestFailed => "request_failed",
            Self::RequestCompleted => "request_completed",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProviderDecisionEventKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("request_started", &[]),
                RuntimeEnumVariant::new("retry_scheduled", &[]),
                RuntimeEnumVariant::new("model_fallback", &[]),
                RuntimeEnumVariant::new("request_failed", &[]),
                RuntimeEnumVariant::new("request_completed", &[]),
            ],
            &[
                Self::RequestStarted,
                Self::RetryScheduled,
                Self::ModelFallback,
                Self::RequestFailed,
                Self::RequestCompleted,
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRefusalCategory {
    Auth,
    RateLimit,
    Capacity,
    PromptTooLong,
    Quota,
    MaxOutput,
    ContentPolicy,
    Network,
    InvalidRequest,
    Unknown,
}

impl RuntimeEncode for ProviderRefusalCategory {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Auth => "auth",
            Self::RateLimit => "rate_limit",
            Self::Capacity => "capacity",
            Self::PromptTooLong => "prompt_too_long",
            Self::Quota => "quota",
            Self::MaxOutput => "max_output",
            Self::ContentPolicy => "content_policy",
            Self::Network => "network",
            Self::InvalidRequest => "invalid_request",
            Self::Unknown => "unknown",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProviderRefusalCategory {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auth", &[]),
                RuntimeEnumVariant::new("rate_limit", &[]),
                RuntimeEnumVariant::new("capacity", &[]),
                RuntimeEnumVariant::new("prompt_too_long", &[]),
                RuntimeEnumVariant::new("quota", &[]),
                RuntimeEnumVariant::new("max_output", &[]),
                RuntimeEnumVariant::new("content_policy", &[]),
                RuntimeEnumVariant::new("network", &[]),
                RuntimeEnumVariant::new("invalid_request", &[]),
                RuntimeEnumVariant::new("unknown", &[]),
            ],
            &[
                Self::Auth,
                Self::RateLimit,
                Self::Capacity,
                Self::PromptTooLong,
                Self::Quota,
                Self::MaxOutput,
                Self::ContentPolicy,
                Self::Network,
                Self::InvalidRequest,
                Self::Unknown,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequestMetadata {
    pub provider_id: RuntimeString,
    pub model: RuntimeString,
    pub service_tier: Option<RuntimeString>,
    pub tool_trace_id: Option<RuntimeString>,
}

impl RuntimeEncode for ProviderRequestMetadata {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("providerId", &self.provider_id)?;
        object.field("model", &self.model)?;
        if let Some(service_tier) = &self.service_tier {
            object.field("serviceTier", service_tier)?;
        }
        if let Some(tool_trace_id) = &self.tool_trace_id {
            object.field("toolTraceId", tool_trace_id)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderRequestMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                provider_id: object.required("providerId", &[])?,
                model: object.required("model", &[])?,
                service_tier: object.optional("serviceTier", &[])?,
                tool_trace_id: object.optional("toolTraceId", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderDebugContext {
    pub request_id: Option<RuntimeString>,
    pub status: Option<u16>,
    pub error_kind: Option<ProviderErrorKind>,
    pub refusal_category: Option<ProviderRefusalCategory>,
    pub fallback_kind: Option<ModelFallbackKind>,
    pub retryable: Option<bool>,
    pub retry_after_ms: Option<u64>,
}

impl RuntimeEncode for ProviderDebugContext {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.request_id {
            object.field("requestId", value)?;
        }
        if let Some(value) = &self.status {
            object.field("status", value)?;
        }
        if let Some(value) = &self.error_kind {
            object.field("errorKind", value)?;
        }
        if let Some(value) = &self.refusal_category {
            object.field("refusalCategory", value)?;
        }
        if let Some(value) = &self.fallback_kind {
            object.field("fallbackKind", value)?;
        }
        if let Some(value) = &self.retryable {
            object.field("retryable", value)?;
        }
        if let Some(value) = &self.retry_after_ms {
            object.field("retryAfterMs", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderDebugContext {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.optional("requestId", &[])?,
                status: object.optional("status", &[])?,
                error_kind: object.optional("errorKind", &[])?,
                refusal_category: object.optional("refusalCategory", &[])?,
                fallback_kind: object.optional("fallbackKind", &[])?,
                retryable: object.optional("retryable", &[])?,
                retry_after_ms: object.optional("retryAfterMs", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageSummary {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub total_tokens: u64,
    pub cache_hit_rate: f64,
}

impl RuntimeEncode for ProviderUsageSummary {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            total_tokens,
            cache_hit_rate,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("inputTokens", input_tokens)?;
        object.field("outputTokens", output_tokens)?;
        object.field("cacheReadInputTokens", cache_read_input_tokens)?;
        object.field("cacheCreationInputTokens", cache_creation_input_tokens)?;
        object.field("totalTokens", total_tokens)?;
        object.field("cacheHitRate", cache_hit_rate)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderUsageSummary {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                input_tokens: object.required("inputTokens", &[])?,
                output_tokens: object.required("outputTokens", &[])?,
                cache_read_input_tokens: object.required("cacheReadInputTokens", &[])?,
                cache_creation_input_tokens: object.required("cacheCreationInputTokens", &[])?,
                total_tokens: object.required("totalTokens", &[])?,
                cache_hit_rate: object.required("cacheHitRate", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPhase {
    /// The stream broke after the request was established; the engine turn loop
    /// re-samples.
    Stream,
    /// The request never established; the provider's bounded establishment
    /// policy scheduled the next attempt.
    Connection,
    /// Establishment kept failing on a dead network past the attempt cap; the
    /// policy waits for the network instead of counting retries.
    NetworkWait,
}

impl RuntimeEncode for RetryPhase {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Stream => "stream",
            Self::Connection => "connection",
            Self::NetworkWait => "networkWait",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for RetryPhase {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("stream", &[]),
                RuntimeEnumVariant::new("connection", &[]),
                RuntimeEnumVariant::new("networkWait", &[]),
            ],
            &[Self::Stream, Self::Connection, Self::NetworkWait],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderDecisionEvent {
    pub kind: ProviderDecisionEventKind,
    pub provider_id: RuntimeString,
    pub model: RuntimeString,
    pub service_tier: Option<RuntimeString>,
    pub tool_trace_id: Option<RuntimeString>,
    pub attempt: u32,
    /// Total attempts the retry budget allows for this phase, so hosts can
    /// render "n/m" without knowing the engine's configured budget. Absent for
    /// decisions that carry no budget (request started / completed / failed).
    pub max_attempts: Option<u32>,
    /// Which retry budget scheduled this decision. Absent for non-retry
    /// decisions.
    pub phase: Option<RetryPhase>,
    pub next_model: Option<RuntimeString>,
    pub delay_ms: Option<u64>,
    pub debug: ProviderDebugContext,
    pub usage: Option<ProviderUsageSummary>,
}

impl RuntimeEncode for ProviderDecisionEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("providerId", &self.provider_id)?;
        object.field("model", &self.model)?;
        if let Some(value) = &self.service_tier {
            object.field("serviceTier", value)?;
        }
        if let Some(value) = &self.tool_trace_id {
            object.field("toolTraceId", value)?;
        }
        object.field("attempt", &self.attempt)?;
        if let Some(value) = &self.max_attempts {
            object.field("maxAttempts", value)?;
        }
        if let Some(value) = &self.phase {
            object.field("phase", value)?;
        }
        if let Some(value) = &self.next_model {
            object.field("nextModel", value)?;
        }
        if let Some(value) = &self.delay_ms {
            object.field("delayMs", value)?;
        }
        object.field("debug", &self.debug)?;
        if let Some(value) = &self.usage {
            object.field("usage", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderDecisionEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                provider_id: object.required("providerId", &[])?,
                model: object.required("model", &[])?,
                service_tier: object.optional("serviceTier", &[])?,
                tool_trace_id: object.optional("toolTraceId", &[])?,
                attempt: object.required("attempt", &[])?,
                max_attempts: object.optional("maxAttempts", &[])?,
                phase: object.optional("phase", &[])?,
                next_model: object.optional("nextModel", &[])?,
                delay_ms: object.optional("delayMs", &[])?,
                debug: object.required("debug", &[])?,
                usage: object.optional("usage", &[])?,
            })
        })
    }
}

impl ProviderRequestMetadata {
    pub fn new(provider_id: impl Into<RuntimeString>, model: impl Into<RuntimeString>) -> Self {
        Self {
            provider_id: provider_id.into(),
            model: model.into(),
            service_tier: None,
            tool_trace_id: None,
        }
    }

    pub fn with_service_tier(mut self, service_tier: impl Into<RuntimeString>) -> Self {
        self.service_tier = Some(service_tier.into());
        self
    }

    pub fn with_tool_trace_id(mut self, tool_trace_id: impl Into<RuntimeString>) -> Self {
        self.tool_trace_id = Some(tool_trace_id.into());
        self
    }
}

pub fn classify_provider_refusal(error: &ProviderError) -> ProviderRefusalCategory {
    match error.kind {
        ProviderErrorKind::Auth => ProviderRefusalCategory::Auth,
        ProviderErrorKind::RateLimit => ProviderRefusalCategory::RateLimit,
        ProviderErrorKind::Overloaded => ProviderRefusalCategory::Capacity,
        ProviderErrorKind::PromptTooLong => ProviderRefusalCategory::PromptTooLong,
        ProviderErrorKind::Quota => ProviderRefusalCategory::Quota,
        ProviderErrorKind::MaxOutput => ProviderRefusalCategory::MaxOutput,
        ProviderErrorKind::Withheld => ProviderRefusalCategory::ContentPolicy,
        ProviderErrorKind::Network => ProviderRefusalCategory::Network,
        ProviderErrorKind::Timeout => ProviderRefusalCategory::Network,
        ProviderErrorKind::InvalidRequest | ProviderErrorKind::RequestTooLarge => {
            ProviderRefusalCategory::InvalidRequest
        }
        ProviderErrorKind::Unknown => ProviderRefusalCategory::Unknown,
    }
}

pub fn request_started(metadata: &ProviderRequestMetadata, attempt: u32) -> ProviderDecisionEvent {
    base_event(ProviderDecisionEventKind::RequestStarted, metadata, attempt)
}

pub fn request_completed(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
    usage: &Usage,
) -> ProviderDecisionEvent {
    request_finished(metadata, attempt, Some(usage))
}

/// Successful requests can settle without a provider usage report.
pub fn request_finished(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
    usage: Option<&Usage>,
) -> ProviderDecisionEvent {
    let mut event = base_event(
        ProviderDecisionEventKind::RequestCompleted,
        metadata,
        attempt,
    );
    event.usage = usage.map(usage_summary);
    event
}

/// A local admission or validation failure may have no upstream error report.
pub(crate) fn request_interrupted(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
) -> ProviderDecisionEvent {
    base_event(ProviderDecisionEventKind::RequestFailed, metadata, attempt)
}

pub fn request_failed(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
    error: &ProviderError,
) -> ProviderDecisionEvent {
    let mut event = base_event(ProviderDecisionEventKind::RequestFailed, metadata, attempt);
    event.debug = debug_context(error);
    event
}

pub fn retry_scheduled(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
    max_attempts: u32,
    phase: RetryPhase,
    delay_ms: u64,
    error: &ProviderError,
) -> ProviderDecisionEvent {
    let mut event = base_event(ProviderDecisionEventKind::RetryScheduled, metadata, attempt);
    event.max_attempts = Some(max_attempts);
    event.phase = Some(phase);
    event.delay_ms = Some(delay_ms);
    event.debug = debug_context(error);
    event
}

pub fn model_fallback(
    metadata: &ProviderRequestMetadata,
    attempt: u32,
    next_model: impl Into<RuntimeString>,
    error: &ProviderError,
) -> Option<ProviderDecisionEvent> {
    classify_model_fallback_error(error)?;
    let mut event = base_event(ProviderDecisionEventKind::ModelFallback, metadata, attempt);
    event.next_model = Some(next_model.into());
    event.debug = debug_context(error);
    Some(event)
}

fn base_event(
    kind: ProviderDecisionEventKind,
    metadata: &ProviderRequestMetadata,
    attempt: u32,
) -> ProviderDecisionEvent {
    ProviderDecisionEvent {
        kind,
        provider_id: metadata.provider_id.clone(),
        model: metadata.model.clone(),
        service_tier: metadata.service_tier.clone(),
        tool_trace_id: metadata.tool_trace_id.clone(),
        attempt,
        max_attempts: None,
        phase: None,
        next_model: None,
        delay_ms: None,
        debug: ProviderDebugContext::default(),
        usage: None,
    }
}

fn usage_summary(usage: &Usage) -> ProviderUsageSummary {
    let prompt_tokens =
        usage.input_tokens + usage.cache_read_input_tokens + usage.cache_creation_input_tokens;
    ProviderUsageSummary {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        total_tokens: usage.total_tokens.unwrap_or(
            usage.input_tokens
                + usage.output_tokens
                + usage.cache_read_input_tokens
                + usage.cache_creation_input_tokens,
        ),
        cache_hit_rate: if prompt_tokens == 0 {
            0.0
        } else {
            usage.cache_read_input_tokens as f64 / prompt_tokens as f64
        },
    }
}

fn debug_context(error: &ProviderError) -> ProviderDebugContext {
    ProviderDebugContext {
        request_id: error.request_id.clone(),
        status: error.status,
        error_kind: Some(error.kind),
        refusal_category: Some(classify_provider_refusal(error)),
        fallback_kind: classify_model_fallback_error(error),
        retryable: Some(is_retryable(error)),
        retry_after_ms: error.retry_after_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> ProviderRequestMetadata {
        ProviderRequestMetadata::new("openai", "gpt-4.1")
            .with_service_tier("flex")
            .with_tool_trace_id("tool-trace-42")
    }

    fn error(kind: ProviderErrorKind, message: &str, status: Option<u16>) -> ProviderError {
        ProviderError {
            kind,
            message: RuntimeString::from(message),
            status,
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            retry_after_ms: None,
            request_id: Some(RuntimeString::from("req_123")),
        }
    }

    #[test]
    fn fallback_event_records_reason_next_model_and_public_request_context() {
        let error = error(
            ProviderErrorKind::Unknown,
            "model gpt-old not found",
            Some(404),
        );

        let event = model_fallback(&metadata(), 2, "gpt-4.1-mini", &error)
            .expect("model-not-found should produce a fallback event");

        assert_eq!(event.kind, ProviderDecisionEventKind::ModelFallback);
        assert_eq!(event.provider_id, "openai");
        assert_eq!(event.model, "gpt-4.1");
        assert!(event
            .service_tier
            .as_ref()
            .is_some_and(|value| value == "flex"));
        assert!(event
            .tool_trace_id
            .as_ref()
            .is_some_and(|value| value == "tool-trace-42"));
        assert_eq!(event.attempt, 2);
        assert!(event
            .next_model
            .as_ref()
            .is_some_and(|value| value == "gpt-4.1-mini"));
        assert_eq!(
            event.debug.fallback_kind,
            Some(ModelFallbackKind::ModelNotFound)
        );
        assert!(event
            .debug
            .request_id
            .as_ref()
            .is_some_and(|value| value == "req_123"));
        assert_eq!(event.debug.status, Some(404));
        assert_eq!(event.debug.retryable, Some(false));
    }

    #[test]
    fn refusal_normalization_uses_public_categories() {
        assert_eq!(
            classify_provider_refusal(&error(
                ProviderErrorKind::PromptTooLong,
                "context too large",
                Some(413),
            )),
            ProviderRefusalCategory::PromptTooLong
        );
        assert_eq!(
            classify_provider_refusal(&error(
                ProviderErrorKind::Withheld,
                "blocked by policy",
                Some(400),
            )),
            ProviderRefusalCategory::ContentPolicy
        );
        let overloaded = error(ProviderErrorKind::Overloaded, "capacity", Some(529));
        assert_eq!(
            classify_provider_refusal(&overloaded),
            ProviderRefusalCategory::Capacity
        );
    }

    #[test]
    fn retry_and_terminal_error_events_preserve_retry_decision() {
        let mut rate_limited = error(ProviderErrorKind::RateLimit, "slow down", Some(429));
        rate_limited.retry_after_ms = Some(1_500);

        let retry = retry_scheduled(
            &metadata(),
            1,
            3,
            RetryPhase::Connection,
            1_500,
            &rate_limited,
        );

        assert_eq!(retry.kind, ProviderDecisionEventKind::RetryScheduled);
        assert_eq!(retry.delay_ms, Some(1_500));
        assert_eq!(retry.debug.retryable, Some(true));
        assert_eq!(retry.debug.retry_after_ms, Some(1_500));
        assert_eq!(
            retry.debug.refusal_category,
            Some(ProviderRefusalCategory::RateLimit)
        );

        let invalid = error(ProviderErrorKind::InvalidRequest, "bad request", Some(400));
        let failed = request_failed(&metadata(), 1, &invalid);

        assert_eq!(failed.kind, ProviderDecisionEventKind::RequestFailed);
        assert_eq!(failed.debug.retryable, Some(false));
        assert_eq!(
            failed.debug.refusal_category,
            Some(ProviderRefusalCategory::InvalidRequest)
        );
    }

    #[test]
    fn completed_event_summarizes_usage_and_cache_rate() {
        let usage = Usage {
            input_tokens: 10,
            output_tokens: 7,
            cache_read_input_tokens: 15,
            cache_creation_input_tokens: 5,
            total_tokens: Some(99),
        };

        let event = request_completed(&metadata(), 1, &usage);
        let summary = event.usage.expect("completion should include usage");

        assert_eq!(summary.input_tokens, 10);
        assert_eq!(summary.output_tokens, 7);
        assert_eq!(summary.cache_read_input_tokens, 15);
        assert_eq!(summary.cache_creation_input_tokens, 5);
        assert_eq!(summary.total_tokens, 99);
        assert_eq!(summary.cache_hit_rate, 0.5);
    }

    #[test]
    fn debug_event_serialization_redacts_provider_message_and_headers() {
        let sensitive = error(
            ProviderErrorKind::Auth,
            "raw body Authorization: Bearer secret-token",
            Some(401),
        );

        let event = request_failed(&metadata(), 1, &sensitive);
        let serialized = crate::runtime_values::runtime_json::stringify(
            &runtime_codec::encode_value(&event).expect("event should encode exactly"),
        );

        assert!(serialized.contains("req_123"));
        assert!(serialized.contains("auth"));
        assert!(!serialized.contains("raw body"));
        assert!(!serialized.contains("Authorization"));
        assert!(!serialized.contains("secret-token"));
    }
}
