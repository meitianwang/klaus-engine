//! Source-facing error vocabulary for the shared direct-cloud transport.
//!
//! The Gemini lineup rides the same authenticated transport as the Antigravity
//! source: one account pool, one backend host, one request identity. This
//! module is the isolation boundary — every error that crosses into a `gemini:`
//! session is rewritten from the shared transport's stable tags into this
//! source's own `gemini_*` vocabulary, so classification, host copy and
//! regression vectors stay source-scoped.

use futures::StreamExt;
use klaus_agent_core::provider::classify::RawProviderError;
use klaus_agent_core::provider::gemini::{
    GeminiApiError, GeminiChunkStream, GeminiClient, GenerateContentParameters,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::AuthHeaders;

/// Stable service tags this source can emit: the shared transport vocabulary
/// re-prefixed for this source, plus the shared context-window tag.
const ERROR_TAGS: [&str; 18] = [
    "gemini_model_access_denied",
    "gemini_model_not_offered",
    "gemini_model_unlisted",
    "gemini_quota_exhausted",
    "gemini_request_rejected",
    "gemini_region_unsupported",
    "gemini_account_disabled",
    "gemini_account_restricted",
    "gemini_verification_required",
    "gemini_permission_denied",
    "gemini_egress_unavailable",
    "gemini_auth_required",
    "gemini_login_expired",
    "gemini_refresh_unavailable",
    "gemini_not_signed_in",
    "gemini_rate_limited",
    "gemini_service_unavailable",
    "model_context_window_exceeded",
];

/// Rewrite the shared transport's tag prefix into this source's vocabulary.
pub(super) fn retag(message: &str) -> String {
    match message.strip_prefix("antigravity_") {
        Some(rest) => format!("gemini_{rest}"),
        None => message.to_string(),
    }
}

pub(super) fn retag_error(mut error: GeminiApiError) -> GeminiApiError {
    error.message = retag(&error.message);
    error
}

fn safe_machine_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Mirror of the shared transport's classifier feed, reading this source's
/// vocabulary. The message enrichments keep the transport semantics the
/// engine's bounded stream recovery relies on.
fn error_details(error: &GeminiApiError) -> RawProviderError {
    let code = ERROR_TAGS.into_iter().find(|tag| {
        error.message == *tag
            || error
                .message
                .strip_prefix(tag)
                .is_some_and(|suffix| suffix.starts_with(":") || suffix.starts_with(" ["))
    });
    let error_type = code
        .and_then(|code| error.message.strip_prefix(code))
        .and_then(|suffix| suffix.strip_prefix(" ["))
        .and_then(|suffix| suffix.split_once(']'))
        .map(|(code, _)| code)
        .filter(|code| safe_machine_code(code));
    let message = match (error.status, error.message.as_str()) {
        (None, "gemini_stream_incomplete") => format!(
            "{}: network stream ended before its terminal event",
            error.message
        ),
        (None, "gemini_stream_interrupted") => format!(
            "{}: network stream interrupted while reading response",
            error.message
        ),
        (None, "gemini_transport_unavailable (connect)") => format!(
            "{}: network connection failed before the request was delivered",
            error.message
        ),
        (None, "gemini_transport_unavailable (sent)") => format!(
            "{}: network transport failed while awaiting the first response byte",
            error.message
        ),
        _ => error.message.clone(),
    };
    RawProviderError {
        status: error.status,
        message,
        code: code.map(str::to_string),
        error_type: error_type.map(str::to_string),
        headers: error.headers.clone().unwrap_or_default(),
        ..Default::default()
    }
}

/// Transport wrapper that keeps the shared client but speaks this source's
/// error vocabulary to the engine.
#[derive(Clone, Default)]
pub(super) struct RetaggingClient {
    inner: super::super::antigravity::cloud::CloudClient,
}

impl RetaggingClient {
    pub(super) fn pooled(conversation_id: String) -> Self {
        Self {
            inner: super::super::antigravity::cloud::CloudClient::pooled(conversation_id),
        }
    }
}

#[async_trait::async_trait]
impl GeminiClient for RetaggingClient {
    fn error_details(&self, error: &GeminiApiError) -> RawProviderError {
        error_details(error)
    }

    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        match self
            .inner
            .generate_content_stream(params, headers, extra_headers, signal)
            .await
        {
            Ok(stream) => Ok(Box::pin(stream.map(|item| item.map_err(retag_error)))),
            Err(error) => Err(retag_error(error)),
        }
    }
}
