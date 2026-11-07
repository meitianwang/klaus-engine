//! Model providers: request assembly, streaming, auth, retry and fallback.

pub mod access_policy;
pub mod anthropic;
pub mod approval_classifier_transport;
pub mod auth;
pub mod aws_eventstream;
pub mod behavior;
pub mod classify;
pub mod claude_subscription;
pub mod codex_subscription;
pub mod gemini;
pub mod gemini_code_assist;
pub mod gemini_subscription;
pub mod interceptor;
pub mod model_catalog;
pub mod model_fallback;
pub mod native_compaction;
pub mod native_media;
pub mod oauth;
pub mod observability;
pub mod openai;
pub(crate) mod output_reservation;
pub mod prewarm;
pub mod retry;
pub mod review_target;
pub mod rotation;
pub mod sigv4;
pub mod tool_choice;
pub(crate) mod util;

/// Reject controls a transport cannot represent before opening a network request.
pub(crate) fn unsupported_inference_control(
    request: &crate::types::provider::ModelRequest,
    provider: &str,
    verbosity_supported: bool,
    summary_supported: bool,
    cached_search_supported: bool,
) -> Option<crate::types::errors::ProviderError> {
    use crate::schemas::config::WebSearchAccess;
    use crate::types::provider::ReasoningSummary;
    let control = if request.model_verbosity.is_some() && !verbosity_supported {
        "modelVerbosity"
    } else if !matches!(
        request.reasoning_summary,
        ReasoningSummary::Auto | ReasoningSummary::None
    ) && !summary_supported
    {
        "reasoningSummary"
    } else if request.web_search.as_ref().is_some_and(|search| {
        search.access == WebSearchAccess::Disabled
            || (!cached_search_supported && search.access != WebSearchAccess::Live)
    }) {
        "webSearch.access"
    } else {
        return None;
    };
    Some(crate::types::errors::ProviderError {
        kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
        message: format!("{provider} cannot apply the requested {control} setting for this model. Choose a supported setting or model.").into(),
        status: None,
        code: Some("unsupported_inference_setting".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false, credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    })
}
