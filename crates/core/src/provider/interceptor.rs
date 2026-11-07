//! Transport-level interception seam for provider HTTP requests.
//!
//! An embedder attaches an interceptor to a provider's construction options to
//! shape and observe that provider's traffic without forking it:
//! per-attempt request headers (corporate gateways, rotating tokens, trace
//! ids), an outright veto (policy/cost guards), and a settled-outcome
//! callback (audit sinks). Interception runs once per establishment ATTEMPT —
//! retries re-run it, so short-lived gateway credentials stay fresh — and only
//! at the transport boundary: prompts and streamed content never pass through
//! it, and authentication/protocol headers keep precedence on conflicts so an
//! interceptor can never corrupt signing.
//!
//! Callbacks execute inline on the request path and must stay fast and
//! non-blocking; derive values from cached state rather than performing I/O.

use std::sync::Arc;

/// The request about to be dispatched, as visible at the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequestInfo<'a> {
    /// Stable provider identity (e.g. `anthropic`, `openai`, `gemini`).
    pub provider_id: &'a str,
    /// The model the request targets.
    pub model: &'a str,
    /// The request URL when the transport exposes it before dispatch; a
    /// provider whose inner client owns URL construction reports the logical
    /// endpoint name instead.
    pub url: &'a str,
    /// 1-based establishment attempt; retries increment it.
    pub attempt: u32,
}

/// What the interceptor wants done with one request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderRequestDirectives {
    /// Headers appended AFTER the provider's static option headers and BEFORE
    /// its authentication/protocol headers, which keep precedence on key
    /// conflicts.
    pub extra_headers: Vec<(String, String)>,
    /// Veto: the request fails before reaching the network, as a
    /// non-retryable error carrying this reason.
    pub deny: Option<String>,
}

/// The settled outcome of one establishment attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponseInfo<'a> {
    pub provider_id: &'a str,
    pub model: &'a str,
    /// Same resolution rules as [`ProviderRequestInfo::url`].
    pub url: &'a str,
    /// 1-based establishment attempt this outcome belongs to.
    pub attempt: u32,
    /// Whether the attempt opened a live stream.
    pub ok: bool,
    /// HTTP status when the outcome carried one; `None` on transport errors,
    /// vetoes, and success paths that never surface a status.
    pub status: Option<u16>,
    /// Failure message when the attempt failed.
    pub error: Option<String>,
    /// Wall time from interception to settlement, in milliseconds.
    pub elapsed_ms: u64,
}

/// The interception hooks. Both have no-op defaults so an implementation
/// overrides only the phases it needs.
pub trait ProviderRequestInterceptor: Send + Sync {
    /// Shape one request before dispatch.
    fn before_request(&self, _request: &ProviderRequestInfo<'_>) -> ProviderRequestDirectives {
        ProviderRequestDirectives::default()
    }

    /// Observe one settled establishment attempt.
    fn after_response(&self, _response: &ProviderResponseInfo<'_>) {}
}

/// The shared handle providers hold in their options.
pub type SharedRequestInterceptor = Arc<dyn ProviderRequestInterceptor>;

/// The failure message a vetoed request surfaces (with the reason appended).
/// Contains no transport vocabulary on purpose: the classifier must never
/// mistake a policy veto for a retryable network condition.
pub const DENIED_REQUEST_PREFIX: &str = "provider request vetoed by interceptor";

/// Render the veto reason into the error message carried by the failure.
pub(crate) fn denied_message(reason: &str) -> String {
    if reason.is_empty() {
        DENIED_REQUEST_PREFIX.to_string()
    } else {
        format!("{DENIED_REQUEST_PREFIX}: {reason}")
    }
}

/// Run the before-request phase for one attempt. `Err` carries the rendered
/// veto message; `Ok` the headers to append. Absent interceptors admit
/// everything with no headers.
pub(crate) fn admit_attempt(
    interceptor: Option<&SharedRequestInterceptor>,
    request: &ProviderRequestInfo<'_>,
) -> Result<Vec<(String, String)>, String> {
    let Some(interceptor) = interceptor else {
        return Ok(Vec::new());
    };
    let directives = interceptor.before_request(request);
    if let Some(reason) = directives.deny {
        return Err(denied_message(&reason));
    }
    Ok(directives.extra_headers)
}

/// Report one settled attempt to the interceptor, when present.
pub(crate) fn report_attempt(
    interceptor: Option<&SharedRequestInterceptor>,
    request: &ProviderRequestInfo<'_>,
    started: std::time::Instant,
    ok: bool,
    status: Option<u16>,
    error: Option<&str>,
) {
    let Some(interceptor) = interceptor else {
        return;
    };
    interceptor.after_response(&ProviderResponseInfo {
        provider_id: request.provider_id,
        model: request.model,
        url: request.url,
        attempt: request.attempt,
        ok,
        status,
        error: error.map(str::to_string),
        elapsed_ms: started.elapsed().as_millis() as u64,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Passive;
    impl ProviderRequestInterceptor for Passive {}

    #[test]
    fn defaults_are_no_ops() {
        let interceptor = Passive;
        let request = ProviderRequestInfo {
            provider_id: "anthropic",
            model: "m",
            url: "https://example.invalid/v1/messages",
            attempt: 1,
        };
        let directives = interceptor.before_request(&request);
        assert!(directives.extra_headers.is_empty());
        assert!(directives.deny.is_none());
        interceptor.after_response(&ProviderResponseInfo {
            provider_id: "anthropic",
            model: "m",
            url: "https://example.invalid/v1/messages",
            attempt: 1,
            ok: true,
            status: None,
            error: None,
            elapsed_ms: 3,
        });
    }

    #[test]
    fn denied_message_carries_the_reason() {
        assert_eq!(denied_message(""), DENIED_REQUEST_PREFIX);
        assert_eq!(
            denied_message("budget exhausted"),
            format!("{DENIED_REQUEST_PREFIX}: budget exhausted")
        );
    }
}
