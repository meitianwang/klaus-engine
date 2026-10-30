//! Fold an arbitrary provider failure onto the engine's [`ProviderError`].
//!
//! One classifier serves every provider because the signals are disjoint: an
//! HTTP `status` (present on initial-response errors), a machine-readable `code`
//! string (`rate_limit_exceeded`, `insufficient_quota`, …), an error `type`
//! (`overloaded_error`, `rate_limit_error`, …), and finally the message text.
//!
//! Errors that arrive MID-STREAM (an `error` event after the response opened)
//! carry no HTTP status — only a code/type and a message. Keying solely on
//! `status` would collapse every in-stream error to `unknown`, losing both the
//! retry decision and the server-provided backoff timing.
//!
//! Precedence is deliberately ordered so a SPECIFIC signal always beats the
//! coarse `invalid_request_error` category (which every provider stamps on most
//! 4xx — including auth failures and over-long prompts):
//!   1. token/request-size signals and 413, separating bytes from the token window
//!   2. other specific codes/types and token-context wording on 400 or SSE — an
//!      over-long prompt must outrank the coarse `invalid_request_error`
//!   3. a gateway prefill-timeout signature in the message — upstream prefill
//!      congestion stamped with a status that carries no retry semantics of its
//!      own (409 observed), so the signature must outrank the status
//!   4. the HTTP status (401/403 → auth, 408 → timeout, 429, 5xx, 400)
//!   5. the coarse `invalid_request_error` / `invalid_prompt` fallback
//!   6. the message text (the only signal left for a status-less in-stream error)
//!
//! The `retry_after_ms` it surfaces is what the retry policy / engine loop
//! prioritize over the exponential schedule.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

use crate::provider::retry::{header_get, parse_retry_after};
use crate::types::errors::{ProviderError, ProviderErrorKind};

/// Raw signals a provider extracts from a failed request — either an HTTP
/// response (a `status` plus a parsed error body) or a transport / mid-stream
/// error (status-less, carrying only a code/type and a message). Providers build
/// one of these from a response/error and hand it to [`classify_error`].
#[derive(Debug, Clone, Default)]
pub struct RawProviderError {
    /// HTTP status from an initial response; absent for transport / mid-stream
    /// errors.
    pub status: Option<u16>,
    /// Human-readable message — the classifier's last-resort signal.
    pub message: String,
    /// Machine code (`code`), e.g. `rate_limit_exceeded` / `insufficient_quota`.
    pub code: Option<String>,
    /// Top-level error discriminant (`type`), e.g. `overloaded_error`.
    pub error_type: Option<String>,
    /// Error-body discriminant one level deep (`error.type`).
    pub body_type: Option<String>,
    /// Error-body discriminant two levels deep (`error.error.type`).
    pub inner_type: Option<String>,
    /// Response headers (lowercase-keyed) — the source of server retry-timing and
    /// the request id.
    pub headers: HashMap<String, String>,
    /// Normalized request id surfaced by the transport.
    pub normalized_request_id: Option<String>,
    /// Top-level response-body `request_id`.
    pub request_id: Option<String>,
    /// Error-body `error.request_id`.
    pub error_request_id: Option<String>,
}

/// Map a provider failure onto a [`ProviderError`], resolving its `kind`,
/// server-provided `retry_after_ms`, and diagnostic `request_id`.
pub fn classify_error(err: &RawProviderError) -> ProviderError {
    let status = err.status;
    let message = err.message.clone();
    // Every place a machine-readable signal can live, across providers.
    let signals = [
        err.code.as_deref(),
        err.error_type.as_deref(),
        err.body_type.as_deref(),
        err.inner_type.as_deref(),
    ];

    let token_signal = signals.iter().any(|signal| {
        matches!(
            *signal,
            Some("context_length_exceeded" | "prompt_too_long" | "model_context_window_exceeded")
        )
    });
    let kind = if token_signal || (status == Some(413) && is_prompt_too_long(&message)) {
        Some(ProviderErrorKind::PromptTooLong)
    } else if status == Some(413)
        || signals.iter().any(|signal| {
            matches!(
                *signal,
                Some("request_too_large" | "request_too_large_error" | "payload_too_large")
            )
        })
    {
        Some(ProviderErrorKind::RequestTooLarge)
    } else {
        first_specific_kind(&signals)
    }
    .or_else(|| {
        if (status.is_none() || status == Some(400)) && is_prompt_too_long(&message) {
            Some(ProviderErrorKind::PromptTooLong)
        } else {
            None
        }
    })
    // The gateway stamps upstream congestion as HTTP 409 with the generic
    // inner-error code `model_proxy_ultra_inner_error`. The status+code pair
    // is the stable signal: it covers every message wording this family may
    // use (first pack, interval pack, etc.) without depending on a growing
    // whitelist. Keep the message regex below as a fallback for gateways that
    // emit the same congestion wording under a different code.
    .or_else(|| {
        if status == Some(409)
            && signals
                .iter()
                .any(|signal| signal == &Some("model_proxy_ultra_inner_error"))
        {
            Some(ProviderErrorKind::Overloaded)
        } else {
            None
        }
    })
    // A gateway pack-timeout verdict is upstream congestion even though
    // its status (409 observed) maps to no kind: without this step the
    // failure lands on Unknown and fails fast instead of waiting the
    // congestion out.
    .or_else(|| {
        if GATEWAY_PACK_TIMEOUT_RE.is_match(&message) {
            Some(ProviderErrorKind::Overloaded)
        } else {
            None
        }
    })
    // An explicit exhausted-allowance notice outranks the status: services stamp
    // it with 429, and letting the status win would hold the request for the whole
    // congestion window against a refusal that only clears on its reset date. A
    // message that also names a rate limit stays transient.
    .or_else(|| {
        if !RATE_LIMIT_RE.is_match(&message) && ALLOWANCE_EXHAUSTED_RE.is_match(&message) {
            Some(ProviderErrorKind::Quota)
        } else {
            None
        }
    })
    .or_else(|| kind_from_status(status))
    .or_else(|| {
        if has_invalid_request_signal(&signals) {
            Some(ProviderErrorKind::InvalidRequest)
        } else {
            None
        }
    })
    .or_else(|| kind_from_message(&message))
    .unwrap_or(ProviderErrorKind::Unknown);

    // Honor a server-provided delay: the response headers first (initial path),
    // then a "try again in N s/ms" hint embedded in a rate-limit message — the
    // only timing available mid-stream, where no headers reach us.
    let retry_after_ms = parse_retry_after(&err.headers).or_else(|| {
        if kind == ProviderErrorKind::RateLimit {
            parse_retry_after_from_message(&message)
        } else {
            None
        }
    });

    let request_id = extract_request_id(err);

    ProviderError {
        kind,
        message: message.into(),
        status,
        code: err.code.clone().map(Into::into),
        media_origins: None,
        media_repair_complete: false,
        error_type: err
            .inner_type
            .as_ref()
            .or(err.body_type.as_ref())
            .or(err.error_type.as_ref())
            .cloned()
            .map(Into::into),
        retry_after_ms,
        request_id: request_id.map(Into::into),
    }
}

/// Server request ids are short alnum/dash tokens; validate before surfacing one.
static REQUEST_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_-]{1,128}$").unwrap());

/// The over-long-prompt message wording. "context window" covers phrasings like
/// "exceeds the context window" that "context length" alone misses.
/// Bounded input-length refusals report the allowed range instead of naming
/// the context window; other parameter ranges remain ordinary request errors.
static PROMPT_TOO_LONG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)prompt is too long|maximum context|context[ -](?:length|window|limit)|too many tokens|input length and `?max_tokens`? exceed context limit|\brange of input length should be\s*\[\s*1\s*,\s*[1-9][0-9]*\s*\]")
        .unwrap()
});

static RATE_LIMIT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)rate.?limit|too many requests").unwrap());

/// A provider rejection whose payload (not the whole prompt) is too large:
/// one oversized image, too many images, or an over-page PDF. Distinct from
/// [`ProviderErrorKind::PromptTooLong`]: the history may be well under the
/// window and only the offending media blocks need shedding, so the turn
/// loop routes these through the same reactive-compaction rescue as a 413
/// (whose summary request strips images) instead of failing the turn.
///
/// The patterns mirror the wording the major providers use for media-size
/// rejections and are matched against the raw error message only; a wording
/// drift degrades gracefully (no match → the error surfaces as-is) rather
/// than mis-rescuing an unrelated 400.
static MEDIA_IMAGE_EXCEEDS_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)image exceeds").unwrap());
static MEDIA_IMAGE_DIMENSIONS_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)image dimensions exceed").unwrap());
static MEDIA_MANY_IMAGE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)many-image").unwrap());
static MEDIA_PDF_PAGES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)maximum of \d+ PDF pages").unwrap());

/// Is this raw provider error message a media-size rejection (oversized
/// image / too many images / over-page PDF)? See the `MEDIA_*_RE` statics
/// for the wording and the degradation policy.
pub fn is_media_size_error(message: &str) -> bool {
    (MEDIA_IMAGE_EXCEEDS_RE.is_match(message) && message.to_lowercase().contains("maximum"))
        || (MEDIA_IMAGE_DIMENSIONS_RE.is_match(message) && MEDIA_MANY_IMAGE_RE.is_match(message))
        || MEDIA_PDF_PAGES_RE.is_match(message)
}

pub(crate) const IMAGE_REJECTIONS: &[&str] = &[
    "could not process image",
    "image exceeds",
    "image dimensions exceed",
    "image does not match the provided media type",
    "image cannot be empty",
    "exceeds api limit",
    "images exceed the api limit",
    "unable to resize image",
    "unable to compress image",
    "image file is empty",
];
pub(crate) const DOCUMENT_REJECTIONS: &[&str] = &[
    "could not process pdf",
    "could not process document",
    "pdf pages",
    "the pdf specified was not valid",
    "the pdf specified is password protected",
    "pdf cannot be empty",
    "too much media",
];
static QUOTA_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)insufficient_quota|exceeded your current quota|quota exceeded|credit balance")
        .unwrap()
});

/// An explicit statement that the allowance is spent, usually with a reset date.
/// It is terminal regardless of the status the service stamps on it, because the
/// allowance cannot clear by waiting.
static ALLOWANCE_EXHAUSTED_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:quota|allowance) (?:has been|is|was) exhausted").unwrap());

static OVERLOADED_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)overloaded|server is overloaded|slow down").unwrap());

/// A gateway verdict that the upstream model stopped producing response
/// packets in time: congestion or a stalled generation on the service side,
/// not a defect in the request. Gateways stamp it with statuses that map to no
/// kind of their own (409 observed), which would leave the failure unknown and
/// fail-fast, so the signature — not the status — is what keeps the congestion
/// retryable. Three wordings are observed: "first pack timeout" (the first
/// packet never arrived), "interval pack timeout" (the gap between two packets
/// exceeded the gateway's limit, also the wording stamped when the upstream
/// stalls before any packet reaches the client), and a bare "request timeout"
/// body. The last is anchored to the whole message so longer texts that merely
/// mention a request timeout (an RPC deadline, a tool timeout) keep their own
/// classification.
static GATEWAY_PACK_TIMEOUT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:first|interval) pack timeout|^request timeout[.!]?$").unwrap()
});

static NETWORK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)network|fetch failed|ECONNRESET|ETIMEDOUT|socket hang up|error sending request|error decoding response body|connection reset|connection timeout|connect timeout|dns|tls handshake").unwrap()
});

/// A rate-limit delay hint like "Please try again in 11.054s" / "try again in
/// 28ms" — the form used on streamed 429s, where no `Retry-After` header exists.
static TRY_AGAIN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)try again in\s*(\d+(?:\.\d+)?)\s*(ms|s|seconds?)").unwrap());

/// The server-assigned request id of the failed call, from wherever the
/// transport exposes it: the normalized id, the response body's `request_id`, or
/// the `request-id` / `x-request-id` response header. Validated so a malformed
/// value is never stamped into a log. Diagnostic only — it does not affect retry.
fn extract_request_id(err: &RawProviderError) -> Option<String> {
    let candidates = [
        err.normalized_request_id.as_deref(),
        err.request_id.as_deref(),
        err.error_request_id.as_deref(),
        header_get(&err.headers, "request-id"),
        header_get(&err.headers, "x-request-id"),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|c| REQUEST_ID_RE.is_match(c))
        .map(str::to_string)
}

/// First signal that maps to a SPECIFIC kind (the coarse invalid_request_* are
/// excluded).
fn first_specific_kind(signals: &[Option<&str>]) -> Option<ProviderErrorKind> {
    signals.iter().find_map(|s| specific_kind(*s))
}

/// Map a machine `code` or error `type` to a SPECIFIC kind. The vocabularies are
/// disjoint, so one match covers every provider. The coarse
/// `invalid_request_error` / `invalid_prompt` are deliberately NOT here — they
/// are a 4xx catch-all that must not mask a more specific status/message
/// (handled by [`has_invalid_request_signal`] as a low-priority fallback).
/// Returns `None` for an unrecognized signal (including the `error` envelope
/// discriminant).
fn specific_kind(signal: Option<&str>) -> Option<ProviderErrorKind> {
    match signal {
        Some(
            "antigravity_model_access_denied"
            | "antigravity_request_rejected"
            | "image_preprocessing_failed",
        ) => Some(ProviderErrorKind::InvalidRequest),
        Some("antigravity_auth_required") => Some(ProviderErrorKind::Auth),
        Some("antigravity_quota_exhausted") => Some(ProviderErrorKind::Quota),
        Some("antigravity_rate_limited") => Some(ProviderErrorKind::RateLimit),
        Some("antigravity_service_unavailable") => Some(ProviderErrorKind::Overloaded),
        Some("rate_limit_exceeded") | Some("rate_limit_error") => {
            Some(ProviderErrorKind::RateLimit)
        }
        Some("context_length_exceeded" | "prompt_too_long" | "model_context_window_exceeded") => {
            Some(ProviderErrorKind::PromptTooLong)
        }
        Some("request_too_large" | "request_too_large_error" | "payload_too_large") => {
            Some(ProviderErrorKind::RequestTooLarge)
        }
        Some("insufficient_quota")
        | Some("pre_consume_token_quota_failed")
        | Some("usage_limit_reached")
        | Some("usage_not_included")
        | Some("billing_error") => Some(ProviderErrorKind::Quota),
        // Token Plan stamps an exhausted monthly allowance on HTTP 429 with this
        // composite code. The allowance only resets on its published date, so the
        // refusal cannot clear by waiting; retrying it burns the congestion hold
        // and the caller sees a request that never returns.
        Some("Throttling.AllocationQuota") => Some(ProviderErrorKind::Quota),
        // A plan gateway that stamps EVERY refusal with HTTP 429 and separates them
        // only by a numeric code. Zhipu's Coding Plan does exactly that, and the
        // codes split into refusals that clear on their own and refusals that never
        // will:
        //
        //   1302 rate limit reached, 1305 model traffic too high, 1313 fair-use
        //        throttling — transient, so they are deliberately NOT here and fall
        //        through to the coarse `api_error` overload below.
        //   1308 allowance exhausted (carries `next_flush_time`), 1309 plan expired,
        //   1310 weekly/monthly allowance exhausted, 1311 the plan has not opened
        //        this model — terminal. Retrying these burns the congestion hold
        //        against a refusal that cannot clear, which the user experiences as
        //        a conversation that hangs for minutes instead of an error.
        //
        // These have to be matched here rather than after the status, because the
        // envelope's `type` is the generic `api_error` and the code is the ONLY
        // signal that distinguishes the two groups. `code` is the first signal read,
        // so a match here outranks it.
        //
        // 1311 is grouped with the allowance codes rather than treated as a request
        // error: its fix is a change to the subscription, not to the request, and
        // the retry policy treats every non-quota kind on a 429 as retryable.
        Some("1308") | Some("1309") | Some("1310") | Some("1311") => Some(ProviderErrorKind::Quota),
        // Volcengine Ark spells the same two refusals as named codes rather than
        // numbers, and its published error table puts both on a billing status:
        // `QuotaExceeded` (429) is an exhausted allowance whose fix is a console
        // action, and `AccountOverdueError` (403) is an overdue account balance.
        // Retrying either burns the congestion hold against a refusal that cannot
        // clear. Ark's rate-limit codes (`RateLimitExceeded.*`,
        // `ModelAccount*RateLimitExceeded`) are deliberately NOT here: those are
        // transient and keep backing off.
        Some("QuotaExceeded") | Some("AccountOverdueError") => Some(ProviderErrorKind::Quota),
        // Server overload / transient signals across providers are treated as a
        // retryable `overloaded`, matching the resilient 529 handling.
        Some("server_is_overloaded")
        | Some("slow_down")
        | Some("server_error")
        | Some("overloaded_error")
        | Some("api_error") => Some(ProviderErrorKind::Overloaded),
        Some("authentication_error") | Some("permission_error") | Some("invalid_api_key") => {
            Some(ProviderErrorKind::Auth)
        }
        _ => None,
    }
}

/// Whether any signal is the coarse 4xx category (used only as a low-priority
/// fallback).
fn has_invalid_request_signal(signals: &[Option<&str>]) -> bool {
    signals
        .iter()
        .any(|s| matches!(*s, Some("invalid_request_error") | Some("invalid_prompt")))
}

/// Map an HTTP status to a kind (the initial-response path). 413 is handled by
/// the caller.
fn kind_from_status(status: Option<u16>) -> Option<ProviderErrorKind> {
    let status = status?;
    if status == 401 || status == 403 {
        return Some(ProviderErrorKind::Auth);
    }
    // 402 is a billing verdict, not a transient fault: retrying it burns the
    // congestion hold against a gateway that will keep refusing.
    if status == 402 {
        return Some(ProviderErrorKind::Quota);
    }
    // A gateway can report a connection timeout in either its initial response
    // or a streamed error envelope. Keep it bounded by the timeout retry policy.
    if status == 408 {
        return Some(ProviderErrorKind::Timeout);
    }
    if status == 429 {
        return Some(ProviderErrorKind::RateLimit);
    }
    if status >= 500 {
        return Some(ProviderErrorKind::Overloaded);
    }
    if status == 400 {
        return Some(ProviderErrorKind::InvalidRequest);
    }
    None
}

/// Whether the message reads as an over-long prompt.
fn is_prompt_too_long(message: &str) -> bool {
    PROMPT_TOO_LONG_RE.is_match(message)
}

/// Last-resort classification from the message text, for status-less in-stream
/// errors that carry no recognized code/type. Rate limit is checked BEFORE
/// quota: a "rate limit reached" notice is transient even when it appends a
/// billing / upgrade URL — only a message with no rate-limit wording and a
/// specific quota phrase is a terminal billing exhaustion. The quota phrases are
/// kept narrow (no bare "billing") so that URL doesn't flip a rate limit to
/// terminal.
fn kind_from_message(message: &str) -> Option<ProviderErrorKind> {
    if RATE_LIMIT_RE.is_match(message) {
        return Some(ProviderErrorKind::RateLimit);
    }
    if QUOTA_RE.is_match(message) {
        return Some(ProviderErrorKind::Quota);
    }
    if OVERLOADED_RE.is_match(message) {
        return Some(ProviderErrorKind::Overloaded);
    }
    if NETWORK_RE.is_match(message) {
        return Some(ProviderErrorKind::Network);
    }
    None
}

/// Extract a retry delay from a rate-limit message like "Please try again in
/// 11.054s" or "try again in 28ms". Returns milliseconds, or `None`.
fn parse_retry_after_from_message(message: &str) -> Option<u64> {
    let caps = TRY_AGAIN_RE.captures(message)?;
    let n: f64 = caps.get(1)?.as_str().parse().ok()?;
    if !n.is_finite() {
        return None;
    }
    let unit = caps.get(2)?.as_str().to_ascii_lowercase();
    let ms = if unit == "ms" {
        n.round()
    } else {
        (n * 1000.0).round()
    };
    Some(ms.max(0.0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> RawProviderError {
        RawProviderError::default()
    }

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn specific_code_beats_status() {
        // A specific `code` outranks the coarse status classification.
        let e = classify_error(&RawProviderError {
            status: Some(400),
            code: Some("rate_limit_exceeded".into()),
            message: "slow down".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RateLimit);
        assert_eq!(e.status, Some(400));
    }

    #[test]
    fn anthropic_overloaded_type() {
        let e = classify_error(&RawProviderError {
            status: Some(529),
            body_type: Some("overloaded_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn bare_connection_timeout_is_a_network_error_not_unknown() {
        // A transport-level connect timeout used to surface as kind=unknown,
        // which told the user nothing about the failure being their network.
        for message in ["connection timeout", "Connect timeout after 30s"] {
            let e = classify_error(&RawProviderError {
                message: message.into(),
                ..raw()
            });
            assert_eq!(e.kind, ProviderErrorKind::Network, "{message}");
        }
    }

    #[test]
    fn nested_error_error_type_is_a_signal() {
        // The real discriminant can sit two levels deep.
        let e = classify_error(&RawProviderError {
            inner_type: Some("api_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn context_length_exceeded_is_prompt_too_long() {
        let e = classify_error(&RawProviderError {
            status: Some(400),
            code: Some("context_length_exceeded".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::PromptTooLong);
    }

    #[test]
    fn quota_codes() {
        for code in [
            "insufficient_quota",
            "pre_consume_token_quota_failed",
            "usage_limit_reached",
            "usage_not_included",
            "billing_error",
        ] {
            let e = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                ..raw()
            });
            assert_eq!(e.kind, ProviderErrorKind::Quota, "code={code}");
        }
    }

    #[test]
    fn auth_codes() {
        for code in [
            "authentication_error",
            "permission_error",
            "invalid_api_key",
        ] {
            for status in [None, Some(408)] {
                let e = classify_error(&RawProviderError {
                    status,
                    error_type: Some(code.into()),
                    ..raw()
                });
                assert_eq!(
                    e.kind,
                    ProviderErrorKind::Auth,
                    "code={code}; status={status:?}"
                );
            }
        }
    }

    #[test]
    fn status_413_without_token_evidence_is_request_too_large() {
        let e = classify_error(&RawProviderError {
            status: Some(413),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RequestTooLarge);
    }

    #[test]
    fn prompt_message_overrides_400_but_not_429() {
        // A 400 with an over-long-prompt message → prompt_too_long.
        let e = classify_error(&RawProviderError {
            status: Some(400),
            message: "prompt is too long: 210000 tokens > 200000".into(),
            error_type: Some("invalid_request_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::PromptTooLong);

        // The same message under a 429 stays a rate limit — the status wins.
        let e = classify_error(&RawProviderError {
            status: Some(429),
            message: "prompt is too long".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RateLimit);
    }

    #[test]
    fn status_mapping() {
        for (status, kind) in [
            (401, ProviderErrorKind::Auth),
            (402, ProviderErrorKind::Quota),
            (403, ProviderErrorKind::Auth),
            (429, ProviderErrorKind::RateLimit),
            (500, ProviderErrorKind::Overloaded),
            (503, ProviderErrorKind::Overloaded),
            (400, ProviderErrorKind::InvalidRequest),
        ] {
            let e = classify_error(&RawProviderError {
                status: Some(status),
                ..raw()
            });
            assert_eq!(e.kind, kind, "status={status}");
        }
    }

    #[test]
    fn coarse_invalid_request_is_a_low_priority_fallback() {
        // Status-less, only the coarse category present → invalid_request.
        let e = classify_error(&RawProviderError {
            error_type: Some("invalid_request_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::InvalidRequest);
    }

    #[test]
    fn auth_status_beats_coarse_invalid_request() {
        // Auth failures arrive as a 401 with the coarse `invalid_request_error`.
        let e = classify_error(&RawProviderError {
            status: Some(401),
            error_type: Some("invalid_request_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Auth);
    }

    #[test]
    fn message_only_classification() {
        let cases = [
            ("rate limit reached for gpt-4", ProviderErrorKind::RateLimit),
            ("too many requests", ProviderErrorKind::RateLimit),
            (
                "You exceeded your current quota, please check your plan",
                ProviderErrorKind::Quota,
            ),
            ("your credit balance is too low", ProviderErrorKind::Quota),
            ("the server is overloaded", ProviderErrorKind::Overloaded),
            ("fetch failed: ECONNRESET", ProviderErrorKind::Network),
            ("socket hang up", ProviderErrorKind::Network),
            // reqwest's opaque transport wording must classify as Network so a
            // dead connection retries instead of failing the turn outright.
            (
                "error sending request for url (http://gw/compatible-mode/v1/chat/completions)",
                ProviderErrorKind::Network,
            ),
            ("error decoding response body", ProviderErrorKind::Network),
            ("something entirely unexpected", ProviderErrorKind::Unknown),
        ];
        for (msg, kind) in cases {
            let e = classify_error(&RawProviderError {
                message: msg.into(),
                ..raw()
            });
            assert_eq!(e.kind, kind, "msg={msg}");
        }
    }

    #[test]
    fn rate_limit_message_beats_quota_url() {
        // A rate-limit notice that appends a billing URL is still transient.
        let e = classify_error(&RawProviderError {
            message: "Rate limit reached. Please add credits to your billing account.".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RateLimit);
    }

    #[test]
    fn quota_code_beats_429_status() {
        // Billing exhaustion arrives as a 429 but must stay terminal.
        let e = classify_error(&RawProviderError {
            status: Some(429),
            code: Some("insufficient_quota".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Quota);
    }

    #[test]
    fn a_plan_gateway_numeric_allowance_code_is_terminal_despite_429() {
        // Measured against Zhipu's Coding Plan: every refusal it makes arrives as
        // HTTP 429 with the generic `api_error` type, and only the numeric code
        // says whether it will clear. Left to the status and the coarse type, an
        // exhausted allowance classified as Overloaded and was retried for the
        // whole congestion hold — minutes of a hung conversation against a refusal
        // that cannot change. (That it is not RETRIED is asserted beside
        // `is_retryable` in retry.rs.)
        for code in ["1308", "1309", "1310", "1311"] {
            let e = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                body_type: Some("api_error".into()),
                message: "[1311][当前订阅套餐暂未开放GLM-5.3-FlashX权限]".into(),
                ..raw()
            });
            assert_eq!(e.kind, ProviderErrorKind::Quota, "code {code}");
        }
    }

    #[test]
    fn the_same_gateway_transient_codes_are_not_classified_as_allowance() {
        // The other half of the same service: rate limiting, model congestion and
        // fair-use throttling are transient. They must not land on Quota, which is
        // the one kind the retry policy treats as terminal on a 429.
        for code in ["1302", "1305", "1313"] {
            let e = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                body_type: Some("api_error".into()),
                ..raw()
            });
            assert_ne!(e.kind, ProviderErrorKind::Quota, "code {code}");
        }
    }

    #[test]
    fn a_named_allowance_code_from_a_plan_gateway_is_terminal() {
        // Volcengine Ark spells its billing refusals as PascalCase named codes in
        // the same `error.code` field, and its published table puts both on a
        // billing status: an exhausted allowance (429) and an overdue account
        // balance (403). Neither clears by waiting, so neither may be retried.
        for (code, status) in [("QuotaExceeded", 429u16), ("AccountOverdueError", 403)] {
            let e = classify_error(&RawProviderError {
                status: Some(status),
                code: Some(code.into()),
                body_type: Some("TooManyRequests".into()),
                message: format!("Your account has exhausted its quota. {code}"),
                ..raw()
            });
            assert_eq!(e.kind, ProviderErrorKind::Quota, "code {code}");
        }
    }

    #[test]
    fn an_exhausted_monthly_allowance_is_terminal_despite_429() {
        // Measured against Token Plan: an exhausted monthly allowance arrives as
        // HTTP 429 with `Throttling.AllocationQuota` and a reset date. Waiting
        // cannot clear it, so the retry policy must not hold the request for the
        // whole congestion window. The code alone is enough; the message alone is
        // covered by the quota phrase below.
        let by_code = classify_error(&RawProviderError {
            status: Some(429),
            code: Some("Throttling.AllocationQuota".into()),
            message: "Your token-plan 1-month quota has been exhausted. The quota will reset at 10-29 16:00:00 UTC.".into(),
            ..raw()
        });
        assert_eq!(by_code.kind, ProviderErrorKind::Quota);
        let by_message = classify_error(&RawProviderError {
            status: Some(429),
            message: "Your token-plan 1-month quota has been exhausted. The quota will reset at 10-29 16:00:00 UTC.".into(),
            ..raw()
        });
        assert_eq!(by_message.kind, ProviderErrorKind::Quota);
        assert!(!crate::provider::retry::is_retryable(&by_code));
        assert!(!crate::provider::retry::is_retryable(&by_message));
    }

    #[test]
    fn a_transient_throttling_message_stays_retryable() {
        // The quota phrase must not swallow the ordinary throttling verdict that
        // carries no exhaustion wording.
        let e = classify_error(&RawProviderError {
            status: Some(429),
            code: Some("Throttling.RateQuota".into()),
            message: "Requests rate limit exceeded, please try again later.".into(),
            ..raw()
        });
        assert_ne!(e.kind, ProviderErrorKind::Quota);
    }

    #[test]
    fn a_named_rate_limit_code_from_the_same_gateway_still_backs_off() {
        // Ark's throttling codes share the 429 with its quota code but DO clear by
        // waiting. Classifying them as terminal would fail a conversation that a
        // short backoff would have carried through.
        for code in [
            "RateLimitExceeded.EndpointRPMExceeded",
            "RateLimitExceeded.EndpointTPMExceeded",
            "ModelAccountRpmRateLimitExceeded",
            "ModelAccountTpmRateLimitExceeded",
            "ServerOverloaded",
        ] {
            let e = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                body_type: Some("TooManyRequests".into()),
                ..raw()
            });
            assert_ne!(e.kind, ProviderErrorKind::Quota, "code {code}");
        }
    }

    #[test]
    fn retry_after_header_seconds() {
        let e = classify_error(&RawProviderError {
            status: Some(429),
            headers: headers(&[("retry-after", "5")]),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(5000));
    }

    #[test]
    fn reset_header_bare_seconds_and_durations() {
        let e = classify_error(&RawProviderError {
            status: Some(429),
            headers: headers(&[("x-ratelimit-reset", "5")]),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(5000));

        let e = classify_error(&RawProviderError {
            status: Some(429),
            headers: headers(&[("anthropic-ratelimit-tokens-reset", "250ms")]),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(250));

        let e = classify_error(&RawProviderError {
            status: Some(429),
            headers: headers(&[("x-ratelimit-reset-tokens", "1.5s")]),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(1500));
    }

    #[test]
    fn retry_after_header_takes_precedence_over_reset() {
        let e = classify_error(&RawProviderError {
            status: Some(429),
            headers: headers(&[("retry-after", "2"), ("x-ratelimit-reset", "9")]),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(2000));
    }

    #[test]
    fn retry_after_from_message_only_when_rate_limit() {
        // Rate limit + message hint, no headers → parse the hint.
        let e = classify_error(&RawProviderError {
            message: "Rate limit reached. Please try again in 11.054s.".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RateLimit);
        assert_eq!(e.retry_after_ms, Some(11054));

        // The same hint under a non-rate-limit kind is ignored.
        let e = classify_error(&RawProviderError {
            message: "the server is overloaded, try again in 5s".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
        assert_eq!(e.retry_after_ms, None);
    }

    #[test]
    fn retry_after_message_ms_unit() {
        let e = classify_error(&RawProviderError {
            message: "rate limit exceeded, try again in 28ms".into(),
            ..raw()
        });
        assert_eq!(e.retry_after_ms, Some(28));
    }

    #[test]
    fn request_id_from_fields_and_headers() {
        let e = classify_error(&RawProviderError {
            normalized_request_id: Some("req_abc-123".into()),
            ..raw()
        });
        assert!(e.request_id.as_ref().is_some_and(|id| id == "req_abc-123"));

        let e = classify_error(&RawProviderError {
            headers: headers(&[("x-request-id", "req_from_header")]),
            ..raw()
        });
        assert!(e
            .request_id
            .as_ref()
            .is_some_and(|id| id == "req_from_header"));
    }

    #[test]
    fn request_id_validation_rejects_malformed() {
        let e = classify_error(&RawProviderError {
            normalized_request_id: Some("not a valid id!".into()),
            ..raw()
        });
        assert_eq!(e.request_id, None);
    }

    #[test]
    fn request_id_precedence_prefers_normalized() {
        let e = classify_error(&RawProviderError {
            normalized_request_id: Some("normalized".into()),
            request_id: Some("body".into()),
            error_request_id: Some("errbody".into()),
            headers: headers(&[("request-id", "hdr")]),
            ..raw()
        });
        assert!(e.request_id.as_ref().is_some_and(|id| id == "normalized"));
    }

    #[test]
    fn message_is_carried_through() {
        let e = classify_error(&RawProviderError {
            message: "boom".into(),
            ..raw()
        });
        assert_eq!(e.message, "boom");
    }

    #[test]
    fn gateway_first_pack_timeout_on_409_is_overloaded() {
        // The observed gateway verdict: HTTP 409 with "first pack timeout" as
        // the whole body. 409 maps to no kind of its own, so without the
        // signature step this lands on Unknown and fails fast.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "first pack timeout".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
        assert_eq!(e.status, Some(409));
    }

    #[test]
    fn first_pack_timeout_without_status_is_overloaded() {
        let e = classify_error(&RawProviderError {
            message: "first pack timeout".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn gateway_interval_pack_timeout_on_409_is_overloaded() {
        // The same gateway family stamps an upstream stall between response
        // packets as HTTP 409 with "interval pack timeout" and an inner-error
        // code; like the first-pack wording it is congestion, not a conflict.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "interval pack timeout".into(),
            code: Some("model_proxy_ultra_inner_error".into()),
            error_type: Some("model_proxy_ultra_inner_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
        assert_eq!(e.status, Some(409));
    }

    #[test]
    fn gateway_409_inner_error_is_overloaded_regardless_of_message_wording() {
        // The 409 + inner-error code pair is the stable congestion verdict;
        // message wording must not be needed, so a wording the regex has never
        // seen still lands on Overloaded instead of Unknown.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "upstream packet spacing exceeded".into(),
            code: Some("model_proxy_ultra_inner_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn gateway_409_inner_error_type_without_code_is_overloaded() {
        // The same verdict can arrive with the code in `type` only.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "unexpected gateway verdict".into(),
            error_type: Some("model_proxy_ultra_inner_error".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn gateway_409_with_a_different_code_keeps_failing_fast() {
        // Only the inner-error code marks congestion. Another 409 with a
        // different code stays a semantic conflict.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "unexpected gateway verdict".into(),
            code: Some("model_proxy_access_control".into()),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Unknown);
    }

    #[test]
    fn bare_request_timeout_verdict_is_overloaded() {
        // A second observed gateway wording for the same prefill congestion:
        // the whole body is "request timeout". Without the signature it lands
        // on Unknown and fails fast instead of waiting the congestion out.
        let e = classify_error(&RawProviderError {
            message: "request timeout".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
    }

    #[test]
    fn request_timeout_mentioned_in_a_longer_message_keeps_its_own_kind() {
        // The bare-verdict arm is anchored to the whole message: a longer text
        // that merely mentions a request timeout (an RPC deadline, a tool
        // timeout) is not a gateway prefill verdict and must not be retried as
        // congestion.
        let e = classify_error(&RawProviderError {
            message: "app-server request timed out: session/resolveDynamicToolCall".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Unknown);
    }

    #[test]
    fn plain_409_without_the_signature_stays_unknown() {
        // A 409 whose message carries no congestion signature is a semantic
        // conflict and must keep failing fast: retrying it would burn the
        // congestion hold against a gateway that will keep refusing.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "conflict: resource state changed".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::Unknown);
    }

    #[test]
    fn specific_code_outranks_the_first_pack_signature() {
        let e = classify_error(&RawProviderError {
            status: Some(409),
            code: Some("rate_limit_exceeded".into()),
            message: "first pack timeout".into(),
            ..raw()
        });
        assert_eq!(e.kind, ProviderErrorKind::RateLimit);
    }
    #[test]
    fn antigravity_source_codes_override_only_the_declared_refusal() {
        for (status, code, kind) in [
            (
                403,
                "antigravity_model_access_denied",
                ProviderErrorKind::InvalidRequest,
            ),
            (429, "antigravity_quota_exhausted", ProviderErrorKind::Quota),
            (401, "antigravity_auth_required", ProviderErrorKind::Auth),
            (
                400,
                "antigravity_request_rejected",
                ProviderErrorKind::InvalidRequest,
            ),
            (
                429,
                "antigravity_rate_limited",
                ProviderErrorKind::RateLimit,
            ),
            (
                503,
                "antigravity_service_unavailable",
                ProviderErrorKind::Overloaded,
            ),
        ] {
            let e = classify_error(&RawProviderError {
                status: Some(status),
                code: Some(code.into()),
                ..raw()
            });
            assert_eq!(e.kind, kind);
            assert_eq!(e.status, Some(status));
        }
        assert_eq!(
            classify_error(&RawProviderError {
                status: Some(403),
                ..raw()
            })
            .kind,
            ProviderErrorKind::Auth
        );
        assert_eq!(
            classify_error(&RawProviderError {
                status: Some(429),
                ..raw()
            })
            .kind,
            ProviderErrorKind::RateLimit
        );
    }
}
