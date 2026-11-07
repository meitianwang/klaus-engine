//! Retry policy for request ESTABLISHMENT. Wraps only the part of a provider
//! call that happens *before any token has streamed* — opening the stream /
//! awaiting the first chunk. Once tokens flow, retrying is the engine loop's job
//! (it tombstones partial output and re-samples); a mid-stream break still
//! surfaces as an error event, never a silent retry here.
//!
//! The matrix:
//!   - 429 / 500 / 502 / 503 / 529 / overloaded → retry with exponential
//!     backoff, PRIORITIZING server-provided timing headers (`retry-after`,
//!     `anthropic-ratelimit-*`, `x-ratelimit-reset-*`).
//!   - timeout / 408 → bounded retry with the same backoff and attempt cap.
//!   - 400 / 401 / 403 (and any other non-retryable kind) → fail fast.
//!   - attempts are capped (default 3 total tries).

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::Stream;
use regex::Regex;

use crate::provider::classify::{classify_error, RawProviderError};
use crate::types::abort::AbortSignal;
use crate::types::errors::{ProviderError, ProviderErrorKind};
use crate::types::provider::ModelStreamEvent;

/// Total attempts including the first.
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Base backoff in ms for the exponential schedule.
const DEFAULT_BASE_DELAY_MS: u64 = 500;
/// Ceiling for any single backoff in ms.
const DEFAULT_MAX_DELAY_MS: u64 = 30_000;
/// Full-jitter fraction applied on top of the computed delay.
const DEFAULT_JITTER: f64 = 0.25;
/// How long a congested (rate-limited / overloaded) establishment is held and
/// re-tried past the attempt cap. The upstream queue drains on its own schedule,
/// so capping attempts would surface a transient "busy" as a terminal failure.
const DEFAULT_CONGESTION_HOLD_MS: u64 = 300_000;
/// First backoff of the unbounded network-wait schedule, in ms. A machine that
/// lost its network recovers on its own clock, so connection failures wait it
/// out instead of burning the attempt cap.
const DEFAULT_NETWORK_WAIT_BASE_DELAY_MS: u64 = 5_000;
/// Ceiling for a single network-wait backoff, in ms.
const DEFAULT_NETWORK_WAIT_MAX_DELAY_MS: u64 = 60_000;

/// HTTP statuses whose request establishment is retryable regardless of the
/// classified kind.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 529)
}

/// Error kinds whose request establishment is safe to retry.
fn is_retryable_kind(kind: ProviderErrorKind) -> bool {
    matches!(
        kind,
        ProviderErrorKind::RateLimit
            | ProviderErrorKind::Overloaded
            | ProviderErrorKind::Network
            | ProviderErrorKind::Timeout
    )
}

/// Marks a backoff that was cut short by cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Aborted;

/// A pending backoff. Resolves `Ok(())` once the delay elapses, or
/// `Err(Aborted)` if the signal is cancelled first.
pub type SleepFuture = Pin<Box<dyn Future<Output = Result<(), Aborted>> + Send>>;

/// Injectable backoff clock (tests pass a no-op / fake clock).
pub type SleepFn = Arc<dyn Fn(u64, AbortSignal) -> SleepFuture + Send + Sync>;

/// Injectable jitter source in `[0, 1)` (tests pass `|| 0.0` for determinism).
pub type RandomFn = Arc<dyn Fn() -> f64 + Send + Sync>;

/// Observer invoked on each retry decision (tests / telemetry).
pub type OnRetryFn = Arc<dyn Fn(RetryInfo) + Send + Sync>;

/// A single retry decision, handed to [`RetryOptions::on_retry`].
#[derive(Debug, Clone)]
pub struct RetryInfo {
    /// 1-based index of the attempt that just failed.
    pub attempt: u32,
    /// Total attempts the policy allows, including the first.
    pub max_attempts: u32,
    /// The backoff about to be slept, in milliseconds.
    pub delay_ms: u64,
    /// The classified error that triggered the retry.
    pub error: ProviderError,
    /// True when this decision is an unbounded network wait past the attempt
    /// cap, so observers can say "waiting for network" instead of a numbered
    /// retry.
    pub network_wait: bool,
}

/// Wraps `opts` so every retry decision is ALSO forwarded through an unbounded
/// channel, preserving any observer already installed. Establishment code that
/// runs inside a stream generator selects on the receiver while awaiting the
/// establishment future, surfacing each scheduled retry in real time (during
/// the backoff, not after it).
pub fn observe_retries(
    mut opts: RetryOptions,
) -> (
    RetryOptions,
    tokio::sync::mpsc::UnboundedReceiver<RetryInfo>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let prior = opts.on_retry.take();
    opts.on_retry = Some(Arc::new(move |info: RetryInfo| {
        if let Some(prior) = &prior {
            prior(info.clone());
        }
        // A dropped receiver only means nobody is listening anymore.
        let _ = tx.send(info);
    }));
    (opts, rx)
}

/// One step of an establishment that surfaces its retries as stream events.
pub(crate) enum EstablishmentStep<T> {
    /// A retry decision, already shaped as the stream event to yield.
    Retry(ModelStreamEvent),
    /// The establishment future settled; always the final step.
    Done(T),
}

/// Drives the establishment future while draining the paired
/// [`observe_retries`] receiver: every retry decision surfaces as a
/// [`EstablishmentStep::Retry`] WHILE the backoff sleeps, and the settled
/// result terminates the stream as [`EstablishmentStep::Done`]. Any decisions
/// still queued when the future settles are flushed before `Done` so the
/// retry trail is complete even for immediate completions.
pub(crate) fn establishment_steps<T>(
    fut: impl Future<Output = T>,
    mut retries: tokio::sync::mpsc::UnboundedReceiver<RetryInfo>,
) -> impl Stream<Item = EstablishmentStep<T>> {
    fn retry_event(info: RetryInfo) -> ModelStreamEvent {
        ModelStreamEvent::RetryScheduled {
            attempt: info.attempt,
            max_attempts: info.max_attempts,
            delay_ms: info.delay_ms,
            error: info.error,
            network_wait: info.network_wait,
        }
    }
    async_stream::stream! {
        tokio::pin!(fut);
        loop {
            tokio::select! {
                biased;
                Some(info) = retries.recv() => {
                    yield EstablishmentStep::Retry(retry_event(info));
                }
                result = &mut fut => {
                    while let Ok(info) = retries.try_recv() {
                        yield EstablishmentStep::Retry(retry_event(info));
                    }
                    yield EstablishmentStep::Done(result);
                    return;
                }
            }
        }
    }
}

/// Establishment classes for a bounded internal request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClassPolicy {
    ApprovalReview,
}

/// Tuning knobs for [`with_retry`]. The abort signal is threaded as a separate
/// [`with_retry`] parameter (it cancels both the in-flight attempt and any
/// pending backoff).
#[derive(Clone, Default)]
pub struct RetryOptions {
    /// Total attempts including the first. Default 3.
    pub max_attempts: Option<u32>,
    /// An internal request may restrict retry classes without changing normal calls.
    pub retry_class_policy: Option<RetryClassPolicy>,
    /// Base backoff in ms for the exponential schedule. Default 500.
    pub base_delay_ms: Option<u64>,
    /// Ceiling for any single backoff in ms. Default 30_000.
    pub max_delay_ms: Option<u64>,
    /// Full-jitter fraction applied on top of the computed delay (0..1). Default 0.25.
    pub jitter: Option<f64>,
    /// Injectable sleep (tests pass a no-op / fake clock).
    pub sleep: Option<SleepFn>,
    /// Injectable RNG for jitter (tests pass `|| 0.0` for determinism).
    pub random: Option<RandomFn>,
    /// Observe each retry decision (tests / telemetry).
    pub on_retry: Option<OnRetryFn>,
    /// Return allowance refusals to the credential owner before same-account backoff.
    pub yield_to_credential_rotation: bool,
    /// How long (ms) a congested (RateLimit/Overloaded) establishment keeps
    /// retrying past the attempt cap. Default 300_000; `Some(0)` disables the
    /// hold and restores the pure attempt bound (tests).
    pub congestion_hold_ms: Option<u64>,
    /// Keep retrying Network-kind establishment failures past the attempt cap,
    /// on a 5s→60s capped backoff, until the network comes back or the signal
    /// aborts. Default true; `Some(false)` restores the pure attempt bound.
    /// RateLimit/Overloaded stay governed by `congestion_hold_ms` instead.
    pub unbounded_network_retries: Option<bool>,
}

impl RetryOptions {
    pub(crate) fn for_approval_review(mut self, total_attempts: u32) -> Self {
        self.max_attempts = Some(total_attempts.max(1));
        self.retry_class_policy = Some(RetryClassPolicy::ApprovalReview);
        self.base_delay_ms = Some(200);
        self.congestion_hold_ms = Some(0);
        self.unbounded_network_retries = Some(false);
        self
    }
}

impl fmt::Debug for RetryOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryOptions")
            .field("max_attempts", &self.max_attempts)
            .field("retry_class_policy", &self.retry_class_policy)
            .field("base_delay_ms", &self.base_delay_ms)
            .field("max_delay_ms", &self.max_delay_ms)
            .field("jitter", &self.jitter)
            .field("sleep", &self.sleep.as_ref().map(|_| "<fn>"))
            .field("random", &self.random.as_ref().map(|_| "<fn>"))
            .field("on_retry", &self.on_retry.as_ref().map(|_| "<fn>"))
            .field(
                "yield_to_credential_rotation",
                &self.yield_to_credential_rotation,
            )
            .field("congestion_hold_ms", &self.congestion_hold_ms)
            .field("unbounded_network_retries", &self.unbounded_network_retries)
            .finish()
    }
}

/// The failure outcome of [`with_retry`].
#[derive(Debug, Clone)]
pub enum RetryError {
    /// The signal was cancelled — the establishment work was aborted and is
    /// never retried.
    Aborted,
    /// The attempt failed and was not (or no longer) retryable; carries the
    /// classified error.
    Provider(ProviderError),
}

impl fmt::Display for RetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryError::Aborted => write!(f, "aborted"),
            RetryError::Provider(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RetryError {}

/// Default backoff clock: sleep `ms`, resolving early with `Err(Aborted)` if the
/// signal is cancelled.
async fn default_sleep(ms: u64, signal: AbortSignal) -> Result<(), Aborted> {
    if ms == 0 {
        return Ok(());
    }
    if signal.is_cancelled() {
        return Err(Aborted);
    }
    tokio::select! {
        biased;
        _ = signal.cancelled() => Err(Aborted),
        _ = tokio::time::sleep(Duration::from_millis(ms)) => Ok(()),
    }
}

/// Run `attempt` (which opens the request and returns its result) under the
/// retry matrix. `attempt` is invoked with the current 0-based attempt index and
/// returns the raw failure signals on error; [`classify_error`] folds them into
/// the retry decision.
///
/// IMPORTANT: only pass establishment work here. The caller is responsible for
/// NOT re-running anything that already produced streamed output.
pub async fn with_retry<T, F, Fut>(
    opts: RetryOptions,
    signal: AbortSignal,
    mut attempt: F,
) -> Result<T, RetryError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, RawProviderError>>,
{
    let max_attempts = opts.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1);
    let base_delay = opts.base_delay_ms.unwrap_or(DEFAULT_BASE_DELAY_MS);
    let max_delay = opts.max_delay_ms.unwrap_or(DEFAULT_MAX_DELAY_MS);
    let jitter = opts.jitter.unwrap_or(DEFAULT_JITTER);
    let sleep: SleepFn = opts
        .sleep
        .clone()
        .unwrap_or_else(|| Arc::new(|ms, sig| Box::pin(default_sleep(ms, sig))));
    let random: RandomFn = opts
        .random
        .clone()
        .unwrap_or_else(|| Arc::new(rand::random::<f64>));

    let mut i: u32 = 0;
    let mut network_waits: u32 = 0;
    let started = Instant::now();
    let congestion_hold = Duration::from_millis(
        opts.congestion_hold_ms
            .unwrap_or(DEFAULT_CONGESTION_HOLD_MS),
    );
    loop {
        // A user abort is not retryable and short-circuits before any attempt.
        if signal.is_cancelled() {
            return Err(RetryError::Aborted);
        }
        match attempt(i).await {
            Ok(value) => return Ok(value),
            Err(raw) => {
                // User aborts propagate untouched and are never retried.
                if signal.is_cancelled() {
                    return Err(RetryError::Aborted);
                }
                let classified = classify_error(&raw);
                if opts.yield_to_credential_rotation
                    && crate::provider::rotation::is_quota_rotation_error(&classified)
                {
                    return Err(RetryError::Provider(classified));
                }
                // Congestion (rate-limited / overloaded) holds the request and keeps
                // retrying past the attempt cap while inside the hold window — the
                // upstream queue drains on its own schedule, so a hard attempt cap
                // would surface a transient "busy" as a terminal failure. Every other
                // retryable kind stays attempt-bounded.
                let congestion = matches!(
                    classified.kind,
                    ProviderErrorKind::RateLimit | ProviderErrorKind::Overloaded
                );
                let hold_active = congestion && started.elapsed() < congestion_hold;
                // A lost network recovers on its own clock: connection failures keep
                // waiting past the attempt cap on the slower network-wait schedule
                // until the signal aborts, mirroring the congestion hold. A body
                // that arrived but failed to decode is a server-side defect, not a
                // dead connection — waiting for the network would spin against a
                // persistently broken server, so it stays attempt-bounded.
                let network_wait = classified.kind == ProviderErrorKind::Network
                    && !classified.message.contains("error decoding response body")
                    && opts.unbounded_network_retries.unwrap_or(true);
                let attempts_exhausted = i + 1 >= max_attempts;
                let retryable = match opts.retry_class_policy {
                    Some(RetryClassPolicy::ApprovalReview) => match raw.status {
                        Some(status) => (500..=599).contains(&status),
                        None => classified.kind == ProviderErrorKind::Network,
                    },
                    None => is_retryable(&classified),
                };
                if !retryable || (attempts_exhausted && !hold_active && !network_wait) {
                    return Err(RetryError::Provider(classified));
                }
                let delay_ms = if opts.retry_class_policy == Some(RetryClassPolicy::ApprovalReview)
                {
                    classified.retry_after_ms.unwrap_or_else(|| {
                        let base = (base_delay as f64) * 2f64.powi(i as i32);
                        (base * (0.9 + 0.2 * random())).max(0.0) as u64
                    })
                } else if network_wait && attempts_exhausted {
                    let delay = network_wait_delay(network_waits);
                    network_waits += 1;
                    delay
                } else {
                    compute_delay(
                        i,
                        &classified,
                        base_delay,
                        max_delay,
                        jitter,
                        random.as_ref(),
                    )
                };
                if let Some(on_retry) = &opts.on_retry {
                    on_retry(RetryInfo {
                        attempt: i + 1,
                        max_attempts,
                        delay_ms,
                        error: classified.clone(),
                        network_wait: network_wait && attempts_exhausted,
                    });
                }
                if sleep(delay_ms, signal.clone()).await.is_err() {
                    return Err(RetryError::Aborted);
                }
            }
        }
        i += 1;
    }
}

/// Whether the classified error's request establishment may be retried.
pub fn is_retryable(error: &ProviderError) -> bool {
    // A billing/usage exhaustion is terminal even though it arrives as HTTP 429 —
    // guard before the status check so it isn't force-retried as a transient
    // limit.
    if error.status == Some(413)
        || matches!(
            error.kind,
            ProviderErrorKind::Quota
                | ProviderErrorKind::PromptTooLong
                | ProviderErrorKind::RequestTooLarge
        )
    {
        return false;
    }
    if let Some(status) = error.status {
        if is_retryable_status(status) {
            return true;
        }
        if status >= 500 {
            return true;
        }
    }
    is_retryable_kind(error.kind)
}

/// Backoff for the unbounded network-wait schedule: `5s * 2^n` capped at 60s,
/// no jitter — a machine waiting for its network back wants a predictable,
/// visible cadence, and the cap keeps a long outage from sleeping for minutes.
pub fn network_wait_delay(waits: u32) -> u64 {
    (DEFAULT_NETWORK_WAIT_BASE_DELAY_MS << waits.min(4)).min(DEFAULT_NETWORK_WAIT_MAX_DELAY_MS)
}

/// Backoff for attempt `i` (0-based). A server-provided delay (Retry-After /
/// rate-limit-reset) takes precedence over the exponential schedule — we never
/// hammer earlier than the server told us to — but we still cap it at
/// `max_delay` and never go below the exponential floor, so a `Retry-After: 0`
/// can't busy-loop.
pub fn compute_delay(
    i: u32,
    error: &ProviderError,
    base_delay: u64,
    max_delay: u64,
    jitter: f64,
    random: impl Fn() -> f64,
) -> u64 {
    let max_delay_f = max_delay as f64;
    let exp = max_delay_f.min(base_delay as f64 * 2f64.powi(i as i32));
    // Honor the header when present, but clamp into [exp, max_delay] so a tiny or
    // huge value can neither stampede nor stall the whole policy.
    let base = match error.retry_after_ms {
        Some(server) => max_delay_f.min(exp.max(server as f64)),
        None => exp,
    };
    let jittered = base + base * jitter * random();
    // Saturating cast: a negative jitter can drive this below zero, which floors
    // to 0 (an immediate retry), matching a non-positive sleep.
    max_delay_f.min(jittered.round()) as u64
}

/// Case-insensitive header lookup mirroring a plain record with lowercase keys.
pub(crate) fn header_get<'a>(headers: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    headers
        .get(key)
        .or_else(|| headers.get(&key.to_ascii_lowercase()))
        .map(String::as_str)
}

/// Reset headers consulted after `retry-after`, most-specific first.
const RESET_HEADER_KEYS: [&str; 7] = [
    "anthropic-ratelimit-input-tokens-reset",
    "anthropic-ratelimit-output-tokens-reset",
    "anthropic-ratelimit-tokens-reset",
    "anthropic-ratelimit-requests-reset",
    "x-ratelimit-reset-tokens",
    "x-ratelimit-reset-requests",
    "x-ratelimit-reset",
];

/// Parse server retry-timing headers, prioritizing the most specific:
///   1. `retry-after` (seconds, or an HTTP-date)
///   2. `*-ratelimit-*-reset` (epoch seconds or seconds-from-now)
///
/// Returns milliseconds-to-wait, or `None` if nothing usable is present.
pub fn parse_retry_after(headers: &HashMap<String, String>) -> Option<u64> {
    parse_retry_after_f64(headers).map(|ms| ms.max(0.0).round() as u64)
}

fn parse_retry_after_f64(headers: &HashMap<String, String>) -> Option<f64> {
    // 1) Retry-After: the canonical, highest-priority signal.
    if let Some(ms) = parse_delay_seconds(header_get(headers, "retry-after")) {
        return Some(ms);
    }
    // 2) Provider rate-limit reset headers.
    for key in RESET_HEADER_KEYS {
        if let Some(ms) = parse_reset_header(header_get(headers, key)) {
            return Some(ms);
        }
    }
    None
}

/// `Retry-After`: integer/float seconds, or an HTTP-date → ms (>= 0).
fn parse_delay_seconds(raw: Option<&str>) -> Option<f64> {
    let raw = match raw {
        Some(r) if !r.is_empty() => r,
        _ => return None,
    };
    if let Some(secs) = js_number(raw).filter(|n| n.is_finite()) {
        return Some((secs * 1000.0).max(0.0));
    }
    if let Some(when) = parse_http_date(raw) {
        return Some((when - now_ms()).max(0.0));
    }
    None
}

static RESET_MS_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d+(?:\.\d+)?)ms$").unwrap());
static RESET_S_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d+(?:\.\d+)?)s$").unwrap());

/// Rate-limit reset headers come in two flavors:
///   - a duration like `5s` / `1.5s` / `250ms`, or bare seconds-from-now
///   - an absolute RFC3339 / epoch-seconds timestamp → delta from now
fn parse_reset_header(raw: Option<&str>) -> Option<f64> {
    let raw = match raw {
        Some(r) if !r.is_empty() => r,
        _ => return None,
    };
    let trimmed = raw.trim();

    if let Some(caps) = RESET_MS_RE.captures(trimmed) {
        let n: f64 = caps[1].parse().ok()?;
        return Some(n.round().max(0.0));
    }
    if let Some(caps) = RESET_S_RE.captures(trimmed) {
        let n: f64 = caps[1].parse().ok()?;
        return Some((n * 1000.0).round().max(0.0));
    }

    if let Some(num) = js_number(trimmed).filter(|n| n.is_finite()) {
        // Epoch-seconds (absolute) vs seconds-from-now (relative). A value larger
        // than a recent epoch boundary is clearly absolute.
        let now_sec = now_ms() / 1000.0;
        if num > 1_000_000_000.0 {
            return Some(((num - now_sec) * 1000.0).round().max(0.0));
        }
        return Some((num * 1000.0).round().max(0.0));
    }

    if let Some(when) = parse_http_date(trimmed) {
        return Some((when - now_ms()).max(0.0));
    }
    None
}

/// Milliseconds since the Unix epoch.
fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

/// Permissive numeric coercion: surrounding whitespace is ignored and an empty
/// (or all-whitespace) string is zero; anything else that is not a finite number
/// yields `None`. Non-finite parses (`inf`/`nan`) are filtered by callers.
fn js_number(raw: &str) -> Option<f64> {
    let t = raw.trim();
    if t.is_empty() {
        return Some(0.0);
    }
    t.parse::<f64>().ok()
}

/// Parse an HTTP-date / RFC3339 timestamp to epoch milliseconds, or `None`.
fn parse_http_date(raw: &str) -> Option<f64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.timestamp_millis() as f64);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(raw) {
        return Some(dt.timestamp_millis() as f64);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    fn perr(kind: ProviderErrorKind, status: Option<u16>) -> ProviderError {
        ProviderError {
            kind,
            message: crate::runtime_values::RuntimeString::default(),
            status,
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms: None,
            request_id: None,
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn raw_status(status: u16) -> RawProviderError {
        RawProviderError {
            status: Some(status),
            message: "boom".into(),
            ..RawProviderError::default()
        }
    }

    /// A transport-level failure with no HTTP status: classifies as Network.
    fn raw_network() -> RawProviderError {
        RawProviderError {
            status: None,
            message: "fetch failed: ECONNRESET".into(),
            ..RawProviderError::default()
        }
    }

    /// A sleep that resolves immediately, so tests never wait real time.
    fn instant_sleep() -> SleepFn {
        Arc::new(|_ms, _sig| Box::pin(async { Ok(()) }))
    }

    /// A sleep that reports cancellation.
    fn aborting_sleep() -> SleepFn {
        Arc::new(|_ms, _sig| Box::pin(async { Err(Aborted) }))
    }

    // ---- is_retryable ----

    #[test]
    fn retryable_by_kind() {
        for kind in [
            ProviderErrorKind::RateLimit,
            ProviderErrorKind::Overloaded,
            ProviderErrorKind::Network,
            ProviderErrorKind::Timeout,
        ] {
            assert!(is_retryable(&perr(kind, None)), "kind={kind:?}");
        }
        for kind in [
            ProviderErrorKind::Auth,
            ProviderErrorKind::InvalidRequest,
            ProviderErrorKind::PromptTooLong,
            ProviderErrorKind::Unknown,
        ] {
            assert!(!is_retryable(&perr(kind, None)), "kind={kind:?}");
        }
    }

    #[test]
    fn retryable_by_status() {
        for status in [429, 500, 502, 503, 529, 501, 599] {
            assert!(
                is_retryable(&perr(ProviderErrorKind::Unknown, Some(status))),
                "status={status}"
            );
        }
        // 405 belongs here for a reason worth keeping: an edge WAF answers a
        // refused request body with it, and that refusal is about the CONTENT, so
        // the identical body is refused on every attempt. Making it retryable
        // turns a dead end into a silent backoff loop that can never land.
        for status in [400, 401, 403, 404, 405, 422] {
            assert!(
                !is_retryable(&perr(ProviderErrorKind::Unknown, Some(status))),
                "status={status}"
            );
        }
    }

    #[test]
    fn quota_is_terminal_even_on_429() {
        assert!(!is_retryable(&perr(ProviderErrorKind::Quota, Some(429))));
    }

    #[test]
    fn a_402_is_classified_quota_and_never_retried() {
        // The Qoder gateway answers billing refusals with HTTP 402; before the
        // status was mapped, the word fell through to 502 and the turn sat in
        // the congestion hold as a fake overload.
        let classified = classify_error(&RawProviderError {
            status: Some(402),
            message: "Qoder upstream PAYMENT_REQUIRED: Error in upstream response".into(),
            ..Default::default()
        });
        assert_eq!(classified.kind, ProviderErrorKind::Quota);
        assert!(!is_retryable(&classified));
    }

    #[test]
    fn a_plan_allowance_code_on_429_is_terminal_and_never_retried() {
        // Zhipu's Coding Plan stamps EVERY refusal with HTTP 429 and separates them
        // only by a numeric code in the body, under the generic `api_error` type.
        // The allowance codes (exhausted, plan expired, weekly cap, model not opened
        // by the subscription) never clear by waiting, so before they were mapped the
        // turn sat in the congestion hold as a fake overload — minutes of a hung
        // conversation ending in a refusal that reads as an unrelated bug.
        for code in ["1308", "1309", "1310", "1311"] {
            let classified = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                body_type: Some("api_error".into()),
                message: "[1308][您的额度已达到上限]".into(),
                ..Default::default()
            });
            assert_eq!(classified.kind, ProviderErrorKind::Quota, "code {code}");
            assert!(
                !is_retryable(&classified),
                "code {code} must not be retried"
            );
        }
    }

    #[test]
    fn the_same_gateway_throttling_codes_on_429_still_back_off() {
        // The transient half of the same service: rate limit reached, model traffic
        // too high, and fair-use throttling all arrive on the same 429 with the same
        // `api_error` type. These DO clear by waiting, so treating them as terminal
        // would fail a conversation that a short backoff would have carried through.
        for code in ["1302", "1305", "1313"] {
            let classified = classify_error(&RawProviderError {
                status: Some(429),
                code: Some(code.into()),
                body_type: Some("api_error".into()),
                message: "[1302][达到速率限制]".into(),
                ..Default::default()
            });
            assert_ne!(classified.kind, ProviderErrorKind::Quota, "code {code}");
            assert!(is_retryable(&classified), "code {code} must still back off");
        }
    }

    // ---- compute_delay ----

    #[test]
    fn exponential_schedule_without_jitter() {
        let e = perr(ProviderErrorKind::RateLimit, None);
        assert_eq!(compute_delay(0, &e, 500, 30_000, 0.25, || 0.0), 500);
        assert_eq!(compute_delay(1, &e, 500, 30_000, 0.25, || 0.0), 1000);
        assert_eq!(compute_delay(2, &e, 500, 30_000, 0.25, || 0.0), 2000);
    }

    #[test]
    fn backoff_is_capped_at_max_delay() {
        let e = perr(ProviderErrorKind::RateLimit, None);
        assert_eq!(compute_delay(10, &e, 500, 30_000, 0.25, || 0.0), 30_000);
    }

    #[test]
    fn server_delay_takes_precedence_but_clamps() {
        // Server delay above the exponential floor is honored.
        let mut e = perr(ProviderErrorKind::RateLimit, None);
        e.retry_after_ms = Some(10_000);
        assert_eq!(compute_delay(0, &e, 500, 30_000, 0.0, || 0.0), 10_000);

        // Server delay below the exponential floor never dips under it.
        e.retry_after_ms = Some(100);
        assert_eq!(compute_delay(3, &e, 500, 30_000, 0.0, || 0.0), 4000);

        // Huge server delay is still capped at max_delay.
        e.retry_after_ms = Some(999_999);
        assert_eq!(compute_delay(0, &e, 500, 30_000, 0.0, || 0.0), 30_000);
    }

    #[test]
    fn jitter_is_added_on_top() {
        let e = perr(ProviderErrorKind::RateLimit, None);
        // 500 + 500 * 0.25 * 0.5 = 562.5 → round 563.
        assert_eq!(compute_delay(0, &e, 500, 30_000, 0.25, || 0.5), 563);
    }

    #[test]
    fn negative_jitter_floors_to_zero() {
        let e = perr(ProviderErrorKind::RateLimit, None);
        assert_eq!(compute_delay(0, &e, 500, 30_000, -1.0, || 1.0), 0);
    }

    // ---- parse_retry_after ----

    #[test]
    fn retry_after_seconds_and_reset_durations() {
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "5")])),
            Some(5000)
        );
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "1.5")])),
            Some(1500)
        );
        assert_eq!(
            parse_retry_after(&headers(&[("x-ratelimit-reset", "250ms")])),
            Some(250)
        );
        assert_eq!(
            parse_retry_after(&headers(&[("x-ratelimit-reset-tokens", "2s")])),
            Some(2000)
        );
        assert_eq!(parse_retry_after(&headers(&[])), None);
    }

    #[test]
    fn retry_after_http_date_in_the_past_is_zero() {
        // A past HTTP-date clamps to 0 (proves the date parses, then max(0,…)).
        let h = headers(&[("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")]);
        assert_eq!(parse_retry_after(&h), Some(0));
    }

    #[test]
    fn reset_header_priority_order() {
        // The anthropic input-tokens reset outranks a generic x-ratelimit-reset.
        let h = headers(&[
            ("x-ratelimit-reset", "9"),
            ("anthropic-ratelimit-input-tokens-reset", "3"),
        ]);
        assert_eq!(parse_retry_after(&h), Some(3000));
    }

    // ---- with_retry ----

    #[tokio::test]
    async fn succeeds_on_the_first_attempt() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let out: Result<u32, RetryError> =
            with_retry(RetryOptions::default(), AbortSignal::new(), move |_i| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(7)
                }
            })
            .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_a_transient_error_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let retries: Arc<Mutex<Vec<RetryInfo>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = retries.clone();
        let opts = RetryOptions {
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            on_retry: Some(Arc::new(move |info| seen.lock().unwrap().push(info))),
            ..RetryOptions::default()
        };
        let c = calls.clone();
        let out: Result<u32, RetryError> = with_retry(opts, AbortSignal::new(), move |i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    Err(raw_status(429))
                } else {
                    Ok(11)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), 11);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let seen = retries.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].attempt, 1);
        assert_eq!(seen[0].delay_ms, 500);
        assert_eq!(seen[0].error.kind, ProviderErrorKind::RateLimit);
    }

    #[tokio::test]
    async fn fails_fast_on_a_non_retryable_error() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            sleep: Some(instant_sleep()),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_status(400))
            }
        })
        .await;
        match out {
            Err(RetryError::Provider(e)) => assert_eq!(e.kind, ProviderErrorKind::InvalidRequest),
            other => panic!("expected Provider error, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausts_the_default_attempt_cap() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            congestion_hold_ms: Some(0),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_status(503))
            }
        })
        .await;
        match out {
            Err(RetryError::Provider(e)) => assert_eq!(e.kind, ProviderErrorKind::Overloaded),
            other => panic!("expected Provider error, got {other:?}"),
        }
        // Default cap is 3 total tries.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn honors_a_custom_attempt_cap() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            max_attempts: Some(2),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            congestion_hold_ms: Some(0),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_status(429))
            }
        })
        .await;
        assert!(matches!(out, Err(RetryError::Provider(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn congestion_hold_retries_past_the_attempt_cap() {
        // A congested (429) establishment keeps retrying inside the hold window even
        // after the attempt cap, then succeeds once the "queue" clears.
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            max_attempts: Some(2),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            congestion_hold_ms: Some(60_000),
            ..RetryOptions::default()
        };
        let out: Result<u32, RetryError> = with_retry(opts, AbortSignal::new(), move |i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                if i < 4 {
                    Err(raw_status(429))
                } else {
                    Ok(9)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), 9);
        // 4 congested failures + 1 success, beyond the cap of 2.
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn network_failures_wait_past_the_attempt_cap_until_abort() {
        // A dead network is not an attempt-budget problem: past the cap the
        // policy keeps waiting on the 5s→60s schedule, flagging each decision
        // as a network wait, until the turn is cancelled.
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let retries: Arc<Mutex<Vec<RetryInfo>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = retries.clone();
        let sleeps = Arc::new(AtomicU32::new(0));
        let s = sleeps.clone();
        let opts = RetryOptions {
            max_attempts: Some(2),
            congestion_hold_ms: Some(0),
            random: Some(Arc::new(|| 0.0)),
            sleep: Some(Arc::new(move |_ms, sig: AbortSignal| {
                let s = s.clone();
                Box::pin(async move {
                    // The fourth backoff stands in for the user giving up.
                    if s.fetch_add(1, Ordering::SeqCst) == 3 {
                        sig.cancel();
                    }
                    Ok(())
                })
            })),
            on_retry: Some(Arc::new(move |info| seen.lock().unwrap().push(info))),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_network())
            }
        })
        .await;
        assert!(matches!(out, Err(RetryError::Aborted)));
        // 2 capped attempts + 2 network waits before the abort.
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let seen = retries.lock().unwrap();
        assert_eq!(seen.len(), 4);
        assert!(!seen[0].network_wait);
        assert!(seen[1].network_wait && seen[2].network_wait && seen[3].network_wait);
        assert_eq!(seen[1].delay_ms, 5_000);
        assert_eq!(seen[2].delay_ms, 10_000);
    }

    #[tokio::test]
    async fn network_wait_can_be_disabled() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            max_attempts: Some(2),
            unbounded_network_retries: Some(false),
            congestion_hold_ms: Some(0),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_network())
            }
        })
        .await;
        match out {
            Err(RetryError::Provider(e)) => assert_eq!(e.kind, ProviderErrorKind::Network),
            other => panic!("expected Provider error, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn network_wait_schedule_doubles_to_the_cap() {
        assert_eq!(network_wait_delay(0), 5_000);
        assert_eq!(network_wait_delay(1), 10_000);
        assert_eq!(network_wait_delay(2), 20_000);
        assert_eq!(network_wait_delay(3), 40_000);
        assert_eq!(network_wait_delay(4), 60_000);
        assert_eq!(network_wait_delay(9), 60_000);
    }

    #[tokio::test]
    async fn body_decode_failures_stay_attempt_bounded() {
        // The connection was fine; the server sent a body we could not decode.
        // Retrying is bounded — an infinite wait would spin against a
        // persistently broken server.
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            max_attempts: Some(2),
            congestion_hold_ms: Some(0),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(RawProviderError {
                    status: None,
                    message: "error decoding response body".into(),
                    ..RawProviderError::default()
                })
            }
        })
        .await;
        match out {
            Err(RetryError::Provider(e)) => assert_eq!(e.kind, ProviderErrorKind::Network),
            other => panic!("expected Provider error, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn aborts_before_the_first_attempt() {
        let signal = AbortSignal::new();
        signal.cancel();
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let out: Result<(), RetryError> = with_retry(RetryOptions::default(), signal, move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;
        assert!(matches!(out, Err(RetryError::Aborted)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn aborts_when_the_backoff_is_cancelled() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let opts = RetryOptions {
            sleep: Some(aborting_sleep()),
            random: Some(Arc::new(|| 0.0)),
            ..RetryOptions::default()
        };
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(raw_status(429))
            }
        })
        .await;
        assert!(matches!(out, Err(RetryError::Aborted)));
        // Aborted during the first backoff — the attempt ran exactly once.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn abort_during_attempt_propagates() {
        let signal = AbortSignal::new();
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let sig = signal.clone();
        let out: Result<(), RetryError> = with_retry(RetryOptions::default(), signal, move |_i| {
            let c = c.clone();
            let sig = sig.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                // The signal fires while the attempt is in flight.
                sig.cancel();
                Err(raw_status(429))
            }
        })
        .await;
        assert!(matches!(out, Err(RetryError::Aborted)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn default_sleep_returns_immediately_for_zero() {
        assert!(default_sleep(0, AbortSignal::new()).await.is_ok());
    }

    #[tokio::test]
    async fn default_sleep_reports_a_pre_cancelled_signal() {
        let signal = AbortSignal::new();
        signal.cancel();
        assert_eq!(default_sleep(10_000, signal).await, Err(Aborted));
    }

    // ---- observe_retries ----

    #[tokio::test]
    async fn observe_retries_forwards_each_decision_and_keeps_the_prior_observer() {
        let prior_seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = prior_seen.clone();
        let opts = RetryOptions {
            max_attempts: Some(3),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            congestion_hold_ms: Some(0),
            on_retry: Some(Arc::new(move |info| {
                seen.lock().unwrap().push(info.attempt)
            })),
            ..RetryOptions::default()
        };
        let (opts, mut rx) = observe_retries(opts);
        let out: Result<(), RetryError> = with_retry(opts, AbortSignal::new(), move |_i| async {
            Err(raw_status(503))
        })
        .await;
        assert!(matches!(out, Err(RetryError::Provider(_))));
        // Both failed attempts before the cap surfaced through the channel …
        let first = rx.recv().await.expect("first retry decision");
        assert_eq!(first.attempt, 1);
        assert_eq!(first.max_attempts, 3);
        assert_eq!(first.delay_ms, 500);
        assert_eq!(first.error.kind, ProviderErrorKind::Overloaded);
        let second = rx.recv().await.expect("second retry decision");
        assert_eq!(second.attempt, 2);
        assert!(rx.recv().await.is_none());
        // … and the pre-existing observer still saw every decision.
        assert_eq!(*prior_seen.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn establishment_steps_surface_every_retry_before_the_result() {
        use futures::StreamExt;

        let opts = RetryOptions {
            max_attempts: Some(3),
            sleep: Some(instant_sleep()),
            random: Some(Arc::new(|| 0.0)),
            ..RetryOptions::default()
        };
        let (opts, rx) = observe_retries(opts);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let fut = with_retry(opts, AbortSignal::new(), move |i| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                if i < 2 {
                    Err(raw_status(503))
                } else {
                    Ok(41_u32)
                }
            }
        });
        let steps: Vec<EstablishmentStep<Result<u32, RetryError>>> =
            establishment_steps(fut, rx).collect().await;
        assert_eq!(steps.len(), 3);
        for (index, step) in steps.iter().take(2).enumerate() {
            match step {
                EstablishmentStep::Retry(ModelStreamEvent::RetryScheduled {
                    attempt,
                    max_attempts,
                    ..
                }) => {
                    assert_eq!(*attempt, index as u32 + 1);
                    assert_eq!(*max_attempts, 3);
                }
                _ => panic!("expected a retry step at index {index}"),
            }
        }
        match steps.last() {
            Some(EstablishmentStep::Done(Ok(v))) => assert_eq!(*v, 41),
            _ => panic!("expected the settled result as the final step"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn gateway_first_pack_timeout_409_is_retryable() {
        // The gateway stamps upstream prefill congestion as HTTP 409 with
        // "first pack timeout"; the signature classifies it Overloaded so the
        // establishment retry (and its surfaced "reconnecting" notice) covers
        // it instead of failing the turn on the first 409.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "first pack timeout".into(),
            ..RawProviderError::default()
        });
        assert!(is_retryable(&e));
    }

    #[test]
    fn gateway_interval_pack_timeout_409_is_retryable() {
        // The sibling gateway wording for an upstream stall between response
        // packets: same 409 verdict family, same congestion retry.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "interval pack timeout".into(),
            code: Some("model_proxy_ultra_inner_error".into()),
            ..RawProviderError::default()
        });
        assert!(is_retryable(&e));
    }

    #[test]
    fn gateway_409_inner_error_is_retryable_for_any_message() {
        // The status+code pair alone selects the congestion retry, so a new
        // wording cannot fall back to a non-retryable Unknown.
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "upstream packet spacing exceeded".into(),
            code: Some("model_proxy_ultra_inner_error".into()),
            ..RawProviderError::default()
        });
        assert_eq!(e.kind, ProviderErrorKind::Overloaded);
        assert!(is_retryable(&e));
    }

    #[test]
    fn plain_409_is_not_retryable() {
        let e = classify_error(&RawProviderError {
            status: Some(409),
            message: "conflict: resource state changed".into(),
            ..RawProviderError::default()
        });
        assert!(!is_retryable(&e));
    }
}
