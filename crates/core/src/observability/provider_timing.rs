//! Always-on, content-free lifecycle records for model requests.
//! Deadline ownership stays with the caller; progress ticks never renew it.

use super::provider_diagnostics::{record_named, TIMING_LOG_FILE_NAME};
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use crate::types::provider::{CallModel, ModelRequest, ModelStream, ModelStreamEvent};
use crate::types::tool::UuidFn;
use futures::{Stream, StreamExt};
use serde_json::{json, Value};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) enum StreamStep {
    Event(Option<ModelStreamEvent>),
    Timeout { total: bool },
    Cancelled,
}

pub(crate) struct RequestTiming {
    fields: Value,
    started: Instant,
    next_progress: Instant,
    events: u64,
    first_model_event: bool,
    last_event_kind: Option<&'static str>,
    idle_ms: u64,
    signal: AbortSignal,
    finished: bool,
    #[cfg(test)]
    test_path: Option<std::path::PathBuf>,
}

impl RequestTiming {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        purpose: &'static str,
        request_id: &str,
        model: &RuntimeString,
        provider_id: Option<&RuntimeString>,
        session_id: Option<&RuntimeString>,
        attempt: u32,
        idle_ms: u64,
        total_ms: u64,
        signal: AbortSignal,
    ) -> Self {
        let started = Instant::now();
        let mut fields = json!({
            "purpose": purpose,
            "requestId": request_id,
            "model": model.try_to_utf8().ok(),
            "attempt": attempt,
            "idleBudgetMs": idle_ms,
            "totalBudgetMs": total_ms,
        });
        if let Some(id) = provider_id.and_then(|id| id.try_to_utf8().ok()) {
            fields["providerId"] = json!(id);
        }
        if let Some(id) = session_id.and_then(|id| id.try_to_utf8().ok()) {
            fields["sessionId"] = json!(id);
        }
        let timing = Self {
            fields,
            started,
            next_progress: started + PROGRESS_INTERVAL,
            events: 0,
            first_model_event: false,
            last_event_kind: None,
            idle_ms,
            signal,
            finished: false,
            #[cfg(test)]
            test_path: None,
        };
        timing.record("started");
        timing
    }

    fn record(&self, kind: &str) {
        let mut fields = self.fields.clone();
        fields["elapsedMs"] = json!(self.started.elapsed().as_millis() as u64);
        fields["eventCount"] = json!(self.events);
        fields["lastEventKind"] = json!(self.last_event_kind);
        #[cfg(test)]
        if let Some(path) = &self.test_path {
            let _ = super::provider_diagnostics::record_to(path, kind, fields);
            return;
        }
        record_named(TIMING_LOG_FILE_NAME, kind, fields);
    }

    fn observe(&mut self, event: &ModelStreamEvent) {
        self.events = self.events.saturating_add(1);
        self.last_event_kind = Some(event_kind(event));
        self.next_progress = Instant::now() + PROGRESS_INTERVAL;
        if let ModelStreamEvent::Error { error } = event {
            self.fields["errorKind"] = json!(error.kind.as_str());
            self.fields["status"] = json!(error.status);
            self.fields["retryAfterMs"] = json!(error.retry_after_ms);
            self.fields["upstreamRequestId"] = json!(error
                .request_id
                .as_ref()
                .and_then(|id| id.try_to_utf8().ok()));
            if let Some(recovery) = &error.credential_recovery {
                let mut attempted = recovery.attempted_account_ids.clone();
                if let Some(failed) = &recovery.failed_account_id {
                    if !attempted.contains(failed) {
                        attempted.push(failed.clone());
                    }
                }
                self.fields["credentialRequestId"] = json!(recovery.diagnostic_request_id);
                self.fields["failedAccountId"] = json!(recovery.failed_account_id);
                self.fields["attemptedAccountCount"] = json!(attempted.len());
                self.fields["attemptedAccountIds"] = json!(attempted);
                self.fields["credentialExhausted"] = json!(recovery.exhausted);
            }
        }
        if let ModelStreamEvent::MessageStop { usage, .. }
        | ModelStreamEvent::UsageSnapshot { usage } = event
        {
            // The terminal record then pairs this request's prefix fingerprints
            // with what the provider actually served from cache, which is the
            // whole point of carrying both on one line.
            self.fields["promptInputTokens"] = json!(usage.input_tokens);
            self.fields["cacheReadInputTokens"] = json!(usage.cache_read_input_tokens);
            self.fields["cacheCreationInputTokens"] = json!(usage.cache_creation_input_tokens);
        }
        if let ModelStreamEvent::RetryScheduled {
            attempt,
            max_attempts,
            delay_ms,
            error,
            network_wait,
        } = event
        {
            self.fields["providerRetryAttempt"] = json!(attempt);
            self.fields["providerRetryLimit"] = json!(max_attempts);
            self.fields["retryDelayMs"] = json!(delay_ms);
            self.fields["lastRetryNetworkWait"] = json!(network_wait);
            self.fields["errorKind"] = json!(format!("{:?}", error.kind));
            self.record("retry");
        } else if !matches!(event, ModelStreamEvent::Error { .. }) && !self.first_model_event {
            self.first_model_event = true;
            self.record("first_event");
        }
    }

    /// One original inter-event wait, with diagnostics inserted without replacing
    /// the pending next() future or resetting its idle deadline on a tick.
    pub(crate) async fn next_event(
        &mut self,
        stream: &mut ModelStream,
        total_deadline: Instant,
        watch_cancellation: bool,
    ) -> StreamStep {
        let idle_deadline = Instant::now() + Duration::from_millis(self.idle_ms);
        let total_is_next = total_deadline <= idle_deadline;
        let deadline = total_deadline.min(idle_deadline);
        let next = stream.next();
        tokio::pin!(next);
        loop {
            if Instant::now() >= total_deadline {
                return StreamStep::Timeout { total: true };
            }
            tokio::select! {
                biased;
                _ = self.signal.cancelled(), if watch_cancellation => return StreamStep::Cancelled,
                event = &mut next => {
                    if let Some(event) = &event {
                        self.observe(event);
                    }
                    return StreamStep::Event(event);
                }
                _ = tokio::time::sleep_until(deadline) => return StreamStep::Timeout { total: total_is_next },
                _ = tokio::time::sleep_until(self.next_progress) => {
                    self.record("progress");
                    self.next_progress = Instant::now() + PROGRESS_INTERVAL;
                }
            }
        }
    }

    /// Attach this request's prefix fingerprints to every later record of this
    /// request, including the terminal one.
    ///
    /// Called once per request before the stream is polled: the digest is a
    /// property of what was sent, so computing it again on each event would only
    /// repeat work. The pass is linear in the request's text, which the provider
    /// layer serializes for the wire anyway.
    pub(crate) fn record_prefix(&mut self, request: &ModelRequest) {
        let fingerprint = super::prefix_fingerprint::PrefixFingerprint::of(request);
        if let (Some(fields), Value::Object(prefix)) =
            (self.fields.as_object_mut(), fingerprint.fields())
        {
            for (key, value) in prefix {
                fields.insert(key, value);
            }
        }
    }

    /// Bind the owning turn without changing other request-timing callers.
    pub(crate) fn bind_turn_id(&mut self, turn_id: &RuntimeString) {
        if let Ok(id) = turn_id.try_to_utf8() {
            self.fields["turnId"] = json!(id);
        }
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        if !self.finished {
            self.finished = true;
            self.record(outcome);
        }
    }
}

impl Drop for RequestTiming {
    fn drop(&mut self) {
        self.finish(if self.signal.is_cancelled() {
            "cancelled"
        } else {
            "dropped"
        });
    }
}

/// Passive instrumentation wrapper for a stream consumed by another loop.
///
/// The foreground turn loop drives its own stream through
/// [`RequestTiming::next_event`] and owns the deadlines. Background consumers —
/// such as the precomputed-compaction summary fork — drive the stream
/// themselves; this wrapper only observes the passing events so those requests
/// share the timing log and its record vocabulary with foreground ones.
/// Deadline ownership stays with the consumer.
struct ObservedStream {
    inner: ModelStream,
    timing: RequestTiming,
}

impl ObservedStream {
    fn boxed(inner: ModelStream, timing: RequestTiming) -> ModelStream {
        Box::pin(Self { inner, timing })
    }
}

impl Stream for ObservedStream {
    type Item = ModelStreamEvent;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Both fields are Unpin, so no pin projection is needed.
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(event)) => {
                this.timing.observe(&event);
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => {
                // A stream that ends on an unrecovered error is a failed
                // request; anything else that reaches the end completed.
                // Mid-flight cancellation surfaces through `Drop`.
                let outcome = if this.timing.last_event_kind == Some("error") {
                    "failed"
                } else {
                    "completed"
                };
                this.timing.finish(outcome);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Identity fields labeling one precompute fork's background summary requests
/// in the timing log.
pub(crate) struct PrecomputeTimingContext {
    pub uuid: UuidFn,
    pub provider_id: RuntimeString,
    pub session_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub idle_ms: u64,
    pub total_ms: u64,
}

/// Wrap the model-call seam handed to a precompute fork so every background
/// summary request lands in the timing log with `purpose=precompute`, next to
/// the foreground `purpose=normal` records. The native summary transport
/// bypasses this seam; its lifecycle is covered by the registry's own records.
pub(crate) fn precompute_call_model(
    inner: CallModel,
    context: PrecomputeTimingContext,
) -> CallModel {
    let PrecomputeTimingContext {
        uuid,
        provider_id,
        session_id,
        turn_id,
        idle_ms,
        total_ms,
    } = context;
    Box::new(move |request: ModelRequest, signal: AbortSignal| {
        let request_id = (uuid)();
        let mut timing = RequestTiming::start(
            "precompute",
            &request_id,
            &request.model,
            Some(&provider_id),
            Some(&session_id),
            1,
            idle_ms,
            total_ms,
            signal.clone(),
        );
        timing.bind_turn_id(&turn_id);
        timing.record_prefix(&request);
        ObservedStream::boxed(inner(request, signal), timing)
    })
}

/// One-shot lifecycle record for background compaction precompute: the
/// arm/ready/consume moments bracketing the request records above. Content-free
/// like every other timing record; callers pass identifiers, counters and
/// static reason codes only.
pub(crate) fn record_precompute(kind: &str, fields: Value) {
    let mut fields = fields;
    if let Value::Object(object) = &mut fields {
        object.insert("purpose".into(), json!("precompute"));
    }
    record_named(TIMING_LOG_FILE_NAME, kind, fields);
}

fn event_kind(event: &ModelStreamEvent) -> &'static str {
    match event {
        ModelStreamEvent::TextMessage { .. } => "text_message",
        ModelStreamEvent::TextMessageCompleted { .. } => "text_message_completed",
        ModelStreamEvent::MessageStart { .. } => "message_start",
        ModelStreamEvent::TextDelta { .. } => "text_delta",
        ModelStreamEvent::ThinkingDelta { .. } => "thinking_delta",
        ModelStreamEvent::ThinkingBlock { .. } => "thinking_block",
        ModelStreamEvent::RedactedThinkingBlock { .. } => "redacted_thinking_block",
        ModelStreamEvent::ToolUseStart { .. } => "tool_use_start",
        ModelStreamEvent::ToolUseInputDelta { .. } => "tool_use_input_delta",
        ModelStreamEvent::ToolUseStop { .. } => "tool_use_stop",
        ModelStreamEvent::ServerSearchQuery { .. } => "server_search_query",
        ModelStreamEvent::WebSearchResult { .. } => "web_search_result",
        ModelStreamEvent::Citation { .. } => "citation",
        ModelStreamEvent::UsageSnapshot { .. } => "usage_snapshot",
        ModelStreamEvent::ContextEditsApplied { .. } => "context_edits_applied",
        ModelStreamEvent::MessageStop { .. } => "message_stop",
        ModelStreamEvent::Error { .. } => "error",
        ModelStreamEvent::RetryScheduled { .. } => "retry_scheduled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timing(idle: u64, total: u64) -> RequestTiming {
        let path = std::env::temp_dir().join(format!("klaus-timing-{}.log", uuid::Uuid::new_v4()));
        let mut timing = RequestTiming::start(
            "summary",
            "test",
            &"model".into(),
            None,
            None,
            1,
            idle,
            total,
            AbortSignal::new(),
        );
        timing.test_path = Some(path);
        timing.record("started");
        timing
    }

    fn records(path: &std::path::Path) -> Vec<Value> {
        let text = std::fs::read_to_string(path).unwrap();
        std::fs::remove_file(path).unwrap();
        text.lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn credential_failures_join_request_scope_without_private_error_contents() {
        use crate::types::errors::{CredentialRecovery, ProviderError, ProviderErrorKind};

        for sequence in [1, 2] {
            let session_id = RuntimeString::from(format!("synthetic-session-{sequence}"));
            let turn_id = RuntimeString::from(format!("synthetic-turn-{sequence}"));
            let outer_id = format!("synthetic-model-request-{sequence}");
            let credential_id = format!("synthetic-credential-request-{sequence}");
            let mut timing = RequestTiming::start(
                "normal",
                &outer_id,
                &"synthetic-model".into(),
                Some(&"synthetic-provider".into()),
                Some(&session_id),
                sequence,
                100,
                200,
                AbortSignal::new(),
            );
            let path = std::env::temp_dir()
                .join(format!("klaus-timing-join-{}.log", uuid::Uuid::new_v4()));
            timing.test_path = Some(path.clone());
            timing.bind_turn_id(&turn_id);
            timing.observe(&ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::RateLimit,
                    message:
                        "private-user@example.invalid Authorization: Bearer private-token body"
                            .into(),
                    status: Some(429),
                    code: Some("private-code".into()),
                    error_type: Some("private-type".into()),
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: Some(Box::new(CredentialRecovery {
                        diagnostic_request_id: Some(credential_id.clone()),
                        failed_account_id: Some("synthetic-account-b".into()),
                        attempted_account_ids: vec!["synthetic-account-a".into()],
                        exhausted: true,
                    })),
                    retry_after_ms: Some(120_000),
                    request_id: Some("synthetic-upstream-request".into()),
                },
            });
            timing.finish("failed");
            drop(timing);
            let records = records(&path);
            assert_eq!(records.len(), 1);
            let failure = &records[0];
            assert_eq!(failure["kind"], "failed");
            assert_eq!(failure["requestId"], outer_id);
            assert_eq!(failure["sessionId"], session_id.try_to_utf8().unwrap());
            assert_eq!(failure["turnId"], turn_id.try_to_utf8().unwrap());
            assert_eq!(failure["credentialRequestId"], credential_id);
            assert_eq!(failure["failedAccountId"], "synthetic-account-b");
            assert_eq!(
                failure["attemptedAccountIds"],
                json!(["synthetic-account-a", "synthetic-account-b"])
            );
            assert_eq!(failure["attemptedAccountCount"], 2);
            assert_eq!(failure["credentialExhausted"], true);
            assert_eq!(failure["errorKind"], "rate_limit");
            assert_eq!(failure["status"], 429);
            assert_eq!(failure["retryAfterMs"], 120_000);
            assert_eq!(failure["upstreamRequestId"], "synthetic-upstream-request");
            let text = serde_json::to_string(&records).unwrap();
            for private in [
                "private",
                "example.invalid",
                "Authorization",
                "Bearer",
                "body",
            ] {
                assert!(!text.contains(private), "{text}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn progress_tick_does_not_extend_idle_deadline() {
        let mut timing = timing(65_000, 120_000);
        let path = timing.test_path.clone().unwrap();
        let mut stream: ModelStream = Box::pin(futures::stream::pending());
        let step = timing
            .next_event(&mut stream, Instant::now() + Duration::from_secs(120), true)
            .await;
        assert!(matches!(step, StreamStep::Timeout { total: false }));
        assert_eq!(timing.started.elapsed(), Duration::from_secs(65));
        timing.finish("idle_timeout");
        drop(timing);
        let records = records(&path);
        assert_eq!(
            records
                .iter()
                .filter(|record| record["kind"] == "progress")
                .count(),
            2
        );
        assert_eq!(records.last().unwrap()["kind"], "idle_timeout");
        assert_eq!(records.last().unwrap()["elapsedMs"], 65_000);
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_records_never_serialize_event_contents_and_cover_failure() {
        let mut timing = timing(100, 200);
        let path = timing.test_path.clone().unwrap();
        timing.observe(&ModelStreamEvent::ThinkingBlock {
            text: "private body Bearer abcdef1234567890".into(),
            signature: Some("private signature".into()),
            encrypted_content: Some("private encryptedContent".into()),
            is_summary: false,
        });
        timing.finish("failed");
        drop(timing);
        let records = records(&path);
        let text = serde_json::to_string(&records).unwrap();
        for secret in [
            "private",
            "Bearer",
            "abcdef",
            "encryptedContent",
            "signature",
        ] {
            assert!(!text.contains(secret), "{text}");
        }
        assert_eq!(records.last().unwrap()["kind"], "failed");
        assert_eq!(records.last().unwrap()["eventCount"], 1);
        assert_eq!(records[1]["kind"], "first_event");
    }

    #[tokio::test(start_paused = true)]
    async fn drop_and_cancel_each_leave_one_terminal_record() {
        for cancelled in [false, true] {
            let timing = timing(100, 200);
            let path = timing.test_path.clone().unwrap();
            if cancelled {
                timing.signal.cancel();
            }
            drop(timing);
            let records = records(&path);
            assert_eq!(records.len(), 2);
            assert_eq!(
                records[1]["kind"],
                if cancelled { "cancelled" } else { "dropped" }
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_records_keep_attempt_and_backoff_without_error_body() {
        let mut timing = timing(100, 200);
        let path = timing.test_path.clone().unwrap();
        let mut stream: ModelStream = Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::RetryScheduled {
                attempt: 7,
                max_attempts: 3,
                delay_ms: 60_000,
                network_wait: true,
                error: crate::types::errors::ProviderError {
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    kind: crate::types::errors::ProviderErrorKind::Network,
                    message: "private retry body".into(),
                    status: None,
                    retry_after_ms: None,
                    request_id: None,
                },
            },
        ]));
        assert!(matches!(
            timing
                .next_event(
                    &mut stream,
                    Instant::now() + Duration::from_millis(200),
                    true,
                )
                .await,
            StreamStep::Event(Some(ModelStreamEvent::RetryScheduled { .. }))
        ));
        drop(timing);
        let text = std::fs::read_to_string(&path).unwrap();
        let entries = records(&path);
        let retry = entries.iter().find(|row| row["kind"] == "retry").unwrap();
        assert_eq!(retry["providerRetryAttempt"], 7);
        assert_eq!(retry["providerRetryLimit"], 3);
        assert_eq!(retry["retryDelayMs"], 60_000);
        assert_eq!(retry["lastRetryNetworkWait"], true);
        assert_eq!(retry["errorKind"], "Network");
        assert!(!text.contains("private retry body"));
    }

    #[tokio::test(start_paused = true)]
    async fn unwritable_log_does_not_change_stream_result() {
        let mut timing = timing(100, 200);
        let path = timing.test_path.clone().unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let mut stream: ModelStream =
            Box::pin(futures::stream::iter(vec![ModelStreamEvent::TextDelta {
                text: "success".into(),
            }]));
        assert!(matches!(
            timing
                .next_event(
                    &mut stream,
                    Instant::now() + Duration::from_millis(200),
                    true
                )
                .await,
            StreamStep::Event(Some(ModelStreamEvent::TextDelta { .. }))
        ));
        timing.finish("completed");
        drop(timing);
        std::fs::remove_dir(path).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn observed_stream_records_precompute_request_lifecycle() {
        let path = std::env::temp_dir().join(format!("klaus-timing-{}.log", uuid::Uuid::new_v4()));
        let mut timing = RequestTiming::start(
            "precompute",
            "synthetic-bg-request",
            &"synthetic-model".into(),
            Some(&"synthetic-provider".into()),
            Some(&"synthetic-session".into()),
            1,
            100,
            200,
            AbortSignal::new(),
        );
        timing.test_path = Some(path.clone());
        timing.record("started");
        let events = vec![
            ModelStreamEvent::MessageStart {
                model: "synthetic-model".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "private summary body".into(),
            },
        ];
        let mut stream = ObservedStream::boxed(Box::pin(futures::stream::iter(events)), timing);
        let mut seen = 0;
        while stream.next().await.is_some() {
            seen += 1;
        }
        drop(stream);
        assert_eq!(seen, 2);
        let records = records(&path);
        let kinds: Vec<&str> = records
            .iter()
            .map(|record| record["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, vec!["started", "first_event", "completed"]);
        let completed = records.last().unwrap();
        assert_eq!(completed["purpose"], "precompute");
        assert_eq!(completed["sessionId"], "synthetic-session");
        assert_eq!(completed["eventCount"], 2);
        let text = serde_json::to_string(&records).unwrap();
        assert!(!text.contains("private summary body"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn observed_stream_terminal_error_finishes_failed() {
        use crate::types::errors::{ProviderError, ProviderErrorKind};
        let path = std::env::temp_dir().join(format!("klaus-timing-{}.log", uuid::Uuid::new_v4()));
        let mut timing = RequestTiming::start(
            "precompute",
            "synthetic-bg-request",
            &"synthetic-model".into(),
            None,
            None,
            1,
            100,
            200,
            AbortSignal::new(),
        );
        timing.test_path = Some(path.clone());
        timing.record("started");
        let events = vec![ModelStreamEvent::Error {
            error: ProviderError {
                kind: ProviderErrorKind::Network,
                message: "private upstream body".into(),
                status: None,
                code: None,
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                credential_recovery: None,
                retry_after_ms: None,
                request_id: None,
            },
        }];
        let mut stream = ObservedStream::boxed(Box::pin(futures::stream::iter(events)), timing);
        while stream.next().await.is_some() {}
        drop(stream);
        let records = records(&path);
        let kinds: Vec<&str> = records
            .iter()
            .map(|record| record["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, vec!["started", "failed"]);
        assert_eq!(records.last().unwrap()["errorKind"], "network");
        let text = serde_json::to_string(&records).unwrap();
        assert!(!text.contains("private upstream body"), "{text}");
    }
}
