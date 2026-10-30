//! Always-on, content-free lifecycle records for model requests.
//! Deadline ownership stays with the caller; progress ticks never renew it.

use super::provider_diagnostics::{record_named, TIMING_LOG_FILE_NAME};
use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;
use crate::types::provider::{ModelRequest, ModelStream, ModelStreamEvent};
use futures::StreamExt;
use serde_json::{json, Value};
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
}
