//! An OpenTelemetry sink that folds the engine's outward event stream into
//! vendor-neutral metrics instruments, plus a convenience tracer accessor.

use crate::context::tokens::tokens_from_usage;
use crate::goal::types::{ThreadGoal, ThreadGoalStatus};
use crate::runtime_values::RuntimeString;
use crate::types::errors::{EngineErrorCode, TerminalReason};
use crate::types::events::{
    CompactionMode, EngineEvent, ItemStatus, NotificationLevel, ThreadItem,
};
use crate::types::permission::ApprovalKind;
use opentelemetry::global::{self, BoxedTracer};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::{InstrumentationScope, KeyValue};
use std::cell::Cell;

/// Options for [`create_otel_observer`].
#[derive(Default)]
pub struct CreateOtelObserverOptions {
    /// Instrumentation scope name for the meter.
    pub meter_name: Option<String>,
    /// Clock seam for turn-duration timing; defaults to wall-clock milliseconds.
    pub now: Option<Box<dyn Fn() -> f64 + Send + Sync>>,
    /// Meter override so tests (or embedding hosts) can supply a non-global
    /// provider; defaults to the global API, which is a no-op until a
    /// `MeterProvider` is registered.
    pub meter: Option<Meter>,
}

impl std::fmt::Debug for CreateOtelObserverOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateOtelObserverOptions")
            .field("meter_name", &self.meter_name)
            .field("now", &self.now.is_some())
            .field("meter", &self.meter.is_some())
            .finish()
    }
}

/// A thin, side-effecting sink that folds the engine's outward event stream into
/// OpenTelemetry instruments. The global metrics/trace API is a no-op until the
/// host process registers a `MeterProvider`/`TracerProvider`, so creating and
/// driving an observer is safe (and cheap) with zero configuration.
pub struct OtelObserver {
    now: Box<dyn Fn() -> f64 + Send + Sync>,
    turns: Counter<u64>,
    tool_calls: Counter<u64>,
    errors: Counter<u64>,
    tokens: Counter<u64>,
    turn_duration: Histogram<f64>,
    cache_tokens: Counter<u64>,
    cache_creation_tokens: Counter<u64>,
    tool_failures: Counter<u64>,
    compactions: Counter<u64>,
    ttft: Histogram<f64>,
    api_requests: Counter<u64>,
    items: Counter<u64>,
    approvals: Counter<u64>,
    notifications: Counter<u64>,
    turn_token_usage: Histogram<u64>,
    turn_tool_calls: Histogram<u64>,
    /// Goal-mode lifecycle instruments. Goal snapshots do NOT travel on the
    /// outward event stream (they go to the host's goal sink), so these are fed
    /// from the goal runtime's publish funnel rather than from `on_event`.
    goal_created: Counter<u64>,
    goal_resumed: Counter<u64>,
    goal_blocked: Counter<u64>,
    goal_usage_limited: Counter<u64>,
    goal_budget_limited: Counter<u64>,
    goal_completed: Counter<u64>,
    goal_token_count: Histogram<u64>,
    goal_duration_seconds: Histogram<u64>,
    /// Most-recent pending turn start; turns are sequential so one slot suffices.
    turn_started_at: Cell<Option<f64>>,
    first_token_seen: Cell<bool>,
    /// Terminal tool calls observed since the current turn started.
    turn_tool_call_count: Cell<u64>,
}

impl std::fmt::Debug for OtelObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelObserver")
            .field("turn_started_at", &self.turn_started_at.get())
            .field("first_token_seen", &self.first_token_seen.get())
            .finish_non_exhaustive()
    }
}

/// Build an [`OtelObserver`] backed by counters and histograms on the named
/// meter. Turn timing is tracked across the `turn.started` -> `turn.completed`
/// pair; because the engine runs turns sequentially, a single pending start
/// timestamp is sufficient (a `turn.failed` simply clears it).
pub fn create_otel_observer(opts: CreateOtelObserverOptions) -> OtelObserver {
    let now: Box<dyn Fn() -> f64 + Send + Sync> = match opts.now {
        Some(f) => f,
        None => Box::new(default_now),
    };
    let meter_name = opts.meter_name.unwrap_or_else(|| "klaus-agent".to_string());
    let meter = match opts.meter {
        Some(meter) => meter,
        None => global::meter_with_scope(InstrumentationScope::builder(meter_name).build()),
    };

    let turns = meter
        .u64_counter("agent.turns")
        .with_description("Count of completed agent turns.")
        .build();
    let tool_calls = meter
        .u64_counter("agent.tool_calls")
        .with_description("Count of terminal tool-call items, partitioned by `tool` and `success`.")
        .build();
    let errors = meter
        .u64_counter("agent.errors")
        .with_description("Count of failed agent turns.")
        .build();
    let tokens = meter
        .u64_counter("agent.tokens")
        .with_description("Total tokens consumed, partitioned by `type` (input|output).")
        .with_unit("token")
        .build();
    let turn_duration = meter
        .f64_histogram("agent.turn.duration_ms")
        .with_description("Wall-clock duration of a turn, from start to completion.")
        .with_unit("ms")
        .build();
    let cache_tokens = meter
        .u64_counter("agent.cache.read_tokens")
        .with_description("Input tokens served from the prompt cache (cache hits).")
        .with_unit("token")
        .build();
    let cache_creation_tokens = meter
        .u64_counter("agent.cache.creation_tokens")
        .with_description("Input tokens written into the prompt cache (cache writes/misses).")
        .with_unit("token")
        .build();
    let tool_failures = meter
        .u64_counter("agent.tool_failures")
        .with_description("Count of tool-call items that ended in failure, partitioned by `tool`.")
        .build();
    let compactions = meter
        .u64_counter("agent.compactions")
        .with_description(
            "Count of compaction events, partitioned by `mode` (micro|full) and `trigger` \
             (auto|manual).",
        )
        .build();
    let ttft = meter
        .f64_histogram("agent.ttft_ms")
        .with_description("Time to first streamed token within a turn.")
        .with_unit("ms")
        .build();
    let api_requests = meter
        .u64_counter("agent.api.requests")
        .with_description(
            "Model API stream requests started within a turn (includes retries and fallbacks).",
        )
        .build();
    let items = meter
        .u64_counter("agent.items")
        .with_description("Completed thread items, partitioned by `item_type`.")
        .build();
    let approvals = meter
        .u64_counter("agent.approvals")
        .with_description("Approval prompts surfaced to the host, partitioned by `kind`.")
        .build();
    let notifications = meter
        .u64_counter("agent.notifications")
        .with_description("Engine notifications, partitioned by `level`.")
        .build();
    let turn_token_usage = meter
        .u64_histogram("agent.turn.token_usage")
        .with_description(
            "Per-turn token usage, partitioned by `token_type` (total|input|cached_input|\
             non_cached_input|output|reasoning_output). `input` is the full submitted prompt \
             (uncached + cache reads + cache writes); `reasoning_output` reports 0 because the \
             provider layer folds thinking tokens into `output` (dimension kept for a \
             stable metric surface).",
        )
        .with_unit("token")
        .build();
    let turn_tool_calls = meter
        .u64_histogram("agent.turn.tool_calls")
        .with_description("Number of terminal tool calls within a single turn.")
        .build();
    let goal_created = meter
        .u64_counter("agent.goal.created")
        .with_description("Thread goals created (one per goal generation).")
        .build();
    let goal_resumed = meter
        .u64_counter("agent.goal.resumed")
        .with_description("Goals put back to `active` from a stop a resume can lift.")
        .build();
    let goal_blocked = meter
        .u64_counter("agent.goal.blocked")
        .with_description("Goals the model declared blocked.")
        .build();
    let goal_usage_limited = meter
        .u64_counter("agent.goal.usage_limited")
        .with_description("Goals stopped by a provider usage limit.")
        .build();
    let goal_budget_limited = meter
        .u64_counter("agent.goal.budget_limited")
        .with_description("Goals stopped by their own token budget.")
        .build();
    let goal_completed = meter
        .u64_counter("agent.goal.completed")
        .with_description("Goals the model declared complete.")
        .build();
    let goal_token_count = meter
        .u64_histogram("agent.goal.token_count")
        .with_description(
            "Tokens charged to a goal when it reached a terminal state, partitioned by `status`.",
        )
        .with_unit("token")
        .build();
    let goal_duration_seconds = meter
        .u64_histogram("agent.goal.duration_s")
        .with_description(
            "Seconds a goal spent active when it reached a terminal state, partitioned by \
             `status`.",
        )
        .with_unit("s")
        .build();

    // No in-process hit-RATE instrument: the cache hit rate is a RATIO derived at the
    // backend from `agent.cache.read_tokens / (agent.tokens{type=input} + read + creation)`
    // -- these three counters carry everything needed. (An ObservableGauge would be wrong
    // here: every per-session observer would add a callback to the one shared, name-keyed
    // async instrument with attribute-less observations, so the callbacks collide and
    // leak. The cost tracker's `cache_hit_rate()` supplies the same number in-process for
    // hosts.)

    OtelObserver {
        now,
        turns,
        tool_calls,
        errors,
        tokens,
        turn_duration,
        cache_tokens,
        cache_creation_tokens,
        tool_failures,
        compactions,
        ttft,
        api_requests,
        items,
        approvals,
        notifications,
        turn_token_usage,
        turn_tool_calls,
        goal_created,
        goal_resumed,
        goal_blocked,
        goal_usage_limited,
        goal_budget_limited,
        goal_completed,
        goal_token_count,
        goal_duration_seconds,
        turn_started_at: Cell::new(None),
        first_token_seen: Cell::new(false),
        turn_tool_call_count: Cell::new(0),
    }
}

impl OtelObserver {
    /// Fold one outward event into the instruments. Every event type not matched
    /// below is intentionally ignored.
    pub fn on_event(&self, e: &EngineEvent) {
        match e {
            EngineEvent::TurnStarted { .. } => {
                self.turn_started_at.set(Some((self.now)()));
                self.first_token_seen.set(false);
                self.turn_tool_call_count.set(0);
            }
            EngineEvent::StreamRequestStart { .. } => {
                // One per model API request within the turn -- retries and model fallbacks
                // each add one, so this exceeds turn count when the stream had to be re-issued.
                self.api_requests.add(1, &[]);
            }
            EngineEvent::TextDelta { .. } => {
                if !self.first_token_seen.get() {
                    if let Some(started) = self.turn_started_at.get() {
                        self.ttft.record((self.now)() - started, &[]);
                        self.first_token_seen.set(true);
                    }
                }
            }
            EngineEvent::TurnCompleted {
                usage, stop_reason, ..
            } => {
                self.turns.add(
                    1,
                    &[KeyValue::new(
                        "stop_reason",
                        terminal_reason_str(*stop_reason),
                    )],
                );
                if let Some(started) = self.turn_started_at.get() {
                    self.turn_duration.record((self.now)() - started, &[]);
                    self.turn_started_at.set(None);
                }
                self.turn_tool_calls
                    .record(self.turn_tool_call_count.get(), &[]);
                self.turn_tool_call_count.set(0);
                if let Some(usage) = usage {
                    self.tokens
                        .add(usage.input_tokens, &[KeyValue::new("type", "input")]);
                    self.tokens
                        .add(usage.output_tokens, &[KeyValue::new("type", "output")]);
                    if usage.cache_read_input_tokens > 0 {
                        self.cache_tokens.add(usage.cache_read_input_tokens, &[]);
                    }
                    if usage.cache_creation_input_tokens > 0 {
                        self.cache_creation_tokens
                            .add(usage.cache_creation_input_tokens, &[]);
                    }
                    // Per-turn token usage broken down along the declared
                    // `token_type` dimensions. The engine's `Usage` keeps the four provider
                    // fields DISJOINT (see `schemas::message::Usage`), so the
                    // full submitted prompt is their sum; `reasoning_output`
                    // is folded into `output` by the provider layer and
                    // recorded as 0 here for surface parity.
                    let cached_input = usage.cache_read_input_tokens;
                    let non_cached_input = usage.input_tokens + usage.cache_creation_input_tokens;
                    let input = non_cached_input + cached_input;
                    for (token_type, value) in [
                        ("total", tokens_from_usage(usage)),
                        ("input", input),
                        ("cached_input", cached_input),
                        ("non_cached_input", non_cached_input),
                        ("output", usage.output_tokens),
                        ("reasoning_output", 0),
                    ] {
                        self.turn_token_usage
                            .record(value, &[KeyValue::new("token_type", token_type)]);
                    }
                }
            }
            EngineEvent::TurnFailed { error, .. } => {
                // `error_code` is the telemetry tag spelling used here.
                self.errors.add(
                    1,
                    &[KeyValue::new(
                        "error_code",
                        engine_error_code_str(error.code),
                    )],
                );
                // A failed turn still ends the end-to-end timer (the e2e
                // duration is recorded on every turn teardown, success or
                // not) and flushes the per-turn tool-call count.
                if let Some(started) = self.turn_started_at.get() {
                    self.turn_duration.record((self.now)() - started, &[]);
                }
                self.turn_tool_calls
                    .record(self.turn_tool_call_count.get(), &[]);
                self.turn_tool_call_count.set(0);
                self.turn_started_at.set(None);
            }
            EngineEvent::ItemCompleted { item, .. } => {
                self.items
                    .add(1, &[KeyValue::new("item_type", item_type_str(item))]);
                if let ThreadItem::ToolCall {
                    tool_name, status, ..
                } = item
                {
                    self.record_terminal_tool_call(tool_name, *status != ItemStatus::Failed);
                }
            }
            EngineEvent::ItemUpdated {
                inference: _,
                item:
                    ThreadItem::ToolCall {
                        tool_name,
                        status: ItemStatus::Failed,
                        ..
                    },
            } => {
                self.record_terminal_tool_call(tool_name, false);
            }
            EngineEvent::ApprovalRequested { request } => {
                self.approvals
                    .add(1, &[KeyValue::new("kind", approval_kind_str(request.kind))]);
            }
            EngineEvent::Notification { level, .. } => {
                self.notifications
                    .add(1, &[KeyValue::new("level", notification_level_str(*level))]);
            }
            EngineEvent::Compaction { mode, .. } => {
                // Everything on the event stream was initiated by the turn
                // loop; host-initiated compaction reports through
                // [`OtelObserver::record_manual_compaction`].
                self.compactions.add(
                    1,
                    &[
                        KeyValue::new("mode", compaction_mode_str(*mode)),
                        KeyValue::new("trigger", "auto"),
                    ],
                );
            }
            _ => {
                // Every other event type is intentionally ignored.
            }
        }
    }

    /// Count a host-triggered (`Session::compact`) compaction. Manual
    /// compaction never flows through the turn event stream, so it reports
    /// here; it always summarizes the full history.
    pub fn record_manual_compaction(&self) {
        self.compactions.add(
            1,
            &[
                KeyValue::new("mode", compaction_mode_str(CompactionMode::Full)),
                KeyValue::new("trigger", "manual"),
            ],
        );
    }

    /// Fold one goal transition into the goal instruments.
    ///
    /// `previous` is the status of the snapshot before this one, and `None` for
    /// the first snapshot of a goal generation -- which is exactly what makes it
    /// a create. A snapshot that only moved the usage counters carries the same
    /// status and records nothing beyond that.
    pub fn record_goal_change(&self, previous: Option<ThreadGoalStatus>, goal: &ThreadGoal) {
        if previous.is_none() {
            self.goal_created.add(1, &[]);
        }
        if goal.status.is_active() && previous.is_some_and(ThreadGoalStatus::is_resumable) {
            self.goal_resumed.add(1, &[]);
        }
        if previous == Some(goal.status) {
            return;
        }
        // Terminal states carry the goal's final cost; `active`/`paused` are
        // waypoints, so they get no terminal record.
        let counter = match goal.status {
            ThreadGoalStatus::Blocked => &self.goal_blocked,
            ThreadGoalStatus::UsageLimited => &self.goal_usage_limited,
            ThreadGoalStatus::BudgetLimited => &self.goal_budget_limited,
            ThreadGoalStatus::Complete => &self.goal_completed,
            ThreadGoalStatus::Active | ThreadGoalStatus::Paused => return,
        };
        counter.add(1, &[]);
        let status = [KeyValue::new("status", goal.status.wire_name())];
        self.goal_token_count
            .record(goal.tokens_used.max(0) as u64, &status);
        self.goal_duration_seconds
            .record(goal.time_used_seconds.max(0) as u64, &status);
    }

    /// Fold one terminal tool call (success or failure) into the per-call
    /// counters and the per-turn tally.
    fn record_terminal_tool_call(&self, tool_name: &RuntimeString, success: bool) {
        let tool = String::from_utf8_lossy(&tool_name.to_node_utf8()).into_owned();
        self.tool_calls.add(
            1,
            &[
                KeyValue::new("tool", tool.clone()),
                KeyValue::new("success", if success { "true" } else { "false" }),
            ],
        );
        if !success {
            self.tool_failures.add(1, &[KeyValue::new("tool", tool)]);
        }
        self.turn_tool_call_count
            .set(self.turn_tool_call_count.get() + 1);
    }
}

/// Convenience accessor for a tracer on the global provider, for callers that
/// want to open their own spans. No-op until a `TracerProvider` is registered.
/// Passing `None` selects the default `klaus-agent` scope name.
pub fn get_tracer(name: Option<String>) -> BoxedTracer {
    global::tracer(name.unwrap_or_else(|| "klaus-agent".to_string()))
}

/// Default clock: whole milliseconds since the Unix epoch.
fn default_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn terminal_reason_str(reason: TerminalReason) -> &'static str {
    match reason {
        TerminalReason::Completed => "completed",
        TerminalReason::Aborted => "aborted",
        TerminalReason::PromptTooLong => "prompt_too_long",
        TerminalReason::AuthFailed => "auth_failed",
        TerminalReason::TurnLimitExhausted => "turn_limit_exhausted",
        TerminalReason::SpendLimitExhausted => "spend_limit_exhausted",
        TerminalReason::Error => "error",
    }
}

fn engine_error_code_str(code: EngineErrorCode) -> &'static str {
    match code {
        EngineErrorCode::AuthFailed => "auth_failed",
        EngineErrorCode::PromptTooLong => "prompt_too_long",
        EngineErrorCode::BudgetExhausted => "budget_exhausted",
        EngineErrorCode::ProviderError => "provider_error",
        EngineErrorCode::Aborted => "aborted",
        EngineErrorCode::InvalidInput => "invalid_input",
        EngineErrorCode::Internal => "internal",
        EngineErrorCode::CompactionThrash => "compaction_thrash",
        EngineErrorCode::LoopDetected => "loop_detected",
    }
}

fn item_type_str(item: &ThreadItem) -> &'static str {
    match item {
        ThreadItem::AgentMessage { .. } => "agent_message",
        ThreadItem::Reasoning { .. } => "reasoning",
        ThreadItem::CommandExecution { .. } => "command_execution",
        ThreadItem::FileChange { .. } => "file_change",
        ThreadItem::ToolCall { .. } => "tool_call",
        ThreadItem::Error { .. } => "error",
        ThreadItem::ProposedPlan { .. } => "proposed_plan",
    }
}

fn approval_kind_str(kind: ApprovalKind) -> &'static str {
    match kind {
        ApprovalKind::Tool => "tool",
        ApprovalKind::Exec => "exec",
        ApprovalKind::Patch => "patch",
        ApprovalKind::Network => "network",
    }
}

fn compaction_mode_str(mode: CompactionMode) -> &'static str {
    match mode {
        CompactionMode::Micro => "micro",
        CompactionMode::Full => "full",
    }
}

fn notification_level_str(level: NotificationLevel) -> &'static str {
    match level {
        NotificationLevel::Info => "info",
        NotificationLevel::Warn => "warn",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimeString};
    use crate::schemas::message::Usage;
    use crate::types::errors::EngineError;
    use crate::types::events::ItemError;
    use crate::types::permission::ApprovalRequest;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::{Histogram as HistogramData, Sum as SumData};
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn completed(
        input_tokens: u64,
        cache_read_input_tokens: u64,
        cache_creation_input_tokens: u64,
    ) -> EngineEvent {
        EngineEvent::TurnCompleted {
            turn_id: "t".into(),
            stop_reason: TerminalReason::Completed,
            usage: Some(Usage {
                input_tokens,
                output_tokens: 0,
                cache_read_input_tokens,
                cache_creation_input_tokens,
                total_tokens: None,
            }),
        }
    }

    // --- create_otel_observer: cache effectiveness instruments ---

    // Drives the turn lifecycle with cache read + creation tokens without
    // panicking. No MeterProvider is registered, so instruments are no-op; this
    // exercises the wiring, including the cache-creation counter on
    // turn.completed.
    #[test]
    fn drives_turn_lifecycle_with_cache_read_and_creation_tokens() {
        let obs = create_otel_observer(CreateOtelObserverOptions {
            now: Some(Box::new(|| 0.0)),
            ..Default::default()
        });
        let started = EngineEvent::TurnStarted {
            turn_id: "t".into(),
        };
        let delta = EngineEvent::TextDelta {
            inference: None,
            message_id: None,
            phase: None,
            text: "hi".into(),
        };
        obs.on_event(&started);
        obs.on_event(&delta);
        // First turn: prefix written (creation), nothing read.
        obs.on_event(&completed(5, 0, 95));
        // Later turns: prefix read back (hits).
        obs.on_event(&started);
        obs.on_event(&completed(5, 95, 0));
    }

    #[test]
    fn tolerates_turn_completed_with_no_usage() {
        let obs = create_otel_observer(CreateOtelObserverOptions {
            now: Some(Box::new(|| 0.0)),
            ..Default::default()
        });
        obs.on_event(&EngineEvent::TurnCompleted {
            turn_id: "t".into(),
            stop_reason: TerminalReason::Completed,
            usage: None,
        });
    }

    // --- create_otel_observer: widened event vocabulary ---

    // Handles api-request / item / approval / notification / failure events
    // without panicking.
    #[test]
    fn handles_widened_event_vocabulary() {
        let obs = create_otel_observer(CreateOtelObserverOptions {
            now: Some(Box::new(|| 0.0)),
            ..Default::default()
        });
        let events = vec![
            EngineEvent::TurnStarted {
                turn_id: "t".into(),
            },
            EngineEvent::StreamRequestStart { inference: None },
            EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::AgentMessage {
                    phase: None,
                    id: "a".into(),
                    text: "hi".into(),
                },
            },
            EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::Reasoning {
                    id: "r".into(),
                    text: "think".into(),
                },
            },
            EngineEvent::ApprovalRequested {
                request: ApprovalRequest {
                    id: "ap".into(),
                    kind: ApprovalKind::Exec,
                    tool_name: "bash".into(),
                    input: exact_json!({}),
                    message: "ok?".into(),
                    suggestions: None,
                },
            },
            EngineEvent::Notification {
                level: NotificationLevel::Warn,
                message: "model X unavailable; falling back to Y".into(),
            },
            EngineEvent::TurnFailed {
                inference: None,
                error: EngineError {
                    code: EngineErrorCode::ProviderError,
                    message: "boom".into(),
                    cause: None,
                    request_id: None,
                    provider_kind: None,
                    retry_after_ms: None,
                },
            },
        ];
        for e in &events {
            obs.on_event(e);
        }
        // The unconfigured manual-compaction path must be a no-op too.
        obs.record_manual_compaction();
    }

    // --- turn-level metric dimensions (in-memory exporter) ---

    /// An observer wired to a private in-memory meter provider so tests can
    /// assert on recorded data points without touching the global API.
    fn observer_with_exporter(
        clock: Arc<AtomicU64>,
    ) -> (OtelObserver, SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let reader =
            PeriodicReader::builder(exporter.clone(), opentelemetry_sdk::runtime::Tokio).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("otel-observer-test");
        let obs = create_otel_observer(CreateOtelObserverOptions {
            now: Some(Box::new(move || clock.load(Ordering::SeqCst) as f64)),
            meter: Some(meter),
            ..Default::default()
        });
        (obs, provider, exporter)
    }

    fn attrs_match(attributes: &[KeyValue], expected: &[(&str, &str)]) -> bool {
        expected.iter().all(|(k, v)| {
            attributes
                .iter()
                .any(|kv| kv.key.as_str() == *k && kv.value.as_str() == *v)
        })
    }

    fn sum_point(
        exporter: &InMemoryMetricExporter,
        name: &str,
        expected: &[(&str, &str)],
    ) -> Option<u64> {
        for rm in exporter.get_finished_metrics().unwrap() {
            for scope in &rm.scope_metrics {
                for metric in &scope.metrics {
                    if metric.name != name {
                        continue;
                    }
                    let sum = metric
                        .data
                        .as_any()
                        .downcast_ref::<SumData<u64>>()
                        .expect("counter aggregates as Sum<u64>");
                    for dp in &sum.data_points {
                        if attrs_match(&dp.attributes, expected) {
                            return Some(dp.value);
                        }
                    }
                }
            }
        }
        None
    }

    /// (count, sum) of the first histogram data point matching `expected`.
    fn histogram_point_u64(
        exporter: &InMemoryMetricExporter,
        name: &str,
        expected: &[(&str, &str)],
    ) -> Option<(u64, u64)> {
        for rm in exporter.get_finished_metrics().unwrap() {
            for scope in &rm.scope_metrics {
                for metric in &scope.metrics {
                    if metric.name != name {
                        continue;
                    }
                    let hist = metric
                        .data
                        .as_any()
                        .downcast_ref::<HistogramData<u64>>()
                        .expect("u64 histogram aggregation");
                    for dp in &hist.data_points {
                        if attrs_match(&dp.attributes, expected) {
                            return Some((dp.count, dp.sum));
                        }
                    }
                }
            }
        }
        None
    }

    fn histogram_point_f64(exporter: &InMemoryMetricExporter, name: &str) -> Option<(u64, f64)> {
        for rm in exporter.get_finished_metrics().unwrap() {
            for scope in &rm.scope_metrics {
                for metric in &scope.metrics {
                    if metric.name != name {
                        continue;
                    }
                    let hist = metric
                        .data
                        .as_any()
                        .downcast_ref::<HistogramData<f64>>()
                        .expect("f64 histogram aggregation");
                    if let Some(dp) = hist.data_points.first() {
                        return Some((dp.count, dp.sum));
                    }
                }
            }
        }
        None
    }

    fn tool_item(name: &str, status: ItemStatus, error: Option<&str>) -> ThreadItem {
        ThreadItem::ToolCall {
            id: "t1".into(),
            tool_name: name.into(),
            arguments: exact_json!({}),
            result: None,
            error: error.map(|message| ItemError {
                message: message.into(),
            }),
            status,
        }
    }

    // Records one histogram point per token dimension: `input` is the full
    // submitted prompt (disjoint fields summed), `total` trusts the summed
    // context, and `reasoning_output` stays 0 (folded into output upstream).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_turn_token_usage_dimensions() {
        let (obs, provider, exporter) = observer_with_exporter(Arc::new(AtomicU64::new(0)));
        obs.on_event(&EngineEvent::TurnStarted {
            turn_id: "t".into(),
        });
        obs.on_event(&EngineEvent::TurnCompleted {
            turn_id: "t".into(),
            stop_reason: TerminalReason::Completed,
            usage: Some(Usage {
                input_tokens: 100,
                output_tokens: 40,
                cache_read_input_tokens: 30,
                cache_creation_input_tokens: 20,
                total_tokens: None,
            }),
        });
        provider.force_flush().unwrap();
        let point = |token_type: &str| {
            histogram_point_u64(
                &exporter,
                "agent.turn.token_usage",
                &[("token_type", token_type)],
            )
        };
        assert_eq!(point("total"), Some((1, 190)));
        assert_eq!(point("input"), Some((1, 150)));
        assert_eq!(point("cached_input"), Some((1, 30)));
        assert_eq!(point("non_cached_input"), Some((1, 120)));
        assert_eq!(point("output"), Some((1, 40)));
        assert_eq!(point("reasoning_output"), Some((1, 0)));
    }

    // Terminal tool calls carry `tool` + `success` labels on the per-call
    // counter, failures also land in the failure counter, and the per-turn
    // tally flushes into the turn histogram at turn end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_tool_call_labels_and_per_turn_count() {
        let (obs, provider, exporter) = observer_with_exporter(Arc::new(AtomicU64::new(0)));
        obs.on_event(&EngineEvent::TurnStarted {
            turn_id: "t".into(),
        });
        obs.on_event(&EngineEvent::ItemCompleted {
            inference: None,
            item: tool_item("read", ItemStatus::Completed, None),
        });
        obs.on_event(&EngineEvent::ItemUpdated {
            inference: None,
            item: tool_item("edit", ItemStatus::Failed, Some("denied")),
        });
        obs.on_event(&EngineEvent::TurnCompleted {
            turn_id: "t".into(),
            stop_reason: TerminalReason::Completed,
            usage: None,
        });
        provider.force_flush().unwrap();
        assert_eq!(
            sum_point(
                &exporter,
                "agent.tool_calls",
                &[("tool", "read"), ("success", "true")],
            ),
            Some(1)
        );
        assert_eq!(
            sum_point(
                &exporter,
                "agent.tool_calls",
                &[("tool", "edit"), ("success", "false")],
            ),
            Some(1)
        );
        assert_eq!(
            sum_point(&exporter, "agent.tool_failures", &[("tool", "edit")]),
            Some(1)
        );
        assert_eq!(
            histogram_point_u64(&exporter, "agent.turn.tool_calls", &[]),
            Some((1, 2))
        );
    }

    // Streamed compaction events count as trigger=auto; the host-triggered
    // path counts as trigger=manual with mode=full.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_compaction_trigger_dimensions() {
        let (obs, provider, exporter) = observer_with_exporter(Arc::new(AtomicU64::new(0)));
        obs.on_event(&EngineEvent::Compaction {
            mode: CompactionMode::Micro,
            freed_tokens: 10,
        });
        obs.record_manual_compaction();
        provider.force_flush().unwrap();
        assert_eq!(
            sum_point(
                &exporter,
                "agent.compactions",
                &[("mode", "micro"), ("trigger", "auto")],
            ),
            Some(1)
        );
        assert_eq!(
            sum_point(
                &exporter,
                "agent.compactions",
                &[("mode", "full"), ("trigger", "manual")],
            ),
            Some(1)
        );
    }

    // The end-to-end timer records on failed turns too (reference semantics:
    // the turn timer flushes on every teardown), and the pending start is
    // consumed so a later turn cannot double-record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_e2e_duration_and_tool_count_for_failed_turns() {
        let clock = Arc::new(AtomicU64::new(0));
        let (obs, provider, exporter) = observer_with_exporter(clock.clone());
        obs.on_event(&EngineEvent::TurnStarted {
            turn_id: "t".into(),
        });
        obs.on_event(&EngineEvent::ItemUpdated {
            inference: None,
            item: tool_item("edit", ItemStatus::Failed, Some("boom")),
        });
        clock.store(250, Ordering::SeqCst);
        obs.on_event(&EngineEvent::TurnFailed {
            inference: None,
            error: EngineError {
                code: EngineErrorCode::ProviderError,
                message: "boom".into(),
                cause: None,
                request_id: None,
                provider_kind: None,
                retry_after_ms: None,
            },
        });
        provider.force_flush().unwrap();
        assert_eq!(
            histogram_point_f64(&exporter, "agent.turn.duration_ms"),
            Some((1, 250.0))
        );
        assert_eq!(
            histogram_point_u64(&exporter, "agent.turn.tool_calls", &[]),
            Some((1, 1))
        );
    }

    fn goal_at(status: ThreadGoalStatus, tokens: i64, seconds: i64) -> ThreadGoal {
        ThreadGoal {
            thread_id: "t1".into(),
            goal_id: "g1".into(),
            objective: "ship it".into(),
            status,
            token_budget: Some(1_000),
            tokens_used: tokens,
            time_used_seconds: seconds,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    // A goal's lifecycle: the first snapshot counts as a create, a usage-only
    // bump counts as nothing, lifting a stop counts as a resume, and the
    // terminal state carries the goal's final token and duration cost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_goal_lifecycle() {
        let (obs, provider, exporter) = observer_with_exporter(Arc::new(AtomicU64::new(0)));
        let created = goal_at(ThreadGoalStatus::Active, 10, 1);
        obs.record_goal_change(None, &created);
        obs.record_goal_change(
            Some(ThreadGoalStatus::Active),
            &goal_at(ThreadGoalStatus::Active, 40, 2),
        );
        obs.record_goal_change(
            Some(ThreadGoalStatus::Active),
            &goal_at(ThreadGoalStatus::Blocked, 60, 3),
        );
        obs.record_goal_change(
            Some(ThreadGoalStatus::Blocked),
            &goal_at(ThreadGoalStatus::Active, 60, 3),
        );
        obs.record_goal_change(
            Some(ThreadGoalStatus::Active),
            &goal_at(ThreadGoalStatus::Complete, 90, 7),
        );
        provider.force_flush().unwrap();
        assert_eq!(sum_point(&exporter, "agent.goal.created", &[]), Some(1));
        assert_eq!(sum_point(&exporter, "agent.goal.resumed", &[]), Some(1));
        assert_eq!(sum_point(&exporter, "agent.goal.blocked", &[]), Some(1));
        assert_eq!(sum_point(&exporter, "agent.goal.completed", &[]), Some(1));
        assert_eq!(
            histogram_point_u64(
                &exporter,
                "agent.goal.token_count",
                &[("status", "complete")]
            ),
            Some((1, 90))
        );
        assert_eq!(
            histogram_point_u64(&exporter, "agent.goal.duration_s", &[("status", "blocked")]),
            Some((1, 3))
        );
        // Paused is a waypoint, not a terminal state: no counter, no cost record.
        assert_eq!(sum_point(&exporter, "agent.goal.usage_limited", &[]), None);
        assert_eq!(
            histogram_point_u64(&exporter, "agent.goal.token_count", &[("status", "paused")]),
            None
        );
    }
}
