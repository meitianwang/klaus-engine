//! Registry + dispatcher for lifecycle hooks.

use crate::extensions::failure::{ExtensionFailure, ExtensionFailureKind, ExtensionFailureSink};
use crate::hooks::command_handler::run_command_hook;
use crate::hooks::types::{
    CombinedHookOutcome, Hook, HookDecision, HookEvent, HookHandler, HookInput, HookOutcome,
    HookResult, HookTrigger,
};
use crate::permission::rules::{parse_rule_string, rule_matches};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::RuntimeString;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use regex::Regex;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

/// A completed background (async) hook's deliverable context, handed to the host sink.
pub type AsyncHookSink = Arc<dyn Fn(HookEvent, HookOutcome, HookInput) + Send + Sync>;

/// Full report used by lifecycle boundaries (notably Setup) that must expose
/// hook failures and, for Claude-compatible init mode, force hooks configured
/// as asynchronous to execute synchronously.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HookDispatchReport {
    pub outcome: CombinedHookOutcome,
    pub matching_hooks: usize,
    pub failed_hooks: usize,
}

/// Background-hook time budget: a hook exceeding it is abandoned.
const ASYNC_HOOK_TIMEOUT_MS: u64 = 15_000;
/// Setup and pre-compaction commands get a ten-minute implicit budget. Preserve
/// explicit timeoutMs values exactly; ordinary hooks keep command_handler's
/// 30-second default.
const SETUP_COMMAND_HOOK_TIMEOUT_MS: u64 = 600_000;

fn command_hook_timeout(event: HookEvent, configured: Option<u64>) -> Option<u64> {
    configured.or_else(|| {
        matches!(event, HookEvent::Setup | HookEvent::PreCompact)
            .then_some(SETUP_COMMAND_HOOK_TIMEOUT_MS)
    })
}

/// A failed hook's own words if it gave any, otherwise the terminal result.
/// Never a fabricated diagnosis — the host shows this verbatim.
fn failure_message(outcome: &HookOutcome) -> RuntimeString {
    outcome
        .system_message
        .clone()
        .or_else(|| outcome.permission_reason.clone())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| {
            RuntimeString::from(if outcome.result == Some(HookResult::FailedAbort) {
                "hook aborted the turn"
            } else {
                "hook failed"
            })
        })
}

/// Free function so the background task can report without holding `&self`
/// (it only owns a clone of the sink handle).
fn report_hook_failure(
    sink: &Arc<Mutex<Option<ExtensionFailureSink>>>,
    event: HookEvent,
    message: RuntimeString,
) {
    let sink = sink.lock().unwrap().clone();
    let Some(sink) = sink else { return };
    // Contain a panicking sink: a broken hook report must not take the session
    // down on top of the broken hook.
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        sink(ExtensionFailure::new(
            ExtensionFailureKind::Hook,
            RuntimeString::from(event.wire_name()),
            message,
        ))
    }));
}

/// Registry + dispatcher for lifecycle hooks.
pub struct HookBus {
    process_context: RuntimeProcessContext,
    hooks: Mutex<Vec<Hook>>,
    async_sink: Arc<Mutex<Option<AsyncHookSink>>>,
    /// Reports a hook that did not run. A failing hook already degrades to
    /// `failed_continue` so the turn survives, which is correct behaviour and
    /// exactly why it is invisible: without this the user's hook can be broken
    /// for days and nothing ever says so.
    failure_sink: Arc<Mutex<Option<ExtensionFailureSink>>>,
    /// In-flight background (async) hook tasks, awaited/abandoned on `drain_pending`.
    pending: Arc<Mutex<HashMap<u64, Shared<BoxFuture<'static, ()>>>>>,
    next_pending_id: AtomicU64,
}

impl HookBus {
    pub fn new(process_context: RuntimeProcessContext, hooks: Vec<Hook>) -> Self {
        HookBus {
            process_context,
            hooks: Mutex::new(hooks),
            async_sink: Arc::new(Mutex::new(None)),
            failure_sink: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_pending_id: AtomicU64::new(0),
        }
    }

    pub fn register(&self, hook: Hook) {
        self.hooks.lock().unwrap().push(hook);
    }

    pub fn has(&self, event: HookEvent) -> bool {
        self.hooks.lock().unwrap().iter().any(|h| h.event == event)
    }

    /// Wire the sink that receives a background hook's outcome when it completes (Session-provided).
    pub fn set_async_sink(&self, sink: AsyncHookSink) {
        *self.async_sink.lock().unwrap() = Some(sink);
    }

    /// Wire the sink that receives a hook failure (Session-provided).
    pub fn set_failure_sink(&self, sink: ExtensionFailureSink) {
        *self.failure_sink.lock().unwrap() = Some(sink);
    }

    /// Report one failed hook. Both the sync and the background path funnel here
    /// so a hook cannot be silently broken on one path and reported on the other.
    fn report_failure(&self, event: HookEvent, outcome: &HookOutcome) {
        report_hook_failure(&self.failure_sink, event, failure_message(outcome));
    }

    /// Run all hooks matching `event` (and the optional tool matcher) and aggregate. Hooks marked
    /// `async` are dispatched fire-and-forget (not awaited, not aggregated); their outcome is handed
    /// to the async sink when they finish. The returned outcome reflects only the SYNCHRONOUS hooks.
    pub async fn dispatch(&self, event: HookEvent, input: &HookInput) -> CombinedHookOutcome {
        self.dispatch_inner(event, input, false).await.outcome
    }

    /// Dispatch every matching hook synchronously, including hooks whose
    /// configuration normally marks them as background work. This mode is used
    /// for Setup init so hook-produced context is available before
    /// SessionStart/the first prompt rather than leaking to a later turn.
    pub async fn dispatch_force_sync(
        &self,
        event: HookEvent,
        input: &HookInput,
    ) -> HookDispatchReport {
        self.dispatch_inner(event, input, true).await
    }

    async fn dispatch_inner(
        &self,
        event: HookEvent,
        input: &HookInput,
        force_sync: bool,
    ) -> HookDispatchReport {
        let matching: Vec<Hook> = self
            .hooks
            .lock()
            .unwrap()
            .iter()
            .filter(|h| h.event == event && matches(h, input))
            .cloned()
            .collect();
        let matching_hooks = matching.len();
        let mut combined = CombinedHookOutcome::default();
        let mut failed_hooks = 0;
        for hook in matching {
            if hook.is_async && !force_sync {
                self.run_async(event, &hook, input);
                continue;
            }
            let outcome = run_hook(&self.process_context, &hook, input).await;
            if matches!(
                outcome.result,
                Some(HookResult::FailedContinue | HookResult::FailedAbort)
            ) {
                failed_hooks += 1;
                self.report_failure(event, &outcome);
            }
            // This hook's own permission reason for the prompt/block message: a dedicated
            // permission_reason (a command hook's permissionDecisionReason), else system_message,
            // else additional_context (the channel a fn-hook may reuse). Kept separate so a
            // DIFFERENT hook's append-context can't masquerade as this hook's deny/ask reason.
            let own_reason = outcome
                .permission_reason
                .clone()
                .or_else(|| outcome.system_message.clone())
                .or_else(|| outcome.additional_context.clone());
            if outcome.decision == Some(HookDecision::Block) {
                combined.blocked = true;
                // First blocker wins (registration order); block is sticky and beats ask in the
                // consumer.
                if combined.block_reason.is_none() {
                    combined.block_reason = own_reason.clone();
                }
            }
            // approve and ask are LAST-WRITER-WINS relative to each other (a single permission
            // result is overwritten per hook), so each clears the other; block stays
            // sticky.
            if outcome.decision == Some(HookDecision::Ask) {
                combined.asked = true;
                combined.approved = false;
                if combined.ask_reason.is_none() {
                    combined.ask_reason = own_reason.clone();
                }
            }
            if outcome.decision == Some(HookDecision::Approve) {
                combined.approved = true;
                combined.asked = false;
            }
            if outcome.result == Some(HookResult::FailedAbort) {
                combined.aborted = true;
            }
            if let Some(updated_input) = outcome.updated_input {
                combined.updated_input = Some(updated_input);
            }
            if let Some(updated_tool_output) = outcome.updated_tool_output {
                combined.updated_tool_output = Some(updated_tool_output);
            }
            if let Some(system_message) = outcome.system_message {
                if !system_message.is_empty() {
                    combined.system_messages.push(system_message);
                }
            }
            if let Some(additional_context) = outcome.additional_context {
                if !additional_context.is_empty() {
                    combined.additional_context.push(additional_context);
                }
            }
            if let Some(worktree_path) = outcome.worktree_path {
                combined.worktree_path = Some(worktree_path);
            }
        }
        HookDispatchReport {
            outcome: combined,
            matching_hooks,
            failed_hooks,
        }
    }

    /// Await all in-flight background hooks, capped at `timeout_ms`. `None`
    /// uses the default background budget.
    pub async fn drain_pending(&self, timeout_ms: Option<u64>) {
        let _ = self.drain_pending_checked(timeout_ms).await;
    }

    /// Checked shutdown form: a timed-out asynchronous hook means the hook
    /// phase did not actually drain and must make cleanup observably partial.
    pub async fn drain_pending_checked(&self, timeout_ms: Option<u64>) -> Result<(), String> {
        let timeout_ms = timeout_ms.unwrap_or(ASYNC_HOOK_TIMEOUT_MS);
        let snapshot: Vec<Shared<BoxFuture<'static, ()>>> =
            self.pending.lock().unwrap().values().cloned().collect();
        if snapshot.is_empty() {
            return Ok(());
        }
        let all = futures::future::join_all(snapshot);
        tokio::time::timeout(Duration::from_millis(timeout_ms), all)
            .await
            .map(|_| ())
            .map_err(|_| format!("background hooks timed out after {timeout_ms}ms"))
    }

    /// Fire a background hook: run it (with a timeout), deliver its outcome to the sink, untrack.
    fn run_async(&self, event: HookEvent, hook: &Hook, input: &HookInput) {
        let id = self.next_pending_id.fetch_add(1, Ordering::Relaxed);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let tracked: Shared<BoxFuture<'static, ()>> = done_rx.map(|_| ()).boxed().shared();
        self.pending.lock().unwrap().insert(id, tracked);
        let hook = hook.clone();
        let input = input.clone();
        let process_context = self.process_context.clone();
        let async_sink = Arc::clone(&self.async_sink);
        let failure_sink = Arc::clone(&self.failure_sink);
        let pending = Arc::clone(&self.pending);
        tokio::spawn(async move {
            // Contain any panic so this fire-and-forget task always untracks itself and can never
            // crash the session.
            let work = AssertUnwindSafe(async {
                // Resolve to the hook's outcome, or abandon it when it exceeds the budget.
                let outcome = tokio::time::timeout(
                    Duration::from_millis(ASYNC_HOOK_TIMEOUT_MS),
                    run_hook(&process_context, &hook, &input),
                )
                .await;
                // A hook that blew its budget failed as surely as one that exited
                // non-zero, and it is the case a user is least likely to notice,
                // so the timeout is reported instead of just being abandoned.
                let Ok(outcome) = outcome else {
                    report_hook_failure(
                        &failure_sink,
                        event,
                        RuntimeString::from(format!(
                            "hook timed out after {ASYNC_HOOK_TIMEOUT_MS}ms"
                        )),
                    );
                    return;
                };
                if matches!(
                    outcome.result,
                    Some(HookResult::FailedContinue | HookResult::FailedAbort)
                ) {
                    report_hook_failure(&failure_sink, event, failure_message(&outcome));
                }
                let sink = async_sink.lock().unwrap().clone();
                if let Some(sink) = sink {
                    // Contain a panicking sink so it can't surface from this fire-and-forget task
                    // (a background hook must never crash the session).
                    let _ =
                        std::panic::catch_unwind(AssertUnwindSafe(|| sink(event, outcome, input)));
                }
            })
            .catch_unwind();
            let _ = work.await;
            pending.lock().unwrap().remove(&id);
            let _ = done_tx.send(());
        });
    }
}

/// The pipe-delimited word-like literal list form of a matcher.
static LITERAL_LIST: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9A-Za-z_:|-]+$").unwrap());

/// The wire/string form of a trigger, for matcher comparison.
fn trigger_str(trigger: HookTrigger) -> &'static str {
    match trigger {
        HookTrigger::Init => "init",
        HookTrigger::Maintenance => "maintenance",
        HookTrigger::Compact => "compact",
        HookTrigger::Manual => "manual",
        HookTrigger::Auto => "auto",
    }
}

fn matches(hook: &Hook, input: &HookInput) -> bool {
    // Agent-lifecycle events match their query field, not a tool name:
    // SubagentStart/SubagentStop → agent_type, Setup → trigger.
    if input.event == HookEvent::SubagentStart || input.event == HookEvent::SubagentStop {
        return matches_runtime_query(hook.matcher.as_deref(), input.agent_type.as_ref());
    }
    if matches!(input.event, HookEvent::Setup | HookEvent::SessionStart) {
        return matches_query(hook.matcher.as_deref(), input.trigger.map(trigger_str));
    }
    // Compaction hooks match on their trigger (`manual` | `auto`), not a tool name.
    if matches!(input.event, HookEvent::PreCompact | HookEvent::PostCompact) {
        return matches_query(hook.matcher.as_deref(), input.trigger.map(trigger_str));
    }
    let matcher = match hook.matcher.as_deref() {
        None | Some("") | Some("*") => return true,
        Some(m) => m,
    };
    let tool_name = match input.tool_name.as_ref() {
        None => return true,
        Some(value) if value.is_empty() => return true,
        Some(t) => t,
    };
    let Some(rule) = parse_rule_string(matcher) else {
        // The matcher isn't the `Tool(arg prefix)` rule grammar → match it against the tool NAME
        // as a pipe-delimited literal list (`Bash|Edit`) or a regex
        // (`Edit.*`). Previously this fell through to `return true`, silently matching EVERY tool —
        // so a `Bash|Edit` or regex matcher fired the hook on all tools, and an unparseable matcher
        // (`[`) matched all too. That's a footgun for an auto-approve/block PreToolUse hook;
        // `matches_query` fails closed.
        return matches_runtime_query(Some(matcher), Some(tool_name));
    };
    let command = input
        .tool_input
        .as_ref()
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("command"))
        .and_then(|c| c.as_str());
    // Permission-rule parsing is a scalar configuration boundary. Runtime values remain
    // authoritative; a non-scalar tool name/command cannot equal a configured scalar rule and
    // therefore fails closed instead of being replacement-decoded into a colliding value.
    let command = match command.map(RuntimeString::try_to_utf8).transpose() {
        Ok(command) => command,
        Err(_) => return false,
    };
    rule_matches(&rule, tool_name, command.as_deref())
}

/// Match a scalar hook configuration against an exact runtime query. Literal comparison stays in
/// UTF-16. Regex matching is an explicit scalar-only configuration boundary and fails closed for a
/// runtime value containing an unpaired surrogate.
fn matches_runtime_query(matcher: Option<&str>, value: Option<&RuntimeString>) -> bool {
    let matcher = match matcher {
        None | Some("") | Some("*") => return true,
        Some(matcher) => matcher,
    };
    let Some(value) = value else {
        return false;
    };
    if LITERAL_LIST.is_match(matcher) {
        return matcher
            .split('|')
            .map(str::trim)
            .filter(|candidate| !candidate.is_empty())
            .any(|candidate| value == candidate);
    }
    let Ok(value) = value.try_to_utf8() else {
        return false;
    };
    match fancy_regex::Regex::new(matcher) {
        Ok(regex) => regex.is_match(&value).unwrap_or(false),
        Err(_) => false,
    }
}

/// Match a hook matcher against a query value: an empty/`*`
/// matcher matches anything; a pipe-delimited list of word-like literals matches
/// exact membership; otherwise the matcher is treated as a regular expression.
fn matches_query(matcher: Option<&str>, value: Option<&str>) -> bool {
    let matcher = match matcher {
        None | Some("") | Some("*") => return true,
        Some(m) => m,
    };
    let value = match value {
        None => return false,
        Some(v) => v,
    };
    if LITERAL_LIST.is_match(matcher) {
        return matcher
            .split('|')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .any(|s| s == value);
    }
    match fancy_regex::Regex::new(matcher) {
        Ok(re) => re.is_match(value).unwrap_or(false),
        // A malformed regex matches nothing (fail closed).
        Err(_) => false,
    }
}

async fn run_hook(
    process_context: &RuntimeProcessContext,
    hook: &Hook,
    input: &HookInput,
) -> HookOutcome {
    match &hook.handler {
        HookHandler::Fn(f) => {
            // A panicking fn-hook degrades to failed_continue rather than tearing down the
            // dispatch.
            match AssertUnwindSafe(f.run(input.clone())).catch_unwind().await {
                Ok(outcome) => outcome,
                Err(_) => HookOutcome {
                    result: Some(HookResult::FailedContinue),
                    ..HookOutcome::default()
                },
            }
        }
        HookHandler::Command {
            command,
            timeout_ms,
        } => {
            run_command_hook(
                process_context,
                command,
                input,
                command_hook_timeout(input.event, *timeout_ms),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::types::HookFn;
    use crate::runtime_values::{runtime_json, RuntimeJson as Json};
    use async_trait::async_trait;
    use tokio::sync::Notify;

    fn exact_json(source: &str) -> Json {
        runtime_json::parse(&RuntimeString::from(source)).expect("valid exact runtime JSON")
    }

    fn test_bus(hooks: Vec<Hook>) -> HookBus {
        HookBus::new(
            crate::runtime_process::test_runtime_process_context(),
            hooks,
        )
    }

    fn base_input(event: HookEvent) -> HookInput {
        HookInput {
            event,
            cwd: RuntimeString::from("/tmp"),
            session_id: RuntimeString::from("s1"),
            tool_name: None,
            tool_input: None,
            tool_result: None,
            error: None,
            is_interrupt: None,
            prompt: None,
            message: None,
            agent_type: None,
            agent_id: None,
            trigger: None,
        }
    }

    struct ClosureHook<F>(F);

    #[async_trait]
    impl<F> HookFn for ClosureHook<F>
    where
        F: Fn(HookInput) -> HookOutcome + Send + Sync,
    {
        async fn run(&self, input: HookInput) -> HookOutcome {
            (self.0)(input)
        }
    }

    fn fn_handler(f: impl Fn(HookInput) -> HookOutcome + Send + Sync + 'static) -> HookHandler {
        HookHandler::Fn(Arc::new(ClosureHook(f)))
    }

    fn fn_hook(
        event: HookEvent,
        f: impl Fn(HookInput) -> HookOutcome + Send + Sync + 'static,
    ) -> Hook {
        Hook {
            event,
            matcher: None,
            handler: fn_handler(f),
            is_async: false,
        }
    }

    fn decision_outcome(decision: HookDecision) -> HookOutcome {
        HookOutcome {
            decision: Some(decision),
            ..HookOutcome::default()
        }
    }

    /// A failing hook degrades the turn instead of breaking it, which is correct
    /// and is exactly why it used to be invisible. These are the reports that
    /// make a broken hook say so.
    #[tokio::test]
    async fn failed_hooks_are_reported_once_each_with_their_own_words() {
        let seen = Arc::new(Mutex::new(Vec::<ExtensionFailure>::new()));
        let sink_log = Arc::clone(&seen);

        let bus = test_bus(vec![
            fn_hook(HookEvent::PreToolUse, |_| HookOutcome {
                result: Some(HookResult::FailedContinue),
                system_message: Some(RuntimeString::from("lint script missing")),
                ..HookOutcome::default()
            }),
            // A second failing hook on the same event: each failure is its own
            // report, because each is a separate thing the user has to fix.
            fn_hook(HookEvent::PreToolUse, |_| HookOutcome {
                result: Some(HookResult::FailedAbort),
                ..HookOutcome::default()
            }),
            // A hook that succeeded must not be reported.
            fn_hook(HookEvent::PreToolUse, |_| HookOutcome::default()),
        ]);
        bus.set_failure_sink(Arc::new(move |failure| {
            sink_log.lock().unwrap().push(failure);
        }));

        bus.dispatch(HookEvent::PreToolUse, &base_input(HookEvent::PreToolUse))
            .await;

        let failures = seen.lock().unwrap().clone();
        assert_eq!(
            failures.len(),
            2,
            "one report per failed hook, none for the healthy one"
        );
        assert!(failures
            .iter()
            .all(|f| f.kind == ExtensionFailureKind::Hook));
        assert!(
            failures
                .iter()
                .all(|f| f.id.try_to_utf8().unwrap() == "PreToolUse"),
            "the id is the hook event name the user wrote in their config"
        );
        // The hook's own message is passed through verbatim; the one that gave no
        // message falls back to its terminal result rather than inventing a cause.
        assert_eq!(
            failures[0].message.try_to_utf8().unwrap(),
            "lint script missing"
        );
        assert_eq!(
            failures[1].message.try_to_utf8().unwrap(),
            "hook aborted the turn"
        );
    }

    /// With no sink wired the bus must behave exactly as before: reporting is an
    /// addition, not a new failure mode.
    #[tokio::test]
    async fn a_missing_failure_sink_changes_nothing() {
        let bus = test_bus(vec![fn_hook(HookEvent::PreToolUse, |_| HookOutcome {
            result: Some(HookResult::FailedAbort),
            ..HookOutcome::default()
        })]);
        let outcome = bus
            .dispatch(HookEvent::PreToolUse, &base_input(HookEvent::PreToolUse))
            .await;
        assert!(outcome.aborted, "the abort still reaches the caller");
    }

    #[test]
    fn setup_command_hooks_default_to_claude_ten_minute_budget() {
        assert_eq!(command_hook_timeout(HookEvent::Setup, None), Some(600_000));
        assert_eq!(
            command_hook_timeout(HookEvent::Setup, Some(31_000)),
            Some(31_000),
            "an explicit Setup timeoutMs must remain authoritative"
        );
        assert_eq!(
            command_hook_timeout(HookEvent::PreToolUse, None),
            None,
            "non-Setup commands must retain command_handler's 30s default"
        );
    }

    // ---- dispatch — decision collection ----

    #[tokio::test]
    async fn collects_approve_and_block_both_can_be_set_caller_resolves_precedence() {
        let approve = fn_hook(HookEvent::PreToolUse, |_| {
            decision_outcome(HookDecision::Approve)
        });
        let block = fn_hook(HookEvent::PreToolUse, |_| {
            decision_outcome(HookDecision::Block)
        });
        let bus = test_bus(vec![approve, block]);
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        input.tool_input = Some(exact_json(r#"{"path":"x"}"#));
        let out = bus.dispatch(HookEvent::PreToolUse, &input).await;
        assert!(out.approved);
        assert!(out.blocked);
    }

    #[tokio::test]
    async fn collects_an_ask_decision_and_its_reason_first_asker_wins() {
        let ask = fn_hook(HookEvent::PreToolUse, |_| HookOutcome {
            decision: Some(HookDecision::Ask),
            additional_context: Some(RuntimeString::from("why?")),
            ..HookOutcome::default()
        });
        let bus = test_bus(vec![ask]);
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        input.tool_input = Some(exact_json(r#"{"path":"x"}"#));
        let out = bus.dispatch(HookEvent::PreToolUse, &input).await;
        assert!(out.asked);
        assert_eq!(out.ask_reason.as_ref(), Some(&RuntimeString::from("why?")));
    }

    #[tokio::test]
    async fn approve_and_ask_are_last_writer_wins_relative_to_each_other() {
        let make_ask = || {
            fn_hook(HookEvent::PreToolUse, |_| {
                decision_outcome(HookDecision::Ask)
            })
        };
        let make_approve = || {
            fn_hook(HookEvent::PreToolUse, |_| {
                decision_outcome(HookDecision::Approve)
            })
        };
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        // [ask, approve] → approve is last → approved wins, asked cleared.
        let a = test_bus(vec![make_ask(), make_approve()])
            .dispatch(HookEvent::PreToolUse, &input)
            .await;
        assert!(a.approved);
        assert!(!a.asked);
        // [approve, ask] → ask is last → asked wins, approved cleared.
        let b = test_bus(vec![make_approve(), make_ask()])
            .dispatch(HookEvent::PreToolUse, &input)
            .await;
        assert!(b.asked);
        assert!(!b.approved);
    }

    #[tokio::test]
    async fn block_and_ask_can_both_be_set_consumer_resolves_block_first() {
        let ask = fn_hook(HookEvent::PreToolUse, |_| {
            decision_outcome(HookDecision::Ask)
        });
        let block = fn_hook(HookEvent::PreToolUse, |_| {
            decision_outcome(HookDecision::Block)
        });
        let bus = test_bus(vec![ask, block]);
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        let out = bus.dispatch(HookEvent::PreToolUse, &input).await;
        assert!(out.asked);
        assert!(out.blocked);
    }

    #[tokio::test]
    async fn updated_tool_output_is_last_writer_wins() {
        let first = fn_hook(HookEvent::PostToolUse, |_| HookOutcome {
            updated_tool_output: Some(exact_json(r#""one""#)),
            ..HookOutcome::default()
        });
        let second = fn_hook(HookEvent::PostToolUse, |_| HookOutcome {
            updated_tool_output: Some(exact_json(r#""two""#)),
            ..HookOutcome::default()
        });
        let bus = test_bus(vec![first, second]);
        let mut input = base_input(HookEvent::PostToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        let out = bus.dispatch(HookEvent::PostToolUse, &input).await;
        assert_eq!(out.updated_tool_output, Some(exact_json(r#""two""#)));
    }

    #[tokio::test]
    async fn failed_abort_sets_aborted() {
        let abort = fn_hook(HookEvent::PreToolUse, |_| HookOutcome {
            result: Some(HookResult::FailedAbort),
            ..HookOutcome::default()
        });
        let bus = test_bus(vec![abort]);
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from("write"));
        let out = bus.dispatch(HookEvent::PreToolUse, &input).await;
        assert!(out.aborted);
    }

    // ---- PreCompact trigger matcher ----

    fn make_pre_compact(matcher: &str) -> Hook {
        Hook {
            event: HookEvent::PreCompact,
            matcher: Some(matcher.to_string()),
            handler: fn_handler(|_| HookOutcome {
                system_message: Some(RuntimeString::from("fired")),
                ..HookOutcome::default()
            }),
            is_async: false,
        }
    }

    #[tokio::test]
    async fn a_matcher_of_auto_matches_the_auto_trigger_but_not_manual() {
        let bus = test_bus(vec![make_pre_compact("auto")]);
        let mut auto_input = base_input(HookEvent::PreCompact);
        auto_input.trigger = Some(HookTrigger::Auto);
        let mut manual_input = base_input(HookEvent::PreCompact);
        manual_input.trigger = Some(HookTrigger::Manual);
        let auto_out = bus.dispatch(HookEvent::PreCompact, &auto_input).await;
        let manual_out = bus.dispatch(HookEvent::PreCompact, &manual_input).await;
        assert!(auto_out
            .system_messages
            .contains(&RuntimeString::from("fired")));
        assert!(!manual_out
            .system_messages
            .contains(&RuntimeString::from("fired")));
    }

    #[tokio::test]
    async fn a_wildcard_matcher_matches_any_trigger() {
        let bus = test_bus(vec![make_pre_compact("*")]);
        let mut input = base_input(HookEvent::PreCompact);
        input.trigger = Some(HookTrigger::Auto);
        let out = bus.dispatch(HookEvent::PreCompact, &input).await;
        assert!(out.system_messages.contains(&RuntimeString::from("fired")));
    }

    // ---- async (background) hooks ----

    struct GatedHook {
        gate: Arc<Notify>,
    }

    #[async_trait]
    impl HookFn for GatedHook {
        async fn run(&self, _input: HookInput) -> HookOutcome {
            self.gate.notified().await;
            HookOutcome {
                additional_context: Some(RuntimeString::from("BACKGROUND RESULT")),
                ..HookOutcome::default()
            }
        }
    }

    #[tokio::test]
    async fn does_not_block_dispatch_and_delivers_the_outcome_to_the_sink_on_completion() {
        let gate = Arc::new(Notify::new());
        let async_hook = Hook {
            event: HookEvent::PostToolUse,
            matcher: None,
            handler: HookHandler::Fn(Arc::new(GatedHook {
                gate: Arc::clone(&gate),
            })),
            is_async: true,
        };
        let delivered: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let bus = test_bus(vec![async_hook]);
        let sink_delivered = Arc::clone(&delivered);
        bus.set_async_sink(Arc::new(move |_event, outcome, _input| {
            if let Some(additional_context) = outcome.additional_context {
                sink_delivered.lock().unwrap().push(additional_context);
            }
        }));

        // dispatch returns immediately — the async hook is NOT awaited and NOT aggregated.
        let mut input = base_input(HookEvent::PostToolUse);
        input.tool_name = Some(RuntimeString::from("w"));
        let out = bus.dispatch(HookEvent::PostToolUse, &input).await;
        assert!(out.additional_context.is_empty());
        assert!(delivered.lock().unwrap().is_empty());

        // Once the hook completes, the sink receives its context.
        gate.notify_one();
        bus.drain_pending(None).await;
        assert_eq!(
            *delivered.lock().unwrap(),
            vec![RuntimeString::from("BACKGROUND RESULT")]
        );
    }

    #[tokio::test]
    async fn hands_the_sink_the_dispatch_input_so_the_host_can_scope_by_agent_id() {
        let bg = Hook {
            event: HookEvent::PostToolUse,
            matcher: None,
            handler: fn_handler(|_| HookOutcome {
                additional_context: Some(RuntimeString::from("X")),
                ..HookOutcome::default()
            }),
            is_async: true,
        };
        let seen_agent_id: Arc<Mutex<Option<Option<RuntimeString>>>> = Arc::new(Mutex::new(None));
        let bus = test_bus(vec![bg]);
        let sink_seen = Arc::clone(&seen_agent_id);
        bus.set_async_sink(Arc::new(move |_event, _outcome, input| {
            *sink_seen.lock().unwrap() = Some(input.agent_id);
        }));
        let mut input = base_input(HookEvent::PostToolUse);
        input.tool_name = Some(RuntimeString::from("w"));
        input.agent_id = Some(RuntimeString::from("sub-7"));
        bus.dispatch(HookEvent::PostToolUse, &input).await;
        bus.drain_pending(None).await;
        assert_eq!(
            *seen_agent_id.lock().unwrap(),
            Some(Some(RuntimeString::from("sub-7")))
        );
    }

    #[tokio::test]
    async fn runs_sync_and_async_hooks_of_the_same_event_together() {
        let sync = fn_hook(HookEvent::PostToolUse, |_| HookOutcome {
            additional_context: Some(RuntimeString::from("SYNC")),
            ..HookOutcome::default()
        });
        let bg = Hook {
            event: HookEvent::PostToolUse,
            matcher: None,
            handler: fn_handler(|_| HookOutcome {
                additional_context: Some(RuntimeString::from("ASYNC")),
                ..HookOutcome::default()
            }),
            is_async: true,
        };
        let delivered: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
        let bus = test_bus(vec![sync, bg]);
        let sink_delivered = Arc::clone(&delivered);
        bus.set_async_sink(Arc::new(move |_event, outcome, _input| {
            if let Some(additional_context) = outcome.additional_context {
                sink_delivered.lock().unwrap().push(additional_context);
            }
        }));
        let mut input = base_input(HookEvent::PostToolUse);
        input.tool_name = Some(RuntimeString::from("w"));
        let out = bus.dispatch(HookEvent::PostToolUse, &input).await;
        assert_eq!(out.additional_context, vec![RuntimeString::from("SYNC")]);
        bus.drain_pending(None).await;
        assert_eq!(
            *delivered.lock().unwrap(),
            vec![RuntimeString::from("ASYNC")]
        );
    }

    #[tokio::test]
    async fn setup_force_sync_waits_for_and_aggregates_an_async_hook() {
        let hook = Hook {
            event: HookEvent::Setup,
            matcher: Some("init".to_string()),
            handler: fn_handler(|_| HookOutcome {
                additional_context: Some(RuntimeString::from("INIT CONTEXT")),
                ..HookOutcome::default()
            }),
            is_async: true,
        };
        let bus = test_bus(vec![hook]);
        let mut input = base_input(HookEvent::Setup);
        input.trigger = Some(HookTrigger::Init);

        let report = bus.dispatch_force_sync(HookEvent::Setup, &input).await;

        assert_eq!(report.matching_hooks, 1);
        assert_eq!(report.failed_hooks, 0);
        assert_eq!(
            report.outcome.additional_context,
            vec![RuntimeString::from("INIT CONTEXT")]
        );
        assert!(bus.pending.lock().unwrap().is_empty());
    }

    // ---- tool-name matcher ----

    fn block_on(matcher: &str) -> Hook {
        Hook {
            event: HookEvent::PreToolUse,
            matcher: Some(matcher.to_string()),
            handler: fn_handler(|_| decision_outcome(HookDecision::Block)),
            is_async: false,
        }
    }

    async fn fires(matcher: &str, tool_name: &str, tool_input: Json) -> bool {
        let bus = test_bus(vec![block_on(matcher)]);
        let mut input = base_input(HookEvent::PreToolUse);
        input.tool_name = Some(RuntimeString::from(tool_name));
        input.tool_input = Some(tool_input);
        bus.dispatch(HookEvent::PreToolUse, &input).await.blocked
    }

    #[tokio::test]
    async fn a_pipe_delimited_list_matches_exact_membership_not_everything() {
        assert!(fires("Bash|Edit", "Bash", exact_json("{}")).await);
        assert!(fires("Bash|Edit", "Edit", exact_json("{}")).await);
        // Regression: previously the rule parser returned nothing → `return true` → matched every
        // tool.
        assert!(!fires("Bash|Edit", "Read", exact_json("{}")).await);
    }

    #[tokio::test]
    async fn a_regex_matcher_matches_by_pattern() {
        assert!(fires("Edit.*", "EditNotebook", exact_json("{}")).await);
        assert!(fires("^Bash$", "Bash", exact_json("{}")).await);
        assert!(!fires("Edit.*", "Read", exact_json("{}")).await);
    }

    #[tokio::test]
    async fn an_unparseable_matcher_fails_closed_matches_nothing_not_everything() {
        // A malformed regex `[` previously fell through to `return true` (match-all footgun).
        assert!(!fires("[", "Bash", exact_json("{}")).await);
    }

    #[tokio::test]
    async fn still_honors_the_tool_arg_prefix_rule_grammar() {
        assert!(
            fires(
                "Bash(git)",
                "Bash",
                exact_json(r#"{"command":"git status"}"#)
            )
            .await
        );
        assert!(!fires("Bash(git)", "Bash", exact_json(r#"{"command":"ls"}"#)).await);
    }

    #[tokio::test]
    async fn star_and_empty_still_match_every_tool() {
        assert!(fires("*", "Anything", exact_json("{}")).await);
        assert!(fires("", "Anything", exact_json("{}")).await);
    }
}
