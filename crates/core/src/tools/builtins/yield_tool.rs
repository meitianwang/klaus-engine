//! `Yield` — the single wait primitive for background work.
//!
//! Waiting is not a reader side effect: `TaskOutput` never blocks, and this
//! tool is the only place a turn pauses for asynchronous work. The call ends
//! as soon as something can be reported (a background task finished, a message
//! or queued input arrived), at once when nothing is under way, when the wait
//! is interrupted, or shortly before the prompt-cache window would expire so a
//! long wait renews the cache instead of silently losing it.
//!
//! The result itself is a fixed header; the reason line is delivered as the
//! message right after the result (the background-notice rail), matching the
//! header's promise. One wait per assistant reply: a second `Yield` in the
//! same reply is refused so waiting cannot replace ending the turn.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::background_shells::BackgroundShellStatus;
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{BackgroundTaskStatus, Tool, ToolContext, ToolProgress, ToolResult};

/// Poll granularity while waiting. Wake sources are queue/registry state, so a
/// short poll keeps resume latency low without a dedicated event channel.
const YIELD_POLL_INTERVAL_MS: u64 = 250;

/// Hard wait budget: resume shortly before the five-minute prompt-cache window
/// would expire so an idle wait never silently drops the cache.
const YIELD_WAIT_BUDGET_MS: u64 = 270_000;

/// Fixed result header; the reason arrives as the line after this result.
const WAIT_ENDED_HEADER: &str = "The wait has ended. The line after this result says why.";

const REASON_NOTHING_RUNNING: &str =
    "none of your background work is running. If you have nothing left to do, end your turn.";
const REASON_SOMETHING_ARRIVED: &str = "something arrived; it follows this line.";
const REASON_MESSAGE_ARRIVED: &str = "a message arrived. If it follows this line, deal with it as you judge right, then end your turn rather than waiting again: a reply before another wait may not be shown to the person. If it doesn't follow, end your turn and it reaches you then.";
const REASON_INTERRUPTED: &str = "the wait was interrupted.";
const REASON_DUPLICATE_CALL: &str = "an earlier call to this tool in this reply already waited.";
const REASON_MESSAGE_WAITING: &str = "a message is waiting for you. If it doesn't follow this line, it reaches you once you end your turn.";

fn yield_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

const YIELD_DESCRIPTION: &str = "Pause until one of your asynchronous tools reports something. Call it only when you have no other work. You resume as soon as a report or a message arrives (shown after the line that follows this call's result, before your next reply, unless that line says it waits for the end of your turn), at once if none of your asynchronous tools has anything under way that could produce a report, or when a tool you are waiting on reaches its own time limit. Also resumes shortly before the prompt cache would expire, with a line saying how many tasks are still running; call it again to keep waiting.";

/// Elapsed-time prefix: whole seconds under two minutes, rounded minutes above.
fn resumed_prefix(elapsed: Duration) -> RuntimeString {
    let millis = elapsed.as_millis() as u64;
    if millis < 1_000 {
        return RuntimeString::from("Resumed at once");
    }
    let seconds = ((millis as f64) / 1000.0).round() as u64;
    if seconds < 120 {
        let unit = if seconds == 1 { "second" } else { "seconds" };
        return RuntimeString::from("Resumed after ")
            .concat(&RuntimeString::from_utf8(&seconds.to_string()))
            .concat(&RuntimeString::from(" "))
            .concat(&RuntimeString::from(unit));
    }
    let minutes = ((seconds as f64) / 60.0).round() as u64;
    RuntimeString::from("Resumed after ")
        .concat(&RuntimeString::from_utf8(&minutes.to_string()))
        .concat(&RuntimeString::from(" minutes"))
}

/// The unified wait primitive for background work.
pub struct YieldTool {
    input_schema: JsonSchema,
}

/// Construct the `Yield` tool as an erased registry-side [`Tool`].
pub fn create_yield_tool() -> Arc<dyn Tool> {
    Arc::new(YieldTool {
        input_schema: yield_schema(),
    })
}

/// Ids of all background work that could still produce a report: running
/// sub-agents plus running shell commands.
fn running_work(ctx: &ToolContext) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    if let Some(registry) = ctx.background_tasks.as_ref() {
        // Only this caller's own children: a wait must not wake on another
        // agent's descendants finishing.
        let caller = registry.caller_agent_id();
        for handle in registry.list_owned(caller.as_ref()) {
            if handle.status == BackgroundTaskStatus::Running {
                if let Ok(id) = handle.agent_id.try_to_utf8() {
                    ids.insert(id);
                }
            }
        }
    }
    for entry in ctx.background_processes.list() {
        if entry.status == BackgroundShellStatus::Running {
            if let Ok(id) = entry.id.try_to_utf8() {
                ids.insert(id);
            }
        }
    }
    ids
}

#[async_trait]
impl Tool for YieldTool {
    fn static_name(&self) -> &str {
        "Yield"
    }

    fn static_description(&self) -> &str {
        YIELD_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("wait for your background work")
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(200_000)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some(RuntimeString::from("Waiting for background work"))
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        // One wait per assistant reply: waiting must not replace ending the turn.
        let duplicate = ctx
            .reply_gate
            .as_ref()
            .is_some_and(|gate| !gate.first_yield_of_reply());
        if duplicate {
            return Ok(self.finish(ctx, Duration::ZERO, REASON_DUPLICATE_CALL, 0));
        }

        let in_subagent = ctx
            .background_tasks
            .as_ref()
            .and_then(|registry| registry.caller_agent_id())
            .is_some();

        let mut watched = running_work(ctx);
        if watched.is_empty() {
            return Ok(self.finish(ctx, Duration::ZERO, REASON_NOTHING_RUNNING, 0));
        }

        let started = Instant::now();
        let deadline = started + Duration::from_millis(YIELD_WAIT_BUDGET_MS);
        loop {
            if ctx.signal.is_cancelled() {
                return Ok(self.finish(ctx, started.elapsed(), REASON_INTERRUPTED, watched.len()));
            }

            // A finished task flipped out of the running set; its completion
            // notice rides the same queue and follows this result.
            let now_running = running_work(ctx);
            if watched.iter().any(|id| !now_running.contains(id)) {
                return Ok(self.finish(
                    ctx,
                    started.elapsed(),
                    REASON_SOMETHING_ARRIVED,
                    now_running.len(),
                ));
            }
            watched = now_running;

            // Queued steering input or mailbox messages wake the wait early.
            let queue_pending = ctx
                .background_tasks
                .as_ref()
                .is_some_and(|registry| registry.has_pending_input());
            let swarm_pending = match &ctx.swarm {
                Some(swarm) => swarm.has_pending_input().await.map_err(ToolError::new)?,
                None => false,
            };
            if queue_pending || swarm_pending {
                let reason = if in_subagent {
                    REASON_MESSAGE_WAITING
                } else {
                    REASON_MESSAGE_ARRIVED
                };
                return Ok(self.finish(ctx, started.elapsed(), reason, watched.len()));
            }

            if Instant::now() >= deadline {
                // Resume before the cache window closes; the count line tells
                // the model what is still under way.
                let count = watched.len();
                let reason = RuntimeString::from_utf8(&format!(
                    "the prompt cache would expire; {count} background task{} still running. Call Yield again to keep waiting.",
                    if count == 1 { "" } else { "s" }
                ));
                return Ok(self.finish_owned(ctx, started.elapsed(), reason, count));
            }

            tokio::time::sleep(Duration::from_millis(YIELD_POLL_INTERVAL_MS)).await;
        }
    }
}

impl YieldTool {
    /// Build the wait result: fixed header as the tool result, reason line as
    /// the message right after it (the notice rail). Without a notice channel
    /// the reason is appended to the result so it is never lost.
    fn finish(
        &self,
        ctx: &ToolContext,
        elapsed: Duration,
        reason: &'static str,
        still_running: usize,
    ) -> ToolResult {
        self.finish_owned(ctx, elapsed, RuntimeString::from(reason), still_running)
    }

    fn finish_owned(
        &self,
        ctx: &ToolContext,
        elapsed: Duration,
        reason: RuntimeString,
        still_running: usize,
    ) -> ToolResult {
        let line = resumed_prefix(elapsed)
            .concat(&RuntimeString::from(": "))
            .concat(&reason);
        let mut delivered = false;
        if let Some(enqueue) = &ctx.enqueue_background_notice {
            enqueue(line.clone());
            delivered = true;
        }
        let text = if delivered {
            RuntimeString::from(WAIT_ENDED_HEADER)
        } else {
            RuntimeString::from(WAIT_ENDED_HEADER)
                .concat(&RuntimeString::from("\n"))
                .concat(&line)
        };
        let mut result = RuntimeObject::new();
        result.insert("content", RuntimeJson::String(text.clone()));
        result.insert("waitedMs", RuntimeJson::from(elapsed.as_millis() as f64));
        result.insert("stillRunning", RuntimeJson::from(still_running as f64));
        text_result(RuntimeJson::Object(result), text, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{BackgroundTaskHandle, BackgroundTaskRegistry, ReplyScopedGate};
    use std::sync::Mutex;

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => {
                t.text.try_to_utf8().unwrap_or_default()
            }
            _ => panic!("expected a text block"),
        }
    }

    fn handle(id: &str, status: BackgroundTaskStatus) -> BackgroundTaskHandle {
        BackgroundTaskHandle {
            agent_id: RuntimeString::from(id),
            output_file: RuntimePath::from(format!("/tmp/klaus-yield-{id}.out")),
            status,
            description: None,
            partial: None,
        }
    }

    /// A registry whose running set the test can flip mid-wait.
    struct FlipRegistry {
        handles: Arc<Mutex<Vec<BackgroundTaskHandle>>>,
    }

    #[async_trait]
    impl BackgroundTaskRegistry for FlipRegistry {
        fn get(&self, agent_id: &RuntimeString) -> Option<BackgroundTaskHandle> {
            self.handles
                .lock()
                .unwrap()
                .iter()
                .find(|h| &h.agent_id == agent_id)
                .cloned()
        }
        fn list(&self) -> Vec<BackgroundTaskHandle> {
            self.handles.lock().unwrap().clone()
        }
        fn stop(&self, _agent_id: &RuntimeString) -> bool {
            false
        }
        async fn wait_for_idle(&self) {}
    }

    fn gated_ctx() -> ToolContext {
        let mut ctx = make_ctx("/tmp");
        ctx.reply_gate = Some(Arc::new(ReplyScopedGate::default()));
        ctx.reply_gate
            .as_ref()
            .unwrap()
            .begin_reply("reply-1".to_string());
        ctx
    }

    #[test]
    fn metadata_matches_the_wait_primitive_contract() {
        let tool = create_yield_tool();
        assert_eq!(tool.name(), "Yield");
        assert_eq!(
            tool.search_hint(),
            Some(RuntimeString::from("wait for your background work"))
        );
        assert!(tool.is_read_only(&runtime_json::parse(&RuntimeString::from("{}")).unwrap()));
        assert!(
            !tool.is_concurrency_safe(&runtime_json::parse(&RuntimeString::from("{}")).unwrap())
        );
        assert_eq!(tool.max_result_size_chars(), Some(200_000));
    }

    #[tokio::test]
    async fn resumes_at_once_when_nothing_is_running() {
        let ctx = gated_ctx();
        let res = create_yield_tool()
            .call(
                runtime_json::parse(&RuntimeString::from("{}")).unwrap(),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let text = text_of(&res);
        // No notice channel in the bare context: the reason rides the result.
        assert!(text.contains("Resumed at once"), "{text}");
        assert!(
            text.contains("none of your background work is running"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn second_wait_in_one_reply_is_refused_and_resets_with_the_reply() {
        let handles = Arc::new(Mutex::new(vec![handle(
            "agent-1",
            BackgroundTaskStatus::Running,
        )]));
        let mut ctx = gated_ctx();
        ctx.background_tasks = Some(Arc::new(FlipRegistry {
            handles: Arc::clone(&handles),
        }));
        // Flip the task to completed almost immediately so the first wait ends.
        let flip = Arc::clone(&handles);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            flip.lock().unwrap()[0].status = BackgroundTaskStatus::Completed;
        });
        let tool = create_yield_tool();
        let empty = runtime_json::parse(&RuntimeString::from("{}")).unwrap();
        let first = tool
            .call(empty.clone(), &ctx, None)
            .await
            .expect("first wait");
        let text = text_of(&first);
        assert!(
            text.contains("something arrived; it follows this line."),
            "{text}"
        );
        let second = tool
            .call(empty.clone(), &ctx, None)
            .await
            .expect("second wait");
        assert!(
            text_of(&second).contains("an earlier call to this tool in this reply already waited"),
            "{}",
            text_of(&second)
        );
        // A new assistant reply may wait again: nothing is running now.
        ctx.reply_gate
            .as_ref()
            .unwrap()
            .begin_reply("reply-2".to_string());
        let third = tool.call(empty, &ctx, None).await.expect("third wait");
        assert!(
            text_of(&third).contains("none of your background work is running"),
            "{}",
            text_of(&third)
        );
    }

    #[tokio::test]
    async fn interrupt_ends_the_wait_with_the_interrupted_line() {
        let handles = Arc::new(Mutex::new(vec![handle(
            "agent-1",
            BackgroundTaskStatus::Running,
        )]));
        let mut ctx = gated_ctx();
        ctx.background_tasks = Some(Arc::new(FlipRegistry { handles }));
        ctx.signal.cancel();
        let res = create_yield_tool()
            .call(
                runtime_json::parse(&RuntimeString::from("{}")).unwrap(),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        assert!(
            text_of(&res).contains("the wait was interrupted"),
            "{}",
            text_of(&res)
        );
    }

    #[test]
    fn elapsed_prefix_formats_whole_units() {
        assert_eq!(
            resumed_prefix(Duration::from_millis(0)),
            RuntimeString::from("Resumed at once")
        );
        assert_eq!(
            resumed_prefix(Duration::from_millis(999)),
            RuntimeString::from("Resumed at once")
        );
        assert_eq!(
            resumed_prefix(Duration::from_secs(1)),
            RuntimeString::from("Resumed after 1 second")
        );
        assert_eq!(
            resumed_prefix(Duration::from_secs(61)),
            RuntimeString::from("Resumed after 61 seconds")
        );
        assert_eq!(
            resumed_prefix(Duration::from_secs(150)),
            RuntimeString::from("Resumed after 3 minutes")
        );
    }
}
