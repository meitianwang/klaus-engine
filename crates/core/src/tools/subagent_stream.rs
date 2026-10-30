//! Real-time forwarding of a sub-agent's nested activity to its spawning tool — the
//! progress-forwarding contract for a spawned agent.
//!
//! This compact spawning-tool summary is separate from the session child-event
//! observer, which publishes the complete conversation stream. Summary forwarding
//! includes the child turn identity and completed tool items, plus
//! live `bash_progress` command output — each wrapped as `agent_progress`
//! attributed by `agentId`. Assistant text is forwarded ONLY when the
//! `forward_subagent_text` option is on. This contract runs over the
//! engine's event model.

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::Message;
use crate::types::errors::TerminalReason;
use crate::types::events::{
    EngineEvent, FileChangeKind, FileChangeStatus, ItemError, ItemStatus, ThreadItem,
    ToolDomainEvent,
};
use crate::types::tool::{ForegroundSubAgentRun, ToolContext};
use futures::StreamExt;
use std::collections::HashMap;

/// The clean result of draining a foreground sub-agent's stream: its final answer
/// plus any turn failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundSubAgentResult {
    pub final_text: RuntimeString,
    pub failure: Option<RuntimeString>,
}

pub(crate) fn terminal_failure(reason: TerminalReason) -> Option<RuntimeString> {
    match reason {
        TerminalReason::Completed => None,
        TerminalReason::Aborted => Some("Sub-agent execution was interrupted.".into()),
        TerminalReason::TurnLimitExhausted => {
            Some("Sub-agent reached its configured turn limit.".into())
        }
        TerminalReason::SpendLimitExhausted => {
            Some("Sub-agent reached the session spending limit.".into())
        }
        other => Some(format!("Sub-agent ended with {other:?}.").into()),
    }
}

pub(crate) fn background_terminal_failure(reason: TerminalReason) -> Option<RuntimeString> {
    // Background handles retain their planned-stop classification; the
    // authoritative lifecycle exposes the separate budget-limited outcome.
    if matches!(
        reason,
        TerminalReason::TurnLimitExhausted | TerminalReason::SpendLimitExhausted
    ) {
        None
    } else {
        terminal_failure(reason)
    }
}

/// Keep interrupted work inspectable without presenting it as a final answer.
pub(crate) fn incomplete_background_output(
    failure: &RuntimeString,
    partial: &RuntimeString,
) -> RuntimeString {
    let notice = RuntimeString::from("[failed] ").concat(failure);
    if partial.as_utf16().starts_with(notice.as_utf16()) {
        return partial.clone();
    }
    if partial.is_empty() {
        notice
    } else {
        notice
            .concat(&RuntimeString::from(
                "\n\nPartial activity; not a completed result:\n",
            ))
            .concat(partial)
    }
}

/// Cap for a background agent's rolling transcript buffer (256 KiB) — the default
/// bound when [`append_transcript`] is called without an explicit one.
const DEFAULT_TRANSCRIPT_CAP: usize = 256 * 1024;

/// The TERMINAL [`ThreadItem`] carried by an event, or `None` for a non-terminal
/// one — the engine's analog of "a completed `tool_use`/`tool_result` content
/// block".
///
/// A tool call that SUCCEEDS arrives as `item.completed`; one that FAILS (a
/// tool-reported error, or a deny/block/throw) arrives as
/// `item.updated{ status:'failed' }` — the engine emits failures as updates, not
/// completions. Error `tool_result` blocks are forwarded just like successful ones,
/// so BOTH are terminal items here. Non-terminal `item.started` /
/// `item.updated{ in_progress }` (filtered stream deltas) return `None`.
pub fn terminal_item(e: &EngineEvent) -> Option<&ThreadItem> {
    match e {
        EngineEvent::ItemCompleted { item, .. } => Some(item),
        EngineEvent::ItemUpdated { item, .. } => {
            // A tool-ish item (tool_call / command_execution / file_change) that
            // reports `failed` is terminal; anything else on an update is not.
            let failed_toolish = match item {
                ThreadItem::ToolCall { status, .. }
                | ThreadItem::CommandExecution { status, .. } => *status == ItemStatus::Failed,
                ThreadItem::FileChange { status, .. } => *status == FileChangeStatus::Failed,
                _ => false,
            };
            if failed_toolish {
                Some(item)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The forward predicate, mapped to engine events.
///
/// The engine folds `tool_use`+`tool_result` into one terminal item
/// (`tool_call` carries arguments+result, `command_execution` carries
/// command+output), so "forward the tool_use/tool_result blocks" becomes "forward
/// terminal tool-ish items" — successes AND failures (see [`terminal_item`]). The
/// child's live tool output (`bash_progress`) is its `tool.event`.
///
/// - `turn.started` establishes the child identity immediately for task tracking.
/// - `tool.event` → always forward (live tool output + already-wrapped grandchild
///   `agent_progress`).
/// - terminal tool-ish item → always forward; `agent_message` / `reasoning` only
///   when `forward_text` (under `forward_subagent_text` ON, non-tool blocks stop
///   being filtered too).
/// - everything else (`text.delta`, `thinking.delta`, non-terminal `item.*`,
///   terminal `turn.*`, …) → DROP (forwarding is from completed messages, never raw token
///   deltas).
pub fn should_forward_child_event(e: &EngineEvent, forward_text: bool) -> bool {
    if matches!(
        e,
        EngineEvent::ToolEvent { .. } | EngineEvent::TurnStarted { .. }
    ) {
        return true;
    }
    let item = match terminal_item(e) {
        Some(item) => item,
        None => return false,
    };
    if matches!(
        item,
        ThreadItem::AgentMessage { .. } | ThreadItem::Reasoning { .. }
    ) {
        return forward_text;
    }
    true
}

/// Drive a foreground sub-agent's event stream: forward its nested activity to the
/// host as `agent_progress` (attributed by the child's `agent_id`) while
/// extracting the clean final answer + any failure. Stops forwarding the moment
/// the turn is aborted. The
/// `forward_subagent_text` toggle comes from the spawning tool's context (a
/// caller-supplied option).
pub async fn consume_foreground_sub_agent(
    ctx: &ToolContext,
    run: ForegroundSubAgentRun,
) -> ForegroundSubAgentResult {
    let forward_text = ctx.forward_subagent_text.unwrap_or(false);
    let mut final_text = RuntimeString::default();
    let mut failure: Option<RuntimeString> = None;
    let mut terminal_seen = false;
    let ForegroundSubAgentRun {
        agent_id,
        mut events,
    } = run;
    loop {
        let e = tokio::select! {
            biased;
            _ = ctx.signal.cancelled() => {
                failure = terminal_failure(TerminalReason::Aborted);
                break;
            }
            event = events.next() => match event {
                Some(event) => event,
                None => break,
            },
        };
        let forward = should_forward_child_event(&e, forward_text);
        // Capture the clean final answer / failure off the same event before it is
        // forwarded (the forward moves it onward).
        match &e {
            EngineEvent::ItemCompleted {
                item: ThreadItem::AgentMessage { text, .. },
                ..
            } => final_text = text.clone(),
            EngineEvent::TurnFailed { error, .. } => {
                terminal_seen = true;
                failure = Some(if error.message.is_empty() {
                    "Sub-agent failed without an error message.".into()
                } else {
                    error.message.clone()
                });
            }
            EngineEvent::TurnCompleted { stop_reason, .. } => {
                terminal_seen = true;
                if let Some(reason) = terminal_failure(*stop_reason) {
                    failure = Some(reason);
                }
            }
            _ => {}
        }
        if forward {
            (ctx.emit)(ToolDomainEvent::AgentProgress {
                agent_id: agent_id.clone(),
                event: Box::new(e),
            })
            .await;
        }
    }
    if !terminal_seen && failure.is_none() {
        failure = Some("Sub-agent stream ended without a terminal event.".into());
    }
    ForegroundSubAgentResult {
        final_text,
        failure,
    }
}

/// Render one completed [`ThreadItem`] as a line of a background agent's running
/// transcript — the engine's analog of an incremental disk transcript (every
/// message/tool-use), which `task_output` returns while a background task is still
/// running. Concise but covers every item type, so the surface is the full
/// activity log, not a curated subset.
pub fn render_item_for_transcript(item: &ThreadItem) -> RuntimeString {
    match item {
        ThreadItem::AgentMessage { text, .. } => text.clone(),
        ThreadItem::Reasoning { text, .. } => prefixed("[thinking] ", text),
        ThreadItem::CommandExecution {
            command,
            aggregated_output,
            exit_code,
            ..
        } => {
            let mut rendered = prefixed("$ ", command);
            if let Some(code) = exit_code.filter(|code| *code != 0) {
                rendered = rendered.concat(&RuntimeString::from(format!(" (exit {code})")));
            }
            if !aggregated_output.is_empty() {
                rendered = rendered
                    .concat(&RuntimeString::from("\n"))
                    .concat(aggregated_output);
            }
            rendered
        }
        ThreadItem::FileChange { changes, .. } => {
            let rendered = changes
                .iter()
                .map(|c| prefixed(&format!("{} ", file_change_kind_str(c.kind)), &c.path))
                .collect::<Vec<_>>();
            prefixed("[edit] ", &join_runtime(rendered, ", "))
        }
        ThreadItem::ToolCall {
            tool_name, error, ..
        } => {
            let mut rendered = prefixed("[tool] ", tool_name);
            if let Some(error) = error {
                rendered = rendered
                    .concat(&RuntimeString::from(" — error: "))
                    .concat(&error.message);
            }
            rendered
        }
        ThreadItem::Error { message, .. } => prefixed("[error] ", message),
        ThreadItem::ProposedPlan { plan, .. } => plan.content.clone(),
    }
}

/// Append a completed item's transcript line to a background agent's rolling
/// `partial` buffer, bounded to the most recent `cap` code units (recent activity
/// matters most; a long-running teammate would otherwise grow the buffer without
/// bound — an on-disk transcript is effectively unbounded, but the engine keeps
/// this in memory). `cap` defaults to [`DEFAULT_TRANSCRIPT_CAP`] when omitted.
/// Returns the new buffer.
pub fn append_transcript(
    partial: &RuntimeString,
    item: &ThreadItem,
    cap: Option<usize>,
) -> RuntimeString {
    let rendered = render_item_for_transcript(item);
    cap_transcript_tail(push_transcript_line(partial, &rendered), cap)
}

/// Rebuild a background agent's rolling transcript buffer from its PERSISTED
/// sub-agent history.
///
/// A live task grows its buffer one terminal item at a time and never writes it
/// to disk — the clean final answer is written only on completion. So a task the
/// parent session lost to a restart has partial work on disk (its sidechain
/// transcript) and no output file. Projecting the saved history back through the
/// SAME renderer and the SAME tail bound is what lets `task_output` return that
/// work again instead of an empty string, without inventing a second transcript
/// format that can drift from the live one.
///
/// Item selection mirrors what the live buffer actually receives: every
/// completed `Reasoning` and `AgentMessage` (no phase filter — the live path
/// emits commentary too) and every terminal tool call, folded with its result
/// into one item the way the engine's `tool_call` carries arguments+result.
/// Within one assistant message the reasoning blocks come first, then the text,
/// matching the order the streaming path emits them.
///
/// Unresolved tool calls — a call whose result never landed because the process
/// died — are rendered as in-progress calls rather than dropped, so the buffer
/// ends where the work actually stopped.
pub fn project_persisted_transcript(messages: &[Message], cap: Option<usize>) -> RuntimeString {
    let mut buffer = RuntimeString::default();
    // tool_use id -> the call block, awaiting its result message.
    let mut open_calls: HashMap<RuntimeString, (RuntimeString, RuntimeJson)> = HashMap::new();
    for message in messages {
        match message {
            Message::Assistant(assistant) => {
                // Two passes, in the order the live stream publishes them.
                for (position, block) in assistant.content.iter().enumerate() {
                    if let ContentBlock::Thinking(thinking) = block {
                        let item = ThreadItem::Reasoning {
                            // Same identity the live path mints for a thinking
                            // block without its own message id.
                            id: thinking.message_id.clone().unwrap_or_else(|| {
                                RuntimeString::from(format!(
                                    "{}-{position}",
                                    assistant.uuid.try_to_utf8().unwrap_or_default()
                                ))
                            }),
                            text: thinking.text.clone(),
                        };
                        buffer = append_transcript(&buffer, &item, cap);
                    }
                }
                for block in &assistant.content {
                    match block {
                        ContentBlock::Text(text) => {
                            if text.text.is_empty() {
                                continue;
                            }
                            let item = ThreadItem::AgentMessage {
                                id: assistant.uuid.clone(),
                                text: text.text.clone(),
                                phase: text.phase.clone(),
                            };
                            buffer = append_transcript(&buffer, &item, cap);
                        }
                        ContentBlock::ToolUse(call) => {
                            open_calls
                                .insert(call.id.clone(), (call.name.clone(), call.input.clone()));
                        }
                        _ => {}
                    }
                }
            }
            Message::ToolResult(result) => {
                let Some((tool_name, arguments)) = open_calls.remove(&result.tool_use_id) else {
                    continue;
                };
                let output = output_blocks_text(&result.content);
                let item = ThreadItem::ToolCall {
                    id: result.tool_use_id.clone(),
                    tool_name,
                    arguments,
                    result: Some(RuntimeJson::String(output)),
                    // Same error text the live event carries, from the shared
                    // policy, so a recovered buffer matches byte for byte.
                    error: result.is_error.then(|| ItemError {
                        message: crate::engine::query::tool_result_error_detail(&result.content),
                    }),
                    status: if result.is_error {
                        ItemStatus::Failed
                    } else {
                        ItemStatus::Completed
                    },
                };
                buffer = append_transcript(&buffer, &item, cap);
            }
            _ => {}
        }
    }
    // Calls left open when the process died: the work stopped here, so the buffer
    // must say so rather than silently ending mid-sentence.
    for (id, (tool_name, arguments)) in open_calls {
        let item = ThreadItem::ToolCall {
            id,
            tool_name,
            arguments,
            result: None,
            error: None,
            status: ItemStatus::InProgress,
        };
        buffer = append_transcript(&buffer, &item, cap);
    }
    buffer
}

fn push_transcript_line(partial: &RuntimeString, rendered: &RuntimeString) -> RuntimeString {
    if partial.is_empty() {
        rendered.clone()
    } else {
        partial.concat(&RuntimeString::from("\n")).concat(rendered)
    }
}

/// Bound a transcript buffer to its most recent `cap` code units (recent activity
/// matters most; an on-disk transcript is effectively unbounded, but the engine
/// keeps this in memory). `cap` defaults to [`DEFAULT_TRANSCRIPT_CAP`].
fn cap_transcript_tail(buffer: RuntimeString, cap: Option<usize>) -> RuntimeString {
    let cap = cap.unwrap_or(DEFAULT_TRANSCRIPT_CAP);
    // Measured in the outward string's code units (UTF-16, the same measure the
    // transcript contract slices on).
    if buffer.len_utf16() > cap {
        buffer.slice_utf16(buffer.len_utf16() - cap..)
    } else {
        buffer
    }
}

/// Flatten a tool result's output blocks to the text the live transcript shows.
fn output_blocks_text(blocks: &[OutputBlock]) -> RuntimeString {
    let parts: Vec<RuntimeString> = blocks
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) if !text.text.is_empty() => Some(text.text.clone()),
            OutputBlock::Image(_) => Some(RuntimeString::from("[image]")),
            _ => None,
        })
        .collect();
    join_runtime(parts, "\n")
}

fn prefixed(prefix: &str, value: &RuntimeString) -> RuntimeString {
    RuntimeString::from(prefix).concat(value)
}

fn join_runtime(values: Vec<RuntimeString>, separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut values = values.into_iter();
    let mut joined = values.next().unwrap_or_default();
    for value in values {
        joined = joined.concat(&separator).concat(&value);
    }
    joined
}

/// The lowercase wire form of a file-change kind, as it appears in a transcript.
fn file_change_kind_str(kind: FileChangeKind) -> &'static str {
    match kind {
        FileChangeKind::Create => "create",
        FileChangeKind::Modify => "modify",
        FileChangeKind::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimePath};
    use crate::types::abort::AbortSignal;
    use crate::types::errors::{EngineError, EngineErrorCode, TerminalReason};
    use crate::types::events::{FileChange, ItemError};
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use crate::types::tool::EmitFn;
    use std::sync::{Arc, Mutex};

    // ---- event fixtures (fresh instances; the outward types are neither Clone
    // nor PartialEq, so tests compare by structural JSON) ----

    fn tool_call() -> EngineEvent {
        EngineEvent::ItemCompleted {
            inference: None,
            item: ThreadItem::ToolCall {
                id: "t1".into(),
                tool_name: "read".into(),
                arguments: RuntimeJson::Object(RuntimeObject::new()),
                result: None,
                error: None,
                status: ItemStatus::Completed,
            },
        }
    }

    fn cmd() -> EngineEvent {
        EngineEvent::ItemCompleted {
            inference: None,
            item: ThreadItem::CommandExecution {
                id: "c1".into(),
                command: "ls".into(),
                aggregated_output: "a\nb".into(),
                exit_code: None,
                status: ItemStatus::Completed,
            },
        }
    }

    fn file_change() -> EngineEvent {
        EngineEvent::ItemCompleted {
            inference: None,
            item: ThreadItem::FileChange {
                id: "f1".into(),
                changes: vec![FileChange {
                    turn_diff: None,
                    path: "/x.ts".into(),
                    kind: FileChangeKind::Modify,
                    diff: None,
                    added: None,
                    removed: None,
                }],
                status: FileChangeStatus::Completed,
            },
        }
    }

    fn agent_msg() -> EngineEvent {
        EngineEvent::ItemCompleted {
            inference: None,
            item: ThreadItem::AgentMessage {
                phase: None,
                id: "a1".into(),
                text: "final answer".into(),
            },
        }
    }

    fn reasoning() -> EngineEvent {
        EngineEvent::ItemCompleted {
            inference: None,
            item: ThreadItem::Reasoning {
                id: "r1".into(),
                text: "hmm".into(),
            },
        }
    }

    fn live_out() -> EngineEvent {
        EngineEvent::ToolEvent {
            turn_id: "turn1".into(),
            tool_use_id: "tu1".into(),
            event: ToolDomainEvent::Stdout { chunk: "x".into() },
        }
    }

    // A FAILED tool call surfaces as item.updated{status:'failed'} (the engine
    // emits failures as updates, successes as completions) — error
    // tool_result blocks are forwarded like successes.
    fn failed_tool() -> EngineEvent {
        EngineEvent::ItemUpdated {
            inference: None,
            item: ThreadItem::ToolCall {
                id: "e1".into(),
                tool_name: "edit".into(),
                arguments: RuntimeJson::Object(RuntimeObject::new()),
                result: None,
                error: Some(ItemError {
                    message: "denied".into(),
                }),
                status: ItemStatus::Failed,
            },
        }
    }

    fn running_tool() -> EngineEvent {
        EngineEvent::ItemUpdated {
            inference: None,
            item: ThreadItem::ToolCall {
                id: "p1".into(),
                tool_name: "edit".into(),
                arguments: RuntimeJson::Object(RuntimeObject::new()),
                result: None,
                error: None,
                status: ItemStatus::InProgress,
            },
        }
    }

    /// The `id` common to every item variant.
    fn item_id(item: &ThreadItem) -> &RuntimeString {
        match item {
            ThreadItem::AgentMessage { id, .. }
            | ThreadItem::Reasoning { id, .. }
            | ThreadItem::CommandExecution { id, .. }
            | ThreadItem::FileChange { id, .. }
            | ThreadItem::ToolCall { id, .. }
            | ThreadItem::Error { id, .. }
            | ThreadItem::ProposedPlan { id, .. } => id,
        }
    }

    /// Pull the item off an item-bearing event.
    fn item_of(e: EngineEvent) -> ThreadItem {
        match e {
            EngineEvent::ItemCompleted { item, .. }
            | EngineEvent::ItemUpdated { item, .. }
            | EngineEvent::ItemStarted { item, .. } => item,
            _ => panic!("event carries no item"),
        }
    }

    fn as_value(e: &EngineEvent) -> RuntimeJson {
        runtime_codec::encode_value(e).unwrap()
    }

    // ---- a minimal ToolContext for the consumer (only emit / signal /
    // forward_subagent_text are read) ----

    struct StubFileState;
    impl FileStateCache for StubFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            unreachable!()
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {
            unreachable!()
        }
        fn has(&self, _path: &RuntimePath) -> bool {
            unreachable!()
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            unreachable!()
        }
        fn delete(&self, _path: &RuntimePath) {
            unreachable!()
        }
        fn clear(&self) {
            unreachable!()
        }
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            unreachable!()
        }
    }

    struct StubSandbox;
    impl SandboxBackend for StubSandbox {
        fn kind(&self) -> SandboxKind {
            unreachable!()
        }
        fn is_available(&self) -> bool {
            unreachable!()
        }
        fn transform(&self, _req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            unreachable!()
        }
    }

    fn fake_ctx(
        forward_text: bool,
        aborted: bool,
    ) -> (ToolContext, Arc<Mutex<Vec<ToolDomainEvent>>>) {
        let emitted: Arc<Mutex<Vec<ToolDomainEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&emitted);
        let emit: EmitFn = Arc::new(move |e| {
            sink.lock().unwrap().push(e);
            Box::pin(async {})
        });
        let signal = AbortSignal::new();
        if aborted {
            signal.cancel();
        }
        let ctx = ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::default(),
            signal,
            file_state: Arc::new(StubFileState),
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(StubSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            emit,
            enqueue_background_notice: None,
            record_rollout_event: None,
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: if forward_text { Some(true) } else { None },
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
        };
        (ctx, emitted)
    }

    fn stream(events: Vec<EngineEvent>) -> crate::types::tool::EngineEventStream {
        Box::pin(futures::stream::iter(events))
    }

    // ---- terminalItem ----

    #[test]
    fn returns_the_item_for_a_completed_event_and_a_failed_tool_update_none_for_in_progress() {
        assert_eq!(
            terminal_item(&tool_call()).map(item_id),
            Some(&RuntimeString::from("t1"))
        );
        // failure = terminal (error results are forwarded)
        assert_eq!(
            terminal_item(&failed_tool()).map(item_id),
            Some(&RuntimeString::from("e1"))
        );
        // in_progress = non-terminal (filtered out)
        assert!(terminal_item(&running_tool()).is_none());
        assert!(terminal_item(&EngineEvent::TextDelta {
            inference: None,
            message_id: None,
            phase: None,
            text: "x".into()
        })
        .is_none());
    }

    // ---- shouldForwardChildEvent (forward predicate) ----

    #[test]
    fn forwards_tool_ish_completed_items_plus_a_failed_tool_call_plus_live_tool_output_always() {
        for e in [tool_call(), cmd(), file_change(), failed_tool(), live_out()] {
            assert!(should_forward_child_event(&e, false));
        }
    }

    #[test]
    fn gates_agent_message_plus_reasoning_behind_forward_text() {
        assert!(!should_forward_child_event(&agent_msg(), false));
        assert!(!should_forward_child_event(&reasoning(), false));
        assert!(should_forward_child_event(&agent_msg(), true));
        assert!(should_forward_child_event(&reasoning(), true));
    }

    #[test]
    fn drops_token_deltas_plus_non_completed_events() {
        assert!(should_forward_child_event(
            &EngineEvent::TurnStarted {
                turn_id: "child".into()
            },
            false
        ));
        let dropped = [
            EngineEvent::TextDelta {
                inference: None,
                message_id: None,
                phase: None,
                text: "a".into(),
            },
            EngineEvent::ThinkingDelta {
                message_id: None,
                inference: None,
                text: "t".into(),
            },
            EngineEvent::ItemStarted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: "s".into(),
                    tool_name: "x".into(),
                    arguments: RuntimeJson::Object(RuntimeObject::new()),
                    result: None,
                    error: None,
                    status: ItemStatus::InProgress,
                },
            },
            EngineEvent::ItemUpdated {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: "u".into(),
                    tool_name: "x".into(),
                    arguments: RuntimeJson::Object(RuntimeObject::new()),
                    result: None,
                    error: None,
                    status: ItemStatus::InProgress,
                },
            },
            EngineEvent::TurnCompleted {
                turn_id: "t".into(),
                usage: None,
                stop_reason: TerminalReason::Completed,
            },
        ];
        for e in &dropped {
            assert!(!should_forward_child_event(e, true));
        }
    }

    // ---- consumeForegroundSubAgent ----

    #[tokio::test]
    async fn forwards_tool_activity_as_agent_progress_and_extracts_the_final_answer() {
        let (ctx, emitted) = fake_ctx(false, false);
        let run = ForegroundSubAgentRun {
            agent_id: "child-7".into(),
            events: stream(vec![
                tool_call(),
                live_out(),
                EngineEvent::TextDelta {
                    inference: None,
                    message_id: None,
                    phase: None,
                    text: "ignored".into(),
                },
                agent_msg(),
                EngineEvent::TurnCompleted {
                    turn_id: "t".into(),
                    usage: None,
                    stop_reason: TerminalReason::Completed,
                },
            ]),
        };
        let res = consume_foreground_sub_agent(&ctx, run).await;
        assert!(res.failure.is_none());
        assert_eq!(res.final_text, "final answer");
        // tool_call + live output forwarded; text.delta + agent_message (default) NOT.
        let em = emitted.lock().unwrap();
        assert_eq!(em.len(), 2);
        for e in em.iter() {
            match e {
                ToolDomainEvent::AgentProgress { agent_id, .. } => {
                    assert_eq!(agent_id.as_string(), Some(&RuntimeString::from("child-7")))
                }
                _ => panic!("expected agent_progress"),
            }
        }
        match &em[0] {
            ToolDomainEvent::AgentProgress { event, .. } => {
                assert_eq!(as_value(event), as_value(&tool_call()))
            }
            _ => panic!("expected agent_progress"),
        }
        match &em[1] {
            ToolDomainEvent::AgentProgress { event, .. } => {
                assert_eq!(as_value(event), as_value(&live_out()))
            }
            _ => panic!("expected agent_progress"),
        }
    }

    #[tokio::test]
    async fn preserves_a_raw_foreground_agent_id_in_progress() {
        let (ctx, emitted) = fake_ctx(false, false);
        consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: RuntimeJson::Bool(true),
                events: stream(vec![tool_call()]),
            },
        )
        .await;

        let emitted = emitted.lock().unwrap();
        assert!(matches!(
            emitted.as_slice(),
            [ToolDomainEvent::AgentProgress { agent_id, .. }] if agent_id == &RuntimeJson::Bool(true)
        ));
    }

    #[tokio::test]
    async fn forwards_a_childs_failed_tool_call() {
        let (ctx, emitted) = fake_ctx(false, false);
        consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "c".into(),
                events: stream(vec![running_tool(), failed_tool(), agent_msg()]),
            },
        )
        .await;
        // in_progress dropped, failed forwarded, agent_message (default) dropped.
        let em = emitted.lock().unwrap();
        assert_eq!(em.len(), 1);
        match &em[0] {
            ToolDomainEvent::AgentProgress { event, .. } => {
                assert_eq!(as_value(event), as_value(&failed_tool()))
            }
            _ => panic!("expected agent_progress"),
        }
    }

    #[tokio::test]
    async fn forwards_agent_message_too_when_forward_subagent_text_is_on() {
        let (ctx, emitted) = fake_ctx(true, false);
        consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "c".into(),
                events: stream(vec![agent_msg()]),
            },
        )
        .await;
        let em = emitted.lock().unwrap();
        assert_eq!(em.len(), 1);
        match &em[0] {
            ToolDomainEvent::AgentProgress { event, .. } => {
                assert_eq!(as_value(event), as_value(&agent_msg()))
            }
            _ => panic!("expected agent_progress"),
        }
    }

    #[tokio::test]
    async fn captures_a_turn_failure_without_emitting_it_as_progress() {
        let (ctx, emitted) = fake_ctx(false, false);
        let res = consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "c".into(),
                events: stream(vec![EngineEvent::TurnFailed {
                    inference: None,
                    error: EngineError {
                        code: EngineErrorCode::Internal,
                        message: "boom".into(),
                        cause: None,
                        request_id: None,
                        provider_kind: None,
                        retry_after_ms: None,
                    },
                }]),
            },
        )
        .await;
        assert_eq!(res.failure, Some(RuntimeString::from("boom")));
        assert_eq!(emitted.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn stops_forwarding_once_the_turn_is_aborted() {
        let (ctx, emitted) = fake_ctx(false, true);
        let result = consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "c".into(),
                events: stream(vec![tool_call(), cmd()]),
            },
        )
        .await;
        assert_eq!(emitted.lock().unwrap().len(), 0);
        assert_eq!(result.failure, terminal_failure(TerminalReason::Aborted));
    }

    #[tokio::test]
    async fn a_stream_without_a_terminal_event_is_a_failure() {
        let (ctx, _) = fake_ctx(false, false);
        let result = consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "child".into(),
                events: stream(vec![agent_msg()]),
            },
        )
        .await;
        assert_eq!(result.final_text, "final answer");
        assert_eq!(
            result.failure,
            Some("Sub-agent stream ended without a terminal event.".into())
        );
    }

    #[tokio::test]
    async fn a_completed_turn_can_have_no_final_text() {
        let (ctx, _) = fake_ctx(false, false);
        let result = consume_foreground_sub_agent(
            &ctx,
            ForegroundSubAgentRun {
                agent_id: "child".into(),
                events: stream(vec![EngineEvent::TurnCompleted {
                    turn_id: "child-turn".into(),
                    usage: None,
                    stop_reason: TerminalReason::Completed,
                }]),
            },
        )
        .await;
        assert!(result.final_text.is_empty());
        assert!(result.failure.is_none());
    }

    // ---- renderItemForTranscript + appendTranscript (Gap B transcript surface) ----

    #[test]
    fn renders_each_item_type_concisely() {
        assert_eq!(
            render_item_for_transcript(&item_of(tool_call())),
            "[tool] read"
        );
        assert_eq!(render_item_for_transcript(&item_of(cmd())), "$ ls\na\nb");
        assert_eq!(
            render_item_for_transcript(&item_of(file_change())),
            "[edit] modify /x.ts"
        );
        assert_eq!(
            render_item_for_transcript(&item_of(agent_msg())),
            "final answer"
        );
        assert_eq!(
            render_item_for_transcript(&item_of(reasoning())),
            "[thinking] hmm"
        );
    }

    // ---- persisted-transcript projection (cold-resume handle recovery) ----

    fn assistant(uuid: &str, content: Vec<ContentBlock>) -> Message {
        Message::Assistant(crate::schemas::message::AssistantMessage {
            uuid: uuid.into(),
            content,
            model: "fixture".into(),
            usage: None,
        })
    }

    fn text(value: &str, phase: Option<&str>) -> ContentBlock {
        ContentBlock::Text(crate::schemas::content::TextBlock {
            text: value.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: phase.map(Into::into),
        })
    }

    fn thinking(value: &str) -> ContentBlock {
        ContentBlock::Thinking(crate::schemas::content::ThinkingBlock {
            text: value.into(),
            message_id: None,
            signature: None,
            encrypted_content: None,
        })
    }

    fn tool_use(id: &str, name: &str) -> ContentBlock {
        ContentBlock::ToolUse(crate::schemas::content::ToolUseBlock {
            id: id.into(),
            name: name.into(),
            input: RuntimeJson::Object(RuntimeObject::new()),
            tool_kind: None,
            thought_signature: None,
        })
    }

    fn tool_result(id: &str, output: &str, is_error: bool) -> Message {
        Message::ToolResult(crate::schemas::message::ToolResultMessage {
            uuid: RuntimeString::from(format!("{id}-result")),
            tool_use_id: id.into(),
            content: vec![crate::schemas::content::OutputBlock::Text(
                crate::schemas::content::TextBlock {
                    text: output.into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                },
            )],
            is_error,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    /// The projection must be byte-identical to feeding the same items through
    /// the live renderer in the same order. That equality is the whole point:
    /// a recovered handle must read exactly like the one the model saw before
    /// the restart, so a wording change cannot silently fork the two formats.
    #[test]
    fn projection_matches_the_live_renderer_byte_for_byte() {
        let history = vec![
            assistant("m1", vec![thinking("hmm"), text("final answer", None)]),
            assistant("m2", vec![tool_use("t1", "read")]),
            tool_result("t1", "file body", false),
        ];
        let projected = project_persisted_transcript(&history, None);

        // Feed the equivalent items through the live path. A tool call renders as
        // its name only — the live renderer never inlines the result body — so the
        // projection must not either, or a recovered handle would read differently
        // from the one the model saw before the restart.
        let mut live = RuntimeString::default();
        for item in [
            item_of(reasoning()),
            item_of(agent_msg()),
            item_of(tool_call()),
        ] {
            live = append_transcript(&live, &item, None);
        }
        assert_eq!(
            projected.try_to_utf8().unwrap(),
            live.try_to_utf8().unwrap(),
            "the recovered buffer must be byte-identical to the live one"
        );
        assert_eq!(
            projected.try_to_utf8().unwrap(),
            "[thinking] hmm\nfinal answer\n[tool] read"
        );
    }

    /// Commentary is part of the live buffer, so the projection keeps it too.
    /// Filtering it would make a recovered handle shorter than the real one.
    #[test]
    fn projection_keeps_commentary_like_the_live_buffer() {
        let history = vec![assistant(
            "m1",
            vec![text("aside", Some("commentary")), text("answer", None)],
        )];
        assert_eq!(
            project_persisted_transcript(&history, None)
                .try_to_utf8()
                .unwrap(),
            "aside\nanswer"
        );
    }

    /// A call whose result never landed is where the work actually stopped, so it
    /// is rendered in progress rather than dropped.
    #[test]
    fn projection_keeps_an_unresolved_call_as_in_progress() {
        let history = vec![assistant("m1", vec![tool_use("t1", "bash")])];
        assert_eq!(
            project_persisted_transcript(&history, None)
                .try_to_utf8()
                .unwrap(),
            "[tool] bash"
        );
    }

    /// A failed tool result is marked as an error, matching the live contract
    /// that failures are terminal items too.
    #[test]
    fn projection_marks_a_failed_tool_result() {
        let history = vec![
            assistant("m1", vec![tool_use("t1", "bash")]),
            tool_result("t1", "boom", true),
        ];
        let rendered = project_persisted_transcript(&history, None)
            .try_to_utf8()
            .unwrap();
        assert!(
            rendered.contains("error"),
            "a failed call must read as a failure: {rendered}"
        );
    }

    /// Empty input projects to an empty buffer, so an agent with no saved
    /// transcript does not manufacture a line.
    #[test]
    fn projection_of_empty_history_is_empty() {
        assert!(project_persisted_transcript(&[], None).is_empty());
        assert!(
            project_persisted_transcript(&[assistant("m1", vec![text("", None)])], None).is_empty()
        );
    }

    /// The recovered buffer obeys the same tail bound as the live one, so a long
    /// child transcript cannot grow the parent's memory without limit.
    #[test]
    fn projection_applies_the_same_tail_cap() {
        let history = vec![assistant("m1", vec![text(&"x".repeat(200), None)])];
        let capped = project_persisted_transcript(&history, Some(20));
        assert_eq!(capped.len_utf16(), 20);
        // Uncapped keeps everything.
        assert_eq!(
            project_persisted_transcript(&history, None).len_utf16(),
            200
        );
    }

    #[test]
    fn appends_items_with_newline_separators_and_bounds_to_the_tail() {
        let mut buf = RuntimeString::default();
        buf = append_transcript(&buf, &item_of(agent_msg()), None);
        buf = append_transcript(&buf, &item_of(tool_call()), None);
        assert_eq!(buf, "final answer\n[tool] read");
        // Cap keeps only the most recent code units.
        let capped = append_transcript(
            &RuntimeString::from("x".repeat(100)),
            &item_of(tool_call()),
            Some(10),
        );
        assert_eq!(capped.len_utf16(), 10);
        // truncated to the last 10 code units
        assert!(!capped.ends_with("[tool] read"));
        assert_eq!(capped, "tool] read");
    }
}
