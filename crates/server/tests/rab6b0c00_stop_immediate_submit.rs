//! Defect: a confirmed Stop could leave the old foreground turn occupied.
//! Root cause: interrupt settlement preceded reservation retirement, while tool
//! execution could ignore cancellation indefinitely.
//! Fix: bound tool cancellation and settle Stop after its captured owner retires.
//! Fixing commit: ab6b0c00713bc6845cae822ad44743c52e5a842c.
//! The public facade case lives here to avoid a core -> server dependency.

use super::*;
use async_trait::async_trait;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimePath};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

const STOP_BUDGET: Duration = Duration::from_secs(2);

#[derive(Default)]
struct SlowExecution {
    entered: Notify,
    cleanup: Notify,
    released: AtomicBool,
    dropped: AtomicUsize,
    returned: AtomicUsize,
    calls: AtomicUsize,
}

impl SlowExecution {
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.cleanup.notify_one();
    }
}

struct ExecutionDrop(Arc<SlowExecution>);

impl Drop for ExecutionDrop {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct UncooperativeTool {
    state: Arc<SlowExecution>,
    schema: JsonSchema,
}

#[async_trait]
impl Tool for UncooperativeTool {
    fn static_name(&self) -> &str {
        "slow_probe"
    }

    fn static_description(&self) -> &str {
        "Wait for external cleanup without responding to cancellation."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _: Json,
        _ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let _drop = ExecutionDrop(self.state.clone());
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        self.state.entered.notify_one();
        // Deliberately ignore the cancellation signal. Only the test can open
        // this barrier, so prompt retirement must drop this execution future.
        self.state.cleanup.notified().await;
        self.state.returned.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            RuntimeJson::Null,
            "external cleanup finished",
            false,
        ))
    }
}

struct Fixture {
    client: TestClient,
    session_id: String,
    session: Arc<Mutex<Option<Arc<Session>>>>,
    state: Arc<SlowExecution>,
    output_gate: Arc<super::output_control_tests::OutputGate>,
    model_calls: Arc<AtomicUsize>,
    _root: tempfile::TempDir,
}

async fn rpc(client: &mut TestClient, id: u64, method: &str, params: Value) -> (Value, Vec<Value>) {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    (reply["result"].clone(), seen)
}

async fn fixture() -> Fixture {
    fixture_with_prompt_gate(false).await
}

async fn fixture_with_prompt_gate(hold_prompt: bool) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let state = Arc::new(SlowExecution::default());
    let tool: Arc<dyn Tool> = Arc::new(UncooperativeTool {
        state: state.clone(),
        schema: json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
    });
    let model_calls = Arc::new(AtomicUsize::new(0));
    let count = model_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            let call = count.fetch_add(1, Ordering::SeqCst);
            let events = if call == 0 && !hold_prompt {
                let mut events = vec![ev::message_start(None)];
                events.extend(ev::tool_use(
                    "slow-call",
                    "slow_probe",
                    &RuntimeJson::Object(Default::default()),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                events
            } else {
                text_turn("new message ran")
            };
            Box::pin(futures::stream::iter(events))
        }),
        Some(json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false})),
    );
    let captured = Arc::new(Mutex::new(None));
    let live = captured.clone();
    let cwd = RuntimePath::from(root.path().to_string_lossy().into_owned());
    let config_dir = cwd.clone();
    let system_prompt_provider: Option<klaus_agent_core::engine::session::SystemPromptProvider> =
        hold_prompt.then(|| {
            let state = state.clone();
            Arc::new(move || {
                let state = state.clone();
                Box::pin(async move {
                    if state.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        let _drop = ExecutionDrop(state.clone());
                        state.entered.notify_one();
                        state.cleanup.notified().await;
                        state.returned.fetch_add(1, Ordering::SeqCst);
                    }
                    Vec::new()
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Vec<
                                        klaus_agent_core::types::provider::SystemPromptPart,
                                    >,
                                > + Send,
                        >,
                    >
            }) as klaus_agent_core::engine::session::SystemPromptProvider
        });
    let create: CreateSession = Arc::new(move |ctx| {
        let session = agent.start_session(StartSessionOptions {
            id: ctx.init.thread_id.clone(),
            cwd: Some(cwd.clone()),
            user_config_dir: Some(config_dir.clone()),
            tools: Some(vec![tool.clone()]),
            system_prompt_provider: system_prompt_provider.clone(),
            durable_permissions: Some(ctx.durable_permissions),
            approval_handler: Some(ctx.approval_handler),
            elicit_handler: Some(ctx.elicit_handler),
            mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            ..Default::default()
        });
        *live.lock().unwrap() = Some(session.clone());
        Box::pin(async move { Ok(session) })
    });
    let (mut client, output_gate) = super::output_control_tests::client(create, root.path());
    client.initialize().await;
    let (reply, _) = rpc(&mut client, 2, "session/start", json!({"init":{}})).await;
    Fixture {
        client,
        session_id: reply["sessionId"].as_str().unwrap().to_owned(),
        session: captured,
        state,
        output_gate,
        model_calls,
        _root: root,
    }
}

async fn start_slow(fixture: &mut Fixture) {
    let (started, _) = rpc(
        &mut fixture.client,
        10,
        "turn/submit",
        json!({
            "sessionId":fixture.session_id,"submissionId":"old-turn",
            "mode":"start-if-idle","input":"run the slow probe"
        }),
    )
    .await;
    assert_eq!(started["status"], "started", "{started}");
    tokio::time::timeout(REPLY_TIMEOUT, fixture.state.entered.notified())
        .await
        .expect("the provider must enter its requested tool before Stop");
}

fn is_interrupt(frame: &Value) -> bool {
    let event = &frame["params"]["event"];
    event["submissionId"] == "stop-old" && event.get("turnInterrupt").is_some()
}

async fn interrupt(fixture: &mut Fixture) -> (Value, Vec<Value>) {
    let (ack, seen) = rpc(
        &mut fixture.client,
        11,
        "turn/interrupt",
        json!({
            "sessionId":fixture.session_id,"submissionId":"stop-old",
            "origin":"user-stop","expectedTurnId":"old-turn"
        }),
    )
    .await;
    assert_eq!(
        ack,
        json!({"submissionId":"stop-old"}),
        "dispatch ACK is not final settlement"
    );
    let mut pending = VecDeque::from(seen);
    let mut frames = Vec::new();
    let outcome = tokio::time::timeout(STOP_BUDGET, async {
        loop {
            let frame = match pending.pop_front() {
                Some(frame) => frame,
                None => fixture.client.next_message().await,
            };
            let done = is_interrupt(&frame);
            frames.push(frame.clone());
            if done {
                return frame["params"]["event"].clone();
            }
        }
    })
    .await
    .expect("Stop must settle promptly while external cleanup remains blocked");
    (outcome, frames)
}

fn completed(frames: &[Value], turn: &str) -> bool {
    frames
        .iter()
        .any(|frame| frame["method"] == "turn/completed" && frame["params"]["turnId"] == turn)
}

async fn collect_completion(client: &mut TestClient, frames: &mut Vec<Value>, turn: &str) {
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while !completed(frames, turn) {
            frames.push(client.next_message().await);
        }
    })
    .await
    .expect("the foreground turn must finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_immediate_submit_starts_once_after_final_acceptance() {
    let mut fixture = fixture().await;
    start_slow(&mut fixture).await;
    let (outcome, mut frames) = interrupt(&mut fixture).await;
    let dropped_at_confirmation = fixture.state.dropped.load(Ordering::SeqCst);
    let returned_at_confirmation = fixture.state.returned.load(Ordering::SeqCst);
    // No wait, idle polling, retry, or cleanup release may precede this send.
    let (new, seen) = rpc(
        &mut fixture.client,
        12,
        "turn/submit",
        json!({
            "sessionId":fixture.session_id,"submissionId":"new-turn",
            "mode":"start-if-idle","input":"the immediate new message"
        }),
    )
    .await;
    frames.extend(seen);
    fixture.state.release();
    collect_completion(&mut fixture.client, &mut frames, "old-turn").await;
    if new["status"] == "started" {
        collect_completion(&mut fixture.client, &mut frames, "new-turn").await;
    }
    let calls = fixture.model_calls.load(Ordering::SeqCst);
    let session = fixture.session.lock().unwrap().clone().unwrap();
    let new_messages = session.history().iter().filter(|message| matches!(message,
        Message::User(user) if user.content.iter().any(|block| matches!(block,
            ContentBlock::Text(text) if text.text.try_to_utf8().unwrap() == "the immediate new message"
        ))
    )).count();
    let queue_empty = session.queue_snapshot().items.is_empty();
    fixture.client.close().await;
    eprintln!("stop outcome={outcome}; immediate submit={new}; dropped_at_confirmation={dropped_at_confirmation}; returned_at_confirmation={returned_at_confirmation}; model_calls={calls}; new_messages={new_messages}");
    assert_eq!(outcome["turnInterrupt"]["accepted"], true, "{outcome}");
    assert_eq!(outcome["turnInterrupt"]["interruptedTurnId"], "old-turn");
    assert_eq!(
        new["status"], "started",
        "final Stop confirmation must free the foreground reservation: {new}"
    );
    assert_eq!(
        dropped_at_confirmation, 1,
        "confirmation must follow old tool future retirement"
    );
    assert_eq!(
        returned_at_confirmation, 0,
        "confirmation cannot depend on external cleanup"
    );
    assert_eq!(calls, 2, "one model request per explicitly submitted turn");
    assert_eq!(
        new_messages, 1,
        "the immediate input must execute exactly once"
    );
    assert!(
        queue_empty,
        "the new input must not be retained for later retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_immediate_submit_cancels_without_external_cleanup() {
    let mut fixture = fixture().await;
    start_slow(&mut fixture).await;
    let started = tokio::time::Instant::now();
    let (outcome, mut frames) = interrupt(&mut fixture).await;
    let finished_before_cleanup = tokio::time::timeout(STOP_BUDGET, async {
        while !completed(&frames, "old-turn") {
            frames.push(fixture.client.next_message().await);
        }
    })
    .await
    .is_ok();
    let elapsed = started.elapsed();
    let dropped = fixture.state.dropped.load(Ordering::SeqCst);
    let returned = fixture.state.returned.load(Ordering::SeqCst);
    let cleanup_still_blocked = !fixture.state.released.load(Ordering::SeqCst);
    fixture.state.release();
    collect_completion(&mut fixture.client, &mut frames, "old-turn").await;
    fixture.client.close().await;
    eprintln!("stop outcome={outcome}; finished_before_cleanup={finished_before_cleanup}; elapsed={elapsed:?}; dropped={dropped}; returned={returned}; cleanup_still_blocked={cleanup_still_blocked}");
    assert_eq!(outcome["turnInterrupt"]["accepted"], true, "{outcome}");
    assert!(finished_before_cleanup, "Stop must retire a tool that ignores cancellation without waiting for its external cleanup barrier");
    assert!(
        elapsed < STOP_BUDGET,
        "Stop exceeded its bounded cancellation budget: {elapsed:?}"
    );
    assert!(cleanup_still_blocked);
    assert_eq!(
        dropped, 1,
        "the engine must drop uncooperative tool execution"
    );
    assert_eq!(
        returned, 0,
        "external cleanup must still be pending when Stop finishes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_output_does_not_block_stop_and_immediate_next_input() {
    let mut fixture = fixture().await;
    start_slow(&mut fixture).await;
    fixture.output_gate.fill(&mut fixture.client).await;
    fixture
        .client
        .send(json!({"id":11,"method":"turn/interrupt","params":{
            "sessionId":fixture.session_id,"submissionId":"stop-old",
            "origin":"user-stop","expectedTurnId":"old-turn"
        }}))
        .await;
    let cancelled = tokio::time::timeout(STOP_BUDGET, async {
        while fixture.state.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Always release the transport before checking the outcome so a failure
    // cannot strand this fixture's session and output tasks.
    fixture.output_gate.resume();
    assert!(
        cancelled.is_ok(),
        "Stop must reach the active tool while output remains full"
    );
    let (ack, mut frames) = fixture.client.response_for(11).await;
    assert!(ack.get("error").is_none(), "{ack}");
    let outcome = tokio::time::timeout(STOP_BUDGET, async {
        loop {
            if let Some(frame) = frames.iter().find(|frame| is_interrupt(frame)) {
                break frame["params"]["event"].clone();
            }
            frames.push(fixture.client.next_message().await);
        }
    })
    .await
    .expect("the Stop terminal must follow draining the writer");
    assert_eq!(outcome["turnInterrupt"]["accepted"], true, "{outcome}");
    let (new, seen) = rpc(
        &mut fixture.client,
        12,
        "turn/submit",
        json!({
            "sessionId":fixture.session_id,"submissionId":"new-turn",
            "mode":"start-if-idle","input":"the immediate new message"
        }),
    )
    .await;
    frames.extend(seen);
    assert_eq!(
        new["status"], "started",
        "final Stop must retire its old reservation: {new}"
    );
    collect_completion(&mut fixture.client, &mut frames, "new-turn").await;
    assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.returned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    fixture.state.release();
    fixture.client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_pending_preparation_confirms_before_immediate_submit() {
    let mut fixture = fixture_with_prompt_gate(true).await;
    let (started, _) = rpc(
        &mut fixture.client,
        10,
        "turn/submit",
        json!({
            "sessionId":fixture.session_id,"submissionId":"old-turn",
            "mode":"start-if-idle","input":"the blocked preparation input"
        }),
    )
    .await;
    assert_eq!(started["status"], "started", "{started}");
    tokio::time::timeout(REPLY_TIMEOUT, fixture.state.entered.notified())
        .await
        .expect("the original turn must enter its prompt preparation");
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 0);

    let before_stop = tokio::time::Instant::now();
    let (ack, seen) = rpc(
        &mut fixture.client,
        11,
        "turn/interrupt",
        json!({
            "sessionId":fixture.session_id,"submissionId":"stop-old",
            "origin":"user-stop","expectedTurnId":"old-turn"
        }),
    )
    .await;
    assert_eq!(ack, json!({"submissionId":"stop-old"}));
    let mut pending = VecDeque::from(seen);
    let mut frames = Vec::new();
    let final_confirmation = tokio::time::timeout(STOP_BUDGET, async {
        loop {
            let frame = match pending.pop_front() {
                Some(frame) => frame,
                None => fixture.client.next_message().await,
            };
            let done = is_interrupt(&frame);
            frames.push(frame.clone());
            if done {
                break frame["params"]["event"].clone();
            }
        }
    })
    .await;
    let elapsed = before_stop.elapsed();
    let dropped_at_confirmation = fixture.state.dropped.load(Ordering::SeqCst);
    let returned_at_confirmation = fixture.state.returned.load(Ordering::SeqCst);
    let calls_at_confirmation = fixture.model_calls.load(Ordering::SeqCst);
    let (confirmed_while_blocked, outcome) = match final_confirmation {
        Ok(outcome) => (true, outcome),
        Err(_) => {
            // Release the failed baseline before asserting, so the fixture
            // never strands its server connection or background run.
            fixture.state.release();
            let outcome = tokio::time::timeout(REPLY_TIMEOUT, async {
                loop {
                    let frame = fixture.client.next_message().await;
                    let done = is_interrupt(&frame);
                    frames.push(frame.clone());
                    if done {
                        break frame["params"]["event"].clone();
                    }
                }
            })
            .await
            .expect("the baseline must settle after preparation is released");
            (false, outcome)
        }
    };
    // The successful path sends immediately after final confirmation. It does
    // not poll idle, retry, or open the original preparation's barrier first.
    let (new, seen) = rpc(
        &mut fixture.client,
        12,
        "turn/submit",
        json!({
            "sessionId":fixture.session_id,"submissionId":"new-turn",
            "mode":"start-if-idle","input":"the immediate new message"
        }),
    )
    .await;
    frames.extend(seen);
    fixture.state.release();
    collect_completion(&mut fixture.client, &mut frames, "old-turn").await;
    if new["status"] == "started" {
        collect_completion(&mut fixture.client, &mut frames, "new-turn").await;
    }
    let calls_after_successor = fixture.model_calls.load(Ordering::SeqCst);
    let session = fixture.session.lock().unwrap().clone().unwrap();
    let new_messages = session
        .history()
        .iter()
        .filter(|message| {
            matches!(message,
                Message::User(user) if user.content.iter().any(|block| matches!(block,
                    ContentBlock::Text(text) if text.text == "the immediate new message"
                ))
            )
        })
        .count();
    let queue_empty = session.queue_snapshot().items.is_empty();
    fixture.client.close().await;
    eprintln!("confirmed_while_blocked={confirmed_while_blocked}; elapsed={elapsed:?}; outcome={outcome}; immediate_submit={new}; dropped_at_confirmation={dropped_at_confirmation}; returned_at_confirmation={returned_at_confirmation}; calls_at_confirmation={calls_at_confirmation}; calls_after_successor={calls_after_successor}; new_messages={new_messages}; queue_empty={queue_empty}");
    assert!(
        confirmed_while_blocked && elapsed < STOP_BUDGET,
        "Stop must confirm without waiting for request preparation: {elapsed:?}"
    );
    assert_eq!(outcome["turnInterrupt"]["accepted"], true, "{outcome}");
    assert_eq!(outcome["turnInterrupt"]["interruptedTurnId"], "old-turn");
    assert_eq!(
        dropped_at_confirmation, 1,
        "retire the old preparation before final Stop confirmation"
    );
    assert_eq!(
        returned_at_confirmation, 0,
        "the old preparation must not finish naturally"
    );
    assert_eq!(
        calls_at_confirmation, 0,
        "the stopped turn cannot contact the provider"
    );
    assert_eq!(new["status"], "started", "{new}");
    assert_eq!(
        calls_after_successor, 1,
        "only the explicit successor calls the provider"
    );
    assert_eq!(new_messages, 1, "the successor input executes exactly once");
    assert!(
        queue_empty,
        "the successor cannot remain queued for a later retry"
    );
}
