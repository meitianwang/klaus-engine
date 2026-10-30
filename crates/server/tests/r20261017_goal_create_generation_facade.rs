//! Real goal tool calls must retain the intent captured by their provider request.

use super::*;
use futures::StreamExt;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::types::provider::ModelRequest;
use std::collections::VecDeque;
use std::sync::Mutex;

struct ToolStep {
    id: &'static str,
    name: &'static str,
    input: Value,
    held: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>)>,
}

enum Step {
    Tool(ToolStep),
    Final(&'static str),
    HeldFinal(
        &'static str,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Semaphore>,
    ),
}

struct ReleaseGateOnDrop(Arc<tokio::sync::Semaphore>);
impl Drop for ReleaseGateOnDrop {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

fn create(id: &'static str, objective: &'static str) -> Step {
    Step::Tool(ToolStep {
        id,
        name: "create_goal",
        input: json!({"objective":objective}),
        held: None,
    })
}

fn complete(id: &'static str) -> Step {
    Step::Tool(ToolStep {
        id,
        name: "update_goal",
        input: json!({"status":"complete"}),
        held: None,
    })
}

fn scripted_goal_agent(steps: Vec<Step>, requests: Arc<Mutex<Vec<ModelRequest>>>) -> Arc<Agent> {
    let steps = Mutex::new(VecDeque::from(steps));
    make_agent_with_call_model_and_config(
        Box::new(move |request, signal| {
            requests.lock().unwrap().push(request);
            let step = steps
                .lock()
                .unwrap()
                .pop_front()
                .expect("only the scripted provider requests may run");
            if let Step::Tool(ToolStep {
                held: Some((entered, _)),
                ..
            }) = &step
            {
                entered.notify_one();
            }
            if let Step::HeldFinal(_, entered, _) = &step {
                entered.notify_one();
            }
            Box::pin(
                futures::stream::once(async move {
                    let mut events = vec![ev::message_start(None)];
                    let reason = match step {
                        Step::Tool(tool) => {
                            if let Some((_, release)) = tool.held {
                                release.acquire().await.unwrap().forget();
                            }
                            let input =
                                runtime_json::parse(&RuntimeString::from(tool.input.to_string()))
                                    .unwrap();
                            events.extend(ev::tool_use(tool.id, tool.name, &input));
                            ModelStopReason::ToolUse
                        }
                        Step::Final(text) => {
                            events.push(ev::text(text));
                            ModelStopReason::EndTurn
                        }
                        Step::HeldFinal(text, _, release) => {
                            tokio::select! {
                                biased;
                                _ = signal.cancelled() => return futures::stream::iter(Vec::new()),
                                permit = release.acquire() => permit.unwrap().forget(),
                            }
                            events.push(ev::text(text));
                            ModelStopReason::EndTurn
                        }
                    };
                    events.push(ev::stop(
                        Some(reason),
                        Some(Usage {
                            input_tokens: 0,
                            output_tokens: 0,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        }),
                    ));
                    futures::stream::iter(events)
                })
                .flatten(),
            )
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
    )
}

async fn start_named_turn(
    client: &mut TestClient,
    session: &str,
    id: u64,
    submission: &str,
) -> Vec<Value> {
    client
        .send(json!({"id":id,"method":"turn/start","params":{
            "sessionId":session,"submissionId":submission,"input":"Run the authorized goal tool operation"
        }}))
        .await;
    let (reply, frames) = client.response_for(id).await;
    assert_eq!(reply["result"]["turnId"], submission, "{reply}");
    frames
}

async fn finish_named_turn(
    client: &mut TestClient,
    submission: &str,
    initial: Vec<Value>,
) -> Value {
    let mut initial = VecDeque::from(initial);
    let mut diagnostic = VecDeque::with_capacity(32);
    let result = tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = match initial.pop_front() {
                Some(frame) => frame,
                None => client.next_message().await,
            };
            if diagnostic.len() == 32 { diagnostic.pop_front(); }
            diagnostic.push_back(frame.clone());
            assert_ne!(frame["method"], "approval/request",
                "private goal control must not ask for workspace-write permission: {frame}");
            if frame["method"] == "turn/completed" && frame["params"]["turnId"] == submission {
                return frame["params"]["result"].clone();
            }
            let envelope = &frame["params"]["event"];
            if envelope["submissionId"] == submission && envelope.get("result").is_some() {
                return envelope["result"].clone();
            }
        }
    })
    .await
    .unwrap_or_else(|error| panic!("the exact named foreground operation must settle: {error}; last frames: {diagnostic:?}"));
    result
}

async fn read_goal(client: &mut TestClient, session: &str, id: u64) -> Value {
    client
        .send(json!({"id":id,"method":"session/goal","params":{"sessionId":session}}))
        .await;
    let (reply, _) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

fn result_for<'a>(items: &'a [RolloutItem], id: &str) -> &'a ToolResultMessage {
    let mut results = items.iter().filter_map(|item| match item {
        RolloutItem::Message {
            message: Message::ToolResult(result),
        } if result.tool_use_id == id => Some(result),
        _ => None,
    });
    let result = results.next().expect("the actual tool result is persisted");
    assert!(results.next().is_none(), "a tool call must execute once");
    result
}

async fn stale_create_after_human_control(had_completed_goal: bool) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let _release_on_failure = ReleaseGateOnDrop(release.clone());
    let mut steps = Vec::new();
    if had_completed_goal {
        steps.extend([
            create("seed-create", "The original objective"),
            complete("seed-complete"),
            Step::Final("The original objective is complete"),
        ]);
    }
    steps.extend([
        Step::Tool(ToolStep {
            id: "held-stale-create",
            name: "create_goal",
            input: json!({"objective":"The old request must not restore this objective"}),
            held: Some((entered.clone(), release.clone())),
        }),
        Step::Final("The rejected old request has settled"),
    ]);
    let agent = scripted_goal_agent(steps, requests.clone());
    let (mut client, store, _agent, session) = controlled_goal_client_with_agent(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        agent,
    )
    .await;
    if had_completed_goal {
        let frames = start_named_turn(&mut client, &session, 3, "seed-completed-goal").await;
        let result = finish_named_turn(&mut client, "seed-completed-goal", frames).await;
        assert_eq!(result["finalText"], "The original objective is complete");
        assert_eq!(
            read_goal(&mut client, &session, 4).await["threadGoal"]["status"],
            "complete"
        );
    }
    let frames = start_named_turn(&mut client, &session, 5, "old-provider-create").await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .expect("the old provider response must be held before its create tool call");
    client
        .send(json!({"id":6,"method":"session/clearGoal","params":{
            "sessionId":session,"origin":"user"
        }}))
        .await;
    let (cleared, _) = client.response_for(6).await;
    assert_eq!(cleared["result"]["goalCleared"], true, "{cleared}");
    let expected = if had_completed_goal {
        client
            .send(json!({"id":7,"method":"session/setGoal","params":{
                "sessionId":session,"objective":"Only the newer human objective is authorized",
                "status":"paused","origin":"user","expectedGoalId":null,
                "expectedGoalRevision":cleared["result"]["goalRevision"]
            }}))
            .await;
        let (created, _) = client.response_for(7).await;
        assert!(created.get("error").is_none(), "{created}");
        created["result"].clone()
    } else {
        cleared["result"].clone()
    };
    release.add_permits(1);
    finish_named_turn(&mut client, "old-provider-create", frames).await;
    let after = read_goal(&mut client, &session, 8).await;
    assert_eq!(after["threadGoal"], expected["threadGoal"]);
    assert_eq!(after["goalRevision"], expected["goalRevision"]);
    let saved = store.read_thread("controlled-goal-thread").await.unwrap();
    let failed = result_for(&saved.raw_items, "held-stale-create");
    assert!(
        failed.is_error,
        "an old model create must be a real failed tool result"
    );
    let error = failed
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(error.contains("intent changed"), "{error}");
    let actual = requests.lock().unwrap().len();
    assert!(actual <= if had_completed_goal { 5 } else { 2 });
    client.close().await;
}

#[tokio::test]
async fn an_old_create_response_cannot_restore_a_goal_after_an_absent_goal_is_cleared() {
    stale_create_after_human_control(false).await;
}

#[tokio::test]
async fn an_old_create_response_cannot_replace_a_new_goal_after_a_completed_goal_is_cleared() {
    stale_create_after_human_control(true).await;
}

#[tokio::test]
async fn a_model_created_goal_after_completion_binds_later_tool_requests_to_its_new_generation() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let agent = scripted_goal_agent(
        vec![
            create("original-create", "The completed original objective"),
            complete("original-complete"),
            Step::Final("Original objective complete"),
            create(
                "replacement-create",
                "The newly authorized replacement objective",
            ),
            complete("replacement-complete"),
            Step::Final("Replacement objective verified and complete"),
        ],
        requests.clone(),
    );
    let (mut client, store, _agent, session) = controlled_goal_client_with_agent(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        agent,
    )
    .await;
    let frames = start_named_turn(&mut client, &session, 3, "original-goal-tools").await;
    let original_result = finish_named_turn(&mut client, "original-goal-tools", frames).await;
    assert_eq!(original_result["finalText"], "Original objective complete");
    let original = read_goal(&mut client, &session, 4).await;
    assert_eq!(original["threadGoal"]["status"], "complete");
    let frames = start_named_turn(&mut client, &session, 5, "replacement-goal-tools").await;
    let replacement_result = finish_named_turn(&mut client, "replacement-goal-tools", frames).await;
    assert_eq!(
        replacement_result["finalText"], "Replacement objective verified and complete",
        "the same foreground turn must retain the new generation through its next provider request"
    );
    let replacement = read_goal(&mut client, &session, 6).await;
    assert_eq!(replacement["threadGoal"]["status"], "complete");
    assert_eq!(
        replacement["threadGoal"]["objective"],
        "The newly authorized replacement objective"
    );
    assert_ne!(
        replacement["threadGoal"]["goalId"],
        original["threadGoal"]["goalId"]
    );
    let saved = store.read_thread("controlled-goal-thread").await.unwrap();
    let created = result_for(&saved.raw_items, "replacement-create");
    let completed = result_for(&saved.raw_items, "replacement-complete");
    assert!(
        !created.is_error,
        "the replacement create tool must succeed"
    );
    assert!(
        !completed.is_error,
        "the next provider request may complete its newly created goal"
    );
    client
        .send(json!({"id":7,"method":"session/history","params":{"sessionId":session}}))
        .await;
    let (history, _) = client.response_for(7).await;
    let ids = &history["result"]["historyTurnIds"];
    let created_turn = &ids[created.uuid.try_to_utf8().unwrap()];
    let completed_turn = &ids[completed.uuid.try_to_utf8().unwrap()];
    assert!(created_turn.is_string(), "{history}");
    assert_eq!(
        created_turn, completed_turn,
        "both tool calls must belong to the same actual core turn"
    );
    assert_eq!(requests.lock().unwrap().len(), 6);
    client.close().await;
}

#[path = "r20261017_goal_model_stop_authority_facade.rs"]
mod model_stop_authority;
