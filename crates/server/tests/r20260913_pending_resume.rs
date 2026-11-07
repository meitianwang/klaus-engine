//! Pending input continuation must be atomic, correlated, and subordinate to stop.
use super::*;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelRequest, ModelStream};
use std::sync::atomic::{AtomicUsize, Ordering};

fn persistent_factory(
    agent: Arc<klaus_agent_core::engine::agent::Agent>,
    root: &std::path::Path,
    initial_goal: Option<&str>,
) -> CreateSession {
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(root.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let root = root.to_string_lossy().into_owned();
    let initial_goal = initial_goal.map(str::to_owned);
    Arc::new(move |ctx| {
        let agent = agent.clone();
        let store = store.clone();
        let root = root.clone();
        let initial_goal = initial_goal.clone();
        Box::pin(async move {
            let session = agent
                .open_session(
                    store,
                    StartSessionOptions {
                        id: ctx.init.thread_id,
                        user_config_dir: Some(root.into()),
                        durable_permissions: Some(ctx.durable_permissions),
                        approval_handler: Some(ctx.approval_handler),
                        elicit_handler: Some(ctx.elicit_handler),
                        mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| Box::new(error) as BoxError)?;
            assert!(session.goal_pursuit_eligible());
            assert!(session.goal_runtime().is_some());
            // Seed before observer publication so the explicit foreground turn
            // owns completion; the repeated wire setter remains independently checked.
            if let Some(objective) = initial_goal {
                session
                    .set_thread_goal(Some(objective.into()), None, None)
                    .await
                    .expect("persistent fixture goal is created");
            }
            Ok(session)
        })
    })
}

fn persistent_client(factory: CreateSession, root: &std::path::Path) -> TestClient {
    TestClient::start_full(
        factory,
        None,
        Some(root.join("permissions.json")),
        Some(root.join("config.json")),
    )
}

async fn checked_response(client: &mut TestClient, id: u64) -> (Value, Vec<Value>) {
    eprintln!("pending-resume phase=await-reply requestId={id}");
    let (reply, seen) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "request {id}: {reply}");
    eprintln!("pending-resume phase=reply requestId={id}");
    (reply, seen)
}

fn interrupt_result(message: &Value, id: &str) -> bool {
    message["method"] == "session/rawEvent"
        && message["params"]["event"]["submissionId"] == id
        && message["params"]["event"].get("turnInterrupt").is_some()
}

async fn start(client: &mut TestClient) -> String {
    client.send(json!({"id":1,"method":"initialize","params":{"capabilities":{"productEvents":true,"optOutNotificationMethods":["session/event"]}}})).await;
    client.response_for(1).await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (reply, _) = client.response_for(2).await;
    reply["result"]["sessionId"].as_str().unwrap().to_string()
}

async fn until(
    client: &mut TestClient,
    mut seen: Vec<Value>,
    predicate: impl Fn(&Value) -> bool,
) -> Vec<Value> {
    while !seen.iter().any(&predicate) {
        assert!(seen.len() < 256, "pending continuation frame flood");
        seen.push(client.next_message().await);
    }
    seen
}

fn completed(message: &Value, id: &str) -> bool {
    message["method"] == "turn/completed" && message["params"]["turnId"] == id
}

fn decision(message: &Value, id: &str) -> bool {
    message["method"] == "session/rawEvent"
        && message["params"]["event"]["submissionId"] == id
        && message["params"]["event"].get("pendingResume").is_some()
}

#[tokio::test]
async fn pending_resume_facade_correlates_batch_and_empty_without_fake_completion() {
    let agent = make_agent_with_call_model_and_config(
        scripted_call_model(vec![text_turn("first result"), text_turn("batch result")]),
        Some(json!({"prewarm":false,"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let mut client = TestClient::start(create_session_for_agent_tests(agent));
    let session = start(&mut client).await;
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session,"submissionId":"first","input":"initial"}})).await;
    let (_, seen) = client.response_for(3).await;
    until(&mut client, seen, |message| completed(message, "first")).await;
    for (request, submission, input, priority) in [
        (4, "steer-one", "one", "next"),
        (5, "steer-two", "two", "now"),
    ] {
        client.send(json!({"id":request,"method":"session/steer","params":{"sessionId":session,"submissionId":submission,"input":input,"priority":priority}})).await;
        client.response_for(request).await;
    }
    client
        .send(json!({"id":6,"method":"session/queueSnapshot","params":{"sessionId":session}}))
        .await;
    let (before, _) = client.response_for(6).await;
    assert_eq!(
        before["result"]["queueSnapshot"]["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    client.send(json!({"id":7,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"first","submissionId":"resume-one"}})).await;
    let (ack, seen) = client.response_for(7).await;
    assert_eq!(ack["result"]["turnId"], "resume-one");
    let seen = until(&mut client, seen, |message| {
        completed(message, "resume-one")
    })
    .await;
    let decision_index = seen
        .iter()
        .position(|message| decision(message, "resume-one"))
        .unwrap();
    assert_eq!(
        seen[decision_index]["params"]["event"]["pendingResume"]["started"],
        true
    );
    let started_index = seen
        .iter()
        .position(|message| {
            message["method"] == "turn/started" && message["params"]["submissionId"] == "resume-one"
        })
        .unwrap();
    assert!(decision_index < started_index);
    for steer in ["steer-one", "steer-two"] {
        assert!(
            seen.iter().any(
                |message| message["params"]["event"]["submissionId"] == steer
                    && message["params"]["event"]["steered"]["applied"] == true
            ),
            "{seen:?}"
        );
    }
    client.send(json!({"id":8,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"resume-one","submissionId":"empty"}})).await;
    let (_, seen) = client.response_for(8).await;
    let mut seen = until(&mut client, seen, |message| decision(message, "empty")).await;
    let result = seen
        .iter()
        .find(|message| decision(message, "empty"))
        .unwrap();
    assert_eq!(
        result["params"]["event"]["pendingResume"],
        json!({"started":false,"reason":"empty"})
    );
    client
        .send(json!({"id":9,"method":"session/queueSnapshot","params":{"sessionId":session}}))
        .await;
    let (after, barrier) = client.response_for(9).await;
    seen.extend(barrier);
    assert!(after["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!seen.iter().any(|message| completed(message, "empty")));
    client.close().await;
}

#[tokio::test]
async fn pending_resume_stop_fences_acked_runs_and_continuations_before_chain_execution() {
    use klaus_agent_core::schemas::{content::ContentBlock, message::Message};
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let successor_started = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let agent = make_agent_with_call_model_and_config(
        {
            let calls = calls.clone();
            let started = started.clone();
            let successor_started = successor_started.clone();
            let requests = requests.clone();
            Box::new(
                move |request: ModelRequest, signal: AbortSignal| -> ModelStream {
                    let index = calls.fetch_add(1, Ordering::SeqCst);
                    assert!(
                        index < 2,
                        "only the two acknowledged ordinary owners may run"
                    );
                    {
                        let mut requests = requests.lock().unwrap();
                        assert!(requests.len() < 2, "bounded provider request capture");
                        requests.push((request, signal.clone()));
                    }
                    eprintln!("pending-resume-stop phase=provider-admitted index={index}");
                    if index == 0 {
                        started.notify_one();
                    } else {
                        successor_started.notify_one();
                    }
                    Box::pin(futures::stream::once(async move {
                        signal.cancelled().await;
                        ev::stop(Some(ModelStopReason::EndTurn), None)
                    }))
                },
            )
        },
        Some(json!({"prewarm":false,"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let root = tempfile::tempdir().unwrap();
    let mut client = persistent_client(persistent_factory(agent, root.path(), None), root.path());
    let session = start(&mut client).await;
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session,"submissionId":"active","input":"active"}})).await;
    let (_, mut seen) = checked_response(&mut client, 3).await;
    tokio::time::timeout(REPLY_TIMEOUT, started.notified())
        .await
        .unwrap();
    let queue_id = "73a0c983-bd4c-44cf-b758-cb598be767a8";
    client.send(json!({"id":4,"method":"session/steer","params":{"sessionId":session,"submissionId":"kept","queueMessageId":queue_id,"input":"keep pending","priority":"next"}})).await;
    let (_, frames) = checked_response(&mut client, 4).await;
    seen.extend(frames);
    client.send(json!({"id":5,"method":"turn/start","params":{"sessionId":session,"submissionId":"waiting-run","input":"surviving queued request"}})).await;
    let (_, frames) = checked_response(&mut client, 5).await;
    seen.extend(frames);
    client.send(json!({"id":6,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"active","submissionId":"waiting-resume"}})).await;
    let (_, frames) = checked_response(&mut client, 6).await;
    seen.extend(frames);
    client
        .send(json!({"id":7,"method":"session/queueSnapshot","params":{"sessionId":session}}))
        .await;
    let (before, frames) = checked_response(&mut client, 7).await;
    seen.extend(frames);
    let pending = before["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap();
    assert_eq!(pending.len(), 1, "{before}");
    assert_eq!(pending[0]["message"]["uuid"], queue_id);
    assert_eq!(pending[0]["message"]["value"], "keep pending");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.send(json!({"id":8,"method":"turn/interrupt","params":{"sessionId":session,"submissionId":"stop-active","expectedTurnId":"active","origin":"user-stop"}})).await;
    let (_, frames) = checked_response(&mut client, 8).await;
    seen.extend(frames);
    seen = until(&mut client, seen, |message| {
        interrupt_result(message, "stop-active")
    })
    .await;
    let stopped = &seen
        .iter()
        .find(|message| interrupt_result(message, "stop-active"))
        .unwrap()["params"]["event"]["turnInterrupt"];
    assert_eq!(stopped["accepted"], true, "{seen:?}");
    assert_eq!(stopped["interruptedTurnId"], "active", "{seen:?}");
    tokio::time::timeout(REPLY_TIMEOUT, successor_started.notified())
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the queued owner survives a Stop captured for active"
    );
    {
        let requests = requests.lock().unwrap();
        assert!(requests[0].1.is_cancelled());
        assert!(!requests[1].1.is_cancelled());
    }
    // A second explicit target owns cleanup; the first Stop cannot adopt it.
    client.send(json!({"id":9,"method":"turn/interrupt","params":{"sessionId":session,"submissionId":"stop-successor","expectedTurnId":"waiting-run","origin":"targeted-turn"}})).await;
    let (_, frames) = checked_response(&mut client, 9).await;
    assert!(seen.len() + frames.len() < 256);
    seen.extend(frames);
    seen = until(&mut client, seen, |message| {
        interrupt_result(message, "stop-successor")
    })
    .await;
    let stopped = &seen
        .iter()
        .find(|message| interrupt_result(message, "stop-successor"))
        .unwrap()["params"]["event"]["turnInterrupt"];
    assert_eq!(stopped["accepted"], true, "{seen:?}");
    assert_eq!(stopped["interruptedTurnId"], "waiting-run", "{seen:?}");
    eprintln!("pending-resume-stop phase=await-resume-decision submissionId=waiting-resume");
    seen = until(&mut client, seen, |message| {
        decision(message, "waiting-resume")
    })
    .await;
    assert!(
        seen.iter()
            .any(|message| decision(message, "waiting-resume")
                && message["params"]["event"]["pendingResume"]
                    == json!({"started":false,"reason":"not_completed"})),
        "{seen:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "{seen:?}");
    assert!(
        !seen.iter().any(
            |message| message["params"]["event"]["submissionId"] == "waiting-run"
                && message["params"]["event"]["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("interrupted before admission"))
        ),
        "the successor was independently admitted"
    );
    client
        .send(json!({"id":10,"method":"session/queueSnapshot","params":{"sessionId":session}}))
        .await;
    let (after, _) = checked_response(&mut client, 10).await;
    let pending = after["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap();
    let delivered = {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|(_, signal)| signal.is_cancelled()));
        assert!(
            requests[1].0.messages.iter().any(|message| match message {
                Message::User(user) => user.content.iter().any(|block| match block {
                    ContentBlock::Text(text) => text
                        .text
                        .try_to_utf8()
                        .is_ok_and(|text| text.contains("surviving queued request")),
                    _ => false,
                }),
                _ => false,
            }),
            "the retained queued owner delivers its actual input"
        );
        requests[1]
            .0
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::User(user) => Some(
                    user.content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
                _ => None,
            })
            .map(|text| text.matches("keep pending").count())
            .sum::<usize>()
    };
    assert!(pending.len() <= 1, "{after}");
    if let Some(item) = pending.first() {
        assert_eq!(item["message"]["uuid"], queue_id);
        assert_eq!(item["message"]["value"], "keep pending");
    }
    assert_eq!(
        pending.len() + delivered,
        1,
        "queued input is retained or delivered exactly once: {after}"
    );
    eprintln!("pending-resume-stop phase=close providerRequests=2");
    client.close().await;
}

#[tokio::test]
async fn pending_resume_rejects_goal_completion_but_allows_the_next_ordinary_user_turn() {
    use futures::StreamExt;
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    let captured = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let calls = Arc::new(AtomicUsize::new(0));
    let first_goal_generation = Arc::new(std::sync::Mutex::new(None));
    let agent = make_agent_with_call_model_and_config(
        {
            let captured = captured.clone();
            let calls = calls.clone();
            let first_goal_generation = first_goal_generation.clone();
            Box::new(move |_request, _signal| {
                let session = captured.lock().unwrap().as_ref().unwrap().clone();
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                let generation = first.then(|| {
                    let identity = session.goal_runtime().unwrap().request_identity();
                    assert!(identity.accounting_enabled && identity.goal_work);
                    let generation = identity
                        .generation
                        .expect("the first provider request owns a goal generation");
                    *first_goal_generation.lock().unwrap() = Some(generation.clone());
                    generation
                });
                let completion_tail = futures::stream::once(async move {
                    if let Some(generation) = generation {
                        let runtime = session.goal_runtime().unwrap();
                        // The preceding MessageStop has registered this request's usage.
                        runtime.flush_progress().await.unwrap();
                        let before = session.thread_goal().await.unwrap().unwrap();
                        assert_eq!(before.goal_id, generation);
                        assert_eq!(before.status, ThreadGoalStatus::Active);
                        assert_eq!(before.tokens_used, 15);
                        let outcome = runtime.update_goal_status(ThreadGoalStatus::Complete).await;
                        assert!(matches!(outcome,
                            klaus_agent_core::goal::runtime::GoalUpdateOutcome::Updated(goal)
                            if goal.status == ThreadGoalStatus::Complete));
                        let after = session.thread_goal().await.unwrap().unwrap();
                        assert_eq!(after.goal_id, generation);
                        assert_eq!(after.status, ThreadGoalStatus::Complete);
                        assert_eq!(after.tokens_used, 15);
                    }
                    None
                });
                Box::pin(
                    futures::stream::iter(text_turn("finished"))
                        .chain(completion_tail.filter_map(futures::future::ready)),
                )
            })
        },
        Some(json!({"prewarm":false,"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let root = tempfile::tempdir().unwrap();
    let create = persistent_factory(agent, root.path(), Some("fixture objective"));
    let factory: CreateSession = {
        let captured = captured.clone();
        Arc::new(move |ctx| {
            let create = create.clone();
            let captured = captured.clone();
            Box::pin(async move {
                let session = create(ctx).await?;
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let mut client = persistent_client(factory, root.path());
    let session = start(&mut client).await;
    client.send(json!({"id":3,"method":"session/setGoal","params":{"sessionId":session,"objective":"fixture objective"}})).await;
    let (created, _) = checked_response(&mut client, 3).await;
    assert_eq!(
        created["result"]["threadGoal"]["status"], "active",
        "{created}"
    );
    client.send(json!({"id":4,"method":"session/steer","params":{"sessionId":session,"input":"queued after goal","priority":"next","submissionId":"goal-queued"}})).await;
    client.response_for(4).await;
    client.send(json!({"id":5,"method":"turn/start","params":{"sessionId":session,"input":"goal work","submissionId":"goal-turn"}})).await;
    let (_, seen) = client.response_for(5).await;
    until(&mut client, seen, |message| completed(message, "goal-turn")).await;
    client.send(json!({"id":6,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"goal-turn","submissionId":"after-goal"}})).await;
    let (_, seen) = client.response_for(6).await;
    let seen = until(&mut client, seen, |message| decision(message, "after-goal")).await;
    assert!(seen.iter().any(|message| decision(message, "after-goal")
        && message["params"]["event"]["pendingResume"]["reason"] == "not_completed"));
    client.send(json!({"id":7,"method":"turn/start","params":{"sessionId":session,"input":"ordinary new request","submissionId":"ordinary-turn"}})).await;
    let (_, seen) = client.response_for(7).await;
    until(&mut client, seen, |message| {
        completed(message, "ordinary-turn")
    })
    .await;
    client.send(json!({"id":8,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"ordinary-turn","submissionId":"ordinary-resume"}})).await;
    let (_, seen) = client.response_for(8).await;
    let seen = until(&mut client, seen, |message| {
        completed(message, "ordinary-resume")
    })
    .await;
    assert!(seen
        .iter()
        .any(|message| decision(message, "ordinary-resume")
            && message["params"]["event"]["pendingResume"]["started"] == true));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    client.close().await;
    let session = captured.lock().unwrap().take().unwrap();
    let goal = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(goal.status, ThreadGoalStatus::Complete);
    assert_eq!(
        Some(&goal.goal_id),
        first_goal_generation.lock().unwrap().as_ref(),
        "ordinary admissions preserve the first provider request's goal generation"
    );
    assert_eq!(
        goal.tokens_used, 15,
        "later ordinary admissions do not charge the completed goal"
    );
}

#[tokio::test]
async fn pending_resume_preserves_standard_goal_creation_accounting_and_continuation() {
    use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject};
    let goal_tool = |id, name, key, value: &str| {
        let mut input = RuntimeObject::new();
        input.insert(key, RuntimeJson::String(value.into()));
        let mut events = vec![ev::message_start(None)];
        events.extend(ev::tool_use(id, name, &RuntimeJson::Object(input)));
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        events
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let scripted = scripted_call_model(vec![
        text_turn("initial"),
        goal_tool(
            "create",
            "create_goal",
            "objective",
            "finish queued fixture",
        ),
        text_turn("goal first pass"),
        goal_tool("complete", "update_goal", "status", "complete"),
        text_turn("goal completed"),
    ]);
    let agent = make_agent_with_call_model_and_config(
        {
            let calls = calls.clone();
            Box::new(move |request, signal| {
                let index = calls.fetch_add(1, Ordering::SeqCst);
                assert!(index < 5, "initial plus four goal provider requests");
                scripted(request, signal)
            })
        },
        Some(json!({"prewarm":false,"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let root = tempfile::tempdir().unwrap();
    let mut client = persistent_client(persistent_factory(agent, root.path(), None), root.path());
    let session = start(&mut client).await;
    client.send(json!({"id":3,"method":"turn/start","params":{"sessionId":session,"input":"initial","submissionId":"initial"}})).await;
    let (_, seen) = client.response_for(3).await;
    until(&mut client, seen, |message| completed(message, "initial")).await;
    client.send(json!({"id":4,"method":"session/steer","params":{"sessionId":session,"input":"create and finish the fixture goal","priority":"next","submissionId":"queued-goal"}})).await;
    client.response_for(4).await;
    client.send(json!({"id":5,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"initial","submissionId":"goal-resume"}})).await;
    let (_, seen) = client.response_for(5).await;
    let mut seen = until(&mut client, seen, |message| {
        message["method"] == "goal.continuation"
            && message["params"]["priorSubmissionId"] == "goal-resume"
            && message["params"]["pending"] == true
    })
    .await;
    let reserved = seen
        .iter()
        .find(|message| {
            message["method"] == "goal.continuation"
                && message["params"]["priorSubmissionId"] == "goal-resume"
                && message["params"]["pending"] == true
        })
        .unwrap()["params"]
        .clone();
    let successor = reserved["successorSubmissionId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(successor, "goal-resume");
    seen = until(&mut client, seen, |message| completed(message, &successor)).await;
    for id in ["goal-resume", successor.as_str()] {
        assert_eq!(
            seen.iter()
                .filter(|message| message["method"] == "turn/started"
                    && message["params"]["submissionId"] == id)
                .count(),
            1,
            "each of the two actual goal admissions starts once: {seen:?}"
        );
        assert!(
            seen.iter().any(|message| completed(message, id)),
            "{seen:?}"
        );
        assert_eq!(
            seen.iter().filter(|message| completed(message, id)).count(),
            1,
            "each actual goal admission completes exactly once: {seen:?}"
        );
    }
    let predecessor_completed = seen
        .iter()
        .position(|message| completed(message, "goal-resume"))
        .unwrap();
    let successor_started = seen
        .iter()
        .position(|message| {
            message["method"] == "turn/started" && message["params"]["submissionId"] == successor
        })
        .unwrap();
    assert!(
        predecessor_completed < successor_started,
        "the predecessor completes before the distinct successor starts: {seen:?}"
    );
    assert_eq!(
        seen.iter()
            .filter(|message| message["method"] == "turn/started")
            .count(),
        2,
        "the existing goal driver must run its second distinct turn: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|message| message["method"] == "goal.continuation"
                && message["params"]["decisionId"] == reserved["decisionId"]
                && message["params"]["priorSubmissionId"] == "goal-resume"
                && message["params"]["successorSubmissionId"] == successor
                && message["params"]["pending"] == false
                && message["params"]["continued"] == true),
        "{seen:?}"
    );
    for name in ["create_goal", "update_goal"] {
        assert!(
            seen.iter()
                .any(|message| message["method"] == "item/completed"
                    && message["params"]["item"]["toolName"] == name
                    && message["params"]["item"]["status"] == "completed"
                    && message["params"]["item"]["result"]["ok"] == true),
            "{seen:?}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    client
        .send(json!({"id":6,"method":"session/goal","params":{"sessionId":session}}))
        .await;
    let (goal, _) = client.response_for(6).await;
    assert_eq!(
        goal["result"]["threadGoal"]["status"], "complete",
        "{goal:?}"
    );
    assert!(goal["result"]["threadGoal"]["tokensUsed"].as_i64().unwrap() > 0);
    assert_eq!(goal["result"]["threadGoal"]["tokensUsed"], 30,
        "two responses between creation and completion charge 15 tokens each; earlier and post-completion requests do not: {goal}");
    client.send(json!({"id":7,"method":"session/resumePending","params":{"sessionId":session,"afterSubmissionId":"goal-resume","submissionId":"no-goal-reentry"}})).await;
    let (_, seen) = client.response_for(7).await;
    let seen = until(&mut client, seen, |message| {
        decision(message, "no-goal-reentry")
    })
    .await;
    assert!(seen
        .iter()
        .any(|message| decision(message, "no-goal-reentry")
            && message["params"]["event"]["pendingResume"]["reason"] == "not_completed"));
    client.close().await;
}
