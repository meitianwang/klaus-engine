//! Defect: automatic completed-goal cleanup revoked the completing turn before its final report.
//! Root cause: automatic cleanup used the same immediate authority removal as human clear.
//! Work item: 20261018 completed-goal turn settlement.

use super::*;
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;

struct HoldCompletion {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}
#[async_trait::async_trait]
impl HookFn for HoldCompletion {
    async fn run(&self, input: HookInput) -> HookOutcome {
        assert_eq!(input.tool_name.as_ref().unwrap(), "update_goal");
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        HookOutcome::default()
    }
}

async fn clear_while_completion_is_held(automatic: bool) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            let call = captured.fetch_add(1, Ordering::SeqCst);
            let mut events = vec![ev::message_start(None)];
            let stop = match call {
                0 => {
                    let input =
                        runtime_json::parse(&RuntimeString::from(r#"{"status":"complete"}"#))
                            .unwrap();
                    events.extend(ev::tool_use("finish-before-report", "update_goal", &input));
                    ModelStopReason::ToolUse
                }
                1 => {
                    events.push(ev::text("Final report was delivered."));
                    ModelStopReason::EndTurn
                }
                _ => panic!("no extra request after the completed report"),
            };
            events.push(ev::stop(
                Some(stop),
                Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                }),
            ));
            Box::pin(futures::stream::iter(events))
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let control = Arc::new(InstructionFlushControl::new());
    let store = Arc::new(ControlledStore {
        inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        })),
        control: control.clone(),
    });
    let factory: CreateSession = Arc::new({
        let agent = agent.clone();
        let store = store.clone();
        let entered = entered.clone();
        let release = release.clone();
        let home = root.path().to_string_lossy().into_owned();
        move |ctx: SessionContext| {
            let agent = agent.clone();
            let store = store.clone();
            let home = home.clone();
            let hook = Hook {
                event: HookEvent::PostToolUse,
                matcher: Some("update_goal".into()),
                handler: HookHandler::Fn(Arc::new(HoldCompletion {
                    entered: entered.clone(),
                    release: release.clone(),
                })),
                is_async: false,
            };
            Box::pin(async move {
                agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            id: ctx.init.thread_id,
                            user_config_dir: Some(home.into()),
                            hooks: Some(vec![hook]),
                            approval_handler: Some(ctx.approval_handler),
                            elicit_handler: Some(ctx.elicit_handler),
                            config: Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| Box::new(error) as BoxError)
            })
        }
    });
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let mut saved = TestClient::start_with(
        Arc::new(|_| Box::pin(async { panic!("saved reader must not construct a session") })),
        Some(lifecycle.clone()),
    );
    saved.initialize().await;
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    client.send(json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"completion-clear"},"capabilities":{"productEvents":true}}})).await;
    client.response_for(1).await;
    client.send(json!({"id":2,"method":"session/start","params":{"init":{"threadId":"completion-clear-thread"}}})).await;
    let (opened, _) = client.response_for(2).await;
    let session = opened["result"]["sessionId"].as_str().unwrap().to_owned();
    client
        .send(
            json!({"id":3,"method":"session/setGoal","params":{"sessionId":session,
        "objective":"Complete the work and deliver the final report", "origin":"user","submit":true,
        "clientRequestId":"completion-clear-request","expectedGoalId":null}}),
        )
        .await;
    let (created, _) = client.response_for(3).await;
    assert!(created.get("error").is_none(), "{created}");
    let generation = created["result"]["threadGoal"]["goalId"].clone();
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the actual completion tool holds the final provider request"
    );
    let completed = klaus_agent_core::goal::store::get_record("completion-clear-thread")
        .await
        .unwrap();
    assert_eq!(
        completed.goal.as_ref().unwrap().status,
        klaus_agent_core::goal::ThreadGoalStatus::Complete
    );
    client
        .send(
            json!({"id":4,"method":"session/clearGoal","params":{"sessionId":session,
        "origin":if automatic {"automatic"} else {"user"},"expectedGoalId":generation,
        "expectedGoalRevision":completed.revision}}),
        )
        .await;
    let mut frames = Vec::new();
    if automatic {
        tokio::time::timeout(REPLY_TIMEOUT, control.completed.notified())
            .await
            .unwrap();
        saved
            .send(json!({"id":5,"method":"session/goal","params":{"threadId":"completion-clear-thread"}}))
            .await;
        let (during, observed) = saved.response_for(5).await;
        assert!(during.get("error").is_none(), "{during}");
        frames.extend(observed);
        assert_eq!(during["result"]["threadGoal"]["goalId"], generation,
            "automatic cleanup must retain the completing turn's generation before the final report");
        assert_eq!(during["result"]["threadGoal"]["status"], "complete");
        assert_eq!(
            during["result"]["goalRevision"], completed.revision,
            "the saved reader observes the exact reserved completion revision"
        );
    } else {
        let (cleared, observed) = client.response_for(4).await;
        frames.extend(observed);
        assert_eq!(
            cleared["result"]["goalCleared"], true,
            "human clear revokes immediately"
        );
    }
    release.add_permits(1);
    let mut clear_reply = None;
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            assert!(frames.len() < 128, "bounded completing-turn frames");
            let frame = client.next_message().await;
            if frame["id"] == 4 {
                clear_reply = Some(frame.clone());
            }
            let terminal = frame["method"] == "turn/completed" || frame["method"] == "turn/failed";
            frames.push(frame);
            if terminal {
                break;
            }
        }
    })
    .await
    .unwrap();
    if automatic {
        assert!(
            frames
                .iter()
                .any(|frame| frame["method"] == "turn/completed"
                    && frame["params"]["result"]["finalText"] == "Final report was delivered."),
            "automatic cleanup cannot interrupt the completing turn's final report: {frames:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        if clear_reply.is_none() {
            clear_reply = Some(client.response_for(4).await.0);
        }
        let conflict = clear_reply.unwrap();
        assert_eq!(
            conflict["error"]["data"]["code"], "GOAL_CONFLICT",
            "settlement retains the original revision precondition: {conflict}"
        );
        assert_eq!(
            conflict["error"]["message"],
            "the goal changed before clear"
        );
        client
            .send(json!({"id":6,"method":"session/goal","params":{"sessionId":session}}))
            .await;
        let (refreshed, _) = client.response_for(6).await;
        assert!(refreshed.get("error").is_none(), "{refreshed}");
        assert_eq!(refreshed["result"]["threadGoal"]["goalId"], generation);
        client.send(json!({"id":7,"method":"session/clearGoal","params":{"sessionId":session,
            "origin":"automatic","expectedGoalId":generation,"expectedGoalRevision":refreshed["result"]["goalRevision"]}})).await;
        let (cleared, _) = client.response_for(7).await;
        assert_eq!(cleared["result"]["goalCleared"], true);
        let saved = store.read_thread("completion-clear-thread").await.unwrap();
        assert_eq!(
            klaus_agent_core::goal::retained_context::completions_from_rollout(&saved.raw_items)
                .len(),
            1
        );
    } else {
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "human revocation forbids the final provider request"
        );
        assert!(
            klaus_agent_core::goal::store::get_goal("completion-clear-thread")
                .await
                .unwrap()
                .is_none()
        );
    }
    saved.close().await;
    client.close().await;
}

#[tokio::test]
async fn automatic_completion_clear_waits_for_the_same_turn_final_report() {
    clear_while_completion_is_held(true).await;
}

#[tokio::test]
async fn human_clear_still_revokes_a_completing_turn_before_its_final_report() {
    clear_while_completion_is_held(false).await;
}
