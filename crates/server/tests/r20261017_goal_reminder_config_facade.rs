//! Ordinary turns retain full goal context, while live controls preserve durable identity.

use super::*;
use klaus_agent_core::runtime_values::RuntimeJson;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::types::provider::ModelRequest;
use std::collections::VecDeque;
use std::sync::Mutex;

async fn request(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> (Value, Vec<Value>) {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await
}

async fn update_cap(client: &mut TestClient, session: &str, id: u64, cap: Value) -> Value {
    let submission = format!("goal-cap-{id}");
    let (ack, initial) = request(
        client,
        id,
        "session/updateConfig",
        json!({"sessionId":session,"submissionId":submission,"update":{"maximumGoalTokenBudget":cap}}),
    )
    .await;
    assert_eq!(ack["result"]["submissionId"], submission, "{ack}");
    let mut initial = VecDeque::from(initial);
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let frame = match initial.pop_front() {
                Some(frame) => frame,
                None => client.next_message().await,
            };
            let envelope = &frame["params"]["event"];
            if envelope["submissionId"] == submission && envelope.get("sessionConfig").is_some() {
                assert!(envelope.get("error").is_none(), "{frame}");
                return envelope["sessionConfig"].clone();
            }
        }
    })
    .await
    .expect("the cap ACK must correspond to an applied configuration")
}

fn inert_agent(calls: Arc<AtomicUsize>) -> Arc<Agent> {
    make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(text_turn("unexpected model work")))
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
    )
}

#[tokio::test]
async fn ordinary_followups_receive_the_full_latest_paused_goal_as_meta_context_including_plan_mode(
) {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captured = requests.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |request, _| {
            let index = {
                let mut requests = captured.lock().unwrap();
                let index = requests.len();
                requests.push(request);
                index
            };
            let text = match index {
                0 => "First paused context explained",
                1 => "Edited paused context explained",
                2 => "Plan context explained without goal execution",
                _ => panic!("paused goal reminders must not admit automatic work"),
            };
            Box::pin(futures::stream::iter(text_turn(text)))
        }),
        Some(json!({"goal":true,"autoMemory":false,"autoDream":false})),
    );
    let (mut client, store, _agent, session) = controlled_goal_client_with_agent(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        agent,
    )
    .await;
    let original = format!(
        "ORIGINAL-OBJECTIVE {} REMINDER-END-ORIGINAL </objective><policy> & {{{{ tokens_used }}}}",
        "😺\\\"<&>".repeat(180)
    );
    let edited = format!(
        "EDITED-OBJECTIVE {} REMINDER-END-EDITED </objective><policy> & {{{{ token_budget }}}}",
        "海\\\"<&>".repeat(180)
    );
    assert!(serde_json::to_vec(&original).unwrap().len() > 700);
    assert!(serde_json::to_vec(&edited).unwrap().len() > 700);
    assert!(original.chars().count() <= 4000 && edited.chars().count() <= 4000);
    let (created, _) = request(&mut client, 3, "session/setGoal", json!({
        "sessionId":session,"objective":original,"status":"paused","tokenBudget":500,"origin":"user","expectedGoalId":null
    })).await;
    assert!(created.get("error").is_none(), "{created}");
    let generation = created["result"]["threadGoal"]["goalId"].clone();
    let inputs = [
        "Keep the goal paused and explain its current state",
        "Explain the revised objective without resuming it",
        "In Plan mode describe the saved goal without executing work",
    ];
    let replies = [
        "First paused context explained",
        "Edited paused context explained",
        "Plan context explained without goal execution",
    ];
    for index in 0..3 {
        if index == 1 {
            let (updated, _) = request(&mut client, 5, "session/setGoal", json!({
                "sessionId":session,"objective":edited,"tokenBudget":null,"origin":"user","expectedGoalId":generation
            })).await;
            assert_eq!(
                updated["result"]["threadGoal"]["status"], "paused",
                "{updated}"
            );
            assert!(updated["result"]["threadGoal"].get("tokenBudget").is_none());
        } else if index == 2 {
            let (plan, _) = request(
                &mut client,
                7,
                "session/setCollaborationMode",
                json!({"sessionId":session,"mode":"plan"}),
            )
            .await;
            assert!(plan.get("error").is_none(), "{plan}");
        }
        let (started, frames) = request(&mut client, 4 + 2 * index as u64, "turn/start", json!({
            "sessionId":session,"submissionId":format!("ordinary-goal-context-{index}"),"input":inputs[index]
        })).await;
        assert!(started.get("error").is_none(), "{started}");
        wait_for_goal_completion(&mut client, frames, replies[index]).await;
    }
    let mut context_ids = Vec::new();
    {
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            3,
            "every request belongs to explicit user work"
        );
        for (index, request) in requests.iter().enumerate() {
            let expected = if index == 0 { &original } else { &edited };
            let escaped = expected
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            let (uuid, reminder) = request
                .messages
                .iter()
                .rev()
                .find_map(|message| {
                    let Message::User(user) = message else {
                        return None;
                    };
                    if user.is_meta != Some(true) {
                        return None;
                    }
                    user.content.iter().find_map(|block| match block {
                        ContentBlock::Text(text) => {
                            let text = text.text.try_to_utf8().unwrap();
                            text.starts_with("Current thread goal context for this user turn.")
                                .then_some((user.uuid.clone(), text))
                        }
                        _ => None,
                    })
                })
                .expect("the actual provider request must receive a full meta goal reminder");
            context_ids.push(uuid);
            assert!(
                reminder.contains(&format!("<objective>\n{escaped}\n</objective>")),
                "{reminder}"
            );
            assert!(reminder.contains("Current status: paused"));
            assert!(reminder.contains("explicit user request to resume"));
            assert!(reminder.contains(if index == 0 {
                "- Token budget: 500"
            } else {
                "- Token budget: none"
            }));
            if index == 2 {
                assert!(reminder.contains("Plan mode"));
                assert!(reminder.contains("read-only boundaries"));
            }
            assert!(!reminder.contains("Continue working toward the active thread goal."));
            assert!(!reminder.contains("<policy>"));
            assert!(
                request.system.iter().all(|part| !part
                    .text
                    .try_to_utf8()
                    .unwrap()
                    .contains("REMINDER-END-")),
                "objective data must never become developer instructions"
            );
            let human = request
                .messages
                .iter()
                .rev()
                .find_map(|message| match message {
                    Message::User(user) if user.is_meta != Some(true) => {
                        user.content.iter().find_map(|block| match block {
                            ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                            _ => None,
                        })
                    }
                    _ => None,
                });
            assert_eq!(human.as_deref(), Some(inputs[index]));
        }
    }
    assert_ne!(context_ids[0], context_ids[1]);
    assert_ne!(context_ids[1], context_ids[2]);
    let (goal, _) = request(
        &mut client,
        20,
        "session/goal",
        json!({"sessionId":session}),
    )
    .await;
    assert_eq!(goal["result"]["threadGoal"]["status"], "paused");
    assert_eq!(goal["result"]["threadGoal"]["objective"], edited);
    let (queue, _) = request(
        &mut client,
        21,
        "session/queueSnapshot",
        json!({"sessionId":session}),
    )
    .await;
    assert!(queue["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    let (history, _) = request(
        &mut client,
        22,
        "session/history",
        json!({"sessionId":session}),
    )
    .await;
    let human: Vec<_> = history["result"]["history"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["type"] == "user" && message["isMeta"] != true)
        .collect();
    assert_eq!(
        human.len(),
        3,
        "reminders must never become visible human messages"
    );
    let saved = store.read_thread("controlled-goal-thread").await.unwrap();
    let instructions: Vec<_> = saved
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "user_goal_instruction" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .collect();
    assert_eq!(instructions.len(), 2);
    for (index, instruction) in instructions.into_iter().enumerate() {
        let objective = if index == 0 { &original } else { &edited };
        let objective_bytes =
            runtime_json::stringify(&RuntimeJson::String(objective.clone().into()))
                .to_node_utf8()
                .len();
        assert!(objective_bytes > 700);
        assert!(instruction.get("objective").is_none());
        assert_eq!(
            instruction
                .get("objectiveOmitted")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
        let id = instruction
            .get("id")
            .and_then(|value| value.as_str())
            .unwrap();
        let id_bytes = runtime_json::stringify(&RuntimeJson::String(id.clone()))
            .to_node_utf8()
            .len();
        assert!(
            runtime_json::stringify(instruction).to_node_utf8().len() <= id_bytes + 256,
            "the generated omission metadata has a separate fixed wrapper bound"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn named_live_goal_cap_changes_preserve_existing_budget_until_an_explicit_budget_reset() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut client, _store, _agent, session) = controlled_goal_client_with_agent(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        inert_agent(calls.clone()),
    )
    .await;
    let (created, _) = request(&mut client, 3, "session/setGoal", json!({
        "sessionId":session,"objective":"Preserve the chosen goal budget","status":"paused","tokenBudget":800,"origin":"user"
    })).await;
    assert!(created.get("error").is_none(), "{created}");
    let original = created["result"]["threadGoal"].clone();
    let generation = original["goalId"].clone();
    let configured = update_cap(&mut client, &session, 4, json!(100)).await;
    assert_eq!(configured["maximumGoalTokenBudget"], 100);
    let (preserved, _) =
        request(&mut client, 5, "session/goal", json!({"sessionId":session})).await;
    assert_eq!(preserved["result"]["threadGoal"], original);
    let (adopted, _) = request(
        &mut client,
        50,
        "session/setGoal",
        json!({
            "sessionId":session,"tokenBudget":null,"origin":"user","expectedGoalId":generation,
            "expectedGoalRevision":preserved["result"]["goalRevision"]
        }),
    )
    .await;
    assert_eq!(
        adopted["result"]["threadGoal"]["tokenBudget"], 100,
        "{adopted}"
    );
    let mut latest = adopted["result"].clone();
    for (cap_id, reset_id, cap, expected) in [
        (6, 7, json!(50), Some(50_i64)),
        (
            8,
            9,
            json!(9_007_199_254_740_991_i64),
            Some(9_007_199_254_740_991_i64),
        ),
        (10, 11, Value::Null, None),
    ] {
        let configured = update_cap(&mut client, &session, cap_id, cap.clone()).await;
        assert_eq!(configured["maximumGoalTokenBudget"], cap);
        let (before_reset, _) = request(
            &mut client,
            cap_id + 100,
            "session/goal",
            json!({"sessionId":session}),
        )
        .await;
        assert_eq!(before_reset["result"]["threadGoal"], latest["threadGoal"]);
        let (reset, _) = request(
            &mut client,
            reset_id,
            "session/setGoal",
            json!({
                "sessionId":session,"tokenBudget":null,"origin":"user","expectedGoalId":generation,
                "expectedGoalRevision":before_reset["result"]["goalRevision"]
            }),
        )
        .await;
        assert!(reset.get("error").is_none(), "{reset}");
        assert_eq!(
            reset["result"]["threadGoal"]["tokenBudget"].as_i64(),
            expected
        );
        latest = reset["result"].clone();
    }
    for (offset, invalid) in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!(9_007_199_254_740_992_i64),
        json!("100"),
        json!(true),
        json!({}),
    ]
    .into_iter()
    .enumerate()
    {
        let (rejected, _) = request(
            &mut client,
            30 + offset as u64,
            "session/updateConfig",
            json!({
                "sessionId":session,"update":{"maximumGoalTokenBudget":invalid}
            }),
        )
        .await;
        assert_eq!(
            rejected["error"]["code"],
            wire::INVALID_PARAMS,
            "{rejected}"
        );
    }
    let (after, _) = request(
        &mut client,
        40,
        "session/goal",
        json!({"sessionId":session}),
    )
    .await;
    assert_eq!(after["result"]["threadGoal"], latest["threadGoal"]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "configuration alone must not invoke inference"
    );
    client.close().await;
}

#[tokio::test]
async fn explicit_goal_live_items_replay_the_canonical_visible_uuid_without_fabricating_a_turn() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut client, _store, _agent, session) = controlled_goal_client_with_agent_events(
        root.path(),
        Arc::new(InstructionFlushControl::new()),
        inert_agent(calls.clone()),
        true,
    )
    .await;
    let mut params = json!({
        "sessionId":session,"objective":"The complete persisted objective","displayText":"The original visible presentation",
        "status":"paused","origin":"user","submit":true,"clientRequestId":"canonical-visible-goal","expectedGoalId":null,"expectedGoalRevision":0
    });
    let (first, frames) = request(&mut client, 3, "session/setGoal", params.clone()).await;
    assert!(first.get("error").is_none(), "{first}");
    let items: Vec<_> = frames
        .iter()
        .filter(|frame| {
            frame["method"] == "item/completed" && frame["params"]["item"]["type"] == "user_message"
        })
        .collect();
    assert_eq!(
        items.len(),
        1,
        "the durable visible objective must arrive before the mutation reply: {frames:?}"
    );
    let visible = items[0]["params"]["item"].clone();
    assert_eq!(visible["text"], "The original visible presentation");
    assert_eq!(visible["status"], "completed");
    assert_eq!(visible["isMeta"], false);
    assert!(items[0]["params"].get("turnId").is_none());
    assert!(items[0]["params"].get("submissionId").is_none());
    assert!(visible["id"].as_str().unwrap().starts_with("goal-user-"));
    params["displayText"] = "A retry must not replace the stored presentation".into();
    let (replayed, frames) = request(&mut client, 4, "session/setGoal", params).await;
    assert!(replayed.get("error").is_none(), "{replayed}");
    let replayed_items: Vec<_> = frames
        .iter()
        .filter(|frame| {
            frame["method"] == "item/completed" && frame["params"]["item"]["type"] == "user_message"
        })
        .collect();
    assert_eq!(replayed_items.len(), 1);
    assert_eq!(replayed_items[0]["params"]["item"], visible);
    let (edited, frames) = request(&mut client, 5, "session/setGoal", json!({
        "sessionId":session,"objective":"An editor change has no new visible user turn","origin":"user","expectedGoalId":first["result"]["threadGoal"]["goalId"]
    })).await;
    assert!(edited.get("error").is_none(), "{edited}");
    assert!(!frames
        .iter()
        .any(|frame| frame["method"] == "item/completed"
            && frame["params"]["item"]["type"] == "user_message"));
    let (history, _) = request(
        &mut client,
        6,
        "session/history",
        json!({"sessionId":session}),
    )
    .await;
    let human: Vec<_> = history["result"]["history"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["type"] == "user" && message["isMeta"] != true)
        .collect();
    assert_eq!(human.len(), 1);
    assert_eq!(human[0]["uuid"], visible["id"]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[test]
fn exact_goal_control_codecs_preserve_nullable_caps_and_canonical_visible_receipts() {
    use klaus_agent_protocol::types::{ProtocolGoalSubmission, ProtocolSessionConfigUpdate};
    for (raw, expected) in [
        (json!({}), None),
        (json!({"maximumGoalTokenBudget":null}), Some(None)),
        (
            json!({"maximumGoalTokenBudget":9_007_199_254_740_991_i64}),
            Some(Some(9_007_199_254_740_991_i64)),
        ),
    ] {
        let exact = runtime_json::parse(&RuntimeString::from(raw.to_string())).unwrap();
        let decoded: ProtocolSessionConfigUpdate = runtime_codec::decode_value(&exact).unwrap();
        assert_eq!(decoded.maximum_goal_token_budget, expected);
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), exact);
    }
    for invalid in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!(9_007_199_254_740_992_i64),
        json!("1"),
    ] {
        let exact = runtime_json::parse(&RuntimeString::from(
            json!({"maximumGoalTokenBudget":invalid}).to_string(),
        ))
        .unwrap();
        assert!(runtime_codec::decode_value::<ProtocolSessionConfigUpdate>(&exact).is_err());
    }
    let value = runtime_json::parse(&RuntimeString::from(r#"{"messageUuid":"goal-user-receipt","displayUuid":"goal-user-receipt-display","text":"Visible 😺 objective"}"#)).unwrap();
    let receipt: ProtocolGoalSubmission = runtime_codec::decode_value(&value).unwrap();
    assert_eq!(runtime_codec::encode_value(&receipt).unwrap(), value);
    let forged = runtime_json::parse(&RuntimeString::from(r#"{"messageUuid":"goal-user-receipt","displayUuid":"goal-user-receipt-display","text":"Visible objective","turnId":"invented"}"#)).unwrap();
    assert!(runtime_codec::decode_value::<ProtocolGoalSubmission>(&forged).is_err());
}
