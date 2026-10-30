//! Structured goal reads and writes retain exact native values at the JSON boundary.

use super::*;
use klaus_agent_core::goal::{store as goal_store, ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::runtime_values::runtime_codec;
use klaus_agent_core::schemas::message::Message;
use std::sync::Mutex;

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

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

fn inert_agent(calls: Arc<AtomicUsize>) -> Arc<Agent> {
    make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(text_turn("unexpected inference")))
        }),
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    )
}

fn instruction_count(items: &[RolloutItem]) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(item,
                RolloutItem::Event(event) if event.event == "user_goal_instruction"
            )
        })
        .count()
}

fn submitted_objective_count(items: &[RolloutItem]) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(item,
                RolloutItem::Message { message: Message::User(user) }
                    if user.uuid.starts_with("goal-user-")
                        && user.is_meta != Some(true)
            )
        })
        .count()
}

#[tokio::test]
async fn named_wire_preflight_precedes_authorization_and_native_unsafe_rows_remain_exact() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = inert_agent(calls.clone());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let live = Arc::new(Mutex::new(None::<Arc<Session>>));
    let creates = Arc::new(AtomicUsize::new(0));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let live = live.clone();
        let creates = creates.clone();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let live = live.clone();
            let creates = creates.clone();
            Box::pin(async move {
                creates.fetch_add(1, Ordering::SeqCst);
                let session = agent.open_session(store, StartSessionOptions {
                    id: ctx.init.thread_id,
                    approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                    config: Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
                    ..Default::default()
                }).await.map_err(|error| Box::new(error) as BoxError)?;
                session
                    .set_maximum_goal_token_budget(Some(i64::MAX))
                    .await
                    .map_err(|error| error.try_to_utf8().unwrap_or_default())?;
                *live.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let mut client = TestClient::start_with(factory, Some(lifecycle.clone()));
    client.initialize().await;
    let (started, _) = request(
        &mut client,
        2,
        "session/start",
        json!({
            "init":{"threadId":"wire-safety-native-row"}
        }),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let session_id = started["result"]["sessionId"].as_str().unwrap().to_owned();
    let (read, _) = request(
        &mut client,
        3,
        "session/goal",
        json!({"sessionId":session_id}),
    )
    .await;
    assert!(read.get("error").is_none(), "{read}");
    assert_eq!(read["result"]["goalPursuitEligible"], true);
    assert_eq!(
        creates.load(Ordering::SeqCst),
        0,
        "a lazy read must not bind a Session"
    );
    let intent = json!({"sessionId":session_id,"objective":"Exact human objective",
        "status":"paused","origin":"user","submit":true,
        "clientRequestId":"unsafe-cap-then-safe-retry","expectedGoalId":null,"expectedGoalRevision":0});
    let (rejected, frames) = request(&mut client, 4, "session/setGoal", intent.clone()).await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tokenBudget"),
        "{rejected}"
    );
    assert!(!frames
        .iter()
        .any(|frame| frame["params"]["item"]["type"] == "user_message"));
    let snapshot = goal_store::get_record("wire-safety-native-row")
        .await
        .unwrap();
    assert!(snapshot.goal.is_none());
    assert_eq!(snapshot.revision, 0);
    assert!(goal_store::get_goal_request_receipt(
        "wire-safety-native-row",
        "unsafe-cap-then-safe-retry"
    )
    .await
    .unwrap()
    .is_none());
    let raw = store
        .read_thread("wire-safety-native-row")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(
        instruction_count(&raw),
        0,
        "unsafe projection must fail before trusted evidence"
    );
    assert_eq!(
        submitted_objective_count(&raw),
        0,
        "unsafe projection must fail before visible input"
    );
    let session = live.lock().unwrap().clone().unwrap();
    assert_eq!(session.maximum_goal_token_budget(), Some(i64::MAX));
    session
        .set_maximum_goal_token_budget(Some(100))
        .await
        .unwrap();
    let (accepted, _) = request(&mut client, 5, "session/setGoal", intent.clone()).await;
    assert!(accepted.get("error").is_none(), "{accepted}");
    assert_eq!(accepted["result"]["threadGoal"]["tokenBudget"], 100);
    let raw = store
        .read_thread("wire-safety-native-row")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(instruction_count(&raw), 1);
    assert_eq!(submitted_objective_count(&raw), 1);

    session.set_maximum_goal_token_budget(None).await.unwrap();
    session
        .set_thread_goal(None, None, Some(Some(i64::MAX)))
        .await
        .unwrap();
    let native = goal_store::get_record("wire-safety-native-row")
        .await
        .unwrap();
    assert_eq!(native.goal.as_ref().unwrap().token_budget, Some(i64::MAX));
    assert_eq!(session.thread_goal().await.unwrap(), native.goal);
    let (replayed, _) = request(&mut client, 6, "session/setGoal", intent).await;
    assert!(replayed.get("error").is_none(), "{replayed}");
    assert_eq!(
        replayed["result"]["threadGoal"], accepted["result"]["threadGoal"],
        "a matching receipt must replay its original safe snapshot before current-value checks"
    );
    assert_eq!(
        replayed["result"]["goalRevision"],
        accepted["result"]["goalRevision"]
    );
    assert_eq!(
        goal_store::get_record("wire-safety-native-row")
            .await
            .unwrap(),
        native
    );
    let (unsafe_read, _) = request(
        &mut client,
        7,
        "session/goal",
        json!({"sessionId":session_id}),
    )
    .await;
    assert!(
        unsafe_read["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tokenBudget"),
        "{unsafe_read}"
    );
    assert!(
        unsafe_read.get("result").is_none(),
        "native i64 must not be rounded into JSON"
    );
    let (unsafe_edit, _) = request(
        &mut client,
        8,
        "session/setGoal",
        json!({
            "sessionId":session_id,"status":"paused","origin":"user"
        }),
    )
    .await;
    assert!(
        unsafe_edit["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tokenBudget"),
        "{unsafe_edit}"
    );
    assert_eq!(
        goal_store::get_record("wire-safety-native-row")
            .await
            .unwrap(),
        native
    );
    let raw = store
        .read_thread("wire-safety-native-row")
        .await
        .unwrap()
        .raw_items;
    assert_eq!(
        instruction_count(&raw),
        1,
        "unsafe edit must not append authorization"
    );
    assert_eq!(submitted_objective_count(&raw), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client.close().await;

    let never_create: CreateSession =
        Arc::new(|_| Box::pin(async { panic!("saved JSON reads must not create a Session") }));
    let mut saved = TestClient::start_with(never_create, Some(lifecycle));
    saved.initialize().await;
    let (unsafe_saved, _) = request(
        &mut saved,
        2,
        "session/goal",
        json!({
            "threadId":"wire-safety-native-row"
        }),
    )
    .await;
    assert!(
        unsafe_saved["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tokenBudget"),
        "{unsafe_saved}"
    );
    assert!(unsafe_saved.get("result").is_none());
    assert_eq!(
        goal_store::get_record("wire-safety-native-row")
            .await
            .unwrap(),
        native
    );
    let (endpoint_alive, _) = request(
        &mut saved,
        3,
        "session/goal",
        json!({"threadId":"empty-after-unsafe"}),
    )
    .await;
    assert!(endpoint_alive.get("error").is_none(), "{endpoint_alive}");
    assert_eq!(endpoint_alive["result"]["goalPursuitEligible"], false);
    assert!(endpoint_alive["result"]["goalPursuitEligibilityNotice"]
        .as_str()
        .unwrap()
        .contains("Open this thread"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    saved.close().await;
}

#[tokio::test]
async fn lazy_goal_eligibility_uses_actual_scope_and_is_independent_of_feature_enablement() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = inert_agent(calls.clone());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let creates = Arc::new(AtomicUsize::new(0));
    let never_create: CreateSession = {
        let creates = creates.clone();
        Arc::new(move |_| {
            creates.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                panic!("availability must be resolved before Session/provider construction")
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let mut client = TestClient::start_with(never_create, Some(lifecycle));
    client.initialize().await;
    for (index, (thread, config, expected)) in [
        ("lazy-feature-disabled", json!({"goal":false}), true),
        (
            "lazy-task-scoped",
            json!({"goal":true,"taskResourceScope":{
                "taskId":"limited-task","mode":"scoped-auto","readRoots":[],"writeRoots":[]
            }}),
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let id = 10 + index as u64 * 4;
        let (started, _) = request(
            &mut client,
            id,
            "session/start",
            json!({
                "init":{"threadId":thread,"config":config}
            }),
        )
        .await;
        assert!(started.get("error").is_none(), "{started}");
        let session = started["result"]["sessionId"].clone();
        let (read, _) = request(
            &mut client,
            id + 1,
            "session/goal",
            json!({"sessionId":session}),
        )
        .await;
        assert!(read.get("error").is_none(), "{read}");
        assert_eq!(read["result"]["goalPursuitEligible"], expected, "{read}");
        assert!(read["result"].get("goalPursuitEligibilityNotice").is_none());
        assert_eq!(read["result"]["goalRevision"], 0);
        assert_eq!(
            goal_store::thread_goal_eligibility(thread).await.unwrap(),
            Some(expected)
        );
        let (closed, _) = request(
            &mut client,
            id + 2,
            "session/close",
            json!({"sessionId":session}),
        )
        .await;
        assert!(closed.get("error").is_none(), "{closed}");
        let (saved, _) = request(
            &mut client,
            id + 3,
            "session/goal",
            json!({"threadId":thread}),
        )
        .await;
        assert!(saved.get("error").is_none(), "{saved}");
        assert_eq!(saved["result"]["goalPursuitEligible"], expected);
        assert!(saved["result"]
            .get("goalPursuitEligibilityNotice")
            .is_none());
    }
    let (unknown, _) = request(
        &mut client,
        30,
        "session/goal",
        json!({"threadId":"unknown-goal-ownership"}),
    )
    .await;
    assert_eq!(unknown["result"]["goalPursuitEligible"], false);
    assert_eq!(
        unknown["result"]["goalPursuitEligibilityNotice"],
        "Open this thread to determine whether goal pursuit is available."
    );
    assert_eq!(
        goal_store::thread_goal_eligibility("unknown-goal-ownership")
            .await
            .unwrap(),
        None
    );
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn lazy_goal_read_without_an_endpoint_classifier_reports_explicit_unavailability() {
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let creates = Arc::new(AtomicUsize::new(0));
    let never_create: CreateSession = {
        let creates = creates.clone();
        Arc::new(move |_| {
            creates.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                panic!("a missing classifier must not fall back to provider startup")
            })
        })
    };
    let mut client = TestClient::start(never_create);
    client.initialize().await;
    let (started, _) = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"classifier-unavailable"}}),
    )
    .await;
    let session = started["result"]["sessionId"].clone();
    let (read, _) = request(&mut client, 3, "session/goal", json!({"sessionId":session})).await;
    assert!(read.get("error").is_none(), "{read}");
    assert_eq!(read["result"]["goalPursuitEligible"], false);
    assert_eq!(
        read["result"]["goalPursuitEligibilityNotice"],
        "Open this thread to determine whether goal pursuit is available."
    );
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert_eq!(
        goal_store::thread_goal_eligibility("classifier-unavailable")
            .await
            .unwrap(),
        None
    );
    client.close().await;
}

#[test]
fn strict_goal_wire_encoding_refuses_unsafe_native_fields_and_revision() {
    let safe = ThreadGoal {
        thread_id: "native-wire-fixture".into(),
        goal_id: "safe-goal".into(),
        objective: "Exact objective".into(),
        status: ThreadGoalStatus::Paused,
        token_budget: Some(MAX_SAFE_INTEGER),
        tokens_used: 0,
        time_used_seconds: 0,
        created_at_ms: MAX_SAFE_INTEGER,
        updated_at_ms: MAX_SAFE_INTEGER,
    };
    let value = runtime_codec::encode_value(&safe).unwrap();
    let text = klaus_agent_core::runtime_values::runtime_json::stringify(&value)
        .try_to_utf8()
        .unwrap();
    assert!(text.contains("9007199254740991"));
    for field in [
        "tokenBudget",
        "tokensUsed",
        "timeUsedSeconds",
        "createdAtMs",
        "updatedAtMs",
    ] {
        let mut unsafe_goal = safe.clone();
        match field {
            "tokenBudget" => unsafe_goal.token_budget = Some(i64::MAX),
            "tokensUsed" => unsafe_goal.tokens_used = i64::MAX,
            "timeUsedSeconds" => unsafe_goal.time_used_seconds = i64::MAX,
            "createdAtMs" => unsafe_goal.created_at_ms = i64::MAX,
            "updatedAtMs" => unsafe_goal.updated_at_ms = i64::MAX,
            _ => unreachable!(),
        }
        assert!(runtime_codec::encode_value(&unsafe_goal)
            .unwrap_err()
            .to_string()
            .contains(field));
        let protocol = klaus_agent_protocol::types::ProtocolThreadGoal::from(&unsafe_goal);
        assert!(runtime_codec::encode_value(&protocol)
            .unwrap_err()
            .to_string()
            .contains(field));
    }
    let event = klaus_agent_protocol::types::ProtocolEvent {
        goal_revision: Some(MAX_SAFE_INTEGER + 1),
        ..Default::default()
    };
    assert!(runtime_codec::encode_value(&event)
        .unwrap_err()
        .to_string()
        .contains("goalRevision"));
}
