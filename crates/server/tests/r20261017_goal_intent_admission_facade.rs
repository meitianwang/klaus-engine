//! Final provider admission must honor objective edits and goal replacement.

use super::*;
use futures::StreamExt;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::provider::ModelRequest;
use std::sync::Mutex;

struct ReleaseBarriers {
    admission: Arc<tokio::sync::Semaphore>,
    provider: Arc<tokio::sync::Semaphore>,
}
impl Drop for ReleaseBarriers {
    fn drop(&mut self) {
        self.admission.add_permits(2);
        self.provider.add_permits(1);
    }
}

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, _) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

pub(super) async fn assert_current_intent_after_final_admission(
    control: FinalGoalAdmissionControl,
) {
    assert!(matches!(
        control,
        FinalGoalAdmissionControl::ObjectiveEdit | FinalGoalAdmissionControl::NewGeneration
    ));
    let _serial = GOAL_CONFIG_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let _config = ConfigGuard::new(root.path());
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let provider_entered = Arc::new(tokio::sync::Notify::new());
    let admission_entered = Arc::new(tokio::sync::Notify::new());
    let admission_count = Arc::new(AtomicUsize::new(0));
    let release_admission = Arc::new(tokio::sync::Semaphore::new(0));
    let release_provider = Arc::new(tokio::sync::Semaphore::new(0));
    let _cleanup = ReleaseBarriers {
        admission: release_admission.clone(),
        provider: release_provider.clone(),
    };
    let agent = make_agent_with_call_model_and_config(
        {
            let requests = requests.clone();
            let provider_entered = provider_entered.clone();
            let release_provider = release_provider.clone();
            Box::new(move |request, _| {
                requests.lock().unwrap().push(request);
                provider_entered.notify_one();
                let release_provider = release_provider.clone();
                Box::pin(
                    futures::stream::iter(vec![
                        ev::message_start(None),
                        ev::text("Current goal intent admitted"),
                    ])
                    .chain(futures::stream::once(async move {
                        release_provider.acquire().await.unwrap().forget();
                        ev::stop(
                            Some(ModelStopReason::EndTurn),
                            Some(Usage {
                                input_tokens: 7,
                                output_tokens: 3,
                                cache_read_input_tokens: 0,
                                cache_creation_input_tokens: 0,
                                total_tokens: None,
                            }),
                        )
                    })),
                )
            })
        },
        Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let factory: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let admission_entered = admission_entered.clone();
        let admission_count = admission_count.clone();
        let release_admission = release_admission.clone();
        Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let admission_entered = admission_entered.clone();
            let admission_count = admission_count.clone();
            let release_admission = release_admission.clone();
            Box::pin(async move {
                let session = agent.open_session(store, StartSessionOptions {
                    id: ctx.init.thread_id, approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                    config: Some(json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false})),
                    ..Default::default()
                }).await.map_err(|error| Box::new(error) as BoxError)?;
                session
                    .goal_runtime()
                    .unwrap()
                    .testing_before_automatic_provider_admission(Some(Arc::new(move || {
                        let admission_entered = admission_entered.clone();
                        let admission_count = admission_count.clone();
                        let release_admission = release_admission.clone();
                        Box::pin(async move {
                            let attempt = admission_count.fetch_add(1, Ordering::SeqCst);
                            assert!(
                                attempt < 2,
                                "only the superseded and current goal admissions are expected"
                            );
                            admission_entered.notify_one();
                            release_admission.acquire().await.unwrap().forget();
                        })
                    })));
                Ok(session)
            })
        })
    };
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store.clone()));
    let mut client = TestClient::start_with(factory, Some(lifecycle));
    client.initialize().await;
    let started = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"intent-final-admission"}}),
    )
    .await;
    let session = started["sessionId"].as_str().unwrap().to_owned();
    let created = request(
        &mut client,
        3,
        "session/setGoal",
        json!({
            "sessionId":session,"objective":"SUPERSEDED-NAMED-OBJECTIVE","tokenBudget":600,
            "origin":"user","expectedGoalId":null,"expectedGoalRevision":0
        }),
    )
    .await;
    tokio::time::timeout(REPLY_TIMEOUT, admission_entered.notified())
        .await
        .unwrap();
    assert_eq!(admission_count.load(Ordering::SeqCst), 1);
    assert!(
        requests.lock().unwrap().is_empty(),
        "the first request is held before provider construction"
    );
    let before = request(&mut client, 4, "session/goal", json!({"sessionId":session})).await;
    let queue = request(
        &mut client,
        5,
        "session/queueSnapshot",
        json!({"sessionId":session}),
    )
    .await;
    assert!(queue["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(before["threadGoal"]["tokensUsed"], 0);
    let old_generation = created["threadGoal"]["goalId"].clone();
    let current = match control {
        FinalGoalAdmissionControl::ObjectiveEdit => {
            let edited = request(&mut client, 6, "session/setGoal", json!({
                "sessionId":session,"objective":"CURRENT-NAMED-OBJECTIVE","tokenBudget":900,
                "origin":"user","expectedGoalId":old_generation,"expectedGoalRevision":before["goalRevision"]
            })).await;
            assert_eq!(edited["threadGoal"]["goalId"], old_generation);
            assert_eq!(
                edited["threadGoal"]["createdAtMs"],
                created["threadGoal"]["createdAtMs"]
            );
            edited
        }
        FinalGoalAdmissionControl::NewGeneration => {
            let cleared = request(
                &mut client,
                6,
                "session/clearGoal",
                json!({
                    "sessionId":session,"origin":"user","expectedGoalId":old_generation,
                    "expectedGoalRevision":before["goalRevision"]
                }),
            )
            .await;
            assert_eq!(cleared["goalCleared"], true);
            let replacement = request(&mut client, 7, "session/setGoal", json!({
                "sessionId":session,"objective":"CURRENT-NAMED-OBJECTIVE","tokenBudget":900,
                "origin":"user","expectedGoalId":null,"expectedGoalRevision":cleared["goalRevision"]
            })).await;
            assert_ne!(replacement["threadGoal"]["goalId"], old_generation);
            replacement
        }
        _ => unreachable!(),
    };
    assert_eq!(current["threadGoal"]["status"], "active");
    assert_eq!(current["threadGoal"]["tokensUsed"], 0);
    assert!(current["goalRevision"].as_i64().unwrap() > before["goalRevision"].as_i64().unwrap());
    assert!(requests.lock().unwrap().is_empty());
    release_admission.add_permits(1);
    tokio::select! {
        _ = provider_entered.notified() => panic!("{control:?} admitted the superseded provider request"),
        result = tokio::time::timeout(REPLY_TIMEOUT, admission_entered.notified()) => result.unwrap(),
    }
    assert_eq!(admission_count.load(Ordering::SeqCst), 2);
    assert!(
        requests.lock().unwrap().is_empty(),
        "the superseded attempt must never call the provider"
    );
    let rechecked = request(&mut client, 8, "session/goal", json!({"sessionId":session})).await;
    let current_generation = current["threadGoal"]["goalId"].clone();
    for key in [
        "goalId",
        "objective",
        "status",
        "tokenBudget",
        "createdAtMs",
    ] {
        assert_eq!(
            rechecked["threadGoal"][key], current["threadGoal"][key],
            "{control:?}: {key}"
        );
    }
    assert_eq!(rechecked["threadGoal"]["tokensUsed"], 0);
    assert!(
        rechecked["threadGoal"]["timeUsedSeconds"].as_i64().unwrap()
            >= current["threadGoal"]["timeUsedSeconds"].as_i64().unwrap()
    );
    release_admission.add_permits(1);
    tokio::time::timeout(REPLY_TIMEOUT, provider_entered.notified())
        .await
        .unwrap();
    {
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "only current intent may create a provider request"
        );
        let context = requests[0]
            .messages
            .iter()
            .rev()
            .filter_map(|message| {
                let Message::User(user) = message else {
                    return None;
                };
                if user.is_meta != Some(true) {
                    return None;
                }
                user.content.iter().find_map(|block| match block {
                    ContentBlock::Text(text) if text.text.contains("<objective>") => {
                        Some(text.text.try_to_utf8().unwrap())
                    }
                    _ => None,
                })
            })
            .next()
            .expect("the admitted automatic request contains full goal context");
        assert!(context.contains("CURRENT-NAMED-OBJECTIVE"), "{context}");
        assert!(!context.contains("SUPERSEDED-NAMED-OBJECTIVE"), "{context}");
        assert!(
            context.contains("900"),
            "the latest budget must reach the model: {context}"
        );
    }
    let paused = request(&mut client, 9, "session/setGoal", json!({
        "sessionId":session,"origin":"user","status":"paused","expectedGoalId":current_generation
    })).await;
    assert_eq!(paused["threadGoal"]["status"], "paused");
    assert_eq!(paused["threadGoal"]["objective"], "CURRENT-NAMED-OBJECTIVE");
    assert_eq!(paused["threadGoal"]["tokensUsed"], 0);
    release_provider.add_permits(1);
    wait_for_goal_completion(&mut client, Vec::new(), "Current goal intent admitted").await;
    let settled = request(
        &mut client,
        10,
        "session/goal",
        json!({"sessionId":session}),
    )
    .await;
    assert_eq!(settled["threadGoal"]["goalId"], current_generation);
    assert_eq!(
        settled["threadGoal"]["objective"],
        "CURRENT-NAMED-OBJECTIVE"
    );
    assert_eq!(settled["threadGoal"]["status"], "paused");
    assert_eq!(settled["threadGoal"]["tokenBudget"], 900);
    assert_eq!(
        settled["threadGoal"]["tokensUsed"], 10,
        "the current generation receives exactly the actual admitted request usage"
    );
    assert!(
        settled["threadGoal"]["timeUsedSeconds"].as_i64().unwrap()
            >= paused["threadGoal"]["timeUsedSeconds"].as_i64().unwrap()
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert_eq!(admission_count.load(Ordering::SeqCst), 2);
    let queue = request(
        &mut client,
        11,
        "session/queueSnapshot",
        json!({"sessionId":session}),
    )
    .await;
    assert!(queue["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    client.close().await;
}

#[tokio::test]
async fn same_generation_objective_edit_at_final_admission_supersedes_the_old_request() {
    final_goal_admission_control(FinalGoalAdmissionControl::ObjectiveEdit).await;
}

#[tokio::test]
async fn replacement_generation_at_final_admission_supersedes_the_old_request() {
    final_goal_admission_control(FinalGoalAdmissionControl::NewGeneration).await;
}
