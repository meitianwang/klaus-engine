//! Defect: side-chat startup replayed the full persisted parent after a separate endpoint init.
//! Root cause: the side-task facade could only bind an existing endpoint from a store.
//! Work item 20261004: atomically start an ephemeral side task from completed live context.
//! This public facade regression belongs to server; core cannot depend on its own facade.
//!
//! Cancellation defect: closing during a blocked factory left startup and promoted RPCs parked.
//! Root cause: event-stream EOF retained reply senders instead of closing their registry.
//! Work item 20261004: settle those consumers and preserve same-identity startup retries.

use super::*;
use crate::app_server::wire;
use futures::StreamExt;
use klaus_agent_core::engine::session::TurnIndex;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::SessionRole;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::provider::ModelRequest;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

use super::r20260908_live_side_task_fork::{request, run_turn};

fn is_retained_skill_catalog(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true) && user.content.len() == 1
            && matches!(&user.content[0],
                klaus_agent_core::schemas::content::ContentBlock::Text(text)
                if text.harness_envelope == Some(klaus_agent_core::schemas::content::HarnessEnvelope::SystemReminder)
                    && text.text.starts_with("<skill-catalog:")))
}

fn assert_visible_side_turn(session: &Session, input: &str) {
    let history = session.visible_history();
    assert_eq!(
        history
            .iter()
            .filter(|message| is_retained_skill_catalog(message))
            .count(),
        1,
        "the side turn retains its own trusted skill catalog"
    );
    let conversation = history
        .iter()
        .filter(|message| !is_retained_skill_catalog(message))
        .collect::<Vec<_>>();
    assert_eq!(
        conversation.len(),
        2,
        "only the selected side input and completed answer are visible"
    );
    assert!(matches!(conversation[0], Message::User(user)
        if user.is_meta != Some(true) && user.content.iter().any(|block| matches!(block,
            klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text == input))));
    assert!(matches!(conversation[1], Message::Assistant(assistant)
        if assistant.content.iter().any(|block| matches!(block,
            klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text == "side answer"))));
}

fn parent_rows() -> Vec<Value> {
    vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":"old","startedAtMs":1}}),
        json!({"kind":"message","message":{"type":"user","uuid":"old-user","content":[{"type":"text","text":"older request"}]}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"old-answer","content":[{"type":"text","text":"older answer"}],"model":"m"}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"old","payload":{"status":"completed","finalMessageId":"old-answer","startedAtMs":1,"finishedAtMs":2,"durationMs":1}}),
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":"complete","startedAtMs":3}}),
        json!({"kind":"message","message":{"type":"user","uuid":"complete-user","content":[{"type":"text","text":"latest complete request"}]}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"complete-call","content":[{"type":"tool_use","id":"parent-call","name":"bash","input":{"command":"true"}}],"model":"m"}}),
        json!({"kind":"message","message":{"type":"tool_result","uuid":"complete-result","toolUseId":"parent-call","content":[{"type":"text","text":"actual completed output"}],"isError":false}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"complete-answer","content":[{"type":"text","text":"latest complete answer"}],"model":"m"}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"complete","payload":{"status":"completed","finalMessageId":"complete-answer","startedAtMs":3,"finishedAtMs":4,"durationMs":1}}),
    ]
}

fn message_ids(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match message {
            Message::User(message) => &message.uuid,
            Message::Assistant(message) => &message.uuid,
            Message::ToolResult(message) => &message.uuid,
            Message::System(message) => &message.uuid,
            Message::CompactBoundary(message) => &message.uuid,
        })
        .map(|id| id.try_to_utf8().unwrap())
        .collect()
}

fn completed_ids() -> Vec<String> {
    [
        "complete-user",
        "complete-call",
        "complete-result",
        "complete-answer",
    ]
    .map(str::to_string)
    .to_vec()
}

#[derive(Default)]
struct FixtureOptions {
    completed_parent: bool,
    persisted_parent: bool,
    ignore_side_intent: bool,
    block_side_factory: bool,
}

#[derive(Default)]
struct SideFactoryGate {
    blocked: AtomicBool,
    entered: Notify,
    cancelled: Notify,
}

struct BlockedSideFactory {
    gate: Arc<SideFactoryGate>,
}

impl Drop for BlockedSideFactory {
    fn drop(&mut self) {
        self.gate.cancelled.notify_one();
    }
}

impl SideFactoryGate {
    async fn wait_if_blocked(self: &Arc<Self>) {
        if self.blocked.load(Ordering::SeqCst) {
            let _pending = BlockedSideFactory { gate: self.clone() };
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

struct Fixture {
    client: TestClient,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    ignore_side: Arc<AtomicBool>,
    side_factory: Arc<SideFactoryGate>,
    parent_entered: Arc<Notify>,
    parent_release: Arc<Notify>,
    root: tempfile::TempDir,
}

impl Fixture {
    async fn new(options: FixtureOptions) -> Self {
        let root = tempfile::tempdir().unwrap();
        let rows = parent_rows();
        let store: Option<Arc<dyn SessionStore>> = if options.persisted_parent {
            let dir = root.path().join("sessions");
            std::fs::create_dir_all(&dir).unwrap();
            let mut persisted = vec![
                json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"parent","createdAt":1700000000000_i64,"cwd":root.path(),"model":"m","role":"main"}}),
            ];
            persisted.extend(rows.clone());
            std::fs::write(
                dir.join("1700000000000-parent.jsonl"),
                persisted
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n",
            )
            .unwrap();
            Some(Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(dir.to_string_lossy().into_owned()),
                archive_dir: None,
            })))
        } else {
            None
        };
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let parent_entered = Arc::new(Notify::new());
        let parent_release = Arc::new(Notify::new());
        let agent = make_agent_with_call_model_and_config(
            Box::new({
                let requests = requests.clone();
                let entered = parent_entered.clone();
                let release = parent_release.clone();
                move |model_request, _| {
                    let blocked = model_request.messages.iter().any(|message| {
                        matches!(message,
                        Message::User(user) if user.content.iter().any(|block| matches!(block,
                            klaus_agent_core::schemas::content::ContentBlock::Text(text)
                            if text.text == "blocking-parent")))
                    });
                    requests.lock().unwrap().push(model_request);
                    if blocked {
                        let entered = entered.clone();
                        let release = release.clone();
                        Box::pin(
                            futures::stream::once(async move {
                                entered.notify_one();
                                release.notified().await;
                                ev::message_start(None)
                            })
                            .chain(futures::stream::iter(vec![
                                ev::text("parent finished"),
                                ev::stop(Some(ModelStopReason::EndTurn), None),
                            ])),
                        )
                    } else {
                        Box::pin(futures::stream::iter(text_turn("side answer")))
                    }
                }
            }),
            Some(
                json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}}),
            ),
        );
        let production = crate::lifecycle::engine_sessions(agent.clone(), store).create_session;
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let ignore_side = Arc::new(AtomicBool::new(options.ignore_side_intent));
        let side_factory = Arc::new(SideFactoryGate {
            blocked: AtomicBool::new(options.block_side_factory),
            ..Default::default()
        });
        let create: CreateSession = Arc::new({
            let sessions = sessions.clone();
            let ignore_side = ignore_side.clone();
            let side_factory = side_factory.clone();
            move |ctx| {
                let agent = agent.clone();
                let production = production.clone();
                let sessions = sessions.clone();
                let rows = rows.clone();
                let ignore_side = ignore_side.clone();
                let side_factory = side_factory.clone();
                Box::pin(async move {
                    if ctx.side_task.is_some() {
                        side_factory.wait_if_blocked().await;
                    }
                    let session = if ctx
                        .init
                        .thread_id
                        .as_ref()
                        .is_some_and(|id| *id == "parent")
                        || ctx.side_task.is_some() && ignore_side.load(Ordering::SeqCst)
                    {
                        let mut index = TurnIndex::default();
                        let history = if options.completed_parent {
                            let mut history = Vec::new();
                            let mut turn = RuntimeString::default();
                            for row in &rows {
                                if row["kind"] == "turn_context" {
                                    turn = row["ctx"]["turnId"].as_str().unwrap().into();
                                } else if row["kind"] == "message" {
                                    let message: Message = runtime_codec::decode_text(
                                        &RuntimeString::from(row["message"].to_string()),
                                    )
                                    .unwrap();
                                    index.message_turn.insert(
                                        message_ids(std::slice::from_ref(&message))[0]
                                            .as_str()
                                            .into(),
                                        turn.clone(),
                                    );
                                    history.push(message);
                                } else if row["kind"] == "event" {
                                    index.turn_status.insert(turn.clone(), "completed".into());
                                }
                            }
                            history
                        } else {
                            Vec::new()
                        };
                        let session = agent.start_session(StartSessionOptions {
                            id: ctx.init.thread_id.clone(),
                            cwd: ctx.init.cwd.clone().map(Into::into),
                            initial_history: Some(history),
                            durable_permissions: Some(ctx.durable_permissions),
                            approval_handler: Some(ctx.approval_handler),
                            elicit_handler: Some(ctx.elicit_handler),
                            ..Default::default()
                        });
                        session.restore_turn_index(index);
                        session
                    } else {
                        production(ctx).await?
                    };
                    sessions
                        .lock()
                        .unwrap()
                        .insert(session.id.try_to_utf8().unwrap(), session.clone());
                    Ok(session)
                })
            }
        });
        let mut client = TestClient::start_full(
            create,
            None,
            Some(root.path().join("rules.json")),
            Some(root.path().join("config.toml")),
        );
        client.initialize().await;
        Self {
            client,
            sessions,
            requests,
            ignore_side,
            side_factory,
            parent_entered,
            parent_release,
            root,
        }
    }

    fn init(&self, thread_id: &str) -> Value {
        json!({"threadId":thread_id,"cwd":self.root.path(),"config":{"model":"m"}})
    }

    async fn start_parent(&mut self, materialize: bool) {
        let init = self.init("parent");
        let started = request(&mut self.client, 2, "session/start", json!({"init":init})).await;
        assert_eq!(started["result"]["sessionId"], "parent", "{started}");
        if materialize {
            let history = request(
                &mut self.client,
                3,
                "session/history",
                json!({"sessionId":"parent"}),
            )
            .await;
            assert!(history.get("error").is_none(), "{history}");
        }
    }

    async fn start_side(&mut self, id: u64, name: &str, explicit_source: bool) -> Value {
        let mut params = json!({"parentThreadId":"parent","init":self.init(name)});
        if explicit_source {
            params["sourceSessionId"] = json!("parent");
        }
        request(&mut self.client, id, "session/startSideTask", params).await
    }

    fn session(&self, id: &str) -> Arc<Session> {
        self.sessions.lock().unwrap().get(id).unwrap().clone()
    }
}

fn assert_started(reply: &Value, id: &str) {
    assert!(reply.get("error").is_none(), "{reply}");
    assert_eq!(reply["result"]["sessionId"], id, "{reply}");
    assert_eq!(reply["result"]["initialized"]["threadId"], id, "{reply}");
    assert_eq!(
        reply["result"]["initialized"]["parentThreadId"], "parent",
        "{reply}"
    );
    assert_eq!(
        reply["result"]["initialized"]["role"], "side_task",
        "{reply}"
    );
    assert_eq!(reply["result"]["initialized"]["ephemeral"], true, "{reply}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_side_start_uses_only_completed_context_while_parent_runs_without_a_store() {
    let mut fixture = Fixture::new(FixtureOptions {
        completed_parent: true,
        ..Default::default()
    })
    .await;
    fixture.start_parent(true).await;
    fixture.client.send(json!({"id":10,"method":"turn/start","params":{"sessionId":"parent","input":"blocking-parent"}})).await;
    assert!(fixture
        .client
        .response_for(10)
        .await
        .0
        .get("error")
        .is_none());
    tokio::time::timeout(REPLY_TIMEOUT, fixture.parent_entered.notified())
        .await
        .unwrap();
    let side_reply = fixture.start_side(11, "side", true).await;
    assert_started(&side_reply, "side");
    let side = fixture.session("side");
    assert_eq!(message_ids(&side.history()), completed_ids());
    assert!(side.visible_history().is_empty());
    assert_eq!(side.meta.role, Some(SessionRole::SideTask));
    let selected = "Explain this selected streaming sentence.";
    run_turn(&mut fixture.client, 12, "side", selected).await;
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(message_ids(&requests[1].messages[..4]), completed_ids());
        assert!(
            !requests[1].messages.iter().any(|message| matches!(message,
            Message::User(user) if user.content.iter().any(|block| matches!(block,
                klaus_agent_core::schemas::content::ContentBlock::Text(text)
                if text.text == "blocking-parent")))),
            "the running parent input must not enter the child request"
        );
        assert!(requests[1].messages.iter().any(|message| matches!(message,
        Message::User(user) if user.content.iter().any(|block| matches!(block,
            klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text == selected)))));
    }
    assert_visible_side_turn(&side, selected);
    fixture.parent_release.notify_one();
    fixture.client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_active_first_parent_turn_does_not_become_inherited_context() {
    let mut fixture = Fixture::new(FixtureOptions::default()).await;
    fixture.start_parent(false).await;
    fixture.client.send(json!({"id":10,"method":"turn/start","params":{"sessionId":"parent","input":"blocking-parent"}})).await;
    assert!(fixture
        .client
        .response_for(10)
        .await
        .0
        .get("error")
        .is_none());
    tokio::time::timeout(REPLY_TIMEOUT, fixture.parent_entered.notified())
        .await
        .unwrap();
    assert_started(&fixture.start_side(11, "side", false).await, "side");
    assert!(fixture.session("side").history().is_empty());
    run_turn(&mut fixture.client, 12, "side", "first side request").await;
    assert_visible_side_turn(&fixture.session("side"), "first side request");
    fixture.parent_release.notify_one();
    fixture.client.close().await;
}

#[tokio::test]
async fn cold_and_unmaterialized_parents_use_persisted_context_without_parent_startup() {
    for unmaterialized in [false, true] {
        let mut fixture = Fixture::new(FixtureOptions {
            persisted_parent: true,
            ..Default::default()
        })
        .await;
        let parent_path = fixture
            .root
            .path()
            .join("sessions/1700000000000-parent.jsonl");
        let before = std::fs::read_to_string(&parent_path).unwrap();
        if unmaterialized {
            fixture.start_parent(false).await;
        }
        assert_started(
            &fixture.start_side(10, "side", unmaterialized).await,
            "side",
        );
        assert_eq!(
            message_ids(&fixture.session("side").history()),
            completed_ids()
        );
        assert!(fixture.session("side").visible_history().is_empty());
        assert!(!fixture.sessions.lock().unwrap().contains_key("parent"));
        run_turn(&mut fixture.client, 11, "side", "cold side request").await;
        assert_eq!(std::fs::read_to_string(&parent_path).unwrap(), before);
        let rollouts: Vec<_> = std::fs::read_dir(parent_path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .collect();
        assert_eq!(rollouts, vec![parent_path.clone()]);
        fixture.client.close().await;
    }
}

#[tokio::test]
async fn invalid_sources_and_duplicate_destinations_preserve_the_existing_side() {
    let mut fixture = Fixture::new(FixtureOptions {
        completed_parent: true,
        ..Default::default()
    })
    .await;
    fixture.start_parent(true).await;
    for (id, parent, source, destination, code) in [
        (10, "other", "parent", "mismatch", wire::INVALID_PARAMS),
        (11, "parent", "missing", "missing", wire::UNKNOWN_SESSION),
        (12, "parent", "parent", "parent", wire::INVALID_PARAMS),
    ] {
        let init = fixture.init(destination);
        let reply = request(
            &mut fixture.client,
            id,
            "session/startSideTask",
            json!({"parentThreadId":parent,"sourceSessionId":source,"init":init}),
        )
        .await;
        assert_eq!(reply["error"]["code"], code, "{reply}");
    }
    assert_started(&fixture.start_side(13, "side", true).await, "side");
    let before = fixture.session("side");
    let repeated = fixture.start_side(14, "side", true).await;
    assert_eq!(
        repeated["error"]["code"],
        wire::SESSION_ALREADY_LIVE,
        "{repeated}"
    );
    assert!(Arc::ptr_eq(&before, &fixture.session("side")));
    run_turn(&mut fixture.client, 15, "side", "still usable").await;
    fixture.client.close().await;
}

#[tokio::test]
async fn a_factory_that_ignores_side_intent_is_rejected_and_the_identity_can_be_retried() {
    let mut fixture = Fixture::new(FixtureOptions {
        completed_parent: true,
        ignore_side_intent: true,
        ..Default::default()
    })
    .await;
    fixture.start_parent(true).await;
    let failed = fixture.start_side(10, "side", true).await;
    assert_eq!(failed["error"]["code"], wire::ENGINE_ERROR, "{failed}");
    let gone = request(
        &mut fixture.client,
        11,
        "session/history",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(gone["error"]["code"], wire::UNKNOWN_SESSION, "{gone}");
    fixture.ignore_side.store(false, Ordering::SeqCst);
    assert_started(&fixture.start_side(12, "side", true).await, "side");
    fixture.client.close().await;
}

#[tokio::test]
async fn closing_an_atomic_side_endpoint_releases_its_identity() {
    let mut fixture = Fixture::new(FixtureOptions {
        completed_parent: true,
        ..Default::default()
    })
    .await;
    fixture.start_parent(true).await;
    assert_started(&fixture.start_side(10, "side", true).await, "side");
    let closed = request(
        &mut fixture.client,
        11,
        "session/close",
        json!({"sessionId":"side"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    let gone = request(
        &mut fixture.client,
        12,
        "session/history",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(gone["error"]["code"], wire::UNKNOWN_SESSION, "{gone}");
    assert_started(&fixture.start_side(13, "side", true).await, "side");
    fixture.client.close().await;
}

#[tokio::test]
async fn closing_during_a_blocked_side_factory_settles_all_replies_and_allows_same_id_retry() {
    let mut fixture = Fixture::new(FixtureOptions {
        completed_parent: true,
        block_side_factory: true,
        ..Default::default()
    })
    .await;
    fixture.start_parent(true).await;
    let init = fixture.init("side");
    fixture
        .client
        .send(json!({
            "id":10,"method":"session/startSideTask",
            "params":{"parentThreadId":"parent","sourceSessionId":"parent","init":init}
        }))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, fixture.side_factory.entered.notified())
        .await
        .expect("side factory reached the deterministic cancellation gate");

    // Real fixture setup is complete. Logical time exercises the endpoint's
    // force-drain path without sleeping through its production shutdown grace.
    // The factory remains blocked until shutdown drops it, so no normal
    // get_history terminal reply can accidentally mask an EOF waiter leak.
    tokio::time::pause();
    for (id, method) in [
        (12, "session/history"),
        (13, "session/cost"),
        (11, "session/close"),
    ] {
        fixture
            .client
            .send(json!({"id":id,"method":method,"params":{"sessionId":"side"}}))
            .await;
    }
    let mut replies = HashMap::new();
    tokio::time::timeout(Duration::from_secs(100), async {
        while replies.len() < 4 {
            let line = fixture
                .client
                .lines
                .next_line()
                .await
                .expect("readable facade reply stream")
                .expect("facade stays connected while its side endpoint closes");
            let reply: Value = serde_json::from_str(&line).expect("valid JSON reply");
            if let Some(id) = reply
                .get("id")
                .and_then(Value::as_u64)
                .filter(|id| [10, 11, 12, 13].contains(id) && reply.get("method").is_none())
            {
                assert!(
                    replies.insert(id, reply).is_none(),
                    "one terminal reply per RPC"
                );
            }
        }
    })
    .await
    .expect("startup, promoted requests, and close all settle after forced shutdown");
    tokio::time::timeout(REPLY_TIMEOUT, fixture.side_factory.cancelled.notified())
        .await
        .expect("shutdown dropped the blocked factory owner");
    tokio::time::resume();

    for id in [10, 12, 13] {
        assert_eq!(
            replies[&id]["error"]["code"],
            wire::ENGINE_ERROR,
            "{:?}",
            replies
        );
    }
    assert_eq!(
        replies[&10]["error"]["message"], "protocol session has shut down",
        "{:?}",
        replies
    );
    // A forced drain is an explicit close failure, never a successful cleanup
    // receipt. Startup still releases its own failed endpoint's identity.
    assert_eq!(
        replies[&11]["error"]["code"],
        wire::ENGINE_ERROR,
        "{:?}",
        replies
    );
    assert!(
        replies[&11]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("session close did not drain"),
        "{:?}",
        replies
    );
    let gone = request(
        &mut fixture.client,
        14,
        "session/history",
        json!({"sessionId":"side"}),
    )
    .await;
    assert_eq!(gone["error"]["code"], wire::UNKNOWN_SESSION, "{gone}");
    fixture.side_factory.blocked.store(false, Ordering::SeqCst);
    assert_started(&fixture.start_side(15, "side", true).await, "side");
    run_turn(
        &mut fixture.client,
        16,
        "side",
        "usable after startup cancellation",
    )
    .await;
    fixture.client.close().await;
}

#[test]
fn atomic_side_start_schema_names_source_and_child_init_without_host_history() {
    let schema = schema_bundle();
    let params = &schema["requests"]["session/startSideTask"]["params"];
    assert!(params["properties"].get("parentThreadId").is_some());
    assert!(params["properties"].get("sourceSessionId").is_some());
    assert!(params["properties"].get("init").is_some());
    assert!(params["properties"].get("history").is_none());
    assert_eq!(params["additionalProperties"], false);
}
