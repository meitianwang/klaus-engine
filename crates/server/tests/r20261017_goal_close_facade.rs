//! Defect: closing a thread cancelled work before pausing and settling its goal.
//! Root cause: retained shutdown drained tasks without a goal persistence boundary.
//! Work item: 20261017 root goal shutdown and descendant usage settlement.

use super::*;
use futures::StreamExt;
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Semaphore;

const CHILD_DIRECTIVE: &str = "Inspect the held shutdown descendant evidence.";

struct ConfigGuard(Option<std::ffi::OsString>);
impl ConfigGuard {
    fn new(path: &Path) -> Self {
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", path);
        Self(previous)
    }
}
impl Drop for ConfigGuard {
    fn drop(&mut self) {
        match self.0.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    IdlePlanning,
    Active,
    Descendant,
    PauseFailure,
}
impl Case {
    fn label(self) -> &'static str {
        match self {
            Self::IdlePlanning => "idle-planning",
            Self::Active => "active",
            Self::Descendant => "descendant",
            Self::PauseFailure => "pause-failure",
        }
    }
    fn expected_tokens(self) -> i64 {
        match self {
            Self::IdlePlanning => 0,
            Self::Active => 14,
            Self::Descendant => 21,
            Self::PauseFailure => 0,
        }
    }
}

fn usage(tokens: u64) -> Usage {
    Usage {
        input_tokens: tokens - 1,
        output_tokens: 1,
        cache_read_input_tokens: 5,
        cache_creation_input_tokens: 3,
        total_tokens: Some(999),
    }
}

fn sampled_stop(reason: ModelStopReason, tokens: u64) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::UsageSnapshot {
            usage: usage(tokens),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: reason,
            usage: usage(tokens),
        },
    ]
}

fn has_child_directive(request: &klaus_agent_core::types::provider::ModelRequest) -> bool {
    request.messages.iter().any(|message| {
        match message {
        Message::User(user) if user.is_meta != Some(true) => user.content.iter().any(|block| {
            matches!(block, ContentBlock::Text(text) if text.text.contains(CHILD_DIRECTIVE))
        }),
        _ => false,
    }
    })
}

struct Observation {
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    held: Semaphore,
    cancelled: Semaphore,
    cancellation_status: Mutex<Vec<String>>,
    requests: Mutex<Vec<klaus_agent_core::types::provider::ModelRequest>>,
}
impl Observation {
    fn new() -> Self {
        Self {
            root_calls: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            held: Semaphore::new(0),
            cancelled: Semaphore::new(0),
            cancellation_status: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

fn held_request(
    observation: Arc<Observation>,
    thread_id: String,
    signal: klaus_agent_core::types::abort::AbortSignal,
    tokens: u64,
    text: &'static str,
) -> klaus_agent_core::types::provider::ModelStream {
    let cancelled = signal.clone();
    let captured = observation.clone();
    tokio::spawn(async move {
        cancelled.cancelled().await;
        let status = klaus_agent_core::goal::store::get_goal(&thread_id)
            .await
            .map(|goal| {
                goal.map(|goal| goal.status.wire_name().to_string())
                    .unwrap_or_else(|| "missing".into())
            })
            .unwrap_or_else(|_| "read_failed".into());
        captured.cancellation_status.lock().unwrap().push(status);
        captured.cancelled.add_permits(1);
    });
    let initial = vec![
        ev::message_start(None),
        ModelStreamEvent::UsageSnapshot {
            usage: usage(tokens),
        },
        ModelStreamEvent::UsageSnapshot {
            usage: usage(tokens),
        },
        ev::text(text),
    ];
    let terminal = futures::stream::once(async move {
        observation.held.add_permits(1);
        signal.cancelled().await;
        None::<ModelStreamEvent>
    })
    .filter_map(futures::future::ready);
    Box::pin(futures::stream::iter(initial).chain(terminal))
}

struct Fixture {
    client: TestClient,
    store: Arc<dyn SessionStore>,
    observation: Arc<Observation>,
    live: Arc<Session>,
    session_id: String,
    thread_id: String,
    root: tempfile::TempDir,
    _config: ConfigGuard,
    _agent: Arc<Agent>,
}

impl Fixture {
    async fn new(case: Case) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = ConfigGuard::new(root.path());
        let thread_id = format!("goal-close-{}", case.label());
        let observation = Arc::new(Observation::new());
        let captured = observation.clone();
        let goal_thread = thread_id.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |request, signal| {
                let child = has_child_directive(&request);
                let index = if child {
                    captured.child_calls.fetch_add(1, Ordering::SeqCst)
                } else {
                    captured.root_calls.fetch_add(1, Ordering::SeqCst)
                };
                {
                    let mut requests = captured.requests.lock().unwrap();
                    assert!(
                        requests.len() < 4,
                        "close must not admit unrelated goal work"
                    );
                    requests.push(request);
                }
                let captured = captured.clone();
                let goal_thread = goal_thread.clone();
                if child {
                    assert!(matches!(case, Case::Descendant));
                    assert_eq!(index, 0, "the descendant owns one actual request");
                    return held_request(
                        captured,
                        goal_thread,
                        signal,
                        13,
                        "The descendant is still inspecting the evidence.",
                    );
                }
                match case {
                    Case::IdlePlanning => panic!("planning prevents goal inference before close"),
                    Case::Active | Case::PauseFailure => {
                        assert_eq!(index, 0, "one root provider request may be active");
                        held_request(
                            captured,
                            goal_thread,
                            signal,
                            14,
                            "The root request is still working.",
                        )
                    }
                    Case::Descendant => {
                        let mut events = vec![ev::message_start(None)];
                        match index {
                            0 => {
                                let input = runtime_json::parse(&RuntimeString::from(json!({"model": "low",
                                "description":"Inspect shutdown evidence", "prompt":CHILD_DIRECTIVE,
                                "fork_turns":"none", "run_in_background":true,
                            }).to_string())).unwrap();
                                events.extend(ev::tool_use("close-child", "task", &input));
                                events.extend(sampled_stop(ModelStopReason::ToolUse, 3));
                            }
                            1 => {
                                events.push(ev::text(
                                    "The root is idle while its descendant continues.",
                                ));
                                events.extend(sampled_stop(ModelStopReason::EndTurn, 5));
                            }
                            other => panic!("unexpected root continuation before close: {other}"),
                        }
                        Box::pin(futures::stream::iter(events))
                    }
                }
            }),
            Some(json!({
                "goal":true, "prewarm":false, "autoMemory":false, "agentMemory":false,
                "autoDream":false, "maxRetries":0, "loopDetection":false,
                "forwardSubagentText":false,
            })),
        );
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let live = Arc::new(Mutex::new(None));
        let captured_live = live.clone();
        let factory_agent = agent.clone();
        let factory_store = store.clone();
        let owned_root = root.path().to_path_buf();
        let create: CreateSession = Arc::new(move |context: SessionContext| {
            let agent = factory_agent.clone();
            let store = factory_store.clone();
            let captured_live = captured_live.clone();
            let owned_root = owned_root.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            id: context.init.thread_id,
                            cwd: Some(owned_root.to_string_lossy().into_owned().into()),
                            user_config_dir: Some(owned_root.to_string_lossy().into_owned().into()),
                            approval_handler: Some(context.approval_handler),
                            elicit_handler: Some(context.elicit_handler),
                            mcp_elicitation_presenter: Some(context.mcp_elicitation_presenter),
                            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| Box::new(error) as BoxError)?;
                *captured_live.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        });
        let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
        let mut client = TestClient::start_full(
            create,
            Some(lifecycle),
            Some(root.path().join("permissions.json")),
            Some(root.path().join("config.json")),
        );
        client.initialize().await;
        client
            .send(json!({"id":2,"method":"session/start","params":{"init":{"threadId":thread_id}}}))
            .await;
        let (started, _) = client.response_for(2).await;
        assert!(started.get("error").is_none(), "{started}");
        let session_id = started["result"]["sessionId"].as_str().unwrap().to_string();
        client.send(json!({"id":100,"method":"session/queueSnapshot","params":{"sessionId":session_id}})).await;
        let (bound, _) = client.response_for(100).await;
        assert!(bound.get("error").is_none(), "{bound}");
        assert!(bound["result"]["queueSnapshot"].is_object(), "{bound}");
        assert_eq!(observation.root_calls.load(Ordering::SeqCst), 0);
        let live = live
            .lock()
            .unwrap()
            .clone()
            .expect("the provider-free queue read materialized the persistent controller");
        assert!(
            live.goal_runtime().is_some(),
            "the actual root must own a persistent goal runtime"
        );
        Self {
            client,
            store,
            observation,
            live,
            session_id,
            thread_id,
            root,
            _config: config,
            _agent: agent,
        }
    }

    async fn activate(&mut self, case: Case) {
        if matches!(case, Case::IdlePlanning) {
            self.client.send(json!({"id":3,"method":"session/setCollaborationMode","params":{"sessionId":self.session_id,"mode":"plan"}})).await;
            let (mode, _) = self.client.response_for(3).await;
            assert!(mode.get("error").is_none(), "{mode}");
        }
        self.client.send(json!({"id":4,"method":"session/setGoal","params":{
            "sessionId":self.session_id,"objective":"Finish the authorized shutdown evidence.",
            "origin":"user","submit":true,"clientRequestId":format!("close-create-{}",case.label()),
            "expectedGoalId":null,
        }})).await;
        let (created, mut frames) = self.client.response_for(4).await;
        assert!(created.get("error").is_none(), "{created}");
        assert_eq!(created["result"]["threadGoal"]["status"], "active");
        if matches!(case, Case::IdlePlanning) {
            assert_eq!(self.observation.root_calls.load(Ordering::SeqCst), 0);
            assert!(!self.live.goal_runtime().unwrap().turn_in_flight());
            return;
        }
        tokio::time::timeout(REPLY_TIMEOUT, self.observation.held.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if matches!(case, Case::Descendant) {
            while !frames.iter().any(|frame| {
                frame["method"] == "turn/completed"
                    && frame["params"]["sessionId"] == self.session_id
            }) {
                assert!(frames.len() < 128, "bounded root completion trace");
                frames.push(self.client.next_message().await);
            }
            tokio::time::timeout(REPLY_TIMEOUT, self.live.wait_for_active_turns())
                .await
                .unwrap();
            assert!(self.live.goal_runtime().unwrap().has_descendant_work());
            assert_eq!(self.observation.root_calls.load(Ordering::SeqCst), 2);
            assert_eq!(self.observation.child_calls.load(Ordering::SeqCst), 1);
        }
    }

    fn artifact(&self, case: Case, close: &Value, frames: &[Value]) {
        let directory =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
        std::fs::create_dir_all(&directory).unwrap();
        let requests =
            runtime_codec::encode_value(&*self.observation.requests.lock().unwrap()).unwrap();
        std::fs::write(
            directory.join(format!("close-{}-requests.json", case.label())),
            runtime_json::stringify_pretty(&requests).to_node_utf8(),
        )
        .unwrap();
        std::fs::write(directory.join(format!("close-{}-trace.json", case.label())), serde_json::to_vec_pretty(&json!({
            "response":close,"frames":frames,"cancelStatuses":*self.observation.cancellation_status.lock().unwrap(),
        })).unwrap()).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_close_pauses_before_interrupt_and_flushes_root_and_descendant_partial_usage() {
    for case in [Case::IdlePlanning, Case::Active, Case::Descendant] {
        let mut fixture = Fixture::new(case).await;
        fixture.activate(case).await;
        fixture
            .client
            .send(
                json!({"id":5,"method":"session/close","params":{"sessionId":fixture.session_id}}),
            )
            .await;
        let (closed, frames) = fixture.client.response_for(5).await;
        assert!(closed.get("error").is_none(), "{case:?}: {closed}");
        if !matches!(case, Case::IdlePlanning) {
            tokio::time::timeout(REPLY_TIMEOUT, fixture.observation.cancelled.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            assert_eq!(
                *fixture.observation.cancellation_status.lock().unwrap(),
                ["paused"],
                "the persisted pause must precede the actual provider cancellation"
            );
        }
        let saved = klaus_agent_core::goal::store::get_goal(&fixture.thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.status, ThreadGoalStatus::Paused);
        assert_eq!(saved.tokens_used, case.expected_tokens(), "partial request snapshots settle once after the pause, including late descendant cancellation");
        assert!(!fixture.live.goal_runtime().unwrap().turn_in_flight());
        assert!(!fixture.live.goal_runtime().unwrap().has_descendant_work());
        let counts = (
            fixture.observation.root_calls.load(Ordering::SeqCst),
            fixture.observation.child_calls.load(Ordering::SeqCst),
        );
        fixture
            .client
            .send(json!({"id":6,"method":"session/goal","params":{"threadId":fixture.thread_id}}))
            .await;
        let (reopened, _) = fixture.client.response_for(6).await;
        assert_eq!(
            reopened["result"]["threadGoal"]["status"], "paused",
            "saved access must not reactivate work after close: {reopened}"
        );
        assert_eq!(
            reopened["result"]["threadGoal"]["tokensUsed"],
            case.expected_tokens()
        );
        assert_eq!(
            (
                fixture.observation.root_calls.load(Ordering::SeqCst),
                fixture.observation.child_calls.load(Ordering::SeqCst)
            ),
            counts
        );
        let detailed = fixture.live.shutdown_and_wait_detailed().await;
        assert!(
            detailed.ok(),
            "the retained shutdown completion must report the complete flush: {detailed:?}"
        );
        assert_eq!(
            klaus_agent_core::goal::store::get_goal(&fixture.thread_id)
                .await
                .unwrap()
                .unwrap()
                .tokens_used,
            saved.tokens_used,
            "re-reading retained shutdown cannot debit usage twice"
        );
        fixture.artifact(case, &closed, &frames);
        fixture.client.close().await;
        drop(fixture.store);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_close_reports_goal_store_failure_and_still_interrupts_the_real_provider() {
    let mut fixture = Fixture::new(Case::PauseFailure).await;
    fixture.activate(Case::PauseFailure).await;
    let database = fixture.root.path().join("goals.sqlite3");
    let backup = fixture.root.path().join("goals-retained.sqlite3");
    std::fs::rename(&database, &backup).unwrap();
    std::fs::create_dir(&database).unwrap();
    fixture
        .client
        .send(json!({"id":5,"method":"session/close","params":{"sessionId":fixture.session_id}}))
        .await;
    let (closed, frames) = fixture.client.response_for(5).await;
    tokio::time::timeout(REPLY_TIMEOUT, fixture.observation.cancelled.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        closed.get("error").is_some(),
        "a goal persistence failure must be reported: {closed}"
    );
    assert!(
        closed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("goal"),
        "{closed}"
    );
    assert_eq!(
        *fixture.observation.cancellation_status.lock().unwrap(),
        ["read_failed"]
    );
    assert_eq!(fixture.observation.root_calls.load(Ordering::SeqCst), 1);
    let detailed = fixture.live.shutdown_and_wait_detailed().await;
    assert!(
        !detailed.ok(),
        "the same retained failure must reach core and facade callers"
    );
    assert!(
        detailed
            .errors
            .iter()
            .any(|error| error.contains("goal shutdown pause failed")),
        "{detailed:?}"
    );
    std::fs::remove_dir(&database).unwrap();
    std::fs::rename(&backup, &database).unwrap();
    fixture
        .client
        .send(json!({"id":6,"method":"session/close","params":{"sessionId":fixture.session_id}}))
        .await;
    let (retry, _) = fixture.client.response_for(6).await;
    assert_eq!(
        retry["error"], closed["error"],
        "failed close retains its original cleanup result instead of pretending to succeed"
    );
    assert_eq!(fixture.observation.root_calls.load(Ordering::SeqCst), 1);
    fixture.artifact(Case::PauseFailure, &closed, &frames);
    fixture.client.close().await;
}
