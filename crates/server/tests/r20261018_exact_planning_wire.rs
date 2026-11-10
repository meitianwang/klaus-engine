//! Exact planning payloads must cross the existing named facade unchanged.
//! Root cause: a scalar JSON bridge rejected isolated units and dropped split deltas.
//! Work item: 20261018 exact planning transport, history and admission receipts.

use super::*;
use crate::app_server::wire;
use futures::StreamExt;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::provider::ModelRequest;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

const MAX_FRAMES: usize = 4096;

fn has(value: &RuntimeJson, path: &str, expected: &str) -> bool {
    value
        .pointer(path)
        .and_then(RuntimeJson::as_string)
        .is_some_and(|text| text == expected)
}

fn exact(value: Value) -> RuntimeJson {
    // This helper only constructs scalar control fields. Text is installed as RuntimeString.
    runtime_json::parse(&value.to_string().into()).unwrap()
}

fn scalar(value: &RuntimeJson) -> String {
    value
        .as_string()
        .expect("string control field")
        .try_to_utf8()
        .unwrap()
}

fn number(value: &RuntimeJson) -> u64 {
    value.as_u64().expect("nonnegative integer control field")
}

fn body(units: &[u16]) -> RuntimeString {
    RuntimeString::from("# Reviewed plan\n\nKeep exact text: ")
        .concat(&RuntimeString::from_utf16(units.to_vec()))
        .concat(&"\nThen verify the result.".into())
}

fn framed(content: &RuntimeString) -> RuntimeString {
    RuntimeString::from("<proposed_plan>\n")
        .concat(content)
        .concat(&"\n</proposed_plan>".into())
}

fn text_events(text: RuntimeString) -> Vec<ModelStreamEvent> {
    vec![
        ev::message_start(None),
        ModelStreamEvent::TextMessage {
            id: "exact-source-message".into(),
            phase: Some("final_answer".into()),
        },
        ModelStreamEvent::TextDelta { text },
        ev::stop(Some(ModelStopReason::EndTurn), None),
    ]
}

struct Script {
    before: Vec<ModelStreamEvent>,
    release: Option<Arc<Notify>>,
    after: Vec<ModelStreamEvent>,
}

impl Script {
    fn text(text: RuntimeString) -> Self {
        Self {
            before: text_events(text),
            release: None,
            after: Vec::new(),
        }
    }
}

#[derive(Default)]
struct Probe {
    creates: AtomicUsize,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    called: Notify,
}

struct Fixture {
    root: tempfile::TempDir,
    agent: Arc<Agent>,
    store: Arc<dyn SessionStore>,
    probe: Arc<Probe>,
}

impl Fixture {
    fn new(scripts: Vec<Script>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
        let probe = Arc::new(Probe::default());
        let observed = probe.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |request, signal| {
                let script = scripts
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("no unexpected provider execution");
                {
                    let mut requests = observed.requests.lock().unwrap();
                    assert!(requests.len() < 16, "bounded provider request evidence");
                    requests.push(request);
                }
                observed.calls.fetch_add(1, Ordering::SeqCst);
                observed.called.notify_one();
                Box::pin(
                    futures::stream::iter(script.before).chain(
                        futures::stream::once(async move {
                            if let Some(release) = script.release {
                                tokio::select! {
                                    () = release.notified() => {}
                                    () = signal.cancelled() => return Vec::new(),
                                }
                            }
                            if signal.is_cancelled() {
                                Vec::new()
                            } else {
                                script.after
                            }
                        })
                        .flat_map(futures::stream::iter),
                    ),
                )
            }),
            Some(
                json!({"model":"test-model","autoMemory":false,"agentMemory":false,"autoDream":false,"loopDetection":false,"prewarm":false}),
            ),
        );
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        Self {
            root,
            agent,
            store,
            probe,
        }
    }

    async fn endpoint(&self) -> ExactClient {
        let agent = self.agent.clone();
        let store = self.store.clone();
        let root = self.root.path().to_path_buf();
        let probe = self.probe.clone();
        let create: CreateSession = Arc::new(move |context: SessionContext| {
            let agent = agent.clone();
            let store = store.clone();
            let root = root.clone();
            let probe = probe.clone();
            Box::pin(async move {
                probe.creates.fetch_add(1, Ordering::SeqCst);
                let options = StartSessionOptions {
                    id: context.init.thread_id,
                    cwd: Some(root.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                    durable_permissions: Some(context.durable_permissions),
                    approval_handler: Some(context.approval_handler),
                    elicit_handler: Some(context.elicit_handler),
                    mcp_elicitation_presenter: Some(context.mcp_elicitation_presenter),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    ..Default::default()
                };
                let session = if let Some(thread) = context.resume {
                    agent.resume_session(store, &thread, options).await
                } else {
                    agent.open_session(store, options).await
                }
                .map_err(|error| Box::new(error) as BoxError)?;
                Ok(session)
            })
        });
        ExactClient::start(
            self.root.path(),
            create,
            crate::lifecycle::protocol_lifecycle(self.agent.clone(), Some(self.store.clone())),
        )
        .await
    }

    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(REPLY_TIMEOUT, async {
            loop {
                let called = self.probe.called.notified();
                if self.probe.calls.load(Ordering::SeqCst) >= count {
                    break;
                }
                called.await;
            }
        })
        .await
        .expect("the real provider must receive the admitted turn");
    }
}

/// The normal line-delimited JSON-RPC connection, with no scalar payload parser.
struct ExactClient {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    server: tokio::task::JoinHandle<()>,
    frames: Vec<RuntimeJson>,
}

impl ExactClient {
    async fn start(
        root: &Path,
        create: CreateSession,
        lifecycle: klaus_agent_protocol::ProtocolLifecycle,
    ) -> Self {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (read, write) = tokio::io::split(server);
        let (client_read, writer) = tokio::io::split(client);
        let task = tokio::spawn(run_app_server(
            read,
            write,
            AppServerOptions {
                windows_sandbox_context: None,
                create_session: create,
                credential_store: None,
                lifecycle: Some(lifecycle),
                plugin_admin: None,
                permission_rules_path: Some(root.join("permissions.json")),
                config_path: Some(root.join("config.json")),
                startup_init_ack_budget: None,
                session_unload_delay: None,
            },
        ));
        let mut client = Self {
            writer,
            lines: BufReader::new(client_read).lines(),
            server: task,
            frames: Vec::new(),
        };
        result(&client.request(1, "initialize", exact(json!({"clientInfo":{"name":"exact-planning-wire"},"capabilities":{"productEvents":true,"revisionedSessionEvents":true}}))).await);
        client.send(exact(json!({"method":"initialized"}))).await;
        client
    }

    async fn send(&mut self, message: RuntimeJson) {
        let mut bytes = runtime_json::stringify(&message).to_node_utf8();
        bytes.push(b'\n');
        self.writer.write_all(&bytes).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    async fn next(&mut self) -> RuntimeJson {
        let line = tokio::time::timeout(REPLY_TIMEOUT, self.lines.next_line())
            .await
            .expect("bounded named facade response")
            .expect("readable facade stream")
            .expect("facade remains open");
        let message = runtime_json::parse(&line.into()).expect("valid exact JSON output");
        assert!(self.frames.len() < MAX_FRAMES, "bounded facade transcript");
        self.frames.push(message.clone());
        message
    }

    async fn response(&mut self, id: u64) -> RuntimeJson {
        for _ in 0..MAX_FRAMES {
            let message = self.next().await;
            if message.get("id").and_then(RuntimeJson::as_u64) == Some(id)
                && message.get("method").is_none()
            {
                return message;
            }
        }
        panic!("correlated response {id} was not published");
    }

    async fn request(&mut self, id: u64, method: &str, params: RuntimeJson) -> RuntimeJson {
        let mut message = exact(json!({"id":id,"method":method,"params":{}}));
        message["params"] = params;
        self.send(message).await;
        self.response(id).await
    }

    async fn until(&mut self, predicate: impl Fn(&RuntimeJson) -> bool) -> RuntimeJson {
        if let Some(frame) = self.frames.iter().find(|frame| predicate(frame)) {
            return frame.clone();
        }
        for _ in 0..MAX_FRAMES {
            let frame = self.next().await;
            if predicate(&frame) {
                return frame;
            }
        }
        panic!("required bounded facade event was not published");
    }

    async fn close(mut self) {
        self.writer.shutdown().await.unwrap();
        drop(self.writer);
        tokio::time::timeout(REPLY_TIMEOUT, self.server)
            .await
            .expect("endpoint cleanup before restart")
            .expect("endpoint task did not panic");
        drop(self.lines);
    }
}

fn result(response: &RuntimeJson) -> &RuntimeJson {
    assert!(response.get("error").is_none(), "{response:?}");
    let result = response.get("result").expect("named result envelope");
    assert!(result.get("error").is_none(), "{response:?}");
    result
}

fn rejected(response: &RuntimeJson) {
    assert!(
        response.get("error").is_some() || response["result"].get("error").is_some(),
        "{response:?}"
    );
}

fn rpc_error(response: &RuntimeJson, id: u64, code: i64, message_parts: &[&str]) {
    assert_eq!(
        response["id"].as_u64(),
        Some(id),
        "the refusal preserves request identity"
    );
    assert_eq!(response["error"]["code"].as_i64(), Some(code));
    assert!(
        response.get("result").is_none(),
        "a refused request cannot also publish a result"
    );
    let message = scalar(&response["error"]["message"]);
    for part in message_parts {
        assert!(
            message.contains(part),
            "expected refusal component {part:?}, got {message:?}"
        );
    }
}

async fn unchanged_planning(client: &mut ExactClient, session: &str, before: &RuntimeJson) {
    let reply = client
        .request(190, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        &result(&reply)["planning"],
        before,
        "invalid requests cannot change plan, revision, mode, questions or receipt authority"
    );
}

async fn start(client: &mut ExactClient, id: u64, thread: &str, resume: bool) -> String {
    let mut params = exact(json!({"init":{"threadId":thread}}));
    if resume {
        params["resume"] = RuntimeJson::String(thread.into());
    }
    scalar(&result(&client.request(id, "session/start", params).await)["sessionId"])
}

async fn propose(
    client: &mut ExactClient,
    thread: &str,
    submission: &str,
) -> (String, RuntimeJson) {
    result(
        &client
            .request(
                2,
                "session/setCollaborationMode",
                exact(json!({"threadId":thread,"mode":"plan","expectedRevision":0})),
            )
            .await,
    );
    let session = start(client, 3, thread, false).await;
    result(&client.request(4, "turn/start", exact(json!({"sessionId":session,"submissionId":submission,"input":"Produce the reviewed plan."}))).await);
    let terminal = client
        .until(|frame| {
            has(frame, "/method", "turn/completed") && has(frame, "/params/turnId", submission)
        })
        .await;
    let reply = client
        .request(5, "session/plans", exact(json!({"sessionId":session})))
        .await;
    let plan = result(&reply)["planning"]["plans"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(plan["status"], RuntimeJson::from("proposed"));
    assert_eq!(terminal["params"]["result"]["finalText"], plan["content"]);
    let history = client
        .request(6, "session/history", exact(json!({"sessionId":session})))
        .await;
    let source = source_message(
        &result(&history)["history"],
        &framed(plan["content"].as_string().unwrap()),
    );
    assert_final_source_identity(&terminal, source, "exact-source-message");
    let source_id = scalar(&source["uuid"]);
    let core_turn_id = scalar(&plan["turnId"]);
    assert_eq!(
        result(&history)["historyTurnIds"][source_id.as_str()],
        plan["turnId"]
    );
    assert_eq!(
        result(&history)["turnFinalMessageIds"][core_turn_id.as_str()],
        source["uuid"]
    );
    (session, plan)
}

fn text_parameter(mut params: RuntimeJson, key: &str, value: &RuntimeString) -> RuntimeJson {
    params[key] = RuntimeJson::String(value.clone());
    params
}

fn source_position(history: &RuntimeJson, expected: &RuntimeString) -> usize {
    let positions: Vec<_> = history
        .as_array()
        .expect("ordered original history")
        .iter()
        .enumerate()
        .filter(|(_, message)| {
            has(message, "/type", "assistant") && message_text(message) == *expected
        })
        .map(|(position, _)| position)
        .collect();
    assert_eq!(
        positions.len(),
        1,
        "the exact original assistant source is retained once; expected={expected:?}, history={history:?}"
    );
    positions[0]
}

fn source_message<'a>(history: &'a RuntimeJson, expected: &RuntimeString) -> &'a RuntimeJson {
    &history[source_position(history, expected)]
}

fn assert_final_source_identity(terminal: &RuntimeJson, source: &RuntimeJson, provider_id: &str) {
    let text_blocks: Vec<_> = source["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| has(block, "/type", "text"))
        .collect();
    assert_eq!(
        text_blocks.len(),
        1,
        "the fixture has one owning final text block"
    );
    let display_id = &text_blocks[0]["messageId"];
    assert_eq!(
        display_id, &source["uuid"],
        "the first displayed text item owns its persisted assistant source"
    );
    assert_ne!(
        display_id,
        &RuntimeJson::from(provider_id),
        "provider message identity is separate from the engine's displayed item identity"
    );
    assert_eq!(
        &terminal["params"]["result"]["finalMessageId"], display_id,
        "the terminal names the exact displayed source item"
    );
}

fn message_text(message: &RuntimeJson) -> RuntimeString {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == RuntimeJson::from("text"))
        .fold(RuntimeString::default(), |text, block| {
            text.concat(block["text"].as_string().unwrap())
        })
}

fn request_contains(request: &ModelRequest, expected: &RuntimeString) -> bool {
    request.messages.iter().any(|message| matches!(message, Message::User(user)
        if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text == *expected))))
}

fn document_inventory(path: &Path) -> BTreeMap<String, Vec<u8>> {
    let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .take(17)
        .collect();
    assert!(
        entries.len() <= 16,
        "bounded document and conflict-backup inventory"
    );
    entries
        .into_iter()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
            assert!(metadata.is_file() && metadata.len() <= 4_000_000);
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn split_supplementary_plan_deltas_survive_live_items_follow_cut_and_replay() {
    let high = RuntimeString::from_utf16(vec![0xd83e]);
    let low = RuntimeString::from_utf16(vec![0xdd80]);
    let prefix = RuntimeString::from("# Split plan\n\nCharacter: ");
    let content = prefix
        .concat(&high)
        .concat(&low)
        .concat(&" verified".into());
    let release = Arc::new(Notify::new());
    let fixture = Fixture::new(vec![Script {
        before: vec![
            ev::message_start(None),
            ModelStreamEvent::TextMessage {
                id: "split-source".into(),
                phase: Some("final_answer".into()),
            },
            ev::text("<proposed_plan>\n# Split plan\n\nCharacter: "),
            ModelStreamEvent::TextDelta { text: high.clone() },
        ],
        release: Some(release.clone()),
        after: vec![
            ModelStreamEvent::TextDelta { text: low.clone() },
            ev::text(" verified\n</proposed_plan>"),
            ev::stop(Some(ModelStopReason::EndTurn), None),
        ],
    }]);
    let mut client = fixture.endpoint().await;
    result(
        &client
            .request(
                2,
                "session/setCollaborationMode",
                exact(json!({"threadId":"split-thread","mode":"plan","expectedRevision":0})),
            )
            .await,
    );
    let session = start(&mut client, 3, "split-thread", false).await;
    result(&client.request(4, "turn/start", exact(json!({"sessionId":session,"submissionId":"split-submission","input":"Produce the split plan."}))).await);
    let high_frame = client
        .until(|frame| {
            has(frame, "/method", "session/event")
                && has(frame, "/params/event/event/type", "plan.delta")
                && frame["params"]["event"]["event"]["delta"].as_string() == Some(&high)
        })
        .await;
    let plan_id = scalar(&high_frame["params"]["event"]["event"]["planId"]);
    let cut = client
        .request(
            5,
            "session/follow",
            exact(json!({"sessionId":session,"includeOriginalHistory":true})),
        )
        .await;
    let cut = result(&cut);
    assert_eq!(cut["mode"], RuntimeJson::from("snapshot"));
    let cut_revision = number(&cut["revision"]);
    let stream = scalar(&cut["streamId"]);
    let active = cut["snapshot"]["activeItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| has(row, "/item/id", &plan_id))
        .unwrap();
    assert_eq!(active["item"]["type"], RuntimeJson::from("proposed_plan"));
    assert_eq!(
        active["item"]["plan"]["content"].as_string(),
        Some(&prefix.concat(&high))
    );
    assert_eq!(
        active["item"]["plan"]["status"],
        RuntimeJson::from("writing")
    );
    let document_revision = number(&active["item"]["plan"]["revision"]);
    release.notify_one();
    let terminal = client
        .until(|frame| {
            has(frame, "/method", "turn/completed")
                && has(frame, "/params/turnId", "split-submission")
        })
        .await;
    assert_eq!(
        terminal["params"]["result"]["finalText"].as_string(),
        Some(&content)
    );
    let completed = client
        .until(|frame| {
            has(frame, "/method", "item/completed")
                && has(frame, "/params/item/id", plan_id.as_str())
        })
        .await;
    assert_eq!(
        completed["params"]["item"]["plan"]["content"].as_string(),
        Some(&content)
    );
    assert_eq!(
        completed["params"]["item"]["plan"]["status"],
        RuntimeJson::from("proposed")
    );
    let core_turn_id = scalar(&high_frame["params"]["event"]["event"]["turnId"]);
    assert_eq!(active["turnId"], RuntimeJson::from(core_turn_id.as_str()));
    let lifecycle: Vec<_> = client
        .frames
        .iter()
        .filter(|frame| {
            ["item/started", "item/updated", "item/completed"]
                .iter()
                .any(|method| has(frame, "/method", method))
                && has(frame, "/params/item/id", &plan_id)
        })
        .collect();
    for method in ["item/started", "item/updated", "item/completed"] {
        assert!(
            lifecycle.iter().any(|frame| has(frame, "/method", method)),
            "{method} preserves the proposed plan lifecycle"
        );
    }
    for method in ["item/started", "item/completed"] {
        assert_eq!(
            lifecycle
                .iter()
                .filter(|frame| has(frame, "/method", method))
                .count(),
            1,
            "{method} is published exactly once for the plan identity"
        );
    }
    for frame in &lifecycle {
        assert!(has(frame, "/params/streamId", &stream));
        assert!(has(frame, "/params/turnId", &core_turn_id));
        assert!(has(frame, "/params/item/plan/id", &plan_id));
        assert!(has(frame, "/params/item/plan/turnId", &core_turn_id));
        assert!(
            client
                .frames
                .iter()
                .any(|raw| has(raw, "/method", "session/event")
                    && has(raw, "/params/streamId", &stream)
                    && raw["params"]["streamRevision"] == frame["params"]["streamRevision"]
                    && raw.pointer("/params/event/event/item") == frame.pointer("/params/item")),
            "item projection reuses its raw event's stream revision"
        );
    }
    let item_revisions: Vec<_> = lifecycle
        .iter()
        .map(|frame| number(&frame["params"]["item"]["plan"]["revision"]))
        .collect();
    let raw: Vec<_> = client
        .frames
        .iter()
        .filter(|frame| {
            has(frame, "/method", "session/event")
                && has(frame, "/params/event/event/type", "plan.delta")
        })
        .collect();
    let deltas: Vec<_> = raw
        .iter()
        .map(|frame| {
            frame["params"]["event"]["event"]["delta"]
                .as_string()
                .unwrap()
                .clone()
        })
        .collect();
    assert!(deltas.contains(&high) && deltas.contains(&low));
    assert_eq!(
        deltas
            .iter()
            .fold(RuntimeString::default(), |all, delta| all.concat(delta)),
        content
    );
    let document_revisions: Vec<_> = raw
        .iter()
        .map(|frame| number(&frame["params"]["event"]["event"]["revision"]))
        .collect();
    let stream_revisions: Vec<_> = raw
        .iter()
        .map(|frame| number(&frame["params"]["streamRevision"]))
        .collect();
    let revision_trace: Vec<_> = document_revisions
        .iter()
        .copied()
        .zip(stream_revisions.iter().copied())
        .take(16)
        .collect();
    eprintln!(
        "[exact-plan-revision-trace] artifactRevision={} deltaCount={} truncated={} entries={:?}",
        document_revision,
        document_revisions.len(),
        document_revisions.len() > 16,
        revision_trace
    );
    assert!(item_revisions.windows(2).all(|pair| pair[1] >= pair[0]));
    assert!(item_revisions
        .iter()
        .all(|revision| *revision == document_revision));
    assert!(document_revisions
        .iter()
        .all(|revision| *revision == document_revision));
    assert!(stream_revisions.windows(2).all(|pair| pair[1] > pair[0]));
    for raw in &raw {
        assert_eq!(
            raw["params"]["event"]["event"]["planId"],
            RuntimeJson::from(plan_id.as_str())
        );
        assert!(has(raw, "/params/event/event/turnId", &core_turn_id));
        assert!(has(raw, "/params/streamId", &stream));
        let revision = &raw["params"]["streamRevision"];
        assert!(
            client
                .frames
                .iter()
                .any(|frame| has(frame, "/method", "session/rawEvent")
                    && has(frame, "/params/streamId", &stream)
                    && frame["params"]["streamRevision"] == *revision
                    && frame["params"]["event"] == raw["params"]["event"]),
            "raw surface must retain the same exact event and stream revision"
        );
    }
    let replay = client.request(6, "session/follow", exact(json!({"sessionId":session,"streamId":stream,"afterRevision":cut_revision,"includeOriginalHistory":true}))).await;
    let replay = result(&replay);
    assert_eq!(replay["mode"], RuntimeJson::from("replay"));
    assert_eq!(replay["streamId"], RuntimeJson::from(stream.as_str()));
    assert_eq!(replay["sessionId"], RuntimeJson::from(session.as_str()));
    let events = replay["events"].as_array().unwrap();
    let replay_revisions: Vec<_> = events
        .iter()
        .map(|event| number(&event["revision"]))
        .collect();
    assert_eq!(replay_revisions.first(), Some(&(cut_revision + 1)));
    assert!(replay_revisions
        .windows(2)
        .all(|pair| pair[1] == pair[0] + 1));
    assert_eq!(
        replay_revisions.last().copied(),
        Some(number(&replay["revision"]))
    );
    assert!(events
        .iter()
        .any(|event| has(event, "/event/event/type", "plan.delta")
            && event["event"]["event"]["delta"].as_string() == Some(&low)));
    for event in events {
        assert!(client
            .frames
            .iter()
            .any(|frame| has(frame, "/method", "session/event")
                && has(frame, "/params/streamId", &stream)
                && frame["params"]["streamRevision"] == event["revision"]
                && frame["params"]["event"] == event["event"]));
    }
    let saved = client
        .request(7, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        result(&saved)["planning"]["plans"][0]["content"].as_string(),
        Some(&content)
    );
    assert_eq!(
        number(&result(&saved)["planning"]["plans"][0]["revision"]),
        document_revision
    );
    let history = client
        .request(
            8,
            "engine/readThreadHistory",
            exact(json!({"threadId":"split-thread"})),
        )
        .await;
    let source = source_message(&result(&history)["history"], &framed(&content));
    assert_final_source_identity(&terminal, source, "split-source");
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    client.close().await;
    let mut reconnected = fixture.endpoint().await;
    let session = start(&mut reconnected, 9, "split-thread", true).await;
    let followed = reconnected.request(10, "session/follow", exact(json!({"sessionId":session,"streamId":stream,"afterRevision":cut_revision,"includeOriginalHistory":true}))).await;
    assert!(
        has(result(&followed), "/mode", "snapshot"),
        "a disconnected connection's cursor cannot replay a different stream epoch"
    );
    assert_ne!(
        result(&followed)["streamId"],
        RuntimeJson::from(stream.as_str())
    );
    assert_eq!(
        source_message(&result(&followed)["snapshot"]["history"], &framed(&content)),
        source_message(&result(&history)["history"], &framed(&content))
    );
    let plans = reconnected
        .request(11, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        result(&plans)["planning"]["plans"][0],
        result(&saved)["planning"]["plans"][0]
    );
    assert_eq!(
        fixture.probe.calls.load(Ordering::SeqCst),
        1,
        "real disconnect and resume do not infer a second turn"
    );
    reconnected.close().await;
}

#[tokio::test]
async fn saved_non_scalar_plan_units_remain_distinct_across_provider_free_restart_reads() {
    let bodies: Vec<_> = [
        vec![0xd800],
        vec![0xdc00],
        vec![0xfffd],
        vec![0xd83e, 0xdd80],
    ]
    .iter()
    .map(|units| body(units))
    .collect();
    assert_eq!(bodies[0].to_node_utf8(), bodies[1].to_node_utf8());
    assert_ne!(bodies[0], bodies[1]);
    assert_ne!(bodies[0], bodies[2]);
    let fixture = Fixture::new(
        bodies
            .iter()
            .map(|content| Script::text(framed(content)))
            .collect(),
    );
    let mut source = Vec::new();
    let mut original_cursor = None;
    let mut client = fixture.endpoint().await;
    for (index, content) in bodies.iter().enumerate() {
        let thread = format!("saved-unit-{index}");
        let (session, plan) = propose(&mut client, &thread, &format!("proposal-{index}")).await;
        assert_eq!(plan["content"].as_string(), Some(content));
        let history = client
            .request(
                10,
                "engine/readThreadHistory",
                exact(json!({"threadId":thread})),
            )
            .await;
        source_message(&result(&history)["history"], &framed(content));
        if index == 0 {
            let follow = client
                .request(
                    11,
                    "session/follow",
                    exact(json!({"sessionId":session,"includeOriginalHistory":true})),
                )
                .await;
            assert_eq!(
                source_message(&result(&follow)["snapshot"]["history"], &framed(content)),
                source_message(&result(&history)["history"], &framed(content))
            );
            original_cursor = Some((
                scalar(&result(&follow)["streamId"]),
                number(&result(&follow)["revision"]),
            ));
        }
        source.push((thread, plan, result(&history)["history"].clone()));
    }
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), bodies.len());
    client.close().await;
    let creates = fixture.probe.creates.load(Ordering::SeqCst);
    let mut cold = fixture.endpoint().await;
    for ((thread, plan, original_history), content) in source.iter().zip(&bodies) {
        for method in ["session/plans", "session/collaborationMode"] {
            let reply = cold
                .request(20, method, exact(json!({"threadId":thread})))
                .await;
            assert_eq!(
                result(&reply)["planning"]["plans"][0]["content"].as_string(),
                Some(content)
            );
        }
        let params = exact(
            json!({"threadId":thread,"planId":scalar(&plan["id"]),"revision":number(&plan["revision"])}),
        );
        let first = cold
            .request(21, "session/readPlanDocument", params.clone())
            .await;
        let second = cold.request(22, "session/readPlanDocument", params).await;
        for reply in [&first, &second] {
            assert_eq!(
                result(reply)["document"]["content"].as_string(),
                Some(content)
            );
            assert_eq!(result(reply)["document"]["revision"], plan["revision"]);
        }
        assert_eq!(
            std::fs::read(scalar(&result(&first)["document"]["path"])).unwrap(),
            content.to_node_utf8()
        );
        let history = cold
            .request(
                23,
                "engine/readThreadHistory",
                exact(json!({"threadId":thread})),
            )
            .await;
        assert_eq!(
            result(&history)["history"],
            *original_history,
            "provider-free restart preserves complete source rows and their positions"
        );
        let projected = cold
            .request(
                24,
                "engine/readThread",
                exact(json!({"threadId":thread,"limit":10})),
            )
            .await;
        let projected_items: Vec<_> = result(&projected)["turns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["items"].as_array().unwrap())
            .collect();
        let positions: Vec<_> = projected_items
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                has(item, "/type", "agentMessage")
                    && item["text"].as_string() == Some(&framed(content))
            })
            .map(|(position, _)| position)
            .collect();
        assert_eq!(
            positions.len(),
            1,
            "full projection retains the exact source item once"
        );
        assert!(
            projected_items[..positions[0]]
                .iter()
                .any(|item| has(item, "/type", "userMessage")),
            "source item stays after its owning user input"
        );
        assert_eq!(
            fixture.probe.creates.load(Ordering::SeqCst),
            creates,
            "cold planning/history reads do not construct Session"
        );
        assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), bodies.len());
    }
    // Restored context and follow still must preserve exact source rows; no model request is allowed.
    let (thread, _, original_history) = &source[0];
    let expected_source = source_message(original_history, &framed(&bodies[0]));
    let expected_position = source_position(original_history, &framed(&bodies[0]));
    let session = start(&mut cold, 30, thread, true).await;
    let history = cold
        .request(31, "session/history", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        source_message(&result(&history)["history"], &framed(&bodies[0])),
        expected_source
    );
    assert_eq!(
        source_position(&result(&history)["history"], &framed(&bodies[0])),
        expected_position
    );
    let (prior_stream, prior_revision) = original_cursor.unwrap();
    let follow = cold.request(32, "session/follow", exact(json!({"sessionId":session,"streamId":prior_stream,"afterRevision":prior_revision,"includeOriginalHistory":true}))).await;
    assert!(has(result(&follow), "/mode", "snapshot"));
    assert_ne!(
        result(&follow)["streamId"],
        RuntimeJson::from(prior_stream.as_str())
    );
    assert_eq!(
        source_message(&result(&follow)["snapshot"]["history"], &framed(&bodies[0])),
        expected_source
    );
    assert_eq!(
        source_position(&result(&follow)["snapshot"]["history"], &framed(&bodies[0])),
        expected_position
    );
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), bodies.len());
    cold.close().await;
}

#[tokio::test]
async fn named_exact_edit_accept_and_lost_ack_admit_the_complete_reviewed_input_once() {
    let original = body(&[0xd800]);
    let edited = body(&[0xdc00, 0xfffd, 0xd83e, 0xdd80]);
    let fixture = Fixture::new(vec![
        Script::text(framed(&original)),
        Script::text("Implementation complete.".into()),
    ]);
    let mut client = fixture.endpoint().await;
    let (session, plan) = propose(&mut client, "exact-implementation", "proposal").await;
    let identity = json!({"sessionId":session,"planId":scalar(&plan["id"]),"revision":number(&plan["revision"])});
    let document = client
        .request(10, "session/readPlanDocument", exact(identity.clone()))
        .await;
    assert_eq!(
        result(&document)["document"]["content"].as_string(),
        Some(&original)
    );
    let document_path = scalar(&result(&document)["document"]["path"]);
    let before = result(&document)["planning"].clone();
    let files_before = document_inventory(Path::new(&document_path));
    let projected_twin = body(&[0xdc00]);
    assert_ne!(projected_twin, original);
    assert_eq!(projected_twin.to_node_utf8(), original.to_node_utf8());
    let wrong_exact_cas = text_parameter(
        text_parameter(exact(identity.clone()), "content", &edited),
        "expectedContent",
        &projected_twin,
    );
    rejected(
        &client
            .request(110, "session/savePlanDocument", wrong_exact_cas)
            .await,
    );
    let unchanged = client
        .request(111, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        result(&unchanged)["planning"],
        before,
        "rejected exact baseline preserves the complete authoritative snapshot"
    );
    assert_eq!(
        document_inventory(Path::new(&document_path)),
        files_before,
        "rejected exact baseline creates no temporary file or conflict backup"
    );
    let save = text_parameter(
        text_parameter(exact(identity.clone()), "content", &edited),
        "expectedContent",
        &original,
    );
    let saved = client.request(11, "session/savePlanDocument", save).await;
    assert_eq!(
        result(&saved)["document"]["content"].as_string(),
        Some(&edited)
    );
    let revision = number(&result(&saved)["document"]["revision"]);
    assert!(revision > number(&plan["revision"]));
    rejected(
        &client
            .request(
                12,
                "session/savePlanDocument",
                text_parameter(exact(identity), "content", &original),
            )
            .await,
    );
    let identity = json!({"sessionId":session,"planId":scalar(&plan["id"]),"revision":revision});
    let wrong_cas = text_parameter(
        text_parameter(exact(identity.clone()), "content", &original),
        "expectedContent",
        &"different external bytes".into(),
    );
    rejected(
        &client
            .request(13, "session/savePlanDocument", wrong_cas)
            .await,
    );
    std::fs::write(&document_path, "# A concurrent editor changed the file.").unwrap();
    let external_files = document_inventory(Path::new(&document_path));
    let external_cas = text_parameter(
        text_parameter(exact(identity.clone()), "content", &original),
        "expectedContent",
        &edited,
    );
    rejected(
        &client
            .request(112, "session/savePlanDocument", external_cas)
            .await,
    );
    let unchanged = client
        .request(113, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(result(&unchanged)["planning"], result(&saved)["planning"]);
    assert_eq!(
        document_inventory(Path::new(&document_path)),
        external_files,
        "a concurrent external edit is never overwritten or backed up by a rejected save"
    );
    std::fs::write(&document_path, edited.to_node_utf8()).unwrap();
    let reread = client
        .request(14, "session/readPlanDocument", exact(identity.clone()))
        .await;
    assert_eq!(
        result(&reread)["document"]["content"].as_string(),
        Some(&edited)
    );
    assert_eq!(
        result(&reread)["document"]["revision"].as_u64(),
        Some(revision)
    );
    let expected = RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&edited);
    for (outcome, request_id) in [
        ("cancelled", "cancelled-exact-action"),
        ("failed", "failed-exact-action"),
    ] {
        let mut prepare = text_parameter(exact(identity.clone()), "content", &edited);
        prepare["clientRequestId"] = RuntimeJson::String(request_id.into());
        let prepared = client.request(120, "session/acceptPlan", prepare).await;
        assert_eq!(result(&prepared)["accepted"], RuntimeJson::Bool(true));
        assert_eq!(
            result(&prepared)["implementationInput"].as_string(),
            Some(&expected)
        );
        let implementation = scalar(&result(&prepared)["implementationId"]);
        let settled = if outcome == "cancelled" {
            client
                .request(
                    121,
                    "session/cancelPlanImplementation",
                    exact(identity.clone()),
                )
                .await
        } else {
            let mut settle = exact(identity.clone());
            settle["clientRequestId"] = RuntimeJson::String(request_id.into());
            settle["outcome"] = RuntimeJson::String(outcome.into());
            client
                .request(121, "session/settlePlanImplementation", settle)
                .await
        };
        assert!(has(
            result(&settled),
            "/planning/implementation/admission",
            outcome
        ));
        assert!(has(
            result(&settled),
            "/planning/plans/0/status",
            "proposed"
        ));
        let mut turn = exact(json!({"sessionId":session,"submissionId":implementation}));
        turn["input"] = RuntimeJson::String(expected.clone());
        result(&client.request(122, "turn/start", turn).await);
        client
            .until(|frame| {
                has(frame, "/params/event/submissionId", &implementation)
                    && frame.pointer("/params/event/error").is_some()
            })
            .await;
        assert_eq!(
            fixture.probe.calls.load(Ordering::SeqCst),
            1,
            "a terminal unadmitted receipt cannot execute a provider"
        );
    }
    let mut accept = text_parameter(exact(identity), "content", &edited);
    accept["clientRequestId"] = RuntimeJson::String("exact-implementation-action".into());
    let accepted = client
        .request(15, "session/acceptPlan", accept.clone())
        .await;
    assert_eq!(result(&accepted)["accepted"], RuntimeJson::Bool(true));
    assert_eq!(
        result(&accepted)["implementationInput"].as_string(),
        Some(&expected)
    );
    let implementation = scalar(&result(&accepted)["implementationId"]);
    let duplicate = client
        .request(16, "session/acceptPlan", accept.clone())
        .await;
    assert_eq!(result(&duplicate)["accepted"], RuntimeJson::Bool(false));
    assert_eq!(
        result(&duplicate)["implementationInput"].as_string(),
        Some(&expected)
    );
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    let mut start_message = exact(
        json!({"id":17,"method":"turn/start","params":{"sessionId":session,"submissionId":implementation}}),
    );
    start_message["params"]["input"] = RuntimeJson::String(expected.clone());
    client.send(start_message).await;
    fixture.wait_calls(2).await;
    // Drop the transport before consuming the turn ACK; admission is the engine's durable receipt.
    client.close().await;
    assert_eq!(
        fixture
            .probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request_contains(request, &expected))
            .count(),
        1
    );
    let mut restarted = fixture.endpoint().await;
    let saved = restarted
        .request(
            20,
            "session/plans",
            exact(json!({"threadId":"exact-implementation"})),
        )
        .await;
    assert_eq!(
        result(&saved)["planning"]["implementation"]["input"].as_string(),
        Some(&expected)
    );
    assert_eq!(
        result(&saved)["planning"]["implementation"]["admission"],
        RuntimeJson::from("started")
    );
    let resumed = start(&mut restarted, 21, "exact-implementation", true).await;
    accept["sessionId"] = RuntimeJson::String(resumed.clone().into());
    let retried = restarted.request(22, "session/acceptPlan", accept).await;
    assert_eq!(result(&retried)["accepted"], RuntimeJson::Bool(false));
    let mut retry = exact(json!({"sessionId":resumed,"submissionId":implementation}));
    retry["input"] = RuntimeJson::String(expected.clone());
    result(&restarted.request(23, "turn/start", retry).await);
    restarted
        .until(|frame| {
            has(frame, "/params/event/submissionId", implementation.as_str())
                && frame.pointer("/params/event/error").is_some()
        })
        .await;
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request_contains(request, &expected))
            .count(),
        1
    );
    restarted.close().await;
}

#[tokio::test]
async fn named_feedback_receipts_reject_changed_units_with_the_same_utf8_projection() {
    let content = body(&[0xd800]);
    let feedback = RuntimeString::from("Revise ").concat(&RuntimeString::from_utf16(vec![0xd800]));
    let changed = RuntimeString::from("Revise ").concat(&RuntimeString::from_utf16(vec![0xdc00]));
    assert_eq!(feedback.to_node_utf8(), changed.to_node_utf8());
    assert_ne!(feedback, changed);
    let fixture = Fixture::new(vec![
        Script::text(framed(&content)),
        Script::text("Feedback received.".into()),
    ]);
    let mut client = fixture.endpoint().await;
    let (session, plan) = propose(&mut client, "exact-feedback", "proposal-feedback").await;
    let mut params = exact(
        json!({"sessionId":session,"planId":scalar(&plan["id"]),"revision":number(&plan["revision"]),"clientRequestId":"feedback-intent"}),
    );
    params["feedback"] = RuntimeJson::String(feedback.clone());
    let first = client
        .request(10, "session/planFeedback", params.clone())
        .await;
    let expected =
        RuntimeString::from("Plan feedback for the current proposed plan:\n\n").concat(&feedback);
    assert_eq!(result(&first)["feedbackInput"].as_string(), Some(&expected));
    assert_eq!(result(&first)["accepted"], RuntimeJson::Bool(true));
    let duplicate = client
        .request(11, "session/planFeedback", params.clone())
        .await;
    assert_eq!(result(&duplicate)["accepted"], RuntimeJson::Bool(false));
    let before = result(&duplicate)["planning"].clone();
    let mut wrong = params.clone();
    wrong["feedback"] = RuntimeJson::String(changed.clone());
    rejected(&client.request(12, "session/planFeedback", wrong).await);
    let unchanged = client
        .request(120, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(result(&unchanged)["planning"], before);
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    client.close().await;
    let creates = fixture.probe.creates.load(Ordering::SeqCst);
    let mut restarted = fixture.endpoint().await;
    let saved = restarted
        .request(
            13,
            "session/plans",
            exact(json!({"threadId":"exact-feedback"})),
        )
        .await;
    assert_eq!(
        result(&saved)["planning"]["implementation"]["input"].as_string(),
        Some(&expected)
    );
    assert_eq!(fixture.probe.creates.load(Ordering::SeqCst), creates);
    let session = start(&mut restarted, 14, "exact-feedback", true).await;
    params["sessionId"] = RuntimeJson::String(session.clone().into());
    let same = restarted
        .request(15, "session/planFeedback", params.clone())
        .await;
    assert_eq!(result(&same)["accepted"], RuntimeJson::Bool(false));
    assert_eq!(result(&same)["feedbackInput"].as_string(), Some(&expected));
    let mut wrong = params;
    wrong["feedback"] = RuntimeJson::String(changed);
    rejected(&restarted.request(150, "session/planFeedback", wrong).await);
    let unchanged = restarted
        .request(151, "session/plans", exact(json!({"sessionId":session})))
        .await;
    assert_eq!(
        result(&unchanged)["planning"],
        result(&same)["planning"],
        "a restarted receipt still compares exact feedback identity"
    );
    let implementation = scalar(&result(&same)["implementationId"]);
    let mut turn = exact(json!({"sessionId":session,"submissionId":implementation}));
    turn["input"] = RuntimeJson::String(expected.clone());
    result(&restarted.request(16, "turn/start", turn).await);
    restarted
        .until(|frame| {
            has(frame, "/method", "turn/completed")
                && has(frame, "/params/turnId", implementation.as_str())
        })
        .await;
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request_contains(request, &expected))
            .count(),
        1
    );
    restarted.close().await;
}

#[tokio::test]
async fn exact_payload_support_preserves_strict_json_fields_revisions_and_session_binding() {
    let content = body(&[0xdc00]);
    let fixture = Fixture::new(vec![Script::text(framed(&content))]);
    let mut client = fixture.endpoint().await;
    let (session, plan) = propose(&mut client, "strict-exact", "strict-proposal").await;
    let identity = exact(
        json!({"sessionId":session,"planId":scalar(&plan["id"]),"revision":number(&plan["revision"])}),
    );
    let document = client
        .request(70, "session/readPlanDocument", identity.clone())
        .await;
    let document_path = scalar(&result(&document)["document"]["path"]);
    let files_before = document_inventory(Path::new(&document_path));
    let mut receipt = text_parameter(identity.clone(), "feedback", &content);
    receipt["clientRequestId"] = RuntimeJson::String("strict-existing-receipt".into());
    let prepared = client.request(71, "session/planFeedback", receipt).await;
    assert_eq!(result(&prepared)["accepted"], RuntimeJson::Bool(true));
    let before = result(&prepared)["planning"].clone();
    assert!(has(
        &before,
        "/implementation/clientRequestId",
        "strict-existing-receipt"
    ));
    assert!(has(&before, "/implementation/admission", "pending"));
    client
        .writer
        .write_all(
            b"{\"id\":80,\"method\":\"session/plans\",\"params\":{\"threadId\":\"bad\\uZZZZ\"}}\n",
        )
        .await
        .unwrap();
    client.writer.flush().await.unwrap();
    let malformed = client
        .until(|frame| frame.pointer("/error/code").and_then(RuntimeJson::as_i64) == Some(-32700))
        .await;
    assert_eq!(malformed["error"]["code"].as_i64(), Some(-32700));
    assert!(malformed["id"].is_null());
    assert!(scalar(&malformed["error"]["message"]).contains("invalid JSON"));
    assert!(scalar(&malformed["error"]["message"]).contains("escape"));
    unchanged_planning(&mut client, &session, &before).await;
    for (index, revision) in [
        exact(json!(-1)),
        exact(json!(1.5)),
        RuntimeJson::Null,
        RuntimeJson::String("1".into()),
        exact(json!(18_446_744_073_709_551_616.0_f64)),
    ]
    .into_iter()
    .enumerate()
    {
        let mut params = identity.clone();
        params["revision"] = revision;
        let id = 90 + index as u64;
        let reply = client
            .request(id, "session/readPlanDocument", params.clone())
            .await;
        rpc_error(&reply, id, wire::INVALID_PARAMS, &["invalid params", "u64"]);
        params["content"] = RuntimeJson::String(content.clone());
        let reply = client
            .request(id + 200, "session/savePlanDocument", params)
            .await;
        rpc_error(&reply, id + 200, wire::INVALID_PARAMS, &["u64"]);
        unchanged_planning(&mut client, &session, &before).await;
        assert_eq!(document_inventory(Path::new(&document_path)), files_before);
    }
    for (index, method) in [
        "session/savePlanDocument",
        "session/acceptPlan",
        "session/planFeedback",
    ]
    .into_iter()
    .enumerate()
    {
        let mut unknown = identity.clone();
        if method == "session/planFeedback" {
            unknown["feedback"] = RuntimeJson::String(content.clone());
        } else {
            unknown["content"] = RuntimeJson::String(content.clone());
        }
        if method != "session/savePlanDocument" {
            unknown["clientRequestId"] =
                RuntimeJson::String(format!("invalid-action-{index}").into());
        }
        unknown["unexpected"] = RuntimeJson::Bool(true);
        let id = 100 + index as u64;
        let reply = client.request(id, method, unknown).await;
        rpc_error(
            &reply,
            id,
            wire::INVALID_PARAMS,
            &["unknown field", "unexpected"],
        );
        unchanged_planning(&mut client, &session, &before).await;
        assert_eq!(document_inventory(Path::new(&document_path)), files_before);
    }
    for (index, (method, field)) in [
        ("session/savePlanDocument", "content"),
        ("session/acceptPlan", "content"),
        ("session/planFeedback", "feedback"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut params = identity.clone();
        params[field] = RuntimeJson::Bool(false);
        if method != "session/savePlanDocument" {
            params["clientRequestId"] = RuntimeJson::String("invalid-type-intent".into());
        }
        let id = 120 + index as u64;
        let reply = client.request(id, method, params).await;
        rpc_error(&reply, id, wire::INVALID_PARAMS, &["string"]);
        unchanged_planning(&mut client, &session, &before).await;
    }
    let mut wrong = identity;
    wrong["threadId"] = RuntimeJson::String("another-thread".into());
    let reply = client.request(110, "session/readPlanDocument", wrong).await;
    rpc_error(
        &reply,
        110,
        wire::INVALID_PARAMS,
        &["Session and thread identities differ"],
    );
    unchanged_planning(&mut client, &session, &before).await;
    let after = client
        .request(
            111,
            "session/plans",
            exact(json!({"threadId":"strict-exact"})),
        )
        .await;
    assert_eq!(result(&after)["planning"], before);
    for key in ["id", "revision", "content", "status"] {
        assert_eq!(result(&after)["planning"]["plans"][0][key], plan[key]);
    }
    assert_eq!(document_inventory(Path::new(&document_path)), files_before);
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test]
async fn scalar_chinese_and_complete_supplementary_controls_use_the_same_named_path() {
    let content = RuntimeString::from("# 中文计划\n\n验证完整标量 🦀，再检查结果。");
    let ordinary =
        RuntimeString::from("Ordinary exact input: ").concat(&RuntimeString::from_utf16(vec![
            0xd800, 0x0020, 0xdc00, 0x0020, 0xfffd, 0x0020, 0xd83e, 0xdd80,
        ]));
    let fixture = Fixture::new(vec![
        Script::text(framed(&content)),
        Script::text("Ordinary exact input received.".into()),
    ]);
    let mut client = fixture.endpoint().await;
    let (session, plan) = propose(&mut client, "scalar-control", "scalar-proposal").await;
    assert_eq!(plan["content"].as_string(), Some(&content));
    let doc = client.request(10, "session/readPlanDocument", exact(json!({"sessionId":session,"planId":scalar(&plan["id"]),"revision":number(&plan["revision"])}))).await;
    assert_eq!(
        result(&doc)["document"]["content"].as_string(),
        Some(&content)
    );
    let history = client
        .request(
            11,
            "engine/readThreadHistory",
            exact(json!({"threadId":"scalar-control"})),
        )
        .await;
    source_message(&result(&history)["history"], &framed(&content));
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    result(
        &client
            .request(
                12,
                "session/setCollaborationMode",
                exact(json!({"sessionId":session,"mode":"default"})),
            )
            .await,
    );
    let mut params = exact(json!({"sessionId":session,"submissionId":"ordinary-exact-input"}));
    params["input"] = RuntimeJson::String(ordinary.clone());
    result(&client.request(13, "turn/start", params).await);
    client
        .until(|frame| {
            has(frame, "/method", "turn/completed")
                && has(frame, "/params/turnId", "ordinary-exact-input")
        })
        .await;
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request_contains(request, &ordinary))
            .count(),
        1,
        "ordinary nonreceipt input crosses the same named exact transport once"
    );
    let history = client
        .request(14, "session/history", exact(json!({"sessionId":session})))
        .await;
    let user_rows: Vec<_> = result(&history)["history"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| has(message, "/type", "user") && message_text(message) == ordinary)
        .collect();
    assert_eq!(user_rows.len(), 1);
    client.close().await;
}
