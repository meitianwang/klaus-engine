//! Realtime lifecycle, durable transcripts, and real task execution through the named facade.
use super::*;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::credential_store::{model_credential_key, CredentialStore};
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::Message as WsMessage;

type CapturedSession = Arc<std::sync::Mutex<Option<Arc<Session>>>>;
struct RealtimePeer {
    root: tempfile::TempDir,
    outgoing: mpsc::UnboundedReceiver<Value>,
    incoming: mpsc::UnboundedSender<Value>,
    listener: tokio::task::JoinHandle<()>,
    base_url: String,
    protocol: &'static str,
}
impl RealtimePeer {
    async fn start(refuse_hangup: bool) -> Self {
        Self::start_protocol(refuse_hangup, "public").await
    }
    async fn start_protocol(refuse_hangup: bool, protocol: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let prefix = match protocol {
            "public" => "/v1",
            "qoder" => "",
            _ => "/backend-api/codex",
        };
        let base_url = format!("http://{}{prefix}/", listener.local_addr().unwrap());
        let (sent, outgoing) = mpsc::unbounded_channel();
        let (incoming, received) = mpsc::unbounded_channel::<Value>();
        let received = Arc::new(tokio::sync::Mutex::new(Some(received)));
        let closed = Arc::new(Notify::new());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let sent = sent.clone();
                let received = received.clone();
                let closed = closed.clone();
                tokio::spawn(async move {
                    let mut first = [0u8; 4];
                    socket.peek(&mut first).await.unwrap();
                    if first == *b"GET " {
                        let mut socket = tokio_tungstenite::accept_hdr_async(socket, |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                            assert_eq!(request.headers().get("authorization").unwrap(), "Bearer fixture-realtime-secret");
                            match protocol {
                                "public" => assert_eq!(request.uri().query(), Some("call_id=rtc_fixture")),
                                "v1" => {
                                    assert_eq!(request.uri().path(), "/backend-api/codex/realtime");
                                    assert_eq!(request.uri().query(), Some("intent=quicksilver&call_id=rtc_fixture"));
                                    assert_eq!(request.headers()["openai-alpha"], "quicksilver=v1");
                                }
                                "frameless" => {
                                    assert_eq!(request.uri().path(), "/backend-api/codex/live/rtc_fixture");
                                    assert!(request.uri().query().is_none());
                                    assert_eq!(request.headers()["openai-alpha"], "quicksilver=v2");
                                }
                                "qoder" => {
                                    assert_eq!(request.uri().path(), "/api/v2/service/ws/asr");
                                    assert!(request.uri().query().is_none());
                                    assert!(request.headers().get("openai-alpha").is_none());
                                    assert!(request.headers().get("x-session-id").is_none());
                                }
                                _ => unreachable!(),
                            }
                            if protocol != "qoder" {
                                assert_eq!(request.headers()["x-session-id"], "voice-thread");
                            }
                            Ok(response)
                        }).await.unwrap();
                        let mut incoming = received.lock().await.take().unwrap();
                        socket.send(WsMessage::Text(json!({"type":"session.created","session":{"id":"fixture-session"}}).to_string())).await.unwrap();
                        loop {
                            tokio::select! {
                                message = incoming.recv() => { let Some(message) = message else { break; }; socket.send(WsMessage::Text(message.to_string())).await.unwrap(); }
                                _ = closed.notified() => { let _ = socket.close(None).await; break; }
                                message = socket.next() => { match message {
                                    Some(Ok(WsMessage::Text(text))) => {
                                        let event:Value=serde_json::from_str(&text).unwrap();
                                        if protocol != "public" && protocol != "qoder" { assert_ne!(event["type"],"session.update"); }
                                        if protocol != "public" { assert_ne!(event["type"],"response.cancel"); }
                                        if event["type"]=="session.update" {socket.send(WsMessage::Text(json!({"type":"session.updated","session":event["session"]}).to_string())).await.unwrap();}
                                        let close=event["type"]=="session.close";
                                        let _ = sent.send(event);
                                        if close {
                                            if protocol == "frameless" {
                                                socket.send(WsMessage::Text(json!({"type":"session.closed"}).to_string())).await.unwrap();
                                            } else { let _=socket.close(None).await; }
                                            break;
                                        }
                                    },
                                    Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                                    _ => {}
                                } }
                            }
                        }
                    } else {
                        let mut bytes = Vec::new();
                        let (headers, offset) = loop {
                            let mut part = [0u8; 4096];
                            let size = socket.read(&mut part).await.unwrap();
                            assert!(size > 0);
                            bytes.extend_from_slice(&part[..size]);
                            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n")
                            {
                                break (String::from_utf8(bytes[..end].to_vec()).unwrap(), end + 4);
                            }
                            assert!(bytes.len() < 1024 * 1024);
                        };
                        assert!(headers
                            .to_lowercase()
                            .contains("authorization: bearer fixture-realtime-secret"));
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(str::to_string)
                            })
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or_default();
                        while bytes.len() < offset + length {
                            let mut part = [0u8; 4096];
                            let size = socket.read(&mut part).await.unwrap();
                            assert!(size > 0);
                            bytes.extend_from_slice(&part[..size]);
                        }
                        if headers.starts_with(&format!("POST {prefix}/realtime/calls"))
                            && !headers.starts_with(&format!("POST {prefix}/realtime/calls/"))
                        {
                            let body = String::from_utf8(bytes[offset..].to_vec()).unwrap();
                            if protocol != "public" {
                                assert!(headers.starts_with("POST /backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas "));
                                let body: Value = serde_json::from_str(&body).unwrap();
                                assert_eq!(body["sdp"], "v=0\r\no=browser\r\n");
                                assert_eq!(body["session"]["audio"]["output"]["voice"], "cove");
                                assert!(!body["session"]
                                    .as_object()
                                    .unwrap()
                                    .contains_key("tools"));
                            }
                            let _ = sent.send(json!({"fixture":"offer","body":body}));
                            let answer = "v=0\r\no=fixture\r\n";
                            socket.write_all(format!("HTTP/1.1 201 Created\r\nLocation: /v1/realtime/calls/rtc_fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}", answer.len()).as_bytes()).await.unwrap();
                        } else {
                            assert_eq!(
                                protocol, "public",
                                "Hosted call closure must use the sideband lifecycle"
                            );
                            assert!(
                                headers.starts_with("POST /v1/realtime/calls/rtc_fixture/hangup ")
                            );
                            let _ = sent.send(json!({"fixture":"hangup"}));
                            socket.write_all(if refuse_hangup { b"HTTP/1.1 500 Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n" } else { b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n" }).await.unwrap();
                            closed.notify_one();
                        }
                    }
                });
            }
        });
        Self {
            root: tempfile::tempdir().unwrap(),
            outgoing,
            incoming,
            listener: task,
            base_url,
            protocol,
        }
    }
    async fn next(&mut self) -> Value {
        tokio::time::timeout(REPLY_TIMEOUT, self.outgoing.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        loop {
            let event = self.next().await;
            if predicate(&event) {
                return event;
            }
        }
    }
    async fn client(&self, replies: &[&str]) -> (TestClient, CapturedSession) {
        self.client_with_agent(make_agent_for_tests(
            replies.iter().map(|reply| text_turn(reply)).collect(),
        ))
        .await
    }
    async fn client_with_agent(&self, agent: Arc<Agent>) -> (TestClient, CapturedSession) {
        let config_path = self.root.path().join("config.toml");
        tokio::fs::write(&config_path, format!("[modelProviders.voice]\nname='Voice source'\nprovider='openai'\nmodel='fixture-task-model'\nbaseUrl='{}'\n[modelProviders.voice.realtime]\nmodel='fixture-voice-model'\ntranscriptionModel='fixture-asr-model'\nprotocol='{}'\n", self.base_url,self.protocol)).await.unwrap();
        let credentials: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        credentials
            .set(&model_credential_key("voice"), "fixture-realtime-secret")
            .await
            .unwrap();
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(self.root.path().join("sessions").to_string_lossy().into()),
                archive_dir: None,
            }));
        let captured = Arc::new(std::sync::Mutex::new(None));
        let captured_inner = captured.clone();
        let create: CreateSession = Arc::new(move |ctx| {
            let agent = agent.clone();
            let store = store.clone();
            let captured = captured_inner.clone();
            Box::pin(async move {
                let id = ctx.init.thread_id.clone().unwrap();
                let rollout = store
                    .create(klaus_agent_core::schemas::rollout::SessionMeta {
                        id: id.clone(),
                        created_at: 1_700_000_000_000,
                        cwd: ".".into(),
                        model: "fixture-task-model".into(),
                        title: None,
                        originator: None,
                        cli_version: None,
                        source: None,
                        role: None,
                        parent_thread_id: None,
                        forked_from_id: None,
                        git: None,
                        host_metadata: None,
                    })
                    .await?;
                let session = agent.start_session(StartSessionOptions {
                    id: Some(id),
                    rollout: Some(Arc::from(rollout)),
                    store: Some(store),
                    approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    config: Some(json!({"prewarm":false,"autoMemory":false})),
                    ..Default::default()
                });
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        });
        let mut client =
            TestClient::start_full_with_credentials(create, Some(config_path), Some(credentials));
        client.initialize().await;
        let reply = request(
            &mut client,
            2,
            "session/start",
            json!({"init":{"threadId":"voice-thread"}}),
        )
        .await;
        assert_eq!(reply["result"]["sessionId"], "voice-thread", "{reply}");
        (client, captured)
    }
}
impl Drop for RealtimePeer {
    fn drop(&mut self) {
        self.listener.abort();
    }
}
async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await.0
}
fn call_params() -> Value {
    json!({"sessionId":"voice-thread","requestId":"voice-generation","modelProviderId":"voice","offerSdp":"v=0\r\no=browser\r\n","flushTranscriptTailOnSessionEnd":true})
}
fn session_params() -> Value {
    json!({"sessionId":"voice-thread","requestId":"voice-generation"})
}
fn hosted_handoff(protocol: &str, id: &str, prompt: &str) -> Value {
    if protocol == "v1" {
        json!({"type":"conversation.handoff.requested","handoff_id":id,"item_id":format!("input-{id}"),"input_transcript":prompt})
    } else {
        json!({"type":"delegation.created","item":{"id":id,"type":"delegation","target":"client","content":[{"type":"input_text","text":prompt}]}})
    }
}
async fn task_notification(client: &mut TestClient, kind: &str, id: &str) -> Value {
    loop {
        let event = client.next_message().await;
        if event["method"] == "session/realtimeEvent"
            && event["params"]["event"]["type"] == kind
            && event["params"]["event"]["handoffId"] == id
        {
            return event["params"]["event"].clone();
        }
    }
}
async fn task_result(client: &mut TestClient, id: &str) -> Value {
    loop {
        let event = task_notification(client, "task.event", id).await;
        if event["event"].get("result").is_some() || event["event"].get("error").is_some() {
            return event;
        }
    }
}

#[tokio::test]
async fn hosted_protocols_execute_real_tasks_and_persist_host_transcripts_once() {
    for protocol in ["v1", "frameless"] {
        let mut peer = RealtimePeer::start_protocol(false, protocol).await;
        let (mut client, captured) = peer.client(&["The real hosted task completed."]).await;
        let started = request(&mut client, 3, "session/realtimeStart", call_params()).await;
        assert!(started.get("error").is_none(), "{started}");
        assert_eq!(started["result"]["protocol"], protocol);
        assert_eq!(started["result"]["transcriptSource"], "engine");
        let _ = peer.until(|event| event["fixture"] == "offer").await;
        peer.incoming
            .send(hosted_handoff(
                protocol,
                "hosted-one",
                "Perform the real hosted task.",
            ))
            .unwrap();
        let queued = task_notification(&mut client, "task.queued", "hosted-one").await;
        assert_eq!(queued["owned"], true);
        assert_eq!(queued["turnId"], "realtime:voice-generation:hosted-one");
        let result = task_result(&mut client, "hosted-one").await;
        assert_eq!(
            result["event"]["result"]["stopReason"], "completed",
            "{result}"
        );
        let output = peer
            .until(|event| {
                if protocol == "v1" {
                    event["output_text"]
                        .as_str()
                        .is_some_and(|text| text.contains("The real hosted task completed."))
                } else {
                    event
                        .pointer("/content/0/text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| text.contains("The real hosted task completed."))
                }
            })
            .await;
        assert_eq!(
            output[if protocol == "v1" {
                "handoff_id"
            } else {
                "delegation_item_id"
            }],
            "hosted-one"
        );
        if protocol == "v1" {
            peer.incoming.send(json!({"type":"conversation.output_transcript.delta","delta":"The spoken result is "})).unwrap();
            peer.incoming.send(json!({"type":"response.output_audio_transcript.done","transcript":"The spoken result is complete."})).unwrap();
        } else {
            peer.incoming.send(json!({"type":"output_transcript.added","item":{"id":"speech-one","text":"The spoken result is "}})).unwrap();
            for _ in 0..2 {
                peer.incoming.send(json!({"type":"turn.done","turn":{"id":"speech-turn","role":"assistant","transcript":"The spoken result is complete."}})).unwrap();
            }
        }
        let stopped = request(&mut client, 4, "session/realtimeStop", session_params()).await;
        assert!(stopped.get("error").is_none(), "{stopped}");
        let core = captured.lock().unwrap().clone().unwrap();
        let history = format!("{:?}", core.history());
        assert_eq!(
            history.matches("The spoken result is complete.").count(),
            1,
            "{history}"
        );
        assert!(
            history.contains("Perform the real hosted task."),
            "{history}"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn hosted_followup_borrows_only_selected_typed_turn_without_cancellation() {
    for protocol in ["v1", "frameless"] {
        let peer = RealtimePeer::start_protocol(false, protocol).await;
        let (release, released) = tokio::sync::watch::channel(false);
        let entered = Arc::new(Notify::new());
        let agent = make_agent_with_call_model(Box::new({
            let entered = entered.clone();
            move |_, _| {
                entered.notify_one();
                let mut released = released.clone();
                Box::pin(
                    futures::stream::iter(vec![
                        ev::message_start(None),
                        ev::text("The selected typed task finished."),
                    ])
                    .chain(futures::stream::once(async move {
                        while !*released.borrow() {
                            released.changed().await.unwrap();
                        }
                        ev::stop(Some(ModelStopReason::EndTurn), None)
                    })),
                )
            }
        }));
        let (mut client, _) = peer.client_with_agent(agent).await;
        let typed=request(&mut client,3,"turn/start",json!({"sessionId":"voice-thread","submissionId":"typed-owner","input":"Keep this selected typed task active."})).await;
        assert!(typed.get("error").is_none(), "{typed}");
        tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
            .await
            .unwrap();
        let other = request(
            &mut client,
            4,
            "session/start",
            json!({"init":{"threadId":"other-thread"}}),
        )
        .await;
        assert!(other.get("error").is_none(), "{other}");
        let started = request(&mut client, 5, "session/realtimeStart", call_params()).await;
        assert!(started.get("error").is_none(), "{started}");
        peer.incoming
            .send(hosted_handoff(
                protocol,
                "borrowed-one",
                "Add this voice followup to the selected typed task.",
            ))
            .unwrap();
        let queued = task_notification(&mut client, "task.queued", "borrowed-one").await;
        assert_eq!(queued["turnId"], "typed-owner");
        assert_eq!(queued["owned"], false);
        let interrupt = request(
            &mut client,
            6,
            "session/realtimeInterrupt",
            session_params(),
        )
        .await;
        assert!(interrupt.get("error").is_none(), "{interrupt}");
        let stopped = request(&mut client, 7, "session/realtimeStop", session_params()).await;
        assert!(stopped.get("error").is_none(), "{stopped}");
        release.send(true).unwrap();
        loop {
            let event = client.next_message().await;
            if event["method"] == "turn/completed" && event["params"]["turnId"] == "typed-owner" {
                assert_eq!(
                    event["params"]["result"]["stopReason"], "completed",
                    "{event}"
                );
                break;
            }
        }
        let selected = request(
            &mut client,
            8,
            "session/history",
            json!({"sessionId":"voice-thread"}),
        )
        .await;
        let other = request(
            &mut client,
            9,
            "session/history",
            json!({"sessionId":"other-thread"}),
        )
        .await;
        assert!(
            selected.to_string().contains("Add this voice followup"),
            "{selected}"
        );
        assert!(
            !other.to_string().contains("Add this voice followup"),
            "{other}"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn hosted_followup_supersedes_handoff_identity_only_after_confirmed_steering() {
    let mut peer = RealtimePeer::start_protocol(false, "v1").await;
    let (release, released) = tokio::sync::watch::channel(false);
    let entered = Arc::new(Notify::new());
    let agent = make_agent_with_call_model(Box::new({
        let entered = entered.clone();
        move |_, _| {
            entered.notify_one();
            let mut released = released.clone();
            Box::pin(
                futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("The updated owned task finished."),
                ])
                .chain(futures::stream::once(async move {
                    while !*released.borrow() {
                        released.changed().await.unwrap();
                    }
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                })),
            )
        }
    }));
    let (mut client, _) = peer.client_with_agent(agent).await;
    let started = request(&mut client, 3, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    peer.incoming
        .send(hosted_handoff(
            "v1",
            "first-handoff",
            "Start the owned task.",
        ))
        .unwrap();
    let first = task_notification(&mut client, "task.queued", "first-handoff").await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    peer.incoming
        .send(hosted_handoff(
            "v1",
            "replacement",
            "Continue the same owned task with this correction.",
        ))
        .unwrap();
    let superseded = task_notification(&mut client, "task.superseded", "first-handoff").await;
    assert_eq!(superseded["turnId"], first["turnId"]);
    assert_eq!(superseded["replacementHandoffId"], "replacement");
    let replacement = task_notification(&mut client, "task.queued", "replacement").await;
    assert_eq!(replacement["turnId"], first["turnId"]);
    assert_eq!(replacement["owned"], true);
    release.send(true).unwrap();
    let result = task_result(&mut client, "replacement").await;
    assert_eq!(
        result["event"]["result"]["stopReason"], "completed",
        "{result}"
    );
    let output = peer
        .until(|event| {
            event["output_text"]
                .as_str()
                .is_some_and(|text| text.contains("The updated owned task finished."))
        })
        .await;
    assert_eq!(output["handoff_id"], "replacement");
    let stopped = request(&mut client, 4, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    client.close().await;
}

#[tokio::test]
async fn realtime_tail_flush_false_preserves_completed_text_and_discards_unfinished_tail() {
    let mut peer = RealtimePeer::start(false).await;
    let (mut client, captured) = peer.client(&[]).await;
    let mut params = call_params();
    params["flushTranscriptTailOnSessionEnd"] = json!(false);
    let started = request(&mut client, 3, "session/realtimeStart", params).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    peer.incoming.send(json!({"type":"response.output_audio_transcript.done","item_id":"complete","transcript":"Keep this completed utterance."})).unwrap();
    peer.incoming.send(json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"unfinished","delta":"Discard this unfinished tail."})).unwrap();
    loop {
        let event = client.next_message().await;
        if event.pointer("/params/event/item_id") == Some(&json!("unfinished")) {
            break;
        }
    }
    let stopped = request(&mut client, 4, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    let history = format!("{:?}", captured.lock().unwrap().clone().unwrap().history());
    assert!(
        history.contains("Keep this completed utterance."),
        "{history}"
    );
    assert!(
        !history.contains("Discard this unfinished tail."),
        "{history}"
    );
    client.close().await;
}

#[tokio::test]
async fn realtime_facade_runs_real_task_and_flushes_unique_transcripts_before_stop() {
    let mut peer = RealtimePeer::start(false).await;
    let (mut client, captured) = peer
        .client(&["The actual task agent completed the request."])
        .await;
    let caps = request(
        &mut client,
        3,
        "session/realtimeCapabilities",
        json!({"modelProviderId":"voice"}),
    )
    .await;
    assert_eq!(caps["result"]["available"], true, "{caps}");
    let started = request(&mut client, 4, "session/realtimeStart", call_params()).await;
    assert_eq!(started["result"]["callId"], "rtc_fixture", "{started}");
    let offer = peer.until(|event| event["fixture"] == "offer").await;
    assert!(offer["body"].as_str().unwrap().contains("delegate_task"));
    assert!(offer["body"]
        .as_str()
        .unwrap()
        .contains("fixture-asr-model"));
    let update = peer.until(|event| event["type"] == "session.update").await;
    assert_eq!(update["session"]["tools"][0]["name"], "delegate_task");
    for _ in 0..2 {
        peer.incoming.send(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"utterance-1","transcript":"Please carry out my request."})).unwrap();
    }
    peer.incoming
        .send(json!({"type":"input_audio_buffer.committed","item_id":"utterance-1"}))
        .unwrap();
    let requested = peer.until(|event| event["type"] == "response.create").await;
    let response_metadata = requested["response"]["metadata"].clone();
    let task = json!({"type":"response.function_call_arguments.done","call_id":"task-call","name":"delegate_task","arguments":"{\"prompt\":\"Carry out the spoken request.\"}"});
    peer.incoming.send(task.clone()).unwrap();
    peer.incoming.send(task).unwrap();
    // Argument completion alone can describe cancelled output and must do no work.
    peer.incoming.send(json!({"type":"response.done","response":{"status":"cancelled","output":[{"type":"function_call","call_id":"cancelled-call","name":"delegate_task","arguments":"{\"prompt\":\"Never execute cancelled speech\"}"}]}})).unwrap();
    let committed = json!({"type":"response.done","response":{"status":"completed","metadata":response_metadata,"output":[{"type":"function_call","call_id":"task-call","name":"delegate_task","arguments":"{\"prompt\":\"Carry out the spoken request.\"}"}]}});
    peer.incoming.send(committed.clone()).unwrap();
    peer.incoming.send(committed).unwrap();
    let output = peer
        .until(|event| event.pointer("/item/type") == Some(&json!("function_call_output")))
        .await;
    let result: Value = serde_json::from_str(output["item"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(
        result["text"],
        "The actual task agent completed the request."
    );
    assert_eq!(result["status"], "completed");
    let _ = peer.until(|event| event["type"] == "response.create").await;
    for _ in 0..2 {
        peer.incoming.send(json!({"type":"response.output_audio_transcript.done","item_id":"spoken-1","transcript":"The task is finished."})).unwrap();
    }
    peer.incoming.send(json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"tail-1","delta":"Keep this unfinished transcript tail"})).unwrap();
    let speech = request(
        &mut client,
        5,
        "session/realtimeAppendSpeech",
        json!({"sessionId":"voice-thread","requestId":"voice-generation","text":"A spoken update"}),
    )
    .await;
    assert!(speech.get("error").is_none(), "{speech}");
    let speech_event = peer
        .until(|event| event.pointer("/response/conversation") == Some(&json!("none")))
        .await;
    assert_eq!(
        speech_event["response"]["input"][0]["content"][0]["text"],
        "A spoken update"
    );
    let stale = request(
        &mut client,
        6,
        "session/realtimeStop",
        json!({"sessionId":"voice-thread","requestId":"stale-generation"}),
    )
    .await;
    assert!(stale.get("error").is_some());
    client
        .send(json!({"id":7,"method":"session/realtimeStop","params":session_params()}))
        .await;
    let (stopped, notifications) = client.response_for(7).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    assert!(
        notifications
            .iter()
            .any(|event| event["method"] == "session/realtimeEvent"
                && event.pointer("/params/event/type") == Some(&json!("closed"))
                && event.pointer("/params/event/transcriptFlushed") == Some(&json!(true))),
        "{notifications:?}"
    );
    let core = captured.lock().unwrap().clone().unwrap();
    let history = format!("{:?}", core.history());
    assert_eq!(
        history.matches("Please carry out my request.").count(),
        1,
        "{history}"
    );
    assert_eq!(
        history.matches("The task is finished.").count(),
        1,
        "{history}"
    );
    assert!(
        history.contains("Keep this unfinished transcript tail"),
        "{history}"
    );
    assert!(
        !history.contains("Never execute cancelled speech"),
        "{history}"
    );
    assert_eq!(
        history.matches("Carry out the spoken request.").count(),
        1,
        "{history}"
    );
    let mut durable = String::new();
    for entry in std::fs::read_dir(peer.root.path().join("sessions")).unwrap() {
        durable.push_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap_or_default());
    }
    assert!(
        durable.contains("Keep this unfinished transcript tail"),
        "{durable}"
    );
    assert!(durable.contains("The actual task agent completed the request."));
    assert!(!durable.contains("fixture-realtime-secret"));
    client.close().await;
}

#[tokio::test]
async fn realtime_facade_refuses_missing_capability_and_preserves_failed_finalization_receipt() {
    let mut peer = RealtimePeer::start(true).await;
    let (mut client, _) = peer.client(&[]).await;
    let caps = request(&mut client, 3, "session/realtimeCapabilities", json!({})).await;
    assert_eq!(caps["result"]["available"], false);
    assert!(caps["result"]["reason"]
        .as_str()
        .unwrap()
        .contains("configured API model source"));
    let started = request(&mut client, 4, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    for id in [5, 6] {
        let stopped = request(&mut client, id, "session/realtimeStop", session_params()).await;
        assert!(
            stopped["error"]["message"]
                .as_str()
                .unwrap()
                .contains("refused call finalization"),
            "{stopped}"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn realtime_interrupt_cancels_queued_voice_task_without_touching_unrelated_turn() {
    let mut peer = RealtimePeer::start(false).await;
    let release = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let agent = make_agent_with_call_model(Box::new({
        let release = release.clone();
        let entered = entered.clone();
        let count = count.clone();
        move |_, _| {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            entered.notify_one();
            let release = release.clone();
            Box::pin(
                futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("The typed task continues."),
                ])
                .chain(futures::stream::once(async move {
                    release.notified().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                })),
            )
        }
    }));
    let (mut client, _) = peer.client_with_agent(agent).await;
    let typed=request(&mut client,3,"turn/start",json!({"sessionId":"voice-thread","submissionId":"typed-task","input":"An unrelated typed task"})).await;
    assert!(typed.get("error").is_none(), "{typed}");
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    let started = request(&mut client, 4, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    peer.incoming
        .send(json!({"type":"input_audio_buffer.committed","item_id":"queued-user"}))
        .unwrap();
    let requested = peer.until(|event| event["type"] == "response.create").await;
    let response_metadata = requested["response"]["metadata"].clone();
    peer.incoming.send(json!({"type":"response.function_call_arguments.done","call_id":"queued-call","name":"delegate_task","arguments":"{\"prompt\":\"A queued voice task\"}"})).unwrap();
    peer.incoming.send(json!({"type":"response.done","response":{"status":"completed","metadata":response_metadata,"output":[{"type":"function_call","call_id":"queued-call","name":"delegate_task","arguments":"{\"prompt\":\"A queued voice task\"}"}]}})).unwrap();
    loop {
        let event = client.next_message().await;
        if event["method"] == "session/realtimeEvent"
            && event.pointer("/params/event/type") == Some(&json!("response.done"))
        {
            break;
        }
    }
    let interrupted = request(
        &mut client,
        5,
        "session/realtimeInterrupt",
        session_params(),
    )
    .await;
    assert!(interrupted.get("error").is_none(), "{interrupted}");
    release.notify_one();
    let output = peer
        .until(|event| event.pointer("/item/type") == Some(&json!("function_call_output")))
        .await;
    let result: Value = serde_json::from_str(output["item"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(result["status"], "failed", "{result}");
    assert_eq!(
        count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the queued voice task must never reach a provider"
    );
    let stopped = request(&mut client, 6, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    let read = request(
        &mut client,
        7,
        "session/history",
        json!({"sessionId":"voice-thread"}),
    )
    .await;
    assert!(
        read.to_string().contains("The typed task continues."),
        "{read}"
    );
    client.close().await;
}

#[tokio::test]
async fn realtime_interrupt_rejects_buffered_completed_response_from_previous_generation() {
    let mut peer = RealtimePeer::start(false).await;
    let (mut client, captured) = peer.client(&["This task must never execute"]).await;
    let started = request(&mut client, 3, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    peer.incoming
        .send(json!({"type":"input_audio_buffer.committed","item_id":"old-user"}))
        .unwrap();
    let requested = peer.until(|event| event["type"] == "response.create").await;
    let interrupted = request(
        &mut client,
        4,
        "session/realtimeInterrupt",
        session_params(),
    )
    .await;
    assert!(interrupted.get("error").is_none(), "{interrupted}");
    peer.incoming.send(json!({"type":"response.done","response":{"status":"completed","metadata":requested["response"]["metadata"],"output":[{"type":"function_call","call_id":"late-call","name":"delegate_task","arguments":"{\"prompt\":\"Do not run stale speech\"}"}]}})).unwrap();
    loop {
        let event = client.next_message().await;
        if event["method"] == "session/realtimeEvent"
            && event.pointer("/params/event/type") == Some(&json!("response.done"))
        {
            break;
        }
    }
    let core = captured.lock().unwrap().clone().unwrap();
    assert!(!format!("{:?}", core.history()).contains("Do not run stale speech"));
    let stopped = request(&mut client, 5, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    client.close().await;
}

async fn completed_turn(client: &mut TestClient, id: u64, session_id: &str, input: &str) {
    let started = request(
        client,
        id,
        "turn/start",
        json!({"sessionId":session_id,"input":input}),
    )
    .await;
    let turn_id = started["result"]["turnId"].as_str().unwrap();
    loop {
        let event = client.next_message().await;
        if event["method"] == "turn/completed"
            && event.pointer("/params/turnId") == Some(&json!(turn_id))
        {
            break;
        }
    }
}
#[tokio::test]
async fn realtime_startup_exports_only_bounded_selected_history_before_any_speech() {
    for protocol in ["public", "v1", "frameless"] {
        let mut peer = RealtimePeer::start_protocol(false, protocol).await;
        let selected = format!("{} Selected startup context marker", "🙂".repeat(20_000));
        let (mut client, _) = peer
            .client(&["Private other-chat startup sentinel", &selected])
            .await;
        let other = request(
            &mut client,
            3,
            "session/start",
            json!({"init":{"threadId":"other-thread"}}),
        )
        .await;
        assert!(other.get("error").is_none(), "{other}");
        completed_turn(&mut client, 4, "other-thread", "Other-chat input sentinel").await;
        completed_turn(
            &mut client,
            5,
            "voice-thread",
            "Selected visible startup request",
        )
        .await;
        let started = request(&mut client, 6, "session/realtimeStart", call_params()).await;
        assert!(started.get("error").is_none(), "{started}");
        let offer = peer.until(|event| event["fixture"] == "offer").await;
        let body = offer["body"].as_str().unwrap();
        let session: Value = if protocol == "public" {
            serde_json::from_str(
                body.split("Content-Type: application/json\r\n\r\n")
                    .nth(1)
                    .unwrap()
                    .split("\r\n--")
                    .next()
                    .unwrap(),
            )
            .unwrap()
        } else {
            serde_json::from_str::<Value>(body).unwrap()["session"].clone()
        };
        let instructions = session["instructions"].as_str().unwrap();
        let prefix = "Current conversation context (reference data):\n";
        let export = &instructions[instructions
            .find(prefix)
            .expect("Startup includes selected history")..];
        assert!(export.len() <= 16 * 1024, "context bytes: {}", export.len());
        assert!(export.contains("Selected startup context marker"));
        assert!(export.contains('🙂'));
        assert!(!export.contains("Private other-chat startup sentinel"));
        assert!(!export.contains("Other-chat input sentinel"));
        assert!(!export.contains("fixture-realtime-secret"));
        if protocol == "public" {
            let update = peer.until(|event| event["type"] == "session.update").await;
            assert_eq!(update["session"]["instructions"], session["instructions"]);
        }
        let stopped = request(&mut client, 7, "session/realtimeStop", session_params()).await;
        assert!(stopped.get("error").is_none(), "{stopped}");
        client.close().await;
    }
}

#[tokio::test]
async fn realtime_context_is_byte_bounded_and_scoped_to_selected_conversation() {
    let mut peer = RealtimePeer::start(false).await;
    let long_text = "🙂".repeat(20_000);
    let (mut client, _) = peer
        .client(&["Private other conversation text", long_text.as_str()])
        .await;
    let other = request(
        &mut client,
        3,
        "session/start",
        json!({"init":{"threadId":"other-thread"}}),
    )
    .await;
    assert!(other.get("error").is_none(), "{other}");
    completed_turn(
        &mut client,
        4,
        "other-thread",
        "Unrelated private conversation input",
    )
    .await;
    completed_turn(
        &mut client,
        5,
        "voice-thread",
        "Visible selected conversation input",
    )
    .await;
    let started = request(&mut client, 6, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    peer.incoming
        .send(json!({"type":"input_audio_buffer.committed","item_id":"context-user"}))
        .unwrap();
    let context = peer
        .until(|event| event.pointer("/item/role") == Some(&json!("system")))
        .await;
    let text = context["item"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("Current conversation context (reference data):"),
        "{text}"
    );
    assert!(text.len() <= 16 * 1024, "context bytes: {}", text.len());
    assert!(text.contains('🙂'));
    assert!(!text.contains("Private other conversation text"));
    assert!(!text.contains("Unrelated private conversation input"));
    assert!(!text.contains("fixture-realtime-secret"));
    let stopped = request(&mut client, 7, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    client.close().await;
}
#[tokio::test]
async fn realtime_capability_requires_an_explicit_transcription_model() {
    let peer = RealtimePeer::start(false).await;
    let (mut client, _) = peer.client(&[]).await;
    let path = peer.root.path().join("config.toml");
    let source = tokio::fs::read_to_string(&path).await.unwrap();
    tokio::fs::write(
        &path,
        source.replace("transcriptionModel='fixture-asr-model'\n", ""),
    )
    .await
    .unwrap();
    let caps = request(
        &mut client,
        3,
        "session/realtimeCapabilities",
        json!({"modelProviderId":"voice"}),
    )
    .await;
    assert_eq!(caps["result"]["available"], false, "{caps}");
    assert!(caps["result"]["reason"]
        .as_str()
        .unwrap()
        .contains("explicit transcription model"));
    client.close().await;
}

#[tokio::test]
async fn realtime_end_preserves_running_task_and_retires_queued_voice_work() {
    let mut peer = RealtimePeer::start(false).await;
    let release = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let agent = make_agent_with_call_model(Box::new({
        let release = release.clone();
        let entered = entered.clone();
        let count = count.clone();
        move |_, _| {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            entered.notify_one();
            let release = release.clone();
            Box::pin(
                futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("The admitted task finishes after voice ends."),
                ])
                .chain(futures::stream::once(async move {
                    release.notified().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                })),
            )
        }
    }));
    let (mut client, _) = peer.client_with_agent(agent).await;
    let started = request(&mut client, 3, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let _ = peer.until(|event| event["type"] == "session.update").await;
    for (id, prompt) in [
        ("active-call", "An admitted task"),
        ("queued-call", "A queued task"),
    ] {
        peer.incoming
            .send(json!({"type":"input_audio_buffer.committed","item_id":format!("input-{id}")}))
            .unwrap();
        let response = peer.until(|event| event["type"] == "response.create").await;
        peer.incoming.send(json!({"type":"response.done","response":{"status":"completed","metadata":response["response"]["metadata"],"output":[{"type":"function_call","call_id":id,"name":"delegate_task","arguments":json!({"prompt":prompt}).to_string()}]}})).unwrap();
        loop {
            let event = client.next_message().await;
            if event["method"] == "session/realtimeEvent"
                && event.pointer("/params/event/type") == Some(&json!("response.done"))
                && event.pointer("/params/event/response/output/0/call_id") == Some(&json!(id))
            {
                break;
            }
        }
        if id == "active-call" {
            tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
                .await
                .unwrap();
        }
    }
    let stopped = request(&mut client, 4, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    let stale = request(
        &mut client,
        5,
        "session/realtimeInterrupt",
        session_params(),
    )
    .await;
    assert!(stale.get("error").is_some(), "{stale}");
    release.notify_one();
    loop {
        let event = client.next_message().await;
        if event["method"] == "turn/completed"
            && event.pointer("/params/turnId")
                == Some(&json!("realtime:voice-generation:active-call"))
        {
            assert_eq!(
                event.pointer("/params/result/stopReason"),
                Some(&json!("completed")),
                "{event}"
            );
            break;
        }
    }
    let history = request(
        &mut client,
        6,
        "session/history",
        json!({"sessionId":"voice-thread"}),
    )
    .await;
    assert!(history
        .to_string()
        .contains("The admitted task finishes after voice ends."));
    assert_eq!(
        count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "queued voice work must not reach the model after End"
    );
    client.close().await;
}

// Regression: subscription discovery advertised a retired handshake and model.
// The hosted source must select the supported session and delegation protocol.
#[tokio::test]
async fn realtime_subscription_capability_selects_the_live_session_protocol() {
    let peer = RealtimePeer::start_protocol(false, "frameless").await;
    let (mut client, _) = peer.client(&[]).await;
    tokio::fs::write(
        peer.root.path().join("openai-subscription-oauth.json"),
        json!({
            "defaultAccountId": "voice-account",
            "accounts": {"voice-account": {
                "accountId": "voice-account",
                "accessToken": "fixture-realtime-secret",
                "accessTokenExpiresAt": i64::MAX
            }}
        })
        .to_string(),
    )
    .await
    .unwrap();
    let caps = request(
        &mut client,
        3,
        "session/realtimeCapabilities",
        json!({"authMode":"codex"}),
    )
    .await;
    assert_eq!(caps["result"]["available"], true, "{caps}");
    assert_eq!(caps["result"]["protocol"], "frameless", "{caps}");
    assert_eq!(caps["result"]["model"], "gpt-live-1-codex", "{caps}");
    client.close().await;
}

async fn realtime_notification(
    client: &mut TestClient,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    loop {
        let event = client.next_message().await;
        if event["method"] == "session/realtimeEvent" {
            let value = event["params"]["event"].clone();
            if predicate(&value) {
                return value;
            }
        }
    }
}

#[tokio::test]
async fn qoder_voice_line_carries_engine_mediated_audio_without_webrtc() {
    let mut peer = RealtimePeer::start_protocol(false, "qoder").await;
    let (mut client, _captured) = peer.client(&[]).await;
    let capabilities = request(
        &mut client,
        7,
        "session/realtimeCapabilities",
        json!({"modelProviderId":"voice"}),
    )
    .await;
    assert!(
        capabilities["result"]["available"]
            .as_bool()
            .unwrap_or(false),
        "{capabilities}"
    );
    assert_eq!(
        capabilities["result"]["protocol"], "qoder",
        "{capabilities}"
    );
    assert_eq!(
        capabilities["result"]["defaultVoice"], "longanqian",
        "{capabilities}"
    );
    assert_eq!(
        capabilities["result"]["voices"],
        json!(["longanqian", "loongeva_v3.6", "longanlufeng"]),
        "{capabilities}"
    );
    let started = request(
        &mut client,
        3,
        "session/realtimeStart",
        json!({"sessionId":"voice-thread","requestId":"voice-generation","modelProviderId":"voice","flushTranscriptTailOnSessionEnd":true}),
    )
    .await;
    assert_eq!(started["result"]["protocol"], "qoder", "{started}");
    assert_eq!(started["result"]["transcriptSource"], "engine", "{started}");
    assert!(
        started["result"]
            .get("answerSdp")
            .is_none_or(|value| value.is_null()),
        "{started}"
    );
    let update = peer.until(|event| event["type"] == "session.update").await;
    assert_eq!(update["session"]["voice"], "longanqian", "{update}");
    assert_eq!(update["session"]["input_audio_format"], "pcm", "{update}");
    assert_eq!(update["session"]["output_audio_format"], "pcm", "{update}");
    assert_eq!(
        update["session"]["turn_detection"]["type"], "smart_turn",
        "{update}"
    );
    assert_eq!(update["session"]["max_history_turns"], 50, "{update}");
    assert_eq!(
        update["session"]["tools"][0]["function"]["name"], "delegate_task",
        "{update}"
    );
    assert!(update["session"].get("model").is_none(), "{update}");
    realtime_notification(&mut client, |event| event["type"] == "started").await;
    peer.incoming
        .send(json!({"type":"response.audio.delta","delta":"cG9uZQ=="}))
        .unwrap();
    let audio =
        realtime_notification(&mut client, |event| event["type"] == "response.audio.delta").await;
    assert_eq!(audio["delta"], "cG9uZQ==", "{audio}");
    let appended = request(
        &mut client,
        4,
        "session/realtimeAppendAudio",
        json!({"sessionId":"voice-thread","requestId":"voice-generation","audioBase64":"UklG"}),
    )
    .await;
    assert!(appended.get("error").is_none(), "{appended}");
    let frame = peer
        .until(|event| event["type"] == "input_audio_buffer.append")
        .await;
    assert_eq!(frame["audio"], "UklG", "{frame}");
    let too_large = request(
        &mut client,
        5,
        "session/realtimeAppendAudio",
        json!({"sessionId":"voice-thread","requestId":"voice-generation","audioBase64":"A".repeat(129 * 1024)}),
    )
    .await;
    assert!(too_large["error"].is_object(), "{too_large}");
    let stopped = request(&mut client, 6, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
}

// Regression: a confirmed session closure was misreported as a transport failure
// when the provider ended the underlying socket without a WebSocket close frame.
#[tokio::test]
async fn realtime_stop_accepts_confirmed_session_closure_without_a_close_frame() {
    let peer = RealtimePeer::start_protocol(false, "frameless").await;
    let (mut client, _) = peer.client(&[]).await;
    let started = request(&mut client, 3, "session/realtimeStart", call_params()).await;
    assert!(started.get("error").is_none(), "{started}");
    let stopped = request(&mut client, 4, "session/realtimeStop", session_params()).await;
    assert!(stopped.get("error").is_none(), "{stopped}");
    client.close().await;
}
