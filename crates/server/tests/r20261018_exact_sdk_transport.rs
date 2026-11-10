//! Typed remote requests and streamed payloads must retain exact runtime text.
//! Root cause: scalar JSON intermediates rejected isolated UTF-16 code units.
//! Work item: 20261018 exact named-facade SDK transport.

use super::*;
use std::path::Path;
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::Session;
use klaus_agent_core::planning::runtime::PlanningRuntime;
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_values::RuntimeJson;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::testing::{ev, fake_provider};
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    CallModel, ModelRequest, ModelStopReason, ModelStreamEvent,
};
use serde_json::json;

use crate::app_server::methods;
use crate::connect::{connect_facade, ConnectOptions};
use crate::lifecycle::{BoxError, CreateSession, SessionContext};
use crate::ws_stream::{ws_to_streams, WsStreams};

const DEADLINE: Duration = Duration::from_secs(20);

fn exact(units: &[u16]) -> RuntimeString {
    RuntimeString::from_utf16(units.to_vec())
}

fn agent(root: &Path, call_model: CallModel) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context: crate::test_process_context(),
        provider: Arc::new(fake_provider()),
        config: Some(
            json!({"model":"test-model","autoMemory":false,"agentMemory":false,"autoDream":false,"loopDetection":false,"prewarm":false}),
        ),
        tools: Some(Vec::new()),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(root.to_string_lossy().into_owned()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: None,
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: Some(false),
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

fn factory(root: &Path, agent: Arc<Agent>, store: Arc<dyn SessionStore>) -> CreateSession {
    let root = root.to_path_buf();
    Arc::new(move |context: SessionContext| {
        let agent = agent.clone();
        let store = store.clone();
        let root = root.clone();
        Box::pin(async move {
            let session = agent
                .open_session(
                    store,
                    StartSessionOptions {
                        id: context.init.thread_id,
                        cwd: Some(root.to_string_lossy().into_owned().into()),
                        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                        durable_permissions: Some(context.durable_permissions),
                        approval_handler: Some(context.approval_handler),
                        elicit_handler: Some(context.elicit_handler),
                        mcp_elicitation_presenter: Some(context.mcp_elicitation_presenter),
                        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                        ..Default::default()
                    },
                )
                .await?;
            Ok::<Arc<Session>, BoxError>(session)
        })
    })
}

fn contains_text(value: &RuntimeJson, expected: &RuntimeString) -> bool {
    match value {
        RuntimeJson::String(text) => text == expected,
        RuntimeJson::Array(values) => values.iter().any(|value| contains_text(value, expected)),
        RuntimeJson::Object(fields) => fields
            .entries()
            .any(|(_, value)| contains_text(value, expected)),
        _ => false,
    }
}

async fn private_remote_endpoint(options: AppServerOptions) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        let WsStreams {
            input,
            output,
            transport,
        } = ws_to_streams(socket);
        run_app_server(input, output, options).await;
        transport.close_and_wait().await;
    });
    (url, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_typed_planning_and_split_stream_retain_exact_text() {
    tokio::time::timeout(DEADLINE, async {
        let dir = tempfile::tempdir().unwrap();
        let thread = "sdk-exact-thread";
        let original = exact(&[0x41, 0x42, 0xd83e, 0xdd80]);
        let first_review = original.concat(&exact(&[0x43, 0xd800]));
        let reviewed = first_review.concat(&exact(&[0x44, 0xdc00]));
        let planning =
            PlanningRuntime::new(thread.into(), Some(dir.path().to_path_buf()), false, false);
        planning.set_mode(CollaborationMode::Plan, None).unwrap();
        planning
            .start_plan("sdk-exact-plan".into(), "source-turn".into())
            .unwrap();
        planning
            .append_plan(&"sdk-exact-plan".into(), &original.try_to_utf8().unwrap())
            .unwrap();
        let plan = planning
            .finish_plan(&"sdk-exact-plan".into(), true)
            .unwrap();
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let recorded = requests.clone();
        let high = exact(&[0xd83e]);
        let low = exact(&[0xdd80]);
        let lone = exact(&[0xd800]);
        let deltas = vec![high.clone(), low.clone(), lone.clone()];
        let agent = agent(
            dir.path(),
            Box::new(move |request, signal| {
                recorded.lock().unwrap().push(request);
                let events = futures::stream::iter(vec![
                    ev::message_start(None),
                    ModelStreamEvent::TextDelta { text: high.clone() },
                    ModelStreamEvent::TextDelta { text: low.clone() },
                    ModelStreamEvent::TextDelta { text: lone.clone() },
                    ev::stop(Some(ModelStopReason::EndTurn), None),
                ]);
                Box::pin(events.take_until(async move { signal.cancelled().await }))
            }),
        );
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let (server_url, server) = private_remote_endpoint(AppServerOptions {
            windows_sandbox_context: None,
            create_session: factory(dir.path(), agent.clone(), store.clone()),
            credential_store: None,
            lifecycle: Some(crate::protocol_lifecycle(agent, Some(store))),
            plugin_admin: None,
            permission_rules_path: Some(dir.path().join("permissions.json")),
            config_path: Some(dir.path().join("config.json")),
            startup_init_ack_budget: None,
            session_unload_delay: None,
        })
        .await;
        let connection = connect_facade(&server_url, ConnectOptions::default())
            .await
            .unwrap();
        // This id cannot survive a binary64 envelope conversion unchanged.
        connection.client.next_id.store(u64::MAX, Ordering::Relaxed);
        connection
            .client
            .request::<methods::Initialize>(wire::InitializeParams {
                capabilities: Some(wire::ClientCapabilities {
                    product_events: true,
                    revisioned_session_events: true,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut notifications = connection.client.notifications();
        let started = connection
            .client
            .request::<methods::SessionStart>(wire::SessionStartParams {
                init: RawJson::from_serialize(&json!({"threadId":thread,"cwd":dir.path()}))
                    .unwrap(),
                resume: None,
                fork: None,
            })
            .await
            .unwrap();
        let saved = connection
            .client
            .request::<methods::SessionSavePlanDocument>(wire::SessionSavePlanDocumentParams {
                session_id: started.session_id.clone(),
                plan_id: "sdk-exact-plan".into(),
                revision: plan.revision,
                content: first_review.clone(),
                expected_content: Some(original),
            })
            .await
            .unwrap();
        let first_document = saved.document.unwrap();
        assert_eq!(first_document.content.as_utf16(), first_review.as_utf16());
        let saved = connection
            .client
            .request::<methods::SessionSavePlanDocument>(wire::SessionSavePlanDocumentParams {
                session_id: started.session_id.clone(),
                plan_id: "sdk-exact-plan".into(),
                revision: first_document.revision,
                content: reviewed.clone(),
                expected_content: Some(first_review),
            })
            .await
            .unwrap();
        let document = saved.document.unwrap();
        assert_eq!(document.content.as_utf16(), reviewed.as_utf16());
        let accepted = connection
            .client
            .request::<methods::SessionAcceptPlan>(wire::SessionAcceptPlanParams {
                session_id: started.session_id.clone(),
                plan_id: "sdk-exact-plan".into(),
                revision: document.revision,
                client_request_id: "sdk-exact-accept".into(),
                content: Some(reviewed.clone()),
            })
            .await
            .unwrap();
        let implementation = accepted.implementation_input.unwrap();
        assert!(implementation
            .as_utf16()
            .windows(reviewed.len_utf16())
            .any(|units| units == reviewed.as_utf16()));
        let turn = connection
            .client
            .request::<methods::TurnStart>(wire::TurnStartParams {
                session_id: started.session_id.clone(),
                input: implementation.clone().into(),
                options: None,
                submission_id: Some("sdk-exact-implementation".into()),
            })
            .await
            .unwrap();
        let mut raw_deltas = Vec::new();
        let mut product_deltas = Vec::new();
        let mut raw_revisions = Vec::new();
        let mut product_revisions = Vec::new();
        while let Some(notification) = notifications.next().await {
            if notification.method == "session/event" {
                let event = notification.event().unwrap().unwrap();
                let event: klaus_agent_protocol::ProtocolEvent =
                    klaus_agent_core::runtime_values::runtime_codec::decode_value(&event.0)
                        .expect("session/event must carry a valid protocol envelope");
                if let Some(klaus_agent_core::types::events::EngineEvent::TextDelta {
                    text, ..
                }) = event.event
                {
                    raw_deltas.push(text);
                    raw_revisions.push(
                        notification
                            .params
                            .control_u64("streamRevision")
                            .unwrap()
                            .unwrap(),
                    );
                }
            }
            if notification.method == "item/agentMessage/delta" {
                let delta: wire::DeltaNotification = notification.params.deserialize().unwrap();
                product_deltas.push(delta.delta);
                product_revisions.push(delta.stream_revision.unwrap());
            }
            if notification.method == "turn/completed"
                && notification
                    .params
                    .control_string("turnId")
                    .unwrap()
                    .as_deref()
                    == Some(turn.turn_id.as_str())
            {
                break;
            }
        }
        assert_eq!(raw_deltas, deltas);
        assert_eq!(product_deltas, deltas);
        assert_eq!(raw_revisions, product_revisions);
        assert!(raw_revisions.windows(2).all(|pair| pair[0] < pair[1]));
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].messages.iter().any(|message| matches!(message,
                Message::User(user) if user.content.iter().any(|block| matches!(block,
                    ContentBlock::Text(text) if text.text == implementation
                ))
            )));
        }
        let history = connection
            .client
            .request::<methods::SessionHistory>(wire::SessionRequestParams {
                session_id: Some(started.session_id.clone()),
                submission_id: None,
            })
            .await
            .unwrap();
        let history = history
            .get("history")
            .unwrap()
            .expect("the history reply carries its native rows")
            .to_runtime()
            .unwrap();
        assert!(contains_text(&history, &implementation));
        assert!(contains_text(&history, &exact(&[0xd83e, 0xdd80, 0xd800])));
        connection
            .client
            .request::<methods::SessionClose>(wire::SessionCloseParams {
                session_id: started.session_id,
            })
            .await
            .unwrap();
        connection.close_and_wait().await;
        server.await.unwrap();
    })
    .await
    .expect("remote exact-runtime regression must finish");
}

#[tokio::test]
async fn opaque_history_results_keep_wide_integer_controls_and_exact_text() {
    const RESULT: &str = r#"{"history":[{"text":"\ud800|\udc00|\ufffd"}],"page":{"order":"newest_first","limit":18446744073709551615,"nextCursor":null,"hasMore":false},"threadId":"thread","agentId":"agent"}"#;
    tokio::time::timeout(DEADLINE, async {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let client = AppServerClient::new(client_read, client_write);
        let call = client.request::<methods::EngineReadSubagentHistory>(
            wire::EngineReadSubagentHistoryParams {
                thread_id: "thread".into(),
                agent_id: "agent".into(),
            },
        );
        let peer = async {
            let mut lines = BufReader::new(server_read).lines();
            let request = RawJson::from_json(lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(
                request.control_string("method").unwrap().as_deref(),
                Some("engine/readSubagentHistory")
            );
            let id = request.control_u64("id").unwrap().unwrap();
            server_write
                .write_all(format!("{{\"id\":{id},\"result\":{RESULT}}}\n").as_bytes())
                .await
                .unwrap();
            server_write.flush().await.unwrap();
        };
        let (reply, ()) = tokio::join!(call, peer);
        let reply = reply.unwrap();
        assert_eq!(reply.as_str(), RESULT);
        assert_eq!(
            reply.get("page").unwrap().unwrap().control_u64("limit"),
            Ok(Some(u64::MAX))
        );
        let rows: Vec<RawJson> = reply
            .get("history")
            .unwrap()
            .unwrap()
            .deserialize()
            .unwrap();
        let text = rows[0].get("text").unwrap().unwrap().to_runtime().unwrap();
        assert_eq!(
            text.as_string().unwrap().as_utf16(),
            &[0xd800, 0x7c, 0xdc00, 0x7c, 0xfffd]
        );
        assert_eq!(serde_json::to_string(&reply).unwrap(), RESULT);
        let correlated = reply.with_default_string("submissionId", "").unwrap();
        assert_eq!(
            correlated.get("page").unwrap().unwrap().as_str(),
            reply.get("page").unwrap().unwrap().as_str()
        );
        assert_eq!(
            correlated.get("history").unwrap().unwrap().as_str(),
            reply.get("history").unwrap().unwrap().as_str()
        );
        assert_eq!(
            correlated.control_string("submissionId").unwrap(),
            Some(String::new())
        );
        assert_eq!(
            correlated
                .with_default_string("submissionId", "other")
                .unwrap()
                .as_str(),
            correlated.as_str()
        );
        assert!(
            RawJson::from_json(r#"{"submissionId":7,"text":"\ud800"}"#.into())
                .unwrap()
                .with_default_string("submissionId", "")
                .is_err()
        );
    })
    .await
    .expect("opaque mixed results must settle within the fixture deadline");
}

#[tokio::test]
async fn raw_notifications_preserve_control_integer_lexemes_and_native_payloads() {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, mut server_write) = tokio::io::split(server_side);
    drop(server_read);
    let client = AppServerClient::new(client_read, client_write);
    let mut events = client.notifications();
    server_write.write_all(br#"{"method":"session/event","params":{"sessionId":"exact","streamRevision":18446744073709551615,"event":{"event":{"type":"text.delta","text":"\ud800\ud83e\udd80"}}}}
"#).await.unwrap();
    let event = tokio::time::timeout(DEADLINE, events.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event.params.control_u64("streamRevision").unwrap(),
        Some(u64::MAX)
    );
    assert_eq!(
        event.event().unwrap().unwrap()["event"]["text"]
            .as_string()
            .unwrap()
            .as_utf16(),
        &[0xd800, 0xd83e, 0xdd80]
    );
}

#[tokio::test]
async fn exact_rpc_errors_retain_message_data_and_integer_controls() {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, mut server_write) = tokio::io::split(server_side);
    let server = tokio::spawn(async move {
        let mut lines = BufReader::new(server_read).lines();
        lines.next_line().await.unwrap().unwrap();
        server_write.write_all(br#"{"id":1,"error":{"code":-32000,"message":"\ud800","data":{"revision":18446744073709551615,"text":"\udc00"}}}
"#).await.unwrap();
    });
    let client = AppServerClient::new(client_read, client_write);
    let error = tokio::time::timeout(
        DEADLINE,
        client.request::<methods::Initialize>(Default::default()),
    )
    .await
    .unwrap()
    .unwrap_err();
    let ClientError::Rpc {
        code,
        message,
        data,
    } = error
    else {
        panic!("expected an exact RPC error")
    };
    assert_eq!(code, wire::ENGINE_ERROR);
    assert_eq!(message.as_utf16(), &[0xd800]);
    let data = data.unwrap();
    assert_eq!(data.control_u64("revision").unwrap(), Some(u64::MAX));
    assert_eq!(
        data.to_runtime().unwrap()["text"]
            .as_string()
            .unwrap()
            .as_utf16(),
        &[0xdc00]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn rpc_error_data_preserves_absence_null_and_exact_values() {
    tokio::time::timeout(DEADLINE, async {
        for expected in [
            None,
            Some("null"),
            Some(r#"{"text":"\udc00","count":18446744073709551615}"#),
        ] {
            let (client_side, server_side) = tokio::io::duplex(64 * 1024);
            let (server_read, mut server_write) = tokio::io::split(server_side);
            let (client_read, client_write) = tokio::io::split(client_side);
            let client = AppServerClient::new(client_read, client_write);
            let peer = async {
                let mut lines = BufReader::new(server_read).lines();
                let request =
                    RawJson::from_json(lines.next_line().await.unwrap().unwrap()).unwrap();
                let id = request.control_u64("id").unwrap().unwrap();
                let data = expected
                    .map(|value| format!(",\"data\":{value}"))
                    .unwrap_or_default();
                let response = format!(
                    "{{\"id\":{id},\"error\":{{\"code\":-32000,\"message\":\"\\ud800\"{data}}}}}\n"
                );
                server_write.write_all(response.as_bytes()).await.unwrap();
                server_write.flush().await.unwrap();
            };
            let (reply, ()) = tokio::join!(
                client.request::<methods::Initialize>(Default::default()),
                peer
            );
            let ClientError::Rpc {
                code,
                message,
                data,
            } = reply.unwrap_err()
            else {
                panic!("the error reply must retain its declared RPC fields");
            };
            assert_eq!(code, -32000);
            assert_eq!(message.as_utf16(), &[0xd800]);
            assert_eq!(data.as_ref().map(RawJson::as_str), expected);
            if let Some(data) = data {
                assert_eq!(serde_json::to_string(&data).unwrap(), expected.unwrap());
            }
            assert!(client.shared.waiters.lock().unwrap().is_empty());
        }
    })
    .await
    .expect("RPC data presence cases must settle within the fixture deadline");
}

#[tokio::test]
async fn unknown_notification_params_remain_opaque_while_a_request_is_pending() {
    const PARAMS: &str =
        r#"{"sessionId":"session","requestId":7,"streamRevision":"next","text":"\ud800"}"#;
    tokio::time::timeout(DEADLINE, async {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let client = AppServerClient::new(client_read, client_write);
        let mut notifications = client.notifications();
        let (allow_reply, wait_for_reply) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let request = RawJson::from_json(lines.next_line().await.unwrap().unwrap()).unwrap();
            let id = request.control_u64("id").unwrap().unwrap();
            server_write
                .write_all(format!("{{\"method\":\"future/event\",\"params\":{PARAMS}}}\n").as_bytes())
                .await
                .unwrap();
            server_write.flush().await.unwrap();
            wait_for_reply.await.unwrap();
            server_write
                .write_all(format!("{{\"id\":{id},\"result\":{{\"userAgent\":\"fixture\",\"protocolVersion\":1}}}}\n").as_bytes())
                .await
                .unwrap();
            server_write.flush().await.unwrap();
        });
        let pending = client.request::<methods::Initialize>(Default::default());
        tokio::pin!(pending);
        let notification = tokio::select! {
            notification = notifications.next() => notification.expect("the unknown notification is delivered"),
            result = &mut pending => panic!("an unknown notification settled an unrelated request: {result:?}"),
        };
        assert_eq!(notification.method, "future/event");
        assert_eq!(notification.params.as_str(), PARAMS);
        assert_eq!(notification.session_id(), Some("session"));
        assert_eq!(notification.params.control_u64("requestId"), Ok(Some(7)));
        assert!(!client.is_closed());
        assert_eq!(client.shared.waiters.lock().unwrap().len(), 1);
        allow_reply.send(()).unwrap();
        let result = pending.await.expect("the original declared reply still arrives");
        assert_eq!(result.user_agent, "fixture");
        assert_eq!(result.protocol_version, 1);
        assert!(client.shared.waiters.lock().unwrap().is_empty());
        peer.await.unwrap();
    })
    .await
    .expect("unknown notification forwarding must finish within the fixture deadline");
}

#[tokio::test]
async fn exact_reverse_requests_use_the_existing_response_writer() {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, mut server_write) = tokio::io::split(server_side);
    let client = AppServerClient::new(client_read, client_write);
    let mut replies = BufReader::new(server_read).lines();
    server_write.write_all(br#"{"id":18446744073709551615,"method":"elicitation/request","params":{"sessionId":"s1","submissionId":"sub1","request":{"prompt":"\ud800"}}}
"#).await.unwrap();
    let request = tokio::time::timeout(DEADLINE, client.next_server_request())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.session_id(), Some("s1"));
    assert_eq!(request.submission_id(), Some("sub1"));
    assert_eq!(
        request
            .params
            .get("request")
            .unwrap()
            .unwrap()
            .to_runtime()
            .unwrap()["prompt"]
            .as_string()
            .unwrap()
            .as_utf16(),
        &[0xd800]
    );
    request.respond(RuntimeValue::from(exact(&[0xdc00])));
    let reply = tokio::time::timeout(DEADLINE, replies.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let reply = RawJson::from_json(reply).unwrap();
    assert_eq!(reply.control_u64("id").unwrap(), Some(u64::MAX));
    assert_eq!(
        reply.get("result").unwrap().unwrap().to_runtime().unwrap()["response"]
            .as_string()
            .unwrap()
            .as_utf16(),
        &[0xdc00]
    );
}

#[tokio::test]
async fn cancelling_a_typed_request_releases_its_waiter() {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, server_write) = tokio::io::split(server_side);
    let client = AppServerClient::new(client_read, client_write);
    let mut lines = BufReader::new(server_read).lines();
    let mut pending = Box::pin(client.request::<methods::Initialize>(Default::default()));
    tokio::time::timeout(DEADLINE, async {
        tokio::select! {
            result = &mut pending => panic!("an unanswered request finished: {result:?}"),
            line = lines.next_line() => { line.unwrap().unwrap(); },
        }
    })
    .await
    .unwrap();
    assert_eq!(client.shared.waiters.lock().unwrap().len(), 1);
    drop(pending);
    assert!(client.shared.waiters.lock().unwrap().is_empty());
    drop(server_write);
}

#[tokio::test]
async fn malformed_frames_settle_all_waiters_with_decode_error() {
    for malformed in [
        "{\n",
        "{\"id\":18446744073709551616,\"result\":{}}\n",
        "{\"id\":1.0,\"result\":{}}\n",
        "{\"id\":\"1\",\"result\":{}}\n",
        "{\"id\":1,\"id\":2,\"result\":{}}\n",
        "{\"method\":\"session/event\",\"params\":{\"streamRevision\":1.5}}\n",
        "{\"method\":\"session/event\",\"params\":{\"sessionId\":\"\\ud800\"}}\n",
        "{\"id\":1,\"result\":{\"userAgent\":\"fixture\",\"protocolVersion\":\"invalid\"}}\n",
    ] {
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_side);
        let (server_read, mut server_write) = tokio::io::split(server_side);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            for _ in 0..2 {
                lines.next_line().await.unwrap().unwrap();
            }
            server_write.write_all(malformed.as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(DEADLINE, lines.next_line())
                    .await
                    .unwrap()
                    .unwrap()
                    .is_none(),
                "the client must close its write direction"
            );
        });
        let client = AppServerClient::new(client_read, client_write);
        let mut notifications = client.notifications();
        let (first, second) = tokio::time::timeout(DEADLINE, async {
            tokio::join!(
                client.request::<methods::Initialize>(Default::default()),
                client.request::<methods::Initialize>(Default::default())
            )
        })
        .await
        .unwrap();
        assert!(
            matches!(first, Err(ClientError::Decode(_))),
            "{malformed:?}: {first:?}"
        );
        assert!(
            matches!(second, Err(ClientError::Decode(_))),
            "{malformed:?}: {second:?}"
        );
        assert!(client.is_closed());
        assert!(client.shared.waiters.lock().unwrap().is_empty());
        assert!(notifications.next().await.is_none());
        assert!(matches!(
            notifications.terminal_error(),
            Some(ClientError::Decode(_))
        ));
        server.await.unwrap();
    }
}
