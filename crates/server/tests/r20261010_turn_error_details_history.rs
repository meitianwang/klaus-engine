//! Defect: public history responses dropped complete failed-turn diagnostics.
//! Root cause: replay and facade replies exposed only message/kind fields.
//! Regression scope: optional full diagnostics across named public read methods.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::engine::session::TurnIndex;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_protocol::types::ProtocolEvent;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

fn details(kind: &str, message: &str, request_id: &str, status: Option<u16>) -> Value {
    let mut provider = json!({"kind":kind,"message":message,"code":if kind == "prompt_too_long" { "context_length_exceeded" } else { "gateway_body_limit" },"type":if kind == "prompt_too_long" { "invalid_request_error" } else { "request_too_large" },"requestId":request_id});
    if let Some(status) = status {
        provider["status"] = json!(status);
    }
    json!({"code":if kind == "prompt_too_long" { "prompt_too_long" } else { "provider_error" },"message":message,"kind":kind,"requestId":request_id,"providerError":provider,"recoveryReason":"reactive-compaction-disabled","futureDiagnostic":{"retained":true}})
}
fn fixture(root: &Path, archived: bool, only_legacy: bool) -> (PathBuf, Vec<Value>, Value) {
    let target = if archived {
        root.join("archived")
    } else {
        root.to_path_buf()
    };
    std::fs::create_dir_all(&target).unwrap();
    let mut rows = vec![
        json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"history","createdAt":1700000000000_i64,"cwd":root.to_string_lossy(),"model":"fixture","title":"Complete errors"}}),
    ];
    let mut expected = serde_json::Map::new();
    let definitions = if only_legacy {
        vec![("legacy", "network", None), ("empty", "network", None)]
    } else {
        vec![
            ("bytes", "request_too_large", Some(413)),
            ("window", "prompt_too_long", Some(413)),
            ("sse", "request_too_large", None),
            ("legacy", "network", None),
            ("empty", "network", None),
        ]
    };
    for (id, kind, status) in definitions {
        let message = format!("Original {id} failure");
        rows.push(json!({"kind":"turn_context","ctx":{"cwd":root.to_string_lossy(),"model":"fixture","permissionMode":"default","turnId":id}}));
        rows.push(json!({"kind":"message","message":{"type":"user","uuid":format!("{id}-user"),"content":[{"type":"text","text":"Read complete diagnostics"},{"type":"image","mimeType":"image/png","base64":"aGVsbG8=","sourcePath":root.join("history.png").to_string_lossy()}]}}));
        let mut payload = json!({"status":"failed","error":message,"kind":kind});
        if !["legacy", "empty"].contains(&id) {
            let detail = details(kind, &message, &format!("req_{id}"), status);
            expected.insert(id.into(), detail.clone());
            let mut private = detail.clone();
            private["media_origins"] = json!(["private-coordinate"]);
            private["providerError"]["mediaOrigins"] = json!(["private-coordinate"]);
            payload["errorDetails"] = private;
        } else if id == "empty" {
            payload["errorDetails"] = json!({});
        }
        rows.push(json!({"kind":"event","event":"turn_lifecycle","turnId":id,"payload":payload}));
    }
    for row in &rows {
        assert!(
            parse_rollout_line(&row.to_string()).is_some(),
            "valid fixture {row}"
        );
    }
    let path = target.join("1700000000000-history.jsonl");
    std::fs::write(
        &path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    std::fs::write(
        root.join("history.png"),
        b"immutable historical image bytes",
    )
    .unwrap();
    (path, rows, Value::Object(expected))
}
fn read_only_client(
    root: &Path,
    activations: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
) -> TestClient {
    let seen = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            panic!("history reads must not invoke a model");
        }),
        Some(json!({"prewarm":false,"autoMemory":false,"agentMemory":false})),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent, Some(store));
    let create: CreateSession = Arc::new(move |_| {
        activations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("saved history must not activate a session".into()) })
    });
    TestClient::start_full(
        create,
        Some(lifecycle),
        Some(root.join("permissions.json")),
        Some(root.join("config.json")),
    )
}
async fn read(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let result = client.response_for(id).await.0;
    assert!(result.get("error").is_none(), "{result}");
    result["result"].clone()
}

#[tokio::test]
async fn public_saved_history_methods_keep_full_errors_and_never_modify_inputs() {
    for archived in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (path, rows, expected) = fixture(root.path(), archived, false);
        let child = path
            .with_extension("")
            .join("subagents")
            .join("agent-worker.jsonl");
        std::fs::create_dir_all(child.parent().unwrap()).unwrap();
        std::fs::write(
            &child,
            rows[1..]
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        let media = root.path().join("history.png");
        let before = [&path, &child, &media]
            .into_iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect::<Vec<_>>();
        let activations = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut client = read_only_client(root.path(), activations.clone(), calls.clone());
        client.initialize().await;
        for round in 0..2 {
            for (method, params) in [
                (
                    "engine/readThreadHistory",
                    json!({"threadId":"history","limit":10,"itemsView":"full"}),
                ),
                (
                    "engine/readSubagentHistory",
                    json!({"threadId":"history","agentId":"worker"}),
                ),
            ] {
                let result = read(&mut client, 10 + round, method, params).await;
                assert_eq!(result["turnErrorDetails"], expected);
                assert_eq!(result["turnErrorKinds"]["bytes"], "request_too_large");
                assert_eq!(result["turnErrorKinds"]["window"], "prompt_too_long");
                assert_eq!(result["turnErrors"]["sse"], "Original sse failure");
                assert!(result["turnErrorDetails"]["sse"]["providerError"]
                    .get("status")
                    .is_none());
                assert!(!result.to_string().contains("private-coordinate"));
            }
        }
        let complete = read(
            &mut client,
            20,
            "engine/readThread",
            json!({"threadId":"history","limit":10}),
        )
        .await;
        assert_eq!(complete["turnErrorDetails"], expected);
        let sse = complete["turns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|turn| turn["id"] == "sse")
            .unwrap();
        assert_eq!(sse["error"]["message"], "Original sse failure");
        assert_eq!(sse["error"]["additionalDetails"], "request_too_large");
        let mut future = complete.clone();
        future["unknownResponseExtension"] = json!({"accepted":true});
        let parsed: wire::ThreadReadResult = serde_json::from_value(future).unwrap();
        assert_eq!(
            serde_json::to_value(parsed).unwrap()["turnErrorDetails"],
            expected
        );
        let page = read(
            &mut client,
            21,
            "engine/readThread",
            json!({"threadId":"history","cursor":"sse","limit":1}),
        )
        .await;
        assert_eq!(page["turns"][0]["id"], "window");
        assert_eq!(
            page["turnErrorDetails"],
            json!({"window":expected["window"]})
        );
        assert_eq!(activations.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            [&path, &child, &media]
                .into_iter()
                .map(|path| std::fs::read(path).unwrap())
                .collect::<Vec<_>>(),
            before
        );
        assert!(!path.with_extension("jsonl.lock").exists());
        client.close().await;
    }
}

#[tokio::test]
async fn old_or_empty_public_history_diagnostics_omit_the_optional_field() {
    let root = tempfile::tempdir().unwrap();
    let (path, _, _) = fixture(root.path(), false, true);
    let before = std::fs::read(&path).unwrap();
    let mut client = read_only_client(
        root.path(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    client.initialize().await;
    for method in ["engine/readThreadHistory", "engine/readThread"] {
        let result = read(
            &mut client,
            30,
            method,
            json!({"threadId":"history","limit":10}),
        )
        .await;
        assert!(result.get("turnErrorDetails").is_none(), "{result}");
    }
    assert_eq!(std::fs::read(&path).unwrap(), before);
    client.close().await;
}

#[test]
fn history_protocol_codec_accepts_future_fields_and_omits_empty_details() {
    for value in [Value::Null, json!({}), json!({"empty":{}})] {
        let exact = runtime_codec::decode_text(&RuntimeString::from(
            json!({"submissionId":"history","turnErrorDetails":value,"unknownEnvelope":true})
                .to_string(),
        ))
        .unwrap();
        let event: ProtocolEvent = runtime_codec::decode_value(&exact).unwrap();
        let output: Value =
            serde_json::from_slice(&runtime_codec::encode_bytes(&event).unwrap()).unwrap();
        assert!(output.get("turnErrorDetails").is_none());
    }
    let expected = details(
        "request_too_large",
        "Future source error",
        "req_future",
        None,
    );
    let mut private = expected.clone();
    private["providerError"]["media_origins"] = json!(["private-coordinate"]);
    let input = json!({"submissionId":"history","turnErrorDetails":{"future":private},"unknownEnvelope":true});
    let exact = runtime_codec::decode_text(&RuntimeString::from(input.to_string())).unwrap();
    let event: ProtocolEvent = runtime_codec::decode_value(&exact).unwrap();
    let output: Value =
        serde_json::from_slice(&runtime_codec::encode_bytes(&event).unwrap()).unwrap();
    assert_eq!(output["turnErrorDetails"]["future"], expected);
}

#[tokio::test]
async fn public_session_history_and_follow_snapshot_preserve_live_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            panic!("history snapshots must not invoke a model");
        }),
        Some(json!({"prewarm":false,"autoMemory":false,"agentMemory":false})),
    );
    let session = agent.start_session(StartSessionOptions {
        id: Some("live-history".into()),
        cwd: Some(root.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.path().to_string_lossy().into_owned().into()),
        initial_history: Some(vec![klaus_agent_core::schemas::message::Message::User(
            klaus_agent_core::schemas::message::UserMessage {
                uuid: "live-user".into(),
                content: vec![klaus_agent_core::schemas::content::ContentBlock::Text(
                    klaus_agent_core::schemas::content::TextBlock {
                        text: "Read the original error.".into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    },
                )],
                is_meta: None,
                question_answer: None,
            },
        )]),
        ..Default::default()
    });
    let expected = details(
        "request_too_large",
        "Original live failure",
        "req_live",
        None,
    );
    let mut index = TurnIndex::default();
    index
        .message_turn
        .insert("live-user".into(), "live-turn".into());
    index
        .turn_status
        .insert("live-turn".into(), "failed".into());
    index
        .turn_errors
        .insert("live-turn".into(), "Original live failure".into());
    index
        .turn_error_kinds
        .insert("live-turn".into(), "request_too_large".into());
    index.turn_error_details.insert(
        "live-turn".into(),
        runtime_codec::decode_text(&RuntimeString::from(expected.to_string())).unwrap(),
    );
    session.restore_turn_index(index);
    let owned = session.clone();
    let create: CreateSession = Arc::new(move |_| {
        let session = owned.clone();
        Box::pin(async move { Ok(session) })
    });
    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.path().join("permissions.json")),
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    let started = read(
        &mut client,
        40,
        "session/start",
        json!({"init":{"threadId":"live-history"}}),
    )
    .await;
    let session_id = started["sessionId"].as_str().unwrap();
    for _ in 0..2 {
        let history = read(
            &mut client,
            41,
            "session/history",
            json!({"sessionId":session_id}),
        )
        .await;
        assert_eq!(history["turnErrorDetails"]["live-turn"], expected);
        assert_eq!(history["turnErrors"]["live-turn"], "Original live failure");
    }
    let follow = read(
        &mut client,
        42,
        "session/follow",
        json!({"sessionId":session_id}),
    )
    .await;
    assert_eq!(follow["mode"], "snapshot");
    assert_eq!(
        follow["snapshot"]["turnErrorDetails"]["live-turn"],
        expected
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client.close().await;
    session.shutdown_and_wait().await.unwrap();
}
