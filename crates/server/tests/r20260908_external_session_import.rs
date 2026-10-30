//! Defect: desktop ecosystem imports wrote engine rollout files themselves.
//! Root cause: no session-less named facade owned normalized history import.
//! Fixed for the 2026-09-08 external-session migration work item.

use super::r20260908_live_side_task_fork::{request, run_turn};
use super::*;
use crate::app_server::wire;
use klaus_agent_core::runtime_values::runtime_codec;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

fn is_retained_skill_catalog(message: &Value) -> bool {
    message["type"] == "user"
        && message["isMeta"] == true
        && message["content"].as_array().is_some_and(|content| {
            content.len() == 1
                && content[0]["type"] == "text"
                && content[0]["harnessEnvelope"] == "system-reminder"
                && content[0]["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("<skill-catalog:"))
        })
}

const METHOD: &str = "engine/importExternalSession";
const THREAD: &str = "614cc8f1-8c61-5f4f-8ef8-8199ba8b267a";

fn import_params(root: &Path) -> Value {
    json!({
        "threadId":THREAD,"cwd":root,"title":"Imported fixture",
        "source":{"providerId":"qoder","format":"claude-jsonl","path":root.join("source-that-must-not-be-opened.jsonl"),"contentSha256":"a".repeat(64)},
        "messages":[
            {"role":"user","text":"Original question 🦀","timestampMs":1700000000000_u64,"sourceMessageId":"old-user"},
            {"role":"assistant","text":"[external_agent_tool_call: Bash] external transcript only","timestampMs":1700000001000_u64},
            {"role":"assistant","text":"Original answer","timestampMs":1700000002000_u64,"sourceMessageId":"old-answer"},
            {"role":"user","text":"Follow-up from the source"}
        ],
        "hostMetadata":{"sourceAgent":"qoder","id":"not-the-engine-id","source":"not-engine-provenance"}
    })
}

fn make_store(root: &Path) -> Arc<dyn SessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}

fn client(
    root: &Path,
    store: Arc<dyn SessionStore>,
    activations: Arc<AtomicUsize>,
    model_calls: Arc<AtomicUsize>,
) -> TestClient {
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            model_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(text_turn(
                "Actual Klaus fixture continuation",
            )))
        }),
        Some(
            json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}}),
        ),
    );
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let original = crate::lifecycle::engine_sessions(agent, Some(store)).create_session;
    let create: CreateSession = Arc::new(move |context| {
        activations.fetch_add(1, Ordering::SeqCst);
        original(context)
    });
    TestClient::start_full(
        create,
        Some(lifecycle),
        Some(root.join("rules.json")),
        Some(root.join("config.toml")),
    )
}

async fn import(client: &mut TestClient, id: u64, params: Value) -> Value {
    request(client, id, METHOD, params).await
}

fn visible_text(read: &Value) -> Vec<String> {
    read["result"]["turns"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .flat_map(|turn| {
            turn["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| {
                    !(item["type"] == "klausMessage" && is_retained_skill_catalog(&item["message"]))
                })
                .map(|item| {
                    item["text"]
                        .as_str()
                        .or_else(|| item["content"][0]["text"].as_str())
                        .unwrap_or_else(|| panic!("History item has no text: {item}"))
                        .to_string()
                })
        })
        .collect()
}

async fn lifecycle(client: &mut TestClient, id: u64, method: &str, field: &str) -> Value {
    let submission = format!("import-lifecycle-{id}");
    client
        .send(
            json!({"id":id,"method":method,"params":{"threadId":THREAD,"submissionId":submission}}),
        )
        .await;
    let (ack, seen) = client.response_for(id).await;
    assert_eq!(ack["result"]["submissionId"], submission, "{ack}");
    let result = |value: &Value| {
        let event = &value["params"]["event"];
        (event["submissionId"] == submission
            && (event.get(field).is_some() || event.get("error").is_some()))
        .then(|| event.clone())
    };
    if let Some(value) = seen.iter().find_map(result) {
        assert!(value.get("error").is_none(), "{value}");
        return value;
    }
    loop {
        if let Some(value) = result(&client.next_message().await) {
            assert!(value.get("error").is_none(), "{value}");
            return value;
        }
    }
}

#[tokio::test]
async fn external_import_is_model_free_visible_and_preserves_only_external_text_origins() {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let activations = Arc::new(AtomicUsize::new(0));
    let model_calls = Arc::new(AtomicUsize::new(0));
    let mut client = client(
        root.path(),
        store.clone(),
        activations.clone(),
        model_calls.clone(),
    );
    client.initialize().await;
    let params = import_params(root.path());
    let result = import(&mut client, 2, params.clone()).await;
    assert_eq!(
        result["result"],
        json!({"threadId":THREAD,"persisted":true,"alreadyImported":false,"messageCount":4}),
        "{result}"
    );
    assert!(!root
        .path()
        .join("source-that-must-not-be-opened.jsonl")
        .exists());
    let listed = request(
        &mut client,
        3,
        "engine/listSessions",
        json!({"options":{"limit":10}}),
    )
    .await;
    assert!(listed.to_string().contains(THREAD), "{listed}");
    let read = request(
        &mut client,
        4,
        "engine/readThread",
        json!({"threadId":THREAD,"limit":10}),
    )
    .await;
    assert_eq!(
        visible_text(&read),
        params["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| message["text"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(read["result"]["thread"]["title"], "Imported fixture");
    for turn in read["result"]["turns"].as_array().unwrap() {
        assert_eq!(turn["status"], "imported");
        assert!(turn["durationMs"].is_null());
        for item in turn["items"].as_array().unwrap() {
            assert!(
                ["userMessage", "agentMessage"].contains(&item["type"].as_str().unwrap()),
                "{item}"
            );
            assert_eq!(item["externalSource"]["providerId"], "qoder");
        }
    }
    let oldest = read["result"]["turns"].as_array().unwrap().last().unwrap();
    assert_eq!(
        oldest["items"][0]["externalSource"]["timestampMs"],
        1700000000000_u64
    );
    assert_eq!(
        oldest["items"][0]["externalSource"]["sourceMessageId"],
        "old-user"
    );
    let snapshot = store.read_thread(THREAD).await.unwrap();
    assert_eq!(snapshot.meta.source.as_ref().unwrap(), "external:qoder");
    assert_eq!(snapshot.meta.id, THREAD);
    let raw = runtime_codec::encode_text(&snapshot.raw_items)
        .unwrap()
        .try_to_utf8()
        .unwrap();
    assert!(raw.contains("external_session_import_complete"));
    for forbidden in [
        "turn_context",
        "turn_lifecycle",
        "usage_ledger",
        "tool_lifecycle",
        "\"type\":\"tool_use\"",
    ] {
        assert!(!raw.contains(forbidden), "{forbidden}");
    }
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(model_calls.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn external_import_resumes_appends_a_real_turn_and_archives_unarchives_deletes() {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let model_calls = Arc::new(AtomicUsize::new(0));
    let mut client = client(
        root.path(),
        store.clone(),
        Arc::new(AtomicUsize::new(0)),
        model_calls.clone(),
    );
    client.initialize().await;
    let params = import_params(root.path());
    assert!(import(&mut client, 2, params.clone())
        .await
        .get("error")
        .is_none());
    let start = request(&mut client, 3, "session/start", json!({"resume":THREAD,"init":{"threadId":THREAD,"cwd":root.path(),"permissionMode":"bypassPermissions"}})).await;
    assert_eq!(start["result"]["sessionId"], THREAD, "{start}");
    let history = request(
        &mut client,
        4,
        "session/history",
        json!({"sessionId":THREAD}),
    )
    .await;
    assert!(
        history.to_string().contains("Original question 🦀"),
        "{history}"
    );
    assert_eq!(model_calls.load(Ordering::SeqCst), 0);
    run_turn(&mut client, 5, THREAD, "Continue after importing").await;
    assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    let read = request(
        &mut client,
        6,
        "engine/readThread",
        json!({"threadId":THREAD,"limit":10}),
    )
    .await;
    assert_eq!(read["result"]["turns"][0]["status"], "completed", "{read}");
    let retained_catalogs = read["result"]["turns"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|turn| turn["items"].as_array().unwrap())
        .filter(|item| {
            item["type"] == "klausMessage" && is_retained_skill_catalog(&item["message"])
        })
        .count();
    assert_eq!(
        retained_catalogs, 1,
        "the real continuation persists trusted skill context separately from imported text"
    );
    let visible = visible_text(&read);
    assert!(visible.contains(&"Actual Klaus fixture continuation".to_string()));
    assert!(visible.contains(&"Continue after importing".to_string()));
    assert!(visible.contains(&"Original question 🦀".to_string()));
    let shutdown = request(
        &mut client,
        7,
        "session/shutdown",
        json!({"sessionId":THREAD}),
    )
    .await;
    assert!(shutdown.get("error").is_none(), "{shutdown}");
    assert_eq!(
        import(&mut client, 8, params.clone()).await["result"]["alreadyImported"],
        true
    );
    lifecycle(&mut client, 9, "session/archiveSession", "archived").await;
    let archived = request(
        &mut client,
        10,
        "engine/readThread",
        json!({"threadId":THREAD}),
    )
    .await;
    assert_eq!(archived["result"]["thread"]["archived"], true, "{archived}");
    assert_eq!(
        import(&mut client, 11, params).await["result"]["alreadyImported"],
        true
    );
    lifecycle(&mut client, 12, "session/unarchiveSession", "unarchived").await;
    let unarchived = request(
        &mut client,
        13,
        "engine/readThread",
        json!({"threadId":THREAD}),
    )
    .await;
    assert_eq!(
        unarchived["result"]["thread"]["archived"], false,
        "{unarchived}"
    );
    lifecycle(&mut client, 14, "session/deleteSession", "deleted").await;
    assert_eq!(
        request(
            &mut client,
            15,
            "engine/readThread",
            json!({"threadId":THREAD})
        )
        .await["error"]["code"],
        wire::UNKNOWN_SESSION
    );
    let listed = request(&mut client, 16, "engine/listSessions", json!({})).await;
    assert!(!listed.to_string().contains(THREAD), "{listed}");
    client.close().await;
}

#[tokio::test]
async fn external_import_retry_is_content_based_across_endpoints_and_rejects_conflicting_ownership()
{
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let counts = Arc::new(AtomicUsize::new(0));
    let mut first = client(root.path(), store.clone(), counts.clone(), counts.clone());
    first.initialize().await;
    let mut params = import_params(root.path());
    params["hostMetadata"] = serde_json::from_str(r#"{"b":2,"a":{"z":3,"y":4}}"#).unwrap();
    assert_eq!(
        import(&mut first, 2, params.clone()).await["result"]["alreadyImported"],
        false
    );
    first.close().await;
    let before = store.read_thread(THREAD).await.unwrap();
    let mut second = client(
        root.path(),
        make_store(root.path()),
        counts.clone(),
        counts.clone(),
    );
    second.initialize().await;
    let mut reordered = params.clone();
    reordered["hostMetadata"] = serde_json::from_str(r#"{"a":{"y":4,"z":3},"b":2}"#).unwrap();
    assert_eq!(
        import(&mut second, 2, reordered).await["result"]["alreadyImported"],
        true
    );
    for (index, pointer, value) in [
        (3, "/messages/0/text", json!("different normalized text")),
        (4, "/source/contentSha256", json!("b".repeat(64))),
        (
            5,
            "/source/path",
            json!(root.path().join("different-source")),
        ),
        (6, "/source/providerId", json!("claude")),
        (7, "/cwd", json!(root.path().join("different-cwd"))),
        (8, "/title", json!("different title")),
        (9, "/hostMetadata", json!({"new":"owner"})),
    ] {
        let mut changed = params.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        let reply = import(&mut second, index, changed).await;
        assert!(reply.get("error").is_some(), "{reply}");
        assert_eq!(store.read_thread(THREAD).await.unwrap(), before);
    }
    assert_eq!(counts.load(Ordering::SeqCst), 0);
    second.close().await;
}

#[tokio::test]
async fn external_import_rejects_invalid_roles_bounds_and_header_sizes_without_creating_threads() {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let counts = Arc::new(AtomicUsize::new(0));
    let mut client = client(root.path(), store.clone(), counts.clone(), counts.clone());
    client.initialize().await;
    let params = import_params(root.path());
    let mut invalid = vec![
        ("/threadId", json!("../not-an-id")),
        ("/threadId", json!(THREAD.to_uppercase())),
        ("/cwd", json!("relative")),
        ("/source/path", json!("relative")),
        ("/source/contentSha256", json!("g".repeat(64))),
        ("/source/providerId", json!("../claude")),
        ("/source/format", json!("raw-rollout")),
        ("/messages", json!([])),
        ("/messages/0/role", json!("tool")),
        ("/messages/0/text", json!("  \n")),
        ("/messages/0/timestampMs", json!(-1)),
        ("/messages/0/timestampMs", json!(1.5)),
        ("/messages/0/timestampMs", json!(253402300800000_u64)),
        ("/messages/0/sourceMessageId", json!("x".repeat(1025))),
        ("/title", json!("🦀".repeat(1025))),
        ("/hostMetadata", json!([])),
        ("/hostMetadata", json!({"large":"x".repeat(64*1024)})),
        ("/hostMetadata", json!({"large":"x".repeat(64*1024-16)})),
        ("/messages/0/text", json!("x".repeat(1024 * 1024 + 1))),
        (
            "/messages",
            json!(vec![json!({"role":"user","text":"x"}); 10001]),
        ),
        (
            "/messages",
            json!(vec![json!({"role":"user","text":"x".repeat(1024*1024)}); 9]),
        ),
        (
            "/messages",
            json!(vec![
                json!({"role":"user","text":"\0".repeat(1024*1024)});
                3
            ]),
        ),
    ];
    for (index, (pointer, value)) in invalid.drain(..).enumerate() {
        let mut candidate = params.clone();
        *candidate.pointer_mut(pointer).unwrap() = value;
        let reply = import(&mut client, index as u64 + 2, candidate).await;
        assert_eq!(
            reply["error"]["code"],
            wire::INVALID_PARAMS,
            "case {pointer}: {reply}"
        );
    }
    let mut unknown = params;
    unknown["messages"][0]["usage"] = json!({"outputTokens":100});
    assert_eq!(
        import(&mut client, 99, unknown).await["error"]["code"],
        wire::INVALID_PARAMS
    );
    assert!(store
        .list(Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
    assert_eq!(counts.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn external_import_cannot_replace_active_or_archived_native_threads_or_accept_a_truncated_prefix(
) {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let counts = Arc::new(AtomicUsize::new(0));
    let params = import_params(root.path());
    let prepared =
        super::super::external_import::prepare(serde_json::from_value(params.clone()).unwrap())
            .unwrap();
    let rollout = store.create(prepared.meta).await.unwrap();
    rollout.shutdown().await.unwrap();
    let before = store.read_thread(THREAD).await.unwrap();
    let mut client = client(root.path(), store.clone(), counts.clone(), counts);
    client.initialize().await;
    assert!(import(&mut client, 2, params.clone())
        .await
        .get("error")
        .is_some());
    assert_eq!(store.read_thread(THREAD).await.unwrap(), before);
    store.archive(THREAD).await.unwrap();
    let archived = store.read_thread(THREAD).await.unwrap();
    assert!(import(&mut client, 3, params.clone())
        .await
        .get("error")
        .is_some());
    assert_eq!(store.read_thread(THREAD).await.unwrap(), archived);
    store.delete(THREAD).await.unwrap();
    assert_eq!(
        import(&mut client, 4, params.clone()).await["result"]["alreadyImported"],
        false
    );
    let path = root
        .path()
        .join("sessions")
        .join(format!("1700000000000-{THREAD}.jsonl"));
    let bytes = std::fs::read(&path).unwrap();
    let last_line = bytes[..bytes.len() - 1]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .unwrap()
        + 1;
    std::fs::write(&path, &bytes[..last_line]).unwrap();
    assert!(import(&mut client, 5, params.clone())
        .await
        .get("error")
        .is_some());
    assert_eq!(std::fs::read(&path).unwrap(), bytes[..last_line]);
    std::fs::write(&path, b"{unreadable header").unwrap();
    let mut no_times = params;
    for message in no_times["messages"].as_array_mut().unwrap() {
        message.as_object_mut().unwrap().remove("timestampMs");
    }
    assert!(import(&mut client, 6, no_times)
        .await
        .get("error")
        .is_some());
    assert_eq!(std::fs::read(&path).unwrap(), b"{unreadable header");
    client.close().await;
}

#[tokio::test]
async fn external_import_two_endpoint_race_has_one_publication_and_deterministic_retry() {
    let root = tempfile::tempdir().unwrap();
    let counts = Arc::new(AtomicUsize::new(0));
    let mut one = client(
        root.path(),
        make_store(root.path()),
        counts.clone(),
        counts.clone(),
    );
    one.initialize().await;
    let mut two = client(
        root.path(),
        make_store(root.path()),
        counts.clone(),
        counts.clone(),
    );
    two.initialize().await;
    let params = import_params(root.path());
    let (first, second) = tokio::join!(
        import(&mut one, 2, params.clone()),
        import(&mut two, 2, params.clone())
    );
    assert_eq!(
        usize::from(first["result"]["alreadyImported"] == false)
            + usize::from(second["result"]["alreadyImported"] == false),
        1,
        "{first}\n{second}"
    );
    for result in [&first, &second] {
        assert!(
            result["result"]["persisted"] == true
                || result["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("concurrent lifecycle")),
            "{result}"
        );
    }
    assert_eq!(
        import(&mut two, 3, params).await["result"]["alreadyImported"],
        true
    );
    let store = make_store(root.path());
    assert_eq!(store.list(Default::default()).await.unwrap().items.len(), 1);
    let entries: Vec<_> = std::fs::read_dir(root.path().join("sessions"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.ends_with(".jsonl"))
            .count(),
        1
    );
    assert!(!entries.iter().any(|entry| entry.ends_with(".tmp")));
    one.close().await;
    two.close().await;
}

#[tokio::test]
async fn external_import_does_not_create_history_under_a_lazy_registered_session() {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let counts = Arc::new(AtomicUsize::new(0));
    let mut client = client(root.path(), store.clone(), counts.clone(), counts.clone());
    client.initialize().await;
    let start = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":THREAD,"cwd":root.path()}}),
    )
    .await;
    assert_eq!(start["result"]["sessionId"], THREAD);
    let reply = import(&mut client, 3, import_params(root.path())).await;
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("already registered"),
        "{reply}"
    );
    assert!(store
        .list(Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
    assert_eq!(counts.load(Ordering::SeqCst), 0);
    request(&mut client, 4, "session/close", json!({"sessionId":THREAD})).await;
    assert_eq!(
        import(&mut client, 5, import_params(root.path())).await["result"]["alreadyImported"],
        false
    );
    client.close().await;
}

#[tokio::test]
async fn external_import_reserves_its_id_until_store_publication_without_blocking_other_ids() {
    let root = tempfile::tempdir().unwrap();
    let store = make_store(root.path());
    let agent = make_agent_for_tests(vec![]);
    let mut hooks = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let original = hooks.import_external_session.take().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    hooks.import_external_session = Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |value| {
            let original = original.clone();
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                original(value).await
            })
        }
    }));
    let create = crate::lifecycle::engine_sessions(agent, Some(store.clone())).create_session;
    let mut client = TestClient::start_full(
        create,
        Some(hooks),
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    client
        .send(json!({"id":2,"method":METHOD,"params":import_params(root.path())}))
        .await;
    entered.notified().await;
    let duplicate = request(
        &mut client,
        3,
        "session/start",
        json!({"init":{"threadId":THREAD,"cwd":root.path()}}),
    )
    .await;
    assert_eq!(
        duplicate["error"]["code"],
        wire::SESSION_ALREADY_LIVE,
        "{duplicate}"
    );
    let other = request(
        &mut client,
        4,
        "session/start",
        json!({"init":{"threadId":"independent-id","cwd":root.path()}}),
    )
    .await;
    assert_eq!(other["result"]["sessionId"], "independent-id", "{other}");
    request(
        &mut client,
        5,
        "session/close",
        json!({"sessionId":"independent-id"}),
    )
    .await;
    release.notify_one();
    let (imported, _) = client.response_for(2).await;
    assert_eq!(imported["result"]["persisted"], true, "{imported}");
    let resumed = request(
        &mut client,
        6,
        "session/start",
        json!({"resume":THREAD,"init":{"threadId":THREAD,"cwd":root.path()}}),
    )
    .await;
    assert_eq!(resumed["result"]["sessionId"], THREAD, "{resumed}");
    request(&mut client, 7, "session/close", json!({"sessionId":THREAD})).await;
    client.close().await;
}
