//! Defect 20261024: next-turn input was owned by a host cache and could drain inline.
//! Root cause: future turns had no durable owner coordinated with turn admission.
//! Fix: named queue methods commit input and receipts around the atomic idle gate.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

fn client_at(root: &std::path::Path, create: CreateSession) -> TestClient {
    TestClient::start_full(
        create,
        None,
        Some(root.join("rules.json")),
        Some(root.join("config.json")),
    )
}

async fn rpc(client: &mut TestClient, id: u64, method: &str, params: Value) -> (Value, Vec<Value>) {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    assert!(reply.get("error").is_none(), "{reply}");
    (reply["result"].clone(), seen)
}

fn receipt_created_at() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn add(thread: &str, client_id: &str) -> Value {
    json!({"threadId":thread,"input":[{"type":"text","text":client_id}],"clientUserMessageId":client_id,
        "clientMetadata":{"item":{"text":client_id,"attachments":[{"path":"/private/retained.pdf"}],"displayMetadata":{"displayText":"Rich input"}}},
        "createdAt":123})
}

async fn list(client: &mut TestClient, id: u64, thread: &str) -> Value {
    rpc(client, id, "thread/queue/list", json!({"threadId":thread}))
        .await
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_projection_preserves_empty_input_and_fences_changed_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let (empty, _) = rpc(
        &mut client,
        801,
        "thread/queue/add",
        json!({"threadId":"projection","clientUserMessageId":"empty","input":[]}),
    )
    .await;
    assert_eq!(empty["queuedSubmission"]["input"], json!([]));
    let id = empty["queuedSubmission"]["id"].clone();
    let (projected, _) = rpc(
        &mut client,
        802,
        "thread/queue/list",
        json!({"threadId":"projection","includeInput":false}),
    )
    .await;
    let row = &projected["data"][0];
    assert!(row.get("input").is_none());
    assert_eq!(row["inputOmitted"], true);
    assert!(row["inputFingerprint"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let (updated, _) = rpc(
        &mut client,
        803,
        "thread/queue/update",
        json!({"threadId":"projection","queuedSubmissionId":id,"includeInput":false,
            "clientMetadata":{"item":{"text":"updated"}}}),
    )
    .await;
    assert_eq!(
        updated["queuedSubmission"]["inputFingerprint"],
        row["inputFingerprint"]
    );
    assert!(updated["queuedSubmission"]["rowVersion"].as_u64() > row["rowVersion"].as_u64());
    let (refused, _) = rpc(&mut client, 804, "thread/queue/start",
        json!({"threadId":"projection","queuedSubmissionId":id,"includeInput":false,
            "expectedInputFingerprint":row["inputFingerprint"],"expectedRowVersion":row["rowVersion"]})).await;
    assert_eq!(refused["status"], "not-submitted");
    assert_eq!(refused["reason"]["code"], "queue-entry-changed");
    let current = list(&mut client, 805, "projection").await;
    assert!(current["data"][0].get("pausedReason").is_none());
    assert_eq!(current["data"][0]["input"], json!([]));
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_atomic_clear_exceeds_single_delete_proof_budget_and_never_revives() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let mut huge = add("clear-large", "huge");
    huge["clientMetadata"]["item"]["text"] = json!("x".repeat(16 * 1024 * 1024 + 1));
    huge.as_object_mut().unwrap().remove("threadId");
    rpc(
        &mut client,
        810,
        "thread/queue/import",
        json!({
            "threadId":"clear-large","migrationId":"existing-large","minimumRevision":0,
            "queuedSubmissions":[huge]
        }),
    )
    .await;
    let (imported, _) = rpc(
        &mut client,
        811,
        "thread/queue/list",
        json!({"threadId":"clear-large","includeInput":false}),
    )
    .await;
    let id = imported["data"][0]["id"].clone();
    client
        .send(json!({"id":812,"method":"thread/queue/delete","params":{
        "threadId":"clear-large","queuedSubmissionId":id,"includeInput":false}}))
        .await;
    let (refused, _) = client.response_for(812).await;
    assert_eq!(refused["error"]["code"], -32602);
    let (cleared, _) = rpc(
        &mut client,
        813,
        "thread/queue/clear",
        json!({"threadId":"clear-large","includeInput":false,"expectedRevision":1}),
    )
    .await;
    assert_eq!(cleared["cleared"].as_array().unwrap().len(), 1);
    assert_eq!(cleared["revision"], 2);
    let (replayed, _) = rpc(
        &mut client,
        814,
        "thread/queue/add",
        add("clear-large", "huge"),
    )
    .await;
    assert_eq!(replayed["replayed"], true);
    assert!(replayed["queuedSubmission"].is_null());
    assert_eq!(replayed["receipt"]["kind"], "cleared");
    assert!(list(&mut client, 815, "clear-large").await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_birth_identity_survives_native_edit_consumption_and_lookup() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, calls, _) = blocking_fixture(root.path()).await;
    let original = add(&thread, "birth");
    let (added, _) = rpc(&mut client, 821, "thread/queue/add", original.clone()).await;
    let id = added["queuedSubmission"]["id"].clone();
    let (edited, _) = rpc(&mut client, 822, "thread/queue/update",
        json!({"threadId":thread,"queuedSubmissionId":id,"input":[{"type":"text","text":"edited native"}]})).await;
    assert_ne!(
        edited["queuedSubmission"]["inputFingerprint"],
        added["queuedSubmission"]["inputFingerprint"]
    );
    let (replayed, _) = rpc(&mut client, 823, "thread/queue/add", original.clone()).await;
    assert_eq!(replayed["replayed"], true);
    assert_eq!(
        replayed["queuedSubmission"]["input"],
        edited["queuedSubmission"]["input"]
    );
    let mut conflict = original.clone();
    conflict["input"] = json!([{"type":"text","text":"wrong birth"}]);
    client
        .send(json!({"id":824,"method":"thread/queue/add","params":conflict}))
        .await;
    assert_eq!(client.response_for(824).await.0["error"]["code"], -32602);
    let (lookup, _) = rpc(&mut client, 825, "thread/queue/add",
        json!({"threadId":thread,"clientUserMessageId":"birth","lookupOnly":true,"includeInput":false})).await;
    assert_eq!(lookup["found"], true);
    assert_eq!(lookup["queuedSubmission"]["id"], id);
    let (started, _) = rpc(
        &mut client,
        826,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":id,"includeInput":false}),
    )
    .await;
    assert_eq!(started["status"], "started");
    wait_calls(&calls, 1).await;
    let (consumed, _) = rpc(&mut client, 827, "thread/queue/add", original).await;
    assert_eq!(consumed["replayed"], true);
    assert!(consumed["queuedSubmission"].is_null());
    assert_eq!(consumed["receipt"]["kind"], "started");
    client
        .send(json!({"id":828,"method":"thread/queue/add","params":conflict}))
        .await;
    assert_eq!(client.response_for(828).await.0["error"]["code"], -32602);
    rpc(
        &mut client,
        829,
        "turn/interrupt",
        json!({"sessionId":thread}),
    )
    .await;
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_restore_advances_version_and_delete_replay_uses_its_historical_proof() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let original = add("proof-replay", "original");
    let (added, _) = rpc(&mut client, 831, "thread/queue/add", original.clone()).await;
    let row = &added["queuedSubmission"];
    let removal = json!({"threadId":"proof-replay","queuedSubmissionId":row["id"],
        "mutationId":"remove-proof","mutationFingerprint":"proof-fingerprint"});
    let (removed, _) = rpc(&mut client, 832, "thread/queue/delete", removal.clone()).await;
    let mut restore = original;
    restore["restore"] = json!(true);
    restore["requireRemovedReceipt"] = json!(true);
    restore["input"] = json!([]);
    let (restored, _) = rpc(&mut client, 833, "thread/queue/add", restore).await;
    assert_eq!(restored["queuedSubmission"]["id"], row["id"]);
    assert!(restored["queuedSubmission"]["rowVersion"].as_u64() > row["rowVersion"].as_u64());
    assert_eq!(
        restored["queuedSubmission"]["inputFingerprint"],
        row["inputFingerprint"]
    );
    rpc(
        &mut client,
        834,
        "thread/queue/update",
        json!({"threadId":"proof-replay",
        "queuedSubmissionId":row["id"],"input":[{"type":"text","text":"new native"}],
        "clientMetadata":{"item":{"text":"new UI"}}}),
    )
    .await;
    let (historical, _) = rpc(&mut client, 835, "thread/queue/delete", removal.clone()).await;
    assert_eq!(historical["replayed"], true);
    assert_eq!(historical["queuedSubmission"], removed["queuedSubmission"]);
    let db = root.path().join("thread-queue.sqlite3");
    assert!(db.exists());
    rusqlite::Connection::open(db)
        .unwrap()
        .execute("DELETE FROM queue_proofs", [])
        .unwrap();
    let (expired, _) = rpc(&mut client, 836, "thread/queue/delete", removal).await;
    assert_eq!(expired["replayed"], true);
    assert!(expired["queuedSubmission"].is_null());
    assert_eq!(expired["receipt"]["kind"], "deleted");
    let current = list(&mut client, 837, "proof-replay").await;
    assert_eq!(
        current["data"][0]["clientMetadata"]["item"]["text"],
        "new UI"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_normal_add_ui_capacity_does_not_charge_native_bytes_or_limit_import() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let mut oversized = add("capacity", "oversized");
    oversized["clientMetadata"]["item"]["text"] = json!("x".repeat(8 * 1024 * 1024));
    client
        .send(json!({"id":841,"method":"thread/queue/add","params":oversized}))
        .await;
    assert_eq!(client.response_for(841).await.0["error"]["code"], -32602);
    assert!(list(&mut client, 842, "capacity").await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    let (native, _) = rpc(
        &mut client,
        843,
        "thread/queue/add",
        json!({
            "threadId":"native-producer","clientUserMessageId":"large-native","includeInput":false,
            "input":[{"type":"image","base64":"a".repeat(9*1024*1024),"mimeType":"image/png"}]
        }),
    )
    .await;
    assert_eq!(native["queuedSubmission"]["inputOmitted"], true);
    let entries = (0..128)
        .map(|i| json!({"clientUserMessageId":format!("import-{i}"),"input":[]}))
        .collect::<Vec<_>>();
    rpc(&mut client, 844, "thread/queue/import", json!({
        "threadId":"row-capacity","migrationId":"existing","minimumRevision":0,"queuedSubmissions":entries
    })).await;
    client
        .send(json!({"id":845,"method":"thread/queue/add","params":add("row-capacity","extra")}))
        .await;
    assert_eq!(client.response_for(845).await.0["error"]["code"], -32602);
    rpc(
        &mut client,
        846,
        "thread/queue/import",
        json!({
            "threadId":"row-capacity","migrationId":"more-existing","minimumRevision":0,
            "queuedSubmissions":[{"clientUserMessageId":"import-128","input":[]}]
        }),
    )
    .await;
    let imported = list(&mut client, 847, "row-capacity").await;
    assert_eq!(imported["nextCursor"], "2:100");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_cold_crud_receipts_pagination_cas_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let factory = make_create_session(&[]);
    let mut client = client_at(root.path(), factory.clone());
    client.initialize().await;
    let thread = "cold-private-thread";
    let (first, seen) = rpc(&mut client, 10, "thread/queue/add", add(thread, "one")).await;
    let first_id = first["queuedSubmission"]["id"].clone();
    assert!(
        seen.iter()
            .any(|event| event["method"] == "thread/queue/changed"
                && event["params"]["revision"] == 1)
    );
    assert_eq!(first["revision"], 1);
    let (second, _) = rpc(&mut client, 11, "thread/queue/add", add(thread, "two")).await;
    let second_id = second["queuedSubmission"]["id"].clone();
    let (page, _) = rpc(
        &mut client,
        12,
        "thread/queue/list",
        json!({"threadId":thread,"limit":1}),
    )
    .await;
    assert_eq!(page["data"][0]["id"], first_id);
    assert_eq!(page["nextCursor"], "2:1");
    let (edited, _) = rpc(&mut client, 13, "thread/queue/update", json!({"threadId":thread,"queuedSubmissionId":first_id,
        "expectedRevision":2,"clientMetadata":{"item":{"text":"edited","files":[{"path":"/tmp/image.png","name":"image"}]}},"pausedReason":"interrupted"})).await;
    assert_eq!(edited["queuedSubmission"]["pausedReason"], "interrupted");
    assert_eq!(
        edited["queuedSubmission"]["input"],
        add(thread, "one")["input"]
    );
    client.send(json!({"id":14,"method":"thread/queue/list","params":{"threadId":thread,"cursor":page["nextCursor"]}})).await;
    let (stale, _) = client.response_for(14).await;
    assert_eq!(stale["error"]["code"], -32014);
    let (_, _) = rpc(
        &mut client,
        15,
        "thread/queue/reorder",
        json!({"threadId":thread,"queuedSubmissionIds":[second_id,first_id],"expectedRevision":3}),
    )
    .await;
    let (missing, _) = rpc(
        &mut client,
        16,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":second_id}),
    )
    .await;
    assert_eq!(missing["reason"]["code"], "session-unavailable");
    client.close().await;
    let mut reopened = client_at(root.path(), factory);
    reopened.initialize().await;
    let snapshot = list(&mut reopened, 20, thread).await;
    assert_eq!(snapshot["revision"], 4);
    assert_eq!(snapshot["data"][0]["id"], second_id);
    assert_eq!(
        snapshot["data"][1]["clientMetadata"],
        edited["queuedSubmission"]["clientMetadata"]
    );
    let (paused, _) = rpc(
        &mut reopened,
        21,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":first_id}),
    )
    .await;
    assert_eq!(paused["reason"]["code"], "queue-paused");
    rpc(
        &mut reopened,
        22,
        "thread/queue/resume",
        json!({"threadId":thread,"reason":"different"}),
    )
    .await;
    assert_eq!(
        list(&mut reopened, 23, thread).await["data"][1]["pausedReason"],
        "interrupted"
    );
    rpc(
        &mut reopened,
        24,
        "thread/queue/resume",
        json!({"threadId":thread,"reason":"interrupted"}),
    )
    .await;
    assert!(list(&mut reopened, 25, thread).await["data"][1]
        .get("pausedReason")
        .is_none());
    reopened.close().await;
}

async fn blocking_fixture(
    root: &std::path::Path,
) -> (
    TestClient,
    String,
    Arc<AtomicUsize>,
    Arc<Mutex<Option<Arc<Session>>>>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, signal| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                Box::pin(futures::stream::once(async move {
                    signal.cancelled().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                }))
            } else {
                Box::pin(futures::stream::iter(text_turn("done")))
            }
        }),
        Some(json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false})),
    );
    let live = Arc::new(Mutex::new(None));
    let capture = live.clone();
    let factory = create_session_for_agent_tests(agent);
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = factory(ctx).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok(session)
        })
    });
    let mut client = client_at(root, create);
    client.initialize().await;
    let (started, _) = rpc(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"live-durable-thread","cwd":root}}),
    )
    .await;
    (
        client,
        started["sessionId"].as_str().unwrap().into(),
        calls,
        live,
    )
}

async fn wait_calls(calls: &AtomicUsize, expected: usize) {
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while calls.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_busy_is_strict_next_turn_steer_transfers_once_and_stop_retains_other_rows() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, calls, live) = blocking_fixture(root.path()).await;
    rpc(
        &mut client,
        10,
        "turn/submit",
        json!({"sessionId":thread,"input":"block","mode":"start-if-idle","submissionId":"owner"}),
    )
    .await;
    wait_calls(&calls, 1).await;
    let (first, _) = rpc(&mut client, 11, "thread/queue/add", add(&thread, "next")).await;
    let row = &first["queuedSubmission"];
    let (busy, _) = rpc(
        &mut client,
        12,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":row["id"],"submissionId":"future"}),
    )
    .await;
    assert_eq!(busy["status"], "not-submitted");
    assert_eq!(busy["reason"]["code"], "not-idle");
    assert_eq!(
        list(&mut client, 13, &thread).await["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let session = live.lock().unwrap().clone().unwrap();
    assert!(
        session.queue_snapshot().items.is_empty(),
        "future turns never enter the pending steer queue"
    );
    tokio::time::timeout(Duration::from_secs(1), rpc(&mut client, 14, "thread/queue/update", json!({"threadId":thread,
        "queuedSubmissionId":row["id"],"clientMetadata":{"item":{"text":"next","displayMetadata":{"displayText":"next display"}}}}))).await.unwrap();
    let queue_id = "c9a02ab1-092b-4206-8b14-bbb7d37d5f20";
    let (steered, _) = rpc(&mut client, 15, "thread/queue/start", json!({"threadId":thread,"queuedSubmissionId":row["id"],
        "mode":"steer","expectedTurnId":"owner","submissionId":"sent-now","queueMessageId":queue_id,"delivery":"user","options":{"model":"ignored-for-steer"}})).await;
    assert_eq!(steered["status"], "steered");
    assert_eq!(steered["turnId"], "owner");
    assert_eq!(steered["submissionId"], "sent-now");
    assert!(list(&mut client, 16, &thread).await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    let pending = session.queue_snapshot();
    assert_eq!(pending.items.len(), 1);
    assert_eq!(
        pending.items[0]
            .message
            .uuid
            .as_ref()
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        queue_id
    );
    let (again, _) = rpc(&mut client, 17, "thread/queue/add", add(&thread, "next")).await;
    assert_eq!(again["replayed"], true);
    assert_eq!(again["receipt"]["kind"], "steered");
    let (keep, _) = rpc(
        &mut client,
        18,
        "thread/queue/add",
        add(&thread, "keep-after-stop"),
    )
    .await;
    rpc(
        &mut client,
        19,
        "turn/interrupt",
        json!({"sessionId":thread}),
    )
    .await;
    assert_eq!(
        list(&mut client, 20, &thread).await["data"][0]["id"],
        keep["queuedSubmission"]["id"]
    );
    client.close().await;
    let mut reopened = client_at(root.path(), make_create_session(&["restarted"]));
    reopened.initialize().await;
    assert_eq!(
        list(&mut reopened, 30, &thread).await["data"][0]["id"],
        keep["queuedSubmission"]["id"]
    );
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_concurrent_starts_admit_one_and_only_successful_start_consumes() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, calls, _) = blocking_fixture(root.path()).await;
    for index in 0..8 {
        rpc(
            &mut client,
            10 + index,
            "thread/queue/add",
            add(&thread, &format!("input-{index}")),
        )
        .await;
    }
    for id in 30..38 {
        client.send(json!({"id":id,"method":"thread/queue/start","params":{"threadId":thread,"submissionId":format!("start-{id}")}})).await;
    }
    let mut results = Vec::new();
    while results.len() < 8 {
        let reply = client.next_message().await;
        if reply["id"]
            .as_u64()
            .is_some_and(|id| (30..38).contains(&id))
        {
            assert!(reply.get("error").is_none(), "{reply}");
            results.push(reply["result"].clone());
        }
    }
    assert_eq!(
        results
            .iter()
            .filter(|result| result["status"] == "started")
            .count(),
        1
    );
    assert!(results
        .iter()
        .filter(|result| result["status"] != "started")
        .all(|result| result["reason"]["code"] == "not-idle"));
    wait_calls(&calls, 1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        list(&mut client, 40, &thread).await["data"]
            .as_array()
            .unwrap()
            .len(),
        7
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_migration_unknown_and_legacy_receipts_survive_reconnect_without_replay() {
    let root = tempfile::tempdir().unwrap();
    let factory = make_create_session(&[]);
    let mut client = client_at(root.path(), factory.clone());
    client.initialize().await;
    let imported = json!({"threadId":"migrated","migrationId":"migration-1","minimumRevision":100,
        "queuedSubmissions":[{"input":[{"type":"text","text":"unknown"}],"clientUserMessageId":"unknown","clientMetadata":{"item":{"text":"unknown"}},"dispatching":true}],
        "receipts":[{"kind":"addition","mutationId":"old-add","fingerprint":"addition-fingerprint","createdAt":receipt_created_at()},
            {"kind":"removal","mutationId":"old-remove","fingerprint":"remove-fingerprint","createdAt":receipt_created_at(),
             "result":{"removed":{"id":"removed-client","item":{"text":"removed","attachments":["/gone/image"]},"pausedReason":null,"createdAt":123}}}]});
    let (result, _) = rpc(&mut client, 10, "thread/queue/import", imported.clone()).await;
    assert_eq!(result["revision"], 101);
    let (removed_replay, _) = rpc(&mut client, 101, "thread/queue/delete", json!({
        "threadId":"migrated","queuedSubmissionId":"removed-client","mutationId":"old-remove","mutationFingerprint":"remove-fingerprint"
    })).await;
    assert_eq!(removed_replay["replayed"], true);
    client.send(json!({"id":102,"method":"thread/queue/delete","params":{
        "threadId":"migrated","queuedSubmissionId":"different-client","mutationId":"old-remove","mutationFingerprint":"remove-fingerprint"
    }})).await;
    assert_eq!(client.response_for(102).await.0["error"]["code"], -32602);
    assert_eq!(
        rpc(&mut client, 11, "thread/queue/import", imported)
            .await
            .0["revision"],
        101
    );
    rpc(
        &mut client,
        12,
        "thread/queue/resume",
        json!({"threadId":"migrated"}),
    )
    .await;
    let (unknown, _) = rpc(
        &mut client,
        13,
        "thread/queue/start",
        json!({"threadId":"migrated"}),
    )
    .await;
    assert_eq!(unknown["reason"]["code"], "admission-unknown");
    let mut duplicate = add("migrated", "legacy-consumed");
    duplicate["mutationId"] = "old-add".into();
    duplicate["mutationFingerprint"] = "addition-fingerprint".into();
    let (replay, _) = rpc(&mut client, 14, "thread/queue/add", duplicate).await;
    assert_eq!(replay["replayed"], true);
    assert!(replay["queuedSubmission"].is_null());
    let restored = json!({"threadId":"migrated","input":[{"type":"text","text":"removed"}],"clientUserMessageId":"removed-client",
        "clientMetadata":{"item":{"text":"removed","attachments":["/gone/image"]}},"createdAt":123,"restore":true,"requireRemovedReceipt":true});
    let (undo, _) = rpc(&mut client, 15, "thread/queue/add", restored).await;
    assert_eq!(
        undo["queuedSubmission"]["clientMetadata"]["item"]["attachments"][0],
        "/gone/image"
    );
    client.close().await;
    let mut reopened = client_at(root.path(), factory);
    reopened.initialize().await;
    let snapshot = list(&mut reopened, 20, "migrated").await;
    assert_eq!(snapshot["data"].as_array().unwrap().len(), 2);
    assert_eq!(snapshot["data"][0]["pausedReason"], "admission-unknown");
    assert!(snapshot["data"][0]["dispatching"].is_object());
    reopened.close().await;
}

#[test]
fn durable_queue_declared_schema_and_notification_cover_the_named_surface() {
    let bundle = schema_bundle();
    for name in [
        "list", "add", "update", "delete", "reorder", "start", "pause", "resume", "import",
        "discard",
    ] {
        assert!(bundle["requests"][format!("thread/queue/{name}")]["params"].is_object());
    }
    assert_eq!(
        bundle["notifications"]["thread/queue/changed"]["params"]["properties"]["threadId"]["type"],
        "string"
    );
}

async fn interrupt_result(
    client: &mut TestClient,
    submission: &str,
    mut seen: Vec<Value>,
) -> Value {
    loop {
        for message in &seen {
            let event = &message["params"]["event"];
            if event["submissionId"] == submission && event.get("turnInterrupt").is_some() {
                return event.clone();
            }
        }
        seen = vec![client.next_message().await];
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_interrupt_pauses_before_cancel_and_stale_target_never_mutates() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, calls, _) = blocking_fixture(root.path()).await;
    rpc(
        &mut client,
        10,
        "turn/submit",
        json!({"sessionId":thread,"input":"block","mode":"start-if-idle","submissionId":"owner"}),
    )
    .await;
    wait_calls(&calls, 1).await;
    rpc(&mut client, 11, "thread/queue/add", add(&thread, "queued")).await;
    let mut boundary = add(&thread, "directory-boundary");
    boundary["pausedReason"] = "Directory changed.".into();
    rpc(&mut client, 111, "thread/queue/add", boundary).await;
    let before = list(&mut client, 12, &thread).await;
    let (_, seen) = rpc(&mut client, 13, "turn/interrupt", json!({"sessionId":thread,"submissionId":"stale-stop","origin":"targeted-turn","expectedTurnId":"retired"})).await;
    let stale = interrupt_result(&mut client, "stale-stop", seen).await;
    assert_eq!(stale["turnInterrupt"]["accepted"], false);
    assert_eq!(list(&mut client, 14, &thread).await, before);
    let (_, seen) = rpc(
        &mut client,
        15,
        "turn/interrupt",
        json!({"sessionId":thread,"submissionId":"real-stop","expectedTurnId":"owner"}),
    )
    .await;
    let stopped = interrupt_result(&mut client, "real-stop", seen).await;
    assert_eq!(stopped["turnInterrupt"]["accepted"], true);
    let paused = list(&mut client, 16, &thread).await;
    assert_eq!(paused["data"][1]["pausedReason"], "Directory changed.");
    assert_eq!(
        paused["data"][0]["pausedReason"],
        "Interrupted before the turn ended."
    );
    assert_eq!(
        paused["revision"].as_u64().unwrap(),
        before["revision"].as_u64().unwrap() + 1
    );
    client.close().await;
    let mut restarted = client_at(root.path(), make_create_session(&[]));
    restarted.initialize().await;
    assert_eq!(
        list(&mut restarted, 20, &thread).await["data"],
        paused["data"]
    );
    restarted.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_cross_endpoint_commits_notify_and_watchers_end_with_connection() {
    let root = tempfile::tempdir().unwrap();
    let factory = make_create_session(&[]);
    let mut observer = client_at(root.path(), factory.clone());
    observer.initialize().await;
    list(&mut observer, 10, "observed").await;
    let mut writer = client_at(root.path(), factory);
    writer.initialize().await;
    rpc(
        &mut writer,
        11,
        "thread/queue/add",
        add("observed", "external"),
    )
    .await;
    let notification = observer
        .next_message_with_timeout(Duration::from_secs(15))
        .await;
    assert_eq!(notification["method"], "thread/queue/changed");
    assert_eq!(
        notification["params"],
        json!({"threadId":"observed","revision":1})
    );
    assert_eq!(
        list(&mut observer, 12, "observed").await["data"][0]["clientUserMessageId"],
        "external"
    );
    observer.close().await;
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_thread_delete_clears_pending_input_and_receipts_inside_engine() {
    let root = tempfile::tempdir().unwrap();
    let lifecycle = klaus_agent_protocol::ProtocolLifecycle {
        delete_session: Some(Arc::new(|thread| {
            Box::pin(async move {
                Ok(klaus_agent_protocol::types::DeleteSessionResult {
                    thread_id: thread.into(),
                    deleted: true,
                })
            })
        })),
        ..Default::default()
    };
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        Some(lifecycle),
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    rpc(&mut client, 10, "thread/queue/add", add("deleted", "input")).await;
    let (_, mut seen) = rpc(
        &mut client,
        11,
        "session/deleteSession",
        json!({"threadId":"deleted","submissionId":"delete-thread"}),
    )
    .await;
    loop {
        if seen
            .iter()
            .any(|message| message["method"] == "thread/deleted")
        {
            break;
        }
        seen.push(client.next_message().await);
    }
    assert!(list(&mut client, 12, "deleted").await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    let (new, _) = rpc(&mut client, 13, "thread/queue/add", add("deleted", "input")).await;
    assert_eq!(
        new["replayed"], false,
        "delete clears idempotency receipts as well as pending rows"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_consumed_add_replays_but_explicit_restore_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, _, _) = blocking_fixture(root.path()).await;
    let mut original = add(&thread, "consumed");
    original["mutationId"] = "consumed-add".into();
    original["mutationFingerprint"] = "consumed-fingerprint".into();
    let (added, _) = rpc(&mut client, 10, "thread/queue/add", original.clone()).await;
    let (started, _) = rpc(
        &mut client,
        11,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":added["queuedSubmission"]["id"]}),
    )
    .await;
    assert_eq!(started["status"], "started");
    let (replayed, _) = rpc(&mut client, 12, "thread/queue/add", original.clone()).await;
    assert_eq!(replayed["replayed"], true);
    assert_eq!(replayed["receipt"]["kind"], "started");
    let mut restore = original;
    restore["restore"] = true.into();
    restore["requireRemovedReceipt"] = true.into();
    client
        .send(json!({"id":13,"method":"thread/queue/add","params":restore}))
        .await;
    let (rejected, _) = client.response_for(13).await;
    assert_eq!(rejected["error"]["code"], -32602);
    assert!(list(&mut client, 14, &thread).await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_delete_receipt_alias_restore_proof_and_discard_after_reload() {
    let root = tempfile::tempdir().unwrap();
    let factory = make_create_session(&[]);
    let mut client = client_at(root.path(), factory.clone());
    client.initialize().await;
    let original = add("undo", "stable-client");
    let (added, _) = rpc(&mut client, 10, "thread/queue/add", original.clone()).await;
    let deletion = json!({"threadId":"undo","queuedSubmissionId":added["queuedSubmission"]["id"],"mutationId":"remove-once","mutationFingerprint":"fingerprint","expectedRevision":1});
    rpc(&mut client, 11, "thread/queue/delete", deletion.clone()).await;
    client.close().await;
    let mut reopened = client_at(root.path(), factory);
    reopened.initialize().await;
    let mut replay = deletion.clone();
    replay["queuedSubmissionId"] = "stable-client".into();
    let (removed, _) = rpc(&mut reopened, 20, "thread/queue/delete", replay.clone()).await;
    assert_eq!(removed["replayed"], true);
    assert_eq!(removed["revision"], 2);
    replay["queuedSubmissionId"] = "wrong-client".into();
    reopened
        .send(json!({"id":21,"method":"thread/queue/delete","params":replay}))
        .await;
    assert_eq!(reopened.response_for(21).await.0["error"]["code"], -32602);
    let mut restore = original.clone();
    restore["restore"] = true.into();
    reopened
        .send(json!({"id":22,"method":"thread/queue/add","params":restore}))
        .await;
    assert_eq!(reopened.response_for(22).await.0["error"]["code"], -32602);
    let mut changed_payload = original.clone();
    changed_payload["restore"] = true.into();
    changed_payload["requireRemovedReceipt"] = true.into();
    changed_payload["clientMetadata"]["item"]["text"] = "tampered".into();
    reopened
        .send(json!({"id":23,"method":"thread/queue/add","params":changed_payload}))
        .await;
    assert_eq!(reopened.response_for(23).await.0["error"]["code"], -32602);
    let mut restore = original.clone();
    restore["restore"] = true.into();
    restore["requireRemovedReceipt"] = true.into();
    restore["input"] = json!([]);
    restore["clientMetadata"]["legacyUnprepared"] = true.into();
    let (restored, _) = rpc(&mut reopened, 24, "thread/queue/add", restore).await;
    assert_eq!(restored["queuedSubmission"]["input"], original["input"]);
    assert_eq!(
        restored["queuedSubmission"]["clientMetadata"],
        original["clientMetadata"]
    );
    assert_eq!(
        restored["queuedSubmission"]["id"],
        added["queuedSubmission"]["id"]
    );
    rpc(&mut reopened,25,"thread/queue/update",json!({"threadId":"undo","queuedSubmissionId":restored["queuedSubmission"]["id"],"pausedReason":"paused"})).await;
    rpc(&mut reopened,26,"thread/queue/update",json!({"threadId":"undo","queuedSubmissionId":restored["queuedSubmission"]["id"],"pausedReason":null})).await;
    assert!(list(&mut reopened, 27, "undo").await["data"][0]
        .get("pausedReason")
        .is_none());
    rpc(
        &mut reopened,
        28,
        "thread/queue/discard",
        json!({"threadId":"undo"}),
    )
    .await;
    assert!(list(&mut reopened, 29, "undo").await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    let (new, _) = rpc(&mut reopened, 30, "thread/queue/add", original).await;
    assert_eq!(new["replayed"], false);
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_portable_image_bytes_survive_source_deletion_undo_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("image.png");
    let image = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==";
    std::fs::write(&source, "temporary source is no longer required").unwrap();
    let parts = json!([{"type":"text","text":"image input"},{"type":"image","base64":image,"mimeType":"image/png","sourcePath":source}]);
    let metadata =
        json!({"item":{"text":"image input","images":[{"path":source}]},"prepared":true});
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let (added,_) = rpc(&mut client,10,"thread/queue/add",json!({"threadId":"portable","clientUserMessageId":"image","input":parts,"clientMetadata":metadata,"createdAt":123})).await;
    std::fs::remove_file(&source).unwrap();
    rpc(
        &mut client,
        11,
        "thread/queue/delete",
        json!({"threadId":"portable","queuedSubmissionId":added["queuedSubmission"]["id"]}),
    )
    .await;
    let (restored,_) = rpc(&mut client,12,"thread/queue/add",json!({"threadId":"portable","clientUserMessageId":"image","input":[],"clientMetadata":{"item":metadata["item"],"legacyUnprepared":true},"createdAt":123,"restore":true,"requireRemovedReceipt":true})).await;
    assert_eq!(restored["queuedSubmission"]["input"], parts);
    assert_eq!(restored["queuedSubmission"]["clientMetadata"], metadata);
    client.close().await;
    let mut reopened = client_at(root.path(), make_create_session(&["portable accepted"]));
    reopened.initialize().await;
    let saved = list(&mut reopened, 20, "portable").await;
    assert_eq!(saved["data"][0]["input"], parts);
    rpc(
        &mut reopened,
        21,
        "session/start",
        json!({"init":{"threadId":"portable","cwd":root.path()}}),
    )
    .await;
    let (started, _) = rpc(
        &mut reopened,
        22,
        "thread/queue/start",
        json!({"threadId":"portable","queuedSubmissionId":restored["queuedSubmission"]["id"]}),
    )
    .await;
    assert_eq!(started["status"], "started", "{started}");
    assert!(!source.exists());
    assert!(list(&mut reopened, 23, "portable").await["data"]
        .as_array()
        .unwrap()
        .is_empty());
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_redacted_interrupt_undo_restores_pause_for_native_and_legacy_receipts() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let mut original = add("redacted", "native");
    original["pausedReason"] = "Interrupted before the turn ended.".into();
    let (row, _) = rpc(&mut client, 10, "thread/queue/add", original.clone()).await;
    rpc(
        &mut client,
        11,
        "thread/queue/delete",
        json!({"threadId":"redacted","queuedSubmissionId":row["queuedSubmission"]["id"]}),
    )
    .await;
    original.as_object_mut().unwrap().remove("pausedReason");
    original["restore"] = true.into();
    original["requireRemovedReceipt"] = true.into();
    let (restored, _) = rpc(&mut client, 12, "thread/queue/add", original).await;
    assert_eq!(
        restored["queuedSubmission"]["pausedReason"],
        "Interrupted before the turn ended."
    );
    rpc(&mut client,13,"thread/queue/import",json!({"threadId":"redacted","migrationId":"legacy-interrupt","minimumRevision":0,"queuedSubmissions":[],
        "receipts":[{"kind":"removal","mutationId":"legacy-delete","fingerprint":"old","createdAt":receipt_created_at(),"result":{"removed":{"id":"legacy","item":{"text":"legacy"},"createdAt":123,"pausedReason":"Interrupted before the turn ended."}}}]})).await;
    let (legacy,_) = rpc(&mut client,14,"thread/queue/add",json!({"threadId":"redacted","clientUserMessageId":"legacy","input":[{"type":"text","text":"legacy"}],
        "clientMetadata":{"item":{"text":"legacy"}},"createdAt":123,"restore":true,"requireRemovedReceipt":true})).await;
    assert_eq!(
        legacy["queuedSubmission"]["pausedReason"],
        "Interrupted before the turn ended."
    );
    assert_eq!(
        legacy["queuedSubmission"]["clientMetadata"]["legacyUnprepared"],
        true
    );
    client.close().await;
    let mut reopened = client_at(root.path(), make_create_session(&[]));
    reopened.initialize().await;
    for row in list(&mut reopened, 20, "redacted").await["data"]
        .as_array()
        .unwrap()
    {
        assert_eq!(row["pausedReason"], "Interrupted before the turn ended.");
    }
    let (paused, _) = rpc(
        &mut reopened,
        21,
        "thread/queue/start",
        json!({"threadId":"redacted"}),
    )
    .await;
    assert_eq!(paused["reason"]["code"], "queue-paused");
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_start_ack_then_immediate_interrupt_keeps_reader_order() {
    let root = tempfile::tempdir().unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(0));
    let factory = make_create_session(&["must be interrupted"]);
    let gate = permit.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        let gate = gate.clone();
        Box::pin(async move {
            gate.acquire().await.unwrap().forget();
            factory(ctx).await
        })
    });
    let mut client = client_at(root.path(), create);
    client.initialize().await;
    rpc(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"reader-order"}}),
    )
    .await;
    rpc(
        &mut client,
        10,
        "thread/queue/add",
        add("reader-order", "future"),
    )
    .await;
    rpc(&mut client,11,"turn/start",json!({"sessionId":"reader-order","input":"blocked core creation","submissionId":"raw-owner"})).await;
    let (_,seen) = rpc(&mut client,12,"turn/interrupt",json!({"sessionId":"reader-order","submissionId":"immediate-stop","origin":"targeted-turn","expectedTurnId":"raw-owner"})).await;
    let stopped = interrupt_result(&mut client, "immediate-stop", seen).await;
    assert_eq!(stopped["turnInterrupt"]["accepted"], true, "{stopped}");
    assert_eq!(
        list(&mut client, 13, "reader-order").await["data"][0]["pausedReason"],
        "Interrupted before the turn ended."
    );
    permit.add_permits(1);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_interrupt_does_not_take_queue_lock_before_reader_admission() {
    let root = tempfile::tempdir().unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let factory = make_create_session(&["queued accepted"]);
    let gate = permit.clone();
    let signal = entered.clone();
    let create: CreateSession = Arc::new(move |ctx| {
        let factory = factory.clone();
        let gate = gate.clone();
        let signal = signal.clone();
        Box::pin(async move {
            signal.notify_one();
            gate.acquire().await.unwrap().forget();
            factory(ctx).await
        })
    });
    let mut client = client_at(root.path(), create);
    client.initialize().await;
    rpc(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"lock-order"}}),
    )
    .await;
    rpc(
        &mut client,
        10,
        "thread/queue/add",
        add("lock-order", "future"),
    )
    .await;
    client.send(json!({"id":11,"method":"thread/queue/start","params":{"threadId":"lock-order","submissionId":"preparing"}})).await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    let (_,seen) = rpc(&mut client,12,"turn/interrupt",json!({"sessionId":"lock-order","submissionId":"stale-during-prepare","origin":"targeted-turn","expectedTurnId":"not-an-owner"})).await;
    let rejected = interrupt_result(&mut client, "stale-during-prepare", seen).await;
    assert_eq!(rejected["turnInterrupt"]["accepted"], false);
    let snapshot = list(&mut client, 13, "lock-order").await;
    assert!(snapshot["data"][0]["dispatching"].is_object());
    permit.add_permits(1);
    let (started, _) = client.response_for(11).await;
    assert_eq!(started["result"]["status"], "started", "{started}");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_lookup_only_recovers_consumed_receipt_without_preparing_missing_media() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, thread, _, _) = blocking_fixture(root.path()).await;
    let source = root.path().join("source.bin");
    std::fs::write(&source, "retained payload").unwrap();
    let mut original = add(&thread, "lookup-client");
    original["mutationId"] = "lookup-add".into();
    original["mutationFingerprint"] = "lookup-fingerprint".into();
    original["input"] = json!([{"type":"text","text":"lookup-client"},{"type":"image","base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==","mimeType":"image/png","sourcePath":source}]);
    let (added, _) = rpc(&mut client, 10, "thread/queue/add", original).await;
    rpc(
        &mut client,
        11,
        "thread/queue/start",
        json!({"threadId":thread,"queuedSubmissionId":added["queuedSubmission"]["id"]}),
    )
    .await;
    std::fs::remove_file(&source).unwrap();
    let before = list(&mut client, 12, &thread).await;
    let lookup = json!({"threadId":thread,"clientUserMessageId":"lookup-client","mutationId":"lookup-add","mutationFingerprint":"lookup-fingerprint","lookupOnly":true});
    let (found, events) = rpc(&mut client, 13, "thread/queue/add", lookup.clone()).await;
    assert_eq!(found["found"], true);
    assert_eq!(found["receipt"]["kind"], "started");
    assert_eq!(found["revision"], before["revision"]);
    assert!(events
        .iter()
        .all(|event| event["method"] != "thread/queue/changed"));
    assert_eq!(list(&mut client, 14, &thread).await, before);
    let mut conflict = lookup;
    conflict["clientUserMessageId"] = "wrong-client".into();
    client
        .send(json!({"id":15,"method":"thread/queue/add","params":conflict}))
        .await;
    assert_eq!(client.response_for(15).await.0["error"]["code"], -32602);
    let (missing, _) = rpc(
        &mut client,
        16,
        "thread/queue/add",
        json!({"threadId":"absent","clientUserMessageId":"absent","lookupOnly":true}),
    )
    .await;
    assert_eq!(missing["found"], false);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_wide_mutations_preserve_untouched_versions_and_prepared_input() {
    let root = tempfile::tempdir().unwrap();
    let mut client = client_at(root.path(), make_create_session(&[]));
    client.initialize().await;
    let entries: Vec<Value> = (0..64).map(|index| json!({
        "clientUserMessageId":format!("client-{index}"),
        "input":[{"type":"text","text":format!("prepared-{index}")}],
        "clientMetadata":{"item":{"text":format!("display-{index}"),"files":["/retained/input.pdf"]}},
        "createdAt":123
    })).collect();
    rpc(
        &mut client,
        1001,
        "thread/queue/import",
        json!({
            "threadId":"wide-versions", "migrationId":"seed", "minimumRevision":0, "queuedSubmissions":entries
        }),
    )
    .await;
    let before = list(&mut client, 1002, "wide-versions").await;
    let original = before["data"].as_array().unwrap();
    assert_eq!(original.len(), 64);
    assert!(original.iter().all(|row| row["rowVersion"] == 1));
    rpc(&mut client, 1003, "thread/queue/delete", json!({
        "threadId":"wide-versions", "queuedSubmissionId":original[63]["id"], "expectedRevision":before["revision"], "includeInput":false
    })).await;
    let tail_removed = list(&mut client, 1004, "wide-versions").await;
    assert_eq!(tail_removed["data"], json!(original[..63]));
    let mut appended = add("wide-versions", "new-tail");
    appended["includeInput"] = false.into();
    rpc(&mut client, 1005, "thread/queue/add", appended).await;
    let expanded = list(&mut client, 1006, "wide-versions").await;
    let prior = expanded["data"].as_array().unwrap();
    assert_eq!(prior.len(), 64);
    assert_eq!(&prior[..63], &original[..63]);
    rpc(&mut client, 1007, "thread/queue/delete", json!({
        "threadId":"wide-versions", "queuedSubmissionId":original[0]["id"], "expectedRevision":expanded["revision"], "includeInput":false
    })).await;
    let shifted = list(&mut client, 1008, "wide-versions").await;
    let rows = shifted["data"].as_array().unwrap();
    assert_eq!(rows.len(), 63);
    for (row, earlier) in rows.iter().zip(prior.iter().skip(1)) {
        assert_eq!(row["id"], earlier["id"]);
        assert_eq!(
            row["rowVersion"].as_u64().unwrap(),
            earlier["rowVersion"].as_u64().unwrap() + 1
        );
        assert_eq!(row["inputFingerprint"], earlier["inputFingerprint"]);
        assert_eq!(row["input"], earlier["input"]);
        assert_eq!(row["clientMetadata"], earlier["clientMetadata"]);
    }
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_queue_endpoint_close_does_not_close_another_endpoints_store() {
    let root = tempfile::tempdir().unwrap();
    let factory = make_create_session(&[]);
    let mut first = client_at(root.path(), factory.clone());
    let mut second = client_at(root.path(), factory.clone());
    first.initialize().await;
    second.initialize().await;
    rpc(
        &mut first,
        901,
        "thread/queue/add",
        add("shared-lifetime", "first"),
    )
    .await;
    assert_eq!(
        list(&mut second, 902, "shared-lifetime").await["revision"],
        1
    );
    first.close().await;
    rpc(
        &mut second,
        903,
        "thread/queue/add",
        add("shared-lifetime", "second"),
    )
    .await;
    let mut reopened = client_at(root.path(), factory);
    reopened.initialize().await;
    let snapshot = list(&mut reopened, 904, "shared-lifetime").await;
    assert_eq!(snapshot["revision"], 2);
    assert_eq!(snapshot["data"].as_array().unwrap().len(), 2);
    second.close().await;
    rpc(
        &mut reopened,
        905,
        "thread/queue/add",
        add("shared-lifetime", "third"),
    )
    .await;
    assert_eq!(
        list(&mut reopened, 906, "shared-lifetime").await["revision"],
        3
    );
    reopened.close().await;
}
