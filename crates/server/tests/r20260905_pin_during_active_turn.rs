//! Defect: sidebar pinning timed out while the model was still running.
//! Root cause: host metadata updates shared the serial model-turn queue.
//! Fixed by: 2026-09-05 sidebar pinning repair.
//! Convention note: this public-facade regression belongs to the server crate;
//! mounting it in core would invert the dependency graph. It uses the existing
//! app-server test harness and submits only named JSON-RPC methods.

use super::*;
use klaus_agent_core::schemas::rollout::SessionMeta;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

async fn update_metadata(client: &mut TestClient, id: u64, session_id: Value, metadata: Value) {
    let submission = format!("pin-{id}");
    let mut observed = Vec::new();
    client.send(json!({
        "id": id,
        "method": "session/updateThreadHostMetadata",
        "params": { "sessionId": session_id, "threadId": "thread-pin", "hostMetadata": metadata, "submissionId": submission }
    })).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        let (reply, mut messages) = client.response_for(id).await;
        observed.push(reply.clone());
        assert_eq!(reply["result"]["submissionId"], submission);
        loop {
            for message in messages.drain(..) {
                observed.push(message.clone());
                let params = &message["params"];
                let event = params.get("event").unwrap_or(params);
                if event["submissionId"] == submission {
                    assert!(event.get("error").is_none(), "{event}");
                    assert_eq!(event["hostMetadataUpdated"]["updated"], true);
                    return;
                }
            }
            messages.push(client.next_message().await);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("{submission} must settle before the blocked turn ends: {observed:?}")
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pin_and_unpin_settle_during_a_turn_and_persist_without_reactivation() {
    let dir = std::env::temp_dir().join(format!("klaus-pin-facade-{}", uuid::Uuid::new_v4()));
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let live = store
        .create(SessionMeta {
            id: "thread-pin".into(),
            created_at: 1_700_000_000,
            cwd: "/tmp".into(),
            model: "m".into(),
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
        .await
        .unwrap();
    live.shutdown().await.unwrap();
    let started = Arc::new(Notify::new());
    let started_model = started.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_model = cancelled.clone();
    let agent = make_agent_with_call_model(Box::new(move |_, signal| {
        started_model.notify_one();
        let cancelled = cancelled_model.clone();
        Box::pin(futures::stream::once(async move {
            signal.cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            ev::stop(Some(ModelStopReason::EndTurn), None)
        }))
    }));
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let resume_store = store.clone();
    let create: CreateSession = Arc::new(move |ctx: SessionContext| {
        let agent = agent.clone();
        let store = resume_store.clone();
        Box::pin(async move {
            agent
                .resume_session(
                    store,
                    &ctx.resume.expect("resume thread"),
                    StartSessionOptions {
                        approval_handler: Some(ctx.approval_handler),
                        elicit_handler: Some(ctx.elicit_handler),
                        mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| Box::new(error) as BoxError)
        })
    });
    let mut client = TestClient::start_full(
        create,
        Some(lifecycle),
        Some(dir.join("permission-rules.json")),
        Some(dir.join("config.json")),
    );
    client.send(json!({ "id": 1, "method": "initialize", "params": { "capabilities": { "productEvents": true, "optOutNotificationMethods": ["session/event"] } } })).await;
    assert!(client.response_for(1).await.0.get("result").is_some());
    client.send(json!({ "method": "initialized" })).await;
    client.send(json!({ "id": 2, "method": "session/start", "params": { "init": {}, "resume": "thread-pin" } })).await;
    let (reply, _) = client.response_for(2).await;
    let session_id = reply["result"]["sessionId"].clone();
    assert!(session_id.is_string(), "{reply}");
    client.send(json!({
        "id": 3, "method": "turn/start",
        "params": { "sessionId": session_id, "input": [{ "type": "text", "text": "keep running" }] }
    })).await;
    let (reply, _) = client.response_for(3).await;
    assert!(reply.get("error").is_none(), "{reply}");
    tokio::time::timeout(REPLY_TIMEOUT, started.notified())
        .await
        .unwrap();

    update_metadata(
        &mut client,
        4,
        session_id.clone(),
        json!({ "sidebarPinnedAt": 1700000000123_i64, "custom": "keep" }),
    )
    .await;
    assert!(!cancelled.load(Ordering::SeqCst));
    let pinned = store.list(ListOptions::default()).await.unwrap().items[0]
        .host_metadata
        .clone();
    assert!(pinned.is_some());
    update_metadata(
        &mut client,
        5,
        session_id.clone(),
        json!({ "custom": "keep" }),
    )
    .await;
    assert!(!cancelled.load(Ordering::SeqCst));
    assert_eq!(
        store.list(ListOptions::default()).await.unwrap().items[0].host_metadata,
        Some(
            klaus_agent_core::runtime_values::runtime_json::parse(&"{\"custom\":\"keep\"}".into())
                .unwrap()
        )
    );
    client
        .send(json!({ "id": 6, "method": "session/close", "params": { "sessionId": session_id } }))
        .await;
    let (reply, _) = client.response_for(6).await;
    assert!(reply.get("error").is_none(), "{reply}");
    update_metadata(
        &mut client,
        7,
        Value::Null,
        json!({ "sidebarPinnedAt": 1700000000123_i64, "custom": "keep" }),
    )
    .await;
    client.close().await;

    let reopened = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    });
    assert_eq!(
        reopened.list(ListOptions::default()).await.unwrap().items[0].host_metadata,
        pinned
    );
    std::fs::remove_dir_all(dir).unwrap();
}
