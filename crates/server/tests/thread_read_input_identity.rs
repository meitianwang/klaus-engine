//! Reconnecting clients recover submitted identity from exact presentation records.
//! The turn's user UUID, not text or runtime state, owns its user_display envelope.
//! Contract: the read-only facade preserves the accepted opener's display identity.
use super::thread_read_facade::{
    fixture_header, fixture_turn, read, read_only_client, store, write_fixture,
};
use super::*;
use futures::StreamExt;
use klaus_agent_core::schemas::rollout::SessionMeta;
use klaus_agent_core::store::rollout::parse_rollout_line;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

fn display(uuid: &str, owner: &str, metadata: Value) -> Value {
    json!({"kind":"message","message":{"type":"system","uuid":uuid,"subtype":"user_display",
        "text":json!({"messageUuid":owner,"localRender":metadata}).to_string()}})
}

fn opener_display(turn: &Value) -> Option<Value> {
    let items = turn["items"].as_array()?;
    let opener = items.iter().find(|item| item["type"] == "userMessage")?;
    items.iter().rev().find_map(|item| {
        let message = &item["message"];
        if item["type"] != "klausMessage" || message["subtype"] != "user_display" {
            return None;
        }
        let envelope: Value = serde_json::from_str(message["text"].as_str()?).ok()?;
        (envelope["messageUuid"] == opener["id"]).then_some(envelope)
    })
}

#[tokio::test]
async fn read_thread_recovers_only_the_exact_saved_opener_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut rows = vec![fixture_header("identity-cold")];
    let mut original = fixture_turn("original", "identical words");
    original.insert(
        2,
        display(
            "original-display",
            "original-user",
            json!({"clientMessageId":"client:original"}),
        ),
    );
    original.insert(
        3,
        json!({"kind":"message","message":{"type":"user","uuid":"steered-user",
        "content":[{"type":"text","text":"identical words"}]}}),
    );
    original.insert(
        4,
        display(
            "steered-display",
            "steered-user",
            json!({"clientMessageId":"steered-identity","steerMessageId":"steered-user"}),
        ),
    );
    original.insert(
        5,
        display(
            "unrelated-display",
            "another-user",
            json!({"clientMessageId":"client:unrelated"}),
        ),
    );
    rows.extend(original);
    let mut legacy = fixture_turn("legacy", "identical words");
    legacy.insert(
        2,
        display(
            "legacy-unrelated",
            "other-input",
            json!({"clientMessageId":"client:not-legacy"}),
        ),
    );
    rows.extend(legacy);
    let mut continuation = fixture_turn("continuation", "a steered first user is not an opener");
    continuation.insert(
        2,
        display(
            "continuation-steer",
            "continuation-user",
            json!({"clientMessageId":"client:steer-only","steerMessageId":"continuation-user"}),
        ),
    );
    rows.extend(continuation);
    let path = write_fixture(dir.path(), "identity-cold", &rows);
    let before = std::fs::read(&path).unwrap();
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(store(dir.path()), activations.clone());
    client.initialize().await;
    let reply = read(
        &mut client,
        10,
        json!({"threadId":"identity-cold","limit":3}),
    )
    .await;
    let turns = reply["result"]["turns"].as_array().unwrap();
    assert_eq!(
        opener_display(&turns[2]).unwrap()["localRender"]["clientMessageId"],
        "client:original",
        "{reply}"
    );
    assert!(opener_display(&turns[1]).is_none(), "{reply}");
    assert_eq!(
        opener_display(&turns[0]).unwrap()["localRender"]["steerMessageId"],
        "continuation-user",
        "{reply}"
    );
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(path).unwrap(), before);
    client.close().await;
}

#[tokio::test]
async fn read_thread_recovers_live_opener_identity_before_the_writer_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let source = store(dir.path());
    let meta: SessionMeta =
        match parse_rollout_line(&fixture_header("identity-live").to_string()).unwrap() {
            klaus_agent_core::schemas::rollout::RolloutItem::SessionMeta { meta, .. } => meta,
            _ => unreachable!(),
        };
    source.create(meta).await.unwrap().shutdown().await.unwrap();
    let entered = Arc::new(Notify::new());
    let notify = entered.clone();
    let agent = make_agent_with_call_model(Box::new(move |_, signal| {
        let notify = notify.clone();
        Box::pin(
            futures::stream::iter([ev::message_start(None), ev::text("working")]).chain(
                futures::stream::once(async move {
                    notify.notify_one();
                    signal.cancelled().await;
                    ev::stop(Some(ModelStopReason::EndTurn), None)
                }),
            ),
        )
    }));
    let mut lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(source.clone()));
    let header = source.read_thread("identity-live").await.unwrap();
    lifecycle.read_thread = Some(Arc::new(move |_| {
        let header = header.clone();
        Box::pin(async move { Ok(header) })
    }));
    let create: CreateSession = Arc::new(move |ctx| {
        let agent = agent.clone();
        let source = source.clone();
        Box::pin(async move {
            agent
                .resume_session(
                    source,
                    &ctx.resume.unwrap(),
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
    let mut client = TestClient::start_full(create, Some(lifecycle), None, None);
    client.initialize().await;
    client
        .send(
            json!({"id":2,"method":"session/start","params":{"init":{},"resume":"identity-live"}}),
        )
        .await;
    let session = client.response_for(2).await.0["result"]["sessionId"].clone();
    client
        .send(
            json!({"id":3,"method":"turn/start","params":{"sessionId":session,
        "submissionId":"identity-facade","input":[{"type":"text","text":"working input"}],
        "options":{"displayMetadata":{"clientMessageId":"submission:live-original"}}}}),
        )
        .await;
    client.response_for(3).await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    let reply = read(&mut client, 4, json!({"threadId":"identity-live"})).await;
    assert_eq!(
        reply["result"]["turns"][0]["status"], "inProgress",
        "{reply}"
    );
    assert_eq!(
        opener_display(&reply["result"]["turns"][0]).unwrap()["localRender"]["clientMessageId"],
        "submission:live-original",
        "{reply}"
    );
    client.close().await;
}
