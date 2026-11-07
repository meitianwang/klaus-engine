use super::*;

#[tokio::test]
async fn preparation_waits_for_selected_core_publication_and_reuses_parallel_recovery() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let base = make_create_session(&[]);
    let create: CreateSession = {
        let calls = calls.clone();
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |context| {
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            let base = base.clone();
            Box::pin(async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                base(context).await
            })
        })
    };
    let mut client = TestClient::start(create);
    client.initialize().await;
    client
        .send(
            json!({"id": 2, "method": "session/start", "params": {"init": {
                "agentDefinitionName": "restored-root",
                "agents": [{"name": "restored-root", "description": "fixture", "tools": []}]
            }}}),
        )
        .await;
    let (started, _) = client.response_for(2).await;
    let session_id = started["result"]["sessionId"].clone();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    client
        .send(json!({"id": 3, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    client
        .send(json!({"id": 4, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    let premature = tokio::time::timeout(Duration::from_millis(30), client.lines.next_line()).await;
    assert!(
        premature.is_err(),
        "readiness must not precede restoration: {premature:?}"
    );
    release.notify_one();
    let mut receipts = std::collections::BTreeSet::new();
    while receipts.len() < 2 {
        let message = client.next_message().await;
        if matches!(message["id"].as_u64(), Some(3 | 4)) {
            assert_eq!(
                message["result"],
                json!({"sessionId": session_id, "ready": true})
            );
            receipts.insert(message["id"].as_u64().unwrap());
        }
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    client
        .send(json!({"id": 5, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    assert_eq!(client.response_for(5).await.0["result"]["ready"], true);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test]
async fn failed_preparation_retains_the_endpoint_for_a_new_attempt() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let base = make_create_session(&[]);
    let create: CreateSession = {
        let calls = calls.clone();
        Arc::new(move |context| {
            let calls = calls.clone();
            let base = base.clone();
            Box::pin(async move {
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Err("restoration fixture failed".into());
                }
                base(context).await
            })
        })
    };
    let mut client = TestClient::start(create);
    client.initialize().await;
    client
        .send(json!({"id": 2, "method": "session/start", "params": {"init": {}}}))
        .await;
    let session_id = client.response_for(2).await.0["result"]["sessionId"].clone();
    client
        .send(json!({"id": 3, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    assert_eq!(
        client.response_for(3).await.0["error"]["message"],
        "restoration fixture failed"
    );
    client
        .send(json!({"id": 4, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    assert_eq!(client.response_for(4).await.0["result"]["ready"], true);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    client.close().await;
}
