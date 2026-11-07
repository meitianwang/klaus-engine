//! Standalone text calls must use configured source identity without creating a chat.
use super::*;
use klaus_agent_core::credential_store::{model_credential_key, CredentialStore};
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn endpoint(
    stall: bool,
) -> (
    String,
    tokio::sync::oneshot::Receiver<Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let (header_size, body_size) = loop {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&bytes[..end]);
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_size + body_size {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        assert!(String::from_utf8_lossy(&bytes[..header_size]).contains("Bearer sk-fixture"));
        let body = serde_json::from_slice(&bytes[header_size..header_size + body_size]).unwrap();
        let _ = sender.send(body);
        let first = json!({"id":"plain-one","object":"chat.completion.chunk","model":"fixture-low","choices":[{"index":0,"delta":{"role":"assistant","content":"First sentence. Final sentence."},"finish_reason":null}]});
        let end = json!({"id":"plain-one","object":"chat.completion.chunk","model":"fixture-low","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
        let body = format!("data: {first}\n\ndata: {end}\n\ndata: [DONE]\n\n");
        if stall {
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
            socket
                .write_all(format!("data: {first}\n\n").as_bytes())
                .await
                .unwrap();
            let _ = socket.read(&mut chunk).await;
        } else {
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    (format!("http://{address}/v1"), receiver, task)
}

async fn client(base: &str) -> (TestClient, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("klaus-text-facade-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(&path,format!("[modelProviders.fixture]\nname=\"Low\"\nprovider=\"openai\"\nmodel=\"fixture-low\"\nbaseUrl=\"{base}\"\nopenaiApi=\"chat\"\nmaxContextTokens=32768\n")).unwrap();
    let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("fixture"), "sk-fixture")
        .await
        .unwrap();
    let create: CreateSession =
        Arc::new(|_| Box::pin(async { panic!("standalone inference must not create a session") }));
    let mut client = TestClient::start_full_with_credentials(create, Some(path), Some(store));
    client.initialize().await;
    (client, dir)
}
fn request(id: u64) -> Value {
    json!({"id":id,"method":"engine/completeText","params":{"operationId":"text-one","source":"custom","modelId":"fixture","system":"Return only text.","prompt":"First sentence. Final sentence.","maxOutputTokens":2048}})
}

#[tokio::test]
async fn named_text_facade_uses_selected_provider_and_returns_complete_text_without_a_session() {
    let (base, body, server) = endpoint(false).await;
    let (mut client, dir) = client(&base).await;
    client.send(request(2)).await;
    let (reply, _) = client.response_for(2).await;
    assert!(reply.get("error").is_none(), "{reply}");
    assert_eq!(
        reply["result"],
        json!({"text":"First sentence. Final sentence.","source":"custom","modelId":"fixture"})
    );
    let body = body.await.unwrap();
    assert_eq!(body["model"], "fixture-low");
    assert_eq!(body["messages"][0]["role"], "system");
    assert!(body
        .get("tools")
        .is_none_or(|tools| tools.as_array().is_some_and(Vec::is_empty)));
    client.close().await;
    server.await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn named_text_cancellation_rejects_a_partial_answer_and_drains_its_admission() {
    let (base, body, server) = endpoint(true).await;
    let (mut client, dir) = client(&base).await;
    client.send(request(2)).await;
    tokio::select! {
        body = body => { body.unwrap(); }
        reply = client.response_for(2) => panic!("provider was not reached: {:?}", reply.0),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("provider was not reached in time"),
    }
    client.send(json!({"id":3,"method":"engine/cancelTextCompletion","params":{"operationId":"text-one"}})).await;
    let (reply, others) = client.response_for(3).await;
    assert_eq!(reply["result"]["cancelled"], true);
    let terminal = if let Some(found) = others.into_iter().find(|entry| entry["id"] == 2) {
        found
    } else {
        client.response_for(2).await.0
    };
    assert!(terminal.get("result").is_none(), "{terminal}");
    assert!(terminal["error"]["message"]
        .as_str()
        .unwrap()
        .contains("cancelled"));
    client.close().await;
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn named_text_facade_rejects_unknown_auth_fields_and_unavailable_models() {
    let (mut client, dir) = client("http://127.0.0.1:1/v1").await;
    let mut invalid = request(2);
    invalid["params"]["auth"] = json!({"apiKey":"no"});
    client.send(invalid).await;
    let (reply, _) = client.response_for(2).await;
    assert_eq!(reply["error"]["code"], -32602);
    let mut invalid = request(3);
    invalid["params"]["modelId"] = "not-configured".into();
    client.send(invalid).await;
    let (reply, _) = client.response_for(3).await;
    assert!(reply["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unavailable"));
    client.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}
