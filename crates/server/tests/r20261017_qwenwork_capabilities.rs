//! Named native operations share the endpoint's credential and cancellation boundary.
use super::*;
use crate::app_server::qwenwork;
use crate::app_server::qwenwork::capabilities::tests::{fixture, setup, Reply};

#[tokio::test]
async fn qwenwork_named_capabilities_image_references_search_and_cancel_use_the_owned_account() {
    let _guard = qwenwork::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let native = fixture().await;
    let _setup = setup(&native, root.path());
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.path().join("rules.json")),
        Some(config),
    );
    client.initialize().await;
    client
        .send(json!({"id":901,"method":"qwenwork/capabilities","params":{}}))
        .await;
    let caps = client.response_for(901).await.0;
    assert_eq!(caps["result"]["imageEdit"]["available"], true);
    assert_eq!(caps["result"]["music"]["defaultModel"], "minimax-music-2.5");
    assert_eq!(caps["result"]["fetch"]["available"], false);
    client.send(json!({"id":902,"method":"qwenwork/generateImage","params":{"prompt":"A green circle","referenceImages":[{"mimeType":"image/png","dataBase64":base64::Engine::encode(&base64::engine::general_purpose::STANDARD,b"\x89PNG\r\n\x1a\nfixture-reference")}],"operationId":"image-one"}})).await;
    let image = client.response_for(902).await.0;
    assert_eq!(image["result"]["mimeType"], "image/png", "{image}");
    assert!(native
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|(path, body)| path.ends_with("/edit")
            && body["images"]
                .as_array()
                .is_some_and(|images| images.len() == 1)));
    client.send(json!({"id":903,"method":"qwenwork/search","params":{"query":"IANA domains","operationId":"search-one"}})).await;
    let search = client.response_for(903).await.0;
    assert_eq!(
        search["result"]["results"][0]["url"],
        "https://www.iana.org/domains/reserved"
    );
    client.send(json!({"id":907,"method":"qwenwork/generateVideo","params":{"prompt":"A moving circle","seconds":4,"size":"720p"}})).await;
    assert_eq!(
        client.response_for(907).await.0["result"]["mimeType"],
        "video/mp4"
    );
    client.send(json!({"id":908,"method":"qwenwork/generateMusic","params":{"prompt":"A quiet piano melody"}})).await;
    assert_eq!(
        client.response_for(908).await.0["result"]["mimeType"],
        "audio/mpeg"
    );
    client.send(json!({"id":909,"method":"qwenwork/transcribeAudio","params":{"audioBase64":base64::Engine::encode(&base64::engine::general_purpose::STANDARD,[0,0,1,0]),"mimeType":"audio/pcm"}})).await;
    assert_eq!(
        client.response_for(909).await.0["result"]["text"],
        "First A green circle"
    );
    client.send(json!({"id":910,"method":"qwenwork/cancelTranscription","params":{"operationId":"absent-transcription"}})).await;
    assert_eq!(
        client.response_for(910).await.0["result"]["cancelled"],
        false
    );
    *native.reply.lock().unwrap() = Reply::Hang;
    client.send(json!({"id":904,"method":"qwenwork/generateVideo","params":{"prompt":"A moving circle","seconds":4,"operationId":"cancel-before-task"}})).await;
    client.send(json!({"id":905,"method":"qwenwork/cancelOperation","params":{"operationId":"cancel-before-task"}})).await;
    let (cancel, early) = client.response_for(905).await;
    assert_eq!(cancel["result"]["cancelled"], true);
    let cancelled = match early.into_iter().find(|event| event["id"] == 904) {
        Some(response) => response,
        None => client.response_for(904).await.0,
    };
    assert_eq!(
        cancelled["error"]["message"],
        "provider_operation_cancelled"
    );
    client.send(json!({"id":906,"method":"qwenwork/generateMusic","params":{"prompt":"A quiet song","instrumental":true}})).await;
    assert!(client.response_for(906).await.0.get("error").is_some());
    client.writer.shutdown().await.unwrap();
    client.close().await;
}
