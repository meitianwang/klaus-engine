//! Defect: native checkpoints could retain images far beyond their history budget.
//! Root cause: retention charged text but treated an entire image message as one token.
//! Fix: e1e0ea02; retain media and text under the same history budget.

use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, RemoteCompactionSupport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn native_checkpoints_charge_retained_images_with_the_shared_history_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (header_end, content_length) = loop {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_end + content_length {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
        }
        let body = runtime_json::parse(&RuntimeString::from(
            String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap(),
        ))
        .unwrap();
        let response = "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"checkpoint\",\"status\":\"completed\"}}\n\n";
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        body
    });
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture-key".into()),
        api: Some(OpenAiApi::Responses),
        remote_compaction: RemoteCompactionSupport::ResponsesV2,
        ..Default::default()
    });
    let mut request: ModelRequest = runtime_codec::decode_value(
        &runtime_json::parse(&RuntimeString::from(
            r#"{"model":"selected","system":[],"messages":[{"type":"user","uuid":"images","content":[]}],"tools":[],"maxOutputTokens":1024}"#,
        ))
        .unwrap(),
    )
    .unwrap();
    let Message::User(user) = &mut request.messages[0] else {
        unreachable!();
    };
    user.content = (0..256)
        .map(|_| {
            ContentBlock::Image(ImageBlock {
                detail: None,
                mime_type: "image/png".into(),
                base64: "QQ==".into(),
                source_path: None,
            })
        })
        .collect();
    let result = provider
        .compact_remote(request, AbortSignal::new())
        .await
        .unwrap();
    let sent = server.await.unwrap();
    assert_eq!(sent["input"][0]["content"].as_array().unwrap().len(), 256);
    let retained_images = result
        .checkpoint
        .retained_input
        .iter()
        .filter_map(|item| item.get("content").and_then(|content| content.as_array()))
        .flatten()
        .filter(|block| {
            block["type"]
                .as_str()
                .is_some_and(|kind| kind == "input_image")
        })
        .count();
    let retained_tokens = result.checkpoint.estimated_context_tokens();
    println!(
        "native retention measurement: input_images=256 retained_images={retained_images} checkpoint_context_tokens={retained_tokens} history_budget_tokens=64000"
    );
    assert!(
        retained_images > 0,
        "The newest image boundary must retain content that fits"
    );
    assert!(
        retained_tokens <= 64_001,
        "Retained media must share the history budget: {retained_tokens}"
    );
}
