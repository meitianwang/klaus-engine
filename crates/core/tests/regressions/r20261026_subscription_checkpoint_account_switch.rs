//! Defect: changing subscription accounts blocked an existing encrypted checkpoint.
//! Root cause: replay treated the checkpoint's historical account as a transport constraint.
//! Work item: 2026-10-26 subscription checkpoint replay with current request credentials.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, InferencePublication, Input, TurnOptions};
use klaus_agent_core::provider::codex_subscription::{
    codex_oauth_defaults, create_codex_subscription_provider, CodexSubscriptionOptions,
};
use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthTokens};
use klaus_agent_core::provider::openai::OpenAIProvider;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const MODEL: &str = "checkpoint-fixture";

fn tokens(account: &str) -> OAuthTokens {
    let payload = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        format!(r#"{{"https://api.openai.com/auth":{{"chatgpt_account_id":"{account}"}}}}"#),
    );
    OAuthTokens {
        access_token: format!("token-{account}").into(),
        id_token: Some(format!("e30.{payload}.signature").into()),
        expires_at: Some(4_000_000_000_000),
        ..Default::default()
    }
}

fn subscription(base: &str, account: &str) -> (OpenAIProvider, Arc<OAuthAuth>) {
    create_codex_subscription_provider(CodexSubscriptionOptions {
        oauth: codex_oauth_defaults("fixture-client", None),
        store: None,
        tokens: Some(tokens(account)),
        base_url: Some(base.into()),
        default_model: Some(MODEL.into()),
        access_policy: None,
        identity: None,
    })
}

fn request(messages: Vec<Message>) -> ModelRequest {
    let mut request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"model":"checkpoint-fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":1024}"#,
    ))
    .unwrap();
    request.messages = messages;
    request
}

fn initial_request() -> ModelRequest {
    request(vec![runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"user","uuid":"original-user","content":[{"type":"text","text":"Preserve this original requirement."}]}"#,
    ))
    .unwrap()])
}

fn compact_response(opaque: &str) -> String {
    format!("data: {{\"type\":\"response.output_item.done\",\"item\":{{\"type\":\"compaction\",\"encrypted_content\":\"{opaque}\"}}}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"compaction-response\",\"status\":\"completed\"}}}}\n\n")
}

fn normal_response() -> String {
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"The existing task continued.\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"next-response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":10}}}\n\n".into()
}

type Captured = Vec<(String, Value)>;

async fn fixture(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!(
        "http://{}/backend-api/codex",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        let mut captured = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
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
            while bytes.len() < header_end + length {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            captured.push((
                String::from_utf8_lossy(&bytes[..header_end]).into_owned(),
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
            ));
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        }
        captured
    });
    (base, task)
}

async fn checkpoint(provider: &OpenAIProvider) -> Message {
    let compacted = provider
        .compact_remote(initial_request(), AbortSignal::new())
        .await
        .unwrap();
    let boundary = Message::CompactBoundary(CompactBoundaryMessage {
        uuid: "persisted-boundary".into(),
        summary: "Display text must not replace opaque model context.".into(),
        native_compaction: Some(compacted.checkpoint),
    });
    runtime_codec::decode_text(&runtime_codec::encode_text(&boundary).unwrap()).unwrap()
}

fn assert_replay(captured: &(String, Value), account: &str, opaque: &str) {
    assert!(captured
        .0
        .starts_with("POST /backend-api/codex/responses HTTP/1.1"));
    assert!(captured
        .0
        .lines()
        .any(|line| line.eq_ignore_ascii_case(&format!("authorization: Bearer token-{account}"))));
    assert!(captured.1.get("previous_response_id").is_none());
    let input = captured.1["input"].as_array().unwrap();
    assert!(input
        .iter()
        .any(|item| item["type"] == "compaction" && item["encrypted_content"] == opaque));
    assert!(captured
        .1
        .to_string()
        .contains("Preserve this original requirement."));
    assert!(!captured
        .1
        .to_string()
        .contains("Display text must not replace"));
}

#[tokio::test]
async fn account_switch_replays_and_recompacts_a_persisted_subscription_checkpoint() {
    let (base, server) = fixture(vec![
        compact_response("opaque-original"),
        normal_response(),
        compact_response("opaque-new-account"),
    ])
    .await;
    let (provider, auth) = subscription(&base, "old-account");
    let boundary = checkpoint(&provider).await;
    let Message::CompactBoundary(saved) = &boundary else {
        unreachable!()
    };
    let original = saved.native_compaction.as_ref().unwrap();
    auth.set_tokens(tokens("new-account")).await.unwrap();
    assert!(provider
        .native_checkpoint_compatibility(original, &MODEL.into(), &AbortSignal::new())
        .await
        .unwrap());
    let original_scope = original.auth_scope.clone();
    let replay = request(vec![boundary]);
    let mut stream = provider.stream(replay.clone(), AbortSignal::new());
    let mut stopped = false;
    while let Some(event) = stream.next().await {
        assert!(
            !matches!(event, ModelStreamEvent::Error { .. }),
            "{event:?}"
        );
        stopped |= matches!(event, ModelStreamEvent::MessageStop { .. });
    }
    assert!(stopped);
    let next = provider
        .compact_remote(replay, AbortSignal::new())
        .await
        .unwrap();
    assert_eq!(next.checkpoint.encrypted_content, "opaque-new-account");
    assert_ne!(next.checkpoint.auth_scope, original_scope);
    let captured = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured.len(), 3);
    assert_replay(&captured[1], "new-account", "opaque-original");
    assert_replay(&captured[2], "new-account", "opaque-original");
    assert_eq!(
        captured[2].1["input"].as_array().unwrap().last().unwrap()["type"],
        "compaction_trigger"
    );
}

#[tokio::test]
async fn restored_session_continues_after_account_replacement_without_summary_conversion() {
    let (base, server) = fixture(vec![
        compact_response("opaque-restored"),
        normal_response(),
        normal_response(),
    ])
    .await;
    let (old, _) = subscription(&base, "old-account");
    let boundary = checkpoint(&old).await;
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (current, _) = subscription(&base, "new-account");
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(current);
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    let config = json!({"model":MODEL,"contextWindow":200_000,"prewarm":false,"fork":false,"thinking":false,"memory":{"enabled":false},"autoMemory":false,"autoDream":false,"memoryRecall":false,"loopDetection":false,"autoCompaction":true,"approval_policy":"never","sandbox_mode":"danger-full-access"});
    options.config = Some(config.clone());
    let session = Agent::new(options)
        .try_start_session_async(StartSessionOptions {
            user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
            initial_history: Some(vec![boundary]),
            base_instructions: Some("Continue the existing task.".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    for account in ["new-account", "third-account"] {
        if account == "third-account" {
            let (next, _) = subscription(&base, account);
            let next: Arc<dyn ModelProvider> = Arc::new(next);
            let requirements = session
                .prepare_review_requirements(next.clone())
                .await
                .unwrap();
            session
                .reconfigure_inference_with_targets(
                    next,
                    resolve_config(config.clone()).unwrap(),
                    None,
                    None,
                    InferencePublication {
                        expected_revision: session.model_revision(),
                        change_id: None,
                        targets: None,
                        advisor_target: None,
                        feature_targets: Default::default(),
                        source: None,
                        rates: None,
                        publication_key: None,
                        review_requirements: Some(requirements),
                    },
                )
                .unwrap();
        }
        let mut turn = session.run_streamed(
            Input::Text("Continue the existing task.".into()),
            TurnOptions::default(),
        );
        let mut events = Vec::new();
        while let Some(event) = turn.events.next().await {
            events.push(event);
        }
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::Compaction { .. })),
            "Account replacement must not require another summary: {events:?}"
        );
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let captured = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured.len(), 3);
    assert_replay(&captured[1], "new-account", "opaque-restored");
    assert_replay(&captured[2], "third-account", "opaque-restored");
    assert!(captured[1].1["input"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["type"] != "compaction_trigger"));
    assert!(captured[2].1["input"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["type"] != "compaction_trigger"));
}
