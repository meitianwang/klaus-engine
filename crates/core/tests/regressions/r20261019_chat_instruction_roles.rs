//! Defect: continuation failed when an endpoint rejected trusted instruction roles.
//! Root cause: mode authority was assumed to imply a universal wire role.
//! Work item: 20261019 instruction-role negotiation before model output.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::session::resolve_config_default;
use klaus_agent_core::planning::{instructions::instruction_part, CollaborationMode};
use klaus_agent_core::provider::openai::{
    OpenAICompatibilityOptions, OpenAIInstructionRole, OpenAIProvider, OpenAIProviderOptions,
    OpenAiApi,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStreamEvent, SystemPromptPart,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(
    respond: impl Fn(&Value) -> (u16, &'static str, String) + Send + 'static,
) -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 8192];
            let body: Value = loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                received.extend_from_slice(&buffer[..count]);
                let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&received[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .unwrap()
                    .1
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                if received.len() >= end + 4 + length {
                    break serde_json::from_slice(&received[end + 4..end + 4 + length]).unwrap();
                }
            };
            captured.lock().unwrap().push(body.clone());
            let (status, content_type, response) = respond(&body);
            socket.write_all(format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()
            ).as_bytes()).await.unwrap();
        }
    });
    Fixture {
        url,
        requests,
        task,
    }
}
fn provider(fixture: &Fixture, api: OpenAiApi) -> OpenAIProvider {
    OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(fixture.url.clone()),
        api: Some(api),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
}
fn request(mode: CollaborationMode, model: &str) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model: model.into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![
            SystemPromptPart {
                name: "stable".into(),
                text: "Stable trusted instructions".into(),
                cacheable: Some(true),
                cache_scope: None,
            },
            instruction_part(&resolve_config_default(), mode),
        ],
        messages: vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "original-request".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Finish checking the saved configuration".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })],
        tools: vec![],
        tool_choice: None,
        max_output_tokens: 100,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}
fn messages(body: &Value) -> &[Value] {
    body.get("messages")
        .or_else(|| body.get("input"))
        .unwrap()
        .as_array()
        .unwrap()
}
fn has_role(body: &Value, role: &str) -> bool {
    messages(body).iter().any(|message| message["role"] == role)
}
fn accepted(api: OpenAiApi) -> (u16, &'static str, String) {
    let text = match api {
        OpenAiApi::Chat => concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Recovered\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ),
        OpenAiApi::Responses => concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Recovered\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"
        ),
    };
    (200, "text/event-stream", text.into())
}
fn rejected(status: u16, sse: bool) -> (u16, &'static str, String) {
    let error = json!({"error_code":400,"error":{"type":"invalid_request_error","code":"invalid_parameter_error",
        "message":"developer is not one of ['system', 'assistant', 'user', 'tool', 'function']"}})
    .to_string();
    (
        status,
        if sse {
            "text/event-stream"
        } else {
            "application/json"
        },
        if sse {
            format!("data: {error}\n\n")
        } else {
            error
        },
    )
}
async fn collect(provider: &OpenAIProvider, req: ModelRequest) -> Vec<ModelStreamEvent> {
    tokio::time::timeout(
        Duration::from_secs(5),
        provider.stream(req, AbortSignal::new()).collect(),
    )
    .await
    .expect("the bounded local request settles")
}
fn assert_success(events: &[ModelStreamEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "{events:?}"
    );
    assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::TextDelta { text } if text == &RuntimeString::from("Recovered"))), "{events:?}");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn negotiates_full_modes_on_explicit_pre_output_refusals_and_remembers_exact_model() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for mode in [CollaborationMode::Default, CollaborationMode::Plan] {
            for (status, sse) in [(400, false), (422, false), (200, false), (200, true)] {
                let server = fixture(move |body| {
                    if body["model"] == "system-only" && has_role(body, "developer") {
                        rejected(status, sse)
                    } else {
                        accepted(api)
                    }
                })
                .await;
                let provider = provider(&server, api);
                let req = request(mode, "system-only");
                let expected_instruction = req.system[1].text.try_to_utf8().unwrap();
                assert_success(&collect(&provider, req.clone()).await);
                assert_success(&collect(&provider, req).await);
                // A sibling model at the same endpoint keeps its native role.
                assert_success(&collect(&provider, request(mode, "developer-capable")).await);
                let bodies = server.requests.lock().unwrap();
                assert_eq!(
                    bodies.len(),
                    4,
                    "one negotiation, one cached request, one sibling model"
                );
                assert!(has_role(&bodies[0], "developer"));
                assert!(!has_role(&bodies[1], "developer"));
                assert_eq!(bodies[1], bodies[2]);
                assert!(has_role(&bodies[3], "developer"));
                let field = if api == OpenAiApi::Chat {
                    "messages"
                } else {
                    "input"
                };
                let mut projected = bodies[0].clone();
                for message in projected[field].as_array_mut().unwrap() {
                    if message["role"] == "developer" {
                        message["role"] = json!("system");
                    }
                }
                assert_eq!(projected, bodies[1], "only the instruction role changes");
                assert_eq!(
                    messages(&bodies[1]).last().unwrap()["content"],
                    expected_instruction
                );
            }
        }
    }
}

#[tokio::test]
async fn supports_developer_only_endpoints_without_cross_endpoint_learning() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let system = fixture(move |body| {
            if has_role(body, "developer") {
                rejected(400, false)
            } else {
                accepted(api)
            }
        })
        .await;
        let native = fixture(move |body| {
            if has_role(body, "system") {
                (400, "application/json", json!({"error":{"message":"Unsupported value: 'messages[0].role' does not support 'system' with this model.","type":"invalid_request_error"}}).to_string())
            } else { accepted(api) }
        }).await;
        assert_success(
            &collect(
                &provider(&system, api),
                request(CollaborationMode::Plan, "same-model"),
            )
            .await,
        );
        let native_provider = provider(&native, api);
        assert_success(
            &collect(
                &native_provider,
                request(CollaborationMode::Plan, "same-model"),
            )
            .await,
        );
        assert!(has_role(&native.requests.lock().unwrap()[0], "developer"));
        if api == OpenAiApi::Chat {
            assert_success(
                &collect(
                    &native_provider,
                    request(CollaborationMode::Default, "same-model"),
                )
                .await,
            );
            let bodies = native.requests.lock().unwrap();
            assert_eq!(bodies.len(), 3);
            assert!(!has_role(&bodies[2], "system"));
        }
    }
}

#[tokio::test]
async fn unrelated_errors_or_post_output_errors_never_negotiate() {
    for (status, sse, message) in [
        (401, false, "Unsupported role: 'developer'"),
        (500, false, "Unsupported role: 'developer'"),
        (400, false, "Unsupported tool schema mentioning developer"),
        (400, false, "Unsupported message role"),
        (400, false, "Invalid schema: developer field is unsupported"),
        (
            200,
            true,
            "Unsupported value: 'developer' for messages[1].role",
        ),
    ] {
        let server = fixture(move |_| {
            let error = json!({"error":{"message":message,"type":"invalid_request_error"}}).to_string();
            let body = if sse {
                format!("data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"Already generated\"}}}}]}}\n\ndata: {error}\n\n")
            } else { error };
            (status, if sse { "text/event-stream" } else { "application/json" }, body)
        }).await;
        let events = collect(
            &provider(&server, OpenAiApi::Chat),
            request(CollaborationMode::Plan, "model"),
        )
        .await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
            "{events:?}"
        );
        assert_eq!(
            server.requests.lock().unwrap().len(),
            1,
            "{status} {message}"
        );
    }
}

#[tokio::test]
async fn both_roles_rejected_fail_after_one_negotiation_without_user_downgrade() {
    let server = fixture(|body| {
        if has_role(body, "developer") { rejected(400, false) }
        else { (400, "application/json", json!({"error":{"message":"Unsupported role: 'system'","type":"invalid_request_error"}}).to_string()) }
    }).await;
    let events = collect(
        &provider(&server, OpenAiApi::Chat),
        request(CollaborationMode::Plan, "model"),
    )
    .await;
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    let bodies = server.requests.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(
        messages(&bodies[1])
            .iter()
            .filter(|message| message["role"] == "user")
            .count(),
        1
    );
}

#[tokio::test]
async fn generation_evidence_in_error_envelopes_prevents_replay() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for generated in [
            json!({"choices":[{"delta":{"tool_calls":[{"id":"call1","function":{"name":"write"}}]}}]}),
            json!({"output":[{"type":"function_call","name":"write","call_id":"call1"}]}),
            json!({"response":{"output":[],"usage":{"output_tokens":1}}}),
            json!({"usage":{"completion_tokens":1}}),
        ] {
            let server = fixture(move |_| {
                let mut body = generated.clone();
                body["error"] = json!({"type":"invalid_request_error","message":"Unsupported role: 'developer'"});
                (200, "application/json", body.to_string())
            }).await;
            let _ = collect(
                &provider(&server, api),
                request(CollaborationMode::Plan, "model"),
            )
            .await;
            assert_eq!(server.requests.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn native_system_transports_preserve_both_complete_modes() {
    use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
    use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
    for mode in [CollaborationMode::Default, CollaborationMode::Plan] {
        for anthropic in [true, false] {
            let server = fixture(move |_| {
                let response = if anthropic {
                    concat!(
                        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg1\",\"model\":\"model\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
                        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Recovered\"}}\n\n",
                        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
                    )
                } else {
                    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Recovered\"}],\"role\":\"model\"},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1}}\n\n"
                };
                (200, "text/event-stream", response.into())
            }).await;
            let provider: Box<dyn ModelProvider> = if anthropic {
                Box::new(AnthropicProvider::new(AnthropicProviderOptions {
                    api_key: Some("fixture-key".into()),
                    base_url: Some(server.url.clone()),
                    ..Default::default()
                }))
            } else {
                Box::new(GeminiProvider::new(GeminiProviderOptions {
                    api_key: Some("fixture-key".into()),
                    base_url: Some(server.url.clone()),
                    ..Default::default()
                }))
            };
            let req = request(mode, "model");
            let expected = req.system[1].text.try_to_utf8().unwrap();
            let events = tokio::time::timeout(
                Duration::from_secs(5),
                provider.stream(req, AbortSignal::new()).collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            assert_success(&events);
            let bodies = server.requests.lock().unwrap();
            assert_eq!(bodies.len(), 1);
            if anthropic {
                assert_eq!(
                    bodies[0]["system"].as_array().unwrap().last().unwrap()["text"],
                    expected
                );
            } else {
                assert!(bodies[0]["systemInstruction"]["parts"][0]["text"]
                    .as_str()
                    .unwrap()
                    .ends_with(&expected));
            }
        }
    }
}

#[tokio::test]
async fn declared_role_preserves_full_modes_without_replaying_generic_refusals() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for mode in [CollaborationMode::Default, CollaborationMode::Plan] {
            let server = fixture(move |body| {
                if has_role(body, "developer") {
                    (
                        400,
                        "application/json",
                        json!({
                            "code": 11128,
                            "msg": "Illegal API invocation from an unapproved channel"
                        })
                        .to_string(),
                    )
                } else {
                    accepted(api)
                }
            })
            .await;
            let native = provider(&server, api);
            let req = request(mode, "same-model");
            let events = collect(&native, req.clone()).await;
            assert!(events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
            assert_eq!(
                server.requests.lock().unwrap().len(),
                1,
                "a generic refusal does not authorize a replay"
            );
            let configured = OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture-key".into()),
                base_url: Some(server.url.clone()),
                api: Some(api),
                compatibility: Some(
                    OpenAICompatibilityOptions::try_new(Default::default(), None)
                        .unwrap()
                        .with_instruction_role(Some(OpenAIInstructionRole::System)),
                ),
                retry: Some(RetryOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            });
            assert_success(&collect(&configured, req).await);
            let bodies = server.requests.lock().unwrap();
            assert_eq!(bodies.len(), 2);
            assert!(has_role(&bodies[0], "developer"));
            assert!(!has_role(&bodies[1], "developer"));
            let mut projected = bodies[0].clone();
            let field = if api == OpenAiApi::Chat {
                "messages"
            } else {
                "input"
            };
            for message in projected[field].as_array_mut().unwrap() {
                if message["role"] == "developer" {
                    message["role"] = json!("system");
                }
            }
            assert_eq!(
                projected, bodies[1],
                "the complete instruction bytes and all other request fields stay unchanged"
            );
        }
    }
}
