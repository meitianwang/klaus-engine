//! Defect: readonly title requests advertised automatic tool choice without tools.
//! Root cause: the authoritative request builder added Auto unconditionally.
//! Original fix: bd0a6abc; pin the public title path and both OpenAI wire surfaces.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{Message, Usage, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, ToolChoice,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let answer = if request.output_schema.is_some() {
            r#"{"title":"A test title"}"#
        } else {
            "Done."
        };
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: answer.into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

struct ProbeTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for ProbeTool {
    fn static_name(&self) -> &str {
        "probe"
    }

    fn static_description(&self) -> &str {
        "Read the test probe."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        panic!("the recording provider never requests tool execution")
    }
}

/// Record an actual public Session request; title generation must hide even
/// explicitly registered tools, while an ordinary turn must keep them available.
pub(super) async fn record_engine_request(for_title: bool) -> ModelRequest {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-tool-choice-regression-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "tool-choice-regression".into(),
            base_url: None,
            env_key: None,
            default_model: "test-model".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("test-model");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.tools = Some(vec![Arc::new(ProbeTool {
        schema: json!({"type": "object", "properties": {}})
            .as_object()
            .unwrap()
            .clone(),
    })]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        initial_history: Some(vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "title-input".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Refactor the parser.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })]),
        ..Default::default()
    });
    if for_title {
        let result = session.generate_title(None, None, false).await.unwrap();
        assert_eq!(result.title, Some(RuntimeString::from("A test title")));
    } else {
        let result = session
            .run(Input::Text("Continue.".into()), TurnOptions::default())
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    let requests = provider.requests.lock().unwrap().clone();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert_eq!(
        requests.len(),
        1,
        "one model response completes the request"
    );
    requests.into_iter().next().unwrap()
}

pub(super) fn single_attempt() -> RetryOptions {
    RetryOptions {
        max_attempts: Some(1),
        congestion_hold_ms: Some(0),
        ..Default::default()
    }
}

/// Capture the provider's real HTTP body, then reject the fixture request.
/// Request assertions do not require a simulated model response or credentials.
pub(super) async fn capture_provider_request(
    request: ModelRequest,
    create_provider: impl FnOnce(String) -> Box<dyn ModelProvider>,
) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 4096];
        let body = loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0, "the complete request must reach the fixture");
            assert!(
                received.len() + count <= 1024 * 1024,
                "fixture request exceeds 1 MiB"
            );
            received.extend_from_slice(&buffer[..count]);
            if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&received[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, length)| length.trim().parse::<usize>().unwrap())
                    .expect("provider JSON requests declare their content length");
                if received.len() >= end + 4 + length {
                    break serde_json::from_slice::<Value>(&received[end + 4..end + 4 + length])
                        .unwrap();
                }
            }
        };
        let rejection =
            r#"{"error":{"type":"invalid_request_error","message":"Captured test request"}}"#;
        let response = format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rejection}",
            rejection.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        body
    });
    let provider = create_provider(base_url);
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = provider.stream(request, AbortSignal::new());
        while stream.next().await.is_some() {}
        (&mut server).await.unwrap()
    })
    .await;
    match outcome {
        Ok(body) => body,
        Err(_) => {
            server.abort();
            panic!("the local provider request must finish within five seconds")
        }
    }
}

async fn openai_body(request: ModelRequest, api: OpenAiApi) -> Value {
    openai_body_with_native_schema(request, api, true).await
}

async fn openai_body_with_native_schema(
    request: ModelRequest,
    api: OpenAiApi,
    native_schema: bool,
) -> Value {
    capture_provider_request(request, |base_url| {
        Box::new(OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("test-key".into()),
            base_url: Some(base_url),
            api: Some(api),
            capabilities: Some(ModelCapabilities {
                structured_output: native_schema,
                ..Default::default()
            }),
            retry: Some(single_attempt()),
            ..Default::default()
        }))
    })
    .await
}

#[tokio::test]
async fn readonly_title_omits_empty_tools_and_choice_on_both_openai_surfaces() {
    let request = record_engine_request(true).await;
    assert!(request.tools.is_empty());
    assert_eq!(request.tool_choice, None);
    assert!(
        request.output_schema.is_some(),
        "exercise the real title path"
    );
    let expected_schema: Value = serde_json::from_slice(
        &runtime_json::stringify(request.output_schema.as_ref().unwrap()).to_node_utf8(),
    )
    .unwrap();
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let body = openai_body(request.clone(), api).await;
        assert!(body.get("tools").is_none(), "{api:?}: {body}");
        assert!(body.get("tool_choice").is_none(), "{api:?}: {body}");
        let format = match api {
            OpenAiApi::Chat => &body["response_format"],
            OpenAiApi::Responses => &body["text"]["format"],
        };
        assert_eq!(format["type"], json!("json_schema"), "{api:?}: {body}");
        let schema = match api {
            OpenAiApi::Chat => &format["json_schema"]["schema"],
            OpenAiApi::Responses => &format["schema"],
        };
        assert_eq!(schema, &expected_schema, "{api:?}: {body}");
    }
}

#[tokio::test]
async fn readonly_title_without_native_schema_forces_only_the_formatter_on_both_surfaces() {
    let request = record_engine_request(true).await;
    assert!(request.tools.is_empty());
    assert_eq!(request.tool_choice, None);
    let expected_schema: Value = serde_json::from_slice(
        &runtime_json::stringify(request.output_schema.as_ref().unwrap()).to_node_utf8(),
    )
    .unwrap();
    let formatter_name = "klaus_emit_structured_result";
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let body = openai_body_with_native_schema(request.clone(), api, false).await;
        let tools = body["tools"]
            .as_array()
            .expect("schema fallback declares its formatter");
        assert_eq!(
            tools.len(),
            1,
            "no executable tool may enter a title request: {api:?}: {body}"
        );
        assert_eq!(tools[0]["type"], json!("function"));
        let formatter = match api {
            OpenAiApi::Chat => &tools[0]["function"],
            OpenAiApi::Responses => &tools[0],
        };
        assert_eq!(formatter["name"], json!(formatter_name));
        assert_eq!(formatter["parameters"], expected_schema);
        let expected_choice = match api {
            OpenAiApi::Chat => json!({"type":"function","function":{"name":formatter_name}}),
            OpenAiApi::Responses => json!({"type":"function","name":formatter_name}),
        };
        assert_eq!(body["tool_choice"], expected_choice, "{api:?}: {body}");
    }
}

#[tokio::test]
async fn ordinary_tool_turn_retains_auto_choice_on_both_openai_surfaces() {
    let request = record_engine_request(false).await;
    assert!(request.tools.iter().any(|tool| tool.name == "probe"));
    assert_eq!(request.tool_choice, Some(ToolChoice::Auto));
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let body = openai_body(request.clone(), api).await;
        assert!(
            !body["tools"].as_array().unwrap().is_empty(),
            "{api:?}: {body}"
        );
        assert_eq!(body["tool_choice"], json!("auto"), "{api:?}: {body}");
    }
}
