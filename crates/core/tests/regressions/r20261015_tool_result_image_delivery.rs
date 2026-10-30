//! Defect: successful image reads reached text-only tool outputs on several wires.
//! Root cause: adapters forwarded extra-user media but ignored output image blocks.
//! Work item: 2026-10-15 tool-result image delivery.
//! Scope: public Session execution, real read tools and captured provider HTTP bodies.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions, UserContentPart};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::gemini::{
    CodeAssistOptions, GeminiProvider, GeminiProviderOptions,
};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{
    ContentBlock, DocumentBlock, ImageBlock, OutputBlock, TextBlock,
};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelStopReason, ModelStreamEvent,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

#[derive(Clone, Copy)]
enum Wire {
    Chat,
    Responses,
    Gemini,
    CodeAssist,
    Anthropic,
}

impl Wire {
    fn provider(self, base_url: String) -> Arc<dyn ModelProvider> {
        let capabilities = Some(ModelCapabilities {
            vision: true,
            native_pdf: true,
            tools: Some(true),
            ..Default::default()
        });
        let retry = Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            ..Default::default()
        });
        match self {
            Self::Chat | Self::Responses => Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("fixture-key".into()),
                base_url: Some(base_url),
                api: Some(if matches!(self, Self::Chat) {
                    OpenAiApi::Chat
                } else {
                    OpenAiApi::Responses
                }),
                capabilities,
                retry,
                ..Default::default()
            })),
            Self::Gemini | Self::CodeAssist => {
                Arc::new(GeminiProvider::new(GeminiProviderOptions {
                    api_key: Some("fixture-key".into()),
                    base_url: Some(base_url.clone()),
                    code_assist: matches!(self, Self::CodeAssist).then_some(CodeAssistOptions {
                        endpoint: Some(base_url),
                        project: Some("fixture-project".into()),
                        session_id: Some("fixture-session".into()),
                    }),
                    capabilities,
                    retry,
                    ..Default::default()
                }))
            }
            Self::Anthropic => Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("fixture-key".into()),
                base_url: Some(base_url),
                capabilities,
                retry,
                ..Default::default()
            })),
        }
    }
}

struct Directory(PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn png(colour: u8) -> Vec<u8> {
    let image = image::RgbImage::from_pixel(64, 64, image::Rgb([colour, 34, 56]));
    let mut bytes = Vec::new();
    image
        .write_with_encoder(image::codecs::png::PngEncoder::new(&mut bytes))
        .unwrap();
    bytes
}

/// The fixture captures the final HTTP request, beyond the model-neutral boundary.
async fn capture_request(listener: tokio::net::TcpListener) -> Value {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut received = Vec::new();
    let mut buffer = [0_u8; 8192];
    let body = loop {
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0, "provider must send the complete HTTP body");
        received.extend_from_slice(&buffer[..count]);
        if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&received[..end]);
            let length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, length)| length.trim().parse::<usize>().unwrap())
                .unwrap();
            if received.len() >= end + 4 + length {
                break serde_json::from_slice(&received[end + 4..end + 4 + length]).unwrap();
            }
        }
    };
    let rejection = r#"{"error":{"type":"invalid_request_error","message":"Captured local fixture request","code":400}}"#;
    let response = format!("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rejection}", rejection.len());
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
    body
}

/// Execute two real image reads, then send their results through the real adapter.
async fn read_images(wire: Wire, mixed_output: bool) -> (Value, Vec<String>) {
    let directory = Directory(
        std::env::temp_dir().join(format!("klaus-tool-image-wire-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&directory.0).unwrap();
    let images: Vec<Vec<u8>> = [12, 78, 140, 200].into_iter().map(png).collect();
    for (index, bytes) in images.iter().take(2).enumerate() {
        std::fs::write(directory.0.join(format!("image-{index}.png")), bytes).unwrap();
    }
    let mut encoded: Vec<String> = images.iter().map(|bytes| BASE64.encode(bytes)).collect();
    let extra_image = ImageBlock {
        mime_type: "image/png".into(),
        base64: encoded[3].clone().into(),
        detail: None,
        source_path: None,
    };
    if !mixed_output {
        encoded.pop();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = wire.provider(format!("http://{}", listener.local_addr().unwrap()));
    let mut server = tokio::spawn(capture_request(listener));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.include_default_agents = Some(false);
    options.tools = Some(vec![create_read_tool()]);
    let calls = AtomicUsize::new(0);
    options.deps.call_model = Some(Box::new(move |mut request, signal| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let read = request
                .tools
                .iter()
                .find(|tool| tool.name == "read" || tool.name == "Read")
                .expect("read is offered to the model");
            let path_key = if read
                .input_schema
                .get("properties")
                .and_then(|p| p.get("file_path"))
                .is_some()
            {
                "file_path"
            } else {
                "path"
            };
            let mut events = vec![ModelStreamEvent::MessageStart {
                model: request.model.clone(),
            }];
            for index in 0..2 {
                let id = RuntimeString::from(format!("read-{index}"));
                events.extend([
                    ModelStreamEvent::ToolUseStart {
                        id: id.clone(),
                        name: read.name.clone(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: id.clone(),
                        partial_json: json!({ path_key: format!("image-{index}.png") })
                            .to_string()
                            .into(),
                    },
                    ModelStreamEvent::ToolUseStop { id },
                ]);
            }
            events.push(ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            });
            return Box::pin(futures::stream::iter(events));
        }
        let results: Vec<_> = request
            .messages
            .iter_mut()
            .filter_map(|message| match message {
                Message::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect();
        assert_eq!(
            results.len(),
            2,
            "both reads must finish before the next model request"
        );
        for result in results {
            assert!(!result.is_error, "read must succeed: {:?}", result.content);
            assert!(
                matches!(result.content.as_slice(), [OutputBlock::Image(_)]),
                "read must return a model image"
            );
            if mixed_output {
                result.content.insert(
                    0,
                    OutputBlock::Text(TextBlock {
                        text: "Image read completed.".into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    }),
                );
                result.extra_user_blocks = Some(vec![ContentBlock::Text(TextBlock {
                    text: "Attached context.".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })]);
                if result.tool_use_id == "read-0" {
                    result.extra_user_blocks.as_mut().unwrap().extend([
                        ContentBlock::Image(extra_image.clone()),
                        ContentBlock::Document(DocumentBlock {
                            mime_type: "application/pdf".into(),
                            base64: "JVBERi0xLjQKJSVFT0Y=".into(),
                            filename: Some("fixture.pdf".into()),
                        }),
                    ]);
                }
            }
        }
        provider.stream(request, signal)
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.0.to_string_lossy().into_owned().into()),
        config: Some(json!({
            "model": "fixture-vision", "prewarm": false, "goal": false,
            "thinking": false, "fork": false, "autoMemory": false,
            "autoDream": false, "memoryRecall": false, "checkpointing": false,
            "speculativeCompaction": false, "earlyMicrocompact": false,
            "idleMicrocompact": false, "compactionSelfCheck": false,
            "loopDetection": false
        })),
        ..Default::default()
    });
    let input = Input::Parts(vec![
        UserContentPart::Text {
            text: "Read both local fixture images.".into(),
        },
        UserContentPart::Image {
            mime_type: "image/png".into(),
            base64: encoded[2].clone().into(),
            source_path: None,
        },
    ]);
    let outcome = tokio::time::timeout(Duration::from_secs(15), async {
        let result = session.run(input, TurnOptions::default()).await;
        assert!(
            result.error.is_some(),
            "the local capture intentionally rejects the request"
        );
        (&mut server).await.unwrap()
    })
    .await;
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    match outcome {
        Ok(body) => (body, encoded),
        Err(_) => {
            server.abort();
            panic!("image delivery must reach the local HTTP fixture within fifteen seconds")
        }
    }
}

fn assert_images(body: &Value, encoded: &[String]) {
    let text = body.to_string();
    for image in encoded {
        assert_eq!(
            text.matches(image).count(),
            1,
            "each input and read image must reach the wire exactly once: {body}"
        );
        let bytes = BASE64.decode(image).unwrap();
        assert_eq!(image::load_from_memory(&bytes).unwrap().width(), 64);
    }
}

#[tokio::test]
async fn chat_read_images_follow_the_complete_parallel_tool_batch() {
    let (body, encoded) = read_images(Wire::Chat, false).await;
    assert_images(&body, &encoded);
    let messages = body["messages"].as_array().unwrap();
    let start = messages
        .iter()
        .position(|message| message["tool_calls"].is_array())
        .unwrap();
    assert_eq!(messages[start + 1]["role"], "tool");
    assert_eq!(messages[start + 2]["role"], "tool");
    assert_eq!(messages[start + 1]["tool_call_id"], "read-0");
    assert_eq!(messages[start + 2]["tool_call_id"], "read-1");
    assert_eq!(messages[start + 3]["role"], "user");
    assert_eq!(messages[start + 3]["content"][0]["type"], "image_url");
    assert_eq!(messages[start + 3]["content"][1]["type"], "image_url");
    for (index, image) in encoded.iter().take(2).enumerate() {
        assert_eq!(
            messages[start + 3]["content"][index]["image_url"]["url"],
            format!("data:image/png;base64,{image}")
        );
    }
}

#[tokio::test]
async fn responses_read_images_are_input_images_after_call_outputs() {
    let (body, encoded) = read_images(Wire::Responses, false).await;
    assert_images(&body, &encoded);
    let input = body["input"].as_array().unwrap();
    for (image_index, id) in ["read-0", "read-1"].into_iter().enumerate() {
        let index = input
            .iter()
            .position(|item| item["type"] == "function_call_output" && item["call_id"] == id)
            .unwrap();
        assert_eq!(input[index + 1]["role"], "user");
        assert_eq!(input[index + 1]["content"][0]["type"], "input_image");
        assert_eq!(
            input[index + 1]["content"][0]["image_url"],
            format!("data:image/png;base64,{}", encoded[image_index])
        );
    }
}

async fn assert_gemini(wire: Wire) {
    let (body, encoded) = read_images(wire, false).await;
    assert_images(&body, &encoded);
    let body = if matches!(wire, Wire::CodeAssist) {
        &body["request"]
    } else {
        &body
    };
    let contents = body["contents"].as_array().unwrap();
    let results = contents
        .iter()
        .find(|content| {
            content["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|part| part["functionResponse"].is_object())
        })
        .unwrap();
    let parts = results["parts"].as_array().unwrap();
    for (image_index, id) in ["read-0", "read-1"].into_iter().enumerate() {
        let index = parts
            .iter()
            .position(|part| part["functionResponse"]["id"] == id)
            .unwrap();
        assert_eq!(parts[index + 1]["inlineData"]["mimeType"], "image/png");
        assert_eq!(parts[index + 1]["inlineData"]["data"], encoded[image_index]);
    }
}

#[tokio::test]
async fn gemini_read_images_reach_inline_data() {
    assert_gemini(Wire::Gemini).await;
}

#[tokio::test]
async fn code_assist_read_images_reach_the_wrapped_request() {
    assert_gemini(Wire::CodeAssist).await;
}

#[tokio::test]
async fn anthropic_read_images_stay_inside_tool_results() {
    let (body, encoded) = read_images(Wire::Anthropic, false).await;
    assert_images(&body, &encoded);
    let blocks: Vec<_> = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .filter(|block| block["type"] == "tool_result")
        .collect();
    assert_eq!(blocks.len(), 2);
    for (index, block) in blocks.into_iter().enumerate() {
        assert_eq!(block["content"][0]["type"], "image");
        assert_eq!(block["content"][0]["source"]["media_type"], "image/png");
        assert_eq!(block["tool_use_id"], format!("read-{index}"));
        assert_eq!(block["content"][0]["source"]["data"], encoded[index]);
    }
}

#[tokio::test]
async fn mixed_image_outputs_preserve_existing_image_and_document_attachments() {
    for wire in [
        Wire::Chat,
        Wire::Responses,
        Wire::Gemini,
        Wire::CodeAssist,
        Wire::Anthropic,
    ] {
        let (body, encoded) = read_images(wire, true).await;
        assert_images(&body, &encoded);
        assert_eq!(body.to_string().matches("Image read completed.").count(), 2);
        assert_eq!(body.to_string().matches("JVBERi0xLjQKJSVFT0Y=").count(), 1);
        if matches!(wire, Wire::Gemini | Wire::CodeAssist | Wire::Anthropic) {
            assert_eq!(body.to_string().matches("Attached context.").count(), 2);
        }
    }
}
