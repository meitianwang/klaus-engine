//! Defects: native schema declarations were ignored and cache writes disappeared.
//! Root cause: one adapter assumed native JSON support and normalized only cache reads.
//! Work item 20261018: preserve declared output and cumulative usage contracts.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStreamEvent, ToolChoice,
    ToolSpec, ToolSpecKind,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEADLINE: Duration = Duration::from_secs(5);
const RESULT: &str = r#"{"title":"A complete result"}"#;
const INTERNAL_NAME: &str = "klaus_emit_structured_result";

fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}

fn function_item_events(item: &Value) -> String {
    let mut added = item.clone();
    added["arguments"] = json!("");
    event(json!({"type":"response.output_item.added", "item":added}))
        + &event(json!({"type":"response.output_item.done", "item":item}))
}

fn request(structured: bool) -> ModelRequest {
    let mut request: ModelRequest = runtime_codec::decode_value(
        &runtime_json::parse(&RuntimeString::from(
            r#"{"model":"contract-fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
        ))
        .unwrap(),
    )
    .unwrap();
    if structured {
        request.output_schema =
            Some(runtime_json::parse(&RuntimeString::from(schema().to_string())).unwrap());
    }
    request
}

fn schema() -> Value {
    json!({
        "type":"object", "required":["title"], "additionalProperties":false,
        "properties":{"title":{"type":"string", "description":"The complete final title"}},
        "$defs":{"unused":{"type":"array", "items":{"enum":[true, false, null]}}}
    })
}

fn user_tool(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: "Read a local value.".into(),
        input_schema: runtime_json::parse(&RuntimeString::from(r#"{"type":"object"}"#)).unwrap(),
        kind: ToolSpecKind::Function,
        format: None,
    }
}

fn provider(api: OpenAiApi, base_url: String, native: bool) -> OpenAIProvider {
    OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("contract-fixture-token".into()),
        base_url: Some(base_url),
        api: Some(api),
        capabilities: Some(ModelCapabilities {
            structured_output: native,
            ..Default::default()
        }),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    })
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut request = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the fixture request must be complete");
        request.extend_from_slice(&chunk[..read]);
        assert!(request.len() <= 1024 * 1024, "fixture requests are bounded");
        let Some(end) = request
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .map(|end| end + 4)
        else {
            continue;
        };
        let length = std::str::from_utf8(&request[..end])
            .unwrap()
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        if request.len() >= end + length {
            return serde_json::from_slice(&request[end..end + length]).unwrap();
        }
    }
}

async fn collect(
    api: OpenAiApi,
    request: ModelRequest,
    native: bool,
    response: impl FnOnce(&Value) -> String + Send + 'static,
) -> (Vec<ModelStreamEvent>, Value) {
    collect_with_truncated_body(api, request, native, 0, response).await
}

async fn collect_with_truncated_body(
    api: OpenAiApi,
    request: ModelRequest,
    native: bool,
    missing_bytes: usize,
    response: impl FnOnce(&Value) -> String + Send + 'static,
) -> (Vec<ModelStreamEvent>, Value) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        let body = response(&request);
        let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len() + missing_bytes);
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        request
    });
    let events = tokio::time::timeout(
        DEADLINE,
        provider(api, format!("http://{address}"), native)
            .stream(request, AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let request = tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    (events, request)
}

fn text_response(api: OpenAiApi, text: &str) -> String {
    match api {
        OpenAiApi::Chat => {
            event(json!({"model":"contract-fixture", "choices":[{
                "delta":{"content":text}, "finish_reason":"stop"
            }]}))
                + "data: [DONE]\n\n"
        }
        OpenAiApi::Responses => {
            event(json!({"type":"response.created", "response":{"model":"contract-fixture"}}))
                + &event(json!({"type":"response.output_text.delta", "delta":text}))
                + &event(json!({"type":"response.completed", "response":{"output":[]}}))
        }
    }
}

fn call_response(api: OpenAiApi, name: &str, arguments: &str, success: bool) -> String {
    match api {
        OpenAiApi::Chat => {
            event(json!({"model":"contract-fixture", "choices":[{
                "delta":{"tool_calls":[{"index":0, "id":"call-1", "type":"function", "function":{"name":name, "arguments":arguments}}]},
                "finish_reason":"tool_calls"
            }]}))
                + "data: [DONE]\n\n"
        }
        OpenAiApi::Responses => {
            let item = json!({"type":"function_call", "id":"item-1", "call_id":"call-1", "name":name, "arguments":arguments});
            event(json!({"type":"response.created", "response":{"model":"contract-fixture"}}))
                + &function_item_events(&item)
                + &if success {
                    event(json!({"type":"response.completed", "response":{"output":[item]}}))
                } else {
                    event(
                        json!({"type":"response.failed", "response":{"error":{"message":"fixture failure"}}}),
                    )
                }
        }
    }
}

fn tool_name(api: OpenAiApi, tool: &Value) -> &str {
    match api {
        OpenAiApi::Chat => tool["function"]["name"].as_str().unwrap(),
        OpenAiApi::Responses => tool["name"].as_str().unwrap(),
    }
}

fn text(events: &[ModelStreamEvent]) -> RuntimeString {
    events
        .iter()
        .fold(RuntimeString::default(), |text, event| match event {
            ModelStreamEvent::TextDelta { text: delta } => text.concat(delta),
            _ => text,
        })
}

fn terminal(events: &[ModelStreamEvent]) -> (&ModelStopReason, &Usage) {
    let Some(ModelStreamEvent::MessageStop { stop_reason, usage }) = events.last() else {
        panic!("the stream must have a successful terminal: {events:?}");
    };
    (stop_reason, usage)
}

fn rejected_error(events: &[ModelStreamEvent]) -> &ProviderError {
    let Some(ModelStreamEvent::Error { error }) = events.last() else {
        panic!("the response must be rejected: {events:?}");
    };
    error
}

fn assert_rejected_without_execution_or_replay(events: &[ModelStreamEvent]) {
    rejected_error(events);
    assert!(
        !events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ToolUseStop { .. }
                | ModelStreamEvent::MessageStop { .. }
                | ModelStreamEvent::RedactedThinkingBlock { .. }
                | ModelStreamEvent::ToolUseStart {
                    thought_signature: Some(_),
                    ..
                }
                | ModelStreamEvent::ThinkingBlock {
                    signature: Some(_),
                    ..
                }
                | ModelStreamEvent::ThinkingBlock {
                    encrypted_content: Some(_),
                    ..
                }
        )),
        "a rejected response cannot authorize execution or retain opaque replay: {events:?}"
    );
}

#[test]
fn native_structured_output_declarations_flow_through_per_model_overrides() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let provider = OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(api),
            capabilities: Some(ModelCapabilities {
                structured_output: false,
                ..Default::default()
            }),
            capabilities_by_model: Some(HashMap::from([
                (
                    "native".into(),
                    ModelCapabilities {
                        structured_output: true,
                        ..Default::default()
                    },
                ),
                (
                    "emulated".into(),
                    ModelCapabilities {
                        structured_output: false,
                        ..Default::default()
                    },
                ),
            ])),
            ..Default::default()
        });
        assert!(provider.capabilities(&"native".into()).structured_output);
        assert!(!provider.capabilities(&"emulated".into()).structured_output);
        assert!(
            !provider
                .capabilities(&"undeclared".into())
                .structured_output
        );
    }
}

#[tokio::test]
async fn undeclared_native_schema_uses_an_internal_forced_tool_without_dispatching_it() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let (events, body) = collect(api, request(true), false, move |body| {
            let name = tool_name(api, &body["tools"][0]);
            assert_eq!(name, INTERNAL_NAME);
            call_response(api, name, RESULT, true)
        })
        .await;
        let schema_field = if api == OpenAiApi::Chat {
            &body["tools"][0]["function"]["parameters"]
        } else {
            &body["tools"][0]["parameters"]
        };
        assert_eq!(schema_field, &schema());
        assert_eq!(body["tool_choice"]["type"], "function");
        assert!(body.get("response_format").is_none());
        assert!(body
            .get("text")
            .is_none_or(|text| text.get("format").is_none()));
        assert_eq!(text(&events), RESULT);
        assert_eq!(*terminal(&events).0, ModelStopReason::EndTurn);
        assert!(!events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ToolUseStart { .. }
                | ModelStreamEvent::ToolUseInputDelta { .. }
                | ModelStreamEvent::ToolUseStop { .. }
        )));
    }
}

#[tokio::test]
async fn declared_native_schema_keeps_the_native_format_without_an_internal_tool() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let (events, body) = collect(api, request(true), true, move |_| {
            text_response(api, RESULT)
        })
        .await;
        let format = if api == OpenAiApi::Chat {
            &body["response_format"]["json_schema"]
        } else {
            &body["text"]["format"]
        };
        assert_eq!(format["schema"], schema());
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
        assert_eq!(text(&events), RESULT);
        assert_eq!(*terminal(&events).0, ModelStopReason::EndTurn);
    }
}

#[tokio::test]
async fn automatic_user_tools_are_not_displaced_by_structured_formatting() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let mut request = request(true);
        request.tools.push(user_tool("read"));
        request.tool_choice = Some(ToolChoice::Auto);
        let (events, body) = collect(api, request, false, move |_| {
            call_response(api, "read", "{}", true)
        })
        .await;
        assert_eq!(body["tool_choice"], "auto");
        assert_eq!(body["tools"].as_array().unwrap().len(), 2);
        assert!(events.iter().any(
            |event| matches!(event, ModelStreamEvent::ToolUseStart { name, .. } if name == "read")
        ));
        assert!(events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ToolUseStop { id } if id == "call-1")));
        assert_eq!(*terminal(&events).0, ModelStopReason::ToolUse);
    }
}

#[tokio::test]
async fn explicit_user_choices_remain_authoritative_and_none_allows_only_internal_formatting() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for choice in [
            ToolChoice::Any,
            ToolChoice::Tool {
                name: "read".into(),
            },
            ToolChoice::None,
        ] {
            let mut request = request(true);
            request.tools.push(user_tool("read"));
            request.tool_choice = Some(choice.clone());
            let none = choice == ToolChoice::None;
            let (events, body) = collect(api, request, false, move |body| {
                let name = tool_name(api, &body["tools"][0]);
                call_response(api, name, if none { RESULT } else { "{}" }, true)
            })
            .await;
            assert_eq!(body["tools"].as_array().unwrap().len(), 1);
            if none {
                assert_eq!(tool_name(api, &body["tools"][0]), INTERNAL_NAME);
                assert_eq!(text(&events), RESULT);
                assert_eq!(*terminal(&events).0, ModelStopReason::EndTurn);
            } else {
                assert_eq!(tool_name(api, &body["tools"][0]), "read");
                if choice == ToolChoice::Any {
                    assert_eq!(body["tool_choice"], "required");
                }
                assert_eq!(*terminal(&events).0, ModelStopReason::ToolUse);
            }
        }
    }
}

#[tokio::test]
async fn a_user_tool_with_the_internal_base_name_is_never_hidden() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        let mut request = request(true);
        request.tools.push(user_tool(INTERNAL_NAME));
        let (events, body) = collect(api, request, false, move |_| {
            call_response(api, INTERNAL_NAME, "{}", true)
        })
        .await;
        assert_eq!(
            tool_name(api, &body["tools"][1]),
            "klaus_emit_structured_result_1"
        );
        assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::ToolUseStart { name, .. } if name == INTERNAL_NAME)));
        assert_eq!(*terminal(&events).0, ModelStopReason::ToolUse);
    }
}

#[tokio::test]
async fn malformed_and_failed_formatting_never_publishes_json_or_success() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for internal in [true, false] {
            let (events, _) = collect(api, request(true), false, move |_| {
                if internal {
                    call_response(api, INTERNAL_NAME, "{incomplete", true)
                } else {
                    text_response(api, "not JSON")
                }
            })
            .await;
            assert!(text(&events).is_empty());
            assert!(matches!(
                events.last(),
                Some(ModelStreamEvent::Error { .. })
            ));
            assert!(!events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
        }
    }
    let (events, _) = collect(OpenAiApi::Responses, request(true), false, |_| {
        call_response(OpenAiApi::Responses, INTERNAL_NAME, RESULT, false)
    })
    .await;
    assert!(text(&events).is_empty());
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::Error { .. })
    ));
    assert!(!events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
}

fn usage_snapshots(events: &[ModelStreamEvent]) -> Vec<&Usage> {
    events
        .iter()
        .filter_map(|event| match event {
            ModelStreamEvent::UsageSnapshot { usage } => Some(usage),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn responses_cache_creation_survives_partial_malformed_and_terminal_usage_updates() {
    let (events, _) = collect(OpenAiApi::Responses, request(false), false, |_| {
        event(json!({"type":"response.in_progress", "response":{"usage":{
            "input_tokens":100,"output_tokens":10,"total_tokens":143,
            "input_tokens_details":{"cached_tokens":20,"cache_creation_tokens":30}
        }}}))
            + &event(json!({"type":"response.in_progress", "usage":{"output_tokens":12}}))
            + &event(json!({"type":"response.in_progress", "usage":{
                "input_tokens":-1,"output_tokens":"bad","total_tokens":{},
                "input_tokens_details":{"cached_tokens":null,"cache_creation_tokens":"bad"}
            }}))
            + &event(
                json!({"type":"response.completed", "response":{"output":[], "usage":{
                    "total_tokens":144,"input_tokens_details":{"cache_creation_tokens":35}
                }}}),
            )
    })
    .await;
    let snapshots = usage_snapshots(&events);
    assert_eq!(
        snapshots.len(),
        3,
        "malformed values do not erase known counters: {events:?}"
    );
    assert_eq!(
        (
            snapshots[0].input_tokens,
            snapshots[0].cache_read_input_tokens,
            snapshots[0].cache_creation_input_tokens,
            snapshots[0].output_tokens,
            snapshots[0].total_tokens
        ),
        (50, 20, 30, 10, Some(143))
    );
    assert_eq!(
        (
            snapshots[1].input_tokens,
            snapshots[1].cache_read_input_tokens,
            snapshots[1].cache_creation_input_tokens,
            snapshots[1].output_tokens,
            snapshots[1].total_tokens
        ),
        (50, 20, 30, 12, Some(143))
    );
    assert_eq!(
        (
            snapshots[2].input_tokens,
            snapshots[2].cache_read_input_tokens,
            snapshots[2].cache_creation_input_tokens,
            snapshots[2].output_tokens,
            snapshots[2].total_tokens
        ),
        (45, 20, 35, 12, Some(144))
    );
    assert_eq!(terminal(&events).1, snapshots[2]);
}

#[tokio::test]
async fn cache_creation_clamps_to_the_remaining_prompt_and_defaults_to_zero() {
    for (details, expected) in [
        (
            json!({"cached_tokens":90,"cache_creation_tokens":50}),
            (0, 90, 10),
        ),
        (json!({}), (100, 0, 0)),
    ] {
        let (events, _) = collect(OpenAiApi::Responses, request(false), false, move |_| {
            event(
                json!({"type":"response.completed", "response":{"output":[], "usage":{
                    "input_tokens":100,"output_tokens":10,"input_tokens_details":details
                }}}),
            )
        })
        .await;
        let usage = terminal(&events).1;
        assert_eq!(
            (
                usage.input_tokens,
                usage.cache_read_input_tokens,
                usage.cache_creation_input_tokens
            ),
            expected
        );
        assert_eq!(usage.total_tokens, Some(110));
        assert_eq!(usage_snapshots(&events), vec![usage]);
    }
    let (events, _) = collect(OpenAiApi::Chat, request(false), false, |_| {
        event(
            json!({"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{
                "prompt_tokens":100,"completion_tokens":10,
                "prompt_tokens_details":{"cached_tokens":20,"cache_creation_tokens":30}
            }}),
        ) + "data: [DONE]\n\n"
    })
    .await;
    let usage = terminal(&events).1;
    assert_eq!(
        (
            usage.input_tokens,
            usage.cache_read_input_tokens,
            usage.cache_creation_input_tokens
        ),
        (80, 20, 0)
    );

    let (events, _) = collect(OpenAiApi::Responses, request(false), false, |_| {
        event(
            json!({"type":"response.completed", "response":{"output":[], "usage":{
                "input_tokens":100,"output_tokens":10,"total_tokens":0,
                "input_tokens_details":{"cached_tokens":20,"cache_creation_tokens":30}
            }}}),
        )
    })
    .await;
    assert_eq!(terminal(&events).1.total_tokens, Some(0));
}

#[tokio::test]
async fn formatting_discards_unreplayable_call_context_but_user_tools_keep_it() {
    for internal in [true, false] {
        let mut request = request(true);
        if !internal {
            request.tools.push(user_tool("read"));
        }
        let (events, _) = collect(OpenAiApi::Responses, request, false, move |_| {
            event(json!({"type":"response.output_item.done","item":{
                "type":"reasoning","id":"reasoning-1","summary":[{"type":"summary_text","text":"Visible thought"}],"encrypted_content":"opaque-replay"
            }})) + &call_response(OpenAiApi::Responses, if internal { INTERNAL_NAME } else { "read" }, if internal { RESULT } else { "{}" }, true)
        }).await;
        assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::ThinkingBlock { text, encrypted_content, .. }
            if text == "Visible thought" && encrypted_content.as_ref().is_some_and(|value| value == "opaque-replay") != internal)));
        assert_eq!(
            *terminal(&events).0,
            if internal {
                ModelStopReason::EndTurn
            } else {
                ModelStopReason::ToolUse
            }
        );
    }
}

#[tokio::test]
async fn mixed_formatting_and_user_calls_cannot_release_execution_or_replay_context() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for internal_first in [true, false] {
            let mut request = request(true);
            request.tools.push(user_tool("read"));
            let (events, _) = collect(api, request, false, move |_| {
                let mut calls = [("read", "{}"), (INTERNAL_NAME, RESULT)];
                if internal_first { calls.reverse(); }
                let body = match api {
                    OpenAiApi::Chat => {
                        let calls = calls.iter().enumerate().map(|(index, (name, arguments))| {
                            json!({"index":index, "id":format!("call-{index}"), "type":"function", "function":{"name":name,"arguments":arguments}})
                        }).collect::<Vec<_>>();
                        event(json!({"model":"contract-fixture", "choices":[{"delta":{"tool_calls":calls}, "finish_reason":"tool_calls"}]})) + "data: [DONE]\n\n"
                    }
                    OpenAiApi::Responses => {
                        let items = calls.iter().enumerate().map(|(index, (name, arguments))| {
                            json!({"type":"function_call", "id":format!("item-{index}"), "call_id":format!("call-{index}"), "name":name,"arguments":arguments})
                        }).collect::<Vec<_>>();
                        items.iter().map(function_item_events).collect::<String>()
                            + &event(json!({"type":"response.completed","response":{"output":items}}))
                    }
                };
                let reasoning = match api {
                    OpenAiApi::Chat => event(json!({"choices":[{"delta":{"reasoning_content":"Visible thought"}}]})),
                    OpenAiApi::Responses => event(json!({"type":"response.output_item.done", "item":{
                        "type":"reasoning", "id":"reasoning-mixed", "summary":[{"text":"Visible thought"}],
                        "encrypted_content":"opaque-mixed-replay"
                    }})),
                };
                reasoning + &body
            }).await;
            assert!(text(&events).is_empty());
            assert_rejected_without_execution_or_replay(&events);
            assert_eq!(rejected_error(&events).kind, ProviderErrorKind::Unknown);
            assert!(rejected_error(&events)
                .message
                .try_to_utf8()
                .unwrap()
                .contains("mixed"));
        }
    }
}

#[tokio::test]
async fn a_chat_tool_finish_followed_by_an_error_cannot_release_executable_stops() {
    let mut request = request(true);
    request.tools.push(user_tool("read"));
    let (events, _) = collect(OpenAiApi::Chat, request, false, |_| {
        let finished =
            call_response(OpenAiApi::Chat, "read", "{}", true).replace("data: [DONE]\n\n", "");
        finished
            + &event(json!({"error":{"message":"late response rejection","type":"server_error"}}))
            + "data: [DONE]\n\n"
    })
    .await;
    assert_rejected_without_execution_or_replay(&events);
    assert!(rejected_error(&events)
        .message
        .try_to_utf8()
        .unwrap()
        .contains("late response rejection"));
}

#[tokio::test]
async fn a_chat_tool_finish_followed_by_a_truncated_socket_cannot_release_executable_stops() {
    let mut request = request(true);
    request.tools.push(user_tool("read"));
    let (events, _) = collect_with_truncated_body(OpenAiApi::Chat, request, false, 100, |_| {
        call_response(OpenAiApi::Chat, "read", "{}", true).replace("data: [DONE]\n\n", "")
    })
    .await;
    assert_rejected_without_execution_or_replay(&events);
    assert_eq!(rejected_error(&events).kind, ProviderErrorKind::Network);
}

#[tokio::test]
async fn a_formatter_after_a_chat_tool_finish_cannot_retroactively_authorize_that_tool() {
    let mut request = request(true);
    request.tools.push(user_tool("read"));
    let (events, _) = collect(OpenAiApi::Chat, request, false, |_| {
        event(json!({"model":"contract-fixture", "choices":[{
            "delta":{"tool_calls":[{"index":0, "id":"user-call", "type":"function", "function":{"name":"read", "arguments":"{}"}}]},
            "finish_reason":"tool_calls"
        }]}))
        + &event(json!({"choices":[{
            "delta":{"tool_calls":[{"index":1, "id":"formatter-call", "type":"function", "function":{"name":INTERNAL_NAME, "arguments":RESULT}}]},
            "finish_reason":"tool_calls"
        }]}))
        + "data: [DONE]\n\n"
    }).await;
    assert_rejected_without_execution_or_replay(&events);
    assert!(rejected_error(&events)
        .message
        .try_to_utf8()
        .unwrap()
        .contains("mixed"));
}

#[tokio::test]
async fn response_tools_must_obey_the_original_declared_choice_and_tool_set() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for (choice, emitted) in [
            (ToolChoice::None, Some("read")),
            (ToolChoice::Any, None),
            (ToolChoice::Any, Some("unknown")),
            (
                ToolChoice::Tool {
                    name: "read".into(),
                },
                None,
            ),
            (
                ToolChoice::Tool {
                    name: "read".into(),
                },
                Some("other"),
            ),
            (ToolChoice::Auto, Some("unknown")),
        ] {
            let mut request = request(true);
            request.tools = vec![user_tool("read"), user_tool("other")];
            request.tool_choice = Some(choice);
            let (events, _) = collect(api, request, false, move |_| match emitted {
                Some(name) => call_response(api, name, "{}", true),
                None => text_response(api, RESULT),
            })
            .await;
            assert_rejected_without_execution_or_replay(&events);
        }
    }
}

#[tokio::test]
async fn responses_without_valid_usage_do_not_publish_an_authoritative_zero() {
    for (reported, expected_total) in [
        (None, None),
        (Some(json!({})), None),
        (
            Some(json!({
                "input_tokens":"unknown", "output_tokens":-1, "total_tokens":{},
                "input_tokens_details":{"cached_tokens":[],"cache_creation_tokens":"0"}
            })),
            None,
        ),
        (Some(json!({"total_tokens":0})), Some(0)),
        (Some(json!({"input_tokens":0})), Some(0)),
    ] {
        let (events, _) = collect(OpenAiApi::Responses, request(false), false, move |_| {
            let mut response = json!({"output":[]});
            if let Some(usage) = reported {
                response["usage"] = usage;
            }
            event(json!({"type":"response.output_text.delta", "delta":"done"}))
                + &event(json!({"type":"response.completed", "response":response}))
        })
        .await;
        let usage = terminal(&events).1;
        assert_eq!(usage.total_tokens, expected_total);
        assert_eq!(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_input_tokens,
                usage.cache_creation_input_tokens
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(
            usage_snapshots(&events).len(),
            usize::from(expected_total.is_some())
        );
    }
}

#[tokio::test]
async fn responses_tool_steps_without_billing_keep_the_context_total_unknown() {
    for reported in [None, Some(json!({}))] {
        let mut request = request(false);
        request.tools.push(user_tool("read"));
        let (events, _) = collect(OpenAiApi::Responses, request, false, move |_| {
            let item = json!({"type":"function_call", "id":"tool-step-item", "call_id":"tool-step-call", "name":"read", "arguments":"{}"});
            let mut response = json!({"output":[item.clone()]});
            if let Some(usage) = reported { response["usage"] = usage; }
            function_item_events(&item)
                + &event(json!({"type":"response.completed", "response":response}))
        }).await;
        assert_eq!(*terminal(&events).0, ModelStopReason::ToolUse);
        assert_eq!(terminal(&events).1.total_tokens, None);
        assert!(usage_snapshots(&events).is_empty());
        assert!(events.iter().any(
            |event| matches!(event, ModelStreamEvent::ToolUseStop { id } if id == "tool-step-call")
        ));
    }
}

#[tokio::test]
async fn responses_partial_billing_survives_an_empty_or_malformed_terminal_report() {
    for terminal_usage in [
        json!({}),
        json!({"total_tokens":false,"output_tokens":"unknown"}),
    ] {
        let (events, _) = collect(OpenAiApi::Responses, request(false), false, move |_| {
            event(json!({"type":"response.in_progress", "usage":{"input_tokens":10,"output_tokens":2}}))
                + &event(json!({"type":"response.completed", "response":{"output":[],"usage":terminal_usage}}))
        }).await;
        let usage = terminal(&events).1;
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.total_tokens),
            (10, 2, Some(12))
        );
        assert_eq!(usage_snapshots(&events), vec![usage]);
    }
}
