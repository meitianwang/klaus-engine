//! Defect: reasoning text was treated as completed summary activity without provenance.
//! Root cause: raw deltas, signatures and completed summaries shared an untyped boundary.
//! Work item 20261017: preserve native summary provenance only at a completed boundary.

use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const DEADLINE: Duration = Duration::from_secs(5);

fn event(value: serde_json::Value) -> String {
    format!("data: {value}\n\n")
}

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Anthropic,
    Gemini,
}

impl Protocol {
    fn provider(self, base_url: String) -> Box<dyn ModelProvider> {
        let retry = Some(RetryOptions {
            max_attempts: Some(1),
            ..Default::default()
        });
        match self {
            Self::Anthropic => Box::new(AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("reasoning-fixture-key".into()),
                base_url: Some(base_url),
                retry,
                ..Default::default()
            })),
            Self::Gemini => Box::new(GeminiProvider::new(GeminiProviderOptions {
                api_key: Some("reasoning-fixture-key".into()),
                base_url: Some(base_url),
                retry,
                ..Default::default()
            })),
        }
    }

    fn partial(self) -> String {
        match self {
            Self::Anthropic => {
                event(
                    json!({"type":"message_start","message":{"model":"claude-sonnet-4-20250514"}}),
                ) + &event(
                    json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
                ) + &event(
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"unfinished thought"}}),
                )
            }
            Self::Gemini => event(
                json!({"modelVersion":"gemini-2.5-pro","candidates":[{"content":{"parts":[{"text":"unfinished thought","thought":true}]}}]}),
            ),
        }
    }
}

struct HttpFixture {
    base_url: String,
    release: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl HttpFixture {
    // None suffix deliberately declares an incomplete response body, producing
    // a real transport failure; Some("") produces a clean body EOF.
    async fn new(prefix: String, suffix: Option<String>, hold: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (release, released) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "the complete fixture request must arrive");
                request.extend_from_slice(&chunk[..read]);
                assert!(request.len() <= 1024 * 1024, "fixture request is bounded");
                if let Some(end) = request
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                    .map(|end| end + 4)
                {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + length {
                        break;
                    }
                }
            }
            let length = prefix.len() + suffix.as_ref().map_or(100, String::len);
            let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n");
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(prefix.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            if hold {
                let _ = released.await;
            }
            if let Some(suffix) = suffix {
                socket.write_all(suffix.as_bytes()).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        Self {
            base_url,
            release: Some(release),
            server,
        }
    }

    async fn finish(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        tokio::time::timeout(DEADLINE, &mut self.server)
            .await
            .unwrap()
            .unwrap();
    }
}

fn request() -> ModelRequest {
    runtime_codec::decode_value(
        &runtime_json::parse(&RuntimeString::from(
            r#"{"model":"reasoning-fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
        ))
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn a_cancelled_signature_carrier_preserves_the_live_thought_in_canonical_history() {
    assert_canonical_thought_after_opaque_metadata(false).await;
}

#[tokio::test]
async fn a_completed_encrypted_carrier_preserves_the_live_thought_in_canonical_history() {
    assert_canonical_thought_after_opaque_metadata(true).await;
}

async fn assert_canonical_thought_after_opaque_metadata(complete: bool) {
    use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::runtime_process::RuntimeProcessContext;
    use klaus_agent_core::schemas::content::ContentBlock;
    use klaus_agent_core::schemas::message::Message;
    use klaus_agent_core::types::events::EngineEvent;
    use std::sync::Arc;
    use tokio::sync::Notify;

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    let carrier_consumed = Arc::new(Notify::new());
    let checkpoint = carrier_consumed.clone();
    let signature = (!complete).then(|| {
        RuntimeString::from(
            r#"gemini-thought:{"parts":[{"thoughtSignature":"opaque"}],"textOffset":0}"#,
        )
    });
    let encrypted_content = complete.then(|| RuntimeString::from("opaque-encrypted"));
    let wire_signature = signature.clone();
    let wire_encrypted = encrypted_content.clone();
    options.deps.call_model = Some(Box::new(move |_, signal| {
        let checkpoint = checkpoint.clone();
        let signature = wire_signature.clone();
        let encrypted_content = wire_encrypted.clone();
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model: "reasoning-fixture".into() };
            yield ModelStreamEvent::ThinkingDelta { text: "unfinished thought".into() };
            yield ModelStreamEvent::ThinkingBlock { text: RuntimeString::default(), signature, encrypted_content, is_summary: false };
            checkpoint.notify_one();
            if complete {
                yield ModelStreamEvent::TextDelta { text: "Done".into() };
                yield ModelStreamEvent::MessageStop {
                    stop_reason: klaus_agent_core::types::provider::ModelStopReason::EndTurn,
                    usage: klaus_agent_core::schemas::message::Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                };
            } else {
                signal.cancelled().await;
            }
        })
    }));
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    let running = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .run_streamed(
                    Input::Text("Think about the task".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>()
                .await
        })
    };
    tokio::time::timeout(DEADLINE, carrier_consumed.notified())
        .await
        .expect("the neutral carrier must reach the public Session");
    if !complete {
        session.interrupt();
    }
    let events = tokio::time::timeout(DEADLINE, running)
        .await
        .unwrap()
        .unwrap();
    let history = session.history();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    assert!(events.iter().any(|event| matches!(event, EngineEvent::ThinkingDelta { text, .. } if text == "unfinished thought")), "the original live thought event must remain unchanged: {events:?}");
    let thoughts: Vec<_> = history
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(message) => {
                Some(message.content.iter().filter_map(|block| match block {
                    ContentBlock::Thinking(thinking) => Some(thinking),
                    _ => None,
                }))
            }
            _ => None,
        })
        .flatten()
        .collect();
    assert!(
        thoughts
            .iter()
            .any(|thinking| thinking.text == "unfinished thought"),
        "accepted partial reasoning must survive neutral signature metadata: {thoughts:?}"
    );
    assert_eq!(
        thoughts.len(),
        1,
        "the carrier must retain its original reasoning block"
    );
    assert_eq!(thoughts[0].signature, signature);
    assert_eq!(thoughts[0].encrypted_content, encrypted_content);
    let live_id = events.iter().find_map(|event| match event {
        EngineEvent::ThinkingDelta {
            text, message_id, ..
        } if text == "unfinished thought" => message_id.as_ref(),
        _ => None,
    });
    assert!(live_id.is_some());
    assert_eq!(thoughts[0].message_id.as_ref(), live_id);
}

fn summaries(events: &[ModelStreamEvent]) -> Vec<&RuntimeString> {
    events
        .iter()
        .filter_map(|event| match event {
            ModelStreamEvent::ThinkingBlock {
                text,
                is_summary: true,
                ..
            } => Some(text),
            _ => None,
        })
        .collect()
}

async fn collect(protocol: Protocol, body: String) -> Vec<ModelStreamEvent> {
    let mut fixture = HttpFixture::new(body, Some(String::new()), false).await;
    let provider = protocol.provider(fixture.base_url.clone());
    let events = tokio::time::timeout(
        DEADLINE,
        provider.stream(request(), AbortSignal::new()).collect(),
    )
    .await
    .unwrap();
    fixture.finish().await;
    events
}

#[tokio::test]
async fn actual_http_gemini_summary_completes_at_a_visible_part_or_native_terminal() {
    for suffix in [
        event(
            json!({"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]}),
        ),
        event(json!({"candidates":[{"finishReason":"STOP"}]})),
        event(
            json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"observe","args":{}}}]},"finishReason":"STOP"}]}),
        ),
    ] {
        let events = collect(Protocol::Gemini, Protocol::Gemini.partial() + &suffix).await;
        assert_eq!(
            summaries(&events),
            vec![&RuntimeString::from("unfinished thought")]
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
    }
}

#[tokio::test]
async fn actual_http_gemini_unterminated_or_empty_reasoning_remains_neutral() {
    for suffix in [
        String::new(),
        event(json!({"candidates":[{"content":{"parts":[{"thoughtSignature":"opaque"}]}}]})),
        event(json!({"candidates":[{"finishReason":"FINISH_REASON_UNSPECIFIED"}]})),
    ] {
        let events = collect(Protocol::Gemini, Protocol::Gemini.partial() + &suffix).await;
        assert!(summaries(&events).is_empty(), "{events:?}");
        assert!(events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ThinkingBlock { text, is_summary: false, .. } if text == "unfinished thought"
        )), "unfinished display text is preserved: {events:?}");
    }
    for parts in [
        json!([{"thought":true,"thoughtSignature":"opaque"}]),
        json!([{"thought":true,"text":" \t\n","thoughtSignature":"opaque"}]),
    ] {
        let events = collect(
            Protocol::Gemini,
            event(json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]})),
        )
        .await;
        assert!(summaries(&events).is_empty(), "{events:?}");
    }
}

#[tokio::test]
async fn actual_http_partial_reasoning_cancellation_never_completes_a_summary() {
    for protocol in [Protocol::Anthropic, Protocol::Gemini] {
        let mut fixture = HttpFixture::new(protocol.partial(), None, true).await;
        let provider = protocol.provider(fixture.base_url.clone());
        let signal = AbortSignal::new();
        let mut stream = provider.stream(request(), signal.clone());
        let mut events = Vec::new();
        loop {
            let next = tokio::time::timeout(DEADLINE, stream.next())
                .await
                .unwrap()
                .expect("the partial thought must be delivered");
            let delta = matches!(&next, ModelStreamEvent::ThinkingDelta { text } if text == "unfinished thought");
            events.push(next);
            if delta {
                break;
            }
            assert!(events.len() <= 8, "unexpected fixture events: {events:?}");
        }
        signal.cancel();
        events.extend(
            tokio::time::timeout(DEADLINE, stream.collect::<Vec<_>>())
                .await
                .unwrap(),
        );
        assert!(summaries(&events).is_empty(), "{protocol:?}: {events:?}");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
            "{protocol:?}: {events:?}"
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn actual_http_partial_reasoning_transport_failure_never_completes_a_summary() {
    for protocol in [Protocol::Anthropic, Protocol::Gemini] {
        let mut fixture = HttpFixture::new(protocol.partial(), None, false).await;
        let provider = protocol.provider(fixture.base_url.clone());
        let events = tokio::time::timeout(
            DEADLINE,
            provider
                .stream(request(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::ThinkingDelta { .. })),
            "{protocol:?}: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
            "{protocol:?}: {events:?}"
        );
        assert!(summaries(&events).is_empty(), "{protocol:?}: {events:?}");
        fixture.finish().await;
    }
}

#[tokio::test]
async fn actual_http_compatible_thinking_signature_and_redaction_remain_unclassified() {
    let body = Protocol::Anthropic.partial()
        + &event(
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque"}}),
        )
        + &event(json!({"type":"content_block_stop","index":0}))
        + &event(
            json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"encrypted"}}),
        )
        + &event(json!({"type":"content_block_stop","index":1}))
        + &event(json!({"type":"message_stop"}));
    let events = collect(Protocol::Anthropic, body).await;
    assert!(summaries(&events).is_empty(), "{events:?}");
    assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::ThinkingBlock { text, signature: Some(_), is_summary: false, .. } if text == "unfinished thought")));
    assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::RedactedThinkingBlock { data } if data == "encrypted")));
}

#[tokio::test]
async fn actual_http_signature_only_chunk_cannot_close_a_live_thought_summary() {
    for complete in [false, true] {
        let prefix = Protocol::Gemini.partial()
            + &event(json!({"candidates":[{"content":{"parts":[{"thoughtSignature":"opaque"}]}}]}))
            + &event(json!({"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":1}}));
        let suffix = complete.then(|| event(json!({"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}]})));
        let mut fixture = HttpFixture::new(prefix, suffix, true).await;
        let provider = Protocol::Gemini.provider(fixture.base_url.clone());
        let signal = AbortSignal::new();
        let mut stream = provider.stream(request(), signal.clone());
        let mut events = Vec::new();
        loop {
            let next = tokio::time::timeout(DEADLINE, stream.next())
                .await
                .unwrap()
                .expect("the usage checkpoint follows the signature-only chunk");
            let checkpoint = matches!(next, ModelStreamEvent::UsageSnapshot { .. });
            events.push(next);
            if checkpoint {
                break;
            }
            assert!(events.len() <= 8, "unexpected fixture events: {events:?}");
        }
        assert!(summaries(&events).is_empty(), "{events:?}");
        assert!(!events.iter().any(|event| matches!(event,
            ModelStreamEvent::ThinkingBlock { text, is_summary, .. } if *is_summary || !text.is_empty()
        )), "a signature cannot flush the pending thought text: {events:?}");
        let retained: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::ThinkingBlock {
                    text,
                    signature: Some(signature),
                    is_summary: false,
                    encrypted_content: None,
                } if text.is_empty() => Some(signature),
                _ => None,
            })
            .collect();
        assert_eq!(
            retained,
            vec![&RuntimeString::from(
                r#"gemini-thought:{"parts":[{"thoughtSignature":"opaque"}],"textOffset":0}"#
            )]
        );
        assert!(events.iter().any(|event| matches!(event,
            ModelStreamEvent::ThinkingDelta { text } if text == "unfinished thought"
        )));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
        if complete {
            fixture.finish().await;
        } else {
            signal.cancel();
        }
        events.extend(
            tokio::time::timeout(DEADLINE, stream.collect::<Vec<_>>())
                .await
                .unwrap(),
        );
        if complete {
            assert_eq!(
                summaries(&events),
                vec![&RuntimeString::from("unfinished thought")]
            );
            let summary = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        ModelStreamEvent::ThinkingBlock {
                            is_summary: true,
                            ..
                        }
                    )
                })
                .unwrap();
            let answer = events.iter().position(|event| matches!(event, ModelStreamEvent::TextDelta { text } if text == "answer")).unwrap();
            assert!(summary < answer);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, ModelStreamEvent::MessageStop { .. }))
                    .count(),
                1
            );
        } else {
            assert!(summaries(&events).is_empty(), "{events:?}");
            assert!(!events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
            fixture.finish().await;
        }
    }
}
