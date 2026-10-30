//! Defect: a provider rejected an oversized output reservation before usable input could run.
//! Root cause: establishment did not reduce the emitted output cap after a precise size rejection.
//! Work item 20261020: recover request-local output reservations without changing stream events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::interceptor::{
    ProviderRequestDirectives, ProviderRequestInfo, ProviderRequestInterceptor,
    ProviderResponseInfo,
};
use klaus_agent_core::provider::openai::{
    OpenAIProvider, OpenAIProviderOptions, OpenAiApi, OpenAiReasoningDialect,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ProviderError;
use klaus_agent_core::types::provider::{
    AuthHeaders, ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent,
    ThinkingCapability, ThinkingConfig,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WAIT: Duration = Duration::from_secs(5);
const OVERFLOW: &str =
    "input length and `max_tokens` exceed context limit: 193000 + 16000 > 200000";

#[derive(Clone, Copy, Debug)]
enum Wire {
    Messages,
    Chat,
    LegacyChat,
    Responses,
    ManagedChat,
}

impl Wire {
    fn output_field(self) -> &'static str {
        match self {
            Self::Messages | Self::LegacyChat => "max_tokens",
            Self::Responses => "max_output_tokens",
            Self::Chat | Self::ManagedChat => "max_completion_tokens",
        }
    }

    fn success(self) -> &'static str {
        match self {
            Self::Messages => "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"fixture\",\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            Self::Responses => "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
            _ => "data: {\"id\":\"fixture-response\",\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"fixture-response\",\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        }
    }
}

#[derive(Default)]
struct AuthCounter(AtomicUsize);

#[async_trait]
impl AuthSource for AuthCounter {
    async fn get_headers(&self, _signal: &AbortSignal) -> Result<AuthHeaders, ProviderError> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(HashMap::from([(
            "Authorization".to_owned(),
            format!("Bearer fixture-{attempt}"),
        )]))
    }
}

#[derive(Default)]
struct Attempts {
    settled: Mutex<Vec<(u32, bool, Option<u16>)>>,
    cancel_after_rejection: Option<AbortSignal>,
}

impl ProviderRequestInterceptor for Attempts {
    fn before_request(&self, request: &ProviderRequestInfo<'_>) -> ProviderRequestDirectives {
        ProviderRequestDirectives {
            extra_headers: vec![("x-fixture-attempt".to_owned(), request.attempt.to_string())],
            deny: None,
        }
    }

    fn after_response(&self, response: &ProviderResponseInfo<'_>) {
        self.settled
            .lock()
            .unwrap()
            .push((response.attempt, response.ok, response.status));
        if response.status == Some(400) {
            if let Some(signal) = &self.cancel_after_rejection {
                signal.cancel();
            }
        }
    }
}

struct Captured {
    body: Value,
    headers: String,
}

async fn read_request(socket: &mut TcpStream) -> Captured {
    let mut bytes = Vec::new();
    let mut buf = [0; 4096];
    let (header_end, length) = loop {
        let count = socket.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let count = socket.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
    }
    Captured {
        body: serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
        headers: String::from_utf8_lossy(&bytes[..header_end]).to_lowercase(),
    }
}

async fn fixture(
    wire: Wire,
    rejections: Vec<(u16, &str)>,
    succeed: bool,
    thinking: bool,
    cancel: bool,
) -> (
    Vec<ModelStreamEvent>,
    Vec<Captured>,
    Arc<Attempts>,
    Arc<AuthCounter>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let responses = rejections
        .into_iter()
        .map(|(status, message)| {
            (
                status,
                "application/json",
                json!({"error":{"type":"invalid_request_error","message":message}}).to_string(),
            )
        })
        .chain(succeed.then(|| (200, "text/event-stream", wire.success().to_owned())))
        .collect::<Vec<_>>();
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for (status, content_type, body) in responses {
            let (mut socket, _) = tokio::time::timeout(WAIT, listener.accept())
                .await
                .unwrap()
                .unwrap();
            captured.push(read_request(&mut socket).await);
            let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "unexpected extra physical request"
        );
        captured
    });
    let signal = AbortSignal::new();
    let attempts = Arc::new(Attempts {
        cancel_after_rejection: cancel.then(|| signal.clone()),
        ..Default::default()
    });
    let auth = Arc::new(AuthCounter::default());
    let retry = RetryOptions {
        max_attempts: Some(1),
        congestion_hold_ms: Some(0),
        unbounded_network_retries: Some(false),
        ..Default::default()
    };
    let mut capabilities = ModelCapabilities {
        context_window: Some(200_000),
        max_output_tokens: Some(16_000),
        thinking: thinking.then(ThinkingCapability::default),
        ..Default::default()
    };
    if matches!(wire, Wire::ManagedChat) {
        capabilities.effort = Some(
            runtime_codec::decode_text(&RuntimeString::from(
                json!({"levels":["low"],"default":"low"}).to_string(),
            ))
            .unwrap(),
        );
    }
    let provider: Box<dyn ModelProvider> = if matches!(wire, Wire::Messages) {
        Box::new(AnthropicProvider::new(AnthropicProviderOptions {
            base_url: Some(base_url),
            default_model: Some("fixture".to_owned()),
            capabilities: Some(capabilities),
            auth_source: Some(auth.clone()),
            interceptor: Some(attempts.clone()),
            retry: Some(retry),
            ..Default::default()
        }))
    } else {
        Box::new(OpenAIProvider::new(OpenAIProviderOptions {
            base_url: Some(base_url),
            default_model: Some("fixture".to_owned()),
            api: Some(if matches!(wire, Wire::Responses) {
                OpenAiApi::Responses
            } else {
                OpenAiApi::Chat
            }),
            reasoning_dialect: Some(match wire {
                Wire::LegacyChat => OpenAiReasoningDialect::LegacyTokens,
                Wire::ManagedChat => OpenAiReasoningDialect::ManagedQwen,
                _ => OpenAiReasoningDialect::OpenAi,
            }),
            capabilities: Some(capabilities),
            auth_source: Some(auth.clone()),
            interceptor: Some(attempts.clone()),
            retry: Some(retry),
            ..Default::default()
        }))
    };
    let mut request: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(json!({
        "model":"fixture","system":[{"name":"base","text":"Retain the task constraints."}],
        "messages":[{"type":"user","uuid":"request","content":[{"type":"text","text":"Continue the task."}]}],
        "tools":[],"maxOutputTokens":16000,"reasoningSummary":"none","effort":"low"
    }).to_string())).unwrap();
    if thinking {
        request.thinking = Some(ThinkingConfig::Enabled {
            budget_tokens: Some(12_000),
        });
    }
    let events = tokio::time::timeout(
        WAIT,
        provider.stream(request.clone(), signal).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert_eq!(request.max_output_tokens, 16_000);
    let captured = tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
    (events, captured, attempts, auth)
}

fn assert_success(events: &[ModelStreamEvent]) {
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::Error { .. } | ModelStreamEvent::RetryScheduled { .. }
        )),
        "{events:?}"
    );
}

#[tokio::test]
async fn physical_recovery_reduces_only_the_emitted_cap_and_refreshes_transport_hooks() {
    for wire in [
        Wire::Messages,
        Wire::Chat,
        Wire::LegacyChat,
        Wire::Responses,
    ] {
        let (events, mut requests, attempts, auth) =
            fixture(wire, vec![(400, OVERFLOW)], true, false, false).await;
        assert_success(&events);
        assert_eq!(requests.len(), 2, "{wire:?}");
        let field = wire.output_field();
        assert_eq!(requests[0].body[field], 16000, "{wire:?}");
        assert_eq!(requests[1].body[field], 6000, "{wire:?}");
        requests[1].body[field] = json!(16000);
        assert_eq!(requests[0].body, requests[1].body, "{wire:?}");
        for (index, request) in requests.iter().enumerate() {
            assert!(request
                .headers
                .contains(&format!("x-fixture-attempt: {}", index + 1)));
            assert!(request
                .headers
                .contains(&format!("authorization: bearer fixture-{}", index + 1)));
        }
        let settled = attempts.settled.lock().unwrap();
        assert_eq!(
            settled.iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(!settled[0].1);
        assert!(settled[1].1);
        assert_eq!(auth.0.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn output_floor_is_usable_and_enabled_thinking_stays_below_the_reduced_cap() {
    for (message, expected) in [
        (OVERFLOW, 6000),
        (
            "input length and `max_tokens` exceed context limit: 196000 + 16000 > 200000",
            3000,
        ),
    ] {
        let (events, requests, _, _) =
            fixture(Wire::Messages, vec![(400, message)], true, true, false).await;
        assert_success(&events);
        assert_eq!(requests[1].body["max_tokens"], expected);
        assert_eq!(requests[0].body["thinking"]["budget_tokens"], 12000);
        assert_eq!(requests[1].body["thinking"]["budget_tokens"], expected - 1);
    }
}

#[tokio::test]
async fn unsupported_or_nonprogressing_rejections_surface_without_physical_retry() {
    for wire in [Wire::Messages, Wire::Chat, Wire::Responses] {
        for (status,message) in [
            (400,"input length and `max_tokens` exceed context limit: 196001 + 16000 > 200000"),
            (400,"input length and `max_tokens` exceed context limit: 183000 + 16000 > 200000"),
            (400,"input length and `max_tokens` exceed context limit: 100000 + 16000 > 200000"),
            (400,"input length and `max_tokens` exceed context limit: not numeric"),
            (400,"input length and `max_tokens` exceed context limit: 18446744073709551616 + 16000 > 200000"),
            (400,"Prompt is too long: 209000 tokens > 200000"),
            (413,OVERFLOW),
        ] {
            let (events,requests,_,_)=fixture(wire,vec![(status,message)],false,false,false).await;
            assert_eq!(requests.len(),1,"{wire:?}: {message}");
            assert_eq!(events.iter().filter(|event|matches!(event,ModelStreamEvent::Error{..})).count(),1,"{events:?}");
            assert!(!events.iter().any(|event|matches!(event,ModelStreamEvent::RetryScheduled{..})),"{events:?}");
        }
    }
}

#[tokio::test]
async fn repeated_same_overflow_stops_after_the_first_reduction() {
    for wire in [Wire::Messages, Wire::Chat, Wire::Responses] {
        let (events, requests, _, _) = fixture(
            wire,
            vec![(400, OVERFLOW), (400, OVERFLOW)],
            false,
            false,
            false,
        )
        .await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].body[wire.output_field()], 6000);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelStreamEvent::Error { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn managed_output_budget_does_not_gain_an_unsupported_output_cap() {
    let (events, requests, _, _) = fixture(
        Wire::ManagedChat,
        vec![(400, OVERFLOW)],
        false,
        false,
        false,
    )
    .await;
    assert_eq!(requests.len(), 1);
    assert!(requests[0].body.get("max_completion_tokens").is_none());
    assert!(requests[0].body.get("max_tokens").is_none());
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
}

#[tokio::test]
async fn cancellation_after_rejection_does_not_resend_or_emit_a_recovery_error() {
    for wire in [Wire::Messages, Wire::Chat, Wire::Responses] {
        let (events, requests, attempts, auth) =
            fixture(wire, vec![(400, OVERFLOW)], false, false, true).await;
        assert_eq!(requests.len(), 1);
        assert_eq!(attempts.settled.lock().unwrap().len(), 1);
        assert_eq!(auth.0.load(Ordering::SeqCst), 1);
        assert!(
            !events.iter().any(|event| matches!(
                event,
                ModelStreamEvent::Error { .. }
                    | ModelStreamEvent::RetryScheduled { .. }
                    | ModelStreamEvent::MessageStop { .. }
            )),
            "{events:?}"
        );
    }
}
