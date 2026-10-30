//! Regression: the named read-only question facade must carry inline binary
//! content and target cancellation without writing a turn into main history.
//! Work item: 2026-09-22 side-question attachments.

use super::*;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::ModelRequest;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

fn question() -> Value {
    json!([
        {"type":"text","text":"Compare this extracted Office/TXT/HTML text."},
        {"type":"image","base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==","mimeType":"image/png"},
        {"type":"document","base64":"JVBERi0xLjQ=","mimeType":"application/pdf","filename":"report.pdf"}
    ])
}

struct Fixture {
    client: TestClient,
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    signals: Arc<Mutex<Vec<AbortSignal>>>,
    entered: Arc<Notify>,
    hold: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    reply_timeout: Duration,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_config(Some(json!({"autoMemory": false}))).await
    }

    async fn with_config(config: Option<Value>) -> Self {
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let requests = Arc::new(Mutex::new(Vec::new()));
        let signals = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let hold = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let agent = make_agent_with_call_model_and_config(
            Box::new({
                let requests = requests.clone();
                let signals = signals.clone();
                let entered = entered.clone();
                let hold = hold.clone();
                let dropped = dropped.clone();
                move |request, signal| {
                    requests.lock().unwrap().push(request);
                    signals.lock().unwrap().push(signal);
                    entered.notify_one();
                    if hold.load(Ordering::SeqCst) {
                        let probe = DropProbe(dropped.clone());
                        Box::pin(futures::stream::once(async move {
                            let _probe = probe;
                            futures::future::pending::<ModelStreamEvent>().await
                        }))
                    } else {
                        Box::pin(futures::stream::iter(text_turn("side answer")))
                    }
                }
            }),
            config,
        );
        let session_slot = Arc::new(Mutex::new(None));
        let factory = create_session_for_agent_tests(agent);
        let captured = session_slot.clone();
        let mut client = TestClient::start(Arc::new(move |context| {
            let factory = factory.clone();
            let captured = captured.clone();
            Box::pin(async move {
                let session = factory(context).await?;
                *captured.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        }));
        client.initialize().await;
        client.send(json!({"id":2,"method":"session/start","params":{"init":{"threadId":"readonly-side"}}})).await;
        let (reply, _) = client.response_for(2).await;
        assert_eq!(reply["result"]["sessionId"], "readonly-side", "{reply}");
        // Lazy session creation happens when the first question is submitted.
        client.send(json!({"id":3,"method":"session/contextSnapshot","params":{"sessionId":"readonly-side"}})).await;
        let (reply, _) = client.response_for(3).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let session = session_slot.lock().unwrap().clone().unwrap();
        Self {
            client,
            session,
            requests,
            signals,
            entered,
            hold,
            dropped,
            reply_timeout: REPLY_TIMEOUT,
        }
    }

    async fn submit(&mut self, id: u64, submission: &str, question: Value) -> (Value, Vec<Value>) {
        self.client
            .send(json!({"id":id,"method":"session/sideQuestion","params":{
                "sessionId":"readonly-side","submissionId":submission,"question":question
            }}))
            .await;
        self.client
            .response_for_with_timeout(id, self.reply_timeout)
            .await
    }

    async fn event(&mut self, submission: &str, mut events: Vec<Value>) -> Value {
        loop {
            if let Some(event) = events
                .iter()
                .find(|event| event["params"]["event"]["submissionId"] == submission)
            {
                assert_ne!(event["method"], "approval/request");
                return event["params"]["event"].clone();
            }
            let event = self
                .client
                .next_message_with_timeout(self.reply_timeout)
                .await;
            assert_ne!(event["method"], "approval/request");
            events.push(event);
        }
    }
}

#[tokio::test]
async fn readonly_side_question_named_facade_delivers_binary_blocks_and_rejects_invalid_input() {
    let mut fixture = Fixture::new().await;
    fixture
        .session
        .run(Input::Text("parent context".into()), TurnOptions::default())
        .await;
    let before = fixture.session.history();
    let (ack, events) = fixture.submit(10, "parts", question()).await;
    assert_eq!(ack["result"]["submissionId"], "parts", "{ack}");
    let event = fixture.event("parts", events).await;
    assert_eq!(event["sideQuestion"]["response"], "side answer", "{event}");
    assert_eq!(fixture.session.history(), before);
    {
        let calls = fixture.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(!calls[0].tools.is_empty());
        assert!(calls[1].tools.is_empty());
        assert_eq!(calls[1].skip_cache_write, Some(true));
        let Message::User(user) = calls[1].messages.last().unwrap() else {
            panic!("user");
        };
        assert!(
            matches!(&user.content[2], ContentBlock::Image(image) if image.base64 == "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==")
        );
        assert!(
            matches!(&user.content[3], ContentBlock::Document(document) if document.base64 == "JVBERi0xLjQ=")
        );
    }
    for (index, invalid) in [
        json!({"path":"/tmp/report.pdf"}),
        json!([{"type":"image","path":"/tmp/image.png"}]),
        json!([{"type":"document","base64":"bm90IHBkZg==","mimeType":"application/pdf"}]),
        json!([{"type":"image","base64":"invalid","mimeType":"image/png"}]),
        json!([]),
    ]
    .into_iter()
    .enumerate()
    {
        let submission = format!("invalid-{index}");
        let (ack, events) = fixture
            .submit(20 + index as u64, &submission, invalid)
            .await;
        if ack.get("error").is_none() {
            let event = fixture.event(&submission, events).await;
            assert!(event.get("error").is_some(), "{event}");
            assert!(event.get("sideQuestion").is_none(), "{event}");
        }
        assert_eq!(fixture.requests.lock().unwrap().len(), 2);
        assert_eq!(fixture.session.history(), before);
    }
    let schema = schema_bundle();
    let input = &schema["requests"]["session/sideQuestion"]["params"]["properties"]["question"];
    assert_eq!(input["title"], "rawInput");
    assert_eq!(input["anyOf"][0]["type"], "string");
    assert_eq!(input["anyOf"][1]["type"], "array");
    fixture.client.close().await;
}

#[tokio::test]
async fn readonly_side_question_named_facade_accepts_fifty_mib_binary_payload() {
    use base64::Engine as _;
    // Native document accounting stays within the captured provider capacity;
    // the transport byte limit does not change model admission.
    let mut fixture = Fixture::new().await;
    // Maximum-size inline payload preprocessing needs a separate debug-build budget.
    // Other requests keep the ordinary reply deadline.
    fixture.reply_timeout = Duration::from_secs(30);
    let snapshot = fixture.session.context_snapshot();
    assert_eq!(snapshot.context_window, 200_000);
    let mut bytes = vec![b' '; 10 * 1024 * 1024];
    bytes[..8].copy_from_slice(b"%PDF-1.4");
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let parts = vec![json!({"type":"document","base64":encoded,"mimeType":"application/pdf"}); 5];
    let probe = Message::User(klaus_agent_core::schemas::message::UserMessage {
        question_answer: None,
        uuid: "size-probe".into(),
        content: (0..5)
            .map(|_| {
                ContentBlock::Document(klaus_agent_core::schemas::content::DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: encoded.as_str().into(),
                    filename: None,
                })
            })
            .collect(),
        is_meta: None,
    });
    assert_eq!(
        klaus_agent_core::context::tokens::estimate_message_tokens(&probe),
        10_000
    );
    eprintln!(
        "readonly payload probe: decoded_bytes={}, estimated_tokens={}, context_window={}",
        bytes.len() * 5,
        klaus_agent_core::context::tokens::estimate_message_tokens(&probe),
        fixture.session.context_snapshot().context_window
    );
    drop(probe);
    let (ack, events) = fixture.submit(10, "fifty-mib", json!(parts)).await;
    assert!(ack.get("error").is_none(), "{ack}");
    let event = fixture.event("fifty-mib", events).await;
    assert_eq!(event["sideQuestion"]["response"], "side answer", "{event}");
    {
        let calls = fixture.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].tools.is_empty());
        assert_eq!(calls[0].skip_cache_write, Some(true));
        let request_tokens = klaus_agent_core::context::tokens::estimate_context_tokens_with_prefix(
            &calls[0].messages,
            klaus_agent_core::context::tokens::estimate_prefix_tokens(
                &calls[0].system,
                &calls[0].tools,
            ),
        );
        assert!(
            request_tokens
                < snapshot
                    .context_window
                    .saturating_sub(snapshot.output_reserve_tokens),
            "the complete request must fit the captured input budget: {request_tokens}"
        );
        eprintln!("readonly complete request probe: estimated_tokens={request_tokens}, context_window={}, output_reserve={}", snapshot.context_window, snapshot.output_reserve_tokens);
        let Message::User(user) = calls[0].messages.last().unwrap() else {
            panic!("user");
        };
        let documents = user
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Document(document) => Some(document),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(documents.len(), 5);
        for document in documents {
            assert_eq!(document.base64, encoded.as_str());
        }
    }
    assert!(fixture.session.history().is_empty());
    fixture.client.close().await;
}

#[tokio::test]
async fn readonly_side_question_named_cancel_aborts_only_the_exact_request() {
    let mut fixture = Fixture::new().await;
    fixture.hold.store(true, Ordering::SeqCst);
    let before = fixture.session.history();
    for (id, submission) in [(10, "held-one"), (11, "held-two")] {
        let (ack, _) = fixture.submit(id, submission, question()).await;
        assert!(ack.get("error").is_none(), "{ack}");
        tokio::time::timeout(REPLY_TIMEOUT, fixture.entered.notified())
            .await
            .unwrap();
    }
    for (id, target) in [(12, "not-the-request"), (13, "held-one")] {
        fixture
            .client
            .send(json!({"id":id,"method":"session/cancelRequest","params":{
                "sessionId":"readonly-side","requestId":target
            }}))
            .await;
        let (ack, events) = fixture.client.response_for(id).await;
        assert!(ack.get("error").is_none(), "{ack}");
        if id == 12 {
            assert!(fixture
                .signals
                .lock()
                .unwrap()
                .iter()
                .all(|signal| !signal.is_cancelled()));
        } else {
            let event = fixture.event("held-one", events).await;
            assert!(
                event["error"]
                    .to_string()
                    .contains("side question cancelled"),
                "{event}"
            );
        }
    }
    assert!(fixture.signals.lock().unwrap()[0].is_cancelled());
    assert!(!fixture.signals.lock().unwrap()[1].is_cancelled());
    assert_eq!(fixture.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.session.history(), before);
    fixture
        .client
        .send(json!({"id":14,"method":"session/cancelRequest","params":{
            "sessionId":"readonly-side","requestId":"held-two"
        }}))
        .await;
    let (_, events) = fixture.client.response_for(14).await;
    fixture.event("held-two", events).await;
    assert!(fixture.signals.lock().unwrap()[1].is_cancelled());
    assert_eq!(fixture.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.session.history(), before);
    fixture.client.close().await;
}
