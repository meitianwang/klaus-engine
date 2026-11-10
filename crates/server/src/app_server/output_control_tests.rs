use super::*;
use futures::task::AtomicWaker;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, DuplexStream, WriteHalf};

#[derive(Default)]
pub(super) struct OutputGate {
    paused: AtomicBool,
    blocked: AtomicUsize,
    wake: AtomicWaker,
}

impl OutputGate {
    pub(super) fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    pub(super) fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.wake.wake();
    }
    pub(super) async fn fill(&self, client: &mut TestClient) {
        self.pause();
        for id in 10000..10256 {
            client
                .send(json!({"id":id,"method":"output-fixture-unavailable","params":{}}))
                .await;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.blocked.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the actual facade writer must reach the paused transport");
    }
}

struct GatedOutput {
    inner: WriteHalf<DuplexStream>,
    gate: Arc<OutputGate>,
}
impl AsyncWrite for GatedOutput {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.gate.paused.load(Ordering::SeqCst) {
            self.gate.wake.register(cx.waker());
            if self.gate.paused.load(Ordering::SeqCst) {
                self.gate.blocked.fetch_add(1, Ordering::SeqCst);
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) fn client(
    create_session: CreateSession,
    root: &std::path::Path,
) -> (TestClient, Arc<OutputGate>) {
    client_with_observer(create_session, root, None)
}

fn client_with_observer(
    create_session: CreateSession,
    root: &std::path::Path,
    observer: Option<tokio::sync::oneshot::Sender<Arc<super::super::service::AppServerState>>>,
) -> (TestClient, Arc<OutputGate>) {
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server_side);
    let (client_read, client_write) = tokio::io::split(client_side);
    let gate = Arc::new(OutputGate::default());
    let output = GatedOutput {
        inner: server_write,
        gate: gate.clone(),
    };
    let options = AppServerOptions {
        windows_sandbox_context: None,
        create_session,
        credential_store: None,
        lifecycle: None,
        plugin_admin: None,
        permission_rules_path: Some(root.join("permissions.json")),
        config_path: Some(root.join("config.json")),
        startup_init_ack_budget: None,
        session_unload_delay: None,
    };
    let server = tokio::spawn(async move {
        match observer {
            Some(observer) => {
                super::super::service::run_app_server_observed(
                    server_read,
                    output,
                    options,
                    observer,
                )
                .await
            }
            None => run_app_server(server_read, output, options).await,
        }
    });
    (
        TestClient {
            writer: client_write,
            lines: BufReader::new(client_read).lines(),
            server,
            diagnostic: None,
        },
        gate,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_targeted_stop_cannot_overtake_an_earlier_queued_start_under_output_backpressure() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |request, _| {
            let next_input = request.messages.iter().any(|message| matches!(message,
            klaus_agent_core::schemas::message::Message::User(user) if user.content.iter().any(|block| matches!(block,
                klaus_agent_core::schemas::content::ContentBlock::Text(text) if text.text.contains("after targeted stop")
            ))
        ));
            count.fetch_add(1, Ordering::SeqCst);
            if next_input {
                Box::pin(futures::stream::iter(text_turn("new input completed")))
            } else {
                Box::pin(futures::stream::pending())
            }
        }),
        Some(
            json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false,"memoryRecall":false}),
        ),
    );
    let home = root.path().to_string_lossy().into_owned();
    let create: CreateSession = Arc::new(move |ctx| {
        let session = agent.start_session(StartSessionOptions {
            id: ctx.init.thread_id,
            cwd: Some(home.clone().into()),
            user_config_dir: Some(home.clone().into()),
            durable_permissions: Some(ctx.durable_permissions),
            approval_handler: Some(ctx.approval_handler),
            ..Default::default()
        });
        Box::pin(async move { Ok(session) })
    });
    let (send_state, receive_state) = tokio::sync::oneshot::channel();
    let (mut client, gate) = client_with_observer(create, root.path(), Some(send_state));
    let state = receive_state.await.unwrap();
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (started, _) = client.response_for(2).await;
    assert!(started.get("error").is_none(), "{started}");
    let thread = started["result"]["sessionId"].as_str().unwrap().to_owned();
    client.send(json!({"id":3,"method":"thread/queue/add","params":{
        "threadId":thread,"clientUserMessageId":"queued-start","input":[{"type":"text","text":"wait for targeted stop"}]
    }})).await;
    let (added, _) = client.response_for(3).await;
    assert!(added.get("error").is_none(), "{added}");
    let queued = added["result"]["queuedSubmission"]["id"].clone();
    gate.fill(&mut client).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.outgoing_capacity() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all outgoing admission slots are occupied");
    client.send(json!({"id":20000,"method":"thread/queue/start","params":{
        "threadId":thread,"queuedSubmissionId":queued,"submissionId":"ordered-start","mode":"start-if-idle"
    }})).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.queued_requests(&thread) != Some(0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real queue start is suspended before mutation admission");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client
        .send(json!({"id":20001,"method":"turn/interrupt","params":{
            "sessionId":thread,"submissionId":"ordered-stop","expectedTurnId":"ordered-start"
        }}))
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.queued_requests(&thread) != Some(1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the later targeted stop waits behind the actual start");
    gate.resume();
    let (start_reply, _) = client.response_for(20000).await;
    assert_eq!(start_reply["result"]["status"], "started", "{start_reply}");
    let (stop_reply, mut seen) = client.response_for(20001).await;
    assert_eq!(
        stop_reply["result"]["submissionId"], "ordered-stop",
        "{stop_reply}"
    );
    let stop = tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            if let Some(frame) = seen.iter().find(|frame| {
                frame["params"]["event"]["submissionId"] == "ordered-stop"
                    && frame["params"]["event"].get("turnInterrupt").is_some()
            }) {
                break frame["params"]["event"].clone();
            }
            seen.push(client.next_message().await);
        }
    })
    .await
    .expect("the targeted cancellation settles after start admission");
    assert_eq!(stop["turnInterrupt"]["accepted"], true, "{stop}");
    assert_eq!(
        stop["turnInterrupt"]["interruptedTurnId"], "ordered-start",
        "{stop}"
    );
    client.send(json!({"id":20002,"method":"turn/submit","params":{
        "sessionId":thread,"submissionId":"after-stop","mode":"start-if-idle","input":"after targeted stop"
    }})).await;
    let (next_reply, mut seen) = client.response_for(20002).await;
    assert_eq!(next_reply["result"]["status"], "started", "{next_reply}");
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while !seen.iter().any(|frame| {
            frame["method"] == "turn/completed" && frame["params"]["turnId"] == "after-stop"
        }) {
            seen.push(client.next_message().await);
        }
    })
    .await
    .expect("new input completes after the targeted stop");
    drop(state);
    client.close().await;
}

struct ApprovalProbe {
    schema: klaus_agent_core::types::json::JsonSchema,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl klaus_agent_core::types::tool::Tool for ApprovalProbe {
    fn static_name(&self) -> &str {
        "output_approval_probe"
    }
    fn static_description(&self) -> &str {
        "Exercise approval without external effects"
    }
    fn static_input_schema(&self) -> &klaus_agent_core::types::json::JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: klaus_agent_core::types::json::Json,
        _: &klaus_agent_core::types::tool::ToolContext,
        _: Option<klaus_agent_core::types::tool::ToolProgress>,
    ) -> Result<klaus_agent_core::types::tool::ToolResult, klaus_agent_core::types::errors::ToolError>
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(klaus_agent_core::tools::build_tool::text_result(
            klaus_agent_core::runtime_values::RuntimeJson::Null,
            "approved fixture completed",
            false,
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_output_does_not_block_a_reverse_approval_response() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let model_calls = Arc::new(AtomicUsize::new(0));
    let model_count = model_calls.clone();
    let agent = make_agent_with_call_model_and_config(
        Box::new(move |_, _| {
            let events = if model_count.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut events = vec![ev::message_start(None)];
                events.extend(ev::tool_use(
                    "approval-probe",
                    "output_approval_probe",
                    &klaus_agent_core::runtime_values::RuntimeJson::Object(Default::default()),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                events
            } else {
                text_turn("approval completed")
            };
            Box::pin(futures::stream::iter(events))
        }),
        Some(
            json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false,
        "memoryRecall":false,"approval_policy":"on-request","sandbox_mode":"read-only","approvals_reviewer":"user"}),
        ),
    );
    let tool: Arc<dyn klaus_agent_core::types::tool::Tool> = Arc::new(ApprovalProbe {
        schema: json!({"type":"object","properties":{},"additionalProperties":false})
            .as_object()
            .unwrap()
            .clone(),
        calls: calls.clone(),
    });
    let home = root.path().to_string_lossy().into_owned();
    let create: CreateSession = Arc::new(move |ctx| {
        let session = agent.start_session(StartSessionOptions {
            id: ctx.init.thread_id,
            cwd: Some(home.clone().into()),
            user_config_dir: Some(home.clone().into()),
            tools: Some(vec![tool.clone()]),
            durable_permissions: Some(ctx.durable_permissions),
            approval_handler: Some(ctx.approval_handler),
            permission_mode: Some(ExternalPermissionMode::Default),
            ..Default::default()
        });
        Box::pin(async move { Ok(session) })
    });
    let (mut client, gate) = client(create, root.path());
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (started, _) = client.response_for(2).await;
    assert!(started.get("error").is_none(), "{started}");
    let id = started["result"]["sessionId"].clone();
    client.send(json!({"id":3,"method":"turn/submit","params":{
        "sessionId":id,"submissionId":"approval-turn","mode":"start-if-idle","input":"exercise the approval boundary"
    }})).await;
    let (reply, mut seen) = client.response_for(3).await;
    assert!(reply.get("error").is_none(), "{reply}");
    let request = loop {
        if let Some(request) = seen
            .iter()
            .find(|frame| frame["method"] == "approval/request")
        {
            break request.clone();
        }
        seen.push(client.next_message().await);
    };
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the tool must actually wait for approval"
    );
    gate.fill(&mut client).await;
    client
        .send(json!({"id":request["id"],"result":{"response":{"decision":"allow"}}}))
        .await;
    let applied = tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    gate.resume();
    assert!(
        applied.is_ok(),
        "reverse approval must reach its waiting tool while output remains full"
    );
    tokio::time::timeout(REPLY_TIMEOUT, async {
        while !seen.iter().any(|frame| {
            frame["method"] == "turn/completed" && frame["params"]["turnId"] == "approval-turn"
        }) {
            seen.push(client.next_message().await);
        }
    })
    .await
    .expect("the approved turn must complete after output resumes");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(model_calls.load(Ordering::SeqCst), 2);
    client.close().await;
}
