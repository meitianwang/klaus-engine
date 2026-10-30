//! Defect: background classifications must govern real pending actions through the native transport.
//! Root cause: isolated sampler checks do not exercise evidence publication and the approval race.
//! Work item: 20261019 adaptive action review and current authorization enforcement.

#![cfg(unix)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{EnqueueOptions, Input, Session, TurnOptions};
use klaus_agent_core::provider::interceptor::{ProviderRequestInterceptor, ProviderResponseInfo};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::runtime_json;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::approval_review::{
    ApprovalReviewDecisionSource, ApprovalReviewRiskLevel, ApprovalReviewStatus,
    ApprovalReviewUserAuthorization,
};
use klaus_agent_core::types::permission::{ApprovalHandler, ApprovalRequest, ApprovalResponse};
use klaus_agent_core::types::provider::ModelCapabilities;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;

use super::r7c876d49_spec_restores_latest_permission_policy::agent_options;

const PARENT_MODEL: &str = "adaptive-parent";
const REVIEW_MODEL: &str = "adaptive-private-review";
const CLASSIFIER_MODEL: &str = "gpt-5.6-luna";
const CALL_ID: &str = "adaptive-file-write";
const CANCEL: &str = "Do not create the output file. The earlier requested action is cancelled.";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    CachedLow,
    HighFallback,
    ChangedAuthorization,
    TimeoutThenAllow,
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("adaptive-review-{}", uuid::Uuid::new_v4()));
        for child in ["workspace", "outside", "config"] {
            std::fs::create_dir_all(root.join(child)).unwrap();
        }
        Self(root)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct Control {
    requests: Mutex<Vec<Value>>,
    main_calls: AtomicUsize,
    sync_calls: AtomicUsize,
    classifier_calls: AtomicUsize,
    sync_cancelled: AtomicUsize,
    provider_retries: AtomicUsize,
    review_establishments: Mutex<Vec<(u32, bool, Option<u16>)>>,
    classifier_started: Notify,
    sync_started: Notify,
    classifier_release: Notify,
    sync_release: Notify,
}

struct EstablishmentRecorder(Arc<Control>);
impl ProviderRequestInterceptor for EstablishmentRecorder {
    fn after_response(&self, response: &ProviderResponseInfo<'_>) {
        if response.model == REVIEW_MODEL {
            let mut outcomes = self.0.review_establishments.lock().unwrap();
            assert!(
                outcomes.len() < 8,
                "review establishment observations are bounded"
            );
            outcomes.push((response.attempt, response.ok, response.status));
        }
    }
}

struct Server {
    url: String,
    control: Arc<Control>,
    stop: AbortSignal,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Server {
    async fn start(mode: Mode, output: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let control = Arc::new(Control::default());
        let stop = AbortSignal::new();
        let task = {
            let control = control.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut connections = JoinSet::new();
                let mut accepted = 0;
                loop {
                    tokio::select! {
                        biased;
                        () = stop.cancelled() => break,
                        joined = connections.join_next(), if !connections.is_empty() => {
                            joined.unwrap().expect("loopback connection completed without panic");
                        }
                        next = listener.accept() => {
                            let (stream, _) = next.unwrap();
                            accepted += 1;
                            assert!(accepted <= 128, "the loopback connection script is bounded");
                            let control = control.clone();
                            let stop = stop.clone();
                            let output = output.clone();
                            connections.spawn(async move {
                                serve(stream, mode, output, control, stop).await;
                            });
                        }
                    }
                }
                connections.abort_all();
                while let Some(joined) = connections.join_next().await {
                    if let Err(error) = joined {
                        assert!(
                            error.is_cancelled(),
                            "loopback connection panicked: {error}"
                        );
                    }
                }
            })
        };
        Self {
            url,
            control,
            stop,
            task: Some(task),
        }
    }

    async fn close(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn request(stream: &mut TcpStream) -> Option<Value> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 32 * 1024, "loopback headers are bounded");
        let mut byte = [0];
        if stream.read_exact(&mut byte).await.is_err() {
            return None;
        }
        header.push(byte[0]);
    }
    let header = String::from_utf8(header).unwrap();
    if header.starts_with("GET ") {
        let _ = stream.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
        return None;
    }
    assert!(header.starts_with("POST /responses "));
    let length = header
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
        .expect("native JSON requests carry a content length");
    assert!(length <= 2 * 1024 * 1024, "loopback bodies are bounded");
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    Some(serde_json::from_slice(&body).unwrap())
}

async fn frame(stream: &mut TcpStream, value: Value) -> bool {
    stream
        .write_all(format!("data: {value}\n\n").as_bytes())
        .await
        .is_ok()
}

async fn text_response(
    stream: &mut TcpStream,
    id: &str,
    model: &str,
    text: &str,
    input: usize,
    output: usize,
) {
    let item = json!({"id":format!("msg-{id}"),"type":"message","role":"assistant",
        "content":[{"type":"output_text","text":text}]});
    if !frame(
        stream,
        json!({"type":"response.output_item.added","output_index":0,"item":{
            "id":format!("msg-{id}"),"type":"message","role":"assistant","content":[]
        }}),
    )
    .await
    {
        return;
    }
    if !frame(
        stream,
        json!({"type":"response.output_text.delta","item_id":format!("msg-{id}"),"output_index":0,"content_index":0,"delta":text}),
    )
    .await
    {
        return;
    }
    if !frame(
        stream,
        json!({"type":"response.output_item.done","item":item.clone()}),
    )
    .await
    {
        return;
    }
    let _ = frame(
        stream,
        json!({"type":"response.completed","response":{
            "id":id,"model":model,"status":"completed","output":[item],
            "usage":{"input_tokens":input,"output_tokens":output,"total_tokens":input+output}
        }}),
    )
    .await;
}

async fn serve(
    mut stream: TcpStream,
    mode: Mode,
    output: PathBuf,
    control: Arc<Control>,
    stop: AbortSignal,
) {
    let Some(body) = request(&mut stream).await else {
        return;
    };
    {
        let mut records = control.requests.lock().unwrap();
        assert!(records.len() < 32, "the model request log is bounded");
        records.push(body.clone());
    }
    let model = body["model"].as_str().expect("request model");
    let review_index =
        (model == REVIEW_MODEL).then(|| control.sync_calls.fetch_add(1, Ordering::SeqCst));
    if mode == Mode::TimeoutThenAllow && review_index == Some(0) {
        control.sync_started.notify_one();
        tokio::select! {
            () = stop.cancelled() => return,
            () = control.sync_release.notified() => {},
        }
        let error = json!({"error":{"type":"request_timeout","code":"request_timeout","message":"The approval request timed out before a response was established."}}).to_string();
        stream.write_all(format!("HTTP/1.1 408 Request Timeout\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{error}", error.len()).as_bytes()).await.unwrap();
        return;
    }
    let _ = stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )
        .await;
    let id = format!("response-{}", uuid::Uuid::new_v4());
    if !frame(
        &mut stream,
        json!({"type":"response.created","response":{"id":id,"model":model}}),
    )
    .await
    {
        return;
    }
    if model == CLASSIFIER_MODEL {
        control.classifier_calls.fetch_add(1, Ordering::SeqCst);
        control.classifier_started.notify_one();
        tokio::select! {
            () = stop.cancelled() => return,
            () = control.classifier_release.notified() => {}
        }
        text_response(
            &mut stream,
            &id,
            model,
            if matches!(mode, Mode::HighFallback | Mode::TimeoutThenAllow) {
                "high"
            } else {
                "low"
            },
            40,
            2,
        )
        .await;
    } else if model == REVIEW_MODEL {
        let index = review_index.expect("a synchronous review has an establishment index");
        if index == 0 {
            control.sync_started.notify_one();
            let mut probe = [0];
            tokio::select! {
                () = stop.cancelled() => return,
                () = control.sync_release.notified() => {},
                read = stream.read(&mut probe) => {
                    match read {
                        Ok(0) => {}
                        Err(error) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::BrokenPipe) => {}
                        other => panic!("a held response must close when cancelled: {other:?}"),
                    }
                    control.sync_cancelled.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            }
        }
        let text = if mode == Mode::TimeoutThenAllow
            || (mode == Mode::ChangedAuthorization && index == 0)
        {
            r#"{"outcome":"allow"}"#
        } else {
            r#"{"outcome":"deny","risk_level":"high","user_authorization":"low","rationale":"The pending action is not authorized by the current request."}"#
        };
        text_response(&mut stream, &id, model, text, 7, 3).await;
    } else {
        assert_eq!(model, PARENT_MODEL);
        if control.main_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let args = json!({"path":output.to_string_lossy(),"content":"approved through current classification"}).to_string();
            let item = json!({"type":"function_call","id":"call-native-write","call_id":CALL_ID,"name":"write","arguments":args});
            let _ = frame(&mut stream, json!({"type":"response.output_item.added","output_index":0,"item":{
                "type":"function_call","id":"call-native-write","call_id":CALL_ID,"name":"write","arguments":""
            }})).await;
            let _ = frame(&mut stream, json!({"type":"response.function_call_arguments.delta","item_id":"call-native-write","output_index":0,"delta":args})).await;
            let _ = frame(&mut stream, json!({"type":"response.function_call_arguments.done","item_id":"call-native-write","output_index":0,"arguments":args})).await;
            let _ = frame(
                &mut stream,
                json!({"type":"response.output_item.done","output_index":0,"item":item.clone()}),
            )
            .await;
            let _ = frame(
                &mut stream,
                json!({"type":"response.completed","response":{
                    "id":id,"model":model,"status":"completed","output":[item],
                    "usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6}
                }}),
            )
            .await;
        } else {
            text_response(
                &mut stream,
                &id,
                model,
                "The actual tool result has been received.",
                5,
                1,
            )
            .await;
        }
    }
}

#[derive(Default)]
struct Human(AtomicUsize);
#[async_trait]
impl ApprovalHandler for Human {
    async fn approve(&self, request: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        assert_eq!(request.tool_name, "write");
        assert!(!signal.is_cancelled());
        self.0.fetch_add(1, Ordering::SeqCst);
        ApprovalResponse::Deny {
            message: Some("No manual approval was supplied.".into()),
        }
    }
}

struct Rig {
    directory: Directory,
    output: PathBuf,
    server: Server,
    session: Arc<Session>,
    process: RuntimeProcessContext,
    human: Arc<Human>,
    signal: AbortSignal,
}
impl Rig {
    async fn new(mode: Mode) -> Self {
        let directory = Directory::new();
        let output = directory.0.join("outside/result.txt");
        let server = Server::start(mode, output.clone()).await;
        let metadata = ModelCapabilities {
            tools: Some(true),
            structured_output: true,
            context_window: Some(200_000),
            max_output_tokens: Some(8192),
            supported_reasoning_efforts: vec!["low".into()],
            ..Default::default()
        };
        let mut parent = metadata.clone();
        parent.auto_review_model_override = Some(REVIEW_MODEL.into());
        parent.guardian =
            Some(runtime_json::parse(&r#"{"file_changes":"adaptive"}"#.into()).unwrap());
        if mode == Mode::HighFallback {
            parent.guardian_v2 = Some(
                runtime_json::parse(&r#"{"async_classifier_mode":"conversation"}"#.into()).unwrap(),
            );
        }
        let retry_control = server.control.clone();
        let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("loopback-key".into()),
            base_url: Some(server.url.clone()),
            default_model: Some(PARENT_MODEL.into()),
            api: Some(OpenAiApi::Responses),
            interceptor: Some(Arc::new(EstablishmentRecorder(server.control.clone()))),
            retry: Some(RetryOptions {
                on_retry: Some(Arc::new(move |info| {
                    assert!(info.attempt > 0);
                    retry_control
                        .provider_retries
                        .fetch_add(1, Ordering::SeqCst);
                })),
                ..Default::default()
            }),
            capabilities: Some(metadata.clone()),
            capabilities_by_model: Some(HashMap::from([
                (PARENT_MODEL.to_owned(), parent),
                (REVIEW_MODEL.to_owned(), metadata.clone()),
                (CLASSIFIER_MODEL.to_owned(), metadata),
            ])),
            ..Default::default()
        }));
        let process = RuntimeProcessContext::initialize(
            klaus_agent_core::testing::shared_test_process_supervisor(),
        )
        .unwrap();
        let human = Arc::new(Human::default());
        let mut options = agent_options(process.clone());
        options.provider = provider;
        options.tools = Some(vec![create_write_tool()]);
        options.approval_handler = Some(human.clone());
        options.include_default_agents = Some(false);
        options.cwd = Some(directory.0.join("workspace").to_string_lossy().into_owned());
        options.config = Some(json!({
            "model":PARENT_MODEL,"permissionMode":"default","approvalPolicy":"on-request",
            "sandboxMode":"workspace-write","approvalsReviewer":"automatic","approvalReview":false,
            "sandbox_workspace_write":{"exclude_tmpdir_env_var":true,"exclude_slash_tmp":true},
            "sandbox":"auto","requireRealSandbox":true,"sandboxNetworkAllowed":false,"sandboxRetryOnDenial":true,
            "maxOutputTokens":4096,"useStoredPermissionRules":false,"prewarm":false,
            "goal":false,"autoMemory":false,"agentMemory":false,"autoDream":false,
            "memoryRecall":false,"memory":{"enabled":false},"loopDetection":false
        }));
        let session = Agent::new(options).start_session(StartSessionOptions {
            user_config_dir: Some(
                directory
                    .0
                    .join("config")
                    .to_string_lossy()
                    .into_owned()
                    .into(),
            ),
            ..Default::default()
        });
        Self {
            directory,
            output,
            server,
            session,
            process,
            human,
            signal: AbortSignal::new(),
        }
    }

    fn spawn(&self) -> tokio::task::JoinHandle<()> {
        let session = self.session.clone();
        let signal = self.signal.clone();
        tokio::spawn(async move {
            session
                .run(
                    Input::Text("Create the requested output file outside the workspace.".into()),
                    TurnOptions {
                        signal: Some(signal),
                        ..Default::default()
                    },
                )
                .await;
        })
    }

    async fn wait_for_race(&self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            self.server.control.classifier_started.notified().await;
            self.server.control.sync_started.notified().await;
        })
        .await
        .expect("native classifier and synchronous review both start");
    }

    async fn wait_for_classifier_usage(&self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self
                    .session
                    .cost_snapshot()
                    .models
                    .iter()
                    .any(|row| row.totals.model == CLASSIFIER_MODEL && row.totals.requests == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the completed native classifier usage is drained");
    }

    async fn finish(&mut self, turn: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(20), turn)
            .await
            .unwrap()
            .unwrap();
        self.session.shutdown_and_wait().await.unwrap();
        assert!(self.directory.0.join("workspace").is_dir());
        let usage = self.session.cost_snapshot();
        let classifier = usage
            .models
            .iter()
            .find(|row| row.totals.model == CLASSIFIER_MODEL)
            .expect("terminal classifier usage is charged to its actual model");
        assert_eq!(classifier.totals.input_tokens, 40);
        assert_eq!(classifier.totals.output_tokens, 2);
        assert_eq!(classifier.totals.requests, 1);
        assert_eq!(self.human.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.server.control.classifier_calls.load(Ordering::SeqCst),
            1
        );
        self.process.shutdown().await.unwrap();
        self.server.close().await;
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        self.signal.cancel();
    }
}

#[tokio::test]
async fn native_low_classification_wins_the_held_review_and_executes_the_real_file_action() {
    let mut rig = Rig::new(Mode::CachedLow).await;
    let turn = rig.spawn();
    rig.wait_for_race().await;
    rig.server.control.classifier_release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), async {
        while std::fs::read(&rig.output).ok().as_deref()
            != Some(b"approved through current classification".as_slice())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the current cached low score permits the actual write");
    assert_eq!(
        std::fs::read_to_string(&rig.output).unwrap(),
        "approved through current classification"
    );
    let review = rig
        .session
        .approval_review_history()
        .last()
        .cloned()
        .unwrap();
    assert_eq!(review.status, ApprovalReviewStatus::Approved);
    assert!(review.decision_source.is_none());
    assert!(review.risk_level.is_none());
    assert!(review.user_authorization.is_none());
    {
        let requests = rig.server.control.requests.lock().unwrap();
        let classifier = requests
            .iter()
            .find(|value| value["model"] == CLASSIFIER_MODEL)
            .unwrap();
        assert_eq!(classifier["reasoning"]["effort"], "low");
        assert_eq!(classifier["store"], false);
        assert_eq!(classifier["tool_choice"], "none");
        assert!(classifier.get("tools").is_none());
        assert!(classifier["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "additional_tools" && item.get("id").is_some()));
    }
    let control = rig.server.control.clone();
    tokio::time::timeout(Duration::from_secs(15), async {
        while control.sync_cancelled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the losing synchronous response is closed and drained");
    rig.finish(turn).await;
    assert_eq!(control.sync_calls.load(Ordering::SeqCst), 1);
    assert_eq!(control.sync_cancelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_high_retained_classification_keeps_the_real_synchronous_assessment_authoritative() {
    let mut rig = Rig::new(Mode::HighFallback).await;
    let turn = rig.spawn();
    rig.wait_for_race().await;
    rig.server.control.classifier_release.notify_one();
    rig.wait_for_classifier_usage().await;
    rig.server.control.sync_release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if rig
                .session
                .approval_review_history()
                .last()
                .is_some_and(|event| event.completed_at_ms.is_some())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!rig.output.exists());
    let review = rig
        .session
        .approval_review_history()
        .last()
        .cloned()
        .unwrap();
    assert_eq!(review.status, ApprovalReviewStatus::Denied);
    assert_eq!(
        review.decision_source,
        Some(ApprovalReviewDecisionSource::Agent)
    );
    rig.finish(turn).await;
}

#[tokio::test]
async fn a_late_native_low_score_cannot_authorize_an_action_after_the_user_cancels_it() {
    let mut rig = Rig::new(Mode::ChangedAuthorization).await;
    let turn = rig.spawn();
    rig.wait_for_race().await;
    rig.session
        .enqueue(Input::Text(CANCEL.into()), EnqueueOptions::default())
        .unwrap();
    rig.server.control.classifier_release.notify_one();
    rig.wait_for_classifier_usage().await;
    rig.server.control.sync_release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if rig.server.control.sync_calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("changed user evidence requires a fresh synchronous assessment");
    let session = rig.session.clone();
    let output = rig.output.clone();
    let control = rig.server.control.clone();
    rig.finish(turn).await;
    assert!(!output.exists());
    assert_eq!(control.sync_calls.load(Ordering::SeqCst), 2);
    let review = session.approval_review_history().last().cloned().unwrap();
    assert_eq!(review.status, ApprovalReviewStatus::Denied);
    assert_eq!(
        review.decision_source,
        Some(ApprovalReviewDecisionSource::Agent)
    );
    let requests = control.requests.lock().unwrap();
    let revised = requests
        .iter()
        .filter(|value| value["model"] == REVIEW_MODEL)
        .nth(1)
        .unwrap();
    assert!(revised["input"].to_string().contains(CANCEL));
}

#[tokio::test]
async fn an_actual_http_408_restarts_synchronous_review_before_allowing_the_pending_file_write() {
    let mut rig = Rig::new(Mode::TimeoutThenAllow).await;
    let turn = rig.spawn();
    rig.wait_for_race().await;
    rig.server.control.classifier_release.notify_one();
    rig.wait_for_classifier_usage().await;
    assert!(
        !rig.output.exists(),
        "a high classification cannot approve the action"
    );
    rig.server.control.sync_release.notify_one();
    rig.finish(turn).await;
    assert_eq!(
        std::fs::read_to_string(&rig.output).unwrap(),
        "approved through current classification"
    );
    let control = &rig.server.control;
    assert_eq!(control.sync_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        control.provider_retries.load(Ordering::SeqCst),
        0,
        "HTTP408 must reach the outer review retry instead of establishment retry"
    );
    let outcomes = control.review_establishments.lock().unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0], (1, false, Some(408)));
    assert_eq!(
        outcomes[1].0, 1,
        "the successful review starts a fresh provider attempt"
    );
    assert!(outcomes[1].1);
    drop(outcomes);
    let requests = control.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|body| body["model"] == REVIEW_MODEL)
            .count(),
        2
    );
    let review = rig
        .session
        .approval_review_history()
        .last()
        .cloned()
        .unwrap();
    assert_eq!(review.status, ApprovalReviewStatus::Approved);
    assert_eq!(
        review.decision_source,
        Some(ApprovalReviewDecisionSource::Agent)
    );
    assert_eq!(review.risk_level, Some(ApprovalReviewRiskLevel::Low));
    assert_eq!(
        review.user_authorization,
        Some(ApprovalReviewUserAuthorization::Unknown)
    );
}
