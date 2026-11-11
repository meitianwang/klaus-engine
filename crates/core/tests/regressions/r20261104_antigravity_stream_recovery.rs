//! Defect 20261104: Antigravity turns died as `antigravity_stream_incomplete`
//! after long agent streams. An unclean close stays a bounded failure and must
//! not run a mutation. A clean HTTP EOF without `finishReason` completes on
//! the first request and keeps the streamed answer. A continuation token opens
//! the next leg instead of resampling the whole request.
//! Root cause: a finished leg and a broken read were both treated as a missing
//! terminal frame, and a checkpoint token was not used to continue.
//! Work item 20261104: interrupted reads stay network failures; a clean EOF
//! completes; a continuation token opens the next leg. This file pins that
//! public behaviour.
//! Mounted only by the server facade tests so it can reach the real
//! Antigravity `decorate`/`CloudClient` over a local HTTP fixture.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{Input, TurnOptions, TurnResult};
use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
    RuntimeProcessSupervisor,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::tools::builtins::bash::create_bash_tool;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::provider::{ModelCapabilities, ModelProvider};
use klaus_agent_protocol::types::ProtocolSessionInit;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::app_server::antigravity::{self, auth};

const MODEL: &str = "gemini-4-argon-medium";

fn decode<T: klaus_agent_core::runtime_values::RuntimeDecode>(value: Value) -> T {
    runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap()
}

fn process_supervisor() -> Arc<RuntimeProcessSupervisor> {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    static SUPERVISOR: OnceLock<Arc<RuntimeProcessSupervisor>> = OnceLock::new();
    SUPERVISOR
        .get_or_init(|| {
            let runtime = RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap()
            });
            RuntimeProcessSupervisor::initialize(runtime.handle().clone()).unwrap()
        })
        .clone()
}

/// One isolated config root; removed on drop so nothing reaches the real profile.
struct FixtureProfile {
    root: PathBuf,
    previous_config: Option<std::ffi::OsString>,
}

impl FixtureProfile {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("klaus-antigravity-stream-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let previous_config = std::env::var_os("KLAUS_CONFIG_DIR");
        antigravity::set_config_root(&root);
        Self {
            root,
            previous_config,
        }
    }
}

impl Drop for FixtureProfile {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous_config {
            set_process_environment_var("KLAUS_CONFIG_DIR", previous);
        } else {
            remove_process_environment_var("KLAUS_CONFIG_DIR");
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// One scripted SSE response leg. When `clean_eof` is false, the server uses
/// chunked transfer encoding and closes the socket without the terminal
/// zero-length chunk so the HTTP client observes a genuine mid-stream read error.
struct SseResponse {
    frames: String,
    clean_eof: bool,
}

/// A local SSE endpoint. `script` returns the response for the Nth model request.
struct SseFixture {
    endpoint: String,
    bodies: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for SseFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SseFixture {
    async fn start(script: impl Fn(usize) -> SseResponse + Send + 'static) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let captured = bodies.clone();
        let task = tokio::spawn(async move {
            let mut index = 0usize;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let Some(body) = read_request(&mut socket).await else {
                    continue;
                };
                captured
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap_or(Value::Null));
                let reply = script(index);
                index += 1;
                let response = if reply.clean_eof {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        reply.frames.len(),
                        reply.frames
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n{:x}\r\n{}\r\n",
                        reply.frames.len(),
                        reply.frames
                    )
                };
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        Self {
            endpoint,
            bodies,
            task,
        }
    }

    fn request_count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    let (header_end, length) = loop {
        let mut chunk = [0u8; 4096];
        let count = socket.read(&mut chunk).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..index]);
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            break (index + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let mut chunk = [0u8; 4096];
        let count = socket.read(&mut chunk).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Some(bytes[header_end..header_end + length].to_vec())
}

fn sse_frame(response: Value) -> String {
    format!("data: {response}\n\n")
}

/// Complete read-only + mutation tool calls, optionally terminated.
fn tool_call_frames(fixture: &Path, counter: &Path, terminated: bool) -> SseResponse {
    let read = fixture.join("fixture.txt");
    let mut frame = json!({
        "response": {
            "modelVersion": MODEL,
            "candidates": [{
                "content": {"role": "model", "parts": [
                    {"functionCall": {"id": "call-read", "name": "read",
                        "args": {"path": read.to_string_lossy()}}},
                    {"functionCall": {"id": "call-write", "name": "bash",
                        "args": {"command": format!("printf 'mutation\\n' >> {}", counter.display()),
                                 "description": "append one mutation line"}}}
                ]}
            }]
        }
    });
    if terminated {
        frame["response"]["candidates"][0]["finishReason"] = json!("STOP");
    }
    SseResponse {
        frames: sse_frame(frame),
        clean_eof: terminated,
    }
}

fn text_frames(text: &str, terminated: bool) -> SseResponse {
    let mut frame = json!({
        "response": {
            "modelVersion": MODEL,
            "candidates": [{"content": {"role": "model", "parts": [{"text": text}]}}]
        }
    });
    if terminated {
        frame["response"]["candidates"][0]["finishReason"] = json!("STOP");
    }
    SseResponse {
        frames: sse_frame(frame),
        clean_eof: terminated,
    }
}

async fn antigravity_provider(endpoint: &str) -> Arc<dyn ModelProvider> {
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture-token".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let capabilities = ModelCapabilities {
        context_window: Some(200_000),
        max_output_tokens: Some(8192),
        tools: Some(true),
        ..Default::default()
    };
    let base: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(MODEL.into()),
        capabilities: Some(capabilities),
        ..Default::default()
    }));
    let init: ProtocolSessionInit = decode(json!({
        "capabilitiesByModel": [],
        "modelSource": {"source": "antigravity", "modelId": MODEL}
    }));
    antigravity::decorate(base, &init)
}

async fn run_turn(root: PathBuf, provider: Arc<dyn ModelProvider>) -> TurnResult {
    let mut options = AgentOptions {
        process_context: RuntimeProcessContext::initialize(process_supervisor()).unwrap(),
        provider,
        config: Some(json!({
            "approval_policy": "never",
            "sandbox_mode": "danger-full-access"
        })),
        tools: Some(vec![create_read_tool(), create_bash_tool()]),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: None,
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    };
    options.include_default_agents = Some(false);
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        config: Some(json!({
            "model": MODEL, "prewarm": false, "thinking": false,
            "maxRetries": 2, "unboundedNetworkRetries": false,
            "autoMemory": false, "autoDream": false, "memoryRecall": false,
            "checkpointing": false, "speculativeCompaction": false,
            "earlyMicrocompact": false, "idleMicrocompact": false,
            "compactionSelfCheck": false, "loopDetection": false
        })),
        ..Default::default()
    });
    session
        .run(
            Input::Text("Update the fixture and report.".into()),
            TurnOptions::default(),
        )
        .await
}

/// A truncated stream after complete tool calls must recover through the bounded
/// resample, and the mutation from the recovered attempt runs exactly once; the
/// later truncated tool-result request must not replay that mutation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_antigravity_stream_recovers_and_runs_the_mutation_once() {
    let _profile = FixtureProfile::new();
    let project = tempfile::tempdir().unwrap();
    let root = project.path().to_path_buf();
    std::fs::write(root.join("fixture.txt"), "PONG").unwrap();
    let counter = root.join("mutation-count.txt");

    let fixture = SseFixture::start({
        let counter = counter.clone();
        let root = root.clone();
        move |index| match index {
            0 => tool_call_frames(&root, &counter, false),
            1 => {
                assert!(
                    !counter.exists(),
                    "the incomplete attempt must not mutate files"
                );
                tool_call_frames(&root, &counter, true)
            }
            2 => text_frames("still working", false),
            _ => text_frames("done", true),
        }
    })
    .await;
    let provider = antigravity_provider(&fixture.endpoint).await;
    let result = run_turn(root.clone(), provider).await;

    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        std::fs::read_to_string(&counter)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "the mutation must run exactly once across the truncated and recovered attempts"
    );
    assert!(
        fixture.request_count() == 4,
        "expected the truncated attempts to resample, saw {} requests",
        fixture.request_count()
    );
    {
        let requests = fixture.bodies.lock().unwrap();
        let parts: Vec<_> = requests[2]["request"]["contents"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|content| content["parts"].as_array().into_iter().flatten())
            .filter_map(|part| part.get("functionResponse"))
            .collect();
        assert_eq!(
            parts.len(),
            2,
            "only the successful attempt's results enter history"
        );
        assert!(parts.iter().any(|part| part.to_string().contains("PONG")));
        assert_eq!(
            requests[2]["request"]["contents"], requests[3]["request"]["contents"],
            "resampling after a completed mutation retains the same tool history"
        );
    }
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

/// A stream that never reaches a terminal frame must fail the turn (never be
/// reported as success) and must not run any mutation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_persistently_truncated_antigravity_stream_is_never_success() {
    let _profile = FixtureProfile::new();
    let project = tempfile::tempdir().unwrap();
    let root = project.path().to_path_buf();
    std::fs::write(root.join("fixture.txt"), "PONG").unwrap();
    let counter = root.join("mutation-count.txt");

    let fixture = SseFixture::start({
        let root = root.clone();
        let counter = counter.clone();
        move |_| tool_call_frames(&root, &counter, false)
    })
    .await;
    let provider = antigravity_provider(&fixture.endpoint).await;
    let result = run_turn(root.clone(), provider).await;

    assert!(result.error.is_some(), "the truncated turn must fail");
    let message = result
        .error
        .as_ref()
        .map(|error| error.message.clone())
        .unwrap_or_default();
    assert!(
        message.contains("antigravity_stream_interrupted"),
        "truncation must stay the reported failure, got {message:?}"
    );
    assert!(
        !counter.exists(),
        "a truncated turn must not run a mutation"
    );
    assert_eq!(
        fixture.request_count(),
        3,
        "the configured retry budget is bounded"
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

/// A stream that reaches clean HTTP EOF after emitting candidates without a
/// `finishReason` frame must complete on the first attempt instead of throwing
/// away the streamed answer and resampling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clean_eof_without_finish_reason_completes_without_resampling() {
    let _profile = FixtureProfile::new();
    let project = tempfile::tempdir().unwrap();
    let root = project.path().to_path_buf();

    let fixture = SseFixture::start(|_| SseResponse {
        frames: sse_frame(json!({
            "response": {
                "modelVersion": MODEL,
                "candidates": [{"content": {"role": "model", "parts": [{"text": "Complete final summary without finishReason."}]}}],
                "usageMetadata": {"promptTokenCount": 120, "candidatesTokenCount": 42, "totalTokenCount": 162}
            }
        })),
        clean_eof: true,
    })
    .await;
    let provider = antigravity_provider(&fixture.endpoint).await;
    let result = run_turn(root, provider).await;

    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        fixture.request_count(),
        1,
        "clean EOF must not trigger full-request resampling"
    );
    let text = result.final_text.try_to_utf8().unwrap_or_default();
    assert!(
        text.contains("Complete final summary without finishReason."),
        "streamed text must be retained: {text}"
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

/// Multi-leg continuation (`FINISH_REASON_CONTINUATION` + `continuationToken`)
/// and mid-stream checkpoint resume must stitch legs into a single assistant
/// response with cumulative usage without re-running from the start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_leg_continuation_and_mid_stream_checkpoint_resume_preserve_streamed_output() {
    let _profile = FixtureProfile::new();
    let project = tempfile::tempdir().unwrap();
    let root = project.path().to_path_buf();

    let fixture = SseFixture::start(|index| match index {
        0 => SseResponse {
            frames: sse_frame(json!({
                "response": {
                    "modelVersion": MODEL,
                    "candidates": [{
                        "content": {"role": "model", "parts": [{"text": "First leg "}]},
                        "finishReason": "FINISH_REASON_CONTINUATION",
                        "continuationToken": "tok-leg-1"
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 1000,
                        "cachedContentTokenCount": 800,
                        "thoughtsTokenCount": 100,
                        "candidatesTokenCount": 200,
                        "totalTokenCount": 1300
                    }
                }
            })),
            clean_eof: true,
        },
        1 => SseResponse {
            frames: sse_frame(json!({
                "response": {
                    "modelVersion": MODEL,
                    "candidates": [{
                        "content": {"role": "model", "parts": [{"text": "second leg "}]},
                        "continuationToken": "tok-leg-2"
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 1200,
                        "cachedContentTokenCount": 900,
                        "thoughtsTokenCount": 50,
                        "candidatesTokenCount": 150,
                        "totalTokenCount": 1400
                    }
                }
            })),
            clean_eof: false,
        },
        _ => SseResponse {
            frames: sse_frame(json!({
                "response": {
                    "modelVersion": MODEL,
                    "candidates": [{
                        "content": {"role": "model", "parts": [{"text": "third leg."}]},
                        "finishReason": "STOP"
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 1350,
                        "cachedContentTokenCount": 1000,
                        "thoughtsTokenCount": 25,
                        "candidatesTokenCount": 75,
                        "totalTokenCount": 1450
                    }
                }
            })),
            clean_eof: true,
        },
    })
    .await;
    let provider = antigravity_provider(&fixture.endpoint).await;
    let result = run_turn(root, provider).await;

    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        fixture.request_count(),
        3,
        "expected initial leg + nominal continuation leg + mid-stream checkpoint resume leg"
    );
    {
        let requests = fixture.bodies.lock().unwrap();
        assert!(requests[0]["request"].get("continuationToken").is_none());
        assert_eq!(
            requests[1]["request"]["continuationToken"],
            json!("tok-leg-1")
        );
        assert_eq!(
            requests[2]["request"]["continuationToken"],
            json!("tok-leg-2")
        );
        assert_eq!(requests[0]["userPromptId"], requests[1]["userPromptId"]);
        assert_eq!(requests[1]["userPromptId"], requests[2]["userPromptId"]);
        assert_ne!(requests[0]["requestId"], requests[1]["requestId"]);
        assert_ne!(requests[1]["requestId"], requests[2]["requestId"]);
    }
    let text = result.final_text.try_to_utf8().unwrap_or_default();
    assert!(
        text.contains("First leg second leg third leg."),
        "all three legs must be concatenated without loss: {text}"
    );
    let usage = result.usage.as_ref().unwrap();
    assert_eq!(usage.input_tokens, 200);
    assert_eq!(usage.cache_read_input_tokens, 800);
    assert_eq!(usage.output_tokens, 600);
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}
