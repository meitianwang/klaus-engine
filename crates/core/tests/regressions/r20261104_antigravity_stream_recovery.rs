//! Defect 20261104: Antigravity turns died as `antigravity_stream_incomplete`
//! after long agent streams; a clean EOF without a `finishReason` frame was
//! classified away from the engine's bounded stream recovery.
//! Root cause: the truncation tag carried no network signal, so the public
//! engine never resampled a request whose tool calls had not been dispatched.
//! Work item 20261104: `cloud::error_details` appends network wording to the
//! exact `antigravity_stream_incomplete` / `antigravity_stream_interrupted`
//! tags; this file pins the resulting public behaviour.
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

/// A local SSE endpoint. `script` returns the body for the Nth model request;
/// every connection answers once and closes, so a body without a terminal
/// `finishReason` is the clean-EOF truncation shape under test.
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
    async fn start(script: impl Fn(usize) -> String + Send + 'static) -> Self {
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
                let frames = script(index);
                index += 1;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{frames}"
                );
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
fn tool_call_frames(fixture: &Path, counter: &Path, terminated: bool) -> String {
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
    sse_frame(frame)
}

fn text_frames(text: &str, terminated: bool) -> String {
    let mut frame = json!({
        "response": {
            "modelVersion": MODEL,
            "candidates": [{"content": {"role": "model", "parts": [{"text": text}]}}]
        }
    });
    if terminated {
        frame["response"]["candidates"][0]["finishReason"] = json!("STOP");
    }
    sse_frame(frame)
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
        message.contains("antigravity_stream_incomplete"),
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
