//! Defect: signed thought parts lost their original text association during replay.
//! Root cause: only opaque signatures survived without the original signed content.
//! Work item: 20261019 signed thought preservation across a public tool loop.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::gemini::{
    GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider,
    GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities, ThinkingCapability};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct FixtureDirectory {
    path: PathBuf,
    previous_config: Option<OsString>,
}

impl FixtureDirectory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-signed-thought-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous_config = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self {
            path,
            previous_config,
        }
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous_config {
            set_process_environment_var("KLAUS_CONFIG_DIR", previous);
        } else {
            remove_process_environment_var("KLAUS_CONFIG_DIR");
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct ScriptedClient {
    responses: Mutex<VecDeque<Vec<GenerateContentResponse>>>,
    requests: Arc<Mutex<Vec<GenerateContentParameters>>>,
}

#[async_trait::async_trait]
impl GeminiClient for ScriptedClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        assert!(headers.is_none());
        assert!(extra_headers.is_empty());
        assert!(!signal.is_cancelled());
        self.requests.lock().unwrap().push(params);
        let chunks = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("two model calls only");
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}

fn chunk(value: Value) -> GenerateContentResponse {
    runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap()
}

async fn run_signed_thought_tool_loop(adapter_policy: GeminiAdapterPolicy, switch_model: bool) {
    let directory = FixtureDirectory::new();
    let file = directory.path.join("fixture.txt");
    std::fs::write(&file, "PONG").unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut first = Vec::new();
    for text in ["The", " user wants"] {
        first.push(chunk(
            json!({"modelVersion":"service-family-version","candidates":[{"content":{"parts":[{"thought":true,"text":text}]}}]}),
        ));
    }
    first.push(chunk(json!({"candidates":[{"content":{"parts":[
        {"thought":true,"text":" the file.","thoughtSignature":"opaque-thought-signature"},
        {"functionCall":{"id":"fixture-call","name":"read","args":{"file_path":file.to_string_lossy()}},"thoughtSignature":"opaque-call-signature"}
    ]},"finishReason":"OTHER"}]})));
    let client = ScriptedClient {
        responses: Mutex::new(VecDeque::from([
            first,
            vec![chunk(
                json!({"candidates":[{"content":{"parts":[{"text":"PONG"}]},"finishReason":"STOP"}]}),
            )],
            vec![chunk(
                json!({"candidates":[{"content":{"parts":[{"text":"SWITCHED"}]},"finishReason":"STOP"}]}),
            )],
        ])),
        requests: requests.clone(),
    };
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.provider = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some("signed-thought-fixture".into()),
        client: Some(Arc::new(client)),
        adapter_policy,
        capabilities: Some(ModelCapabilities {
            context_window: Some(200_000),
            max_output_tokens: Some(8192),
            tools: Some(true),
            thinking: Some(ThinkingCapability::default()),
            ..Default::default()
        }),
        ..Default::default()
    }));
    options.include_default_agents = Some(false);
    options.tools = Some(vec![create_read_tool()]);
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.path.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.path.to_string_lossy().into_owned().into()),
        config: Some(json!({
            "model":"signed-thought-fixture", "prewarm":false, "thinking":true,
            "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "checkpointing":false, "speculativeCompaction":false,
            "earlyMicrocompact":false, "idleMicrocompact":false,
            "compactionSelfCheck":false, "loopDetection":false
        })),
        ..Default::default()
    });
    let result = session
        .run(
            Input::Text("Read the fixture and report PONG.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    if switch_model {
        session.set_model("other-model-fixture").unwrap();
        let switched = session
            .run(
                Input::Text("Reply SWITCHED.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(switched.error.is_none(), "{:?}", switched.error);
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), if switch_model { 3 } else { 2 });
    let replay = requests[1]
        .contents
        .iter()
        .find(|content| content.role == "model")
        .expect("assistant tool turn");
    let call_index = if adapter_policy.replay_signed_parts {
        assert_eq!(replay.parts.len(), 4);
        assert_eq!(replay.parts[0].thought, Some(true));
        assert_eq!(replay.parts[0].text.as_ref().unwrap(), "The");
        assert_eq!(replay.parts[1].text.as_ref().unwrap(), " user wants");
        assert_eq!(replay.parts[2].text.as_ref().unwrap(), " the file.");
        assert_eq!(
            replay.parts[2].thought_signature.as_ref().unwrap(),
            "opaque-thought-signature"
        );
        3
    } else {
        assert_eq!(replay.parts.len(), 1);
        assert!(replay.parts.iter().all(|part| part.thought != Some(true)));
        0
    };
    assert_eq!(
        replay.parts[call_index].thought_signature.as_ref().unwrap(),
        "opaque-call-signature"
    );
    assert_eq!(
        replay.parts[call_index]
            .function_call
            .as_ref()
            .unwrap()
            .id
            .as_ref()
            .unwrap(),
        "fixture-call"
    );
    if switch_model {
        let switched = requests[2]
            .contents
            .iter()
            .find(|content| content.role == "model")
            .expect("original tool exchange remains in history");
        let call = switched
            .parts
            .iter()
            .find(|part| part.function_call.is_some())
            .unwrap();
        if adapter_policy.scope_signatures_to_model {
            assert!(switched.parts.iter().all(|part| part.thought != Some(true)));
            assert_eq!(
                call.thought_signature.as_ref().unwrap(),
                "skip_thought_signature_validator"
            );
        } else {
            assert_eq!(
                call.thought_signature.as_ref().unwrap(),
                "opaque-call-signature"
            );
        }
    }
    assert!(requests[1]
        .contents
        .iter()
        .flat_map(|content| &content.parts)
        .any(|part| part
            .function_response
            .as_ref()
            .is_some_and(|response| response.id == "fixture-call" && response.name == "read")));
}

#[tokio::test]
async fn public_tool_loop_preserves_original_signed_thought_parts_before_the_call() {
    run_signed_thought_tool_loop(
        GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
        false,
    )
    .await;
}

#[tokio::test]
async fn public_default_tool_loop_drops_thought_parts_but_preserves_call_signature() {
    run_signed_thought_tool_loop(GeminiAdapterPolicy::default(), false).await;
}

// A signature from one cloud model was sent to another model after a tool loop,
// causing the next request to fail with a corrupted-signature verdict.
#[tokio::test]
async fn public_model_switch_scopes_signatures_only_when_the_adapter_requests_it() {
    for scope_signatures_to_model in [false, true] {
        run_signed_thought_tool_loop(
            GeminiAdapterPolicy {
                replay_signed_parts: true,
                scope_signatures_to_model,
                ..Default::default()
            },
            true,
        )
        .await;
    }
}
