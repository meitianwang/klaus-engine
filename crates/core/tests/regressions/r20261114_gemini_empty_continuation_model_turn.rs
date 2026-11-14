//! Defect: clicking Continue (`Input::Parts(vec![])`) on a Gemini / Antigravity
//! session after an interrupted or partial assistant turn failed with HTTP 400
//! `antigravity_request_rejected [INVALID_ARGUMENT]: Requests ending with a model turn are not supported.`
//! Root cause: `GeminiProvider` serialized trailing assistant history directly
//! as a final `role: "model"` Content without appending a continuation user
//! reminder or dropping orphaned thought-only model turns, and coalesced
//! back-to-back signed model turns across empty-input continuations.
//! Work item: 2026-11-14 Gemini empty-input continuation model-turn rejection fix.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::gemini::{
    Content, GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider,
    GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities, ThinkingCapability};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct FixtureDirectory {
    path: PathBuf,
    previous_config: Option<OsString>,
}

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-gemini-empty-continue-{}",
            uuid::Uuid::new_v4()
        ));
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

enum ScriptStep {
    Chunks(Vec<GenerateContentResponse>),
    StreamThenAbort {
        chunks: Vec<GenerateContentResponse>,
        abort: AbortSignal,
    },
}

struct StrictGeminiContinuationClient {
    steps: Mutex<VecDeque<ScriptStep>>,
    requests: Arc<Mutex<Vec<GenerateContentParameters>>>,
}

fn validate_strict_gemini_contents(contents: &[Content]) -> Result<(), GeminiApiError> {
    if contents.is_empty() {
        return Err(GeminiApiError {
            status: Some(400),
            message: "antigravity_request_rejected [INVALID_ARGUMENT]: contents must not be empty."
                .into(),
            headers: None,
        });
    }
    if contents.last().is_some_and(|last| last.role == "model") {
        return Err(GeminiApiError {
            status: Some(400),
            message: "antigravity_request_rejected [INVALID_ARGUMENT]: Requests ending with a model turn are not supported."
                .into(),
            headers: None,
        });
    }
    let mut prev_role: Option<&RuntimeString> = None;
    for (idx, content) in contents.iter().enumerate() {
        if prev_role.is_some_and(|role| role == &content.role) {
            return Err(GeminiApiError {
                status: Some(400),
                message: format!(
                    "antigravity_request_rejected [INVALID_ARGUMENT]: non-alternating role at index {idx}"
                ),
                headers: None,
            });
        }
        prev_role = Some(&content.role);
        if content.parts.is_empty() {
            return Err(GeminiApiError {
                status: Some(400),
                message: format!(
                    "antigravity_request_rejected [INVALID_ARGUMENT]: empty parts at index {idx}"
                ),
                headers: None,
            });
        }
        if content.role == "model" {
            let has_visible_or_tool = content.parts.iter().any(|part| {
                part.function_call.is_some()
                    || part.inline_data.is_some()
                    || (part.thought != Some(true)
                        && part.text.as_ref().is_some_and(|text| !text.is_empty()))
            });
            if !has_visible_or_tool {
                return Err(GeminiApiError {
                    status: Some(400),
                    message: format!(
                        "antigravity_request_rejected [INVALID_ARGUMENT]: orphaned thought-only model turn at index {idx}"
                    ),
                    headers: None,
                });
            }
            let signed_thoughts: Vec<_> = content
                .parts
                .iter()
                .filter(|part| part.thought == Some(true) && part.thought_signature.is_some())
                .collect();
            if signed_thoughts.len() > 1 {
                return Err(GeminiApiError {
                    status: Some(400),
                    message: format!(
                        "antigravity_request_rejected [INVALID_ARGUMENT]: coalesced multiple signed thought turns at index {idx}"
                    ),
                    headers: None,
                });
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl GeminiClient for StrictGeminiContinuationClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        _headers: Option<AuthHeaders>,
        _extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        self.requests.lock().unwrap().push(params.clone());
        validate_strict_gemini_contents(&params.contents)?;
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted Gemini step");
        match step {
            ScriptStep::Chunks(chunks) => {
                Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
            }
            ScriptStep::StreamThenAbort { chunks, abort } => Ok(Box::pin(async_stream::stream! {
                for item in chunks {
                    yield Ok(item);
                }
                abort.cancel();
                while !signal.is_cancelled() {
                    tokio::task::yield_now().await;
                }
            })),
        }
    }
}

fn chunk(value: Value) -> GenerateContentResponse {
    runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap()
}

fn session_config(model: &str) -> Value {
    json!({
        "model": model,
        "prewarm": false,
        "thinking": true,
        "maxRetries": 0,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false,
        "speculativeCompaction": false,
        "earlyMicrocompact": false,
        "idleMicrocompact": false,
        "compactionSelfCheck": false,
        "loopDetection": false
    })
}

#[tokio::test]
async fn empty_continuation_after_interrupted_visible_model_turn_succeeds_and_preserves_prefix_across_tool_loop(
) {
    let directory = FixtureDirectory::new();
    let target_file = directory.path.join("phase1.txt");
    std::fs::write(&target_file, "PHASE1_OK").unwrap();

    let abort_turn_1 = AbortSignal::new();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let client = StrictGeminiContinuationClient {
        steps: Mutex::new(VecDeque::from([
            // Turn 1 emits signed reasoning + partial visible text, then the user stops the turn.
            ScriptStep::StreamThenAbort {
                chunks: vec![chunk(json!({
                    "candidates": [{
                        "content": {
                            "parts": [
                                {
                                    "thought": true,
                                    "text": "All unit tests passed; starting phase 1.",
                                    "thoughtSignature": "sig-turn-1-thought"
                                },
                                {
                                    "text": "Starting phase 1: fixing streaming output."
                                }
                            ]
                        }
                    }]
                }))],
                abort: abort_turn_1.clone(),
            },
            // Turn 2 (Continue with empty input), step 1: receives continuation reminder,
            // emits its own signed reasoning + a tool call.
            ScriptStep::Chunks(vec![chunk(json!({
                "candidates": [{
                    "content": {
                        "parts": [
                            {
                                "thought": true,
                                "text": "Resuming phase 1 by reading phase1.txt.",
                                "thoughtSignature": "sig-turn-2-thought"
                            },
                            {
                                "functionCall": {
                                    "id": "call-read-1",
                                    "name": "read",
                                    "args": { "file_path": target_file.to_string_lossy() }
                                },
                                "thoughtSignature": "sig-turn-2-call"
                            }
                        ]
                    },
                    "finishReason": "STOP"
                }]
            }))]),
            // Turn 2, step 2 (after tool result): must keep the exact 3-content prefix
            // from step 1 so signed thoughts from turn 1 and turn 2 stay separated.
            ScriptStep::Chunks(vec![chunk(json!({
                "candidates": [{
                    "content": {
                        "parts": [
                            { "text": "Phase 1 complete: PHASE1_OK." }
                        ]
                    },
                    "finishReason": "STOP"
                }]
            }))]),
        ])),
        requests: requests.clone(),
    };

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.provider = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some("gemini-4-carbon-high".into()),
        client: Some(Arc::new(client)),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
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
        config: Some(session_config("gemini-4-carbon-high")),
        ..Default::default()
    });

    // 1. Run initial turn and stop it mid-stream after partial text is emitted.
    let turn_1_events: Vec<_> = session
        .run_streamed(
            Input::Text("Execute all phases sequentially.".into()),
            TurnOptions {
                signal: Some(abort_turn_1.clone()),
                ..Default::default()
            },
        )
        .events
        .collect()
        .await;
    assert!(
        turn_1_events.iter().any(|ev| matches!(
            ev,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
        )),
        "expected aborted turn 1, got: {turn_1_events:?}"
    );

    // 2. Continue with empty input (`chat:continue` -> `Input::Parts(vec![])`).
    let continued = session
        .run(Input::Parts(Vec::new()), TurnOptions::default())
        .await;
    assert!(
        continued.error.is_none(),
        "empty continuation must succeed without model-turn 400: {:?}",
        continued.error
    );
    assert_eq!(continued.final_text, "Phase 1 complete: PHASE1_OK.");

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 3);
    // Request 1 (Continue step 1): [user(prompt), model(partial), user(continuation reminder)]
    assert_eq!(captured[1].contents.len(), 3);
    assert_eq!(captured[1].contents[0].role, "user");
    assert_eq!(captured[1].contents[1].role, "model");
    assert_eq!(captured[1].contents[2].role, "user");
    assert!(captured[1].contents[2].parts[0]
        .text
        .as_ref()
        .is_some_and(|t| t.contains("Continue from where you left off")));

    // Request 2 (Continue step 2 after tool result):
    // [user(prompt), model(partial), user(continuation reminder), model(tool call), user(tool response)]
    assert_eq!(captured[2].contents.len(), 5);
    assert_eq!(
        runtime_codec::encode_value(&captured[2].contents[..3].to_vec()).unwrap(),
        runtime_codec::encode_value(&captured[1].contents).unwrap(),
        "step 2 must preserve the exact step 1 prefix for KV cache and signature integrity"
    );

    // Genuine user history remains untouched (no persisted synthetic user message).
    let history = session.history();
    let real_users: Vec<_> = history
        .iter()
        .filter_map(|m| match m {
            Message::User(u) if u.is_meta != Some(true) => Some(u),
            _ => None,
        })
        .collect();
    assert_eq!(real_users.len(), 1);
    assert!(matches!(
        real_users[0].content.as_slice(),
        [ContentBlock::Text(t)] if t.text == "Execute all phases sequentially."
    ));
}

#[tokio::test]
async fn empty_continuation_after_thought_only_failure_drops_orphan_signed_thought_and_ends_on_original_user(
) {
    let directory = FixtureDirectory::new();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let signed_thought_only = || {
        ScriptStep::Chunks(vec![chunk(json!({
            "candidates": [{
                "content": {
                    "parts": [
                        {
                            "thought": true,
                            "text": "Still analyzing without emitting any visible text.",
                            "thoughtSignature": "sig-orphan-thought"
                        }
                    ]
                },
                "finishReason": "STOP"
            }]
        }))])
    };
    let client = StrictGeminiContinuationClient {
        steps: Mutex::new(VecDeque::from([
            // Turn 1 step 1 + empty-closing retry: both return signed thought only,
            // persisting a signed thought-only Assistant message in history before failing.
            signed_thought_only(),
            signed_thought_only(),
            // Turn 2 (Continue with empty input) must drop the orphaned signed thought-only
            // model turn and replay ending on the original user message.
            ScriptStep::Chunks(vec![chunk(json!({
                "candidates": [{
                    "content": {
                        "parts": [
                            { "text": "Direct answer after thought-only failure." }
                        ]
                    },
                    "finishReason": "STOP"
                }]
            }))]),
        ])),
        requests: requests.clone(),
    };

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.provider = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some("gemini-4-carbon-high".into()),
        client: Some(Arc::new(client)),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
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

    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.path.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.path.to_string_lossy().into_owned().into()),
        config: Some(session_config("gemini-4-carbon-high")),
        ..Default::default()
    });

    let failed = session
        .run(
            Input::Text("Analyze performance bottlenecks.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(
        failed.error.as_ref().is_some_and(|e| e
            .message
            .contains("the model returned no content for this turn")),
        "expected empty-content failure, got: {:?}",
        failed.error
    );

    let continued = session
        .run(Input::Parts(Vec::new()), TurnOptions::default())
        .await;
    assert!(
        continued.error.is_none(),
        "continuation after signed thought-only failure must succeed: {:?}",
        continued.error
    );
    assert_eq!(
        continued.final_text,
        "Direct answer after thought-only failure."
    );

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 3);
    assert_eq!(captured[2].contents.len(), 1);
    assert_eq!(captured[2].contents[0].role, "user");
    assert!(captured[2]
        .contents
        .iter()
        .flat_map(|c| &c.parts)
        .all(|p| !p
            .text
            .as_ref()
            .is_some_and(|t| t.contains("Continue from where you left off"))));
}
