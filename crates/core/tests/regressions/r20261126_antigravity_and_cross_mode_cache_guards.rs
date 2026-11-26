//! Regression 20261126: quota-metered connectors must budget tool results,
//! while raw API and Codex sessions retain their opt-in policy.

use klaus_agent_core::schemas::config::EngineConfig;
use serde_json::json;

#[test]
fn coding_plan_sources_enable_tool_result_budget_by_default() {
    for source in [
        "subscription",
        "antigravity",
        "cursor",
        "kiro",
        "qoder",
        "qwenwork",
        "workbuddy",
        "trae",
        "kimi",
        "bailian",
        "minimax",
        "zhipu",
        "ark",
        "codex",
        "gemini",
        "openrouter",
    ] {
        let config: EngineConfig =
            serde_json::from_value(json!({ "modelSource": { "source": source } })).unwrap();
        assert_eq!(
            config.effective_aggregate_tool_results(),
            !matches!(source, "codex" | "gemini" | "openrouter"),
            "{source}"
        );
    }
    let unset: EngineConfig = serde_json::from_value(json!({})).unwrap();
    assert!(!unset.effective_aggregate_tool_results());
}

use super::r20261117_antigravity_gemini_prompt_cache::{
    start_antigravity_gemini_session_with_overrides, GeminiFixtureOverrides, ScriptedGeminiClient,
    ScriptedReply,
};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::types::abort::AbortSignal;

#[tokio::test]
async fn direct_gemini_prediction_at_turn_stop_preserves_prefix() {
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("Done"),
        ScriptedReply::Text(r#"{"suggestion":"Run tests"}"#),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        vec![],
        200_000,
        None,
        None,
        GeminiFixtureOverrides {
            config: Some(
                json!({"modelSource":{"source":"gemini"},"thinking":true,"effort":"high"}),
            ),
            now: Some(std::sync::Arc::new(|| 1_800_000_000_000)),
            thinking_level: true,
        },
    );
    let result = session
        .run(
            Input::Text("Inspect the project".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .unwrap()
        .try_to_utf8()
        .unwrap();
    assert!(session
        .predict_message_with_signal(
            "Suggest the next message".into(),
            source,
            None,
            AbortSignal::new()
        )
        .await
        .unwrap()
        .is_some());
    let calls = client.taken();
    assert_eq!(calls.len(), 2);
    let main = &calls[0].wire;
    let prediction = &calls[1].wire;
    assert_eq!(
        main["generationConfig"]["thinkingConfig"]["includeThoughts"],
        json!(true)
    );
    assert_eq!(
        prediction["generationConfig"]["thinkingConfig"]["includeThoughts"],
        json!(false)
    );
    assert!(
        prediction["generationConfig"]["maxOutputTokens"]
            .as_u64()
            .unwrap()
            <= 512
    );
    for key in ["systemInstruction", "tools"] {
        assert_eq!(main[key], prediction[key], "{key}");
    }
    let contents = main["contents"].as_array().unwrap();
    assert_eq!(
        contents,
        &prediction["contents"].as_array().unwrap()[..contents.len()]
    );
}

#[tokio::test]
async fn antigravity_manual_compaction_reuses_live_wire_prefix() {
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("Turn complete."),
        ScriptedReply::Text("Summary of the project investigation."),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        vec![],
        200_000,
        None,
        None,
        GeminiFixtureOverrides {
            config: None,
            now: None,
            thinking_level: false,
        },
    );
    let result = session
        .run(
            Input::Text("Investigate the project architecture.".into()),
            TurnOptions {
                system_prompt_append: Some("Stable manual compaction prefix marker.".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let result = session.compact(None).await.unwrap();
    assert!(result.compacted);
    let calls = client.taken();
    assert_eq!(calls.len(), 2);
    let main = &calls[0].wire;
    let summary = &calls[1].wire;
    assert!(main["contents"].to_string().contains("<system-reminder>"));
    for key in ["systemInstruction", "tools"] {
        assert_eq!(main[key], summary[key], "{key}");
    }
    let contents = main["contents"].as_array().unwrap();
    assert_eq!(
        contents,
        &summary["contents"].as_array().unwrap()[..contents.len()]
    );
    assert!(!format!("{:?}", session.history()).contains("runtime-reminder-step:"));
}

mod signed_replay {
    use serde_json::{json, Value};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::provider::gemini::{
        GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider,
        GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
    };
    use klaus_agent_core::runtime_process::RuntimeProcessContext;
    use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
    use klaus_agent_core::tools::builtins::read::create_read_tool;
    use klaus_agent_core::types::abort::AbortSignal;
    use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities, ThinkingCapability};

    use super::super::r7c876d49_spec_restores_latest_permission_policy::{
        agent_options, process_supervisor,
    };

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

    #[tokio::test]
    async fn effort_tiers_replay_signed_tool_history() {
        let adapter_policy = GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            ..Default::default()
        };
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("fixture.txt");
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
                vec![chunk(
                    json!({"candidates":[{"content":{"parts":[{"text":"CARBON"}]},"finishReason":"STOP"}]}),
                )],
            ])),
            requests: requests.clone(),
        };
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let mut options = agent_options(process);
        options.provider = Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("gemini-4-neon-high".into()),
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
            cwd: Some(directory.path().to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
            config: Some(json!({
                "model":"gemini-4-neon-high", "prewarm":false, "thinking":true,
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
        for model in ["gemini-4-neon-medium", "gemini-4-carbon-high"] {
            session.set_model(model).unwrap();
            let result = session
                .run(
                    Input::Text("Report the previous result.".into()),
                    TurnOptions::default(),
                )
                .await;
            assert!(result.error.is_none(), "{:?}", result.error);
        }
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for (request, same_family) in [(&requests[2], true), (&requests[3], false)] {
            let parts = request
                .contents
                .iter()
                .flat_map(|c| &c.parts)
                .collect::<Vec<_>>();
            assert_eq!(
                parts.iter().any(|p| p.thought == Some(true)
                    && p.thought_signature
                        .as_ref()
                        .is_some_and(|s| s == "opaque-thought-signature")),
                same_family
            );
            let call = parts.iter().find(|p| p.function_call.is_some()).unwrap();
            assert_eq!(
                call.thought_signature.as_ref().unwrap(),
                if same_family {
                    "opaque-call-signature"
                } else {
                    "skip_thought_signature_validator"
                }
            );
        }
    }
}

#[tokio::test]
async fn resumed_antigravity_manual_compaction_projects_persisted_step() {
    use super::r7c876d49_spec_restores_latest_permission_policy::{
        agent_options, process_supervisor,
    };
    use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
    use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
    use klaus_agent_core::runtime_process::RuntimeProcessContext;
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use klaus_agent_core::store::session_store::SessionStore;
    use klaus_agent_core::types::provider::ModelCapabilities;
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_string_lossy().into_owned();
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("Investigation complete."),
        ScriptedReply::Text("A concise project summary."),
    ]);
    let mut opts = agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    opts.provider = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some("gemini-cache-resume-fixture".into()),
        client: Some(client.clone()),
        capabilities: Some(ModelCapabilities {
            context_window: Some(200_000),
            max_output_tokens: Some(8192),
            tools: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }));
    opts.tools = Some(vec![]);
    opts.config = Some(
        json!({"model":"gemini-cache-resume-fixture","modelSource":{"source":"antigravity"},"prewarm":false,"autoMemory":false,"autoDream":false,"memoryRecall":false,"checkpointing":false,"compactionSelfCheck":false}),
    );
    let start = || StartSessionOptions {
        cwd: Some(path.clone().into()),
        user_config_dir: Some(path.clone().into()),
        ..Default::default()
    };
    let agent = Agent::new(opts);
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(path.clone()),
        archive_dir: None,
    }));
    let session = agent.open_session(store.clone(), start()).await.unwrap();
    let result = session
        .run(
            Input::Text("Inspect the restored project context.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    session.flush_rollout().await.unwrap();
    let id = session.id.try_to_utf8().unwrap();
    session.dispose().await;
    let resumed = agent.resume_session(store, &id, start()).await.unwrap();
    assert!(format!("{:?}", resumed.history()).contains("runtime-reminder-step:"));
    assert!(resumed.compact(None).await.unwrap().compacted);
    let calls = client.taken();
    assert_eq!(calls.len(), 2);
    let original = calls[0].wire["contents"].as_array().unwrap();
    let summary = calls[1].wire["contents"].as_array().unwrap();
    assert!(original
        .iter()
        .any(|c| c.to_string().contains("<system-reminder>")));
    assert_eq!(original, &summary[..original.len()]);
    assert!(!format!("{:?}", resumed.history()).contains("runtime-reminder-step:"));
}
