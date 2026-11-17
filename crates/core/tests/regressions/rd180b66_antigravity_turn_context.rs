//! Defect: turn reminders and hook context still changed the system prefix.
//! Root cause: the source-scoped snapshot selector omitted those volatile slots.
//! Fixing commit: d180b669ce8f810baf878493030403fd2d9f062d.

use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiApiError, GeminiChunkStream, GeminiClient, GeminiProvider,
    GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities, SystemPromptPart};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const GEMINI: &str = "gemini-turn-context-fixture";

#[derive(Default)]
struct CapturingClient(Mutex<Vec<Value>>);

#[async_trait::async_trait]
impl GeminiClient for CapturingClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        _: Option<AuthHeaders>,
        _: Vec<(String, String)>,
        _: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        let wire = generate_content_request(&params);
        self.0.lock().unwrap().push(
            serde_json::from_str(&runtime_json::stringify(&wire).try_to_utf8().unwrap()).unwrap(),
        );
        let response: GenerateContentResponse = runtime_codec::decode_text(&RuntimeString::from(
            json!({
                "modelVersion": params.model.try_to_utf8().unwrap(),
                "candidates": [{"content": {"role": "model", "parts": [{"text": "done"}]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 1}
            }).to_string(),
        ))
        .unwrap();
        Ok(Box::pin(futures::stream::iter(vec![Ok(response)])))
    }
}

fn part(name: &str, text: &str, cacheable: bool) -> SystemPromptPart {
    SystemPromptPart {
        name: name.into(),
        text: text.into(),
        cacheable: Some(cacheable),
        cache_scope: None,
    }
}

fn context(version: Option<&str>, volatile: bool) -> Vec<SystemPromptPart> {
    let mut parts = vec![
        part("base", "Stable base instructions.", true),
        part("permissions", "Fixed authority must remain system.", false),
        part(
            "custom",
            "Unknown custom context must remain system.",
            false,
        ),
    ];
    if let Some(version) = version {
        parts.push(part("reminder", &format!("TURN-{version}"), !volatile));
        parts.push(part("hook-context", &format!("HOOK-{version}"), !volatile));
    }
    parts
}

struct Fixture {
    // The temporary sandbox must outlive the session using it.
    _directory: tempfile::TempDir,
    session: Arc<Session>,
    client: Arc<CapturingClient>,
    parts: Arc<Mutex<Vec<SystemPromptPart>>>,
}

fn fixture(source: &str, model: &str, volatile: bool) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let client = Arc::new(CapturingClient::default());
    let provider = GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(model.into()),
        client: Some(client.clone()),
        capabilities: Some(ModelCapabilities {
            context_window: Some(200000),
            max_output_tokens: Some(8192),
            tools: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });
    let parts = Arc::new(Mutex::new(context(Some("ONE"), volatile)));
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = Arc::new(provider);
    options.config = Some(json!({
        "model": model,
        "modelSource": {"source": source, "selection": format!("{source}:{model}")},
        "prewarm": false, "autoMemory": false, "autoDream": false, "memoryRecall": false,
        "memory": {"enabled": false}, "checkpointing": false, "speculativeCompaction": false,
        "earlyMicrocompact": false, "idleMicrocompact": false, "compactionSelfCheck": false,
        "loopDetection": false, "autoCompaction": true, "contextWindow": 200000,
        "maxRetries": 1, "unboundedNetworkRetries": false, "totalTokensReminder": "off",
        "approval_policy": "never", "sandbox_mode": "danger-full-access"
    }));
    options.tools = Some(Vec::new());
    options.include_default_agents = Some(false);
    options.system_prompt_provider = Some(Arc::new({
        let parts = parts.clone();
        move || {
            let current = parts.lock().unwrap().clone();
            Box::pin(async move { current })
        }
    }));
    let root = directory.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        ..Default::default()
    });
    Fixture {
        _directory: directory,
        session,
        client,
        parts,
    }
}

async fn run(fixture: &Fixture, text: &str) {
    let result = fixture
        .session
        .run(Input::Text(text.into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.final_text, "done");
}

fn snapshots(wire: &Value) -> usize {
    wire["contents"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|content| content["parts"].as_array().unwrap())
        .filter(|part| {
            part["text"]
                .as_str()
                .is_some_and(|text| text.contains("Runtime reminder update."))
        })
        .count()
}

#[tokio::test]
async fn public_gemini_turn_context_updates_and_clear_preserve_wire_history_prefixes() {
    let fixture = fixture("antigravity", GEMINI, true);
    run(&fixture, "Begin the fixture.").await;
    *fixture.parts.lock().unwrap() = context(Some("TWO"), true);
    run(&fixture, "Continue with updated context.").await;
    *fixture.parts.lock().unwrap() = context(None, true);
    run(&fixture, "The turn context has cleared.").await;
    run(&fixture, "Continue without another snapshot.").await;
    {
        let wires = fixture.client.0.lock().unwrap();
        assert_eq!(wires.len(), 4);
        assert_eq!(
            wires.iter().map(snapshots).collect::<Vec<_>>(),
            [1, 2, 3, 3]
        );
        for pair in wires.windows(2) {
            assert_eq!(pair[0]["systemInstruction"], pair[1]["systemInstruction"]);
            assert_eq!(pair[0]["tools"], pair[1]["tools"]);
            assert!(
                pair[1]["contents"]
                    .as_array()
                    .unwrap()
                    .starts_with(pair[0]["contents"].as_array().unwrap()),
                "the prior encoded history must remain an exact prefix"
            );
        }
        let system = wires[0]["systemInstruction"].to_string();
        assert!(!system.contains("TURN-ONE"));
        assert!(!system.contains("HOOK-ONE"));
        assert!(system.contains("Fixed authority"));
        assert!(system.contains("Unknown custom"));
        let last = wires[3]["contents"].to_string();
        assert!(last.contains("TURN-ONE") && last.contains("TURN-TWO"));
        assert!(last.contains("HOOK-ONE") && last.contains("HOOK-TWO"));
        assert!(last.contains("No runtime reminder entries are currently active."));
    }
    let steps = fixture
        .session
        .history()
        .iter()
        .filter(|message| {
            matches!(message, Message::User(user) if user.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(text) if text.phase.as_ref()
                    .is_some_and(|phase| phase == "antigravity-runtime-reminders"))
            }))
        })
        .count();
    assert_eq!(
        steps, 3,
        "clearing is persisted once, not on every later request"
    );
    fixture.session.dispose().await;
}

#[tokio::test]
async fn public_turn_context_stays_system_outside_antigravity_gemini() {
    for (source, model) in [
        ("gemini", GEMINI),
        ("antigravity", "claude-turn-context-fixture"),
    ] {
        let fixture = fixture(source, model, true);
        run(&fixture, "Begin the fixture.").await;
        *fixture.parts.lock().unwrap() = context(Some("TWO"), true);
        run(&fixture, "Continue the fixture.").await;
        {
            let wires = fixture.client.0.lock().unwrap();
            assert_eq!(wires.len(), 2);
            assert!(wires[0]["systemInstruction"]
                .to_string()
                .contains("TURN-ONE"));
            assert!(wires[0]["systemInstruction"]
                .to_string()
                .contains("HOOK-ONE"));
            assert!(wires[1]["systemInstruction"]
                .to_string()
                .contains("TURN-TWO"));
            assert!(wires[1]["systemInstruction"]
                .to_string()
                .contains("HOOK-TWO"));
            assert_eq!(wires.iter().map(snapshots).collect::<Vec<_>>(), [0, 0]);
        }
        fixture.session.dispose().await;
    }
}

#[tokio::test]
async fn public_cacheable_turn_slots_retain_their_system_authority() {
    let fixture = fixture("antigravity", GEMINI, false);
    run(&fixture, "Begin the fixture.").await;
    {
        let wires = fixture.client.0.lock().unwrap();
        assert_eq!(wires.len(), 1);
        let system = wires[0]["systemInstruction"].to_string();
        assert!(system.contains("TURN-ONE") && system.contains("HOOK-ONE"));
        assert_eq!(snapshots(&wires[0]), 0);
    }
    fixture.session.dispose().await;
}
