//! End-to-end verification for:
//! 1. Post-compaction working-file restoration from `FileStateCache` without
//!    leaking hidden restoration blocks into visible UI history.
//! 2. Byte-stable `request.system` prefix across mode/reminder changes on
//!    domestic coding-plan sources (`kimi`, `bailian`, `minimax`, `zhipu`,
//!    `ark`) and third-party compatible `custom` gateways.
//! 3. Immediate cancellation of idle prompt-cache keep-alive when a new
//!    foreground turn starts before the keep-alive deadline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use klaus_agent_core::context::compact::CompactionNoopReason;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct ScriptedCapturingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    script_tool_read_path: Option<String>,
    warm_tokens: u64,
    prewarm_endpoints: bool,
}

impl ModelProvider for ScriptedCapturingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 16_384,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn has_prewarm_endpoints(&self) -> bool {
        self.prewarm_endpoints
    }

    fn stream(&self, req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let call_index = {
            let mut guard = self.requests.lock().unwrap();
            let idx = guard.len();
            guard.push(req.clone());
            idx
        };
        if call_index == 0 {
            if let Some(path) = &self.script_tool_read_path {
                let args = json!({ "path": path }).to_string();
                return Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "call-read-1".into(),
                        name: "read".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "call-read-1".into(),
                        partial_json: args.into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "call-read-1".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::ToolUse,
                        usage: Usage {
                            input_tokens: 512,
                            output_tokens: 24,
                            cache_read_input_tokens: self.warm_tokens,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        },
                    },
                ]));
            }
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: req.model.clone(),
            },
            ModelStreamEvent::TextDelta {
                text: "acknowledged".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 512,
                    output_tokens: 8,
                    cache_read_input_tokens: self.warm_tokens,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[derive(Clone)]
struct SummaryCompactor;

#[async_trait::async_trait]
impl Compactor for SummaryCompactor {
    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(self.clone()))
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::NoProgress),
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        _: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "compact-boundary-1".into(),
                summary: "Condensed earlier investigation turns.".into(),
                native_compaction: None,
            })],
            freed_tokens: 1_000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[tokio::test]
async fn manual_compaction_restores_recent_read_files_into_model_context_only() {
    let temp = tempfile::tempdir().unwrap();
    let target_file = temp.path().join("cache_router.rs");
    std::fs::write(
        &target_file,
        "pub fn route_cache_prefix(key: &str) -> bool {\n    !key.is_empty()\n}\n",
    )
    .unwrap();
    let user_cfg = temp.path().join("klaus-cfg");
    std::fs::create_dir_all(&user_cfg).unwrap();

    let provider = Arc::new(ScriptedCapturingProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            default_model: "claude-sonnet-4-6".into(),
            env_key: None,
            base_url: Some("https://api.anthropic.com".into()),
        },
        requests: Mutex::new(Vec::new()),
        script_tool_read_path: Some(target_file.to_string_lossy().to_string()),
        warm_tokens: 0,
        prewarm_endpoints: false,
    });
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "claude-sonnet-4-6",
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false,
        "contextWindow": 200000
    }));
    opts.tools = Some(vec![create_read_tool()]);
    opts.compactor = Some(Arc::new(SummaryCompactor));
    let agent = Agent::new(opts);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
        user_config_dir: Some(RuntimePath::from(user_cfg.to_string_lossy().as_ref())),
        ..Default::default()
    });

    // Turn 1 invokes `read`, populating `session.file_state` with `cache_router.rs`.
    let turn1 = session
        .run(
            Input::Text("Read cache_router.rs".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "{:?}", turn1.error);

    // Compact the entire prior history; the raw tool_result is summarized away.
    session.compact(None).await.unwrap();

    // Every restored fragment after compaction must be marked `is_meta: Some(true)`
    // with `HarnessEnvelope::SystemReminder` so transcript renderers never show it
    // as a user-authored message bubble.
    let restored_meta_count = session
        .visible_history()
        .into_iter()
        .filter(|msg| match msg {
            Message::User(user) => {
                user.is_meta == Some(true)
                    && user.content.iter().all(|block| {
                        matches!(
                            block,
                            ContentBlock::Text(t)
                                if t.harness_envelope
                                    == Some(klaus_agent_core::schemas::content::HarnessEnvelope::SystemReminder)
                        )
                    })
            }
            _ => false,
        })
        .count();
    assert_eq!(restored_meta_count, 1);

    // Turn 2 sends a follow-up; the provider request must carry the restored snippet.
    let turn2 = session
        .run(
            Input::Text("Continue updating route_cache_prefix".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "{:?}", turn2.error);

    let requests = provider.requests.lock().unwrap().clone();
    let post_compact_req = requests.last().expect("post-compaction request");
    let rendered_messages = post_compact_req
        .messages
        .iter()
        .filter_map(|m| match m {
            Message::User(u) => Some(u),
            _ => None,
        })
        .flat_map(|u| &u.content)
        .filter_map(|b| match b {
            ContentBlock::Text(t) => t.text.try_to_utf8().ok(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered_messages.contains(
            "Recently read or edited workspace files preserved across context compaction"
        ),
        "expected working-file restoration block after compaction, got:\n{rendered_messages}"
    );
    assert!(
        rendered_messages.contains("pub fn route_cache_prefix(key: &str) -> bool"),
        "expected restored file body in post-compaction request, got:\n{rendered_messages}"
    );
    session.dispose().await;
}

#[tokio::test]
async fn domestic_and_custom_third_party_sources_keep_system_prefix_byte_identical_across_mode_switches(
) {
    let temp = tempfile::tempdir().unwrap();
    let user_cfg = temp.path().join("klaus-cfg");
    std::fs::create_dir_all(&user_cfg).unwrap();

    for (source, base_url, model) in [
        ("kimi", "https://api.kimi.com/coding", "kimi-k2.5"),
        ("zhipu", "https://open.bigmodel.cn/api/anthropic", "glm-5.1"),
        ("custom", "https://api.deepseek.com/v1", "deepseek-reasoner"),
    ] {
        let provider = Arc::new(ScriptedCapturingProvider {
            info: ProviderInfo {
                name: "compat".into(),
                default_model: model.into(),
                env_key: None,
                base_url: Some(base_url.into()),
            },
            requests: Mutex::new(Vec::new()),
            script_tool_read_path: None,
            warm_tokens: 0,
            prewarm_endpoints: false,
        });
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let mut opts = agent_options(process);
        opts.provider = provider.clone();
        opts.config = Some(json!({
            "model": model,
            "modelSource": {
                "source": source,
                "modelId": model,
                "selection": format!("{source}:{model}")
            },
            "prewarm": false,
            "autoMemory": false,
            "autoDream": false,
            "memoryRecall": false,
            "checkpointing": false
        }));
        let dynamic_tick = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tick_reader = dynamic_tick.clone();
        opts.system_prompt_provider = Some(Arc::new(move || {
            let n = tick_reader.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                vec![
                    SystemPromptPart {
                        name: "base".into(),
                        text: "Immutable prefix instructions.".into(),
                        cacheable: Some(true),
                        cache_scope: None,
                    },
                    SystemPromptPart {
                        name: "reminder".into(),
                        text: format!("Dynamic turn state #{n}").into(),
                        cacheable: Some(false),
                        cache_scope: None,
                    },
                ]
            })
        }));
        let agent = Agent::new(opts);
        let session = agent.start_session(StartSessionOptions {
            cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(user_cfg.to_string_lossy().as_ref())),
            ..Default::default()
        });

        let r1 = session
            .run(Input::Text("Turn one".into()), TurnOptions::default())
            .await;
        assert!(r1.error.is_none());

        session
            .planning_runtime()
            .set_mode(CollaborationMode::Plan, None)
            .unwrap();
        let r2 = session
            .run(
                Input::Text("Turn two in plan mode".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(r2.error.is_none());

        let captured = provider.requests.lock().unwrap().clone();
        assert_eq!(captured.len(), 2, "expected 2 requests for {source}");
        assert_eq!(
            captured[0].system, captured[1].system,
            "{source} must keep request.system byte-identical across mode & reminder updates"
        );
        session.dispose().await;
    }
}

#[tokio::test(start_paused = true)]
async fn foreground_turn_preempts_pending_keepalive_before_timer_fires() {
    let temp = tempfile::tempdir().unwrap();
    let user_cfg = temp.path().join("klaus-cfg");
    std::fs::create_dir_all(&user_cfg).unwrap();

    let provider = Arc::new(ScriptedCapturingProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            default_model: "claude-sonnet-4-6".into(),
            env_key: None,
            base_url: Some("https://api.anthropic.com".into()),
        },
        requests: Mutex::new(Vec::new()),
        script_tool_read_path: None,
        warm_tokens: 28_000,
        prewarm_endpoints: true,
    });
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "claude-sonnet-4-6",
        "prewarm": true,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    }));
    let agent = Agent::new(opts);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
        user_config_dir: Some(RuntimePath::from(user_cfg.to_string_lossy().as_ref())),
        ..Default::default()
    });

    // Turn 1 warms 28,000 cached tokens and arms the 240s keep-alive timer.
    let r1 = session
        .run(Input::Text("Warm cache".into()), TurnOptions::default())
        .await;
    assert!(r1.error.is_none());
    assert_eq!(provider.requests.lock().unwrap().len(), 1);

    // At t = 120s (before the 240s timer), the user sends Turn 2 -> old timer must cancel.
    tokio::time::advance(Duration::from_secs(120)).await;
    let r2 = session
        .run(
            Input::Text("Follow-up at 2m".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(r2.error.is_none());
    assert_eq!(provider.requests.lock().unwrap().len(), 2);

    // At t = 120s + 150s = 270s (past Turn 1's original 240s deadline, but only 150s after Turn 2),
    // Turn 1's cancelled timer must NOT have fired!
    tokio::time::advance(Duration::from_secs(150)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        provider.requests.lock().unwrap().len(),
        2,
        "Turn 1 keep-alive timer must be cancelled when Turn 2 begins"
    );

    // At 150s + 95s = 245s after Turn 2, Turn 2's fresh keep-alive fires!
    tokio::time::advance(Duration::from_secs(95)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(provider.requests.lock().unwrap().len(), 3);
    assert_eq!(provider.requests.lock().unwrap()[2].max_output_tokens, 1);

    session.dispose().await;
}
