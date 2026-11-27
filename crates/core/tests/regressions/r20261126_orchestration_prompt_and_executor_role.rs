//! Defect: orchestration mode suffered three compounding runtime regressions in production:
//! (1) `system_prompt::orchestration_guidance_with` used `SystemPromptPart.name = "orchestration-mode"`,
//!     colliding with `multi_agent::MODE_PART` so `query/orchestration.rs` stripped `ORCHESTRATION_GUIDANCE`
//!     from 100% of main-model requests and left post-compaction `<system-reminder>` snapshots without any
//!     reminder that `write`/`edit`/`multi_edit`/`apply_patch` are gated on the main model;
//! (2) `session_spawn.rs` checked `def.prompt` before `opts.orchestration_guard.is_some()`, so built-in
//!     `general-purpose` shadowed `EXECUTOR_SYSTEM_ROLE`, causing workers to apply concurrent sub-agent
//!     worktree rules and refuse `resume` stages that expanded Stage 1 file lists;
//! (3) long multi-minute foreign executor stages let eligible main-model 5-minute prompt caches expire
//!     because `PromptCacheKeepAlive` only armed after `TurnCompleted`, never during stage waits.
//! Root cause: part-name collision in `system_prompt.rs`, inverted role precedence in `session_spawn.rs`,
//! and missing stage-scoped cache keep-alive in `session_orchestrator.rs`.
//! Fixed by: 2026-11-26 orchestration prompt retention, serial executor role precedence, and stage keep-alive.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{
    InferenceTarget, InferenceTargetState, Input, Session, TurnOptions,
};
use klaus_agent_core::observability::cost_tracker::ModelPricing;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn warm_usage() -> Usage {
    Usage {
        input_tokens: 1_000,
        output_tokens: 32,
        cache_read_input_tokens: 24_000,
        cache_creation_input_tokens: 2_000,
        total_tokens: None,
    }
}

fn text_reply(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::TextDelta { text: text.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: warm_usage(),
        },
    ]
}

fn tool_call_reply_with_sig(
    id: &str,
    name: &str,
    args: serde_json::Value,
    thought_signature: Option<&str>,
) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: thought_signature.map(Into::into),
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: args.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: warm_usage(),
        },
    ]
}

fn tool_call_reply(id: &str, name: &str, args: serde_json::Value) -> Vec<ModelStreamEvent> {
    tool_call_reply_with_sig(id, name, args, None)
}

enum ScriptStep {
    Immediate(Vec<ModelStreamEvent>),
    Delayed(Duration, Vec<ModelStreamEvent>),
}

struct ScriptedProvider {
    info: ProviderInfo,
    steps: Mutex<VecDeque<ScriptStep>>,
    requests: Mutex<Vec<ModelRequest>>,
    prewarm_endpoints: bool,
}

impl ScriptedProvider {
    fn new(name: &str, model: &str, steps: Vec<ScriptStep>, prewarm_endpoints: bool) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            steps: Mutex::new(steps.into()),
            requests: Mutex::new(Vec::new()),
            prewarm_endpoints,
        })
    }

    fn captured_requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ModelProvider for ScriptedProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn has_prewarm_endpoints(&self) -> bool {
        self.prewarm_endpoints
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn model_metadata(&self, _model: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities::default()
    }

    fn estimate_tokens(&self, _request: &ModelRequest) -> Option<u64> {
        Some(1_000)
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        let next = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| ScriptStep::Immediate(text_reply("fallback reply")));
        match next {
            ScriptStep::Immediate(events) => Box::pin(futures::stream::iter(events)),
            ScriptStep::Delayed(delay, events) => Box::pin(async_stream::stream! {
                tokio::select! {
                    biased;
                    _ = signal.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                for event in events {
                    yield event;
                }
            }),
        }
    }
}

fn pricing_entry() -> ModelPricing {
    ModelPricing {
        input_per_m_tok: 1.0,
        output_per_m_tok: 2.0,
        cache_read_per_m_tok: 0.1,
        cache_write_per_m_tok: 1.25,
    }
}

fn extract_agent_id_from_tool_results(request: &ModelRequest) -> Option<String> {
    for message in request.messages.iter().rev() {
        if let Message::ToolResult(tr) = message {
            for out in &tr.content {
                if let klaus_agent_core::schemas::content::OutputBlock::Text(tb) = out {
                    let s = tb.text.try_to_utf8().ok()?;
                    if let Some(rest) = s.split("agentId: ").nth(1) {
                        let id: String = rest
                            .chars()
                            .take_while(|c| *c != ',' && *c != ')')
                            .collect();
                        if !id.is_empty() {
                            return Some(id);
                        }
                    }
                }
            }
        }
    }
    None
}

fn flatten_message_texts(request: &ModelRequest) -> String {
    let mut out = String::new();
    for message in &request.messages {
        if let Message::User(user) = message {
            for block in &user.content {
                if let ContentBlock::Text(tb) = block {
                    if let Ok(s) = tb.text.try_to_utf8() {
                        out.push_str(&s);
                        out.push('\n');
                    }
                }
            }
        }
    }
    out
}

async fn build_orchestrated_session(
    main: Arc<ScriptedProvider>,
    executor: Arc<ScriptedProvider>,
    extra_config: serde_json::Value,
) -> (RuntimeProcessContext, tempfile::TempDir, Arc<Session>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let root = temp_dir.path();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = main;
    options.tools = None;
    options.include_default_agents = Some(true);
    options.system_prompt_provider = Some(Arc::new(|| {
        Box::pin(async {
            vec![SystemPromptPart {
                name: "base".into(),
                text: "Base instructions for workspace engineering.".into(),
                cacheable: Some(true),
                cache_scope: Some(klaus_agent_core::types::provider::CacheScope::Global),
            }]
        })
    }));
    for (key, value) in json!({
        "model": "gemini-4-carbon-high",
        "approvalPolicy": "never",
        "sandboxMode": "danger-full-access",
        "prewarm": false,
        "fork": false,
        "autoMemory": false,
        "agentMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "speculativeCompaction": false,
        "earlyMicrocompact": false,
        "idleMicrocompact": false,
        "compactionSelfCheck": false,
        "loopDetection": false,
        "compactBufferTokens": 0,
        "maxOutputTokens": 4096
    })
    .as_object()
    .unwrap()
    {
        options.config.as_mut().unwrap()[key] = value.clone();
    }
    for (key, value) in extra_config.as_object().unwrap() {
        options.config.as_mut().unwrap()[key] = value.clone();
    }
    options.pricing = Some(std::collections::HashMap::from([
        ("gemini-4-carbon-high".into(), pricing_entry()),
        ("claude-sonnet-4-6".into(), pricing_entry()),
    ]));
    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    session
        .install_orchestration(
            true,
            Some(InferenceTargetState::Ready(InferenceTarget {
                source: "codex".into(),
                model_id: "gpt-6.2-sol".into(),
                model: "gpt-6.2-sol".into(),
                provider: executor,
                review_requirements: None,
                pricing: vec![("gpt-6.2-sol".into(), pricing_entry())],
            })),
            true,
        )
        .unwrap();
    (process, temp_dir, session)
}

#[tokio::test]
async fn orchestration_main_retains_guidance_across_compaction_and_executor_receives_serial_role() {
    // Main model script:
    // 1. Dispatch Stage 1 via orchestrate_execution
    // 2. Inspect Stage 1 result and dynamically resume the returned agentId for Stage 2
    //    (we pre-seed a custom provider wrapper or inspect Stage 1's deterministic first turn).
    // Even simpler: drive Stage 1 in Turn 1, extract agentId from history, and drive Stage 2 with
    // `resume: agentId` after manual compaction in Turn 2! That tests BOTH `resume` AND
    // post-compaction guidance retention in one clean flow!
    let main = ScriptedProvider::new(
        "gemini",
        "gemini-4-carbon-high",
        vec![
            ScriptStep::Immediate(tool_call_reply_with_sig(
                "call-stage-1",
                "orchestrate_execution",
                json!({
                    "description": "Stage 1 discovery and initial edit",
                    "plan": "Inspect target module and apply initial change."
                }),
                Some("sig-stage-1"),
            )),
            ScriptStep::Immediate(text_reply(
                "Stage 1 completed; ready for follow-up stage after compaction.",
            )),
            // Step 3 is the manual `session.compact(None)` summary call on the main model:
            ScriptStep::Immediate(text_reply(
                "<summary>Stage 1 completed cleanly; Stage 2 remains.</summary>",
            )),
        ],
        false,
    );
    let executor = ScriptedProvider::new(
        "openai",
        "gpt-6.2-sol",
        vec![
            ScriptStep::Immediate(text_reply("Stage 1 worker report: updated initial file.")),
            ScriptStep::Immediate(text_reply(
                "Stage 2 resumed worker report: extended scope cleanly.",
            )),
        ],
        false,
    );

    let (_process, _temp_dir, session) = build_orchestrated_session(
        main.clone(),
        executor.clone(),
        json!({
            "modelSource": { "source": "antigravity" }
        }),
    )
    .await;

    // Turn 1: Main model dispatches Stage 1 to executor.
    let turn1 = session
        .run(
            Input::Text("Implement the two-stage workspace change.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "turn1 failed: {:?}", turn1.error);

    let main_reqs_turn1 = main.captured_requests();
    assert_eq!(
        main_reqs_turn1.len(),
        2,
        "expected 2 main requests in turn 1"
    );

    // 1. Main model request MUST retain `orchestration-guidance` in `system` (previously stripped
    //    because its name collided with `multi_agent::MODE_PART = "orchestration-mode"`).
    for (idx, req) in main_reqs_turn1.iter().enumerate() {
        let guidance = req
            .system
            .iter()
            .find(|part| part.name == "orchestration-guidance");
        assert!(
            guidance.is_some(),
            "main request #{idx} lost `orchestration-guidance` from request.system: {:?}",
            req.system.iter().map(|p| &p.name).collect::<Vec<_>>()
        );
        assert!(
            guidance
                .unwrap()
                .text
                .try_to_utf8()
                .unwrap()
                .contains("never call them directly; dispatch workspace file mutations"),
            "unexpected orchestration guidance text"
        );
    }

    // Extract the spawned worker's agentId from main request #2's ToolResult so Turn 2 can pass `resume`.
    let stage1_agent_id =
        extract_agent_id_from_tool_results(&main_reqs_turn1[1]).unwrap_or_else(|| {
            panic!(
                "main step 2 must contain Stage 1 agentId in tool result; messages={:?}",
                main_reqs_turn1[1].messages
            )
        });

    // 2. Executor Stage 1 request MUST receive `EXECUTOR_SYSTEM_ROLE` rather than built-in
    //    `general-purpose`'s prompt, and MUST NOT inherit main-only `orchestration-guidance`.
    let exec_reqs_after_stage1 = executor.captured_requests();
    assert_eq!(exec_reqs_after_stage1.len(), 1);
    let exec_stage1_sys: Vec<String> = exec_reqs_after_stage1[0]
        .system
        .iter()
        .map(|p| {
            format!(
                "{}: {}",
                p.name.try_to_utf8().unwrap(),
                p.text.try_to_utf8().unwrap()
            )
        })
        .collect();
    let joined_exec_sys = exec_stage1_sys.join("\n");
    assert!(
        joined_exec_sys.contains("dedicated serial workspace executor"),
        "executor system prompt missing EXECUTOR_SYSTEM_ROLE:\n{joined_exec_sys}"
    );
    assert!(
        !joined_exec_sys.contains("You are a general-purpose sub-agent"),
        "built-in general-purpose prompt must not shadow EXECUTOR_SYSTEM_ROLE:\n{joined_exec_sys}"
    );
    assert!(
        exec_reqs_after_stage1[0]
            .system
            .iter()
            .all(|p| p.name != "orchestration-guidance"),
        "executor child must not inherit main-only orchestration-guidance"
    );

    // Compact the main conversation to simulate the post-compaction boundary where 95.7% of
    // PLANNER_EDIT_GATE collisions previously occurred.
    let compact_outcome = session.compact(None).await.expect("compact must succeed");
    assert!(
        compact_outcome.compacted,
        "expected compaction to run: {:?}",
        compact_outcome.reason
    );

    // Queue Turn 2 scripts on main: resume the Stage 1 worker with expanded Stage 2 files, then finish.
    {
        let mut steps = main.steps.lock().unwrap();
        steps.push_back(ScriptStep::Immediate(tool_call_reply_with_sig(
            "call-stage-2",
            "orchestrate_execution",
            json!({
                "description": "Stage 2 expanded files via resume",
                "plan": "Extend changes to adjacent module files and verify.",
                "resume": stage1_agent_id
            }),
            Some("sig-stage-2"),
        )));
        steps.push_back(ScriptStep::Immediate(text_reply(
            "Both stages completed cleanly.",
        )));
    }

    let turn2 = session
        .run(
            Input::Text("Continue with Stage 2.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "turn2 failed: {:?}", turn2.error);

    let all_main_reqs = main.captured_requests();
    assert!(
        all_main_reqs.len() >= 5,
        "expected Turn 1 (2) + compact (1) + Turn 2 (2) main requests, got {}",
        all_main_reqs.len()
    );
    let post_compact_req = &all_main_reqs[3];
    assert!(
        post_compact_req
            .system
            .iter()
            .any(|part| part.name == "orchestration-guidance"),
        "post-compaction main request must still carry `orchestration-guidance` in system"
    );
    let post_compact_messages = flatten_message_texts(post_compact_req);
    assert!(
        post_compact_messages.contains("Workspace edit orchestration is active")
            && post_compact_messages.contains("including after context compaction"),
        "post-compaction runtime reminder must explicitly restate the orchestration edit gate:\n{post_compact_messages}"
    );

    // Verify Stage 2 (`resume`) hit the same executor with `EXECUTOR_RESUME_ROLE_HEADER` and kept
    // `EXECUTOR_SYSTEM_ROLE`.
    let all_exec_reqs = executor.captured_requests();
    assert_eq!(
        all_exec_reqs.len(),
        2,
        "expected Stage 1 + resumed Stage 2 on executor"
    );
    let resumed_messages = flatten_message_texts(&all_exec_reqs[1]);
    assert!(
        resumed_messages.contains(
            "continuing as the execution model for the next stage prepared by the main planning model"
        ) && resumed_messages.contains("subagent-message-origin provenance marks message transport only"),
        "resumed stage must carry explicit stage-continuation scope authorization:\n{resumed_messages}"
    );

    // Verify direct main-model `write` remains mechanically blocked by `PLANNER_EDIT_GATE` too.
    {
        let mut steps = main.steps.lock().unwrap();
        steps.push_back(ScriptStep::Immediate(tool_call_reply_with_sig(
            "call-illegal-write",
            "write",
            json!({
                "path": "/tmp/should-not-write-from-planner.txt",
                "content": "blocked"
            }),
            Some("sig-write"),
        )));
        steps.push_back(ScriptStep::Immediate(text_reply(
            "Acknowledged planner edit gate.",
        )));
    }
    let turn3 = session
        .run(
            Input::Text("Try direct write.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn3.error.is_none());
    let final_req = main.captured_requests().pop().unwrap();
    let saw_gate_refusal = final_req.messages.iter().any(|m| match m {
        Message::ToolResult(tr) => {
            tr.is_error
                && tr.content.iter().any(|o| match o {
                    klaus_agent_core::schemas::content::OutputBlock::Text(t) => t
                        .text
                        .try_to_utf8()
                        .unwrap_or_default()
                        .contains("Orchestration mode is active: dispatch workspace edits through orchestrate_execution"),
                    _ => false,
                })
        }
        _ => false,
    });
    assert!(
        saw_gate_refusal,
        "planner edit gate must still block direct write"
    );
}

#[tokio::test(start_paused = true)]
async fn stage_cache_keepalive_refreshes_eligible_main_model_cache_during_long_foreign_stage() {
    let main = ScriptedProvider::new(
        "anthropic",
        "claude-sonnet-4-6",
        vec![
            // Step 1: Main model warms >= 16,384 cache tokens and dispatches a 250s stage (> 240s TTL refresh).
            ScriptStep::Immediate(tool_call_reply(
                "call-long-stage",
                "orchestrate_execution",
                json!({
                    "description": "Run full acceptance suite",
                    "plan": "Execute full build and test suite."
                }),
            )),
            // Step 2: Background stage keep-alive ping fires at t = 240s (`max_output_tokens == 1`).
            ScriptStep::Immediate(text_reply("k")),
            // Step 3: Main model post-stage synthesis at t = 250s.
            ScriptStep::Immediate(text_reply("Acceptance suite passed.")),
        ],
        true,
    );
    let executor = ScriptedProvider::new(
        "openai",
        "gpt-6.2-sol",
        vec![ScriptStep::Delayed(
            Duration::from_secs(250),
            text_reply("250s suite completed."),
        )],
        false,
    );

    let (_process, _temp_dir, session) = build_orchestrated_session(
        main.clone(),
        executor.clone(),
        json!({
            "model": "claude-sonnet-4-6",
            "prewarm": true
        }),
    )
    .await;

    let turn = session
        .run(
            Input::Text("Run full acceptance verification.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn.error.is_none(), "turn failed: {:?}", turn.error);

    let main_reqs = main.captured_requests();
    assert_eq!(
        main_reqs.len(),
        3,
        "expected initial step + 240s stage keep-alive ping + post-stage step, got {}",
        main_reqs.len()
    );
    assert_eq!(
        main_reqs[1].max_output_tokens, 1,
        "second main request during 250s foreign stage must be the 1-token cache keep-alive ping"
    );
    assert_eq!(
        main_reqs[1].messages.len(),
        main_reqs[0].messages.len(),
        "stage keep-alive ping must replay the exact prefix from step 1 without mutating history"
    );
    assert!(
        main_reqs[2].messages.len() > main_reqs[0].messages.len(),
        "third main request must be the post-stage continuation carrying the stage tool result"
    );
}
