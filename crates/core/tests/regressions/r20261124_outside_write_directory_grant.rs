//! Defect: file writes outside workspace roots under `WorkspaceWrite` offered no
//! directory-scoped session grant, forcing repeated single-file approvals across
//! sibling edits in the same external directory.
//! Root cause: `StaticPermissionEngine` hardcoded `suggestions: None` on the
//! `WorkspaceWrite` prompt and `SessionPermissionState` only accumulated tool rules.
//! Fixed by: 2026-11-24 outside-workspace directory write grant work item.

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::tools::apply_patch::tool::create_apply_patch_tool;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalPolicy, ApprovalRequest, ApprovalResponse, PermissionContext,
    PermissionDestination, PermissionUpdate, SandboxMode,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::ToolContext;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct TempTree(PathBuf);
impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn json_input(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

fn make_ctx(cwd: &str, process: RuntimeProcessContext, sandbox_mode: SandboxMode) -> ToolContext {
    ToolContext {
        reply_gate: None,
        async_question: None,
        background_processes: Arc::new(
            klaus_agent_core::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process,
        cwd: RuntimePath::from(cwd),
        signal: AbortSignal::new(),
        file_state: Arc::new(InMemoryFileStateCache::new()),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(ProcessSandbox),
        planning_read_only: false,
        approval_policy: Some(ApprovalPolicy::OnRequest),
        sandbox_mode: Some(sandbox_mode),
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: Some(true),
        emit: Arc::new(|_| Box::pin(async {})),
        native_pdf: None,
        vision: None,
        shell_env: None,
        depth: None,
        forward_subagent_text: None,
        spawn_sub_agent: None,
        spawn_background_agent: None,
        spawn_workflow_agent: None,
        surface_tools: None,
        activate_skill_paths: None,
        tool_catalog: None,
        elicit: None,
        plan_file: None,
        apply_skill_context: None,
        run_bash: None,
        mcp_clients: None,
        background_tasks: None,
        task_list_id: None,
        task_plan: None,
        worktree: None,
        workflow_journal: None,
        swarm: None,
        cron: None,
        goal: None,
        goal_generation: None,
        goal_owned_resources: None,
        lsp: None,
        browser: None,
        browser_tool_mapping: None,
        run_hook: None,
        invoke_tool: None,
        advisor: None,
        sandbox_retry_on_denial: Some(false),
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}

#[test]
fn add_directories_permission_update_round_trips_wire_codec() {
    let original = PermissionUpdate::AddDirectories {
        directories: vec![
            RuntimePath::from("/workspace/ext-a"),
            RuntimePath::from("/workspace/ext-b"),
        ],
        destination: PermissionDestination::Session,
    };
    let encoded = runtime_codec::encode_value(&original).expect("encode addDirectories");
    assert_eq!(
        encoded,
        json_input(json!({
            "type": "addDirectories",
            "directories": ["/workspace/ext-a", "/workspace/ext-b"],
            "destination": "session"
        }))
    );
    let decoded: PermissionUpdate =
        runtime_codec::decode_value(&encoded).expect("decode addDirectories");
    assert_eq!(decoded, original);
}

#[tokio::test]
async fn workspace_write_suggests_only_safe_non_broad_outside_directories() {
    let tree =
        TempTree(std::env::temp_dir().join(format!("outside-dir-grant-{}", uuid::Uuid::new_v4())));
    let cwd = tree.0.join("work");
    let ext_dir = tree.0.join("external-repo");
    let secret_dir = tree.0.join("secret-target");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&ext_dir).unwrap();
    std::fs::create_dir_all(&secret_dir).unwrap();

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let engine = StaticPermissionEngine::new(&default_config());
    let write = create_write_tool();
    let apply_patch = create_apply_patch_tool();
    let ctx = make_ctx(
        cwd.to_str().unwrap(),
        process.clone(),
        SandboxMode::WorkspaceWrite,
    );

    // 1) Normal outside-workspace file write suggests its parent directory for the session.
    let ext_file = ext_dir.join("notes.md");
    let decision = engine
        .evaluate(
            write.as_ref(),
            &json_input(json!({ "path": ext_file, "content": "hello" })),
            &ctx,
        )
        .await;
    match decision {
        PermissionDecision::Ask { suggestions, .. } => {
            assert_eq!(
                suggestions,
                Some(vec![PermissionUpdate::AddDirectories {
                    directories: vec![RuntimePath::from(ext_dir.to_str().unwrap())],
                    destination: PermissionDestination::Session,
                }])
            );
        }
        other => panic!("expected Ask with directory suggestion, got {other:?}"),
    }

    // 2) Mixed patch (one file inside cwd, two files in external-repo) suggests only external-repo once.
    let patch_text = format!(
        "*** Begin Patch\n*** Add File: inside.txt\n+ok\n*** Add File: {}\n+one\n*** Add File: {}\n+two\n*** End Patch",
        ext_dir.join("a.txt").display(),
        ext_dir.join("b.txt").display()
    );
    let patch_decision = engine
        .evaluate(
            apply_patch.as_ref(),
            &json_input(json!({ "patch": patch_text })),
            &ctx,
        )
        .await;
    match patch_decision {
        PermissionDecision::Ask { suggestions, .. } => {
            assert_eq!(
                suggestions,
                Some(vec![PermissionUpdate::AddDirectories {
                    directories: vec![RuntimePath::from(ext_dir.to_str().unwrap())],
                    destination: PermissionDestination::Session,
                }])
            );
        }
        other => panic!("expected Ask for mixed patch, got {other:?}"),
    }

    // 3) Broad roots (`/tmp/direct.txt`, `$HOME/direct.txt`), sensitive paths, and ReadOnly mode omit suggestions.
    let top_tmp = engine
        .evaluate(
            write.as_ref(),
            &json_input(json!({ "path": "/tmp/direct-top-level.txt", "content": "x" })),
            &ctx,
        )
        .await;
    assert!(matches!(
        top_tmp,
        PermissionDecision::Ask {
            suggestions: None,
            ..
        }
    ));

    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() {
            let home_top = engine
                .evaluate(
                    write.as_ref(),
                    &json_input(json!({
                        "path": PathBuf::from(&home).join("direct-home-file.txt"),
                        "content": "x"
                    })),
                    &ctx,
                )
                .await;
            assert!(matches!(
                home_top,
                PermissionDecision::Ask {
                    suggestions: None,
                    ..
                }
            ));
        }
    }

    let sensitive = engine
        .evaluate(
            write.as_ref(),
            &json_input(json!({
                "path": ext_dir.join(".klaus/settings.json"),
                "content": "{}"
            })),
            &ctx,
        )
        .await;
    assert!(matches!(
        sensitive,
        PermissionDecision::Ask {
            suggestions: None,
            ..
        }
    ));

    let readonly_ctx = make_ctx(
        cwd.to_str().unwrap(),
        process.clone(),
        SandboxMode::ReadOnly,
    );
    let readonly_decision = engine
        .evaluate(
            write.as_ref(),
            &json_input(json!({ "path": ext_dir.join("notes.md"), "content": "x" })),
            &readonly_ctx,
        )
        .await;
    assert!(matches!(
        readonly_decision,
        PermissionDecision::Ask {
            suggestions: None,
            ..
        }
    ));

    // 4) Symlink leaf escaping `ext_dir` into `secret_dir` refuses directory suggestion.
    #[cfg(unix)]
    {
        let target_real = secret_dir.join("escaped.txt");
        std::fs::write(&target_real, "secret").unwrap();
        let link_path = ext_dir.join("escaped-link.txt");
        std::os::unix::fs::symlink(&target_real, &link_path).unwrap();
        let link_decision = engine
            .evaluate(
                write.as_ref(),
                &json_input(json!({ "path": link_path, "content": "overwrite" })),
                &ctx,
            )
            .await;
        assert!(matches!(
            link_decision,
            PermissionDecision::Ask {
                suggestions: None,
                ..
            }
        ));
    }

    process.shutdown().await.unwrap();
}

struct AcceptDirectoryGrantApproval {
    seen_targets: Arc<Mutex<Vec<RuntimeString>>>,
}

#[async_trait]
impl ApprovalHandler for AcceptDirectoryGrantApproval {
    async fn approve(&self, req: ApprovalRequest, _cancel: CancellationToken) -> ApprovalResponse {
        let path = req
            .input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        self.seen_targets.lock().unwrap().push(path);
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: req.suggestions,
        }
    }
}

struct ScriptedWriteProvider {
    info: ProviderInfo,
    turns: Mutex<Vec<Vec<ModelStreamEvent>>>,
}

impl ModelProvider for ScriptedWriteProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let events = self.turns.lock().unwrap().remove(0);
        Box::pin(futures::stream::iter(events))
    }
}

fn tool_use_events(id: &str, name: &str, input: &RuntimeJson) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: RuntimeString::from(id),
            name: RuntimeString::from(name),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: RuntimeString::from(id),
            partial_json: runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop {
            id: RuntimeString::from(id),
        },
    ]
}

#[tokio::test]
async fn session_directory_grant_unlocks_subsequent_writes_in_same_directory_only() {
    let tree =
        TempTree(std::env::temp_dir().join(format!("outside-dir-flow-{}", uuid::Uuid::new_v4())));
    let cwd = tree.0.join("work");
    let ext_a = tree.0.join("ext-a");
    let ext_b = tree.0.join("ext-b");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&ext_a).unwrap();
    std::fs::create_dir_all(&ext_b).unwrap();

    let first_file = ext_a.join("first.txt");
    let nested_second_file = ext_a.join("nested/second.txt");
    let sensitive_in_granted = ext_a.join(".vscode/settings.json");
    let other_dir_file = ext_b.join("third.txt");

    let mut first_turn = vec![ModelStreamEvent::MessageStart {
        model: "dir-grant-fixture".into(),
    }];
    first_turn.extend(tool_use_events(
        "tu_1",
        "write",
        &json_input(json!({ "path": first_file, "content": "one" })),
    ));
    first_turn.extend(tool_use_events(
        "tu_2",
        "write",
        &json_input(json!({ "path": nested_second_file, "content": "two" })),
    ));
    first_turn.extend(tool_use_events(
        "tu_3",
        "write",
        &json_input(json!({ "path": sensitive_in_granted, "content": "{}" })),
    ));
    first_turn.extend(tool_use_events(
        "tu_4",
        "write",
        &json_input(json!({ "path": other_dir_file, "content": "three" })),
    ));
    first_turn.push(ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::ToolUse,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: Some(15),
        },
    });
    let second_turn = vec![
        ModelStreamEvent::MessageStart {
            model: "dir-grant-fixture".into(),
        },
        ModelStreamEvent::TextDelta {
            text: "done".into(),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: Usage {
                input_tokens: 8,
                output_tokens: 2,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: Some(10),
            },
        },
    ];

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let seen_targets = Arc::new(Mutex::new(Vec::new()));
    let mut options = agent_options(process.clone());
    options.cwd = Some(cwd.to_str().unwrap().to_owned());
    options.tools = Some(vec![create_write_tool()]);
    options.config = Some(json!({
        "approval_policy": "on-request",
        "sandbox_mode": "workspace-write",
        "sandbox_workspace_write": {
            "exclude_tmpdir_env_var": true,
            "exclude_slash_tmp": true
        }
    }));
    options.approval_handler = Some(Arc::new(AcceptDirectoryGrantApproval {
        seen_targets: Arc::clone(&seen_targets),
    }));
    options.provider = Arc::new(ScriptedWriteProvider {
        info: ProviderInfo {
            name: "dir-grant-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "dir-grant-fixture".into(),
        },
        turns: Mutex::new(vec![first_turn, second_turn]),
    });

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let _result = session
        .run(Input::Text("write files".into()), TurnOptions::default())
        .await;
    drop(session);
    process.shutdown().await.unwrap();

    // `first.txt` prompted and granted `ext-a`; `nested/second.txt` ran without prompting;
    // `.vscode/settings.json` inside `ext-a` still prompted (sensitive path); `ext-b/third.txt` prompted.
    assert_eq!(
        *seen_targets.lock().unwrap(),
        vec![
            RuntimeString::from(first_file.to_str().unwrap()),
            RuntimeString::from(sensitive_in_granted.to_str().unwrap()),
            RuntimeString::from(other_dir_file.to_str().unwrap()),
        ]
    );
    assert_eq!(std::fs::read_to_string(&first_file).unwrap(), "one");
    assert_eq!(std::fs::read_to_string(&nested_second_file).unwrap(), "two");
    assert_eq!(
        std::fs::read_to_string(&sensitive_in_granted).unwrap(),
        "{}"
    );
    assert_eq!(std::fs::read_to_string(&other_dir_file).unwrap(), "three");
}
