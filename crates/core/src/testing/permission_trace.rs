use crate::engine::deps::{noop_compactor, EngineDeps};
use crate::engine::query::{query, QueryParams, SystemPromptResolvable};
use crate::permission::engine::{PermissionDecision, PermissionEngine, StaticPermissionEngine};
use crate::permission::permission_update::{
    apply_add_rules, empty_session_permission_state, SessionPermissionState,
};
use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{
    runtime_codec, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::config::{default_config, ResolvedConfig};
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::testing::{ev, make_usage, scripted_call_model, ScriptedTurns};
use crate::tools::apply_patch::tool::create_apply_patch_tool;
use crate::tools::build_tool::text_result;
use crate::tools::builtins::bash::create_bash_tool;
use crate::tools::builtins::glob::create_glob_tool;
use crate::tools::builtins::read::create_read_tool;
use crate::tools::builtins::write::create_write_tool;
use crate::tools::pool::to_tool_specs;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{
    ApprovalHandler, ApprovalPolicy, ApprovalRequest, ApprovalResponse, PermissionContext,
    PermissionReason, PermissionResult, PermissionRule, SandboxMode,
};
use crate::types::provider::{ModelStopReason, ModelStreamEvent, SystemPromptPart};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SCENARIO: &str = "permission-decisions-approval-flow";
const WORK: &str = "/tmp/klaus-permission-trace/work";
const OUTSIDE: &str = "/tmp/klaus-permission-trace/outside";

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ({}) => {
        RuntimeJson::Object(RuntimeObject::new())
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("exact trace JSON expression must encode")
    };
}

pub async fn permission_trace(process_context: &RuntimeProcessContext) -> Json {
    exact_json!({
        "scenario": SCENARIO,
        "decisions": (decision_trace(process_context).await),
        "approvalFlow": (approval_flow_trace(process_context).await),
    })
}

async fn decision_trace(process_context: &RuntimeProcessContext) -> Json {
    let ctx = |sandbox, approval, planning| ctx(process_context, sandbox, approval, planning);
    let read = create_read_tool();
    let write = create_write_tool();
    let glob = create_glob_tool();
    let bash = create_bash_tool();
    let apply_patch = create_apply_patch_tool();
    let mcp = Arc::new(NamedTool::new("mcp__github__get_issue", false, false));
    let self_deny = Arc::new(NamedTool::new("selfdeny", false, true));

    let mut out: Vec<Json> = Vec::new();
    eval(
        &mut out,
        "read-only-policy-read-allows",
        config(&[], &[]),
        read.clone(),
        exact_json!({ "path": "src/index.ts" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "read-only-policy-write-requires-approval",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "src/index.ts", "content": "x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "full-access-write-allows",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "src/index.ts", "content": "x" }),
        ctx(SandboxMode::DangerFullAccess, ApprovalPolicy::Never, false),
    )
    .await;
    eval(
        &mut out,
        "plan-write-denies",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "src/index.ts", "content": "x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, true),
    )
    .await;
    eval(
        &mut out,
        "never-policy-write-denies",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "src/index.ts", "content": "x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::Never, false),
    )
    .await;
    eval(
        &mut out,
        "config-bash-allow-prefix-allows",
        config(&["Bash(git status)"], &[]),
        bash.clone(),
        exact_json!({ "command": "git status -sb" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "config-bash-deny-beats-full-access",
        config(&[], &["Bash(rm)"]),
        bash.clone(),
        exact_json!({ "command": "rm -rf /" }),
        ctx(SandboxMode::DangerFullAccess, ApprovalPolicy::Never, false),
    )
    .await;
    eval(
        &mut out,
        "bash-dangerous-removal-asks-without-suggestion",
        config(&["Bash(rm)"], &[]),
        bash.clone(),
        exact_json!({ "command": "rm -rf /" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "unsafe-compound-bash-ask-omits-suggestion",
        config(&[], &[]),
        bash.clone(),
        exact_json!({ "command": "npm install foo && rm -rf /tmp/x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    let mut ask_ctx = ctx(SandboxMode::DangerFullAccess, ApprovalPolicy::Never, false);
    ask_ctx.permission.ask_rules = vec![rule("write", None)];
    eval(
        &mut out,
        "generic-ask-rule-beats-full-access",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "x", "content": "y" }),
        ask_ctx,
    )
    .await;
    eval(
        &mut out,
        "read-deny-path-rule-blocks-read",
        config(&[], &["Read(secrets/**)"]),
        read.clone(),
        exact_json!({ "path": "secrets/key.txt" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "path-allow-rule-cannot-widen-read-only-policy",
        config(&["Write(dist/**)"], &[]),
        write.clone(),
        exact_json!({ "path": "dist/bundle.js", "content": "x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "glob-read-deny-path-rule-blocks-glob",
        config(&[], &["Read(logs/**)"]),
        glob.clone(),
        exact_json!({ "pattern": "*.log", "path": "logs" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "sensitive-write-asks-under-read-only",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": ".klaus/settings.json", "content": "x" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "sensitive-write-asks-under-workspace",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": ".klaus/settings.json", "content": "x" }),
        ctx(
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        ),
    )
    .await;
    // Full filesystem access removes path-based prompts; explicit rules remain.
    eval(
        &mut out,
        "sensitive-write-allows-under-full-access",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": ".klaus/settings.json", "content": "x" }),
        ctx(SandboxMode::DangerFullAccess, ApprovalPolicy::Never, false),
    )
    .await;
    eval(
        &mut out,
        "workspace-inside-cwd-allows",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": "src/a.ts", "content": "x" }),
        ctx(
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        ),
    )
    .await;
    eval(
        &mut out,
        "workspace-outside-roots-asks",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": (RuntimeString::from(format!("{OUTSIDE}/notes.txt"))), "content": "x" }),
        ctx(SandboxMode::WorkspaceWrite, ApprovalPolicy::OnRequest, false),
    )
    .await;
    let mut root_ctx = ctx(
        SandboxMode::WorkspaceWrite,
        ApprovalPolicy::OnRequest,
        false,
    );
    root_ctx.permission.additional_writable_roots = vec![RuntimePath::from(OUTSIDE)];
    eval(
        &mut out,
        "workspace-additional-root-allows",
        config(&[], &[]),
        write.clone(),
        exact_json!({ "path": (RuntimeString::from(format!("{OUTSIDE}/notes.txt"))), "content": "x" }),
        root_ctx,
    )
    .await;
    eval(
        &mut out,
        "empty-apply-patch-asks-not-vacuous-allow",
        config(&[], &[]),
        apply_patch.clone(),
        exact_json!({ "patch": "*** Begin Patch\n*** End Patch" }),
        ctx(
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        ),
    )
    .await;
    eval(
        &mut out,
        "confined-outside-read-asks-before-plan-baseline",
        config(&[], &[]),
        read.clone(),
        exact_json!({ "path": "/etc/hosts" }),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, true),
    )
    .await;
    eval(
        &mut out,
        "mcp-wildcard-allow-covers-server-tool",
        config(&["mcp__github__*"], &[]),
        mcp.clone(),
        exact_json!({}),
        ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false),
    )
    .await;
    eval(
        &mut out,
        "mcp-wildcard-deny-beats-full-access",
        config(&[], &["mcp__github__*"]),
        mcp,
        exact_json!({}),
        ctx(SandboxMode::DangerFullAccess, ApprovalPolicy::Never, false),
    )
    .await;
    let mut self_ctx = ctx(SandboxMode::ReadOnly, ApprovalPolicy::OnRequest, false);
    self_ctx.permission.allow_rules = vec![rule("selfdeny", None)];
    eval(
        &mut out,
        "tool-self-deny-beats-session-allow",
        config(&[], &[]),
        self_deny,
        exact_json!({}),
        self_ctx,
    )
    .await;

    Json::Array(out)
}

async fn eval(
    out: &mut Vec<Json>,
    name: &str,
    config: ResolvedConfig,
    tool: Arc<dyn Tool>,
    input: Json,
    ctx: ToolContext,
) {
    let engine = StaticPermissionEngine::new(&config);
    let decision = engine.evaluate(tool.as_ref(), &input, &ctx).await;
    out.push(exact_json!({
        "name": name,
        "tool": (tool.name()),
        "input": (input),
        "decision": (normalize_decision(decision)),
    }));
}

async fn approval_flow_trace(process_context: &RuntimeProcessContext) -> Json {
    let state = Arc::new(Mutex::new(empty_session_permission_state()));
    let requests: Arc<Mutex<Vec<Json>>> = Arc::new(Mutex::new(Vec::new()));
    let updates: Arc<Mutex<Vec<Json>>> = Arc::new(Mutex::new(Vec::new()));
    let persisted_messages: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(NamedTool::new("mutate", false, false))];
    let config = default_config();

    let params = approval_query_params(
        process_context,
        tools,
        Arc::clone(&state),
        Arc::clone(&requests),
        Arc::clone(&updates),
        Arc::clone(&persisted_messages),
        config,
    );
    let events: Vec<_> = query(params, approval_deps()).collect().await;
    let event_values: Vec<Json> = events
        .iter()
        .map(|event| runtime_codec::encode_value(event).unwrap())
        .collect();
    let event_types: Vec<Json> = event_values
        .iter()
        .map(|event| event["type"].clone())
        .collect();
    let persisted_values: Vec<Json> = persisted_messages
        .lock()
        .unwrap()
        .iter()
        .map(|message| runtime_codec::encode_value(message).unwrap())
        .collect();

    exact_json!({
        "eventTypes": (event_types),
        "events": (event_values),
        "approvalRequests": (requests.lock().unwrap().clone()),
        "permissionUpdates": (updates.lock().unwrap().clone()),
        "finalSessionRules": (session_state_value(&state.lock().unwrap())),
        "persistedMessages": (persisted_values),
    })
}

fn approval_query_params(
    process_context: &RuntimeProcessContext,
    tools: Vec<Arc<dyn Tool>>,
    state: Arc<Mutex<SessionPermissionState>>,
    requests: Arc<Mutex<Vec<Json>>>,
    updates: Arc<Mutex<Vec<Json>>>,
    persisted_messages: Arc<Mutex<Vec<Message>>>,
    config: ResolvedConfig,
) -> QueryParams {
    let specs = to_tool_specs(&tools);
    let context_state = Arc::clone(&state);
    let update_state = Arc::clone(&state);
    let process_context = process_context.clone();
    QueryParams {
        retained_tool_tasks: None,
        multi_agent_instructions: false,
        thread_id: "thread-permission".into(),
        turn_id: "turn-permission".into(),
        model: "fake-model".into(),
        provider_info: None,
        messages: vec![user_message("mutate twice")],
        compaction_protected_message_ids: std::collections::HashSet::new(),
        context_window: None,
        precompute: None,
        compaction_history: None,
        recover_compaction_history: None,
        compaction_output: None,
        tools,
        tool_specs: specs,
        refresh_tools: None,
        refresh_tools_async: None,
        agent_tool_policy: None,
        system_prompt: SystemPromptResolvable::Parts(vec![SystemPromptPart {
            name: RuntimeString::from("sys"),
            text: RuntimeString::from("you are a permission test"),
            cacheable: None,
            cache_scope: None,
        }]),
        config: config.clone(),
        max_turns: None,
        cost_so_far: None,
        tool_context_base: Arc::new(move || {
            let snapshot = context_state.lock().unwrap().clone();
            let mut ctx = ctx(
                &process_context,
                SandboxMode::ReadOnly,
                ApprovalPolicy::OnRequest,
                false,
            );
            ctx.permission.allow_rules = snapshot.allow_rules;
            ctx.permission.deny_rules = snapshot.deny_rules;
            ctx.permission.ask_rules = snapshot.ask_rules;
            ctx
        }),
        permission_engine: Arc::new(StaticPermissionEngine::new(&config)),
        hook_bus: None,
        signal: AbortSignal::new(),
        approval_handler: Some(Arc::new(RecordingApproval {
            requests: Arc::clone(&requests),
        })),
        approval_reviewer: None,
        output_schema: None,
        token_budget: None,
        goal: None,
        goal_request_identity: None,
        goal_pending_input: None,
        goal_execution_enabled: None,
        goal_usage: None,
        web_search: None,
        on_message: Some(Box::new(move |message| {
            let persisted_messages = persisted_messages.clone();
            Box::pin(async move {
                persisted_messages.lock().unwrap().push(message);
                Ok(())
            })
        })),
        prepare_request_usage: None,
        message_rollout: None,
        on_permission_update: Some(Box::new(move |items| {
            updates
                .lock()
                .unwrap()
                .push(runtime_codec::encode_value(&items).unwrap());
            let mut state = update_state.lock().unwrap();
            for item in &items {
                apply_add_rules(&mut state, item);
            }
        })),
        on_compaction: None,
        surfaced_tools: None,
        on_surface_tools: None,
        on_tool_lifecycle: None,
        provider_call_admission: None,
        on_provider_request_admitted: None,
        emit_thread_started: true,
        reminder_providers: Vec::new(),
        edited_files_reminder: None,
        effort: None,
        ultra_mode: false,
        thinking: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        plan_file_path: None,
        message_queue: None,
        input_pdf_bytes: 0,
        input_pdf_documents: 0,
        validate_native_pdf: None,
        agent_id: None,
        agent_type: None,
        context_hint_state: None,
        file_state: None,
        tool_result_budget_state: None,
        on_tool_result_budget_records: None,
        thinking_context_cold: false,
        padded_countdown_spend: Default::default(),
        task_countdown_anchor: None,
        live_model_override: None,
    }
}

fn approval_deps() -> EngineDeps {
    EngineDeps {
        provider_id: RuntimeString::from("test-provider"),
        call_model: scripted_call_model(approval_turns()),
        estimate_tokens: Arc::new(|_| 0),
        compactor: noop_compactor(),
        uuid: seq_uuid(),
        now: Arc::new(|| 0),
    }
}

fn approval_turns() -> ScriptedTurns {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use(
        "tu_mutate_1",
        "mutate",
        &exact_json!({ "value": "first" }),
    ));
    first.extend(exact_tool_use(
        "tu_mutate_2",
        "mutate",
        &exact_json!({ "value": "second" }),
    ));
    first.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(Some(13), Some(3))),
    ));
    vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("mutations complete"),
            ev::stop(
                Some(ModelStopReason::EndTurn),
                Some(make_usage(Some(5), Some(4))),
            ),
        ],
    ]
}

fn exact_tool_use(id: &str, name: &str, input: &Json) -> Vec<ModelStreamEvent> {
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
            partial_json: crate::runtime_values::runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop {
            id: RuntimeString::from(id),
        },
    ]
}

struct RecordingApproval {
    requests: Arc<Mutex<Vec<Json>>>,
}

#[async_trait]
impl ApprovalHandler for RecordingApproval {
    async fn approve(&self, req: ApprovalRequest, _signal: AbortSignal) -> ApprovalResponse {
        self.requests
            .lock()
            .unwrap()
            .push(runtime_codec::encode_value(&req).unwrap());
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: req.suggestions,
        }
    }
}

struct NamedTool {
    name: String,
    schema: JsonSchema,
    read_only: bool,
    self_deny: bool,
}

impl NamedTool {
    fn new(name: &str, read_only: bool, self_deny: bool) -> Self {
        NamedTool {
            name: name.to_string(),
            schema: named_tool_schema(),
            read_only,
            self_deny,
        }
    }
}

fn named_tool_schema() -> JsonSchema {
    let mut value = JsonSchema::new();
    value.insert("type".to_owned(), Value::String("string".to_owned()));
    let mut properties = JsonSchema::new();
    properties.insert("value".to_owned(), Value::Object(value));
    let mut schema = JsonSchema::new();
    schema.insert("type".to_owned(), Value::String("object".to_owned()));
    schema.insert("properties".to_owned(), Value::Object(properties));
    schema.insert("additionalProperties".to_owned(), Value::Bool(true));
    schema
}

#[async_trait]
impl Tool for NamedTool {
    fn static_name(&self) -> &str {
        &self.name
    }

    fn static_description(&self) -> &str {
        "permission trace test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        self.read_only
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        if self.self_deny {
            return Some(PermissionResult::Deny {
                message: RuntimeString::from("tool self-deny"),
                reason: PermissionReason::ToolSelfCheck,
            });
        }
        None
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let value = input.get("value").and_then(Json::as_str).cloned();
        let text_value = value.clone().unwrap_or_default();
        let text = RuntimeString::from(format!("{}:", self.name)).concat(&text_value);
        Ok(text_result(exact_json!({ "ran": (value) }), text, false))
    }
}

fn normalize_decision(decision: PermissionDecision) -> Json {
    match decision {
        PermissionDecision::Allow { input, reason } => exact_json!({
            "decision": "allow",
            "input": (input),
            "reason": (reason),
        }),
        PermissionDecision::Deny { message, reason } => exact_json!({
            "decision": "deny",
            "message": (message),
            "reason": (reason),
        }),
        PermissionDecision::Ask {
            message,
            input,
            reason,
            suggestions,
        } => {
            let mut value = exact_json!({
                "decision": "ask",
                "message": (message),
                "input": (input),
                "reason": (reason),
            });
            if let Some(suggestions) = suggestions {
                value["suggestions"] = runtime_codec::encode_value(&suggestions).unwrap();
            }
            value
        }
        PermissionDecision::Fatal { error } => exact_json!({
            "decision": "fatal",
            "message": (error.message),
        }),
    }
}

fn config(allow: &[&str], deny: &[&str]) -> ResolvedConfig {
    let mut config = default_config();
    config.allow = allow
        .iter()
        .map(|value| RuntimeString::from(*value))
        .collect();
    config.deny = deny
        .iter()
        .map(|value| RuntimeString::from(*value))
        .collect();
    config
}

fn rule(tool_name: &str, argument_prefix: Option<&str>) -> PermissionRule {
    PermissionRule {
        tool_name: RuntimeString::from(tool_name),
        argument_prefix: argument_prefix.map(RuntimeString::from),
        tool_name_prefix: None,
    }
}

fn ctx(
    process_context: &RuntimeProcessContext,
    sandbox_mode: SandboxMode,
    approval_policy: ApprovalPolicy,
    planning_read_only: bool,
) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process_context.clone(),
        cwd: RuntimePath::from(WORK),
        signal: AbortSignal::new(),
        file_state: Arc::new(NoopFileState),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(NoopSandbox),
        planning_read_only,
        approval_policy: Some(approval_policy),
        sandbox_mode: Some(sandbox_mode),
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        emit: std::sync::Arc::new(|_| Box::pin(async {})),
        enqueue_background_notice: None,
        record_rollout_event: None,
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
    }
}

fn user_message(text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: RuntimeString::from("perm-user-0"),
        content: vec![ContentBlock::Text(TextBlock {
            text: RuntimeString::from(text),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

fn session_state_value(state: &SessionPermissionState) -> Json {
    exact_json!({
        "allowRules": (state.allow_rules.clone()),
        "denyRules": (state.deny_rules.clone()),
        "askRules": (state.ask_rules.clone()),
    })
}

fn seq_uuid() -> Arc<dyn Fn() -> String + Send + Sync> {
    let counter = Arc::new(AtomicUsize::new(0));
    Arc::new(move || format!("perm-id-{}", counter.fetch_add(1, Ordering::SeqCst)))
}

struct NoopFileState;

impl FileStateCache for NoopFileState {
    fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
        None
    }
    fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
    fn has(&self, _path: &RuntimePath) -> bool {
        false
    }
    fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
        Vec::new()
    }
    fn delete(&self, _path: &RuntimePath) {}
    fn clear(&self) {}
    fn clone_box(&self) -> Box<dyn FileStateCache> {
        Box::new(NoopFileState)
    }
}

struct NoopSandbox;

impl SandboxBackend for NoopSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }
    fn is_available(&self) -> bool {
        false
    }
    fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
        SpawnSpec {
            file: req.argv.first().cloned().unwrap_or_default(),
            args: req.argv.iter().skip(1).cloned().collect(),
            cwd: req.cwd.clone(),
            env: RuntimeEnv::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::events::EngineEvent;

    /// The internal memory root exemption is scoped to that exact canonical root:
    /// a symlink leaving it, and a host-selected writable root that merely looks
    /// like it, both stay gated under a workspace filesystem policy.
    #[cfg(unix)]
    #[tokio::test]
    async fn internal_memory_root_bypasses_sensitive_gate_without_permitting_symlink_escape() {
        let temp = std::env::temp_dir().join(format!(
            "klaus-internal-memory-permission-{}",
            std::process::id()
        ));
        let internal = temp.join(".klaus/projects/main");
        let outside = temp.join("outside");
        std::fs::create_dir_all(&internal).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, internal.join("escape")).unwrap();

        let process = crate::runtime_process::test_runtime_process_context();
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let mut internal_ctx = ctx(
            &process,
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        );
        let canonical_internal = crate::fs::node_path::canonicalize_host(&internal).unwrap();
        internal_ctx.permission.internal_writable_roots = vec![RuntimePath::from(
            canonical_internal.to_string_lossy().into_owned(),
        )];
        // The exempted memory root must also be inside the writable boundary.
        internal_ctx.permission.additional_writable_roots =
            vec![RuntimePath::from(internal.to_string_lossy().into_owned())];

        let inside = engine
            .evaluate(
                tool.as_ref(),
                &exact_json!({
                    "path": (RuntimeString::from(internal.join("MEMORY.md").to_string_lossy().into_owned())),
                    "content": "ok"
                }),
                &internal_ctx,
            )
            .await;
        assert!(matches!(inside, PermissionDecision::Allow { .. }));

        let escaped = engine
            .evaluate(
                tool.as_ref(),
                &exact_json!({
                    "path": (RuntimeString::from(internal.join("escape/pwn.md").to_string_lossy().into_owned())),
                    "content": "no"
                }),
                &internal_ctx,
            )
            .await;
        assert!(matches!(
            escaped,
            PermissionDecision::Ask {
                reason: PermissionReason::SafetyCheck,
                ..
            }
        ));

        let mut host_ctx = ctx(
            &process,
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        );
        host_ctx.permission.additional_writable_roots =
            vec![RuntimePath::from(internal.to_string_lossy().into_owned())];
        let arbitrary_host_root = engine
            .evaluate(
                tool.as_ref(),
                &exact_json!({
                    "path": (RuntimeString::from(internal.join("host.md").to_string_lossy().into_owned())),
                    "content": "no"
                }),
                &host_ctx,
            )
            .await;
        assert!(matches!(
            arbitrary_host_root,
            PermissionDecision::Ask {
                reason: PermissionReason::SafetyCheck,
                ..
            }
        ));
        std::fs::remove_dir_all(temp).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_itself_cannot_be_promoted_to_an_internal_memory_root() {
        let temp = std::env::temp_dir().join(format!(
            "klaus-internal-memory-root-symlink-{}",
            std::process::id()
        ));
        let outside = temp.join("outside");
        let internal_link = temp.join("internal-memory");
        std::fs::create_dir_all(outside.join(".klaus")).unwrap();
        let sentinel = outside.join(".klaus/settings.json");
        std::fs::write(&sentinel, "external sentinel\n").unwrap();
        std::os::unix::fs::symlink(&outside, &internal_link).unwrap();

        let process = crate::runtime_process::test_runtime_process_context();
        let engine = StaticPermissionEngine::new(&default_config());
        let tool = create_write_tool();
        let mut internal_ctx = ctx(
            &process,
            SandboxMode::WorkspaceWrite,
            ApprovalPolicy::OnRequest,
            false,
        );
        internal_ctx.permission.internal_writable_roots = vec![RuntimePath::from(
            internal_link.to_string_lossy().into_owned(),
        )];

        let decision = engine
            .evaluate(
                tool.as_ref(),
                &exact_json!({
                    "path": (RuntimeString::from(internal_link.join(".klaus/settings.json").to_string_lossy().into_owned())),
                    "content": "must not overwrite the sentinel"
                }),
                &internal_ctx,
            )
            .await;
        assert!(matches!(
            decision,
            PermissionDecision::Ask {
                reason: PermissionReason::SafetyCheck,
                ..
            }
        ));
        assert_eq!(
            std::fs::read_to_string(&sentinel).unwrap(),
            "external sentinel\n"
        );
        std::fs::remove_dir_all(temp).ok();
    }

    #[test]
    fn exact_permission_event_and_message_wires_preserve_lone_surrogates() {
        let strings = [
            RuntimeString::from_utf16([0xd800]),
            RuntimeString::from_utf16([0xdc00]),
            RuntimeString::from_utf16([0xfffd]),
        ];
        let wires = strings
            .iter()
            .map(|text| {
                let decision = normalize_decision(PermissionDecision::Allow {
                    input: exact_json!({ "value": (text.clone()) }),
                    reason: PermissionReason::ToolSelfCheck,
                });
                let event = EngineEvent::TextDelta {
                    inference: None,
                    message_id: None,
                    phase: None,
                    text: text.clone(),
                };
                let message = Message::User(UserMessage {
                    question_answer: None,
                    uuid: text.clone(),
                    content: vec![ContentBlock::Text(TextBlock {
                        text: text.clone(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                    is_meta: None,
                });
                let value = exact_json!({
                    "decision": (decision),
                    "event": (runtime_codec::encode_value(&event).unwrap()),
                    "message": (runtime_codec::encode_value(&message).unwrap()),
                });
                assert_eq!(
                    value
                        .pointer("/decision/input/value")
                        .and_then(Json::as_str),
                    Some(text)
                );
                assert_eq!(
                    value.pointer("/event/text").and_then(Json::as_str),
                    Some(text)
                );
                assert_eq!(
                    value
                        .pointer("/message/content/0/text")
                        .and_then(Json::as_str),
                    Some(text)
                );
                crate::runtime_values::runtime_json::stringify(&value)
            })
            .collect::<Vec<_>>();

        assert_ne!(wires[0], wires[1]);
        assert_ne!(wires[0], wires[2]);
        assert_ne!(wires[1], wires[2]);
    }
}
