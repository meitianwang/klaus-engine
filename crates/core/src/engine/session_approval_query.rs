//! Isolated read-only queries used to assess actions that require approval.

#[path = "session_approval_query_usage.rs"]
mod compaction_usage;

use super::{bare_tool_context, AnyTool, ApprovalQueryAdmission, InferenceBundle, Session};
use crate::context::total_tokens_reminder::PaddedCountdownSpend;
use crate::engine::deps::{production_deps, EngineDepsOverrides};
use crate::engine::query::{
    query_isolated, QueryParams, SystemPromptResolvable, ToolContextFactory,
};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{RuntimeJson, RuntimePath, RuntimeString};
use crate::schemas::config::{ResolvedConfig, TotalTokensReminderMode, WebSearchAccess};
use crate::schemas::content::{ImageDetail, OutputBlock};
use crate::schemas::effort::EffortValue;
use crate::schemas::message::{Message, Usage};
use crate::sync_ext::MutexExt;
use crate::tools::builtins::interactive_shell::{
    create_exec_command_tool, create_write_stdin_tool,
};
use crate::tools::file_state::InMemoryFileStateCache;
use crate::tools::pool::to_tool_specs;
use crate::types::abort::AbortSignal;
use crate::types::errors::{EngineError, EngineErrorCode, TerminalReason, ToolError};
use crate::types::events::{EngineEvent, ThreadItem};
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{ApprovalPolicy, SandboxMode};
use crate::types::provider::{
    ModelCapabilities, ModelProvider, ProviderCapabilities, SystemPromptPart, ThinkingConfig,
    ToolSpec,
};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolInputKind, ToolProgress, ToolResult,
    ToolResultSerializable,
};
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const INTERRUPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Process and tool resources belong to one reusable reviewer, never to its parent.
pub(crate) struct ApprovalQueryState {
    process_context: RuntimeProcessContext,
    background: Arc<crate::command_runtime::background::BackgroundCommandRegistry>,
    file_state: Arc<InMemoryFileStateCache>,
    tools: Vec<AnyTool>,
}

impl ApprovalQueryState {
    /// Initialize the independent runtime before a review needs its first model request.
    pub(crate) fn new(
        session: &Session,
        _config: &ResolvedConfig,
        metadata: &ModelCapabilities,
    ) -> Result<Arc<Self>, EngineError> {
        let process_context = session
            .process_context
            .isolated_context()
            .map_err(|error| {
                internal_error(format!(
                    "Cannot initialize approval review runtime: {error}"
                ))
            })?;
        let tools: Vec<AnyTool> = vec![
            inspection_tool(create_exec_command_tool()),
            inspection_tool(create_write_stdin_tool()),
            Arc::new(ViewImageTool::new(metadata)),
        ];
        Ok(Arc::new(Self {
            process_context,
            background: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            file_state: Arc::new(InMemoryFileStateCache::new()),
            tools,
        }))
    }

    fn filtered_tools(&self, capabilities: &ProviderCapabilities) -> Vec<AnyTool> {
        if !capabilities.tools {
            return Vec::new();
        }
        self.tools
            .iter()
            .filter(|tool| capabilities.vision || tool.name() != "view_image")
            .cloned()
            .collect()
    }

    pub(crate) fn tool_specs(&self, capabilities: &ProviderCapabilities) -> Vec<ToolSpec> {
        to_tool_specs(&self.filtered_tools(capabilities))
    }

    async fn interrupt(&self) -> Result<(), RuntimeString> {
        for process in self.background.snapshot().processes {
            if !process.status.is_terminal() {
                self.background
                    .stop_with_signal(&process.shell_id, crate::tools::shell::Signal::Kill);
            }
        }
        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> Result<(), RuntimeString> {
        let background = self
            .background
            .close_and_wait(Instant::now() + INTERRUPT_DRAIN_TIMEOUT)
            .await;
        let process = self
            .process_context
            .shutdown()
            .await
            .map_err(|error| RuntimeString::from(error.to_string()));
        background.and(process)
    }
}

/// Inputs are already prepared by the context builder and model target resolver.
pub(crate) struct ApprovalQueryInput {
    pub state: Arc<ApprovalQueryState>,
    pub provider: Arc<dyn ModelProvider>,
    pub accounting: Arc<InferenceBundle>,
    pub config: ResolvedConfig,
    pub model: RuntimeString,
    pub effort: Option<EffortValue>,
    pub system: Vec<SystemPromptPart>,
    pub history: Vec<Message>,
    pub protected_message_ids: HashSet<RuntimeString>,
    pub restore_retained: Option<super::ApprovalQueryRestore>,
    /// Effective selected-model input limit after the shared safety margin.
    pub max_input_tokens: usize,
    pub schema: RuntimeJson,
    pub thread_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub cache_key: RuntimeString,
    pub signal: AbortSignal,
    pub deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ApprovalQueryBudgetExceeded {
    pub estimated_tokens: usize,
    pub max_input_tokens: usize,
}

#[derive(Debug)]
pub(crate) enum ApprovalQueryOutcome {
    Completed,
    Failed(EngineError),
    InputBudgetExceeded(ApprovalQueryBudgetExceeded),
    TimedOut,
    Aborted,
}

pub(crate) struct ApprovalQueryResult {
    pub history: Vec<Message>,
    pub final_text: Option<RuntimeString>,
    pub usage: Option<Usage>,
    pub outcome: ApprovalQueryOutcome,
    pub reusable: bool,
    pub turn_completed: bool,
}

impl Session {
    /// Drive exactly one isolated turn through the normal model and tool loop.
    pub(crate) async fn run_approval_query(
        self: &Arc<Self>,
        input: ApprovalQueryInput,
    ) -> ApprovalQueryResult {
        let ApprovalQueryInput {
            state,
            provider,
            accounting,
            mut config,
            model,
            effort,
            system,
            history,
            protected_message_ids,
            restore_retained,
            max_input_tokens,
            schema,
            thread_id,
            turn_id,
            cache_key,
            signal,
            deadline,
        } = input;
        let history = Arc::new(Mutex::new(history));
        if signal.is_cancelled() {
            return query_result(
                &history,
                None,
                None,
                ApprovalQueryOutcome::Aborted,
                true,
                false,
            );
        }
        if Instant::now() >= deadline {
            return query_result(
                &history,
                None,
                None,
                ApprovalQueryOutcome::TimedOut,
                true,
                false,
            );
        }
        let policy = self.runtime_policy_snapshot();
        if !policy.sandbox_backend.is_available()
            || policy.sandbox_backend.kind() == SandboxKind::Process
        {
            return query_result(
                &history,
                None,
                None,
                ApprovalQueryOutcome::Failed(internal_error(
                    "Approval review requires an available filesystem sandbox.",
                )),
                true,
                false,
            );
        }
        let admission = ApprovalQueryAdmission::new(
            max_input_tokens,
            &protected_message_ids,
            history.clone(),
            provider.clone(),
            restore_retained,
        );
        let caps = provider.capabilities(&model);
        let parent_permission = self.permission_context_for_policy(&policy);
        let planning = policy.sandbox_backend.enforces_planning_read_only();
        let network_allowed = !planning
            && self
                .permission_engine
                .bound_task_resource_scope()
                .is_none_or(|scope| scope.network_allowed)
            && (super::runtime_network_allowed(
                policy.state.sandbox_mode,
                Some(policy.state.network_allowed),
                &policy.state.sandbox_workspace_write,
            ) || config.network_proxy.enabled);
        let sandbox: Arc<dyn SandboxBackend> = Arc::new(ReviewSandbox {
            inner: policy.sandbox_backend.clone(),
            network_allowed,
            planning,
        });
        let mut permission = parent_permission;
        permission.should_avoid_prompts = true;
        permission.should_avoid_prompts = true;
        permission.additional_writable_roots.clear();
        permission.internal_writable_roots.clear();
        config.model = model.clone();
        config.effort = effort.clone();
        config.context_window = caps.context_window;
        config.model_context_window = Some(caps.context_window);
        config.model_max_output_tokens = Some(caps.max_output_tokens);
        config.max_output_tokens = Some(
            config
                .max_output_tokens
                .unwrap_or(caps.max_output_tokens)
                .min(caps.max_output_tokens),
        );
        config.approval_policy = Some(ApprovalPolicy::Never);
        config.sandbox_mode = Some(SandboxMode::ReadOnly);
        config.context_management = None;
        config.task_budget_tokens = None;
        config.memory.enabled = false;
        config.agent_memory = false;
        config.memory_recall = false;
        config.agent_memory_snapshot = false;
        config.auto_memory = false;
        config.auto_dream = false;
        config.fork = false;
        config.goal = false;
        config.swarm = false;
        config.cron = false;
        config.worktree = false;
        config.checkpointing = false;
        config.history_snip = false;
        config.web_search.access = WebSearchAccess::Disabled;
        config.edited_files = false;
        config.lsp = false;
        config.speculative_compaction = false;
        config.early_microcompact = false;
        config.idle_microcompact = false;
        config.total_tokens_reminder = TotalTokensReminderMode::Off;
        // The provider owns establishment retries; this private loop permits one
        // interrupted-stream reconnect. The caller owns assessment retries.
        config.max_retries = 0;
        config.unbounded_network_retries = false;
        config.fallback_models.clear();
        let tools = state.filtered_tools(&caps);
        let local_signal = AbortSignal::new();
        let tool_context_base: ToolContextFactory = {
            let state = state.clone();
            let cwd = self.cwd_now();
            let local_signal = local_signal.clone();
            let permission = permission.clone();
            Arc::new(move || {
                let mut ctx = bare_tool_context(
                    state.process_context.clone(),
                    state.background.clone(),
                    cwd.clone(),
                    local_signal.clone(),
                    state.file_state.clone(),
                    permission.clone(),
                    sandbox.clone(),
                );
                ctx.approval_policy = Some(ApprovalPolicy::Never);
                ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
                ctx.network_allowed = Some(network_allowed);
                ctx.sandbox_retry_on_denial = Some(false);
                ctx.planning_read_only = planning;
                ctx.vision = Some(caps.vision);
                ctx.native_pdf = Some(caps.native_pdf);
                ctx
            })
        };
        let summary_call = {
            let provider = provider.clone();
            let this = self.clone();
            let captured = accounting.clone();
            compaction_usage::track_compaction_usage(
                Box::new(move |request, signal| provider.stream(request, signal)),
                Arc::new(move |assistant| this.account_assistant_usage_for(&assistant, &captured)),
                self.persistence.usage_receipts().cloned(),
                self.deps.uuid.clone(),
            )
        };
        let compactor = match accounting
            .deps
            .compactor
            .fork_isolated(summary_call, &config)
        {
            Ok(compactor) => compactor,
            Err(error) => {
                return query_result(
                    &history,
                    None,
                    None,
                    ApprovalQueryOutcome::Failed(internal_error(format!(
                        "Cannot initialize approval review compaction: {error}"
                    ))),
                    false,
                    false,
                )
            }
        };
        let on_message = {
            let history = history.clone();
            Box::new(move |message: Message| {
                let history = history.clone();
                Box::pin(async move {
                    history.lock_recover().push(message);
                    Ok(())
                }) as futures::future::BoxFuture<'static, Result<(), RuntimeString>>
            }) as crate::engine::query::OnMessage
        };
        let params = QueryParams {
            multi_agent_instructions: false,
            thread_id,
            turn_id: turn_id.clone(),
            model: model.clone(),
            provider_info: Some(provider.info().clone()),
            messages: history.lock_recover().clone(),
            compaction_protected_message_ids: protected_message_ids,
            context_window: None,
            compaction_history: None,
            recover_compaction_history: None,
            compaction_output: None,
            tool_specs: to_tool_specs(&tools),
            tools,
            refresh_tools: None,
            refresh_tools_async: None,
            agent_tool_policy: None,
            system_prompt: SystemPromptResolvable::Parts(system),
            config: config.clone(),
            max_turns: None,
            cost_so_far: Some({
                let cost = self.cost.clone();
                Box::new(move || cost.lock_recover().total_cost_usd())
            }),
            tool_context_base,
            permission_engine: self.permission_engine.clone(),
            hook_bus: None,
            signal: local_signal.clone(),
            approval_handler: None,
            approval_reviewer: None,
            output_schema: Some(schema),
            token_budget: None,
            goal: None,
            goal_request_identity: None,
            goal_pending_input: None,
            goal_execution_enabled: None,
            goal_usage: None,
            web_search: None,
            on_message: Some(on_message),
            prepare_request_usage: Some(self.request_usage_recorder({
                let accounting = accounting.clone();
                Arc::new(move || accounting.clone())
            })),
            retained_tool_tasks: Some(self.persistence.tool_tasks.clone()),
            message_rollout: None,
            on_permission_update: None,
            on_compaction: Some({
                let history = history.clone();
                Box::new(move |record| {
                    *history.lock_recover() = record.replacement_history;
                    Box::pin(futures::future::ready(Ok(())))
                })
            }),
            surfaced_tools: Some(vec!["exec_command".into(), "write_stdin".into()]),
            on_surface_tools: None,
            on_tool_lifecycle: None,
            provider_call_admission: Some(admission.clone()),
            on_provider_request_admitted: None,
            emit_thread_started: false,
            reminder_providers: Vec::new(),
            edited_files_reminder: None,
            effort,
            ultra_mode: false,
            thinking: (config.thinking && caps.thinking).then_some(ThinkingConfig::Adaptive),
            skip_cache_write: None,
            cache_ttl: None,
            cache_key: Some(cache_key),
            plan_file_path: None,
            message_queue: None,
            input_pdf_bytes: 0,
            input_pdf_documents: 0,
            validate_native_pdf: None,
            agent_id: None,
            context_hint_state: None,
            tool_result_budget_state: None,
            on_tool_result_budget_records: None,
            thinking_context_cold: false,
            padded_countdown_spend: PaddedCountdownSpend::default(),
            task_countdown_anchor: None,
            agent_type: Some("approval-review".into()),
            live_model_override: None,
        };
        let deps = production_deps(
            provider,
            EngineDepsOverrides {
                uuid: Some(self.deps.uuid.clone()),
                now: Some(self.deps.now.clone()),
                compactor: Some(compactor),
                ..Default::default()
            },
        );
        let mut events = query_isolated(params, deps);
        let mut final_text = None;
        let mut usage = None;
        let mut last_error = None;
        let outcome = loop {
            let event = tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break ApprovalQueryOutcome::TimedOut,
                _ = signal.cancelled() => break ApprovalQueryOutcome::Aborted,
                event = events.next() => event,
            };
            match event {
                Some(EngineEvent::StreamRequestStart { .. }) => final_text = None,
                Some(EngineEvent::ItemCompleted {
                    item: ThreadItem::AgentMessage { text, .. },
                    ..
                }) => final_text = Some(text),
                Some(EngineEvent::TurnFailed { error, .. }) => last_error = Some(error),
                Some(EngineEvent::TurnCompleted {
                    turn_id: completed_id,
                    usage: completed_usage,
                    stop_reason,
                }) if completed_id == turn_id => {
                    usage = completed_usage;
                    let outcome = if let Some(budget) = admission.rejected() {
                        ApprovalQueryOutcome::InputBudgetExceeded(budget)
                    } else if stop_reason == TerminalReason::Aborted {
                        ApprovalQueryOutcome::Aborted
                    } else if stop_reason != TerminalReason::Completed {
                        ApprovalQueryOutcome::Failed(last_error.take().unwrap_or_else(|| {
                            EngineError {
                                code: match stop_reason {
                                    TerminalReason::AuthFailed => EngineErrorCode::AuthFailed,
                                    TerminalReason::PromptTooLong => EngineErrorCode::PromptTooLong,
                                    TerminalReason::SpendLimitExhausted
                                    | TerminalReason::TurnLimitExhausted => {
                                        EngineErrorCode::BudgetExhausted
                                    }
                                    _ => EngineErrorCode::Internal,
                                },
                                ..internal_error(
                                    "Approval review ended before producing its assessment.",
                                )
                            }
                        }))
                    } else if final_text.is_none() && last_error.is_some() {
                        ApprovalQueryOutcome::Failed(last_error.take().expect("checked error"))
                    } else {
                        ApprovalQueryOutcome::Completed
                    };
                    return query_result(&history, final_text, usage, outcome, true, true);
                }
                Some(_) => {}
                None => {
                    return query_result(
                        &history,
                        final_text,
                        usage,
                        match admission.rejected() {
                            Some(budget) => ApprovalQueryOutcome::InputBudgetExceeded(budget),
                            None => ApprovalQueryOutcome::Failed(last_error.unwrap_or_else(|| {
                                internal_error(
                                    "Approval review stream ended before its turn completed.",
                                )
                            })),
                        },
                        false,
                        false,
                    )
                }
            }
        };
        local_signal.cancel();
        // Cancellation has already decided this assessment. Retain the actual
        // private-query cleanup while allowing its Aborted receipt to publish.
        self.approval_review.tasks.tasks.spawn(async move {
            let _ = tokio::time::timeout(INTERRUPT_DRAIN_TIMEOUT, async {
                let drain = async {
                    while let Some(event) = events.next().await {
                        if let EngineEvent::TurnCompleted {
                            turn_id: completed_id,
                            usage: completed_usage,
                            ..
                        } = event
                        {
                            if completed_id == turn_id {
                                let _ = completed_usage;
                                return true;
                            }
                        }
                    }
                    false
                };
                let (_, completed) = tokio::join!(state.interrupt(), drain);
                completed
            })
            .await;
        });
        query_result(&history, final_text, usage, outcome, false, false)
    }
}

fn query_result(
    history: &Mutex<Vec<Message>>,
    final_text: Option<RuntimeString>,
    usage: Option<Usage>,
    outcome: ApprovalQueryOutcome,
    reusable: bool,
    turn_completed: bool,
) -> ApprovalQueryResult {
    ApprovalQueryResult {
        history: history.lock_recover().clone(),
        final_text,
        usage,
        outcome,
        reusable,
        turn_completed,
    }
}

fn internal_error(message: impl Into<RuntimeString>) -> EngineError {
    EngineError {
        code: EngineErrorCode::Internal,
        message: message.into(),
        cause: None,
        request_id: None,
        provider_kind: None,
        retry_after_ms: None,
    }
}

/// Tightens filesystem effects while preserving the parent's managed network route.
struct ReviewSandbox {
    inner: Arc<dyn SandboxBackend>,
    network_allowed: bool,
    planning: bool,
}
impl SandboxBackend for ReviewSandbox {
    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }
    fn is_available(&self) -> bool {
        self.inner.is_available()
    }
    fn enforces_planning_read_only(&self) -> bool {
        self.planning || self.inner.enforces_planning_read_only()
    }
    fn task_resource_scope(
        &self,
    ) -> Option<Arc<crate::permission::task_scope::CanonicalTaskResourceScope>> {
        self.inner.task_resource_scope()
    }
    fn task_runtime_program(&self, program: &RuntimePath) -> Option<Arc<dyn SandboxBackend>> {
        self.inner.task_runtime_program(program).map(|inner| {
            Arc::new(Self {
                inner,
                network_allowed: self.network_allowed,
                planning: self.planning,
            }) as Arc<dyn SandboxBackend>
        })
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let mut policy = policy.clone();
        policy.enforce_filesystem_read_only();
        policy.runtime_network_ceiling = Some(self.network_allowed);
        policy.network_allowed &= self.network_allowed;
        if !self.network_allowed {
            policy.enforce_no_network();
        }
        if self.planning {
            policy.enforce_planning_read_only();
        }
        self.inner.transform(request, &policy)
    }
}

/// These control tools have no filesystem effects except their gated nested calls.
struct InspectionTool(AnyTool);
fn inspection_tool(tool: AnyTool) -> AnyTool {
    Arc::new(InspectionTool(tool))
}
impl InspectionTool {
    fn prepare_input(&self, input: &Json) -> Result<Json, ToolError> {
        if self.0.name() != "exec_command" {
            return Ok(input.clone());
        }
        if ["additional_permissions", "additionalPermissions"]
            .iter()
            .any(|field| input.get(*field).is_some_and(|value| !value.is_null()))
        {
            return Err(ToolError::new(
                "Additional permissions are unavailable during approval review.",
            ));
        }
        let mut prepared = input.clone();
        if let Some(object) = prepared.as_object_mut() {
            for field in [
                "additional_permissions",
                "additionalPermissions",
                "sandbox_permissions",
                "sandboxPermissions",
                "justification",
            ] {
                if object.get(field).is_some_and(RuntimeJson::is_null) {
                    object.remove(field);
                }
            }
            if let Some(value) = object.remove("sandbox_permissions") {
                if object.get("sandboxPermissions").is_some() {
                    return Err(ToolError::new("Specify only one sandbox permission field."));
                }
                object.insert("sandboxPermissions", value);
            }
        }
        Ok(prepared)
    }
}
#[async_trait::async_trait]
impl Tool for InspectionTool {
    fn name(&self) -> RuntimeString {
        self.0.name()
    }
    fn description(&self) -> RuntimeString {
        self.0.description()
    }
    fn input_schema(&self) -> Json {
        let mut schema = self.0.input_schema();
        if self.0.name() == "exec_command" {
            if let Some(properties) = schema
                .get_mut("properties")
                .and_then(RuntimeJson::as_object_mut)
            {
                properties.remove("sandboxPermissions");
                properties.remove("justification");
            }
        }
        schema
    }
    fn input_kind(&self) -> ToolInputKind {
        self.0.input_kind()
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.0.parse_input(&self.prepare_input(input)?)
    }
    fn task_scope_host_capability(&self) -> bool {
        true
    }
    fn is_enabled(&self) -> bool {
        self.0.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.0.is_concurrency_safe(input)
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.0.to_command_string(input)
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.0
            .call(self.prepare_input(&input)?, ctx, progress)
            .await
    }
}

struct ViewImageTool {
    schema: JsonSchema,
    output_schema: JsonSchema,
    original_supported: bool,
    image_limits: Option<crate::types::provider::ImageLimits>,
}
impl ViewImageTool {
    fn new(metadata: &ModelCapabilities) -> Self {
        let mut schema = serde_json::json!({
            "type": "object", "properties": {
                "path": {"type": "string", "description": "Local filesystem path to an image file."}
            }, "required": ["path"], "additionalProperties": false
        });
        if metadata.supports_image_detail_original {
            schema["properties"]["detail"] = serde_json::json!({
                "type": "string", "enum": ["high", "original"],
                "description": "Image detail level. Defaults to high; use original to preserve exact resolution."
            });
        }
        let output_schema = serde_json::json!({
            "type": "object", "properties": {
                "image_url": {"type": "string", "description": "Data URL for the loaded image."},
                "detail": {"type": "string", "enum": ["high", "original"]}
            }, "required": ["image_url", "detail"], "additionalProperties": false
        });
        Self {
            schema: schema
                .as_object()
                .cloned()
                .expect("static image input schema"),
            output_schema: output_schema
                .as_object()
                .cloned()
                .expect("static image output schema"),
            original_supported: metadata.supports_image_detail_original,
            image_limits: metadata.image_limits,
        }
    }
}
#[async_trait::async_trait]
impl Tool for ViewImageTool {
    fn static_name(&self) -> &str {
        "view_image"
    }
    fn task_scope_host_capability(&self) -> bool {
        true
    }
    fn static_description(&self) -> &str {
        "View a local image file from the filesystem when visual inspection is needed. Use this for images already available on disk."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn static_output_schema(&self) -> Option<&JsonSchema> {
        Some(&self.output_schema)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        // Retain legacy hints even when the selected model does not advertise them.
        let mut schema = self.input_schema();
        let properties = schema
            .get_mut("properties")
            .and_then(RuntimeJson::as_object_mut)
            .expect("image schema properties");
        properties.insert(
            "detail",
            crate::types::json::runtime_schema_from_static(
                serde_json::json!({"type": ["string", "null"]})
                    .as_object()
                    .expect("static optional string schema"),
            ),
        );
        properties.insert(
            "environment_id",
            crate::types::json::runtime_schema_from_static(
                serde_json::json!({"type": ["string", "null"]})
                    .as_object()
                    .expect("static optional string schema"),
            ),
        );
        crate::tools::input_parser::parse_native_input(&schema, input, &|_| None)
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Read)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .map(|path| PathTargets::One(RuntimePath::from(path)))
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        use crate::context::inspection_images::{
            prepare_inspection_image, MAX_INSPECTION_IMAGE_INPUT_BYTES,
        };
        use crate::fs::node_path::{project_runtime_path, resolve_runtime};
        use tokio::io::AsyncReadExt;
        if ctx.vision == Some(false) {
            return Err(ToolError::new(
                "view_image is not allowed because this model does not support image inputs.",
            ));
        }
        if input
            .get("environment_id")
            .is_some_and(|value| !value.is_null())
        {
            return Err(ToolError::new("view_image environment_id is unavailable; this session has one local environment. Omit environment_id."));
        }
        let detail = match input.get("detail").filter(|value| !value.is_null()) {
            None => ImageDetail::High,
            Some(value) if value.as_str().is_some_and(|value| value == "high") => ImageDetail::High,
            Some(value) if value.as_str().is_some_and(|value| value == "original") => ImageDetail::Original,
            Some(_) => return Err(ToolError::new("view_image.detail only supports high or original; omit detail for default high resized behavior.")),
        };
        if detail == ImageDetail::Original && !self.original_supported {
            return Err(ToolError::new("Original image detail is unavailable for this model. Use high detail or omit detail."));
        }
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| ToolError::new("view_image.path must be a non-empty string."))?;
        let absolute = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path)])
            .map_err(|error| ToolError::new(error.message().clone()))?;
        let os_path = project_runtime_path(&absolute, "path")
            .map_err(|error| ToolError::new(error.message().clone()))?;
        let goal_snapshot = match &ctx.goal_owned_resources {
            Some(resources) => resources
                .snapshot_read(&absolute)
                .await
                .map_err(|error| ToolError::new(format!("Goal image read failed: {error}")))?,
            None => None,
        };
        let os_path = goal_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path().to_path_buf())
            .unwrap_or(os_path);
        let pinned_snapshot =
            match ctx
                .permission
                .pinned_memory_root
                .as_ref()
                .filter(|_| goal_snapshot.is_none())
            {
                Some(root) => Some(root.snapshot_file(&os_path).map_err(|error| {
                    ToolError::new(format!("Pinned image read failed: {error}"))
                })?),
                None => None,
            };
        let os_path = pinned_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path().to_path_buf())
            .unwrap_or(os_path);
        let file = tokio::fs::File::open(&os_path)
            .await
            .map_err(|error| ToolError::new(format!("Unable to read image: {error}")))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|error| ToolError::new(format!("Unable to locate image: {error}")))?;
        if !metadata.is_file() {
            return Err(ToolError::new("Image path is not a file."));
        }
        let maximum_raw_bytes = MAX_INSPECTION_IMAGE_INPUT_BYTES / 4 * 3;
        if metadata.len() > maximum_raw_bytes as u64 {
            return Err(ToolError::new(
                "Image input exceeds the supported 1 GiB representation limit.",
            ));
        }
        let mut bytes = Vec::new();
        let mut bounded = file.take(maximum_raw_bytes as u64 + 1);
        let read = bounded.read_to_end(&mut bytes);
        tokio::select! {
            _ = ctx.signal.cancelled() => return Err(ToolError::new("Image read was cancelled.")),
            result = read => { result.map_err(|error| ToolError::new(format!("Unable to read image: {error}")))?; }
        }
        if bytes.len() > maximum_raw_bytes {
            return Err(ToolError::new(
                "Image input exceeds the supported 1 GiB representation limit.",
            ));
        }
        let declaration = self.image_limits;
        let source_path = absolute.as_runtime_string().clone();
        let image = tokio::task::spawn_blocking(move || {
            prepare_inspection_image(bytes, detail, declaration.as_ref(), Some(source_path))
        })
        .await
        .map_err(|error| ToolError::new(format!("Image preparation failed: {error}")))??;
        if let Some(progress) = progress {
            progress(crate::types::events::ToolDomainEvent::Status {
                message: "Image loaded.".into(),
            })
            .await;
        }
        let image_url = RuntimeString::from("data:")
            .concat(&image.mime_type)
            .concat(&RuntimeString::from(";base64,"))
            .concat(&image.base64);
        let mut output = crate::runtime_values::RuntimeObject::new();
        output.insert("image_url", RuntimeJson::from(image_url));
        output.insert("detail", RuntimeJson::from(detail.as_str()));
        Ok(ToolResult {
            serializable: ToolResultSerializable {
                output: RuntimeJson::Object(output),
                model_content: vec![OutputBlock::Image(image)],
                is_error: Some(false),
                mcp_meta: None,
                extra_user_blocks: None,
            },
            side_effects: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_schema_and_parser_keep_unsupported_hints_visible() {
        let ordinary = ViewImageTool::new(&ModelCapabilities::default());
        assert!(ordinary.input_schema()["properties"]
            .get("detail")
            .is_none());
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"path":"image.data","detail":"original","environment_id":"other"}"#,
        ))
        .unwrap();
        let parsed = ordinary.parse_input(&input).unwrap();
        assert_eq!(parsed.get("detail"), input.get("detail"));
        assert_eq!(parsed.get("environment_id"), input.get("environment_id"));
        assert_eq!(
            ordinary.permission_path_kind(),
            Some(PermissionPathKind::Read)
        );
        let original = ViewImageTool::new(&ModelCapabilities {
            supports_image_detail_original: true,
            ..Default::default()
        });
        assert!(original.input_schema()["properties"]
            .get("detail")
            .is_some());
    }

    #[tokio::test]
    async fn image_tool_rejects_unavailable_detail_and_environment() {
        let tool = ViewImageTool::new(&ModelCapabilities::default());
        let ctx = crate::tools::builtins::test_ctx::make_ctx("/workspace");
        for (input, expected) in [
            (
                r#"{"path":"unused","detail":"original"}"#,
                "Original image detail is unavailable",
            ),
            (
                r#"{"path":"unused","detail":"low"}"#,
                "only supports high or original",
            ),
            (
                r#"{"path":"unused","environment_id":"other"}"#,
                "one local environment",
            ),
        ] {
            let input =
                crate::runtime_values::runtime_json::parse(&RuntimeString::from(input)).unwrap();
            let error = tool
                .call(tool.parse_input(&input).unwrap(), &ctx, None)
                .await
                .err()
                .expect("unsupported input must be explicit");
            assert!(error.message.contains(expected), "{:?}", error.message);
        }
    }

    #[tokio::test]
    async fn image_tool_prepares_large_valid_files_without_extension_guessing() {
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = Cleanup(
            std::env::temp_dir().join(format!("klaus-inspection-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir_all(&directory.0).unwrap();
        let image = image::RgbImage::from_pixel(2, 2, image::Rgb([17, 29, 43]));
        let mut bytes = Vec::new();
        image
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        bytes.resize(6 * 1024 * 1024, 0);
        std::fs::write(directory.0.join("picture.data"), bytes).unwrap();
        let ctx = crate::tools::builtins::test_ctx::make_ctx(directory.0.to_str().unwrap());
        let tool = ViewImageTool::new(&ModelCapabilities::default());
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"path":"picture.data"}"#,
        ))
        .unwrap();
        let result = tool.call(input, &ctx, None).await.unwrap();
        let OutputBlock::Image(image) = &result.serializable.model_content[0] else {
            panic!("image output required")
        };
        assert_eq!(image.detail, Some(ImageDetail::High));
        assert!(image.base64.len_utf16() > crate::context::images::IMAGE_MAX_BASE64_CHARS);
        assert!(
            image.base64.len_utf16()
                <= crate::context::inspection_images::MAX_INSPECTION_IMAGE_INPUT_BYTES
        );
        assert_eq!(
            result.serializable.output["detail"].as_str().unwrap(),
            "high"
        );
        let image_url = result.serializable.output["image_url"].as_str().unwrap();
        assert!(image_url.starts_with("data:image/png;base64,"));
    }

    #[derive(Default)]
    struct Capture(Mutex<Option<SandboxPolicy>>);
    impl SandboxBackend for Capture {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.0.lock_recover() = Some(policy.clone());
            SpawnSpec {
                file: "true".into(),
                args: Vec::new(),
                cwd: request.cwd.clone(),
                env: Default::default(),
            }
        }
    }
    fn request() -> ExecRequest {
        ExecRequest {
            argv: vec!["true".into()],
            cwd: "/workspace".into(),
            env: None,
            timeout_ms: None,
        }
    }
    fn inherited_policy() -> SandboxPolicy {
        SandboxPolicy {
            writable_roots: vec!["/workspace".into()],
            readable_roots: Some(vec!["/workspace".into()]),
            network_allowed: true,
            allow_loopback_ports: Some(vec![4312]),
            deny_read_subpaths: Some(vec!["/workspace/private".into()]),
            unconfined_writes: Some(true),
            ..Default::default()
        }
    }
    #[test]
    fn readonly_review_preserves_read_denies_and_network_routes() {
        let capture = Arc::new(Capture::default());
        let review = ReviewSandbox {
            inner: capture.clone(),
            network_allowed: true,
            planning: false,
        };
        review.transform(&request(), &inherited_policy());
        let policy = capture.0.lock_recover().clone().unwrap();
        assert!(policy.writable_roots.is_empty());
        assert!(policy.filesystem_read_only);
        assert!(!policy.planning_read_only);
        assert_eq!(policy.exclude_cwd, Some(true));
        assert_eq!(policy.exclude_slash_tmp, Some(true));
        assert_eq!(policy.exclude_tmpdir_env_var, Some(true));
        assert_eq!(policy.unconfined_writes, None);
        assert_eq!(policy.readable_roots, Some(vec!["/workspace".into()]));
        assert_eq!(
            policy.deny_read_subpaths,
            Some(vec!["/workspace/private".into()])
        );
        assert!(policy.network_allowed);
        assert_eq!(policy.allow_loopback_ports, Some(vec![4312]));
    }
    #[test]
    fn planning_review_cannot_reopen_network_access() {
        let capture = Arc::new(Capture::default());
        let review = ReviewSandbox {
            inner: capture.clone(),
            network_allowed: true,
            planning: true,
        };
        review.transform(&request(), &inherited_policy());
        let policy = capture.0.lock_recover().clone().unwrap();
        assert!(policy.planning_read_only);
        assert!(!policy.network_allowed);
        assert_eq!(policy.allow_loopback_ports, None);
    }
    #[tokio::test]
    async fn process_namespace_shutdown_does_not_close_the_parent_namespace() {
        let shared = crate::runtime_process::test_runtime_process_context();
        let parent = shared.isolated_context().unwrap();
        let reviewer = parent.isolated_context().unwrap();
        assert_ne!(parent.namespace_id(), reviewer.namespace_id());
        reviewer.shutdown().await.unwrap();
        let later = parent.isolated_context().unwrap();
        assert_ne!(later.namespace_id(), reviewer.namespace_id());
        later.shutdown().await.unwrap();
        parent.shutdown().await.unwrap();
        let still_shared = shared.isolated_context().unwrap();
        still_shared.shutdown().await.unwrap();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn readonly_review_shell_reads_files_and_denies_write_side_effects() {
        struct TemporaryDirectory(std::path::PathBuf);
        impl TemporaryDirectory {
            fn path(&self) -> &std::path::Path {
                &self.0
            }
        }
        impl Drop for TemporaryDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
            "klaus-approval-query-{}-{unique}",
            std::process::id()
        )));
        std::fs::create_dir_all(directory.path()).unwrap();
        std::fs::write(
            directory.path().join("sentinel"),
            "readonly-review-sentinel",
        )
        .unwrap();
        let mut ctx =
            crate::tools::builtins::test_ctx::make_ctx(directory.path().to_str().unwrap());
        ctx.process_context = ctx.process_context.isolated_context().unwrap();
        ctx.sandbox = Arc::new(ReviewSandbox {
            inner: Arc::new(crate::sandbox::seatbelt::SeatbeltSandbox::new()),
            network_allowed: false,
            planning: false,
        });
        ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
        ctx.approval_policy = Some(ApprovalPolicy::Never);
        ctx.network_allowed = Some(false);
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"command":"/bin/cat sentinel && /usr/bin/touch denied"}"#,
        ))
        .unwrap();
        let result = create_exec_command_tool()
            .call(input, &ctx, None)
            .await
            .unwrap();
        assert!(!directory.path().join("denied").exists());
        assert!(result.serializable.model_content.iter().any(|block| matches!(block,
            crate::schemas::content::OutputBlock::Text(text) if text.text.contains("readonly-review-sentinel"))), "actual result: {:?}", result.serializable.model_content);
        let inspection = inspection_tool(create_exec_command_tool());
        for (index, permissions) in [
            r#""sandboxPermissions":"require_escalated","justification":"Inspect outside the sandbox""#,
            r#""sandbox_permissions":"require_escalated""#,
            r#""additional_permissions":{"file_system":{"write":["."]}}"#,
            r#""additionalPermissions":{"fileSystem":{"write":["."]}}"#,
        ].iter().enumerate() {
            let raw = crate::runtime_values::runtime_json::parse(&RuntimeString::from(format!(
                "{{\"command\":\"/usr/bin/touch escalated-side-effect\",{permissions}}}"
            ))).unwrap();
            let parsed = inspection.parse_input(&raw);
            let call_input = if index < 2 {
                let parsed = parsed.expect("hidden controls must reach the execution policy");
                assert_eq!(parsed.get("sandboxPermissions").and_then(RuntimeJson::as_str), Some(&RuntimeString::from("require_escalated")));
                parsed
            } else {
                assert!(parsed.is_err(), "unsupported permissions cannot disappear during parsing");
                raw
            };
            let error = inspection.call(call_input, &ctx, None).await.err()
                .expect("approval review cannot acquire additional permissions");
            assert!(error.message.contains(if index < 2 { "approval policy does not permit" } else { "Additional permissions are unavailable" }), "actual error: {error}");
            assert!(!directory.path().join("escalated-side-effect").exists());
        }
        ctx.background_processes
            .close_and_wait(Instant::now() + INTERRUPT_DRAIN_TIMEOUT)
            .await
            .unwrap();
        ctx.process_context.shutdown().await.unwrap();
    }

    #[test]
    fn reviewer_exec_does_not_expose_escalation_controls() {
        let tool = inspection_tool(create_exec_command_tool());
        let schema = tool.input_schema();
        assert!(schema["properties"].get("sandboxPermissions").is_none());
        assert!(schema["properties"].get("justification").is_none());
        assert!(schema["properties"].get("command").is_some());
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"command":"pwd","sandboxPermissions":"require_escalated"}"#,
        ))
        .unwrap();
        let parsed = tool.parse_input(&input).unwrap();
        assert_eq!(
            parsed.get("sandboxPermissions"),
            input.get("sandboxPermissions")
        );
        let snake = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"command":"pwd","sandbox_permissions":"require_escalated"}"#,
        ))
        .unwrap();
        assert_eq!(
            tool.parse_input(&snake).unwrap().get("sandboxPermissions"),
            input.get("sandboxPermissions")
        );
        let additional = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"command":"pwd","additional_permissions":{"network":true}}"#,
        ))
        .unwrap();
        assert!(tool.parse_input(&additional).is_err());
    }
    #[test]
    fn reviewer_exec_optional_permission_nulls_are_absent() {
        let tool = inspection_tool(create_exec_command_tool());
        for field in [
            "additional_permissions",
            "additionalPermissions",
            "sandbox_permissions",
            "sandboxPermissions",
            "justification",
        ] {
            let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(format!(
                "{{\"command\":\"pwd\",\"{field}\":null}}"
            )))
            .unwrap();
            let parsed = tool.parse_input(&input).unwrap();
            assert!(parsed.get(field).is_none());
            assert!(parsed.get("sandboxPermissions").is_none());
        }
        for input in [
            r#"{"command":"pwd","sandboxPermissions":null,"sandbox_permissions":"require_escalated","justification":null}"#,
            r#"{"command":"pwd","sandbox_permissions":null,"sandboxPermissions":"require_escalated","additional_permissions":null}"#,
        ] {
            let input =
                crate::runtime_values::runtime_json::parse(&RuntimeString::from(input)).unwrap();
            let parsed = tool.parse_input(&input).unwrap();
            assert_eq!(
                parsed
                    .get("sandboxPermissions")
                    .and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from("require_escalated"))
            );
        }
    }

    #[test]
    fn reviewer_non_command_arguments_use_the_original_tool_parser() {
        let inner = create_write_stdin_tool();
        let tool = inspection_tool(inner.clone());
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"sessionId":"session-1","sandboxPermissions":"require_escalated","additional_permissions":{"network":true}}"#,
        )).unwrap();
        assert_eq!(
            tool.parse_input(&input).unwrap(),
            inner.parse_input(&input).unwrap()
        );
    }
}
