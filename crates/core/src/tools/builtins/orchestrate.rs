//! Orchestration-mode execution handoff and deterministic tool guardrails.
//!
//! When orchestration mode is active on a root conversation, the main model
//! retains ownership of planning, ambiguity resolution, stage review, and final
//! acceptance while delegating workspace mutations and verification runs to the
//! configured execution model through `orchestrate_execution`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::BoxFuture;
use serde_json::json;

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::sync_ext::MutexExt;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::PermissionResult;
use crate::types::tool::{
    BrowserDependencyPersistence, PathTargets, PlanningToolCapability, Tool, ToolContext,
    ToolExposure, ToolInputKind, ToolProgress, ToolResult, ValidationResult,
};

pub const ORCHESTRATE_EXECUTION_TOOL_NAME: &str = "orchestrate_execution";

/// Maximum tool invocations permitted in a single execution stage before
/// returning a checkpoint report to the main model.
pub const MAX_ORCHESTRATED_STAGE_STEPS: usize = 30;

/// Consecutive verification or mutation failures allowed before halting blind
/// retries and returning control to the main model.
pub const MAX_CONSECUTIVE_EXECUTION_FAILURES: usize = 2;

const PLANNER_GATED_EDIT_TOOLS: &[&str] = &[
    "write",
    "edit",
    "multi_edit",
    "apply_patch",
    "notebook_edit",
];

const EXECUTOR_EXCLUDED_TOOLS: &[&str] = &[
    "advisor",
    ORCHESTRATE_EXECUTION_TOOL_NAME,
    "task",
    "workflow",
    "create_thread",
    "fork_thread",
    "send_message_to_thread",
];

const EXECUTOR_VERIFICATION_OR_EDIT_TOOLS: &[&str] = &[
    "bash",
    "write",
    "edit",
    "multi_edit",
    "apply_patch",
    "notebook_edit",
];

const PLANNER_EDIT_GATE_REFUSAL: &str = "Orchestration mode is active: keep the main model focused on read-only investigation, planning, and verification, and dispatch workspace file edits through orchestrate_execution.";

#[derive(Debug, Clone)]
pub struct OrchestrateExecutionRequest {
    pub description: RuntimeString,
    pub plan: RuntimeString,
    pub resume: Option<RuntimeString>,
}

pub type OrchestrateExecuteFn = Arc<
    dyn for<'a> Fn(
            OrchestrateExecutionRequest,
            &'a ToolContext,
        ) -> BoxFuture<'a, Result<ToolResult, ToolError>>
        + Send
        + Sync,
>;

pub struct OrchestrateExecutionTool {
    schema: JsonSchema,
    execute: OrchestrateExecuteFn,
}

pub fn create_orchestrate_execution_tool(execute: OrchestrateExecuteFn) -> Arc<dyn Tool> {
    Arc::new(OrchestrateExecutionTool {
        schema: json!({
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Short summary (3-8 words) of the implementation unit in the current interface language."
                },
                "plan": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Self-contained implementation specification for the execution model: relevant context (including any visual details transcribed into text), ordered file changes, verification commands with expected outputs, and explicit conditions that require stopping and reporting back."
                },
                "resume": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Optional execution agentId returned by an earlier orchestrate_execution stage in this conversation to continue from its existing execution context."
                }
            },
            "required": ["description", "plan"],
            "additionalProperties": false
        })
        .as_object()
        .cloned()
        .expect("orchestrate_execution schema is an object"),
        execute,
    })
}

#[async_trait]
impl Tool for OrchestrateExecutionTool {
    fn static_name(&self) -> &str {
        ORCHESTRATE_EXECUTION_TOOL_NAME
    }

    fn static_description(&self) -> &str {
        "Dispatch a self-contained implementation and verification plan to the configured orchestration execution model. Runs in an isolated execution context sharing this workspace, enforces automatic stop guardrails on repeated verification failures or stage step limits, and returns a structured status report for the main model to review."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        if object_path.is_empty() {
            Some(&["description", "plan", "resume"])
        } else {
            None
        }
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        input
            .get("description")
            .and_then(Json::as_str)
            .filter(|text| !crate::engine::session::runtime_trim(text).is_empty())
            .cloned()
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let description = input
            .get("description")
            .and_then(Json::as_str)
            .filter(|value| !crate::engine::session::runtime_trim(value).is_empty())
            .cloned()
            .ok_or_else(|| ToolError::new("description must be nonempty"))?;
        let plan = input
            .get("plan")
            .and_then(Json::as_str)
            .filter(|value| !crate::engine::session::runtime_trim(value).is_empty())
            .cloned()
            .ok_or_else(|| ToolError::new("plan must be nonempty"))?;
        let resume = input
            .get("resume")
            .and_then(Json::as_str)
            .filter(|value| !crate::engine::session::runtime_trim(value).is_empty())
            .cloned();
        (self.execute)(
            OrchestrateExecutionRequest {
                description,
                plan,
                resume,
            },
            ctx,
        )
        .await
    }
}

/// Gate direct workspace file-editing tools on the root session while
/// orchestration mode is active so edits flow through `orchestrate_execution`.
pub fn wrap_orchestration_planner_tools(tools: &mut [Arc<dyn Tool>]) {
    for slot in tools.iter_mut() {
        let name = slot.name();
        if PLANNER_GATED_EDIT_TOOLS.iter().any(|gated| name == *gated) {
            *slot = Arc::new(OrchestrationPlannerGateTool {
                inner: slot.clone(),
            });
        }
    }
}

struct OrchestrationPlannerGateTool {
    inner: Arc<dyn Tool>,
}

#[async_trait]
impl Tool for OrchestrationPlannerGateTool {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
        })
    }
    fn browser_dependency_persistence(&self) -> Option<BrowserDependencyPersistence> {
        self.inner.browser_dependency_persistence()
    }
    fn browser_dependency_owners(&self) -> Vec<RuntimeString> {
        self.inner.browser_dependency_owners()
    }
    fn browser_dependency_authorization(
        &self,
        caller: Option<&RuntimeString>,
        input: &Json,
    ) -> Option<bool> {
        self.inner.browser_dependency_authorization(caller, input)
    }
    fn name(&self) -> RuntimeString {
        self.inner.name()
    }
    fn aliases(&self) -> Vec<RuntimeString> {
        self.inner.aliases()
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn input_kind(&self) -> ToolInputKind {
        self.inner.input_kind()
    }
    fn output_schema(&self) -> Option<Json> {
        self.inner.output_schema()
    }
    fn exposure(&self) -> ToolExposure {
        self.inner.exposure()
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.inner.search_hint()
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.inner.max_result_size_chars()
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.inner.persistence_identity()
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata()
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.inner.mcp_server()
    }
    fn task_scope_host_capability(&self) -> bool {
        self.inner.task_scope_host_capability()
    }
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    fn is_goal_state_control(&self) -> bool {
        self.inner.is_goal_state_control()
    }
    fn planning_capability(&self, input: &Json) -> PlanningToolCapability {
        self.inner.planning_capability(input)
    }
    fn is_destructive(&self, input: &Json) -> Option<bool> {
        self.inner.is_destructive(input)
    }
    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        self.inner.input_schema_property_order(object_path)
    }
    fn preprocess_input(&self, input: &Json) -> Json {
        self.inner.preprocess_input(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        let _ = self.inner.parse_input(input)?;
        Err(ToolError::new(PLANNER_EDIT_GATE_REFUSAL))
    }
    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::new(PLANNER_EDIT_GATE_REFUSAL))
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.summarize(input)
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.to_command_string(input)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        self.inner.get_path(input)
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.inner.get_raw_path(input)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStopReason {
    Completed,
    VerificationFailedTwice,
    StageCheckpointReached,
    InterruptedByUserMessage,
}

impl ExecutionStopReason {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::VerificationFailedTwice => "verification_failed_twice",
            Self::StageCheckpointReached => "stage_checkpoint_reached",
            Self::InterruptedByUserMessage => "interrupted_by_user_message",
        }
    }

    fn lock_message(self) -> &'static str {
        match self {
            Self::Completed => "",
            Self::VerificationFailedTwice => {
                "Two consecutive verification or edit steps failed. Further tool calls are locked for this stage: stop attempting fixes and return a concise status report now (completed changes, exact failure details, and what requires re-planning)."
            }
            Self::StageCheckpointReached => {
                "Stage checkpoint reached (30 tool invocations). Further tool calls are locked for this stage: return a concise status report now (completed steps, verification results, and remaining steps)."
            }
            Self::InterruptedByUserMessage => {
                "A new user message arrived in the main conversation. Further tool calls are locked: stop immediately and return a concise status report of completed and remaining work so the main model can address the user's message."
            }
        }
    }
}

/// Shared runtime state tracking deterministic guardrails across one execution leg.
pub struct OrchestratedExecutionGuard {
    steps_used: AtomicUsize,
    consecutive_failures: AtomicUsize,
    stop_reason: Mutex<Option<ExecutionStopReason>>,
    has_pending_user_input: Arc<dyn Fn() -> bool + Send + Sync>,
    max_steps: usize,
    max_consecutive_failures: usize,
}

impl OrchestratedExecutionGuard {
    pub fn new(has_pending_user_input: Arc<dyn Fn() -> bool + Send + Sync>) -> Arc<Self> {
        Self::with_limits(
            has_pending_user_input,
            MAX_ORCHESTRATED_STAGE_STEPS,
            MAX_CONSECUTIVE_EXECUTION_FAILURES,
        )
    }

    pub fn with_limits(
        has_pending_user_input: Arc<dyn Fn() -> bool + Send + Sync>,
        max_steps: usize,
        max_consecutive_failures: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            steps_used: AtomicUsize::new(0),
            consecutive_failures: AtomicUsize::new(0),
            stop_reason: Mutex::new(None),
            has_pending_user_input,
            max_steps: max_steps.max(1),
            max_consecutive_failures: max_consecutive_failures.max(1),
        })
    }

    pub fn steps_used(&self) -> usize {
        self.steps_used.load(Ordering::Acquire)
    }

    pub fn stop_reason(&self) -> ExecutionStopReason {
        self.stop_reason
            .lock_recover()
            .unwrap_or(ExecutionStopReason::Completed)
    }

    fn check_before_call(&self) -> Result<(), ToolError> {
        if let Some(reason) = *self.stop_reason.lock_recover() {
            return Err(ToolError::new(reason.lock_message()));
        }
        if (self.has_pending_user_input)() {
            let mut guard = self.stop_reason.lock_recover();
            let reason = *guard.get_or_insert(ExecutionStopReason::InterruptedByUserMessage);
            return Err(ToolError::new(reason.lock_message()));
        }
        let current = self.steps_used.load(Ordering::Acquire);
        if current >= self.max_steps {
            let mut guard = self.stop_reason.lock_recover();
            let reason = *guard.get_or_insert(ExecutionStopReason::StageCheckpointReached);
            return Err(ToolError::new(reason.lock_message()));
        }
        Ok(())
    }

    fn record_outcome(&self, tool_name: &RuntimeString, failed: bool) -> Option<&'static str> {
        let step = self.steps_used.fetch_add(1, Ordering::AcqRel) + 1;
        let tracked = EXECUTOR_VERIFICATION_OR_EDIT_TOOLS
            .iter()
            .any(|candidate| tool_name == *candidate);
        if tracked {
            if failed {
                let count = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                if count >= self.max_consecutive_failures {
                    let mut guard = self.stop_reason.lock_recover();
                    if guard.is_none() {
                        *guard = Some(ExecutionStopReason::VerificationFailedTwice);
                    }
                    return Some(ExecutionStopReason::VerificationFailedTwice.lock_message());
                }
            } else {
                self.consecutive_failures.store(0, Ordering::Release);
            }
        }
        if step >= self.max_steps {
            let mut guard = self.stop_reason.lock_recover();
            if guard.is_none() {
                *guard = Some(ExecutionStopReason::StageCheckpointReached);
                return Some(ExecutionStopReason::StageCheckpointReached.lock_message());
            }
        }
        None
    }
}

pub fn wrap_orchestrated_executor_tools(
    tools: Vec<Arc<dyn Tool>>,
    guard: Arc<OrchestratedExecutionGuard>,
) -> Vec<Arc<dyn Tool>> {
    tools
        .into_iter()
        .filter(|tool| {
            let name = tool.name();
            !EXECUTOR_EXCLUDED_TOOLS
                .iter()
                .any(|excluded| name == *excluded)
        })
        .map(|inner| {
            Arc::new(OrchestratedExecutorGuardTool {
                inner,
                guard: guard.clone(),
            }) as Arc<dyn Tool>
        })
        .collect()
}

struct OrchestratedExecutorGuardTool {
    inner: Arc<dyn Tool>,
    guard: Arc<OrchestratedExecutionGuard>,
}

impl OrchestratedExecutorGuardTool {
    fn append_notice(mut result: ToolResult, notice: &str) -> ToolResult {
        if notice.is_empty() {
            return result;
        }
        let suffix = RuntimeString::from("\n\n[Orchestration checkpoint] ")
            .concat(&RuntimeString::from(notice));
        result
            .serializable
            .model_content
            .push(crate::schemas::content::OutputBlock::Text(
                crate::schemas::content::TextBlock {
                    text: suffix.clone(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                },
            ));
        if let RuntimeJson::String(existing) = &result.serializable.output {
            result.serializable.output = RuntimeJson::String(existing.concat(&suffix));
        }
        result
    }
}

#[async_trait]
impl Tool for OrchestratedExecutorGuardTool {
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        self.inner.native_mcp_binding()
    }
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        Arc::new(Self {
            inner: self.inner.rebind_native_mcp(replacement),
            guard: self.guard.clone(),
        })
    }
    fn browser_dependency_persistence(&self) -> Option<BrowserDependencyPersistence> {
        self.inner.browser_dependency_persistence()
    }
    fn browser_dependency_owners(&self) -> Vec<RuntimeString> {
        self.inner.browser_dependency_owners()
    }
    fn browser_dependency_authorization(
        &self,
        caller: Option<&RuntimeString>,
        input: &Json,
    ) -> Option<bool> {
        self.inner.browser_dependency_authorization(caller, input)
    }
    fn name(&self) -> RuntimeString {
        self.inner.name()
    }
    fn aliases(&self) -> Vec<RuntimeString> {
        self.inner.aliases()
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn input_kind(&self) -> ToolInputKind {
        self.inner.input_kind()
    }
    fn output_schema(&self) -> Option<Json> {
        self.inner.output_schema()
    }
    fn exposure(&self) -> ToolExposure {
        self.inner.exposure()
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.inner.search_hint()
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.inner.max_result_size_chars()
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.inner.persistence_identity()
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.inner.approval_metadata()
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.inner.mcp_server()
    }
    fn task_scope_host_capability(&self) -> bool {
        self.inner.task_scope_host_capability()
    }
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    fn is_goal_state_control(&self) -> bool {
        self.inner.is_goal_state_control()
    }
    fn planning_capability(&self, input: &Json) -> PlanningToolCapability {
        self.inner.planning_capability(input)
    }
    fn is_destructive(&self, input: &Json) -> Option<bool> {
        self.inner.is_destructive(input)
    }
    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        self.inner.input_schema_property_order(object_path)
    }
    fn preprocess_input(&self, input: &Json) -> Json {
        self.inner.preprocess_input(input)
    }
    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.guard.check_before_call()?;
        self.inner.parse_input(input)
    }
    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        if let Err(err) = self.guard.check_before_call() {
            return Some(ValidationResult::Err {
                message: String::from_utf8_lossy(&err.message.to_node_utf8()).into_owned(),
                error_code: None,
            });
        }
        self.inner.validate_input(input, ctx).await
    }
    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.inner.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.guard.check_before_call()?;
        let tool_name = self.inner.name();
        match self.inner.call(input, ctx, on_progress).await {
            Ok(result) => {
                let failed = result.serializable.is_error == Some(true);
                if let Some(notice) = self.guard.record_outcome(&tool_name, failed) {
                    Ok(Self::append_notice(result, notice))
                } else {
                    Ok(result)
                }
            }
            Err(error) => {
                if let Some(notice) = self.guard.record_outcome(&tool_name, true) {
                    Err(ToolError::new(
                        error
                            .message
                            .concat(&RuntimeString::from("\n\n[Orchestration checkpoint] "))
                            .concat(&RuntimeString::from(notice)),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
    async fn call_with_id(
        &self,
        call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.guard.check_before_call()?;
        let tool_name = self.inner.name();
        match self
            .inner
            .call_with_id(call_id, input, ctx, on_progress)
            .await
        {
            Ok(result) => {
                let failed = result.serializable.is_error == Some(true);
                if let Some(notice) = self.guard.record_outcome(&tool_name, failed) {
                    Ok(Self::append_notice(result, notice))
                } else {
                    Ok(result)
                }
            }
            Err(error) => {
                if let Some(notice) = self.guard.record_outcome(&tool_name, true) {
                    Err(ToolError::new(
                        error
                            .message
                            .concat(&RuntimeString::from("\n\n[Orchestration checkpoint] "))
                            .concat(&RuntimeString::from(notice)),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.summarize(input)
    }
    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        self.inner.to_command_string(input)
    }
    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        self.inner.get_path(input)
    }
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.inner.get_raw_path(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::build_tool::text_result;
    use crate::tools::builtins::test_ctx::make_ctx;

    struct DummyTool {
        name: &'static str,
        schema: JsonSchema,
        fail: bool,
    }

    impl DummyTool {
        fn new(name: &'static str, fail: bool) -> Arc<dyn Tool> {
            Arc::new(Self {
                name,
                schema: json!({"type": "object", "properties": {}, "additionalProperties": true})
                    .as_object()
                    .cloned()
                    .unwrap(),
                fail,
            })
        }
    }

    #[async_trait]
    impl Tool for DummyTool {
        fn static_name(&self) -> &str {
            self.name
        }
        fn static_description(&self) -> &str {
            "dummy"
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
            self.name == "read"
        }
        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok(text_result(
                RuntimeJson::String("done".into()),
                "done",
                self.fail,
            ))
        }
    }

    #[test]
    fn planner_gate_refuses_file_edits_while_allowing_read_tools() {
        let mut tools = vec![DummyTool::new("read", false), DummyTool::new("edit", false)];
        wrap_orchestration_planner_tools(&mut tools);
        let empty = RuntimeJson::Object(Default::default());
        assert!(tools[0].parse_input(&empty).is_ok());
        let err = tools[1].parse_input(&empty).unwrap_err();
        assert!(
            String::from_utf8_lossy(&err.message.to_node_utf8()).contains("orchestrate_execution")
        );
    }

    #[tokio::test]
    async fn executor_guard_locks_after_two_consecutive_failures() {
        let guard = OrchestratedExecutionGuard::with_limits(Arc::new(|| false), 30, 2);
        let wrapped =
            wrap_orchestrated_executor_tools(vec![DummyTool::new("bash", true)], guard.clone());
        let bash = &wrapped[0];
        let ctx = make_ctx("/tmp");
        let empty = RuntimeJson::Object(Default::default());

        let first = bash.call(empty.clone(), &ctx, None).await.unwrap();
        assert_eq!(first.serializable.is_error, Some(true));
        assert_eq!(guard.stop_reason(), ExecutionStopReason::Completed);

        let second = bash.call(empty.clone(), &ctx, None).await.unwrap();
        assert_eq!(
            guard.stop_reason(),
            ExecutionStopReason::VerificationFailedTwice
        );
        assert!(second.serializable.model_content.len() >= 2);
        assert!(bash.parse_input(&empty).is_err());
    }

    #[tokio::test]
    async fn executor_guard_locks_on_stage_checkpoint_and_user_interruption() {
        let guard = OrchestratedExecutionGuard::with_limits(Arc::new(|| false), 2, 2);
        let wrapped =
            wrap_orchestrated_executor_tools(vec![DummyTool::new("read", false)], guard.clone());
        let read = &wrapped[0];
        let ctx = make_ctx("/tmp");
        let empty = RuntimeJson::Object(Default::default());

        read.call(empty.clone(), &ctx, None).await.unwrap();
        read.call(empty.clone(), &ctx, None).await.unwrap();
        assert_eq!(
            guard.stop_reason(),
            ExecutionStopReason::StageCheckpointReached
        );
        assert!(read.parse_input(&empty).is_err());

        let preempted = OrchestratedExecutionGuard::with_limits(Arc::new(|| true), 30, 2);
        let wrapped_preempt = wrap_orchestrated_executor_tools(
            vec![DummyTool::new("read", false)],
            preempted.clone(),
        );
        assert!(wrapped_preempt[0].parse_input(&empty).is_err());
        assert_eq!(
            preempted.stop_reason(),
            ExecutionStopReason::InterruptedByUserMessage
        );
    }
}
