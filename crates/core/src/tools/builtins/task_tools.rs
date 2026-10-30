//! The durable, dependency-aware task tools (TaskCreate / TaskGet / TaskUpdate /
//! TaskList), backed by the file task store.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value as SchemaValue};

use crate::hooks::types::HookEvent;
use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use crate::tasks::task_store::{
    block_task_bound, create_task_bound_receipt, delete_task_bound, get_task_bound,
    get_task_list_id, list_plan_view, update_task_bound, write_task_plan_approved, CreateTaskInput,
    Task, TaskPlanApprover, TaskPlanWriteEntry, TaskStatus, TaskUpdate,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{PermissionReason, PermissionResult};
use crate::types::tool::{RunHookInput, Tool, ToolContext, ToolProgress, ToolResult};

/// Admit the durable task tools while planning.
///
/// Planning may use a durable progress checklist without changing project files.
/// These tools write only the engine's own task store, never the user's
/// workspace, so admitting them does not weaken "no edits until the plan is
/// approved". They are admitted through a self-check rather than by flipping
/// `is_read_only`, because that flag also drives eager pre-start of execution in
/// `query/streaming.rs` — a semantic these tools deliberately do not claim.
fn plan_mode_task_write_permission(ctx: &ToolContext) -> Option<PermissionResult> {
    if ctx.planning_read_only {
        return Some(PermissionResult::Allow {
            updated_input: None,
            reason: PermissionReason::ToolSelfCheck,
        });
    }
    None
}

/// Resolve which durable task list this call reads/writes.
fn list_id_of(ctx: &ToolContext) -> RuntimeString {
    ctx.task_list_id
        .clone()
        .unwrap_or_else(|| get_task_list_id(None, None))
}

/// Render a task id as `#<id>`.
fn hash(id: &RuntimeString) -> RuntimeString {
    RuntimeString::from("#").concat(id)
}

/// Loose truthiness for a metadata flag (`false`, `0`, `""`, `null` are falsy).
fn is_truthy(v: &RuntimeJson) -> bool {
    match v {
        RuntimeJson::Null => false,
        RuntimeJson::Bool(b) => *b,
        RuntimeJson::Number(n) => n.as_f64() != 0.0,
        RuntimeJson::String(s) => !s.is_empty(),
        RuntimeJson::Array(_) | RuntimeJson::Object(_) => true,
    }
}

/// Parse a status string into a [`TaskStatus`] (`deleted` is handled separately).
fn parse_task_status(s: &RuntimeString) -> Option<TaskStatus> {
    if s == "pending" {
        Some(TaskStatus::Pending)
    } else if s == "in_progress" {
        Some(TaskStatus::InProgress)
    } else if s == "completed" {
        Some(TaskStatus::Completed)
    } else {
        None
    }
}

/// Pull a `string[]` argument, dropping non-string entries.
fn string_array(value: Option<&RuntimeJson>) -> Vec<RuntimeString> {
    value
        .and_then(RuntimeJson::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().cloned()).collect())
        .unwrap_or_default()
}

/// The reusable `metadata` schema fragment.
fn metadata_schema() -> SchemaValue {
    json!({
        "type": "object",
        "propertyNames": { "type": "string" },
        "additionalProperties": {},
        "description": "Arbitrary metadata to attach (a value of null deletes that key on update)"
    })
}

/// Map an io failure from the task store to a recoverable tool error.
fn store_err(e: std::io::Error) -> ToolError {
    ToolError::new(e.to_string())
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn task_summary(task: &Task) -> RuntimeJson {
    runtime_object([
        ("id", RuntimeJson::String(task.id.clone())),
        ("subject", RuntimeJson::String(task.subject.clone())),
    ])
}

// ---- TaskCreate ------------------------------------------------------------

fn task_create_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "subject": {
                "type": "string",
                "description": "A short, imperative title for the task"
            },
            "description": {
                "type": "string",
                "description": "What the task involves"
            },
            "activeForm": {
                "type": "string",
                "description": "Present-continuous label shown while the task is in progress"
            },
            "metadata": metadata_schema()
        },
        "required": ["subject", "description"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Create a task in the durable task list (starts pending).
pub struct TaskCreateTool {
    input_schema: JsonSchema,
}

/// Construct the `TaskCreate` tool.
pub fn create_task_create_tool() -> Arc<dyn Tool> {
    Arc::new(TaskCreateTool {
        input_schema: task_create_schema(),
    })
}

#[async_trait]
impl Tool for TaskCreateTool {
    fn static_name(&self) -> &str {
        "TaskCreate"
    }
    fn static_description(&self) -> &str {
        "Append one task to the current plan (starts pending). Use TaskWrite for a new independent plan."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        plan_mode_task_write_permission(ctx)
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let s = input
            .get("subject")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Creating task: ").concat(&s))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let list_id = list_id_of(ctx);
        let subject = input
            .get("subject")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let description = input
            .get("description")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let active_form = input
            .get("activeForm")
            .and_then(RuntimeJson::as_str)
            .cloned();
        let metadata = input
            .get("metadata")
            .and_then(RuntimeJson::as_object)
            .cloned();
        // In a swarm team, a new task is auto-owned by the creating peer.
        let owner = ctx
            .swarm
            .as_ref()
            .and_then(|s| s.current())
            .map(|id| id.peer);

        let (task, receipt) = create_task_bound_receipt(
            &list_id,
            CreateTaskInput {
                subject,
                description,
                active_form,
                owner,
                metadata,
            },
            ctx.task_plan.as_ref(),
        )
        .await
        .map_err(store_err)?;

        // A TaskCreated hook may veto the new task — roll it back if so.
        if let Some(run_hook) = &ctx.run_hook {
            let hook = run_hook(
                HookEvent::TaskCreated,
                RunHookInput {
                    tool_name: Some(RuntimeString::from("TaskCreate")),
                    tool_input: Some(runtime_object([
                        ("taskId", RuntimeJson::String(task.id.clone())),
                        ("subject", RuntimeJson::String(task.subject.clone())),
                        ("description", RuntimeJson::String(task.description.clone())),
                    ])),
                },
            )
            .await;
            if hook.blocked {
                let _ = delete_task_bound(&list_id, &task.id, Some(&receipt))
                    .await
                    .map_err(store_err)?;
                let joined = join_runtime_strings(&hook.system_messages, "\n");
                let msg = if joined.is_empty() {
                    RuntimeString::from("Task creation vetoed by a hook.")
                } else {
                    joined
                };
                return Ok(text_result(
                    runtime_object([("task", task_summary(&task))]),
                    msg,
                    true,
                ));
            }
        }
        let message = RuntimeString::from("Created task #")
            .concat(&task.id)
            .concat(&RuntimeString::from(": "))
            .concat(&task.subject);
        Ok(text_result(
            runtime_object([("task", task_summary(&task))]),
            message,
            false,
        ))
    }
}

// ---- TaskGet ---------------------------------------------------------------

fn task_get_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "taskId": { "type": "string", "description": "The id of the task to read" }
        },
        "required": ["taskId"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Read one task by id (subject, status, description, dependencies).
pub struct TaskGetTool {
    input_schema: JsonSchema,
}

/// Construct the `TaskGet` tool.
pub fn create_task_get_tool() -> Arc<dyn Tool> {
    Arc::new(TaskGetTool {
        input_schema: task_get_schema(),
    })
}

#[async_trait]
impl Tool for TaskGetTool {
    fn static_name(&self) -> &str {
        "TaskGet"
    }
    fn static_description(&self) -> &str {
        "Read one task by id (subject, status, description, dependencies)."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let task_id = input
            .get("taskId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let task = get_task_bound(&list_id_of(ctx), &task_id, ctx.task_plan.as_ref())
            .await
            .map_err(store_err)?;
        let Some(task) = task else {
            return Ok(text_result(
                runtime_object([("task", RuntimeJson::Null)]),
                RuntimeString::from("Task #")
                    .concat(&task_id)
                    .concat(&RuntimeString::from(" does not exist.")),
                false,
            ));
        };
        let mut lines = vec![
            RuntimeString::from("Task #")
                .concat(&task.id)
                .concat(&RuntimeString::from(": "))
                .concat(&task.subject),
            RuntimeString::from("Status: ").concat(&RuntimeString::from(task.status.as_str())),
            RuntimeString::from("Description: ").concat(&task.description),
        ];
        if !task.blocked_by.is_empty() {
            lines.push(
                RuntimeString::from("Blocked by: ").concat(&join_runtime_strings(
                    &task.blocked_by.iter().map(hash).collect::<Vec<_>>(),
                    ", ",
                )),
            );
        }
        if !task.blocks.is_empty() {
            lines.push(
                RuntimeString::from("Blocks: ").concat(&join_runtime_strings(
                    &task.blocks.iter().map(hash).collect::<Vec<_>>(),
                    ", ",
                )),
            );
        }
        let encoded = runtime_codec::encode_value(&task).map_err(|error| {
            ToolError::new(
                RuntimeString::from("failed to encode task: ")
                    .concat(&RuntimeString::from(error.to_string())),
            )
        })?;
        let output = runtime_object([("task", encoded)]);
        Ok(text_result(
            output,
            join_runtime_strings(&lines, "\n"),
            false,
        ))
    }
}

// ---- TaskUpdate ------------------------------------------------------------

fn task_update_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "taskId": { "type": "string", "description": "The id of the task to update" },
            "subject": { "type": "string" },
            "description": { "type": "string" },
            "activeForm": { "type": "string" },
            "status": {
                "description": "New status; \"deleted\" removes the task",
                "anyOf": [
                    { "type": "string", "enum": ["pending", "in_progress", "completed"] },
                    { "type": "string", "const": "deleted" }
                ]
            },
            "addBlocks": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Task ids this task should block"
            },
            "addBlockedBy": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Task ids that should block this task"
            },
            "owner": { "type": "string" },
            "metadata": metadata_schema()
        },
        "required": ["taskId"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Update a task: status, fields, metadata, or dependency edges.
pub struct TaskUpdateTool {
    input_schema: JsonSchema,
}

#[cfg(test)]
type TaskUpdateAfterReadGate =
    std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>;
#[cfg(test)]
static TASK_UPDATE_AFTER_READ_GATE: std::sync::OnceLock<TaskUpdateAfterReadGate> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn install_task_update_after_read_gate() -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *TASK_UPDATE_AFTER_READ_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap() = Some((entered.clone(), release.clone()));
    (entered, release)
}

#[cfg(test)]
async fn wait_at_task_update_after_read_gate() {
    let gate = TASK_UPDATE_AFTER_READ_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap()
        .take();
    if let Some((entered, release)) = gate {
        entered.notify_one();
        release.notified().await;
    }
}

#[cfg(not(test))]
async fn wait_at_task_update_after_read_gate() {}

/// Construct the `TaskUpdate` tool.
pub fn create_task_update_tool() -> Arc<dyn Tool> {
    Arc::new(TaskUpdateTool {
        input_schema: task_update_schema(),
    })
}

#[async_trait]
impl Tool for TaskUpdateTool {
    fn static_name(&self) -> &str {
        "TaskUpdate"
    }
    fn static_description(&self) -> &str {
        "Update a task: status, fields, metadata, or dependency edges."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path.is_empty().then_some(&[
            "taskId",
            "subject",
            "description",
            "activeForm",
            "status",
            "addBlocks",
            "addBlockedBy",
            "owner",
            "metadata",
        ])
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        plan_mode_task_write_permission(ctx)
    }
    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let id = input
            .get("taskId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Updating task #").concat(&id))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let list_id = list_id_of(ctx);
        let task_id = input
            .get("taskId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let operation_binding = ctx
            .task_plan
            .as_ref()
            .map(crate::tasks::task_store::TaskPlanBinding::fork)
            .unwrap_or_else(|| crate::tasks::task_store::TaskPlanBinding::capture(&list_id));
        let current = get_task_bound(&list_id, &task_id, Some(&operation_binding))
            .await
            .map_err(store_err)?;
        let Some(current) = current else {
            // Non-error result: a missing task shouldn't cancel sibling tool calls.
            return Ok(text_result(
                runtime_object([
                    ("success", RuntimeJson::Bool(false)),
                    ("taskId", RuntimeJson::String(task_id.clone())),
                    ("error", RuntimeJson::from("not found")),
                ]),
                RuntimeString::from("Task #")
                    .concat(&task_id)
                    .concat(&RuntimeString::from(" does not exist.")),
                false,
            ));
        };
        wait_at_task_update_after_read_gate().await;

        let mut updates = TaskUpdate::default();
        let mut updated_fields: Vec<RuntimeString> = Vec::new();

        if let Some(subject) = input.get("subject").and_then(RuntimeJson::as_str) {
            if subject != &current.subject {
                updates.subject = Some(subject.clone());
                updated_fields.push(RuntimeString::from("subject"));
            }
        }
        if let Some(description) = input.get("description").and_then(RuntimeJson::as_str) {
            if description != &current.description {
                updates.description = Some(description.clone());
                updated_fields.push(RuntimeString::from("description"));
            }
        }
        if let Some(active_form) = input.get("activeForm").and_then(RuntimeJson::as_str) {
            if Some(active_form) != current.active_form.as_ref() {
                updates.active_form = Some(active_form.clone());
                updated_fields.push(RuntimeString::from("activeForm"));
            }
        }
        let in_owner = input.get("owner").and_then(RuntimeJson::as_str);
        if let Some(owner) = in_owner {
            if Some(owner) != current.owner.as_ref() {
                updates.owner = Some(owner.clone());
                updated_fields.push(RuntimeString::from("owner"));
            }
        }
        let in_status = input.get("status").and_then(RuntimeJson::as_str);
        // Swarm auto-claim: a peer moving an UNowned task to in_progress claims it.
        if in_owner.is_none()
            && in_status.is_some_and(|status| status == "in_progress")
            && current.owner.is_none()
        {
            if let Some(peer) = ctx
                .swarm
                .as_ref()
                .and_then(|s| s.current())
                .map(|id| id.peer)
            {
                updates.owner = Some(peer);
                updated_fields.push(RuntimeString::from("owner"));
            }
        }
        // Metadata: shallow-merge; a null value deletes that key.
        if let Some(meta) = input.get("metadata").and_then(RuntimeJson::as_object) {
            let mut merged = current.metadata.clone().unwrap_or_default();
            for (key, value) in meta.entries() {
                if value.is_null() {
                    merged.remove(key);
                } else {
                    merged.insert(key, value.clone());
                }
            }
            updates.metadata = Some(merged);
            updated_fields.push(RuntimeString::from("metadata"));
        }

        // Status: 'deleted' removes the task; otherwise it's a normal field change.
        if in_status.is_some_and(|status| status == "deleted") {
            let deleted = delete_task_bound(&list_id, &task_id, Some(&operation_binding))
                .await
                .map_err(store_err)?;
            if !deleted {
                return Ok(text_result(
                    runtime_object([
                        ("success", RuntimeJson::Bool(false)),
                        ("taskId", RuntimeJson::String(task_id.clone())),
                        ("updatedFields", RuntimeJson::Array(Vec::new())),
                        ("error", RuntimeJson::from("Failed to delete task")),
                    ]),
                    RuntimeString::from("Failed to delete task #")
                        .concat(&task_id)
                        .concat(&RuntimeString::from(".")),
                    false,
                ));
            }
            return Ok(text_result(
                runtime_object([
                    ("success", RuntimeJson::Bool(true)),
                    ("taskId", RuntimeJson::String(task_id.clone())),
                    (
                        "updatedFields",
                        RuntimeJson::Array(vec![RuntimeJson::from("deleted")]),
                    ),
                    (
                        "statusChange",
                        runtime_object([
                            ("from", RuntimeJson::from(current.status.as_str())),
                            ("to", RuntimeJson::from("deleted")),
                        ]),
                    ),
                ]),
                RuntimeString::from("Deleted task #")
                    .concat(&task_id)
                    .concat(&RuntimeString::from(".")),
                false,
            ));
        }
        // Completing a task may be vetoed by a TaskCompleted hook.
        if in_status.is_some_and(|status| status == "completed")
            && current.status != TaskStatus::Completed
        {
            if let Some(run_hook) = &ctx.run_hook {
                let hook = run_hook(
                    HookEvent::TaskCompleted,
                    RunHookInput {
                        tool_name: Some(RuntimeString::from("TaskUpdate")),
                        tool_input: Some(runtime_object([
                            ("taskId", RuntimeJson::String(task_id.clone())),
                            ("subject", RuntimeJson::String(current.subject.clone())),
                        ])),
                    },
                )
                .await;
                if hook.blocked {
                    let joined = join_runtime_strings(&hook.system_messages, "\n");
                    let msg = if joined.is_empty() {
                        RuntimeString::from("Completing #")
                            .concat(&task_id)
                            .concat(&RuntimeString::from(" was vetoed by a hook."))
                    } else {
                        joined
                    };
                    return Ok(text_result(
                        runtime_object([
                            ("success", RuntimeJson::Bool(false)),
                            ("taskId", RuntimeJson::String(task_id.clone())),
                            ("error", RuntimeJson::from("blocked by hook")),
                        ]),
                        msg,
                        false,
                    ));
                }
            }
        }
        let mut status_change: Option<(RuntimeString, RuntimeString)> = None;
        if let Some(st) = in_status {
            if st != current.status.as_str() {
                if let Some(ts) = parse_task_status(st) {
                    updates.status = Some(ts);
                    updated_fields.push(RuntimeString::from("status"));
                    status_change =
                        Some((RuntimeString::from(current.status.as_str()), st.clone()));
                }
            }
        }

        if !updated_fields.is_empty() {
            update_task_bound(&list_id, &task_id, &updates, Some(&operation_binding))
                .await
                .map_err(store_err)?;
        }
        // Dependency edges (bidirectional). addBlockedBy is the reverse direction.
        let add_blocks = string_array(input.get("addBlocks"));
        let add_blocked_by = string_array(input.get("addBlockedBy"));
        for id in &add_blocks {
            if !current.blocks.iter().any(|b| b == id) {
                block_task_bound(&list_id, &task_id, id, Some(&operation_binding))
                    .await
                    .map_err(store_err)?;
            }
        }
        for id in &add_blocked_by {
            if !current.blocked_by.iter().any(|b| b == id) {
                block_task_bound(&list_id, id, &task_id, Some(&operation_binding))
                    .await
                    .map_err(store_err)?;
            }
        }
        if !add_blocks.is_empty() {
            updated_fields.push(RuntimeString::from("blocks"));
        }
        if !add_blocked_by.is_empty() {
            updated_fields.push(RuntimeString::from("blockedBy"));
        }

        let joined = if updated_fields.is_empty() {
            RuntimeString::from("no change")
        } else {
            join_runtime_strings(&updated_fields, ", ")
        };
        let mut output = runtime_object([
            ("success", RuntimeJson::Bool(true)),
            ("taskId", RuntimeJson::String(task_id.clone())),
            (
                "updatedFields",
                RuntimeJson::Array(
                    updated_fields
                        .iter()
                        .cloned()
                        .map(RuntimeJson::String)
                        .collect(),
                ),
            ),
        ]);
        if let Some((from, to)) = &status_change {
            output.as_object_mut().unwrap().insert(
                "statusChange",
                runtime_object([
                    ("from", RuntimeJson::String(from.clone())),
                    ("to", RuntimeJson::String(to.clone())),
                ]),
            );
        }
        let text = RuntimeString::from("Updated #")
            .concat(&task_id)
            .concat(&RuntimeString::from(" ("))
            .concat(&joined)
            .concat(&RuntimeString::from(")"));
        Ok(text_result(output, text, false))
    }
}

// ---- TaskList --------------------------------------------------------------

fn task_list_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// List all tasks with their status, owner, and unresolved blockers.
pub struct TaskListTool {
    input_schema: JsonSchema,
}

/// Construct the `TaskList` tool.
pub fn create_task_list_tool() -> Arc<dyn Tool> {
    Arc::new(TaskListTool {
        input_schema: task_list_schema(),
    })
}

/// Whether a task carries the `_internal` metadata flag (hidden from listings).
fn is_internal(task: &Task) -> bool {
    task.metadata
        .as_ref()
        .and_then(|m| m.get("_internal"))
        .is_some_and(is_truthy)
}

#[async_trait]
impl Tool for TaskListTool {
    fn static_name(&self) -> &str {
        "TaskList"
    }
    fn static_description(&self) -> &str {
        "List the current plan with real task IDs, status, owner, and unresolved blockers. Archived tasks remain accessible with TaskGet."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
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

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let view = list_plan_view(&list_id_of(ctx), ctx.task_plan.as_ref())
            .await
            .map_err(store_err)?;
        let all: Vec<Task> = view
            .tasks
            .into_iter()
            .filter(|task| !is_internal(task))
            .collect();
        let entries: Vec<(&Task, Vec<RuntimeString>)> = all
            .iter()
            .map(|task| (task, task.blocked_by.clone()))
            .collect();
        let entry_ids = view.entry_ids;
        let tasks_json: Vec<RuntimeJson> = entries
            .iter()
            .map(|(t, blocked_by)| {
                let mut object = RuntimeObject::new();
                object.insert("id", RuntimeJson::String(t.id.clone()));
                if let Some(key) = entry_ids.get(&t.id) {
                    object.insert("entryId", RuntimeJson::String(key.clone()));
                }
                object.insert("subject", RuntimeJson::String(t.subject.clone()));
                object.insert("status", RuntimeJson::from(t.status.as_str()));
                if let Some(owner) = &t.owner {
                    object.insert("owner", RuntimeJson::String(owner.clone()));
                }
                object.insert(
                    "blockedBy",
                    RuntimeJson::Array(
                        blocked_by
                            .iter()
                            .cloned()
                            .map(RuntimeJson::String)
                            .collect(),
                    ),
                );
                RuntimeJson::Object(object)
            })
            .collect();

        if entries.is_empty() {
            return Ok(text_result(
                runtime_object([("tasks", RuntimeJson::Array(tasks_json))]),
                "No tasks yet.",
                false,
            ));
        }
        let lines: Vec<RuntimeString> = entries
            .iter()
            .map(|(t, blocked_by)| {
                let owner = match &t.owner {
                    Some(owner) if !owner.is_empty() => RuntimeString::from(" @").concat(owner),
                    _ => RuntimeString::default(),
                };
                let blockers = if blocked_by.is_empty() {
                    RuntimeString::default()
                } else {
                    RuntimeString::from(" \u{2014} blocked by ").concat(&join_runtime_strings(
                        &blocked_by.iter().map(hash).collect::<Vec<_>>(),
                        ", ",
                    ))
                };
                RuntimeString::from("#")
                    .concat(&t.id)
                    .concat(&RuntimeString::from(" "))
                    .concat(&RuntimeString::from(t.status.as_str()))
                    .concat(&RuntimeString::from(": "))
                    .concat(&t.subject)
                    .concat(&owner)
                    .concat(&blockers)
            })
            .collect();
        Ok(text_result(
            runtime_object([("tasks", RuntimeJson::Array(tasks_json))]),
            join_runtime_strings(&lines, "\n"),
            false,
        ))
    }
}

/// Write a complete new plan, or merge entries by their stable plan-local IDs.
pub fn create_task_write_tool() -> Arc<dyn Tool> {
    Arc::new(TaskWriteTool { input_schema: json!({
        "type": "object",
        "properties": {
            "tasks": { "type": "array", "minItems": 1, "items": {
                "type": "object", "properties": {
                    "id": { "type": "string", "minLength": 1, "description": "Stable plan-local merge key; not the durable task ID returned by this tool." },
                    "subject": { "type": "string", "minLength": 1 },
                    "description": { "type": "string" },
                    "activeForm": { "type": "string" },
                    "status": { "type": "string", "enum": ["pending", "in_progress", "completed"] }
                }, "additionalProperties": false
            }},
            "merge": { "type": "boolean", "default": false, "description": "False replaces the current plan; true updates matching entry IDs and appends new entries." }
        }, "required": ["tasks"], "additionalProperties": false
    }).as_object().cloned().unwrap_or_default() })
}

struct TaskWriteTool {
    input_schema: JsonSchema,
}

#[async_trait]
impl Tool for TaskWriteTool {
    fn static_name(&self) -> &str {
        "TaskWrite"
    }
    fn static_description(&self) -> &str {
        "Create or update the current progress checklist when it helps carry out multi-step work. This tool does not enter or exit plan mode. By default it replaces the visible plan and archives the previous one; merge=true updates stable entry IDs and appends new entries. Completion and user messages never clear a plan. Use returned real task IDs with TaskUpdate and TaskGet."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn check_permissions(
        &self,
        _input: &Json,
        ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        plan_mode_task_write_permission(ctx)
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let values = input
            .get("tasks")
            .and_then(RuntimeJson::as_array)
            .ok_or_else(|| ToolError::new("TaskWrite requires a non-empty tasks array"))?;
        let mut entries = Vec::with_capacity(values.len());
        for value in values {
            let subject = value.get("subject").and_then(RuntimeJson::as_str).cloned();
            let status = value
                .get("status")
                .map(|value| {
                    value
                        .as_str()
                        .and_then(parse_task_status)
                        .ok_or_else(|| ToolError::new("Invalid task status"))
                })
                .transpose()?;
            entries.push(TaskPlanWriteEntry {
                entry_id: value.get("id").and_then(RuntimeJson::as_str).cloned(),
                subject,
                description: value
                    .get("description")
                    .and_then(RuntimeJson::as_str)
                    .cloned(),
                active_form: value
                    .get("activeForm")
                    .and_then(RuntimeJson::as_str)
                    .cloned(),
                status,
            });
        }
        let merge = match input.get("merge") {
            None => false,
            Some(RuntimeJson::Bool(value)) => *value,
            _ => return Err(ToolError::new("TaskWrite merge must be a boolean")),
        };
        let approve: Option<TaskPlanApprover> = ctx.run_hook.clone().map(|run_hook| {
            Box::new(
                move |changes: Vec<crate::tasks::task_store::TaskPlanChange>| {
                    Box::pin(async move {
                        for change in changes {
                            let mut events = Vec::new();
                            if change.previous.is_none() {
                                events.push(HookEvent::TaskCreated);
                            }
                            if change.task.status == TaskStatus::Completed
                                && change
                                    .previous
                                    .as_ref()
                                    .is_none_or(|old| old.status != TaskStatus::Completed)
                            {
                                events.push(HookEvent::TaskCompleted);
                            }
                            for event in events {
                                let outcome = run_hook(
                                    event,
                                    RunHookInput {
                                        tool_name: Some("TaskWrite".into()),
                                        tool_input: Some(runtime_object([
                                            ("taskId", RuntimeJson::String(change.task.id.clone())),
                                            (
                                                "subject",
                                                RuntimeJson::String(change.task.subject.clone()),
                                            ),
                                            (
                                                "description",
                                                RuntimeJson::String(
                                                    change.task.description.clone(),
                                                ),
                                            ),
                                        ])),
                                    },
                                )
                                .await;
                                if outcome.blocked {
                                    let reason =
                                        join_runtime_strings(&outcome.system_messages, "\n");
                                    return Err(std::io::Error::other(if reason.is_empty() {
                                        "TaskWrite vetoed by a task hook".to_string()
                                    } else {
                                        String::from_utf8_lossy(&reason.to_node_utf8()).into_owned()
                                    }));
                                }
                            }
                        }
                        Ok(())
                    })
                        as futures::future::BoxFuture<'static, std::io::Result<()>>
                },
            ) as TaskPlanApprover
        });
        let view = write_task_plan_approved(
            &list_id_of(ctx),
            entries,
            merge,
            ctx.task_plan.as_ref(),
            approve,
        )
        .await
        .map_err(store_err)?;
        let tasks = view.tasks;
        let entry_ids = view.entry_ids;
        let mut values = Vec::with_capacity(tasks.len());
        let mut lines = Vec::with_capacity(tasks.len());
        for task in &tasks {
            let mut value = runtime_codec::encode_value(task)
                .map_err(|error| ToolError::new(error.to_string()))?;
            let key = entry_ids.get(&task.id);
            if let (Some(key), RuntimeJson::Object(object)) = (key, &mut value) {
                object.insert("entryId", RuntimeJson::String(key.clone()));
            }
            values.push(value);
            let mut line = hash(&task.id)
                .concat(&RuntimeString::from(" "))
                .concat(&RuntimeString::from(task.status.as_str()))
                .concat(&RuntimeString::from(": "))
                .concat(&task.subject);
            if let Some(key) = key {
                line = line
                    .concat(&RuntimeString::from(" [entryId: "))
                    .concat(key)
                    .concat(&RuntimeString::from("]"));
            }
            lines.push(line);
        }
        Ok(text_result(
            runtime_object([("tasks", RuntimeJson::Array(values))]),
            join_runtime_strings(&lines, "\n"),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::tasks::task_store::{get_task, list_plan_tasks};
    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;
    use crate::tools::builtins::test_ctx::make_ctx;
    use std::path::PathBuf;
    use std::sync::MutexGuard;

    const LIST: &str = "tl";

    macro_rules! rjson {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn rs(value: &str) -> RuntimeString {
        RuntimeString::from(value)
    }

    fn task_status(id: &RuntimeString, status: &str) -> RuntimeJson {
        runtime_object([
            ("taskId", RuntimeJson::String(id.clone())),
            ("status", RuntimeJson::String(rs(status))),
        ])
    }

    struct TestHome {
        dir: PathBuf,
        _guard: MutexGuard<'static, ()>,
    }

    impl TestHome {
        fn new() -> Self {
            let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let mut dir = std::env::temp_dir();
            dir.push(format!(
                "ae-tasktools-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &dir);
            TestHome { dir, _guard: guard }
        }
    }

    impl Drop for TestHome {
        fn drop(&mut self) {
            crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR");
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn ctx() -> ToolContext {
        let mut c = make_ctx("/tmp");
        c.task_list_id = Some(rs(LIST));
        c
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    /// Progress checklists update engine-owned state without editing project files.
    /// Keep these tools available in plan mode without requiring their use.
    #[tokio::test]
    async fn task_writes_are_admitted_in_plan_mode_and_unopinionated_elsewhere() {
        for (name, tool, input) in [
            ("TaskWrite", create_task_write_tool(), rjson!({"tasks": []})),
            (
                "TaskCreate",
                create_task_create_tool(),
                rjson!({"subject": "s", "description": "d"}),
            ),
            (
                "TaskUpdate",
                create_task_update_tool(),
                rjson!({"taskId": "t", "status": "pending"}),
            ),
        ] {
            // Plan mode: admitted. These write the engine's task store, never the
            // user's workspace, so "no edits until the plan is approved" still holds.
            let mut plan_ctx = ctx();
            plan_ctx.planning_read_only = true;
            assert_eq!(
                tool.check_permissions(&input, &plan_ctx).await,
                Some(PermissionResult::Allow {
                    updated_input: None,
                    reason: PermissionReason::ToolSelfCheck,
                }),
                "{name} must be usable while planning"
            );

            // Ordinary filesystem policies do not change the tool's own contract.
            for sandbox_mode in [
                crate::types::permission::SandboxMode::ReadOnly,
                crate::types::permission::SandboxMode::WorkspaceWrite,
                crate::types::permission::SandboxMode::DangerFullAccess,
            ] {
                let mut c = ctx();
                c.sandbox_mode = Some(sandbox_mode);
                assert_eq!(
                    tool.check_permissions(&input, &c).await,
                    None,
                    "{name} must not short-circuit {sandbox_mode:?}"
                );
            }
        }
    }

    #[test]
    fn schemas_and_metadata() {
        let create = create_task_create_tool();
        assert_eq!(create.name(), "TaskCreate");
        assert!(!create.is_read_only(&rjson!({})));
        assert_eq!(
            create.input_schema().get("required").unwrap(),
            &rjson!(["subject", "description"])
        );
        let update = create_task_update_tool();
        let update_schema = update.input_schema();
        let status = update_schema
            .get("properties")
            .and_then(|p| p.get("status"))
            .unwrap();
        assert!(status.get("anyOf").is_some());
        let get = create_task_get_tool();
        assert!(get.is_read_only(&rjson!({})));
        let list = create_task_list_tool();
        assert!(list.is_read_only(&rjson!({})));
        assert_eq!(list.input_schema().get("properties").unwrap(), &rjson!({}));
    }

    #[tokio::test]
    async fn create_get_list_update_round_trip() {
        let _home = TestHome::new();
        let ctx = ctx();

        let create = create_task_create_tool();
        let res = create
            .call(
                rjson!({ "subject": "do a thing", "description": "the details" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "Created task #1: do a thing");
        assert_eq!(
            res.serializable.output,
            rjson!({ "task": { "id": "1", "subject": "do a thing" } })
        );

        let get = create_task_get_tool();
        let res = get
            .call(rjson!({ "taskId": "1" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "Task #1: do a thing\nStatus: pending\nDescription: the details"
        );

        let list = create_task_list_tool();
        let res = list.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(text_of(&res), "#1 pending: do a thing");

        let update = create_task_update_tool();
        let res = update
            .call(
                rjson!({ "taskId": "1", "status": "in_progress" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Updated #1 (status)");
        assert_eq!(
            res.serializable.output,
            rjson!({
                "success": true,
                "taskId": "1",
                "updatedFields": ["status"],
                "statusChange": { "from": "pending", "to": "in_progress" }
            })
        );
    }

    #[tokio::test]
    async fn get_and_update_report_a_missing_task_without_erroring() {
        let _home = TestHome::new();
        let ctx = ctx();
        let get = create_task_get_tool();
        let res = get
            .call(rjson!({ "taskId": "42" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "Task #42 does not exist.");
        assert_eq!(res.serializable.output, rjson!({ "task": null }));

        let update = create_task_update_tool();
        let res = update
            .call(
                rjson!({ "taskId": "42", "status": "completed" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            rjson!({ "success": false, "taskId": "42", "error": "not found" })
        );
    }

    #[tokio::test]
    async fn update_deletes_a_task_and_merges_metadata() {
        let _home = TestHome::new();
        let ctx = ctx();
        let create = create_task_create_tool();
        create
            .call(
                rjson!({ "subject": "s", "description": "d", "metadata": { "a": 1, "b": 2 } }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let update = create_task_update_tool();
        // Merge: set a=9, delete b.
        let res = update
            .call(
                rjson!({ "taskId": "1", "metadata": { "a": 9, "b": null } }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Updated #1 (metadata)");
        let stored = get_task(&rs(LIST), &rs("1")).await.unwrap().unwrap();
        assert_eq!(
            stored.metadata.unwrap(),
            rjson!({ "a": 9 }).as_object().unwrap().clone()
        );

        // Delete via status.
        let res = update
            .call(rjson!({ "taskId": "1", "status": "deleted" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Deleted task #1.");
        assert!(get_task(&rs(LIST), &rs("1")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_race_after_task_update_pre_read_returns_cc_false_result() {
        let _home = TestHome::new();
        let ctx = ctx();
        create_task_create_tool()
            .call(
                rjson!({ "subject": "racy", "description": "d" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let (entered, release) = install_task_update_after_read_gate();
        let update = create_task_update_tool();
        let call = tokio::spawn(async move {
            update
                .call(rjson!({ "taskId": "1", "status": "deleted" }), &ctx, None)
                .await
        });
        entered.notified().await;
        std::fs::remove_file(PathBuf::from(crate::fs::paths::tasks_dir(LIST)).join("1.json"))
            .unwrap();
        release.notify_one();

        let result = call.await.unwrap().unwrap();
        assert_eq!(
            result.serializable.output,
            rjson!({
                "success": false,
                "taskId": "1",
                "updatedFields": [],
                "error": "Failed to delete task"
            })
        );
        assert_eq!(text_of(&result), "Failed to delete task #1.");
    }

    #[tokio::test]
    async fn list_hides_internal_tasks_and_drops_completed_blockers() {
        let _home = TestHome::new();
        let ctx = ctx();
        let create = create_task_create_tool();
        create
            .call(rjson!({ "subject": "one", "description": "d" }), &ctx, None)
            .await
            .unwrap(); // #1
        create
            .call(rjson!({ "subject": "two", "description": "d" }), &ctx, None)
            .await
            .unwrap(); // #2
        create
            .call(rjson!({ "subject": "hidden", "description": "d", "metadata": { "_internal": true } }), &ctx, None)
            .await
            .unwrap(); // #3 internal

        let update = create_task_update_tool();
        // #1 blocked by #2.
        update
            .call(rjson!({ "taskId": "1", "addBlockedBy": ["2"] }), &ctx, None)
            .await
            .unwrap();

        let list = create_task_list_tool();
        let res = list.call(rjson!({}), &ctx, None).await.unwrap();
        let text = text_of(&res);
        assert!(
            text.contains("#1 pending: one \u{2014} blocked by #2"),
            "{text:?}"
        );
        assert!(!text.contains("hidden"), "{text:?}");

        // Complete #2 -> it stops blocking #1.
        update
            .call(rjson!({ "taskId": "2", "status": "completed" }), &ctx, None)
            .await
            .unwrap();
        let res = list.call(rjson!({}), &ctx, None).await.unwrap();
        let text = text_of(&res);
        assert!(text.contains("#1 pending: one"), "{text:?}");
        assert!(!text.contains("blocked by"), "{text:?}");
    }
    #[tokio::test]
    async fn stable_agent_plans_restore_independently_and_keep_shared_tasks_addressable() {
        use crate::tasks::task_store::{query_task_list, TaskPlanBinding};
        let _home = TestHome::new();
        let mut parent = ctx();
        parent.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let write = create_task_write_tool();
        let create = create_task_create_tool();
        let mut child_a = ctx();
        child_a.task_plan = Some(parent.task_plan.as_ref().unwrap().for_agent(&rs("agent-a")));
        let mut child_b = ctx();
        child_b.task_plan = Some(parent.task_plan.as_ref().unwrap().for_agent(&rs("agent-b")));
        write
            .call(
                rjson!({"tasks":[{"id":"same-key","subject":"child A"}]}),
                &child_a,
                None,
            )
            .await
            .unwrap();
        let empty_main = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert!(empty_main.tasks.is_empty());
        assert!(empty_main.plan_id.is_none());
        // A private plan created first must not invalidate the root's None binding.
        create
            .call(
                rjson!({"subject":"root task","description":"shared durable work"}),
                &parent,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            list_plan_tasks(&rs(LIST), parent.task_plan.as_ref())
                .await
                .unwrap()[0]
                .id,
            "1"
        );
        write
            .call(
                rjson!({"tasks":[{"id":"same-key","subject":"child B"}]}),
                &child_b,
                None,
            )
            .await
            .unwrap();
        let main = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(main.tasks[0].subject, "root task");
        let mut restored_a = ctx();
        restored_a.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)).for_agent(&rs("agent-a")));
        let a_tasks = list_plan_tasks(&rs(LIST), restored_a.task_plan.as_ref())
            .await
            .unwrap();
        assert_eq!(a_tasks.len(), 1);
        assert_eq!(a_tasks[0].subject, "child A");
        write
            .call(
                rjson!({"merge":true,"tasks":[{"id":"same-key","status":"completed"}]}),
                &restored_a,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            list_plan_tasks(&rs(LIST), child_b.task_plan.as_ref())
                .await
                .unwrap()[0]
                .subject,
            "child B"
        );
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap(),
            main
        );
        // Private plans never prevent the parent from starting its next independent plan.
        write
            .call(
                rjson!({"tasks":[{"subject":"new parent plan"}]}),
                &parent,
                None,
            )
            .await
            .unwrap();
        create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &restored_a, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(
                rjson!({"taskId":"1","status":"completed"}),
                &restored_a,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            get_task(&rs(LIST), &rs("1")).await.unwrap().unwrap().status,
            TaskStatus::Completed
        );
        // A child without a private plan sees its captured parent plan, but cannot
        // append to a newer plan after the parent replaces that captured view.
        let mut inherited = ctx();
        inherited.task_plan = Some(
            parent
                .task_plan
                .as_ref()
                .unwrap()
                .for_agent(&rs("inherited")),
        );
        create
            .call(
                rjson!({"subject":"shared append","description":"continuation"}),
                &inherited,
                None,
            )
            .await
            .unwrap();
        write
            .call(
                rjson!({"tasks":[{"subject":"latest parent"}]}),
                &parent,
                None,
            )
            .await
            .unwrap();
        assert!(create
            .call(
                rjson!({"subject":"late append","description":"stale"}),
                &inherited,
                None
            )
            .await
            .is_err());
        write
            .call(
                rjson!({"tasks":[{"subject":"independent child"}]}),
                &inherited,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap()
                .tasks[0]
                .subject,
            "latest parent"
        );
    }

    #[tokio::test]
    async fn task_write_replaces_the_current_plan_without_reusing_durable_ids() {
        use crate::tasks::task_store::{query_task_list, TaskPlanBinding};
        let _home = TestHome::new();
        let mut ctx = ctx();
        ctx.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let create = create_task_create_tool();
        create
            .call(
                rjson!({"subject":"legacy","description":"old task"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let write = create_task_write_tool();
        write.call(rjson!({"tasks":[{"id":"random-b","subject":"B"},{"id":"random-a","subject":"A","status":"completed"}]}), &ctx, None).await.unwrap();
        let first = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(first.plan_kind, "plan");
        assert_eq!(
            first
                .tasks
                .iter()
                .map(|task| task.subject.clone())
                .collect::<Vec<_>>(),
            vec![rs("B"), rs("A")]
        );
        assert_eq!(first.tasks[1].status, TaskStatus::Completed);
        let old_run = ctx.task_plan.as_ref().unwrap().fork();
        write
            .call(
                rjson!({"tasks":[{"id":"random-b","subject":"independent new work"}]}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let second = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_ne!(first.plan_id, second.plan_id);
        assert_eq!(second.tasks.len(), 1);
        assert!(second.tasks[0].id.starts_with("plan-step-"));
        assert_ne!(second.tasks[0].id, first.tasks[0].id);
        assert!(get_task(&rs(LIST), &first.tasks[0].id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            list_plan_tasks(&rs(LIST), Some(&old_run))
                .await
                .unwrap()
                .len(),
            2
        );
        let mut old_run_ctx = self::ctx();
        old_run_ctx.task_plan = Some(old_run);
        assert!(create
            .call(
                rjson!({"subject":"late old run","description":"must not append to new work"}),
                &old_run_ctx,
                None
            )
            .await
            .is_err());
        assert!(write
            .call(
                rjson!({"tasks":[{"subject":"late replacement"}]}),
                &old_run_ctx,
                None
            )
            .await
            .is_err());
        create_task_update_tool()
            .call(
                task_status(&first.tasks[0].id, "completed"),
                &old_run_ctx,
                None,
            )
            .await
            .unwrap();
        let after_old_update = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(
            after_old_update, second,
            "archived updates cannot repaint current plan"
        );
        create_task_update_tool()
            .call(task_status(&second.tasks[0].id, "completed"), &ctx, None)
            .await
            .unwrap();
        let completed = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(completed.tasks.len(), 1, "completion retains the plan");
        create
            .call(
                rjson!({"subject":"deliberate append","description":"same plan"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let appended = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(appended.plan_id, second.plan_id);
        assert_eq!(appended.plan_kind, "plan");
        assert_eq!(appended.tasks[1].id, "2");
    }

    #[tokio::test]
    async fn task_write_merge_preserves_order_and_rejects_empty_without_mutation() {
        use crate::tasks::task_store::{query_task_list, TaskListQueryOptions, TaskPlanBinding};
        let _home = TestHome::new();
        let mut ctx = ctx();
        ctx.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let write = create_task_write_tool();
        write
            .call(
                rjson!({"tasks":[{"id":"b","subject":"B"},{"id":"a","subject":"A"}]}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let before = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        write.call(rjson!({"merge":true,"tasks":[{"id":"a","subject":"A done","status":"completed"},{"id":"c","subject":"C"}]}), &ctx, None).await.unwrap();
        let merged = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(merged.plan_id, before.plan_id);
        assert_eq!(
            merged
                .tasks
                .iter()
                .map(|task| task.subject.clone())
                .collect::<Vec<_>>(),
            vec![rs("B"), rs("A done"), rs("C")]
        );
        assert_eq!(merged.tasks[1].id, before.tasks[1].id);
        assert!(write.call(rjson!({"tasks":[]}), &ctx, None).await.is_err());
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap(),
            merged
        );
        let recovered = TaskPlanBinding::capture(&rs(LIST));
        assert_eq!(
            list_plan_tasks(&rs(LIST), Some(&recovered)).await.unwrap(),
            merged.tasks
        );
        let page = query_task_list(
            &rs(LIST),
            &rs(LIST),
            TaskListQueryOptions {
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        let tail = query_task_list(
            &rs(LIST),
            &rs(LIST),
            TaskListQueryOptions {
                cursor: page.next_cursor.clone(),
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!([page.tasks, tail.tasks].concat(), merged.tasks);
        assert_eq!(tail.plan_id, merged.plan_id);
        // Concurrent append calls share the same current-plan binding and remain ordered.
        let create = create_task_create_tool();
        let (a, b) = tokio::join!(
            create.call(rjson!({"subject":"D","description":"append"}), &ctx, None),
            create.call(rjson!({"subject":"E","description":"append"}), &ctx, None)
        );
        a.unwrap();
        b.unwrap();
        let final_snapshot = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(final_snapshot.tasks.len(), 5);
        assert_eq!(final_snapshot.plan_id, merged.plan_id);
        let mut ids: Vec<_> = final_snapshot
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5);
    }

    #[tokio::test]
    async fn archived_blocker_completion_updates_the_current_plan_projection() {
        use crate::tasks::task_store::{block_task, query_task_list};
        let _home = TestHome::new();
        let ctx = ctx();
        let create = create_task_create_tool();
        create
            .call(
                rjson!({"subject":"old prerequisite","description":"old"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        create_task_write_tool()
            .call(rjson!({"tasks":[{"subject":"new dependent"}]}), &ctx, None)
            .await
            .unwrap();
        let current = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        block_task(&rs(LIST), &rs("1"), &current.tasks[0].id)
            .await
            .unwrap();
        let blocked = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(blocked.tasks[0].blocked_by, vec![rs("1")]);
        create_task_update_tool()
            .call(rjson!({"taskId":"1","status":"completed"}), &ctx, None)
            .await
            .unwrap();
        let ready = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert!(ready.tasks[0].blocked_by.is_empty());
        assert!(ready.revision > blocked.revision);
        assert_eq!(ready.plan_id, blocked.plan_id);
    }
    #[tokio::test]
    async fn task_write_partial_merge_and_hooks_preserve_the_previous_complete_plan() {
        use crate::tasks::task_store::query_task_list;
        use crate::types::tool::RunHookResult;
        use std::sync::atomic::{AtomicBool, Ordering};
        let _home = TestHome::new();
        let mut ctx = ctx();
        let write = create_task_write_tool();
        write.call(rjson!({"tasks":[{"id":"a","subject":"Keep title","description":"Keep details","activeForm":"Working"}]}), &ctx, None).await.unwrap();
        let before = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        let veto = Arc::new(AtomicBool::new(true));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        ctx.run_hook = Some(Arc::new({
            let veto = veto.clone();
            let seen = seen.clone();
            move |event, _input| {
                let veto = veto.clone();
                let seen = seen.clone();
                Box::pin(async move {
                    // A hook can query the facade's store without deadlocking a write lock.
                    query_task_list(&rs(LIST), &rs(LIST), Default::default())
                        .await
                        .unwrap();
                    seen.lock().unwrap().push(event);
                    RunHookResult {
                        blocked: veto.load(Ordering::SeqCst),
                        system_messages: vec![rs("hook veto")],
                    }
                })
            }
        }));
        assert!(tokio::time::timeout(
            std::time::Duration::from_secs(10),
            write.call(
                rjson!({"merge":true,"tasks":[{"id":"a","status":"completed"}]}),
                &ctx,
                None
            )
        )
        .await
        .unwrap()
        .is_err());
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap(),
            before
        );
        assert!(seen.lock().unwrap().contains(&HookEvent::TaskCompleted));
        assert!(write
            .call(
                rjson!({"tasks":[{"id":"b","subject":"Vetoed new plan"}]}),
                &ctx,
                None
            )
            .await
            .is_err());
        assert!(seen.lock().unwrap().contains(&HookEvent::TaskCreated));
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap(),
            before
        );
        veto.store(false, Ordering::SeqCst);
        write
            .call(
                rjson!({"merge":true,"tasks":[{"id":"a","status":"completed"}]}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let complete = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(complete.plan_id, before.plan_id);
        assert_eq!(complete.tasks[0].subject, "Keep title");
        assert_eq!(complete.tasks[0].description, "Keep details");
        assert_eq!(complete.tasks[0].active_form, Some(rs("Working")));
        assert_eq!(complete.tasks[0].status, TaskStatus::Completed);
        write
            .call(
                rjson!({"merge":true,"tasks":[{"id":"a","activeForm":"Finishing"}]}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let updated = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(updated.tasks[0].subject, "Keep title");
        assert_eq!(updated.tasks[0].active_form, Some(rs("Finishing")));
        assert!(write
            .call(
                rjson!({"merge":true,"tasks":[{"id":"new","status":"pending"}]}),
                &ctx,
                None
            )
            .await
            .is_err());
        assert_eq!(
            query_task_list(&rs(LIST), &rs(LIST), Default::default())
                .await
                .unwrap(),
            updated
        );
    }

    #[tokio::test]
    async fn task_write_hook_race_does_not_overwrite_a_newer_plan() {
        use crate::tasks::task_store::{create_task, query_task_list};
        use crate::types::tool::RunHookResult;
        let _home = TestHome::new();
        let mut ctx = ctx();
        create_task_create_tool()
            .call(
                rjson!({"subject":"original","description":"old"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        ctx.run_hook = Some(Arc::new(move |_event, _input| {
            Box::pin(async move {
                create_task(
                    &rs(LIST),
                    CreateTaskInput {
                        subject: rs("concurrent continuation"),
                        description: rs("hook wrote"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                RunHookResult {
                    blocked: false,
                    system_messages: Vec::new(),
                }
            })
        }));
        let write = create_task_write_tool();
        assert!(write
            .call(
                rjson!({"tasks":[{"subject":"must not clobber newer work"}]}),
                &ctx,
                None
            )
            .await
            .is_err());
        let current = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(current.tasks.len(), 2);
        assert_eq!(current.tasks[1].subject, "concurrent continuation");
    }
    #[tokio::test]
    async fn recycled_task_numbers_reject_stale_actors_but_new_tasks_can_be_learned() {
        use crate::tasks::task_store::{query_task_list, TaskPlanBinding};
        let _home = TestHome::new();
        let mut parent = ctx();
        parent.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let mut child = ctx();
        child.task_plan = Some(parent.task_plan.as_ref().unwrap().fork());
        let create = create_task_create_tool();
        create
            .call(
                rjson!({"subject":"child task","description":"coordinate"}),
                &child,
                None,
            )
            .await
            .unwrap();
        // A parent captured before its child created #1 may learn that new identity.
        create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &parent, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("1"), "in_progress"), &parent, None)
            .await
            .unwrap();
        let mut stale = ctx();
        stale.task_plan = Some(parent.task_plan.as_ref().unwrap().fork());
        create_task_update_tool()
            .call(task_status(&rs("1"), "deleted"), &parent, None)
            .await
            .unwrap();
        let replacement = create
            .call(
                rjson!({"subject":"new incarnation","description":"reuse highest"}),
                &parent,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            replacement
                .serializable
                .output
                .pointer("/task/id")
                .and_then(RuntimeJson::as_str),
            Some(&rs("1"))
        );
        assert!(create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &stale, None)
            .await
            .is_err());
        assert!(create_task_update_tool()
            .call(task_status(&rs("1"), "completed"), &stale, None)
            .await
            .is_err());
        assert!(create_task_update_tool()
            .call(task_status(&rs("1"), "deleted"), &stale, None)
            .await
            .is_err());
        create_task_update_tool()
            .call(task_status(&rs("1"), "completed"), &parent, None)
            .await
            .unwrap();
        let mut restored = ctx();
        restored.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &restored, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("1"), "pending"), &restored, None)
            .await
            .unwrap();
        // TaskList is another supported way to learn a genuinely new child-created ID.
        child.task_plan = Some(parent.task_plan.as_ref().unwrap().fork());
        create
            .call(
                rjson!({"subject":"second child task","description":"learn through list"}),
                &child,
                None,
            )
            .await
            .unwrap();
        create_task_list_tool()
            .call(rjson!({}), &parent, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("2"), "completed"), &parent, None)
            .await
            .unwrap();
        let snapshot = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.tasks[0].subject, "new incarnation");
        assert_eq!(snapshot.tasks[1].status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn archived_membership_never_resolves_a_recycled_number() {
        use crate::tasks::task_store::{query_task_list, TaskPlanBinding};
        let _home = TestHome::new();
        let mut parent = ctx();
        parent.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let create = create_task_create_tool();
        create
            .call(
                rjson!({"subject":"old numeric task","description":"archive"}),
                &parent,
                None,
            )
            .await
            .unwrap();
        let mut old = ctx();
        old.task_plan = Some(parent.task_plan.as_ref().unwrap().fork());
        create_task_write_tool()
            .call(rjson!({"tasks":[{"subject":"new plan"}]}), &parent, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("1"), "deleted"), &parent, None)
            .await
            .unwrap();
        create
            .call(
                rjson!({"subject":"new numeric task","description":"new membership"}),
                &parent,
                None,
            )
            .await
            .unwrap();
        assert!(list_plan_tasks(&rs(LIST), old.task_plan.as_ref())
            .await
            .unwrap()
            .is_empty());
        assert!(create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &old, None)
            .await
            .is_err());
        let snapshot = query_task_list(&rs(LIST), &rs(LIST), Default::default())
            .await
            .unwrap();
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[1].id, "1");
    }
    #[tokio::test]
    async fn one_update_keeps_its_identity_when_a_sibling_recycles_the_number_during_a_hook() {
        use crate::tasks::task_store::{create_task_bound, TaskPlanBinding};
        use crate::types::tool::RunHookResult;
        let _home = TestHome::new();
        let mut ctx = ctx();
        ctx.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        create_task_create_tool()
            .call(
                rjson!({"subject":"old task","description":"before hook"}),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let binding = ctx.task_plan.clone().unwrap();
        ctx.run_hook = Some(Arc::new(move |event, _input| {
            let binding = binding.clone();
            Box::pin(async move {
                assert_eq!(event, HookEvent::TaskCompleted);
                delete_task_bound(&rs(LIST), &rs("1"), Some(&binding))
                    .await
                    .unwrap();
                let new = create_task_bound(
                    &rs(LIST),
                    CreateTaskInput {
                        subject: rs("new sibling task"),
                        description: rs("after hook"),
                        ..Default::default()
                    },
                    Some(&binding),
                )
                .await
                .unwrap();
                assert_eq!(new.id, "1");
                RunHookResult {
                    blocked: false,
                    system_messages: Vec::new(),
                }
            })
        }));
        assert!(create_task_update_tool()
            .call(task_status(&rs("1"), "completed"), &ctx, None)
            .await
            .is_err());
        let new = get_task(&rs(LIST), &rs("1")).await.unwrap().unwrap();
        assert_eq!(new.subject, "new sibling task");
        assert_eq!(new.status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn creation_veto_cannot_delete_a_sibling_reusing_the_same_number() {
        use crate::tasks::task_store::{create_task_bound, TaskPlanBinding};
        use crate::types::tool::RunHookResult;
        let _home = TestHome::new();
        let mut ctx = ctx();
        ctx.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let binding = ctx.task_plan.clone().unwrap();
        ctx.run_hook = Some(Arc::new(move |event, _input| {
            let binding = binding.clone();
            Box::pin(async move {
                assert_eq!(event, HookEvent::TaskCreated);
                delete_task_bound(&rs(LIST), &rs("1"), Some(&binding))
                    .await
                    .unwrap();
                create_task_bound(
                    &rs(LIST),
                    CreateTaskInput {
                        subject: rs("surviving sibling"),
                        description: rs("same public number"),
                        ..Default::default()
                    },
                    Some(&binding),
                )
                .await
                .unwrap();
                RunHookResult {
                    blocked: true,
                    system_messages: vec![rs("veto the original creation")],
                }
            })
        }));
        assert!(create_task_create_tool()
            .call(
                rjson!({"subject":"vetoed original","description":"first incarnation"}),
                &ctx,
                None
            )
            .await
            .is_err());
        assert_eq!(
            get_task(&rs(LIST), &rs("1"))
                .await
                .unwrap()
                .unwrap()
                .subject,
            "surviving sibling"
        );
    }

    #[tokio::test]
    async fn a_new_run_does_not_inherit_deleted_task_tombstones() {
        use crate::tasks::task_store::TaskPlanBinding;
        let _home = TestHome::new();
        let mut old = ctx();
        old.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let create = create_task_create_tool();
        create
            .call(
                rjson!({"subject":"removed","description":"old run"}),
                &old,
                None,
            )
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("1"), "deleted"), &old, None)
            .await
            .unwrap();
        let mut parent = ctx();
        parent.task_plan = Some(TaskPlanBinding::capture(&rs(LIST)));
        let mut child = ctx();
        child.task_plan = Some(parent.task_plan.as_ref().unwrap().fork());
        create
            .call(
                rjson!({"subject":"reused after capture","description":"child"}),
                &child,
                None,
            )
            .await
            .unwrap();
        create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &parent, None)
            .await
            .unwrap();
        create_task_update_tool()
            .call(task_status(&rs("1"), "completed"), &parent, None)
            .await
            .unwrap();
        assert!(create_task_get_tool()
            .call(rjson!({"taskId":"1"}), &old, None)
            .await
            .is_err());
    }
}
