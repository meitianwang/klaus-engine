use crate::engine::deps::{noop_compactor, EngineDeps};
use crate::engine::query::{query, QueryParams, SystemPromptResolvable};
use crate::permission::engine::StaticPermissionEngine;
use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::testing::{ev, make_usage, scripted_call_model, ScriptedTurns};
use crate::tools::build_tool::text_result;
use crate::tools::pool::to_tool_specs;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::PermissionContext;
use crate::types::provider::{ModelStopReason, ModelStreamEvent, SystemPromptPart};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SCENARIO: &str = "query-loop-scripted-provider";

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
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

pub async fn query_loop_trace(
    process_context: &RuntimeProcessContext,
) -> Result<Json, RuntimeCodecError> {
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool {
        schema: echo_schema(),
    })];
    let persisted_messages: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let params = query_params(process_context, tools, Arc::clone(&persisted_messages));
    let events: Vec<_> = query(params, deps()).collect().await;
    let event_values: Vec<Json> = events
        .iter()
        .map(runtime_codec::encode_value)
        .collect::<Result<_, _>>()?;
    let persisted_values: Vec<Json> = persisted_messages
        .lock()
        .unwrap()
        .iter()
        .map(runtime_codec::encode_value)
        .collect::<Result<_, _>>()?;
    let event_types: Vec<Json> = event_values
        .iter()
        .filter_map(|event| event.get("type").cloned())
        .collect();

    Ok(exact_json!({
        "scenario": SCENARIO,
        "eventTypes": (event_types),
        "events": (event_values),
        "persistedMessages": (persisted_values),
    }))
}

fn query_params(
    process_context: &RuntimeProcessContext,
    tools: Vec<Arc<dyn Tool>>,
    persisted_messages: Arc<Mutex<Vec<Message>>>,
) -> QueryParams {
    let config = crate::schemas::config::default_config();
    let specs = to_tool_specs(&tools);
    QueryParams {
        retained_tool_tasks: None,
        multi_agent_instructions: false,
        thread_id: "thread-1".into(),
        turn_id: "turn-1".into(),
        model: "fake-model".into(),
        provider_info: None,
        messages: vec![user_message("please echo")],
        compaction_protected_message_ids: std::collections::HashSet::new(),
        context_window: None,
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
            text: RuntimeString::from("you are a test"),
            cacheable: None,
            cache_scope: None,
        }]),
        config: config.clone(),
        max_turns: None,
        cost_so_far: None,
        tool_context_base: {
            let process_context = process_context.clone();
            Arc::new(move || make_tool_context(&process_context, "/tmp/klaus-query-trace"))
        },
        permission_engine: Arc::new(StaticPermissionEngine::new(&config)),
        hook_bus: None,
        signal: AbortSignal::new(),
        approval_handler: None,
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
        on_permission_update: None,
        message_rollout: None,
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
        tool_result_budget_state: None,
        on_tool_result_budget_records: None,
        thinking_context_cold: false,
        padded_countdown_spend: Default::default(),
        task_countdown_anchor: None,
        live_model_override: None,
    }
}

fn deps() -> EngineDeps {
    EngineDeps {
        provider_id: RuntimeString::from("test-provider"),
        call_model: scripted_call_model(scripted_turns()),
        estimate_tokens: Arc::new(|_| 0),
        compactor: noop_compactor(),
        uuid: seq_uuid(),
        now: Arc::new(|| 0),
    }
}

fn scripted_turns() -> ScriptedTurns {
    let mut first = vec![ev::message_start(None), ev::text("I will call echo.")];
    first.extend(exact_tool_use(
        "tu_echo",
        "echo",
        &exact_json!({ "value": "alpha" }),
    ));
    first.push(ev::stop(
        Some(ModelStopReason::ToolUse),
        Some(make_usage(Some(11), Some(2))),
    ));
    vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("Echo says alpha."),
            ev::stop(
                Some(ModelStopReason::EndTurn),
                Some(make_usage(Some(7), Some(4))),
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

fn echo_schema() -> JsonSchema {
    let mut value = JsonSchema::new();
    value.insert("type".to_owned(), Value::String("string".to_owned()));
    let mut properties = JsonSchema::new();
    properties.insert("value".to_owned(), Value::Object(value));
    let mut schema = JsonSchema::new();
    schema.insert("type".to_owned(), Value::String("object".to_owned()));
    schema.insert("properties".to_owned(), Value::Object(properties));
    schema.insert(
        "required".to_owned(),
        Value::Array(vec![Value::String("value".to_owned())]),
    );
    schema.insert("additionalProperties".to_owned(), Value::Bool(false));
    schema
}

struct EchoTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for EchoTool {
    fn static_name(&self) -> &str {
        "echo"
    }

    fn static_description(&self) -> &str {
        "echo tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
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
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let value = input
            .get("value")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        Ok(text_result(
            exact_json!({ "echo": (value.clone()) }),
            RuntimeString::from("echo-result: ").concat(&value),
            false,
        ))
    }
}

fn user_message(text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: RuntimeString::from("u0"),
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

fn seq_uuid() -> Arc<dyn Fn() -> String + Send + Sync> {
    let counter = Arc::new(AtomicUsize::new(0));
    Arc::new(move || format!("id-{}", counter.fetch_add(1, Ordering::SeqCst)))
}

fn make_tool_context(process_context: &RuntimeProcessContext, cwd: &str) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process_context.clone(),
        cwd: RuntimePath::from(cwd),
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
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
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

    #[test]
    fn exact_event_and_message_codecs_preserve_lone_surrogates() {
        let strings = [
            RuntimeString::from_utf16([0xd800]),
            RuntimeString::from_utf16([0xdc00]),
            RuntimeString::from_utf16([0xfffd]),
        ];
        let wires = strings
            .iter()
            .map(|text| {
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
                    "event": (runtime_codec::encode_value(&event).unwrap()),
                    "message": (runtime_codec::encode_value(&message).unwrap()),
                });
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
