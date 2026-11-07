//! Defect: completed ordinary and exploration children could not accept follow-up
//! work across parent turns or a restart after harmless runtime catalog refreshes.
//! Root cause: recovery required whole skill-catalog, tool-schema, provider-info,
//! and role-definition digests to match instead of restoring the child's scope.
//! Work item: 20261024 subagent message recovery and runtime refresh.

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor, NoopCompactor};
use klaus_agent_core::engine::session::{
    InferenceTarget, InferenceTargetState, Input, Session, TurnOptions,
};
use klaus_agent_core::engine::BUILTIN_AGENTS;
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::swarm_tools::create_send_message_tool;
use klaus_agent_core::tools::builtins::task::{
    create_task_tool, AgentDefinition as TaskAgentDefinition, TaskToolOptions,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    CallModel, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskStatus, SubagentMessageRequest, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ROOT: &str = "recovery-refresh-parent";
const ORIGINAL: &str = "original-child-assignment-20261024";
const ANSWER: &str = "original-child-answer-20261024";
const QUEUED: &str = "queued-child-context-20261024";
const WARM: &str = "warm-followup-20261024";
const COLD: &str = "cold-followup-20261024";
const NEXT: &str = "next-followup-20261024";
const ROLE_REFRESH: &str = "refreshed-role-direction-20261024";
const BASE_ORIGINAL: &str = "original-base-instructions-20261024";
const BASE_REFRESH: &str = "refreshed-parent-base-instructions-20261024";

type ParentTool = Option<(&'static str, &'static str, Value)>;

struct ParentSummary;

#[async_trait]
impl Compactor for ParentSummary {
    fn fork_isolated(
        &self,
        _: CallModel,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self))
    }

    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self))
    }

    fn reactive_enabled(&self) -> bool {
        false
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        tokens: u64,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        NoopCompactor
            .maybe_compact(messages, tokens, config, instructions)
            .await
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        NoopCompactor
            .reactive_compact(messages, config, instructions)
            .await
    }

    async fn manual_compact(
        &self,
        _: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "parent-summary-20261024".into(),
                summary: "Continue the parent work using retained worker identities.".into(),
                native_compaction: None,
            })],
            freed_tokens: 1_000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

struct RecordingProvider {
    info: ProviderInfo,
    child_role_part: RuntimeString,
    parent_tool: Mutex<ParentTool>,
    parent_requests: Mutex<Vec<ModelRequest>>,
    child_requests: Mutex<Vec<ModelRequest>>,
}

fn user_count(messages: &[Message], needle: &str) -> usize {
    messages
        .iter()
        .map(|message| match message {
            Message::User(message) => message
                .content
                .iter()
                .filter(
                    |block| matches!(block, ContentBlock::Text(text) if text.text.contains(needle)),
                )
                .count(),
            _ => 0,
        })
        .sum()
}

fn tool_error(request: &ModelRequest, id: &str) -> Option<bool> {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result.is_error),
            Message::User(message) => message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult(result) if result.tool_use_id == id => {
                    Some(result.is_error == Some(true))
                }
                _ => None,
            }),
            _ => None,
        })
}

fn stopped(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 20,
            output_tokens: 5,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn text_events(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: "recovery-test".into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stopped(ModelStopReason::EndTurn),
    ]
}

fn tool_events(id: &str, name: &str, input: Value) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: "recovery-test".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stopped(ModelStopReason::ToolUse),
    ]
}

impl RecordingProvider {
    fn new(refreshed: bool, role: &str) -> Arc<Self> {
        Arc::new(Self {
            child_role_part: format!("agent:{role}").into(),
            info: ProviderInfo {
                name: "recovery-test".into(),
                // This is descriptive metadata on a fake provider, never a network request.
                base_url: Some(if refreshed {
                    "https://refreshed.invalid/api".into()
                } else {
                    "https://original.invalid/api".into()
                }),
                env_key: None,
                default_model: "recovery-test".into(),
            },
            parent_tool: Mutex::new(None),
            parent_requests: Mutex::new(Vec::new()),
            child_requests: Mutex::new(Vec::new()),
        })
    }
}

impl ModelProvider for RecordingProvider {
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
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let events = if request
            .system
            .iter()
            .any(|part| part.name == self.child_role_part)
        {
            assert_eq!(
                user_count(&request.messages, ORIGINAL),
                1,
                "the identified child must retain its assignment history"
            );
            let marker = [NEXT, COLD, WARM]
                .into_iter()
                .find(|marker| user_count(&request.messages, marker) > 0);
            let events = if let Some(marker) = marker {
                let read_id = format!("read-{marker}");
                let write_id = format!("write-{marker}");
                if tool_error(&request, &read_id).is_none() {
                    tool_events(&read_id, "read", json!({"query":"fixture"}))
                } else if tool_error(&request, &write_id).is_none() {
                    // Try the excluded tool as well: a role restriction must gate dispatch.
                    tool_events(&write_id, "write", json!({"query":"fixture"}))
                } else {
                    text_events(&format!("completed-{marker}"))
                }
            } else {
                text_events(ANSWER)
            };
            self.child_requests.lock().unwrap().push(request);
            events
        } else {
            self.parent_requests.lock().unwrap().push(request);
            match self.parent_tool.lock().unwrap().take() {
                Some((id, name, input)) => tool_events(id, name, input),
                None => text_events("Parent turn complete."),
            }
        };
        Box::pin(futures::stream::iter(events))
    }
}

struct Probe {
    name: &'static str,
    schema: JsonSchema,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "Record a fixture tool dispatch."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some(format!("fixture-tool:{}", self.name).into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        self.name == "read"
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(input, "Fixture dispatched.", false))
    }
}

fn options(
    process: RuntimeProcessContext,
    root: &Path,
    provider: Arc<RecordingProvider>,
    role: &str,
    refreshed: bool,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
) -> AgentOptions {
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.provider = provider;
    options.compactor = Some(Arc::new(ParentSummary));
    let base = if refreshed {
        BASE_REFRESH
    } else {
        BASE_ORIGINAL
    };
    options.system_prompt_provider = Some(Arc::new(move || {
        Box::pin(async move {
            vec![SystemPromptPart {
                name: "fixture-base".into(),
                text: base.into(),
                cacheable: Some(true),
                cache_scope: None,
            }]
        })
    }));
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({"model":if refreshed { "refreshed-parent-model" } else { "recovery-test" },
            "effort":"low", "thinking":true, "prewarm":false, "swarm":false,
            "autoMemory":false, "agentMemory":false, "memoryRecall":false,
            "goal":false, "forwardSubagentText":false,
            "modelSource":{"source":"custom", "modelId":if refreshed { "refreshed-parent-model" } else { "recovery-test" }},
            "subagentModelCatalog":true,
            "subagentModelOptions":[{
                "value":if refreshed { "custom:refreshed-parent-model" } else { "custom:recovery-test" },
                "label":"Selected recovery fixture model"
            }]})
            .as_object()
            .unwrap()
            .clone(),
        );
    let schema = json!({"type":"object", "properties":{
        "query":{"type":"string"},
        "catalogRevision":{"type":"string", "description":if refreshed { "revision two" } else { "revision one" }}
    }, "required":["query"]}).as_object().unwrap().clone();
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions {
            model_catalog: true,
            model_options: vec![klaus_agent_core::schemas::config::SubagentModelOption {
                value: if refreshed {
                    "custom:refreshed-parent-model"
                } else {
                    "custom:recovery-test"
                }
                .into(),
                label: "Selected recovery fixture model".into(),
            }],
            agents: vec![TaskAgentDefinition {
                name: role.into(),
                description: "Fixture worker role.".into(),
                ..Default::default()
            }],
        }),
        create_send_message_tool(),
        Arc::new(Probe {
            name: "read",
            schema: schema.clone(),
            calls: reads,
        }),
        Arc::new(Probe {
            name: "write",
            schema,
            calls: writes,
        }),
    ]);
    let skill_name = if refreshed {
        "refreshed-fixture-skill"
    } else {
        "original-fixture-skill"
    };
    let skill_path = root
        .join(skill_name)
        .join("SKILL.md")
        .to_string_lossy()
        .into_owned();
    options.skills = Some(Arc::new(move || {
        vec![Skill {
            name: skill_name.into(),
            description: "Fixture catalog entry.".into(),
            body: "Read fixture evidence.".into(),
            path: skill_path.clone().into(),
            ..Default::default()
        }]
    }));
    let skills = options.skills.clone().unwrap();
    options.tools.as_mut().unwrap().push(
        klaus_agent_core::extensions::skills::skill_tool::create_skill_tool(move || skills()),
    );
    let mut definition = BUILTIN_AGENTS
        .iter()
        .find(|entry| entry.name == role)
        .unwrap()
        .clone();
    if refreshed {
        // Reload current role instructions and effort while keeping its tool restrictions.
        definition.description = "Updated role catalog description.".into();
        definition.prompt = Some(
            definition
                .prompt
                .as_ref()
                .unwrap()
                .concat(&format!("\n{ROLE_REFRESH}").into()),
        );
        definition.effort = Some(EffortValue::Level(EffortLevel::High));
        definition.model = Some("refreshed-role-model".into());
    }
    // An explicit tool pool requires explicit role registration as well.
    options.agents = Some(vec![definition]);
    options.include_default_agents = Some(false);
    options
}

fn session_options(root: &Path) -> StartSessionOptions {
    StartSessionOptions {
        id: Some(ROOT.into()),
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    }
}

fn store(root: &Path) -> Arc<JsonlSessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}

fn completions(session: &Arc<Session>) -> tokio::sync::mpsc::UnboundedReceiver<RuntimeString> {
    let (send, receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |notice| {
        if notice.status == "completed" {
            if let Some(id) = &notice.agent_id {
                let _ = send.send(id.clone());
            }
        }
    })));
    receive
}

async fn completed(
    receive: &mut tokio::sync::mpsc::UnboundedReceiver<RuntimeString>,
) -> RuntimeString {
    tokio::time::timeout(Duration::from_secs(10), receive.recv())
        .await
        .expect("child must complete without a real service")
        .expect("completion sink remains open")
}

async fn parent_turn(session: &Arc<Session>) {
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.run(
            Input::Text("An unrelated parent turn.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("parent turn must settle");
    assert!(result.error.is_none(), "{:?}", result.error);
}

async fn message(session: &Arc<Session>, id: &RuntimeString, text: &str, trigger: bool) {
    // These are the public queue-only and explicit-follow-up routes used by the tools.
    let result = session
        .send_subagent_message(SubagentMessageRequest {
            target: id.clone(),
            message: text.into(),
            summary: "Continue the fixture assignment.".into(),
            trigger_turn: trigger,
            sender_agent_id: None,
            user_initiated: false,
        })
        .await
        .expect("the original child remains addressable")
        .unwrap_or_else(|error| {
            panic!("refresh must not reject original child recovery: {error:?}")
        });
    assert_eq!(result.agent_id, *id, "follow-up must retain child identity");
    assert_eq!(result.resumed, trigger);
}

fn assert_followup(provider: &RecordingProvider, marker: &str, explore: bool) {
    let requests = provider.child_requests.lock().unwrap();
    let requests: Vec<_> = requests
        .iter()
        .filter(|request| {
            user_count(&request.messages, marker) > 0
                && tool_error(request, &format!("read-{marker}")).is_some()
        })
        .collect();
    let last = requests
        .last()
        .expect("follow-up must actually reach the provider after dispatch");
    assert_eq!(tool_error(last, &format!("read-{marker}")), Some(false));
    assert_eq!(tool_error(last, &format!("write-{marker}")), Some(explore));
    assert_eq!(user_count(&last.messages, ORIGINAL), 1);
    assert_eq!(user_count(&last.messages, marker), 1);
    assert!(last
        .system
        .iter()
        .any(|part| part.text.contains(BASE_ORIGINAL)));
    assert!(!last
        .system
        .iter()
        .any(|part| part.text.contains(BASE_REFRESH)));
    if marker != WARM {
        assert!(last
            .system
            .iter()
            .any(|part| part.text.contains(ROLE_REFRESH)));
        assert_eq!(last.effort, Some(EffortValue::Level(EffortLevel::High)));
        let has_current_skill = last
            .system
            .iter()
            .any(|part| part.text.contains("refreshed-fixture-skill"))
            || user_count(&last.messages, "refreshed-fixture-skill") > 0;
        assert!(
            has_current_skill,
            "a new child turn must load the current skill catalog"
        );
    }
    assert!(last.messages.iter().any(|message| matches!(message,
        Message::Assistant(message) if message.content.iter().any(|block|
            matches!(block, ContentBlock::Text(text) if text.text == ANSWER)))));
    assert!(last.tools.iter().any(|tool| tool.name == "read"));
    assert_eq!(last.tools.iter().any(|tool| tool.name == "write"), !explore);
    if explore {
        assert!(!last.tools.iter().any(|tool| tool.name == "task"));
        assert!(last
            .system
            .iter()
            .any(|part| part.text.contains("read-only exploration sub-agent")));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_ordinary_child_recovers_after_runtime_catalog_refresh() {
    recovery_after_refresh("general-purpose").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_explore_child_recovers_after_runtime_catalog_refresh() {
    recovery_after_refresh("Explore").await;
}

fn install_model_resolver(session: &Arc<Session>, provider: Arc<RecordingProvider>) {
    session.install_subagent_target_resolver(Arc::new(move |source, model_id| {
        assert_eq!(source, "custom");
        assert_eq!(model_id, "recovery-test");
        let provider = provider.clone();
        Box::pin(async move {
            Ok(InferenceTargetState::Ready(InferenceTarget {
                source,
                model_id,
                model: "recovery-test".into(),
                provider,
                pricing: Vec::new(),
                review_requirements: None,
            }))
        })
    }));
}

async fn recovery_after_refresh(role: &str) {
    let root = tempfile::tempdir().unwrap();
    let supervisor = super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor();
    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let warm_provider = RecordingProvider::new(false, role);
    *warm_provider.parent_tool.lock().unwrap() = Some((
        "spawn-child",
        "task",
        json!({
            "description":"Recoverable fixture child", "prompt":ORIGINAL, "model":"custom:recovery-test",
            "subagent_type":role, "fork_turns":"none", "run_in_background":true
        }),
    ));
    let agent = Agent::new(options(
        process.clone(),
        root.path(),
        warm_provider.clone(),
        role,
        false,
        reads.clone(),
        writes.clone(),
    ));
    let session = agent
        .open_session(store(root.path()), session_options(root.path()))
        .await
        .unwrap();
    install_model_resolver(&session, warm_provider.clone());
    let mut notices = completions(&session);
    parent_turn(&session).await;
    assert!(
        warm_provider
            .parent_requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| tool_error(request, "spawn-child") == Some(false)),
        "the source-bound directory choice must start the original child"
    );
    let id = completed(&mut notices).await;
    {
        let requests = warm_provider.child_requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "the initial child makes one model request"
        );
        let initial = &requests[0];
        assert!(initial
            .system
            .iter()
            .any(|part| part.name == warm_provider.child_role_part));
        assert_eq!(
            initial.tools.iter().any(|tool| tool.name == "write"),
            role != "Explore"
        );
        assert_eq!(
            initial.tools.iter().any(|tool| tool.name == "task"),
            role != "Explore"
        );
    }
    assert!(warm_provider
        .parent_requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| tool_error(request, "spawn-child") == Some(false)));
    parent_turn(&session).await;
    assert!(session.compact(None).await.unwrap().compacted);
    message(&session, &id, WARM, true).await;
    assert_eq!(completed(&mut notices).await, id);
    assert_followup(&warm_provider, WARM, role == "Explore");
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    drop(session);
    drop(agent);

    // A fresh process context, provider, Agent and store retain only persisted state.
    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let cold_provider = RecordingProvider::new(true, role);
    assert_ne!(warm_provider.info().base_url, cold_provider.info().base_url);
    let agent = Agent::new(options(
        process.clone(),
        root.path(),
        cold_provider.clone(),
        role,
        true,
        reads.clone(),
        writes.clone(),
    ));
    let session = agent
        .resume_session(store(root.path()), ROOT, session_options(root.path()))
        .await
        .unwrap();
    install_model_resolver(&session, cold_provider.clone());
    message(&session, &id, QUEUED, false).await;
    parent_turn(&session).await;
    assert!(
        cold_provider.child_requests.lock().unwrap().is_empty(),
        "queue-only mail cannot infer"
    );
    let summaries = session.background_agent_summaries();
    assert_eq!(
        summaries
            .iter()
            .find(|summary| summary.id == id)
            .unwrap()
            .status,
        BackgroundTaskStatus::Completed
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    drop(session);
    drop(agent);

    // Reopen once more so queue durability, rather than an in-memory mailbox, is exercised.
    let process = RuntimeProcessContext::initialize(supervisor).unwrap();
    let provider = RecordingProvider::new(true, role);
    let agent = Agent::new(options(
        process.clone(),
        root.path(),
        provider.clone(),
        role,
        true,
        reads.clone(),
        writes.clone(),
    ));
    let disk = store(root.path());
    let session = agent
        .resume_session(disk.clone(), ROOT, session_options(root.path()))
        .await
        .unwrap();
    install_model_resolver(&session, provider.clone());
    let mut notices = completions(&session);
    parent_turn(&session).await;
    assert!(provider.child_requests.lock().unwrap().is_empty());
    message(&session, &id, COLD, true).await;
    assert_eq!(completed(&mut notices).await, id);
    assert_followup(&provider, COLD, role == "Explore");
    {
        let requests = provider.child_requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            3,
            "one explicit trigger starts exactly one tool loop"
        );
        assert_eq!(user_count(&requests[0].messages, QUEUED), 1);
        assert_eq!(
            requests[0].model,
            warm_provider.child_requests.lock().unwrap()[0].model
        );
    }
    parent_turn(&session).await;
    message(&session, &id, NEXT, true).await;
    assert_eq!(completed(&mut notices).await, id);
    assert_followup(&provider, NEXT, role == "Explore");
    assert_eq!(provider.child_requests.lock().unwrap().len(), 6);
    assert_eq!(reads.load(Ordering::SeqCst), 3);
    assert_eq!(
        writes.load(Ordering::SeqCst),
        if role == "Explore" { 0 } else { 3 }
    );
    assert_eq!(session.subagent_lifecycle_snapshot().agents.len(), 1);

    // Unknown targets still go through the tool's explicit error path.
    *provider.parent_tool.lock().unwrap() = Some((
        "unknown-target",
        "SendMessage",
        json!({
            "to":"missing-child-20261024", "message":"More work.", "summary":"Missing recipient.",
            "trigger_turn":true
        }),
    ));
    parent_turn(&session).await;
    assert_eq!(
        tool_error(
            provider.parent_requests.lock().unwrap().last().unwrap(),
            "unknown-target"
        ),
        Some(true)
    );
    assert_eq!(provider.child_requests.lock().unwrap().len(), 6);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let saved = disk
        .read_sub_agent(ROOT, &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let saved_messages = klaus_agent_core::store::projection::project_for_prompt(&saved.raw_items);
    for marker in [ORIGINAL, QUEUED, WARM, COLD, NEXT] {
        assert_eq!(
            user_count(&saved_messages, marker),
            1,
            "mail must persist and be consumed once: {marker}"
        );
    }
    let output =
        std::env::temp_dir().join(format!("klaus-agent-bg-{}.txt", id.try_to_utf8().unwrap()));
    if output.exists() {
        std::fs::remove_file(output).unwrap();
    }
}
