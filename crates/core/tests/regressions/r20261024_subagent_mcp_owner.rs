//! Defect: inherited native MCP calls and questions retained the parent's live connection owner.
//! Root cause: child tool, resource and prompt views reused connection closures instead of their admitted declarations.
//! Work items: 2026-10-24 child-owned MCP runtimes; 2026-10-26 bounded native namespace updates.
//! Namespace defect: full tool schemas were duplicated into system instructions and survived snapshot reload.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::mcp::elicitation::{
    McpElicitationAction, McpElicitationEvent, McpElicitationPresenter, McpElicitationRequest,
    McpElicitationResponse,
};
use klaus_agent_core::mcp::transports::{Transport, TransportEvent};
use klaus_agent_core::mcp::types::{McpInProcessServer, McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::mcp_prompts::{
    create_get_mcp_prompt_tool, create_list_mcp_prompts_tool,
};
use klaus_agent_core::tools::builtins::mcp_resources::{
    create_list_mcp_resources_tool, create_read_mcp_resource_tool,
};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_stop_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskStatus, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

const CHILD: &str = "native-owner-child-directive";
const GRAND: &str = "native-owner-grandchild-directive";
const OPAQUE_INSTRUCTIONS: &str = "# MCP Server Instructions\n\nThe following MCP servers have provided usage instructions:\n\n## owner\nHistorical native initialize guidance.\n\n## owner\nOpaque host usage guidance.\n## Embedded heading is opaque content\nPreserve this mixed legacy block exactly.";
const TIMEOUT: Duration = Duration::from_secs(20);

fn wire(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}
fn scalar(value: &RuntimeJson) -> Value {
    serde_json::from_str(&runtime_json::stringify(value).try_to_utf8().unwrap()).unwrap()
}

#[derive(Debug, Clone)]
enum Evidence {
    Connected(usize),
    Called(usize, String),
    Answer(usize, u64, String, Option<String>),
    Cancelled(usize),
    Resource(usize),
    Prompt(usize),
    Closed(usize),
}

struct NativeServer {
    empty_root_pool: bool,
    connections: AtomicUsize,
    extra_calls: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    events: mpsc::UnboundedSender<Evidence>,
}

async fn question(transport: &dyn Transport, label: &str, phase: u64) {
    transport.send(wire(json!({"jsonrpc":"2.0","id":40+phase,"method":"elicitation/create","params":{
        "message":format!("{label}:{phase}"),
        "requestedSchema":{"type":"object","properties":{"tag":{"type":"string"}},"required":["tag"]}
    }}))).await.unwrap();
}

#[async_trait]
impl McpInProcessServer for NativeServer {
    async fn connect(&self, transport: Box<dyn Transport>) {
        let connection = self.connections.fetch_add(1, Ordering::SeqCst);
        self.events.send(Evidence::Connected(connection)).unwrap();
        let (send, mut receive) = mpsc::unbounded_channel();
        transport.start(send).await.unwrap();
        let transport: Arc<dyn Transport> = Arc::from(transport);
        let events = self.events.clone();
        let empty = self.empty_root_pool;
        let extra_calls = self.extra_calls.clone();
        let closes = self.closes.clone();
        // The server endpoint owns its connection state; request IDs deliberately repeat across endpoints.
        tokio::spawn(async move {
            let mut pending: Option<(Value, String)> = None;
            while let Some(event) = receive.recv().await {
                let frame = match event {
                    TransportEvent::Message(frame) => scalar(&frame),
                    TransportEvent::Close => break,
                    TransportEvent::Error(error) => panic!("native transport error: {error}"),
                };
                let id = frame.get("id").cloned().unwrap_or(Value::Null);
                let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
                let result = match method {
                    "initialize" => Some(
                        json!({"protocolVersion":frame["params"]["protocolVersion"],"capabilities":{"tools":{},"resources":{},"prompts":{}},"serverInfo":{"name":"owner","version":"1"},"instructions":format!("Native initialize connection {connection}.")}),
                    ),
                    "tools/list" => {
                        let mut tools = Vec::new();
                        if !empty || connection > 0 {
                            tools.push(json!({"name":"hold","description":"Native owner gate","inputSchema":{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]},"annotations":{"readOnlyHint":true}}));
                        }
                        if connection > 0 {
                            tools.push(json!({"name":"surprise","description":"A newly discovered tool","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}}));
                        }
                        transport
                            .send(wire(
                                json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools}}),
                            ))
                            .await
                            .unwrap();
                        if connection == 1 && !empty {
                            question(transport.as_ref(), "startup", 0).await;
                        }
                        None
                    }
                    "resources/list" => {
                        let mut resources = vec![json!({"uri":"owner://common","name":"Common"})];
                        if connection > 0 {
                            resources.push(json!({"uri":"owner://surprise","name":"New"}));
                        }
                        Some(json!({"resources":resources}))
                    }
                    "resources/templates/list" => Some(json!({"resourceTemplates":[]})),
                    "prompts/list" => {
                        let mut prompts =
                            vec![json!({"name":"common","description":"Common prompt"})];
                        if connection > 0 {
                            prompts.push(json!({"name":"surprise","description":"New prompt"}));
                        }
                        Some(json!({"prompts":prompts}))
                    }
                    "resources/read" => {
                        events.send(Evidence::Resource(connection)).unwrap();
                        Some(
                            json!({"contents":[{"uri":"owner://common","mimeType":"text/plain","text":format!("resource:{connection}")}]}),
                        )
                    }
                    "prompts/get" => {
                        events.send(Evidence::Prompt(connection)).unwrap();
                        Some(
                            json!({"messages":[{"role":"user","content":{"type":"text","text":format!("prompt:{connection}")}}]}),
                        )
                    }
                    "tools/call" => {
                        let name = frame["params"]["name"].as_str().unwrap();
                        if name != "hold" {
                            extra_calls.fetch_add(1, Ordering::SeqCst);
                        }
                        let label = frame["params"]["arguments"]["label"]
                            .as_str()
                            .unwrap_or("surprise")
                            .to_string();
                        events
                            .send(Evidence::Called(connection, label.clone()))
                            .unwrap();
                        if name == "hold" {
                            assert!(pending.is_none(), "one owner gate per live connection");
                            pending = Some((id.clone(), label.clone()));
                            question(transport.as_ref(), &label, 1).await;
                            None
                        } else {
                            Some(
                                json!({"content":[{"type":"text","text":"unexpected native executor"}],"isError":false}),
                            )
                        }
                    }
                    "notifications/cancelled" => {
                        events.send(Evidence::Cancelled(connection)).unwrap();
                        None
                    }
                    "" if frame.get("result").is_some() => {
                        let server_id = id.as_u64().unwrap();
                        let action = frame["result"]["action"].as_str().unwrap().to_string();
                        let tag = frame["result"]["content"]["tag"]
                            .as_str()
                            .map(str::to_string);
                        events
                            .send(Evidence::Answer(connection, server_id, action.clone(), tag))
                            .unwrap();
                        if server_id == 41 && action == "accept" {
                            question(transport.as_ref(), &pending.as_ref().unwrap().1, 2).await;
                        } else if server_id != 40 {
                            let (call_id, label) = pending.take().unwrap();
                            transport.send(wire(json!({"jsonrpc":"2.0","id":call_id,"result":{"content":[{"type":"text","text":format!("native:{label}:{connection}")}],"isError":action!="accept"}}))).await.unwrap();
                        }
                        None
                    }
                    _ => None,
                };
                if let Some(result) = result {
                    transport
                        .send(wire(json!({"jsonrpc":"2.0","id":id,"result":result})))
                        .await
                        .unwrap();
                }
            }
            closes.fetch_add(1, Ordering::SeqCst);
            events.send(Evidence::Closed(connection)).unwrap();
        });
    }
}

struct Presenter {
    presented: mpsc::UnboundedSender<McpElicitationEvent>,
    cancelled: mpsc::UnboundedSender<RuntimeString>,
}
#[async_trait]
impl McpElicitationPresenter for Presenter {
    async fn present(&self, event: McpElicitationEvent) {
        self.presented.send(event).unwrap();
    }
    async fn cancelled(&self, id: RuntimeString) {
        self.cancelled.send(id).unwrap();
    }
}

struct OpaqueTool(Arc<AtomicUsize>);
#[async_trait]
impl Tool for OpaqueTool {
    fn static_name(&self) -> &str {
        "mcp__opaque__ping"
    }
    fn static_description(&self) -> &str {
        "Opaque host callback"
    }
    fn input_schema(&self) -> RuntimeJson {
        wire(json!({"type":"object"}))
    }
    fn static_mcp_server(&self) -> Option<&str> {
        Some("owner")
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        true
    }
    async fn call(
        &self,
        _: RuntimeJson,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            wire(json!({"opaque":true})),
            "opaque-callback",
            false,
        ))
    }
}

fn stopped(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}
fn call(id: &str, name: &str, input: Value) -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "owner-model".into(),
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
    ]))
}
fn finish() -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "owner-model".into(),
        },
        ModelStreamEvent::TextDelta {
            text: "owner checks complete".into(),
        },
        stopped(ModelStopReason::EndTurn),
    ]))
}
fn result<'a>(request: &'a ModelRequest, id: &str) -> &'a ToolResultMessage {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result),
            _ => None,
        })
        .expect("actual tool result")
}
fn text(result: &ToolResultMessage) -> String {
    result
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn unknown(request: &ModelRequest, id: &str, name: &str) {
    let result = result(request, id);
    assert!(result.is_error);
    assert!(
        text(result).contains(&format!("Unknown tool: {name}")),
        "{:?}",
        result.content
    );
}
fn has(request: &ModelRequest, directive: &str) -> bool {
    let forked = klaus_agent_core::engine::fork::build_fork_boilerplate(directive);
    request.messages.iter().any(|message| match message {
        Message::User(user) => user
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text.text == directive || text.text == forked.as_str())),
        _ => false,
    })
}

fn assert_instruction_owner(request: &ModelRequest, connection: usize, empty: bool) {
    assert!(request
        .system
        .iter()
        .any(|part| part.name == "mcp-instructions" && part.text == OPAQUE_INSTRUCTIONS));
    if connection != 0 {
        let update = request.system.iter().rev().find(|part| part.name == "mcp-runtime-namespace-update")
            .unwrap_or_else(|| panic!("expected current native namespace update for connection {connection}; system={:?}", request.system));
        let update = update.text.try_to_utf8().unwrap();
        let (scope, payload) = update
            .split_once("\n\n")
            .expect("scoped native metadata payload");
        assert!(scope.starts_with("Updated instructions for namespace \"owner\":\n"));
        assert!(
            scope.contains("only for the listed native bindings of this current MCP declaration")
        );
        assert!(scope.contains("does not replace base instructions, agent-role instructions, permission constraints, or opaque/host-injected tools and guidance"));
        let metadata: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(metadata["namespace"], json!("owner"));
        assert_eq!(
            metadata["instructions"],
            json!(format!("Native initialize connection {connection}.")),
            "expected native connection {connection}; metadata={metadata}"
        );
        let tools = metadata["tools"].as_array().unwrap();
        assert_eq!(tools.len(), usize::from(!empty));
        if !empty {
            assert_eq!(tools[0]["name"], json!("mcp__owner__hold"));
            assert_eq!(tools[0]["nativeName"], json!("hold"));
            assert!(tools[0].get("description").is_none());
            assert!(tools[0].get("inputSchema").is_none());
            let tool = request
                .tools
                .iter()
                .find(|tool| tool.name == "mcp__owner__hold")
                .unwrap();
            assert_eq!(tool.description, "Native owner gate");
            assert_eq!(scalar(&tool.input_schema)["required"], json!(["label"]));
        }
        assert_eq!(metadata["resources"], json!(["owner://common"]));
        assert_eq!(metadata["resourceTemplates"], json!([]));
        assert_eq!(metadata["prompts"], json!(["common"]));
        assert!(!payload.contains("mcp__opaque__ping"));
        assert!(!payload.contains("surprise"));
        assert!(!payload.contains("Opaque host usage guidance"));
        assert!(!payload.contains("inputSchema"));
        assert!(
            update.len() < 2_048,
            "initialize guidance must not include full catalogs"
        );
        assert_eq!(
            request
                .system
                .iter()
                .filter(|part| part.name == "mcp-runtime-namespace-update")
                .count(),
            1
        );
        return;
    }
    let native = request
        .system
        .iter()
        .filter(|part| {
            part.name == "plugin-mcp-instructions" || part.name == "child-mcp-instructions"
        })
        .map(|part| part.text.try_to_utf8().unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        native.contains(&format!("Native initialize connection {connection}.")),
        "expected native connection {connection}; native={native}"
    );
    for other in 0..3 {
        if other != connection {
            assert!(
                !native.contains(&format!("Native initialize connection {other}.")),
                    "expected native connection {connection}, unexpected connection {other}; native={native}"
            );
        }
    }
}

struct Provider {
    info: ProviderInfo,
    empty: bool,
    grand_fork: &'static str,
    root: AtomicUsize,
    child: AtomicUsize,
    grand: AtomicUsize,
    ids: Mutex<BTreeMap<&'static str, RuntimeString>>,
    finished: mpsc::UnboundedSender<&'static str>,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if has(&request, GRAND) {
            assert_instruction_owner(&request, 2, self.empty);
            assert!(!request
                .tools
                .iter()
                .any(|tool| tool.name == "mcp__owner__surprise"));
            match self.grand.fetch_add(1, Ordering::SeqCst) {
                0 => call("grand-extra", "mcp__owner__surprise", json!({})),
                1 => {
                    unknown(&request, "grand-extra", "mcp__owner__surprise");
                    call("grand-opaque", "mcp__opaque__ping", json!({}))
                }
                2 => {
                    assert!(!result(&request, "grand-opaque").is_error);
                    call("grand-hold", "mcp__owner__hold", json!({"label":"grand"}))
                }
                3 => {
                    let result = result(&request, "grand-hold");
                    assert!(!result.is_error, "{:?}", result.content);
                    assert!(
                        text(result).contains("native:grand:2"),
                        "{:?}",
                        result.content
                    );
                    self.finished.send("grand").unwrap();
                    finish()
                }
                _ => panic!("unexpected grandchild request"),
            }
        } else if has(&request, CHILD) {
            assert_instruction_owner(&request, 1, self.empty);
            assert!(!request
                .tools
                .iter()
                .any(|tool| tool.name == "mcp__owner__surprise"));
            if self.empty {
                assert!(!request
                    .tools
                    .iter()
                    .any(|tool| tool.name == "mcp__owner__hold"));
            }
            match self.child.fetch_add(1, Ordering::SeqCst) {
                0 => call("child-extra", "mcp__owner__surprise", json!({})),
                1 => {
                    unknown(&request, "child-extra", "mcp__owner__surprise");
                    call("child-opaque", "mcp__opaque__ping", json!({}))
                }
                2 => {
                    let result = result(&request, "child-opaque");
                    assert!(!result.is_error);
                    assert_eq!(
                        result
                            .output
                            .as_ref()
                            .unwrap()
                            .get("opaque")
                            .and_then(RuntimeJson::as_bool),
                        Some(true)
                    );
                    call(
                        "child-resources",
                        "list_mcp_resources",
                        json!({"server":"owner"}),
                    )
                }
                3 => {
                    let result = result(&request, "child-resources");
                    assert!(!result.is_error);
                    let values = result
                        .output
                        .as_ref()
                        .unwrap()
                        .get("resources")
                        .unwrap()
                        .as_array()
                        .unwrap();
                    assert_eq!(values.len(), 1);
                    assert_eq!(
                        values[0].get("uri").and_then(RuntimeJson::as_str),
                        Some(&RuntimeString::from("owner://common"))
                    );
                    call(
                        "child-resource",
                        "read_mcp_resource",
                        json!({"server":"owner","uri":"owner://common"}),
                    )
                }
                4 => {
                    let result = result(&request, "child-resource");
                    assert!(!result.is_error);
                    assert!(text(result).contains("resource:1"), "{:?}", result.content);
                    call(
                        "child-prompts",
                        "list_mcp_prompts",
                        json!({"server":"owner"}),
                    )
                }
                5 => {
                    let result = result(&request, "child-prompts");
                    assert!(!result.is_error);
                    let values = result
                        .output
                        .as_ref()
                        .unwrap()
                        .get("prompts")
                        .unwrap()
                        .as_array()
                        .unwrap();
                    assert_eq!(values.len(), 1);
                    assert_eq!(
                        values[0].get("name").and_then(RuntimeJson::as_str),
                        Some(&RuntimeString::from("common"))
                    );
                    call(
                        "child-prompt",
                        "get_mcp_prompt",
                        json!({"server":"owner","name":"common"}),
                    )
                }
                6 => {
                    let result = result(&request, "child-prompt");
                    assert!(!result.is_error);
                    assert!(text(result).contains("prompt:1"), "{:?}", result.content);
                    if self.empty {
                        call(
                            "child-empty-hold",
                            "mcp__owner__hold",
                            json!({"label":"child"}),
                        )
                    } else {
                        call(
                            "spawn-grand",
                            "task",
                            json!({"model": "owner-model", "description":"Grandchild owner check","prompt":GRAND,"subagent_type":"owner-worker","fork_turns":self.grand_fork,"run_in_background":true}),
                        )
                    }
                }
                7 => {
                    if self.empty {
                        unknown(&request, "child-empty-hold", "mcp__owner__hold");
                        self.finished.send("empty").unwrap();
                        finish()
                    } else {
                        let result = result(&request, "spawn-grand");
                        assert!(!result.is_error);
                        let id = result
                            .output
                            .as_ref()
                            .unwrap()
                            .get("agentId")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .clone();
                        self.ids.lock().unwrap().insert("grand", id);
                        call("child-hold", "mcp__owner__hold", json!({"label":"child"}))
                    }
                }
                _ => panic!("the targeted child must be interrupted before another model request"),
            }
        } else {
            assert_instruction_owner(&request, 0, self.empty);
            match self.root.fetch_add(1, Ordering::SeqCst) {
                0 => call(
                    "spawn-child",
                    "task",
                    json!({"model": "owner-model", "description":"Child owner check","prompt":CHILD,"subagent_type":"owner-worker","fork_turns":"all","run_in_background":true}),
                ),
                1 => {
                    let result = result(&request, "spawn-child");
                    assert!(!result.is_error);
                    let id = result
                        .output
                        .as_ref()
                        .unwrap()
                        .get("agentId")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .clone();
                    self.ids.lock().unwrap().insert("child", id);
                    if self.empty {
                        finish()
                    } else {
                        call("root-hold", "mcp__owner__hold", json!({"label":"root"}))
                    }
                }
                2 => {
                    let id = self.ids.lock().unwrap()["child"].try_to_utf8().unwrap();
                    call("stop-child", "task_stop", json!({"task_id":id}))
                }
                _ => {
                    let result = result(&request, "stop-child");
                    assert!(!result.is_error);
                    assert_eq!(
                        result
                            .output
                            .as_ref()
                            .unwrap()
                            .get("task_id")
                            .and_then(RuntimeJson::as_str),
                        self.ids.lock().unwrap().get("child")
                    );
                    finish()
                }
            }
        }
    }
}

fn message(event: &McpElicitationEvent) -> String {
    match &event.request {
        McpElicitationRequest::Form { message, .. } => message.try_to_utf8().unwrap(),
        _ => panic!("expected a form"),
    }
}
fn accept(
    session: &klaus_agent_core::engine::session::Session,
    event: &McpElicitationEvent,
    tag: &str,
) {
    session
        .resolve_mcp_elicitation(
            &event.request_id.try_to_utf8().unwrap(),
            McpElicitationResponse {
                action: McpElicitationAction::Accept,
                content: Some(wire(json!({"tag":tag}))),
                meta: None,
            },
        )
        .unwrap();
}
async fn gather(
    events: &mut mpsc::UnboundedReceiver<Evidence>,
    seen: &mut Vec<Evidence>,
    predicate: impl Fn(&[Evidence]) -> bool,
) {
    tokio::time::timeout(TIMEOUT, async {
        while !predicate(seen) {
            seen.push(events.recv().await.expect("transport evidence channel"));
        }
    })
    .await
    .expect("native transport evidence arrives");
}

#[tokio::test]
async fn native_child_connections_keep_question_owners_and_admitted_catalogs() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    for (grand_fork, empty) in [("all", false), ("none", false), ("none", true)] {
        let directory =
            std::env::temp_dir().join(format!("klaus-mcp-owner-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let (wire_send, mut wire_receive) = mpsc::unbounded_channel();
        let server = Arc::new(NativeServer {
            empty_root_pool: empty,
            connections: AtomicUsize::new(0),
            extra_calls: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
            events: wire_send,
        });
        let (present_send, mut presentations) = mpsc::unbounded_channel();
        let (cancel_send, mut cancellations) = mpsc::unbounded_channel();
        let (done_send, mut finished) = mpsc::unbounded_channel();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "owner-model".into(),
                default_model: "owner-model".into(),
                base_url: None,
                env_key: None,
            },
            empty,
            grand_fork,
            root: AtomicUsize::new(0),
            child: AtomicUsize::new(0),
            grand: AtomicUsize::new(0),
            ids: Mutex::new(BTreeMap::new()),
            finished: done_send,
        });
        let opaque = Arc::new(AtomicUsize::new(0));
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = provider.clone();
        options.config.as_mut().unwrap()["approval_policy"] = json!("on-request");
        options.config.as_mut().unwrap()["prewarm"] = json!(false);
        options.config.as_mut().unwrap()["allow"] = json!(["task", "task_stop"]);
        options.tools = Some(vec![
            create_task_tool(TaskToolOptions::default()),
            create_task_stop_tool(),
            create_list_mcp_resources_tool(),
            create_read_mcp_resource_tool(),
            create_list_mcp_prompts_tool(),
            create_get_mcp_prompt_tool(),
            Arc::new(OpaqueTool(opaque.clone())),
        ]);
        options.include_default_agents = Some(false);
        options.agents = Some(vec![AgentDefinition {
            name: "owner-worker".into(),
            tools: None,
            ..Default::default()
        }]);
        options.system_prompt_provider = Some(Arc::new(|| {
            Box::pin(async {
                vec![klaus_agent_core::types::provider::SystemPromptPart {
                    name: "mcp-instructions".into(),
                    text: OPAQUE_INSTRUCTIONS.into(),
                    cacheable: Some(true),
                    cache_scope: None,
                }]
            })
        }));
        let session = Agent::new(options).start_session(StartSessionOptions {
            cwd: Some(directory.to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
            mcp_server_configs: Some(vec![McpServerConfig::InProcess {
                name: "owner".into(),
                server: server.clone(),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }]),
            mcp_elicitation_presenter: Some(Arc::new(Presenter {
                presented: present_send,
                cancelled: cancel_send,
            })),
            ..Default::default()
        });
        let root = tokio::spawn({
            let session = session.clone();
            async move {
                session
                    .run(
                        Input::Text("Run native owner checks".into()),
                        TurnOptions::default(),
                    )
                    .await
            }
        });
        let mut evidence = Vec::new();
        if empty {
            let result = tokio::time::timeout(TIMEOUT, root).await.unwrap().unwrap();
            assert!(result.error.is_none());
            assert_eq!(result.stop_reason, TerminalReason::Completed);
            assert_eq!(
                tokio::time::timeout(TIMEOUT, finished.recv())
                    .await
                    .unwrap(),
                Some("empty")
            );
            gather(&mut wire_receive, &mut evidence, |seen| {
                seen.iter()
                    .any(|event| matches!(event, Evidence::Resource(1)))
                    && seen
                        .iter()
                        .any(|event| matches!(event, Evidence::Prompt(1)))
            })
            .await;
            assert!(!evidence
                .iter()
                .any(|event| matches!(event, Evidence::Called(_, _))));
            assert!(presentations.try_recv().is_err());
            assert_eq!(opaque.load(Ordering::SeqCst), 1);
        } else {
            let mut first = BTreeMap::new();
            let mut startup = None;
            tokio::time::timeout(TIMEOUT, async {
                while first.len() != 3 || startup.is_none() {
                    let event = presentations.recv().await.unwrap();
                    let name = message(&event);
                    if name == "startup:0" {
                        startup = Some(event);
                    } else {
                        assert!(name.ends_with(":1"));
                        first.insert(name, event);
                    }
                }
            })
            .await
            .expect("root, child and grandchild present independently");
            let startup = startup.unwrap();
            assert_eq!(
                first.keys().cloned().collect::<Vec<_>>(),
                vec!["child:1", "grand:1", "root:1"]
            );
            let public_ids: BTreeSet<_> = first
                .values()
                .map(|event| event.request_id.clone())
                .collect();
            assert_eq!(public_ids.len(), 3);
            for (name, event) in &first {
                accept(&session, event, name);
            }
            let mut second = BTreeMap::new();
            tokio::time::timeout(TIMEOUT, async {
                while second.len() != 3 {
                    let event = presentations.recv().await.unwrap();
                    let name = message(&event);
                    assert!(name.ends_with(":2"));
                    second.insert(name, event);
                }
            })
            .await
            .unwrap();
            gather(&mut wire_receive, &mut evidence, |seen| {
                seen.iter()
                    .filter(|event| matches!(event, Evidence::Called(_, _)))
                    .count()
                    == 3
            })
            .await;
            let calls: BTreeMap<_, _> = evidence
                .iter()
                .filter_map(|event| match event {
                    Evidence::Called(connection, label) => Some((label.clone(), *connection)),
                    _ => None,
                })
                .collect();
            assert_eq!(calls.len(), 3);
            assert_eq!(calls.values().copied().collect::<BTreeSet<_>>().len(), 3);
            let root_connection = calls["root"];
            let child_connection = calls["child"];
            let grand_connection = calls["grand"];
            session.interrupt();
            let result = tokio::time::timeout(TIMEOUT, root).await.unwrap().unwrap();
            assert!(result.error.is_none());
            assert_eq!(result.stop_reason, TerminalReason::Aborted);
            let cancelled = tokio::time::timeout(TIMEOUT, cancellations.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cancelled, second["root:2"].request_id);
            assert!(cancellations.try_recv().is_err());
            assert!(session
                .background_agent_summaries()
                .iter()
                .any(|agent| agent.id
                    == provider.ids.lock().unwrap()["child"].try_to_utf8().unwrap()
                    && agent.status == BackgroundTaskStatus::Running));
            accept(&session, &startup, "startup-survived-root");
            let stopped = tokio::time::timeout(
                TIMEOUT,
                session.run(
                    Input::Text("Stop the selected child".into()),
                    TurnOptions::default(),
                ),
            )
            .await
            .unwrap();
            assert!(stopped.error.is_none());
            let cancelled = tokio::time::timeout(TIMEOUT, cancellations.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cancelled, second["child:2"].request_id);
            assert!(cancellations.try_recv().is_err());
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    let id = provider.ids.lock().unwrap()["child"].try_to_utf8().unwrap();
                    if session.background_agent_summaries().iter().any(|agent| {
                        agent.id == id && agent.status == BackgroundTaskStatus::Interrupted
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the selected child reaches its interrupted terminal state");
            assert!(session
                .background_agent_summaries()
                .iter()
                .any(|agent| agent.id
                    == provider.ids.lock().unwrap()["grand"].try_to_utf8().unwrap()
                    && agent.status == BackgroundTaskStatus::Running));
            accept(&session, &second["grand:2"], "grand-survived-child");
            assert_eq!(
                tokio::time::timeout(TIMEOUT, finished.recv())
                    .await
                    .unwrap(),
                Some("grand")
            );
            gather(&mut wire_receive, &mut evidence, |seen| seen.iter().any(|event| matches!(event, Evidence::Cancelled(connection) if *connection == root_connection)) && seen.iter().any(|event| matches!(event, Evidence::Cancelled(connection) if *connection == child_connection)) && seen.iter().filter(|event| matches!(event, Evidence::Answer(_, 41, action, _) if action == "accept")).count() == 3 && seen.iter().any(|event| matches!(event, Evidence::Answer(connection, 42, action, _) if *connection == grand_connection && action == "accept")) && seen.iter().any(|event| matches!(event, Evidence::Answer(connection, 40, action, Some(tag)) if *connection == child_connection && action == "accept" && tag == "startup-survived-root")) && [root_connection, child_connection].iter().all(|target| seen.iter().any(|event| matches!(event, Evidence::Answer(connection, 42, action, _) if connection == target && action == "cancel")))).await;
            assert!(evidence.iter().any(|event| matches!(event, Evidence::Resource(connection) if *connection == child_connection)));
            assert!(evidence.iter().any(|event| matches!(event, Evidence::Prompt(connection) if *connection == child_connection)));
            assert!(!evidence.iter().any(|event| matches!(event, Evidence::Cancelled(connection) if *connection == grand_connection)));
            for (label, connection) in &calls {
                assert!(evidence.iter().any(|event| matches!(event, Evidence::Answer(id, 41, action, Some(tag)) if id == connection && action == "accept" && tag == &format!("{label}:1"))));
            }
            assert_eq!(opaque.load(Ordering::SeqCst), 2);
        }
        assert!(
            !evidence
                .iter()
                .any(|event| matches!(event, Evidence::Closed(_))),
            "query termination keeps resident connections loaded"
        );
        assert_eq!(
            server.closes.load(Ordering::SeqCst),
            0,
            "query end and target stop retain loaded connections"
        );
        let expected = if empty { 2 } else { 3 };
        assert_eq!(server.connections.load(Ordering::SeqCst), expected);
        assert_eq!(server.extra_calls.load(Ordering::SeqCst), 0);
        let outputs: Vec<_> = session
            .background_agent_summaries()
            .into_iter()
            .map(|agent| agent.output_file)
            .collect();
        session.shutdown_and_wait().await.unwrap();
        gather(&mut wire_receive, &mut evidence, |seen| {
            seen.iter()
                .filter_map(|event| match event {
                    Evidence::Closed(id) => Some(*id),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .len()
                == expected
        })
        .await;
        assert_eq!(server.closes.load(Ordering::SeqCst), expected);
        assert_eq!(
            evidence
                .iter()
                .filter_map(|event| match event {
                    Evidence::Connected(id) => Some(*id),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .len(),
            expected
        );
        for output in outputs {
            let _ = std::fs::remove_file(output);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    process.shutdown().await.unwrap();
}

const NAMESPACE_CHILD: &str = "namespace-catalog-child-directive";
const CATALOG_MARKER: &str = "schema-only-catalog-payload";

struct NamespaceServer {
    connections: AtomicUsize,
    child_instructions: Option<String>,
    catalog: Vec<Value>,
}

#[async_trait]
impl McpInProcessServer for NamespaceServer {
    async fn connect(&self, transport: Box<dyn Transport>) {
        let connection = self.connections.fetch_add(1, Ordering::SeqCst);
        let instructions = if connection == 0 {
            Some("Initial namespace guidance.".to_string())
        } else {
            self.child_instructions.clone()
        };
        let catalog = self.catalog.clone();
        let (send, mut receive) = mpsc::unbounded_channel();
        transport.start(send).await.unwrap();
        tokio::spawn(async move {
            while let Some(event) = receive.recv().await {
                let frame = match event {
                    TransportEvent::Message(frame) => scalar(&frame),
                    TransportEvent::Close => break,
                    TransportEvent::Error(error) => panic!("namespace transport error: {error}"),
                };
                let id = frame.get("id").cloned().unwrap_or(Value::Null);
                let result = match frame["method"].as_str().unwrap_or("") {
                    "initialize" => {
                        let mut result = json!({
                            "protocolVersion":frame["params"]["protocolVersion"],
                            "capabilities":{"tools":{},"resources":{},"prompts":{}},
                            "serverInfo":{"name":"owner","version":"1"}
                        });
                        if let Some(instructions) = &instructions {
                            result["instructions"] = json!(instructions);
                        }
                        result
                    }
                    "tools/list" => json!({"tools":catalog}),
                    "resources/list" => {
                        json!({"resources":[{"uri":"owner://common","name":"Common"}]})
                    }
                    "resources/templates/list" => {
                        json!({"resourceTemplates":[{"uriTemplate":"owner://items/{id}","name":"Item"}]})
                    }
                    "prompts/list" => {
                        json!({"prompts":[{"name":"common","description":"Common prompt"}]})
                    }
                    _ => continue,
                };
                transport
                    .send(wire(json!({"jsonrpc":"2.0","id":id,"result":result})))
                    .await
                    .unwrap();
            }
        });
    }
}

struct NamespaceProvider {
    info: ProviderInfo,
    root: AtomicUsize,
    children: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for NamespaceProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 2_000_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if has(&request, NAMESPACE_CHILD) {
            self.children.lock().unwrap().push(request);
            return finish();
        }
        let step = self.root.fetch_add(1, Ordering::SeqCst);
        if step.is_multiple_of(2) {
            call(
                "namespace-spawn",
                "task",
                json!({"model": "low",
                    "description":"Inspect native namespace declarations",
                    "prompt":NAMESPACE_CHILD,
                    "subagent_type":"namespace-worker",
                    "fork_turns":"all",
                    "run_in_background":false
                }),
            )
        } else {
            let result = result(&request, "namespace-spawn");
            assert!(!result.is_error, "{}", text(result));
            finish()
        }
    }
}

fn legacy_namespace_update(schema: Value) -> klaus_agent_core::types::provider::SystemPromptPart {
    let metadata = wire(json!({
        "namespace":"owner",
        "instructions":"Retired native initialize guidance.",
        "tools":[{"name":"mcp__owner__retired","nativeName":"retired","description":"Retired tool","inputSchema":schema}],
        "resources":[{"uri":"owner://retired","name":"Retired resource"}],
        "resourceTemplates":["owner://retired/{id}"],
        "prompts":[{"name":"retired","description":"Retired prompt"}]
    }));
    klaus_agent_core::types::provider::SystemPromptPart {
        name: "mcp-runtime-namespace-update".into(),
        text: RuntimeString::from("Updated instructions for namespace \"owner\":\nThe latest runtime definition below replaces earlier initialize instructions and tool/resource/prompt metadata only for the listed native bindings of this current MCP declaration. Namespace membership is exactly the listed tools, resources, resource templates and prompts. This update does not replace base instructions, agent-role instructions, permission constraints, or opaque/host-injected tools and guidance, including entries with the same display name.\n\n")
            .concat(&runtime_json::stringify_pretty(&metadata)),
        cacheable: Some(true),
        cache_scope: None,
    }
}

fn opaque_namespace_parts() -> Vec<klaus_agent_core::types::provider::SystemPromptPart> {
    let legacy = legacy_namespace_update(json!({"type":"object"}));
    let mut renamed = legacy.clone();
    renamed.name = "host-owned-namespace-definition".into();
    let mut uncached = legacy.clone();
    uncached.cacheable = Some(false);
    let mut extended = legacy;
    extended.text = extended
        .text
        .concat(&RuntimeString::from("\nOpaque host suffix."));
    vec![renamed, uncached, extended]
}

fn namespace_config(server: Arc<NamespaceServer>) -> McpServerConfig {
    McpServerConfig::InProcess {
        name: "owner".into(),
        server,
        options: McpServerOptions {
            always_load: Some(true),
            ..Default::default()
        },
    }
}

fn namespace_session(
    process: RuntimeProcessContext,
    directory: &std::path::Path,
    server: Arc<NamespaceServer>,
    legacy: klaus_agent_core::types::provider::SystemPromptPart,
) -> (
    Arc<klaus_agent_core::engine::session::Session>,
    Arc<NamespaceProvider>,
) {
    let (options, provider) = namespace_options(process, legacy);
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
        mcp_server_configs: Some(vec![namespace_config(server)]),
        ..Default::default()
    });
    (session, provider)
}

fn namespace_options(
    process: RuntimeProcessContext,
    legacy: klaus_agent_core::types::provider::SystemPromptPart,
) -> (
    klaus_agent_core::engine::agent::AgentOptions,
    Arc<NamespaceProvider>,
) {
    let provider = Arc::new(NamespaceProvider {
        info: ProviderInfo {
            name: "namespace-model".into(),
            default_model: "namespace-model".into(),
            base_url: None,
            env_key: None,
        },
        root: AtomicUsize::new(0),
        children: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.provider = provider.clone();
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    options.config.as_mut().unwrap()["allow"] = json!(["task"]);
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        Arc::new(OpaqueTool(Arc::new(AtomicUsize::new(0)))),
    ]);
    options.include_default_agents = Some(false);
    options.agents = Some(vec![AgentDefinition {
        name: "namespace-worker".into(),
        tools: None,
        ..Default::default()
    }]);
    let mut parts = opaque_namespace_parts();
    parts.push(klaus_agent_core::types::provider::SystemPromptPart {
        name: "mcp-instructions".into(),
        text: OPAQUE_INSTRUCTIONS.into(),
        cacheable: Some(true),
        cache_scope: None,
    });
    parts.push(legacy);
    options.system_prompt_provider = Some(Arc::new(move || {
        let parts = parts.clone();
        Box::pin(async move { parts })
    }));
    (options, provider)
}

async fn run_namespace_child(session: &Arc<klaus_agent_core::engine::session::Session>) {
    let result = tokio::time::timeout(
        TIMEOUT,
        session.run(
            Input::Text("Inspect the current namespace".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .unwrap();
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.stop_reason, TerminalReason::Completed);
}

fn current_namespace_update(request: &ModelRequest) -> Option<Value> {
    let opaque = opaque_namespace_parts();
    for part in &opaque {
        assert!(
            request.system.contains(part),
            "host-owned envelope must stay exact"
        );
    }
    assert!(request
        .system
        .iter()
        .any(|part| part.name == "mcp-instructions" && part.text == OPAQUE_INSTRUCTIONS));
    assert!(
        request
            .tools
            .iter()
            .any(|tool| tool.name == "mcp__opaque__ping"
                && tool.description == "Opaque host callback")
    );
    let updates: Vec<_> = request
        .system
        .iter()
        .filter(|part| part.name == "mcp-runtime-namespace-update" && !opaque.contains(part))
        .collect();
    assert!(
        updates.len() <= 1,
        "old updates must be replaced rather than accumulated"
    );
    updates.first().map(|part| {
        let text = part.text.try_to_utf8().unwrap();
        assert!(!text.contains("inputSchema"));
        assert!(!text.contains("Retired native initialize guidance"));
        assert!(!text.contains("retired"));
        serde_json::from_str(text.split_once("\n\n").unwrap().1).unwrap()
    })
}

#[tokio::test]
async fn large_native_catalog_keeps_complete_schemas_out_of_system_updates() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let schema = json!({
        "type":"object",
        "properties":{"payload":{"type":"string","description":format!("{CATALOG_MARKER}:{}", "x".repeat(8_192))}},
        "required":["payload"],
        "additionalProperties":false
    });
    let catalog = (0..191)
        .map(|index| {
            json!({
                "name":format!("catalog_{index}"),
                "description":format!("Complete native tool {index}"),
                "inputSchema":schema,
                "annotations":{"readOnlyHint":true}
            })
        })
        .collect::<Vec<_>>();
    let instructions = format!("Current native usage guidance.{}", "z".repeat(780));
    let server = Arc::new(NamespaceServer {
        connections: AtomicUsize::new(0),
        child_instructions: Some(instructions.clone()),
        catalog,
    });
    // This is the exact serialized update shape persisted by older runtimes.
    let legacy = legacy_namespace_update(
        json!({"type":"object","description":CATALOG_MARKER.repeat(50_000)}),
    );
    assert!(legacy.text.len_utf16() > 1_048_576);
    let (session, provider) = namespace_session(process.clone(), directory.path(), server, legacy);
    run_namespace_child(&session).await;
    {
        let requests = provider.children.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        let native: Vec<_> = request
            .tools
            .iter()
            .filter(|tool| tool.name.starts_with("mcp__owner__catalog_"))
            .collect();
        assert_eq!(native.len(), 191);
        for (index, tool) in native.iter().enumerate() {
            let name = tool.name.try_to_utf8().unwrap();
            let native_index: usize = name
                .strip_prefix("mcp__owner__catalog_")
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(
                tool.description,
                RuntimeString::from(format!("Complete native tool {native_index}"))
            );
            assert_eq!(
                scalar(&tool.input_schema),
                schema,
                "complete schema at catalog entry {index}"
            );
        }
        let update = current_namespace_update(request).unwrap();
        assert_eq!(update["instructions"], json!(instructions));
        assert_eq!(update["tools"].as_array().unwrap().len(), 191);
        assert_eq!(update["resources"], json!(["owner://common"]));
        assert_eq!(update["resourceTemplates"], json!(["owner://items/{id}"]));
        assert_eq!(update["prompts"], json!(["common"]));
        assert!(request
            .system
            .iter()
            .all(|part| !part.text.contains(CATALOG_MARKER)));
        let system_chars: usize = request
            .system
            .iter()
            .map(|part| part.text.len_utf16())
            .sum();
        assert!(
            system_chars < 128_000,
            "system instructions were {system_chars} characters"
        );
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_namespace_guidance_changes_clears_and_reconnects_without_touching_opaque_parts() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    for instructions in [
        Some("Changed native usage guidance.".to_string()),
        Some(String::new()),
        None,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let server = Arc::new(NamespaceServer {
            connections: AtomicUsize::new(0),
            child_instructions: instructions.clone(),
            catalog: vec![
                json!({"name":"current","description":"Current native tool","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}}),
            ],
        });
        let (session, provider) = namespace_session(
            process.clone(),
            directory.path(),
            server.clone(),
            legacy_namespace_update(json!({"type":"object"})),
        );
        run_namespace_child(&session).await;
        session.remove_mcp_server("owner").await.unwrap();
        run_namespace_child(&session).await;
        session
            .reload_mcp_servers(vec![namespace_config(server)])
            .await
            .unwrap();
        run_namespace_child(&session).await;
        {
            let requests = provider.children.lock().unwrap();
            assert_eq!(requests.len(), 3);
            for index in [0, 2] {
                let update = current_namespace_update(&requests[index]).unwrap();
                assert_eq!(
                    update["instructions"],
                    json!(instructions.clone().unwrap_or_default())
                );
                assert_eq!(
                    update["tools"],
                    json!([{"name":"mcp__owner__current","nativeName":"current"}])
                );
                assert!(requests[index]
                    .tools
                    .iter()
                    .any(|tool| tool.name == "mcp__owner__current"));
            }
            assert!(current_namespace_update(&requests[1]).is_none());
            assert!(!requests[1]
                .tools
                .iter()
                .any(|tool| tool.name == "mcp__owner__current"));
        }
        session.shutdown_and_wait().await.unwrap();
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn cold_child_recovery_replaces_oversized_saved_namespace_updates() {
    use klaus_agent_core::runtime_values::runtime_codec;
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use klaus_agent_core::types::tool::SubagentMessageRequest;

    let supervisor = super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor();
    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let schema = json!({"type":"object","properties":{"payload":{"type":"string","description":CATALOG_MARKER.repeat(400)}}});
    let make_server = || {
        Arc::new(NamespaceServer {
            // The fixture's first connection deliberately advertises historical
            // parent guidance. A recovered process starts with current guidance.
            connections: AtomicUsize::new(1),
            child_instructions: Some("Recovered current native usage guidance.".into()),
            catalog: vec![
                json!({"name":"current","description":"Current native tool","inputSchema":schema}),
            ],
        })
    };
    let make_store = || {
        Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(
                directory
                    .path()
                    .join("sessions")
                    .to_string_lossy()
                    .into_owned(),
            ),
            archive_dir: None,
        }))
    };
    let session_options = |server| StartSessionOptions {
        id: Some("namespace-cold-parent".into()),
        cwd: Some(directory.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
        mcp_server_configs: Some(vec![namespace_config(server)]),
        ..Default::default()
    };
    let legacy = legacy_namespace_update(
        json!({"type":"object","description":CATALOG_MARKER.repeat(50_000)}),
    );
    let (options, _) = namespace_options(process.clone(), legacy.clone());
    let agent = Agent::new(options);
    let session = agent
        .open_session(make_store(), session_options(make_server()))
        .await
        .unwrap();
    run_namespace_child(&session).await;
    let id = session.subagent_lifecycle_snapshot().agents[0]
        .agent_id
        .clone();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    drop(session);
    drop(agent);

    // Restore the exact old engine update in a saved child; only the scratch
    // profile is edited, after its writer and process have fully stopped.
    let parent_file = std::fs::read_dir(directory.path().join("sessions"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .unwrap();
    let child_file = parent_file
        .with_extension("")
        .join("subagents")
        .join(format!("agent-{}.jsonl", id.try_to_utf8().unwrap()));
    let old_part = scalar(&runtime_codec::encode_value(&legacy).unwrap());
    let mut rows = Vec::new();
    for line in std::fs::read_to_string(&child_file).unwrap().lines() {
        let mut row: Value = serde_json::from_str(line).unwrap();
        if row["event"] == "ordinary_subagent_creation" {
            for key in ["baseInstructions", "systemPromptParts"] {
                if let Some(parts) = row["payload"][key].as_array_mut() {
                    parts.push(old_part.clone());
                }
            }
        }
        rows.push(row.to_string());
    }
    std::fs::write(&child_file, rows.join("\n") + "\n").unwrap();
    let process = RuntimeProcessContext::initialize(supervisor).unwrap();
    let (options, provider) = namespace_options(process.clone(), legacy);
    let agent = Agent::new(options);
    let session = agent
        .resume_session(
            make_store(),
            "namespace-cold-parent",
            session_options(make_server()),
        )
        .await
        .unwrap();
    // The production follow-up tool runs inside a prepared parent turn. Finish
    // one resumed parent turn to initialize its current MCP catalog first.
    provider.root.store(1, Ordering::SeqCst);
    run_namespace_child(&session).await;
    let (send, mut receive) = mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |notice| {
        if notice.status == "completed" {
            let _ = send.send(());
        }
    })));
    let receipt = session
        .send_subagent_message(SubagentMessageRequest {
            target: id,
            message: "Continue with the recovered namespace.".into(),
            summary: "Recover the existing child.".into(),
            trigger_turn: true,
            sender_agent_id: None,
            user_initiated: false,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.resumed);
    tokio::time::timeout(TIMEOUT, receive.recv())
        .await
        .unwrap()
        .unwrap();
    {
        let requests = provider.children.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let update = current_namespace_update(&requests[0]).unwrap();
        assert_eq!(
            update["instructions"],
            "Recovered current native usage guidance."
        );
        assert!(
            requests[0]
                .system
                .iter()
                .map(|part| part.text.len_utf16())
                .sum::<usize>()
                < 128_000
        );
        let tool = requests[0]
            .tools
            .iter()
            .find(|tool| tool.name == "mcp__owner__current")
            .unwrap_or_else(|| {
                panic!(
                    "current callable tool missing from {:?}",
                    requests[0]
                        .tools
                        .iter()
                        .map(|tool| tool.name.clone())
                        .collect::<Vec<_>>()
                )
            });
        assert_eq!(scalar(&tool.input_schema), schema);
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
