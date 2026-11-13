//! Defect: explicit composer plugin/skill/MCP references only appended raw JSON metadata without surfacing small plugin MCP tool sets, preloading single-skill instructions, or prefixing plugin skills in the catalog.
//! Root cause: turn startup only expanded file and sub-agent `@`-mentions, leaving `<referenced-resources>` and `klaus:` inline references unresolved while `<skill-catalog>` omitted `plugin_namespace:`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::json;
use tokio::sync::mpsc;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::extensions::plugins::loader::LoadedPlugin;
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::extensions::skills::skill_tool::create_skill_tool;
use klaus_agent_core::mcp::transports::{Transport, TransportEvent};
use klaus_agent_core::mcp::types::{McpInProcessServer, McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::ToolExposure;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl RecordingProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "plugin-preassembly-test".into(),
                base_url: None,
                env_key: None,
                default_model: "plugin-preassembly-test".into(),
            },
            requests: Mutex::new(Vec::new()),
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

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "plugin-preassembly-test".into(),
            },
            ModelStreamEvent::TextDelta { text: "ok".into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 12,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(14),
                },
            },
        ]))
    }
}

struct StaticToolMcpServer {
    server_name: String,
    tool_names: Vec<String>,
}

fn rt_json(val: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(serde_json::to_string(&val).unwrap())).unwrap()
}

fn rpc_method_not_found(id: RuntimeJson) -> RuntimeJson {
    let mut envelope = RuntimeObject::new();
    envelope.insert("jsonrpc", "2.0".into());
    envelope.insert("id", id);
    envelope.insert(
        "error",
        rt_json(json!({ "code": -32601, "message": "Method not found" })),
    );
    RuntimeJson::Object(envelope)
}

fn rpc_success(id: RuntimeJson, result: RuntimeJson) -> RuntimeJson {
    let mut envelope = RuntimeObject::new();
    envelope.insert("jsonrpc", "2.0".into());
    envelope.insert("id", id);
    envelope.insert("result", result);
    RuntimeJson::Object(envelope)
}

#[async_trait]
impl McpInProcessServer for StaticToolMcpServer {
    async fn connect(&self, transport: Box<dyn Transport>) {
        let transport: Arc<dyn Transport> = Arc::from(transport);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _ = transport.start(tx).await;
        let server_name = self.server_name.clone();
        let tool_names = self.tool_names.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let TransportEvent::Message(message) = event else {
                    continue;
                };
                let Some(method) = message.get("method").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(id) = message.get("id").cloned() else {
                    continue;
                };
                let result = if method == "initialize" {
                    Some(rt_json(json!({
                        "protocolVersion": "2025-06-18",
                        "capabilities": { "tools": {} },
                        "serverInfo": { "name": server_name, "version": "1.0.0" }
                    })))
                } else if method == "tools/list" {
                    let tools: Vec<RuntimeJson> = tool_names
                        .iter()
                        .map(|name| {
                            rt_json(json!({
                                "name": name,
                                "description": "Plugin MCP test tool",
                                "inputSchema": { "type": "object", "properties": {} }
                            }))
                        })
                        .collect();
                    let mut obj = RuntimeObject::new();
                    obj.insert("tools", RuntimeJson::Array(tools));
                    Some(RuntimeJson::Object(obj))
                } else {
                    None
                };
                let reply = match result {
                    Some(res) => rpc_success(id, res),
                    None => rpc_method_not_found(id),
                };
                let _ = transport.send(reply).await;
            }
        });
    }
}

async fn run_turn(session: &Arc<Session>, prompt: &str) {
    let mut events = session
        .run_streamed(Input::Text(prompt.into()), TurnOptions::default())
        .events;
    while events.next().await.is_some() {}
}

fn flatten_request_text(request: &ModelRequest) -> String {
    let mut parts = Vec::new();
    for msg in &request.messages {
        if let Message::User(user) = msg {
            for block in &user.content {
                if let ContentBlock::Text(text) = block {
                    if let Ok(utf8) = text.text.try_to_utf8() {
                        parts.push(utf8);
                    }
                }
            }
        }
    }
    parts.join("\n")
}

#[tokio::test]
async fn explicit_single_skill_plugin_reference_surfaces_mcp_tools_and_preloads_skill_once() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = RecordingProvider::new();
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("plugin-preassembly-test");

    let authoring_server: Arc<dyn McpInProcessServer> = Arc::new(StaticToolMcpServer {
        server_name: "ppt-authoring".to_string(),
        tool_names: vec!["ppt_prepare".to_string(), "ppt_photo_search".to_string()],
    });
    let host_server: Arc<dyn McpInProcessServer> = Arc::new(StaticToolMcpServer {
        server_name: "klaus-ppt".to_string(),
        tool_names: vec!["ppt_open".to_string(), "ppt_publish".to_string()],
    });

    options.plugins.push(LoadedPlugin {
        name: "ppt".to_string(),
        version: Some("1.3.6".to_string()),
        display_name: Some("PPT".to_string()),
        description: Some("Create and edit presentations.".to_string()),
        plugin_namespace: "ppt".to_string(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: Default::default(),
        settings: Default::default(),
        dir: "/plugins/ppt".to_string(),
        skills: vec![Skill {
            name: "ppt".into(),
            description: "Design and edit polished presentations".into(),
            body: "UNIQUE_PPT_SKILL_INSTRUCTIONS_MARKER".into(),
            path: "/plugins/ppt/skills/ppt/SKILL.md".into(),
            ..Default::default()
        }],
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: Vec::new(),
        mcp_servers: vec![
            McpServerConfig::HostManaged {
                name: "klaus-ppt".to_string(),
                options: McpServerOptions::default(),
            },
            McpServerConfig::InProcess {
                name: "ppt-authoring".to_string(),
                server: authoring_server,
                options: McpServerOptions::default(),
            },
        ],
        lsp_servers: Default::default(),
        runtime_capabilities: Vec::new(),
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    });

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        mcp_server_configs: Some(vec![McpServerConfig::InProcess {
            name: "klaus-ppt".to_string(),
            server: host_server,
            options: McpServerOptions::default(),
        }]),
        ..Default::default()
    });

    // Turn 1: without naming the plugin, MCP tools stay deferred and skill body is not preloaded,
    // while `<skill-catalog>` prefixes the plugin skill as `ppt:ppt`.
    run_turn(&session, "Say hello.").await;
    {
        let reqs = provider.requests.lock().unwrap();
        let req = reqs.last().unwrap();
        let active_names: Vec<String> = req
            .tools
            .iter()
            .filter_map(|t| t.name.try_to_utf8().ok())
            .collect();
        assert!(!active_names.iter().any(|n| n == "mcp__klaus-ppt__ppt_open"));
        assert!(!active_names
            .iter()
            .any(|n| n == "mcp__ppt-authoring__ppt_prepare"));
        let text = flatten_request_text(req);
        assert!(
            text.contains("- ppt:ppt: Design and edit polished presentations"),
            "catalog should prefix plugin skill with plugin namespace: {text}"
        );
        assert!(!text.contains("UNIQUE_PPT_SKILL_INSTRUCTIONS_MARKER"));
    }

    // Turn 2: explicitly referencing `[演示文稿](klaus:plugin/ppt@klaus-official)` surfaces both
    // host-managed and plugin MCP tools and preloads the single `SKILL.md` inline on step 1.
    let referenced_prompt = concat!(
        "用 [演示文稿](klaus:plugin/ppt%40klaus-official) 做一个产品介绍\n\n",
        "<referenced-resources>\n",
        "The user selected these inline references.\n",
        "[{\"kind\":\"plugin\",\"name\":\"演示文稿\",\"id\":\"ppt@klaus-official\"}]\n",
        "</referenced-resources>"
    );
    run_turn(&session, referenced_prompt).await;
    {
        let reqs = provider.requests.lock().unwrap();
        let req = reqs.last().unwrap();
        let active_names: Vec<String> = req
            .tools
            .iter()
            .filter_map(|t| t.name.try_to_utf8().ok())
            .collect();
        assert!(
            active_names.iter().any(|n| n == "mcp__klaus-ppt__ppt_open"),
            "host-managed plugin MCP tool should be surfaced: {active_names:?}"
        );
        assert!(
            active_names
                .iter()
                .any(|n| n == "mcp__ppt-authoring__ppt_prepare"),
            "plugin stdio/in-process MCP tool should be surfaced: {active_names:?}"
        );
        let text = flatten_request_text(req);
        assert!(text.contains("Capabilities from the referenced `PPT` (`ppt`) plugin:"));
        assert!(text.contains("UNIQUE_PPT_SKILL_INSTRUCTIONS_MARKER"));
    }

    // Turn 3: referencing the same plugin again keeps tools active without duplicating the full skill body.
    run_turn(&session, referenced_prompt).await;
    {
        let reqs = provider.requests.lock().unwrap();
        let req = reqs.last().unwrap();
        let text = flatten_request_text(req);
        let occurrences = text.matches("UNIQUE_PPT_SKILL_INSTRUCTIONS_MARKER").count();
        assert_eq!(
            occurrences, 1,
            "skill body should only appear once across conversation history"
        );
        assert!(text.contains("already loaded in this conversation"));
    }

    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn oversized_plugin_keeps_mcp_tools_deferred_and_multi_skill_plugin_lists_qualified_skills() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let ctx_process = process.clone();
    let provider = RecordingProvider::new();
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("plugin-preassembly-test");

    let twenty_tools: Vec<String> = (0..20).map(|i| format!("inspect_item_{i}")).collect();
    let wide_server: Arc<dyn McpInProcessServer> = Arc::new(StaticToolMcpServer {
        server_name: "wide-inspector".to_string(),
        tool_names: twenty_tools,
    });

    options.plugins.push(LoadedPlugin {
        name: "inspector".to_string(),
        version: Some("1.0.0".to_string()),
        display_name: Some("Inspector Suite".to_string()),
        description: None,
        plugin_namespace: "inspector".to_string(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: Default::default(),
        settings: Default::default(),
        dir: "/plugins/inspector".to_string(),
        skills: vec![
            Skill {
                name: "quick-check".into(),
                description: "Run a quick check".into(),
                body: "QUICK_CHECK_BODY".into(),
                path: "/plugins/inspector/skills/quick-check/SKILL.md".into(),
                ..Default::default()
            },
            Skill {
                name: "deep-setup".into(),
                description: "Configure deep setup".into(),
                body: "DEEP_SETUP_BODY".into(),
                path: "/plugins/inspector/skills/deep-setup/SKILL.md".into(),
                ..Default::default()
            },
        ],
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: Vec::new(),
        mcp_servers: vec![McpServerConfig::InProcess {
            name: "wide-inspector".to_string(),
            server: wide_server,
            options: McpServerOptions::default(),
        }],
        lsp_servers: Default::default(),
        runtime_capabilities: Vec::new(),
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    });

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());

    let prompt = concat!(
        "Check with [Inspector](klaus:plugin/inspector%40klaus-official)\n\n",
        "<referenced-resources>\n",
        "[{\"kind\":\"plugin\",\"name\":\"Inspector\",\"id\":\"inspector@klaus-official\"}]\n",
        "</referenced-resources>"
    );
    run_turn(&session, prompt).await;
    {
        let reqs = provider.requests.lock().unwrap();
        let req = reqs.last().unwrap();
        let active_names: Vec<String> = req
            .tools
            .iter()
            .filter_map(|t| t.name.try_to_utf8().ok())
            .collect();
        assert!(
            !active_names
                .iter()
                .any(|n| n.starts_with("mcp__wide-inspector__")),
            "oversized plugin MCP inventory should stay deferred: {active_names:?}"
        );
        let text = flatten_request_text(req);
        assert!(text.contains("20 tools kept deferred to fit context limits"));
        assert!(text.contains("`wide-inspector`"));
        assert!(text.contains("`inspector:quick-check`"));
        assert!(text.contains("`inspector:deep-setup`"));
        assert!(!text.contains("QUICK_CHECK_BODY"));
        assert!(!text.contains("DEEP_SETUP_BODY"));
    }

    // Both `inspector:quick-check` and bare `quick-check` resolve in the `skill` tool.
    let tagged = Skill {
        name: "quick-check".into(),
        plugin_namespace: Some("inspector".into()),
        description: "Run a quick check".into(),
        body: "QUICK_CHECK_BODY".into(),
        path: "/plugins/inspector/skills/quick-check/SKILL.md".into(),
        ..Default::default()
    };
    let tool = create_skill_tool(move || vec![tagged.clone()]);
    assert_eq!(tool.exposure(), ToolExposure::Direct);
    let ctx = super::r20261015_browser_diagnostics_and_targets::make_ctx("/tmp", ctx_process);
    let qualified_res = tool
        .call(
            rt_json(json!({ "name": "inspector:quick-check" })),
            &ctx,
            None,
        )
        .await
        .unwrap();
    assert!(!qualified_res.serializable.is_error.unwrap_or(false));
    let bare_res = tool
        .call(rt_json(json!({ "name": "quick-check" })), &ctx, None)
        .await
        .unwrap();
    assert!(!bare_res.serializable.is_error.unwrap_or(false));

    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn explicit_skill_reference_preloads_inline_skill_and_surfaces_owning_plugin_mcp() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = RecordingProvider::new();
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.include_default_agents = Some(false);
    options.config.as_mut().unwrap()["model"] = json!("plugin-preassembly-test");

    let diagram_server: Arc<dyn McpInProcessServer> = Arc::new(StaticToolMcpServer {
        server_name: "klaus-digram".to_string(),
        tool_names: vec!["digram_open".to_string(), "digram_publish".to_string()],
    });

    options.plugins.push(LoadedPlugin {
        name: "digram".to_string(),
        version: Some("1.2.1".to_string()),
        display_name: Some("Diagram".to_string()),
        description: None,
        plugin_namespace: "digram".to_string(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: Default::default(),
        settings: Default::default(),
        dir: "/plugins/digram".to_string(),
        skills: vec![Skill {
            name: "digram".into(),
            description: "Draw diagrams".into(),
            body: "DIAGRAM_SKILL_INLINE_BODY".into(),
            path: "/plugins/digram/skills/digram/SKILL.md".into(),
            ..Default::default()
        }],
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: Vec::new(),
        mcp_servers: vec![McpServerConfig::InProcess {
            name: "klaus-digram".to_string(),
            server: diagram_server,
            options: McpServerOptions::default(),
        }],
        lsp_servers: Default::default(),
        runtime_capabilities: Vec::new(),
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    });

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());

    let skill_ref_prompt = concat!(
        "画一张架构图\n\n",
        "<referenced-resources>\n",
        "[{\"kind\":\"skill\",\"name\":\"digram\",\"id\":\"digram\",\"path\":\"/plugins/digram/skills/digram/SKILL.md\"}]\n",
        "</referenced-resources>"
    );
    run_turn(&session, skill_ref_prompt).await;
    {
        let reqs = provider.requests.lock().unwrap();
        let req = reqs.last().unwrap();
        let active_names: Vec<String> = req
            .tools
            .iter()
            .filter_map(|t| t.name.try_to_utf8().ok())
            .collect();
        assert!(
            active_names.iter().any(|n| n == "mcp__klaus-digram__digram_open"),
            "explicitly referencing a plugin skill should surface its small MCP inventory: {active_names:?}"
        );
        let text = flatten_request_text(req);
        assert!(text.contains("DIAGRAM_SKILL_INLINE_BODY"));
    }

    session.shutdown_and_wait().await.unwrap();
}
