//! Defect: plugin capabilities stayed stale in loaded sessions and MCP changes waited until a later turn.
//! Root cause: construction-time snapshots and a whole-turn MCP lease blocked publication at the next model step.
//! Work item: 2026-09-27 plugin live capability refresh.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::StreamExt;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::extensions::output_styles::OutputStyle;
use klaus_agent_core::extensions::plugins::loader::{LoadedPlugin, PluginLoadReport};
use klaus_agent_core::mcp::types::{McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
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
                model: "plugin-live-test".into(),
            },
            ModelStreamEvent::TextDelta { text: "ok".into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(12),
                },
            },
        ]))
    }
}

fn plugin() -> LoadedPlugin {
    let mut lsp_servers = klaus_agent_core::types::json::JsonObject::new();
    lsp_servers.insert(
        "rust".to_string(),
        json!({ "command": "rust-analyzer", "extensions": ["rs"] }),
    );
    LoadedPlugin {
        name: "live-browser".to_string(),
        version: Some("1.0.0".to_string()),
        display_name: None,
        description: None,
        plugin_namespace: "live-browser".to_string(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: Default::default(),
        settings: Default::default(),
        dir: "/plugins/live-browser".to_string(),
        skills: Vec::new(),
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: vec![OutputStyle {
            name: "focused".into(),
            description: None,
            body: "A distinctive plugin-style marker.".into(),
        }],
        mcp_servers: vec![McpServerConfig::Stdio {
            name: "browser".to_string(),
            command: "false".to_string(),
            args: None,
            env: None,
            options: McpServerOptions::default(),
        }],
        lsp_servers,
        runtime_capabilities: vec!["browser".to_string()],
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    }
}

struct MidTurnMcpProvider {
    info: ProviderInfo,
    agent: Mutex<Option<Weak<Agent>>>,
    session: Mutex<Option<Weak<Session>>>,
    calls: AtomicUsize,
    observed_servers: Arc<Mutex<Vec<Vec<RuntimeString>>>>,
    next_report: Mutex<Option<PluginLoadReport>>,
}

impl ModelProvider for MidTurnMcpProvider {
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

    fn stream(&self, _request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.agent
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap()
                .refresh_plugins(self.next_report.lock().unwrap().take().unwrap())
                .unwrap();
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "plugin-midturn-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "read-once".into(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "read-once".into(),
                    partial_json: json!({ "path": "missing-plugin-refresh-fixture.txt" })
                        .to_string()
                        .into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "read-once".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: Some(12),
                    },
                },
            ]));
        }
        let session = self
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let observed = self.observed_servers.clone();
        Box::pin(async_stream::stream! {
            let snapshot = session.mcp_runtime_snapshot().await;
            observed.lock().unwrap().push(
                snapshot.servers.into_iter().map(|server| server.name).collect(),
            );
            yield ModelStreamEvent::MessageStart {
                model: "plugin-midturn-test".into(),
            };
            yield ModelStreamEvent::TextDelta { text: "done".into() };
            yield ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(12),
                },
            };
        })
    }
}

#[tokio::test]
async fn loaded_session_refreshes_plugin_mcp_before_the_next_model_step() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(MidTurnMcpProvider {
        info: ProviderInfo {
            name: "plugin-midturn-test".into(),
            base_url: None,
            env_key: None,
            default_model: "plugin-midturn-test".into(),
        },
        agent: Mutex::new(None),
        session: Mutex::new(None),
        calls: AtomicUsize::new(0),
        observed_servers: Arc::new(Mutex::new(Vec::new())),
        next_report: Mutex::new(Some(PluginLoadReport {
            enabled: vec![plugin()],
            ..Default::default()
        })),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.config.as_mut().unwrap()["model"] = json!("plugin-midturn-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let agent = Arc::new(Agent::new(options));
    let session = agent.start_session(StartSessionOptions::default());
    *provider.agent.lock().unwrap() = Some(Arc::downgrade(&agent));
    *provider.session.lock().unwrap() = Some(Arc::downgrade(&session));
    let turn = session.run_streamed(
        Input::Text("read before plugin refresh".into()),
        TurnOptions::default(),
    );
    let events: Vec<_> = turn.events.collect().await;
    assert!(!events.iter().any(|event| matches!(
        event,
        klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
    )));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *provider.observed_servers.lock().unwrap(),
        vec![vec![RuntimeString::from("browser")]],
    );
    session.dispose().await;
}

#[tokio::test]
async fn loaded_session_removes_plugin_mcp_before_the_next_model_step() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(MidTurnMcpProvider {
        info: ProviderInfo {
            name: "plugin-midturn-test".into(),
            base_url: None,
            env_key: None,
            default_model: "plugin-midturn-test".into(),
        },
        agent: Mutex::new(None),
        session: Mutex::new(None),
        calls: AtomicUsize::new(0),
        observed_servers: Arc::new(Mutex::new(Vec::new())),
        next_report: Mutex::new(Some(PluginLoadReport::default())),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.tools = None;
    options.config.as_mut().unwrap()["model"] = json!("plugin-midturn-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let agent = Arc::new(Agent::new(options));
    agent
        .refresh_plugins(PluginLoadReport {
            enabled: vec![plugin()],
            ..Default::default()
        })
        .unwrap();
    let session = agent.start_session(StartSessionOptions::default());
    *provider.agent.lock().unwrap() = Some(Arc::downgrade(&agent));
    *provider.session.lock().unwrap() = Some(Arc::downgrade(&session));
    let turn = session.run_streamed(
        Input::Text("remove plugin MCP".into()),
        TurnOptions::default(),
    );
    let events: Vec<_> = turn.events.collect().await;
    assert!(!events.iter().any(|event| matches!(
        event,
        klaus_agent_core::types::events::EngineEvent::TurnFailed { .. }
    )));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *provider.observed_servers.lock().unwrap(),
        vec![Vec::<RuntimeString>::new()]
    );
    session.dispose().await;
}

#[tokio::test]
async fn old_session_adopts_plugin_style_lsp_and_browser_mapping_on_next_turn() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "plugin-live-test".into(),
            base_url: None,
            env_key: None,
            default_model: "plugin-live-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = None;
    options.config.as_mut().unwrap()["model"] = json!("plugin-live-test");
    options.config.as_mut().unwrap()["outputStyle"] = json!("focused");
    options.config.as_mut().unwrap()["lsp"] = json!(true);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    assert!(!session.lsp_available());
    assert_eq!(session.active_output_style_name(), None);
    assert!(session
        .runtime_capability_snapshot_for_turn(&TurnOptions::default())
        .capabilities
        .browser_tool_mapping
        .is_none());

    let held = session.run_streamed(Input::Text("held".into()), TurnOptions::default());
    agent
        .refresh_plugins(PluginLoadReport {
            enabled: vec![plugin()],
            ..Default::default()
        })
        .unwrap();
    assert!(!session.lsp_available());
    assert_eq!(session.active_output_style_name(), None);
    assert!(session
        .runtime_capability_snapshot_for_turn(&TurnOptions::default())
        .capabilities
        .browser_tool_mapping
        .is_none());
    drop(held);

    let result = session
        .run(Input::Text("new generation".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert!(session.lsp_available());
    assert_eq!(session.active_output_style_name(), Some("focused".into()));
    assert!(session
        .runtime_capability_snapshot_for_turn(&TurnOptions::default())
        .capabilities
        .browser_tool_mapping
        .is_some());
    {
        let requests = provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        // The adopted browser capability is a dispatch mapping only: the browser
        // surface belongs to the mapped MCP server's own tools, so adopting the
        // capability must not register engine browser_* builtins.
        assert!(
            !request
                .tools
                .iter()
                .any(|tool| tool.name.starts_with("browser_")),
            "mapping adoption alone must not surface engine browser builtins"
        );
        assert!(request
            .system
            .iter()
            .any(|part| part.text.contains("A distinctive plugin-style marker.")));
    }

    agent.refresh_plugins(PluginLoadReport::default()).unwrap();
    let result = session
        .run(
            Input::Text("removed generation".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert!(!session.lsp_available());
    assert_eq!(session.active_output_style_name(), None);
    assert!(session
        .runtime_capability_snapshot_for_turn(&TurnOptions::default())
        .capabilities
        .browser_tool_mapping
        .is_none());
    {
        let requests = provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert!(!request
            .tools
            .iter()
            .any(|tool| tool.name == "browser_navigate"));
        assert!(!request
            .system
            .iter()
            .any(|part| part.text.contains("A distinctive plugin-style marker.")));
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn refreshed_plugin_does_not_override_host_or_session_capabilities() {
    use klaus_agent_core::browser::BrowserHostToolMapping;
    use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
    use klaus_agent_core::extensions::runtime_snapshot::ExtensionSourceKind;

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "plugin-live-test".into(),
            base_url: None,
            env_key: None,
            default_model: "plugin-live-test".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = None;
    options.config.as_mut().unwrap()["model"] = json!("plugin-live-test");
    options.config.as_mut().unwrap()["outputStyle"] = json!("focused");
    options.config.as_mut().unwrap()["lsp"] = json!(true);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.output_styles = Some(Arc::new(|| {
        vec![OutputStyle {
            name: "focused".into(),
            description: None,
            body: "Host style marker.".into(),
        }]
    }));
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        output_styles: Some(Arc::new(|| {
            vec![OutputStyle {
                name: "focused".into(),
                description: None,
                body: "Session style marker.".into(),
            }]
        })),
        lsp_servers: Some(Arc::new(Vec::new)),
        capability_roots: Some(vec![RuntimeCapabilityRoot::host("session-browser")
            .with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server(
                "host-browser",
            ))]),
        ..Default::default()
    });
    agent
        .refresh_plugins(PluginLoadReport {
            enabled: vec![plugin()],
            ..Default::default()
        })
        .unwrap();
    let result = session
        .run(
            Input::Text("session overrides".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert!(!session.lsp_available());
    assert_eq!(session.active_output_style_name(), Some("focused".into()));
    let snapshot = session.extension_runtime_snapshot();
    assert_eq!(
        snapshot.output_styles[0].source.kind,
        ExtensionSourceKind::Session
    );
    let mapping = session
        .runtime_capability_snapshot_for_turn(&TurnOptions::default())
        .capabilities
        .browser_tool_mapping
        .unwrap();
    assert_eq!(mapping.navigate, "mcp__host-browser__navigate");
    {
        let requests = provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert!(request
            .system
            .iter()
            .any(|part| part.text.contains("Session style marker.")));
        assert!(!request
            .system
            .iter()
            .any(|part| part.text.contains("Host style marker.")));
        assert!(!request
            .system
            .iter()
            .any(|part| part.text.contains("A distinctive plugin-style marker.")));
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
