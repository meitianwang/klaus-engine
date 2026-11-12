//! Defect: fully authorized MCP calls failed at an empty application confirmation.
//! Root cause: unattended elicitation rejected confirmations without reading sandbox authority.
//! Work item: 20261112 full-access MCP confirmation routing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::mcp::elicitation::{
    McpElicitationAction, McpElicitationEvent, McpElicitationPresenter, McpElicitationResponse,
};
use klaus_agent_core::mcp::transports::{Transport, TransportEvent};
use klaus_agent_core::mcp::types::{McpInProcessServer, McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

fn wire(value: Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

struct ConfirmationServer(Arc<Mutex<Vec<Value>>>, bool);

#[async_trait]
impl McpInProcessServer for ConfirmationServer {
    async fn connect(&self, transport: Box<dyn Transport>) {
        let (send, mut receive) = mpsc::unbounded_channel();
        transport.start(send).await.unwrap();
        let answers = self.0.clone();
        let input_form = self.1;
        tokio::spawn(async move {
            let mut pending = None;
            while let Some(event) = receive.recv().await {
                let TransportEvent::Message(frame) = event else {
                    break;
                };
                let frame: Value =
                    serde_json::from_str(&runtime_json::stringify(&frame).try_to_utf8().unwrap())
                        .unwrap();
                let id = frame["id"].clone();
                if id == "confirm-app" && frame.get("method").is_none() {
                    answers.lock().unwrap().push(frame["result"].clone());
                    let tool_id = pending.take().expect("confirmation belongs to a tool call");
                    transport
                        .send(wire(json!({
                            "jsonrpc":"2.0", "id":tool_id,
                            "result":{"content":[{"type":"text","text":"finished"}]}
                        })))
                        .await
                        .unwrap();
                    continue;
                }
                let result = match frame["method"].as_str().unwrap_or("") {
                    "initialize" => json!({
                        "protocolVersion":frame["params"]["protocolVersion"],
                        "capabilities":{"tools":{},"resources":{},"prompts":{}},
                        "serverInfo":{"name":"confirm","version":"1"}
                    }),
                    "tools/list" => json!({"tools":[{
                        "name":"get_state", "description":"Read a fixture application",
                        "inputSchema":{"type":"object","properties":{}},
                        "annotations":{"readOnlyHint":true}
                    }]}),
                    "resources/list" => json!({"resources":[]}),
                    "resources/templates/list" => json!({"resourceTemplates":[]}),
                    "prompts/list" => json!({"prompts":[]}),
                    "tools/call" => {
                        pending = Some(id);
                        let params = if input_form {
                            json!({
                                "message":"Enter the application label",
                                "requestedSchema":{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}
                            })
                        } else {
                            json!({
                                "message":"Allow access to the fixture application?",
                                "_meta":{"persist":["always"],"klaus":{"computerUseApp":"Fixture App"}},
                                "requestedSchema":{"type":"object","properties":{}}
                            })
                        };
                        transport
                            .send(wire(json!({
                                "jsonrpc":"2.0", "id":"confirm-app", "method":"elicitation/create",
                                "params":params
                            })))
                            .await
                            .unwrap();
                        continue;
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

struct Presenter(Arc<AtomicUsize>);

#[async_trait]
impl McpElicitationPresenter for Presenter {
    async fn present(&self, _: McpElicitationEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Provider {
    info: ProviderInfo,
    steps: AtomicUsize,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
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
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        let first = self.steps.fetch_add(1, Ordering::SeqCst) == 0;
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "fixture-model".into(),
        }];
        if first {
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: "read-app".into(),
                    name: "mcp__confirm__get_state".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "read-app".into(),
                    partial_json: "{}".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "read-app".into(),
                },
            ]);
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "complete".into(),
            });
        }
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: if first {
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            },
            usage: Usage {
                input_tokens: 10,
                output_tokens: 4,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

#[tokio::test]
async fn full_access_confirms_native_mcp_calls_without_a_human_prompt() {
    for (sandbox, expected) in [
        ("danger-full-access", "accept"),
        ("workspace-write", "decline"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let answers = Arc::new(Mutex::new(Vec::new()));
        let presentations = Arc::new(AtomicUsize::new(0));
        let mut options = super::r7c876d49_spec_restores_latest_permission_policy::agent_options(
            klaus_agent_core::runtime_process::RuntimeProcessContext::initialize(
                super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
            )
            .unwrap(),
        );
        options.provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "fixture-model".into(),
                default_model: "fixture-model".into(),
                base_url: None,
                env_key: None,
            },
            steps: AtomicUsize::new(0),
        });
        options.config.as_mut().unwrap()["sandbox_mode"] = json!(sandbox);
        options.config.as_mut().unwrap()["prewarm"] = json!(false);
        let session = Agent::new(options).start_session(StartSessionOptions {
            cwd: Some(directory.path().to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
            mcp_elicitation_presenter: Some(Arc::new(Presenter(presentations.clone()))),
            mcp_server_configs: Some(vec![McpServerConfig::InProcess {
                name: "confirm".into(),
                server: Arc::new(ConfirmationServer(answers.clone(), false)),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }]),
            ..Default::default()
        });
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            session.run(
                Input::Text("Read the fixture application".into()),
                TurnOptions::default(),
            ),
        )
        .await
        .expect("confirmation must not wait for a human");
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(presentations.load(Ordering::SeqCst), 0);
        assert_eq!(
            answers.lock().unwrap().len(),
            1,
            "the native MCP tool must execute"
        );
        assert_eq!(
            answers.lock().unwrap()[0]["action"],
            expected,
            "sandbox={sandbox}"
        );
        if expected == "accept" {
            assert_eq!(answers.lock().unwrap()[0]["content"], json!({}));
        }
        session.shutdown_and_wait().await.unwrap();
    }
}

struct InputPresenter(mpsc::UnboundedSender<McpElicitationEvent>);

#[async_trait]
impl McpElicitationPresenter for InputPresenter {
    async fn present(&self, event: McpElicitationEvent) {
        self.0.send(event).unwrap();
    }
}

#[tokio::test]
async fn full_access_root_turn_collects_real_input_but_internal_turn_does_not_prompt() {
    for meta_input in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let answers = Arc::new(Mutex::new(Vec::new()));
        let (events, mut receive) = mpsc::unbounded_channel();
        let mut options = super::r7c876d49_spec_restores_latest_permission_policy::agent_options(
            klaus_agent_core::runtime_process::RuntimeProcessContext::initialize(
                super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
            )
            .unwrap(),
        );
        options.provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "fixture-model".into(),
                default_model: "fixture-model".into(),
                base_url: None,
                env_key: None,
            },
            steps: AtomicUsize::new(0),
        });
        options.config.as_mut().unwrap()["prewarm"] = json!(false);
        let session = Agent::new(options).start_session(StartSessionOptions {
            cwd: Some(directory.path().to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
            mcp_elicitation_presenter: Some(Arc::new(InputPresenter(events))),
            mcp_server_configs: Some(vec![McpServerConfig::InProcess {
                name: "confirm".into(),
                server: Arc::new(ConfirmationServer(answers.clone(), true)),
                options: McpServerOptions {
                    always_load: Some(true),
                    ..Default::default()
                },
            }]),
            ..Default::default()
        });
        let running = tokio::spawn({
            let session = session.clone();
            async move {
                session
                    .run(
                        Input::Text("Read the fixture application".into()),
                        TurnOptions {
                            meta_input,
                            ..Default::default()
                        },
                    )
                    .await
            }
        });
        if !meta_input {
            let event = tokio::time::timeout(Duration::from_secs(15), receive.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                answers.lock().unwrap().is_empty(),
                "input must not be fabricated"
            );
            session
                .resolve_mcp_elicitation(
                    &event.request_id.try_to_utf8().unwrap(),
                    McpElicitationResponse {
                        action: McpElicitationAction::Accept,
                        content: Some(wire(json!({"label":"supplied by the user"}))),
                        meta: None,
                    },
                )
                .unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(15), running)
            .await
            .unwrap()
            .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
        let expected = if meta_input { "decline" } else { "accept" };
        assert_eq!(answers.lock().unwrap().len(), 1);
        assert_eq!(answers.lock().unwrap()[0]["action"], expected);
        if !meta_input {
            assert_eq!(
                answers.lock().unwrap()[0]["content"],
                json!({"label":"supplied by the user"})
            );
        }
        assert!(
            receive.try_recv().is_err(),
            "at most one input prompt may be presented"
        );
        session.shutdown_and_wait().await.unwrap();
    }
}
