//! Defect: delegated queries reused the parent's mutable compactor and could
//! overwrite its request prefix, consume its summary or cancel its pending work.
//! Root cause: child inference captures cloned a shared compactor dependency.
//! Work item 20261020: isolate child context state and clean up only child work.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    CallModel, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SpawnSubAgentOptions, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;

const MODEL: &str = "context-isolation-model";
const CHILD: &str = "ISOLATED_CONTEXT_CHILD_DIRECTIVE";
const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq)]
enum Event {
    ToolBegin,
    ToolEnd,
    Fork(usize, usize),
    Prepared(usize),
}

struct RecordingCompactor {
    owner: usize,
    forkable: bool,
    next: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingCompactor {
    fn unchanged(messages: &[Message]) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[async_trait]
impl Compactor for RecordingCompactor {
    fn fork_isolated(
        &self,
        _: CallModel,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        if !self.forkable {
            return Err("fixture compactor does not support isolated sessions".to_owned());
        }
        let owner = self.next.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .unwrap()
            .push(Event::Fork(self.owner, owner));
        Ok(Arc::new(Self {
            owner,
            forkable: true,
            next: self.next.clone(),
            events: self.events.clone(),
        }))
    }

    fn prepare_request(&self, _: &ModelRequest, _: &AbortSignal) {
        self.events
            .lock()
            .unwrap()
            .push(Event::Prepared(self.owner));
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        Self::unchanged(messages)
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        Self::unchanged(messages)
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 1,
            output_tokens: 1,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

struct Provider {
    info: ProviderInfo,
    child_requests: AtomicUsize,
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
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| match message {
            Message::User(user) => user.content.iter().any(
                |block| matches!(block, ContentBlock::Text(text) if text.text.contains(CHILD)),
            ),
            _ => false,
        });
        let events = if is_child {
            self.child_requests.fetch_add(1, Ordering::SeqCst);
            vec![
                ModelStreamEvent::TextDelta {
                    text: "child began".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]
        } else if request
            .messages
            .iter()
            .any(|message| matches!(message, Message::ToolResult(_)))
        {
            vec![
                ModelStreamEvent::TextDelta {
                    text: "parent finished".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]
        } else {
            vec![
                ModelStreamEvent::ToolUseStart {
                    id: "delegate".into(),
                    name: "context_delegate".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "delegate".into(),
                    partial_json: "{}".into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "delegate".into(),
                },
                stop(ModelStopReason::ToolUse),
            ]
        };
        Box::pin(futures::stream::iter(events))
    }
}

struct Delegate {
    fork: bool,
    drop_early: bool,
    events: Arc<Mutex<Vec<Event>>>,
    failures: Arc<Mutex<Vec<RuntimeString>>>,
}

#[async_trait]
impl Tool for Delegate {
    fn static_name(&self) -> &str {
        "context_delegate"
    }
    fn static_description(&self) -> &str {
        "Run the isolated child fixture."
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&r#"{"type":"object"}"#.into()).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        context: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.events.lock().unwrap().push(Event::ToolBegin);
        let spawn = context
            .spawn_sub_agent
            .as_ref()
            .expect("public tool context exposes child dispatch");
        let run = match spawn(SpawnSubAgentOptions {
            prompt: CHILD.into(),
            agent_type: (!self.fork).then(|| "context-role".into()),
            ..Default::default()
        })
        .await
        {
            Ok(run) => run,
            Err(error) => {
                self.failures.lock().unwrap().push(error.clone());
                self.events.lock().unwrap().push(Event::ToolEnd);
                return Err(ToolError::new(error));
            }
        };
        let mut events = run.events;
        let mut count = 0;
        let mut observed_text = false;
        while let Some(event) = events.next().await {
            count += 1;
            assert!(count <= 128, "bounded child fixture");
            match event {
                EngineEvent::TurnFailed { error, .. } => {
                    self.failures.lock().unwrap().push(error.message)
                }
                EngineEvent::TextDelta { .. } => {
                    observed_text = true;
                    if self.drop_early {
                        break;
                    }
                }
                _ => {}
            }
        }
        drop(events);
        if self.drop_early {
            assert!(observed_text, "drop after child request preparation");
        }
        self.events.lock().unwrap().push(Event::ToolEnd);
        Ok(text_result(input, "delegation finished", false))
    }
}

async fn run_case(
    fork: bool,
    drop_early: bool,
    forkable: bool,
) -> (Vec<Event>, Vec<RuntimeString>, usize) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory =
        std::env::temp_dir().join(format!("klaus-child-context-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "child-context-fixture".into(),
            base_url: None,
            env_key: None,
            default_model: MODEL.into(),
        },
        child_requests: AtomicUsize::new(0),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(directory.to_string_lossy().into_owned());
    options.include_default_agents = Some(false);
    options.agents = Some(vec![AgentDefinition {
        name: "context-role".into(),
        tools: Some(Vec::new()),
        ..Default::default()
    }]);
    options.tools = Some(vec![Arc::new(Delegate {
        fork,
        drop_early,
        events: events.clone(),
        failures: failures.clone(),
    })]);
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({
                "model": MODEL, "fork": fork, "prewarm": false, "autoMemory": false,
                "autoDream": false, "memoryRecall": false, "speculativeCompaction": false,
                "earlyMicrocompact": false, "idleMicrocompact": false, "loopDetection": false,
                "editedFiles": false, "maxTurns": 5,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
    options.deps.compactor = Some(Arc::new(RecordingCompactor {
        owner: 0,
        forkable,
        next: Arc::new(AtomicUsize::new(1)),
        events: events.clone(),
    }));
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let result = tokio::time::timeout(
        WAIT,
        session.run(
            Input::Text("Run the child fixture.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .unwrap();
    assert!(result.error.is_none(), "{:?}", result.error);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    let observed = events.lock().unwrap().clone();
    let failures = failures.lock().unwrap().clone();
    (
        observed,
        failures,
        provider.child_requests.load(Ordering::SeqCst),
    )
}

fn delegation_events(events: &[Event]) -> &[Event] {
    let start = events
        .iter()
        .position(|event| *event == Event::ToolBegin)
        .unwrap();
    let end = events
        .iter()
        .position(|event| *event == Event::ToolEnd)
        .unwrap();
    &events[start + 1..end]
}

#[tokio::test]
async fn ordinary_children_receive_an_isolated_compactor() {
    let (events, failures, child_calls) = run_case(false, false, true).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(child_calls, 1);
    let child = delegation_events(&events);
    assert!(child.contains(&Event::Fork(0, 1)), "{events:?}");
    assert!(!child.contains(&Event::Prepared(0)), "{events:?}");
}

#[tokio::test]
async fn dropping_a_child_stream_preserves_the_parent_compactor_binding() {
    let (events, failures, child_calls) = run_case(false, true, true).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(child_calls, 1);
    let child = delegation_events(&events);
    assert!(child.contains(&Event::Fork(0, 1)), "{events:?}");
    assert!(!child.contains(&Event::Prepared(0)), "{events:?}");
}

#[tokio::test]
async fn explicit_forks_receive_independent_compactor_instances() {
    let (events, failures, child_calls) = run_case(true, false, true).await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(child_calls, 1);
    let child = delegation_events(&events);
    assert!(child.contains(&Event::Fork(0, 1)), "{events:?}");
    assert!(!child.contains(&Event::Prepared(0)), "{events:?}");
}

#[tokio::test]
async fn unsupported_custom_isolation_fails_explicitly_before_a_child_provider_request() {
    let (events, failures, child_calls) = run_case(false, false, false).await;
    assert_eq!(child_calls, 0);
    assert_eq!(failures.len(), 1, "{events:?}");
    assert!(failures[0].contains("Cannot initialize child compaction"));
    assert!(failures[0].contains("does not support isolated sessions"));
}
