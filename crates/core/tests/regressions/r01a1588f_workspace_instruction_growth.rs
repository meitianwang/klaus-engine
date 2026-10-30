//! Defect: tool continuations exhausted the input window with duplicate instructions.
//! Root cause: request-local workspace projections became conversation history.
//! Work item: 01a1588f keep projections ephemeral and repair polluted histories.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, SystemMessage, SystemSubtype, Usage, UserMessage,
};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const INSTRUCTIONS: &str = "Preserve the user's workspace changes. ";

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn projection() -> Message {
    Message::User(UserMessage {
        uuid: "request-workspace-instructions-7".into(),
        content: vec![text(INSTRUCTIONS.repeat(400))],
        is_meta: Some(true),
        question_answer: None,
    })
}

fn projected(message: &Message) -> bool {
    matches!(message, Message::User(user) if user.is_meta == Some(true) && user.uuid.starts_with("request-workspace-instructions-"))
}

struct Probe(JsonSchema);

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        "instruction_probe"
    }
    fn static_description(&self) -> &str {
        "Complete one bounded fixture step."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.0
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, "Step complete.", false))
    }
}

struct Provider {
    info: ProviderInfo,
    rounds: usize,
    requests: Mutex<Vec<ModelRequest>>,
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
            context_window: 32_768,
            max_output_tokens: 1024,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            let index = requests.len();
            requests.push(request.clone());
            index
        };
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: request.model,
        }];
        if index < self.rounds {
            let id: RuntimeString = format!("instruction-step-{index}").into();
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: id.clone(),
                    name: "instruction_probe".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: id.clone(),
                    partial_json: "{}".into(),
                },
                ModelStreamEvent::ToolUseStop { id },
            ]);
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Finished.".into(),
            });
        }
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: if index < self.rounds {
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            },
            usage: Usage {
                input_tokens: 5_000,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct Fixture {
    session: Arc<Session>,
    provider: Arc<Provider>,
    store: Arc<JsonlSessionStore>,
    directory: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

async fn fixture(rounds: usize, history: Vec<Message>) -> Fixture {
    let directory =
        std::env::temp_dir().join(format!("klaus-instruction-growth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "instruction-growth-fixture".into(),
            default_model: "instruction-model".into(),
            base_url: None,
            env_key: None,
        },
        rounds,
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process);
    options.provider = provider.clone();
    options.include_default_agents = Some(false);
    options.tools = Some(vec![Arc::new(Probe(
        json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
    ))]);
    let root = directory.to_string_lossy().into_owned();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = Agent::new(options).open_session(store.clone(), StartSessionOptions {
        cwd: Some(root.clone().into()), user_config_dir: Some(root.into()), initial_history: Some(history),
        system_prompt_provider: Some(Arc::new(|| Box::pin(async {
            vec![SystemPromptPart { name: "memory".into(), text: INSTRUCTIONS.repeat(400).into(), cacheable: Some(true), cache_scope: None }]
        }))),
        config: Some(json!({"model":"instruction-model", "contextWindow":32_768, "maxOutputTokens":1024,
            "prewarm":false, "thinking":false, "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "totalTokensReminder":"padded-countdown", "totalTokensReminderBudget":1_000_000,
            "speculativeCompaction":false, "earlyMicrocompact":false, "idleMicrocompact":false,
            "compactionSelfCheck":false, "loopDetection":false})),
        ..Default::default()
    }).await.unwrap();
    Fixture {
        session,
        provider,
        store,
        directory,
    }
}

#[tokio::test]
async fn tool_continuations_send_one_instruction_copy_without_exhausting_the_window() {
    let fixture = fixture(10, Vec::new()).await;
    for _ in 0..2 {
        let result = fixture
            .session
            .run(
                Input::Text("Finish the fixture steps.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    {
        let requests = fixture.provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 12);
        let first = &requests[0];
        let without_countdown: Vec<_> = first
            .system
            .iter()
            .filter(|part| part.name != "total-tokens")
            .cloned()
            .collect();
        let estimated =
            klaus_agent_core::context::tokens::estimate_prefix_tokens(
                &without_countdown,
                &first.tools,
            ) + klaus_agent_core::context::tokens::estimate_messages_tokens(&first.messages);
        let countdown = first
            .system
            .iter()
            .find(|part| part.name == "total-tokens")
            .unwrap();
        assert_eq!(
            countdown.text,
            RuntimeString::from(format!(
                "<total_tokens>{} tokens left</total_tokens>",
                1_000_000 - estimated
            )),
            "count the current workspace prefix exactly once without a usage anchor"
        );
        let counts: Vec<_> = requests
            .iter()
            .map(|request| request.messages.iter().filter(|m| projected(m)).count())
            .collect();
        assert_eq!(
            counts,
            vec![1; 12],
            "workspace projections per provider request"
        );
    }
    assert!(!fixture.session.history().iter().any(projected));
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn resumed_polluted_history_is_repaired_before_input_validation_and_persisted() {
    let mut history = vec![projection(); 40];
    // A matching human ID is ordinary user input and must survive cleanup.
    let mut human = projection();
    if let Message::User(user) = &mut human {
        user.is_meta = None;
        user.content = vec![text("Keep this human request.")];
    }
    history.push(human.clone());
    let display = Message::System(SystemMessage {
        uuid: "prior-display".into(),
        text: "Preserved rendering metadata.".into(),
        subtype: SystemSubtype::UserDisplay,
    });
    history.push(display.clone());
    history.push(Message::Assistant(AssistantMessage {
        uuid: "old-inflated-response".into(),
        model: "instruction-model".into(),
        content: vec![text("Earlier work.")],
        usage: Some(Usage {
            input_tokens: 500_000,
            output_tokens: 20,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        }),
    }));
    let fixture = fixture(0, history).await;
    let result = fixture
        .session
        .run(
            Input::Text("Continue.".into()),
            TurnOptions {
                display_metadata: Some(
                    runtime_json::parse(&RuntimeString::from(
                        r#"{"displayText":"Continue with the current task."}"#,
                    ))
                    .unwrap(),
                ),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = fixture.provider.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "cleanup must not require a summary request"
        );
        assert_eq!(
            requests[0].messages.iter().filter(|m| projected(m)).count(),
            1
        );
        assert!(requests[0].messages.contains(&human));
        let old = requests[0]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::Assistant(message) if message.uuid == "old-inflated-response" => {
                    Some(message)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            old.usage, None,
            "inflated request usage is no longer a valid anchor"
        );
    }
    let repaired = fixture.session.history();
    assert!(!repaired.iter().any(projected));
    assert!(repaired.contains(&human));
    assert!(
        repaired.contains(&display),
        "repair preserves earlier host rendering metadata"
    );
    assert_eq!(
        repaired
            .iter()
            .filter(|m| matches!(m, Message::System(s) if s.subtype == SystemSubtype::UserDisplay))
            .count(),
        2,
        "repair preserves the submitted turn's rendering metadata"
    );
    fixture.session.flush_rollout().await.unwrap();
    let durable = fixture
        .store
        .read_thread(&fixture.session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let restored = project_for_prompt(&durable.raw_items);
    assert!(!restored.iter().any(projected));
    assert!(restored.contains(&human));
    assert!(restored.contains(&display));
    let result = fixture
        .session
        .run(Input::Text("Finish.".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = fixture.provider.requests.lock().unwrap();
        let countdown = requests
            .last()
            .unwrap()
            .system
            .iter()
            .find(|part| part.name == "total-tokens")
            .unwrap();
        let remaining: u64 = countdown
            .text
            .try_to_utf8()
            .unwrap()
            .trim_start_matches("<total_tokens>")
            .split(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            remaining > 900_000,
            "history repair must not retire inflated usage: {remaining}"
        );
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[test]
fn request_projection_replaces_stale_instructions_and_removes_absent_sources() {
    use klaus_agent_core::context::instruction_policy::project_workspace_instructions;
    let parts = vec![SystemPromptPart {
        name: "memory".into(),
        text: "Current workspace policy.".into(),
        cacheable: Some(true),
        cache_scope: None,
    }];
    let (_, once) = project_workspace_instructions(&parts, &vec![projection(); 40]);
    let (_, twice) = project_workspace_instructions(&parts, &once);
    assert!(once == twice, "reprojection must replace prior copies");
    assert_eq!(twice.len(), 1);
    assert!(
        matches!(&twice[0], Message::User(user) if user.content == vec![text("Current workspace policy.")])
    );
    let (_, absent) = project_workspace_instructions(&[], &twice);
    assert!(absent.is_empty());
}

#[tokio::test]
async fn summary_fork_keeps_one_current_instruction_copy_outside_compacted_history() {
    assert_summary_instruction_ownership(false).await;
}

#[tokio::test]
async fn completed_empty_summary_keeps_current_instructions_once() {
    assert_summary_instruction_ownership(true).await;
}

async fn assert_summary_instruction_ownership(fallback: bool) {
    use klaus_agent_core::context::compact::full_compact::{
        create_full_compactor, FullCompactorOptions,
    };
    use klaus_agent_core::schemas::config::default_config;
    let fixture = fixture(0, Vec::new()).await;
    let result = fixture
        .session
        .run(Input::Text("Start.".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let request = fixture.provider.requests.lock().unwrap()[0].clone();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let seen = captured.clone();
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(move |request, _| {
            let first = {
                let mut requests = seen.lock().unwrap();
                requests.push(request);
                requests.len() == 1
            };
            if fallback && first {
                return Box::pin(futures::stream::iter(vec![ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 0,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                }]));
            }
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "## Goal\nContinue the fixture task.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        }),
        model: "instruction-model".into(),
        max_output_tokens: Some(1024),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    });
    compactor.prepare_request(&request, &AbortSignal::new());
    let result = compactor
        .full_compact(fixture.session.history(), &default_config(), None, false)
        .await;
    assert!(result.compacted);
    assert!(!result.messages.iter().any(projected));
    {
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        for request in captured.iter() {
            assert_eq!(request.messages.iter().filter(|m| projected(m)).count(), 1);
        }
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}
