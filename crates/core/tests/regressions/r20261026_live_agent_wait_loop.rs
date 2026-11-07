//! Defect: repeated bounded waits on a running child terminated the parent turn.
//! Root cause: argument-only repetition detection treated live observations as actions.
//! Work item: 20261026 live-agent wait classification at both stream ingress paths.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_output_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{EngineErrorCode, TerminalReason};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const CHILD_PROMPT: &str = "bounded-wait-child-directive";
const CHILD_RESULT: &str = "The child finished its assigned work.";
const WAIT_COUNT: usize = 12;

#[derive(Clone, Copy, PartialEq)]
enum Scenario {
    Live,
    Missing,
    NonBlocking,
    ZeroTimeout,
    Completed,
}

struct WaitProvider {
    info: ProviderInfo,
    scenario: Scenario,
    alias: bool,
    omitted_stop: bool,
    parent_calls: AtomicUsize,
    child_calls: AtomicUsize,
    waits: AtomicUsize,
    finish_child: Arc<Semaphore>,
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
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

impl WaitProvider {
    fn tool(&self, name: &str, id: String, input: Value) -> ModelStream {
        let mut events = vec![
            ModelStreamEvent::MessageStart {
                model: self.info.default_model.clone(),
            },
            ModelStreamEvent::ToolUseStart {
                id: id.clone().into(),
                name: name.into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            },
            ModelStreamEvent::ToolUseInputDelta {
                id: id.clone().into(),
                partial_json: input.to_string().into(),
            },
        ];
        if !self.omitted_stop {
            events.push(ModelStreamEvent::ToolUseStop { id: id.into() });
        }
        events.push(stop(ModelStopReason::ToolUse));
        Box::pin(futures::stream::iter(events))
    }
}

impl ModelProvider for WaitProvider {
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
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| {
            matches!(message,
            Message::User(user) if user.content.iter().any(|block| matches!(block,
                klaus_agent_core::schemas::content::ContentBlock::Text(text)
                    if text.text == CHILD_PROMPT)))
        });
        if is_child {
            self.child_calls.fetch_add(1, Ordering::SeqCst);
            let finish = self.finish_child.clone();
            let model = request.model.clone();
            let completed = self.scenario == Scenario::Completed;
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model };
                if !completed {
                    tokio::select! {
                        permit = finish.acquire() => { permit.unwrap().forget(); }
                        _ = signal.cancelled() => { return; }
                    }
                }
                yield ModelStreamEvent::TextDelta { text: CHILD_RESULT.into() };
                yield stop(ModelStopReason::EndTurn);
            });
        }
        let round = self.parent_calls.fetch_add(1, Ordering::SeqCst);
        if round == 0 && self.scenario != Scenario::Missing {
            return self.tool(
                if self.alias { "spawn_agent" } else { "task" },
                "spawn-child".into(),
                if self.alias {
                    json!({"model": "low", "task_name":"Observe a running child","message":CHILD_PROMPT,"fork_turns":"none","run_in_background":true})
                } else {
                    json!({"model": "low", "description":"Observe a running child","prompt":CHILD_PROMPT,"fork_turns":"none","run_in_background":true})
                },
            );
        }
        let child_id = request
            .messages
            .iter()
            .find_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id == "spawn-child" => {
                    result.output.as_ref()?.get("agentId")?.as_str().cloned()
                }
                _ => None,
            })
            .unwrap_or_else(|| "missing-child".into())
            .try_to_utf8()
            .unwrap();
        let got_final = request.messages.iter().any(|message| match message {
            Message::ToolResult(result) => result.output.as_ref().is_some_and(|output| {
                output
                    .get("retrieval_status")
                    .and_then(|value| value.as_str())
                    .is_some_and(|status| status == "success")
                    && output
                        .get("output")
                        .and_then(|value| value.as_str())
                        .is_some_and(|text| text.contains(CHILD_RESULT))
            }),
            _ => false,
        });
        if got_final && self.scenario == Scenario::Live {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: self.info.default_model.clone(),
                },
                ModelStreamEvent::TextDelta {
                    text: "The parent received the final child result.".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]));
        }
        let wait = self.waits.fetch_add(1, Ordering::SeqCst);
        if wait > WAIT_COUNT && self.scenario != Scenario::Live {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: request.model,
                },
                ModelStreamEvent::TextDelta {
                    text: "The observations finished.".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]));
        }
        assert!(
            wait < 30,
            "the live fixture must finish within its observation allowance"
        );
        if wait == WAIT_COUNT {
            self.finish_child.add_permits(1);
        }
        let block = self.scenario != Scenario::NonBlocking;
        let timeout = if self.scenario == Scenario::ZeroTimeout {
            0
        } else {
            50
        };
        self.tool(
            if self.alias {
                "wait_agent"
            } else {
                "task_output"
            },
            format!("wait-{wait}"),
            if self.alias {
                json!({"target":child_id,"timeout_ms":timeout})
            } else {
                json!({"task_id":child_id,"block":block,"timeout":timeout})
            },
        )
    }
}

async fn run(scenario: Scenario, alias: bool, omitted_stop: bool) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let model = if alias { "gpt-6.1-sol" } else { "wait-fixture" };
    let provider = Arc::new(WaitProvider {
        info: ProviderInfo {
            name: "wait-fixture".into(),
            default_model: model.into(),
            base_url: None,
            env_key: None,
        },
        scenario,
        alias,
        omitted_stop,
        parent_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        waits: AtomicUsize::new(0),
        finish_child: Arc::new(Semaphore::new(0)),
    });
    let directory = tempfile::tempdir().unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        create_task_output_tool(),
    ]);
    options.config.as_mut().unwrap()["model"] = json!(model);
    options.config.as_mut().unwrap()["loopDetection"] = json!(true);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let sink = notifications.clone();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        sink.lock().unwrap().push(receipt.clone());
    })));
    let events = tokio::time::timeout(
        Duration::from_secs(20),
        session
            .run_streamed(
                Input::Text("Delegate and wait for the result".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("parent and child finish within the fixture deadline");
    let history = session.history();
    let live = scenario == Scenario::Live;
    let failed = events.iter().any(|event| {
        matches!(event,
        EngineEvent::TurnFailed { error, .. } if error.code == EngineErrorCode::LoopDetected)
    });
    // Clean up the real child even when the pre-fix engine rejects the parent.
    provider.finish_child.add_permits(1);
    session.shutdown_and_wait().await.unwrap();
    for output in history.iter().filter_map(|message| match message {
        Message::ToolResult(result) if result.tool_use_id == "spawn-child" => {
            result.output.as_ref()?.get("outputFile")?.as_str().cloned()
        }
        _ => None,
    }) {
        if let Err(error) = std::fs::remove_file(output.try_to_utf8().unwrap()) {
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        }
    }
    if live {
        assert!(
            !failed,
            "a live wait must not trip the loop guard: {events:?}"
        );
        assert!(events.iter().all(|event| !matches!(event,
            EngineEvent::Notification { message, .. } if message.contains("Possible loop detected"))));
        assert!(events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )));
        assert!(provider.waits.load(Ordering::SeqCst) > WAIT_COUNT);
        assert_eq!(
            provider.child_calls.load(Ordering::SeqCst),
            1,
            "observation timeout must never restart the child"
        );
        let results: Vec<_> = history
            .iter()
            .filter_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id.starts_with("wait-") => {
                    result.output.as_ref()
                }
                _ => None,
            })
            .collect();
        assert!(
            results
                .iter()
                .filter(|output| output
                    .get("status")
                    .and_then(|value| value.as_str())
                    .is_some_and(|status| status == "running"))
                .count()
                >= WAIT_COUNT
        );
        assert!(results.iter().any(|output| output
            .get("retrieval_status")
            .and_then(|value| value.as_str())
            .is_some_and(|status| status == "success")));
        assert_eq!(
            notifications
                .lock()
                .unwrap()
                .iter()
                .filter(|receipt| receipt.status == "completed")
                .count(),
            1
        );
    } else {
        assert!(
            !failed,
            "repeated observations must remain tool results: {events:?}"
        );
        assert!(provider.waits.load(Ordering::SeqCst) > WAIT_COUNT);
    }
}

#[tokio::test]
async fn repeated_live_waits_survive_timeout_and_retrieve_the_final_result() {
    run(Scenario::Live, false, false).await;
}

#[tokio::test]
async fn visible_wait_alias_uses_the_same_live_target_semantics() {
    run(Scenario::Live, true, false).await;
}

#[tokio::test]
async fn live_wait_without_tool_stop_uses_the_same_classification() {
    run(Scenario::Live, false, true).await;
}

#[tokio::test]
async fn repeated_missing_target_queries_remain_tool_results() {
    run(Scenario::Missing, false, false).await;
}

#[tokio::test]
async fn repeated_completed_target_queries_remain_tool_results() {
    run(Scenario::Completed, false, false).await;
}

#[tokio::test]
async fn repeated_nonblocking_queries_do_not_abort_the_turn() {
    run(Scenario::NonBlocking, false, false).await;
}

#[tokio::test]
async fn repeated_zero_timeout_queries_do_not_abort_the_turn() {
    run(Scenario::ZeroTimeout, false, false).await;
}
