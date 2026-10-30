//! Defect: parents occupying every sub-agent slot could wait forever for
//! synchronous descendants, while interrupted children returned empty success.
//! Root cause: saturated admission queued descendants behind their waiting
//! parents, and foreground result extraction ignored aborted terminal events.
//! Work item: 2026-10-14 bounded sub-agent admission and interruption outcomes.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::subagent_lifecycle::SubagentStatus;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::swarm_tools::create_send_message_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_stop_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind, ToolError};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskStatus, SwarmIdentity, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

const TIMEOUT: Duration = Duration::from_secs(5);
// A fresh process hashes the aggregate test executable before its first child.
const SETUP_TIMEOUT: Duration = Duration::from_secs(20);
const MODEL: &str = "capacity-test";

#[derive(Debug)]
struct ToolReceipt {
    is_error: bool,
    text: String,
}

fn tool_receipt(request: &ModelRequest, id: &str) -> Option<ToolReceipt> {
    let receipt = |is_error, content: &[OutputBlock]| ToolReceipt {
        is_error,
        text: content
            .iter()
            .filter_map(|block| match block {
                OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    };
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => {
                Some(receipt(result.is_error, &result.content))
            }
            Message::User(message) => message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult(result) if result.tool_use_id == id => {
                    Some(receipt(result.is_error == Some(true), &result.content))
                }
                _ => None,
            }),
            _ => None,
        })
}

fn has_directive(request: &ModelRequest, directive: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text.text == directive)),
        _ => false,
    })
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

fn task_events(calls: Vec<(String, Value)>) -> Vec<ModelStreamEvent> {
    tool_events("task", calls)
}

fn tool_events(name: &str, calls: Vec<(String, Value)>) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: MODEL.into(),
    }];
    for (id, input) in calls {
        events.extend([
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
            ModelStreamEvent::ToolUseStop { id: id.into() },
        ]);
    }
    events.push(stop(ModelStopReason::ToolUse));
    events
}

fn final_events(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stop(ModelStopReason::EndTurn),
    ]
}

fn info() -> ProviderInfo {
    ProviderInfo {
        name: MODEL.into(),
        default_model: MODEL.into(),
        base_url: None,
        env_key: None,
    }
}

fn capabilities() -> ProviderCapabilities {
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

struct SaturatedProvider {
    info: ProviderInfo,
    parent_count: usize,
    root_calls: AtomicUsize,
    parent_calls: Vec<AtomicUsize>,
    nested_calls: AtomicUsize,
    overflow_calls: AtomicUsize,
    recovered_calls: AtomicUsize,
    entered: mpsc::UnboundedSender<usize>,
    nested_receipts: mpsc::UnboundedSender<(usize, ToolReceipt)>,
    overflow_receipts: mpsc::UnboundedSender<ToolReceipt>,
    start_nested: Arc<Semaphore>,
    finish_parents: Arc<Semaphore>,
}

impl ModelProvider for SaturatedProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        capabilities()
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        for index in 0..self.parent_count {
            if has_directive(&request, &format!("capacity-nested-{index}")) {
                self.nested_calls.fetch_add(1, Ordering::SeqCst);
                return Box::pin(futures::stream::iter(final_events("nested child ran")));
            }
            if !has_directive(&request, &format!("capacity-parent-{index}")) {
                continue;
            }
            if self.parent_calls[index].fetch_add(1, Ordering::SeqCst) == 0 {
                let _ = self.entered.send(index);
                let start = self.start_nested.clone();
                return Box::pin(async_stream::stream! {
                    tokio::select! {
                        permit = start.acquire() => permit.unwrap().forget(),
                        _ = signal.cancelled() => return,
                    }
                    for event in task_events(vec![(format!("nested-{index}"), json!({"model": "low",
                        "description": "synchronous nested child",
                        "prompt": format!("capacity-nested-{index}"),
                        "fork_turns":"none",
                        "run_in_background": false
                    }))]) { yield event; }
                });
            }
            let receipt = tool_receipt(&request, &format!("nested-{index}"))
                .expect("the parent receives its nested task result");
            let _ = self.nested_receipts.send((index, receipt));
            let finish = self.finish_parents.clone();
            return Box::pin(async_stream::stream! {
                tokio::select! {
                    permit = finish.acquire() => permit.unwrap().forget(),
                    _ = signal.cancelled() => return,
                }
                for event in final_events("parent finished after rejected child") { yield event; }
            });
        }
        if has_directive(&request, "capacity-overflow-child") {
            self.overflow_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter(final_events("overflow child ran")));
        }
        if has_directive(&request, "capacity-recovered-child") {
            self.recovered_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter(final_events("recovered slot ran")));
        }
        let root_call = self.root_calls.fetch_add(1, Ordering::SeqCst);
        if root_call == 3 || root_call == 5 {
            let id = if root_call == 3 {
                "spawn-overflow"
            } else {
                "spawn-recovered"
            };
            let _ = self
                .overflow_receipts
                .send(tool_receipt(&request, id).expect("the root receives the creation result"));
        }
        let calls = match root_call {
            0 => Some(
                (0..self.parent_count)
                    .map(|index| {
                        (
                            format!("spawn-parent-{index}"),
                            json!({"model": "low",
                                "description": format!("capacity parent {index}"),
                                "prompt": format!("capacity-parent-{index}"),
                                "fork_turns":"none",
                                "run_in_background": true
                            }),
                        )
                    })
                    .collect(),
            ),
            2 => Some(vec![(
                "spawn-overflow".into(),
                json!({"model": "low",
                    "description": "overflow child", "prompt": "capacity-overflow-child",
                    "fork_turns":"none", "run_in_background": true
                }),
            )]),
            4 => Some(vec![(
                "spawn-recovered".into(),
                json!({"model": "low",
                    "description": "recovered slot child", "prompt": "capacity-recovered-child",
                    "fork_turns":"none", "run_in_background": true
                }),
            )]),
            _ => None,
        };
        Box::pin(futures::stream::iter(match calls {
            Some(calls) => task_events(calls),
            None => final_events("root turn finished"),
        }))
    }
}

async fn run_root(session: &Arc<Session>, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        let mut events = session
            .run_streamed(
                Input::Text("continue capacity fixture".into()),
                TurnOptions::default(),
            )
            .events;
        while let Some(event) = events.next().await {
            if let klaus_agent_core::types::events::EngineEvent::TurnFailed { error, .. } = event {
                panic!("the root turn failed: {error:?}");
            }
        }
    })
    .await
    .expect("the root turn does not wait for detached children");
}

async fn wait_until(mut ready: impl FnMut() -> bool, reason: &str) {
    tokio::time::timeout(TIMEOUT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{reason}"));
}

async fn nested_fixture(
    parent_count: usize,
    child_capacity: usize,
    max_depth: u32,
    nested_admitted: bool,
) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (overflow_tx, mut overflow_receipts) = mpsc::unbounded_channel();
    let provider = Arc::new(SaturatedProvider {
        info: info(),
        parent_count,
        root_calls: AtomicUsize::new(0),
        parent_calls: (0..parent_count).map(|_| AtomicUsize::new(0)).collect(),
        nested_calls: AtomicUsize::new(0),
        overflow_calls: AtomicUsize::new(0),
        recovered_calls: AtomicUsize::new(0),
        entered: entered_tx,
        nested_receipts: receipt_tx,
        overflow_receipts: overflow_tx,
        start_nested: Arc::new(Semaphore::new(0)),
        finish_parents: Arc::new(Semaphore::new(0)),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    let config = options.config.as_mut().unwrap();
    config["model"] = json!(MODEL);
    config["maxConcurrentSubAgents"] = json!(child_capacity);
    config["maxSubAgentDepth"] = json!(max_depth);
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    let directory = std::env::temp_dir().join(format!("klaus-capacity-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let directory_host = directory.to_string_lossy().into_owned();
    let session = Agent::new(options)
        .open_session(
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
                ..Default::default()
            })),
            StartSessionOptions {
                cwd: Some(directory_host.clone().into()),
                user_config_dir: Some(directory_host.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let outcome = std::panic::AssertUnwindSafe(async {
        run_root(&session, SETUP_TIMEOUT).await;
        for _ in 0..parent_count {
            tokio::time::timeout(TIMEOUT, entered.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            session
                .subagent_lifecycle_snapshot()
                .agents
                .iter()
                .filter(|record| record.status == SubagentStatus::Running)
                .count(),
            parent_count,
            "all parents are running before testing nested admission"
        );

        // Hold every parent until every synchronous nested call has returned. This
        // rules out a later nested call borrowing a slot from an earlier finisher.
        provider.start_nested.add_permits(parent_count);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..parent_count {
            let (index, receipt) = tokio::time::timeout(TIMEOUT, receipts.recv())
                .await
                .expect("a saturated nested request returns instead of deadlocking")
                .unwrap();
            assert!(
                seen.insert(index),
                "each parent returns exactly one nested result"
            );
            if nested_admitted {
                assert!(!receipt.is_error, "{receipt:?}");
                assert!(receipt.text.contains("nested child ran"), "{receipt:?}");
            } else {
                assert!(receipt.is_error, "{receipt:?}");
                assert!(
                    receipt.text.contains(&format!(
                        "Sub-agent execution capacity reached (maximum {child_capacity})"
                    )),
                    "{receipt:?}"
                );
            }
        }
        assert_eq!(
            provider.nested_calls.load(Ordering::SeqCst),
            if nested_admitted { parent_count } else { 0 }
        );

        if !nested_admitted {
            // Rejected creation returns an error before publishing any identity.
            let before = session.subagent_lifecycle_snapshot();
            run_root(&session, TIMEOUT).await;
            let receipt = tokio::time::timeout(TIMEOUT, overflow_receipts.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(receipt.is_error, "{receipt:?}");
            assert!(
                receipt.text.contains(&format!(
                    "Sub-agent execution capacity reached (maximum {child_capacity})"
                )),
                "{receipt:?}"
            );
            assert_eq!(
                session
                    .subagent_lifecycle_snapshot()
                    .agents
                    .iter()
                    .map(|entry| entry.agent_id.clone())
                    .collect::<std::collections::HashSet<_>>(),
                before
                    .agents
                    .iter()
                    .map(|entry| entry.agent_id.clone())
                    .collect::<std::collections::HashSet<_>>()
            );
            assert!(!session
                .background_agent_summaries()
                .iter()
                .any(|entry| { entry.description == "capacity-overflow-child" }));
            assert_eq!(provider.overflow_calls.load(Ordering::SeqCst), 0);
        }

        provider.finish_parents.add_permits(parent_count);
        wait_until(
            || {
                session
                    .background_agent_summaries()
                    .iter()
                    .filter(|entry| {
                        entry.description.starts_with("capacity-parent-")
                            && entry.status == BackgroundTaskStatus::Completed
                    })
                    .count()
                    == parent_count
            },
            "every parent can finish after the rejected nested request",
        )
        .await;

        if nested_admitted {
            return;
        }
        run_root(&session, TIMEOUT).await;
        let recovered = tokio::time::timeout(TIMEOUT, overflow_receipts.recv()).await.unwrap().unwrap();
        assert!(!recovered.is_error, "released capacity must admit the replacement: {recovered:?}; agents={:?}; lifecycle={:?}", session.background_agent_summaries(), session.subagent_lifecycle_snapshot());
        wait_until(
            || provider.recovered_calls.load(Ordering::SeqCst) == 1,
            "a completed parent releases its admission slot",
        )
        .await;
        wait_until(
            || {
                session.background_agent_summaries().iter().any(|entry| {
                    entry.description == "capacity-recovered-child"
                        && entry.status == BackgroundTaskStatus::Completed
                })
            },
            "the replacement child completes in the recovered slot",
        )
        .await;
    })
    .catch_unwind()
    .await;
    let output_files: Vec<_> = session
        .background_agent_summaries()
        .into_iter()
        .map(|entry| entry.output_file)
        .collect();
    session.interrupt();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    for output in output_files {
        if std::path::Path::new(&output).exists() {
            std::fs::remove_file(output).unwrap();
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_parent_rejects_nested_spawn_at_capacity_and_recovers_the_slot() {
    nested_fixture(1, 1, 3, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_parents_reject_nested_spawns_without_deadlocking() {
    nested_fixture(4, 4, 3, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_spawn_ignores_legacy_depth_when_capacity_is_available() {
    nested_fixture(1, 2, 1, true).await;
}

struct InterruptTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for InterruptTool {
    fn static_name(&self) -> &str {
        "fixture_interrupt"
    }
    fn static_description(&self) -> &str {
        "Interrupt this fixture's child turn."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        ctx.signal.cancel();
        Ok(text_result(Json::Bool(true), "interrupted fixture", false))
    }
}

struct InterruptedProvider {
    info: ProviderInfo,
    root_calls: AtomicUsize,
    receipt: mpsc::UnboundedSender<ToolReceipt>,
}

impl ModelProvider for InterruptedProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        capabilities()
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        if has_directive(&request, "capacity-interrupted-child") {
            return Box::pin(futures::stream::iter(tool_events(
                "fixture_interrupt",
                vec![("interrupt-child".into(), json!({}))],
            )));
        }
        if self.root_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Box::pin(futures::stream::iter(task_events(vec![(
                "spawn-interrupted".into(),
                json!({"model": "low",
                    "description": "interrupted foreground child",
                    "prompt": "capacity-interrupted-child", "fork_turns":"none",
                    "run_in_background": false
                }),
            )])));
        }
        let receipt = tool_receipt(&request, "spawn-interrupted")
            .expect("the root receives its interrupted child's result");
        let _ = self.receipt.send(receipt);
        Box::pin(futures::stream::iter(final_events(
            "root handled interruption",
        )))
    }
}

#[tokio::test]
async fn interrupted_foreground_child_returns_failure_instead_of_empty_success() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(InterruptedProvider {
        info: info(),
        root_calls: AtomicUsize::new(0),
        receipt: receipt_tx,
    });
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        Arc::new(InterruptTool {
            schema: json!({"type": "object", "properties": {}})
                .as_object()
                .unwrap()
                .clone(),
        }),
    ]);
    options.config.as_mut().unwrap()["model"] = json!(MODEL);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let directory = std::env::temp_dir().join(format!("klaus-capacity-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let directory_host = directory.to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory_host.clone().into()),
        user_config_dir: Some(directory_host.into()),
        ..Default::default()
    });
    let outcome = std::panic::AssertUnwindSafe(async {
        run_root(&session, SETUP_TIMEOUT).await;
        let receipt = tokio::time::timeout(TIMEOUT, receipts.recv())
            .await
            .expect("the root receives the interruption result promptly")
            .unwrap();
        assert!(receipt.is_error, "{receipt:?}");
        assert!(receipt.text.contains("interrupted"), "{receipt:?}");
        assert!(!receipt.text.contains("produced no output"), "{receipt:?}");
        let records = session.subagent_lifecycle_snapshot().agents;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, SubagentStatus::Cancelled);
    })
    .catch_unwind()
    .await;
    let output_files: Vec<_> = session
        .background_agent_summaries()
        .into_iter()
        .map(|entry| entry.output_file)
        .collect();
    session.interrupt();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    for output in output_files {
        if std::path::Path::new(&output).exists() {
            std::fs::remove_file(output).unwrap();
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

struct MailboxProvider {
    info: ProviderInfo,
    root_calls: AtomicUsize,
    peer_calls: AtomicUsize,
    cancelled_peer_calls: AtomicUsize,
    initial_failures: AtomicUsize,
    blocker_entered: mpsc::UnboundedSender<()>,
    finish_blocker: Arc<Semaphore>,
    stop_target: Arc<Mutex<Option<String>>>,
    receipts: mpsc::UnboundedSender<(&'static str, ToolReceipt)>,
}

fn contains_directive(request: &ModelRequest, directive: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message.content.iter().any(
            |block| matches!(block, ContentBlock::Text(text) if text.text.contains(directive)),
        ),
        _ => false,
    })
}

impl ModelProvider for MailboxProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        capabilities()
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let is_root = has_directive(&request, "continue capacity fixture");
        if !is_root && contains_directive(&request, "capacity-mailbox-blocker") {
            let _ = self.blocker_entered.send(());
            let finish = self.finish_blocker.clone();
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: MODEL.into() };
                tokio::select! {
                    permit = finish.acquire() => permit.unwrap().forget(),
                    _ = signal.cancelled() => return,
                }
                yield ModelStreamEvent::TextDelta { text: "blocker complete".into() };
                yield stop(ModelStopReason::EndTurn);
            });
        }
        if !is_root
            && (contains_directive(&request, "capacity-mailbox-retry-initial")
                || contains_directive(&request, "capacity-mailbox-stop-initial"))
            && !contains_directive(&request, "capacity-mailbox-retry-direction")
        {
            self.initial_failures.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::InvalidRequest,
                    message: "persistent peer initial request failed".into(),
                    status: Some(400),
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    retry_after_ms: None,
                    request_id: None,
                },
            }]));
        }
        if !is_root && contains_directive(&request, "capacity-mailbox-retry") {
            self.peer_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter(final_events(
                "mailbox retry complete",
            )));
        }
        if !is_root && contains_directive(&request, "capacity-mailbox-stop-initial") {
            self.cancelled_peer_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter(final_events(
                "stopped peer unexpectedly ran",
            )));
        }
        let call = self.root_calls.fetch_add(1, Ordering::SeqCst);
        if call == 3 {
            for id in ["spawn-retry-peer", "spawn-stop-peer"] {
                let receipt = tool_receipt(&request, id)
                    .expect("the root receives each persistent creation result");
                let _ = self.receipts.send((id, receipt));
            }
        }
        let receipt_id = match call {
            5 => Some("stop-failed-peer"),
            7 => Some("retry-peer-message"),
            9 => Some("stop-recovered-peer"),
            _ => None,
        };
        if let Some(id) = receipt_id {
            let receipt =
                tool_receipt(&request, id).expect("the root receives its coordination tool result");
            let _ = self.receipts.send((id, receipt));
        }
        let events = match call {
            0 => task_events(vec![(
                "spawn-mailbox-blocker".into(),
                json!({"model": "low",
                    "description": "mailbox capacity blocker", "prompt": "capacity-mailbox-blocker",
                    "fork_turns":"none", "run_in_background": true
                }),
            )]),
            2 => task_events(vec![
                (
                    "spawn-retry-peer".into(),
                    json!({"model": "low",
                        "description": "retry peer", "prompt": "capacity-mailbox-retry-initial",
                        "peer": "retry-peer", "persistent": true,
                        "fork_turns":"none", "run_in_background": true
                    }),
                ),
                (
                    "spawn-stop-peer".into(),
                    json!({"model": "low",
                        "description": "stop peer", "prompt": "capacity-mailbox-stop-initial",
                        "peer": "stop-peer", "persistent": true,
                        "fork_turns":"none", "run_in_background": true
                    }),
                ),
            ]),
            4 => tool_events(
                "task_stop",
                vec![(
                    "stop-failed-peer".into(),
                    json!({
                        "task_id": self.stop_target.lock().unwrap().clone().unwrap()
                    }),
                )],
            ),
            6 => tool_events(
                "SendMessage",
                vec![(
                    "retry-peer-message".into(),
                    json!({
                        "to": "retry-peer", "summary": "Retry after capacity becomes available",
                        "message": "capacity-mailbox-retry-direction"
                    }),
                )],
            ),
            8 => tool_events(
                "task_stop",
                vec![(
                    "stop-recovered-peer".into(),
                    json!({
                        "task_id": self.stop_target.lock().unwrap().clone().unwrap()
                    }),
                )],
            ),
            _ => final_events("mailbox fixture root complete"),
        };
        Box::pin(futures::stream::iter(events))
    }
}

struct ConfigDirRestore(Option<std::ffi::OsString>);

impl Drop for ConfigDirRestore {
    fn drop(&mut self) {
        match &self.0 {
            Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                previous,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "KLAUS_CONFIG_DIR",
            ),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_persistent_peers_can_retry_by_mailbox_or_stop_while_failed() {
    let directory =
        std::env::temp_dir().join(format!("klaus-capacity-mailbox-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let _restore = ConfigDirRestore(std::env::var_os("KLAUS_CONFIG_DIR"));
    klaus_agent_core::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &directory);
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let stop_target = Arc::new(Mutex::new(None));
    let provider = Arc::new(MailboxProvider {
        info: info(),
        root_calls: AtomicUsize::new(0),
        peer_calls: AtomicUsize::new(0),
        cancelled_peer_calls: AtomicUsize::new(0),
        initial_failures: AtomicUsize::new(0),
        blocker_entered: entered_tx,
        finish_blocker: Arc::new(Semaphore::new(0)),
        stop_target: stop_target.clone(),
        receipts: receipt_tx,
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        create_send_message_tool(),
        create_task_stop_tool(),
    ]);
    let config = options.config.as_mut().unwrap();
    config["model"] = json!(MODEL);
    config["maxConcurrentSubAgents"] = json!(3);
    config["swarm"] = json!(true);
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    let directory_host = directory.to_string_lossy().into_owned();
    let session = Agent::new(options)
        .open_session(
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
                ..Default::default()
            })),
            StartSessionOptions {
                cwd: Some(directory_host.clone().into()),
                user_config_dir: Some(directory_host.into()),
                swarm: Some(SwarmIdentity {
                    team: "capacity-mailbox-team".into(),
                    peer: "lead".into(),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let outcome = std::panic::AssertUnwindSafe(async {
        run_root(&session, SETUP_TIMEOUT).await;
        tokio::time::timeout(TIMEOUT, entered.recv())
            .await
            .unwrap()
            .unwrap();
        run_root(&session, TIMEOUT).await;
        for expected in ["spawn-retry-peer", "spawn-stop-peer"] {
            let (id, receipt) = tokio::time::timeout(TIMEOUT, receipts.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(id, expected);
            assert!(
                !receipt.is_error,
                "the peer must be admitted before its provider fails: {receipt:?}"
            );
        }
        wait_until(
            || {
                session
                    .background_agent_summaries()
                    .iter()
                    .filter(|entry| {
                        entry.teammate && entry.idle && entry.status == BackgroundTaskStatus::Failed
                    })
                    .count()
                    == 2
            },
            "both admitted peers report provider failure while their mailbox readers remain parked",
        )
        .await;
        assert_eq!(provider.initial_failures.load(Ordering::SeqCst), 2);
        assert_eq!(provider.peer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.cancelled_peer_calls.load(Ordering::SeqCst), 0);
        let peers = session.background_agent_summaries();
        let retry = peers
            .iter()
            .find(|entry| entry.description == "capacity-mailbox-retry-initial")
            .unwrap();
        let stopped = peers
            .iter()
            .find(|entry| entry.description == "capacity-mailbox-stop-initial")
            .unwrap();
        for peer in [retry, stopped] {
            let record = session
                .subagent_lifecycle_snapshot()
                .agents
                .into_iter()
                .find(|record| record.agent_id == peer.id.as_str())
                .unwrap();
            assert_eq!(
                record.status,
                SubagentStatus::Failed,
                "parking a failed mailbox reader must not overwrite its lifecycle with Idle"
            );
            let output = std::fs::read_to_string(&peer.output_file).unwrap();
            assert!(
                output.contains("persistent peer initial request failed"),
                "{output}"
            );
        }
        *stop_target.lock().unwrap() = Some(stopped.id.clone());
        run_root(&session, TIMEOUT).await;
        let (id, receipt) = tokio::time::timeout(TIMEOUT, receipts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id, "stop-failed-peer");
        assert!(
            !receipt.is_error,
            "a failed persistent reader still needs to be stoppable: {receipt:?}"
        );
        wait_until(
            || {
                session
                    .subagent_lifecycle_snapshot()
                    .agents
                    .iter()
                    .any(|record| {
                        record.agent_id == stopped.id.as_str()
                            && record.status == SubagentStatus::Cancelled
                    })
            },
            "stopping a failed persistent reader preserves its Cancelled lifecycle",
        )
        .await;

        provider.finish_blocker.add_permits(1);
        wait_until(
            || {
                session.background_agent_summaries().iter().any(|entry| {
                    entry.description == "capacity-mailbox-blocker"
                        && entry.status == BackgroundTaskStatus::Completed
                })
            },
            "the blocker releases its capacity slot",
        )
        .await;
        run_root(&session, TIMEOUT).await;
        let (id, receipt) = tokio::time::timeout(TIMEOUT, receipts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id, "retry-peer-message");
        assert!(
            !receipt.is_error,
            "the failed admitted peer remains addressable by name: {receipt:?}"
        );
        wait_until(
            || {
                session
                    .subagent_lifecycle_snapshot()
                    .agents
                    .iter()
                    .any(|record| {
                        record.agent_id == retry.id.as_str()
                            && record.status == SubagentStatus::Idle
                            && record
                                .last_result
                                .as_ref()
                                .is_some_and(|result| result.contains("mailbox retry complete"))
                    })
            },
            "the mailbox retry runs once a slot becomes available and returns to idle",
        )
        .await;
        assert_eq!(provider.peer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.cancelled_peer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.initial_failures.load(Ordering::SeqCst), 2);
        assert_eq!(
            session
                .subagent_lifecycle_snapshot()
                .agents
                .iter()
                .find(|record| record.agent_id == stopped.id.as_str())
                .unwrap()
                .status,
            SubagentStatus::Cancelled,
            "a cancelled reader must not be overwritten by Idle"
        );

        *stop_target.lock().unwrap() = Some(retry.id.clone());
        run_root(&session, TIMEOUT).await;
        let recovered = tokio::time::timeout(TIMEOUT, async {
            loop {
                let (id, receipt) = receipts.recv().await.unwrap();
                if id == "stop-recovered-peer" {
                    break receipt;
                }
            }
        })
        .await
        .unwrap();
        assert!(!recovered.is_error, "{recovered:?}");
        wait_until(
            || {
                session
                    .subagent_lifecycle_snapshot()
                    .agents
                    .iter()
                    .any(|record| {
                        record.agent_id == retry.id.as_str()
                            && record.status == SubagentStatus::Cancelled
                    })
            },
            "the recovered persistent reader can be stopped after returning idle",
        )
        .await;
    })
    .catch_unwind()
    .await;
    let output_files: Vec<_> = session
        .background_agent_summaries()
        .into_iter()
        .map(|entry| entry.output_file)
        .collect();
    session.interrupt();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    for output in output_files {
        if std::path::Path::new(&output).exists() {
            std::fs::remove_file(output).unwrap();
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
