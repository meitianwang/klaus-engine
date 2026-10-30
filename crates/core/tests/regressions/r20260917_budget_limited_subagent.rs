//! Defect: a child that stopped on a configured budget (turn cap or the session
//! spend cap) was indistinguishable from a crashed child — the lifecycle row
//! said `failed`, the parent's task tool saw no failure at all (so it treated
//! the child's half-finished text as a complete answer), and the resume copy
//! admitted the ambiguity with "turn or spending limit".
//! Root cause: one shared `TerminalReason::BudgetExhausted` fed a `failed`
//! lifecycle transition and a TurnFailed-only failure extractor.
//! Fixed by: 2026-09-17 split budget semantics — split terminal reasons
//! (`turn_limit_exhausted` / `spend_limit_exhausted`), a dedicated
//! `budget_limited` lifecycle status that carries the child's partial result,
//! and a foreground failure message that names the ceiling instead of
//! reporting success.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::schemas::subagent_lifecycle::SubagentStatus;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

struct CappedProvider {
    info: ProviderInfo,
    spawned: AtomicBool,
    child_calls: AtomicUsize,
}

impl ModelProvider for CappedProvider {
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
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block,
                ContentBlock::Text(text) if text.text.contains("capped-child-directive"))
            }),
            _ => false,
        });
        if is_child {
            // The child keeps calling its tool forever: with maxTurns=2 the
            // only thing that can end it is the turn ceiling.
            let call = self.child_calls.fetch_add(1, Ordering::SeqCst);
            let tool_id = RuntimeString::from(format!("child-read-{call}"));
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "capped-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: format!("partial work {call}").into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: tool_id.clone(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: tool_id.clone(),
                    partial_json: json!({"file_path":"missing.txt"}).to_string().into(),
                },
                ModelStreamEvent::ToolUseStop { id: tool_id },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: usage(),
                },
            ]));
        }
        if !self.spawned.swap(true, Ordering::SeqCst) {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "capped-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "spawn-capped".into(),
                    name: "task".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "spawn-capped".into(),
                    partial_json: json!({"model": "low", "description":"capped child","prompt":"capped-child-directive","subagent_type":"capped-fixture","run_in_background":false}).to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "spawn-capped".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage: usage(),
                },
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "capped-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "parent done".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: usage(),
            },
        ]))
    }
}

#[tokio::test]
async fn budget_capped_child_is_budget_limited_not_failed() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(CappedProvider {
        info: ProviderInfo {
            name: "capped-test".into(),
            default_model: "capped-test".into(),
            base_url: None,
            env_key: None,
        },
        spawned: AtomicBool::new(false),
        child_calls: AtomicUsize::new(0),
    });
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions::default()),
        create_read_tool(),
    ]);
    options.agents = Some(vec![AgentDefinition {
        name: "capped-fixture".into(),
        description: "A fixture agent that loops until its turn cap".into(),
        tools: Some(vec!["read".into()]),
        max_turns: Some(2),
        ..Default::default()
    }]);
    let directory =
        std::env::temp_dir().join(format!("klaus-budget-limited-{}", uuid::Uuid::new_v4()));
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let agent = Agent::new(options);
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                id: Some("budget-parent".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // The parent's own tool result must NAME the ceiling: before the fix the
    // parent saw `ok=true` plus the child's half-finished text and believed it.
    let mut parent_report: Option<String> = None;
    let mut events = session
        .run_streamed(
            Input::Text("Delegate to the capped child".into()),
            TurnOptions::default(),
        )
        .events;
    while let Some(event) = events.next().await {
        // Tool items settle through ItemUpdated, not ItemCompleted.
        let item = match &event {
            EngineEvent::ItemCompleted { item, .. } => Some(item),
            EngineEvent::ItemUpdated { item, .. } => Some(item),
            _ => None,
        };
        if let Some(ThreadItem::ToolCall {
            tool_name,
            error,
            result,
            ..
        }) = item
        {
            if tool_name.try_to_utf8().ok().as_deref() == Some("task") && error.is_some() {
                parent_report = Some(format!("{error:?} {result:?}"));
            }
        }
    }
    let failure = parent_report.expect("the task tool reports the budget ceiling");
    assert!(
        failure.contains("turn limit"),
        "the parent is told WHICH ceiling stopped the child, got: {failure}"
    );

    // The lifecycle row is budget_limited (terminal, not failed) and carries
    // the child's partial text as its result.
    let snapshot = session.subagent_lifecycle_snapshot();
    let record = snapshot
        .agents
        .iter()
        .find(|record| record.status != SubagentStatus::Spawned)
        .expect("one child ran");
    assert_eq!(record.status, SubagentStatus::BudgetLimited);
    let result = record
        .last_result
        .as_ref()
        .expect("the partial result survives for the parent");
    assert!(result.try_to_utf8().unwrap().contains("partial work"));

    // The durable copy says the same thing, so a resumed session cannot
    // misread the stop as a crash.
    session.shutdown_and_wait().await.unwrap();
    let resumed = store.resume("budget-parent").await.unwrap();
    let persisted = resumed.raw_items.iter().rev().find_map(|item| match item {
        RolloutItem::SubagentLifecycle(entry) => Some(entry.record.clone()),
        _ => None,
    });
    let persisted = persisted.expect("the lifecycle record is durable");
    assert_eq!(persisted.status, SubagentStatus::BudgetLimited);
    assert!(persisted.last_result.is_some());

    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
