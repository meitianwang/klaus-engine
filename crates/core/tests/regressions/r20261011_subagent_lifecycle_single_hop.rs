//! Defect: a nested child's lifecycle rows landed in the ROOT session rollout
//! without turn attribution, and its terminal receipt entered the root ledger
//! with no owning turn, stranding one row at the transcript tail forever.
//! Root cause: lifecycle rows always wrote to the spawning Session object's
//! rollout (the root for every depth) and completion receipts stamped the
//! session's currently-active turn, which is empty once the turn closed.
//! Fixed by: 2026-10-11 subagent lifecycle attribution work item — rows settle
//! one hop from their spawner, receipts carry the stamped spawn turn, and a
//! nested child's terminal receipt never enters the root ledger.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::TurnOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

struct NestedBackgroundProvider {
    spawned: AtomicBool,
    child_spawned: AtomicBool,
    finish_child: Arc<Semaphore>,
}

impl klaus_agent_core::types::provider::ModelProvider for NestedBackgroundProvider {
    fn info(&self) -> &klaus_agent_core::types::provider::ProviderInfo {
        static INFO: std::sync::OnceLock<klaus_agent_core::types::provider::ProviderInfo> =
            std::sync::OnceLock::new();
        INFO.get_or_init(|| klaus_agent_core::types::provider::ProviderInfo {
            name: "single-hop-test".into(),
            default_model: "single-hop-test".into(),
            base_url: None,
            env_key: None,
        })
    }
    fn capabilities(
        &self,
        _: &RuntimeString,
    ) -> klaus_agent_core::types::provider::ProviderCapabilities {
        klaus_agent_core::types::provider::ProviderCapabilities {
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
    fn stream(
        &self,
        request: klaus_agent_core::types::provider::ModelRequest,
        signal: klaus_agent_core::types::abort::AbortSignal,
    ) -> klaus_agent_core::types::provider::ModelStream {
        use klaus_agent_core::schemas::content::ContentBlock;
        use klaus_agent_core::schemas::message::Message;
        use klaus_agent_core::schemas::message::Usage;
        use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};
        let stopped = |reason: ModelStopReason| ModelStreamEvent::MessageStop {
            stop_reason: reason,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 4,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        };
        let has = |needle: &str| {
            request.messages.iter().any(|message| match message {
                Message::User(message) => message.content.iter().any(
                    |block| matches!(block, ContentBlock::Text(text) if text.text.contains(needle)),
                ),
                _ => false,
            })
        };
        // The direct child spawns ONE nested (depth-2) foreground child, then
        // waits for the release before finishing: its terminal lands after the
        // parent turn already closed.
        if has("single-hop-child") {
            if !self.child_spawned.swap(true, Ordering::SeqCst) {
                let input = serde_json::json!({"model": "low", "description":"nested probe","prompt":"single-hop-grandchild","run_in_background":false});
                return Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "single-hop-test".into(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "spawn-grand".into(),
                        name: "task".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "spawn-grand".into(),
                        partial_json: input.to_string().into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "spawn-grand".into(),
                    },
                    stopped(ModelStopReason::ToolUse),
                ]));
            }
            let finish = self.finish_child.clone();
            return Box::pin(async_stream::stream! {
                yield ModelStreamEvent::MessageStart { model: "single-hop-test".into() };
                yield ModelStreamEvent::TextDelta { text: "child final".into() };
                tokio::select! {
                    permit = finish.acquire() => { let _permit = permit.unwrap(); }
                    _ = signal.cancelled() => { return; }
                }
                yield stopped(ModelStopReason::EndTurn);
            });
        }
        if has("single-hop-grandchild") {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "single-hop-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "grandchild final".into(),
                },
                stopped(ModelStopReason::EndTurn),
            ]));
        }
        if !self.spawned.swap(true, Ordering::SeqCst) {
            let input = serde_json::json!({"model": "low", "description":"single hop","prompt":"single-hop-child","run_in_background":true});
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "single-hop-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "spawn-child".into(),
                    name: "task".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "spawn-child".into(),
                    partial_json: input.to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "spawn-child".into(),
                },
                stopped(ModelStopReason::ToolUse),
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "single-hop-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "parent done".into(),
            },
            stopped(ModelStopReason::EndTurn),
        ]))
    }
}

fn lifecycle_rows(path: &std::path::Path) -> Vec<Value> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|row| row["kind"] == "subagent_lifecycle")
        .collect()
}

fn walk_jsonl(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_jsonl(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "jsonl") {
            out.push(path);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_lifecycle_rows_settle_one_hop_and_carry_their_spawn_turn() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let finish_child = Arc::new(Semaphore::new(0));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = Arc::new(NestedBackgroundProvider {
        spawned: AtomicBool::new(false),
        child_spawned: AtomicBool::new(false),
        finish_child: finish_child.clone(),
    });
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    options.config.as_mut().unwrap()["forwardSubagentText"] = serde_json::json!(false);
    let agent = Agent::new(options);
    let directory = std::env::temp_dir().join(format!("klaus-single-hop-{}", uuid::Uuid::new_v4()));
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                id: Some("single-hop-root".into()),
                user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (receipt_send, mut receipt_receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        let _ = receipt_send.send(receipt.clone());
    })));
    let stream = session.run_streamed(
        klaus_agent_core::engine::session::Input::Text("start single hop".into()),
        TurnOptions::default(),
    );
    let mut events = stream.events;
    let mut root_turn_id: Option<RuntimeString> = None;
    while let Some(event) = events.next().await {
        if let klaus_agent_core::types::events::EngineEvent::TurnStarted { turn_id } = &event {
            root_turn_id = Some(turn_id.clone());
        }
    }
    let root_turn_id = root_turn_id.expect("the root turn announced its id");
    // The background child is still parked on the semaphore: release it so its
    // terminal lands AFTER the root turn closed.
    finish_child.add_permits(1);
    let mut receipts = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), receipt_receive.recv()).await {
            Ok(Some(receipt)) => {
                let done = receipt.status == "completed" || receipt.status == "failed";
                receipts.push(receipt);
                if done {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(
        receipts.iter().any(|receipt| receipt.status == "completed"),
        "the direct child's terminal receipt must reach the root ledger: {receipts:?}"
    );

    // Locate the root rollout and every sidechain file.
    let mut files = Vec::new();
    walk_jsonl(&directory, &mut files);
    let root = files
        .iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("single-hop-root"))
        })
        .expect("root rollout exists");
    let root_rows = lifecycle_rows(root);
    assert!(
        root_rows
            .iter()
            .all(|row| row["record"]["context"]["depth"] == 1),
        "the root rollout must only carry rows for turns it spawned: {root_rows:?}"
    );
    let child_id = root_rows
        .first()
        .and_then(|row| row["record"]["agentId"].as_str())
        .expect("the direct child's row is in the root rollout")
        .to_owned();

    // The nested row lives in the DIRECT parent's sidechain, stamped with the
    // child's own turn.
    // Each lifecycle transition appends one row, so group by agent: exactly
    // one nested agent, settled in exactly one sidechain.
    let mut nested_rows: Vec<(std::path::PathBuf, Value)> = Vec::new();
    for path in &files {
        if path == root {
            continue;
        }
        for row in lifecycle_rows(path) {
            if row["record"]["context"]["depth"] == 2 {
                nested_rows.push((path.clone(), row));
            }
        }
    }
    assert!(
        !nested_rows.is_empty(),
        "the nested child's rows must settle somewhere: {nested_rows:?}"
    );
    let nested_agents: std::collections::HashSet<&str> = nested_rows
        .iter()
        .map(|(_, row)| row["record"]["agentId"].as_str().unwrap())
        .collect();
    assert_eq!(
        nested_agents.len(),
        1,
        "exactly one nested agent: {nested_agents:?}"
    );
    let nested_paths: std::collections::HashSet<&std::path::PathBuf> =
        nested_rows.iter().map(|(path, _)| path).collect();
    assert_eq!(
        nested_paths.len(),
        1,
        "the nested rows settle in one sidechain: {nested_paths:?}"
    );
    let (nested_path, nested_row) = nested_rows
        .iter()
        .find(|(_, row)| row["record"]["status"] == "completed")
        .expect("the nested child reached a terminal row");
    let nested_agent = nested_row["record"]["agentId"].as_str().unwrap().to_owned();
    let nested_path = (*nested_path).clone();
    let spawn_turn = nested_row["record"]["context"]["spawnTurnId"]
        .as_str()
        .expect("the nested row carries its spawn turn");
    let sidechain_text = std::fs::read_to_string(&nested_path).unwrap();
    let child_turn = sidechain_text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|row| row["ctx"]["turnId"].as_str().map(str::to_owned))
        .expect("the sidechain carries its turn context");
    assert_eq!(
        spawn_turn, child_turn,
        "the nested row settles into the child's turn"
    );
    assert!(
        nested_path
            .to_string_lossy()
            .contains(&format!("agent-{child_id}")),
        "the nested row settles in its DIRECT parent's sidechain, got {nested_path:?}"
    );

    // The nested child's terminal receipt never entered the root ledger.
    assert!(
        receipts.iter().all(|receipt| receipt.agent_id.as_ref()
            != Some(&RuntimeString::from(nested_agent.as_str()))),
        "a nested child's receipt must not reach the root ledger: {receipts:?}"
    );
    let child_receipt = receipts
        .iter()
        .find(|receipt| receipt.agent_id.as_ref() == Some(&RuntimeString::from(child_id.as_str())))
        .expect("the direct child's receipt is in the root ledger");
    assert_eq!(
        child_receipt.turn_id.clone().unwrap_or_default(),
        root_turn_id,
        "a late receipt still settles into the turn that spawned the child"
    );
    std::fs::remove_dir_all(&directory).ok();
}
