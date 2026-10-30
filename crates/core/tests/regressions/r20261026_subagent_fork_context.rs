//! Defect: a completed full-history child lost inherited call results on resume.
//! Root cause: only parent message ids survived, then the raw seed was prepended
//! after the child had already replayed its compaction checkpoints.
//! Work item: 20261026 durable child fork context and provider history pairing.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::engine::BUILTIN_AGENTS;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{CompactionRecord, SessionStore};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use klaus_agent_core::types::tool::SubagentMessageRequest;
use serde_json::json;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ROOT: &str = "durable-fork-parent";
const PARENT: &str = "immutable-parent-context-20261026";
const ASSIGNMENT: &str = "original-child-assignment-20261026";
const WARM: &str = "warm-child-followup-20261026";
const COLD: &str = "cold-child-followup-20261026";
const CUT: &str = "compact-child-context-20261026";
const AFTER: &str = "after-child-compaction-20261026";

fn has_text(messages: &[Message], text: &str) -> bool {
    messages.iter().any(|message| match message {
        Message::User(message) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(block) if block.text.contains(text))),
        _ => false,
    })
}

fn assert_paired(messages: &[Message]) {
    let calls: HashSet<_> = messages
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(message) => Some(message.content.iter()),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::ToolUse(call) => Some(call.id.clone()),
            _ => None,
        })
        .collect();
    let outputs: HashSet<_> = messages
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result.tool_use_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        calls, outputs,
        "the provider rejects missing or orphaned call outputs"
    );
}

struct Provider {
    info: ProviderInfo,
    spawn: AtomicBool,
    children: Mutex<Vec<ModelRequest>>,
}

impl Provider {
    fn new(spawn: bool) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "durable-fork-test".into(),
                base_url: None,
                env_key: None,
                default_model: "durable-fork-test".into(),
            },
            spawn: AtomicBool::new(spawn),
            children: Mutex::new(Vec::new()),
        })
    }
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
        assert_paired(&request.messages);
        let child = request
            .system
            .iter()
            .any(|part| part.name == "agent:general-purpose");
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "durable-fork-test".into(),
        }];
        let reason = if child {
            self.children.lock().unwrap().push(request);
            events.push(ModelStreamEvent::TextDelta {
                text: "Child work completed.".into(),
            });
            ModelStopReason::EndTurn
        } else if self.spawn.swap(false, Ordering::SeqCst) {
            events.extend([
                ModelStreamEvent::ToolUseStart { id: "parent-spawn-call".into(), name: "task".into(), thought_signature: None, tool_kind: None, input_format: None },
                ModelStreamEvent::ToolUseInputDelta { id: "parent-spawn-call".into(), partial_json: json!({"model": "low", "description":"Durable fork child", "prompt":ASSIGNMENT, "subagent_type":"general-purpose", "fork_turns":"all", "run_in_background":true}).to_string().into() },
                ModelStreamEvent::ToolUseStop { id: "parent-spawn-call".into() },
            ]);
            ModelStopReason::ToolUse
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Parent work completed.".into(),
            });
            ModelStopReason::EndTurn
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: reason,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 2,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

fn store(root: &Path) -> Arc<JsonlSessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}

fn new_agent(process: RuntimeProcessContext, provider: Arc<Provider>, root: &Path) -> Agent {
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.provider = provider;
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.system_prompt_provider = Some(Arc::new(|| {
        Box::pin(async {
            vec![SystemPromptPart {
                name: "fixture-base".into(),
                text: "Use the retained child context.".into(),
                cacheable: Some(true),
                cache_scope: None,
            }]
        })
    }));
    options.config.as_mut().unwrap().as_object_mut().unwrap().extend(json!({"model":"durable-fork-test", "prewarm":false, "swarm":false, "autoMemory":false, "agentMemory":false, "memoryRecall":false, "goal":false, "forwardSubagentText":false}).as_object().unwrap().clone());
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    options.agents = Some(vec![BUILTIN_AGENTS
        .iter()
        .find(|agent| agent.name == "general-purpose")
        .unwrap()
        .clone()]);
    options.include_default_agents = Some(false);
    Agent::new(options)
}

fn session_options(root: &Path) -> StartSessionOptions {
    StartSessionOptions {
        id: Some(ROOT.into()),
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    }
}

fn notices(session: &Arc<Session>) -> tokio::sync::mpsc::UnboundedReceiver<RuntimeString> {
    let (send, receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |notice| {
        if notice.status == "completed" {
            if let Some(id) = &notice.agent_id {
                let _ = send.send(id.clone());
            }
        }
    })));
    receive
}

async fn completed(
    receive: &mut tokio::sync::mpsc::UnboundedReceiver<RuntimeString>,
) -> RuntimeString {
    tokio::time::timeout(Duration::from_secs(10), receive.recv())
        .await
        .expect("child must reach its terminal notification")
        .unwrap()
}

async fn followup(session: &Arc<Session>, id: &RuntimeString, text: &str) {
    let receipt = session
        .send_subagent_message(SubagentMessageRequest {
            target: id.clone(),
            message: text.into(),
            summary: "Continue the existing child.".into(),
            trigger_turn: true,
            sender_agent_id: None,
            user_initiated: false,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.resumed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_fork_owns_its_context_across_warm_cold_and_compacted_continuations() {
    fork_context_recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_fork_is_upgraded_before_followup_and_preserves_child_compaction() {
    fork_context_recovery(true).await;
}

async fn fork_context_recovery(legacy: bool) {
    let root = tempfile::tempdir().unwrap();
    let supervisor = super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor();
    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let provider = Provider::new(true);
    let agent = new_agent(process.clone(), provider.clone(), root.path());
    let disk = store(root.path());
    let session = agent
        .open_session(disk.clone(), session_options(root.path()))
        .await
        .unwrap();
    session
        .record_user_goal_submission_instruction(
            &"fork-context-retained-control".into(),
            Some(&"Preserve the original child context.".into()),
            Some(&"active".into()),
        )
        .await
        .unwrap();
    let mut receive = notices(&session);
    let result = session
        .run(Input::Text(PARENT.into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    let id = completed(&mut receive).await;
    followup(&session, &id, WARM).await;
    assert_eq!(completed(&mut receive).await, id);
    let requests = provider.children.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(has_text(&request.messages, PARENT));
        assert!(has_text(&request.messages, ASSIGNMENT));
    }
    let saved = disk
        .read_sub_agent(ROOT, &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let creation = saved
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .unwrap();
    let prefix: Vec<Message> = runtime_codec::decode_value(
        creation
            .get("forkHistory")
            .expect("a child must own its fork context, not only parent ids"),
    )
    .unwrap();
    assert!(has_text(&prefix, PARENT));
    assert!(has_text(&prefix, "Preserve the original child context."));
    assert!(
        !has_text(&prefix, ASSIGNMENT),
        "the child directive belongs to its local suffix"
    );
    assert_paired(&prefix);
    assert!(prefix.iter().any(|message| matches!(message, Message::ToolResult(result) if result.tool_use_id == "parent-spawn-call")));
    assert!(
        !has_text(
            &disk
                .load_sub_agent(ROOT, &id.try_to_utf8().unwrap())
                .await
                .unwrap()
                .messages,
            PARENT
        ),
        "the child display transcript must not show inherited parent messages"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    drop(session);
    drop(agent);

    // Replace only the scratch parent's message bodies. Its child still owns
    // the original prefix and must never rebuild it from these current rows.
    let parent = disk.read_thread(ROOT).await.unwrap();
    let parent_file = std::fs::read_dir(root.path().join("sessions"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .unwrap();
    let text = std::fs::read_to_string(&parent_file).unwrap();
    if legacy {
        let mut parent_ids: HashSet<_> = parent
            .raw_items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::Message { message } => Some(match message {
                    Message::User(message) => message.uuid.clone(),
                    Message::Assistant(message) => message.uuid.clone(),
                    Message::ToolResult(message) => message.uuid.clone(),
                    Message::System(message) => message.uuid.clone(),
                    Message::CompactBoundary(message) => message.uuid.clone(),
                }),
                _ => None,
            })
            .collect();
        parent_ids.extend(
            klaus_agent_core::goal::retained_context::instructions_from_rollout(&parent.raw_items)
                .into_iter()
                .filter_map(|message| match message {
                    Message::User(message) => Some(message.uuid),
                    _ => None,
                }),
        );
        let child_file = parent_file
            .with_extension("")
            .join("subagents")
            .join(format!("agent-{}.jsonl", id.try_to_utf8().unwrap()));
        let mut rows = Vec::new();
        for line in std::fs::read_to_string(&child_file).unwrap().lines() {
            let mut row: serde_json::Value = serde_json::from_str(line).unwrap();
            if row["event"] == "ordinary_subagent_creation" {
                let payload = row["payload"].as_object_mut().unwrap();
                payload.insert("version".into(), json!(5));
                payload.remove("forkHistory");
                let ids = payload["forkSeedIds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|id| parent_ids.contains(&RuntimeString::from(id.as_str().unwrap())))
                    .cloned()
                    .collect::<Vec<_>>();
                payload.insert("forkSeedIds".into(), json!(ids));
            }
            rows.push(row.to_string());
        }
        std::fs::write(&child_file, rows.join("\n") + "\n").unwrap();
    } else {
        std::fs::write(
            &parent_file,
            text.replace(PARENT, "replaced-parent-context-20261026"),
        )
        .unwrap();
    }
    assert!(!parent.raw_items.is_empty());

    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let provider = Provider::new(false);
    let agent = new_agent(process.clone(), provider.clone(), root.path());
    let session = agent
        .resume_session(store(root.path()), ROOT, session_options(root.path()))
        .await
        .unwrap();
    let mut receive = notices(&session);
    followup(&session, &id, COLD).await;
    assert_eq!(completed(&mut receive).await, id);
    assert!(has_text(
        &provider.children.lock().unwrap()[0].messages,
        PARENT
    ));
    let upgraded = store(root.path())
        .read_sub_agent(ROOT, &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let latest = upgraded
        .raw_items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_creation" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        runtime_codec::decode_value::<u32>(latest.get("version").unwrap()).unwrap(),
        6
    );
    assert!(latest.get("forkHistory").is_some());
    followup(&session, &id, CUT).await;
    assert_eq!(completed(&mut receive).await, id);
    let cut = provider.children.lock().unwrap().last().unwrap().clone();
    assert!(has_text(&cut.messages, PARENT));
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    drop(session);
    drop(agent);

    // Install a real durable replacement through the store's compaction API.
    // Resume must replay it after the owned prefix, just as the live writer does.
    let disk = store(root.path());
    let saved = disk
        .load_sub_agent(ROOT, &id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let writer = disk
        .open_sub_agent(ROOT, &id.try_to_utf8().unwrap(), saved.meta.unwrap())
        .await
        .unwrap();
    writer
        .append_compaction(CompactionRecord {
            summary: "The inherited work has been summarized.".into(),
            replaced_range: (0, cut.messages.len() as u64),
            replacement_history: Some(vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "child-context-checkpoint".into(),
                summary: "The inherited work has been summarized.".into(),
                native_compaction: None,
            })]),
            source_message_ids: None,
            approval_parent_history: None,
        })
        .await;
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();
    drop(writer);

    let process = RuntimeProcessContext::initialize(supervisor).unwrap();
    let provider = Provider::new(false);
    let agent = new_agent(process.clone(), provider.clone(), root.path());
    let session = agent
        .resume_session(store(root.path()), ROOT, session_options(root.path()))
        .await
        .unwrap();
    let mut receive = notices(&session);
    followup(&session, &id, AFTER).await;
    assert_eq!(completed(&mut receive).await, id);
    let after = provider.children.lock().unwrap()[0].clone();
    assert!(after
        .messages
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    assert!(
        !has_text(&after.messages, PARENT),
        "a child checkpoint must not resurrect retired inherited context"
    );
    assert!(!has_text(&after.messages, ASSIGNMENT));
    assert!(has_text(&after.messages, AFTER));
    assert_paired(&after.messages);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
