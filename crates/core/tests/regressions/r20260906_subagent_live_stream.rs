//! Defect: child conversations only advanced after completed tools or history polls.
//! Root cause: the spawning tool filtered deltas and detached children had no live sink.
//! Fixed by: 2026-09-06 child conversation streaming work item.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, SubagentStreamEvent, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::permission::PermissionResult;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskRegistry, BackgroundTaskStatus, PlanningToolCapability, Tool, ToolContext,
    ToolProgress, ToolResult,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

struct StreamingProvider {
    info: ProviderInfo,
    spawned: AtomicBool,
    child_spawned: AtomicBool,
    nested: bool,
    background: bool,
    mailbox: bool,
    finish_child: Arc<Semaphore>,
}

struct CaptureTaskRegistry {
    task: Arc<dyn Tool>,
    captured: tokio::sync::mpsc::UnboundedSender<Arc<dyn BackgroundTaskRegistry>>,
}

#[async_trait::async_trait]
impl Tool for CaptureTaskRegistry {
    fn name(&self) -> RuntimeString {
        self.task.name()
    }

    fn description(&self) -> RuntimeString {
        self.task.description()
    }

    fn input_schema(&self) -> Json {
        self.task.input_schema()
    }

    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.task.persistence_identity()
    }

    fn is_enabled(&self) -> bool {
        self.task.is_enabled()
    }

    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.task.is_concurrency_safe(input)
    }

    fn is_read_only(&self, input: &Json) -> bool {
        self.task.is_read_only(input)
    }

    fn planning_capability(&self, input: &Json) -> PlanningToolCapability {
        self.task.planning_capability(input)
    }

    fn parse_input(&self, input: &Json) -> Result<Json, ToolError> {
        self.task.parse_input(input)
    }

    async fn check_permissions(&self, input: &Json, ctx: &ToolContext) -> Option<PermissionResult> {
        self.task.check_permissions(input, ctx).await
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        assert!(self
            .captured
            .send(ctx.background_tasks.clone().expect("session task registry"))
            .is_ok());
        self.task.call(input, ctx, progress).await
    }
}

fn stopped(reason: ModelStopReason) -> ModelStreamEvent {
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

impl ModelProvider for StreamingProvider {
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
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block,
                ContentBlock::Text(text) if text.text.contains("stream-grandchild-directive"))
            }),
            _ => false,
        }) {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "stream-test".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "Nested grandchild text".into(),
                },
                stopped(ModelStopReason::EndTurn),
            ]));
        }
        let child = request.messages.iter().any(|message| match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block,
                ContentBlock::Text(text) if text.text.contains("stream-child-directive"))
            }),
            _ => false,
        });
        if child {
            if self.nested && !self.child_spawned.swap(true, Ordering::SeqCst) {
                let input = json!({"model": "low", "description":"nested delegation","prompt":"stream-grandchild-directive","run_in_background":false});
                return Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::MessageStart {
                        model: "stream-test".into(),
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
                yield ModelStreamEvent::MessageStart { model: "stream-test".into() };
                yield ModelStreamEvent::ThinkingDelta { text: "Inspecting".into() };
                yield ModelStreamEvent::TextDelta { text: "Live child text".into() };
                tokio::select! {
                    permit = finish.acquire() => { let _permit = permit.unwrap(); }
                    _ = signal.cancelled() => { return; }
                }
                yield stopped(ModelStopReason::EndTurn);
            });
        }
        if !self.spawned.swap(true, Ordering::SeqCst) {
            let mut input = json!({"model": "low", "description":"stream observer", "prompt":"stream-child-directive", "run_in_background":self.background});
            if self.mailbox {
                input["peer"] = json!("reviewer");
            }
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "stream-test".into(),
                },
                ModelStreamEvent::ToolUseStart {
                    id: "spawn-call".into(),
                    name: "task".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "spawn-call".into(),
                    partial_json: input.to_string().into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "spawn-call".into(),
                },
                stopped(ModelStopReason::ToolUse),
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "stream-test".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "Parent done".into(),
            },
            stopped(ModelStopReason::EndTurn),
        ]))
    }
}

pub(super) async fn assert_live_child(background: bool, interrupted: bool) {
    assert_live_child_with_mailbox(background, interrupted, false).await;
}

pub(super) async fn assert_live_child_with_mailbox(
    background: bool,
    interrupted: bool,
    mailbox: bool,
) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let finish_child = Arc::new(Semaphore::new(0));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = Arc::new(StreamingProvider {
        info: ProviderInfo {
            name: "stream-test".into(),
            default_model: "stream-test".into(),
            base_url: None,
            env_key: None,
        },
        spawned: AtomicBool::new(false),
        child_spawned: AtomicBool::new(false),
        nested: false,
        background,
        mailbox,
        finish_child: finish_child.clone(),
    });
    let (registry_send, mut registry_receive) = tokio::sync::mpsc::unbounded_channel();
    let task = create_task_tool(TaskToolOptions::default());
    options.tools = Some(vec![if interrupted {
        Arc::new(CaptureTaskRegistry {
            task,
            captured: registry_send,
        }) as Arc<dyn Tool>
    } else {
        task
    }]);
    options.config.as_mut().unwrap()["forwardSubagentText"] = json!(false);
    options.config.as_mut().unwrap()["swarm"] = json!(mailbox);
    let agent = Agent::new(options);
    let directory =
        std::env::temp_dir().join(format!("klaus-child-stream-{}", uuid::Uuid::new_v4()));
    let previous_config = std::env::var_os("KLAUS_CONFIG_DIR");
    if mailbox {
        klaus_agent_core::runtime_process::set_process_environment_var(
            "KLAUS_CONFIG_DIR",
            &directory,
        );
        klaus_agent_core::tasks::swarm_store::send_message(
            &"receipt-team".into(),
            &"reviewer".into(),
            &"sender".into(),
            &"intermediate report".into(),
            20,
        )
        .await
        .unwrap();
    }
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                id: Some("stream-parent".into()),
                swarm: mailbox.then(|| klaus_agent_core::types::tool::SwarmIdentity {
                    team: "receipt-team".into(),
                    peer: "team-lead".into(),
                }),
                user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (notification_send, mut notification_receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        let _ = notification_send.send(receipt.clone());
    })));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_subagent_event_sink(Some(Arc::new(move |agent_id, event| {
        let (kind, value) = match event {
            SubagentStreamEvent::Event(event) => {
                ("event", runtime_codec::encode_value(event).unwrap())
            }
            SubagentStreamEvent::Notification(receipt) => (
                "notification",
                runtime_codec::encode_value(receipt).unwrap(),
            ),
            SubagentStreamEvent::Prompt(message) => {
                ("prompt", runtime_codec::encode_value(message).unwrap())
            }
        };
        let _ = send.send((agent_id.to_string(), kind, value));
    })));
    let parent_session = session.clone();
    let parent_turn_send = Arc::new(tokio::sync::Mutex::new(None::<RuntimeString>));
    let parent_turn_slot = parent_turn_send.clone();
    let mut parent = tokio::spawn(async move {
        let mut events = parent_session
            .run_streamed(
                Input::Text("Delegate one child".into()),
                TurnOptions::default(),
            )
            .events;
        while let Some(event) = events.next().await {
            if let klaus_agent_core::types::events::EngineEvent::TurnStarted { turn_id } = &event {
                *parent_turn_slot.lock().await = Some(turn_id.clone());
            }
        }
    });
    let mut seen = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let entry = receive.recv().await.unwrap();
            let is_text = entry
                .2
                .get("type")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "text.delta");
            seen.push(entry);
            if is_text {
                break;
            }
        }
    })
    .await
    .expect("child text arrives before completion or history polling");
    assert!(seen.iter().any(|(_, kind, _)| *kind == "prompt"));
    let child_receipts: Vec<_> = seen
        .iter()
        .filter(|(_, kind, _)| *kind == "notification")
        .collect();
    assert_eq!(
        child_receipts.len(),
        usize::from(mailbox),
        "receipt arrives live before the child finishes"
    );
    if mailbox {
        assert_eq!(
            child_receipts[0].2["agentName"].as_str(),
            Some(&RuntimeString::from("sender"))
        );
        assert_eq!(
            child_receipts[0].2["source"].as_str(),
            Some(&RuntimeString::from("team-mailbox"))
        );
    }
    assert!(seen.iter().any(|(_, _, value)| value
        .get("type")
        .and_then(|v| v.as_str())
        .is_some_and(|v| v == "thinking.delta")));
    assert!(!seen.iter().any(|(_, _, value)| value
        .get("type")
        .and_then(|v| v.as_str())
        .is_some_and(|v| v == "turn.completed")));
    assert!(
        notification_receive.try_recv().is_err(),
        "live child text/tools are not notifications"
    );
    if background {
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut parent)
            .await
            .unwrap()
            .unwrap();
    }
    if interrupted {
        let registry =
            tokio::time::timeout(std::time::Duration::from_secs(5), registry_receive.recv())
                .await
                .expect("the real task tool captures its session registry")
                .expect("the registry capture channel stays open");
        let child_id = RuntimeString::from(seen[0].0.clone());
        assert_eq!(
            registry
                .get(&child_id)
                .expect("the live child handle")
                .status,
            BackgroundTaskStatus::Running
        );
        assert!(
            registry.stop(&child_id),
            "targeted stop must cancel the child that published the live stream"
        );
    } else {
        finish_child.add_permits(1);
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let (_, _, event) = receive.recv().await.unwrap();
            if event
                .get("type")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "turn.completed")
            {
                break;
            }
        }
    })
    .await
    .expect("child completion survives the parent tool and turn");
    if !background {
        parent.await.unwrap();
    }
    if background {
        let receipt = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            notification_receive.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(receipt.source, "background-task");
        assert_eq!(receipt.status, "completed");
        assert_eq!(
            receipt.agent_id.as_ref().unwrap().try_to_utf8().unwrap(),
            seen[0].0
        );
        assert!(
            receipt.agent_name.is_none(),
            "the background description contains a prompt, not a sender name"
        );
        let parent_turn = parent_turn_send
            .lock()
            .await
            .clone()
            .expect("parent turn id");
        assert_eq!(
            receipt.turn_id.clone().unwrap_or_default(),
            parent_turn,
            "the late receipt settles into the turn that spawned the child, not the live stack"
        );
        assert!(
            notification_receive.try_recv().is_err(),
            "one enqueue produces one receipt"
        );
    } else {
        assert!(
            notification_receive.try_recv().is_err(),
            "foreground result is not a background notification"
        );
    }
    session.set_subagent_event_sink(None);
    session.shutdown_and_wait().await.unwrap();
    let parent_history = store.resume("stream-parent").await.unwrap();
    assert_eq!(
        parent_history.turn_index.agent_notifications.len(),
        usize::from(background)
    );
    let transcript = store
        .load_sub_agent("stream-parent", &seen[0].0)
        .await
        .unwrap();
    let index = transcript
        .turn_index
        .expect("child history carries a turn index");
    assert_eq!(index.turn_status.len(), 1);
    assert_eq!(index.agent_notifications.len(), usize::from(mailbox));
    if mailbox {
        let receipt = &index.agent_notifications[0];
        assert_eq!(
            receipt.notification_id,
            child_receipts[0].2["notificationId"]
                .as_str()
                .unwrap()
                .clone()
        );
        assert!(index
            .turn_status
            .contains_key(receipt.turn_id.as_ref().unwrap()));
    }
    let (turn_id, status) = index.turn_status.iter().next().unwrap();
    assert_eq!(
        status,
        if interrupted {
            "interrupted"
        } else {
            "completed"
        }
    );
    assert!(index.turn_started_at_ms[turn_id] > 0);
    assert!(index.turn_finished_at_ms[turn_id] >= index.turn_started_at_ms[turn_id]);
    assert_eq!(
        index.turn_duration_ms[turn_id] as i64,
        index.turn_finished_at_ms[turn_id] - index.turn_started_at_ms[turn_id]
    );
    assert_eq!(index.turn_final_message.contains_key(turn_id), !interrupted);
    assert!(
        !index.message_turn.is_empty(),
        "the child prompt retains turn attribution"
    );
    if !interrupted {
        assert!(
            index.message_turn.len() >= 2,
            "the child reply retains turn attribution"
        );
    }
    process.shutdown().await.unwrap();
    if background {
        let output = std::env::temp_dir().join(format!("klaus-agent-bg-{}.txt", seen[0].0));
        if output.exists() {
            std::fs::remove_file(output).unwrap();
        }
    }
    if mailbox {
        match previous_config {
            Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                previous,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "KLAUS_CONFIG_DIR",
            ),
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn foreground_child_streams_before_its_task_tool_completes() {
    assert_live_child(false, false).await;
}

#[tokio::test]
async fn detached_child_stream_survives_parent_turn_completion() {
    assert_live_child(true, false).await;
}

#[test]
fn automatic_compaction_progress_is_part_of_the_public_event_wire() {
    for phase in ["started", "completed"] {
        let event: klaus_agent_core::types::events::EngineEvent = runtime_codec::decode_text(
            &format!(r#"{{"type":"compaction.progress","phase":"{phase}"}}"#).into(),
        )
        .unwrap();
        let encoded = runtime_codec::encode_value(&event).unwrap();
        assert_eq!(encoded["phase"].as_str(), Some(&RuntimeString::from(phase)));
    }
}

#[tokio::test]
async fn interrupted_child_settles_live_stream_and_durable_turn_clock() {
    assert_live_child(false, true).await;
}

/// Nested children publish their own stream identities while their durable
/// transcripts and results retain direct-parent ownership.
#[tokio::test]
async fn nested_child_events_keep_separate_host_stream_identities() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let directory =
        std::env::temp_dir().join(format!("klaus-nested-stream-{}", uuid::Uuid::new_v4()));
    let cwd = directory.join("project");
    let user_config_dir = directory.join("config");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&user_config_dir).unwrap();
    let finish_child = Arc::new(Semaphore::new(0));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = Arc::new(StreamingProvider {
        info: ProviderInfo {
            name: "stream-test".into(),
            default_model: "stream-test".into(),
            base_url: None,
            env_key: None,
        },
        spawned: AtomicBool::new(false),
        child_spawned: AtomicBool::new(false),
        nested: true,
        background: false,
        mailbox: false,
        finish_child: finish_child.clone(),
    });
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    options.config = Some(json!({
        "permissionMode": "bypassPermissions",
        "sandbox": "process",
        "sandboxNetworkAllowed": true,
        "requireRealSandbox": false,
        "sandboxRetryOnDenial": false,
        "forwardSubagentText": false,
        "fork": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "cron": false
    }));
    options.cwd = Some(cwd.to_string_lossy().into_owned());
    let agent = Agent::new(options);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.to_string_lossy().into_owned()),
        ..Default::default()
    }));
    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                id: Some("stream-parent".into()),
                cwd: Some(cwd.to_string_lossy().into_owned().into()),
                user_config_dir: Some(user_config_dir.to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    session.set_subagent_event_sink(Some(Arc::new(move |agent_id, event| {
        let (kind, value) = match event {
            SubagentStreamEvent::Event(event) => {
                ("event", runtime_codec::encode_value(event).unwrap())
            }
            SubagentStreamEvent::Notification(receipt) => (
                "notification",
                runtime_codec::encode_value(receipt).unwrap(),
            ),
            SubagentStreamEvent::Prompt(message) => {
                ("prompt", runtime_codec::encode_value(message).unwrap())
            }
        };
        let _ = send.send((agent_id.to_string(), kind, value));
    })));
    let parent_session = session.clone();
    let parent_turn_send = Arc::new(tokio::sync::Mutex::new(None::<RuntimeString>));
    let parent_turn_slot = parent_turn_send.clone();
    let mut parent = tokio::spawn(async move {
        let mut events = parent_session
            .run_streamed(
                Input::Text("Delegate one child".into()),
                TurnOptions::default(),
            )
            .events;
        while let Some(event) = events.next().await {
            if let klaus_agent_core::types::events::EngineEvent::TurnStarted { turn_id } = &event {
                *parent_turn_slot.lock().await = Some(turn_id.clone());
            }
        }
    });
    finish_child.add_permits(1);

    let mut direct_child_id: Option<String> = None;
    let mut nested_child_id: Option<String> = None;
    let mut prompts = 0usize;
    let mut child_saw_nested_result = false;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let (agent_id, kind, value) = receive.recv().await.unwrap();
            let direct = direct_child_id.as_deref() == Some(agent_id.as_str());
            if kind == "prompt" {
                prompts += 1;
                if direct_child_id.is_none() {
                    direct_child_id = Some(agent_id.clone());
                } else {
                    assert_ne!(direct_child_id.as_ref(), Some(&agent_id));
                    nested_child_id = Some(agent_id.clone());
                }
                continue;
            }
            assert!(
                direct || nested_child_id.as_deref() == Some(agent_id.as_str()),
                "events belong to a separately published child identity"
            );
            if value.to_string().contains("Nested grandchild text") {
                child_saw_nested_result = true;
            }
            if direct
                && value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .is_some_and(|v| v == "turn.completed")
            {
                break;
            }
        }
    })
    .await
    .expect("the direct child turn settles independently of nested stream publication");
    let direct_child_id = direct_child_id.expect("direct child prompt frame");
    assert_eq!(prompts, 2, "each child publishes its own initial directive");
    assert!(
        child_saw_nested_result,
        "the nested child really ran inside the direct child's tool result"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut parent)
        .await
        .unwrap()
        .unwrap();
    session.set_subagent_event_sink(None);
    session.shutdown_and_wait().await.unwrap();

    let children = store.list_sub_agents("stream-parent").await.unwrap();
    assert_eq!(
        children.len(),
        2,
        "both child transcripts remain addressable under the owning session"
    );
    let grand_id = children
        .into_iter()
        .find(|id| *id != direct_child_id)
        .expect("the nested child transcript");
    assert_eq!(nested_child_id.as_ref(), Some(&grand_id));
    let grand = store
        .load_sub_agent("stream-parent", &grand_id)
        .await
        .unwrap();
    let grand_index = grand
        .turn_index
        .expect("the nested child keeps its own turn index");
    assert_eq!(grand_index.turn_status.len(), 1);
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
