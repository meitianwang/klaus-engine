//! Defect: after a restart the parent session could not read a background
//! sub-agent it had spawned before the crash. `task_output` answered
//! "No background task with id …", even though the child's transcript was still
//! on disk, because cold resume rebuilt only the lifecycle ledger while the
//! handle table stayed empty.
//!
//! Root cause: `hydrate_ordinary_workers` — the one path that reloads persisted
//! children into live workers AND repopulates the `task_output` handle table —
//! ran only lazily when the model addressed a specific child. `resume_session`
//! never triggered it, so a resumed session reported its children through the
//! ledger while the handle table was empty.
//!
//! Fix scope: `resume_session` reloads this session's persisted sub-agents before
//! handing the session over, so every child is addressable from the first turn.
//! The reload publishes each handle with the lifecycle record's REAL status
//! instead of a hardcoded `completed`, and reattaches the partial work projected
//! from the child's saved transcript.
//!
//! Work item: 2026-10-13 recovery-scope alignment (sub-agent reload +
//! interrupt guidance).

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::TurnOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::{EngineEvent, ItemStatus, ThreadItem};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent, ProviderInfo,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, io::Write};

const CHILD_MARKER: &str = "child work that must survive the restart";
const ROOT_THREAD_ID: &str = "reload-on-resume-root";

// Diagnostic-only output is opt-in and bounded independently of the test stream.
struct WarmDiagnosticState {
    file: std::fs::File,
    started: std::time::Instant,
    records: usize,
    bytes: usize,
    capped: bool,
}

struct WarmDiagnostic(Mutex<Option<WarmDiagnosticState>>);

struct DiagnosticLine {
    text: String,
    limit: usize,
}

impl fmt::Write for DiagnosticLine {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let remaining = self.limit.saturating_sub(self.text.len());
        if text.len() <= remaining {
            self.text.push_str(text);
            return Ok(());
        }
        let mut end = remaining;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&text[..end]);
        Err(fmt::Error)
    }
}

impl WarmDiagnostic {
    fn new(root: &Path) -> Self {
        if std::env::var_os("KLAUS_PHASE_PROBE").as_deref() != Some(std::ffi::OsStr::new("1")) {
            return Self(Mutex::new(None));
        }
        let path = root.with_extension(format!("warm-{}.log", std::process::id()));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path);
        let _ = writeln!(
            std::io::stderr().lock(),
            "[cold-reload-diagnostic] pid={} path={} opened={}",
            std::process::id(),
            path.display(),
            file.is_ok()
        );
        Self(Mutex::new(file.ok().map(|file| WarmDiagnosticState {
            file,
            started: std::time::Instant::now(),
            records: 0,
            bytes: 0,
            capped: false,
        })))
    }

    fn record(&self, arguments: fmt::Arguments<'_>) {
        use std::fmt::Write as _;
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let Some(state) = state.as_mut() else { return };
        if state.records >= 32 || state.bytes + 32 >= 8192 {
            if !state.capped {
                state.capped = true;
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "[cold-reload-diagnostic] capped records={} bytes={}",
                    state.records,
                    state.bytes
                );
            }
            return;
        }
        let mut line = DiagnosticLine {
            text: String::new(),
            limit: (8192 - state.bytes).min(1024).saturating_sub(12),
        };
        let clipped = write!(
            &mut line,
            "record={} elapsed_us={} {}",
            state.records,
            state.started.elapsed().as_micros(),
            arguments
        )
        .is_err();
        if clipped {
            line.text.push_str(" [clipped]");
        }
        line.text.push('\n');
        state.records += 1;
        state.bytes += line.text.len();
        if let Err(error) = state.file.write_all(line.text.as_bytes()) {
            let _ = writeln!(
                std::io::stderr().lock(),
                "[cold-reload-diagnostic] write_error_kind={:?}",
                error.kind()
            );
        }
    }

    fn event(&self, event: &EngineEvent) {
        match event {
            EngineEvent::TurnStarted { .. }
            | EngineEvent::TurnCompleted { .. }
            | EngineEvent::TurnFailed { .. } => {
                self.record(format_args!("warm_parent_terminal_or_start={event:?}"))
            }
            EngineEvent::ItemUpdated {
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        result,
                        error,
                        status,
                        ..
                    },
                ..
            }
            | EngineEvent::ItemCompleted {
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        result,
                        error,
                        status,
                        ..
                    },
                ..
            } if *status != ItemStatus::InProgress => {
                self.record(format_args!(
                    "warm_tool_end id={id:?} tool={tool_name:?} status={status:?} error={error:?}"
                ));
                self.record(format_args!(
                    "warm_tool_end_result id={id:?} result={result:?}"
                ));
            }
            _ => {}
        }
    }

    fn state(&self, session: &klaus_agent_core::engine::session::Session, phase: &str) {
        if self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            return;
        }
        let summaries = session.background_agent_summaries();
        let index = session.turn_index_snapshot();
        self.record(format_args!(
            "phase={phase} summaries={} turn_status={:?} turn_errors={:?} receipt_count={}",
            summaries.len(),
            index.turn_status,
            index.turn_errors,
            index.agent_notifications.len()
        ));
        for summary in summaries.iter().take(4) {
            self.record(format_args!("phase={phase} background id={:?} status={:?} idle={} teammate={} description={:?} transcript={:?}", summary.id, summary.status, summary.idle, summary.teammate, summary.description, summary.transcript));
        }
        for receipt in index.agent_notifications.iter().take(8) {
            self.record(format_args!("phase={phase} turn_index_receipt={receipt:?}"));
        }
    }
}

/// Parent spawns one background child; the child answers once and finishes.
struct BackgroundChildProvider {
    info: ProviderInfo,
    spawned: AtomicBool,
}

impl ModelProvider for BackgroundChildProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(
        &self,
        _model: &RuntimeString,
    ) -> klaus_agent_core::types::provider::ProviderCapabilities {
        klaus_agent_core::types::provider::ProviderCapabilities {
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
        if has("reload-child-directive") {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "reload".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: CHILD_MARKER.into(),
                },
                stopped(ModelStopReason::EndTurn),
            ]));
        }
        if !self.spawned.swap(true, Ordering::SeqCst) {
            let input = serde_json::json!({"model": "low",
                "description": "reload probe",
                "prompt": "reload-child-directive",
                "run_in_background": true,
            });
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "reload".into(),
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
                model: "reload".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "parent done".into(),
            },
            stopped(ModelStopReason::EndTurn),
        ]))
    }
}

fn options(
    process: RuntimeProcessContext,
    root: &Path,
) -> klaus_agent_core::engine::agent::AgentOptions {
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process);
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.compactor = Some(Arc::new(NoopCompactor));
    options.provider = Arc::new(BackgroundChildProvider {
        info: ProviderInfo {
            name: "reload".into(),
            base_url: None,
            env_key: None,
            default_model: "reload".into(),
        },
        spawned: AtomicBool::new(false),
    });
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            serde_json::json!({
                "model": "reload",
                "prewarm": false,
                "autoMemory": false,
                "agentMemory": false,
                "goal": false,
                "forwardSubagentText": false,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
    options
}

fn session_options(root: &Path) -> StartSessionOptions {
    StartSessionOptions {
        id: Some(ROOT_THREAD_ID.into()),
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_resume_reloads_a_spawned_background_child_into_the_handle_table() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!("klaus-reload-resume-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    // Run the parent to completion so the child's lifecycle record and its own
    // sidechain transcript are both durable.
    let child_id = {
        let agent = Agent::new(options(process.clone(), &root));
        let session = agent
            .open_session(store.clone(), session_options(&root))
            .await
            .unwrap();
        let diagnostic = Arc::new(WarmDiagnostic::new(&root));
        #[cfg(feature = "testing")]
        klaus_agent_core::goal::testing_phase::mark("coldreload.warm.begin", ROOT_THREAD_ID);
        let (receipt_send, mut receipts) = tokio::sync::mpsc::unbounded_channel();
        let sink_diagnostic = diagnostic.clone();
        session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
            sink_diagnostic.record(format_args!("sink_receipt={receipt:?}"));
            let _ = receipt_send.send(receipt.clone());
        })));
        let mut events = session
            .run_streamed(
                klaus_agent_core::engine::session::Input::Text("spawn a child".into()),
                TurnOptions::default(),
            )
            .events;
        while let Some(event) = events.next().await {
            diagnostic.event(&event);
        }
        diagnostic.state(&session, "warm_stream_drained");
        #[cfg(feature = "testing")]
        klaus_agent_core::goal::testing_phase::mark("coldreload.warm.stream-ended", ROOT_THREAD_ID);

        let mut child_id = None;
        let receive_started = std::time::Instant::now();
        let mut receive_exit = "deadline_20s";
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            let received =
                tokio::time::timeout(std::time::Duration::from_millis(500), receipts.recv()).await;
            receive_exit = match &received {
                Ok(Some(receipt)) => {
                    if receipt.status == "completed" {
                        "completed_receipt"
                    } else {
                        "deadline_20s"
                    }
                }
                Ok(None) => "channel_closed",
                Err(_) => "timeout_500ms",
            };
            diagnostic.record(format_args!(
                "receipt_receive elapsed_us={} result={received:?}",
                receive_started.elapsed().as_micros()
            ));
            match received {
                Ok(Some(receipt)) if receipt.status == "completed" => {
                    child_id = receipt.agent_id.clone();
                    break;
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => continue,
            }
        }
        diagnostic.record(format_args!(
            "receive_exit={receive_exit} elapsed_us={} child_id={child_id:?}",
            receive_started.elapsed().as_micros()
        ));
        diagnostic.state(&session, "warm_receive_exit");
        #[cfg(feature = "testing")]
        klaus_agent_core::goal::testing_phase::mark("coldreload.warm.receive-exit", receive_exit);
        let child_id = child_id.expect("the background child reported completion");
        session.flush_rollout().await.unwrap();
        session.shutdown_and_wait().await.unwrap();
        child_id.try_to_utf8().unwrap()
    };

    // The child's own transcript is on disk, which is what makes the reload
    // meaningful: its work is recoverable, not gone.
    let sidechain = {
        let wanted = format!("agent-{child_id}.jsonl");
        let mut found = None;
        let mut stack = vec![root.join("sessions")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy() == wanted)
                {
                    found = Some(path);
                }
            }
        }
        found.expect("the child's sidechain transcript is on disk")
    };
    let sidechain_text = std::fs::read_to_string(&sidechain).unwrap();
    assert!(
        sidechain_text.contains(CHILD_MARKER),
        "the child's transcript persisted its work"
    );

    // Cold resume in a FRESH agent, as a restarted process would.
    let resumed = Agent::new(options(process.clone(), &root))
        .resume_session(store.clone(), ROOT_THREAD_ID, session_options(&root))
        .await
        .unwrap();

    // The handle table is repopulated, so task_output can address the child
    // again instead of answering "no background task with id …".
    let summaries = resumed.background_agent_summaries();
    let handle = summaries
        .iter()
        .find(|summary| summary.id == child_id)
        .unwrap_or_else(|| {
            panic!(
                "cold resume must reload the spawned child into the handle table; got {:?}",
                summaries
                    .iter()
                    .map(|summary| (&summary.id, summary.status))
                    .collect::<Vec<_>>()
            )
        });

    // The status comes from the lifecycle record, not a hardcoded "completed":
    // this child really did finish, so Completed is the truthful answer, and the
    // interrupted case is pinned by the in-crate recovery test.
    assert_eq!(
        handle.status,
        klaus_agent_core::types::tool::BackgroundTaskStatus::Completed,
        "a child that finished before the restart keeps its real terminal status"
    );
    // `description` is the spawning prompt, both live and recovered, so a
    // recovered handle reads the same as the one the model saw before the
    // restart — not an empty label.
    assert_eq!(
        handle.description, "reload-child-directive",
        "the recovered handle carries the same description the live handle had"
    );
    assert!(
        handle.transcript.contains(CHILD_MARKER),
        "the recovered handle reattaches the work projected from the child's saved transcript: {:?}",
        handle.transcript
    );

    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(&root).ok();
    let stray = std::env::temp_dir().join(format!("klaus-agent-bg-{child_id}.txt"));
    if stray.exists() {
        std::fs::remove_file(stray).ok();
    }
}
