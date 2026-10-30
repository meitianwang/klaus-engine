mod callbacks;
mod globals;
mod module_loader;
mod timers;
pub(super) mod value;

use std::collections::HashMap;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc as std_mpsc;
use std::thread;

use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::engine::code_mode::protocol::{CodeModeToolKind, NestedToolMetadata, OutputContentItem};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::CodeModeToolRejection;
use crate::v8_platform::initialize_v8;

use super::TaskFailureHandler;

const EXIT_SENTINEL: &str = "__codex_code_mode_exit__";

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    ToolResponse {
        id: RuntimeString,
        result: RuntimeJson,
    },
    ToolError {
        id: RuntimeString,
        rejection: CodeModeToolRejection,
    },
    TimeoutFired {
        id: u64,
    },
    ObservePendingFrontier,
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum PendingRuntimeMode {
    PauseUntilResumed,
}

#[derive(Debug)]
pub(crate) enum RuntimeControlCommand {
    Continue,
    Resume,
    Terminate,
}

#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    Started,
    Pending,
    ContentItem(OutputContentItem),
    YieldRequested,
    ToolCall {
        id: RuntimeString,
        name: RuntimeString,
        kind: CodeModeToolKind,
        input: Option<RuntimeJson>,
    },
    Notify {
        call_id: RuntimeString,
        text: RuntimeString,
    },
    Result {
        stored_value_writes: HashMap<RuntimeString, RuntimeJson>,
        error_text: Option<RuntimeString>,
    },
    ThreadPanicked,
}

pub(crate) struct ExecuteRequest {
    pub(crate) tool_call_id: RuntimeString,
    pub(crate) enabled_tools: Vec<NestedToolMetadata>,
    pub(crate) source: RuntimeString,
}

pub(crate) fn spawn_runtime(
    stored_values: HashMap<RuntimeString, RuntimeJson>,
    request: ExecuteRequest,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_mode: PendingRuntimeMode,
    task_failure_handler: Option<TaskFailureHandler>,
) -> Result<
    (
        std_mpsc::Sender<RuntimeCommand>,
        std_mpsc::Sender<RuntimeControlCommand>,
        v8::IsolateHandle,
    ),
    String,
> {
    initialize_v8()?;

    let (command_tx, command_rx) = std_mpsc::channel();
    let (control_tx, control_rx) = std_mpsc::channel();
    let runtime_command_tx = command_tx.clone();
    let (isolate_handle_tx, isolate_handle_rx) = std_mpsc::sync_channel(1);
    let timer_handle =
        Handle::try_current().map_err(|err| format!("code mode timers require Tokio: {err}"))?;
    let config = RuntimeConfig {
        tool_call_id: request.tool_call_id,
        enabled_tools: request.enabled_tools,
        source: request.source,
        stored_values,
        timer_handle,
    };

    spawn_supervised_runtime_thread(event_tx.clone(), task_failure_handler, move || {
        run_runtime(
            config,
            event_tx,
            command_rx,
            control_rx,
            pending_mode,
            isolate_handle_tx,
            runtime_command_tx,
        );
    });

    let isolate_handle = isolate_handle_rx
        .recv()
        .map_err(|_| "failed to initialize code mode runtime".to_string())?;
    Ok((command_tx, control_tx, isolate_handle))
}

fn spawn_supervised_runtime_thread(
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    task_failure_handler: Option<TaskFailureHandler>,
    runtime: impl FnOnce() + Send + 'static,
) {
    thread::spawn(move || {
        if catch_unwind(AssertUnwindSafe(runtime)).is_err() {
            if let Some(task_failure_handler) = task_failure_handler {
                task_failure_handler("code-mode V8 runtime thread panicked".to_string());
            }
            let _ = event_tx.send(RuntimeEvent::ThreadPanicked);
        }
    });
}

#[derive(Clone)]
struct RuntimeConfig {
    tool_call_id: RuntimeString,
    enabled_tools: Vec<NestedToolMetadata>,
    source: RuntimeString,
    stored_values: HashMap<RuntimeString, RuntimeJson>,
    timer_handle: Handle,
}

pub(super) struct RuntimeState {
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_tool_calls: HashMap<RuntimeString, v8::Global<v8::PromiseResolver>>,
    pending_timeouts: HashMap<u64, timers::ScheduledTimeout>,
    stored_values: HashMap<RuntimeString, RuntimeJson>,
    stored_value_writes: HashMap<RuntimeString, RuntimeJson>,
    enabled_tools: Vec<NestedToolMetadata>,
    next_tool_call_id: u64,
    next_timeout_id: u64,
    tool_call_id: RuntimeString,
    runtime_command_tx: std_mpsc::Sender<RuntimeCommand>,
    timer_handle: Handle,
    exit_requested: bool,
}

pub(super) enum CompletionState {
    Pending,
    Completed {
        stored_value_writes: HashMap<RuntimeString, RuntimeJson>,
        error_text: Option<RuntimeString>,
    },
}

impl Drop for RuntimeState {
    fn drop(&mut self) {
        for (_, timeout) in self.pending_timeouts.drain() {
            timeout.cancel();
        }
    }
}

fn run_runtime(
    config: RuntimeConfig,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: std_mpsc::Receiver<RuntimeCommand>,
    control_rx: std_mpsc::Receiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
    isolate_handle_tx: std_mpsc::SyncSender<v8::IsolateHandle>,
    runtime_command_tx: std_mpsc::Sender<RuntimeCommand>,
) {
    let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
    let isolate_handle = isolate.thread_safe_handle();
    if isolate_handle_tx.send(isolate_handle).is_err() {
        return;
    }
    isolate.set_host_import_module_dynamically_callback(module_loader::dynamic_import_callback);

    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);

    scope.set_slot(RuntimeState {
        event_tx: event_tx.clone(),
        pending_tool_calls: HashMap::new(),
        pending_timeouts: HashMap::new(),
        stored_values: config.stored_values,
        stored_value_writes: HashMap::new(),
        enabled_tools: config.enabled_tools,
        next_tool_call_id: 1,
        next_timeout_id: 1,
        tool_call_id: config.tool_call_id,
        runtime_command_tx,
        timer_handle: config.timer_handle,
        exit_requested: false,
    });

    if let Err(error_text) = globals::install_globals(scope) {
        send_result(&event_tx, HashMap::new(), Some(error_text));
        return;
    }

    let _ = event_tx.send(RuntimeEvent::Started);

    let pending_promise = match module_loader::evaluate_main_module(scope, &config.source) {
        Ok(pending_promise) => pending_promise,
        Err(error_text) => {
            capture_scope_send_error(scope, &event_tx, Some(error_text));
            return;
        }
    };

    match module_loader::completion_state(scope, pending_promise.as_ref()) {
        CompletionState::Completed {
            stored_value_writes,
            error_text,
        } => {
            send_result(&event_tx, stored_value_writes, error_text);
            return;
        }
        CompletionState::Pending => {}
    }

    let mut pending_promise = pending_promise;
    while let Some(command) =
        next_runtime_command(&event_tx, &command_rx, &control_rx, pending_mode)
    {
        match command {
            RuntimeCommand::Terminate => break,
            RuntimeCommand::ToolResponse { id, result } => {
                if let Err(error_text) =
                    module_loader::resolve_tool_response(scope, &id, Ok(result))
                {
                    capture_scope_send_error(scope, &event_tx, Some(error_text));
                    return;
                }
            }
            RuntimeCommand::ToolError { id, rejection } => {
                if let Err(runtime_error) =
                    module_loader::resolve_tool_response(scope, &id, Err(rejection))
                {
                    capture_scope_send_error(scope, &event_tx, Some(runtime_error));
                    return;
                }
            }
            RuntimeCommand::TimeoutFired { id } => {
                if let Err(runtime_error) = timers::invoke_timeout_callback(scope, id) {
                    capture_scope_send_error(scope, &event_tx, Some(runtime_error));
                    return;
                }
            }
            RuntimeCommand::ObservePendingFrontier => {}
        }

        scope.perform_microtask_checkpoint();
        match module_loader::completion_state(scope, pending_promise.as_ref()) {
            CompletionState::Completed {
                stored_value_writes,
                error_text,
            } => {
                send_result(&event_tx, stored_value_writes, error_text);
                return;
            }
            CompletionState::Pending => {}
        }

        if let Some(promise) = pending_promise.as_ref() {
            let promise = v8::Local::new(scope, promise);
            if promise.state() != v8::PromiseState::Pending {
                pending_promise = None;
            }
        }
    }
}

fn next_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
    control_rx: &std_mpsc::Receiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
) -> Option<RuntimeCommand> {
    loop {
        match command_rx.try_recv() {
            Ok(command) => return Some(command),
            Err(std_mpsc::TryRecvError::Disconnected) => return None,
            Err(std_mpsc::TryRecvError::Empty) => {}
        }

        let _ = event_tx.send(RuntimeEvent::Pending);
        match pending_mode {
            PendingRuntimeMode::PauseUntilResumed => match control_rx.recv().ok()? {
                RuntimeControlCommand::Continue => return command_rx.recv().ok(),
                RuntimeControlCommand::Resume => continue,
                RuntimeControlCommand::Terminate => return Some(RuntimeCommand::Terminate),
            },
        }
    }
}

fn capture_scope_send_error(
    scope: &mut v8::PinScope<'_, '_>,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    error_text: Option<RuntimeString>,
) {
    let stored_value_writes = scope
        .get_slot::<RuntimeState>()
        .map(|state| state.stored_value_writes.clone())
        .unwrap_or_default();

    send_result(event_tx, stored_value_writes, error_text);
}

fn send_result(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    stored_value_writes: HashMap<RuntimeString, RuntimeJson>,
    error_text: Option<RuntimeString>,
) {
    let _ = event_tx.send(RuntimeEvent::Result {
        stored_value_writes,
        error_text,
    });
}
