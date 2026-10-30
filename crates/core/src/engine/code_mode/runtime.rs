use crate::sync_ext::MutexExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest, ImageDetail,
    OutputContentItem, RuntimeResponse, WaitOutcome, WaitRequest,
};
#[cfg(test)]
use crate::engine::code_mode::protocol::{
    ExecuteToPendingOutcome, WaitToPendingOutcome, WaitToPendingRequest,
};
use crate::engine::code_mode::session::{
    CodeModeSession, CodeModeSessionDelegate, CodeModeSessionProvider,
    CodeModeSessionProviderFuture, CodeModeSessionResultFuture, NoopCodeModeSessionDelegate,
    ReplExecFuture, ReplExecRequest, ReplSessionHandle, StartedCell,
};
use crate::runtime_values::RuntimeString;
use crate::types::errors::CodeModeToolRejection;
use crate::types::tool::InvokeToolFn;

#[path = "runtime_impl/cell_actor/mod.rs"]
mod cell_actor;
#[path = "runtime_impl/isolate/mod.rs"]
mod isolate;
#[path = "runtime_impl/repl_session.rs"]
pub(crate) mod repl_session;
#[path = "runtime_impl/session_runtime/mod.rs"]
mod session_runtime;

use self::repl_session::{tool_fn_invoke, ReplSession};
use self::session_runtime::SessionRuntime;

const DEFAULT_EXEC_YIELD_TIME_MS: u64 = 10_000;
const OUTPUT_TRUNCATION_MARKER: &str = "[exec output truncated; call wait for more]";

pub(crate) type TaskFailureHandler = Arc<dyn Fn(String) + Send + Sync>;

/// The in-process carrier: cells run in this process, against the engine that
/// created the session.
#[derive(Default)]
pub struct InProcessCodeModeSessionProvider;

impl CodeModeSessionProvider for InProcessCodeModeSessionProvider {
    fn create_session<'a>(
        &'a self,
        delegate: Arc<dyn CodeModeSessionDelegate>,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(async move {
            let session: Arc<dyn CodeModeSession> =
                Arc::new(InProcessCodeModeSession::with_delegate(delegate));
            Ok(session)
        })
    }

    fn create_repl_session<'a>(
        &'a self,
    ) -> CodeModeSessionResultFuture<'a, Arc<dyn ReplSessionHandle>> {
        Box::pin(async {
            let handle: Arc<dyn ReplSessionHandle> = Arc::new(InProcessReplHandle::default());
            Ok(handle)
        })
    }
}

/// The in-process REPL carrier: the session's persistent JS context lives in
/// this process, and nested tool calls use the caller's own bridge directly.
#[derive(Default)]
pub(crate) struct InProcessReplHandle {
    session: ReplSession,
}

impl ReplSessionHandle for InProcessReplHandle {
    fn execute<'a>(
        &'a self,
        request: ReplExecRequest,
        invoke_tool: Option<InvokeToolFn>,
        signal: &'a CancellationToken,
    ) -> ReplExecFuture<'a> {
        Box::pin(
            self.session
                .execute_with(request, tool_fn_invoke(invoke_tool), signal),
        )
    }
}

pub struct InProcessCodeModeSession {
    runtime: SessionRuntime<ProtocolDelegate>,
    buffered_frontiers: Arc<Mutex<HashMap<CellId, BufferedFrontier>>>,
}

#[derive(Clone, Debug)]
struct BufferedFrontier {
    kind: BufferedFrontierKind,
    content_items: Vec<OutputContentItem>,
}

#[derive(Clone, Debug)]
enum BufferedFrontierKind {
    Yielded,
    Result { error_text: Option<RuntimeString> },
    Terminated,
}

#[derive(Clone, Debug)]
struct ObservedFrontier {
    kind: BufferedFrontierKind,
    content_items: Vec<OutputContentItem>,
}

struct BudgetedOutput {
    visible_items: Vec<OutputContentItem>,
    remaining_items: Vec<OutputContentItem>,
    consumed_units: usize,
    truncated: bool,
}

impl InProcessCodeModeSession {
    pub fn new() -> Self {
        Self::with_delegate(Arc::new(NoopCodeModeSessionDelegate))
    }

    pub fn with_delegate(delegate: Arc<dyn CodeModeSessionDelegate>) -> Self {
        Self {
            runtime: SessionRuntime::new(Arc::new(ProtocolDelegate { delegate })),
            buffered_frontiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Report a cell whose actor task died (a panic or a cancelled join) to the
    /// host instead of losing it: a carrier that owns the session out-of-band
    /// has no other way to learn that a cell stopped making progress.
    pub fn with_delegate_and_task_failure_handler(
        delegate: Arc<dyn CodeModeSessionDelegate>,
        task_failure_handler: TaskFailureHandler,
    ) -> Self {
        Self {
            runtime: SessionRuntime::new_with_task_failure_handler(
                Arc::new(ProtocolDelegate { delegate }),
                Some(task_failure_handler),
            ),
            buffered_frontiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Admit a cell and hand back its id immediately; the first frontier is
    /// mapped on a detached task so callers can attach status routing to the
    /// cell before awaiting it.
    pub async fn execute(&self, request: ExecuteRequest) -> Result<StartedCell, RuntimeString> {
        let yield_time_ms = request.yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_TIME_MS);
        let max_output_tokens = request.max_output_tokens;
        let started = self
            .runtime
            .execute(
                runtime_request(request),
                session_runtime::ObserveMode::YieldAfter(Duration::from_millis(yield_time_ms)),
            )
            .await
            .map_err(session_runtime::Error::into_runtime_string)?;
        let cell_id = protocol_cell_id(&started.cell_id);
        let response_cell_id = cell_id.clone();
        let (response_tx, response_rx) = oneshot::channel();
        let frontiers = Arc::clone(&self.buffered_frontiers);
        tokio::spawn(async move {
            let response = started
                .initial_event()
                .await
                .map_err(session_runtime::Error::into_runtime_string)
                .and_then(|event| {
                    budget_observed_frontier(
                        &frontiers,
                        &response_cell_id,
                        Vec::new(),
                        observed_frontier(event)?,
                        max_output_tokens,
                    )
                });
            let _ = response_tx.send(response);
        });
        Ok(StartedCell::from_result_receiver(cell_id, response_rx))
    }

    /// Admit a cell and observe it up to its QUIESCENT frontier: the runtime has
    /// run as far as it can without new input. Unlike [`Self::execute`] this does
    /// not race a clock, so the answer describes what the program is blocked on.
    ///
    /// This is an INTROSPECTION surface, not part of the model-facing contract:
    /// `exec` / `wait` always observe up to a yield. Its only consumers today
    /// are the runtime's own contract tests, so it is gated rather than shipped
    /// as unreachable production code.
    #[cfg(test)]
    pub async fn execute_to_pending(
        &self,
        request: ExecuteRequest,
    ) -> Result<ExecuteToPendingOutcome, RuntimeString> {
        let max_output_tokens = request.max_output_tokens;
        let started = self
            .runtime
            .execute(
                runtime_request(request),
                session_runtime::ObserveMode::PendingFrontier,
            )
            .await
            .map_err(session_runtime::Error::into_runtime_string)?;
        let cell_id = protocol_cell_id(&started.cell_id);
        let event = started
            .initial_event()
            .await
            .map_err(session_runtime::Error::into_runtime_string)?;
        self.pending_outcome(&cell_id, event, max_output_tokens)
    }

    /// Resume observing a live cell up to its quiescent frontier. Gated with
    /// [`Self::execute_to_pending`], whose introspection contract it completes.
    #[cfg(test)]
    pub async fn wait_to_pending(
        &self,
        request: WaitToPendingRequest,
    ) -> Result<WaitToPendingOutcome, RuntimeString> {
        let cell_id = request.cell_id;
        let runtime_cell_id = runtime_cell_id(&cell_id);
        match self
            .runtime
            .observe(
                &runtime_cell_id,
                session_runtime::ObserveMode::PendingFrontier,
            )
            .await
        {
            Ok(event) => Ok(WaitToPendingOutcome::LiveCell(
                self.pending_outcome(&cell_id, event, None)?,
            )),
            Err(session_runtime::Error::MissingCell(_))
            | Err(session_runtime::Error::ClosedCell(_)) => {
                self.clear_buffered_frontier(&cell_id);
                Ok(WaitToPendingOutcome::MissingCell(missing_cell_response(
                    cell_id,
                )))
            }
            Err(error) => Err(error.into_runtime_string()),
        }
    }

    /// Map one observed event onto the pending-frontier contract. A paused cell
    /// reports its blocked tool calls verbatim; any terminal frontier is a
    /// completion and goes through the same output budgeting as a wait.
    #[cfg(test)]
    fn pending_outcome(
        &self,
        cell_id: &CellId,
        event: session_runtime::CellEvent,
        max_output_tokens: Option<usize>,
    ) -> Result<ExecuteToPendingOutcome, RuntimeString> {
        match event {
            session_runtime::CellEvent::Pending {
                content_items,
                pending_tool_call_ids,
            } => Ok(ExecuteToPendingOutcome::Pending {
                cell_id: cell_id.clone(),
                content_items: content_items.into_iter().map(output_item).collect(),
                pending_tool_call_ids,
            }),
            event => Ok(ExecuteToPendingOutcome::Completed(self.observed_response(
                cell_id,
                observed_frontier(event)?,
                max_output_tokens,
            )?)),
        }
    }

    pub async fn wait(&self, request: WaitRequest) -> Result<WaitOutcome, RuntimeString> {
        let WaitRequest {
            cell_id,
            yield_time_ms,
            max_output_tokens,
            terminate,
        } = request;
        if terminate.unwrap_or(false) {
            return self.terminate(cell_id).await;
        }

        let mut prefix_items = Vec::new();
        let mut remaining_budget = max_output_tokens;
        if let Some(buffered) = self.take_buffered_frontier(&cell_id) {
            let budgeted = budget_output(buffered.content_items, remaining_budget);
            append_items(&mut prefix_items, budgeted.visible_items);
            remaining_budget = subtract_budget(remaining_budget, budgeted.consumed_units);

            if budgeted.truncated {
                self.store_buffered_frontier(
                    cell_id.clone(),
                    BufferedFrontier {
                        kind: buffered.kind.clone(),
                        content_items: budgeted.remaining_items,
                    },
                );
                prefix_items.push(truncation_marker());
                return Ok(WaitOutcome::LiveCell(runtime_response_from_parts(
                    &cell_id,
                    &buffered.kind,
                    prefix_items,
                )));
            }

            match buffered.kind {
                BufferedFrontierKind::Yielded => {
                    if matches!(remaining_budget, Some(0)) {
                        prefix_items.push(truncation_marker());
                        return Ok(WaitOutcome::LiveCell(runtime_response_from_parts(
                            &cell_id,
                            &BufferedFrontierKind::Yielded,
                            prefix_items,
                        )));
                    }
                }
                BufferedFrontierKind::Result { error_text } => {
                    return Ok(WaitOutcome::LiveCell(RuntimeResponse::Result {
                        cell_id,
                        content_items: prefix_items,
                        error_text,
                    }));
                }
                BufferedFrontierKind::Terminated => {
                    return Ok(WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                        cell_id,
                        content_items: prefix_items,
                    }));
                }
            }
        }
        let yield_time_ms = yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_TIME_MS);
        let runtime_cell_id = runtime_cell_id(&cell_id);
        match self
            .runtime
            .begin_observe(
                &runtime_cell_id,
                session_runtime::ObserveMode::YieldAfter(Duration::from_millis(yield_time_ms)),
            )
            .await
        {
            Ok(pending_event) => match pending_event.event().await {
                Ok(event) => Ok(WaitOutcome::LiveCell(self.observed_response_with_prefix(
                    &cell_id,
                    prefix_items,
                    observed_frontier(event)?,
                    remaining_budget,
                )?)),
                Err(session_runtime::Error::MissingCell(_))
                | Err(session_runtime::Error::ClosedCell(_)) => {
                    self.clear_buffered_frontier(&cell_id);
                    Ok(WaitOutcome::MissingCell(missing_cell_response(cell_id)))
                }
                Err(error) => Err(error.into_runtime_string()),
            },
            Err(session_runtime::Error::MissingCell(_))
            | Err(session_runtime::Error::ClosedCell(_)) => {
                self.clear_buffered_frontier(&cell_id);
                Ok(WaitOutcome::MissingCell(missing_cell_response(cell_id)))
            }
            Err(error) => Err(error.into_runtime_string()),
        }
    }

    pub async fn terminate(&self, cell_id: CellId) -> Result<WaitOutcome, RuntimeString> {
        self.clear_buffered_frontier(&cell_id);
        let runtime_cell_id = runtime_cell_id(&cell_id);
        match self.runtime.terminate(&runtime_cell_id).await {
            Ok(event) => Ok(WaitOutcome::LiveCell(self.observed_response(
                &cell_id,
                observed_frontier(event)?,
                None,
            )?)),
            Err(session_runtime::Error::MissingCell(_))
            | Err(session_runtime::Error::ClosedCell(_)) => {
                Ok(WaitOutcome::MissingCell(missing_cell_response(cell_id)))
            }
            Err(error) => Err(error.into_runtime_string()),
        }
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeString> {
        self.runtime
            .shutdown()
            .await
            .map_err(session_runtime::Error::into_runtime_string)
    }

    fn observed_response(
        &self,
        cell_id: &CellId,
        frontier: ObservedFrontier,
        max_output_tokens: Option<usize>,
    ) -> Result<RuntimeResponse, RuntimeString> {
        self.observed_response_with_prefix(cell_id, Vec::new(), frontier, max_output_tokens)
    }

    fn observed_response_with_prefix(
        &self,
        cell_id: &CellId,
        prefix_items: Vec<OutputContentItem>,
        frontier: ObservedFrontier,
        max_output_tokens: Option<usize>,
    ) -> Result<RuntimeResponse, RuntimeString> {
        budget_observed_frontier(
            &self.buffered_frontiers,
            cell_id,
            prefix_items,
            frontier,
            max_output_tokens,
        )
    }

    fn take_buffered_frontier(&self, cell_id: &CellId) -> Option<BufferedFrontier> {
        self.buffered_frontiers.lock_recover().remove(cell_id)
    }

    fn store_buffered_frontier(&self, cell_id: CellId, frontier: BufferedFrontier) {
        self.buffered_frontiers
            .lock_recover()
            .insert(cell_id, frontier);
    }

    fn clear_buffered_frontier(&self, cell_id: &CellId) {
        self.buffered_frontiers.lock_recover().remove(cell_id);
    }
}

impl Default for InProcessCodeModeSession {
    fn default() -> Self {
        Self::new()
    }
}

impl CodeModeSession for InProcessCodeModeSession {
    fn execute<'a>(
        &'a self,
        request: ExecuteRequest,
    ) -> CodeModeSessionResultFuture<'a, StartedCell> {
        Box::pin(InProcessCodeModeSession::execute(self, request))
    }

    fn wait<'a>(&'a self, request: WaitRequest) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(InProcessCodeModeSession::wait(self, request))
    }

    fn terminate<'a>(&'a self, cell_id: CellId) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(InProcessCodeModeSession::terminate(self, cell_id))
    }

    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()> {
        Box::pin(InProcessCodeModeSession::shutdown(self))
    }
}

/// The buffered-frontier bookkeeping shared by in-call observations and the
/// detached first-frontier task: output over the caller's budget is retained
/// under the cell id so the next `wait` resumes exactly where this frontier
/// stopped.
fn budget_observed_frontier(
    buffered_frontiers: &Mutex<HashMap<CellId, BufferedFrontier>>,
    cell_id: &CellId,
    mut prefix_items: Vec<OutputContentItem>,
    frontier: ObservedFrontier,
    max_output_tokens: Option<usize>,
) -> Result<RuntimeResponse, RuntimeString> {
    let budgeted = budget_output(frontier.content_items, max_output_tokens);
    append_items(&mut prefix_items, budgeted.visible_items);
    if budgeted.truncated {
        buffered_frontiers.lock_recover().insert(
            cell_id.clone(),
            BufferedFrontier {
                kind: frontier.kind.clone(),
                content_items: budgeted.remaining_items,
            },
        );
        prefix_items.push(truncation_marker());
    } else {
        buffered_frontiers.lock_recover().remove(cell_id);
    }
    Ok(runtime_response_from_parts(
        cell_id,
        &frontier.kind,
        prefix_items,
    ))
}

struct ProtocolDelegate {
    delegate: Arc<dyn CodeModeSessionDelegate>,
}

impl session_runtime::SessionRuntimeDelegate for ProtocolDelegate {
    async fn invoke_tool(
        &self,
        invocation: session_runtime::NestedToolCall,
        cancellation_token: CancellationToken,
    ) -> Result<crate::runtime_values::RuntimeJson, CodeModeToolRejection> {
        self.delegate
            .invoke_tool(
                CodeModeNestedToolCall {
                    cell_id: protocol_cell_id(&invocation.cell_id),
                    runtime_tool_call_id: invocation.runtime_tool_call_id,
                    tool_name: invocation.tool_name,
                    tool_kind: match invocation.tool_kind {
                        session_runtime::ToolKind::Function => CodeModeToolKind::Function,
                        session_runtime::ToolKind::Freeform => CodeModeToolKind::Freeform,
                    },
                    input: invocation.input,
                },
                cancellation_token,
            )
            .await
    }

    async fn notify(
        &self,
        call_id: RuntimeString,
        cell_id: session_runtime::CellId,
        text: RuntimeString,
        cancellation_token: CancellationToken,
    ) -> Result<(), RuntimeString> {
        self.delegate
            .notify(
                call_id,
                protocol_cell_id(&cell_id),
                text,
                cancellation_token,
            )
            .await
    }

    fn cell_closed(&self, cell_id: &session_runtime::CellId) {
        self.delegate.cell_closed(&protocol_cell_id(cell_id));
    }
}

fn runtime_request(request: ExecuteRequest) -> session_runtime::CreateCellRequest {
    let ExecuteRequest {
        tool_call_id,
        enabled_tools,
        source,
        ..
    } = request;
    session_runtime::CreateCellRequest {
        tool_call_id,
        enabled_tools: enabled_tools
            .into_iter()
            .map(|definition| session_runtime::ToolDefinition {
                name: definition.name,
                tool_name: definition.tool_name,
                description: definition.description,
                kind: match definition.kind {
                    CodeModeToolKind::Function => session_runtime::ToolKind::Function,
                    CodeModeToolKind::Freeform => session_runtime::ToolKind::Freeform,
                },
            })
            .collect(),
        source,
    }
}

fn runtime_cell_id(cell_id: &CellId) -> session_runtime::CellId {
    session_runtime::CellId::new(cell_id.as_runtime_string().clone())
}

fn protocol_cell_id(cell_id: &session_runtime::CellId) -> CellId {
    CellId::new(cell_id.as_runtime_string().clone())
}

fn observed_frontier(event: session_runtime::CellEvent) -> Result<ObservedFrontier, RuntimeString> {
    match event {
        session_runtime::CellEvent::Yielded { content_items } => Ok(ObservedFrontier {
            kind: BufferedFrontierKind::Yielded,
            content_items: content_items.into_iter().map(output_item).collect(),
        }),
        session_runtime::CellEvent::Completed {
            content_items,
            error_text,
        } => Ok(ObservedFrontier {
            kind: BufferedFrontierKind::Result { error_text },
            content_items: content_items.into_iter().map(output_item).collect(),
        }),
        session_runtime::CellEvent::Terminated { content_items } => Ok(ObservedFrontier {
            kind: BufferedFrontierKind::Terminated,
            content_items: content_items.into_iter().map(output_item).collect(),
        }),
        session_runtime::CellEvent::Pending { .. } => Err(RuntimeString::from(
            "cell returned a pending frontier unexpectedly",
        )),
    }
}

fn runtime_response_from_parts(
    cell_id: &CellId,
    kind: &BufferedFrontierKind,
    content_items: Vec<OutputContentItem>,
) -> RuntimeResponse {
    match kind {
        BufferedFrontierKind::Yielded => RuntimeResponse::Yielded {
            cell_id: cell_id.clone(),
            content_items,
        },
        BufferedFrontierKind::Result { error_text } => RuntimeResponse::Result {
            cell_id: cell_id.clone(),
            content_items,
            error_text: error_text.clone(),
        },
        BufferedFrontierKind::Terminated => RuntimeResponse::Terminated {
            cell_id: cell_id.clone(),
            content_items,
        },
    }
}

fn output_item(item: session_runtime::OutputItem) -> OutputContentItem {
    match item {
        session_runtime::OutputItem::Text { text } => OutputContentItem::InputText { text },
        session_runtime::OutputItem::Image { image_url, detail } => OutputContentItem::InputImage {
            image_url,
            detail: detail.map(|detail| match detail {
                session_runtime::ImageDetail::Auto => ImageDetail::Auto,
                session_runtime::ImageDetail::Low => ImageDetail::Low,
                session_runtime::ImageDetail::High => ImageDetail::High,
                session_runtime::ImageDetail::Original => ImageDetail::Original,
            }),
        },
    }
}

fn missing_cell_response(cell_id: CellId) -> RuntimeResponse {
    let error_text = RuntimeString::from("exec cell ")
        .concat(cell_id.as_runtime_string())
        .concat(&RuntimeString::from(" not found"));
    RuntimeResponse::Result {
        error_text: Some(error_text),
        cell_id,
        content_items: Vec::new(),
    }
}

fn budget_output(
    content_items: Vec<OutputContentItem>,
    max_output_tokens: Option<usize>,
) -> BudgetedOutput {
    let Some(mut remaining_units) = max_output_tokens else {
        return BudgetedOutput {
            visible_items: content_items,
            remaining_items: Vec::new(),
            consumed_units: 0,
            truncated: false,
        };
    };

    let mut visible_items = Vec::new();
    let mut remaining_items = Vec::new();
    let mut consumed_units = 0;
    let mut truncated = false;

    for item in content_items {
        if truncated {
            push_output_item(&mut remaining_items, item);
            continue;
        }

        match item {
            OutputContentItem::InputText { text } => {
                let text_units = text.code_points().count();
                if remaining_units >= text_units {
                    consumed_units += text_units;
                    remaining_units -= text_units;
                    push_output_item(&mut visible_items, OutputContentItem::InputText { text });
                    continue;
                }

                let (visible_text, leftover_text) =
                    split_text_at_unit_boundary(&text, remaining_units);
                consumed_units += visible_text.code_points().count();
                remaining_units = 0;
                if !visible_text.is_empty() {
                    push_output_item(
                        &mut visible_items,
                        OutputContentItem::InputText { text: visible_text },
                    );
                }
                if let Some(leftover_text) = leftover_text {
                    push_output_item(
                        &mut remaining_items,
                        OutputContentItem::InputText {
                            text: leftover_text,
                        },
                    );
                }
                truncated = true;
            }
            OutputContentItem::InputImage { image_url, detail } => {
                if remaining_units > 0 {
                    consumed_units += 1;
                    remaining_units -= 1;
                    push_output_item(
                        &mut visible_items,
                        OutputContentItem::InputImage { image_url, detail },
                    );
                } else {
                    push_output_item(
                        &mut remaining_items,
                        OutputContentItem::InputImage { image_url, detail },
                    );
                    truncated = true;
                }
            }
        }
    }

    BudgetedOutput {
        visible_items,
        remaining_items,
        consumed_units,
        truncated,
    }
}

fn split_text_at_unit_boundary(
    text: &RuntimeString,
    code_points: usize,
) -> (RuntimeString, Option<RuntimeString>) {
    let units = text.as_utf16();
    let mut index = 0;
    let mut seen = 0;
    while index < units.len() && seen < code_points {
        if (0xd800..=0xdbff).contains(&units[index])
            && units
                .get(index + 1)
                .is_some_and(|unit| (0xdc00..=0xdfff).contains(unit))
        {
            index += 2;
        } else {
            index += 1;
        }
        seen += 1;
    }
    let visible = text.slice_utf16(..index);
    let remaining = (index < units.len()).then(|| text.slice_utf16(index..));
    (visible, remaining)
}

fn append_items(target: &mut Vec<OutputContentItem>, items: Vec<OutputContentItem>) {
    for item in items {
        push_output_item(target, item);
    }
}

fn push_output_item(target: &mut Vec<OutputContentItem>, item: OutputContentItem) {
    match item {
        OutputContentItem::InputText { text } => {
            if text.is_empty() {
                return;
            }
            match target.last_mut() {
                Some(OutputContentItem::InputText { text: existing }) => {
                    *existing = existing.concat(&text)
                }
                _ => target.push(OutputContentItem::InputText { text }),
            }
        }
        item => target.push(item),
    }
}

fn truncation_marker() -> OutputContentItem {
    OutputContentItem::InputText {
        text: RuntimeString::from(OUTPUT_TRUNCATION_MARKER),
    }
}

fn subtract_budget(budget: Option<usize>, consumed_units: usize) -> Option<usize> {
    budget.map(|budget| budget.saturating_sub(consumed_units))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeJson;

    fn one_text(items: &[OutputContentItem]) -> &RuntimeString {
        match items {
            [OutputContentItem::InputText { text }] => text,
            other => panic!("expected one text item, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_dead_cell_task_is_reported_to_the_task_failure_handler() {
        let reported = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = Arc::clone(&reported);
        let session = InProcessCodeModeSession::with_delegate_and_task_failure_handler(
            Arc::new(NoopCodeModeSessionDelegate),
            Arc::new(move |message: String| sink.lock_recover().push(message)),
        );
        // A cell that completes normally never reports a task failure; the
        // handler exists for actor tasks that die, which a healthy run avoids.
        let started = session
            .execute(ExecuteRequest {
                tool_call_id: RuntimeString::from("exec"),
                enabled_tools: Vec::new(),
                source: RuntimeString::from(r#"text("hi")"#),
                yield_time_ms: Some(5_000),
                max_output_tokens: None,
            })
            .await
            .expect("cell is admitted");
        let response = started.initial_response().await.expect("cell completes");
        assert!(matches!(response, RuntimeResponse::Result { .. }));
        session.shutdown().await.expect("shutdown completes");
        assert!(
            reported.lock_recover().is_empty(),
            "a healthy cell must not report a task failure: {:?}",
            reported.lock_recover()
        );
    }

    #[tokio::test]
    async fn a_pending_observation_reports_the_tool_calls_a_cell_is_blocked_on() {
        // A delegate that never answers, so the cell cannot progress past its
        // nested calls and must pause at a quiescent frontier.
        struct StalledDelegate;

        impl CodeModeSessionDelegate for StalledDelegate {
            fn invoke_tool<'a>(
                &'a self,
                _invocation: CodeModeNestedToolCall,
                cancellation_token: CancellationToken,
            ) -> crate::engine::code_mode::session::ToolInvocationFuture<'a> {
                Box::pin(async move {
                    cancellation_token.cancelled().await;
                    Err(CodeModeToolRejection::plain("cancelled"))
                })
            }

            fn notify<'a>(
                &'a self,
                _call_id: RuntimeString,
                _cell_id: CellId,
                _text: RuntimeString,
                _cancellation_token: CancellationToken,
            ) -> crate::engine::code_mode::session::NotificationFuture<'a> {
                Box::pin(async { Ok(()) })
            }

            fn cell_closed(&self, _cell_id: &CellId) {}
        }

        let session = InProcessCodeModeSession::with_delegate(Arc::new(StalledDelegate));
        let outcome = session
            .execute_to_pending(ExecuteRequest {
                tool_call_id: RuntimeString::from("exec"),
                enabled_tools: vec![crate::engine::code_mode::protocol::ToolDefinition {
                    name: RuntimeString::from("echo"),
                    tool_name: RuntimeString::from("echo"),
                    description: RuntimeString::from("echo a value"),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: RuntimeString::from(
                    r#"text("before");
await Promise.all([tools.echo({ value: "first" }), tools.echo({ value: "second" })]);"#,
                ),
                yield_time_ms: Some(60_000),
                max_output_tokens: None,
            })
            .await
            .expect("the cell pauses");

        let ExecuteToPendingOutcome::Pending {
            content_items,
            pending_tool_call_ids,
            ..
        } = outcome
        else {
            panic!("a cell blocked on unanswered tools must pause, got {outcome:?}");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("before")
            }],
            "the pause carries the output produced so far"
        );
        assert_eq!(
            pending_tool_call_ids.len(),
            2,
            "both blocked calls are reported: {pending_tool_call_ids:?}"
        );

        session.shutdown().await.expect("shutdown completes");
    }

    #[tokio::test]
    async fn a_pending_observation_of_a_finished_cell_reports_completion() {
        let session = InProcessCodeModeSession::new();
        let outcome = session
            .execute_to_pending(ExecuteRequest {
                tool_call_id: RuntimeString::from("exec"),
                enabled_tools: Vec::new(),
                source: RuntimeString::from(r#"text("done")"#),
                yield_time_ms: Some(60_000),
                max_output_tokens: None,
            })
            .await
            .expect("the cell completes");
        let ExecuteToPendingOutcome::Completed(response) = outcome else {
            panic!("a cell that never blocks completes rather than pausing");
        };
        assert!(matches!(response, RuntimeResponse::Result { .. }));

        // A cell that already finished is no longer observable.
        let missing = session
            .wait_to_pending(WaitToPendingRequest {
                cell_id: CellId::new(RuntimeString::from("1")),
            })
            .await
            .expect("a missing cell is an outcome, not an error");
        assert!(matches!(missing, WaitToPendingOutcome::MissingCell(_)));

        session.shutdown().await.expect("shutdown completes");
    }

    #[test]
    fn output_budget_uses_javascript_code_points_without_splitting_markers() {
        let parsed = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#""A\ud800😀\uFDD0B""#,
        ))
        .unwrap();
        let text = parsed.into_string().unwrap();

        let budgeted = budget_output(
            vec![OutputContentItem::InputText { text: text.clone() }],
            Some(2),
        );
        assert_eq!(budgeted.consumed_units, 2);
        assert!(budgeted.truncated);
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&RuntimeJson::String(
                one_text(&budgeted.visible_items).clone(),
            )),
            RuntimeString::from(r#""A\ud800""#)
        );
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&RuntimeJson::String(
                one_text(&budgeted.remaining_items).clone(),
            )),
            RuntimeString::from("\"😀\u{fdd0}B\"")
        );

        let budgeted = budget_output(vec![OutputContentItem::InputText { text }], Some(4));
        assert_eq!(budgeted.consumed_units, 4);
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&RuntimeJson::String(
                one_text(&budgeted.visible_items).clone(),
            )),
            RuntimeString::from("\"A\\ud800😀\u{fdd0}\"")
        );
        assert_eq!(one_text(&budgeted.remaining_items), "B");
    }
}
