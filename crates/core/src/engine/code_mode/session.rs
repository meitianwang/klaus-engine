//! The code-mode session seam.
//!
//! A session owns the cells of one thread: cells executed in the same session
//! share stored values, and separate sessions keep those values isolated. The
//! seam is the injection point for WHERE script runtimes run — in this
//! process, in a host subprocess, or on a remote host — so the surrounding
//! engine wiring is identical for every carrier. Two kinds of runtime hang
//! off the seam: cell sessions ([`CodeModeSession`]) and the persistent REPL
//! session ([`ReplSessionHandle`]).
//!
//! Host callbacks (nested tool invocation, live status text, cell teardown)
//! flow the other way through [`CodeModeSessionDelegate`], which the engine
//! supplies when it creates a session. A remote carrier forwards those
//! callbacks over its transport and replays them against the same delegate, so
//! nested tool calls always run through the owning engine's pipeline —
//! permissions and hooks included — no matter where the script itself executes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, ExecuteRequest, RuntimeResponse, WaitOutcome, WaitRequest,
};
use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::CodeModeToolRejection;
use crate::types::tool::InvokeToolFn;

pub use crate::engine::code_mode::runtime::repl_session::{
    ReplExecOutcome, ReplExecRequest, ReplToolBinding,
};

/// A fallible session operation.
pub type CodeModeSessionResultFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RuntimeString>> + Send + 'a>>;

/// A session handed out by a [`CodeModeSessionProvider`].
pub type CodeModeSessionProviderFuture<'a> =
    CodeModeSessionResultFuture<'a, Arc<dyn CodeModeSession>>;

/// One nested tool invocation routed back into the owning engine.
pub type ToolInvocationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeJson, CodeModeToolRejection>> + Send + 'a>>;

/// One live status message routed back into the owning engine.
pub type NotificationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), RuntimeString>> + Send + 'a>>;

/// A cell that has been admitted and assigned its id, whose first frontier is
/// still resolving.
///
/// The id is available BEFORE the response: callers use it to attach live
/// status routing to the cell they just started, then await the response.
pub struct StartedCell {
    pub cell_id: CellId,
    initial_response: CodeModeSessionResultFuture<'static, RuntimeResponse>,
}

impl StartedCell {
    /// Build from a channel carrying the already-mapped response.
    pub fn from_result_receiver(
        cell_id: CellId,
        initial_response_rx: oneshot::Receiver<Result<RuntimeResponse, RuntimeString>>,
    ) -> Self {
        Self {
            cell_id,
            initial_response: Box::pin(async move {
                initial_response_rx
                    .await
                    .map_err(|_| RuntimeString::from("exec runtime ended unexpectedly"))?
            }),
        }
    }

    /// Build from an arbitrary future producing the first frontier.
    pub fn from_future(
        cell_id: CellId,
        initial_response: CodeModeSessionResultFuture<'static, RuntimeResponse>,
    ) -> Self {
        Self {
            cell_id,
            initial_response,
        }
    }

    /// Await the cell's first frontier (yield, result, or termination).
    pub async fn initial_response(self) -> Result<RuntimeResponse, RuntimeString> {
        self.initial_response.await
    }
}

/// The engine-side callbacks a session invokes while its cells run.
///
/// Every method carries the `cell_id` it belongs to, so one delegate serves
/// every cell of a session and can route work per cell.
pub trait CodeModeSessionDelegate: Send + Sync {
    /// Invoke another tool on behalf of a running cell. The cancellation token
    /// fires when the cell is terminated or its observation budget expires; an
    /// implementation MUST stop the nested call when it does.
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a>;

    /// Surface a live status message from a running cell.
    fn notify<'a>(
        &'a self,
        call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
        cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a>;

    /// Release delegate state for a cell that reached a terminal state.
    fn cell_closed(&self, cell_id: &CellId);
}

/// A durable code-mode session owning the cells of one thread.
///
/// Implementations may execute cells in this process or remotely; the contract
/// below is identical either way.
pub trait CodeModeSession: Send + Sync {
    /// Admit a new cell and observe it up to its first frontier.
    fn execute<'a>(
        &'a self,
        request: ExecuteRequest,
    ) -> CodeModeSessionResultFuture<'a, StartedCell>;

    /// Resume observing a live cell. A cell that no longer exists resolves to
    /// [`WaitOutcome::MissingCell`] rather than an error.
    fn wait<'a>(&'a self, request: WaitRequest) -> CodeModeSessionResultFuture<'a, WaitOutcome>;

    /// Terminate a live cell and report the frontier it stopped at.
    fn terminate<'a>(&'a self, cell_id: CellId) -> CodeModeSessionResultFuture<'a, WaitOutcome>;

    /// Cancel every cell and await their teardown.
    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()>;
}

/// Creates code-mode sessions for engine threads.
///
/// A provider MAY share one carrier (for example a single host subprocess)
/// across every session it creates; session isolation is the session's job,
/// not the carrier's.
pub trait CodeModeSessionProvider: Send + Sync {
    fn create_session<'a>(
        &'a self,
        delegate: Arc<dyn CodeModeSessionDelegate>,
    ) -> CodeModeSessionProviderFuture<'a>;

    /// Create the REPL handle for one engine session. A provider that cannot
    /// serve the requested carrier fails HERE; callers surface the failure as
    /// an infrastructure error instead of silently switching carriers.
    fn create_repl_session<'a>(
        &'a self,
    ) -> CodeModeSessionResultFuture<'a, Arc<dyn ReplSessionHandle>>;
}

/// A fallible REPL evaluation.
pub type ReplExecFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReplExecOutcome, String>> + Send + 'a>>;

/// The REPL runtime seam: a handle to one session's persistent JS context,
/// wherever that context runs — in this process or in a host subprocess.
///
/// Nested tool calls always route back through the caller's `invoke_tool`
/// bridge, so permissions and hooks apply to every nested call regardless of
/// the carrier evaluating the script.
pub trait ReplSessionHandle: Send + Sync {
    /// Run one evaluation to completion. `signal` interrupts the evaluation;
    /// the outcome preserves whatever the context captured before it stopped.
    fn execute<'a>(
        &'a self,
        request: ReplExecRequest,
        invoke_tool: Option<InvokeToolFn>,
        signal: &'a CancellationToken,
    ) -> ReplExecFuture<'a>;
}

/// A delegate with no engine behind it: nested tools are unavailable, status
/// messages are dropped.
///
/// `invoke_tool` waits for cancellation before failing, so a program that
/// awaits an unavailable tool is stopped by the cell's own lifecycle instead
/// of racing an immediate rejection.
pub struct NoopCodeModeSessionDelegate;

impl CodeModeSessionDelegate for NoopCodeModeSessionDelegate {
    fn invoke_tool<'a>(
        &'a self,
        _invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        Box::pin(async move {
            cancellation_token.cancelled().await;
            Err(CodeModeToolRejection::plain(
                "code mode nested tools are unavailable",
            ))
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: RuntimeString,
        _cell_id: CellId,
        _text: RuntimeString,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, _cell_id: &CellId) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn started_cell_reports_a_dropped_sender_as_a_runtime_end() {
        let (tx, rx) = oneshot::channel();
        let started = StartedCell::from_result_receiver(CellId::new(RuntimeString::from("7")), rx);
        assert_eq!(started.cell_id, CellId::new(RuntimeString::from("7")));
        drop(tx);
        assert_eq!(
            started.initial_response().await.unwrap_err(),
            "exec runtime ended unexpectedly"
        );
    }

    #[tokio::test]
    async fn noop_delegate_fails_nested_tools_only_after_cancellation() {
        let delegate = NoopCodeModeSessionDelegate;
        let token = CancellationToken::new();
        let invocation = CodeModeNestedToolCall {
            cell_id: CellId::new(RuntimeString::from("1")),
            runtime_tool_call_id: RuntimeString::from("call-1"),
            tool_name: RuntimeString::from("read"),
            tool_kind: crate::engine::code_mode::protocol::CodeModeToolKind::Function,
            input: None,
        };
        let pending = delegate.invoke_tool(invocation, token.clone());
        tokio::pin!(pending);
        assert!(
            futures::poll!(pending.as_mut()).is_pending(),
            "an uncancelled nested call must stay pending"
        );
        token.cancel();
        assert_eq!(
            pending.await.unwrap_err().message,
            "code mode nested tools are unavailable"
        );
    }
}
