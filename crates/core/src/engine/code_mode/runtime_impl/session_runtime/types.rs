use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::CodeModeToolRejection;

/// Identifies one execution cell within a session runtime.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CellId(RuntimeString);

impl CellId {
    pub(crate) fn new(value: RuntimeString) -> Self {
        Self(value)
    }

    pub(crate) fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }
}

/// Selects the next observable frontier for a running cell.
///
/// `PendingFrontier` is reachable only through the runtime's introspection
/// surface (`execute_to_pending` / `wait_to_pending`), which production does not
/// use — `exec` / `wait` always observe up to a yield.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObserveMode {
    YieldAfter(Duration),
    PendingFrontier,
}

/// An observable cell lifecycle event.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CellEvent {
    Yielded {
        content_items: Vec<OutputItem>,
    },
    Pending {
        content_items: Vec<OutputItem>,
        pending_tool_call_ids: Vec<RuntimeString>,
    },
    Completed {
        content_items: Vec<OutputItem>,
        error_text: Option<RuntimeString>,
    },
    Terminated {
        content_items: Vec<OutputItem>,
    },
}

/// Output emitted by a cell since its preceding observation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OutputItem {
    Text {
        text: RuntimeString,
    },
    Image {
        image_url: RuntimeString,
        detail: Option<ImageDetail>,
    },
}

/// Requested image fidelity for an output image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

/// Transport-neutral input for creating a cell.
///
/// The owning session assigns the cell ID when it admits the request.
pub(crate) struct CreateCellRequest {
    pub(crate) tool_call_id: RuntimeString,
    pub(crate) enabled_tools: Vec<ToolDefinition>,
    pub(crate) source: RuntimeString,
}

/// Tool metadata exposed to code running inside a cell.
pub(crate) struct ToolDefinition {
    pub(crate) name: RuntimeString,
    pub(crate) tool_name: RuntimeString,
    pub(crate) description: RuntimeString,
    pub(crate) kind: ToolKind,
}

/// The JavaScript calling convention for a tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ToolKind {
    Function,
    Freeform,
}

/// A nested tool request emitted by a running cell.
#[allow(dead_code)]
pub(crate) struct NestedToolCall {
    pub(crate) cell_id: CellId,
    pub(crate) runtime_tool_call_id: RuntimeString,
    pub(crate) tool_name: RuntimeString,
    pub(crate) tool_kind: ToolKind,
    pub(crate) input: Option<RuntimeJson>,
}

/// Host callbacks used by cells owned by a [`super::SessionRuntime`].
///
/// Implementations must honor cancellation tokens. `cell_closed` is called
/// after the runtime has stopped routing requests to the cell.
pub(crate) trait SessionRuntimeDelegate: Send + Sync + 'static {
    fn invoke_tool(
        &self,
        invocation: NestedToolCall,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<RuntimeJson, CodeModeToolRejection>> + Send;

    fn notify(
        &self,
        call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<(), RuntimeString>> + Send;

    fn cell_closed(&self, cell_id: &CellId);
}

/// A failure reported by a session runtime operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Error {
    ShuttingDown,
    CellIdSpaceExhausted,
    DuplicateCell(CellId),
    MissingCell(CellId),
    BusyObserver(CellId),
    AlreadyTerminating(CellId),
    ClosedCell(CellId),
    Runtime(RuntimeString),
}

impl Error {
    pub(crate) fn into_runtime_string(self) -> RuntimeString {
        match self {
            Self::ShuttingDown => RuntimeString::from("code mode session is shutting down"),
            Self::CellIdSpaceExhausted => {
                RuntimeString::from("code mode session exhausted its cell ID space")
            }
            Self::DuplicateCell(cell_id) => cell_error(cell_id, " already exists"),
            Self::MissingCell(cell_id) => cell_error(cell_id, " not found"),
            Self::BusyObserver(cell_id) => cell_error(cell_id, " already has an active observer"),
            Self::AlreadyTerminating(cell_id) => cell_error(cell_id, " is already terminating"),
            Self::ClosedCell(cell_id) => cell_error(cell_id, " closed unexpectedly"),
            Self::Runtime(error_text) => error_text,
        }
    }
}

fn cell_error(cell_id: CellId, suffix: &str) -> RuntimeString {
    RuntimeString::from("exec cell ")
        .concat(cell_id.as_runtime_string())
        .concat(&RuntimeString::from(suffix))
}
