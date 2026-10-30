//! Retired `exec` / `wait` tool pair.
//!
//! These tools are no longer mounted into any session: the `REPL` tool is the
//! single programmatic-orchestration surface. The implementation is kept, and
//! compiled only under `test` / the `testing` feature, because the frozen
//! provider-behavior fixtures render their recorded specs from it. Dead-code
//! warnings are expected for the parts only the fixture path exercises.
#![allow(dead_code)]
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

use crate::engine::code_mode::protocol::{
    parse_exec_source, CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest,
    OutputContentItem, RuntimeResponse, ToolDefinition, WaitRequest,
    CODE_MODE_CELL_COMPLETED_EVENT, CODE_MODE_CELL_TERMINATED_EVENT, CODE_MODE_CELL_YIELDED_EVENT,
};
use crate::engine::code_mode::runtime::InProcessCodeModeSessionProvider;
use crate::engine::code_mode::session::{
    CodeModeSession, CodeModeSessionDelegate, CodeModeSessionProvider, NotificationFuture,
    ToolInvocationFuture,
};
use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::content::{ImageBlock, OutputBlock};
use crate::sync_ext::MutexExt;
use crate::tools::build_tool::text_result;
use crate::types::errors::{CodeModeToolRejection, ToolError};
use crate::types::events::ToolDomainEvent;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    InvokeToolFn, InvokeToolResult, Tool, ToolContext, ToolInputKind, ToolProgress, ToolResult,
};

/// A name → identifier-safe key for the `tools` bridge (most engine tool names
/// already are): every unit outside `[A-Za-z0-9_$]` becomes `_`.
pub(crate) fn normalize_tool_name(name: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        name.as_utf16()
            .iter()
            .map(|unit| {
                let ascii = u8::try_from(*unit).ok();
                if ascii.is_some_and(|value| {
                    value.is_ascii_alphanumeric() || value == b'_' || value == b'$'
                }) {
                    *unit
                } else {
                    u16::from(b'_')
                }
            })
            .collect::<Vec<_>>(),
    )
}

/// One live status message a running cell asked the engine to surface.
#[derive(Clone, Debug)]
struct CellStatus {
    cell_id: CellId,
    text: RuntimeString,
}

/// The engine side of the session seam for the `exec` / `wait` tool pair.
///
/// Nested tool calls cross the full engine pipeline (permission + hooks +
/// sandbox) of the call that STARTED the cell, so a cell that outlives its
/// `exec` keeps invoking tools against its own turn even while later calls
/// drive other cells. Live status text is broadcast per cell and picked up by
/// whichever call is observing that cell, so a status raised after a yield —
/// when no call is watching — is dropped instead of being attached to a
/// finished tool call.
struct EngineCodeModeDelegate {
    /// The bridge of the call currently starting a cell, claimed by the first
    /// of `bind_started_cell` or the cell's first nested call — whichever
    /// observes the new cell id first.
    pending_bridge: Mutex<Option<InvokeToolFn>>,
    cell_bridges: Mutex<HashMap<CellId, Option<InvokeToolFn>>>,
    statuses: tokio::sync::broadcast::Sender<CellStatus>,
}

impl EngineCodeModeDelegate {
    fn new() -> Self {
        let (statuses, _) = tokio::sync::broadcast::channel(128);
        Self {
            pending_bridge: Mutex::new(None),
            cell_bridges: Mutex::new(HashMap::new()),
            statuses,
        }
    }

    /// Offer the starting call's bridge to the cell it is about to admit.
    /// `exec` is not concurrency-safe, so at most one cell is being admitted at
    /// a time and the offer cannot be claimed by the wrong cell.
    fn offer_bridge(&self, invoke_tool: Option<InvokeToolFn>) {
        *self.pending_bridge.lock_recover() = invoke_tool;
    }

    /// Bind the offered bridge to the cell that was just admitted, unless the
    /// cell's first nested call already claimed it.
    fn bind_started_cell(&self, cell_id: &CellId) {
        let pending = self.pending_bridge.lock_recover().clone();
        self.cell_bridges
            .lock_recover()
            .entry(cell_id.clone())
            .or_insert(pending);
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CellStatus> {
        self.statuses.subscribe()
    }

    /// The bridge a cell invokes tools through. A cell whose first nested call
    /// arrives before [`Self::bind_started_cell`] claims the pending offer here.
    fn bridge_for_cell(&self, cell_id: &CellId) -> Option<InvokeToolFn> {
        let pending = self.pending_bridge.lock_recover().clone();
        self.cell_bridges
            .lock_recover()
            .entry(cell_id.clone())
            .or_insert(pending)
            .clone()
    }
}

impl CodeModeSessionDelegate for EngineCodeModeDelegate {
    fn invoke_tool<'a>(
        &'a self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        let bridge = self.bridge_for_cell(&invocation.cell_id);
        Box::pin(async move {
            let Some(invoke_tool) = bridge else {
                return Err(CodeModeToolRejection::plain(
                    "code mode nested tools are unavailable",
                ));
            };
            let tool_name = invocation.tool_name.clone();
            let tool_input = invocation.input.unwrap_or(RuntimeJson::Null);
            let result = tokio::select! {
                result = invoke_tool(tool_name.clone(), tool_input) => result,
                _ = cancellation_token.cancelled() => {
                    return Err(CodeModeToolRejection::plain("cancelled"))
                },
            }
            .map_err(CodeModeToolRejection::tool)?;
            invoke_tool_result_to_json(&tool_name, result)
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: RuntimeString,
        cell_id: CellId,
        text: RuntimeString,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        let _ = self.statuses.send(CellStatus { cell_id, text });
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        self.cell_bridges.lock_recover().remove(cell_id);
    }
}

/// Map one nested tool result into the JSON value the cell's `tools.*` call
/// resolves to: a failing tool rejects with its text (or a synthesized message
/// when it produced none), empty output resolves to null, and text that parses
/// as JSON resolves to that value rather than to a string.
fn invoke_tool_result_to_json(
    tool_name: &RuntimeString,
    result: InvokeToolResult,
) -> Result<RuntimeJson, CodeModeToolRejection> {
    let text = text_content(&result.content);
    if result.is_error {
        if text.is_empty() {
            Err(CodeModeToolRejection::plain(
                RuntimeString::from("tool ")
                    .concat(tool_name)
                    .concat(&RuntimeString::from(" failed")),
            ))
        } else {
            Err(CodeModeToolRejection::plain(text))
        }
    } else if result
        .content
        .iter()
        .any(|block| matches!(block, OutputBlock::Image(_)))
    {
        let content = result
            .content
            .into_iter()
            .map(|block| {
                let mut item = RuntimeObject::new();
                match block {
                    OutputBlock::Text(text) => {
                        item.insert("type", RuntimeJson::from("text"));
                        item.insert("text", RuntimeJson::String(text.text));
                    }
                    OutputBlock::Image(image) => {
                        item.insert("type", RuntimeJson::from("image"));
                        item.insert("data", RuntimeJson::String(image.base64));
                        item.insert("mimeType", RuntimeJson::String(image.mime_type));
                    }
                }
                RuntimeJson::Object(item)
            })
            .collect();
        let mut media = RuntimeObject::new();
        media.insert("content", RuntimeJson::Array(content));
        media.insert(
            "structuredContent",
            result.output.unwrap_or(RuntimeJson::Null),
        );
        Ok(RuntimeJson::Object(media))
    } else if text.is_empty() {
        Ok(RuntimeJson::Null)
    } else {
        Ok(crate::runtime_values::runtime_json::parse(&text).unwrap_or(RuntimeJson::String(text)))
    }
}

fn text_content(content: &[OutputBlock]) -> RuntimeString {
    let mut output = RuntimeString::default();
    let mut has_text = false;
    for text in content.iter().filter_map(|block| match block {
        OutputBlock::Text(text) => Some(&text.text),
        OutputBlock::Image(_) => None,
    }) {
        if has_text {
            output = output.concat(&RuntimeString::from("\n"));
        }
        output = output.concat(text);
        has_text = true;
    }
    output
}

/// Drive `future` while forwarding the observed cell's live status text to the
/// calling tool's event stream.
async fn observe_with_statuses<T, F>(
    cell_id: &CellId,
    ctx: &ToolContext,
    statuses: &mut tokio::sync::broadcast::Receiver<CellStatus>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    tokio::pin!(future);
    loop {
        tokio::select! {
            result = &mut future => return result,
            status = statuses.recv() => match status {
                Ok(status) if &status.cell_id == cell_id => {
                    (ctx.emit)(ToolDomainEvent::Status { message: status.text }).await;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
            }
        }
    }
}

struct SharedCodeModeState {
    runtime_id: RuntimeString,
    provider: Arc<dyn CodeModeSessionProvider>,
    /// Created on first use: a remote carrier has to spawn or connect to its
    /// host, which cannot happen while the tool set is being built.
    session: tokio::sync::OnceCell<Arc<dyn CodeModeSession>>,
    delegate: Arc<EngineCodeModeDelegate>,
    tools: Mutex<Vec<Arc<dyn Tool>>>,
}

impl Default for SharedCodeModeState {
    fn default() -> Self {
        Self::with_provider(Arc::new(InProcessCodeModeSessionProvider))
    }
}

impl SharedCodeModeState {
    fn with_provider(provider: Arc<dyn CodeModeSessionProvider>) -> Self {
        Self {
            runtime_id: next_code_mode_runtime_id(),
            provider,
            session: tokio::sync::OnceCell::new(),
            delegate: Arc::new(EngineCodeModeDelegate::new()),
            tools: Mutex::new(Vec::new()),
        }
    }

    async fn session(&self) -> Result<&Arc<dyn CodeModeSession>, ToolError> {
        self.session
            .get_or_try_init(|| {
                self.provider
                    .create_session(Arc::clone(&self.delegate) as Arc<dyn CodeModeSessionDelegate>)
            })
            .await
            .map_err(infrastructure_tool_error)
    }
}

impl SharedCodeModeState {
    fn set_tool_registry(&self, tools: Vec<Arc<dyn Tool>>) {
        *self.tools.lock().unwrap() = tools;
    }

    fn enabled_tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .lock()
            .unwrap()
            .iter()
            .filter(|tool| tool.name() != "exec" && tool.name() != "wait")
            .map(|tool| ToolDefinition {
                name: normalize_tool_name(&tool.name()),
                tool_name: tool.name(),
                description: tool.description(),
                kind: match tool.input_kind() {
                    ToolInputKind::JsonSchema => CodeModeToolKind::Function,
                    ToolInputKind::Freeform { .. } => CodeModeToolKind::Freeform,
                },
                input_schema: match tool.input_kind() {
                    ToolInputKind::JsonSchema => Some(tool.input_schema()),
                    ToolInputKind::Freeform { .. } => None,
                },
                output_schema: tool.output_schema(),
            })
            .collect()
    }
}

static CODE_MODE_RUNTIME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_code_mode_runtime_id() -> RuntimeString {
    let seq = CODE_MODE_RUNTIME_COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    RuntimeString::from(format!("{}-{nanos}-{seq}", std::process::id()))
}

pub struct CodeModeToolSet {
    pub exec: Arc<dyn Tool>,
    pub wait: Arc<dyn Tool>,
    shared: Arc<SharedCodeModeState>,
}

impl CodeModeToolSet {
    /// Cancel active cells while preserving the initialized session for later turns.
    pub(crate) async fn interrupt(&self) -> Result<(), RuntimeString> {
        let Some(session) = self.shared.session.get() else {
            return Ok(());
        };
        let cells: Vec<_> = self
            .shared
            .delegate
            .cell_bridges
            .lock_recover()
            .keys()
            .cloned()
            .collect();
        for cell in cells {
            session.terminate(cell).await?;
        }
        Ok(())
    }

    /// Stop the initialized session without creating one solely for cleanup.
    pub(crate) async fn shutdown(&self) -> Result<(), RuntimeString> {
        match self.shared.session.get() {
            Some(session) => session.shutdown().await,
            None => Ok(()),
        }
    }

    pub fn set_tool_registry(&self, tools: Vec<Arc<dyn Tool>>) {
        self.shared.set_tool_registry(tools);
    }
}

pub fn create_code_mode_tools() -> CodeModeToolSet {
    create_code_mode_tools_with_provider(Arc::new(InProcessCodeModeSessionProvider))
}

/// Build the `exec` / `wait` pair against a chosen carrier.
pub fn create_code_mode_tools_with_provider(
    provider: Arc<dyn CodeModeSessionProvider>,
) -> CodeModeToolSet {
    let shared = Arc::new(SharedCodeModeState::with_provider(provider));
    CodeModeToolSet {
        exec: Arc::new(ExecTool {
            shared: shared.clone(),
            input_schema: build_exec_schema(),
        }),
        wait: Arc::new(WaitTool {
            shared: shared.clone(),
            input_schema: build_wait_schema(),
        }),
        shared,
    }
}

struct ExecTool {
    shared: Arc<SharedCodeModeState>,
    input_schema: JsonSchema,
}

struct WaitTool {
    shared: Arc<SharedCodeModeState>,
    input_schema: JsonSchema,
}

const EXEC_DESCRIPTION: &str = concat!(
    "Execute JavaScript in code-mode. Pass raw JavaScript source.\n\n",
    "Every other tool is available as an async function on the global `tools` object, e.g.\n",
    "`const r = await tools.read({ path: \"a.ts\" })`. Each call runs through the same permission,\n",
    "hook, and sandbox checks as a normal tool call. A tool returns a parsed object when its output\n",
    "is JSON, otherwise a string; a failing tool rejects (use try/catch).\n\n",
    "A tool with images returns {content, structuredContent}. Forward its media in order: ",
    "for (const block of r.content) { if (block.type === 'text') text(block.text); ",
    "else if (block.type === 'image') image(block); }. Saved paths remain model-visible text.\n\n",
    "Global helpers:\n",
    "- `text(value)`: append visible text output.\n",
    "- `image(value)`: append a data-URI or MCP image block output.\n",
    "- `store(key, value)` / `load(key)`: persist a JSON value across exec cells in this session.\n",
    "- `notify(value)`: surface a live status message while the program runs.\n",
    "- `yield_control()`: yield the current cell response and keep running in the background.\n",
    "- `exit()`: end the program successfully right now.\n",
    "- `setTimeout()` / `clearTimeout()`: schedule asynchronous JavaScript work.\n",
    "- `ALL_TOOLS`: array of `{ name, description }` for the available nested tools.\n\n",
    "Environment: plain JavaScript only; no Node, no require, no file system, no network, no console."
);

const WAIT_DESCRIPTION: &str = "Wait for additional output from a live exec cell, or terminate it. Input uses `cell_id`, with optional `yield_time_ms`, `max_output_tokens`, and `terminate`.";

fn build_exec_schema() -> JsonSchema {
    let mut properties = Map::new();
    properties.insert(
        "source".to_string(),
        json!({
            "type": "string",
            "description": "Raw javascript source for the freeform tool."
        }),
    );

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), json!(["source"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn build_wait_schema() -> JsonSchema {
    let mut properties = Map::new();
    properties.insert(
        "cell_id".to_string(),
        json!({
            "type": "string",
            "description": "The cell id returned by exec."
        }),
    );
    properties.insert(
        "yield_time_ms".to_string(),
        json!({
            "type": "integer",
            "minimum": 0,
            "description": "How long to wait for new output before yielding again."
        }),
    );
    properties.insert(
        "max_output_tokens".to_string(),
        json!({
            "type": "integer",
            "minimum": 0,
            "description": "Limit this observation's returned output without discarding the remainder."
        }),
    );
    properties.insert(
        "terminate".to_string(),
        json!({
            "type": "boolean",
            "description": "Terminate the live cell instead of waiting for more output."
        }),
    );

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), json!(["cell_id"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

fn runtime_response_result(response: RuntimeResponse) -> ToolResult {
    let is_error = matches!(
        &response,
        RuntimeResponse::Result {
            error_text: Some(_),
            ..
        }
    );
    let response_json = runtime_codec::encode_value(&response).unwrap_or(RuntimeJson::Null);
    let response_text = crate::runtime_values::runtime_json::stringify(&response_json);
    let items = match &response {
        RuntimeResponse::Result { content_items, .. }
        | RuntimeResponse::Yielded { content_items, .. }
        | RuntimeResponse::Terminated { content_items, .. } => content_items,
    };
    if !items
        .iter()
        .any(|item| matches!(item, OutputContentItem::InputImage { .. }))
    {
        return text_result(response_json, response_text, is_error);
    }
    let mut control = response_json.clone();
    if let RuntimeJson::Object(object) = &mut control {
        object.remove("contentItems");
    }
    let mut result = text_result(
        response_json,
        crate::runtime_values::runtime_json::stringify(&control),
        is_error,
    );
    for item in items {
        match item {
            OutputContentItem::InputText { text } => result.serializable.model_content.extend(
                text_result(RuntimeJson::Null, text.clone(), false)
                    .serializable
                    .model_content,
            ),
            OutputContentItem::InputImage { image_url, detail } => {
                let image = image_url.try_to_utf8().ok().and_then(|url| {
                    let (declaration, base64) = url.strip_prefix("data:")?.split_once(',')?;
                    let mime = declaration.strip_suffix(";base64")?;
                    if !mime.starts_with("image/")
                        || base64.is_empty()
                        || base64::engine::general_purpose::STANDARD
                            .decode(base64)
                            .is_err()
                    {
                        return None;
                    }
                    Some(ImageBlock {
                        mime_type: RuntimeString::from(mime),
                        base64: RuntimeString::from(base64),
                        detail: detail.map(|detail| match detail {
                            crate::engine::code_mode::protocol::ImageDetail::Auto => {
                                crate::schemas::content::ImageDetail::Auto
                            }
                            crate::engine::code_mode::protocol::ImageDetail::Low => {
                                crate::schemas::content::ImageDetail::Low
                            }
                            crate::engine::code_mode::protocol::ImageDetail::High => {
                                crate::schemas::content::ImageDetail::High
                            }
                            crate::engine::code_mode::protocol::ImageDetail::Original => {
                                crate::schemas::content::ImageDetail::Original
                            }
                        }),
                        source_path: None,
                    })
                });
                if let Some(image) = image {
                    result
                        .serializable
                        .model_content
                        .push(OutputBlock::Image(image));
                } else {
                    result.serializable.model_content.extend(
                        text_result(
                            RuntimeJson::Null,
                            "[image unavailable: expected a base64 image data URI]",
                            false,
                        )
                        .serializable
                        .model_content,
                    );
                }
            }
        }
    }
    result
}

async fn record_code_mode_response(
    ctx: &ToolContext,
    runtime_id: &RuntimeString,
    response: &RuntimeResponse,
) -> Result<(), ToolError> {
    let Some(record) = &ctx.record_rollout_event else {
        return Ok(());
    };
    match response {
        RuntimeResponse::Yielded { cell_id, .. } => {
            record(
                CODE_MODE_CELL_YIELDED_EVENT.into(),
                Some(code_mode_event_data(cell_id, runtime_id, None)),
            )
            .await
        }
        RuntimeResponse::Terminated { cell_id, .. } => {
            record(
                CODE_MODE_CELL_TERMINATED_EVENT.into(),
                Some(code_mode_event_data(cell_id, runtime_id, None)),
            )
            .await
        }
        RuntimeResponse::Result {
            cell_id,
            error_text,
            ..
        } => {
            record(
                CODE_MODE_CELL_COMPLETED_EVENT.into(),
                Some(code_mode_event_data(
                    cell_id,
                    runtime_id,
                    Some(error_text.is_none()),
                )),
            )
            .await
        }
    }
}

fn code_mode_event_data(
    cell_id: &crate::engine::code_mode::protocol::CellId,
    runtime_id: &RuntimeString,
    ok: Option<bool>,
) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        RuntimeString::from("cellId"),
        RuntimeJson::String(cell_id.as_runtime_string().clone()),
    );
    object.insert(
        RuntimeString::from("runtimeId"),
        RuntimeJson::String(runtime_id.clone()),
    );
    if let Some(ok) = ok {
        object.insert(RuntimeString::from("ok"), RuntimeJson::Bool(ok));
    }
    RuntimeJson::Object(object)
}

fn parse_exec_input(
    input: Json,
) -> Result<crate::engine::code_mode::protocol::ParsedExecSource, RuntimeString> {
    let input = match input.into_string() {
        Ok(source) => return parse_exec_source(&source),
        Err(input) => input,
    };
    if let Some(source) = input
        .as_object()
        .and_then(|object| object.get("source"))
        .and_then(RuntimeJson::as_string)
    {
        parse_exec_source(source)
    } else {
        Err(RuntimeString::from(
            "exec expects raw JavaScript source text (freeform input) or the provider fallback object `{ \"source\": \"...\" }`.",
        ))
    }
}

fn parse_wait_input(input: Json) -> Result<WaitRequest, RuntimeString> {
    let object = input.as_object().ok_or_else(|| {
        RuntimeString::from("wait expects an object with required field `cell_id`.")
    })?;
    let cell_id = object
        .get("cell_id")
        .or_else(|| object.get("cellId"))
        .and_then(RuntimeJson::as_string)
        .ok_or_else(|| RuntimeString::from("wait requires a string `cell_id`."))?;

    Ok(WaitRequest {
        cell_id: crate::engine::code_mode::protocol::CellId::new(cell_id.clone()),
        yield_time_ms: parse_optional_u64(object, "yield_time_ms", "yieldTimeMs")?,
        max_output_tokens: parse_optional_usize(object, "max_output_tokens", "maxOutputTokens")?,
        terminate: parse_optional_bool(object, "terminate", "terminate")?,
    })
}

fn parse_optional_u64(
    object: &RuntimeObject,
    snake: &str,
    camel: &str,
) -> Result<Option<u64>, RuntimeString> {
    match object.get(snake).or_else(|| object.get(camel)) {
        None | Some(RuntimeJson::Null) => Ok(None),
        Some(RuntimeJson::Number(number)) => RuntimeJson::Number(*number)
            .as_u64()
            .ok_or_else(|| RuntimeString::from(format!("`{snake}` must be a non-negative integer")))
            .map(Some),
        Some(_) => Err(RuntimeString::from(format!(
            "`{snake}` must be a non-negative integer"
        ))),
    }
}

fn parse_optional_usize(
    object: &RuntimeObject,
    snake: &str,
    camel: &str,
) -> Result<Option<usize>, RuntimeString> {
    parse_optional_u64(object, snake, camel)?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| RuntimeString::from(format!("`{snake}` is too large")))
        })
        .transpose()
}

fn parse_optional_bool(
    object: &RuntimeObject,
    snake: &str,
    camel: &str,
) -> Result<Option<bool>, RuntimeString> {
    match object.get(snake).or_else(|| object.get(camel)) {
        None | Some(RuntimeJson::Null) => Ok(None),
        Some(RuntimeJson::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(RuntimeString::from(format!("`{snake}` must be a boolean"))),
    }
}

fn rejected_input_result(message: RuntimeString) -> ToolResult {
    let mut output = RuntimeObject::new();
    output.insert(RuntimeString::from("ok"), RuntimeJson::Bool(false));
    text_result(RuntimeJson::Object(output), message, true)
}

fn infrastructure_tool_error(error: RuntimeString) -> ToolError {
    match error.try_to_utf8() {
        Ok(message) => ToolError::new(message),
        Err(_) => ToolError::new(format!(
            "non-scalar code mode infrastructure error: {error:?}"
        )),
    }
}

#[async_trait]
impl Tool for ExecTool {
    fn static_name(&self) -> &str {
        "exec"
    }

    fn static_description(&self) -> &str {
        EXEC_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_kind(&self) -> ToolInputKind {
        ToolInputKind::Freeform {
            format: RuntimeString::from("javascript"),
        }
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some("executing JavaScript in code-mode".into())
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let parsed = match parse_exec_input(input) {
            Ok(parsed) => parsed,
            Err(message) => return Ok(rejected_input_result(message)),
        };
        self.shared.delegate.offer_bridge(ctx.invoke_tool.clone());
        let mut statuses = self.shared.delegate.subscribe();
        let started = self
            .shared
            .session()
            .await?
            .execute(ExecuteRequest {
                tool_call_id: RuntimeString::from("exec"),
                enabled_tools: self.shared.enabled_tools(),
                source: parsed.source,
                yield_time_ms: parsed.yield_time_ms,
                max_output_tokens: parsed.max_output_tokens,
            })
            .await
            .map_err(infrastructure_tool_error)?;
        let cell_id = started.cell_id.clone();
        self.shared.delegate.bind_started_cell(&cell_id);
        let response =
            observe_with_statuses(&cell_id, ctx, &mut statuses, started.initial_response())
                .await
                .map_err(infrastructure_tool_error)?;
        let persisted = record_code_mode_response(ctx, &self.shared.runtime_id, &response).await;
        let mut result = runtime_response_result(response);
        if let Err(error) = persisted {
            result.serializable.is_error = Some(true);
            result.serializable.model_content.push(OutputBlock::Text(crate::schemas::content::TextBlock {
                text: RuntimeString::from("The code cell returned this result, but its trace could not be persisted. Its effects may already be complete; do not repeat it automatically. Persistence error: ").concat(&error.message),
                citations: None, harness_envelope: None, message_id: None, phase: None,
            }));
        }
        Ok(result)
    }
}

#[async_trait]
impl Tool for WaitTool {
    fn static_name(&self) -> &str {
        "wait"
    }

    fn static_description(&self) -> &str {
        WAIT_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, input: &Json) -> Option<bool> {
        Some(
            input
                .as_object()
                .and_then(|object| object.get("terminate"))
                .and_then(RuntimeJson::as_bool)
                .unwrap_or(false),
        )
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        if input
            .as_object()
            .and_then(|object| object.get("terminate"))
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false)
        {
            Some("terminating exec cell".into())
        } else {
            Some("waiting for exec cell".into())
        }
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let request = match parse_wait_input(input) {
            Ok(request) => request,
            Err(message) => return Ok(rejected_input_result(message)),
        };
        let mut statuses = self.shared.delegate.subscribe();
        let session = self.shared.session().await?;
        let cell_id = request.cell_id.clone();
        let outcome = observe_with_statuses(&cell_id, ctx, &mut statuses, session.wait(request))
            .await
            .map_err(infrastructure_tool_error)?;
        let response = outcome.into();
        let persisted = record_code_mode_response(ctx, &self.shared.runtime_id, &response).await;
        let mut result = runtime_response_result(response);
        if let Err(error) = persisted {
            result.serializable.is_error = Some(true);
            result.serializable.model_content.push(OutputBlock::Text(crate::schemas::content::TextBlock {
                text: RuntimeString::from("The code cell returned this result, but its trace could not be persisted. Its effects may already be complete; do not repeat it automatically. Persistence error: ").concat(&error.message),
                citations: None, harness_envelope: None, message_id: None, phase: None,
            }));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::code_mode::protocol::OutputContentItem;
    use crate::schemas::content::{OutputBlock, TextBlock};
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{InvokeToolResult, ToolCatalogEntry, ToolExposure};
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    type RolloutEvents = Arc<StdMutex<Vec<(RuntimeString, Option<Json>)>>>;

    struct MockTool {
        name: String,
        description: String,
        input_kind: ToolInputKind,
        input_schema: JsonSchema,
    }

    #[async_trait]
    impl Tool for MockTool {
        fn static_name(&self) -> &str {
            &self.name
        }

        fn static_description(&self) -> &str {
            &self.description
        }

        fn static_input_schema(&self) -> &JsonSchema {
            &self.input_schema
        }

        fn input_kind(&self) -> ToolInputKind {
            self.input_kind.clone()
        }

        fn is_enabled(&self) -> bool {
            true
        }

        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }

        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }

        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            Ok(text_result(Json::Null, String::new(), false))
        }
    }

    fn text_response(result: &ToolResult) -> RuntimeResponse {
        runtime_codec::decode_value(&result.serializable.output).expect("runtime response JSON")
    }

    fn runtime_string(json: &str) -> RuntimeString {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(json))
            .expect("runtime string JSON")
            .into_string()
            .expect("JSON string")
    }

    fn exact_json(json: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(json))
            .expect("valid exact test JSON")
    }

    fn runtime_source(source: impl Into<RuntimeString>) -> RuntimeJson {
        RuntimeJson::String(source.into())
    }

    fn wait_input(
        cell_id: &crate::engine::code_mode::protocol::CellId,
        max_output_tokens: Option<usize>,
        terminate: Option<bool>,
    ) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        object.insert(
            RuntimeString::from("cell_id"),
            RuntimeJson::String(cell_id.as_runtime_string().clone()),
        );
        if let Some(max_output_tokens) = max_output_tokens {
            object.insert(
                RuntimeString::from("max_output_tokens"),
                RuntimeJson::from(max_output_tokens),
            );
        }
        if let Some(terminate) = terminate {
            object.insert(
                RuntimeString::from("terminate"),
                RuntimeJson::Bool(terminate),
            );
        }
        RuntimeJson::Object(object)
    }

    fn nested_tool_result_text(
        ok: Option<bool>,
        context: Option<&str>,
        name: &RuntimeString,
        input: &RuntimeJson,
    ) -> RuntimeString {
        let mut object = RuntimeObject::new();
        if let Some(ok) = ok {
            object.insert(RuntimeString::from("ok"), RuntimeJson::Bool(ok));
        }
        if let Some(context) = context {
            object.insert(RuntimeString::from("ctx"), RuntimeJson::from(context));
        }
        object.insert(
            RuntimeString::from("name"),
            RuntimeJson::String(name.clone()),
        );
        object.insert(RuntimeString::from("input"), input.clone());
        crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(object))
    }

    fn setup_tools(extra: Vec<Arc<dyn Tool>>) -> CodeModeToolSet {
        let tool_set = create_code_mode_tools();
        let mut registry = extra;
        registry.push(tool_set.exec.clone());
        registry.push(tool_set.wait.clone());
        tool_set.set_tool_registry(registry);
        tool_set
    }

    fn ctx_with_catalog(extra: &[Arc<dyn Tool>]) -> ToolContext {
        let mut ctx = make_ctx(".");
        let entries = extra
            .iter()
            .map(|tool| ToolCatalogEntry {
                name: tool.name(),
                description: tool.description(),
                search_hint: None,
                exposure: ToolExposure::Direct,
                spec: None,
            })
            .collect::<Vec<_>>();
        ctx.tool_catalog = Some(Box::new(move || entries.clone()));
        ctx
    }

    fn ctx_recording_rollout_events(events: RolloutEvents) -> ToolContext {
        let mut ctx = make_ctx(".");
        ctx.record_rollout_event = Some(Arc::new(move |event, payload| {
            events.lock().unwrap().push((event, payload));
            Box::pin(async { Ok(()) })
        }));
        ctx
    }

    /// A provider that hands out an in-process session and counts how often it
    /// was asked for one.
    struct CountingProvider {
        created: Arc<StdMutex<usize>>,
    }

    impl CodeModeSessionProvider for CountingProvider {
        fn create_session<'a>(
            &'a self,
            delegate: Arc<dyn CodeModeSessionDelegate>,
        ) -> crate::engine::code_mode::session::CodeModeSessionProviderFuture<'a> {
            *self.created.lock().unwrap() += 1;
            Box::pin(async move {
                let session: Arc<dyn CodeModeSession> = Arc::new(
                    crate::engine::code_mode::runtime::InProcessCodeModeSession::with_delegate(
                        delegate,
                    ),
                );
                Ok(session)
            })
        }

        fn create_repl_session<'a>(
            &'a self,
        ) -> crate::engine::code_mode::session::CodeModeSessionResultFuture<
            'a,
            Arc<dyn crate::engine::code_mode::session::ReplSessionHandle>,
        > {
            Box::pin(async {
                let handle: Arc<dyn crate::engine::code_mode::session::ReplSessionHandle> =
                    Arc::new(crate::engine::code_mode::runtime::InProcessReplHandle::default());
                Ok(handle)
            })
        }
    }

    #[tokio::test]
    async fn the_session_is_created_once_by_the_provider_and_shared_by_exec_and_wait() {
        let created = Arc::new(StdMutex::new(0_usize));
        let shared = Arc::new(SharedCodeModeState::with_provider(Arc::new(
            CountingProvider {
                created: Arc::clone(&created),
            },
        )));
        let exec: Arc<dyn Tool> = Arc::new(ExecTool {
            shared: Arc::clone(&shared),
            input_schema: build_exec_schema(),
        });
        let wait: Arc<dyn Tool> = Arc::new(WaitTool {
            shared: Arc::clone(&shared),
            input_schema: build_wait_schema(),
        });
        shared.set_tool_registry(vec![exec.clone(), wait.clone()]);
        let ctx = make_ctx(".");

        assert_eq!(
            *created.lock().unwrap(),
            0,
            "building the tool set must not create a session"
        );

        let first = exec
            .call(
                runtime_source(r#"store("seen", 1); text("one")"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            text_response(&first),
            RuntimeResponse::Result { .. }
        ));
        assert_eq!(*created.lock().unwrap(), 1);

        // The second cell sees the first cell's stored value, proving both
        // calls share one session rather than each creating their own.
        let second = exec
            .call(
                runtime_source(r#"text(String(load("seen")))"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Result { content_items, .. } = text_response(&second) else {
            panic!("expected a completed cell");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("1")
            }]
        );
        assert_eq!(*created.lock().unwrap(), 1);

        // A wait for a cell that already finished reuses the same session too.
        let missing = wait
            .call(
                wait_input(&CellId::new(RuntimeString::from("99")), None, None),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            text_response(&missing),
            RuntimeResponse::Result { .. }
        ));
        assert_eq!(*created.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn a_failing_provider_surfaces_as_an_infrastructure_error() {
        struct FailingProvider;

        impl CodeModeSessionProvider for FailingProvider {
            fn create_session<'a>(
                &'a self,
                _delegate: Arc<dyn CodeModeSessionDelegate>,
            ) -> crate::engine::code_mode::session::CodeModeSessionProviderFuture<'a> {
                Box::pin(async { Err(RuntimeString::from("host is unreachable")) })
            }

            fn create_repl_session<'a>(
                &'a self,
            ) -> crate::engine::code_mode::session::CodeModeSessionResultFuture<
                'a,
                Arc<dyn crate::engine::code_mode::session::ReplSessionHandle>,
            > {
                Box::pin(async { Err(RuntimeString::from("host is unreachable")) })
            }
        }

        let shared = Arc::new(SharedCodeModeState::with_provider(Arc::new(
            FailingProvider,
        )));
        let exec: Arc<dyn Tool> = Arc::new(ExecTool {
            shared,
            input_schema: build_exec_schema(),
        });
        let outcome = exec
            .call(
                runtime_source(r#"text("hi")"#.to_string()),
                &make_ctx("."),
                None,
            )
            .await;
        let Err(error) = outcome else {
            panic!("an unavailable carrier must fail the call");
        };
        assert!(
            error.message.contains("host is unreachable"),
            "{:?}",
            error.message
        );
    }

    #[tokio::test]
    async fn exec_runs_text_in_the_in_process_runtime() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(runtime_source(r#"text("hi")"#.to_string()), &ctx, None)
            .await
            .unwrap();

        assert_eq!(
            text_response(&result),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("hi")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn code_mode_and_locale_sort_share_the_single_v8_platform() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let before = tool_set
            .exec
            .call(runtime_source(r#"text("before")"#.to_string()), &ctx, None)
            .await
            .unwrap();
        assert!(matches!(
            text_response(&before),
            RuntimeResponse::Result {
                content_items,
                error_text: None,
                ..
            } if content_items == vec![OutputContentItem::InputText { text: RuntimeString::from("before") }]
        ));

        let mut names = vec!["\u{2010}.rs".to_string(), "\u{10d6e}.rs".to_string()];
        crate::tools::locale_sort::sort_by_key(&mut names, String::as_str);
        assert_eq!(names, ["\u{10d6e}.rs", "\u{2010}.rs"]);

        let after = tool_set
            .exec
            .call(runtime_source(r#"text("after")"#.to_string()), &ctx, None)
            .await
            .unwrap();
        assert!(matches!(
            text_response(&after),
            RuntimeResponse::Result {
                content_items,
                error_text: None,
                ..
            } if content_items == vec![OutputContentItem::InputText { text: RuntimeString::from("after") }]
        ));
    }

    #[tokio::test]
    async fn locale_sort_can_initialize_the_platform_before_code_mode() {
        let mut names = vec!["\u{2010}.rs".to_string(), "\u{10d6e}.rs".to_string()];
        crate::tools::locale_sort::sort_by_key(&mut names, String::as_str);
        assert_eq!(names, ["\u{10d6e}.rs", "\u{2010}.rs"]);

        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");
        let result = tool_set
            .exec
            .call(
                runtime_source(r#"text("after locale sort")"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            text_response(&result),
            RuntimeResponse::Result {
                content_items,
                error_text: None,
                ..
            } if content_items == vec![OutputContentItem::InputText {
                text: RuntimeString::from("after locale sort")
            }]
        ));
    }

    #[tokio::test]
    async fn exec_accepts_provider_source_fallback_but_rejects_code_field() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let fallback = tool_set
            .exec
            .call(exact_json(r#"{"source":"text(\"hi\")"}"#), &ctx, None)
            .await
            .unwrap();
        assert!(matches!(
            text_response(&fallback),
            RuntimeResponse::Result { .. }
        ));

        let rejected = tool_set
            .exec
            .call(exact_json(r#"{"code":"text(\"nope\")"}"#), &ctx, None)
            .await
            .unwrap();
        let OutputBlock::Text(TextBlock { text, .. }) = &rejected.serializable.model_content[0]
        else {
            panic!("expected text error");
        };
        assert!(text.contains("provider fallback object"), "{text:?}");
    }

    #[tokio::test]
    async fn interrupt_cancels_active_cells_without_closing_the_session() {
        let tool_set = create_code_mode_tools();
        let ctx = make_ctx(".");
        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"yield_control(); await new Promise(() => {});"#.to_owned()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            text_response(&initial),
            RuntimeResponse::Yielded { .. }
        ));
        tool_set.interrupt().await.unwrap();
        assert!(tool_set
            .shared
            .delegate
            .cell_bridges
            .lock_recover()
            .is_empty());
        let next = tool_set
            .exec
            .call(
                runtime_source(r#"text("continued")"#.to_owned()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            text_response(&next),
            RuntimeResponse::Result {
                error_text: None,
                ..
            }
        ));
        tool_set.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn exec_and_wait_support_pending_cells() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"text("before"); yield_control(); text("after");"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let initial = text_response(&initial);
        let RuntimeResponse::Yielded {
            cell_id,
            content_items,
        } = initial
        else {
            panic!("expected yielded response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("before")
            }]
        );

        let waited = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&waited),
            RuntimeResponse::Result {
                cell_id,
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("after")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn exec_and_wait_record_persistable_cell_events() {
        let tool_set = setup_tools(Vec::new());
        let events = Arc::new(StdMutex::new(Vec::new()));
        let ctx = ctx_recording_rollout_events(events.clone());

        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"text("before"); yield_control(); text("after");"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded { cell_id, .. } = text_response(&initial) else {
            panic!("expected yielded response");
        };

        tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();

        let recorded = events.lock().unwrap().clone();
        let runtime_id = recorded[0]
            .1
            .as_ref()
            .and_then(|payload| payload.get("runtimeId"))
            .and_then(RuntimeJson::as_string)
            .expect("runtime id")
            .clone();
        assert_eq!(
            recorded,
            vec![
                (
                    RuntimeString::from(CODE_MODE_CELL_YIELDED_EVENT),
                    Some(code_mode_event_data(
                        &crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                        &runtime_id,
                        None,
                    )),
                ),
                (
                    RuntimeString::from(CODE_MODE_CELL_COMPLETED_EVENT),
                    Some(code_mode_event_data(
                        &crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                        &runtime_id,
                        Some(true),
                    )),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn exec_preserves_yield_control_text_in_raw_source() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(r#"text("yield_control()")"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            text_response(&result),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("yield_control()")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn local_yield_control_shadowing_uses_plain_javascript_semantics() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"function yield_control() { text("local"); }
yield_control();"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            text_response(&result),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("local")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn yield_control_does_not_pause_script_progress_in_the_session() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(
                    r#"yield_control(); store("finished", true); text("done");"#.to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_response(&initial),
            RuntimeResponse::Yielded {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: Vec::new(),
            }
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let probe = tool_set
                    .exec
                    .call(
                        runtime_source(r#"text(String(load("finished")));"#.to_string()),
                        &ctx,
                        None,
                    )
                    .await
                    .unwrap();
                if matches!(
                    text_response(&probe),
                    RuntimeResponse::Result {
                        content_items,
                        error_text: None,
                        ..
                    } if content_items == vec![OutputContentItem::InputText {
                        text: RuntimeString::from("true")
                    }]
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("yielded script never finished");

        let waited = tool_set
            .wait
            .call(exact_json(r#"{"cell_id":"1"}"#), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&waited),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("done")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn exec_pragma_max_output_tokens_limits_initial_response() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(
                    "// @exec: {\"max_output_tokens\": 3}\ntext(\"abcdef\"); yield_control(); text(\"gh\")"
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Yielded {
            cell_id,
            content_items,
        } = text_response(&initial)
        else {
            panic!("expected yielded response");
        };
        assert_eq!(
            content_items,
            vec![
                OutputContentItem::InputText {
                    text: RuntimeString::from("abc")
                },
                OutputContentItem::InputText {
                    text: RuntimeString::from("[exec output truncated; call wait for more]")
                }
            ]
        );

        let waited = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&waited),
            RuntimeResponse::Result {
                cell_id,
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("defgh")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn wait_max_output_tokens_limits_only_that_observation() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"yield_control(); text("abcdef"); text("gh");"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded { cell_id, .. } = text_response(&initial) else {
            panic!("expected yielded response");
        };

        let waited = tool_set
            .wait
            .call(wait_input(&cell_id, Some(4), None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&waited),
            RuntimeResponse::Result {
                cell_id: cell_id.clone(),
                content_items: vec![
                    OutputContentItem::InputText {
                        text: RuntimeString::from("abcd")
                    },
                    OutputContentItem::InputText {
                        text: RuntimeString::from("[exec output truncated; call wait for more]")
                    }
                ],
                error_text: None,
            }
        );

        let remainder = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&remainder),
            RuntimeResponse::Result {
                cell_id,
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("efgh")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn multiple_waits_return_incremental_output_only() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(
                    r#"text("a"); yield_control(); text("b"); yield_control(); text("c");"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded {
            cell_id,
            content_items,
        } = text_response(&initial)
        else {
            panic!("expected yielded response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("a")
            }]
        );

        let first_wait = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&first_wait),
            RuntimeResponse::Result {
                cell_id: cell_id.clone(),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("bc")
                }],
                error_text: None,
            }
        );

        let second_wait = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&second_wait),
            RuntimeResponse::Result {
                cell_id,
                content_items: Vec::new(),
                error_text: Some(RuntimeString::from("exec cell 1 not found")),
            }
        );
    }

    #[tokio::test]
    async fn wait_can_terminate_a_live_cell() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"yield_control(); await new Promise(() => {});"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded { cell_id, .. } = text_response(&initial) else {
            panic!("expected yielded response");
        };

        let terminated = tool_set
            .wait
            .call(wait_input(&cell_id, None, Some(true)), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&terminated),
            RuntimeResponse::Terminated {
                cell_id,
                content_items: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn wait_returns_a_deterministic_missing_cell_error() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let missing = tool_set
            .wait
            .call(exact_json(r#"{"cell_id":"missing"}"#), &ctx, None)
            .await
            .unwrap();

        assert_eq!(
            text_response(&missing),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from(
                    "missing"
                )),
                content_items: Vec::new(),
                error_text: Some(RuntimeString::from("exec cell missing not found")),
            }
        );
    }

    #[tokio::test]
    async fn second_wait_after_final_returns_missing_instead_of_repeating_output() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let initial = tool_set
            .exec
            .call(
                runtime_source(r#"yield_control(); text("done");"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded { cell_id, .. } = text_response(&initial) else {
            panic!("expected yielded response");
        };

        let final_wait = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&final_wait),
            RuntimeResponse::Result {
                cell_id: cell_id.clone(),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("done")
                }],
                error_text: None,
            }
        );

        let missing = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_response(&missing),
            RuntimeResponse::Result {
                cell_id,
                content_items: Vec::new(),
                error_text: Some(RuntimeString::from("exec cell 1 not found")),
            }
        );
    }

    #[tokio::test]
    async fn store_and_load_persist_across_exec_calls_in_one_session() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        tool_set
            .exec
            .call(
                runtime_source(r#"store("k", {"n": 7});"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let loaded = tool_set
            .exec
            .call(
                runtime_source(r#"text(JSON.stringify(load("k")));"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            text_response(&loaded),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("2")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from(r#"{"n":7}"#)
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn v8_store_serializes_value_before_coercing_key_like_ts() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"
                    const effects = [];
                    const key = { toString() { effects.push("key"); return "memo"; } };
                    const value = { toJSON() { effects.push("value"); return { n: 7 }; } };
                    store(key, value);
                    text(effects.join(","));
                    "#
                    .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("value,key"),
            }]
        );
    }

    #[tokio::test]
    async fn v8_store_uses_javascript_string_coercion_for_symbol_keys() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"const key = Symbol("memo"); store(key, { n: 7 }); text(JSON.stringify(load(key)));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(r#"{"n":7}"#),
            }]
        );
    }

    #[tokio::test]
    async fn v8_store_coalesces_undefined_values_to_null_like_ts() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"store("missing", undefined); text(JSON.stringify(load("missing")));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from("null"),
            }]
        );
    }

    #[tokio::test]
    async fn v8_store_value_stringify_errors_are_actionable_and_precede_key_coercion() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"
                    let keyCoerced = false;
                    const key = { toString() { keyCoerced = true; return "memo"; } };
                    try {
                      store(key, 1n);
                    } catch (error) {
                      text(JSON.stringify({
                        name: error.name,
                        message: error.message,
                        string: String(error),
                        keyCoerced,
                      }));
                    }
                    "#
                    .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(
                    r#"{"name":"Error","message":"store() requires a JSON-serializable value (no BigInt, function, or circular reference)","string":"Error: store() requires a JSON-serializable value (no BigInt, function, or circular reference)","keyCoerced":false}"#,
                ),
            }]
        );
    }

    #[tokio::test]
    async fn nested_tool_calls_route_through_invoke_tool() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let mut ctx = ctx_with_catalog(&extra);
        let seen = Arc::new(StdMutex::new(Vec::<(RuntimeString, RuntimeJson)>::new()));
        let seen_clone = seen.clone();
        ctx.invoke_tool = Some(Arc::new(move |name: RuntimeString, input: RuntimeJson| {
            seen_clone
                .lock()
                .unwrap()
                .push((name.clone(), input.clone()));
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(TextBlock {
                        text: nested_tool_result_text(Some(true), None, &name, &input),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            })
        }));

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"text(JSON.stringify(await tools.echo({"value":"x"})));"#.to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[(RuntimeString::from("echo"), exact_json(r#"{"value":"x"}"#))]
        );
        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(r#"{"ok":true,"name":"echo","input":{"value":"x"}}"#)
            }]
        );
    }

    #[tokio::test]
    async fn nested_tool_json_round_trips_a_lone_surrogate_without_sentinel_leakage() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let mut ctx = ctx_with_catalog(&extra);
        let seen = Arc::new(StdMutex::new(None::<Json>));
        let seen_for_bridge = Arc::clone(&seen);
        ctx.invoke_tool = Some(Arc::new(move |_name: RuntimeString, input: RuntimeJson| {
            *seen_for_bridge.lock().unwrap() = Some(input.clone());
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(TextBlock {
                        text: crate::runtime_values::runtime_json::stringify(&input),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            })
        }));

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"const value = await tools.echo(JSON.parse('{"lone":"\\ud800"}')); text(JSON.stringify(value));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_ref(),
            Some(&exact_json(r#"{"lone":"\ud800"}"#))
        );
        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(r#"{"lone":"\ud800"}"#)
            }]
        );
    }

    #[tokio::test]
    async fn direct_lone_surrogate_text_is_escaped_in_the_model_visible_runtime_response() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(runtime_source(r#"text("\uD800");"#.to_string()), &ctx, None)
            .await
            .unwrap();

        let expected_wire = r#"{"type":"result","cellId":"1","contentItems":[{"type":"input_text","text":"\ud800"}]}"#;
        let OutputBlock::Text(TextBlock { text, .. }) = &result.serializable.model_content[0]
        else {
            panic!("expected text response");
        };
        assert_eq!(text, expected_wire);
        assert_eq!(result.serializable.output, exact_json(expected_wire));
    }

    #[tokio::test]
    async fn actual_lone_surrogate_in_exec_source_reaches_v8_as_one_javascript_code_unit() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");
        let source = runtime_string(r#""text('\ud800');""#);

        let result = tool_set
            .exec
            .call(runtime_source(source), &ctx, None)
            .await
            .unwrap();

        assert_eq!(
            text_response(&result),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: runtime_string(r#""\ud800""#),
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn store_keys_keep_lone_surrogate_distinct_from_replacement_character() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"store("\uD800", "surrogate"); store("\uFFFD", "replacement"); text(JSON.stringify([load("\uD800"), load("\uFFFD")]));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(r#"["surrogate","replacement"]"#),
            }]
        );
    }

    #[tokio::test]
    async fn uncaught_v8_error_preserves_lone_surrogate_message() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(r#"throw new Error("\uD800");"#.to_string()),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { error_text, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(error_text, Some(runtime_string(r#""Error: \ud800""#)));
    }

    #[tokio::test]
    async fn generated_image_output_hint_preserves_lone_surrogate() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"generatedImage({ image_url: "data:image/png;base64,AA==", output_hint: "\uD800" });"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(
            content_items,
            vec![
                OutputContentItem::InputImage {
                    image_url: RuntimeString::from("data:image/png;base64,AA=="),
                    detail: Some(crate::engine::code_mode::protocol::ImageDetail::Auto),
                },
                OutputContentItem::InputText {
                    text: runtime_string(r#""\ud800""#),
                },
            ]
        );
    }

    #[tokio::test]
    async fn nested_fatal_tool_error_rejects_with_tool_error_identity() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let mut ctx = ctx_with_catalog(&extra);
        ctx.invoke_tool = Some(Arc::new(
            move |_name: RuntimeString, _input: RuntimeJson| {
                Box::pin(async move { Err(ToolError::fatal("nested fatal")) })
            },
        ));

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"try { await tools.echo({}); } catch (e) { text(JSON.stringify({name:e.name,kind:e.kind,message:e.message,string:String(e)})); }"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(
                    r#"{"name":"ToolError","kind":"fatal","message":"nested fatal","string":"ToolError: nested fatal"}"#,
                )
            }]
        );
    }

    #[tokio::test]
    async fn nested_tool_rejection_preserves_lone_surrogate_message_in_v8() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let mut ctx = ctx_with_catalog(&extra);
        let expected = runtime_string(r#""nested \ud800""#);
        let rejection_message = expected.clone();
        ctx.invoke_tool = Some(Arc::new(
            move |_name: RuntimeString, _input: RuntimeJson| {
                let rejection_message = rejection_message.clone();
                Box::pin(async move {
                    Ok(InvokeToolResult {
                        is_error: true,
                        output: None,
                        content: vec![OutputBlock::Text(TextBlock {
                            text: rejection_message,
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                    })
                })
            },
        ));

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"try { await tools.echo({}); } catch (error) { text(error.message); }"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText { text: expected }]
        );
    }

    #[tokio::test]
    async fn nested_is_error_result_rejects_with_plain_error_identity() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let mut ctx = ctx_with_catalog(&extra);
        ctx.invoke_tool = Some(Arc::new(
            move |_name: RuntimeString, _input: RuntimeJson| {
                Box::pin(async move {
                    Ok(InvokeToolResult {
                        is_error: true,
                        output: None,
                        content: vec![OutputBlock::Text(TextBlock {
                            text: RuntimeString::from("normal failure"),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                    })
                })
            },
        ));

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"try { await tools.echo({}); } catch (e) { text(JSON.stringify({name:e.name,kind:e.kind,message:e.message,string:String(e)})); }"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text,
            ..
        } = text_response(&result)
        else {
            panic!("expected result response");
        };
        assert_eq!(error_text, None);
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(
                    r#"{"name":"Error","message":"normal failure","string":"Error: normal failure"}"#,
                )
            }]
        );
    }

    #[tokio::test]
    async fn live_cells_keep_their_original_nested_tool_context() {
        let extra = vec![Arc::new(MockTool {
            name: "whoami".to_string(),
            description: "whoami".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());

        let mut ctx_a = ctx_with_catalog(&extra);
        let seen_a = Arc::new(StdMutex::new(Vec::<(RuntimeString, RuntimeJson)>::new()));
        let seen_a_clone = seen_a.clone();
        ctx_a.invoke_tool = Some(Arc::new(move |name: RuntimeString, input: RuntimeJson| {
            seen_a_clone
                .lock()
                .unwrap()
                .push((name.clone(), input.clone()));
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(TextBlock {
                        text: nested_tool_result_text(None, Some("A"), &name, &input),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            })
        }));

        let mut ctx_b = ctx_with_catalog(&extra);
        let seen_b = Arc::new(StdMutex::new(Vec::<(RuntimeString, RuntimeJson)>::new()));
        let seen_b_clone = seen_b.clone();
        ctx_b.invoke_tool = Some(Arc::new(move |name: RuntimeString, input: RuntimeJson| {
            seen_b_clone
                .lock()
                .unwrap()
                .push((name.clone(), input.clone()));
            Box::pin(async move {
                Ok(InvokeToolResult {
                    is_error: false,
                    output: None,
                    content: vec![OutputBlock::Text(TextBlock {
                        text: nested_tool_result_text(None, Some("B"), &name, &input),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                })
            })
        }));

        let initial_a = tool_set
            .exec
            .call(
                runtime_source(
                    r#"yield_control();
await new Promise((resolve) => setTimeout(resolve, 20));
text(JSON.stringify(await tools.whoami({"from":"a"})));"#
                        .to_string(),
                ),
                &ctx_a,
                None,
            )
            .await
            .unwrap();
        let RuntimeResponse::Yielded { cell_id, .. } = text_response(&initial_a) else {
            panic!("expected yielded response");
        };

        let _ = tool_set
            .exec
            .call(runtime_source(r#"text("b");"#), &ctx_b, None)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(40)).await;

        let waited = tool_set
            .wait
            .call(wait_input(&cell_id, None, None), &ctx_a, None)
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&waited) else {
            panic!("expected final result");
        };
        assert_eq!(
            content_items,
            vec![OutputContentItem::InputText {
                text: RuntimeString::from(r#"{"ctx":"A","name":"whoami","input":{"from":"a"}}"#)
            }]
        );
        assert_eq!(
            seen_a.lock().unwrap().as_slice(),
            &[(RuntimeString::from("whoami"), exact_json(r#"{"from":"a"}"#))]
        );
        assert!(seen_b.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn clear_timeout_prevents_callback_output() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"const timeoutId = setTimeout(() => text("boom"), 20);
clearTimeout(timeoutId);
await new Promise((resolve) => setTimeout(resolve, 40));
text("ok");"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            text_response(&result),
            RuntimeResponse::Result {
                cell_id: crate::engine::code_mode::protocol::CellId::new(RuntimeString::from("1")),
                content_items: vec![OutputContentItem::InputText {
                    text: RuntimeString::from("ok")
                }],
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn all_tools_and_tools_exclude_exec_and_wait() {
        let extra = vec![Arc::new(MockTool {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_kind: ToolInputKind::JsonSchema,
            input_schema: JsonSchema::new(),
        }) as Arc<dyn Tool>];
        let tool_set = setup_tools(extra.clone());
        let ctx = ctx_with_catalog(&extra);

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"text(JSON.stringify({ all: ALL_TOOLS.map((t) => t.name), hasExec: Object.hasOwn(tools, "exec"), hasWait: Object.hasOwn(tools, "wait"), hasEcho: Object.hasOwn(tools, "echo") }));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        let [OutputContentItem::InputText { text }] = content_items.as_slice() else {
            panic!("expected one text item");
        };
        assert_eq!(
            crate::runtime_values::runtime_json::parse(text).unwrap(),
            exact_json(r#"{"all":["echo"],"hasExec":false,"hasWait":false,"hasEcho":true}"#,)
        );
    }

    #[tokio::test]
    async fn ambient_capabilities_are_absent() {
        let tool_set = setup_tools(Vec::new());
        let ctx = make_ctx(".");

        let result = tool_set
            .exec
            .call(
                runtime_source(
                    r#"text(JSON.stringify({ require: typeof require, process: typeof process, console: Object.hasOwn(globalThis, "console") }));"#
                        .to_string(),
                ),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let RuntimeResponse::Result { content_items, .. } = text_response(&result) else {
            panic!("expected result response");
        };
        let [OutputContentItem::InputText { text }] = content_items.as_slice() else {
            panic!("expected one text item");
        };
        assert_eq!(
            crate::runtime_values::runtime_json::parse(text).unwrap(),
            exact_json(r#"{"require":"undefined","process":"undefined","console":false}"#)
        );
    }

    #[test]
    fn exec_metadata_exposes_freeform_javascript_and_closed_source_fallback_schema() {
        let tool_set = setup_tools(Vec::new());
        assert_eq!(tool_set.exec.name(), "exec");
        assert!(matches!(
            tool_set.exec.input_kind(),
            ToolInputKind::Freeform { ref format } if format == "javascript"
        ));
        assert_eq!(
            tool_set.exec.input_schema(),
            exact_json(
                r#"{"type":"object","properties":{"source":{"type":"string","description":"Raw javascript source for the freeform tool."}},"required":["source"],"additionalProperties":false}"#,
            )
        );
        assert_eq!(tool_set.wait.name(), "wait");
        assert!(tool_set.wait.input_schema().get("properties").is_some());
    }
}
