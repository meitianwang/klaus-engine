//! Version-pinned payload shapes of the host connection.
//!
//! Each `Wire*` type owns the V1 representation of one runtime value. The
//! conversions to and from the internal [`crate::engine::code_mode::protocol`]
//! types are total in both directions EXCEPT where V1 is narrower than the
//! runtime (see [`WireExecuteRequest`]), which converts fallibly rather than
//! silently clamping.

use std::num::TryFromIntError;

use crate::engine::code_mode::protocol::{
    CellId, CodeModeNestedToolCall, CodeModeToolKind, ExecuteRequest, ImageDetail,
    OutputContentItem, RuntimeResponse, ToolDefinition, WaitOutcome, WaitRequest,
};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::json::Json;

/// A cell identifier as it travels on the connection.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct WireCellId(RuntimeString);

impl WireCellId {
    pub fn new(value: impl Into<RuntimeString>) -> Self {
        Self(value.into())
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }
}

impl From<CellId> for WireCellId {
    fn from(value: CellId) -> Self {
        Self(value.into_runtime_string())
    }
}

impl From<&CellId> for WireCellId {
    fn from(value: &CellId) -> Self {
        Self(value.as_runtime_string().clone())
    }
}

impl From<WireCellId> for CellId {
    fn from(value: WireCellId) -> Self {
        Self::new(value.0)
    }
}

impl RuntimeEncode for WireCellId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for WireCellId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeString::decode_runtime(value).map(Self)
    }
}

/// The tool invocation shape V1 understands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireToolKind {
    Function,
    Freeform,
}

impl From<CodeModeToolKind> for WireToolKind {
    fn from(value: CodeModeToolKind) -> Self {
        match value {
            CodeModeToolKind::Function => Self::Function,
            CodeModeToolKind::Freeform => Self::Freeform,
        }
    }
}

impl From<WireToolKind> for CodeModeToolKind {
    fn from(value: WireToolKind) -> Self {
        match value {
            WireToolKind::Function => Self::Function,
            WireToolKind::Freeform => Self::Freeform,
        }
    }
}

impl RuntimeEncode for WireToolKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Function => "function",
                Self::Freeform => "freeform",
            },
        )
    }
}

impl RuntimeDecode for WireToolKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(
            value,
            &[
                runtime_codec::RuntimeEnumVariant::new("function", &[]),
                runtime_codec::RuntimeEnumVariant::new("freeform", &[]),
            ],
            |variant| match variant {
                runtime_codec::RuntimeExternalVariant::Unit { variant: 0 } => Ok(Self::Function),
                runtime_codec::RuntimeExternalVariant::Unit { .. } => Ok(Self::Freeform),
                runtime_codec::RuntimeExternalVariant::Payload { .. } => Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::InvalidEnumShape),
                ),
            },
        )
    }
}

/// One tool exposed to a cell, as it travels on the connection.
#[derive(Clone, Debug, PartialEq)]
pub struct WireToolDefinition {
    pub name: RuntimeString,
    pub tool_name: RuntimeString,
    pub description: RuntimeString,
    pub kind: WireToolKind,
    pub input_schema: Option<Json>,
    pub output_schema: Option<Json>,
}

impl From<ToolDefinition> for WireToolDefinition {
    fn from(value: ToolDefinition) -> Self {
        Self {
            name: value.name,
            tool_name: value.tool_name,
            description: value.description,
            kind: value.kind.into(),
            input_schema: value.input_schema,
            output_schema: value.output_schema,
        }
    }
}

impl From<WireToolDefinition> for ToolDefinition {
    fn from(value: WireToolDefinition) -> Self {
        Self {
            name: value.name,
            tool_name: value.tool_name,
            description: value.description,
            kind: value.kind.into(),
            input_schema: value.input_schema,
            output_schema: value.output_schema,
        }
    }
}

impl RuntimeEncode for WireToolDefinition {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("toolName", &self.tool_name)?;
        object.field("description", &self.description)?;
        object.field("kind", &self.kind)?;
        if let Some(value) = &self.input_schema {
            object.field("inputSchema", value)?;
        }
        if let Some(value) = &self.output_schema {
            object.field("outputSchema", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireToolDefinition {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                tool_name: object.required("toolName", &[])?,
                description: object.required("description", &[])?,
                kind: object.required("kind", &[])?,
                input_schema: object.optional("inputSchema", &[])?,
                output_schema: object.optional("outputSchema", &[])?,
            })
        })
    }
}

/// The execute request shape V1 understands.
///
/// `max_output_tokens` is a 32-bit count on the wire, so a runtime request that
/// asks for more than `i32::MAX` cannot be represented and fails conversion
/// instead of being silently truncated.
#[derive(Clone, Debug, PartialEq)]
pub struct WireExecuteRequest {
    pub tool_call_id: RuntimeString,
    pub enabled_tools: Vec<WireToolDefinition>,
    pub source: RuntimeString,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<i32>,
}

impl TryFrom<ExecuteRequest> for WireExecuteRequest {
    type Error = TryFromIntError;

    fn try_from(value: ExecuteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            tool_call_id: value.tool_call_id,
            enabled_tools: value.enabled_tools.into_iter().map(Into::into).collect(),
            source: value.source,
            yield_time_ms: value.yield_time_ms,
            max_output_tokens: value.max_output_tokens.map(i32::try_from).transpose()?,
        })
    }
}

impl TryFrom<WireExecuteRequest> for ExecuteRequest {
    type Error = TryFromIntError;

    fn try_from(value: WireExecuteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            tool_call_id: value.tool_call_id,
            enabled_tools: value.enabled_tools.into_iter().map(Into::into).collect(),
            source: value.source,
            yield_time_ms: value.yield_time_ms,
            max_output_tokens: value.max_output_tokens.map(usize::try_from).transpose()?,
        })
    }
}

impl RuntimeEncode for WireExecuteRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("toolCallId", &self.tool_call_id)?;
        object.field("enabledTools", &self.enabled_tools)?;
        object.field("source", &self.source)?;
        if let Some(value) = &self.yield_time_ms {
            object.field("yieldTimeMs", value)?;
        }
        if let Some(value) = &self.max_output_tokens {
            object.field("maxOutputTokens", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireExecuteRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                tool_call_id: object.required("toolCallId", &[])?,
                enabled_tools: object.required("enabledTools", &[])?,
                source: object.required("source", &[])?,
                yield_time_ms: object.optional("yieldTimeMs", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
            })
        })
    }
}

/// The wait request shape V1 understands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireWaitRequest {
    pub cell_id: WireCellId,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<i32>,
    pub terminate: Option<bool>,
}

impl TryFrom<WaitRequest> for WireWaitRequest {
    type Error = TryFromIntError;

    fn try_from(value: WaitRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            cell_id: value.cell_id.into(),
            yield_time_ms: value.yield_time_ms,
            max_output_tokens: value.max_output_tokens.map(i32::try_from).transpose()?,
            terminate: value.terminate,
        })
    }
}

impl TryFrom<WireWaitRequest> for WaitRequest {
    type Error = TryFromIntError;

    fn try_from(value: WireWaitRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            cell_id: value.cell_id.into(),
            yield_time_ms: value.yield_time_ms,
            max_output_tokens: value.max_output_tokens.map(usize::try_from).transpose()?,
            terminate: value.terminate,
        })
    }
}

impl RuntimeEncode for WireWaitRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("cellId", &self.cell_id)?;
        if let Some(value) = &self.yield_time_ms {
            object.field("yieldTimeMs", value)?;
        }
        if let Some(value) = &self.max_output_tokens {
            object.field("maxOutputTokens", value)?;
        }
        if let Some(value) = &self.terminate {
            object.field("terminate", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireWaitRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                cell_id: object.required("cellId", &[])?,
                yield_time_ms: object.optional("yieldTimeMs", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
                terminate: object.optional("terminate", &[])?,
            })
        })
    }
}

/// Image detail values V1 accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireImageDetail {
    Auto,
    Low,
    High,
    Original,
}

impl From<ImageDetail> for WireImageDetail {
    fn from(value: ImageDetail) -> Self {
        match value {
            ImageDetail::Auto => Self::Auto,
            ImageDetail::Low => Self::Low,
            ImageDetail::High => Self::High,
            ImageDetail::Original => Self::Original,
        }
    }
}

impl From<WireImageDetail> for ImageDetail {
    fn from(value: WireImageDetail) -> Self {
        match value {
            WireImageDetail::Auto => Self::Auto,
            WireImageDetail::Low => Self::Low,
            WireImageDetail::High => Self::High,
            WireImageDetail::Original => Self::Original,
        }
    }
}

impl RuntimeEncode for WireImageDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Auto => "auto",
                Self::Low => "low",
                Self::High => "high",
                Self::Original => "original",
            },
        )
    }
}

impl RuntimeDecode for WireImageDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(
            value,
            &[
                runtime_codec::RuntimeEnumVariant::new("auto", &[]),
                runtime_codec::RuntimeEnumVariant::new("low", &[]),
                runtime_codec::RuntimeEnumVariant::new("high", &[]),
                runtime_codec::RuntimeEnumVariant::new("original", &[]),
            ],
            |variant| match variant {
                runtime_codec::RuntimeExternalVariant::Unit { variant: 0 } => Ok(Self::Auto),
                runtime_codec::RuntimeExternalVariant::Unit { variant: 1 } => Ok(Self::Low),
                runtime_codec::RuntimeExternalVariant::Unit { variant: 2 } => Ok(Self::High),
                runtime_codec::RuntimeExternalVariant::Unit { .. } => Ok(Self::Original),
                runtime_codec::RuntimeExternalVariant::Payload { .. } => Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::InvalidEnumShape),
                ),
            },
        )
    }
}

/// One output item of a runtime response, as it travels on the connection.
#[derive(Clone, Debug, PartialEq)]
pub enum WireContentItem {
    InputText {
        text: RuntimeString,
    },
    InputImage {
        image_url: RuntimeString,
        detail: Option<WireImageDetail>,
    },
}

impl From<OutputContentItem> for WireContentItem {
    fn from(value: OutputContentItem) -> Self {
        match value {
            OutputContentItem::InputText { text } => Self::InputText { text },
            OutputContentItem::InputImage { image_url, detail } => Self::InputImage {
                image_url,
                detail: detail.map(Into::into),
            },
        }
    }
}

impl From<WireContentItem> for OutputContentItem {
    fn from(value: WireContentItem) -> Self {
        match value {
            WireContentItem::InputText { text } => Self::InputText { text },
            WireContentItem::InputImage { image_url, detail } => Self::InputImage {
                image_url,
                detail: detail.map(Into::into),
            },
        }
    }
}

impl RuntimeEncode for WireContentItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::InputText { text } => {
                object.field("text", text)?;
                runtime_codec::encode_internal(out, "type", &[], "input_text", &object.finish())
            }
            Self::InputImage { image_url, detail } => {
                object.field("imageUrl", image_url)?;
                if let Some(detail) = detail {
                    object.field("detail", detail)?;
                }
                runtime_codec::encode_internal(out, "type", &[], "input_image", &object.finish())
            }
        }
    }
}

impl RuntimeDecode for WireContentItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("input_text", &[]),
                runtime_codec::RuntimeEnumVariant::new("input_image", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::InputText {
                    text: object.required("text", &[])?,
                }),
                _ => Ok(Self::InputImage {
                    image_url: object.required("imageUrl", &[])?,
                    detail: object.optional("detail", &[])?,
                }),
            },
        )
    }
}

/// Runtime output returned over the connection.
#[derive(Clone, Debug, PartialEq)]
pub enum WireRuntimeResponse {
    Yielded {
        cell_id: WireCellId,
        content_items: Vec<WireContentItem>,
    },
    Terminated {
        cell_id: WireCellId,
        content_items: Vec<WireContentItem>,
    },
    Result {
        cell_id: WireCellId,
        content_items: Vec<WireContentItem>,
        error_text: Option<RuntimeString>,
    },
}

impl From<RuntimeResponse> for WireRuntimeResponse {
    fn from(value: RuntimeResponse) -> Self {
        match value {
            RuntimeResponse::Yielded {
                cell_id,
                content_items,
            } => Self::Yielded {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
            },
            RuntimeResponse::Terminated {
                cell_id,
                content_items,
            } => Self::Terminated {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
            },
            RuntimeResponse::Result {
                cell_id,
                content_items,
                error_text,
            } => Self::Result {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
                error_text,
            },
        }
    }
}

impl From<WireRuntimeResponse> for RuntimeResponse {
    fn from(value: WireRuntimeResponse) -> Self {
        match value {
            WireRuntimeResponse::Yielded {
                cell_id,
                content_items,
            } => Self::Yielded {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
            },
            WireRuntimeResponse::Terminated {
                cell_id,
                content_items,
            } => Self::Terminated {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
            },
            WireRuntimeResponse::Result {
                cell_id,
                content_items,
                error_text,
            } => Self::Result {
                cell_id: cell_id.into(),
                content_items: content_items.into_iter().map(Into::into).collect(),
                error_text,
            },
        }
    }
}

impl RuntimeEncode for WireRuntimeResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::Yielded {
                cell_id,
                content_items,
            } => {
                object.field("cellId", cell_id)?;
                object.field("contentItems", content_items)?;
                runtime_codec::encode_internal(out, "type", &[], "yielded", &object.finish())
            }
            Self::Terminated {
                cell_id,
                content_items,
            } => {
                object.field("cellId", cell_id)?;
                object.field("contentItems", content_items)?;
                runtime_codec::encode_internal(out, "type", &[], "terminated", &object.finish())
            }
            Self::Result {
                cell_id,
                content_items,
                error_text,
            } => {
                object.field("cellId", cell_id)?;
                object.field("contentItems", content_items)?;
                if let Some(error_text) = error_text {
                    object.field("errorText", error_text)?;
                }
                runtime_codec::encode_internal(out, "type", &[], "result", &object.finish())
            }
        }
    }
}

impl RuntimeDecode for WireRuntimeResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("yielded", &[]),
                runtime_codec::RuntimeEnumVariant::new("terminated", &[]),
                runtime_codec::RuntimeEnumVariant::new("result", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Yielded {
                    cell_id: object.required("cellId", &[])?,
                    content_items: object.required("contentItems", &[])?,
                }),
                1 => Ok(Self::Terminated {
                    cell_id: object.required("cellId", &[])?,
                    content_items: object.required("contentItems", &[])?,
                }),
                _ => Ok(Self::Result {
                    cell_id: object.required("cellId", &[])?,
                    content_items: object.required("contentItems", &[])?,
                    error_text: object.optional("errorText", &[])?,
                }),
            },
        )
    }
}

/// Whether the cell a client waited for was still known to the host.
#[derive(Clone, Debug, PartialEq)]
pub enum WireWaitOutcome {
    LiveCell(WireRuntimeResponse),
    MissingCell(WireRuntimeResponse),
}

impl From<WaitOutcome> for WireWaitOutcome {
    fn from(value: WaitOutcome) -> Self {
        match value {
            WaitOutcome::LiveCell(response) => Self::LiveCell(response.into()),
            WaitOutcome::MissingCell(response) => Self::MissingCell(response.into()),
        }
    }
}

impl From<WireWaitOutcome> for WaitOutcome {
    fn from(value: WireWaitOutcome) -> Self {
        match value {
            WireWaitOutcome::LiveCell(response) => Self::LiveCell(response.into()),
            WireWaitOutcome::MissingCell(response) => Self::MissingCell(response.into()),
        }
    }
}

impl RuntimeEncode for WireWaitOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::LiveCell(response) => {
                runtime_codec::encode_external_payload(out, "liveCell", response)
            }
            Self::MissingCell(response) => {
                runtime_codec::encode_external_payload(out, "missingCell", response)
            }
        }
    }
}

impl RuntimeDecode for WireWaitOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(
            value,
            &[
                runtime_codec::RuntimeEnumVariant::new("liveCell", &[]),
                runtime_codec::RuntimeEnumVariant::new("missingCell", &[]),
            ],
            |variant| match variant {
                runtime_codec::RuntimeExternalVariant::Payload { variant, value } => {
                    let response = WireRuntimeResponse::decode_runtime(value)?;
                    Ok(match variant {
                        0 => Self::LiveCell(response),
                        _ => Self::MissingCell(response),
                    })
                }
                runtime_codec::RuntimeExternalVariant::Unit { .. } => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::InvalidEnumShape,
                )),
            },
        )
    }
}

/// A nested tool invocation sent from the host to the engine.
#[derive(Clone, Debug, PartialEq)]
pub struct WireNestedToolCall {
    pub cell_id: WireCellId,
    pub runtime_tool_call_id: RuntimeString,
    pub tool_name: RuntimeString,
    pub tool_kind: WireToolKind,
    pub input: Option<Json>,
}

impl From<CodeModeNestedToolCall> for WireNestedToolCall {
    fn from(value: CodeModeNestedToolCall) -> Self {
        Self {
            cell_id: value.cell_id.into(),
            runtime_tool_call_id: value.runtime_tool_call_id,
            tool_name: value.tool_name,
            tool_kind: value.tool_kind.into(),
            input: value.input,
        }
    }
}

impl From<WireNestedToolCall> for CodeModeNestedToolCall {
    fn from(value: WireNestedToolCall) -> Self {
        Self {
            cell_id: value.cell_id.into(),
            runtime_tool_call_id: value.runtime_tool_call_id,
            tool_name: value.tool_name,
            tool_kind: value.tool_kind.into(),
            input: value.input,
        }
    }
}

impl RuntimeEncode for WireNestedToolCall {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("cellId", &self.cell_id)?;
        object.field("runtimeToolCallId", &self.runtime_tool_call_id)?;
        object.field("toolName", &self.tool_name)?;
        object.field("toolKind", &self.tool_kind)?;
        if let Some(input) = &self.input {
            object.field("input", input)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireNestedToolCall {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                cell_id: object.required("cellId", &[])?,
                runtime_tool_call_id: object.required("runtimeToolCallId", &[])?,
                tool_name: object.required("toolName", &[])?,
                tool_kind: object.required("toolKind", &[])?,
                input: object.optional("input", &[])?,
            })
        })
    }
}

/// The REPL evaluation request shape V1 understands.
///
/// `exec_id` is assigned by the client and echoed by every frame of this
/// evaluation (delegate calls, termination, completion), so one connection can
/// tell concurrent evaluations of different sessions apart and a termination
/// can never hit an evaluation other than the one it names.
#[derive(Clone, Debug, PartialEq)]
pub struct WireReplEvalRequest {
    pub exec_id: RuntimeString,
    pub code: RuntimeString,
    pub timeout_ms: u64,
    pub tools: Vec<WireToolDefinition>,
}

impl RuntimeEncode for WireReplEvalRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("execId", &self.exec_id)?;
        object.field("code", &self.code)?;
        object.field("timeoutMs", &self.timeout_ms)?;
        object.field("tools", &self.tools)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireReplEvalRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                exec_id: object.required("execId", &[])?,
                code: object.required("code", &[])?,
                timeout_ms: object.required("timeoutMs", &[])?,
                tools: object.required("tools", &[])?,
            })
        })
    }
}

/// The outcome of one REPL evaluation: the captured console sections, the
/// formatted last-expression text, and the structured result value.
#[derive(Clone, Debug, PartialEq)]
pub struct WireReplOutcome {
    pub result: Json,
    pub result_defined: bool,
    pub result_text: RuntimeString,
    pub stdout: RuntimeString,
    pub stderr: RuntimeString,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for WireReplOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("result", &self.result)?;
        object.field("resultDefined", &self.result_defined)?;
        object.field("resultText", &self.result_text)?;
        object.field("stdout", &self.stdout)?;
        object.field("stderr", &self.stderr)?;
        if let Some(error) = &self.error {
            object.field("error", error)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WireReplOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                result: object.required("result", &[])?,
                result_defined: object.required("resultDefined", &[])?,
                result_text: object.required("resultText", &[])?,
                stdout: object.required("stdout", &[])?,
                stderr: object.required("stderr", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}
