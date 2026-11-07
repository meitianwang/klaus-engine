use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use std::collections::BTreeSet;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

pub const EXEC_TOOL_NAME: &str = "exec";
pub const WAIT_TOOL_NAME: &str = "wait";
pub const CODE_MODE_PRAGMA_PREFIX: &str = "// @exec:";
pub const CODE_MODE_CELL_YIELDED_EVENT: &str = "code_mode_cell_yielded";
pub const CODE_MODE_CELL_COMPLETED_EVENT: &str = "code_mode_cell_completed";
pub const CODE_MODE_CELL_TERMINATED_EVENT: &str = "code_mode_cell_terminated";

const MAX_JS_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedExecSource {
    pub source: RuntimeString,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

impl RuntimeEncode for ParsedExecSource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
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

impl RuntimeDecode for ParsedExecSource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                source: object.required("source", &[])?,
                yield_time_ms: object.optional("yieldTimeMs", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
            })
        })
    }
}

pub fn parse_exec_source(input: &RuntimeString) -> Result<ParsedExecSource, RuntimeString> {
    if trim_runtime(input).is_empty() {
        return Err(RuntimeString::from(
            "exec expects raw JavaScript source text (non-empty). Provide JS only, optionally with first-line `// @exec: {\"yield_time_ms\": 10000, \"max_output_tokens\": 1000}`.",
        ));
    }

    let newline_index = input.as_utf16().iter().position(|unit| *unit == 0x000a);
    let first_line = input.slice_utf16(..newline_index.unwrap_or(input.len_utf16()));
    let rest = newline_index
        .map(|index| input.slice_utf16(index + 1..))
        .unwrap_or_default();
    let trimmed = trim_start_runtime(&first_line);
    if !trimmed.starts_with(CODE_MODE_PRAGMA_PREFIX) {
        return Ok(ParsedExecSource {
            source: input.clone(),
            yield_time_ms: None,
            max_output_tokens: None,
        });
    }

    if trim_runtime(&rest).is_empty() {
        return Err(RuntimeString::from(
            "exec pragma must be followed by JavaScript source on subsequent lines",
        ));
    }

    let directive =
        trim_runtime(&trimmed.slice_utf16(CODE_MODE_PRAGMA_PREFIX.encode_utf16().count()..));
    if directive.is_empty() {
        return Err(RuntimeString::from(
            "exec pragma must be a JSON object with supported fields `yield_time_ms` and `max_output_tokens`",
        ));
    }
    let entries = scan_top_level_pragma_entries(&directive);
    reject_duplicate_pragma_keys(entries.as_deref())?;

    let value = crate::runtime_values::runtime_json::parse(&directive).map_err(|error| {
        RuntimeString::from(
            "exec pragma must be valid JSON with supported fields `yield_time_ms` and `max_output_tokens`: ",
        )
        .concat(&RuntimeString::from(error.to_string()))
    })?;
    let object = value.as_object().ok_or_else(|| {
        RuntimeString::from(
            "exec pragma must be a JSON object with supported fields `yield_time_ms` and `max_output_tokens`",
        )
    })?;

    for key in object.keys() {
        if key != "yield_time_ms" && key != "max_output_tokens" {
            return Err(RuntimeString::from(
                "exec pragma only supports `yield_time_ms` and `max_output_tokens`; got `",
            )
            .concat(&key)
            .concat(&RuntimeString::from("`")));
        }
    }

    assert_safe_integer_field_syntax(entries.as_deref(), object, "yield_time_ms")?;
    assert_safe_integer_field_syntax(entries.as_deref(), object, "max_output_tokens")?;
    let yield_time_ms = parse_safe_integer_field(object.get("yield_time_ms"), "yield_time_ms")?;
    let max_output_tokens =
        parse_safe_integer_field(object.get("max_output_tokens"), "max_output_tokens")?
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    RuntimeString::from(
                        "exec pragma field `max_output_tokens` must be a non-negative safe integer",
                    )
                })
            })
            .transpose()?;

    Ok(ParsedExecSource {
        source: rest,
        yield_time_ms,
        max_output_tokens,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PragmaEntry {
    key: RuntimeString,
    raw_value: RuntimeString,
}

fn scan_top_level_pragma_entries(directive: &RuntimeString) -> Option<Vec<PragmaEntry>> {
    let units = directive.as_utf16();
    let mut index = skip_whitespace(units, 0);
    if units.get(index).copied() != Some(u16::from(b'{')) {
        return None;
    }
    index += 1;
    let mut entries = Vec::new();

    while index < units.len() {
        index = skip_whitespace(units, index);
        if units.get(index).copied() == Some(u16::from(b'}')) {
            return Some(entries);
        }
        if units.get(index).copied() != Some(u16::from(b'"')) {
            return None;
        }

        let key_end = find_json_string_end(units, index)?;
        let key =
            crate::runtime_values::runtime_json::parse(&directive.slice_utf16(index..key_end))
                .ok()?
                .into_string()
                .ok()?;

        index = skip_whitespace(units, key_end);
        if units.get(index).copied() != Some(u16::from(b':')) {
            return None;
        }
        let value_start = skip_whitespace(units, index + 1);
        let value_end = skip_json_value(units, value_start);
        entries.push(PragmaEntry {
            key,
            raw_value: trim_runtime(&directive.slice_utf16(value_start..value_end)),
        });
        index = skip_whitespace(units, value_end);
        if units.get(index).copied() == Some(u16::from(b',')) {
            index += 1;
            continue;
        }
        if units.get(index).copied() == Some(u16::from(b'}')) {
            return Some(entries);
        }
        return None;
    }
    None
}

fn reject_duplicate_pragma_keys(entries: Option<&[PragmaEntry]>) -> Result<(), RuntimeString> {
    let Some(entries) = entries else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for entry in entries {
        if !seen.insert(entry.key.clone()) {
            return Err(RuntimeString::from("duplicate exec pragma field `")
                .concat(&entry.key)
                .concat(&RuntimeString::from("`")));
        }
    }
    Ok(())
}

fn skip_whitespace(source: &[u16], index: usize) -> usize {
    let mut cursor = index;
    while source
        .get(cursor)
        .copied()
        .is_some_and(is_ecmascript_whitespace)
    {
        cursor += 1;
    }
    cursor
}

fn find_json_string_end(source: &[u16], start: usize) -> Option<usize> {
    let mut escaped = false;
    for (index, &character) in source.iter().enumerate().skip(start + 1) {
        if escaped {
            escaped = false;
        } else if character == u16::from(b'\\') {
            escaped = true;
        } else if character == u16::from(b'"') {
            return Some(index + 1);
        }
    }
    None
}

fn skip_json_value(source: &[u16], index: usize) -> usize {
    let mut cursor = index;
    let mut depth = 0_u64;
    let mut in_string = false;
    let mut escaped = false;
    while cursor < source.len() {
        let character = source[cursor];
        if in_string {
            if escaped {
                escaped = false;
            } else if character == u16::from(b'\\') {
                escaped = true;
            } else if character == u16::from(b'"') {
                in_string = false;
            }
            cursor += 1;
            continue;
        }
        if character == u16::from(b'"') {
            in_string = true;
        } else if character == u16::from(b'{') || character == u16::from(b'[') {
            depth += 1;
        } else if character == u16::from(b'}') || character == u16::from(b']') {
            if depth == 0 {
                return cursor;
            }
            depth -= 1;
        } else if character == u16::from(b',') && depth == 0 {
            return cursor;
        }
        cursor += 1;
    }
    cursor
}

fn assert_safe_integer_field_syntax(
    entries: Option<&[PragmaEntry]>,
    object: &RuntimeObject,
    field: &'static str,
) -> Result<(), RuntimeString> {
    if object.get(field).is_none() {
        return Ok(());
    }
    let token = entries
        .and_then(|entries| entries.iter().find(|entry| entry.key == field))
        .map(|entry| &entry.raw_value);
    if !token.is_some_and(is_canonical_nonnegative_integer_token) {
        return Err(safe_integer_error(field));
    }
    Ok(())
}

fn is_canonical_nonnegative_integer_token(token: &RuntimeString) -> bool {
    match token.as_utf16() {
        [zero] if *zero == u16::from(b'0') => true,
        [first, rest @ ..]
            if (u16::from(b'1')..=u16::from(b'9')).contains(first)
                && rest
                    .iter()
                    .all(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit)) =>
        {
            true
        }
        _ => false,
    }
}

fn parse_safe_integer_field(
    value: Option<&RuntimeJson>,
    field: &'static str,
) -> Result<Option<u64>, RuntimeString> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(number) = value.as_f64() else {
        return Err(safe_integer_error(field));
    };
    if !number.is_finite()
        || number.fract() != 0.0
        || number < 0.0
        || number > MAX_JS_SAFE_INTEGER as f64
    {
        return Err(safe_integer_error(field));
    }
    Ok(Some(number as u64))
}

fn safe_integer_error(field: &'static str) -> RuntimeString {
    RuntimeString::from("exec pragma field `")
        .concat(&RuntimeString::from(field))
        .concat(&RuntimeString::from(
            "` must be a non-negative safe integer",
        ))
}

fn trim_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(start, |index| index + 1);
    value.slice_utf16(start..end)
}

fn trim_start_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let start = units
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(units.len());
    value.slice_utf16(start..)
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodeModeToolKind {
    Function,
    Freeform,
}

impl RuntimeEncode for CodeModeToolKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Function => "function",
            Self::Freeform => "freeform",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CodeModeToolKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("function", &[]),
                RuntimeEnumVariant::new("freeform", &[]),
            ],
            &[Self::Function, Self::Freeform],
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolDefinition {
    pub name: RuntimeString,
    pub tool_name: RuntimeString,
    pub description: RuntimeString,
    pub kind: CodeModeToolKind,
    pub input_schema: Option<RuntimeJson>,
    pub output_schema: Option<RuntimeJson>,
}

impl RuntimeEncode for ToolDefinition {
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

impl RuntimeDecode for ToolDefinition {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
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

#[derive(Clone, Debug, PartialEq)]
pub struct NestedToolMetadata {
    pub tool_name: RuntimeString,
    pub global_name: RuntimeString,
    pub description: RuntimeString,
    pub kind: CodeModeToolKind,
}

impl RuntimeEncode for NestedToolMetadata {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("toolName", &self.tool_name)?;
        object.field("globalName", &self.global_name)?;
        object.field("description", &self.description)?;
        object.field("kind", &self.kind)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for NestedToolMetadata {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tool_name: object.required("toolName", &[])?,
                global_name: object.required("globalName", &[])?,
                description: object.required("description", &[])?,
                kind: object.required("kind", &[])?,
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteRequest {
    pub tool_call_id: RuntimeString,
    pub enabled_tools: Vec<ToolDefinition>,
    pub source: RuntimeString,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

impl RuntimeEncode for ExecuteRequest {
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

impl RuntimeDecode for ExecuteRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CellId(RuntimeString);

impl CellId {
    pub fn new(value: RuntimeString) -> Self {
        Self(value)
    }

    pub fn as_runtime_string(&self) -> &RuntimeString {
        &self.0
    }

    pub fn into_runtime_string(self) -> RuntimeString {
        self.0
    }
}

impl RuntimeEncode for CellId {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for CellId {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeString::decode_runtime(value).map(Self)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WaitRequest {
    pub cell_id: CellId,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
    pub terminate: Option<bool>,
}

impl RuntimeEncode for WaitRequest {
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

impl RuntimeDecode for WaitRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                cell_id: object.required("cellId", &[])?,
                yield_time_ms: object.optional("yieldTimeMs", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
                terminate: object.optional("terminate", &[])?,
            })
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

impl RuntimeEncode for ImageDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Auto => "auto",
            Self::Low => "low",
            Self::High => "high",
            Self::Original => "original",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ImageDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("high", &[]),
                RuntimeEnumVariant::new("original", &[]),
            ],
            &[Self::Auto, Self::Low, Self::High, Self::Original],
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutputContentItem {
    InputText {
        text: RuntimeString,
    },
    InputImage {
        image_url: RuntimeString,
        detail: Option<ImageDetail>,
    },
}

impl RuntimeEncode for OutputContentItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::InputText { text } => {
                fields.field("text", text)?;
                "input_text"
            }
            Self::InputImage { image_url, detail } => {
                fields.field("image_url", image_url)?;
                if let Some(value) = detail {
                    fields.field("detail", value)?;
                }
                "input_image"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for OutputContentItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("input_text", &[]),
                RuntimeEnumVariant::new("input_image", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::InputText {
                    text: object.required("text", &[])?,
                }),
                1 => Ok(Self::InputImage {
                    image_url: object.required("image_url", &[])?,
                    detail: object.optional("detail", &[])?,
                }),
                _ => unreachable!("variant index is bounded by the declaration"),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeResponse {
    Yielded {
        cell_id: CellId,
        content_items: Vec<OutputContentItem>,
    },
    Terminated {
        cell_id: CellId,
        content_items: Vec<OutputContentItem>,
    },
    Result {
        cell_id: CellId,
        content_items: Vec<OutputContentItem>,
        error_text: Option<RuntimeString>,
    },
}

impl RuntimeEncode for RuntimeResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::Yielded {
                cell_id,
                content_items,
            } => {
                fields.field("cellId", cell_id)?;
                fields.field("contentItems", content_items)?;
                "yielded"
            }
            Self::Terminated {
                cell_id,
                content_items,
            } => {
                fields.field("cellId", cell_id)?;
                fields.field("contentItems", content_items)?;
                "terminated"
            }
            Self::Result {
                cell_id,
                content_items,
                error_text,
            } => {
                fields.field("cellId", cell_id)?;
                fields.field("contentItems", content_items)?;
                if let Some(value) = error_text {
                    fields.field("errorText", value)?;
                }
                "result"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for RuntimeResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("yielded", &[]),
                RuntimeEnumVariant::new("terminated", &[]),
                RuntimeEnumVariant::new("result", &[]),
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
                2 => Ok(Self::Result {
                    cell_id: object.required("cellId", &[])?,
                    content_items: object.required("contentItems", &[])?,
                    error_text: object.optional("errorText", &[])?,
                }),
                _ => unreachable!("variant index is bounded by the declaration"),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WaitOutcome {
    LiveCell(RuntimeResponse),
    MissingCell(RuntimeResponse),
}

/// A cell observed up to its QUIESCENT frontier rather than up to a yield or a
/// result: the runtime has run as far as it can without new input.
///
/// `Pending` reports the tool calls the program is blocked on, which is what
/// makes a paused cell inspectable — a yield-based observation cannot
/// distinguish "still computing" from "waiting on these calls".
#[derive(Clone, Debug, PartialEq)]
pub enum ExecuteToPendingOutcome {
    Pending {
        cell_id: CellId,
        content_items: Vec<OutputContentItem>,
        pending_tool_call_ids: Vec<RuntimeString>,
    },
    /// The cell reached a terminal frontier before pausing.
    Completed(RuntimeResponse),
}

/// The same observation for a cell that may no longer exist.
#[derive(Clone, Debug, PartialEq)]
pub enum WaitToPendingOutcome {
    LiveCell(ExecuteToPendingOutcome),
    MissingCell(RuntimeResponse),
}

/// Resume observing a live cell up to its quiescent frontier.
#[derive(Clone, Debug, PartialEq)]
pub struct WaitToPendingRequest {
    pub cell_id: CellId,
}

impl RuntimeEncode for WaitOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::LiveCell(response) => runtime_codec::encode_adjacent_payload(
                out,
                "type",
                &[],
                "response",
                &[],
                "live_cell",
                response,
            ),
            Self::MissingCell(response) => runtime_codec::encode_adjacent_payload(
                out,
                "type",
                &[],
                "response",
                &[],
                "missing_cell",
                response,
            ),
        }
    }
}

impl RuntimeDecode for WaitOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_adjacent(
            value,
            false,
            "type",
            &[],
            "response",
            &[],
            &[
                RuntimeEnumVariant::new("live_cell", &[]),
                RuntimeEnumVariant::new("missing_cell", &[]),
            ],
            &[],
            |variant, response| {
                let response = RuntimeResponse::decode_runtime(
                    response.expect("payload variants always require adjacent content"),
                )?;
                match variant {
                    0 => Ok(Self::LiveCell(response)),
                    1 => Ok(Self::MissingCell(response)),
                    _ => unreachable!("variant index is bounded by the declaration"),
                }
            },
        )
    }
}

impl From<WaitOutcome> for RuntimeResponse {
    fn from(outcome: WaitOutcome) -> Self {
        match outcome {
            WaitOutcome::LiveCell(response) | WaitOutcome::MissingCell(response) => response,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodeModeNestedToolCall {
    pub cell_id: CellId,
    pub runtime_tool_call_id: RuntimeString,
    pub tool_name: RuntimeString,
    pub tool_kind: CodeModeToolKind,
    pub input: Option<RuntimeJson>,
}

impl RuntimeEncode for CodeModeNestedToolCall {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("cellId", &self.cell_id)?;
        object.field("runtimeToolCallId", &self.runtime_tool_call_id)?;
        object.field("toolName", &self.tool_name)?;
        object.field("toolKind", &self.tool_kind)?;
        if let Some(value) = &self.input {
            object.field("input", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CodeModeNestedToolCall {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    const MAX_JS_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;

    fn parse_scalar(input: &str) -> Result<ParsedExecSource, RuntimeString> {
        parse_exec_source(&RuntimeString::from(input))
    }

    fn exact_json(input: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(input))
            .expect("test fixture must be exact JSON")
    }

    fn assert_error_contains(error: &RuntimeString, expected: &str) {
        assert!(error.contains(expected), "{error:?}");
    }

    fn assert_runtime_round_trip<T>(value: &T)
    where
        T: RuntimeDecode + RuntimeEncode + Debug + PartialEq,
    {
        let encoded = runtime_codec::encode_value(value).expect("runtime encoding succeeds");
        let decoded: T = runtime_codec::decode_value(&encoded).expect("runtime decoding succeeds");
        assert_eq!(&decoded, value);
    }

    #[test]
    fn code_mode_protocol_public_tool_names_are_exec_and_wait() {
        assert_eq!(EXEC_TOOL_NAME, "exec");
        assert_eq!(WAIT_TOOL_NAME, "wait");
    }

    #[test]
    fn parse_exec_source_rejects_empty_source() {
        let error = parse_scalar(" \n\t ").unwrap_err();
        assert_error_contains(&error, "non-empty");

        let exact_whitespace = RuntimeString::from_utf16(vec![0xfeff, 0x00a0, 0x2028, 0x3000]);
        let error = parse_exec_source(&exact_whitespace).unwrap_err();
        assert_error_contains(&error, "non-empty");
    }

    #[test]
    fn parse_exec_source_accepts_valid_pragma() {
        let parsed = parse_scalar(
            "// @exec: {\"yield_time_ms\": 250, \"max_output_tokens\": 42}\ntext('ok')",
        )
        .unwrap();

        assert_eq!(parsed.source, "text('ok')");
        assert_eq!(parsed.yield_time_ms, Some(250));
        assert_eq!(parsed.max_output_tokens, Some(42));
    }

    #[test]
    fn parse_exec_source_accepts_escaped_supported_pragma_keys() {
        let parsed = parse_scalar(
            "// @exec: {\"\\u0079ield_time_ms\": 250, \"max_output_\\u0074okens\": 42}\ntext('ok')",
        )
        .unwrap();

        assert_eq!(parsed.source, "text('ok')");
        assert_eq!(parsed.yield_time_ms, Some(250));
        assert_eq!(parsed.max_output_tokens, Some(42));
    }

    #[test]
    fn parse_exec_source_rejects_malformed_pragma_json() {
        let error = parse_scalar("// @exec: { nope }\ntext('ok')").unwrap_err();
        assert_error_contains(&error, "valid JSON");
    }

    #[test]
    fn parse_exec_source_rejects_non_object_pragma() {
        let error = parse_scalar("// @exec: []\ntext('ok')").unwrap_err();
        assert_error_contains(&error, "JSON object");
    }

    #[test]
    fn parse_exec_source_rejects_unknown_pragma_field() {
        let error = parse_scalar("// @exec: {\"timeout_ms\": 1}\ntext('ok')").unwrap_err();
        assert_error_contains(&error, "only supports");
        assert_error_contains(&error, "timeout_ms");
    }

    #[test]
    fn parse_exec_source_accepts_safe_integer_limit() {
        let parsed = parse_exec_source(&RuntimeString::from(format!(
            "// @exec: {{\"yield_time_ms\": {MAX_JS_SAFE_INTEGER}, \"max_output_tokens\": {MAX_JS_SAFE_INTEGER}}}\ntext('ok')"
        )))
        .unwrap();

        assert_eq!(parsed.yield_time_ms, Some(MAX_JS_SAFE_INTEGER));
        assert_eq!(parsed.max_output_tokens, Some(MAX_JS_SAFE_INTEGER as usize));
    }

    #[test]
    fn parse_exec_source_rejects_values_above_safe_integer_limit() {
        let too_large = MAX_JS_SAFE_INTEGER + 1;
        let error = parse_exec_source(&RuntimeString::from(format!(
            "// @exec: {{\"yield_time_ms\": {too_large}}}\ntext('ok')"
        )))
        .unwrap_err();

        assert_error_contains(&error, "safe integer");
        assert_error_contains(&error, "yield_time_ms");
    }

    #[test]
    fn parse_exec_source_rejects_negative_field_values() {
        let error = parse_scalar("// @exec: {\"max_output_tokens\": -1}\ntext('ok')").unwrap_err();

        assert_error_contains(&error, "non-negative");
        assert_error_contains(&error, "max_output_tokens");
    }

    #[test]
    fn parse_exec_source_rejects_decimal_syntax_field_values() {
        for value in ["1.0", "1.5", "1e3", "-0"] {
            let error = parse_exec_source(&RuntimeString::from(format!(
                "// @exec: {{\"yield_time_ms\": {value}}}\ntext('ok')"
            )))
            .unwrap_err();

            assert_error_contains(&error, "non-negative");
            assert_error_contains(&error, "yield_time_ms");
        }
    }

    #[test]
    fn parse_exec_source_rejects_non_number_field_values() {
        for value in ["\"1\"", "true"] {
            let error = parse_exec_source(&RuntimeString::from(format!(
                "// @exec: {{\"max_output_tokens\": {value}}}\ntext('ok')"
            )))
            .unwrap_err();

            assert_error_contains(&error, "non-negative");
            assert_error_contains(&error, "max_output_tokens");
        }
    }

    #[test]
    fn parse_exec_source_rejects_duplicate_pragma_fields() {
        for field in ["yield_time_ms", "max_output_tokens"] {
            let error = parse_exec_source(&RuntimeString::from(format!(
                "// @exec: {{\"{field}\": 1, \"{field}\": 1.0}}\ntext('ok')"
            )))
            .unwrap_err();

            assert_error_contains(&error, "duplicate");
            assert_error_contains(&error, field);
        }
    }

    #[test]
    fn parse_exec_source_rejects_escaped_duplicate_pragma_fields() {
        let error = parse_scalar(
            "// @exec: {\"yield_time_ms\": 1, \"\\u0079ield_time_ms\": 2}\ntext('ok')",
        )
        .unwrap_err();
        assert_error_contains(&error, "duplicate");
        assert_error_contains(&error, "yield_time_ms");

        let error = parse_scalar(
            "// @exec: {\"max_output_tokens\": 1, \"max_output_\\u0074okens\": 2}\ntext('ok')",
        )
        .unwrap_err();
        assert_error_contains(&error, "duplicate");
        assert_error_contains(&error, "max_output_tokens");
    }

    #[test]
    fn parse_exec_source_rejects_pragma_without_subsequent_source() {
        let error = parse_scalar("// @exec: {\"yield_time_ms\": 1}").unwrap_err();
        assert_error_contains(&error, "followed by JavaScript source");
    }

    #[test]
    fn parse_exec_source_returns_ordinary_js_unchanged() {
        let source = "  // not a pragma\ntext('plain')\n";
        let parsed = parse_scalar(source).unwrap();

        assert_eq!(parsed.source, source);
        assert_eq!(parsed.yield_time_ms, None);
        assert_eq!(parsed.max_output_tokens, None);
    }

    #[test]
    fn parse_exec_source_uses_exact_utf16_slicing_and_ecmascript_whitespace() {
        let prefix =
            RuntimeString::from("\u{feff}\u{00a0}// @exec: {\"yield_time_ms\": 0}\u{3000}\n");
        let source = RuntimeString::from_utf16(vec![
            u16::from(b't'),
            u16::from(b'e'),
            u16::from(b'x'),
            u16::from(b't'),
            u16::from(b'('),
            0xd800,
            u16::from(b')'),
        ]);
        let input = prefix.concat(&source);
        let parsed = parse_exec_source(&input).unwrap();
        assert_eq!(parsed.source, source);
        assert_eq!(parsed.yield_time_ms, Some(0));

        let ordinary = RuntimeString::from_utf16(vec![u16::from(b'x'), 0xdc00, u16::from(b'y')]);
        let parsed = parse_exec_source(&ordinary).unwrap();
        assert_eq!(parsed.source, ordinary);
    }

    #[test]
    fn parse_exec_source_preserves_non_scalar_unknown_key_in_error() {
        let error = parse_scalar("// @exec: {\"\\ud800\": 1}\ntext('ok')").unwrap_err();
        assert_error_contains(&error, "only supports");
        assert!(error.as_utf16().contains(&0xd800), "{error:?}");
    }

    #[test]
    fn runtime_response_variants_encode_with_exact_ts_fields() {
        assert_eq!(
            runtime_codec::encode_value(&RuntimeResponse::Yielded {
                cell_id: CellId::new("cell_1".into()),
                content_items: vec![OutputContentItem::InputText {
                    text: "partial".into()
                }],
            })
            .unwrap(),
            exact_json(
                r#"{"type":"yielded","cellId":"cell_1","contentItems":[{"type":"input_text","text":"partial"}]}"#,
            )
        );

        assert_eq!(
            runtime_codec::encode_value(&RuntimeResponse::Terminated {
                cell_id: CellId::new("cell_2".into()),
                content_items: vec![],
            })
            .unwrap(),
            exact_json(r#"{"type":"terminated","cellId":"cell_2","contentItems":[]}"#)
        );

        assert_eq!(
            runtime_codec::encode_value(&RuntimeResponse::Result {
                cell_id: CellId::new("cell_3".into()),
                content_items: vec![],
                error_text: Some("boom".into()),
            })
            .unwrap(),
            exact_json(
                r#"{"type":"result","cellId":"cell_3","contentItems":[],"errorText":"boom"}"#,
            )
        );
    }

    #[test]
    fn protocol_dtos_round_trip_exact_runtime_carriers() {
        let lone_surrogate = exact_json(r#""\ud800""#)
            .into_string()
            .expect("fixture is a string");
        let schema = exact_json(r#"{"type":"object","title":"\ud800"}"#);
        let tool = ToolDefinition {
            name: lone_surrogate.clone(),
            tool_name: "Read".into(),
            description: "read exact text".into(),
            kind: CodeModeToolKind::Function,
            input_schema: Some(schema.clone()),
            output_schema: None,
        };
        assert_runtime_round_trip(&tool);

        let nested = NestedToolMetadata {
            tool_name: lone_surrogate.clone(),
            global_name: "global".into(),
            description: "description".into(),
            kind: CodeModeToolKind::Freeform,
        };
        assert_runtime_round_trip(&nested);

        let execute = ExecuteRequest {
            tool_call_id: lone_surrogate.clone(),
            enabled_tools: vec![tool],
            source: RuntimeString::from_utf16(vec![0xd800, u16::from(b';')]),
            yield_time_ms: Some(250),
            max_output_tokens: Some(42),
        };
        assert_runtime_round_trip(&execute);

        let parsed = ParsedExecSource {
            source: execute.source.clone(),
            yield_time_ms: Some(250),
            max_output_tokens: Some(42),
        };
        assert_runtime_round_trip(&parsed);

        let wait = WaitRequest {
            cell_id: CellId::new(lone_surrogate.clone()),
            yield_time_ms: Some(10),
            max_output_tokens: Some(20),
            terminate: Some(true),
        };
        assert_runtime_round_trip(&wait);

        let response = RuntimeResponse::Result {
            cell_id: CellId::new(lone_surrogate.clone()),
            content_items: vec![OutputContentItem::InputImage {
                image_url: lone_surrogate.clone(),
                detail: Some(ImageDetail::Original),
            }],
            error_text: Some(lone_surrogate.clone()),
        };
        assert_runtime_round_trip(&response);
        assert_runtime_round_trip(&WaitOutcome::MissingCell(response));

        let call = CodeModeNestedToolCall {
            cell_id: CellId::new(lone_surrogate.clone()),
            runtime_tool_call_id: lone_surrogate.clone(),
            tool_name: "Read".into(),
            tool_kind: CodeModeToolKind::Function,
            input: Some(schema),
        };
        assert_runtime_round_trip(&call);
    }
}
