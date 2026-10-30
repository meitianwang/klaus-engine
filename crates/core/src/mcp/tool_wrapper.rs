//! Wrap a single MCP tool descriptor as an engine-side [`Tool`].
//!
//! The wrapped tool:
//!  - is named `mcp__${server}__${tool}` (collision-resistant namespace),
//!  - validates input permissively (any object passes; the server is the real authority
//!    on its own schema), while still advertising the server's JSON Schema to the
//!    provider via the normalized input schema,
//!  - is treated as neither read-only nor concurrency-safe unless the server's
//!    `annotations` say otherwise (fail closed: an MCP server is an opaque external
//!    effect),
//!  - maps the MCP result content into `OutputBlock`s, trimming untrusted text to
//!    `max_result_chars` so a hostile/buggy server cannot blow up the context.
//!
//! Deferred loading: an MCP tool is marked `Deferred` exposure by default — its schema
//! stays OUT of the prompt until `tool_search` surfaces it — so connecting a server with
//! many tools doesn't balloon every request. A tool opts out (loads eagerly) when its
//! server config sets `alwaysLoad`, or its `_meta["anthropic/alwaysLoad"]` is true.
//! `_meta["anthropic/searchHint"]` (whitespace-collapsed) becomes the tool's search hint.

use crate::mcp::persist_blob::persist_mcp_blob;
use crate::mcp::types::{McpCallResult, McpContentBlock, McpToolDescriptor};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::schemas::content::{ImageBlock, OutputBlock, TextBlock};
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::json::Json;
use crate::types::permission::{
    ApprovalActionCallSource, ApprovalActionInputKind, ApprovalActionMetadata,
    ApprovalActionSource, ApprovalMcpMetadata, ApprovalMcpRegistration,
};

use crate::types::tool::{
    BoxFuture, McpMeta, Tool, ToolContext, ToolExposure, ToolProgress, ToolResult,
    ToolResultSerializable,
};
use async_trait::async_trait;

/// Default cap on total text returned to the model from one MCP tool call.
const DEFAULT_MAX_RESULT_CHARS: usize = 100 * 1024;

/// Fallback mime type for image blocks that omit one.
const DEFAULT_IMAGE_MIME: &str = "image/png";

fn runtime_concat(parts: impl IntoIterator<Item = RuntimeString>) -> RuntimeString {
    parts
        .into_iter()
        .fold(RuntimeString::default(), |output, part| {
            output.concat(&part)
        })
}

fn normalize_mcp_input_schema(schema: &RuntimeJson) -> RuntimeJson {
    let mut object = schema.as_object().cloned().unwrap_or_default();
    object.remove("$schema");
    if !object.contains_key("type") {
        object.insert("type", RuntimeJson::from("object"));
    }
    RuntimeJson::Object(object)
}

/// Invoke the underlying server tool by name with arguments. Threads the tool-event
/// progress sink (server progress → `status` events) and the turn's abort signal so a
/// turn abort cancels the in-flight server round-trip.
pub type McpToolCall = std::sync::Arc<
    dyn Fn(
            RuntimeString,
            RuntimeObject,
            Option<ToolProgress>,
            AbortSignal,
        ) -> BoxFuture<'static, Result<McpCallResult, ToolError>>
        + Send
        + Sync,
>;

/// Options for [`wrap_mcp_tool`].
#[derive(Default)]
pub struct WrapMcpToolOptions {
    /// Override for the text trim budget.
    pub max_result_chars: Option<usize>,
    /// Server-level `alwaysLoad` (from its config): force eager-load all its tools.
    pub server_always_load: Option<bool>,
    /// Whether to default to `Deferred` exposure (true) or `Direct` (false).
    /// Set to `false` when the provider does not support `tool_search`.
    pub deferred_by_default: Option<bool>,
    /// Pre-computed model-visible name from the disambiguation pipeline.
    /// When set, the wrapper uses this instead of the naive `mcp__{server}__{tool}`.
    pub override_name: Option<RuntimeString>,
    /// Account and connector attribution captured by the registration owner.
    /// Missing values remain unknown; arguments never supply this attribution.
    pub approval_registration: Option<ApprovalMcpRegistration>,
}

/// Wrap a single MCP tool descriptor as an engine-side [`Tool`].
pub fn wrap_mcp_tool(
    server_name: &str,
    mcp_tool: McpToolDescriptor,
    call: McpToolCall,
    opts: Option<WrapMcpToolOptions>,
) -> std::sync::Arc<dyn Tool> {
    wrap_registered_mcp_tool(server_name, mcp_tool, call, opts, None)
}

/// A native connection declaration and raw tool identity, never decoded from model input.
#[derive(Clone)]
pub struct NativeMcpToolBinding {
    pub(crate) config: std::sync::Arc<crate::mcp::types::McpServerConfig>,
    pub(crate) tool_name: RuntimeString,
}

pub(crate) fn wrap_native_mcp_tool(
    server_name: &str,
    mcp_tool: McpToolDescriptor,
    call: McpToolCall,
    opts: Option<WrapMcpToolOptions>,
    config: std::sync::Arc<crate::mcp::types::McpServerConfig>,
) -> std::sync::Arc<dyn Tool> {
    let binding = NativeMcpToolBinding {
        config,
        tool_name: mcp_tool.name.clone(),
    };
    wrap_registered_mcp_tool(server_name, mcp_tool, call, opts, Some(binding))
}

fn wrap_registered_mcp_tool(
    server_name: &str,
    mcp_tool: McpToolDescriptor,
    call: McpToolCall,
    opts: Option<WrapMcpToolOptions>,
    native_binding: Option<NativeMcpToolBinding>,
) -> std::sync::Arc<dyn Tool> {
    let opts = opts.unwrap_or_default();
    let max_result_chars = opts.max_result_chars.unwrap_or(DEFAULT_MAX_RESULT_CHARS);
    let registration = opts.approval_registration.unwrap_or_default();
    let mut annotations = RuntimeObject::new();
    if let Some(hints) = &mcp_tool.annotations {
        for (key, value) in [
            ("destructive_hint", hints.destructive_hint),
            ("open_world_hint", hints.open_world_hint),
            ("read_only_hint", hints.read_only_hint),
        ] {
            if let Some(value) = value {
                annotations.insert(key, RuntimeJson::Bool(value));
            }
        }
    }
    let approval_metadata = ApprovalMcpMetadata {
        native_registration: registration.native_registration,
        server: server_name.into(),
        tool_name: mcp_tool.name.clone(),
        connector_id: registration.connector_id,
        connector_name: registration.connector_name,
        connector_description: registration.connector_description,
        connected_account_email: registration.connected_account_email,
        tool_title: mcp_tool.title.clone(),
        tool_description: mcp_tool.description.clone(),
        annotations: (!annotations.is_empty()).then_some(RuntimeJson::Object(annotations)),
    };
    let tool_name = match opts.override_name {
        Some(name) => name,
        None => runtime_concat([
            RuntimeString::from(format!("mcp__{server_name}__")),
            mcp_tool.name.clone(),
        ]),
    };

    // Deferred-loading hints (server-asserted, via tools/list `_meta`). alwaysLoad is an
    // OR of the server-level config flag and the per-tool `_meta` flag; the search hint
    // is the per-tool phrase.
    let search_hint = mcp_tool
        .meta
        .as_ref()
        .and_then(|m| m.get("anthropic/searchHint"))
        .and_then(normalize_search_hint);
    let always_load = opts.server_always_load == Some(true)
        || mcp_tool
            .meta
            .as_ref()
            .and_then(|m| m.get("anthropic/alwaysLoad"))
            == Some(&RuntimeJson::Bool(true));

    let input_schema = match &mcp_tool.input_schema {
        Some(schema) => normalize_mcp_input_schema(schema),
        None => normalize_mcp_input_schema(&RuntimeJson::Object(RuntimeObject::new())),
    };

    let description = mcp_tool.description.clone().unwrap_or_else(|| {
        runtime_concat([
            RuntimeString::from("MCP tool "),
            mcp_tool.name.clone(),
            RuntimeString::from(format!(" on server {server_name}.")),
        ])
    });

    // MCP `annotations` are SERVER-ASSERTED behavior hints (untrusted). They drive both
    // scheduling AND the permission gate:
    //  - is_concurrency_safe ← `readOnlyHint` alone: a read-only tool may run in PARALLEL
    //    with other concurrency-safe tools — even one also flagged destructive.
    //  - is_read_only ← `readOnlyHint` AND NOT `destructiveHint` (destructive wins over
    //    read-only). is_read_only feeds the permission engine's read-only auto-allow, so a
    //    tool the server declares read-only & non-destructive runs WITHOUT prompting; a
    //    read-only tool ALSO flagged destructive is NOT auto-allowed and falls through to
    //    the ask. `openWorldHint`/`idempotentHint` do NOT override read-only.
    let read_only_hint = mcp_tool.annotations.as_ref().and_then(|a| a.read_only_hint) == Some(true);
    let destructive_hint = mcp_tool
        .annotations
        .as_ref()
        .and_then(|a| a.destructive_hint)
        == Some(true);

    // Deferred by default when the provider supports tool_search; alwaysLoad
    // opts back into eager exposure. When the provider does NOT support deferred
    // tools, everything is Direct regardless.
    let deferred_mode = opts.deferred_by_default.unwrap_or(true);
    let exposure = if always_load || !deferred_mode {
        ToolExposure::Direct
    } else {
        ToolExposure::Deferred
    };

    std::sync::Arc::new(WrappedMcpTool {
        native_binding,
        name: tool_name,
        approval_metadata,
        mcp_tool_name: mcp_tool.name.clone(),
        server_name: server_name.into(),
        description,
        input_schema,
        exposure,
        search_hint,
        read_only_hint,
        destructive_hint,
        max_result_chars,
        call,
    })
}

struct WrappedMcpTool {
    native_binding: Option<NativeMcpToolBinding>,
    name: RuntimeString,
    approval_metadata: ApprovalMcpMetadata,
    mcp_tool_name: RuntimeString,
    server_name: RuntimeString,
    description: RuntimeString,
    input_schema: RuntimeJson,
    exposure: ToolExposure,
    search_hint: Option<RuntimeString>,
    read_only_hint: bool,
    destructive_hint: bool,
    max_result_chars: usize,
    call: McpToolCall,
}

#[async_trait]
impl Tool for WrappedMcpTool {
    fn native_mcp_binding(&self) -> Option<NativeMcpToolBinding> {
        self.native_binding.clone()
    }
    fn name(&self) -> RuntimeString {
        self.name.clone()
    }

    fn description(&self) -> RuntimeString {
        self.description.clone()
    }

    fn input_schema(&self) -> RuntimeJson {
        self.input_schema.clone()
    }

    fn exposure(&self) -> ToolExposure {
        self.exposure
    }

    fn search_hint(&self) -> Option<RuntimeString> {
        self.search_hint.clone()
    }

    fn approval_metadata(&self) -> Option<ApprovalActionMetadata> {
        Some(ApprovalActionMetadata {
            registered_tool_name: self.name.clone(),
            source: ApprovalActionSource::Mcp(Box::new(self.approval_metadata.clone())),
            call_source: ApprovalActionCallSource::Direct,
            originating_item_id: None,
            input_kind: ApprovalActionInputKind::Function,
            paths: Vec::new(),
            path_kind: None,
            cwd: None,
            terminal_launch: None,
            network_trigger: None,
        })
    }

    // Tag the originating server so the query loop can count MCP tools across a
    // mid-session refresh.
    fn mcp_server(&self) -> Option<RuntimeString> {
        Some(self.server_name.clone())
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        self.read_only_hint
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        self.read_only_hint && !self.destructive_hint
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        // Accept any object so the server validates its own args.
        let args: RuntimeObject = match &input {
            Json::Object(m) => m.clone(),
            _ => RuntimeObject::new(),
        };
        // Pass the tool-event sink as the MCP progress callback and the turn's abort
        // signal, so a turn abort cancels the in-flight server round-trip. On abort the
        // call rejects, which the executor turns into an interrupted tool_result.
        let result = (self.call)(
            self.mcp_tool_name.clone(),
            args,
            on_progress,
            ctx.signal.clone(),
        )
        .await?;
        let model_content = map_content(&result, self.max_result_chars, &self.server_name).await;
        let is_error = result.is_error.unwrap_or(false);
        let mcp_meta = build_mcp_meta(&result);
        let output = runtime_codec::encode_value(&result).unwrap_or(RuntimeJson::Null);
        Ok(ToolResult {
            serializable: ToolResultSerializable {
                output,
                model_content,
                is_error: Some(is_error),
                mcp_meta,
                extra_user_blocks: None,
            },
            side_effects: None,
        })
    }
}

/// Map MCP content blocks to engine `OutputBlock`s the model can use, trimming total text
/// to `max_result_chars` of (untrusted) characters. Embedded resources, audio, and
/// resource links are homogenized into text/image rather than dumped as raw JSON — a
/// `resource` text → a labeled text block; an image-blob resource → an image block; other
/// blobs / audio → persisted to disk and referenced by path; `resource_link` → a
/// reference line. `structuredContent` is appended as a text block. Image base64 is not
/// counted against the text budget. When trimming occurs, an elision note is appended.
async fn map_content(
    result: &McpCallResult,
    max_result_chars: usize,
    server_name: &RuntimeString,
) -> Vec<OutputBlock> {
    let mut blocks: Vec<OutputBlock> = Vec::new();
    // Reserve room for the (engine-authored) truncation marker so that even when it is
    // appended the TOTAL emitted text still respects `max_result_chars`.
    let elision_note = RuntimeString::from(format!(
        "\n[mcp: result truncated to {max_result_chars} characters]"
    ));
    let mut budget = max_result_chars.saturating_sub(elision_note.len_utf16());
    let mut trimmed = false;

    for c in &result.content {
        if c.block_type == "image" {
            blocks.push(OutputBlock::Image(ImageBlock {
                mime_type: c
                    .mime_type
                    .clone()
                    .unwrap_or_else(|| RuntimeString::from(DEFAULT_IMAGE_MIME)),
                base64: c.data.clone().unwrap_or_default(),
                detail: None,
                source_path: None,
            }));
        } else if c.block_type == "text" {
            push_text(
                &mut blocks,
                &mut budget,
                &mut trimmed,
                c.text.clone().unwrap_or_default(),
            );
        } else if c.block_type == "audio" {
            let saved = saved_note(c.data.as_ref(), c.mime_type.as_ref()).await;
            let note = runtime_concat([
                RuntimeString::from("[Audio from "),
                server_name.clone(),
                mime_note(c.mime_type.as_ref()),
                saved,
                RuntimeString::from("]"),
            ]);
            push_text(&mut blocks, &mut budget, &mut trimmed, note);
        } else if c.block_type == "resource_link" {
            let name = c
                .name
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| runtime_concat([RuntimeString::from(": "), value.clone()]))
                .unwrap_or_default();
            let description = c
                .description
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| {
                    runtime_concat([
                        RuntimeString::from(" ("),
                        value.clone(),
                        RuntimeString::from(")"),
                    ])
                })
                .unwrap_or_default();
            let line = runtime_concat([
                RuntimeString::from("[Resource link"),
                name,
                RuntimeString::from("] "),
                c.uri.clone().unwrap_or_default(),
                description,
            ]);
            push_text(
                &mut blocks,
                &mut budget,
                &mut trimmed,
                trim_end_ecmascript(&line),
            );
        } else if let Some(resource) = c.resource.as_ref().filter(|_| c.block_type == "resource") {
            let at = resource
                .uri
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| runtime_concat([RuntimeString::from(" at "), value.clone()]))
                .unwrap_or_default();
            if let Some(text) = &resource.text {
                push_text(
                    &mut blocks,
                    &mut budget,
                    &mut trimmed,
                    runtime_concat([
                        RuntimeString::from("[Resource from "),
                        server_name.clone(),
                        at,
                        RuntimeString::from("]\n"),
                        text.clone(),
                    ]),
                );
            } else if resource.blob.is_some() && is_image_mime(resource.mime_type.as_ref()) {
                push_text(
                    &mut blocks,
                    &mut budget,
                    &mut trimmed,
                    runtime_concat([
                        RuntimeString::from("[Image resource from "),
                        server_name.clone(),
                        at,
                        RuntimeString::from("]"),
                    ]),
                );
                blocks.push(OutputBlock::Image(ImageBlock {
                    mime_type: resource
                        .mime_type
                        .clone()
                        .unwrap_or_else(|| RuntimeString::from(DEFAULT_IMAGE_MIME)),
                    base64: resource.blob.clone().unwrap_or_default(),
                    detail: None,
                    source_path: None,
                }));
            } else if let Some(blob) = &resource.blob {
                let saved = saved_note(Some(blob), resource.mime_type.as_ref()).await;
                let note = runtime_concat([
                    RuntimeString::from("[Resource from "),
                    server_name.clone(),
                    at,
                    mime_note(resource.mime_type.as_ref()),
                    saved,
                    RuntimeString::from("]"),
                ]);
                push_text(&mut blocks, &mut budget, &mut trimmed, note);
            } else {
                push_text(
                    &mut blocks,
                    &mut budget,
                    &mut trimmed,
                    runtime_concat([
                        RuntimeString::from("[Resource from "),
                        server_name.clone(),
                        at,
                        mime_note(resource.mime_type.as_ref()),
                        RuntimeString::from("]"),
                    ]),
                );
            }
        } else {
            // Unknown/other block types: stringify defensively.
            push_text(&mut blocks, &mut budget, &mut trimmed, stringify_block(c));
        }
    }

    if let Some(sc) = &result.structured_content {
        let json = safe_stringify(sc);
        if !json.is_empty() {
            push_text(
                &mut blocks,
                &mut budget,
                &mut trimmed,
                runtime_concat([RuntimeString::from("Structured content:\n"), json]),
            );
        }
    }

    if trimmed {
        blocks.push(OutputBlock::Text(TextBlock {
            text: elision_note,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
    }
    // Ensure there is always at least one block so the model gets a tool_result.
    if blocks.is_empty() {
        blocks.push(OutputBlock::Text(TextBlock {
            text: RuntimeString::default(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
    }
    blocks
}

fn push_text(
    blocks: &mut Vec<OutputBlock>,
    budget: &mut usize,
    trimmed: &mut bool,
    text: RuntimeString,
) {
    if *budget == 0 {
        *trimmed = true;
        return;
    }
    let tlen = text.len_utf16();
    if tlen > *budget {
        let sliced = text.slice_utf16(..*budget);
        blocks.push(OutputBlock::Text(TextBlock {
            text: sliced,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
        *budget = 0;
        *trimmed = true;
    } else {
        blocks.push(OutputBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
        *budget -= tlen;
    }
}

/// Normalize a server-provided search hint: must be a string; collapse whitespace runs;
/// empty → `None`.
fn normalize_search_hint(value: &RuntimeJson) -> Option<RuntimeString> {
    let value = value.as_string()?;
    let mut output = Vec::with_capacity(value.len_utf16());
    let mut prev_ws = false;
    for unit in value.as_utf16().iter().copied() {
        if is_ecmascript_whitespace(unit) {
            if !prev_ws {
                output.push(u16::from(b' '));
                prev_ws = true;
            }
        } else {
            output.push(unit);
            prev_ws = false;
        }
    }
    let trimmed = trim_end_ecmascript(&trim_start_ecmascript(&RuntimeString::from_utf16(output)));
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn mime_note(mime: Option<&RuntimeString>) -> RuntimeString {
    match mime {
        Some(mime) if !mime.is_empty() => runtime_concat([
            RuntimeString::from(" ("),
            mime.clone(),
            RuntimeString::from(")"),
        ]),
        _ => RuntimeString::default(),
    }
}

/// Approximate decoded byte count of a base64 string, as a `, ≈N bytes` note.
fn byte_note(base64: Option<&RuntimeString>) -> RuntimeString {
    match base64 {
        Some(base64) if !base64.is_empty() => {
            let bytes = (base64.len_utf16() * 3) / 4;
            RuntimeString::from(format!(", ≈{bytes} bytes"))
        }
        _ => RuntimeString::default(),
    }
}

/// Persist a binary blob to disk and return a ` saved to <path>` note. Best-effort: a
/// write failure degrades to a `≈N bytes — not inlined` reference so a bad blob can never
/// sink the tool call.
async fn saved_note(base64: Option<&RuntimeString>, mime: Option<&RuntimeString>) -> RuntimeString {
    let base64 = match base64 {
        Some(base64) if !base64.is_empty() => base64,
        _ => return RuntimeString::default(),
    };
    match persist_mcp_blob(base64, mime).await {
        Ok(path) => runtime_concat([
            RuntimeString::from(" saved to "),
            path.into_runtime_string(),
        ]),
        Err(_) => runtime_concat([
            byte_note(Some(base64)),
            RuntimeString::from(" — not inlined"),
        ]),
    }
}

fn is_image_mime(mime: Option<&RuntimeString>) -> bool {
    mime.is_some_and(|m| m.starts_with("image/"))
}

fn safe_stringify(value: &Json) -> RuntimeString {
    runtime_json::stringify_pretty(value)
}

/// Render an unknown content block as JSON text (defensive fallback).
fn stringify_block(c: &McpContentBlock) -> RuntimeString {
    runtime_codec::encode_value(c)
        .map(|value| runtime_json::stringify(&value))
        .unwrap_or_else(|_| {
            runtime_concat([
                RuntimeString::from("[mcp: unserializable content block of type \""),
                c.block_type.clone(),
                RuntimeString::from("\"]"),
            ])
        })
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

fn trim_start_ecmascript(value: &RuntimeString) -> RuntimeString {
    let start = value
        .as_utf16()
        .iter()
        .position(|unit| !is_ecmascript_whitespace(*unit))
        .unwrap_or(value.len_utf16());
    value.slice_utf16(start..)
}

fn trim_end_ecmascript(value: &RuntimeString) -> RuntimeString {
    let end = value
        .as_utf16()
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(0, |index| index + 1);
    value.slice_utf16(..end)
}

/// Carry the server's `_meta` / `structuredContent` through to the serializable tool
/// result, but only when present.
fn build_mcp_meta(result: &McpCallResult) -> Option<McpMeta> {
    let mut meta = McpMeta {
        meta: None,
        structured_content: None,
    };
    let mut has = false;
    if let Some(v) = &result.meta {
        meta.meta = Some(v.clone());
        has = true;
    }
    if let Some(v) = &result.structured_content {
        meta.structured_content = Some(v.clone());
        has = true;
    }
    if has {
        Some(meta)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::types::{McpEmbeddedResource, McpToolAnnotations};
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::events::ToolDomainEvent;
    use std::sync::{Arc, Mutex};

    fn runtime(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).unwrap()
    }

    macro_rules! json {
        ($($tokens:tt)+) => {
            runtime(stringify!($($tokens)+))
        };
    }

    fn block(kind: &str) -> McpContentBlock {
        McpContentBlock {
            block_type: kind.into(),
            text: None,
            data: None,
            mime_type: None,
            resource: None,
            uri: None,
            name: None,
            description: None,
        }
    }

    fn content_text(text: impl Into<RuntimeString>) -> McpContentBlock {
        let mut b = block("text");
        b.text = Some(text.into());
        b
    }

    fn result(content: Vec<McpContentBlock>) -> McpCallResult {
        McpCallResult {
            content,
            is_error: None,
            meta: None,
            structured_content: None,
        }
    }

    fn desc(name: &str) -> McpToolDescriptor {
        McpToolDescriptor {
            name: name.into(),
            title: None,
            description: None,
            input_schema: None,
            annotations: None,
            meta: None,
        }
    }

    fn annot(
        read_only: Option<bool>,
        idempotent: Option<bool>,
        destructive: Option<bool>,
        open_world: Option<bool>,
    ) -> McpToolAnnotations {
        McpToolAnnotations {
            read_only_hint: read_only,
            idempotent_hint: idempotent,
            destructive_hint: destructive,
            open_world_hint: open_world,
        }
    }

    fn meta_map(pairs: &[(&str, RuntimeJson)]) -> RuntimeObject {
        let mut m = RuntimeObject::new();
        for (k, v) in pairs {
            m.insert(*k, v.clone());
        }
        m
    }

    fn noop_call() -> McpToolCall {
        Arc::new(
            |_n, _a, _p, _s| -> BoxFuture<'static, Result<McpCallResult, ToolError>> {
                Box::pin(async { Ok(result(vec![content_text("ok")])) })
            },
        )
    }

    /// Build a wrapped MCP tool whose underlying call returns `res`, then invoke it.
    async fn run(res: McpCallResult) -> Vec<OutputBlock> {
        run_with_max_result_chars(res, None).await
    }

    async fn run_with_max_result_chars(
        res: McpCallResult,
        max_result_chars: Option<usize>,
    ) -> Vec<OutputBlock> {
        let call: McpToolCall = Arc::new(
            move |_n, _a, _p, _s| -> BoxFuture<'static, Result<McpCallResult, ToolError>> {
                let r = res.clone();
                Box::pin(async move { Ok(r) })
            },
        );
        let opts = max_result_chars.map(|max_result_chars| WrapMcpToolOptions {
            max_result_chars: Some(max_result_chars),
            ..Default::default()
        });
        let tool = wrap_mcp_tool("srv", desc("t"), call, opts);
        let ctx = make_ctx("/tmp");
        let out = tool.call(json!({}), &ctx, None).await.unwrap();
        out.serializable.model_content
    }

    fn text_of(blocks: &[OutputBlock]) -> RuntimeString {
        let mut output = RuntimeString::default();
        let mut first = true;
        for block in blocks {
            let OutputBlock::Text(text) = block else {
                continue;
            };
            if !first {
                output = output.concat(&RuntimeString::from("\n"));
            }
            output = output.concat(&text.text);
            first = false;
        }
        output
    }

    // ---- content-block mapping -------------------------------------------------

    #[tokio::test]
    async fn maps_a_text_resource_to_a_labeled_text_block() {
        let mut b = block("resource");
        b.resource = Some(McpEmbeddedResource {
            uri: Some("file://a.txt".into()),
            text: Some("HELLO".into()),
            blob: None,
            mime_type: None,
        });
        let blocks = run(result(vec![b])).await;
        let t = text_of(&blocks);
        assert!(t.contains("Resource from srv at file://a.txt"));
        assert!(t.contains("HELLO"));
        assert!(!t.contains("\"type\":\"resource\""));
    }

    #[tokio::test]
    async fn maps_an_image_blob_resource_to_an_image_block() {
        let mut b = block("resource");
        b.resource = Some(McpEmbeddedResource {
            uri: Some("file://p.png".into()),
            text: None,
            blob: Some("QUJD".into()),
            mime_type: Some("image/png".into()),
        });
        let blocks = run(result(vec![b])).await;
        let img = blocks.iter().find_map(|b| match b {
            OutputBlock::Image(i) => Some(i),
            _ => None,
        });
        let img = img.unwrap();
        assert_eq!(img.base64, "QUJD");
        assert_eq!(img.mime_type, "image/png");
    }

    #[tokio::test]
    async fn persists_a_non_image_blob_resource_and_references_the_path() {
        let mut b = block("resource");
        b.resource = Some(McpEmbeddedResource {
            uri: Some("file://x.bin".into()),
            text: None,
            blob: Some("AAAA".into()),
            mime_type: Some("application/octet-stream".into()),
        });
        let blocks = run(result(vec![b])).await;
        let t = text_of(&blocks);
        assert!(t.contains("Resource from srv at file://x.bin"));
        assert!(t.contains("saved to "));
        assert!(t.contains("klaus-agent-mcp-"));
        assert!(t.contains(".bin"));
        assert!(!blocks.iter().any(|b| matches!(b, OutputBlock::Image(_))));
    }

    #[tokio::test]
    async fn persists_audio_and_references_the_path() {
        let mut b = block("audio");
        b.data = Some("QUJDRA==".into());
        b.mime_type = Some("audio/wav".into());
        let blocks = run(result(vec![b])).await;
        let t = text_of(&blocks);
        assert!(t.contains("Audio from srv"));
        assert!(t.contains("audio/wav"));
        assert!(t.contains("saved to "));
        assert!(t.contains("klaus-agent-mcp-"));
        assert!(t.contains(".wav"));
    }

    #[tokio::test]
    async fn maps_a_resource_link_to_a_reference_line() {
        let mut b = block("resource_link");
        b.uri = Some("https://x/y".into());
        b.name = Some("doc".into());
        b.description = Some("the doc".into());
        let blocks = run(result(vec![b])).await;
        let t = text_of(&blocks);
        assert!(t.contains("Resource link: doc"));
        assert!(t.contains("https://x/y"));
        assert!(t.contains("the doc"));
    }

    #[tokio::test]
    async fn appends_structured_content_as_a_visible_text_block() {
        let mut r = result(vec![content_text("ok")]);
        r.structured_content = Some(json!({ "score": 42, "label": "good" }));
        let blocks = run(r).await;
        let t = text_of(&blocks);
        assert!(t.contains("Structured content:"));
        assert!(t.contains("\"score\": 42"));
        assert!(t.contains("\"label\": \"good\""));
    }

    #[tokio::test]
    async fn structured_content_preserves_runtime_lone_surrogate_escapes() {
        let mut r = result(vec![content_text("ok")]);
        r.structured_content = Some(runtime(r#"{"surrogate":"\ud800","literal":"\uFDD0ud800"}"#));

        let blocks = run(r).await;
        let text = text_of(&blocks);

        assert!(text.contains("\"surrogate\": \"\\ud800\""));
        assert!(text.contains("\"literal\": \"﷐ud800\""));
    }

    #[tokio::test]
    async fn passes_text_and_image_through_unchanged() {
        let mut img = block("image");
        img.data = Some("SU1H".into());
        img.mime_type = Some("image/jpeg".into());
        let blocks = run(result(vec![content_text("plain"), img])).await;
        assert!(text_of(&blocks).contains("plain"));
        let i = blocks
            .iter()
            .find_map(|b| match b {
                OutputBlock::Image(i) => Some(i),
                _ => None,
            })
            .unwrap();
        assert_eq!(i.base64, "SU1H");
    }

    #[tokio::test]
    async fn trims_text_by_js_utf16_units_to_match_ts() {
        let blocks = run_with_max_result_chars(result(vec![content_text("A😀BC")]), Some(44)).await;
        let text_blocks = blocks
            .iter()
            .filter_map(|b| match b {
                OutputBlock::Text(t) => Some(&t.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(text_blocks[0], "A😀");
        assert_eq!(text_blocks[1], "\n[mcp: result truncated to 44 characters]");
    }

    #[tokio::test]
    async fn trims_runtime_lone_surrogates_as_one_javascript_utf16_unit() {
        let value = runtime(r#""A\ud800BC""#);
        let text = value.as_string().unwrap().clone();
        let blocks = run_with_max_result_chars(result(vec![content_text(text)]), Some(44)).await;
        let text_blocks = blocks
            .iter()
            .filter_map(|block| match block {
                OutputBlock::Text(text) => Some(&text.text),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            runtime_json::stringify(&Json::String(text_blocks[0].clone())),
            RuntimeString::from(r#""A\ud800B""#)
        );
        assert_eq!(text_blocks[1], "\n[mcp: result truncated to 44 characters]");
    }

    // ---- progress --------------------------------------------------------------

    #[tokio::test]
    async fn forwards_mcp_progress_to_the_tool_event_sink_as_a_status_event() {
        let events: Arc<Mutex<Vec<ToolDomainEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let ev = events.clone();
        let on_progress: ToolProgress = Box::new(move |e| {
            ev.lock().unwrap().push(e);
            Box::pin(async {})
        });
        // The underlying call invokes the on_progress it receives.
        let call: McpToolCall = Arc::new(
            |_n, _a, on_progress, _s| -> BoxFuture<'static, Result<McpCallResult, ToolError>> {
                Box::pin(async move {
                    if let Some(cb) = &on_progress {
                        cb(ToolDomainEvent::Status {
                            message: "working 1/2".into(),
                        })
                        .await;
                    }
                    Ok(result(vec![content_text("done")]))
                })
            },
        );
        let tool = wrap_mcp_tool("srv", desc("t"), call, None);
        let ctx = make_ctx("/tmp");
        tool.call(json!({}), &ctx, Some(on_progress)).await.unwrap();
        let evs = events.lock().unwrap();
        assert!(evs
            .iter()
            .any(|e| matches!(e, ToolDomainEvent::Status { message } if message == "working 1/2")));
    }

    // ---- annotation-driven exec hints ------------------------------------------

    #[test]
    fn marks_read_only_and_concurrency_safe_when_read_only_hint_true() {
        let mut d = desc("search");
        d.annotations = Some(annot(Some(true), None, None, None));
        let t = wrap_mcp_tool("srv", d, noop_call(), None);
        assert!(t.is_read_only(&json!({})));
        assert!(t.is_concurrency_safe(&json!({})));
    }

    #[test]
    fn keeps_the_conservative_barrier_default_when_annotations_absent() {
        let t = wrap_mcp_tool("srv", desc("write_file"), noop_call(), None);
        assert!(!t.is_read_only(&json!({})));
        assert!(!t.is_concurrency_safe(&json!({})));
        assert_eq!(t.is_destructive(&json!({})), Some(false));
    }

    #[test]
    fn does_not_treat_read_only_false_or_idempotent_only_as_read_only() {
        let mut ro = desc("a");
        ro.annotations = Some(annot(Some(false), None, None, None));
        let ro = wrap_mcp_tool("srv", ro, noop_call(), None);
        assert!(!ro.is_read_only(&json!({})));
        let mut idem = desc("b");
        idem.annotations = Some(annot(None, Some(true), None, None));
        let idem = wrap_mcp_tool("srv", idem, noop_call(), None);
        assert!(!idem.is_read_only(&json!({})));
        assert!(!idem.is_concurrency_safe(&json!({})));
    }

    #[test]
    fn destructive_wins_over_read_only_but_still_parallel_safe() {
        let mut d = desc("wipe");
        d.annotations = Some(annot(Some(true), None, Some(true), None));
        let t = wrap_mcp_tool("srv", d, noop_call(), None);
        assert!(!t.is_read_only(&json!({})));
        assert!(t.is_concurrency_safe(&json!({})));
    }

    #[test]
    fn open_world_hint_does_not_override_a_read_only_tool() {
        let mut d = desc("lookup");
        d.annotations = Some(annot(Some(true), None, None, Some(true)));
        let t = wrap_mcp_tool("srv", d, noop_call(), None);
        assert!(t.is_read_only(&json!({})));
        assert!(t.is_concurrency_safe(&json!({})));
    }

    #[tokio::test]
    async fn threads_the_turns_abort_signal_into_the_underlying_server_call() {
        let seen: Arc<Mutex<Option<AbortSignal>>> = Arc::new(Mutex::new(None));
        let seen2 = seen.clone();
        let call: McpToolCall = Arc::new(
            move |_n, _a, _p, signal| -> BoxFuture<'static, Result<McpCallResult, ToolError>> {
                let seen2 = seen2.clone();
                Box::pin(async move {
                    *seen2.lock().unwrap() = Some(signal);
                    Ok(result(vec![content_text("ok")]))
                })
            },
        );
        let tool = wrap_mcp_tool("srv", desc("t"), call, None);
        let token = AbortSignal::new();
        let mut ctx = make_ctx("/tmp");
        ctx.signal = token.clone();
        tool.call(json!({}), &ctx, None).await.unwrap();
        // Cancelling the turn's token cancels the threaded signal — proving it is the
        // same token that reached the underlying server call.
        token.cancel();
        let st = seen.lock().unwrap().clone().unwrap();
        assert!(st.is_cancelled());
    }

    // ---- deferred-loading hints (_meta) ----------------------------------------

    #[test]
    fn mcp_tools_default_to_deferred_exposure() {
        let t = wrap_mcp_tool("srv", desc("t"), noop_call(), None);
        assert_eq!(t.exposure(), ToolExposure::Deferred);
    }

    #[test]
    fn reads_and_whitespace_collapses_search_hint() {
        let mut d = desc("t");
        d.meta = Some(meta_map(&[(
            "anthropic/searchHint",
            json!("  search   the\nweb  "),
        )]));
        let t = wrap_mcp_tool("srv", d, noop_call(), None);
        assert_eq!(t.search_hint(), Some(RuntimeString::from("search the web")));
    }

    #[test]
    fn blank_or_non_string_search_hint_is_none() {
        fn hint(meta: Option<RuntimeObject>) -> Option<RuntimeString> {
            let mut d = desc("t");
            d.meta = meta;
            wrap_mcp_tool("srv", d, noop_call(), None).search_hint()
        }
        assert_eq!(
            hint(Some(meta_map(&[("anthropic/searchHint", json!("   "))]))),
            None
        );
        assert_eq!(
            hint(Some(meta_map(&[("anthropic/searchHint", json!(42))]))),
            None
        );
        assert_eq!(hint(None), None);
    }

    #[test]
    fn always_load_meta_opts_out_of_deferral() {
        let mut d = desc("t");
        d.meta = Some(meta_map(&[("anthropic/alwaysLoad", json!(true))]));
        let t = wrap_mcp_tool("srv", d, noop_call(), None);
        assert_eq!(t.exposure(), ToolExposure::Direct);
    }

    #[test]
    fn server_level_always_load_opts_out_of_deferral() {
        let t = wrap_mcp_tool(
            "srv",
            desc("t"),
            noop_call(),
            Some(WrapMcpToolOptions {
                server_always_load: Some(true),
                ..Default::default()
            }),
        );
        assert_eq!(t.exposure(), ToolExposure::Direct);
    }
}
