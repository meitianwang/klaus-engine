//! `lsp_diagnostics` / `lsp_hover` / `lsp_definition` / `lsp_references` /
//! `lsp_document_symbols` (and the call-hierarchy family) — code intelligence
//! backed by a host-configured language server (the engine is only the LSP
//! client). Positions are 1-based to the model (matching how files are read) and
//! converted to LSP's 0-based internally. Present only when `config.lsp` is
//! enabled; all fail-soft.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map as SchemaMap, Value as SchemaValue};

use crate::fs::node_path::{relative_posix_runtime, resolve_runtime};
use crate::lsp::types::{LspQueryError, LspSeverity};
use crate::runtime_values::{
    runtime_codec, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};

macro_rules! exact_json {
    (null) => {
        RuntimeJson::Null
    };
    ([ $($value:tt),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_json!($value)),*])
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        #[allow(unused_mut)]
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_json!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        runtime_codec::encode_value(&$value).expect("exact LSP tool JSON expression must encode")
    };
}

// ---- shared helpers ----

/// The fail-soft result returned when no language server is wired for the
/// session.
fn not_enabled() -> ToolResult {
    let m = "Language-server support is not enabled for this session.";
    text_result(exact_json!({ "ok": false, "message": m }), m, true)
}

/// A document eligibility rejection is a normal, actionable LSP result (CC's
/// LSPTool returns `{data: output}` for the size limit) rather than a tool-use
/// execution failure. The stable code/byte fields let clients distinguish it
/// from missing files and empty language-server results.
fn query_rejected(error: LspQueryError) -> ToolResult {
    match error {
        LspQueryError::FileTooLarge {
            size_bytes,
            max_bytes,
        } => {
            let rounded_mb = size_bytes / 1_000_000 + u64::from(size_bytes % 1_000_000 != 0);
            let message = RuntimeString::from(format!(
                "File too large for LSP analysis ({rounded_mb}MB exceeds 10MB limit)"
            ));
            text_result(
                exact_json!({
                    "ok": false,
                    "code": "file_too_large",
                    "sizeBytes": (size_bytes),
                    "maxBytes": (max_bytes),
                    "message": (message.clone()),
                }),
                message,
                false,
            )
        }
        LspQueryError::UnsupportedFileType => {
            let message =
                RuntimeString::from("File is not a regular file and cannot be analyzed by LSP.");
            text_result(
                exact_json!({
                    "ok": false,
                    "code": "unsupported_file_type",
                    "message": (message.clone()),
                }),
                message,
                false,
            )
        }
    }
}

/// The lowercase wire form of a diagnostic severity.
fn severity_str(s: LspSeverity) -> &'static str {
    match s {
        LspSeverity::Error => "error",
        LspSeverity::Warning => "warning",
        LspSeverity::Information => "information",
        LspSeverity::Hint => "hint",
    }
}

/// Read the required `path` field, defaulting to an empty string when absent
/// (already-validated input always carries it).
fn read_path(input: &Json) -> RuntimePath {
    input
        .get("path")
        .and_then(Json::as_str)
        .cloned()
        .map(RuntimePath::from)
        .unwrap_or_default()
}

/// Read a 1-based `line`/`character` pair and convert to the 0-based positions
/// the LSP expects.
fn read_position(input: &Json) -> (u32, u32) {
    let line = input.get("line").and_then(Json::as_u64).unwrap_or(1);
    let character = input.get("character").and_then(Json::as_u64).unwrap_or(1);
    (
        line.saturating_sub(1) as u32,
        character.saturating_sub(1) as u32,
    )
}

fn resolve_for_cwd(cwd: &RuntimePath, path: RuntimePath) -> RuntimePath {
    resolve_runtime(cwd, &[path]).expect("an explicit RuntimePath cwd cannot fail resolution")
}

fn relative(cwd: &RuntimePath, from: &RuntimePath, to: &RuntimePath) -> RuntimePath {
    relative_posix_runtime(cwd, from, to)
        .expect("explicit RuntimePath values cannot fail relative-path calculation")
}

/// Show a result path relative to cwd when that's shorter and stays inside the
/// tree.
fn rel_for_cwd(cwd: &RuntimePath, path: &RuntimeString) -> RuntimeString {
    let path = RuntimePath::from(path.clone());
    // POSIX-rendered: this string is shown to the model.
    let rel = relative(cwd, cwd, &path);
    let rel = rel.into_runtime_string();
    if !rel.is_empty()
        && !rel.starts_with("..")
        && rel.len_utf16() < path.as_runtime_string().len_utf16()
    {
        rel
    } else {
        path.into_runtime_string()
    }
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

fn scalar(value: impl ToString) -> RuntimeString {
    RuntimeString::from(value.to_string())
}

fn is_runtime_blank(value: &RuntimeString) -> bool {
    value.as_utf16().iter().copied().all(|unit| {
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
    })
}

// ---- schema builders ----

fn string_prop(desc: &str) -> SchemaValue {
    let mut value = SchemaMap::new();
    value.insert("type".into(), SchemaValue::String("string".into()));
    value.insert("description".into(), SchemaValue::String(desc.into()));
    SchemaValue::Object(value)
}

fn positive_int_prop(desc: &str) -> SchemaValue {
    let mut value = SchemaMap::new();
    value.insert("type".into(), SchemaValue::String("integer".into()));
    value.insert("exclusiveMinimum".into(), SchemaValue::from(0));
    value.insert(
        "maximum".into(),
        SchemaValue::from(9_007_199_254_740_991_i64),
    );
    value.insert("description".into(), SchemaValue::String(desc.into()));
    SchemaValue::Object(value)
}

fn build_object_schema(props: Vec<(&str, SchemaValue)>, required: &[&str]) -> JsonSchema {
    let mut properties = SchemaMap::new();
    for (k, v) in props {
        properties.insert(k.to_string(), v);
    }
    let mut schema = SchemaMap::new();
    schema.insert("type".to_string(), SchemaValue::String("object".into()));
    schema.insert("properties".to_string(), SchemaValue::Object(properties));
    schema.insert(
        "required".to_string(),
        SchemaValue::Array(
            required
                .iter()
                .map(|value| SchemaValue::String((*value).into()))
                .collect(),
        ),
    );
    schema.insert("additionalProperties".to_string(), SchemaValue::Bool(false));
    schema
}

fn position_schema() -> JsonSchema {
    build_object_schema(
        vec![
            (
                "path",
                string_prop("File path (relative to the working directory or absolute)"),
            ),
            ("line", positive_int_prop("1-based line number")),
            ("character", positive_int_prop("1-based column number")),
        ],
        &["path", "line", "character"],
    )
}

// ---- lsp_diagnostics ----

/// Get language-server diagnostics (errors/warnings) for a file.
pub struct LspDiagnosticsTool {
    input_schema: JsonSchema,
}

impl LspDiagnosticsTool {
    fn new() -> Self {
        LspDiagnosticsTool {
            input_schema: build_object_schema(
                vec![("path", string_prop("File path to get diagnostics for"))],
                &["path"],
            ),
        }
    }
}

pub fn create_lsp_diagnostics_tool() -> Arc<dyn Tool> {
    Arc::new(LspDiagnosticsTool::new())
}

#[async_trait]
impl Tool for LspDiagnosticsTool {
    fn static_name(&self) -> &str {
        "lsp_diagnostics"
    }
    fn static_description(&self) -> &str {
        "Get language-server diagnostics (errors/warnings) for a file."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some("compile errors and warnings for a file")
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let display_path = path.as_runtime_string().clone();
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let diags = match lsp.try_diagnostics(&abs).await {
            Ok(diags) => diags,
            Err(error) => return Ok(query_rejected(error)),
        };
        if diags.is_empty() {
            let text = RuntimeString::from("No diagnostics for ")
                .concat(&display_path)
                .concat(&RuntimeString::from(" (or no language server handles it)."));
            return Ok(text_result(
                exact_json!({
                    "ok": true,
                    "diagnostics": [],
                    "message": "No diagnostics.",
                }),
                text,
                false,
            ));
        }
        let lines: Vec<RuntimeString> = diags
            .iter()
            .map(|d| {
                let source_part = d
                    .source
                    .as_ref()
                    .filter(|source| !source.is_empty())
                    .map(|source| {
                        RuntimeString::from(" [")
                            .concat(source)
                            .concat(&RuntimeString::from("]"))
                    })
                    .unwrap_or_default();
                display_path
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(d.line + 1))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(d.character + 1))
                    .concat(&RuntimeString::from(" "))
                    .concat(&RuntimeString::from(severity_str(d.severity)))
                    .concat(&source_part)
                    .concat(&RuntimeString::from(": "))
                    .concat(&d.message)
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "diagnostics": (diags.clone()),
                "message": (format!("{} diagnostic(s).", diags.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

// ---- lsp_hover ----

/// Get hover info (type/signature/docs) at a position from the language server.
pub struct LspHoverTool {
    input_schema: JsonSchema,
}

impl LspHoverTool {
    fn new() -> Self {
        LspHoverTool {
            input_schema: position_schema(),
        }
    }
}

pub fn create_lsp_hover_tool() -> Arc<dyn Tool> {
    Arc::new(LspHoverTool::new())
}

#[async_trait]
impl Tool for LspHoverTool {
    fn static_name(&self) -> &str {
        "lsp_hover"
    }
    fn static_description(&self) -> &str {
        "Get hover info (type/signature/docs) at a position from the language server."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some("type or documentation at a position")
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let (line, character) = read_position(&input);
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let res = match lsp.try_hover(&abs, line, character).await {
            Ok(result) => result,
            Err(error) => return Ok(query_rejected(error)),
        };
        match res {
            Some(h) if !is_runtime_blank(&h.contents) => {
                let contents = h.contents;
                Ok(text_result(
                    exact_json!({
                        "ok": true,
                        "contents": (contents.clone()),
                        "message": "ok",
                    }),
                    contents,
                    false,
                ))
            }
            _ => Ok(text_result(
                exact_json!({ "ok": true, "message": "No hover info." }),
                "No hover information here.",
                false,
            )),
        }
    }
}

// ---- lsp_definition / lsp_references / lsp_implementation ----

/// Which location query a [`LspLocationTool`] runs.
#[derive(Clone, Copy)]
enum LocationKind {
    Definition,
    References,
    Implementation,
}

/// A go-to-location tool (definition / references / implementation).
pub struct LspLocationTool {
    name: &'static str,
    description: &'static str,
    search_hint: &'static str,
    input_schema: JsonSchema,
    kind: LocationKind,
}

impl LspLocationTool {
    fn new(
        name: &'static str,
        description: &'static str,
        search_hint: &'static str,
        kind: LocationKind,
    ) -> Self {
        LspLocationTool {
            name,
            description,
            search_hint,
            input_schema: position_schema(),
            kind,
        }
    }
}

pub fn create_lsp_definition_tool() -> Arc<dyn Tool> {
    Arc::new(LspLocationTool::new(
        "lsp_definition",
        "Find where the symbol at a position is defined (go to definition).",
        "find where the symbol at a position is defined (go to definition).",
        LocationKind::Definition,
    ))
}

pub fn create_lsp_references_tool() -> Arc<dyn Tool> {
    Arc::new(LspLocationTool::new(
        "lsp_references",
        "Find all references to the symbol at a position.",
        "find all references to the symbol at a position.",
        LocationKind::References,
    ))
}

pub fn create_lsp_implementation_tool() -> Arc<dyn Tool> {
    Arc::new(LspLocationTool::new(
        "lsp_implementation",
        "Find the implementation(s) of the interface/abstract symbol at a position (go to implementation).",
        "find the implementation(s) of the interface/abstract symbol at a position (go to implementation).",
        LocationKind::Implementation,
    ))
}

#[async_trait]
impl Tool for LspLocationTool {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        self.description
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some(self.search_hint)
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let (line, character) = read_position(&input);
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let locs = match self.kind {
            LocationKind::Definition => lsp.try_definition(&abs, line, character).await,
            LocationKind::References => lsp.try_references(&abs, line, character).await,
            LocationKind::Implementation => lsp.try_implementation(&abs, line, character).await,
        };
        let locs = match locs {
            Ok(locations) => locations,
            Err(error) => return Ok(query_rejected(error)),
        };
        if locs.is_empty() {
            return Ok(text_result(
                exact_json!({
                    "ok": true,
                    "locations": [],
                    "message": "No results.",
                }),
                "No results.",
                false,
            ));
        }
        let lines: Vec<RuntimeString> = locs
            .iter()
            .map(|location| {
                location
                    .path
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(location.line + 1))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(location.character + 1))
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "locations": (locs.clone()),
                "message": (format!("{} location(s).", locs.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

// ---- lsp_document_symbols ----

/// List the symbols (functions/classes/etc.) declared in a file.
pub struct LspDocumentSymbolsTool {
    input_schema: JsonSchema,
}

impl LspDocumentSymbolsTool {
    fn new() -> Self {
        LspDocumentSymbolsTool {
            input_schema: build_object_schema(
                vec![("path", string_prop("File path to outline"))],
                &["path"],
            ),
        }
    }
}

pub fn create_lsp_document_symbols_tool() -> Arc<dyn Tool> {
    Arc::new(LspDocumentSymbolsTool::new())
}

#[async_trait]
impl Tool for LspDocumentSymbolsTool {
    fn static_name(&self) -> &str {
        "lsp_document_symbols"
    }
    fn static_description(&self) -> &str {
        "List the symbols (functions/classes/etc.) declared in a file."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some("outline of a file\u{2019}s symbols")
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let syms = match lsp.try_document_symbols(&abs).await {
            Ok(symbols) => symbols,
            Err(error) => return Ok(query_rejected(error)),
        };
        if syms.is_empty() {
            return Ok(text_result(
                exact_json!({
                    "ok": true,
                    "symbols": [],
                    "message": "No symbols.",
                }),
                "No symbols found.",
                false,
            ));
        }
        let lines: Vec<RuntimeString> = syms
            .iter()
            .map(|symbol| {
                let detail = symbol
                    .detail
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .map(|value| RuntimeString::from(" ").concat(value))
                    .unwrap_or_default();
                symbol
                    .kind
                    .concat(&RuntimeString::from(" "))
                    .concat(&symbol.name)
                    .concat(&detail)
                    .concat(&RuntimeString::from(" ("))
                    .concat(&scalar(symbol.line + 1))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(symbol.character + 1))
                    .concat(&RuntimeString::from(")"))
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "symbols": (syms.clone()),
                "message": (format!("{} symbol(s).", syms.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

// ---- lsp_workspace_symbol ----

/// Search the whole workspace for symbols (functions/classes/etc.) by name.
pub struct LspWorkspaceSymbolTool {
    input_schema: JsonSchema,
}

impl LspWorkspaceSymbolTool {
    fn new() -> Self {
        LspWorkspaceSymbolTool {
            input_schema: build_object_schema(
                vec![(
                    "query",
                    string_prop(
                        "Symbol-name query to search the whole workspace for (empty = all symbols).",
                    ),
                )],
                &["query"],
            ),
        }
    }
}

pub fn create_lsp_workspace_symbol_tool() -> Arc<dyn Tool> {
    Arc::new(LspWorkspaceSymbolTool::new())
}

#[async_trait]
impl Tool for LspWorkspaceSymbolTool {
    fn static_name(&self) -> &str {
        "lsp_workspace_symbol"
    }
    fn static_description(&self) -> &str {
        "Search the whole workspace for symbols (functions/classes/etc.) by name."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some("find a symbol by name across the workspace")
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let query = input
            .get("query")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let syms = lsp.workspace_symbol(&query).await;
        if syms.is_empty() {
            return Ok(text_result(
                exact_json!({
                    "ok": true,
                    "symbols": [],
                    "message": "No symbols.",
                }),
                "No symbols found.",
                false,
            ));
        }
        let lines: Vec<RuntimeString> = syms
            .iter()
            .map(|symbol| {
                let where_str = rel_for_cwd(&ctx.cwd, &symbol.path)
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(symbol.line + 1))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(symbol.character + 1));
                let in_what = symbol
                    .container_name
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .map(|value| RuntimeString::from(" in ").concat(value))
                    .unwrap_or_default();
                symbol
                    .kind
                    .concat(&RuntimeString::from(" "))
                    .concat(&symbol.name)
                    .concat(&in_what)
                    .concat(&RuntimeString::from(" ("))
                    .concat(&where_str)
                    .concat(&RuntimeString::from(")"))
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "symbols": (syms.clone()),
                "message": (format!("{} symbol(s).", syms.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

// ---- lsp_prepare_call_hierarchy ----

/// Resolve the call-hierarchy item(s) at a position.
pub struct LspPrepareCallHierarchyTool {
    input_schema: JsonSchema,
}

impl LspPrepareCallHierarchyTool {
    fn new() -> Self {
        LspPrepareCallHierarchyTool {
            input_schema: position_schema(),
        }
    }
}

pub fn create_lsp_prepare_call_hierarchy_tool() -> Arc<dyn Tool> {
    Arc::new(LspPrepareCallHierarchyTool::new())
}

#[async_trait]
impl Tool for LspPrepareCallHierarchyTool {
    fn static_name(&self) -> &str {
        "lsp_prepare_call_hierarchy"
    }
    fn static_description(&self) -> &str {
        "Resolve the call-hierarchy item(s) at a position (the entry point for incoming/outgoing calls)."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some("resolve the call-hierarchy node at a position")
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let (line, character) = read_position(&input);
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let items = match lsp.try_prepare_call_hierarchy(&abs, line, character).await {
            Ok(items) => items,
            Err(error) => return Ok(query_rejected(error)),
        };
        if items.is_empty() {
            return Ok(text_result(
                exact_json!({
                    "ok": true,
                    "items": [],
                    "message": "No call-hierarchy item.",
                }),
                "No call-hierarchy item found at this position.",
                false,
            ));
        }
        let lines: Vec<RuntimeString> = items
            .iter()
            .map(|item| {
                let detail = item
                    .detail
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .map(|value| RuntimeString::from(" ").concat(value))
                    .unwrap_or_default();
                item.kind
                    .concat(&RuntimeString::from(" "))
                    .concat(&item.name)
                    .concat(&detail)
                    .concat(&RuntimeString::from(" ("))
                    .concat(&rel_for_cwd(&ctx.cwd, &item.path))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(item.line + 1))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(item.character + 1))
                    .concat(&RuntimeString::from(")"))
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "items": (items.clone()),
                "message": (format!("{} item(s).", items.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

// ---- lsp_incoming_calls / lsp_outgoing_calls ----

/// Which call-graph direction a [`LspCallsTool`] renders.
#[derive(Clone, Copy)]
enum CallsKind {
    Incoming,
    Outgoing,
}

/// A call-hierarchy edge tool (incoming or outgoing calls), rendering the
/// callee/caller plus its call sites.
pub struct LspCallsTool {
    name: &'static str,
    description: &'static str,
    search_hint: &'static str,
    verb: &'static str,
    input_schema: JsonSchema,
    kind: CallsKind,
}

impl LspCallsTool {
    fn new(
        name: &'static str,
        description: &'static str,
        search_hint: &'static str,
        verb: &'static str,
        kind: CallsKind,
    ) -> Self {
        LspCallsTool {
            name,
            description,
            search_hint,
            verb,
            input_schema: position_schema(),
            kind,
        }
    }
}

pub fn create_lsp_incoming_calls_tool() -> Arc<dyn Tool> {
    Arc::new(LspCallsTool::new(
        "lsp_incoming_calls",
        "List the callers of the function at a position (who calls it, and where).",
        "list the callers of the function at a position (who calls it, and where).",
        "calls",
        CallsKind::Incoming,
    ))
}

pub fn create_lsp_outgoing_calls_tool() -> Arc<dyn Tool> {
    Arc::new(LspCallsTool::new(
        "lsp_outgoing_calls",
        "List the functions the function at a position calls (its callees, and the call sites).",
        "list the functions the function at a position calls (its callees, and the call sites).",
        "called from",
        CallsKind::Outgoing,
    ))
}

#[async_trait]
impl Tool for LspCallsTool {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        self.description
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }
    fn static_search_hint(&self) -> Option<&str> {
        Some(self.search_hint)
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
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let lsp = match &ctx.lsp {
            Some(l) => l,
            None => return Ok(not_enabled()),
        };
        let path = read_path(&input);
        let (line, character) = read_position(&input);
        let abs = resolve_for_cwd(&ctx.cwd, path);
        let calls = match self.kind {
            CallsKind::Incoming => lsp.try_incoming_calls(&abs, line, character).await,
            CallsKind::Outgoing => lsp.try_outgoing_calls(&abs, line, character).await,
        };
        let calls = match calls {
            Ok(calls) => calls,
            Err(error) => return Ok(query_rejected(error)),
        };
        if calls.is_empty() {
            return Ok(text_result(
                exact_json!({ "ok": true, "calls": [], "message": "No calls." }),
                format!("No {} found.", self.verb),
                false,
            ));
        }
        let lines: Vec<RuntimeString> = calls
            .iter()
            .map(|call| {
                let sites = call
                    .from_ranges
                    .iter()
                    .map(|range| {
                        scalar(range.line + 1)
                            .concat(&RuntimeString::from(":"))
                            .concat(&scalar(range.character + 1))
                    })
                    .collect::<Vec<_>>();
                let at = if sites.is_empty() {
                    RuntimeString::default()
                } else {
                    RuntimeString::from(" [")
                        .concat(&RuntimeString::from(self.verb))
                        .concat(&RuntimeString::from(" at: "))
                        .concat(&join_runtime(&sites, ", "))
                        .concat(&RuntimeString::from("]"))
                };
                call.item
                    .kind
                    .concat(&RuntimeString::from(" "))
                    .concat(&call.item.name)
                    .concat(&RuntimeString::from(" ("))
                    .concat(&rel_for_cwd(&ctx.cwd, &call.item.path))
                    .concat(&RuntimeString::from(":"))
                    .concat(&scalar(call.item.line + 1))
                    .concat(&RuntimeString::from(")"))
                    .concat(&at)
            })
            .collect();
        Ok(text_result(
            exact_json!({
                "ok": true,
                "calls": (calls.clone()),
                "message": (format!("{} call(s).", calls.len())),
            }),
            join_runtime(&lines, "\n"),
            false,
        ))
    }
}

/// All LSP tools, included when `config.lsp` is enabled.
pub fn create_lsp_tools() -> Vec<Arc<dyn Tool>> {
    vec![
        create_lsp_diagnostics_tool(),
        create_lsp_hover_tool(),
        create_lsp_definition_tool(),
        create_lsp_references_tool(),
        create_lsp_implementation_tool(),
        create_lsp_document_symbols_tool(),
        create_lsp_workspace_symbol_tool(),
        create_lsp_prepare_call_hierarchy_tool(),
        create_lsp_incoming_calls_tool(),
        create_lsp_outgoing_calls_tool(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::types::{
        LspCall, LspCallHierarchyItem, LspController, LspDiagnostic, LspHoverResult, LspLocation,
        LspPosition, LspSymbol, LspWorkspaceSymbol,
    };
    use crate::runtime_process::RuntimeEnv;
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };

    struct NoopFileState;
    impl FileStateCache for NoopFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(NoopFileState)
        }
    }

    struct NoopSandbox;
    impl SandboxBackend for NoopSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            false
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: RuntimeEnv::new(),
            }
        }
    }

    /// A configurable in-memory language server for the tool tests.
    #[derive(Default)]
    struct MockLsp {
        diagnostics: Vec<LspDiagnostic>,
        hover: Option<LspHoverResult>,
        locations: Vec<LspLocation>,
        doc_symbols: Vec<LspSymbol>,
        workspace_symbols: Vec<LspWorkspaceSymbol>,
        items: Vec<LspCallHierarchyItem>,
        calls: Vec<LspCall>,
        query_error: Option<LspQueryError>,
        seen_paths: Arc<std::sync::Mutex<Vec<RuntimePath>>>,
    }

    impl MockLsp {
        fn checked<T>(&self, value: T) -> crate::lsp::types::LspQueryResult<T> {
            match self.query_error {
                Some(error) => Err(error),
                None => Ok(value),
            }
        }
    }

    #[async_trait]
    impl LspController for MockLsp {
        fn available(&self) -> bool {
            true
        }
        async fn diagnostics(&self, path: &RuntimePath) -> Vec<LspDiagnostic> {
            self.seen_paths.lock().unwrap().push(path.clone());
            self.diagnostics.clone()
        }
        async fn hover(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Option<LspHoverResult> {
            self.hover.clone()
        }
        async fn definition(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspLocation> {
            self.locations.clone()
        }
        async fn references(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspLocation> {
            self.locations.clone()
        }
        async fn implementation(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspLocation> {
            self.locations.clone()
        }
        async fn document_symbols(&self, _path: &RuntimePath) -> Vec<LspSymbol> {
            self.doc_symbols.clone()
        }
        async fn workspace_symbol(&self, _query: &RuntimeString) -> Vec<LspWorkspaceSymbol> {
            self.workspace_symbols.clone()
        }
        async fn prepare_call_hierarchy(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspCallHierarchyItem> {
            self.items.clone()
        }
        async fn incoming_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspCall> {
            self.calls.clone()
        }
        async fn outgoing_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<LspCall> {
            self.calls.clone()
        }
        async fn try_diagnostics(
            &self,
            path: &RuntimePath,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspDiagnostic>> {
            self.seen_paths.lock().unwrap().push(path.clone());
            self.checked(self.diagnostics.clone())
        }
        async fn try_hover(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Option<LspHoverResult>> {
            self.checked(self.hover.clone())
        }
        async fn try_definition(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspLocation>> {
            self.checked(self.locations.clone())
        }
        async fn try_references(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspLocation>> {
            self.checked(self.locations.clone())
        }
        async fn try_implementation(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspLocation>> {
            self.checked(self.locations.clone())
        }
        async fn try_document_symbols(
            &self,
            _path: &RuntimePath,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspSymbol>> {
            self.checked(self.doc_symbols.clone())
        }
        async fn try_prepare_call_hierarchy(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspCallHierarchyItem>> {
            self.checked(self.items.clone())
        }
        async fn try_incoming_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspCall>> {
            self.checked(self.calls.clone())
        }
        async fn try_outgoing_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> crate::lsp::types::LspQueryResult<Vec<LspCall>> {
            self.checked(self.calls.clone())
        }
        async fn begin_document_mutation(
            &self,
            _paths: &[RuntimePath],
        ) -> crate::lsp::types::LspDocumentMutationGuard {
            crate::lsp::types::LspDocumentMutationGuard::empty()
        }
        async fn before_edited(&self, _path: &RuntimePath) {}
        async fn notify_edited(&self, _path: &RuntimePath, _text: Option<&RuntimeString>) {}
        async fn notify_deleted(&self, _path: &RuntimePath) {}
    }

    fn make_ctx(cwd: &str, lsp: Option<Arc<dyn LspController>>) -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::from(cwd),
            signal: AbortSignal::new(),
            file_state: Arc::new(NoopFileState),
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(NoopSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            emit: std::sync::Arc::new(|_| Box::pin(async {})),
            enqueue_background_notice: None,
            record_rollout_event: None,
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: None,
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn diag(
        line: u32,
        character: u32,
        sev: LspSeverity,
        message: &str,
        source: Option<&str>,
    ) -> LspDiagnostic {
        LspDiagnostic {
            line,
            character,
            end_line: None,
            end_character: None,
            severity: sev,
            message: message.into(),
            source: source.map(RuntimeString::from),
            code: None,
        }
    }

    #[test]
    fn create_lsp_tools_lists_ten_in_order() {
        let tools = create_lsp_tools();
        let names: Vec<RuntimeString> = tools.iter().map(|t| t.name().clone()).collect();
        assert_eq!(
            names,
            [
                "lsp_diagnostics",
                "lsp_hover",
                "lsp_definition",
                "lsp_references",
                "lsp_implementation",
                "lsp_document_symbols",
                "lsp_workspace_symbol",
                "lsp_prepare_call_hierarchy",
                "lsp_incoming_calls",
                "lsp_outgoing_calls",
            ]
            .map(RuntimeString::from)
            .to_vec()
        );
    }

    #[test]
    fn tools_are_read_only_and_concurrency_safe() {
        for t in create_lsp_tools() {
            let input = exact_json!({});
            assert!(t.is_read_only(&input), "{:?} should be read-only", t.name());
            assert!(
                t.is_concurrency_safe(&input),
                "{:?} should be concurrency-safe",
                t.name()
            );
            assert_eq!(t.is_destructive(&input), Some(false));
            assert!(t.is_enabled());
            assert!(t.search_hint().is_some());
        }
    }

    #[tokio::test]
    async fn every_file_backed_tool_surfaces_stable_oversize_result() {
        let ctx = make_ctx(
            "/work",
            Some(Arc::new(MockLsp {
                query_error: Some(LspQueryError::FileTooLarge {
                    size_bytes: 10_000_001,
                    max_bytes: 10_000_000,
                }),
                ..Default::default()
            })),
        );
        let mut checked = 0;
        for tool in create_lsp_tools() {
            if tool.name() == "lsp_workspace_symbol" {
                continue;
            }
            let result = tool
                .call(
                    exact_json!({
                        "path": "oversize.ts",
                        "line": 1_u64,
                        "character": 1_u64,
                    }),
                    &ctx,
                    None,
                )
                .await
                .unwrap();
            checked += 1;
            assert_eq!(
                result.serializable.is_error,
                Some(false),
                "{:?}",
                tool.name()
            );
            assert_eq!(
                text_of(&result),
                "File too large for LSP analysis (11MB exceeds 10MB limit)",
                "{:?}",
                tool.name()
            );
            assert_eq!(
                result.serializable.output,
                exact_json!({
                    "ok": false,
                    "code": "file_too_large",
                    "sizeBytes": 10_000_001_u64,
                    "maxBytes": 10_000_000_u64,
                    "message": "File too large for LSP analysis (11MB exceeds 10MB limit)",
                }),
                "{:?}",
                tool.name()
            );
        }
        assert_eq!(checked, 9);
    }

    #[test]
    fn position_schema_carries_integer_bounds() {
        let tool = create_lsp_hover_tool();
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("type").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("object"))
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &exact_json!(["path", "line", "character"])
        );
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&exact_json!(false))
        );
        let line = schema
            .get("properties")
            .and_then(|p| p.get("line"))
            .expect("line prop");
        assert_eq!(
            line.get("type").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("integer"))
        );
        assert_eq!(line.get("exclusiveMinimum"), Some(&exact_json!(0)));
        assert_eq!(
            line.get("maximum"),
            Some(&exact_json!(9_007_199_254_740_991_i64))
        );
        assert_eq!(
            line.get("description").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("1-based line number"))
        );
    }

    #[test]
    fn diagnostics_schema_describes_path() {
        let tool = create_lsp_diagnostics_tool();
        let schema = tool.input_schema();
        let path = schema
            .get("properties")
            .and_then(|p| p.get("path"))
            .expect("path prop");
        assert_eq!(
            path.get("description").and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("File path to get diagnostics for"))
        );
        assert_eq!(schema.get("required").unwrap(), &exact_json!(["path"]));
    }

    #[tokio::test]
    async fn diagnostics_reports_when_not_enabled() {
        let ctx = make_ctx("/work", None);
        let tool = create_lsp_diagnostics_tool();
        let res = tool
            .call(exact_json!({ "path": "a.ts" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Language-server support is not enabled for this session."
        );
        assert_eq!(
            res.serializable.output,
            exact_json!({ "ok": false, "message": "Language-server support is not enabled for this session." })
        );
    }

    #[tokio::test]
    async fn diagnostics_empty_notes_no_diagnostics() {
        let ctx = make_ctx("/work", Some(Arc::new(MockLsp::default())));
        let tool = create_lsp_diagnostics_tool();
        let res = tool
            .call(exact_json!({ "path": "foo.ts" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "No diagnostics for foo.ts (or no language server handles it)."
        );
        assert_eq!(
            res.serializable.output,
            exact_json!({ "ok": true, "diagnostics": [], "message": "No diagnostics." })
        );
    }

    #[tokio::test]
    async fn diagnostics_formats_each_line() {
        let lsp = MockLsp {
            diagnostics: vec![
                diag(2, 4, LspSeverity::Error, "bad", Some("tsserver")),
                diag(0, 0, LspSeverity::Warning, "meh", None),
            ],
            ..Default::default()
        };
        let ctx = make_ctx("/work", Some(Arc::new(lsp)));
        let tool = create_lsp_diagnostics_tool();
        let res = tool
            .call(exact_json!({ "path": "foo.ts" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "foo.ts:3:5 error [tsserver]: bad\nfoo.ts:1:1 warning: meh"
        );
        assert_eq!(
            res.serializable.output.get("message").unwrap(),
            &exact_json!("2 diagnostic(s).")
        );
    }

    #[tokio::test]
    async fn diagnostics_preserves_exact_utf16_input_output_and_model_text() {
        let cases = [
            RuntimeString::from_utf16(vec![0xd800]),
            RuntimeString::from_utf16(vec![0xdc00]),
            RuntimeString::from_utf16(vec![0xfffd]),
        ];
        let mut wires = Vec::new();

        for marker in cases {
            let path_text = RuntimeString::from("src/")
                .concat(&marker)
                .concat(&RuntimeString::from(".ts"));
            let expected_path = resolve_for_cwd(
                &RuntimePath::from("/work"),
                RuntimePath::from(path_text.clone()),
            );
            let seen_paths = Arc::new(std::sync::Mutex::new(Vec::new()));
            let lsp = MockLsp {
                diagnostics: vec![LspDiagnostic {
                    line: 0,
                    character: 0,
                    end_line: None,
                    end_character: None,
                    severity: LspSeverity::Error,
                    message: marker.clone(),
                    source: Some(marker.clone()),
                    code: Some(marker.clone()),
                }],
                seen_paths: seen_paths.clone(),
                ..Default::default()
            };
            let ctx = make_ctx("/work", Some(Arc::new(lsp)));
            let result = create_lsp_diagnostics_tool()
                .call(exact_json!({ "path": (path_text) }), &ctx, None)
                .await
                .unwrap();

            assert_eq!(seen_paths.lock().unwrap().as_slice(), &[expected_path]);
            assert_eq!(
                result
                    .serializable
                    .output
                    .pointer("/diagnostics/0/message")
                    .and_then(RuntimeJson::as_string),
                Some(&marker)
            );
            assert!(text_of(&result)
                .as_utf16()
                .windows(marker.len_utf16())
                .any(|window| window == marker.as_utf16()));
            wires.push(crate::runtime_values::runtime_json::stringify(
                &result.serializable.output,
            ));
        }

        assert_ne!(wires[0], wires[1]);
        assert_ne!(wires[0], wires[2]);
        assert_ne!(wires[1], wires[2]);
    }

    #[tokio::test]
    async fn hover_none_and_some() {
        let empty = make_ctx("/work", Some(Arc::new(MockLsp::default())));
        let tool = create_lsp_hover_tool();
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &empty,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "No hover information here.");
        assert_eq!(
            res.serializable.output,
            exact_json!({ "ok": true, "message": "No hover info." })
        );

        let lsp = MockLsp {
            hover: Some(LspHoverResult {
                contents: "fn foo(): void".into(),
            }),
            ..Default::default()
        };
        let ctx = make_ctx("/work", Some(Arc::new(lsp)));
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 3, "character": 2 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "fn foo(): void");
        assert_eq!(
            res.serializable.output,
            exact_json!({ "ok": true, "contents": "fn foo(): void", "message": "ok" })
        );
    }

    #[tokio::test]
    async fn hover_blank_contents_treated_as_missing() {
        let lsp = MockLsp {
            hover: Some(LspHoverResult {
                contents: "   \n".into(),
            }),
            ..Default::default()
        };
        let ctx = make_ctx("/work", Some(Arc::new(lsp)));
        let tool = create_lsp_hover_tool();
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "No hover information here.");
    }

    #[tokio::test]
    async fn hover_blank_uses_ecmascript_whitespace() {
        let ecmascript_blank = MockLsp {
            hover: Some(LspHoverResult {
                contents: RuntimeString::from_utf16(vec![0xfeff, 0x2028]),
            }),
            ..Default::default()
        };
        let blank_ctx = make_ctx("/work", Some(Arc::new(ecmascript_blank)));
        let tool = create_lsp_hover_tool();
        let blank = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &blank_ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&blank), "No hover information here.");

        let non_ecmascript_whitespace = RuntimeString::from_utf16(vec![0x0085]);
        let visible = MockLsp {
            hover: Some(LspHoverResult {
                contents: non_ecmascript_whitespace.clone(),
            }),
            ..Default::default()
        };
        let visible_ctx = make_ctx("/work", Some(Arc::new(visible)));
        let result = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &visible_ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&result), non_ecmascript_whitespace);
    }

    #[tokio::test]
    async fn definition_empty_and_lists() {
        let tool = create_lsp_definition_tool();
        let empty = make_ctx("/work", Some(Arc::new(MockLsp::default())));
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &empty,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "No results.");
        assert_eq!(
            res.serializable.output,
            exact_json!({ "ok": true, "locations": [], "message": "No results." })
        );

        let lsp = MockLsp {
            locations: vec![LspLocation {
                path: "/proj/x.ts".into(),
                line: 1,
                character: 2,
                end_line: None,
                end_character: None,
            }],
            ..Default::default()
        };
        let ctx = make_ctx("/proj", Some(Arc::new(lsp)));
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "/proj/x.ts:2:3");
        assert_eq!(
            res.serializable.output.get("message").unwrap(),
            &exact_json!("1 location(s).")
        );
    }

    #[tokio::test]
    async fn workspace_symbol_uses_rel_for_cwd() {
        let lsp = MockLsp {
            workspace_symbols: vec![LspWorkspaceSymbol {
                name: "foo".into(),
                kind: "Function".into(),
                path: "/proj/src/a.ts".into(),
                line: 0,
                character: 0,
                container_name: Some("Bar".into()),
            }],
            ..Default::default()
        };
        let ctx = make_ctx("/proj", Some(Arc::new(lsp)));
        let tool = create_lsp_workspace_symbol_tool();
        let res = tool
            .call(exact_json!({ "query": "foo" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Function foo in Bar (src/a.ts:1:1)");
    }

    #[tokio::test]
    async fn document_symbols_lists_with_detail() {
        let lsp = MockLsp {
            doc_symbols: vec![LspSymbol {
                name: "foo".into(),
                kind: "Function".into(),
                line: 1,
                character: 2,
                detail: Some("(): void".into()),
            }],
            ..Default::default()
        };
        let ctx = make_ctx("/proj", Some(Arc::new(lsp)));
        let tool = create_lsp_document_symbols_tool();
        let res = tool
            .call(exact_json!({ "path": "a.ts" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Function foo (): void (2:3)");
    }

    #[tokio::test]
    async fn prepare_call_hierarchy_lists() {
        let lsp = MockLsp {
            items: vec![LspCallHierarchyItem {
                name: "baz".into(),
                kind: "Function".into(),
                path: "/proj/a.ts".into(),
                line: 2,
                character: 1,
                detail: Some("sig".into()),
            }],
            ..Default::default()
        };
        let ctx = make_ctx("/proj", Some(Arc::new(lsp)));
        let tool = create_lsp_prepare_call_hierarchy_tool();
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "Function baz sig (a.ts:3:2)");
    }

    #[tokio::test]
    async fn incoming_calls_render_sites() {
        let lsp = MockLsp {
            calls: vec![LspCall {
                item: LspCallHierarchyItem {
                    name: "bar".into(),
                    kind: "Function".into(),
                    path: "/proj/a.ts".into(),
                    line: 4,
                    character: 0,
                    detail: None,
                },
                from_ranges: vec![
                    LspPosition {
                        line: 9,
                        character: 2,
                    },
                    LspPosition {
                        line: 11,
                        character: 0,
                    },
                ],
            }],
            ..Default::default()
        };
        let ctx = make_ctx("/proj", Some(Arc::new(lsp)));
        let tool = create_lsp_incoming_calls_tool();
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            text_of(&res),
            "Function bar (a.ts:5) [calls at: 10:3, 12:1]"
        );
    }

    #[tokio::test]
    async fn outgoing_calls_empty_message() {
        let ctx = make_ctx("/proj", Some(Arc::new(MockLsp::default())));
        let tool = create_lsp_outgoing_calls_tool();
        let res = tool
            .call(
                exact_json!({ "path": "a.ts", "line": 1, "character": 1 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(text_of(&res), "No called from found.");
    }

    #[test]
    fn relative_matches_node_posix() {
        let cwd = RuntimePath::from("/");
        let relative_from = |from: &str, to: &str| {
            relative(&cwd, &RuntimePath::from(from), &RuntimePath::from(to)).into_runtime_string()
        };
        assert_eq!(relative_from("/a/b", "/a/b/c"), "c");
        assert_eq!(relative_from("/a/b/c", "/a/b"), "..");
        assert_eq!(relative_from("/a/b", "/a/c"), "../c");
        assert_eq!(relative_from("/a/b", "/a/b"), "");
        assert_eq!(relative_from("/", "/foo"), "foo");
        // Two rooted paths that share no first segment: Node's win32 relative hands back
        // the target itself (they may sit on different drives) where POSIX climbs out with
        // `..`. This helper follows the host dialect, so both answers are pinned.
        let unrelated = relative_from("/home/u/proj", "/etc/passwd");
        if cfg!(windows) {
            assert_eq!(unrelated, "/etc/passwd");
        } else {
            assert_eq!(unrelated, "../../../etc/passwd");
        }
    }

    #[test]
    fn rel_for_cwd_prefers_shorter_inside_tree() {
        let cwd = RuntimePath::from("/home/u/proj");
        assert_eq!(
            rel_for_cwd(&cwd, &RuntimeString::from("/home/u/proj/src/x.ts")),
            "src/x.ts"
        );
        // Outside the tree: keep the absolute path.
        assert_eq!(
            rel_for_cwd(&cwd, &RuntimeString::from("/etc/passwd")),
            "/etc/passwd"
        );
    }
}
