//! Engine-facing LSP types.
//!
//! These are the SIMPLIFIED, engine-internal shapes the tools and reminder
//! surface (positions are 0-based, matching LSP).

use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};

/// Opaque exclusive lease spanning a real filesystem mutation and its
/// subsequent LSP close/synchronization. Dropping it on an error releases
/// queries; their normal disk revalidation then observes whatever state the
/// failed operation actually left behind.
pub struct LspDocumentMutationGuard {
    _guards: Vec<OwnedRwLockWriteGuard<()>>,
}

impl LspDocumentMutationGuard {
    pub(crate) fn new(guards: Vec<OwnedRwLockWriteGuard<()>>) -> Self {
        Self { _guards: guards }
    }

    pub(crate) fn empty() -> Self {
        Self::new(Vec::new())
    }
}

/// A stable, user-actionable eligibility failure for a file-backed LSP query.
/// Transport/server failures remain fail-soft empty results; these failures are
/// kept typed so tools cannot misreport a rejected document as "no results" or
/// as a deleted/missing file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LspQueryError {
    FileTooLarge { size_bytes: u64, max_bytes: u64 },
    UnsupportedFileType,
}

pub type LspQueryResult<T> = Result<T, LspQueryError>;

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

/// A host-supplied language server: how to start it + which files it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspServerConfig {
    /// Logical language id (sent as `languageId` on didOpen).
    pub language: RuntimeString,
    /// File extensions this server handles, e.g. ['.ts', '.tsx']. Leading dot
    /// optional.
    pub extensions: Vec<RuntimeString>,
    /// The server executable.
    pub command: RuntimeString,
    /// Arguments to the server.
    pub args: Option<Vec<RuntimeString>>,
    /// Workspace root (defaults to the session cwd).
    pub root_dir: Option<RuntimeString>,
}

impl RuntimeEncode for LspServerConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("language", &self.language)?;
        object.field("extensions", &self.extensions)?;
        object.field("command", &self.command)?;
        if let Some(value) = &self.args {
            object.field("args", value)?;
        }
        if let Some(value) = &self.root_dir {
            object.field("rootDir", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspServerConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                language: object.required("language", &[])?,
                extensions: object.required("extensions", &[])?,
                command: object.required("command", &[])?,
                args: object.optional("args", &[])?,
                root_dir: object.optional("rootDir", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LspSeverity {
    Error,
    Warning,
    Information,
    Hint,
}

impl RuntimeEncode for LspSeverity {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Error => "error",
            Self::Warning => "warning",
            Self::Information => "information",
            Self::Hint => "hint",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for LspSeverity {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("error", &[]),
                RuntimeEnumVariant::new("warning", &[]),
                RuntimeEnumVariant::new("information", &[]),
                RuntimeEnumVariant::new("hint", &[]),
            ],
            &[Self::Error, Self::Warning, Self::Information, Self::Hint],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspDiagnostic {
    /// 0-based line of the range start.
    pub line: u32,
    /// 0-based character of the range start.
    pub character: u32,
    /// 0-based line of the range end.
    pub end_line: Option<u32>,
    /// 0-based character of the range end.
    pub end_character: Option<u32>,
    pub severity: LspSeverity,
    pub message: RuntimeString,
    pub source: Option<RuntimeString>,
    pub code: Option<RuntimeString>,
}

impl RuntimeEncode for LspDiagnostic {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("line", &self.line)?;
        object.field("character", &self.character)?;
        if let Some(value) = self.end_line {
            object.field("endLine", &value)?;
        }
        if let Some(value) = self.end_character {
            object.field("endCharacter", &value)?;
        }
        object.field("severity", &self.severity)?;
        object.field("message", &self.message)?;
        if let Some(value) = &self.source {
            object.field("source", value)?;
        }
        if let Some(value) = &self.code {
            object.field("code", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspDiagnostic {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
                end_line: object.optional("endLine", &[])?,
                end_character: object.optional("endCharacter", &[])?,
                severity: object.required("severity", &[])?,
                message: object.required("message", &[])?,
                source: object.optional("source", &[])?,
                code: object.optional("code", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspLocation {
    pub path: RuntimeString,
    pub line: u32,
    pub character: u32,
    pub end_line: Option<u32>,
    pub end_character: Option<u32>,
}

impl RuntimeEncode for LspLocation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("path", &self.path)?;
        object.field("line", &self.line)?;
        object.field("character", &self.character)?;
        if let Some(value) = self.end_line {
            object.field("endLine", &value)?;
        }
        if let Some(value) = self.end_character {
            object.field("endCharacter", &value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspLocation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                path: object.required("path", &[])?,
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
                end_line: object.optional("endLine", &[])?,
                end_character: object.optional("endCharacter", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspSymbol {
    pub name: RuntimeString,
    pub kind: RuntimeString,
    pub line: u32,
    pub character: u32,
    pub detail: Option<RuntimeString>,
}

impl RuntimeEncode for LspSymbol {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("kind", &self.kind)?;
        object.field("line", &self.line)?;
        object.field("character", &self.character)?;
        if let Some(value) = &self.detail {
            object.field("detail", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspSymbol {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                kind: object.required("kind", &[])?,
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
                detail: object.optional("detail", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspHoverResult {
    pub contents: RuntimeString,
}

impl RuntimeEncode for LspHoverResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("contents", &self.contents)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspHoverResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                contents: object.required("contents", &[])?,
            })
        })
    }
}

/// A workspace-wide symbol (workspace/symbol) — unlike a document symbol it
/// carries its file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspWorkspaceSymbol {
    pub name: RuntimeString,
    pub kind: RuntimeString,
    pub path: RuntimeString,
    pub line: u32,
    pub character: u32,
    /// The enclosing symbol name the server reported, if any.
    pub container_name: Option<RuntimeString>,
}

impl RuntimeEncode for LspWorkspaceSymbol {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("kind", &self.kind)?;
        object.field("path", &self.path)?;
        object.field("line", &self.line)?;
        object.field("character", &self.character)?;
        if let Some(value) = &self.container_name {
            object.field("containerName", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspWorkspaceSymbol {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                kind: object.required("kind", &[])?,
                path: object.required("path", &[])?,
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
                container_name: object.optional("containerName", &[])?,
            })
        })
    }
}

/// A call-hierarchy node (the function/method a call points to or from).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspCallHierarchyItem {
    pub name: RuntimeString,
    pub kind: RuntimeString,
    pub path: RuntimeString,
    pub line: u32,
    pub character: u32,
    pub detail: Option<RuntimeString>,
}

impl RuntimeEncode for LspCallHierarchyItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("kind", &self.kind)?;
        object.field("path", &self.path)?;
        object.field("line", &self.line)?;
        object.field("character", &self.character)?;
        if let Some(value) = &self.detail {
            object.field("detail", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspCallHierarchyItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                kind: object.required("kind", &[])?,
                path: object.required("path", &[])?,
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
                detail: object.optional("detail", &[])?,
            })
        })
    }
}

/// A 0-based source position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LspPosition {
    pub line: u32,
    pub character: u32,
}

impl RuntimeEncode for LspPosition {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { line, character } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("line", line)?;
        object.field("character", character)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspPosition {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                line: object.required("line", &[])?,
                character: object.required("character", &[])?,
            })
        })
    }
}

/// One edge of the call graph: the other end + the call-site positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspCall {
    /// The caller (incoming) or callee (outgoing).
    pub item: LspCallHierarchyItem,
    /// Call-site start positions within the queried function.
    pub from_ranges: Vec<LspPosition>,
}

impl RuntimeEncode for LspCall {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("item", &self.item)?;
        object.field("fromRanges", &self.from_ranges)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for LspCall {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                item: object.required("item", &[])?,
                from_ranges: object.required("fromRanges", &[])?,
            })
        })
    }
}

/// The `ctx.lsp` seam (present only with `config.lsp`). Transport/server errors,
/// timeouts, and no-matching-server remain fail-soft empty/`None` results. The
/// `try_*` file-backed variants additionally preserve stable document
/// eligibility failures (for example the 10 MB limit) for user-facing tools;
/// their defaults keep third-party/test controllers source-compatible.
#[async_trait]
pub trait LspController: Send + Sync {
    /// True when at least one server is configured (so tools can explain when
    /// not).
    fn available(&self) -> bool;
    async fn diagnostics(&self, path: &RuntimePath) -> Vec<LspDiagnostic>;
    async fn try_diagnostics(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspDiagnostic>> {
        Ok(self.diagnostics(path).await)
    }
    async fn hover(&self, path: &RuntimePath, line: u32, character: u32) -> Option<LspHoverResult>;
    async fn try_hover(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Option<LspHoverResult>> {
        Ok(self.hover(path, line, character).await)
    }
    async fn definition(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspLocation>;
    async fn try_definition(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        Ok(self.definition(path, line, character).await)
    }
    async fn references(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspLocation>;
    async fn try_references(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        Ok(self.references(path, line, character).await)
    }
    /// Find the implementation(s) of the symbol at a position.
    async fn implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> Vec<LspLocation>;
    async fn try_implementation(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspLocation>> {
        Ok(self.implementation(path, line, character).await)
    }
    async fn document_symbols(&self, path: &RuntimePath) -> Vec<LspSymbol>;
    async fn try_document_symbols(&self, path: &RuntimePath) -> LspQueryResult<Vec<LspSymbol>> {
        Ok(self.document_symbols(path).await)
    }
    /// Search workspace symbols by name. Empty query = all symbols.
    async fn workspace_symbol(&self, query: &RuntimeString) -> Vec<LspWorkspaceSymbol>;
    /// Resolve the call-hierarchy item(s) at a position.
    async fn prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> Vec<LspCallHierarchyItem>;
    async fn try_prepare_call_hierarchy(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCallHierarchyItem>> {
        Ok(self.prepare_call_hierarchy(path, line, character).await)
    }
    /// Callers of the function at a position.
    async fn incoming_calls(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspCall>;
    async fn try_incoming_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        Ok(self.incoming_calls(path, line, character).await)
    }
    /// Callees of the function at a position.
    async fn outgoing_calls(&self, path: &RuntimePath, line: u32, character: u32) -> Vec<LspCall>;
    async fn try_outgoing_calls(
        &self,
        path: &RuntimePath,
        line: u32,
        character: u32,
    ) -> LspQueryResult<Vec<LspCall>> {
        Ok(self.outgoing_calls(path, line, character).await)
    }
    /// Acquire an exclusive, canonical per-document mutation boundary for all
    /// paths. Callers hold the returned guard from immediately before the first
    /// filesystem mutation through the matching notify_edited/notify_deleted.
    async fn begin_document_mutation(&self, paths: &[RuntimePath]) -> LspDocumentMutationGuard;
    /// Snapshot a file's PRE-edit diagnostics baseline. An edit tool MUST call
    /// this immediately BEFORE it writes the new content. No-op if already
    /// snapshotted this turn.
    async fn before_edited(&self, path: &RuntimePath);
    /// Tell the LSP a file was just edited (didOpen/didChange); `text` avoids a
    /// disk re-read.
    async fn notify_edited(&self, path: &RuntimePath, text: Option<&RuntimeString>);
    /// Tell the LSP a file was deleted. Implementations close the document only
    /// when that exact URI is currently open; repeated deletes are a no-op.
    async fn notify_deleted(&self, path: &RuntimePath);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use std::fmt::Debug;

    fn assert_exact_round_trip<T>(value: &T) -> RuntimeJson
    where
        T: RuntimeEncode + RuntimeDecode + Clone + Debug + Eq,
    {
        let encoded = runtime_codec::encode_value(value).unwrap();
        let decoded = runtime_codec::decode_value::<T>(&encoded).unwrap();
        assert_eq!(&decoded, value);
        encoded
    }

    #[test]
    fn lsp_dtos_preserve_high_low_and_replacement_code_units() {
        let cases = [
            RuntimeString::from_utf16(vec![0xd800]),
            RuntimeString::from_utf16(vec![0xdc00]),
            RuntimeString::from_utf16(vec![0xfffd]),
        ];
        let mut diagnostic_wires = Vec::new();
        let mut call_wires = Vec::new();

        for marker in cases {
            let config = LspServerConfig {
                language: marker.clone(),
                extensions: vec![marker.clone()],
                command: marker.clone(),
                args: Some(vec![marker.clone()]),
                root_dir: Some(marker.clone()),
            };
            assert_exact_round_trip(&config);

            let diagnostic = LspDiagnostic {
                line: 1,
                character: 2,
                end_line: Some(3),
                end_character: Some(4),
                severity: LspSeverity::Warning,
                message: marker.clone(),
                source: Some(marker.clone()),
                code: Some(marker.clone()),
            };
            let diagnostic_json = assert_exact_round_trip(&diagnostic);
            assert_eq!(
                diagnostic_json
                    .get("message")
                    .and_then(RuntimeJson::as_string),
                Some(&marker)
            );
            diagnostic_wires.push(runtime_json::stringify(&diagnostic_json));

            assert_exact_round_trip(&LspLocation {
                path: marker.clone(),
                line: 1,
                character: 2,
                end_line: Some(3),
                end_character: Some(4),
            });
            assert_exact_round_trip(&LspSymbol {
                name: marker.clone(),
                kind: marker.clone(),
                line: 1,
                character: 2,
                detail: Some(marker.clone()),
            });
            assert_exact_round_trip(&LspHoverResult {
                contents: marker.clone(),
            });
            assert_exact_round_trip(&LspWorkspaceSymbol {
                name: marker.clone(),
                kind: marker.clone(),
                path: marker.clone(),
                line: 1,
                character: 2,
                container_name: Some(marker.clone()),
            });

            let item = LspCallHierarchyItem {
                name: marker.clone(),
                kind: marker.clone(),
                path: marker.clone(),
                line: 1,
                character: 2,
                detail: Some(marker.clone()),
            };
            assert_exact_round_trip(&item);
            assert_exact_round_trip(&LspPosition {
                line: 3,
                character: 4,
            });
            let call = LspCall {
                item,
                from_ranges: vec![LspPosition {
                    line: 3,
                    character: 4,
                }],
            };
            let call_json = assert_exact_round_trip(&call);
            assert_eq!(
                call_json
                    .pointer("/item/path")
                    .and_then(RuntimeJson::as_string),
                Some(&marker)
            );
            call_wires.push(runtime_json::stringify(&call_json));
        }

        for wires in [&diagnostic_wires, &call_wires] {
            assert_ne!(wires[0], wires[1]);
            assert_ne!(wires[0], wires[2]);
            assert_ne!(wires[1], wires[2]);
        }
    }

    #[test]
    fn lsp_codec_keeps_serde_field_order_omission_and_unknown_field_policy() {
        let config = LspServerConfig {
            language: "typescript".into(),
            extensions: vec![".ts".into()],
            command: "typescript-language-server".into(),
            args: None,
            root_dir: None,
        };
        let config_json = runtime_codec::encode_value(&config).unwrap();
        assert_eq!(
            runtime_json::stringify(&config_json),
            r#"{"language":"typescript","extensions":[".ts"],"command":"typescript-language-server"}"#
        );

        let with_nulls_and_unknown = runtime_json::parse(&RuntimeString::from(
            r#"{"language":"x","extensions":[],"command":"c","args":null,"rootDir":null,"\ud800":1}"#,
        ))
        .unwrap();
        assert_eq!(
            runtime_codec::decode_value::<LspServerConfig>(&with_nulls_and_unknown).unwrap(),
            LspServerConfig {
                language: "x".into(),
                extensions: Vec::new(),
                command: "c".into(),
                args: None,
                root_dir: None,
            }
        );

        assert_eq!(
            runtime_json::stringify(
                &runtime_codec::encode_value(&LspSeverity::Information).unwrap()
            ),
            r#""information""#
        );
    }
}
