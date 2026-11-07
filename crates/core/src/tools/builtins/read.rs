//! The `read` [`Tool`]: read a file for the model. Text files come back with
//! line numbers (honoring `offset`/`limit`); recognized image types come back as
//! a base64 image block; PDFs are delivered either as a native `document` block
//! or as rasterized page images; other binary content is refused. Records the
//! file's mtime + size (and, on a complete text read, its raw content) for
//! stale-write detection and external-edit diffing.

use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use serde_json::json;

use crate::fs::node_path::{
    basename_runtime, posix_rendered_runtime, project_runtime_path, resolve_runtime,
};
use crate::fs::pdf::{
    extract_pdf_pages, get_pdf_page_count, parse_pdf_page_range, read_pdf,
    PDF_INLINE_PAGE_THRESHOLD, PDF_MAX_PAGES_PER_READ,
};
use crate::fs::text_file::{decode_text_buffer, Encoding as FsEncoding};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::schemas::content::{
    ContentBlock, DocumentBlock, HarnessEnvelope, ImageBlock, OutputBlock, TextBlock,
};
use crate::tools::build_tool::{text_result, text_result_with_envelope};
use crate::types::errors::ToolError;
use crate::types::file_state::{Encoding as StateEncoding, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
    ToolResultSerializable,
};

/// Hard cap on total file size for a text read (256 KB default). A file larger
/// than this is REFUSED (an error result), not truncated. Applies to every text
/// read, full or ranged: `offset`/`limit` do not bypass it — the cap gates on
/// total file size for a limit-less read.
const DEFAULT_MAX_BYTES: u64 = 256 * 1024;
/// Cap on the estimated TOKENS of a text read's output (25 000 default). A read
/// whose numbered output would exceed this is refused with guidance to narrow it
/// via `offset`/`limit`.
const DEFAULT_MAX_TOKENS: usize = 25_000;
/// Cap for image files read as base64 blocks (5 MB default).
const DEFAULT_MAX_IMAGE_BYTES: u64 = 5 * 1024 * 1024;

/// The static, model-facing description (becomes the ToolSpec description).
const READ_DESCRIPTION: &str = "Read a file. Text files return contents prefixed with line numbers; image files (png/jpg/jpeg/gif/webp) return an image. Small PDFs can be delivered natively when the model supports PDFs; use pages with a bounded, 1-based range to render selected PDF pages as images when needed (requires vision). Large PDFs require selected page ranges. Other binary formats are refused. For a large text file, read only the range you need with offset/limit rather than the whole file. Avoid redundant reads solely to confirm that a successful edit/write call completed; inspect the current contents when needed to validate the requested behavior, resolve uncertainty, or account for concurrent changes.";
const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &["path", "offset", "limit", "pages"];

/// Options controlling the read tool's size/token caps.
#[derive(Debug, Clone, Default)]
pub struct ReadToolOptions {
    pub max_bytes: Option<u64>,
    pub max_tokens: Option<usize>,
    pub max_image_bytes: Option<u64>,
}

/// The `read` tool.
pub struct ReadTool {
    input_schema: JsonSchema,
    max_bytes: u64,
    max_tokens: usize,
    max_image_bytes: u64,
}

impl ReadTool {
    fn new(opts: ReadToolOptions) -> Self {
        ReadTool {
            input_schema: build_input_schema(),
            max_bytes: opts.max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
            max_tokens: opts.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            max_image_bytes: opts.max_image_bytes.unwrap_or(DEFAULT_MAX_IMAGE_BYTES),
        }
    }
}

/// Construct the `read` tool with default caps.
pub fn create_read_tool() -> Arc<dyn Tool> {
    Arc::new(ReadTool::new(ReadToolOptions::default()))
}

/// Construct the `read` tool with custom caps.
pub fn create_read_tool_with_options(opts: ReadToolOptions) -> Arc<dyn Tool> {
    Arc::new(ReadTool::new(opts))
}

/// Build the tool's input JSON Schema (the exact normalized shape the model sees).
fn build_input_schema() -> JsonSchema {
    let pages_desc = format!(
        "PDF only: bounded page range to render as images, e.g. \"5\" or \"1-10\". Use 1-based page numbers and request at most {PDF_MAX_PAGES_PER_READ} pages. Open-ended ranges are not supported. Omit pages to read a small PDF natively when the current model supports it."
    );
    let schema = json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or cwd-relative file path"
            },
            "offset": {
                "description": "1-based start line (text only)",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9007199254740991_i64
            },
            "limit": {
                "description": "Max number of lines to read (text only)",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9007199254740991_i64
            },
            "pages": {
                "description": pages_desc,
                "type": "string"
            }
        },
        "required": ["path"],
        "additionalProperties": false
    });
    schema.as_object().cloned().unwrap_or_default()
}

#[async_trait]
impl Tool for ReadTool {
    fn static_name(&self) -> &str {
        "read"
    }

    fn static_description(&self) -> &str {
        READ_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(INPUT_SCHEMA_PROPERTY_ORDER)
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

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Reading ").concat(&path))
    }

    /// Read-class: the operand path is matched against the permission engine's
    /// read rules.
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Read)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(PathTargets::One(RuntimePath::from(path)))
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let offset = input.get("offset").and_then(json_positive_integer);
        let limit = input.get("limit").and_then(json_positive_integer);
        let pages = input.get("pages").and_then(RuntimeJson::as_string).cloned();

        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path.clone())])
            .map_err(|error| ToolError::new(runtime_to_host_summary(error.message())))?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ToolError::new(runtime_to_host_summary(error.message())))?;

        // Touching this path may activate a conditional skill whose `paths` glob
        // matches.
        if let Some(activate) = &ctx.activate_skill_paths {
            activate(vec![abs.clone()]);
        }

        let goal_snapshot = match &ctx.goal_owned_resources {
            Some(resources) => resources
                .snapshot_read(&abs)
                .await
                .map_err(|error| ToolError::new(format!("goal file read failed: {error}")))?,
            None => None,
        };
        let os_abs = goal_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path().to_path_buf())
            .unwrap_or(os_abs);

        // Restricted memory forks never reopen the admitted root by pathname.
        // A private, metadata-preserving snapshot also keeps PDF helper-process
        // behavior intact without inheriting the pinned directory handle.
        let pinned_snapshot = match ctx
            .permission
            .pinned_memory_root
            .as_ref()
            .filter(|_| goal_snapshot.is_none())
        {
            Some(root) => match root.snapshot_file(&os_abs) {
                Ok(snapshot) => Some(snapshot),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(text_result(
                        read_output(&abs, false, None, []),
                        RuntimeString::from("File not found: ").concat(&path),
                        false,
                    ));
                }
                Err(error) => {
                    return Err(ToolError::new(format!(
                        "pinned memory read failed: {error}"
                    )))
                }
            },
            None => None,
        };
        let os_abs = pinned_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path().to_path_buf())
            .unwrap_or(os_abs);

        let st = match tokio::fs::metadata(&os_abs).await {
            Ok(m) if m.is_file() => m,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(text_result(
                    read_output(&abs, false, None, []),
                    RuntimeString::from("File not found: ").concat(&path),
                    false,
                ));
            }
            Ok(_) => {
                return Ok(text_result(
                    read_output(&abs, false, None, []),
                    RuntimeString::from("Not a file: ").concat(&path),
                    true,
                ));
            }
            Err(error) => {
                return Err(ToolError::new(format!("read metadata failed: {error}")));
            }
        };

        let mtime_ms = stat_mtime_ms(&st);
        let size = st.len();

        // Baseline mtime + size now (every kind); a complete text read also
        // records the raw content below, enabling external-edit diffing.
        ctx.file_state.record(
            &abs,
            mtime_ms,
            RecordOptions {
                size: Some(size),
                ..Default::default()
            },
        );

        let ext = ascii_lower_runtime(&extname_runtime(abs.as_runtime_string()));

        // Image path: recognized extension → return a base64 image block.
        if let Some(mime_type) = image_mime(&ext) {
            if ctx.vision == Some(false) {
                return Ok(text_result(
                    read_output(&abs, true, Some("image"), []),
                    RuntimeString::from(
                        "This model cannot understand images, so the image was not loaded. Tell the user to switch to a model with vision support and try again.",
                    ),
                    true,
                ));
            }
            if size > self.max_image_bytes {
                return Ok(text_result(
                    read_output(
                        &abs,
                        true,
                        Some("image"),
                        [("mimeType", RuntimeJson::from(mime_type))],
                    ),
                    concat_runtime(&[
                        RuntimeString::from("Image is too large to read ("),
                        RuntimeString::from(size.to_string()),
                        RuntimeString::from(" bytes > "),
                        RuntimeString::from(self.max_image_bytes.to_string()),
                        RuntimeString::from(" byte limit): "),
                        path.clone(),
                    ]),
                    true,
                ));
            }
            let buf = tokio::fs::read(&os_abs)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let b64 = BASE64_STANDARD.encode(&buf);
            return Ok(image_result(
                read_output(
                    &abs,
                    true,
                    Some("image"),
                    [("mimeType", RuntimeJson::from(mime_type))],
                ),
                mime_type,
                b64,
            ));
        }

        // PDF path. A PDF is binary, so it MUST be handled before the
        // binary-refusal/byte-cap checks below.
        if runtime_ascii_eq(&ext, ".pdf") {
            if let Some(pages_str) = pages.as_ref() {
                if !runtime_trim_is_empty(pages_str) {
                    // Validate + cap the range BEFORE extraction: an unparseable
                    // range is refused, and an open-ended "N-" counts as cap+1 so
                    // it is always rejected rather than rasterizing the whole PDF.
                    let range = match runtime_ascii_scalar(pages_str)
                        .and_then(|value| parse_pdf_page_range(&value))
                    {
                        Some(r) => r,
                        None => {
                            return Ok(text_result(
                                read_output(&abs, true, Some("pdf"), []),
                                concat_runtime(&[
                                    RuntimeString::from("Invalid pages parameter: \""),
                                    pages_str.clone(),
                                    RuntimeString::from("\". Use formats like \"1-5\", \"3\", or \"10-20\". Pages are 1-indexed."),
                                ]),
                                true,
                            ));
                        }
                    };
                    let range_size = match range.last_page {
                        None => PDF_MAX_PAGES_PER_READ + 1,
                        Some(last) => last - range.first_page + 1,
                    };
                    if range_size > PDF_MAX_PAGES_PER_READ {
                        return Ok(text_result(
                            read_output(&abs, true, Some("pdf"), []),
                            concat_runtime(&[
                                RuntimeString::from("Page range \""),
                                pages_str.clone(),
                                RuntimeString::from("\" exceeds maximum of "),
                                RuntimeString::from(PDF_MAX_PAGES_PER_READ.to_string()),
                                RuntimeString::from(
                                    " pages per request. Please use a smaller range.",
                                ),
                            ]),
                            true,
                        ));
                    }
                    // Page extraction rasterizes to IMAGES: a model without
                    // vision cannot read them either.
                    if ctx.vision == Some(false) {
                        return Ok(text_result(
                            read_output(&abs, true, Some("pdf"), []),
                            RuntimeString::from(
                                "This model cannot understand images, so PDF pages cannot be rendered for it. Tell the user to switch to a model with vision or native PDF support and try again.",
                            ),
                            true,
                        ));
                    }
                    match extract_pdf_pages(&ctx.process_context, &os_abs, Some(range)).await {
                        Err(e) => {
                            return Ok(text_result(
                                read_output(&abs, true, Some("pdf"), []),
                                e.message,
                                true,
                            ));
                        }
                        Ok(data) => {
                            let media: Vec<ContentBlock> = data
                                .pages
                                .into_iter()
                                .map(|p| {
                                    ContentBlock::Image(ImageBlock {
                                        mime_type: p.mime_type,
                                        base64: p.base64,
                                        detail: None,
                                        source_path: None,
                                    })
                                })
                                .collect();
                            let n = media.len();
                            return Ok(pdf_result(
                                read_output(
                                    &abs,
                                    true,
                                    Some("pdf"),
                                    [
                                        ("pages", RuntimeJson::from(n as u64)),
                                        ("pdfDelivery", RuntimeJson::from("page-images")),
                                    ],
                                ),
                                concat_runtime(&[
                                    RuntimeString::from("Read PDF "),
                                    path.clone(),
                                    RuntimeString::from(" pages \""),
                                    pages_str.clone(),
                                    RuntimeString::from("\" — "),
                                    RuntimeString::from(n.to_string()),
                                    RuntimeString::from(" page image(s) attached below."),
                                ]),
                                media,
                            ));
                        }
                    }
                }
            }

            // No page range: gate on page count, then deliver natively (or steer
            // to `pages`).
            if let Some(pc) = get_pdf_page_count(&ctx.process_context, &os_abs).await {
                if pc > PDF_INLINE_PAGE_THRESHOLD {
                    return Ok(text_result(
                        read_output(&abs, true, Some("pdf"), []),
                        concat_runtime(&[
                            RuntimeString::from("This PDF has "),
                            RuntimeString::from(pc.to_string()),
                            RuntimeString::from(" pages, which is too many to read at once. Use the pages parameter to read specific page ranges (e.g. pages: \"1-5\"). Maximum "),
                            RuntimeString::from(PDF_MAX_PAGES_PER_READ.to_string()),
                            RuntimeString::from(" pages per request."),
                        ]),
                        true,
                    ));
                }
            }
            if ctx.native_pdf == Some(false) {
                return Ok(text_result(
                    read_output(&abs, true, Some("pdf"), []),
                    concat_runtime(&[
                        RuntimeString::from("Reading full PDFs is not supported with this model. Use a model with native PDF support, or the pages parameter to read specific page ranges (e.g. pages: \"1-5\", maximum "),
                        RuntimeString::from(PDF_MAX_PAGES_PER_READ.to_string()),
                        RuntimeString::from(" pages per request). Page extraction requires poppler-utils (`brew install poppler` on macOS or `apt-get install poppler-utils` on Debian/Ubuntu)."),
                    ]),
                    true,
                ));
            }
            match read_pdf(&os_abs).await {
                Err(e) => {
                    return Ok(text_result(
                        read_output(&abs, true, Some("pdf"), []),
                        e.message,
                        true,
                    ));
                }
                Ok(data) => {
                    let media = vec![ContentBlock::Document(DocumentBlock {
                        mime_type: RuntimeString::from("application/pdf"),
                        base64: data.base64,
                        filename: Some(basename_runtime(&abs).into_runtime_string()),
                    })];
                    return Ok(pdf_result(
                        read_output(
                            &abs,
                            true,
                            Some("pdf"),
                            [("pdfDelivery", RuntimeJson::from("native"))],
                        ),
                        concat_runtime(&[
                            RuntimeString::from("Read PDF "),
                            path.clone(),
                            RuntimeString::from(" ("),
                            RuntimeString::from(fmt_bytes(data.original_size)),
                            RuntimeString::from(") — delivered natively below."),
                        ]),
                        media,
                    ));
                }
            }
        }

        // Byte cap: refuse a too-large file BEFORE reading it, but ONLY for a
        // limit-less read (a full read or an offset-only read gates on total file
        // size; an explicit-`limit` read is bounded solely by the token cap).
        if limit.is_none() && size > self.max_bytes {
            return Ok(text_result(
                read_output(
                    &abs,
                    true,
                    Some("text"),
                    [("oversize", RuntimeJson::Bool(true))],
                ),
                concat_runtime(&[
                    path.clone(),
                    RuntimeString::from(" is "),
                    RuntimeString::from(size.to_string()),
                    RuntimeString::from(" bytes, over the "),
                    RuntimeString::from(self.max_bytes.to_string()),
                    RuntimeString::from("-byte read limit. Read a section with the offset and limit parameters, search it with grep, or extract part of it with a shell command (e.g. sed -n)."),
                ]),
                true,
            ));
        }

        let buf = tokio::fs::read(&os_abs)
            .await
            .map_err(|e| ToolError::new(e.to_string()))?;

        // Empty file: advise explicitly rather than return a silent empty string.
        if buf.is_empty() {
            // A full read of an empty file is a COMPLETE view — record empty
            // content (not just mtime+size) so a later Write/Edit isn't wrongly
            // blocked as "not read". A partial read keeps the mtime+size baseline
            // only.
            if offset.is_none() && limit.is_none() {
                ctx.file_state.record(
                    &abs,
                    mtime_ms,
                    RecordOptions {
                        size: Some(0),
                        content: Some(RuntimeString::default()),
                        encoding: None,
                    },
                );
            }
            return Ok(read_advisory_result(
                read_output(
                    &abs,
                    true,
                    Some("text"),
                    [
                        ("lines", RuntimeJson::from(0_u64)),
                        ("empty", RuntimeJson::Bool(true)),
                    ],
                ),
                concat_runtime(&[
                    RuntimeString::from("The file "),
                    path.clone(),
                    RuntimeString::from(" exists but is empty — there is nothing to read."),
                ]),
            ));
        }

        // Decode with the BOM-detected encoding and normalize CRLF→LF so the model
        // sees a single newline form — and a later Edit's LF `old_string` matches a
        // CRLF file.
        let decoded = decode_text_buffer(&buf);
        let text = decoded.content;
        let encoding = decoded.encoding;

        // Binary path: refuse content we can't render as text — checked on the
        // DECODED text so a utf16le-BOM binary with an aligned 0x0000 code unit is
        // caught, while a genuine utf16le text file passes.
        if looks_binary_text(&text) {
            return Ok(text_result(
                read_output(&abs, true, Some("binary"), []),
                concat_runtime(&[
                    RuntimeString::from("Cannot read binary file as text: "),
                    path.clone(),
                    RuntimeString::from(". (Detected NUL bytes.)"),
                ]),
                true,
            ));
        }

        let all_lines = split_runtime_lines(&text);
        let start = match offset {
            Some(o) => (o - 1) as usize,
            None => 0,
        };
        // Start line past the end of the file: advise rather than return an empty
        // slice.
        if let Some(o) = offset {
            if start >= all_lines.len() {
                return Ok(read_advisory_result(
                    read_output(
                        &abs,
                        true,
                        Some("text"),
                        [
                            ("lines", RuntimeJson::from(0_u64)),
                            ("beyondEof", RuntimeJson::Bool(true)),
                        ],
                    ),
                    concat_runtime(&[
                        RuntimeString::from("Start line "),
                        RuntimeString::from(o.to_string()),
                        RuntimeString::from(" is past the end of "),
                        path.clone(),
                        RuntimeString::from(", which has "),
                        RuntimeString::from(all_lines.len().to_string()),
                        RuntimeString::from(" line(s)."),
                    ]),
                ));
            }
        }
        let end = match limit {
            Some(l) => (start + l as usize).min(all_lines.len()),
            None => all_lines.len(),
        };
        let numbered = render_numbered_runtime_line_slice(&all_lines, start, end);

        // Token cap. Estimated on the RAW slice content (not the line-numbered
        // render), by UTF-8 BYTE length / chars-per-token; the ratio is denser for
        // JSON.
        let bytes_per_token: usize = if runtime_ascii_eq(&ext, ".json")
            || runtime_ascii_eq(&ext, ".jsonl")
            || runtime_ascii_eq(&ext, ".jsonc")
        {
            2
        } else {
            4
        };
        let raw_slice = join_runtime(&all_lines[start..end], "\n");
        let output_tokens = raw_slice.to_node_utf8().len().div_ceil(bytes_per_token);
        if output_tokens > self.max_tokens {
            let ranged = offset.is_some() || limit.is_some();
            let how = if ranged {
                "Request a smaller range with offset and limit."
            } else {
                "Read a section with the offset and limit parameters, or search it with grep."
            };
            let range_word = if ranged { "this range of " } else { "" };
            return Ok(text_result(
                read_output(
                    &abs,
                    true,
                    Some("text"),
                    [("oversize", RuntimeJson::Bool(true))],
                ),
                concat_runtime(&[
                    RuntimeString::from("Reading "),
                    RuntimeString::from(range_word),
                    path.clone(),
                    RuntimeString::from(" would return ~"),
                    RuntimeString::from(output_tokens.to_string()),
                    RuntimeString::from(" tokens, over the "),
                    RuntimeString::from(self.max_tokens.to_string()),
                    RuntimeString::from("-token read limit. "),
                    RuntimeString::from(how),
                ]),
                true,
            ));
        }

        // Record the RAW full content as the external-edit baseline — only for a
        // complete read (no offset/limit).
        if offset.is_none() && limit.is_none() {
            ctx.file_state.record(
                &abs,
                mtime_ms,
                RecordOptions {
                    size: Some(size),
                    content: Some(text.clone()),
                    encoding: Some(to_state_encoding(encoding)),
                },
            );
        }

        Ok(text_result(
            read_output(
                &abs,
                true,
                Some("text"),
                [("lines", RuntimeJson::from((end - start) as u64))],
            ),
            numbered,
            false,
        ))
    }
}

// ---- small local helpers ----

fn render_numbered_runtime_line_slice(
    lines: &[RuntimeString],
    start: usize,
    end: usize,
) -> RuntimeString {
    let rendered = lines[start..end]
        .iter()
        .enumerate()
        .map(|(index, line)| {
            RuntimeString::from(format!("{:>6}\t", start + index + 1)).concat(line)
        })
        .collect::<Vec<_>>();
    join_runtime(&rendered, "\n")
}

#[allow(dead_code)] // Frozen runtime-path contract evidence names this helper directly.
pub(crate) fn render_numbered_line_slice(
    lines: &[&str],
    start: usize,
    end: usize,
) -> RuntimeString {
    let exact = lines
        .iter()
        .map(|line| RuntimeString::from(*line))
        .collect::<Vec<_>>();
    render_numbered_runtime_line_slice(&exact, start, end)
}

/// mtime in milliseconds (whole + fractional), matching Node's `stat.mtimeMs`.
fn stat_mtime_ms(meta: &std::fs::Metadata) -> f64 {
    meta.modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as f64 * 1000.0 + f64::from(d.subsec_nanos()) / 1e6)
        .unwrap_or(0.0)
}

/// Read an optional positive safe-integer field. Accepts an integer JSON number
/// and a whole float, mirroring `Number.isSafeInteger` at the tool boundary.
pub(crate) fn json_positive_integer(value: &RuntimeJson) -> Option<u64> {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    value
        .as_u64()
        .filter(|number| (1..=MAX_SAFE_INTEGER).contains(number))
        .or_else(|| {
            let number = value.as_f64()?;
            (number.is_finite()
                && number.fract() == 0.0
                && number >= 1.0
                && number <= MAX_SAFE_INTEGER as f64)
                .then_some(number as u64)
        })
}

/// Lexical `path.extname`: the final `.suffix` of the basename; a leading dot is
/// not an extension, and `.`/`..` have no extension.
fn extname_runtime(path: &RuntimeString) -> RuntimeString {
    let units = path.as_utf16();
    let base_start = units
        .iter()
        .rposition(|unit| *unit == u16::from(b'/') || *unit == u16::from(b'\\'))
        .map_or(0, |index| index + 1);
    let base = &units[base_start..];
    if base == [u16::from(b'.')] || base == [u16::from(b'.'), u16::from(b'.')] {
        return RuntimeString::default();
    }
    match base.iter().rposition(|unit| *unit == u16::from(b'.')) {
        None | Some(0) => RuntimeString::default(),
        Some(index) => RuntimeString::from_utf16(base[index..].to_vec()),
    }
}

/// The image MIME type for a recognized (lowercased, dot-prefixed) extension.
fn image_mime(ext_lower: &RuntimeString) -> Option<&'static str> {
    if runtime_ascii_eq(ext_lower, ".png") {
        Some("image/png")
    } else if runtime_ascii_eq(ext_lower, ".jpg") || runtime_ascii_eq(ext_lower, ".jpeg") {
        Some("image/jpeg")
    } else if runtime_ascii_eq(ext_lower, ".gif") {
        Some("image/gif")
    } else if runtime_ascii_eq(ext_lower, ".webp") {
        Some("image/webp")
    } else {
        None
    }
}

/// Cheap binary heuristic on DECODED text: a NUL code unit (U+0000) within the
/// first 8000 UTF-16 code units marks the content as binary.
fn looks_binary_text(text: &RuntimeString) -> bool {
    text.as_utf16().iter().take(8000).any(|unit| *unit == 0)
}

/// Keep the advisory body canonical and mark it for one-time provider rendering
/// at Query's tool-result boundary.
pub(crate) fn read_advisory_result(output: Json, text: impl Into<RuntimeString>) -> ToolResult {
    text_result_with_envelope(output, text, false, HarnessEnvelope::SystemReminder)
}

/// Human-readable byte size: whole MB / KB, or a raw byte count.
fn fmt_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    if bytes >= MB {
        format!("{}MB", ((bytes as f64) / (MB as f64)).round() as u64)
    } else if bytes >= KB {
        format!("{}KB", ((bytes as f64) / (KB as f64)).round() as u64)
    } else {
        format!("{bytes}B")
    }
}

/// Map the text-file encoding onto the file-state encoding.
fn to_state_encoding(e: FsEncoding) -> StateEncoding {
    match e {
        FsEncoding::Utf8 => StateEncoding::Utf8,
        FsEncoding::Utf16Le => StateEncoding::Utf16Le,
    }
}

/// A tool result carrying an image block for the model.
fn image_result(output: Json, mime_type: &str, base64: String) -> ToolResult {
    ToolResult {
        serializable: ToolResultSerializable {
            output,
            model_content: vec![OutputBlock::Image(ImageBlock {
                mime_type: RuntimeString::from(mime_type),
                base64: RuntimeString::from(base64),
                detail: None,
                source_path: None,
            })],
            is_error: Some(false),
            mcp_meta: None,
            extra_user_blocks: None,
        },
        side_effects: None,
    }
}

/// A tool result delivering a PDF: a one-line text summary in the tool_result
/// itself, plus the actual media on `extra_user_blocks` so it lands as a SIBLING
/// of the tool_result in the same user turn.
fn pdf_result(output: Json, summary: RuntimeString, media: Vec<ContentBlock>) -> ToolResult {
    ToolResult {
        serializable: ToolResultSerializable {
            output,
            model_content: vec![OutputBlock::Text(TextBlock {
                text: summary,
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: Some(false),
            mcp_meta: None,
            extra_user_blocks: Some(media),
        },
        side_effects: None,
    }
}

fn read_output<const N: usize>(
    path: &RuntimePath,
    found: bool,
    kind: Option<&str>,
    extras: [(&str, RuntimeJson); N],
) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "path",
        RuntimeJson::String(posix_rendered_runtime(path).as_runtime_string().clone()),
    );
    output.insert("found", RuntimeJson::Bool(found));
    if let Some(kind) = kind {
        output.insert("kind", RuntimeJson::from(kind));
    }
    for (key, value) in extras {
        output.insert(key, value);
    }
    RuntimeJson::Object(output)
}

fn concat_runtime(parts: &[RuntimeString]) -> RuntimeString {
    parts
        .iter()
        .fold(RuntimeString::default(), |output, part| output.concat(part))
}

fn join_runtime(parts: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut output = RuntimeString::default();
    for (index, part) in parts.iter().enumerate() {
        if index != 0 {
            output = output.concat(&separator);
        }
        output = output.concat(part);
    }
    output
}

fn split_runtime_lines(text: &RuntimeString) -> Vec<RuntimeString> {
    let units = text.as_utf16();
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            lines.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    lines.push(RuntimeString::from_utf16(units[start..].to_vec()));
    lines
}

fn ascii_lower_runtime(value: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(
        value
            .as_utf16()
            .iter()
            .copied()
            .map(|unit| {
                if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
                    unit + u16::from(b'a' - b'A')
                } else {
                    unit
                }
            })
            .collect::<Vec<_>>(),
    )
}

fn runtime_ascii_eq(value: &RuntimeString, expected: &str) -> bool {
    value.as_utf16().iter().copied().eq(expected.encode_utf16())
}

fn runtime_ascii_scalar(value: &RuntimeString) -> Option<String> {
    value.as_utf16().iter().all(|unit| *unit <= 0x7f).then(|| {
        value
            .as_utf16()
            .iter()
            .map(|unit| char::from(*unit as u8))
            .collect()
    })
}

fn runtime_trim_is_empty(value: &RuntimeString) -> bool {
    value.as_utf16().iter().all(|unit| {
        matches!(
            *unit,
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

fn runtime_to_host_summary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 summary projection is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::bash::node_path::posix_rendered;
    use crate::runtime_process::RuntimeEnv;
    use crate::runtime_values::runtime_json;
    use crate::runtime_values::{runtime_codec, ScalarJson};
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{Encoding as StateEncoding, FileStateCache, FileStateEntry};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A unique temp dir (atomic counter + pid + nanos).
    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-read-tool-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A file-state cache that captures every `record` call.
    #[derive(Default)]
    struct RecordingFileState {
        records: Mutex<Vec<(RuntimePath, f64, RecordOptions)>>,
    }

    impl FileStateCache for RecordingFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions) {
            self.records
                .lock()
                .unwrap()
                .push((path.clone(), mtime_ms, opts));
        }
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(RecordingFileState::default())
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

    fn make_ctx(
        cwd: &str,
        file_state: Arc<dyn FileStateCache>,
        native_pdf: Option<bool>,
    ) -> ToolContext {
        make_ctx_full(cwd, file_state, native_pdf, None)
    }

    fn make_ctx_full(
        cwd: &str,
        file_state: Arc<dyn FileStateCache>,
        native_pdf: Option<bool>,
        vision: Option<bool>,
    ) -> ToolContext {
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::from(cwd),
            signal: AbortSignal::new(),
            file_state,
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
            emit: std::sync::Arc::new(|_| Box::pin(async {})),
            native_pdf,
            vision,
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
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
        }
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn runtime(value: serde_json::Value) -> RuntimeJson {
        runtime_codec::encode_value(
            &ScalarJson::validate_static_schema(value).expect("test fixture is scalar JSON"),
        )
        .expect("scalar test fixture has an exact runtime encoding")
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_read_tool();
        assert_eq!(tool.name(), RuntimeString::from("read"));
        assert_eq!(tool.description(), RuntimeString::from(READ_DESCRIPTION));
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Read));
        let input = runtime(json!({ "path": "a.txt" }));
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input),
            Some(RuntimeString::from("Reading a.txt"))
        );
        assert!(tool.max_result_size_chars().is_none());
    }

    #[test]
    fn input_schema_matches_the_normalized_shape() {
        let tool = create_read_tool();
        let expected = json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Absolute or cwd-relative file path" },
                "offset": {
                    "description": "1-based start line (text only)",
                    "type": "integer",
                    "exclusiveMinimum": 0,
                    "maximum": 9007199254740991_i64
                },
                "limit": {
                    "description": "Max number of lines to read (text only)",
                    "type": "integer",
                    "exclusiveMinimum": 0,
                    "maximum": 9007199254740991_i64
                },
                "pages": {
                    "description": "PDF only: bounded page range to render as images, e.g. \"5\" or \"1-10\". Use 1-based page numbers and request at most 20 pages. Open-ended ranges are not supported. Omit pages to read a small PDF natively when the current model supports it.",
                    "type": "string"
                }
            },
            "required": ["path"],
            "additionalProperties": false
        });
        assert_eq!(tool.input_schema(), runtime(expected));
    }

    #[test]
    fn get_path_returns_the_input_path() {
        let tool = create_read_tool();
        assert_eq!(
            tool.get_path(&runtime(json!({ "path": "foo/bar.txt" }))),
            Some(PathTargets::One(RuntimePath::from("foo/bar.txt")))
        );
    }

    #[tokio::test]
    async fn reports_a_missing_file() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();
        let missing = dir.join("nope.txt").to_string_lossy().into_owned();

        let res = tool
            .call(runtime(json!({ "path": missing })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output["found"], runtime(json!(false)));
        assert_eq!(text_of(&res), format!("File not found: {missing}"));
        assert!(fs.records.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_a_missing_file_through_a_pinned_memory_root() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let mut ctx = make_ctx(&cwd, fs.clone(), None);
        ctx.permission.pinned_memory_root =
            Some(crate::memory::pinned_root::PinnedMemoryRoot::pin(&dir).unwrap());
        let tool = create_read_tool();
        let missing = dir.join("nope.txt").to_string_lossy().into_owned();

        let res = tool
            .call(runtime(json!({ "path": missing })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output["found"], runtime(json!(false)));
        assert_eq!(text_of(&res), format!("File not found: {missing}"));
        assert!(fs.records.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_a_directory_as_a_read_error_not_a_missing_file() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": cwd })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), format!("Not a file: {cwd}"));
        assert!(fs.records.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reads_text_with_line_numbers_and_records_content() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("file.txt");
        std::fs::write(&p, "line1\nline2\nline3\n").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            "     1\tline1\n     2\tline2\n     3\tline3\n     4\t"
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "text", "lines": 4 })
            )
        );
        // A record with the full content + encoding was captured.
        let records = fs.records.lock().unwrap();
        let full = records
            .iter()
            .find(|(_, _, o)| o.content.is_some())
            .expect("a content record");
        assert_eq!(
            full.2.content.as_ref(),
            Some(&RuntimeString::from("line1\nline2\nline3\n"))
        );
        assert_eq!(full.2.encoding, Some(StateEncoding::Utf8));
        assert_eq!(full.2.size, Some(18));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parsed_lone_surrogate_path_uses_node_utf8_at_the_fs_boundary() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("\u{fffd}.txt"), "node-boundary").unwrap();
        let ctx = make_ctx(&cwd, Arc::new(RecordingFileState::default()), None);
        let tool = create_read_tool();

        let surrogate =
            runtime_json::parse(&RuntimeString::from(r#"{"path":"\ud800.txt"}"#)).unwrap();
        let from_surrogate = tool.call(surrogate, &ctx, None).await.unwrap();
        assert_eq!(from_surrogate.serializable.is_error, Some(false));
        assert!(text_of(&from_surrogate).contains("node-boundary"));

        let from_replacement = tool
            .call(runtime(json!({ "path": "\u{fffd}.txt" })), &ctx, None)
            .await
            .unwrap();
        assert_eq!(from_replacement.serializable.is_error, Some(false));
        assert!(text_of(&from_replacement).contains("node-boundary"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn token_cap_uses_node_utf8_byte_length_for_utf16_lone_surrogate() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let path = dir.join("lone.txt");
        std::fs::write(&path, [0xff, 0xfe, 0x00, 0xd8]).unwrap();
        let ctx = make_ctx(&cwd, Arc::new(RecordingFileState::default()), None);
        let tool = create_read_tool_with_options(ReadToolOptions {
            max_tokens: Some(2),
            ..Default::default()
        });

        let result = tool
            .call(
                runtime(json!({ "path": path.to_string_lossy() })),
                &ctx,
                None,
            )
            .await
            .unwrap();

        // BOM + lone surrogate is six Node UTF-8 bytes => ceil(6 / 4) = 2.
        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(result.serializable.output.get("oversize"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn honors_offset_and_limit_without_recording_content() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("file.txt");
        std::fs::write(&p, "line1\nline2\nline3\nline4\n").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(
                runtime(json!({ "path": abs, "offset": 2, "limit": 2 })),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(text_of(&res), "     2\tline2\n     3\tline3");
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "text", "lines": 2 })
            )
        );
        // Partial reads never capture content.
        assert!(fs
            .records
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, o)| o.content.is_none()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn empty_file_advises_and_records_empty_content() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("empty.txt");
        std::fs::write(&p, "").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            format!("The file {abs} exists but is empty — there is nothing to read.")
        );
        let OutputBlock::Text(advisory) = &res.serializable.model_content[0] else {
            panic!("expected text advisory")
        };
        assert_eq!(
            advisory.harness_envelope,
            Some(HarnessEnvelope::SystemReminder)
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "text", "lines": 0, "empty": true })
            )
        );
        let records = fs.records.lock().unwrap();
        let empty = records
            .iter()
            .find(|(_, _, o)| o.content.is_some())
            .expect("an empty-content record");
        assert_eq!(empty.2.content.as_ref(), Some(&RuntimeString::default()));
        assert_eq!(empty.2.size, Some(0));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn out_of_range_offset_advises() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("file.txt");
        std::fs::write(&p, "a\nb\n").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs, "offset": 5 })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            text_of(&res),
            format!("Start line 5 is past the end of {abs}, which has 3 line(s).")
        );
        let OutputBlock::Text(advisory) = &res.serializable.model_content[0] else {
            panic!("expected text advisory")
        };
        assert_eq!(
            advisory.harness_envelope,
            Some(HarnessEnvelope::SystemReminder)
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "text", "lines": 0, "beyondEof": true })
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_binary_with_nul_bytes() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("blob.dat");
        std::fs::write(&p, b"abc\0def").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            format!("Cannot read binary file as text: {abs}. (Detected NUL bytes.)")
        );
        assert_eq!(
            res.serializable.output,
            runtime(json!({ "path": posix_rendered(&abs), "found": true, "kind": "binary" }))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_a_limitless_read_over_the_byte_cap() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("big.txt");
        std::fs::write(&p, "abcdef\n").unwrap(); // 7 bytes
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool_with_options(ReadToolOptions {
            max_bytes: Some(4),
            ..Default::default()
        });

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            format!(
                "{abs} is 7 bytes, over the 4-byte read limit. Read a section with the offset and limit parameters, search it with grep, or extract part of it with a shell command (e.g. sed -n)."
            )
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "text", "oversize": true })
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_a_read_over_the_token_cap() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("dense.txt");
        std::fs::write(&p, "hello world").unwrap(); // 11 bytes, 1 line
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool_with_options(ReadToolOptions {
            max_tokens: Some(1),
            ..Default::default()
        });

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        // ceil(11 / 4) = 3 tokens.
        assert_eq!(
            text_of(&res),
            format!(
                "Reading {abs} would return ~3 tokens, over the 1-token read limit. Read a section with the offset and limit parameters, or search it with grep."
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reads_an_image_as_a_base64_block() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("pic.png");
        let bytes = [1u8, 2, 3, 4, 5];
        std::fs::write(&p, bytes).unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        match &res.serializable.model_content[0] {
            OutputBlock::Image(img) => {
                assert_eq!(img.mime_type, "image/png");
                assert_eq!(img.base64, BASE64_STANDARD.encode(bytes));
            }
            _ => panic!("expected an image block"),
        }
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "image", "mimeType": "image/png" })
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_an_oversize_image() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("pic.jpg");
        std::fs::write(&p, [0u8; 5]).unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool_with_options(ReadToolOptions {
            max_image_bytes: Some(2),
            ..Default::default()
        });

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            format!("Image is too large to read (5 bytes > 2 byte limit): {abs}")
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "image", "mimeType": "image/jpeg" })
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_an_unparseable_pdf_page_range() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nnot a real pdf").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs, "pages": "abc" })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Invalid pages parameter: \"abc\". Use formats like \"1-5\", \"3\", or \"10-20\". Pages are 1-indexed."
        );
        assert_eq!(
            res.serializable.output,
            runtime(json!({ "path": posix_rendered(&abs), "found": true, "kind": "pdf" }))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_a_pdf_range_over_the_page_maximum() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nnot a real pdf").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(
                runtime(json!({ "path": abs, "pages": "1-100" })),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Page range \"1-100\" exceeds maximum of 20 pages per request. Please use a smaller range."
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_native_pdf_when_the_model_lacks_support() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nnot a real pdf").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), Some(false));
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        let text = text_of(&res);
        assert!(
            text.starts_with("Reading full PDFs is not supported with this model."),
            "text: {text:?}"
        );
        assert!(
            text.contains("maximum 20 pages per request"),
            "text: {text:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_an_image_when_the_model_lacks_vision() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("shot.png");
        std::fs::write(&p, b"\x89PNG\r\n\x1a\n").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx_full(&cwd, fs.clone(), None, Some(false));
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        let text = text_of(&res);
        assert!(
            text.starts_with("This model cannot understand images"),
            "text: {text:?}"
        );
        assert!(text.contains("vision support"), "text: {text:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_pdf_page_images_when_the_model_lacks_vision() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nnot a real pdf").unwrap();
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx_full(&cwd, fs.clone(), Some(false), Some(false));
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs, "pages": "1" })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        let text = text_of(&res);
        assert!(
            text.starts_with("This model cannot understand images"),
            "text: {text:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reads_a_pdf_natively() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nnot a real pdf").unwrap(); // 22 bytes
        let abs = p.to_string_lossy().into_owned();
        let fs = Arc::new(RecordingFileState::default());
        let ctx = make_ctx(&cwd, fs.clone(), None);
        let tool = create_read_tool();

        let res = tool
            .call(runtime(json!({ "path": abs })), &ctx, None)
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        let text = text_of(&res);
        assert!(
            text.starts_with(&format!("Read PDF {abs} (")),
            "text: {text:?}"
        );
        assert!(
            text.ends_with("— delivered natively below."),
            "text: {text:?}"
        );
        assert_eq!(
            res.serializable.output,
            runtime(
                json!({ "path": posix_rendered(&abs), "found": true, "kind": "pdf", "pdfDelivery": "native" })
            )
        );
        let media = res.serializable.extra_user_blocks.as_ref().unwrap();
        assert_eq!(media.len(), 1);
        match &media[0] {
            ContentBlock::Document(d) => {
                assert_eq!(d.mime_type, "application/pdf");
                assert_eq!(d.filename.as_ref(), Some(&RuntimeString::from("doc.pdf")));
            }
            _ => panic!("expected a document block"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
