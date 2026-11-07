//! PDF reading support for the file-read tool. Two delivery paths:
//!
//!   - NATIVE: base64 the whole PDF into a `document` block the provider renders
//!     server-side (text + per-page images). Used when the model supports native
//!     PDFs and the caller did not ask for a specific page range.
//!   - PAGE-IMAGES: rasterize pages to JPEG with `pdftoppm` (poppler-utils) and
//!     send those as image blocks. Used when a page range is requested, or as the
//!     fallback for models without native PDF support.
//!
//! The binaries (`pdftoppm`, `pdfinfo`) come from poppler-utils — read-only
//! external tools the engine shells out to directly, the same way it invokes
//! ripgrep, rather than through a bundled library. A `%PDF-` magic check, the
//! page-range grammar, and the size/page caps below mirror the provider's own
//! limits.

use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use rand::RngCore;
use regex::Regex;

use crate::fs::node_path::runtime_string_from_native;
use crate::runtime_process::{
    RuntimeCompletedProcessRequest, RuntimeCompletedProcessSurface, RuntimeProcessContext,
};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

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

/// Maximum raw PDF size delivered NATIVELY. The API caps a request at 32 MB;
/// base64 inflates by ~33% (4/3), so 20 MB raw ≈ 27 MB encoded, leaving room for
/// context.
pub const PDF_TARGET_RAW_SIZE: u64 = 20 * 1024 * 1024; // 20 MB
/// Maximum number of pages the API accepts in one native PDF.
pub const API_PDF_MAX_PAGES: u32 = 100;
/// Size above which telemetry treats a PDF as an extraction candidate. Kept for
/// parity; the actual delivery choice is driven by native-support + page-count,
/// not this threshold (the no-pages branch extracts only for a telemetry log and
/// still delivers the document block).
pub const PDF_EXTRACT_SIZE_THRESHOLD: u64 = 3 * 1024 * 1024; // 3 MB
/// Maximum PDF size for the page-extraction path; larger files are rejected.
pub const PDF_MAX_EXTRACT_SIZE: u64 = 100 * 1024 * 1024; // 100 MB
/// Advisory cap on pages extracted in one `pages` request (surfaced in guidance
/// text).
pub const PDF_MAX_PAGES_PER_READ: u32 = 20;
/// A no-`pages` read of a PDF with more pages than this is refused — use the
/// `pages` param.
pub const PDF_INLINE_PAGE_THRESHOLD: u32 = 10;

/// Supported document extensions (without the leading dot, lowercased).
static DOCUMENT_EXTENSIONS: &[&str] = &["pdf"];

/// Whether a file extension (with or without leading dot) is a supported
/// document type.
pub fn is_pdf_extension(ext: &str) -> bool {
    let normalized = ext.strip_prefix('.').unwrap_or(ext).to_lowercase();
    DOCUMENT_EXTENSIONS.contains(&normalized.as_str())
}

/// A 1-indexed page range. `last_page` is `None` for an open-ended `"N-"`
/// range (conceptually +∞).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PdfPageRange {
    pub first_page: u32,
    /// Inclusive last page; `None` marks an open-ended `"N-"` range.
    pub last_page: Option<u32>,
}

impl RuntimeEncode for PdfPageRange {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            first_page,
            last_page,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("firstPage", first_page)?;
        object.field("lastPage", last_page)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PdfPageRange {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                first_page: object.required("firstPage", &[])?,
                last_page: object.optional("lastPage", &[])?,
            })
        })
    }
}

/// Parse a leading base-10 integer the way `parseInt(_, 10)` does: skip leading
/// whitespace, accept an optional sign, then read digits until the first
/// non-digit. `None` when there are no digits (the `NaN` case).
fn parse_int_base10(s: &str) -> Option<i64> {
    let t = s.trim_start();
    let (neg, rest) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    let digits = &rest[..end];
    if digits.is_empty() {
        return None;
    }
    // An absurdly long digit run clamps high; real page numbers never reach it.
    let v: i64 = digits.parse().unwrap_or(i64::MAX);
    Some(if neg { -v } else { v })
}

/// Parse a page-range string into 1-indexed first/last page numbers:
///   - `""`      → None (no range)
///   - `"5"`     → { first_page: 5, last_page: Some(5) }
///   - `"1-10"`  → { first_page: 1, last_page: Some(10) }
///   - `"3-"`    → { first_page: 3, last_page: None } (open-ended)
///
/// Returns None on invalid input (non-numeric, zero/negative, inverted range).
pub fn parse_pdf_page_range(pages: &str) -> Option<PdfPageRange> {
    let trimmed = pages.trim();
    if trimmed.is_empty() {
        return None;
    }

    // "N-" open-ended range.
    if let Some(head) = trimmed.strip_suffix('-') {
        let first = parse_int_base10(head)?;
        if first < 1 {
            return None;
        }
        return Some(PdfPageRange {
            first_page: first as u32,
            last_page: None,
        });
    }

    match trimmed.find('-') {
        None => {
            // Single page: "5".
            let page = parse_int_base10(trimmed)?;
            if page < 1 {
                return None;
            }
            Some(PdfPageRange {
                first_page: page as u32,
                last_page: Some(page as u32),
            })
        }
        Some(dash_index) => {
            // Range: "1-10".
            let first = parse_int_base10(&trimmed[..dash_index]);
            let last = parse_int_base10(&trimmed[dash_index + 1..]);
            match (first, last) {
                (Some(first), Some(last)) if first >= 1 && last >= 1 && last >= first => {
                    Some(PdfPageRange {
                        first_page: first as u32,
                        last_page: Some(last as u32),
                    })
                }
                _ => None,
            }
        }
    }
}

/// Why a PDF operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PdfErrorReason {
    Empty,
    TooLarge,
    PasswordProtected,
    Corrupted,
    Unknown,
    Unavailable,
}

impl RuntimeEncode for PdfErrorReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Empty => "empty",
            Self::TooLarge => "too_large",
            Self::PasswordProtected => "password_protected",
            Self::Corrupted => "corrupted",
            Self::Unknown => "unknown",
            Self::Unavailable => "unavailable",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PdfErrorReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("empty", &[]),
                RuntimeEnumVariant::new("too_large", &[]),
                RuntimeEnumVariant::new("password_protected", &[]),
                RuntimeEnumVariant::new("corrupted", &[]),
                RuntimeEnumVariant::new("unknown", &[]),
                RuntimeEnumVariant::new("unavailable", &[]),
            ],
            &[
                Self::Empty,
                Self::TooLarge,
                Self::PasswordProtected,
                Self::Corrupted,
                Self::Unknown,
                Self::Unavailable,
            ],
        )
    }
}

/// A classified PDF failure carrying a human-readable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PdfError {
    pub reason: PdfErrorReason,
    pub message: RuntimeString,
}

impl RuntimeEncode for PdfError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("reason", &self.reason)?;
        object.field("message", &self.message)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PdfError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                reason: object.required("reason", &[])?,
                message: object.required("message", &[])?,
            })
        })
    }
}

/// The outcome of a PDF operation: either the payload or a classified error.
pub type PdfResult<T> = Result<T, PdfError>;

/// Payload of a successful [`read_pdf`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPdfResult {
    pub base64: RuntimeString,
    pub original_size: u64,
}

impl RuntimeEncode for ReadPdfResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("base64", &self.base64)?;
        object.field("originalSize", &self.original_size)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ReadPdfResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                base64: object.required("base64", &[])?,
                original_size: object.required("originalSize", &[])?,
            })
        })
    }
}

/// A single rasterized page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PdfPageImage {
    pub mime_type: RuntimeString,
    pub base64: RuntimeString,
}

impl RuntimeEncode for PdfPageImage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mimeType", &self.mime_type)?;
        object.field("base64", &self.base64)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PdfPageImage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mime_type: object.required("mimeType", &[])?,
                base64: object.required("base64", &[])?,
            })
        })
    }
}

/// Payload of a successful [`extract_pdf_pages`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedPdfPages {
    pub pages: Vec<PdfPageImage>,
}

impl RuntimeEncode for ExtractedPdfPages {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("pages", &self.pages)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExtractedPdfPages {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                pages: object.required("pages", &[])?,
            })
        })
    }
}

/// The captured result of running a binary: exit code plus decoded output. Never
/// fails — a spawn error or timeout maps to code 1 with empty streams.
struct ExecResult {
    code: i32,
    stdout: String,
    stderr: String,
}

/// Run a binary capturing exit code + stdout/stderr, never throwing. A spawn
/// failure (e.g. the binary is absent) or a timeout resolves as code 1 with
/// empty streams; a signal-killed process also resolves as code 1.
async fn exec_file_no_throw(
    process_context: &RuntimeProcessContext,
    file: &str,
    args: &[OsString],
    timeout_ms: u64,
) -> ExecResult {
    let request = RuntimeCompletedProcessRequest::new(
        RuntimeCompletedProcessSurface::PdfExecFile,
        RuntimeString::from(file),
    )
    .with_args(
        args.iter()
            .map(|arg| runtime_string_from_native(arg.as_os_str()))
            .collect(),
    )
    .with_timeout(Duration::from_millis(timeout_ms));
    match process_context.exec_completed(request).await {
        Ok(output) => ExecResult {
            code: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        },
        Err(_) => ExecResult {
            code: 1,
            stdout: String::new(),
            stderr: String::new(),
        },
    }
}

/// Process-lifetime cache of `pdftoppm` availability.
static PDFTOPPM_AVAILABLE: Mutex<Option<bool>> = Mutex::new(None);

/// Reset the `pdftoppm` availability cache. Tests only.
pub fn reset_pdftoppm_cache() {
    *PDFTOPPM_AVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

/// Whether the `pdftoppm` binary (poppler-utils) is available. Cached for the
/// process lifetime. pdftoppm prints its version to stderr and exits 0 (or 99 on
/// some builds), so either a clean exit OR any stderr output counts as present.
pub async fn is_pdftoppm_available(process_context: &RuntimeProcessContext) -> bool {
    if let Some(v) = *PDFTOPPM_AVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        return v;
    }
    let res = exec_file_no_throw(process_context, "pdftoppm", &[OsString::from("-v")], 5000).await;
    let available = res.code == 0 || !res.stderr.is_empty();
    *PDFTOPPM_AVAILABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(available);
    available
}

static PDFINFO_PAGES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^Pages:\s+([0-9]+)").unwrap());

/// Page count via `pdfinfo` (poppler-utils). Returns `None` when pdfinfo is
/// absent or the count can't be parsed — the caller treats `None` as "unknown"
/// and proceeds.
pub async fn get_pdf_page_count(
    process_context: &RuntimeProcessContext,
    file_path: &Path,
) -> Option<u32> {
    let res = exec_file_no_throw(
        process_context,
        "pdfinfo",
        &[file_path.as_os_str().to_os_string()],
        10_000,
    )
    .await;
    if res.code != 0 {
        return None;
    }
    let caps = PDFINFO_PAGES_RE.captures(&res.stdout)?;
    let digits = caps.get(1)?.as_str();
    // The regex guarantees ≥1 digit; a value that overflows the page-count type
    // is treated as unparseable — matching the runtime's `NaN` guard.
    digits.parse::<u32>().ok()
}

/// Read a PDF as base64 for native `document` delivery. Validates: non-empty,
/// within [`PDF_TARGET_RAW_SIZE`], and a real `%PDF-` header — an invalid PDF
/// that reaches the message history makes EVERY later request fail ("PDF was not
/// valid"), so it is rejected up front.
pub async fn read_pdf(file_path: &Path) -> PdfResult<ReadPdfResult> {
    let st = match tokio::fs::metadata(file_path).await {
        Ok(s) => s,
        Err(e) => return Err(unknown_error(&e)),
    };
    let original_size = st.len();
    if original_size == 0 {
        return Err(PdfError {
            reason: PdfErrorReason::Empty,
            message: RuntimeString::from(format!(
                "PDF file is empty: {}",
                pdf_path_error_label(file_path)
            )),
        });
    }
    if original_size > PDF_TARGET_RAW_SIZE {
        return Err(PdfError {
            reason: PdfErrorReason::TooLarge,
            message: RuntimeString::from(format!(
                "PDF file exceeds the maximum allowed size of {}.",
                format_bytes(PDF_TARGET_RAW_SIZE)
            )),
        });
    }
    let buf = match tokio::fs::read(file_path).await {
        Ok(b) => b,
        Err(e) => return Err(unknown_error(&e)),
    };
    // First five bytes, decoded as 7-bit ASCII (high bit masked), must be `%PDF-`.
    let header_ok = buf.len() >= 5
        && buf[..5]
            .iter()
            .zip(b"%PDF-")
            .all(|(&b, &t)| (b & 0x7f) == t);
    if !header_ok {
        return Err(PdfError {
            reason: PdfErrorReason::Corrupted,
            message: RuntimeString::from(format!(
                "File is not a valid PDF (missing %PDF- header): {}",
                pdf_path_error_label(file_path)
            )),
        });
    }
    Ok(ReadPdfResult {
        base64: RuntimeString::from(base64_encode(&buf)),
        original_size,
    })
}

/// Rasterize PDF pages to JPEG via `pdftoppm -jpeg -r 100` and return them as
/// base64 image data. An optional 1-indexed inclusive range maps to `-f`/`-l`.
/// The JPEGs are written to a fresh temp dir which is removed after they are
/// encoded.
pub async fn extract_pdf_pages(
    process_context: &RuntimeProcessContext,
    file_path: &Path,
    range: Option<PdfPageRange>,
) -> PdfResult<ExtractedPdfPages> {
    let st = match tokio::fs::metadata(file_path).await {
        Ok(s) => s,
        Err(e) => return Err(unknown_error(&e)),
    };
    if st.len() == 0 {
        return Err(PdfError {
            reason: PdfErrorReason::Empty,
            message: RuntimeString::from(format!(
                "PDF file is empty: {}",
                pdf_path_error_label(file_path)
            )),
        });
    }
    if st.len() > PDF_MAX_EXTRACT_SIZE {
        return Err(PdfError {
            reason: PdfErrorReason::TooLarge,
            message: RuntimeString::from(format!(
                "PDF file exceeds the maximum allowed size for page extraction ({}).",
                format_bytes(PDF_MAX_EXTRACT_SIZE)
            )),
        });
    }
    if !is_pdftoppm_available(process_context).await {
        return Err(PdfError {
            reason: PdfErrorReason::Unavailable,
            message: RuntimeString::from("pdftoppm is not installed. Install poppler-utils (`brew install poppler` on macOS or `apt-get install poppler-utils` on Debian/Ubuntu) to enable PDF page rendering."),
        });
    }

    let output_dir = std::env::temp_dir().join(format!("klaus-pdf-{}", random_uuid()));
    if let Err(e) = tokio::fs::create_dir_all(&output_dir).await {
        return Err(unknown_error(&e));
    }
    let result = extract_pages_body(process_context, &output_dir, file_path, range.as_ref()).await;
    // Always remove the temp dir, ignoring any cleanup error.
    let _ = tokio::fs::remove_dir_all(&output_dir).await;
    result
}

/// The core of [`extract_pdf_pages`] (the caller owns temp-dir cleanup).
async fn extract_pages_body(
    process_context: &RuntimeProcessContext,
    output_dir: &std::path::Path,
    file_path: &Path,
    range: Option<&PdfPageRange>,
) -> PdfResult<ExtractedPdfPages> {
    let prefix = output_dir.join("page").into_os_string();
    let mut args: Vec<OsString> = vec!["-jpeg".into(), "-r".into(), "100".into()];
    if let Some(r) = range {
        if r.first_page != 0 {
            args.push("-f".into());
            args.push(r.first_page.to_string().into());
        }
        // An open-ended range (`None`) is +∞ and gets no `-l`; a zero last page
        // is falsy and is likewise skipped.
        if let Some(last) = r.last_page {
            if last != 0 {
                args.push("-l".into());
                args.push(last.to_string().into());
            }
        }
    }
    args.push(file_path.as_os_str().to_os_string());
    args.push(prefix);

    let res = exec_file_no_throw(process_context, "pdftoppm", &args, 120_000).await;
    if res.code != 0 {
        let low = res.stderr.to_ascii_lowercase();
        if low.contains("password") {
            return Err(PdfError {
                reason: PdfErrorReason::PasswordProtected,
                message: RuntimeString::from(
                    "PDF is password-protected. Please provide an unprotected version.",
                ),
            });
        }
        if low.contains("damaged") || low.contains("corrupt") || low.contains("invalid") {
            return Err(PdfError {
                reason: PdfErrorReason::Corrupted,
                message: RuntimeString::from("PDF file is corrupted or invalid."),
            });
        }
        return Err(PdfError {
            reason: PdfErrorReason::Unknown,
            message: RuntimeString::from(format!("pdftoppm failed: {}", res.stderr)),
        });
    }

    // Read the produced page images in natural (sorted) order.
    let mut names: Vec<String> = Vec::new();
    let mut rd = match tokio::fs::read_dir(output_dir).await {
        Ok(rd) => rd,
        Err(e) => return Err(unknown_error(&e)),
    };
    loop {
        match rd.next_entry().await {
            Ok(Some(entry)) => {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".jpg") {
                    names.push(name);
                }
            }
            Ok(None) => break,
            Err(e) => return Err(unknown_error(&e)),
        }
    }
    names.sort();
    if names.is_empty() {
        return Err(PdfError {
            reason: PdfErrorReason::Corrupted,
            message: RuntimeString::from(
                "pdftoppm produced no output pages. The PDF may be invalid.",
            ),
        });
    }

    let mut pages: Vec<PdfPageImage> = Vec::new();
    for name in &names {
        let buf = match tokio::fs::read(output_dir.join(name)).await {
            Ok(b) => b,
            Err(e) => return Err(unknown_error(&e)),
        };
        // Skip a 0-byte render: the provider rejects an empty image block ("image
        // cannot be empty"), and an empty page carries no signal.
        if buf.is_empty() {
            continue;
        }
        pages.push(PdfPageImage {
            mime_type: RuntimeString::from("image/jpeg"),
            base64: RuntimeString::from(base64_encode(&buf)),
        });
    }
    if pages.is_empty() {
        return Err(PdfError {
            reason: PdfErrorReason::Corrupted,
            message: RuntimeString::from(
                "pdftoppm produced only empty pages. The PDF may be invalid.",
            ),
        });
    }
    Ok(ExtractedPdfPages { pages })
}

// ---- small local helpers ----

fn pdf_path_error_label(path: &Path) -> String {
    String::from_utf8(
        crate::fs::node_path::runtime_string_from_native(path.as_os_str()).to_node_utf8(),
    )
    .expect("Node UTF-8 PDF error-path projection is valid UTF-8")
}

/// Wrap an I/O failure that would otherwise propagate as a thrown exception into
/// an `unknown` PDF error.
fn unknown_error(e: &std::io::Error) -> PdfError {
    PdfError {
        reason: PdfErrorReason::Unknown,
        message: RuntimeString::from(e.to_string()),
    }
}

/// Human-readable byte size: whole MB / KB, or a raw byte count.
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{}MB", ((bytes as f64) / (1024.0 * 1024.0)).round() as u64)
    } else if bytes >= 1024 {
        format!("{}KB", ((bytes as f64) / 1024.0).round() as u64)
    } else {
        format!("{bytes}B")
    }
}

/// A random v4-shaped UUID, used only to name a scratch directory.
fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// Standard (padded) base64 alphabet.
const B64_STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes as standard, padded base64.
fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_STD[((n >> 18) & 63) as usize] as char);
        out.push(B64_STD[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_STD[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_STD[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-pdf-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Decode standard (padded) base64 — the inverse of [`base64_encode`], used to
    /// verify round-trips.
    fn base64_decode(s: &str) -> Vec<u8> {
        fn val(c: u8) -> Option<u32> {
            match c {
                b'A'..=b'Z' => Some((c - b'A') as u32),
                b'a'..=b'z' => Some((c - b'a') as u32 + 26),
                b'0'..=b'9' => Some((c - b'0') as u32 + 52),
                b'+' => Some(62),
                b'/' => Some(63),
                _ => None,
            }
        }
        let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
        let mut out = Vec::new();
        for chunk in bytes.chunks(4) {
            let mut n = 0u32;
            for (i, &b) in chunk.iter().enumerate() {
                n |= val(b).unwrap() << (18 - 6 * i);
            }
            out.push((n >> 16) as u8);
            if chunk.len() > 2 {
                out.push((n >> 8) as u8);
            }
            if chunk.len() > 3 {
                out.push(n as u8);
            }
        }
        out
    }

    // ---- parse_pdf_page_range grammar ----

    #[test]
    fn empty_or_whitespace_is_none() {
        assert_eq!(parse_pdf_page_range(""), None);
        assert_eq!(parse_pdf_page_range("   "), None);
    }

    #[test]
    fn single_page_five() {
        assert_eq!(
            parse_pdf_page_range("5"),
            Some(PdfPageRange {
                first_page: 5,
                last_page: Some(5)
            })
        );
        assert_eq!(
            parse_pdf_page_range("  7 "),
            Some(PdfPageRange {
                first_page: 7,
                last_page: Some(7)
            })
        );
    }

    #[test]
    fn closed_range_one_ten() {
        assert_eq!(
            parse_pdf_page_range("1-10"),
            Some(PdfPageRange {
                first_page: 1,
                last_page: Some(10)
            })
        );
    }

    #[test]
    fn open_ended_three_dash() {
        assert_eq!(
            parse_pdf_page_range("3-"),
            Some(PdfPageRange {
                first_page: 3,
                last_page: None
            })
        );
    }

    #[test]
    fn invalid_inputs_are_none() {
        assert_eq!(parse_pdf_page_range("0"), None); // zero
        assert_eq!(parse_pdf_page_range("-5"), None); // range with empty first → NaN
        assert_eq!(parse_pdf_page_range("abc"), None); // non-numeric
        assert_eq!(parse_pdf_page_range("10-1"), None); // inverted
        assert_eq!(parse_pdf_page_range("0-"), None); // open-ended from zero
        assert_eq!(parse_pdf_page_range("2-x"), None); // non-numeric end
    }

    // ---- is_pdf_extension ----

    #[test]
    fn accepts_pdf_with_or_without_dot_case_insensitively() {
        assert!(is_pdf_extension(".pdf"));
        assert!(is_pdf_extension("pdf"));
        assert!(is_pdf_extension(".PDF"));
    }

    #[test]
    fn rejects_non_pdf_extensions() {
        assert!(!is_pdf_extension(".txt"));
        assert!(!is_pdf_extension(".png"));
        assert!(!is_pdf_extension(""));
    }

    // ---- read_pdf validation before delivery ----

    #[tokio::test]
    async fn base64_encodes_a_valid_pdf_file() {
        let dir = temp_dir();
        let p = dir.join("doc.pdf");
        std::fs::write(&p, "%PDF-1.7\nbody").unwrap();
        let res = read_pdf(&p).await;
        assert!(res.is_ok());
        let data = res.unwrap();
        assert!(base64_decode(&data.base64.try_to_utf8().unwrap()).starts_with(b"%PDF-"));
        assert!(data.original_size > 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_an_empty_file() {
        let dir = temp_dir();
        let p = dir.join("empty.pdf");
        std::fs::write(&p, "").unwrap();
        let res = read_pdf(&p).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().reason, PdfErrorReason::Empty);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rejects_a_file_lacking_the_pdf_header() {
        let dir = temp_dir();
        let p = dir.join("fake.pdf");
        std::fs::write(&p, "<html>not a pdf</html>").unwrap();
        let res = read_pdf(&p).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().reason, PdfErrorReason::Corrupted);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn exposes_the_20mb_native_size_ceiling() {
        assert_eq!(PDF_TARGET_RAW_SIZE, 20 * 1024 * 1024);
    }

    #[test]
    fn pdf_runtime_payload_preserves_lone_surrogates() {
        let message = RuntimeString::from_utf16([b'p' as u16, 0xd800]);
        let error = PdfError {
            reason: PdfErrorReason::Unknown,
            message: message.clone(),
        };
        let encoded = runtime_codec::encode_value(&error).unwrap();
        let decoded = runtime_codec::decode_value::<PdfError>(&encoded).unwrap();

        assert_eq!(decoded.message, message);
    }
}
