//! Full-text search across persisted session rollouts.
//!
//! Two-phase design: an optional ripgrep pass (the bundled binary, spawned
//! through the supervised runtime) narrows the candidate files fast, then a
//! precise Rust pass decodes each candidate's rollout lines and matches ONLY
//! human-visible conversation text — user/assistant text blocks, tool-result
//! text, and compaction summaries — so JSON keys, ids, or encoded blobs never
//! produce false hits. Without an accelerator every indexed rollout is scanned
//! directly; results are identical, just slower.

use std::collections::HashSet;
use std::io;

use crate::fs::ripgrep::{run_ripgrep, RipgrepError};
use crate::runtime_process::RuntimeProcessContext;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::Message;
use crate::schemas::rollout::RolloutItem;
use crate::store::rollout::parse_rollout_line;
use crate::store::session_store::ThreadSummary;
use crate::types::abort::AbortSignal;

/// Default cap on returned sessions when the caller does not pass a limit.
pub const DEFAULT_SEARCH_LIMIT: usize = 20;
/// Characters of context kept on each side of the first match in a snippet.
const SNIPPET_CONTEXT_CHARS: usize = 80;

/// Options for a session full-text search.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionSearchOptions {
    /// Literal text to look for (ASCII case-insensitive; never a regex).
    pub query: RuntimeString,
    /// Max sessions to return. Missing means [`DEFAULT_SEARCH_LIMIT`]; `0`
    /// returns an empty page.
    pub limit: Option<usize>,
    /// Include archived sessions (default: false).
    pub include_archived: Option<bool>,
}

impl RuntimeEncode for SessionSearchOptions {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("query", &self.query)?;
        if let Some(value) = &self.limit {
            object.field("limit", value)?;
        }
        if let Some(value) = &self.include_archived {
            object.field("includeArchived", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SessionSearchOptions {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                query: object.required("query", &[])?,
                limit: object.optional("limit", &[])?,
                include_archived: object.optional("includeArchived", &[])?,
            })
        })
    }
}

/// One matching session: its header projection plus the first matching record.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionSearchHit {
    pub thread: ThreadSummary,
    /// Which record kind produced the first match: `user`, `assistant`,
    /// `tool`, or `summary`.
    pub role: RuntimeString,
    /// Single-line display excerpt around the first match, ellipsized.
    pub snippet: RuntimeString,
    /// Number of conversation records in this session that match.
    pub matches: u64,
}

impl RuntimeEncode for SessionSearchHit {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("thread", &self.thread)?;
        object.field("role", &self.role)?;
        object.field("snippet", &self.snippet)?;
        object.field("matches", &self.matches)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SessionSearchHit {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                thread: object.required("thread", &[])?,
                role: object.required("role", &[])?,
                snippet: object.required("snippet", &[])?,
                matches: object.required("matches", &[])?,
            })
        })
    }
}

/// A page of search hits, newest session first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionSearchPage {
    pub items: Vec<SessionSearchHit>,
}

impl RuntimeEncode for SessionSearchPage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("items", &self.items)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SessionSearchPage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                items: object.required("items", &[])?,
            })
        })
    }
}

/// What one rollout file contributed: the first matching record + total count.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileMatches {
    pub role: &'static str,
    pub snippet: String,
    pub matches: u64,
}

/// Locate `needle` in `haystack`: exact match first, then an ASCII
/// case-insensitive scan (non-ASCII characters must match exactly). Returns
/// the byte offset in the ORIGINAL haystack, which keeps snippet slicing on
/// valid char boundaries.
fn find_ci(haystack: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    if let Some(index) = haystack.find(needle) {
        return Some(index);
    }
    for (index, _) in haystack.char_indices() {
        let Some(window) = haystack.get(index..index + needle.len()) else {
            continue;
        };
        if window.eq_ignore_ascii_case(needle) {
            return Some(index);
        }
    }
    None
}

/// A single-line excerpt of up to [`SNIPPET_CONTEXT_CHARS`] characters around
/// the match, ellipsized on clipped ends; newlines collapse to spaces.
fn snippet_around(text: &str, match_start: usize, match_len: usize) -> String {
    let mut start = match_start;
    for _ in 0..SNIPPET_CONTEXT_CHARS {
        let Some(prev) = text[..start].chars().next_back() else {
            break;
        };
        start -= prev.len_utf8();
    }
    let mut end = (match_start + match_len).min(text.len());
    for (taken, ch) in text[end..].chars().enumerate() {
        if taken == SNIPPET_CONTEXT_CHARS {
            break;
        }
        end += ch.len_utf8();
    }
    let mut snippet = String::new();
    if start > 0 {
        snippet.push('…');
    }
    for ch in text[start..end].chars() {
        snippet.push(if ch == '\n' || ch == '\r' { ' ' } else { ch });
    }
    if end < text.len() {
        snippet.push('…');
    }
    snippet
}

/// The human-visible texts of one rollout item, each tagged with its record
/// kind. Anything else (turn contexts, usage, checkpoints, events, system
/// reminders) is not conversation content and never matches.
fn visible_texts(item: &RolloutItem) -> Vec<(&'static str, String)> {
    let mut texts = Vec::new();
    let mut push = |role: &'static str, text: &RuntimeString| {
        if let Ok(utf8) = text.try_to_utf8() {
            if !utf8.is_empty() {
                texts.push((role, utf8));
            }
        }
    };
    match item {
        RolloutItem::Message { message } => match message {
            Message::User(user) => {
                for block in &user.content {
                    if let ContentBlock::Text(text) = block {
                        push("user", &text.text);
                    }
                }
            }
            Message::Assistant(assistant) => {
                for block in &assistant.content {
                    if let ContentBlock::Text(text) = block {
                        push("assistant", &text.text);
                    }
                }
            }
            Message::ToolResult(result) => {
                for block in &result.content {
                    if let OutputBlock::Text(text) = block {
                        push("tool", &text.text);
                    }
                }
            }
            Message::CompactBoundary(boundary) => push("summary", &boundary.summary),
            Message::System(_) => {}
        },
        RolloutItem::Compacted(compacted) => push("summary", &compacted.summary),
        _ => {}
    }
    texts
}

/// Whether a raw-content prefilter is sound for this query: JSONL string
/// escaping only rewrites `"`, `\`, control characters, and (for some writers)
/// non-ASCII text, so a query free of those always appears verbatim in the
/// raw file whenever it appears in a decoded text.
fn raw_prefilter_is_sound(query: &str) -> bool {
    query
        .chars()
        .all(|ch| ch.is_ascii() && !ch.is_ascii_control() && ch != '"' && ch != '\\')
}

/// Scan one rollout file for the query. Returns `None` when nothing visible
/// matches. Corrupt or mid-write lines are skipped, mirroring resume.
pub(crate) async fn scan_rollout_file(path: &str, query: &str) -> io::Result<Option<FileMatches>> {
    let bytes = crate::store::compression::read(path).await?;
    let Ok(content) = String::from_utf8(bytes) else {
        return Ok(None);
    };
    if raw_prefilter_is_sound(query) && find_ci(&content, query).is_none() {
        return Ok(None);
    }
    let mut first: Option<(&'static str, String)> = None;
    let mut matches = 0_u64;
    for line in content.split('\n') {
        if line.is_empty() {
            continue;
        }
        let Some(item) = parse_rollout_line(line) else {
            continue;
        };
        let mut line_matched = false;
        for (role, text) in visible_texts(&item) {
            let Some(start) = find_ci(&text, query) else {
                continue;
            };
            line_matched = true;
            if first.is_none() {
                first = Some((role, snippet_around(&text, start, query.len())));
            }
        }
        if line_matched {
            matches += 1;
        }
    }
    Ok(first.map(|(role, snippet)| FileMatches {
        role,
        snippet,
        matches,
    }))
}

/// Ripgrep pass over one root: the FILE NAMES of rollouts containing the query
/// anywhere in their raw bytes (a superset of the visible-text matches the
/// precise pass then confirms). `None` means the accelerator is unusable here
/// (missing or unspawnable binary) and the caller must scan every file;
/// a timeout is a real failure and propagates.
pub(crate) async fn ripgrep_candidate_names(
    process_context: &RuntimeProcessContext,
    root: &str,
    query: &str,
    signal: &AbortSignal,
) -> io::Result<Option<HashSet<String>>> {
    let args: Vec<RuntimeString> = [
        "--files-with-matches",
        "--fixed-strings",
        "--ignore-case",
        "--no-messages",
        "--glob",
        "*.jsonl",
        "--",
        query,
    ]
    .into_iter()
    .map(RuntimeString::from)
    .collect();
    let target = RuntimePath::from(RuntimeString::from(root));
    match run_ripgrep(
        process_context,
        &args,
        std::slice::from_ref(&target),
        signal,
    )
    .await
    {
        Ok(lines) => Ok(Some(
            lines
                .iter()
                .filter_map(|line| {
                    let path = line.try_to_utf8().ok()?;
                    let name = path.rsplit(['/', '\\']).next()?.to_string();
                    (!name.is_empty()).then_some(name)
                })
                .collect(),
        )),
        // No accelerator — the caller degrades to scanning every file.
        Err(RipgrepError::Unavailable(_)) | Err(RipgrepError::Spawn(_)) => Ok(None),
        Err(error @ RipgrepError::Timeout { .. }) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("session search: ripgrep candidate pass timed out: {error}"),
        )),
    }
}

/// File name of a path (the segment after the last separator).
pub(crate) fn file_name_of(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_ci_prefers_exact_then_ascii_case_insensitive() {
        assert_eq!(find_ci("Deadlock in store", "Deadlock"), Some(0));
        assert_eq!(find_ci("saw a DEADLOCK today", "deadlock"), Some(6));
        // Non-ASCII must match exactly.
        assert_eq!(find_ci("修复了死锁问题", "死锁"), Some(9));
        assert_eq!(find_ci("no such thing", "deadlock"), None);
        assert_eq!(find_ci("anything", ""), None);
        // Multi-byte neighbors never panic the window slicing.
        assert_eq!(find_ci("😀ABC😀", "abc"), Some(4));
    }

    #[test]
    fn snippets_are_single_line_and_ellipsized() {
        let text = format!("{}needle{}", "a".repeat(200), "b\nc".repeat(100));
        let start = find_ci(&text, "needle").unwrap();
        let snippet = snippet_around(&text, start, "needle".len());
        assert!(snippet.starts_with('…'));
        assert!(snippet.ends_with('…'));
        assert!(snippet.contains("needle"));
        assert!(!snippet.contains('\n'));

        let short = "tiny";
        assert_eq!(snippet_around(short, 0, 4), "tiny");
    }

    #[test]
    fn raw_prefilter_soundness_reflects_json_escaping() {
        assert!(raw_prefilter_is_sound("plain ascii query"));
        assert!(!raw_prefilter_is_sound("with \"quote"));
        assert!(!raw_prefilter_is_sound("back\\slash"));
        assert!(!raw_prefilter_is_sound("死锁"));
        assert!(!raw_prefilter_is_sound("tab\there"));
    }
}
