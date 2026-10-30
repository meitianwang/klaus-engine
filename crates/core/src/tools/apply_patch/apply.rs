//! Applier for parsed `apply_patch` ops. Implements the patch application
//! algorithm for the `*** Begin Patch` format.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::runtime_values::RuntimeString;

use super::parser::{Hunk, HunkChangeType, PatchOp};

/// Filesystem operations the applier depends on. Injectable so tests can run
/// against an in-memory map with no real filesystem.
#[async_trait::async_trait]
pub trait FileIO: Send + Sync {
    /// Return file contents, or `None` if the file does not exist.
    async fn read(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError>;
    /// Read inspection content without changing write metadata.
    async fn snapshot(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError> {
        self.read(path).await
    }
    /// Write `content` to `path`. `from` (set only on a rename) names the file
    /// the content was READ from, so the implementation can carry that source
    /// file's encoding + line endings to the new destination — otherwise a
    /// renamed utf16le/CRLF file would silently downgrade to utf8/LF.
    async fn write(
        &self,
        path: &RuntimeString,
        content: &RuntimeString,
        from: Option<&RuntimeString>,
    ) -> Result<(), ApplyError>;
    async fn remove(&self, path: &RuntimeString) -> Result<(), ApplyError>;

    /// Whether the most recent failed write for `path` crossed into the real
    /// OS mutation call. The default is conservative for external/custom
    /// implementations; the production adapter distinguishes preflight
    /// rejection from an indeterminate filesystem error exactly.
    fn write_error_may_have_mutated(&self, _path: &RuntimeString) -> bool {
        true
    }

    /// Remove-side equivalent of [`Self::write_error_may_have_mutated`].
    fn remove_error_may_have_mutated(&self, _path: &RuntimeString) -> bool {
        true
    }
}

/// Error raised while applying patch ops (missing files, unlocatable context).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyError {
    pub message: RuntimeString,
}

impl ApplyError {
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        ApplyError {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.message.to_node_utf8())
                .expect("Node UTF-8 apply error is valid UTF-8")
        )
    }
}

impl std::error::Error for ApplyError {}

/// The kind of op an [`ApplyResult`] describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyKind {
    Add,
    Update,
    Delete,
}

/// Outcome of applying a single op.
///
/// - `exact` is false when any hunk for the file matched only via fuzzy
///   (whitespace/unicode-normalized) fallback rather than a verbatim match.
/// - `added`/`removed` count the net inserted and deleted physical lines for the
///   file (an Add reports every line as added; a Delete reports every line as
///   removed; an Update reports the lines its chunks insert/remove).
/// - `move_path` is set for an Update that renamed the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub path: RuntimeString,
    pub kind: ApplyKind,
    pub exact: bool,
    pub added: usize,
    pub removed: usize,
    pub move_path: Option<RuntimeString>,
    /// The exact mutation inputs, retained for complete edit inspection.
    pub before_content: Option<RuntimeString>,
    pub after_content: Option<RuntimeString>,
    /// Readable source content before mutation; empty means a missing file.
    pub turn_before_content: Option<RuntimeString>,
    pub turn_before_exists: bool,
    /// Readable rename destination content before mutation, including empty.
    pub move_before_content: Option<RuntimeString>,
    pub move_before_exists: bool,
}

/// Internal progress retained when a multi-operation patch fails. The public
/// `ApplyError` shape stays source-compatible; only the tool adapter consumes
/// this detail to repair LSP state after a partial filesystem outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApplyOpsFailure {
    pub(crate) error: ApplyError,
    pub(crate) completed: Vec<ApplyResult>,
    pub(crate) reconcile_paths: Vec<RuntimeString>,
}

fn failure(
    error: ApplyError,
    completed: &[ApplyResult],
    reconcile_paths: impl IntoIterator<Item = RuntimeString>,
) -> ApplyOpsFailure {
    let mut unique_paths = Vec::new();
    for path in reconcile_paths {
        if !unique_paths.iter().any(|candidate| candidate == &path) {
            unique_paths.push(path);
        }
    }
    ApplyOpsFailure {
        error,
        completed: completed.to_vec(),
        reconcile_paths: unique_paths,
    }
}

/// Apply parsed patch ops against `io`, in order.
///
/// - Add writes the joined lines (mkdir handled by the `FileIO`).
/// - Delete removes an existing file (errors if missing).
/// - Update reads the file, locates each chunk (context header first, then the
///   removed/context lines via fuzzy [`seek_sequence`]), splices the edits, and
///   writes the result — renaming to `move_path` when present.
///
/// Returns a descriptive [`ApplyError`] if a file is missing or a chunk's
/// context cannot be located.
pub async fn apply_patch_ops(
    ops: &[PatchOp],
    io: &dyn FileIO,
) -> Result<Vec<ApplyResult>, ApplyError> {
    apply_patch_ops_tracked(ops, io)
        .await
        .map_err(|failure| failure.error)
}

pub(crate) async fn apply_patch_ops_tracked(
    ops: &[PatchOp],
    io: &dyn FileIO,
) -> Result<Vec<ApplyResult>, ApplyOpsFailure> {
    let mut results: Vec<ApplyResult> = Vec::new();
    for op in ops {
        match op {
            PatchOp::Add { path, lines } => {
                let content = add_file_contents(lines);
                let snapshot = io.snapshot(path).await.ok();
                let turn_before_exists = snapshot.as_ref().is_none_or(|content| content.is_some());
                let turn_before_content = snapshot.map(|content| content.unwrap_or_default());
                if let Err(error) = io.write(path, &content, None).await {
                    let paths = io.write_error_may_have_mutated(path).then(|| path.clone());
                    return Err(failure(error, &results, paths));
                }
                results.push(ApplyResult {
                    turn_before_content,
                    turn_before_exists,
                    move_before_content: None,
                    move_before_exists: false,
                    path: path.clone(),
                    kind: ApplyKind::Add,
                    before_content: None,
                    after_content: Some(content.clone()),
                    exact: true,
                    added: lines.len(),
                    removed: 0,
                    move_path: None,
                });
            }
            PatchOp::Delete { path } => {
                let existing = io
                    .read(path)
                    .await
                    .map_err(|error| failure(error, &results, []))?;
                let existing = match existing {
                    Some(e) => e,
                    None => {
                        return Err(failure(
                            ApplyError::new(
                                RuntimeString::from("apply_patch: cannot delete missing file: ")
                                    .concat(path),
                            ),
                            &results,
                            [],
                        ))
                    }
                };
                if let Err(error) = io.remove(path).await {
                    let paths = io.remove_error_may_have_mutated(path).then(|| path.clone());
                    return Err(failure(error, &results, paths));
                }
                results.push(ApplyResult {
                    turn_before_content: Some(existing.clone()),
                    turn_before_exists: true,
                    move_before_content: None,
                    move_before_exists: false,
                    path: path.clone(),
                    kind: ApplyKind::Delete,
                    before_content: Some(existing.clone()),
                    after_content: None,
                    exact: true,
                    added: 0,
                    removed: count_lines(&existing),
                    move_path: None,
                });
            }
            PatchOp::Update {
                path,
                move_path,
                hunks,
            } => {
                let original = io
                    .read(path)
                    .await
                    .map_err(|error| failure(error, &results, []))?;
                let original = match original {
                    Some(o) => o,
                    None => {
                        return Err(failure(
                            ApplyError::new(
                                RuntimeString::from("apply_patch: cannot update missing file: ")
                                    .concat(path),
                            ),
                            &results,
                            [],
                        ))
                    }
                };
                let derived = derive_new_contents(path, &original, hunks)
                    .map_err(|error| failure(error, &results, []))?;

                match move_path {
                    Some(mp) if mp != path => {
                        let snapshot = io.snapshot(mp).await.ok();
                        let move_before_exists =
                            snapshot.as_ref().is_none_or(|content| content.is_some());
                        let move_before_content =
                            snapshot.map(|content| content.unwrap_or_default());
                        // Carry the source file's encoding + line endings to the
                        // rename destination (`from`).
                        if let Err(error) = io.write(mp, &derived.contents, Some(path)).await {
                            let paths = io.write_error_may_have_mutated(mp).then(|| mp.clone());
                            return Err(failure(error, &results, paths));
                        }
                        if let Err(error) = io.remove(path).await {
                            // The destination write definitely completed even
                            // if the source-remove failure was preflight-only.
                            let mut paths = Vec::new();
                            if io.remove_error_may_have_mutated(path) {
                                paths.push(path.clone());
                            }
                            paths.push(mp.clone());
                            return Err(failure(error, &results, paths));
                        }
                        results.push(ApplyResult {
                            turn_before_content: Some(original.clone()),
                            turn_before_exists: true,
                            move_before_content,
                            move_before_exists,
                            path: path.clone(),
                            kind: ApplyKind::Update,
                            before_content: Some(original.clone()),
                            after_content: Some(derived.contents.clone()),
                            exact: derived.exact,
                            added: derived.added,
                            removed: derived.removed,
                            move_path: Some(mp.clone()),
                        });
                    }
                    _ => {
                        if let Err(error) = io.write(path, &derived.contents, None).await {
                            let paths = io.write_error_may_have_mutated(path).then(|| path.clone());
                            return Err(failure(error, &results, paths));
                        }
                        results.push(ApplyResult {
                            turn_before_content: Some(original.clone()),
                            turn_before_exists: true,
                            move_before_content: None,
                            move_before_exists: false,
                            path: path.clone(),
                            kind: ApplyKind::Update,
                            before_content: Some(original.clone()),
                            after_content: Some(derived.contents.clone()),
                            exact: derived.exact,
                            added: derived.added,
                            removed: derived.removed,
                            move_path: None,
                        });
                    }
                }
            }
        }
    }
    Ok(results)
}

/// Join the body lines of an `Add File` hunk into the file contents. A
/// trailing newline is appended after every added line, so the resulting file
/// ends in a newline.
fn add_file_contents(lines: &[RuntimeString]) -> RuntimeString {
    if lines.is_empty() {
        RuntimeString::default()
    } else {
        join_runtime(lines, "\n").concat(&RuntimeString::from("\n"))
    }
}

/// Count the lines a file's text represents (a trailing newline is not a line).
fn count_lines(content: &RuntimeString) -> usize {
    if content.is_empty() {
        return 0;
    }
    let mut parts = split_lines(content);
    if parts.last().is_some_and(RuntimeString::is_empty) {
        parts.pop();
    }
    parts.len()
}

struct Replacement {
    /// Index in the original (newline-stripped) line array where removal starts.
    start: usize,
    /// How many original lines to remove.
    old_len: usize,
    /// Replacement lines to insert.
    new_lines: Vec<RuntimeString>,
    /// Whether the located old region matched verbatim.
    #[allow(dead_code)]
    exact: bool,
}

struct DerivedContents {
    contents: RuntimeString,
    exact: bool,
    added: usize,
    removed: usize,
}

/// Compute the new file contents after applying `hunks`. The algorithm
/// proceeds in four steps:
///
///  1. Split the file on `\n`, dropping the trailing empty element so line
///     counts match standard `diff` behaviour.
///  2. Compute the replacement regions (context-anchored, in order).
///  3. Apply them in descending order so earlier edits don't shift later
///     indices.
///  4. Re-append a trailing newline if the result lacks one.
fn derive_new_contents(
    path: &RuntimeString,
    original: &RuntimeString,
    hunks: &[Hunk],
) -> Result<DerivedContents, ApplyError> {
    let mut original_lines = split_lines(original);
    if original_lines.last().is_some_and(RuntimeString::is_empty) {
        original_lines.pop();
    }

    let (replacements, exact) = compute_replacements(path, &original_lines, hunks)?;

    let mut added = 0usize;
    let mut removed = 0usize;
    for r in &replacements {
        added += r.new_lines.len();
        removed += r.old_len;
    }

    let mut new_lines = apply_replacements(&original_lines, &replacements);
    if !new_lines.last().is_some_and(RuntimeString::is_empty) {
        new_lines.push(RuntimeString::default());
    }
    Ok(DerivedContents {
        contents: join_runtime(&new_lines, "\n"),
        exact,
        added,
        removed,
    })
}

/// Locate every chunk against the original lines, returning the regions to
/// replace.
fn compute_replacements(
    path: &RuntimeString,
    original_lines: &[RuntimeString],
    hunks: &[Hunk],
) -> Result<(Vec<Replacement>, bool), ApplyError> {
    let mut replacements: Vec<Replacement> = Vec::new();
    let mut line_index = 0usize;
    let mut all_exact = true;

    for hunk in hunks {
        if let Some(ctx_header) = &hunk.context_header {
            if !ctx_header.is_empty() {
                // The `@@ <context>` line is sought literally and we advance
                // past it. We treat it as a best-effort anchor: when the context
                // line genuinely exists in the file we narrow the search to
                // start after it, but when the header is merely
                // a human label that is not a real file line we fall back to
                // anchoring on the chunk's own context/removed lines rather than
                // failing the whole patch.
                let hit = seek_sequence(
                    original_lines,
                    std::slice::from_ref(ctx_header),
                    line_index,
                    false,
                );
                if let Some(hit) = hit {
                    if !hit.exact {
                        all_exact = false;
                    }
                    line_index = hit.index + 1;
                }
            }
        }

        let old_lines: Vec<RuntimeString> = hunk
            .changes
            .iter()
            .filter(|c| c.r#type == HunkChangeType::Keep || c.r#type == HunkChangeType::Remove)
            .map(|c| c.text.clone())
            .collect();
        let new_lines: Vec<RuntimeString> = hunk
            .changes
            .iter()
            .filter(|c| c.r#type == HunkChangeType::Keep || c.r#type == HunkChangeType::Add)
            .map(|c| c.text.clone())
            .collect();
        let is_eof = hunk.is_end_of_file;

        if old_lines.is_empty() {
            // Pure addition: insert at end of file (before a trailing blank line
            // if one is present in `original_lines`).
            let insertion_idx = if original_lines.last().is_some_and(RuntimeString::is_empty) {
                original_lines.len() - 1
            } else {
                original_lines.len()
            };
            replacements.push(Replacement {
                start: insertion_idx,
                old_len: 0,
                new_lines,
                exact: true,
            });
            continue;
        }

        let mut pattern = old_lines.clone();
        let mut replacement = new_lines.clone();
        let mut hit = seek_sequence(original_lines, &pattern, line_index, is_eof);

        if hit.is_none() && pattern.last().is_some_and(RuntimeString::is_empty) {
            // The pattern ends with the sentinel blank line that stands in for
            // the file's terminating newline; that sentinel is not present in
            // `original_lines`. Retry without it (and drop the matching trailing
            // blank from the replacement so the result stays balanced).
            pattern.pop();
            if replacement.last().is_some_and(RuntimeString::is_empty) {
                replacement.pop();
            }
            hit = seek_sequence(original_lines, &pattern, line_index, is_eof);
        }

        let hit = match hit {
            Some(h) => h,
            None => {
                let ctx = match &hunk.context_header {
                    Some(h) if !h.is_empty() => RuntimeString::from(" (context: ")
                        .concat(h)
                        .concat(&RuntimeString::from(")")),
                    _ => RuntimeString::default(),
                };
                return Err(ApplyError::new(
                    RuntimeString::from("apply_patch: could not locate context for a hunk in ")
                        .concat(path)
                        .concat(&ctx)
                        .concat(&RuntimeString::from("; missing lines:\n"))
                        .concat(&join_runtime(&old_lines, "\n")),
                ));
            }
        };
        if !hit.exact {
            all_exact = false;
        }

        let pattern_len = pattern.len();
        replacements.push(Replacement {
            start: hit.index,
            old_len: pattern_len,
            new_lines: replacement,
            exact: hit.exact,
        });
        line_index = hit.index + pattern_len;
    }

    // Sort by start index so descending application below is well-defined even
    // when chunks were authored out of file order.
    replacements.sort_by(|a, b| a.start.cmp(&b.start));
    Ok((replacements, all_exact))
}

/// Apply `(start, old_len, new_lines)` replacements to `lines`, returning the
/// new line array. Applied in descending order of `start` so earlier
/// replacements do not shift the indices of later ones.
fn apply_replacements(lines: &[RuntimeString], replacements: &[Replacement]) -> Vec<RuntimeString> {
    let mut out: Vec<RuntimeString> = lines.to_vec();
    for rep in replacements.iter().rev() {
        let start = rep.start;
        // Clamp removal to what remains: min(old_len, out.len().saturating_sub(start)).
        let avail = out.len().saturating_sub(start);
        let remove = rep.old_len.min(avail);
        let clamped_start = start.min(out.len());
        let end = (clamped_start + remove).min(out.len());
        out.splice(clamped_start..end, rep.new_lines.iter().cloned());
    }
    out
}

/// Which escalating pass located a match. Lower = stricter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekPass {
    Exact,
    Rstrip,
    Trim,
    Normalize,
}

type SeekPassPredicate = fn(&RuntimeString, &RuntimeString) -> bool;

/// A located match from [`seek_sequence`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeekHit {
    pub index: usize,
    /// True only when the verbatim (pass 1) match succeeded.
    pub exact: bool,
    /// Which pass produced the match (for diagnostics/telemetry).
    pub pass: SeekPass,
}

/// Find where `pattern` occurs in `lines`, starting at or after `from`, using
/// escalating fuzzy passes:
///
///  1. exact, full-string equality;
///  2. ignoring trailing whitespace on each line;
///  3. ignoring leading and trailing whitespace on each line;
///  4. normalizing common Unicode punctuation (dashes, smart quotes) and odd
///     spaces (NBSP, en/em spaces, etc.) to their ASCII equivalents, then
///     trimming — so ASCII-authored diffs apply to typographically-rich source.
///
/// When `eof` is true the search starts at the position where `pattern` would
/// align with the end of the file (so end-of-file edits are anchored at the
/// tail), falling back to `from` only implicitly via the chosen `search_start`.
///
/// Returns the match index plus an `exact` flag (true only for pass 1) and the
/// `pass` that matched, or `None` if nothing matches. An empty `pattern` is a
/// no-op match at `from`; a `pattern` longer than `lines` cannot match.
pub fn seek_sequence(
    lines: &[RuntimeString],
    pattern: &[RuntimeString],
    from: usize,
    eof: bool,
) -> Option<SeekHit> {
    if pattern.is_empty() {
        return Some(SeekHit {
            index: from.min(lines.len()),
            exact: true,
            pass: SeekPass::Exact,
        });
    }
    if pattern.len() > lines.len() {
        return None;
    }

    let search_start = if eof && lines.len() >= pattern.len() {
        lines.len() - pattern.len()
    } else {
        from
    };
    // last_start = lines.len() - pattern.len(); guaranteed >= 0 here since
    // pattern.len() <= lines.len().
    let last_start = lines.len() - pattern.len();

    let passes: [(SeekPass, SeekPassPredicate); 4] = [
        (SeekPass::Exact, exact_eq),
        (SeekPass::Rstrip, rstrip_eq),
        (SeekPass::Trim, trim_eq),
        (SeekPass::Normalize, normalize_eq),
    ];

    for (name, eq) in passes {
        let mut i = search_start;
        while i <= last_start {
            if matches_at(lines, pattern, i, eq) {
                return Some(SeekHit {
                    index: i,
                    exact: name == SeekPass::Exact,
                    pass: name,
                });
            }
            i += 1;
        }
    }

    None
}

fn matches_at(
    haystack: &[RuntimeString],
    pattern: &[RuntimeString],
    start: usize,
    eq: SeekPassPredicate,
) -> bool {
    for j in 0..pattern.len() {
        let h = haystack.get(start + j);
        let p = pattern.get(j);
        match (h, p) {
            (Some(h), Some(p)) if eq(h, p) => {}
            _ => return false,
        }
    }
    true
}

fn exact_eq(left: &RuntimeString, right: &RuntimeString) -> bool {
    left == right
}

fn rstrip_eq(left: &RuntimeString, right: &RuntimeString) -> bool {
    trim_end_runtime(left) == trim_end_runtime(right)
}

fn trim_eq(left: &RuntimeString, right: &RuntimeString) -> bool {
    trim_runtime(left) == trim_runtime(right)
}

fn normalize_eq(left: &RuntimeString, right: &RuntimeString) -> bool {
    normalize(left) == normalize(right)
}

/// Match JavaScript's trailing whitespace stripping directly over UTF-16.
fn trim_end_runtime(value: &RuntimeString) -> RuntimeString {
    let units = value.as_utf16();
    let end = units
        .iter()
        .rposition(|unit| !is_ecmascript_whitespace(*unit))
        .map_or(0, |index| index + 1);
    value.slice_utf16(..end)
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

/// Normalize a line for the most permissive matching pass: trim, then fold
/// common Unicode dashes, smart quotes, and exotic spaces to their ASCII
/// equivalents.
fn normalize(value: &RuntimeString) -> RuntimeString {
    let mut out = Vec::new();
    for point in trim_runtime(value).code_points() {
        match FOLD.get(&point) {
            Some(replacement) => out.extend(replacement.encode_utf16()),
            None => match char::from_u32(point) {
                Some(scalar) => {
                    let mut encoded = [0_u16; 2];
                    out.extend_from_slice(scalar.encode_utf16(&mut encoded));
                }
                // Runtime code-point iteration deliberately exposes an unpaired
                // surrogate as its raw code-unit value. Preserve it exactly.
                None => out.push(point as u16),
            },
        }
    }
    RuntimeString::from_utf16(out)
}

fn split_lines(value: &RuntimeString) -> Vec<RuntimeString> {
    let units = value.as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            output.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    output.push(RuntimeString::from_utf16(units[start..].to_vec()));
    output
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    if values.is_empty() {
        return RuntimeString::default();
    }
    let separator = separator.encode_utf16().collect::<Vec<_>>();
    let content_len = values.iter().map(RuntimeString::len_utf16).sum::<usize>();
    let mut output = Vec::with_capacity(
        content_len
            + separator
                .len()
                .saturating_mul(values.len().saturating_sub(1)),
    );
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.extend_from_slice(&separator);
        }
        output.extend_from_slice(value.as_utf16());
    }
    RuntimeString::from_utf16(output)
}

/// Code points folded to an ASCII equivalent in the most permissive
/// matching pass: assorted dashes/hyphens
/// to "-", typographic single quotes to "'", typographic double quotes to the
/// double-quote character, and non-breaking/exotic spaces to a plain space.
static FOLD: LazyLock<HashMap<u32, &'static str>> = LazyLock::new(|| {
    let mut m: HashMap<u32, &'static str> = HashMap::new();
    // Dash / hyphen code points -> ASCII "-"
    for cp in [0x2010, 0x2011, 0x2012, 0x2013, 0x2014, 0x2015, 0x2212] {
        m.insert(cp, "-");
    }
    // Fancy single quotes -> "'"
    for cp in [0x2018, 0x2019, 0x201a, 0x201b] {
        m.insert(cp, "'");
    }
    // Fancy double quotes -> double-quote character
    for cp in [0x201c, 0x201d, 0x201e, 0x201f] {
        m.insert(cp, "\"");
    }
    // Non-breaking space and other odd spaces -> normal space
    for cp in [
        0x00a0, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x202f,
        0x205f, 0x3000,
    ] {
        m.insert(cp, " ");
    }
    m
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::apply_patch::parser::{self, PatchParseError};
    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex;

    /// An in-memory FileIO backed by a Map, so no real filesystem is touched.
    struct MemIO {
        files: Mutex<StdHashMap<RuntimeString, RuntimeString>>,
    }

    impl MemIO {
        fn new(initial: &[(&str, &str)]) -> Self {
            let mut m = StdHashMap::new();
            for (k, v) in initial {
                m.insert(RuntimeString::from(*k), RuntimeString::from(*v));
            }
            MemIO {
                files: Mutex::new(m),
            }
        }
        fn get(&self, path: &str) -> Option<String> {
            self.get_exact(path).map(|value| {
                String::from_utf8(value.to_node_utf8())
                    .expect("Node UTF-8 test content is valid UTF-8")
            })
        }
        fn get_exact(&self, path: &str) -> Option<RuntimeString> {
            self.files
                .lock()
                .unwrap()
                .get(&RuntimeString::from(path))
                .cloned()
        }
        fn has(&self, path: &str) -> bool {
            self.files
                .lock()
                .unwrap()
                .contains_key(&RuntimeString::from(path))
        }
    }

    #[async_trait::async_trait]
    impl FileIO for MemIO {
        async fn read(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError> {
            Ok(self.files.lock().unwrap().get(path).cloned())
        }
        async fn write(
            &self,
            path: &RuntimeString,
            content: &RuntimeString,
            _from: Option<&RuntimeString>,
        ) -> Result<(), ApplyError> {
            self.files
                .lock()
                .unwrap()
                .insert(path.clone(), content.clone());
            Ok(())
        }
        async fn remove(&self, path: &RuntimeString) -> Result<(), ApplyError> {
            self.files.lock().unwrap().remove(path);
            Ok(())
        }
    }

    fn parse_patch(text: impl Into<RuntimeString>) -> Result<Vec<PatchOp>, PatchParseError> {
        let text = text.into();
        parser::parse_patch(&text)
    }

    fn wrap(body: &[&str]) -> String {
        let mut lines = vec!["*** Begin Patch".to_string()];
        lines.extend(body.iter().map(|s| s.to_string()));
        lines.push("*** End Patch".to_string());
        lines.join("\n")
    }

    // --- apply_patch parser + applier ---

    #[tokio::test]
    async fn adds_a_new_file() {
        let patch = [
            "*** Begin Patch",
            "*** Add File: greeting.txt",
            "+hello",
            "+world",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[]);
        let results = apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap();
        assert_eq!(
            results,
            vec![ApplyResult {
                turn_before_content: Some(RuntimeString::default()),
                turn_before_exists: false,
                move_before_content: None,
                move_before_exists: false,
                path: RuntimeString::from("greeting.txt"),
                before_content: None,
                after_content: Some("hello\nworld\n".into()),
                kind: ApplyKind::Add,
                exact: true,
                added: 2,
                removed: 0,
                move_path: None,
            }]
        );
        assert_eq!(io.get("greeting.txt").as_deref(), Some("hello\nworld\n"));
    }

    #[tokio::test]
    async fn updates_a_file_with_an_exact_match_hunk() {
        let patch = [
            "*** Begin Patch",
            "*** Update File: code.txt",
            "@@ fn",
            " const a = 1",
            "-const b = 2",
            "+const b = 22",
            " const c = 3",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[("code.txt", "const a = 1\nconst b = 2\nconst c = 3")]);
        let results = apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap();
        assert_eq!(
            results[0],
            ApplyResult {
                turn_before_content: Some("const a = 1\nconst b = 2\nconst c = 3".into()),
                turn_before_exists: true,
                move_before_content: None,
                move_before_exists: false,
                path: RuntimeString::from("code.txt"),
                before_content: Some("const a = 1\nconst b = 2\nconst c = 3".into()),
                after_content: Some("const a = 1\nconst b = 22\nconst c = 3\n".into()),
                kind: ApplyKind::Update,
                exact: true,
                added: 3,
                removed: 3,
                move_path: None,
            }
        );
        assert_eq!(
            io.get("code.txt").as_deref(),
            Some("const a = 1\nconst b = 22\nconst c = 3\n")
        );
    }

    #[tokio::test]
    async fn updates_a_file_via_fuzzy_match_when_context_has_extra_trailing_whitespace() {
        let patch = [
            "*** Begin Patch",
            "*** Update File: code.txt",
            " const a = 1",
            "-const b = 2",
            "+const b = 22",
            " const c = 3",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[("code.txt", "const a = 1   \nconst b = 2\t\nconst c = 3  ")]);
        let results = apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap();
        assert_eq!(results[0].kind, ApplyKind::Update);
        assert!(!results[0].exact);
        assert_eq!(
            io.get("code.txt").as_deref(),
            Some("const a = 1\nconst b = 22\nconst c = 3\n")
        );
    }

    #[tokio::test]
    async fn applies_multiple_hunks_in_order() {
        let patch = [
            "*** Begin Patch",
            "*** Update File: m.txt",
            "@@",
            " one",
            "-two",
            "+TWO",
            "@@",
            " four",
            "-five",
            "+FIVE",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[("m.txt", "one\ntwo\nthree\nfour\nfive\nsix")]);
        apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap();
        assert_eq!(
            io.get("m.txt").as_deref(),
            Some("one\nTWO\nthree\nfour\nFIVE\nsix\n")
        );
    }

    #[tokio::test]
    async fn deletes_a_file() {
        let patch = [
            "*** Begin Patch",
            "*** Delete File: gone.txt",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[("gone.txt", "bye")]);
        let results = apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap();
        assert_eq!(
            results,
            vec![ApplyResult {
                turn_before_content: Some("bye".into()),
                turn_before_exists: true,
                move_before_content: None,
                move_before_exists: false,
                path: RuntimeString::from("gone.txt"),
                before_content: Some("bye".into()),
                after_content: None,
                kind: ApplyKind::Delete,
                exact: true,
                added: 0,
                removed: 1,
                move_path: None,
            }]
        );
        assert!(!io.has("gone.txt"));
    }

    #[tokio::test]
    async fn throws_when_an_update_hunk_context_cannot_be_located() {
        let patch = [
            "*** Begin Patch",
            "*** Update File: code.txt",
            " this context",
            "-does not exist",
            "+replacement",
            "*** End Patch",
        ]
        .join("\n");
        let io = MemIO::new(&[("code.txt", "completely\ndifferent\ncontent")]);
        let err = apply_patch_ops(&parse_patch(&patch).unwrap(), &io)
            .await
            .unwrap_err();
        assert!(err.message.contains("could not locate context"));
    }

    // --- applier: patch application ---

    #[tokio::test]
    async fn add_file_writes_contents_with_a_trailing_newline() {
        let io = MemIO::new(&[]);
        apply_patch_ops(
            &parse_patch(wrap(&["*** Add File: a/b/c.txt", "+x", "+y"])).unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("a/b/c.txt").as_deref(), Some("x\ny\n"));
    }

    #[tokio::test]
    async fn applies_multiple_chunks_anchored_by_their_context_lines() {
        let io = MemIO::new(&[("multi.txt", "foo\nbar\nbaz\nqux\n")]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: multi.txt",
                "@@",
                " foo",
                "-bar",
                "+BAR",
                "@@",
                " baz",
                "-qux",
                "+QUX",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("multi.txt").as_deref(), Some("foo\nBAR\nbaz\nQUX\n"));
    }

    #[tokio::test]
    async fn handles_interleaved_replace_replace_append_at_eof_chunks() {
        let io = MemIO::new(&[("i.txt", "a\nb\nc\nd\ne\nf\n")]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: i.txt",
                "@@",
                " a",
                "-b",
                "+B",
                "@@",
                " c",
                " d",
                "-e",
                "+E",
                "@@",
                " f",
                "+g",
                "*** End of File",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("i.txt").as_deref(), Some("a\nB\nc\nd\nE\nf\ng\n"));
    }

    #[tokio::test]
    async fn handles_a_pure_addition_chunk_followed_by_a_multi_line_removal() {
        let io = MemIO::new(&[("panic.txt", "line1\nline2\nline3\n")]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: panic.txt",
                "@@",
                "+after-context",
                "+second-line",
                "@@",
                " line1",
                "-line2",
                "-line3",
                "+line2-replacement",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(
            io.get("panic.txt").as_deref(),
            Some("line1\nline2-replacement\nafter-context\nsecond-line\n")
        );
    }

    #[tokio::test]
    async fn inserts_a_pure_addition_chunk_before_the_trailing_newline() {
        let io = MemIO::new(&[("end.txt", "one\ntwo\n")]);
        apply_patch_ops(
            &parse_patch(wrap(&["*** Update File: end.txt", "@@", "+three"])).unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("end.txt").as_deref(), Some("one\ntwo\nthree\n"));
    }

    #[tokio::test]
    async fn replaces_the_first_line_of_a_file() {
        let io = MemIO::new(&[("f.txt", "old\nkeep\n")]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: f.txt",
                "@@",
                "-old",
                "+new",
                " keep",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("f.txt").as_deref(), Some("new\nkeep\n"));
    }

    #[tokio::test]
    async fn locates_an_end_of_file_edit_even_when_the_file_lacks_a_final_newline() {
        let io = MemIO::new(&[("noeol.txt", "a\nb\nlast")]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: noeol.txt",
                "@@",
                " b",
                "-last",
                "+LAST",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("noeol.txt").as_deref(), Some("a\nb\nLAST\n"));
    }

    #[tokio::test]
    async fn renames_a_file_via_move_to_and_removes_the_original() {
        let io = MemIO::new(&[("old.txt", "hi\nthere\n")]);
        let results = apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: old.txt",
                "*** Move to: new.txt",
                "@@",
                "-hi",
                "+HI",
                " there",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert!(!io.has("old.txt"));
        assert_eq!(io.get("new.txt").as_deref(), Some("HI\nthere\n"));
        assert_eq!(results[0].kind, ApplyKind::Update);
        assert_eq!(results[0].path, "old.txt");
        assert_eq!(
            results[0].move_path.as_ref(),
            Some(&RuntimeString::from("new.txt"))
        );
    }

    #[tokio::test]
    async fn reports_accurate_added_removed_counts() {
        let io = MemIO::new(&[("count.txt", "a\nb\nc\nd\n")]);
        let results = apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: count.txt",
                "@@",
                " a",
                "-b",
                "-c",
                "+B",
                " d",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        // old region = [a, b, c, d] (4); new region = [a, B, d] (3)
        assert_eq!(results[0].added, 3);
        assert_eq!(results[0].removed, 4);
    }

    #[tokio::test]
    async fn applies_a_pure_addition_to_an_empty_file() {
        let io = MemIO::new(&[("empty.txt", "")]);
        apply_patch_ops(
            &parse_patch(wrap(&["*** Update File: empty.txt", "@@", "+first"])).unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("empty.txt").as_deref(), Some("first\n"));
    }

    // --- unicode and whitespace tolerance ---

    #[tokio::test]
    async fn matches_an_ascii_patch_against_typographic_dashes_nb_hyphen_in_the_file() {
        let original = "import asyncio  # local import \u{2013} avoids top\u{2011}level dep\n";
        assert!(original.contains('\u{2013}'));
        assert!(original.contains('\u{2011}'));
        let io = MemIO::new(&[("u.py", original)]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: u.py",
                "@@",
                "-import asyncio  # local import - avoids top-level dep",
                "+import asyncio  # HELLO",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("u.py").as_deref(), Some("import asyncio  # HELLO\n"));
    }

    #[tokio::test]
    async fn matches_a_non_breaking_space_in_the_file_against_an_ascii_space_in_the_patch() {
        let file_line = "foo\u{00A0}bar";
        assert!(file_line.contains('\u{00A0}'));
        let io = MemIO::new(&[("nbsp.txt", &format!("{file_line}\n"))]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: nbsp.txt",
                "@@",
                "-foo bar",
                "+ok",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("nbsp.txt").as_deref(), Some("ok\n"));
    }

    #[tokio::test]
    async fn matches_smart_quotes_in_the_file_against_straight_quotes_in_the_patch() {
        let file_line = "\u{201C}quoted\u{201D} line";
        assert!(file_line.contains('\u{201C}'));
        let io = MemIO::new(&[("q.txt", &format!("{file_line}\n"))]);
        apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: q.txt",
                "@@",
                "-\"quoted\" line",
                "+done",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert_eq!(io.get("q.txt").as_deref(), Some("done\n"));
    }

    #[tokio::test]
    async fn marks_a_fuzzy_non_exact_match_when_only_trailing_whitespace_differs() {
        let io = MemIO::new(&[("w.txt", "alpha   \nbeta\n")]);
        let results = apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: w.txt",
                "@@",
                " alpha",
                "-beta",
                "+BETA",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert!(!results[0].exact);
    }

    #[tokio::test]
    async fn keeps_exact_true_when_the_match_is_byte_for_byte() {
        let io = MemIO::new(&[("e.txt", "alpha\nbeta\n")]);
        let results = apply_patch_ops(
            &parse_patch(wrap(&[
                "*** Update File: e.txt",
                "@@",
                " alpha",
                "-beta",
                "+BETA",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap();
        assert!(results[0].exact);
    }

    // --- failure modes ---

    #[tokio::test]
    async fn failure_retains_completed_operations_for_authoritative_reconciliation() {
        let io = MemIO::new(&[("gone.txt", "gone\n")]);
        let error = apply_patch_ops_tracked(
            &parse_patch(wrap(&[
                "*** Delete File: gone.txt",
                "*** Delete File: missing.txt",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap_err();

        assert!(error.error.message.contains("cannot delete missing file"));
        assert_eq!(
            error.completed,
            vec![ApplyResult {
                turn_before_content: Some("gone\n".into()),
                turn_before_exists: true,
                move_before_content: None,
                move_before_exists: false,
                path: RuntimeString::from("gone.txt"),
                before_content: Some("gone\n".into()),
                after_content: None,
                kind: ApplyKind::Delete,
                exact: true,
                added: 0,
                removed: 1,
                move_path: None,
            }]
        );
        assert!(error.reconcile_paths.is_empty());
    }

    struct FailRenameSourceRemoveIo {
        inner: MemIO,
    }

    #[async_trait::async_trait]
    impl FileIO for FailRenameSourceRemoveIo {
        async fn read(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError> {
            self.inner.read(path).await
        }

        async fn write(
            &self,
            path: &RuntimeString,
            content: &RuntimeString,
            from: Option<&RuntimeString>,
        ) -> Result<(), ApplyError> {
            self.inner.write(path, content, from).await
        }

        async fn remove(&self, path: &RuntimeString) -> Result<(), ApplyError> {
            if path == "old.txt" {
                return Err(ApplyError::new("injected source remove failure"));
            }
            self.inner.remove(path).await
        }
    }

    #[tokio::test]
    async fn failed_rename_exposes_both_indeterminate_paths_after_destination_write() {
        let io = FailRenameSourceRemoveIo {
            inner: MemIO::new(&[("old.txt", "old\n")]),
        };
        let error = apply_patch_ops_tracked(
            &parse_patch(wrap(&[
                "*** Update File: old.txt",
                "*** Move to: new.txt",
                "@@",
                "-old",
                "+new",
            ]))
            .unwrap(),
            &io,
        )
        .await
        .unwrap_err();

        assert!(error.completed.is_empty());
        assert_eq!(
            error.reconcile_paths,
            vec![
                RuntimeString::from("old.txt"),
                RuntimeString::from("new.txt")
            ]
        );
        assert_eq!(io.inner.get("old.txt").as_deref(), Some("old\n"));
        assert_eq!(io.inner.get("new.txt").as_deref(), Some("new\n"));
    }

    #[tokio::test]
    async fn throws_when_updating_a_missing_file() {
        let io = MemIO::new(&[]);
        let err = apply_patch_ops(
            &parse_patch(wrap(&["*** Update File: nope.txt", "@@", "-x", "+y"])).unwrap(),
            &io,
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("cannot update missing file"));
    }

    #[tokio::test]
    async fn throws_when_deleting_a_missing_file() {
        let io = MemIO::new(&[]);
        let err = apply_patch_ops(
            &parse_patch(wrap(&["*** Delete File: nope.txt"])).unwrap(),
            &io,
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("cannot delete missing file"));
    }

    #[tokio::test]
    async fn throws_with_the_offending_lines_when_a_hunk_cannot_be_located() {
        let io = MemIO::new(&[("x.txt", "totally\nunrelated\n")]);
        let err = apply_patch_ops(
            &parse_patch(wrap(&["*** Update File: x.txt", "@@", "-missing", "+new"])).unwrap(),
            &io,
        )
        .await
        .unwrap_err();
        assert!(err.message.contains("could not locate context"));
    }

    #[tokio::test]
    async fn preserves_lone_surrogates_through_parse_and_apply() {
        let lone = RuntimeString::from_utf16(vec![0xd800]);
        let path = RuntimeString::from("path-")
            .concat(&lone)
            .concat(&RuntimeString::from(".txt"));
        let line = RuntimeString::from("value-").concat(&lone);
        let patch = RuntimeString::from("*** Begin Patch\n*** Add File: ")
            .concat(&path)
            .concat(&RuntimeString::from("\n+"))
            .concat(&line)
            .concat(&RuntimeString::from("\n*** End Patch"));
        let io = MemIO::new(&[]);

        let ops = parser::parse_patch(&patch).unwrap();
        apply_patch_ops(&ops, &io).await.unwrap();

        let files = io.files.lock().unwrap();
        assert_eq!(
            files.get(&path),
            Some(&line.concat(&RuntimeString::from("\n")))
        );
    }

    // --- seek_sequence ---

    fn v(items: &[&str]) -> Vec<RuntimeString> {
        items.iter().map(|s| RuntimeString::from(*s)).collect()
    }

    #[test]
    fn finds_an_exact_subsequence() {
        let hit = seek_sequence(&v(&["foo", "bar", "baz"]), &v(&["bar", "baz"]), 0, false).unwrap();
        assert_eq!(hit.index, 1);
        assert!(hit.exact);
        assert_eq!(hit.pass, SeekPass::Exact);
    }

    #[test]
    fn ignores_trailing_whitespace_rstrip_pass() {
        let hit = seek_sequence(&v(&["foo   ", "bar\t\t"]), &v(&["foo", "bar"]), 0, false).unwrap();
        assert_eq!(hit.index, 0);
        assert!(!hit.exact);
        assert_eq!(hit.pass, SeekPass::Rstrip);
    }

    #[test]
    fn ignores_leading_and_trailing_whitespace_trim_pass() {
        let hit = seek_sequence(
            &v(&["    foo   ", "   bar\t"]),
            &v(&["foo", "bar"]),
            0,
            false,
        )
        .unwrap();
        assert_eq!(hit.index, 0);
        assert_eq!(hit.pass, SeekPass::Trim);
    }

    #[test]
    fn normalizes_unicode_punctuation_normalize_pass() {
        let lines = v(&["x \u{2013} y"]);
        assert!(lines[0].contains("\u{2013}"));
        let hit = seek_sequence(&lines, &v(&["x - y"]), 0, false).unwrap();
        assert_eq!(hit.pass, SeekPass::Normalize);
    }

    #[test]
    fn returns_a_no_op_match_at_from_for_an_empty_pattern() {
        let hit = seek_sequence(&v(&["a", "b"]), &[], 1, false).unwrap();
        assert_eq!(hit.index, 1);
        assert!(hit.exact);
    }

    #[test]
    fn returns_null_when_the_pattern_is_longer_than_the_input() {
        assert!(
            seek_sequence(&v(&["only one"]), &v(&["too", "many", "lines"]), 0, false).is_none()
        );
    }

    #[test]
    fn anchors_at_end_of_file_when_eof_is_set() {
        let lines = v(&["a", "a", "a"]);
        assert_eq!(seek_sequence(&lines, &v(&["a"]), 0, true).unwrap().index, 2);
    }

    #[test]
    fn searches_from_start_and_respects_the_offset() {
        let lines = v(&["dup", "mid", "dup"]);
        assert_eq!(
            seek_sequence(&lines, &v(&["dup"]), 1, false).unwrap().index,
            2
        );
    }

    #[test]
    fn defaults_the_eof_argument_so_legacy_three_arg_callers_still_work() {
        // Rust has no default args; the three-arg form maps to eof = false.
        assert_eq!(
            seek_sequence(&v(&["a", "b"]), &v(&["b"]), 0, false)
                .unwrap()
                .index,
            1
        );
    }
}
