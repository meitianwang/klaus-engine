//! A compact line-level diff snippet for the edited-files reminder. It shows
//! NEW-side context lines around each change, with line numbers, hunks
//! separated by an ellipsis, capped in JavaScript UTF-16 `.length` units,
//! using an LCS-based implementation.
//! Deleted lines are dropped; a change is surfaced by the surrounding
//! context.

use crate::runtime_values::RuntimeString;
use std::time::{Duration, Instant};

const DEFAULT_CONTEXT_LINES: usize = 3;
const DEFAULT_MAX_BYTES: usize = 8_000;
/// Above this, skip the O(n·m) LCS and fall back to a whole-middle replacement.
const LCS_CELL_BUDGET: usize = 4_000_000;

/// Counting has its own budgets, deliberately not the snippet's.
///
/// The snippet above may declare the whole middle replaced when the table gets
/// too big: a reader only loses some surrounding context. A COUNT that degrades
/// the same way reports a rewritten file for a two-line edit — a 10k-line source
/// file with one edit near the top and one near the bottom trims almost nothing
/// off either end, so the middle alone needs ~10^8 cells and every such edit read
/// as ±10k. So the counting path never guesses: it measures exactly or says it
/// could not.
///
/// Two tiers, because the affordable cases are different shapes. The table is
/// bounded by area, so it takes any edit distance as long as the middle is small.
/// Myers is bounded by edit distance, so it takes any size as long as the edit is
/// small — which is the shape a source file being edited actually has.
const MAX_COUNT_DP_CELLS: usize = 2_000_000;
const MAX_COUNT_EDIT_DISTANCE: usize = 2_048;
const MAX_COUNT_DURATION: Duration = Duration::from_millis(75);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Eq,
    Add,
    Del,
}

struct Op {
    kind: OpKind,
    line: RuntimeString,
    /// NEW-side 0-based index, or -1 for a deletion.
    new_index: i64,
}

/// Line ops aligning `a`→`b`, with a common prefix/suffix fast-path + size guard.
fn diff_lines(a: &[RuntimeString], b: &[RuntimeString]) -> Vec<Op> {
    let mut ops: Vec<Op> = Vec::new();
    // Trim the common prefix.
    let mut p = 0usize;
    while p < a.len() && p < b.len() && a[p] == b[p] {
        ops.push(Op {
            kind: OpKind::Eq,
            line: b[p].clone(),
            new_index: p as i64,
        });
        p += 1;
    }
    // Trim the common suffix (not overlapping the prefix).
    let mut sa = a.len();
    let mut sb = b.len();
    while sa > p && sb > p && a[sa - 1] == b[sb - 1] {
        sa -= 1;
        sb -= 1;
    }
    let a_mid = &a[p..sa];
    let b_mid = &b[p..sb];
    let mut mid_ops: Vec<Op> = Vec::new();
    if a_mid.is_empty() {
        for (k, line) in b_mid.iter().enumerate() {
            mid_ops.push(Op {
                kind: OpKind::Add,
                line: line.clone(),
                new_index: (p + k) as i64,
            });
        }
    } else if b_mid.is_empty() {
        for line in a_mid {
            mid_ops.push(Op {
                kind: OpKind::Del,
                line: line.clone(),
                new_index: -1,
            });
        }
    } else if a_mid.len().saturating_mul(b_mid.len()) > LCS_CELL_BUDGET {
        // Too large for LCS: treat the whole middle as a replacement.
        for line in a_mid {
            mid_ops.push(Op {
                kind: OpKind::Del,
                line: line.clone(),
                new_index: -1,
            });
        }
        for (k, line) in b_mid.iter().enumerate() {
            mid_ops.push(Op {
                kind: OpKind::Add,
                line: line.clone(),
                new_index: (p + k) as i64,
            });
        }
    } else {
        mid_ops.extend(lcs_ops(a_mid, b_mid, p as i64));
    }
    ops.extend(mid_ops);
    // Re-attach the common suffix as eq ops.
    for (k, line) in b.iter().enumerate().skip(sb) {
        ops.push(Op {
            kind: OpKind::Eq,
            line: line.clone(),
            new_index: k as i64,
        });
    }
    ops
}

/// Classic LCS backtrack over `a`/`b`; NEW-side indices offset by `base`.
fn lcs_ops(a: &[RuntimeString], b: &[RuntimeString], base: i64) -> Vec<Op> {
    let n = a.len();
    let m = b.len();
    let mut dp = vec![vec![0i32; m + 1]; n + 1];
    for (i, a_line) in a.iter().enumerate().rev() {
        for (j, b_line) in b.iter().enumerate().rev() {
            dp[i][j] = if a_line == b_line {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }
    let mut ops = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < n && j < m {
        if a[i] == b[j] {
            ops.push(Op {
                kind: OpKind::Eq,
                line: b[j].clone(),
                new_index: base + j as i64,
            });
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            ops.push(Op {
                kind: OpKind::Del,
                line: a[i].clone(),
                new_index: -1,
            });
            i += 1;
        } else {
            ops.push(Op {
                kind: OpKind::Add,
                line: b[j].clone(),
                new_index: base + j as i64,
            });
            j += 1;
        }
    }
    while i < n {
        ops.push(Op {
            kind: OpKind::Del,
            line: a[i].clone(),
            new_index: -1,
        });
        i += 1;
    }
    while j < m {
        ops.push(Op {
            kind: OpKind::Add,
            line: b[j].clone(),
            new_index: base + j as i64,
        });
        j += 1;
    }
    ops
}

fn number_line(n: i64, line: &RuntimeString) -> RuntimeString {
    RuntimeString::from(format!("{n:>6}\t")).concat(line)
}

/// JavaScript `String.prototype.split("\n")` over exact UTF-16 code units.
fn split_lines(content: &RuntimeString) -> Vec<RuntimeString> {
    let units = content.as_utf16();
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().copied().enumerate() {
        if unit == u16::from(b'\n') {
            lines.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    lines.push(RuntimeString::from_utf16(units[start..].to_vec()));
    lines
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let Some(mut output) = values.first().cloned() else {
        return RuntimeString::default();
    };
    let separator = RuntimeString::from(separator);
    for value in &values[1..] {
        output = output.concat(&separator).concat(value);
    }
    output
}

/// The lines a file HAS, for counting.
///
/// [`split_lines`] is a faithful `split("\n")`, so it reports a trailing empty
/// slot after a final newline and reports one empty line for an empty file. Both
/// are right for rendering an aligned snippet and wrong for counting: they made
/// a creation report a removed line (the phantom empty baseline line has nothing
/// to align with unless the new content happens to end in a newline), so the
/// same edit counted differently depending on whether the file ended with `\n`.
fn counted_lines(content: &RuntimeString) -> Vec<RuntimeString> {
    let mut lines = split_lines(content);
    if lines.last().is_some_and(|line| line.as_utf16().is_empty()) {
        lines.pop();
    }
    lines
}

/// Complete unified diff for durable conversation inspection. Myers operates on
/// exact UTF-16 lines; hunks retain all changes and final-newline markers without
/// a time limit or output budget. The caller alone decides how much to display.
pub fn full_file_diff(before: &RuntimeString, after: &RuntimeString) -> RuntimeString {
    use similar::{capture_diff_slices, group_diff_ops, Algorithm, ChangeTag};
    if before == after {
        return RuntimeString::default();
    }
    let old: Vec<&[u16]> = before
        .as_utf16()
        .split_inclusive(|unit| *unit == u16::from(b'\n'))
        .collect();
    let new: Vec<&[u16]> = after
        .as_utf16()
        .split_inclusive(|unit| *unit == u16::from(b'\n'))
        .collect();
    let mut output: Vec<u16> = "--- before\n+++ after\n".encode_utf16().collect();
    for group in group_diff_ops(capture_diff_slices(Algorithm::Myers, &old, &new), 3) {
        let old_start = group.first().unwrap().old_range().start;
        let old_end = group.last().unwrap().old_range().end;
        let new_start = group.first().unwrap().new_range().start;
        let new_end = group.last().unwrap().new_range().end;
        let old_count = old_end - old_start;
        let new_count = new_end - new_start;
        output.extend(
            format!(
                "@@ -{},{} +{},{} @@\n",
                old_start + usize::from(old_count > 0),
                old_count,
                new_start + usize::from(new_count > 0),
                new_count
            )
            .encode_utf16(),
        );
        for op in group {
            for change in op.iter_changes(&old, &new) {
                output.push(u16::from(match change.tag() {
                    ChangeTag::Equal => b' ',
                    ChangeTag::Insert => b'+',
                    ChangeTag::Delete => b'-',
                }));
                let line = change.value();
                output.extend_from_slice(line);
                if line.last() != Some(&u16::from(b'\n')) {
                    output.extend("\n\\ No newline at end of file\n".encode_utf16());
                }
            }
        }
    }
    RuntimeString::from_utf16(output)
}

/// Preserve existence changes even when both text snapshots are empty. The
/// mode headers are conventional patch markers for creation/deletion; a true
/// content-and-existence cancellation remains an empty diff.
pub fn full_turn_file_diff(
    before: &RuntimeString,
    after: &RuntimeString,
    existed_before: bool,
    exists_after: bool,
) -> RuntimeString {
    let diff = full_file_diff(before, after);
    let mode = match (existed_before, exists_after) {
        (false, true) => "new file mode 100644\n",
        (true, false) => "deleted file mode 100644\n",
        _ => return diff,
    };
    let diff = if diff.as_utf16().is_empty() {
        RuntimeString::from("--- before\n+++ after\n")
    } else {
        diff
    };
    RuntimeString::from(mode).concat(&diff)
}

/// How many lines an edit added and removed.
///
/// An edit tool reports this on its `file_change` event so a host can show the
/// volume a turn produced. `None` means no exact answer was affordable, and a
/// host must render that as unknown — never as zero, and never as the whole file.
///
/// A creation passes an empty `before` and a deletion an empty `after`; an empty
/// file counts as zero lines, so those degenerate to all-add / all-del without a
/// special case.
pub fn line_change_counts(before: &RuntimeString, after: &RuntimeString) -> Option<(u64, u64)> {
    if before == after {
        return Some((0, 0));
    }
    let a = counted_lines(before);
    let b = counted_lines(after);
    // Only the middle needs measuring, and trimming it is what makes the budgets
    // below reachable at all: an edit inside a large file leaves the matching head
    // and tail out of the problem entirely.
    let mut prefix = 0usize;
    while prefix < a.len() && prefix < b.len() && a[prefix] == b[prefix] {
        prefix += 1;
    }
    let mut end_a = a.len();
    let mut end_b = b.len();
    while end_a > prefix && end_b > prefix && a[end_a - 1] == b[end_b - 1] {
        end_a -= 1;
        end_b -= 1;
    }
    let a_mid = &a[prefix..end_a];
    let b_mid = &b[prefix..end_b];
    if a_mid.is_empty() || b_mid.is_empty() {
        return Some((b_mid.len() as u64, a_mid.len() as u64));
    }
    let deadline = Instant::now() + MAX_COUNT_DURATION;
    let cells = (a_mid.len() + 1).saturating_mul(b_mid.len() + 1);
    let common = if cells <= MAX_COUNT_DP_CELLS {
        lcs_length(a_mid, b_mid, deadline)?
    } else {
        // Myers yields the edit distance; the alignment it implies has the same
        // common-line count an LCS table would have found, so both tiers answer
        // in the same units.
        let distance = myers_edit_distance(a_mid, b_mid, deadline)?;
        (a_mid.len() + b_mid.len() - distance) / 2
    };
    Some(((b_mid.len() - common) as u64, (a_mid.len() - common) as u64))
}

/// Length of the longest common subsequence, over two rolling rows.
///
/// Only the length is needed here, so this holds O(m) instead of the O(n·m) the
/// op-emitting [`lcs_ops`] must keep to backtrack through.
fn lcs_length(a: &[RuntimeString], b: &[RuntimeString], deadline: Instant) -> Option<usize> {
    let mut previous = vec![0usize; b.len() + 1];
    let mut current = vec![0usize; b.len() + 1];
    for (i, a_line) in a.iter().enumerate() {
        if i % 64 == 0 && Instant::now() > deadline {
            return None;
        }
        for (j, b_line) in b.iter().enumerate() {
            current[j + 1] = if a_line == b_line {
                previous[j] + 1
            } else {
                previous[j + 1].max(current[j])
            };
        }
        std::mem::swap(&mut previous, &mut current);
    }
    Some(previous[b.len()])
}

/// The shortest edit distance between the two line sequences, or `None` when it
/// exceeds [`MAX_COUNT_EDIT_DISTANCE`] or the deadline.
///
/// Myers' greedy forward pass, furthest-reaching path per diagonal. No trace is
/// kept because the count needs the distance only, which also keeps this to O(D)
/// memory instead of O(D²).
fn myers_edit_distance(
    a: &[RuntimeString],
    b: &[RuntimeString],
    deadline: Instant,
) -> Option<usize> {
    let n = a.len() as i64;
    let m = b.len() as i64;
    let max_distance = (a.len() + b.len()).min(MAX_COUNT_EDIT_DISTANCE) as i64;
    // `furthest[k + max_distance]` is the largest x reached on diagonal k = x - y.
    // A diagonal is only ever read at the opposite parity to the one being
    // written, so one buffer serves every round.
    let mut furthest = vec![0i64; (2 * max_distance + 3) as usize];
    let offset = max_distance + 1;
    for distance in 0..=max_distance {
        if Instant::now() > deadline {
            return None;
        }
        let mut k = -distance;
        while k <= distance {
            let index = (k + offset) as usize;
            let mut x = if distance == 0 {
                0
            } else if k == -distance || (k != distance && furthest[index - 1] < furthest[index + 1])
            {
                furthest[index + 1]
            } else {
                furthest[index - 1] + 1
            };
            let mut y = x - k;
            while x < n && y < m && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            furthest[index] = x;
            if x >= n && y >= m {
                return Some(distance as usize);
            }
            k += 2;
        }
    }
    None
}

/// Options for [`diff_snippet`].
#[derive(Debug, Clone, Default)]
pub struct DiffSnippetOptions {
    pub context: Option<usize>,
    pub max_bytes: Option<usize>,
}

/// A compact NEW-side diff snippet, or "" when the contents are line-identical.
pub fn diff_snippet(
    old_content: &RuntimeString,
    new_content: &RuntimeString,
    opts: DiffSnippetOptions,
) -> RuntimeString {
    if old_content == new_content {
        return RuntimeString::default();
    }
    let context = opts.context.unwrap_or(DEFAULT_CONTEXT_LINES) as i64;
    let max_bytes = opts.max_bytes.unwrap_or(DEFAULT_MAX_BYTES);
    let a = split_lines(old_content);
    let b = split_lines(new_content);
    let ops = diff_lines(&a, &b);

    // Keep ops within `context` lines of any change.
    let changed: Vec<bool> = ops.iter().map(|o| o.kind != OpKind::Eq).collect();
    if !changed.iter().any(|&c| c) {
        return RuntimeString::default();
    }
    let len = ops.len() as i64;
    let keep: Vec<bool> = (0..ops.len())
        .map(|k| {
            for d in -context..=context {
                let x = k as i64 + d;
                if x >= 0 && x < len && changed[x as usize] {
                    return true;
                }
            }
            false
        })
        .collect();

    // Emit kept NEW-side lines (eq + add; deletions dropped), with an ellipsis
    // where the NEW line numbers jump.
    let mut out: Vec<RuntimeString> = Vec::new();
    let mut prev_n: Option<i64> = None;
    for (k, o) in ops.iter().enumerate() {
        if !keep[k] || o.kind == OpKind::Del {
            continue;
        }
        if !out.is_empty() && prev_n.map(|p| p + 1) != Some(o.new_index) {
            out.push(RuntimeString::from("…"));
        }
        out.push(number_line(o.new_index + 1, &o.line));
        prev_n = Some(o.new_index);
    }
    if out.is_empty() {
        return RuntimeString::default();
    }

    // The option is named `maxBytes` for historical reasons, but the contract
    // compares JavaScript `.length`: truncate by UTF-16 code unit at a line
    // boundary.
    let mut snippet = join_runtime_strings(&out, "\n");
    if snippet.len_utf16() > max_bytes {
        // The truncation contract uses `.length`/`lastIndexOf`/`slice`
        // semantics in UTF-16 code units. The fallback may intentionally split
        // a surrogate pair when no earlier newline exists.
        let cut = snippet.as_utf16()[..=max_bytes]
            .iter()
            .rposition(|&unit| unit == u16::from(b'\n'));
        let head_end = match cut {
            Some(index) if index > 0 => index,
            _ => max_bytes,
        };
        let head = snippet.slice_utf16(..head_end);
        let shown_lines = if head.is_empty() {
            0
        } else {
            head.as_utf16()
                .iter()
                .filter(|&&unit| unit == u16::from(b'\n'))
                .count()
                + 1
        };
        snippet = head
            .concat(&RuntimeString::from("\n… ["))
            .concat(&RuntimeString::from((out.len() - shown_lines).to_string()))
            .concat(&RuntimeString::from(" more changed lines truncated]"));
    }
    snippet
}

#[cfg(test)]
mod tests {
    #[test]
    fn full_diff_preserves_missing_newlines_and_distant_changes_without_snippet_limits() {
        use super::full_file_diff;
        use crate::runtime_values::RuntimeString;
        let newline = full_file_diff(&"hello".into(), &"hello\n".into())
            .try_to_utf8()
            .unwrap();
        assert_eq!(newline,"--- before\n+++ after\n@@ -1,1 +1,1 @@\n-hello\n\\ No newline at end of file\n+hello\n");
        let before = (0..10_000)
            .map(|index| format!("line {index}\n"))
            .collect::<String>();
        let after = before
            .replace("line 2\n", "changed first\n")
            .replace("line 9998\n", "changed last\n");
        let diff = full_file_diff(&before.into(), &after.into())
            .try_to_utf8()
            .unwrap();
        assert_eq!(diff.matches("@@ -").count(), 2);
        assert!(diff.contains("-line 2\n+changed first\n"));
        assert!(diff.contains("-line 9998\n+changed last\n"));
        assert!(!diff.contains("line 5000"));
        let long = "🦀完整\n".repeat(6000);
        let full = full_file_diff(&RuntimeString::default(), &long.clone().into())
            .try_to_utf8()
            .unwrap();
        assert_eq!(full.matches("+🦀完整\n").count(), 6000);
    }
    use super::*;

    fn snip(old: &str, new: &str) -> RuntimeString {
        diff_snippet(
            &RuntimeString::from(old),
            &RuntimeString::from(new),
            DiffSnippetOptions::default(),
        )
    }

    #[test]
    fn empty_for_identical() {
        assert_eq!(snip("a\nb\nc", "a\nb\nc"), RuntimeString::default());
    }

    #[test]
    fn changed_line_with_context_and_numbers() {
        let out = snip("one\ntwo\nthree", "one\nTWO\nthree");
        assert!(out.contains("TWO"));
        assert!(out.contains("     2\tTWO"));
        assert!(out.contains("one"));
        assert!(!out.contains("two"));
    }

    #[test]
    fn shows_inserted_lines() {
        let out = snip("a\nb", "a\nNEW\nb");
        assert!(out.contains("NEW"));
        assert!(out.contains("     2\tNEW"));
    }

    #[test]
    fn separates_distant_hunks_with_ellipsis() {
        let lines: Vec<String> = (0..40).map(|i| format!("line{i}")).collect();
        let mut next = lines.clone();
        next[2] = "CHANGED_A".into();
        next[37] = "CHANGED_B".into();
        let out = snip(&lines.join("\n"), &next.join("\n"));
        assert!(out.contains("CHANGED_A"));
        assert!(out.contains("CHANGED_B"));
        assert!(out.contains("…"));
        assert!(!out.contains("line20"));
    }

    #[test]
    fn caps_at_line_boundary_with_note() {
        let next: String = (0..2000)
            .map(|i| format!("newline-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let out = diff_snippet(
            &RuntimeString::from("x"),
            &RuntimeString::from(next),
            DiffSnippetOptions {
                context: None,
                max_bytes: Some(200),
            },
        );
        assert!(out.len_utf16() < 400);
        assert!(out.contains("truncated"));
    }

    #[test]
    fn huge_replacement_no_lcs_blowup() {
        let old: String = (0..3000)
            .map(|i| format!("a{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let neu: String = (0..3000)
            .map(|i| format!("b{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let out = diff_snippet(
            &RuntimeString::from(old),
            &RuntimeString::from(neu),
            DiffSnippetOptions {
                context: None,
                max_bytes: Some(400),
            },
        );
        assert!(out.contains("b0"));
        assert!(out.len_utf16() < 600);
    }

    #[test]
    fn preserves_lone_surrogate_and_replacement_as_distinct_changed_lines() {
        let old = RuntimeString::from("old");
        let lone = RuntimeString::from("new-").concat(&RuntimeString::from_utf16(vec![0xd800]));
        let replacement = RuntimeString::from("new-�");

        let lone_out = diff_snippet(&old, &lone, DiffSnippetOptions::default());
        assert!(lone_out.as_utf16().contains(&0xd800));
        assert!(!lone_out.as_utf16().contains(&0xfffd));

        let replacement_out = diff_snippet(&old, &replacement, DiffSnippetOptions::default());
        assert!(replacement_out.as_utf16().contains(&0xfffd));
        assert!(!replacement_out.as_utf16().contains(&0xd800));
    }

    #[test]
    fn truncation_matches_javascript_utf16_half_pair_slice() {
        let out = diff_snippet(
            &RuntimeString::from("x"),
            &RuntimeString::from("😀"),
            DiffSnippetOptions {
                context: None,
                // Seven prefix units (`     1\t`) plus only the high surrogate.
                max_bytes: Some(8),
            },
        );
        assert!(out.as_utf16().contains(&0xd83d));
        assert!(!out.as_utf16().contains(&0xde00));
        assert!(out.contains("truncated"));
    }

    fn counts(before: &str, after: &str) -> (u64, u64) {
        maybe_counts(before, after).expect("counts are measurable")
    }

    fn maybe_counts(before: &str, after: &str) -> Option<(u64, u64)> {
        line_change_counts(&RuntimeString::from(before), &RuntimeString::from(after))
    }

    #[test]
    fn counts_only_the_changed_lines() {
        assert_eq!(counts("a\nb\nc\n", "a\nb\nc\n"), (0, 0));
        assert_eq!(counts("a\nb\nc\n", "a\nB\nc\n"), (1, 1));
        assert_eq!(counts("a\nc\n", "a\nb\nc\n"), (1, 0));
        assert_eq!(counts("a\nb\nc\n", "a\nc\n"), (0, 1));
    }

    /// The defect this path exists for: a file long enough that an LCS table over
    /// the middle is unaffordable, edited near BOTH ends so neither the prefix nor
    /// the suffix trim shrinks that middle. Counting used to answer with the whole
    /// file (±10k) for what is a two-line edit.
    #[test]
    fn distant_edits_in_a_large_file_count_only_those_edits() {
        let before: Vec<String> = (0..10_000).map(|i| format!("line{i}")).collect();
        let mut after = before.clone();
        after[1] = "CHANGED_NEAR_THE_TOP".into();
        after[9_998] = "CHANGED_NEAR_THE_BOTTOM".into();
        assert_eq!(counts(&before.join("\n"), &after.join("\n")), (2, 2));
    }

    /// Both tiers refuse: the middle is too large for the table and too different
    /// for Myers. The honest answer is that the volume is unknown — reporting the
    /// file's own length instead is what made a two-line edit read as a rewrite.
    ///
    /// The size is chosen to clear both budgets and nothing more: 1501×1501 cells
    /// is past [`MAX_COUNT_DP_CELLS`] and an edit distance of 3000 is past
    /// [`MAX_COUNT_EDIT_DISTANCE`].
    #[test]
    fn an_unmeasurable_rewrite_reports_unknown_rather_than_the_whole_file() {
        let before: Vec<String> = (0..1_500).map(|i| format!("before{i}")).collect();
        let after: Vec<String> = (0..1_500).map(|i| format!("after{i}")).collect();
        assert_eq!(maybe_counts(&before.join("\n"), &after.join("\n")), None);
    }

    #[test]
    fn a_creation_counts_its_lines_without_the_trailing_newline_slot() {
        // A 3-line file written with a trailing newline is +3, not +4: the empty
        // string after the last newline is not a line the edit added.
        assert_eq!(counts("", "one\ntwo\nthree\n"), (3, 0));
        assert_eq!(counts("one\ntwo\nthree\n", ""), (0, 3));
    }

    /// A creation removes nothing, whether or not the new content ends in a
    /// newline. Splitting on "\n" reports one empty line for an empty file, and
    /// that phantom line used to align with the trailing slot of newline-
    /// terminated content and with nothing otherwise — so the same creation was
    /// +3 -0 or +1 -1 depending on the final byte.
    #[test]
    fn a_creation_removes_nothing_however_the_content_ends() {
        assert_eq!(counts("", "only line, no trailing newline"), (1, 0));
        assert_eq!(counts("", "one\ntwo"), (2, 0));
        assert_eq!(counts("only line, no trailing newline", ""), (0, 1));
        // An empty file created empty changed nothing at all.
        assert_eq!(counts("", ""), (0, 0));
        // A blank line IS a line: only the slot after the final newline is not.
        assert_eq!(counts("", "a\n\n"), (2, 0));
    }
}
