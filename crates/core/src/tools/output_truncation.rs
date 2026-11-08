//! Shared byte/token truncation for model-visible tool output.
//!
//! One policy type expresses every model-visible output budget: raw bytes or
//! approximate tokens (4 bytes per token). Middle truncation keeps the head and
//! the tail of an oversized body — where the invocation context and the final
//! error usually are — and marks the elided span with its size, so the model
//! sees both the shape of the loss and enough content to act on. Surfaces that
//! own a recovery path (pagination, on-disk artifacts) pair this with their own
//! guidance so the elided middle stays reachable.

use std::ops::Mul;
use std::ops::Range;

use crate::runtime_values::RuntimeString;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::TextBlock;

const APPROX_BYTES_PER_TOKEN: usize = 4;

/// The default model-visible budget for tool output surfaces: command output,
/// file reads, and bridged external tool results.
pub const DEFAULT_OUTPUT_TRUNCATION_POLICY: TruncationPolicy = TruncationPolicy::Bytes(10_000);

/// How a model-visible budget is expressed: raw UTF-8 bytes or approximate
/// tokens at 4 bytes per token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncationPolicy {
    Bytes(usize),
    Tokens(usize),
}

impl TruncationPolicy {
    pub fn token_budget(&self) -> usize {
        match self {
            TruncationPolicy::Bytes(bytes) => {
                usize::try_from(approx_tokens_from_byte_count(*bytes)).unwrap_or(usize::MAX)
            }
            TruncationPolicy::Tokens(tokens) => *tokens,
        }
    }

    pub fn byte_budget(&self) -> usize {
        match self {
            TruncationPolicy::Bytes(bytes) => *bytes,
            TruncationPolicy::Tokens(tokens) => approx_bytes_for_tokens(*tokens),
        }
    }
}

impl Mul<f64> for TruncationPolicy {
    type Output = Self;

    fn mul(self, multiplier: f64) -> Self::Output {
        match self {
            TruncationPolicy::Bytes(bytes) => {
                TruncationPolicy::Bytes((bytes as f64 * multiplier).ceil() as usize)
            }
            TruncationPolicy::Tokens(tokens) => {
                TruncationPolicy::Tokens((tokens as f64 * multiplier).ceil() as usize)
            }
        }
    }
}

/// Adds the existing 20% allowance for serialization and headers.
pub fn with_serialization_allowance(policy: TruncationPolicy) -> TruncationPolicy {
    policy * 1.2
}

pub fn approx_token_count(text: &str) -> usize {
    let len = text.len();
    len.saturating_add(APPROX_BYTES_PER_TOKEN.saturating_sub(1)) / APPROX_BYTES_PER_TOKEN
}

pub const fn approx_bytes_for_tokens(tokens: usize) -> usize {
    tokens.saturating_mul(APPROX_BYTES_PER_TOKEN)
}

pub fn approx_tokens_from_byte_count(bytes: usize) -> u64 {
    let bytes_u64 = bytes as u64;
    bytes_u64.saturating_add((APPROX_BYTES_PER_TOKEN as u64).saturating_sub(1))
        / (APPROX_BYTES_PER_TOKEN as u64)
}

/// Renders the byte-omission notice placed where a capped collection dropped
/// bytes from the middle of a stream. Stream surfaces (interactive shell
/// sessions) pair it with their own token-count metadata so the model can tell
/// collection loss apart from model-facing truncation.
pub fn format_output_omission_marker(omitted_bytes: u64) -> String {
    format!("... {omitted_bytes} bytes omitted ...")
}

/// One UTF-8-aligned range replaced by a notice. Callers can apply the same
/// edit to annotated text without losing the sources of the retained slices.
pub struct TextTruncation {
    pub removed: Range<usize>,
    pub marker: String,
}

/// Plans the head/tail token truncation. The marker sits outside the
/// retained-text budget, just as it does in [`truncate_middle_with_token_budget`].
pub fn plan_token_truncation(s: &str, max_tokens: usize) -> Option<TextTruncation> {
    let max_bytes = approx_bytes_for_tokens(max_tokens);
    if s.len() <= max_bytes {
        return None;
    }
    let (left_budget, right_budget) = split_budget(max_bytes);
    let removed = s.floor_char_boundary(left_budget)..s.ceil_char_boundary(s.len() - right_budget);
    let removed_tokens = approx_tokens_from_byte_count(s.len() - max_bytes);
    Some(TextTruncation {
        removed,
        marker: format!("…{removed_tokens} tokens truncated…"),
    })
}

/// Retain at most `max_bytes` from a string's ends and add a character-count
/// marker outside that budget.
///
/// Use [`truncate_middle_with_marker`] when the complete result must fit a hard
/// byte limit.
pub fn truncate_middle_chars(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let (left_budget, right_budget) = split_budget(max_bytes);
    let (removed_chars, left, right) = split_string(s, left_budget, right_budget);
    assemble_truncated_output(left, right, &format!("…{removed_chars} chars truncated…"))
}

/// Truncate the middle of a UTF-8 string so the result, including `marker`, is
/// at most `max_bytes` bytes.
///
/// Returns the original string when it already fits. If the marker itself does
/// not fit, returns the longest original prefix that does.
pub fn truncate_middle_with_marker(s: &str, max_bytes: usize, marker: &str) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }

    let Some(content_budget) = max_bytes.checked_sub(marker.len()) else {
        return s[..s.floor_char_boundary(max_bytes)].to_string();
    };
    let (left_budget, right_budget) = split_budget(content_budget);
    let prefix_end = s.floor_char_boundary(left_budget);
    let suffix_start = s.ceil_char_boundary(s.len().saturating_sub(right_budget));

    assemble_truncated_output(&s[..prefix_end], &s[suffix_start..], marker)
}

/// Truncate the middle of a UTF-8 string to at most `max_tokens` approximate
/// tokens, preserving the beginning and the end. Returns the possibly
/// truncated string and `Some(original_token_count)` if truncation occurred;
/// otherwise returns the original string and `None`.
pub fn truncate_middle_with_token_budget(s: &str, max_tokens: usize) -> (String, Option<u64>) {
    let Some(truncation) = plan_token_truncation(s, max_tokens) else {
        return (s.to_string(), None);
    };
    let truncated = assemble_truncated_output(
        &s[..truncation.removed.start],
        &s[truncation.removed.end..],
        &truncation.marker,
    );
    let total_tokens = u64::try_from(approx_token_count(s)).unwrap_or(u64::MAX);

    if truncated == s {
        (truncated, None)
    } else {
        (truncated, Some(total_tokens))
    }
}

pub fn truncate_text(content: &str, policy: TruncationPolicy) -> String {
    match policy {
        TruncationPolicy::Bytes(bytes) => truncate_middle_chars(content, bytes),
        TruncationPolicy::Tokens(tokens) => truncate_middle_with_token_budget(content, tokens).0,
    }
}

/// [`truncate_text`] plus the metadata header a consumer needs to judge the
/// loss: the original approximate token count and the original line count.
pub fn formatted_truncate_text(content: &str, policy: TruncationPolicy) -> String {
    if content.len() <= policy.byte_budget() {
        return content.to_string();
    }

    let original_token_count = approx_token_count(content);
    let total_lines = content.lines().count();
    let result = truncate_text(content, policy);
    format!(
        "Warning: truncated output (original token count: {original_token_count})\nTotal output lines: {total_lines}\n\n{result}"
    )
}

fn split_string(s: &str, beginning_bytes: usize, end_bytes: usize) -> (usize, &str, &str) {
    if s.is_empty() {
        return (0, "", "");
    }

    let len = s.len();
    let tail_start_target = len.saturating_sub(end_bytes);
    let mut prefix_end = 0usize;
    let mut suffix_start = len;
    let mut removed_chars = 0usize;
    let mut suffix_started = false;

    for (idx, ch) in s.char_indices() {
        let char_end = idx + ch.len_utf8();
        if char_end <= beginning_bytes {
            prefix_end = char_end;
            continue;
        }

        if idx >= tail_start_target {
            if !suffix_started {
                suffix_start = idx;
                suffix_started = true;
            }
            continue;
        }

        removed_chars = removed_chars.saturating_add(1);
    }

    if suffix_start < prefix_end {
        suffix_start = prefix_end;
    }

    let before = &s[..prefix_end];
    let after = &s[suffix_start..];

    (removed_chars, before, after)
}

fn split_budget(budget: usize) -> (usize, usize) {
    let left = budget / 2;
    (left, budget - left)
}

fn assemble_truncated_output(prefix: &str, suffix: &str, marker: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + marker.len() + suffix.len() + 1);
    out.push_str(prefix);
    out.push_str(marker);
    out.push_str(suffix);
    out
}

/// [`truncate_text`] over runtime text. Returns the text and whether it was
/// truncated.
///
/// Non-scalar runtime text (lone surrogates from hostile or lossy sources) has
/// no UTF-8 byte length, so it passes through unchanged: byte budgets are
/// undefined for it and rewriting through a lossy conversion would corrupt the
/// only record of the value.
pub fn truncate_runtime_text(
    content: &RuntimeString,
    policy: TruncationPolicy,
) -> (RuntimeString, bool) {
    let Ok(utf8) = content.try_to_utf8() else {
        return (content.clone(), false);
    };
    if utf8.len() <= policy.byte_budget() {
        return (content.clone(), false);
    }
    (
        RuntimeString::from_utf8(&truncate_text(&utf8, policy)),
        true,
    )
}

/// [`formatted_truncate_text`] over runtime text. Returns the text and whether
/// it was truncated. Non-scalar text passes through unchanged.
pub fn formatted_truncate_runtime_text(
    content: &RuntimeString,
    policy: TruncationPolicy,
) -> (RuntimeString, bool) {
    let Ok(utf8) = content.try_to_utf8() else {
        return (content.clone(), false);
    };
    if utf8.len() <= policy.byte_budget() {
        return (content.clone(), false);
    }
    (
        RuntimeString::from_utf8(&formatted_truncate_text(&utf8, policy)),
        true,
    )
}

/// Apply one shared budget across model-visible output blocks.
///
/// Text blocks consume the budget in order; the crossing block is reduced to a
/// snippet of the remaining budget and later text blocks are omitted with a
/// count notice. Empty text blocks drop out: they contribute no model content
/// but still cost an API array slot. Images are retained unconditionally —
/// they carry their own provider-side budget, and dropping one silently would
/// lose the only copy of a visual result. Non-scalar text has no measurable
/// byte cost and is retained whole without consuming budget.
pub fn truncate_output_blocks(
    blocks: Vec<OutputBlock>,
    policy: TruncationPolicy,
) -> Vec<OutputBlock> {
    let mut out: Vec<OutputBlock> = Vec::with_capacity(blocks.len());
    let mut remaining_budget = match policy {
        TruncationPolicy::Bytes(_) => policy.byte_budget(),
        TruncationPolicy::Tokens(_) => policy.token_budget(),
    };
    let mut omitted_text_items = 0usize;

    for block in blocks {
        match block {
            OutputBlock::Text(text) => {
                if text.text.is_empty() {
                    continue;
                }
                if remaining_budget == 0 {
                    omitted_text_items += 1;
                    continue;
                }
                let Ok(utf8) = text.text.try_to_utf8() else {
                    out.push(OutputBlock::Text(text));
                    continue;
                };
                let cost = match policy {
                    TruncationPolicy::Bytes(_) => utf8.len(),
                    TruncationPolicy::Tokens(_) => approx_token_count(&utf8),
                };
                if cost <= remaining_budget {
                    remaining_budget = remaining_budget.saturating_sub(cost);
                    out.push(OutputBlock::Text(TextBlock {
                        text: RuntimeString::from_utf8(&utf8),
                        ..text
                    }));
                } else {
                    let snippet_policy = match policy {
                        TruncationPolicy::Bytes(_) => TruncationPolicy::Bytes(remaining_budget),
                        TruncationPolicy::Tokens(_) => TruncationPolicy::Tokens(remaining_budget),
                    };
                    let snippet = truncate_text(&utf8, snippet_policy);
                    if snippet.is_empty() {
                        omitted_text_items += 1;
                    } else {
                        out.push(OutputBlock::Text(TextBlock {
                            text: RuntimeString::from_utf8(&snippet),
                            ..text
                        }));
                    }
                    remaining_budget = 0;
                }
            }
            OutputBlock::Image(image) => out.push(OutputBlock::Image(image)),
        }
    }

    if omitted_text_items > 0 {
        out.push(OutputBlock::Text(TextBlock {
            text: RuntimeString::from_utf8(&format!(
                "[omitted {omitted_text_items} text items ...]"
            )),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::ImageBlock;

    fn text_block(value: &str) -> OutputBlock {
        OutputBlock::Text(TextBlock {
            text: RuntimeString::from(value),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn block_text(block: &OutputBlock) -> String {
        match block {
            OutputBlock::Text(text) => text.text.try_to_utf8().unwrap(),
            OutputBlock::Image(_) => panic!("expected a text block"),
        }
    }

    #[test]
    fn policy_budgets_convert_at_four_bytes_per_token() {
        assert_eq!(TruncationPolicy::Bytes(10_000).token_budget(), 2_500);
        assert_eq!(TruncationPolicy::Tokens(2_500).byte_budget(), 10_000);
        assert_eq!(TruncationPolicy::Bytes(7).token_budget(), 2);
        assert_eq!(approx_token_count("abcd"), 1);
        assert_eq!(approx_token_count("abcde"), 2);
        assert_eq!(approx_bytes_for_tokens(3), 12);
    }

    #[test]
    fn serialization_allowance_scales_the_budget_with_a_ceiling() {
        assert_eq!(
            with_serialization_allowance(TruncationPolicy::Bytes(10_000)),
            TruncationPolicy::Bytes(12_000)
        );
        assert_eq!(
            with_serialization_allowance(TruncationPolicy::Bytes(5)),
            TruncationPolicy::Bytes(6)
        );
        assert_eq!(
            with_serialization_allowance(TruncationPolicy::Tokens(100)),
            TruncationPolicy::Tokens(120)
        );
    }

    #[test]
    fn middle_truncation_keeps_head_and_tail_and_counts_the_removed_chars() {
        let source = "abcdefghijklmnopqrstuvwxyz012345";
        assert_eq!(truncate_middle_chars(source, source.len()), source);
        let truncated = truncate_middle_chars(source, 10);
        // left 5 + right 5 are retained; the 22 removed chars are marked.
        assert_eq!(truncated, "abcde…22 chars truncated…12345");
    }

    #[test]
    fn middle_truncation_respects_multibyte_boundaries() {
        // 6 chars × 2 bytes = 12 bytes; a 7-byte budget must not split a char.
        let source = "éééééé";
        let truncated = truncate_middle_chars(source, 7);
        // left budget 3 keeps one char, right budget 4 keeps two chars.
        assert_eq!(truncated, "é…3 chars truncated…éé");
    }

    #[test]
    fn tiny_budgets_degrade_to_the_marker_and_a_sliver() {
        let truncated = truncate_middle_chars("hello", 1);
        assert_eq!(truncated, "…4 chars truncated…o");
        let empty_policy = truncate_middle_chars("hello", 0);
        assert_eq!(empty_policy, "…5 chars truncated…");
    }

    #[test]
    fn with_marker_keeps_the_whole_result_inside_the_hard_limit() {
        let source = "abcdefghijklmnopqrstuvwxyz";
        let marker = "[cut]";
        let truncated = truncate_middle_with_marker(source, 15, marker);
        assert!(truncated.len() <= 15);
        assert!(truncated.contains(marker));
        assert!(truncated.starts_with("abcde"));
        assert!(truncated.ends_with("vwxyz"));
        // An oversized marker leaves the longest fitting prefix.
        let huge_marker = "0123456789abcdef";
        assert_eq!(
            truncate_middle_with_marker(source, 10, huge_marker),
            "abcdefghij"
        );
        // A string that fits is returned unchanged.
        assert_eq!(truncate_middle_with_marker("short", 15, marker), "short");
    }

    #[test]
    fn token_budget_truncation_reports_the_original_token_count() {
        let source = "x".repeat(1_000);
        let (unchanged, tokens) = truncate_middle_with_token_budget(&source, 250);
        assert_eq!(unchanged, source);
        assert_eq!(tokens, None);

        let (truncated, tokens) = truncate_middle_with_token_budget(&source, 100);
        assert_eq!(tokens, Some(250));
        assert!(truncated.contains("…150 tokens truncated…"));
        assert!(truncated.starts_with('x'));
        assert!(truncated.ends_with('x'));
        // The retained text stays within the token budget; the marker sits
        // outside it.
        let retained = truncated.replace("…150 tokens truncated…", "");
        assert_eq!(retained.len(), 400);
    }

    #[test]
    fn truncate_text_dispatches_on_the_policy() {
        let source = "y".repeat(500);
        let by_bytes = truncate_text(&source, TruncationPolicy::Bytes(100));
        assert!(by_bytes.contains("chars truncated…"));
        let by_tokens = truncate_text(&source, TruncationPolicy::Tokens(25));
        assert!(by_tokens.contains("tokens truncated…"));
    }

    #[test]
    fn formatted_truncation_leads_with_the_loss_metadata() {
        let fits = "small body";
        assert_eq!(
            formatted_truncate_text(fits, TruncationPolicy::Bytes(100)),
            fits
        );
        let source = format!("first line\n{}\nlast line", "z".repeat(500));
        let formatted = formatted_truncate_text(&source, TruncationPolicy::Bytes(100));
        assert!(formatted.starts_with(
            "Warning: truncated output (original token count: 131)\nTotal output lines: 3\n\n"
        ));
        assert!(formatted.contains("chars truncated…"));
        assert!(formatted.ends_with("last line"));
    }

    #[test]
    fn runtime_bridges_pass_non_scalar_text_through_unchanged() {
        let scalar = RuntimeString::from_utf8(&"q".repeat(20_000));
        let (truncated, changed) = truncate_runtime_text(&scalar, DEFAULT_OUTPUT_TRUNCATION_POLICY);
        assert!(changed);
        assert!(truncated.contains("chars truncated…"));

        let non_scalar = RuntimeString::from_utf16(vec![b'a' as u16, 0xd800, b'b' as u16]);
        let (kept, changed) = truncate_runtime_text(&non_scalar, TruncationPolicy::Bytes(1));
        assert!(!changed);
        assert_eq!(kept, non_scalar);
        let (kept, changed) =
            formatted_truncate_runtime_text(&non_scalar, TruncationPolicy::Bytes(1));
        assert!(!changed);
        assert_eq!(kept, non_scalar);
    }

    #[test]
    fn block_budgets_are_consumed_in_order_and_images_survive() {
        let image = OutputBlock::Image(ImageBlock {
            mime_type: RuntimeString::from("image/png"),
            base64: RuntimeString::from("AAAA"),
            detail: None,
            source_path: None,
        });
        let blocks = vec![
            text_block("first"),
            image,
            text_block("second"),
            text_block(""),
            text_block("third"),
        ];
        // Budget fits "first" (5) and part of "second" (6): the crossing block
        // becomes a snippet, later text blocks are counted as omitted, and the
        // image is retained unconditionally. Empty text blocks drop out.
        let out = truncate_output_blocks(blocks, TruncationPolicy::Bytes(8));
        assert_eq!(out.len(), 4);
        assert_eq!(block_text(&out[0]), "first");
        assert!(matches!(out[1], OutputBlock::Image(_)));
        // Remaining budget 3 keeps one head byte and two tail bytes.
        assert_eq!(block_text(&out[2]), "s…3 chars truncated…nd");
        assert_eq!(block_text(&out[3]), "[omitted 1 text items ...]");
    }

    #[test]
    fn exhausted_budget_omits_later_text_with_one_notice() {
        let blocks = vec![text_block("0123456789"), text_block("a"), text_block("b")];
        let out = truncate_output_blocks(blocks, TruncationPolicy::Bytes(10));
        assert_eq!(out.len(), 2);
        assert_eq!(block_text(&out[0]), "0123456789");
        assert_eq!(block_text(&out[1]), "[omitted 2 text items ...]");
    }

    #[test]
    fn everything_within_budget_passes_through() {
        let blocks = vec![text_block("one"), text_block("two")];
        let out = truncate_output_blocks(blocks.clone(), DEFAULT_OUTPUT_TRUNCATION_POLICY);
        assert_eq!(out.len(), 2);
        assert_eq!(block_text(&out[0]), "one");
        assert_eq!(block_text(&out[1]), "two");
    }
}
