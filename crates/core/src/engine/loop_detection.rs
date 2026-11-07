//! Streamed-text repetition detector with markdown / code-block carve-outs.
//!
//! Run-scoped: observe tool boundaries and feed text deltas. The first detection is a
//! "strike 1" the caller recovers from via [`LoopDetector::clear_detection`]
//! (clears the latch, keeps the count) so the next detection reports `count == 2`.

use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::runtime_values::RuntimeString;

/// The detector's verdict after a feed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LoopCheck {
    /// 0 = no loop; 1 = first detection (recoverable); ≥2 = repeat (halt).
    pub count: u64,
    /// Human-readable detail for the recovery nudge / halt message.
    pub detail: Option<RuntimeString>,
}

const CONTENT_LOOP_THRESHOLD: usize = 10;
const CONTENT_CHUNK_SIZE: usize = 50;
const MAX_HISTORY_LENGTH: usize = 5000;

fn sha256_runtime(s: &RuntimeString) -> String {
    let mut h = Sha256::new();
    // Node's string hash ingress performs the same V8 UTF-8 projection for
    // unpaired surrogates as every other string-to-bytes boundary.
    h.update(s.to_node_utf8());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Default)]
pub struct LoopDetector {
    // Content chanting.
    stream_content_history: Vec<u16>,
    content_stats: HashMap<String, Vec<usize>>,
    last_content_index: usize,
    in_code_block: bool,

    // Detection latch (cumulative across the run).
    loop_detected: bool,
    detected_count: u64,
    last_detail: Option<RuntimeString>,
}

impl LoopDetector {
    pub fn new() -> Self {
        LoopDetector::default()
    }

    fn latched(&self) -> LoopCheck {
        LoopCheck {
            count: self.detected_count,
            detail: self.last_detail.clone(),
        }
    }

    fn trip(&mut self, detail: RuntimeString) -> LoopCheck {
        self.loop_detected = true;
        self.detected_count += 1;
        self.last_detail = Some(detail);
        self.latched()
    }

    /// Current verdict without feeding anything new.
    pub fn result(&self) -> LoopCheck {
        if self.loop_detected {
            self.latched()
        } else {
            LoopCheck::default()
        }
    }

    /// A tool boundary separates streamed text passages without classifying the action.
    pub fn observe_tool_call(&mut self) {
        self.reset_content_tracking();
    }

    /// Feed one streamed text delta.
    pub fn add_text(&mut self, content: &RuntimeString) -> LoopCheck {
        if self.loop_detected {
            return self.latched();
        }
        if self.check_content_loop(content) {
            let start = self.last_content_index.saturating_sub(20);
            let end = (self.last_content_index + CONTENT_CHUNK_SIZE)
                .min(self.stream_content_history.len());
            let around = trim_ecmascript(&RuntimeString::from_utf16(
                self.stream_content_history[start..end].to_vec(),
            ));
            return self.trip(
                RuntimeString::from("repeating content: \"")
                    .concat(&around)
                    .concat(&RuntimeString::from("…\"")),
            );
        }
        LoopCheck::default()
    }

    /// Clear the detection latch and text history while keeping the strike count.
    pub fn clear_detection(&mut self) {
        self.loop_detected = false;
        self.reset_content_tracking();
    }

    /// Full reset (start of a new run). Production builds a fresh detector
    /// per run instead; only tests reuse an instance across runs.
    #[cfg(test)]
    pub fn reset(&mut self) {
        self.reset_content_tracking();
        self.loop_detected = false;
        self.detected_count = 0;
        self.last_detail = None;
    }

    fn reset_content_tracking(&mut self) {
        self.stream_content_history.clear();
        self.content_stats.clear();
        self.last_content_index = 0;
    }

    // --- content tracking ---

    fn check_content_loop(&mut self, content: &RuntimeString) -> bool {
        let units = content.as_utf16();
        let num_fences = count_ascii_sequence(units, b"```");
        let structures = markdown_structures(units);
        let has_table = structures.has_table;
        let has_list_item = structures.has_list_item;
        let has_heading = structures.has_heading;
        let has_blockquote = structures.has_blockquote;
        let is_divider = is_divider(units);
        if num_fences > 0
            || has_table
            || has_list_item
            || has_heading
            || has_blockquote
            || is_divider
        {
            self.reset_content_tracking();
        }
        let was_in_code_block = self.in_code_block;
        if !num_fences.is_multiple_of(2) {
            self.in_code_block = !self.in_code_block;
        }
        if was_in_code_block || self.in_code_block || is_divider {
            return false;
        }

        self.stream_content_history.extend_from_slice(units);
        self.truncate_and_update();
        self.analyze_content_chunks_for_loop()
    }

    fn truncate_and_update(&mut self) {
        if self.stream_content_history.len() <= MAX_HISTORY_LENGTH {
            return;
        }
        let cut = self.stream_content_history.len() - MAX_HISTORY_LENGTH;
        self.stream_content_history.drain(0..cut);
        self.last_content_index = self.last_content_index.saturating_sub(cut);
        // Keep the existing buckets and occurrence buffers while sliding the window.
        // Rebuilding the hash table on every small delta repeatedly hashes all keys.
        self.content_stats.retain(|_, indices| {
            indices.retain_mut(|index| {
                if *index < cut {
                    false
                } else {
                    *index -= cut;
                    true
                }
            });
            !indices.is_empty()
        });
    }

    fn analyze_content_chunks_for_loop(&mut self) -> bool {
        while self.last_content_index + CONTENT_CHUNK_SIZE <= self.stream_content_history.len() {
            let idx = self.last_content_index;
            let chunk = RuntimeString::from_utf16(
                self.stream_content_history[idx..idx + CONTENT_CHUNK_SIZE].to_vec(),
            );
            let hash = sha256_runtime(&chunk);
            if is_loop_detected_for_chunk(
                &mut self.content_stats,
                &self.stream_content_history,
                idx,
                &chunk,
                hash,
            ) {
                return true;
            }
            self.last_content_index += 1;
        }
        false
    }
}

fn is_loop_detected_for_chunk(
    stats: &mut HashMap<String, Vec<usize>>,
    history: &[u16],
    idx: usize,
    chunk: &RuntimeString,
    hash: String,
) -> bool {
    let existing = match stats.get_mut(&hash) {
        None => {
            stats.insert(hash, vec![idx]);
            return false;
        }
        Some(e) => e,
    };
    // Guard against hash collisions with an exact content compare.
    let first = existing[0];
    let original = RuntimeString::from_utf16(
        history[first..(first + CONTENT_CHUNK_SIZE).min(history.len())].to_vec(),
    );
    if &original != chunk {
        return false;
    }

    existing.push(idx);
    if existing.len() < CONTENT_LOOP_THRESHOLD {
        return false;
    }

    // The recent occurrences must be clustered (small average distance)…
    let recent = &existing[existing.len() - CONTENT_LOOP_THRESHOLD..];
    let avg = (recent[recent.len() - 1] - recent[0]) as f64 / (CONTENT_LOOP_THRESHOLD - 1) as f64;
    if avg > (CONTENT_CHUNK_SIZE * 5) as f64 {
        return false;
    }

    // …AND the periods between occurrences must be mostly identical.
    let mut periods: HashSet<RuntimeString> = HashSet::new();
    for i in 0..recent.len() - 1 {
        periods.insert(RuntimeString::from_utf16(
            history[recent[i]..recent[i + 1]].to_vec(),
        ));
    }
    if periods.len() > CONTENT_LOOP_THRESHOLD / 2 {
        return false;
    }
    true
}

#[derive(Default)]
struct MarkdownStructures {
    has_table: bool,
    has_list_item: bool,
    has_heading: bool,
    has_blockquote: bool,
}

fn markdown_structures(content: &[u16]) -> MarkdownStructures {
    let mut result = MarkdownStructures::default();
    for line in content.split(|unit| *unit == u16::from(b'\n')) {
        let trimmed = line
            .iter()
            .position(|unit| !is_ecmascript_whitespace(*unit))
            .map_or(&[][..], |start| &line[start..]);

        result.has_table |= (trimmed.len() >= 2
            && trimmed.first() == Some(&u16::from(b'|'))
            && trimmed.last() == Some(&u16::from(b'|')))
            || (trimmed.len() >= 3
                && trimmed.iter().all(|unit| {
                    *unit == u16::from(b'|') || *unit == u16::from(b'+') || *unit == u16::from(b'-')
                }));

        result.has_list_item |= trimmed.first().is_some_and(|unit| {
            *unit == u16::from(b'*') || *unit == u16::from(b'-') || *unit == u16::from(b'+')
        }) && trimmed
            .get(1)
            .is_some_and(|unit| is_ecmascript_whitespace(*unit));

        let digit_end = trimmed
            .iter()
            .position(|unit| !(u16::from(b'0')..=u16::from(b'9')).contains(unit))
            .unwrap_or(trimmed.len());
        result.has_list_item |= digit_end > 0
            && trimmed.get(digit_end) == Some(&u16::from(b'.'))
            && trimmed
                .get(digit_end + 1)
                .is_some_and(|unit| is_ecmascript_whitespace(*unit));

        let heading_end = line
            .iter()
            .position(|unit| *unit != u16::from(b'#'))
            .unwrap_or(line.len());
        result.has_heading |= heading_end > 0
            && line
                .get(heading_end)
                .is_some_and(|unit| is_ecmascript_whitespace(*unit));
        result.has_blockquote |= line.first() == Some(&u16::from(b'>'))
            && line
                .get(1)
                .is_some_and(|unit| is_ecmascript_whitespace(*unit));
    }
    result
}

fn is_divider(content: &[u16]) -> bool {
    !content.is_empty()
        && content.iter().all(|unit| {
            *unit == u16::from(b'+')
                || *unit == u16::from(b'-')
                || *unit == u16::from(b'_')
                || *unit == u16::from(b'=')
                || *unit == u16::from(b'*')
                || (0x2500..=0x257f).contains(unit)
        })
}

fn count_ascii_sequence(content: &[u16], needle: &[u8]) -> usize {
    let needle = needle.iter().copied().map(u16::from).collect::<Vec<_>>();
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut offset = 0;
    while offset + needle.len() <= content.len() {
        if content[offset..].starts_with(&needle) {
            count += 1;
            offset += needle.len();
        } else {
            offset += 1;
        }
    }
    count
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

fn trim_ecmascript(value: &RuntimeString) -> RuntimeString {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn add_text(detector: &mut LoopDetector, text: &str) -> LoopCheck {
        detector.add_text(&RuntimeString::from(text))
    }

    #[test]
    fn repeated_tool_boundaries_do_not_create_a_verdict() {
        let mut detector = LoopDetector::new();
        for _ in 0..100 {
            detector.observe_tool_call();
            assert_eq!(detector.result().count, 0);
        }
    }

    #[test]
    fn tool_boundaries_preserve_a_latched_content_verdict_and_strikes() {
        let mut detector = LoopDetector::new();
        assert_eq!(add_text(&mut detector, &"loopy ".repeat(30)).count, 1);
        detector.observe_tool_call();
        assert_eq!(detector.result().count, 1);
        detector.clear_detection();
        detector.observe_tool_call();
        assert_eq!(add_text(&mut detector, &"loopy ".repeat(30)).count, 2);
    }

    #[test]
    fn content_recovery_resets_tracking() {
        let mut d = LoopDetector::new();
        assert_eq!(add_text(&mut d, &"loopy ".repeat(30)).count, 1);
        d.clear_detection();
        assert_eq!(
            add_text(
                &mut d,
                "Now I will take a completely different approach and inspect the config file.",
            )
            .count,
            0
        );
        assert_eq!(d.result().count, 0);
    }

    #[test]
    fn content_continued_chant_strikes_two() {
        let mut d = LoopDetector::new();
        assert_eq!(add_text(&mut d, &"loopy ".repeat(30)).count, 1);
        d.clear_detection();
        assert_eq!(add_text(&mut d, &"loopy ".repeat(30)).count, 2);
    }

    #[test]
    fn tier2_trips_on_chant() {
        let mut d = LoopDetector::new();
        let r = add_text(&mut d, &"loopy ".repeat(30));
        assert_eq!(r.count, 1);
    }

    #[test]
    fn tier2_counts_exact_utf16_units_without_replacing_surrogates() {
        let mut detector = LoopDetector::new();
        let mut units = Vec::new();
        for _ in 0..80 {
            units.extend_from_slice(&[0xd800, u16::from(b'x')]);
        }
        let result = detector.add_text(&RuntimeString::from_utf16(units));
        assert_eq!(result.count, 1);
    }

    #[test]
    fn tier2_no_trip_on_prose() {
        let mut d = LoopDetector::new();
        let prose = "The quick brown fox jumps over the lazy dog while the engine streams a varied, non-repetitive explanation of what it is doing and why, with distinct sentences throughout.";
        assert_eq!(add_text(&mut d, prose).count, 0);
    }

    #[test]
    fn tier2_retains_detection_after_the_content_window_slides() {
        let mut detector = LoopDetector::new();
        let varied = (0..1000)
            .map(|index| format!("unique passage {index:08} contains varied text. "))
            .collect::<String>();
        for chunk in varied.as_bytes().chunks(16) {
            assert_eq!(
                add_text(&mut detector, std::str::from_utf8(chunk).unwrap()).count,
                0
            );
        }
        let verdict = add_text(&mut detector, &"loopy ".repeat(30));
        assert_eq!(verdict.count, 1);
        detector.clear_detection();
        assert_eq!(add_text(&mut detector, &"loopy ".repeat(30)).count, 2);
    }

    #[test]
    fn tier2_no_trip_on_list() {
        let mut d = LoopDetector::new();
        let list: String = (0..30)
            .map(|i| format!("- item number {i} with its own text\n"))
            .collect();
        assert_eq!(add_text(&mut d, &list).count, 0);
    }

    #[test]
    fn tool_call_resets_content_tracking() {
        let mut d = LoopDetector::new();
        add_text(&mut d, &"loopy ".repeat(5));
        d.observe_tool_call();
        assert_eq!(add_text(&mut d, &"loopy ".repeat(5)).count, 0);
    }

    #[test]
    fn reset_clears_content_and_strikes() {
        let mut detector = LoopDetector::new();
        assert_eq!(add_text(&mut detector, &"loopy ".repeat(30)).count, 1);
        detector.reset();
        assert_eq!(detector.result().count, 0);
        assert_eq!(add_text(&mut detector, &"loopy ".repeat(30)).count, 1);
    }
}
