//! A dependency-free fuzzy filename matcher — the ranking core behind
//! `FileIndex::search`. It is a hand-written scorer rather than a wrapper
//! around an off-the-shelf fuzzy-matching crate.
//!
//! Unlike a greedy first-occurrence subsequence match, this finds the GLOBALLY
//! OPTIMAL alignment of the query within the target via an O(m·n) dynamic
//! program (an fzf/Smith-Waterman-style fill with backtracking), so a later run
//! of consecutive characters outranks an earlier scattered one. It still prefers
//! a match contained in the basename (people type the file name, not the
//! directory chain).

use crate::runtime_values::RuntimeString;

/// One scored match: higher `score` is better; `indices` mark matched chars.
#[derive(Debug, Clone, PartialEq)]
pub struct FuzzyMatch {
    pub score: f64,
    /// Char indices into the ORIGINAL target string that the query matched,
    /// ascending.
    pub indices: Vec<usize>,
}

fn is_separator(unit: u16) -> bool {
    matches!(unit, 0x002f | 0x005c | 0x005f | 0x002d | 0x002e | 0x0020)
}

// Scoring weights (hand-tuned; documented so they can be reasoned about).
const BONUS_CONSECUTIVE: f64 = 6.0; // matched char immediately follows the previous match
const BONUS_SEPARATOR: f64 = 5.0; // matched char follows a path/word separator
const BONUS_CAMEL: f64 = 5.0; // matched char is a camelCase hump (aB)
const BONUS_START: f64 = 6.0; // matched char is the first char of the segment
const BONUS_BASENAME: f64 = 10.0; // the whole query matched within the basename
const LENGTH_PENALTY_UNIT: usize = 16; // 1 point shaved per this many target chars

const NEG: f64 = f64::NEG_INFINITY;

/// Score `query` against `target`. Returns `None` when `query` is not a
/// (case-insensitive) subsequence of `target`. An empty query matches everything
/// with score 0.
///
/// Strategy: prefer the optimal match contained entirely within the basename
/// (with a basename bonus); otherwise fall back to the optimal match across the
/// full path. A length penalty gently favors shorter targets.
pub fn fuzzy_score(query: &RuntimeString, target: &RuntimeString) -> Option<FuzzyMatch> {
    let query_units = query.as_utf16();
    let target_units = target.as_utf16();
    if query_units.is_empty() {
        return Some(FuzzyMatch {
            score: 0.0,
            indices: Vec::new(),
        });
    }
    if target_units.is_empty() {
        return None;
    }

    let base_start = last_separator_index(target_units)
        .map(|i| i + 1)
        .unwrap_or(0);

    if let Some(in_base) = optimal_match(query_units, target_units, base_start) {
        return Some(FuzzyMatch {
            score: in_base.score + BONUS_BASENAME - length_penalty(target_units),
            indices: in_base.indices,
        });
    }

    if let Some(full) = optimal_match(query_units, target_units, 0) {
        return Some(FuzzyMatch {
            score: full.score - length_penalty(target_units),
            indices: full.indices,
        });
    }
    None
}

struct SegmentMatch {
    score: f64,
    indices: Vec<usize>,
}

/// Optimal alignment of `query` against `target[from..]` via dynamic
/// programming. Returns `None` if `query` is not a subsequence of that slice.
fn optimal_match(query: &[u16], target: &[u16], from: usize) -> Option<SegmentMatch> {
    let q: Vec<u16> = query.iter().copied().map(ascii_lower).collect();
    let t: Vec<u16> = target.iter().copied().map(ascii_lower).collect();
    let m = q.len();
    let n = target.len() - from;
    if m == 0 {
        return Some(SegmentMatch {
            score: 0.0,
            indices: Vec::new(),
        });
    }
    if m > n {
        return None;
    }

    let mut mat = make_grid(m + 1, n + 1, NEG);
    let mut best = make_grid(m + 1, n + 1, NEG);
    let mut best_arg: Vec<Vec<usize>> = vec![vec![0usize; n + 1]; m + 1];
    let mut consec: Vec<Vec<bool>> = vec![vec![false; n + 1]; m + 1];

    // Empty query matches with score 0 anywhere.
    best[0].fill(0.0);

    for i in 1..=m {
        // best[i][j] requires at least i target chars consumed → start j at i.
        for j in 1..=n {
            let t_idx = from + j - 1;
            let mut m_score = NEG;
            let mut via_consec = false;
            if j >= i && q[i - 1] == t[t_idx] {
                let b = char_bonus(target, t_idx, from);
                let prev_match = mat[i - 1][j - 1];
                let from_consec = if prev_match > NEG {
                    prev_match + b + BONUS_CONSECUTIVE
                } else {
                    NEG
                };
                let prev_best = best[i - 1][j - 1];
                let from_gap = if prev_best > NEG { prev_best + b } else { NEG };
                if from_consec >= from_gap {
                    m_score = from_consec;
                    via_consec = true;
                } else {
                    m_score = from_gap;
                }
            }
            mat[i][j] = m_score;
            consec[i][j] = via_consec;

            // best[i][j] = max(best[i][j-1], match[i][j]); track the argmax j.
            let carry = best[i][j - 1];
            if m_score >= carry {
                best[i][j] = m_score;
                best_arg[i][j] = j;
            } else {
                best[i][j] = carry;
                best_arg[i][j] = best_arg[i][j - 1];
            }
        }
    }

    let score = best[m][n];
    if score == NEG {
        return None;
    }

    // Backtrack from the ending column of the optimal full-query match.
    let mut indices: Vec<usize> = Vec::new();
    let mut i = m;
    let mut j = best_arg[m][n];
    while i > 0 {
        indices.push(from + j - 1);
        if consec[i][j] {
            j -= 1;
        } else {
            // Gap step: the previous query char ended at best_arg[i-1][j-1].
            j = best_arg[i - 1][j - 1];
        }
        i -= 1;
    }
    indices.reverse();
    Some(SegmentMatch { score, indices })
}

fn make_grid(rows: usize, cols: usize, fill: f64) -> Vec<Vec<f64>> {
    vec![vec![fill; cols]; rows]
}

/// Positional bonus for aligning a query char at target index `idx`.
fn char_bonus(target: &[u16], idx: usize, from: usize) -> f64 {
    let mut bonus = 1.0;
    if idx == from {
        bonus += BONUS_START;
    } else {
        let prev = target[idx - 1];
        let cur = target[idx];
        if is_separator(prev) {
            bonus += BONUS_SEPARATOR;
        } else if is_lower(prev) && is_upper(cur) {
            bonus += BONUS_CAMEL;
        }
    }
    bonus
}

fn length_penalty(target: &[u16]) -> f64 {
    (target.len() / LENGTH_PENALTY_UNIT) as f64
}

fn last_separator_index(s: &[u16]) -> Option<usize> {
    (0..s.len())
        .rev()
        .find(|&i| s[i] == u16::from(b'/') || s[i] == u16::from(b'\\'))
}

fn is_lower(unit: u16) -> bool {
    (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
}
fn is_upper(unit: u16) -> bool {
    (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
}

fn ascii_lower(unit: u16) -> u16 {
    if is_upper(unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn score(query: &str, candidate: &str) -> Option<FuzzyMatch> {
        fuzzy_score(&RuntimeString::from(query), &RuntimeString::from(candidate))
    }

    #[test]
    fn empty_query_matches_with_zero_score() {
        let m = score("", "anything").unwrap();
        assert_eq!(m.score, 0.0);
        assert!(m.indices.is_empty());
    }

    #[test]
    fn non_subsequence_returns_none() {
        assert!(score("zzz", "src/main.rs").is_none());
    }

    #[test]
    fn matches_basename_and_reports_indices() {
        let m = score("main", "src/main.rs").expect("should match");
        // indices point at m,a,i,n inside "main"
        let matched: String = m
            .indices
            .iter()
            .map(|&i| "src/main.rs".chars().nth(i).unwrap())
            .collect();
        assert_eq!(matched, "main");
    }

    #[test]
    fn consecutive_run_outranks_scattered() {
        let consecutive = score("abc", "abc_x_y_z").unwrap().score;
        let scattered = score("abc", "a_b_c_xyz").unwrap().score;
        assert!(consecutive > scattered);
    }
}
