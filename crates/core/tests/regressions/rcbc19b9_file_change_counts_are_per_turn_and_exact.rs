//! Defect: the desktop capsule reported +17243 -17092 for a turn whose real
//! change was +53 -3. Two independent faults compounded.
//! Root cause: (1) `line_change_counts` reused the SNIPPET's diff, which
//! declares the whole middle replaced once an LCS table would exceed its budget
//! — a 10945-line file edited near both ends trims almost nothing off either
//! end, so every such edit read as ±10k; (2) the session folded per-edit counts
//! with `+=`, charging every line again each time a later edit touched the same
//! file.
//! Fixed by: per-turn baselines + a counting path that measures exactly or
//! reports unknown (based on cbc19b9).

use klaus_agent_core::context::edited_files_diff::line_change_counts;
use klaus_agent_core::engine::session::SessionFileChanges;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::events::FileChangeKind;
use klaus_agent_core::types::file_state::FileStateCache;

fn lines(count: usize, tag: &str) -> Vec<String> {
    (0..count).map(|i| format!("{tag}{i}")).collect()
}

/// Fault (1): a large file edited near both ends is measured, not guessed.
#[test]
fn distant_edits_in_a_large_file_are_counted_not_estimated() {
    let before = lines(10_945, "line");
    let mut after = before.clone();
    after[16] = "CHANGED_NEAR_THE_TOP".into();
    after[10_800] = "CHANGED_NEAR_THE_BOTTOM".into();
    let counts = line_change_counts(
        &RuntimeString::from(before.join("\n")),
        &RuntimeString::from(after.join("\n")),
    );
    assert_eq!(
        counts,
        Some((2, 2)),
        "two replaced lines, not the file's own length"
    );
}

/// Fault (2): several edits to one file in one turn are ONE measurement against
/// what the turn first found, so the same lines are never charged twice.
#[test]
fn repeated_edits_to_one_file_in_one_turn_do_not_accumulate() {
    let cache = InMemoryFileStateCache::new();
    let path = RuntimePath::from("/w/app.rs");
    let original = RuntimeString::from("a\nb\nc\n");
    let after_first_edit = RuntimeString::from("a\nB\nc\nd\n");
    let after_second_edit = RuntimeString::from("a\nB\nc\nd\ne\n");

    cache.begin_turn();
    let first = line_change_counts(
        &cache
            .turn_baseline(&path, Some(&original))
            .expect("baseline held"),
        &after_first_edit,
    );
    // The second edit still measures from the turn's ORIGINAL content, even
    // though what is on disk now is the first edit's output.
    let second = line_change_counts(
        &cache
            .turn_baseline(&path, Some(&after_first_edit))
            .expect("baseline held"),
        &after_second_edit,
    );
    assert_eq!(first, Some((2, 1)), "b→B plus d");
    assert_eq!(second, Some((3, 1)), "b→B plus d plus e, measured once");

    let mut changes = SessionFileChanges::default();
    let turn = RuntimeString::from("turn-1");
    let file = RuntimeString::from("app.rs");
    for counts in [first, second] {
        changes.record(
            Some(&turn),
            &file,
            FileChangeKind::Modify,
            counts.map(|(added, _)| added),
            counts.map(|(_, removed)| removed),
        );
    }
    assert_eq!(changes.turns.len(), 1);
    assert_eq!(
        (
            changes.turns[0].files[0].added,
            changes.turns[0].files[0].removed
        ),
        (Some(3), Some(1)),
        "the turn's own measurement stands; summing would have read +5 -2"
    );
}

/// A new turn measures from what IT finds, so the previous turn's volume never
/// carries over.
#[test]
fn the_next_turn_measures_from_its_own_baseline() {
    let cache = InMemoryFileStateCache::new();
    let path = RuntimePath::from("/w/app.rs");
    cache.begin_turn();
    cache.turn_baseline(&path, Some(&RuntimeString::from("a\nb\n")));
    cache.begin_turn();
    assert_eq!(
        cache.turn_baseline(&path, Some(&RuntimeString::from("a\nB\n"))),
        Some(RuntimeString::from("a\nB\n")),
    );
}
