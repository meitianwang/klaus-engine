//! Memory freshness/staleness helpers. A recalled memory reflects
//! what was true WHEN IT WAS WRITTEN, not live state — and a `file:line` citation in a
//! stale note reads as MORE authoritative, not less, so an old memory needs an explicit
//! "verify before relying" caveat. These compute a memory's age from its file mtime and
//! render the caveat from that age using a couple of fixed day
//! thresholds.
//!
//! The clock is injected (`now_ms`) rather than read from the wall clock directly, so
//! callers thread the engine's `now()` seam and tests stay
//! deterministic.

use crate::runtime_values::RuntimeString;

const MS_PER_DAY: i64 = 86_400_000;

/// Minimal shape of a memory record needed by [`memory_section`].
///
/// This captures only the fields [`memory_section`] needs: `name`, `description`,
/// `body`, and `mtime_ms`. `description` is optional (may be
/// empty/absent).
#[derive(Debug, Clone, PartialEq)]
pub struct MemorySectionInput {
    /// Memory name (heading).
    pub name: RuntimeString,
    /// Optional one-line description shown after the name.
    pub description: Option<RuntimeString>,
    /// Memory body text.
    pub body: RuntimeString,
    /// File mtime in epoch milliseconds.
    pub mtime_ms: i64,
}

/// Whole days between `mtime_ms` and `now_ms`, floor-rounded: 0 = today, 1 = yesterday,
/// 2+ = older. A future mtime / clock skew (negative delta) clamps to 0.
pub fn memory_age_days(mtime_ms: i64, now_ms: i64) -> i64 {
    // Floor of the division: (now - mtime) / MS_PER_DAY, then max(0, .).
    // For non-negative deltas Rust integer division already floors; for negative deltas
    // the result is clamped to 0 anyway, so div_euclid is unnecessary here.
    let delta = now_ms - mtime_ms;
    let days = (delta as f64 / MS_PER_DAY as f64).floor() as i64;
    days.max(0)
}

/// Human-readable age: "today" (0), "yesterday" (1), or "N days ago" (>=2).
pub fn memory_age(mtime_ms: i64, now_ms: i64) -> String {
    let d = memory_age_days(mtime_ms, now_ms);
    if d == 0 {
        return "today".to_string();
    }
    if d == 1 {
        return "yesterday".to_string();
    }
    format!("{} days ago", d)
}

/// Plain-text staleness caveat for memories older than one day. Empty for today/
/// yesterday (a freshness warning there is just noise). The >1-day threshold and the
/// "point-in-time, verify against current code" intent are the point of the caveat.
pub fn memory_freshness_text(mtime_ms: i64, now_ms: i64) -> String {
    let d = memory_age_days(mtime_ms, now_ms);
    if d <= 1 {
        return String::new();
    }
    format!(
        "This memory was saved {} days ago. It is a point-in-time note, not live state — any file paths, line numbers, or code details it cites may be out of date. Verify against the current code before relying on it as fact.",
        d
    )
}

/// The per-memory freshness caveat wrapped in a single `<system-reminder>` block (with a
/// trailing newline), or empty when the memory is fresh (<=1 day). Use this when the
/// surrounding context does not already supply a reminder wrapper; callers that wrap a
/// whole batch themselves use [`memory_freshness_text`] instead.
pub fn memory_freshness_note(mtime_ms: i64, now_ms: i64) -> String {
    let text = memory_freshness_text(mtime_ms, now_ms);
    if text.is_empty() {
        return String::new();
    }
    format!("<system-reminder>{}</system-reminder>\n", text)
}

/// Render one memory for injection, tagged with its freshness: a
/// stale memory (>1 day) leads with the verify-before-relying caveat; a fresh one just
/// notes "(saved today/yesterday)". Shared by the store's prompt projection and the live
/// recall reminder so the two can never diverge.
pub fn memory_section(rec: &MemorySectionInput, now_ms: i64) -> RuntimeString {
    let heading = match &rec.description {
        Some(desc) if !desc.is_empty() => RuntimeString::from("## ")
            .concat(&rec.name)
            .concat(&RuntimeString::from(" — "))
            .concat(desc),
        _ => RuntimeString::from("## ").concat(&rec.name),
    };
    if memory_age_days(rec.mtime_ms, now_ms) >= 2 {
        return trim_end_runtime(
            &RuntimeString::from(memory_freshness_text(rec.mtime_ms, now_ms))
                .concat(&RuntimeString::from("\n"))
                .concat(&heading)
                .concat(&RuntimeString::from("\n"))
                .concat(&rec.body),
        );
    }
    trim_end_runtime(
        &heading
            .concat(&RuntimeString::from(" (saved "))
            .concat(&RuntimeString::from(memory_age(rec.mtime_ms, now_ms)))
            .concat(&RuntimeString::from(")\n"))
            .concat(&rec.body),
    )
}

/// Strip trailing whitespace only.
fn trim_end_runtime(value: &RuntimeString) -> RuntimeString {
    let mut end = value.len_utf16();
    while end > 0 {
        let unit = value.as_utf16()[end - 1];
        if !char::from_u32(u32::from(unit)).is_some_and(char::is_whitespace) {
            break;
        }
        end -= 1;
    }
    value.slice_utf16(..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;
    const NOW: i64 = 1_000 * DAY; // a fixed reference time

    // memory_age_days
    #[test]
    fn floors_to_whole_days_today_yesterday_older() {
        assert_eq!(memory_age_days(NOW, NOW), 0);
        assert_eq!(memory_age_days(NOW - DAY + 1, NOW), 0); // <1 day still today
        assert_eq!(memory_age_days(NOW - DAY, NOW), 1);
        assert_eq!(memory_age_days(NOW - 5 * DAY, NOW), 5);
    }

    #[test]
    fn clamps_future_mtime_or_clock_skew_to_0() {
        assert_eq!(memory_age_days(NOW + 3 * DAY, NOW), 0);
    }

    // memory_age
    #[test]
    fn renders_today_yesterday_n_days_ago() {
        assert_eq!(memory_age(NOW, NOW), "today");
        assert_eq!(memory_age(NOW - DAY, NOW), "yesterday");
        assert_eq!(memory_age(NOW - 4 * DAY, NOW), "4 days ago");
    }

    // memory_freshness_text
    #[test]
    fn freshness_text_empty_for_today_and_yesterday() {
        assert_eq!(memory_freshness_text(NOW, NOW), "");
        assert_eq!(memory_freshness_text(NOW - DAY, NOW), "");
    }

    #[test]
    fn freshness_text_warns_for_older_memories() {
        let text = memory_freshness_text(NOW - 3 * DAY, NOW);
        assert!(text.contains("3 days ago"));
        assert!(text.to_lowercase().contains("verify"));
    }

    // memory_freshness_note
    #[test]
    fn freshness_note_wraps_caveat_in_system_reminder() {
        let note = memory_freshness_note(NOW - 2 * DAY, NOW);
        assert!(note.contains("<system-reminder>"));
        assert!(note.contains("</system-reminder>"));
        assert!(note.contains("2 days ago"));
    }

    #[test]
    fn freshness_note_empty_for_fresh_memory() {
        assert_eq!(memory_freshness_note(NOW, NOW), "");
        assert_eq!(memory_freshness_note(NOW - DAY, NOW), "");
    }

    #[test]
    fn memory_section_keeps_non_scalar_utf16_until_the_model_visible_prompt() {
        let name = RuntimeString::from_utf16([b'n' as u16, 0xd800]);
        let body = RuntimeString::from_utf16([b'b' as u16, 0xdc00]);
        let section = memory_section(
            &MemorySectionInput {
                name: name.clone(),
                description: Some("description".into()),
                body: body.clone(),
                mtime_ms: NOW,
            },
            NOW,
        );

        assert!(section
            .as_utf16()
            .windows(name.len_utf16())
            .any(|value| value == name.as_utf16()));
        assert!(section
            .as_utf16()
            .windows(body.len_utf16())
            .any(|value| value == body.as_utf16()));
        assert!(!section.as_utf16().contains(&0xfffd));
    }
}
