//! Minimal cron expression parsing and next-run calculation.
//!
//! Supports the standard 5-field cron subset:
//!   minute hour day-of-month month day-of-week
//!
//! Field syntax: wildcard, N, step (star-slash-N), range (N-M), list (N,M,...).
//! No L, W, ?, or name aliases. All times are interpreted in the process's
//! local timezone — "0 9 * * *" means 9am wherever the engine is running.

use chrono::{DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, TimeZone, Timelike};

/// A parsed 5-field cron expression, each field expanded into the sorted set
/// of matching values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronFields {
    pub minute: Vec<u32>,
    pub hour: Vec<u32>,
    pub day_of_month: Vec<u32>,
    pub month: Vec<u32>,
    pub day_of_week: Vec<u32>,
}

struct FieldRange {
    min: u32,
    max: u32,
}

const FIELD_RANGES: [FieldRange; 5] = [
    FieldRange { min: 0, max: 59 }, // minute
    FieldRange { min: 0, max: 23 }, // hour
    FieldRange { min: 1, max: 31 }, // dayOfMonth
    FieldRange { min: 1, max: 12 }, // month
    FieldRange { min: 0, max: 6 },  // dayOfWeek (0=Sunday; 7 accepted as Sunday alias)
];

fn parse_u32(text: &str) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<u32>().ok()
}

/// Parse a single cron field into a sorted array of matching values.
/// Supports: wildcard, N, star-slash-N (step), N-M (range), and comma-lists.
/// Returns `None` if invalid.
fn expand_field(field: &str, range: &FieldRange) -> Option<Vec<u32>> {
    let FieldRange { min, max } = *range;
    let mut out = std::collections::BTreeSet::new();

    for part in field.split(',') {
        // wildcard or star-slash-N
        if let Some(rest) = part.strip_prefix('*') {
            let step = if rest.is_empty() {
                1
            } else if let Some(step_text) = rest.strip_prefix('/') {
                parse_u32(step_text)?
            } else {
                return None;
            };
            if step < 1 {
                return None;
            }
            let mut i = min;
            while i <= max {
                out.insert(i);
                i += step;
            }
            continue;
        }

        // N-M or N-M/S
        if let Some((lo_text, rest)) = part.split_once('-') {
            let (hi_text, step_text) = match rest.split_once('/') {
                Some((hi, step)) => (hi, Some(step)),
                None => (rest, None),
            };
            let lo = parse_u32(lo_text)?;
            let hi = parse_u32(hi_text)?;
            let step = match step_text {
                Some(text) => parse_u32(text)?,
                None => 1,
            };
            // dayOfWeek: accept 7 as Sunday alias in ranges (e.g. 5-7 = Fri,Sat,Sun → [5,6,0])
            let is_dow = min == 0 && max == 6;
            let eff_max = if is_dow { 7 } else { max };
            if lo > hi || step < 1 || lo < min || hi > eff_max {
                return None;
            }
            let mut i = lo;
            while i <= hi {
                out.insert(if is_dow && i == 7 { 0 } else { i });
                i += step;
            }
            continue;
        }

        // plain N
        if let Some(mut n) = parse_u32(part) {
            // dayOfWeek: accept 7 as Sunday alias → 0
            if min == 0 && max == 6 && n == 7 {
                n = 0;
            }
            if n < min || n > max {
                return None;
            }
            out.insert(n);
            continue;
        }

        return None;
    }

    if out.is_empty() {
        return None;
    }
    Some(out.into_iter().collect())
}

/// Parse a 5-field cron expression into expanded number arrays.
/// Returns `None` if invalid or unsupported syntax.
pub fn parse_cron_expression(expr: &str) -> Option<CronFields> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }

    let mut expanded: Vec<Vec<u32>> = Vec::with_capacity(5);
    for (part, range) in parts.iter().zip(FIELD_RANGES.iter()) {
        expanded.push(expand_field(part, range)?);
    }

    let day_of_week = expanded.pop().unwrap();
    let month = expanded.pop().unwrap();
    let day_of_month = expanded.pop().unwrap();
    let hour = expanded.pop().unwrap();
    let minute = expanded.pop().unwrap();
    Some(CronFields {
        minute,
        hour,
        day_of_month,
        month,
        day_of_week,
    })
}

/// Resolve a local wall-clock time the way the source runtime's `Date` setters
/// normalize it: an ambiguous fall-back time takes the first occurrence, and a
/// spring-forward gap rolls forward minute-by-minute to the first time that
/// exists in local time.
fn resolve_local(date: NaiveDate, hour: u32, minute: u32) -> Option<DateTime<Local>> {
    let mut naive = date.and_hms_opt(hour, minute, 0)?;
    // A DST gap is at most a few hours on real timezones; bound the walk.
    for _ in 0..=(3 * 60) {
        match Local.from_local_datetime(&naive) {
            LocalResult::Single(t) => return Some(t),
            LocalResult::Ambiguous(first, _) => return Some(first),
            LocalResult::None => naive += Duration::minutes(1),
        }
    }
    None
}

/// Compute the next local time strictly after `from` that matches the cron
/// fields. Walks forward minute-by-minute with month/day/hour jumps. Bounded
/// at 366 days; returns `None` if no match (impossible for a valid cron that
/// matches any calendar date, but satisfies the type).
///
/// Standard cron semantics: when both dayOfMonth and dayOfWeek are constrained
/// (neither is the full range), a date matches if EITHER matches.
///
/// DST: fixed-hour crons targeting a spring-forward gap (e.g. `30 2 * * *`
/// in a US timezone) skip the transition day — the gap hour never appears
/// in local time, so the hour-set check fails and the loop moves on.
/// Wildcard-hour crons (`30 * * * *`) fire at the first valid minute after
/// the gap. This matches vixie-cron behavior.
pub fn compute_next_cron_run(
    fields: &CronFields,
    from: DateTime<Local>,
) -> Option<DateTime<Local>> {
    let minute_set: std::collections::BTreeSet<u32> = fields.minute.iter().copied().collect();
    let hour_set: std::collections::BTreeSet<u32> = fields.hour.iter().copied().collect();
    let dom_set: std::collections::BTreeSet<u32> = fields.day_of_month.iter().copied().collect();
    let month_set: std::collections::BTreeSet<u32> = fields.month.iter().copied().collect();
    let dow_set: std::collections::BTreeSet<u32> = fields.day_of_week.iter().copied().collect();

    // Is the field wildcarded (full range)?
    let dom_wild = fields.day_of_month.len() == 31;
    let dow_wild = fields.day_of_week.len() == 7;

    // Round up to the next whole minute (strictly after `from`).
    let from_ts = from.timestamp_millis();
    let next_minute_ms = from_ts.div_euclid(60_000) * 60_000 + 60_000;
    let mut t = Local.timestamp_millis_opt(next_minute_ms).single()?;

    let max_iter = 366 * 24 * 60;
    for _ in 0..max_iter {
        let month = t.month();
        if !month_set.contains(&month) {
            // Jump to start of next month.
            let (year, next_month) = if month == 12 {
                (t.year() + 1, 1)
            } else {
                (t.year(), month + 1)
            };
            t = resolve_local(NaiveDate::from_ymd_opt(year, next_month, 1)?, 0, 0)?;
            continue;
        }

        let dom = t.day();
        // Sunday = 0, matching the source runtime's getDay().
        let dow = t.weekday().num_days_from_sunday();
        // When both dom/dow are constrained, either match is sufficient (OR semantics).
        let day_matches = if dom_wild && dow_wild {
            true
        } else if dom_wild {
            dow_set.contains(&dow)
        } else if dow_wild {
            dom_set.contains(&dom)
        } else {
            dom_set.contains(&dom) || dow_set.contains(&dow)
        };

        if !day_matches {
            // Jump to start of next day.
            t = resolve_local(t.date_naive().succ_opt()?, 0, 0)?;
            continue;
        }

        if !hour_set.contains(&t.hour()) {
            // Jump to the start of the next hour (field-based, rolling the day).
            let hour = t.hour();
            t = if hour >= 23 {
                resolve_local(t.date_naive().succ_opt()?, 0, 0)?
            } else {
                resolve_local(t.date_naive(), hour + 1, 0)?
            };
            continue;
        }

        if !minute_set.contains(&t.minute()) {
            t += Duration::minutes(1);
            continue;
        }

        return Some(t);
    }

    None
}

/// Next fire time in epoch ms for a cron string, strictly after `from_ms`.
/// Returns `None` if invalid or no match in the next 366 days.
pub fn next_cron_run_ms(cron: &str, from_ms: i64) -> Option<i64> {
    let fields = parse_cron_expression(cron)?;
    let from = Local.timestamp_millis_opt(from_ms).single()?;
    compute_next_cron_run(&fields, from).map(|t| t.timestamp_millis())
}

// --- cron_to_human -----------------------------------------------------------
// Intentionally narrow: covers common patterns; falls through to the raw cron
// string for anything else. Local scheduled tasks always use local-time cron
// strings, so no UTC translation is required.

const DAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// Format a local wall-clock time the way the source runtime renders
/// `toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })`.
fn format_local_time(minute: u32, hour: u32) -> String {
    let hour12 = (hour + 11) % 12 + 1;
    let suffix = if hour < 12 { "AM" } else { "PM" };
    format!("{hour12}:{minute:02} {suffix}")
}

/// Render a human-readable description for common cron shapes; anything else
/// falls through to the raw cron string.
pub fn cron_to_human(cron: &str) -> String {
    let parts: Vec<&str> = cron.split_whitespace().collect();
    if parts.len() != 5 {
        return cron.to_string();
    }

    let (minute, hour, day_of_month, month, day_of_week) =
        (parts[0], parts[1], parts[2], parts[3], parts[4]);

    // Every N minutes: */N * * * *
    if let Some(step_text) = minute.strip_prefix("*/") {
        if let Some(n) = parse_u32(step_text) {
            if hour == "*" && day_of_month == "*" && month == "*" && day_of_week == "*" {
                return if n == 1 {
                    "Every minute".to_string()
                } else {
                    format!("Every {n} minutes")
                };
            }
        }
    }

    // Every hour: M * * * *
    if let Some(m) = parse_u32(minute) {
        if hour == "*" && day_of_month == "*" && month == "*" && day_of_week == "*" {
            if m == 0 {
                return "Every hour".to_string();
            }
            return format!("Every hour at :{m:02}");
        }
    }

    // Every N hours: M */N * * *
    if let (Some(m), Some(step_text)) = (parse_u32(minute), hour.strip_prefix("*/")) {
        if let Some(n) = parse_u32(step_text) {
            if day_of_month == "*" && month == "*" && day_of_week == "*" {
                let suffix = if m == 0 {
                    String::new()
                } else {
                    format!(" at :{m:02}")
                };
                return if n == 1 {
                    format!("Every hour{suffix}")
                } else {
                    format!("Every {n} hours{suffix}")
                };
            }
        }
    }

    // --- Remaining cases reference a pinned hour+minute ----------------------

    let (Some(m), Some(h)) = (parse_u32(minute), parse_u32(hour)) else {
        return cron.to_string();
    };
    if h > 23 || m > 59 {
        return cron.to_string();
    }

    // Daily at specific time: M H * * *
    if day_of_month == "*" && month == "*" && day_of_week == "*" {
        return format!("Every day at {}", format_local_time(m, h));
    }

    // Specific day of week: M H * * D
    if day_of_month == "*" && month == "*" && day_of_week.len() == 1 {
        if let Some(day_index) = parse_u32(day_of_week) {
            if day_index <= 7 {
                // normalize 7 (Sunday alias) -> 0
                let day_name = DAY_NAMES[(day_index % 7) as usize];
                return format!("Every {day_name} at {}", format_local_time(m, h));
            }
        }
    }

    // Weekdays: M H * * 1-5
    if day_of_month == "*" && month == "*" && day_of_week == "1-5" {
        return format!("Weekdays at {}", format_local_time(m, h));
    }

    cron.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Local> {
        Local
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .expect("unambiguous local test time")
    }

    // ---- parse_cron_expression -----------------------------------------------

    #[test]
    fn parses_wildcards_steps_ranges_and_lists() {
        let fields = parse_cron_expression("*/15 9-17 1,15 * 1-5").unwrap();
        assert_eq!(fields.minute, vec![0, 15, 30, 45]);
        assert_eq!(fields.hour, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
        assert_eq!(fields.day_of_month, vec![1, 15]);
        assert_eq!(fields.month, (1..=12).collect::<Vec<_>>());
        assert_eq!(fields.day_of_week, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn parses_stepped_ranges() {
        let fields = parse_cron_expression("10-40/10 * * * *").unwrap();
        assert_eq!(fields.minute, vec![10, 20, 30, 40]);
    }

    #[test]
    fn day_of_week_seven_is_sunday_alias() {
        let fields = parse_cron_expression("0 0 * * 7").unwrap();
        assert_eq!(fields.day_of_week, vec![0]);
        // 5-7 = Fri,Sat,Sun → [0, 5, 6] once sorted.
        let fields = parse_cron_expression("0 0 * * 5-7").unwrap();
        assert_eq!(fields.day_of_week, vec![0, 5, 6]);
    }

    #[test]
    fn rejects_invalid_expressions() {
        for expr in [
            "",
            "* * * *",
            "* * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * 32 * *",
            "* * * 13 *",
            "* * * * 8",
            "a * * * *",
            "*/0 * * * *",
            "5-3 * * * *",
            "1-70 * * * *",
            "@daily",
            "1,,2 * * * *",
            "-5 * * * *",
        ] {
            assert!(parse_cron_expression(expr).is_none(), "accepted {expr:?}");
        }
    }

    #[test]
    fn accepts_extra_whitespace() {
        assert!(parse_cron_expression("  0   9  *  *  *  ").is_some());
    }

    // ---- compute_next_cron_run -------------------------------------------------

    #[test]
    fn next_run_is_strictly_after_from() {
        let fields = parse_cron_expression("* * * * *").unwrap();
        let from = local(2026, 3, 10, 12, 0);
        let next = compute_next_cron_run(&fields, from).unwrap();
        assert_eq!(next, local(2026, 3, 10, 12, 1));
        // Mid-minute rounds up to the NEXT whole minute.
        let from = from + Duration::seconds(30);
        let next = compute_next_cron_run(&fields, from).unwrap();
        assert_eq!(next, local(2026, 3, 10, 12, 1));
    }

    #[test]
    fn every_five_minutes_snaps_to_step() {
        let fields = parse_cron_expression("*/5 * * * *").unwrap();
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 8, 3)).unwrap();
        assert_eq!(next, local(2026, 6, 1, 8, 5));
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 8, 55)).unwrap();
        assert_eq!(next, local(2026, 6, 1, 9, 0));
    }

    #[test]
    fn daily_time_rolls_to_tomorrow_when_passed() {
        let fields = parse_cron_expression("30 9 * * *").unwrap();
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 8, 0)).unwrap();
        assert_eq!(next, local(2026, 6, 1, 9, 30));
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 10, 0)).unwrap();
        assert_eq!(next, local(2026, 6, 2, 9, 30));
    }

    #[test]
    fn weekday_constraint_walks_to_matching_day() {
        // 2026-06-01 is a Monday; * * 5 = Friday.
        let fields = parse_cron_expression("0 12 * * 5").unwrap();
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 0, 0)).unwrap();
        assert_eq!(next, local(2026, 6, 5, 12, 0));
    }

    #[test]
    fn dom_and_dow_together_use_or_semantics() {
        // Vixie-cron: day 15 OR Friday, whichever comes first.
        // From Mon 2026-06-01: Friday 2026-06-05 precedes the 15th.
        let fields = parse_cron_expression("0 0 15 * 5").unwrap();
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 12, 0)).unwrap();
        assert_eq!(next, local(2026, 6, 5, 0, 0));
        // From Sat 2026-06-06: the 15th (a Monday) precedes next Friday the 12th?
        // No — Friday 2026-06-12 precedes Monday the 15th.
        let next = compute_next_cron_run(&fields, local(2026, 6, 6, 12, 0)).unwrap();
        assert_eq!(next, local(2026, 6, 12, 0, 0));
    }

    #[test]
    fn month_jump_lands_on_pinned_date() {
        let fields = parse_cron_expression("30 14 28 2 *").unwrap();
        let next = compute_next_cron_run(&fields, local(2026, 6, 1, 0, 0)).unwrap();
        assert_eq!(next, local(2027, 2, 28, 14, 30));
    }

    #[test]
    fn impossible_date_returns_none() {
        // February 30 never exists; bounded walk gives up after 366 days.
        let fields = parse_cron_expression("0 0 30 2 *").unwrap();
        assert!(compute_next_cron_run(&fields, local(2026, 6, 1, 0, 0)).is_none());
    }

    #[test]
    fn next_cron_run_ms_round_trips_epoch_millis() {
        let from = local(2026, 6, 1, 8, 3);
        let expected = local(2026, 6, 1, 8, 5).timestamp_millis();
        assert_eq!(
            next_cron_run_ms("*/5 * * * *", from.timestamp_millis()),
            Some(expected)
        );
        assert_eq!(
            next_cron_run_ms("not a cron", from.timestamp_millis()),
            None
        );
    }

    // ---- cron_to_human -----------------------------------------------------

    #[test]
    fn humanizes_common_patterns() {
        assert_eq!(cron_to_human("*/1 * * * *"), "Every minute");
        assert_eq!(cron_to_human("*/5 * * * *"), "Every 5 minutes");
        assert_eq!(cron_to_human("0 * * * *"), "Every hour");
        assert_eq!(cron_to_human("7 * * * *"), "Every hour at :07");
        assert_eq!(cron_to_human("0 */2 * * *"), "Every 2 hours");
        assert_eq!(cron_to_human("15 */3 * * *"), "Every 3 hours at :15");
        assert_eq!(cron_to_human("0 9 * * *"), "Every day at 9:00 AM");
        assert_eq!(cron_to_human("30 14 * * *"), "Every day at 2:30 PM");
        assert_eq!(cron_to_human("0 0 * * *"), "Every day at 12:00 AM");
        assert_eq!(cron_to_human("0 12 * * *"), "Every day at 12:00 PM");
        assert_eq!(cron_to_human("0 9 * * 1"), "Every Monday at 9:00 AM");
        assert_eq!(cron_to_human("0 9 * * 7"), "Every Sunday at 9:00 AM");
        assert_eq!(cron_to_human("0 9 * * 1-5"), "Weekdays at 9:00 AM");
    }

    #[test]
    fn falls_through_to_raw_cron_for_uncommon_shapes() {
        assert_eq!(cron_to_human("0 9 15 * *"), "0 9 15 * *");
        assert_eq!(cron_to_human("*/5 9 * * *"), "*/5 9 * * *");
        assert_eq!(cron_to_human("not a cron"), "not a cron");
    }
}
