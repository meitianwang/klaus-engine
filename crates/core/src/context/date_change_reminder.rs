//! Date-change reminder: when the LOCAL calendar
//! date rolls over between turns, tell the model the new date so it stops citing
//! the stale session-start date — without disturbing the cached prompt prefix.

use crate::runtime_values::RuntimeString;
use crate::types::reminder::{ReminderContext, ReminderProvider};
use async_trait::async_trait;
use chrono::{Local, TimeZone};
use std::sync::Mutex;

/// Local calendar date as `YYYY-MM-DD`. Built from the
/// LOCAL timezone so a midnight crossing is detected in the user's own timezone.
pub fn local_iso_date(now_ms: i64) -> String {
    Local
        .timestamp_millis_opt(now_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// The rollover notice text.
fn date_change_text(date: &str) -> RuntimeString {
    RuntimeString::from("The current date is now ")
        .concat(&RuntimeString::from(date))
        .concat(&RuntimeString::from("."))
}

/// A date-change reminder provider. Hold ONE instance per conversation so the
/// baseline persists across turns. The first turn only RECORDS the baseline (no
/// notice); thereafter it fires EXACTLY ONCE per rollover.
pub struct DateChangeReminderProvider {
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    last_date: Mutex<Option<String>>,
}

impl DateChangeReminderProvider {
    /// The synchronous core (the async trait method delegates to this; also lets
    /// callers drive it without an async runtime).
    pub fn tick(&self) -> Option<RuntimeString> {
        let today = local_iso_date((self.now)());
        let mut last = self.last_date.lock().unwrap();
        match last.as_deref() {
            None => {
                *last = Some(today); // baseline only — no notice on the first turn
                None
            }
            Some(prev) if prev == today => None,
            Some(_) => {
                *last = Some(today.clone()); // overwrite → fire exactly once per rollover
                Some(date_change_text(&today))
            }
        }
    }
}

#[async_trait]
impl ReminderProvider for DateChangeReminderProvider {
    async fn reminder(&self, _ctx: &ReminderContext) -> Option<RuntimeString> {
        self.tick()
    }
}

/// Build a date-change reminder provider from a clock seam.
pub fn create_date_change_reminder_provider(
    now: impl Fn() -> i64 + Send + Sync + 'static,
) -> DateChangeReminderProvider {
    DateChangeReminderProvider {
        now: Box::new(now),
        last_date: Mutex::new(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    const DAY: i64 = 86_400_000;
    const T: i64 = 1_700_000_000_000;

    #[test]
    fn local_iso_date_format() {
        let re = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
        assert!(re.is_match(&local_iso_date(T)));
    }

    #[test]
    fn records_baseline_first_turn() {
        let provider = create_date_change_reminder_provider(|| T);
        assert_eq!(provider.tick(), None);
    }

    #[test]
    fn stays_silent_same_day() {
        let clock = Arc::new(AtomicI64::new(T));
        let c = clock.clone();
        let provider = create_date_change_reminder_provider(move || c.load(Ordering::SeqCst));
        assert_eq!(provider.tick(), None); // baseline
        assert_eq!(provider.tick(), None); // same day
        clock.store(T + 1000, Ordering::SeqCst);
        assert_eq!(provider.tick(), None);
    }

    #[test]
    fn fires_once_on_rollover() {
        let clock = Arc::new(AtomicI64::new(T));
        let c = clock.clone();
        let provider = create_date_change_reminder_provider(move || c.load(Ordering::SeqCst));
        assert_eq!(provider.tick(), None); // baseline
        clock.store(T + 2 * DAY, Ordering::SeqCst);
        let out = provider.tick().unwrap();
        assert_eq!(
            out,
            RuntimeString::from(format!(
                "The current date is now {}.",
                local_iso_date(T + 2 * DAY)
            ))
        );
        assert_eq!(provider.tick(), None); // same new day → silent
    }
}
