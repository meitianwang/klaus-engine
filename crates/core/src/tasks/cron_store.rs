//! Scheduled prompts, stored in `<project>/.klaus/scheduled_tasks.json`.
//!
//! Tasks come in two flavors:
//!   - One-shot (`recurring: false`) — fire once, then auto-delete.
//!   - Recurring (`recurring: true`) — fire on schedule, reschedule from now,
//!     persist until explicitly deleted via CronDelete or auto-expire after
//!     a configurable limit (`DEFAULT_CRON_JITTER_CONFIG.recurring_max_age_ms`).
//!
//! File format:
//!   `{ "tasks": [{ id, cron, prompt, createdAt, lastFiredAt?, recurring?, permanent? }] }`
//!
//! Read-modify-write cycles are serialized with the shared cross-process file
//! lock ([`with_lock`]); the scheduler additionally holds a per-project lease
//! (see `cron_lock`) so only one session drives fires.

use crate::fs::lockfile::{with_lock, LockOptions};
use crate::runtime_values::{
    runtime_codec, runtime_json, NodeBufferUtf8, RuntimeCodecError, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use std::io;

/// One scheduled prompt.
#[derive(Debug, Clone, PartialEq)]
pub struct CronTask {
    /// Short 8-hex-char id (a UUID prefix); plenty for `MAX_JOBS` and stable
    /// across the tool layer and disk.
    pub id: RuntimeString,
    /// 5-field cron string (local time) — validated on write, re-validated on read.
    pub cron: RuntimeString,
    /// Prompt to enqueue when the task fires.
    pub prompt: RuntimeString,
    /// Epoch ms when the task was created. Anchor for missed-task detection.
    pub created_at: i64,
    /// Epoch ms of the most recent fire. Written back by the scheduler after
    /// each recurring fire so next-fire computation survives process restarts.
    /// The scheduler anchors first-sight from `last_fired_at ?? created_at` — a
    /// never-fired task uses `created_at` (correct for pinned crons like
    /// `30 14 27 2 *` whose next-from-now is next year); a fired-before task
    /// reconstructs the same next-fire the prior process had in memory.
    /// Never set for one-shots (they're deleted on fire).
    pub last_fired_at: Option<i64>,
    /// When true, the task reschedules after firing instead of being deleted.
    pub recurring: bool,
    /// When true, the task is exempt from `recurring_max_age_ms` auto-expiry.
    /// Never settable through CronCreate; only honored when written directly
    /// to `scheduled_tasks.json`.
    pub permanent: bool,
    /// Runtime-only flag. `Some(false)` → session-scoped (never written to
    /// disk). File-backed tasks leave this `None`; [`write_cron_tasks`] strips
    /// it so the on-disk shape stays
    /// `{ id, cron, prompt, createdAt, lastFiredAt?, recurring?, permanent?,
    /// ownerSessionId? }`.
    pub durable: Option<bool>,
    /// The session that created this task, when known. Durable tasks outlive the
    /// session that made them, so this is provenance rather than ownership: it
    /// answers "did this conversation schedule anything" without changing who
    /// may run, edit or delete the task.
    ///
    /// Optional on purpose. A file written before this field existed has no
    /// owner, and the decoder tolerates unknown keys in both directions, so
    /// adding it needs no migration of `scheduled_tasks.json`.
    pub owner_session_id: Option<RuntimeString>,
}

impl RuntimeEncode for CronTask {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("cron", &self.cron)?;
        object.field("prompt", &self.prompt)?;
        object.field("createdAt", &self.created_at)?;
        if let Some(value) = &self.last_fired_at {
            object.field("lastFiredAt", value)?;
        }
        // Falsy flags are omitted, matching the source runtime's conditional
        // spreads; the runtime-only `durable` marker never reaches disk.
        if self.recurring {
            object.field("recurring", &true)?;
        }
        if self.permanent {
            object.field("permanent", &true)?;
        }
        // Provenance is written only when known, so a task created before this
        // field existed round-trips byte-identically instead of gaining a null.
        if let Some(owner) = &self.owner_session_id {
            object.field("ownerSessionId", owner)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CronTask {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                cron: object.required("cron", &[])?,
                prompt: object.required("prompt", &[])?,
                created_at: object.required("createdAt", &[])?,
                last_fired_at: object.optional("lastFiredAt", &[])?,
                recurring: object.optional("recurring", &[])?.unwrap_or(false),
                permanent: object.optional("permanent", &[])?.unwrap_or(false),
                durable: None,
                owner_session_id: object.optional("ownerSessionId", &[])?,
            })
        })
    }
}

const CRON_DIR_REL: &str = ".klaus";
const CRON_FILE_NAME: &str = "scheduled_tasks.json";

/// Path to the cron file inside `dir` (the project root).
pub fn cron_file_path(dir: &str) -> String {
    format!("{dir}/{CRON_DIR_REL}/{CRON_FILE_NAME}")
}

fn cron_dir_path(dir: &str) -> String {
    format!("{dir}/{CRON_DIR_REL}")
}

/// Decode one file row, dropping anything malformed. A row must carry the
/// exact required field types AND a parseable cron string; a single bad entry
/// never blocks the whole file.
fn decode_task_row(row: &RuntimeJson) -> Option<CronTask> {
    let task = CronTask::decode_runtime(row).ok()?;
    let cron = task.cron.try_to_utf8().ok()?;
    super::cron::parse_cron_expression(&cron)?;
    Some(task)
}

fn parse_task_file(raw: &RuntimeString) -> Vec<CronTask> {
    let Ok(parsed) = runtime_json::parse(raw) else {
        return Vec::new();
    };
    let Some(tasks) = parsed.get("tasks").and_then(RuntimeJson::as_array) else {
        return Vec::new();
    };
    tasks.iter().filter_map(decode_task_row).collect()
}

/// Read and parse `.klaus/scheduled_tasks.json`. Returns an empty task list if
/// the file is missing, empty, or malformed. Tasks with invalid cron strings
/// are silently dropped so a single bad entry never blocks the whole file.
pub async fn read_cron_tasks(dir: &str) -> Vec<CronTask> {
    let Ok(bytes) = tokio::fs::read(cron_file_path(dir)).await else {
        return Vec::new();
    };
    parse_task_file(&NodeBufferUtf8::decode(&bytes))
}

/// The durable tasks a given session scheduled. Answers "did this conversation
/// leave anything behind" for a summary surface, which is a different question
/// from "what runs in this project" ([`read_cron_tasks`]) — the two must not be
/// conflated, or every conversation in a project would claim every task.
///
/// A task written before provenance existed has no owner and is therefore
/// attributed to nobody, which is the honest answer rather than a guess.
pub async fn cron_tasks_for_session(dir: &str, session_id: &str) -> Vec<CronTask> {
    if session_id.is_empty() {
        return Vec::new();
    }
    read_cron_tasks(dir)
        .await
        .into_iter()
        .filter(|task| {
            task.owner_session_id
                .as_ref()
                .and_then(|owner| owner.try_to_utf8().ok())
                .is_some_and(|owner| owner == session_id)
        })
        .collect()
}

/// Sync check for whether the cron file has any valid tasks. Used by the
/// scheduler start path to decide whether to auto-enable. One file read.
pub fn has_cron_tasks_sync(dir: &str) -> bool {
    let Ok(bytes) = std::fs::read(cron_file_path(dir)) else {
        return false;
    };
    let Ok(parsed) = runtime_json::parse(&NodeBufferUtf8::decode(&bytes)) else {
        return false;
    };
    parsed
        .get("tasks")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|tasks| !tasks.is_empty())
}

fn render_task_file(tasks: &[CronTask]) -> RuntimeString {
    let rows = tasks
        .iter()
        .map(|task| runtime_codec::encode_value(task).expect("CronTask encodes to exact JSON"))
        .collect::<Vec<_>>();
    let mut body = crate::runtime_values::RuntimeObject::new();
    body.insert("tasks", RuntimeJson::Array(rows));
    runtime_json::stringify_pretty(&RuntimeJson::Object(body)).concat(&RuntimeString::from("\n"))
}

/// Overwrite `.klaus/scheduled_tasks.json` with the given tasks. Creates
/// `.klaus/` if missing. An empty task list writes an empty file (rather than
/// deleting) so pollers observe a change on last-task-removed.
pub async fn write_cron_tasks(tasks: &[CronTask], dir: &str) -> io::Result<()> {
    tokio::fs::create_dir_all(cron_dir_path(dir)).await?;
    let body = render_task_file(tasks);
    tokio::fs::write(cron_file_path(dir), body.to_node_utf8()).await
}

/// Append a durable task under the file lock. The caller has already
/// validated the cron string (the tool layer does this before create).
pub async fn add_cron_task(task: &CronTask, dir: &str) -> io::Result<()> {
    let path = cron_file_path(dir);
    tokio::fs::create_dir_all(cron_dir_path(dir)).await?;
    let task = task.clone();
    let dir = dir.to_string();
    with_lock(
        &path,
        move || async move {
            let mut tasks = read_cron_tasks(&dir).await;
            tasks.push(task);
            write_cron_tasks(&tasks, &dir).await
        },
        LockOptions::default(),
    )
    .await
}

/// Remove file-backed tasks by id. No-op if none match (e.g. another session
/// raced us). Used for both fire-once cleanup and explicit CronDelete.
pub async fn remove_cron_tasks(ids: &[RuntimeString], dir: &str) -> io::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let path = cron_file_path(dir);
    let ids: Vec<RuntimeString> = ids.to_vec();
    let dir = dir.to_string();
    with_lock(
        &path,
        move || async move {
            let tasks = read_cron_tasks(&dir).await;
            let remaining: Vec<CronTask> = tasks
                .iter()
                .filter(|task| !ids.contains(&task.id))
                .cloned()
                .collect();
            if remaining.len() == tasks.len() {
                return Ok(());
            }
            write_cron_tasks(&remaining, &dir).await
        },
        LockOptions::default(),
    )
    .await
}

/// Stamp `lastFiredAt` on the given recurring tasks and write back. Batched so
/// N fires in one scheduler tick = one read-modify-write, not N. Only touches
/// file-backed tasks — session tasks die with the process. No-op if none of
/// the ids match (task was deleted between fire and write).
pub async fn mark_cron_tasks_fired(
    ids: &[RuntimeString],
    fired_at: i64,
    dir: &str,
) -> io::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let path = cron_file_path(dir);
    let ids: Vec<RuntimeString> = ids.to_vec();
    let dir = dir.to_string();
    with_lock(
        &path,
        move || async move {
            let mut tasks = read_cron_tasks(&dir).await;
            let mut changed = false;
            for task in &mut tasks {
                if ids.contains(&task.id) {
                    task.last_fired_at = Some(fired_at);
                    changed = true;
                }
            }
            if !changed {
                return Ok(());
            }
            write_cron_tasks(&tasks, &dir).await
        },
        LockOptions::default(),
    )
    .await
}

// ---- jitter ------------------------------------------------------------------

/// Cron scheduler tuning knobs. The defaults preserve the source runtime's
/// fleet behavior exactly; there is no remote-config override in the engine.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CronJitterConfig {
    /// Recurring-task forward delay as a fraction of the interval between fires.
    pub recurring_frac: f64,
    /// Upper bound on recurring forward delay regardless of interval length.
    pub recurring_cap_ms: f64,
    /// One-shot backward lead: maximum ms a task may fire early.
    pub one_shot_max_ms: f64,
    /// One-shot backward lead: minimum ms a task fires early when the
    /// minute-mod gate matches.
    pub one_shot_floor_ms: f64,
    /// Jitter fires landing on minutes where `minute % N == 0`. 30 → :00/:30
    /// (the human-rounding hotspots).
    pub one_shot_minute_mod: u32,
    /// Recurring tasks auto-expire this many ms after creation (unless marked
    /// `permanent`). Bounds worst-case scheduler lifetime while covering
    /// "check my PRs every hour this week" workflows. `0` = unlimited.
    pub recurring_max_age_ms: i64,
}

pub const DEFAULT_CRON_JITTER_CONFIG: CronJitterConfig = CronJitterConfig {
    recurring_frac: 0.5,
    recurring_cap_ms: 30.0 * 60.0 * 1000.0,
    one_shot_max_ms: 90.0 * 1000.0,
    one_shot_floor_ms: 0.0,
    one_shot_minute_mod: 30,
    recurring_max_age_ms: 7 * 24 * 60 * 60 * 1000,
};

/// The recurring auto-expiry window in whole days (surfaced in tool text).
pub const DEFAULT_MAX_AGE_DAYS: i64 =
    DEFAULT_CRON_JITTER_CONFIG.recurring_max_age_ms / (24 * 60 * 60 * 1000);

/// `task_id` is an 8-hex-char UUID prefix → parse as u32 → [0, 1). Stable
/// across restarts, uniformly distributed. Non-hex ids (hand-edited JSON)
/// fall back to 0 = no jitter.
fn jitter_frac(task_id: &RuntimeString) -> f64 {
    let Ok(id) = task_id.try_to_utf8() else {
        return 0.0;
    };
    let prefix: String = id.chars().take(8).collect();
    match u32::from_str_radix(&prefix, 16) {
        Ok(bits) => f64::from(bits) / 4_294_967_296.0,
        Err(_) => 0.0,
    }
}

/// Same as [`super::cron::next_cron_run_ms`], plus a deterministic per-task
/// delay to avoid a thundering herd when many sessions schedule the same cron
/// string (e.g. `0 * * * *` → everyone hits inference at :00).
///
/// The delay is proportional to the current gap between fires
/// (`recurring_frac`, capped at `recurring_cap_ms`) so at defaults an hourly
/// task spreads across [:00, :06) but a per-minute task only spreads by a few
/// seconds. Only used for recurring tasks.
pub fn jittered_next_cron_run_ms(
    cron: &str,
    from_ms: i64,
    task_id: &RuntimeString,
    cfg: &CronJitterConfig,
) -> Option<i64> {
    let t1 = super::cron::next_cron_run_ms(cron, from_ms)?;
    // No second match in the next year (e.g. pinned date) → nothing to
    // proportion against, and near-certainly not a herd risk. Fire on t1.
    let Some(t2) = super::cron::next_cron_run_ms(cron, t1) else {
        return Some(t1);
    };
    let jitter =
        (jitter_frac(task_id) * cfg.recurring_frac * (t2 - t1) as f64).min(cfg.recurring_cap_ms);
    Some(t1 + jitter as i64)
}

/// Same as [`super::cron::next_cron_run_ms`], minus a deterministic per-task
/// lead time when the fire time lands on a minute boundary matching
/// `one_shot_minute_mod`.
///
/// One-shot tasks are user-pinned ("remind me at 3pm") so delaying them breaks
/// the contract — but firing slightly early is invisible and spreads the
/// inference spike from everyone picking the same round wall-clock time.
/// Checks the computed fire time rather than the cron string so `0 15 * * *`,
/// step expressions, and `0,30 9 * * *` all get jitter when they land on a
/// matching minute. Clamped to `from_ms` so a task created inside its own
/// jitter window doesn't fire before it was created.
pub fn one_shot_jittered_next_cron_run_ms(
    cron: &str,
    from_ms: i64,
    task_id: &RuntimeString,
    cfg: &CronJitterConfig,
) -> Option<i64> {
    let t1 = super::cron::next_cron_run_ms(cron, from_ms)?;
    // Cron resolution is 1 minute → computed times always have :00 seconds,
    // so a minute-field check in LOCAL time is sufficient to identify the hot
    // marks ("user picked a round time" means round in *their* timezone).
    use chrono::{Local, TimeZone, Timelike};
    let minute = Local
        .timestamp_millis_opt(t1)
        .single()
        .map(|t| t.minute())?;
    if cfg.one_shot_minute_mod == 0 || minute % cfg.one_shot_minute_mod != 0 {
        return Some(t1);
    }
    // floor + frac * (max - floor) → uniform over [floor, max).
    let lead = cfg.one_shot_floor_ms
        + jitter_frac(task_id) * (cfg.one_shot_max_ms - cfg.one_shot_floor_ms);
    Some((t1 - lead as i64).max(from_ms))
}

/// A task is "missed" when its next scheduled run (computed from `created_at`)
/// is in the past. Surfaced at startup. Works for both one-shot and recurring
/// tasks — a recurring task whose window passed while the engine was down is
/// still "missed".
pub fn find_missed_tasks(tasks: &[CronTask], now_ms: i64) -> Vec<CronTask> {
    tasks
        .iter()
        .filter(|task| {
            let Ok(cron) = task.cron.try_to_utf8() else {
                return false;
            };
            super::cron::next_cron_run_ms(&cron, task.created_at).is_some_and(|next| next < now_ms)
        })
        .cloned()
        .collect()
}

/// True when a recurring task was created more than `max_age_ms` ago and
/// should be deleted on its next fire. Permanent tasks never age.
/// `max_age_ms == 0` means unlimited (never ages out). Extracted for
/// testability — the scheduler's check loop is buried under timer machinery.
pub fn is_recurring_task_aged(task: &CronTask, now_ms: i64, max_age_ms: i64) -> bool {
    if max_age_ms == 0 {
        return false;
    }
    task.recurring && !task.permanent && now_ms - task.created_at >= max_age_ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Local, TimeZone};

    fn temp_dir(tag: &str) -> String {
        let base = std::env::temp_dir();
        let uniq = format!(
            "klaus-cron-store-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    fn task(id: &str, cron: &str, recurring: bool) -> CronTask {
        CronTask {
            id: RuntimeString::from(id),
            cron: RuntimeString::from(cron),
            prompt: RuntimeString::from("do the thing"),
            created_at: 1_000,
            last_fired_at: None,
            recurring,
            permanent: false,
            durable: None,
            owner_session_id: None,
        }
    }

    #[tokio::test]
    async fn session_provenance_is_optional_and_never_guessed() {
        let dir = temp_dir("provenance");

        // A file written before provenance existed: no `ownerSessionId` key at
        // all. It must still parse, and it must belong to nobody rather than
        // being attributed to whoever asks.
        let legacy = RuntimeString::from(
            r#"{"tasks":[{"id":"aaaa0000","cron":"*/5 * * * *","prompt":"legacy","createdAt":1}]}"#,
        );
        tokio::fs::create_dir_all(cron_dir_path(&dir))
            .await
            .unwrap();
        tokio::fs::write(cron_file_path(&dir), legacy.to_node_utf8())
            .await
            .unwrap();
        let parsed = read_cron_tasks(&dir).await;
        assert_eq!(parsed.len(), 1, "a file without provenance still parses");
        assert!(
            parsed[0].owner_session_id.is_none(),
            "no owner is not an owner"
        );
        assert!(
            cron_tasks_for_session(&dir, "sess-1").await.is_empty(),
            "an unowned task must not be claimed by a session"
        );

        // A stamped task round-trips through disk and is found by its creator
        // only. Another session in the same project sees nothing of it.
        let mut owned = task("bbbb1111", "*/5 * * * *", true);
        owned.owner_session_id = Some(RuntimeString::from("sess-1"));
        write_cron_tasks(&[owned], &dir).await.unwrap();
        let mine = cron_tasks_for_session(&dir, "sess-1").await;
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].id.try_to_utf8().unwrap(), "bbbb1111");
        assert!(
            cron_tasks_for_session(&dir, "sess-2").await.is_empty(),
            "a shared project file must not leak one session's tasks to another"
        );
        // An empty session id asks nothing and gets nothing, instead of matching
        // every unowned row.
        assert!(cron_tasks_for_session(&dir, "").await.is_empty());
    }

    #[tokio::test]
    async fn round_trips_tasks_and_strips_runtime_only_flags() {
        let dir = temp_dir("roundtrip");
        let mut stored = task("11aabb22", "*/5 * * * *", true);
        stored.durable = Some(false); // runtime-only marker must not reach disk
        write_cron_tasks(&[stored], &dir).await.unwrap();

        let raw = std::fs::read_to_string(cron_file_path(&dir)).unwrap();
        assert!(raw.contains("\"recurring\": true"));
        assert!(!raw.contains("durable"));
        assert!(raw.ends_with('\n'));

        let read = read_cron_tasks(&dir).await;
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].id, RuntimeString::from("11aabb22"));
        assert!(read[0].recurring);
        assert_eq!(read[0].durable, None);
        assert!(has_cron_tasks_sync(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_or_malformed_file_reads_as_empty() {
        let dir = temp_dir("malformed");
        assert!(read_cron_tasks(&dir).await.is_empty());
        assert!(!has_cron_tasks_sync(&dir));

        std::fs::create_dir_all(cron_dir_path(&dir)).unwrap();
        std::fs::write(cron_file_path(&dir), b"not json").unwrap();
        assert!(read_cron_tasks(&dir).await.is_empty());
        std::fs::write(cron_file_path(&dir), b"{\"tasks\": 5}").unwrap();
        assert!(read_cron_tasks(&dir).await.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn malformed_rows_and_invalid_crons_are_dropped_individually() {
        let dir = temp_dir("rows");
        std::fs::create_dir_all(cron_dir_path(&dir)).unwrap();
        std::fs::write(
            cron_file_path(&dir),
            br#"{"tasks":[
                {"id":"good0001","cron":"*/5 * * * *","prompt":"p","createdAt":1},
                {"id":"badcron1","cron":"@daily","prompt":"p","createdAt":1},
                {"id":42,"cron":"*/5 * * * *","prompt":"p","createdAt":1},
                {"id":"missing1","cron":"*/5 * * * *","createdAt":1}
            ]}"#,
        )
        .unwrap();
        let read = read_cron_tasks(&dir).await;
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].id, RuntimeString::from("good0001"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn add_remove_and_mark_fired_mutate_the_file() {
        let dir = temp_dir("mutate");
        add_cron_task(&task("aaaaaaaa", "*/5 * * * *", true), &dir)
            .await
            .unwrap();
        add_cron_task(&task("bbbbbbbb", "0 9 * * *", false), &dir)
            .await
            .unwrap();
        assert_eq!(read_cron_tasks(&dir).await.len(), 2);

        mark_cron_tasks_fired(&[RuntimeString::from("aaaaaaaa")], 5_000, &dir)
            .await
            .unwrap();
        let read = read_cron_tasks(&dir).await;
        let fired = read.iter().find(|t| t.id == "aaaaaaaa").unwrap();
        assert_eq!(fired.last_fired_at, Some(5_000));
        let other = read.iter().find(|t| t.id == "bbbbbbbb").unwrap();
        assert_eq!(other.last_fired_at, None);

        remove_cron_tasks(&[RuntimeString::from("aaaaaaaa")], &dir)
            .await
            .unwrap();
        let read = read_cron_tasks(&dir).await;
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].id, RuntimeString::from("bbbbbbbb"));

        // Removing the last task keeps an (empty) file on disk.
        remove_cron_tasks(&[RuntimeString::from("bbbbbbbb")], &dir)
            .await
            .unwrap();
        assert!(std::fs::metadata(cron_file_path(&dir)).is_ok());
        assert!(read_cron_tasks(&dir).await.is_empty());
        assert!(!has_cron_tasks_sync(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn jitter_frac_is_deterministic_and_bounded() {
        let frac = |id: &str| jitter_frac(&RuntimeString::from(id));
        assert_eq!(frac("00000000"), 0.0);
        assert!(frac("ffffffff") > 0.999);
        assert_eq!(frac("not-hex!"), 0.0);
        assert_eq!(frac("8f3a2b1c"), frac("8f3a2b1c"));
    }

    #[test]
    fn recurring_jitter_delays_forward_proportionally() {
        let from = Local
            .with_ymd_and_hms(2026, 6, 1, 8, 3, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let base = super::super::cron::next_cron_run_ms("0 * * * *", from).unwrap();
        // Zero hash → no jitter.
        let zero = jittered_next_cron_run_ms(
            "0 * * * *",
            from,
            &RuntimeString::from("00000000"),
            &DEFAULT_CRON_JITTER_CONFIG,
        )
        .unwrap();
        assert_eq!(zero, base);
        // Max hash → half the hourly interval, capped at 30min.
        let high = jittered_next_cron_run_ms(
            "0 * * * *",
            from,
            &RuntimeString::from("ffffffff"),
            &DEFAULT_CRON_JITTER_CONFIG,
        )
        .unwrap();
        assert!(high > base);
        assert!(high - base <= 30 * 60 * 1000);
    }

    #[test]
    fn one_shot_jitter_only_applies_on_hot_minute_marks() {
        let from = Local
            .with_ymd_and_hms(2026, 6, 1, 8, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        // :30 mark → jitter fires early.
        let base = super::super::cron::next_cron_run_ms("30 9 1 6 *", from).unwrap();
        let jittered = one_shot_jittered_next_cron_run_ms(
            "30 9 1 6 *",
            from,
            &RuntimeString::from("ffffffff"),
            &DEFAULT_CRON_JITTER_CONFIG,
        )
        .unwrap();
        assert!(jittered < base);
        assert!(base - jittered <= 90 * 1000);
        // :07 mark → untouched.
        let base = super::super::cron::next_cron_run_ms("7 9 1 6 *", from).unwrap();
        let jittered = one_shot_jittered_next_cron_run_ms(
            "7 9 1 6 *",
            from,
            &RuntimeString::from("ffffffff"),
            &DEFAULT_CRON_JITTER_CONFIG,
        )
        .unwrap();
        assert_eq!(jittered, base);
    }

    #[test]
    fn one_shot_jitter_clamps_to_creation_time() {
        // A task created INSIDE its own lead window cannot fire before creation.
        let mark = Local
            .with_ymd_and_hms(2026, 6, 1, 9, 30, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let from = mark - 10_000; // 10s before the :30 mark, inside the 90s lead
        let jittered = one_shot_jittered_next_cron_run_ms(
            "30 9 1 6 *",
            from,
            &RuntimeString::from("ffffffff"),
            &DEFAULT_CRON_JITTER_CONFIG,
        )
        .unwrap();
        assert!(jittered >= from);
    }

    #[test]
    fn find_missed_tasks_anchors_from_created_at() {
        let now = Local
            .with_ymd_and_hms(2026, 6, 2, 12, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let mut past = task("aaaaaaaa", "0 9 * * *", false);
        past.created_at = now - 24 * 60 * 60 * 1000; // 9am fell in between
        let mut future = task("bbbbbbbb", "0 9 * * *", false);
        future.created_at = now; // next 9am is tomorrow
        let missed = find_missed_tasks(&[past.clone(), future], now);
        assert_eq!(missed.len(), 1);
        assert_eq!(missed[0].id, past.id);
    }

    #[test]
    fn recurring_age_out_respects_permanent_and_unlimited() {
        let max_age = DEFAULT_CRON_JITTER_CONFIG.recurring_max_age_ms;
        let mut aged = task("aaaaaaaa", "0 * * * *", true);
        aged.created_at = 0;
        assert!(is_recurring_task_aged(&aged, max_age, max_age));
        assert!(!is_recurring_task_aged(&aged, max_age - 1, max_age));
        assert!(!is_recurring_task_aged(&aged, max_age, 0)); // unlimited
        let mut permanent = aged.clone();
        permanent.permanent = true;
        assert!(!is_recurring_task_aged(&permanent, max_age, max_age));
        let mut one_shot = aged.clone();
        one_shot.recurring = false;
        assert!(!is_recurring_task_aged(&one_shot, max_age, max_age));
    }
}
