use super::Session;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::QueuedValue;
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// The session-level cron scheduler: a 1s tick loop over
// `.klaus/scheduled_tasks.json` plus the in-memory session-only task store.
//
// Lifecycle: the loop starts when the project file already has tasks at
// Session construction, or when the first CronCreate registers a task. Fired
// prompts are enqueued as pending `later`-priority meta messages, exactly like
// the source runtime's scheduled-task queue path; a running turn is never
// interrupted (fires are deferred while a turn is active). A per-project lease
// (`.klaus/scheduled_tasks.lock`) guarantees only one session drives
// file-backed fires; session-only tasks are process-private and exempt.
// ---------------------------------------------------------------------------

use crate::tasks::cron::cron_to_human;
use crate::tasks::cron_lock::{release_cron_scheduler_lock, try_acquire_cron_scheduler_lock};
use crate::tasks::cron_store::{
    add_cron_task, cron_file_path, find_missed_tasks, has_cron_tasks_sync, is_recurring_task_aged,
    jittered_next_cron_run_ms, mark_cron_tasks_fired, one_shot_jittered_next_cron_run_ms,
    read_cron_tasks, remove_cron_tasks, CronTask, DEFAULT_CRON_JITTER_CONFIG,
};
use crate::types::tool::{CronController, CronJobInfo};

/// Scheduler tick cadence.
pub(crate) const CRON_CHECK_INTERVAL_MS: u64 = 1_000;
/// How often (in ticks) a non-owning session re-probes the scheduler lock.
/// Coarse because takeover only matters when the owning session has crashed.
pub(crate) const CRON_LOCK_PROBE_TICKS: u64 = 5;

/// The Session's cron scheduler state. All interior-mutable so the tick task,
/// the controller, and tool calls share it through the `Arc<Session>`.
#[derive(Default)]
pub(crate) struct CronRuntime {
    /// One-way latch: the tick loop has been spawned.
    pub(crate) started: AtomicBool,
    /// Whether this session currently owns the per-project scheduler lease.
    pub(crate) is_owner: AtomicBool,
    /// One-way latch: the initial file load (with missed-task surfacing) ran.
    pub(crate) initial_loaded: AtomicBool,
    /// Session-only tasks (`durable: false`); never written to disk.
    pub(crate) session_tasks: Mutex<Vec<CronTask>>,
    /// File-backed tasks as of the last (re)load.
    pub(crate) file_tasks: Mutex<Vec<CronTask>>,
    /// Change signature of the cron file at the last load: (mtime ms, size).
    pub(crate) file_signature: Mutex<Option<(i64, u64)>>,
    /// Per-task next-fire times (epoch ms; `i64::MAX` = never).
    pub(crate) next_fire_at: Mutex<HashMap<String, i64>>,
    /// Ids already surfaced as "missed" — prevents re-asking before an answer.
    pub(crate) missed_asked: Mutex<HashSet<String>>,
}

fn task_key(id: &RuntimeString) -> String {
    String::from_utf8(id.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
}

/// Read the cron file's change signature (mtime ms + length), or `None` when
/// the file is missing. Poll-based equivalent of the source runtime's file
/// watcher.
fn cron_file_signature(dir: &str) -> Option<(i64, u64)> {
    let metadata = std::fs::metadata(cron_file_path(dir)).ok()?;
    let mtime_ms = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Some((mtime_ms, metadata.len()))
}

/// Build the missed-task notification text. Guidance precedes the task list
/// and each prompt is wrapped in a code fence so a multi-line imperative
/// prompt is not interpreted as immediate instructions (self-inflicted prompt
/// injection). The full prompt body is preserved — this path DOES need the
/// model to execute the prompt after user confirmation, and the tasks are
/// already deleted from the JSON before the model sees this notification.
pub(crate) fn build_missed_task_notification(missed: &[CronTask]) -> RuntimeString {
    let plural = missed.len() > 1;
    let header = format!(
        "The following one-shot scheduled task{} missed while the agent was not running. \
         {} already been removed from .klaus/scheduled_tasks.json.\n\n\
         Do NOT execute {} yet. \
         First use the AskUserQuestion tool to ask whether to run {} now. \
         Only execute if the user confirms.",
        if plural { "s were" } else { " was" },
        if plural { "They have" } else { "It has" },
        if plural {
            "these prompts"
        } else {
            "this prompt"
        },
        if plural { "each one" } else { "it" },
    );

    let mut out = RuntimeString::from(header);
    for task in missed {
        let cron_text = task.cron.try_to_utf8().unwrap_or_default();
        let meta = format!(
            "[{}, created {}]",
            cron_to_human(&cron_text),
            format_local_timestamp(task.created_at)
        );
        // Use a fence one longer than any backtick run in the prompt so a
        // prompt containing ``` cannot close the fence early and un-wrap the
        // trailing text (CommonMark fence-matching rule).
        let mut longest_run = 0usize;
        let mut current_run = 0usize;
        for unit in task.prompt.as_utf16() {
            if *unit == u16::from(b'`') {
                current_run += 1;
                longest_run = longest_run.max(current_run);
            } else {
                current_run = 0;
            }
        }
        let fence = "`".repeat(3.max(longest_run + 1));
        out = out
            .concat(&RuntimeString::from(format!("\n\n{meta}\n{fence}\n")))
            .concat(&task.prompt)
            .concat(&RuntimeString::from(format!("\n{fence}")));
    }
    out
}

/// Local wall-clock rendering for the missed-task metadata line.
fn format_local_timestamp(epoch_ms: i64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_millis_opt(epoch_ms).single() {
        Some(t) => t.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => epoch_ms.to_string(),
    }
}

impl Session {
    /// The project directory keyed by the cron store. Deliberately the
    /// pre-worktree cwd: entering a worktree must not fork the schedule file.
    pub(crate) fn cron_project_dir(&self) -> String {
        let path = self.workspace.original_cwd();
        String::from_utf8(path.as_runtime_string().to_node_utf8())
            .expect("Node UTF-8 projection is valid UTF-8")
    }

    /// Auto-start at construction when the project file already has tasks
    /// (the counterpart of the source runtime's `hasCronTasksSync` gate).
    pub(crate) fn start_cron_scheduler_if_project_has_tasks(self: &Arc<Self>) {
        if !self.config.cron {
            return;
        }
        if has_cron_tasks_sync(&self.cron_project_dir()) {
            self.ensure_cron_scheduler_started();
        }
    }

    /// Spawn the 1s tick loop once. The loop holds only a `Weak<Session>`
    /// and exits within one tick of disposal (releasing the lease if owned).
    pub(crate) fn ensure_cron_scheduler_started(self: &Arc<Self>) {
        if !self.config.cron {
            return;
        }
        if self.cron.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick: u64 = 0;
            loop {
                let Some(session) = weak.upgrade() else {
                    return;
                };
                if session.lifecycle.is_disposed() {
                    if session.cron.is_owner.swap(false, Ordering::SeqCst) {
                        let dir = session.cron_project_dir();
                        release_cron_scheduler_lock(&dir, &session.id).await;
                    }
                    return;
                }
                session.cron_scheduler_tick(tick).await;
                drop(session);
                tick += 1;
                tokio::time::sleep(std::time::Duration::from_millis(CRON_CHECK_INTERVAL_MS)).await;
            }
        });
    }

    /// One scheduler tick: lease probing, (re)load, then the fire check.
    pub(crate) async fn cron_scheduler_tick(self: &Arc<Self>, tick: u64) {
        let dir = self.cron_project_dir();

        // Acquire (or re-probe) the per-project scheduler lease. Only the
        // owning session drives file-backed tasks; passive sessions keep
        // probing so they can take over if the owner dies.
        if !self.cron.is_owner.load(Ordering::SeqCst) && tick.is_multiple_of(CRON_LOCK_PROBE_TICKS)
        {
            let owned = try_acquire_cron_scheduler_lock(&dir, &self.id, (self.deps.now)())
                .await
                .unwrap_or(false);
            self.cron.is_owner.store(owned, Ordering::SeqCst);
        }

        // Initial load: surface missed one-shot tasks exactly once. Poll-based
        // reloads leave overdue tasks to the check below (which anchors from
        // createdAt and fires immediately) — that avoids a misleading "missed
        // while the agent was not running" prompt for tasks that became
        // overdue mid-session.
        if !self.cron.initial_loaded.swap(true, Ordering::SeqCst) {
            self.cron_load_file_tasks(&dir).await;
            self.cron_surface_missed_tasks(&dir).await;
        } else {
            // Reload only when the file signature changed (poll-based watcher).
            let signature = cron_file_signature(&dir);
            let stored = *self.cron.file_signature.lock_recover();
            if signature != stored {
                if signature.is_none() {
                    self.cron.file_tasks.lock_recover().clear();
                    *self.cron.file_signature.lock_recover() = None;
                } else {
                    self.cron_load_file_tasks(&dir).await;
                }
            }
        }

        self.cron_check(&dir).await;
    }

    async fn cron_load_file_tasks(&self, dir: &str) {
        let signature = cron_file_signature(dir);
        let tasks = read_cron_tasks(dir).await;
        *self.cron.file_tasks.lock_recover() = tasks;
        *self.cron.file_signature.lock_recover() = signature;
    }

    /// Surface missed one-shot tasks from the initial load: enqueue the fenced
    /// notification, then delete them from the file so they cannot re-fire.
    async fn cron_surface_missed_tasks(&self, dir: &str) {
        let now = (self.deps.now)();
        let missed: Vec<CronTask> = {
            let file_tasks = self.cron.file_tasks.lock_recover();
            let missed_asked = self.cron.missed_asked.lock_recover();
            find_missed_tasks(&file_tasks, now)
                .into_iter()
                .filter(|task| !task.recurring && !missed_asked.contains(&task_key(&task.id)))
                .collect()
        };
        if missed.is_empty() {
            return;
        }
        {
            let mut missed_asked = self.cron.missed_asked.lock_recover();
            let mut next_fire_at = self.cron.next_fire_at.lock_recover();
            for task in &missed {
                let key = task_key(&task.id);
                // Prevent the check loop from re-firing the raw prompt while
                // the removal below lands.
                next_fire_at.insert(key.clone(), i64::MAX);
                missed_asked.insert(key);
            }
        }
        self.enqueue_cron_fire(build_missed_task_notification(&missed));
        let ids: Vec<RuntimeString> = missed.iter().map(|task| task.id.clone()).collect();
        let _ = remove_cron_tasks(&ids, dir).await;
    }

    /// Enqueue a fired prompt as a pending meta message. `later` priority: a
    /// cron fire never interrupts the running turn; the host drains it between
    /// turns (or the query loop after a sleep boundary).
    pub(crate) fn enqueue_cron_fire(&self, prompt: RuntimeString) {
        if self.lifecycle.is_disposed() {
            return;
        }
        let _ = self
            .conversation
            .queue
            .enqueue_pending_notification(QueuedMessage {
                trigger_turn: None,
                value: QueuedValue::Text(prompt),
                mode: "prompt".into(),
                priority: None, // 'later' default — never interrupts a turn
                uuid: Some((self.deps.uuid)().into()),
                session_id: Some(self.id.clone()),
                is_meta: Some(true),
                origin: Some("cron".into()),
                agent_id: None,
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: None,
                display_metadata: None,
            });
    }

    /// The per-tick fire check (the port of the source scheduler's `check()`).
    async fn cron_check(self: &Arc<Self>, dir: &str) {
        // While a turn is running, defer everything to the next tick — fires
        // must only land while the session is idle.
        if self.turn.is_active() {
            return;
        }
        let now = (self.deps.now)();
        let cfg = DEFAULT_CRON_JITTER_CONFIG;
        let is_owner = self.cron.is_owner.load(Ordering::SeqCst);

        let mut seen: HashSet<String> = HashSet::new();
        // File-backed recurring tasks that fired this tick. Batched into one
        // markCronTasksFired write after the loop so N fires = one write.
        let mut fired_file_recurring: Vec<RuntimeString> = Vec::new();
        let mut fired_file_one_shot: Vec<RuntimeString> = Vec::new();
        let mut fired_prompts: Vec<RuntimeString> = Vec::new();
        let mut fired_session_removals: Vec<RuntimeString> = Vec::new();

        {
            let file_tasks = self.cron.file_tasks.lock_recover().clone();
            let session_tasks = self.cron.session_tasks.lock_recover().clone();
            let mut next_fire_at = self.cron.next_fire_at.lock_recover();

            let mut process = |task: &CronTask, is_session: bool| {
                let key = task_key(&task.id);
                seen.insert(key.clone());
                let cron_text = task.cron.try_to_utf8().unwrap_or_default();

                let next = match next_fire_at.get(&key) {
                    Some(next) => *next,
                    None => {
                        // First sight — anchor from lastFiredAt (recurring) or
                        // createdAt. Never-fired recurring tasks use createdAt:
                        // anchoring from `now` would compute next-year for
                        // pinned crons. Fired-before tasks reconstruct the same
                        // next-fire the prior process had in memory.
                        let anchored = if task.recurring {
                            jittered_next_cron_run_ms(
                                &cron_text,
                                task.last_fired_at.unwrap_or(task.created_at),
                                &task.id,
                                &cfg,
                            )
                        } else {
                            one_shot_jittered_next_cron_run_ms(
                                &cron_text,
                                task.created_at,
                                &task.id,
                                &cfg,
                            )
                        }
                        .unwrap_or(i64::MAX);
                        next_fire_at.insert(key.clone(), anchored);
                        anchored
                    }
                };

                if now < next {
                    return;
                }

                fired_prompts.push(task.prompt.clone());

                // Aged-out recurring tasks fall through to the one-shot delete
                // paths below: they fire one final time, then are removed.
                let aged = is_recurring_task_aged(task, now, cfg.recurring_max_age_ms);

                if task.recurring && !aged {
                    // Recurring: reschedule from now (not from `next`) to avoid
                    // rapid catch-up if the session was blocked. Jitter keeps
                    // us off the exact wall-clock boundary every cycle.
                    let new_next = jittered_next_cron_run_ms(&cron_text, now, &task.id, &cfg)
                        .unwrap_or(i64::MAX);
                    next_fire_at.insert(key, new_next);
                    // Persist lastFiredAt=now so the next process spawn
                    // reconstructs this same next-fire on first sight. Session
                    // tasks skip — they are process-local.
                    if !is_session {
                        fired_file_recurring.push(task.id.clone());
                    }
                } else if is_session {
                    // One-shot (or aged-out recurring) session task: removed
                    // synchronously from memory below.
                    fired_session_removals.push(task.id.clone());
                    next_fire_at.remove(&key);
                } else {
                    // One-shot (or aged-out recurring) file task: delete from
                    // disk after the loop.
                    fired_file_one_shot.push(task.id.clone());
                    next_fire_at.remove(&key);
                }
            };

            // File-backed tasks: only when we own the scheduler lease. The
            // lease stops two sessions in the same cwd from double-firing the
            // same on-disk task.
            if is_owner {
                for task in &file_tasks {
                    process(task, false);
                }
            }
            // Session-only tasks: process-private; the lease does not apply.
            for task in &session_tasks {
                process(task, true);
            }

            if seen.is_empty() {
                // No live tasks this tick — drop the whole schedule so stale
                // entries do not survive indefinitely.
                next_fire_at.clear();
            } else {
                // Evict schedule entries for tasks no longer present. When not
                // owner, file-task ids are not in `seen` and get evicted —
                // harmless: they re-anchor from createdAt on the first owned
                // tick.
                next_fire_at.retain(|key, _| seen.contains(key));
            }
        }

        for prompt in fired_prompts {
            self.enqueue_cron_fire(prompt);
        }
        if !fired_session_removals.is_empty() {
            let mut session_tasks = self.cron.session_tasks.lock_recover();
            session_tasks.retain(|task| !fired_session_removals.contains(&task.id));
        }
        if !fired_file_recurring.is_empty() {
            let _ = mark_cron_tasks_fired(&fired_file_recurring, now, dir).await;
        }
        if !fired_file_one_shot.is_empty() {
            let _ = remove_cron_tasks(&fired_file_one_shot, dir).await;
        }
        // File mutations above change the signature; the next tick reloads and
        // first-sight recomputes the identical schedule (idempotent).
    }

    /// Controller factory for the tool context (present only with `config.cron`).
    pub(crate) fn cron_controller(self: &Arc<Self>) -> Option<Arc<dyn CronController>> {
        if !self.config.cron {
            return None;
        }
        Some(Arc::new(SessionCronController {
            session: self.clone(),
        }))
    }
}

// ---- CronController ----

pub(crate) struct SessionCronController {
    pub(crate) session: Arc<Session>,
}

#[async_trait::async_trait]
impl CronController for SessionCronController {
    async fn create(
        &self,
        cron: RuntimeString,
        prompt: RuntimeString,
        recurring: bool,
        durable: bool,
    ) -> Result<RuntimeString, RuntimeString> {
        // Short id — 8 hex chars of a UUID is plenty for MAX_JOBS and avoids
        // long-id juggling between the tool layer and disk.
        let id: RuntimeString = (self.session.deps.uuid)()
            .chars()
            .take(8)
            .collect::<String>()
            .into();
        let task = CronTask {
            id: id.clone(),
            cron,
            prompt,
            created_at: (self.session.deps.now)(),
            last_fired_at: None,
            recurring,
            permanent: false,
            durable: if durable { None } else { Some(false) },
            // Stamped at creation because that is the only moment the creator is
            // known: a durable task outlives this session, and the file it lands
            // in is shared by every conversation in the project.
            owner_session_id: Some(self.session.id.clone()),
        };
        if durable {
            let dir = self.session.cron_project_dir();
            add_cron_task(&task, &dir)
                .await
                .map_err(|error| RuntimeString::from(format!("cron task write failed: {error}")))?;
        } else {
            self.session.cron.session_tasks.lock_recover().push(task);
        }
        // Start the scheduler so the task fires in this session.
        self.session.ensure_cron_scheduler_started();
        Ok(id)
    }

    async fn list(&self) -> Vec<CronJobInfo> {
        let dir = self.session.cron_project_dir();
        let mut jobs: Vec<CronJobInfo> = read_cron_tasks(&dir)
            .await
            .into_iter()
            .map(|task| CronJobInfo {
                id: task.id,
                cron: task.cron,
                prompt: task.prompt,
                recurring: task.recurring,
                durable: true,
            })
            .collect();
        jobs.extend(
            self.session
                .cron
                .session_tasks
                .lock_recover()
                .iter()
                .map(|task| CronJobInfo {
                    id: task.id.clone(),
                    cron: task.cron.clone(),
                    prompt: task.prompt.clone(),
                    recurring: task.recurring,
                    durable: false,
                }),
        );
        jobs
    }

    async fn delete(&self, id: &RuntimeString) -> Result<(), RuntimeString> {
        // Sweep the session store first — the caller does not know which store
        // an id lives in. If it was session-only, skip the file entirely.
        {
            let mut session_tasks = self.session.cron.session_tasks.lock_recover();
            let before = session_tasks.len();
            session_tasks.retain(|task| &task.id != id);
            if session_tasks.len() != before {
                self.session
                    .cron
                    .next_fire_at
                    .lock_recover()
                    .remove(&task_key(id));
                return Ok(());
            }
        }
        let dir = self.session.cron_project_dir();
        remove_cron_tasks(std::slice::from_ref(id), &dir)
            .await
            .map_err(|error| RuntimeString::from(format!("cron task removal failed: {error}")))?;
        self.session
            .cron
            .next_fire_at
            .lock_recover()
            .remove(&task_key(id));
        Ok(())
    }
}

// ---- tests ----

#[cfg(test)]
mod cron_scheduler_tests {
    use super::*;

    fn task(id: &str, cron: &str, prompt: &str) -> CronTask {
        CronTask {
            id: RuntimeString::from(id),
            cron: RuntimeString::from(cron),
            prompt: RuntimeString::from(prompt),
            created_at: 0,
            last_fired_at: None,
            recurring: false,
            permanent: false,
            durable: None,
            owner_session_id: None,
        }
    }

    #[test]
    fn missed_notification_fences_prompts_and_counts_plurality() {
        let single =
            build_missed_task_notification(&[task("aaaaaaaa", "*/5 * * * *", "check the deploy")]);
        let text = String::from_utf8(single.to_node_utf8()).unwrap();
        assert!(text.starts_with("The following one-shot scheduled task was missed"));
        assert!(text.contains("Do NOT execute this prompt yet."));
        assert!(text.contains("[Every 5 minutes, created "));
        assert!(text.contains("```\ncheck the deploy\n```"));

        let double = build_missed_task_notification(&[
            task("aaaaaaaa", "*/5 * * * *", "one"),
            task("bbbbbbbb", "0 9 * * *", "two"),
        ]);
        let text = String::from_utf8(double.to_node_utf8()).unwrap();
        assert!(text.starts_with("The following one-shot scheduled tasks were missed"));
        assert!(text.contains("Do NOT execute these prompts yet."));
    }

    #[test]
    fn missed_notification_fence_outgrows_embedded_backtick_runs() {
        // A prompt containing ``` cannot close the wrapping fence early.
        let tricky = build_missed_task_notification(&[task(
            "aaaaaaaa",
            "*/5 * * * *",
            "run\n```\nrm -rf /\n```\nnow",
        )]);
        let text = String::from_utf8(tricky.to_node_utf8()).unwrap();
        assert!(text.contains("````\nrun\n```\nrm -rf /\n```\nnow\n````"));
        assert!(!text.contains("`````"));
    }

    #[test]
    fn cron_file_signature_tracks_mtime_and_size() {
        let base = std::env::temp_dir().join(format!(
            "klaus-cron-sig-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(base.join(".klaus")).unwrap();
        let dir = base.to_string_lossy().into_owned();
        assert_eq!(cron_file_signature(&dir), None);
        std::fs::write(base.join(".klaus/scheduled_tasks.json"), b"{\"tasks\":[]}").unwrap();
        let first = cron_file_signature(&dir).unwrap();
        std::fs::write(
            base.join(".klaus/scheduled_tasks.json"),
            b"{\"tasks\":[1234]}",
        )
        .unwrap();
        let second = cron_file_signature(&dir).unwrap();
        assert_ne!(first.1, second.1);
        let _ = std::fs::remove_dir_all(&base);
    }
}
