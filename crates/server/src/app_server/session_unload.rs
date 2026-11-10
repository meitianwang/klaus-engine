//! Idle-session unloading.
//!
//! A loaded session keeps the child processes it spawned (its MCP servers and
//! their descendants) alive for as long as the session itself lives. Sessions
//! nobody watches and that run no work are therefore unloaded after a quiet
//! period: the session goes through the same teardown an explicit
//! `session/close` uses — which reclaims its processes — and clients learn
//! about it through the broadcast `session/closed` notification. Opening the
//! session again resumes it from stored history.
//!
//! Two conditions gate the timer, both observed through watch channels so the
//! wait is event-driven rather than polled:
//!   * no client is subscribed to the session's event stream, and
//!   * the session is not active (no turn in flight, no approval or
//!     elicitation waiting on a client answer).
//!
//! Any subscription or activity resets the quiet period. While a session is
//! actually being unloaded it is marked pending, and `session/subscribe` and
//! `session/start` refuse it until teardown finishes, so a client never
//! attaches to a half-closed session.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use klaus_agent_core::config_governance::ConfigGovernanceSource;
use klaus_agent_core::config_store;

use super::{realtime, wire, AppServerState, ReverseKind, SessionHandle};

/// Quiet period applied when configuration does not provide one.
const DEFAULT_SESSION_UNLOAD_DELAY_SECS: u64 = 1800;
/// Bound on the teardown wait before it is reported as slow. The wait itself
/// continues: teardown must finish; the deadline only bounds the warning.
const SHUTDOWN_SLOW_WARN_SECS: u64 = 10;

/// Per-session lifecycle status: one watch channel per loaded session feeding
/// the idle-unload machine, plus change notifications for the broadcast.
#[derive(Default)]
pub(super) struct SessionStatusRegistry {
    watchers: std::sync::Mutex<HashMap<String, watch::Sender<wire::SessionStatus>>>,
}

impl SessionStatusRegistry {
    /// Start tracking one session as idle.
    pub(super) fn register(&self, session_id: &str) {
        let (tx, _rx) = watch::channel(wire::SessionStatus::Idle);
        self.watchers
            .lock()
            .unwrap()
            .insert(session_id.to_string(), tx);
    }

    pub(super) fn subscribe(
        &self,
        session_id: &str,
    ) -> Option<watch::Receiver<wire::SessionStatus>> {
        self.watchers
            .lock()
            .unwrap()
            .get(session_id)
            .map(watch::Sender::subscribe)
    }

    /// Publish a new status. Returns it when it differs from the previous one
    /// and clients must be notified.
    fn update(&self, session_id: &str, status: wire::SessionStatus) -> Option<wire::SessionStatus> {
        let tx = self.watchers.lock().unwrap().get(session_id)?.clone();
        let mut changed = None;
        tx.send_if_modified(|current| {
            if *current == status {
                return false;
            }
            *current = status.clone();
            changed = Some(status.clone());
            true
        });
        changed
    }

    /// Stop tracking one session: observers see `NotLoaded`, then the watcher
    /// closes. Returns the status to broadcast, if any.
    pub(super) fn remove(&self, session_id: &str) -> Option<wire::SessionStatus> {
        let tx = self.watchers.lock().unwrap().remove(session_id)?;
        let previous = tx.borrow().clone();
        let _ = tx.send(wire::SessionStatus::NotLoaded);
        if previous != wire::SessionStatus::NotLoaded {
            Some(wire::SessionStatus::NotLoaded)
        } else {
            None
        }
    }
}

/// Derive one session's lifecycle status from its in-flight turns and the
/// reverse requests waiting on a client answer.
fn compute_status(
    state: &AppServerState,
    handle: &SessionHandle,
    session_id: &str,
) -> wire::SessionStatus {
    let mut flags = Vec::new();
    for pending in state.pending_reverse.lock().unwrap().values() {
        if pending.session_id != session_id {
            continue;
        }
        match pending.kind {
            ReverseKind::Approval { .. } => {
                flags.push(wire::SessionActiveFlag::WaitingOnApproval);
            }
            ReverseKind::Elicitation { .. } => {
                flags.push(wire::SessionActiveFlag::WaitingOnUserInput);
            }
        }
    }
    let running = !handle.turns.lock().unwrap().is_empty();
    if !running && flags.is_empty() {
        return wire::SessionStatus::Idle;
    }
    flags.sort();
    flags.dedup();
    wire::SessionStatus::Active {
        active_flags: flags,
    }
}

/// Recompute and broadcast one session's status after its turns or pending
/// reverse requests changed.
pub(super) async fn refresh_status(state: &Arc<AppServerState>, session_id: &str) {
    let Some(handle) = state.session(session_id) else {
        return;
    };
    let status = compute_status(state, &handle, session_id);
    broadcast_status(state, session_id, status).await;
}

/// Publish a status through the registry and broadcast the change. The
/// broadcast bypasses the per-session subscription gate on purpose: list
/// surfaces must see lifecycle transitions even while unsubscribed.
pub(super) async fn broadcast_status(
    state: &Arc<AppServerState>,
    session_id: &str,
    status: wire::SessionStatus,
) {
    if state
        .session_status
        .update(session_id, status.clone())
        .is_some()
    {
        state
            .notify(
                "session/statusChanged",
                serde_json::to_value(wire::SessionStatusChangedNotification {
                    session_id: session_id.to_string(),
                    status,
                })
                .expect("session status notification serializes"),
            )
            .await;
    }
}

/// Make sure one live session has an idle-unload watcher. Idempotent; also
/// called when a client re-subscribes so a watcher lost to a failed unload is
/// rebuilt.
pub(super) fn ensure_watcher(
    state: &Arc<AppServerState>,
    session_id: &str,
    handle: &Arc<SessionHandle>,
) {
    if state
        .pending_session_unloads
        .lock()
        .unwrap()
        .contains(session_id)
    {
        return;
    }
    let mut watchers = state.unload_watchers.lock().unwrap();
    if watchers.contains_key(session_id) {
        return;
    }
    let (cancel_tx, cancel_rx) = oneshot::channel();
    watchers.insert(session_id.to_string(), cancel_tx);
    drop(watchers);
    let state = Arc::clone(state);
    let session_id = session_id.to_string();
    let handle = Arc::clone(handle);
    tokio::spawn(run_unload_watcher(state, session_id, handle, cancel_rx));
}

/// Re-arm a session's event stream when a request needs its events, the way
/// an explicit subscribe does. Turn admission uses this so a headless turn on
/// an unwatched session still streams to the client.
pub(super) fn rearm_subscription(
    state: &Arc<AppServerState>,
    session_id: &str,
    handle: &Arc<SessionHandle>,
) {
    state
        .unsubscribed_sessions
        .lock()
        .unwrap()
        .remove(session_id);
    if let Some(tx) = state.session_subscriptions.lock().unwrap().get(session_id) {
        let _ = tx.send(true);
    }
    ensure_watcher(state, session_id, handle);
}

/// Drop the endpoint bookkeeping for a session that is gone (explicit close,
/// failed startup, or completed idle unload) and tell clients it is no longer
/// loaded.
pub(super) async fn discard_session_bookkeeping(state: &Arc<AppServerState>, session_id: &str) {
    if let Some(cancel) = state.unload_watchers.lock().unwrap().remove(session_id) {
        let _ = cancel.send(());
    }
    state
        .session_subscriptions
        .lock()
        .unwrap()
        .remove(session_id);
    state
        .unsubscribed_sessions
        .lock()
        .unwrap()
        .remove(session_id);
    // Reverse requests waiting on this session will never be answered.
    state
        .pending_reverse
        .lock()
        .unwrap()
        .retain(|_, pending| pending.session_id != session_id);
    if let Some(status) = state.session_status.remove(session_id) {
        state
            .notify(
                "session/statusChanged",
                serde_json::to_value(wire::SessionStatusChangedNotification {
                    session_id: session_id.to_string(),
                    status,
                })
                .expect("session status notification serializes"),
            )
            .await;
    }
}

/// The quiet-period state machine for one session: both gate conditions carry
/// the instant they last flipped, and the unload target is the later of the
/// two plus the delay, so any subscription or activity restarts the wait.
struct UnloadingState {
    delay: Duration,
    has_subscribers_rx: watch::Receiver<bool>,
    has_subscribers: (bool, Instant),
    status_rx: watch::Receiver<wire::SessionStatus>,
    is_active: (bool, Instant),
}

impl UnloadingState {
    fn new(
        delay: Duration,
        has_subscribers_rx: watch::Receiver<bool>,
        status_rx: watch::Receiver<wire::SessionStatus>,
    ) -> Self {
        let has_subscribers = (*has_subscribers_rx.borrow(), Instant::now());
        let is_active = (
            matches!(*status_rx.borrow(), wire::SessionStatus::Active { .. }),
            Instant::now(),
        );
        Self {
            delay,
            has_subscribers_rx,
            has_subscribers,
            status_rx,
            is_active,
        }
    }

    fn unloading_target(&self) -> Option<Instant> {
        match (self.has_subscribers, self.is_active) {
            ((false, has_no_subscribers_since), (false, is_inactive_since)) => {
                std::cmp::max(has_no_subscribers_since, is_inactive_since).checked_add(self.delay)
            }
            _ => None,
        }
    }

    fn sync_receiver_values(&mut self) {
        let has_subscribers = *self.has_subscribers_rx.borrow();
        if self.has_subscribers.0 != has_subscribers {
            self.has_subscribers = (has_subscribers, Instant::now());
        }
        let is_active = matches!(*self.status_rx.borrow(), wire::SessionStatus::Active { .. });
        if self.is_active.0 != is_active {
            self.is_active = (is_active, Instant::now());
        }
    }

    fn should_unload_now(&mut self) -> bool {
        self.sync_receiver_values();
        self.unloading_target()
            .is_some_and(|target| target <= Instant::now())
    }

    fn note_session_activity_observed(&mut self) {
        if !self.is_active.0 {
            self.is_active = (false, Instant::now());
        }
    }

    /// Wait until the quiet period elapses. `false` means a watched sender
    /// closed (the session is being torn down) and the waiter must stop.
    async fn wait_for_unloading_trigger(&mut self) -> bool {
        loop {
            self.sync_receiver_values();
            let unloading_target = self.unloading_target();
            if let Some(target) = unloading_target {
                if target <= Instant::now() {
                    tokio::task::yield_now().await;
                    return true;
                }
            }
            let unloading_sleep = async {
                if let Some(target) = unloading_target {
                    tokio::time::sleep_until(target).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = unloading_sleep => return true,
                changed = self.has_subscribers_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                }
                changed = self.status_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                }
            }
        }
    }
}

async fn run_unload_watcher(
    state: Arc<AppServerState>,
    session_id: String,
    handle: Arc<SessionHandle>,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    let delay = resolve_unload_delay(&state).await;
    let Some(has_subscribers_rx) = state
        .session_subscriptions
        .lock()
        .unwrap()
        .get(&session_id)
        .map(watch::Sender::subscribe)
    else {
        state.unload_watchers.lock().unwrap().remove(&session_id);
        return;
    };
    let Some(status_rx) = state.session_status.subscribe(&session_id) else {
        state.unload_watchers.lock().unwrap().remove(&session_id);
        return;
    };
    let mut unloading = UnloadingState::new(delay, has_subscribers_rx, status_rx);
    loop {
        tokio::select! {
            biased;
            _ = &mut cancel_rx => break,
            watchers_open = unloading.wait_for_unloading_trigger() => {
                if !watchers_open {
                    break;
                }
                if !unloading.should_unload_now() {
                    continue;
                }
                // Live re-check: work admitted between the timer firing and
                // this point keeps the session loaded, and the quiet period
                // restarts when the session goes idle again.
                let live = compute_status(&state, &handle, &session_id);
                if matches!(live, wire::SessionStatus::Active { .. }) {
                    broadcast_status(&state, &session_id, live).await;
                    unloading.note_session_activity_observed();
                    continue;
                }
                {
                    let mut pending = state.pending_session_unloads.lock().unwrap();
                    if !pending.insert(session_id.clone()) {
                        continue;
                    }
                }
                // Teardown owns the session from here; this watcher is done.
                // Release the status receiver before teardown removes its
                // watch entry.
                drop(unloading);
                state.unload_watchers.lock().unwrap().remove(&session_id);
                let state = Arc::clone(&state);
                let session_id = session_id.clone();
                let handle = Arc::clone(&handle);
                tokio::spawn(async move {
                    unload_session(state, session_id, handle).await;
                });
                return;
            }
        }
    }
    state.unload_watchers.lock().unwrap().remove(&session_id);
}

/// Tear one idle session down and publish its removal. Mirrors the explicit
/// `session/close` path, with a warning bound around the wait so a slow
/// teardown is visible but never abandoned.
async fn unload_session(
    state: Arc<AppServerState>,
    session_id: String,
    handle: Arc<SessionHandle>,
) {
    let shutdown = async {
        if let Err(error) = realtime::stop_session(&state, &session_id).await {
            eprintln!("[app-server] failed to stop realtime state for session {session_id} before unload: {error}");
        }
        handle.ps.shutdown_and_wait().await
    };
    tokio::pin!(shutdown);
    let result = match tokio::time::timeout(
        Duration::from_secs(SHUTDOWN_SLOW_WARN_SECS),
        &mut shutdown,
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            eprintln!(
                    "[app-server] session {session_id} shutdown is taking longer than expected; continuing to wait"
                );
            shutdown.await
        }
    };
    match result {
        Ok(()) => {
            let removed = {
                let mut sessions = state.sessions.lock().unwrap();
                if sessions
                    .get(&session_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &handle))
                {
                    sessions.remove(&session_id);
                    true
                } else {
                    false
                }
            };
            if !removed {
                // A delayed unload can finish after the session was replaced
                // or closed under the same id; only the load that scheduled
                // this unload may publish the teardown.
                eprintln!(
                    "[app-server] session {session_id} was replaced or removed before unload finalized"
                );
                state
                    .pending_session_unloads
                    .lock()
                    .unwrap()
                    .remove(&session_id);
                return;
            }
            discard_session_bookkeeping(&state, &session_id).await;
            state
                .notify(
                    "session/closed",
                    serde_json::to_value(wire::SessionClosedNotification {
                        session_id: session_id.clone(),
                    })
                    .expect("session/closed notification serializes"),
                )
                .await;
            state
                .pending_session_unloads
                .lock()
                .unwrap()
                .remove(&session_id);
        }
        Err(error) => {
            state
                .pending_session_unloads
                .lock()
                .unwrap()
                .remove(&session_id);
            eprintln!(
                "[app-server] failed to shut down session {session_id} for idle unload: {error}"
            );
        }
    }
}

/// Resolve the quiet period for a freshly spawned watcher: an explicit
/// endpoint override wins, then the engine configuration file, then the
/// default.
async fn resolve_unload_delay(state: &AppServerState) -> Duration {
    if let Some(delay) = state.session_unload_delay_override {
        return sanitize_delay(delay);
    }
    let fallback = Duration::from_secs(DEFAULT_SESSION_UNLOAD_DELAY_SECS);
    let configured = match config_store::load(&state.config_path).await {
        Ok(document) => {
            config_store::resolve_view(&[document.engine_layer(ConfigGovernanceSource::User)])
                .ok()
                .and_then(|view| {
                    view.resolved
                        .get("sessionUnloadDelaySecs")
                        .and_then(serde_json::Value::as_u64)
                })
                .map(Duration::from_secs)
        }
        Err(_) => None,
    };
    sanitize_delay(configured.unwrap_or(fallback))
}

fn sanitize_delay(delay: Duration) -> Duration {
    // A quiet period that cannot be placed on the clock would pin the watcher
    // in an unbounded wait; fall back to the default instead.
    match Instant::now().checked_add(delay) {
        Some(_) => delay,
        None => {
            eprintln!(
                "[app-server] configured session unload delay is too large; using the default"
            );
            Duration::from_secs(DEFAULT_SESSION_UNLOAD_DELAY_SECS)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn target_requires_both_quiet_conditions() {
        let (subs_tx, subs_rx) = watch::channel(true);
        let (status_tx, status_rx) = watch::channel(wire::SessionStatus::Idle);
        let mut state = UnloadingState::new(Duration::from_secs(30), subs_rx, status_rx);
        assert!(
            state.unloading_target().is_none(),
            "subscribed sessions never schedule an unload"
        );
        subs_tx.send(false).unwrap();
        state.sync_receiver_values();
        assert!(
            state.unloading_target().is_some(),
            "unsubscribed and idle schedules the unload"
        );
        status_tx
            .send(wire::SessionStatus::Active {
                active_flags: vec![],
            })
            .unwrap();
        state.sync_receiver_values();
        assert!(
            state.unloading_target().is_none(),
            "activity cancels the schedule"
        );
        status_tx.send(wire::SessionStatus::Idle).unwrap();
        state.sync_receiver_values();
        assert!(state.unloading_target().is_some());
    }

    #[tokio::test]
    async fn quiet_period_restarts_on_each_transition() {
        let (subs_tx, subs_rx) = watch::channel(false);
        let (_status_tx, status_rx) = watch::channel(wire::SessionStatus::Idle);
        let mut state = UnloadingState::new(Duration::from_millis(40), subs_rx, status_rx);
        let first = state.unloading_target().expect("scheduled target");
        // A subscription flip restarts the quiet period even when it lands
        // back on the same value combination.
        subs_tx.send(true).unwrap();
        state.sync_receiver_values();
        subs_tx.send(false).unwrap();
        state.sync_receiver_values();
        let second = state.unloading_target().expect("scheduled target");
        assert!(second >= first);
        assert!(!state.should_unload_now() || second <= Instant::now());
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(state.should_unload_now());
    }

    #[tokio::test]
    async fn trigger_waits_out_the_quiet_period() {
        let (subs_tx, subs_rx) = watch::channel(true);
        let (_status_tx, status_rx) = watch::channel(wire::SessionStatus::Idle);
        let mut state = UnloadingState::new(Duration::from_millis(30), subs_rx, status_rx);
        subs_tx.send(false).unwrap();
        assert!(state.wait_for_unloading_trigger().await);
        assert!(state.should_unload_now());
    }

    #[tokio::test]
    async fn closed_watchers_end_the_wait() {
        let (subs_tx, subs_rx) = watch::channel(true);
        let (_status_tx, status_rx) = watch::channel(wire::SessionStatus::Idle);
        let mut state = UnloadingState::new(Duration::from_secs(30), subs_rx, status_rx);
        subs_tx.send(false).unwrap();
        drop(subs_tx);
        assert!(
            !state.wait_for_unloading_trigger().await,
            "a closed subscription watcher ends the wait without unloading"
        );
    }
}
