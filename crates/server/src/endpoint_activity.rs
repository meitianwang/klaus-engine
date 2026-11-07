use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EndpointActivityKind {
    HttpConnection,
    WebSocketUpgrade,
    SseRegistration,
}

#[derive(Default)]
struct EndpointActivityState {
    admission_closed: bool,
    http_connections: usize,
    websocket_upgrades: usize,
    sse_registrations: usize,
}

impl EndpointActivityState {
    fn count(&self, kind: EndpointActivityKind) -> usize {
        match kind {
            EndpointActivityKind::HttpConnection => self.http_connections,
            EndpointActivityKind::WebSocketUpgrade => self.websocket_upgrades,
            EndpointActivityKind::SseRegistration => self.sse_registrations,
        }
    }

    fn count_mut(&mut self, kind: EndpointActivityKind) -> &mut usize {
        match kind {
            EndpointActivityKind::HttpConnection => &mut self.http_connections,
            EndpointActivityKind::WebSocketUpgrade => &mut self.websocket_upgrades,
            EndpointActivityKind::SseRegistration => &mut self.sse_registrations,
        }
    }

    fn is_idle(&self) -> bool {
        self.http_connections == 0 && self.websocket_upgrades == 0 && self.sse_registrations == 0
    }
}

/// Admission and retained-completion state for detached network tasks.
///
/// Registration and admission close share one mutex. A task either acquires a
/// guard before shutdown starts, in which case the retained close owner waits
/// for it, or it observes closed admission and never starts.
#[derive(Default)]
pub(crate) struct EndpointActivity {
    state: Mutex<EndpointActivityState>,
    idle: Notify,
    #[cfg(test)]
    admission_closed: Notify,
}

impl EndpointActivity {
    pub(crate) fn begin(
        self: &Arc<Self>,
        kind: EndpointActivityKind,
    ) -> Option<EndpointActivityGuard> {
        let mut state = self.state.lock().unwrap();
        if state.admission_closed {
            return None;
        }
        let count = state.count_mut(kind);
        *count = count
            .checked_add(1)
            .expect("endpoint activity count overflow");
        Some(EndpointActivityGuard {
            activity: self.clone(),
            kind,
        })
    }

    pub(crate) fn close_admission(&self) {
        let mut state = self.state.lock().unwrap();
        state.admission_closed = true;
        #[cfg(test)]
        self.admission_closed.notify_waiters();
        if state.is_idle() {
            self.idle.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            // Register before checking retained state. This remains correct if
            // the final guard drops between the check and `.await`, including
            // if the notification implementation changes permit semantics.
            notified.as_mut().enable();
            if self.state.lock().unwrap().is_idle() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_for_kind_idle(&self, kind: EndpointActivityKind) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().count(kind) == 0 {
                return;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_admission_closed(&self) {
        loop {
            let notified = self.admission_closed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().admission_closed {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct EndpointActivityGuard {
    activity: Arc<EndpointActivity>,
    kind: EndpointActivityKind,
}

impl Drop for EndpointActivityGuard {
    fn drop(&mut self) {
        let mut state = self.activity.state.lock().unwrap();
        let count = state.count_mut(self.kind);
        *count = count
            .checked_sub(1)
            .expect("endpoint activity guard underflow");
        self.activity.idle.notify_waiters();
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct EndpointTestGate {
    entered: Notify,
    release: Notify,
}

#[cfg(test)]
impl EndpointTestGate {
    pub(crate) async fn pause(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }

    pub(crate) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

/// Test-only two-phase gates at the three admission/registration windows. In a
/// production build this is a zero-sized no-op value.
#[derive(Clone, Default)]
pub(crate) struct EndpointTestGates {
    #[cfg(test)]
    pub(crate) http_after_admission: Option<Arc<EndpointTestGate>>,
    #[cfg(test)]
    pub(crate) websocket_before_registration: Option<Arc<EndpointTestGate>>,
    #[cfg(test)]
    pub(crate) sse_before_registration: Option<Arc<EndpointTestGate>>,
}

impl EndpointTestGates {
    pub(crate) async fn pause_http_after_admission(&self) {
        #[cfg(test)]
        if let Some(gate) = &self.http_after_admission {
            gate.pause().await;
        }
    }

    pub(crate) async fn pause_websocket_before_registration(&self) {
        #[cfg(test)]
        if let Some(gate) = &self.websocket_before_registration {
            gate.pause().await;
        }
    }

    pub(crate) async fn pause_sse_before_registration(&self) {
        #[cfg(test)]
        if let Some(gate) = &self.sse_before_registration {
            gate.pause().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn idle_waiter_observes_the_final_guard_drop() {
        let activity = Arc::new(EndpointActivity::default());
        let guard = activity
            .begin(EndpointActivityKind::HttpConnection)
            .expect("HTTP activity admitted");
        activity.close_admission();

        let waiter_activity = activity.clone();
        let waiter = tokio::spawn(async move { waiter_activity.wait_for_idle().await });
        tokio::task::yield_now().await;
        drop(guard);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("idle waiter lost final guard notification")
            .expect("idle waiter task panicked");
    }

    #[tokio::test]
    async fn admission_waiter_observes_synchronous_close() {
        let activity = Arc::new(EndpointActivity::default());
        let waiter_activity = activity.clone();
        let waiter = tokio::spawn(async move { waiter_activity.wait_for_admission_closed().await });
        tokio::task::yield_now().await;
        activity.close_admission();

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("admission waiter lost close notification")
            .expect("admission waiter task panicked");
    }

    #[tokio::test]
    async fn kind_waiter_does_not_wait_for_unrelated_activity() {
        let activity = Arc::new(EndpointActivity::default());
        let _http_guard = activity
            .begin(EndpointActivityKind::HttpConnection)
            .expect("HTTP activity admitted");
        let sse_guard = activity
            .begin(EndpointActivityKind::SseRegistration)
            .expect("SSE activity admitted");
        activity.close_admission();

        let waiter_activity = activity.clone();
        let waiter = tokio::spawn(async move {
            waiter_activity
                .wait_for_kind_idle(EndpointActivityKind::SseRegistration)
                .await
        });
        tokio::task::yield_now().await;
        drop(sse_guard);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("kind waiter lost final guard notification")
            .expect("kind waiter task panicked");
    }
}
