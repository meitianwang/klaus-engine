//! Serialize complete requests that mutate the same conversation resource.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use super::RawJson;

type Request = BoxFuture<'static, ()>;

/// Select only validated routing identities; the original runtime parameters
/// remain untouched for their named method's ordinary decoder.
pub(super) fn thread_scope(method: &str, params: &RawJson) -> Option<String> {
    let field = match method {
        // Snapshots must remain readable while a queued turn prepares its Session.
        "thread/queue/list" => return None,
        "thread/queue/add"
        | "thread/queue/update"
        | "thread/queue/delete"
        | "thread/queue/reorder"
        | "thread/queue/start"
        | "thread/queue/pause"
        | "thread/queue/resume"
        | "thread/queue/import"
        | "thread/queue/clear"
        | "thread/queue/discard" => "threadId",
        "turn/submit" | "turn/steer" | "turn/interrupt" => "sessionId",
        _ => return None,
    };
    let id = params.control_string(field).ok()??;
    (!id.is_empty()).then_some(id)
}

#[derive(Clone, Default)]
pub(super) struct RequestSerializationQueues {
    inner: Arc<Mutex<HashMap<String, VecDeque<Request>>>>,
    preparing: Arc<Mutex<std::collections::HashSet<String>>>,
}

pub(super) struct PreparationOwner {
    queues: RequestSerializationQueues,
    key: String,
}

impl Drop for PreparationOwner {
    fn drop(&mut self) {
        self.queues
            .preparing
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.key);
    }
}

impl RequestSerializationQueues {
    /// Session construction is not foreground admission. Preserve the reader's
    /// existing control path during this phase without releasing queued writes.
    pub(super) fn preparing_session(&self, key: &str) -> PreparationOwner {
        self.preparing
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.to_owned());
        PreparationOwner {
            queues: self.clone(),
            key: key.to_owned(),
        }
    }

    pub(super) fn control_can_bypass_preparation(&self, key: &str) -> bool {
        let queues = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        queues.get(key).is_some_and(VecDeque::is_empty)
            && self
                .preparing
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .contains(key)
    }

    /// Exactly one drain owner runs each resource's FIFO. Its future lives in
    /// the connection's existing continuation set, so EOF drains or drops all
    /// accepted requests under the same connection deadline.
    pub(super) fn enqueue(&self, key: String, request: Request) -> Option<Request> {
        {
            let mut queues = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(queue) = queues.get_mut(&key) {
                queue.push_back(request);
                return None;
            }
            queues.insert(key.clone(), VecDeque::from([request]));
        }
        let owner = ScopeOwner {
            queues: self.clone(),
            key,
            finished: false,
        };
        Some(Box::pin(owner.drain()))
    }

    #[cfg(test)]
    pub(super) fn queued(&self, key: &str) -> Option<usize> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
            .map(VecDeque::len)
    }
}

struct ScopeOwner {
    queues: RequestSerializationQueues,
    key: String,
    finished: bool,
}

impl ScopeOwner {
    async fn drain(mut self) {
        loop {
            let request = {
                let mut queues = self
                    .queues
                    .inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                match queues.get_mut(&self.key).and_then(VecDeque::pop_front) {
                    Some(request) => request,
                    None => {
                        queues.remove(&self.key);
                        self.finished = true;
                        return;
                    }
                }
            };
            // Response admission is part of this request. Releasing the scope
            // at the first suspension would let a later Stop overtake Start.
            request.await;
        }
    }
}

impl Drop for ScopeOwner {
    fn drop(&mut self) {
        if !self.finished {
            self.queues
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{poll, FutureExt};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn a_later_stop_waits_for_start_admission_but_other_threads_continue() {
        let queues = RequestSerializationQueues::default();
        let order = Arc::new(Mutex::new(Vec::new()));
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(()).await.unwrap();
        let first_order = order.clone();
        let mut worker = queues
            .enqueue(
                "thread".into(),
                Box::pin(async move {
                    let permit = tx.reserve().await.unwrap();
                    first_order.lock().unwrap().push("start");
                    permit.send(());
                }),
            )
            .unwrap();
        assert!(poll!(&mut worker).is_pending());
        let stop_order = order.clone();
        assert!(queues
            .enqueue(
                "thread".into(),
                Box::pin(async move {
                    stop_order.lock().unwrap().push("stop");
                })
            )
            .is_none());
        let other_order = order.clone();
        queues
            .enqueue(
                "other".into(),
                Box::pin(async move {
                    other_order.lock().unwrap().push("other");
                }),
            )
            .unwrap()
            .await;
        assert_eq!(*order.lock().unwrap(), ["other"]);
        assert_eq!(queues.queued("thread"), Some(1));
        rx.recv().await.unwrap();
        worker.await;
        assert_eq!(*order.lock().unwrap(), ["other", "start", "stop"]);
        assert_eq!(queues.queued("thread"), None);
    }

    #[tokio::test]
    async fn preparation_controls_do_not_overtake_an_earlier_queued_request() {
        let queues = RequestSerializationQueues::default();
        let mut worker = queues
            .enqueue("thread".into(), futures::future::pending().boxed())
            .unwrap();
        assert!(poll!(&mut worker).is_pending());
        assert!(!queues.control_can_bypass_preparation("thread"));
        let preparation = queues.preparing_session("thread");
        assert!(queues.control_can_bypass_preparation("thread"));
        assert!(queues.enqueue("thread".into(), async {}.boxed()).is_none());
        assert!(!queues.control_can_bypass_preparation("thread"));
        drop(worker);
        drop(preparation);
        assert!(!queues.control_can_bypass_preparation("thread"));
        assert!(queues.preparing.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_a_connection_owner_discards_pending_work_and_releases_the_scope() {
        let queues = RequestSerializationQueues::default();
        let worker = queues
            .enqueue("thread".into(), futures::future::pending().boxed())
            .unwrap();
        assert!(queues
            .enqueue("thread".into(), futures::future::pending().boxed())
            .is_none());
        drop(worker);
        assert_eq!(queues.queued("thread"), None);
        queues
            .enqueue("thread".into(), async {}.boxed())
            .unwrap()
            .await;
        assert_eq!(queues.queued("thread"), None);
    }

    #[test]
    fn scope_selection_does_not_scalarize_runtime_input_or_accept_an_unrelated_identity() {
        let params = RawJson::from_json(
            r#"{"sessionId":"live","threadId":"unrelated","input":"\ud800"}"#.into(),
        )
        .unwrap();
        assert_eq!(
            thread_scope("turn/submit", &params).as_deref(),
            Some("live")
        );
        assert_eq!(
            thread_scope("thread/queue/start", &params).as_deref(),
            Some("unrelated")
        );
        assert!(params.as_str().contains("\\ud800"));
        assert!(thread_scope("model/list", &params).is_none());
        assert!(thread_scope("thread/queue/list", &params).is_none());
    }
}
