//! A single-consumer async queue: the turn logic runs concurrently and `push`es
//! events; the query loop drains them via `next()`. This inversion lets tools
//! stream live (their `emit` pushes here too) without threading a yield through
//! every layer. Unbounded by default; an optional `coalesce` folds mergeable items
//! into the tail once the buffer crosses `high_water_mark` (backpressure for the
//! only high-volume producer).

use crate::sync_ext::MutexExt;
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Merge `next` into the buffered `last`, returning the merged item, or `None` to
/// keep both.
pub type CoalesceFn<T> = Box<dyn Fn(&T, &T) -> Option<T> + Send + Sync>;

/// Construction options.
pub struct AsyncQueueOptions<T> {
    /// Above this many buffered items, attempt to coalesce an incoming item.
    pub high_water_mark: Option<usize>,
    /// Merge fn used when the buffer is at/above the high-water mark.
    pub coalesce: Option<CoalesceFn<T>>,
}

impl<T> Default for AsyncQueueOptions<T> {
    fn default() -> Self {
        Self {
            high_water_mark: None,
            coalesce: None,
        }
    }
}

struct State<T> {
    items: VecDeque<T>,
    /// The parked consumer, if any (single consumer).
    waiter: Option<oneshot::Sender<Option<T>>>,
    closed: bool,
}

/// A single-consumer async queue (see module docs).
pub struct AsyncQueue<T> {
    state: Mutex<State<T>>,
    high_water_mark: usize,
    coalesce: Option<CoalesceFn<T>>,
}

impl<T: Send> AsyncQueue<T> {
    pub fn new() -> Self {
        Self::with_options(AsyncQueueOptions::default())
    }

    pub fn with_options(opts: AsyncQueueOptions<T>) -> Self {
        Self {
            state: Mutex::new(State {
                items: VecDeque::new(),
                waiter: None,
                closed: false,
            }),
            high_water_mark: opts.high_water_mark.unwrap_or(usize::MAX),
            coalesce: opts.coalesce,
        }
    }

    /// Enqueue an item. A no-op once closed. Hands the item straight to a parked
    /// consumer; otherwise buffers it, folding into the tail under backpressure.
    pub fn push(&self, item: T) {
        let mut st = self.state.lock_recover();
        if st.closed {
            return;
        }
        if let Some(tx) = st.waiter.take() {
            let _ = tx.send(Some(item));
            return;
        }
        // Backpressure: when the buffer is large, fold mergeable items (e.g. text
        // deltas) into the tail instead of growing unboundedly.
        if let Some(coalesce) = &self.coalesce {
            if st.items.len() >= self.high_water_mark {
                let merged = st.items.back().and_then(|last| coalesce(last, &item));
                if let Some(m) = merged {
                    if let Some(slot) = st.items.back_mut() {
                        *slot = m;
                    }
                    return;
                }
            }
        }
        st.items.push_back(item);
    }

    /// Close the queue: no further items are accepted and a parked consumer is
    /// released with `None`.
    pub fn close(&self) {
        let mut st = self.state.lock_recover();
        if st.closed {
            return;
        }
        st.closed = true;
        if let Some(tx) = st.waiter.take() {
            let _ = tx.send(None);
        }
    }

    /// Drain the next item, or `None` once the queue is closed and empty. Single
    /// consumer: at most one `next()` may be outstanding at a time.
    pub async fn next(&self) -> Option<T> {
        let rx = {
            let mut st = self.state.lock_recover();
            if let Some(item) = st.items.pop_front() {
                return Some(item);
            }
            if st.closed {
                return None;
            }
            let (tx, rx) = oneshot::channel();
            st.waiter = Some(tx);
            rx
        };
        // The queue being dropped without closing is also treated as done.
        rx.await.unwrap_or_default()
    }
}

impl<T: Send> Default for AsyncQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for AsyncQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (len, closed, waiting) = match self.state.lock() {
            Ok(st) => (st.items.len(), st.closed, st.waiter.is_some()),
            Err(_) => (0, false, false),
        };
        f.debug_struct("AsyncQueue")
            .field("len", &len)
            .field("closed", &closed)
            .field("waiting", &waiting)
            .field("high_water_mark", &self.high_water_mark)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn buffers_then_drains_in_order() {
        let q = AsyncQueue::new();
        q.push(1);
        q.push(2);
        assert_eq!(q.next().await, Some(1));
        assert_eq!(q.next().await, Some(2));
        q.close();
        assert_eq!(q.next().await, None);
    }

    #[tokio::test]
    async fn push_after_close_is_ignored() {
        let q = AsyncQueue::new();
        q.close();
        q.push(9);
        assert_eq!(q.next().await, None);
    }

    #[tokio::test]
    async fn parked_consumer_gets_direct_handoff() {
        let q = Arc::new(AsyncQueue::new());
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.next().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.push(42);
        assert_eq!(h.await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn close_wakes_parked_consumer() {
        let q = Arc::new(AsyncQueue::<i32>::new());
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.next().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.close();
        assert_eq!(h.await.unwrap(), None);
    }

    #[tokio::test]
    async fn coalesces_tail_at_high_water_mark() {
        let q: AsyncQueue<i32> = AsyncQueue::with_options(AsyncQueueOptions {
            high_water_mark: Some(2),
            coalesce: Some(Box::new(|last: &i32, next: &i32| Some(last + next))),
        });
        q.push(1);
        q.push(2);
        // buffer is [1, 2]; the next push is at the high-water mark -> fold into tail.
        q.push(3);
        assert_eq!(q.next().await, Some(1));
        assert_eq!(q.next().await, Some(5));
        q.close();
        assert_eq!(q.next().await, None);
    }
}
