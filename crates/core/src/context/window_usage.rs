//! Active context accounting and the first request's prefix for each compacted window.

use crate::runtime_values::{RuntimeJson, RuntimeObject};
use crate::schemas::rollout::RolloutItem;
use std::sync::{Arc, Mutex};

pub const WINDOW_USAGE_EVENT: &str = "context_window_usage";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WindowUsageState {
    pub active_tokens: Option<u64>,
    pub prefill_input_tokens: Option<u64>,
    pub window_number: u64,
}

#[derive(Clone)]
pub struct ContextWindowUsage {
    state: Arc<Mutex<WindowUsageState>>,
    record: Arc<dyn Fn(WindowUsageState) -> futures::future::BoxFuture<'static, ()> + Send + Sync>,
    update_gate: Arc<tokio::sync::Mutex<()>>,
}

impl ContextWindowUsage {
    pub fn new(
        state: Arc<Mutex<WindowUsageState>>,
        record: Arc<
            dyn Fn(WindowUsageState) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Self {
        Self::with_update_gate(state, record, Arc::new(tokio::sync::Mutex::new(())))
    }

    pub(crate) fn with_update_gate(
        state: Arc<Mutex<WindowUsageState>>,
        record: Arc<
            dyn Fn(WindowUsageState) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
        >,
        update_gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            state,
            record,
            update_gate,
        }
    }

    pub fn sample(&self, estimated_tokens: u64) -> u64 {
        estimated_tokens.max(self.state.lock().unwrap().active_tokens.unwrap_or(0))
    }

    pub fn prefill(&self) -> Option<u64> {
        self.state.lock().unwrap().prefill_input_tokens
    }

    pub async fn mark_full(&self, capacity: u64) {
        self.update(|state| state.active_tokens = Some(capacity))
            .await;
    }

    pub async fn reset(&self, active_tokens: u64) {
        self.update(|state| {
            state.active_tokens = Some(active_tokens);
            state.prefill_input_tokens = None;
            state.window_number = state.window_number.saturating_add(1);
        })
        .await;
    }

    pub async fn observe(&self, input_tokens: u64, active_tokens: u64) {
        self.update(|state| {
            state.prefill_input_tokens.get_or_insert(input_tokens);
            state.active_tokens = Some(active_tokens);
        })
        .await;
    }

    async fn update(&self, mutate: impl FnOnce(&mut WindowUsageState)) {
        let _update = self.update_gate.lock().await;
        let next = {
            let state = self.state.lock().unwrap();
            let mut next = state.clone();
            mutate(&mut next);
            if next == *state {
                return;
            }
            next
        };
        (self.record)(next.clone()).await;
        *self.state.lock().unwrap() = next;
    }
}

pub fn encode(state: &WindowUsageState) -> RuntimeJson {
    let mut value = RuntimeObject::new();
    value.insert("version", RuntimeJson::String("1".into()));
    value.insert(
        "windowNumber",
        RuntimeJson::String(state.window_number.to_string().into()),
    );
    if let Some(tokens) = state.active_tokens {
        value.insert(
            "activeTokens",
            RuntimeJson::String(tokens.to_string().into()),
        );
    }
    if let Some(tokens) = state.prefill_input_tokens {
        value.insert(
            "prefillInputTokens",
            RuntimeJson::String(tokens.to_string().into()),
        );
    }
    RuntimeJson::Object(value)
}

pub fn restore(items: &[RolloutItem], capacity: u64) -> WindowUsageState {
    for item in items.iter().rev() {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        let Some(payload) = &event.payload else {
            continue;
        };
        if event.event == WINDOW_USAGE_EVENT
            && payload
                .get("version")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|version| version == "1")
        {
            let number = |key| {
                payload
                    .get(key)
                    .and_then(RuntimeJson::as_str)
                    .and_then(|text| text.try_to_utf8().ok())
                    .and_then(|text| text.parse::<u64>().ok())
            };
            return WindowUsageState {
                active_tokens: number("activeTokens"),
                prefill_input_tokens: number("prefillInputTokens"),
                window_number: number("windowNumber").unwrap_or(0),
            };
        }
        // Upgrade the previous checkpoint format without retaining a second pressure policy.
        if event.event == "context_window_pressure" {
            return WindowUsageState {
                active_tokens: payload
                    .get("full")
                    .and_then(RuntimeJson::as_bool)
                    .filter(|full| *full)
                    .map(|_| capacity),
                ..Default::default()
            };
        }
    }
    WindowUsageState::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::rollout::EventItem;

    #[tokio::test]
    async fn the_first_input_baseline_is_reestablished_only_after_a_committed_checkpoint() {
        let state = Arc::new(Mutex::new(WindowUsageState::default()));
        let records = Arc::new(Mutex::new(Vec::new()));
        let sink = records.clone();
        let window = ContextWindowUsage::new(
            state.clone(),
            Arc::new(move |state| {
                sink.lock().unwrap().push(state);
                Box::pin(async {})
            }),
        );
        window.observe(40_000, 42_000).await;
        window.observe(55_000, 57_000).await;
        assert_eq!(window.prefill(), Some(40_000));
        assert_eq!(window.sample(50_000), 57_000);
        window.mark_full(95_000).await;
        assert_eq!(window.sample(20), 95_000);
        assert_eq!(window.prefill(), Some(40_000));
        window.reset(2_000).await;
        assert_eq!(window.prefill(), None);
        assert_eq!(window.sample(2_001), 2_001);
        window.observe(3_000, 3_100).await;
        assert_eq!(window.prefill(), Some(3_000));
        assert_eq!(state.lock().unwrap().window_number, 1);
        assert_eq!(records.lock().unwrap().len(), 5);
    }

    #[test]
    fn persisted_window_counts_roundtrip_without_floating_point_precision_loss() {
        let expected = WindowUsageState {
            active_tokens: Some(9_007_199_254_740_993),
            prefill_input_tokens: Some(9_007_199_254_740_991),
            window_number: 12,
        };
        let item = RolloutItem::Event(EventItem {
            event: WINDOW_USAGE_EVENT.into(),
            turn_id: None,
            payload: Some(encode(&expected)),
        });
        assert_eq!(restore(&[item], 95_000), expected);
    }
}
