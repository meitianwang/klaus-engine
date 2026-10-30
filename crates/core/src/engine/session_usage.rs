//! Request-owned accounting preserves observed usage across query cancellation.

use super::{InferenceBundle, Session};
use crate::engine::query::{PrepareRequestUsage, RequestUsageReceipt};
use crate::runtime_values::RuntimeString;
use crate::schemas::message::{AssistantMessage, Usage};
use crate::store::receipt_broker::ReceiptSlot;
use std::sync::Arc;

struct SessionRequestUsage {
    session: Arc<Session>,
    bundle: Arc<InferenceBundle>,
    id: RuntimeString,
    model: RuntimeString,
    slot: Option<ReceiptSlot>,
    actual: Option<Usage>,
}

impl RequestUsageReceipt for SessionRequestUsage {
    fn observe(&mut self, usage: &Usage) {
        self.actual = Some(usage.clone());
    }

    fn settle(&mut self) {
        if let Some(usage) = self.actual.take() {
            self.session.record_reserved_assistant_usage_for(
                &AssistantMessage {
                    uuid: self.id.clone(),
                    model: self.model.clone(),
                    content: Vec::new(),
                    usage: Some(usage),
                },
                &self.bundle,
                self.slot.take(),
            );
        }
    }
}

impl Drop for SessionRequestUsage {
    fn drop(&mut self) {
        self.settle();
    }
}

impl Session {
    /// The factory captures the same inference revision as this request before
    /// waiting for capacity. The guard only observes events consumed by query.
    pub(crate) fn request_usage_recorder(
        self: &Arc<Self>,
        capture: Arc<dyn Fn() -> Arc<InferenceBundle> + Send + Sync>,
    ) -> PrepareRequestUsage {
        let session = self.clone();
        Arc::new(move |id, model, signal| {
            let session = session.clone();
            let bundle = capture();
            Box::pin(async move {
                let slot = session
                    .reserve_usage_receipt(&signal)
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                Ok(Box::new(SessionRequestUsage {
                    session,
                    bundle,
                    id,
                    model,
                    slot,
                    actual: None,
                }) as Box<dyn RequestUsageReceipt>)
            })
        })
    }
}
