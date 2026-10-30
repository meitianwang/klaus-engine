//! Session-scoped provider transport prewarm: schedule at session creation,
//! consume at the first turn, cancel at shutdown. See
//! [`crate::provider::prewarm`] for the transport semantics; this part only
//! owns the lifecycle wiring: the startup-prewarm
//! schedule/consume/abort flow.

use crate::provider::prewarm::{TransportPrewarm, PREWARM_CONNECT_TIMEOUT};
use crate::sync_ext::MutexExt;

use super::Session;

impl Session {
    /// Spawn the startup prewarm for this session's provider (no-op when the
    /// config switch is off or the provider exposes no HTTP endpoints).
    pub(crate) fn schedule_transport_prewarm(self: &std::sync::Arc<Self>) {
        if !self.config.prewarm {
            return;
        }
        let bundle = self.inference_snapshot();
        let Some(prewarm) = TransportPrewarm::schedule(bundle.provider.clone()) else {
            return;
        };
        let mut slot = self.transport_prewarm.lock_recover();
        if self.model_revision() == bundle.revision {
            *slot = Some((bundle.revision, prewarm));
        } else {
            prewarm.cancel();
        }
    }

    /// The first turn's consumption point: resolve the startup prewarm with
    /// the connect budget so a ready warm pool is actually used, then hand
    /// the handle back for the keep-alive loop. Later turns skip (the pool
    /// stays warm via keep-alive re-probes).
    pub(crate) async fn consume_transport_prewarm_for(&self, revision: u64) {
        let prewarm = {
            let mut slot = self.transport_prewarm.lock_recover();
            if slot
                .as_ref()
                .is_none_or(|(version, _)| *version != revision)
            {
                return;
            }
            if self
                .transport_prewarm_consumed
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return;
            }
            slot.take()
        };
        if let Some((_, prewarm)) = &prewarm {
            let _ = prewarm.resolve(PREWARM_CONNECT_TIMEOUT).await;
        }
        // On a miss the first request pays the cold handshake; the handle is
        // retained either way so keep-alive re-warms the pool for later turns.
        let mut slot = self.transport_prewarm.lock_recover();
        if self.model_revision() == revision && slot.is_none() {
            *slot = prewarm;
        } else if let Some((_, prewarm)) = prewarm {
            prewarm.cancel();
        }
    }

    /// Stop the keep-alive loop at session shutdown.
    pub(crate) fn cancel_transport_prewarm(&self) {
        if let Some((_, prewarm)) = self.transport_prewarm.lock_recover().take() {
            prewarm.cancel();
        }
    }
}
