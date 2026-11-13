//! Connection-owned admission and cancellation for standalone provider operations.

use klaus_agent_core::types::abort::AbortSignal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Key = (&'static str, String);
const MAX_OPERATIONS: usize = 4;

#[derive(Default)]
pub(crate) struct Operations {
    pending: Mutex<HashMap<Key, AbortSignal>>,
}

pub(crate) struct Operation {
    owner: Arc<Operations>,
    key: Key,
    signal: AbortSignal,
    publication_check: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

impl Operations {
    /// Registration precedes spawning so the next request may cancel immediately.
    pub(crate) fn register(
        self: &Arc<Self>,
        source: &'static str,
        id: Option<&str>,
    ) -> Result<Operation, String> {
        if id.is_some_and(|id| !valid_id(id)) {
            return Err("provider_operation_id_invalid".into());
        }
        let key = (
            source,
            id.map(str::to_owned)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        );
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        if pending.len() >= MAX_OPERATIONS {
            return Err("provider_operation_limit".into());
        }
        if pending.contains_key(&key) {
            return Err("provider_operation_id_in_use".into());
        }
        let signal = AbortSignal::new();
        pending.insert(key.clone(), signal.clone());
        Ok(Operation {
            owner: self.clone(),
            key,
            signal,
            publication_check: None,
        })
    }

    pub(crate) fn cancel(&self, source: &'static str, id: &str) -> Result<bool, String> {
        if !valid_id(id) {
            return Err("provider_operation_id_invalid".into());
        }
        let pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        let found = pending.get(&(source, id.to_owned()));
        if let Some(signal) = found {
            signal.cancel();
        }
        Ok(found.is_some())
    }

    pub(crate) fn cancel_all(&self) {
        for signal in self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            signal.cancel();
        }
    }
}

impl Operation {
    pub(crate) fn signal(&self) -> AbortSignal {
        self.signal.clone()
    }

    pub(crate) fn bind_publication_check(&mut self, check: Arc<dyn Fn() -> bool + Send + Sync>) {
        self.publication_check = Some(check);
    }

    pub(crate) fn publication_error(&self) -> Option<String> {
        if self.signal.is_cancelled() {
            return Some("provider_operation_cancelled".into());
        }
        if self
            .publication_check
            .as_ref()
            .is_some_and(|check| !check())
        {
            return Some("provider_credential_changed".into());
        }
        None
    }

    pub(crate) async fn invalidated(&self) {
        loop {
            if self.publication_error().is_some() {
                return;
            }
            tokio::select! {
                _ = self.signal.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.signal.cancel();
        self.owner
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_keys_are_source_scoped_and_admission_lasts_until_publication() {
        let operations = Arc::new(Operations::default());
        let a = operations.register("claude", Some("one")).unwrap();
        let b = operations.register("codex", Some("one")).unwrap();
        assert!(operations.cancel("claude", "one").unwrap());
        assert!(a.signal().is_cancelled());
        assert!(!b.signal().is_cancelled());
        let c = operations.register("qoder", None).unwrap();
        let d = operations.register("qwenwork", None).unwrap();
        assert!(operations.register("codex", None).is_err());
        drop(a);
        let e = operations.register("codex", None).unwrap();
        drop((b, c, d, e));
        assert!(!operations.cancel("claude", "one").unwrap());
    }

    #[tokio::test]
    async fn publication_rejects_replaced_credentials_and_cancellation() {
        let operations = Arc::new(Operations::default());
        let mut operation = operations.register("claude", None).unwrap();
        operation.bind_publication_check(Arc::new(|| false));
        assert_eq!(
            operation.publication_error().as_deref(),
            Some("provider_credential_changed")
        );
        operation.invalidated().await;
        operations.cancel_all();
        assert!(operation.signal().is_cancelled());
    }
}
