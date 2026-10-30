//! Shared asynchronous secret-store contract.
//!
//! MCP and model-provider OAuth both persist opaque credential values through
//! this boundary. Values must never be logged or included in runtime snapshots.

use std::fmt;
use std::sync::Arc;

/// Error returned by a credential-store backend.
pub type CredentialStoreError = Box<dyn std::error::Error + Send + Sync>;

/// A minimal asynchronous secret store keyed by an opaque string.
#[async_trait::async_trait]
pub trait CredentialStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>, CredentialStoreError>;
    async fn set(&self, key: &str, value: &str) -> Result<(), CredentialStoreError>;
    async fn delete(&self, key: &str) -> Result<(), CredentialStoreError>;

    /// Atomically replace a value only while it matches `expected`. `None`
    /// means the key must be absent. All backend writers, including `set` and
    /// `delete`, must share this operation's synchronization boundary.
    /// Backends without that guarantee must reject the operation.
    async fn compare_exchange(
        &self,
        _key: &str,
        _expected: Option<&str>,
        _replacement: &str,
    ) -> Result<bool, CredentialStoreError> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "credential store does not support atomic conditional writes",
        )
        .into())
    }
}

/// Isolates one credential family inside a shared backend.
///
/// The namespace is length-prefixed so caller-controlled names cannot collide
/// by choosing delimiter characters. The wrapped backend and stored values are
/// deliberately absent from `Debug` output.
#[derive(Clone)]
pub struct NamespacedCredentialStore {
    inner: Arc<dyn CredentialStore>,
    namespace: String,
}

impl NamespacedCredentialStore {
    pub fn new(inner: Arc<dyn CredentialStore>, namespace: impl Into<String>) -> Self {
        Self {
            inner,
            namespace: namespace.into(),
        }
    }

    fn qualified_key(&self, key: &str) -> String {
        format!("ns:{}:{}{}", self.namespace.len(), self.namespace, key)
    }
}

impl fmt::Debug for NamespacedCredentialStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespacedCredentialStore")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl CredentialStore for NamespacedCredentialStore {
    async fn get(&self, key: &str) -> Result<Option<String>, CredentialStoreError> {
        self.inner.get(&self.qualified_key(key)).await
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), CredentialStoreError> {
        self.inner.set(&self.qualified_key(key), value).await
    }

    async fn delete(&self, key: &str) -> Result<(), CredentialStoreError> {
        self.inner.delete(&self.qualified_key(key)).await
    }

    async fn compare_exchange(
        &self,
        key: &str,
        expected: Option<&str>,
        replacement: &str,
    ) -> Result<bool, CredentialStoreError> {
        self.inner
            .compare_exchange(&self.qualified_key(key), expected, replacement)
            .await
    }
}

/// The credential-store key holding a BYO model provider's API key. The engine
/// is the only reader (session assembly) and writer (`model/credential/*`); the
/// shell references the provider by id and never sees the key. Kept in one place
/// so the writer and the session-assembly reader cannot drift apart.
pub fn model_credential_key(id: &str) -> String {
    format!("modelProvider:apiKey:{id}")
}

/// Whether a model-provider id is safe to embed in a credential-store key: ASCII
/// letters, digits, `.`, `_`, `-`, non-empty, at most 128 bytes. Rejecting the
/// rest keeps a host-supplied id from reaching into another credential family.
pub fn is_valid_model_provider_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<String, String>>);

    #[async_trait::async_trait]
    impl CredentialStore for MemoryStore {
        async fn get(&self, key: &str) -> Result<Option<String>, CredentialStoreError> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }

        async fn set(&self, key: &str, value: &str) -> Result<(), CredentialStoreError> {
            self.0
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), CredentialStoreError> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }

        async fn compare_exchange(
            &self,
            key: &str,
            expected: Option<&str>,
            replacement: &str,
        ) -> Result<bool, CredentialStoreError> {
            let mut values = self.0.lock().unwrap();
            if values.get(key).map(String::as_str) != expected {
                return Ok(false);
            }
            values.insert(key.to_string(), replacement.to_string());
            Ok(true)
        }
    }

    #[tokio::test]
    async fn namespaces_isolate_identical_keys() {
        let inner: Arc<dyn CredentialStore> = Arc::new(MemoryStore::default());
        let account_a = NamespacedCredentialStore::new(inner.clone(), "provider/openai/a");
        let account_b = NamespacedCredentialStore::new(inner, "provider/openai/b");

        account_a.set("oauth:client", "secret-a").await.unwrap();
        account_b.set("oauth:client", "secret-b").await.unwrap();

        assert_eq!(
            account_a.get("oauth:client").await.unwrap().as_deref(),
            Some("secret-a")
        );
        assert_eq!(
            account_b.get("oauth:client").await.unwrap().as_deref(),
            Some("secret-b")
        );
        assert!(!format!("{account_a:?}").contains("secret-a"));
        assert!(account_a
            .compare_exchange("oauth:client", Some("secret-a"), "rotated-a")
            .await
            .unwrap());
        assert!(!account_b
            .compare_exchange("oauth:client", Some("secret-a"), "wrong-account")
            .await
            .unwrap());
        assert_eq!(
            account_b.get("oauth:client").await.unwrap().as_deref(),
            Some("secret-b")
        );
    }
}
