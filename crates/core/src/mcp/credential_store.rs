//! Encrypted-file credential store, plus an in-memory store for
//! tests/ephemeral runs.
//!
//! All platforms share one backend: AES-256-GCM encrypted JSON at
//! `<config home>/credentials.enc` with the data key in `<config home>/cred.key`, both
//! created owner-only (0600). We deliberately do not shell out to OS keychain
//! CLIs (macOS `security`, Linux `secret-tool`): their prompt behavior is
//! platform-fragile — macOS `security` with a bare `-w` reads the prompt from
//! the controlling tty instead of a piped stdin, so the child blocked on the
//! terminal and stored an empty secret — while an owner-only file behaves
//! identically everywhere, the same shape other agent CLIs use (codex stores
//! its key in ~/.codex/auth.json at 0600, likewise aws/gh).
//!
//! Stored secret values are never logged.
//! The config home follows `KLAUS_CONFIG_DIR`, defaulting to `~/.klaus`.

pub use crate::credential_store::{
    CredentialStore, CredentialStoreError as StoreError, NamespacedCredentialStore,
};
use crate::fs::exclusive_lock::{try_lock_exclusive, LockAttempt};
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::Aes256Gcm;
use rand::RngCore;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

/// An in-memory store backed by a map. Intended for tests and ephemeral runs.
#[derive(Default)]
pub struct InMemoryCredentialStore {
    map: StdMutex<HashMap<String, String>>,
}

impl InMemoryCredentialStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl CredentialStore for InMemoryCredentialStore {
    async fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    async fn set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        self.map
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
    async fn compare_exchange(
        &self,
        key: &str,
        expected: Option<&str>,
        replacement: &str,
    ) -> Result<bool, StoreError> {
        let mut values = self.map.lock().unwrap();
        if values.get(key).map(String::as_str) != expected {
            return Ok(false);
        }
        values.insert(key.to_string(), replacement.to_string());
        Ok(true)
    }
}

/// The on-disk credential store: one AES-256-GCM blob holding a JSON map of
/// key -> secret, with the 32-byte data key in a sibling file. Both files are
/// written owner-only from the start.
pub struct FileCredentialStore {
    dir: String,
    file_path: String,
    key_path: String,
    read_cache: StdMutex<StoreReadCache>,
}

/// Identity of the encrypted blob on disk: modification time and length. Every
/// writer rewrites the whole file, so any change moves at least one of the two
/// on any filesystem clock this engine runs on; an unchanged pair means
/// unchanged bytes, which is what lets a read skip the lock, the disk and the
/// decryption.
#[derive(Clone, Copy, PartialEq, Eq)]
struct StoreFingerprint {
    modified: std::time::SystemTime,
    len: u64,
}

#[derive(Default)]
struct StoreReadCache {
    fingerprint: Option<StoreFingerprint>,
    values: Map<String, Value>,
}

impl Default for FileCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FileCredentialStore {
    /// Capture the engine's config home when the store is constructed. Later
    /// environment changes must not redirect an existing store's credentials.
    pub fn new() -> Self {
        Self::at_config_root(crate::fs::paths::config_home())
    }

    /// Use an embedder's explicit profile root. Pass this store through the
    /// existing credential-store override alongside that profile's config.
    pub fn at_config_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let dir = root.to_string_lossy().into_owned();
        let file_path = root.join("credentials.enc").to_string_lossy().into_owned();
        let key_path = root.join("cred.key").to_string_lossy().into_owned();
        Self {
            dir,
            file_path,
            key_path,
            read_cache: StdMutex::new(StoreReadCache::default()),
        }
    }

    async fn locked<T, F, Fut>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, StoreError>> + Send,
    {
        tokio::fs::create_dir_all(&self.dir).await?;
        set_mode(&self.dir, 0o700).await;
        // Every instance and process locks the same persistent inode. Never
        // unlink this file or reclaim a live owner by age: that would permit
        // overlapping read/modify/write sections after a stalled process.
        let lock_path = Path::new(&self.dir).join("credentials.write-lock");
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(&lock_path)?;
        set_mode(&lock_path.to_string_lossy(), 0o600).await;
        for _ in 0..600 {
            match try_lock_exclusive(&lock)? {
                LockAttempt::Acquired => {
                    let result = operation().await;
                    drop(lock);
                    return result;
                }
                LockAttempt::Contended => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        Err("credential store write lock timed out".into())
    }

    async fn load_key(&self) -> Result<Vec<u8>, StoreError> {
        match tokio::fs::read(&self.key_path).await {
            Ok(existing) if existing.len() == 32 => return Ok(existing),
            Ok(_) => return Err("invalid credential encryption key".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut key = vec![0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        write_private_file(&self.key_path, &key)?;
        Ok(key)
    }

    /// The decrypted map without touching the lock or the disk when the blob
    /// on disk is byte-identical to the one this process already decrypted.
    /// Readers are the hot path: a reconfiguration storm asks the same handful
    /// of keys once per live conversation, and every miss used to queue on the
    /// global write lock behind a fresh read plus AES-GCM decryption.
    async fn cached_read(&self) -> Result<Map<String, Value>, StoreError> {
        let fingerprint = match tokio::fs::metadata(&self.file_path).await {
            Ok(metadata) => StoreFingerprint {
                modified: metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
                len: metadata.len(),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.remember(None, Map::new());
                return Ok(Map::new());
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(values) = self.cached_values(fingerprint) {
            return Ok(values);
        }
        let values = self.read_store().await?;
        self.remember(Some(fingerprint), values.clone());
        Ok(values)
    }

    fn cached_values(&self, fingerprint: StoreFingerprint) -> Option<Map<String, Value>> {
        let cache = self
            .read_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (cache.fingerprint == Some(fingerprint)).then(|| cache.values.clone())
    }

    fn remember(&self, fingerprint: Option<StoreFingerprint>, values: Map<String, Value>) {
        let mut cache = self
            .read_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.fingerprint = fingerprint;
        cache.values = values;
    }

    fn invalidate(&self) {
        let mut cache = self
            .read_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.fingerprint = None;
        cache.values = Map::new();
    }

    async fn read_store(&self) -> Result<Map<String, Value>, StoreError> {
        let key = self.load_key().await?;
        let raw = match tokio::fs::read(&self.file_path).await {
            Ok(r) => r,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Map::new()),
            Err(error) => return Err(error.into()),
        };
        if raw.len() < 28 {
            return Err("invalid encrypted credential store".into());
        }
        // Layout: [12-byte IV][16-byte auth tag][ciphertext].
        let iv = &raw[0..12];
        let tag_bytes = &raw[12..28];
        let ciphertext = &raw[28..];
        let cipher = match Aes256Gcm::new_from_slice(&key) {
            Ok(c) => c,
            Err(_) => return Err("invalid credential encryption key".into()),
        };
        let nonce = GenericArray::from_slice(iv);
        let tag = GenericArray::from_slice(tag_bytes);
        let mut buffer = ciphertext.to_vec();
        match cipher.decrypt_in_place_detached(nonce, b"", &mut buffer, tag) {
            Ok(()) => match serde_json::from_slice::<Value>(&buffer) {
                Ok(Value::Object(map)) => Ok(map),
                _ => Err("invalid credential store document".into()),
            },
            Err(_) => Err("credential store decryption failed".into()),
        }
    }

    async fn write_store(&self, store: &Map<String, Value>) -> Result<(), StoreError> {
        let key = self.load_key().await?;
        let mut iv = [0u8; 12];
        rand::rngs::OsRng.fill_bytes(&mut iv);
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| -> StoreError { format!("key init failed: {e}").into() })?;
        let nonce = GenericArray::from_slice(&iv);
        let mut buffer = Value::Object(store.clone()).to_string().into_bytes();
        let tag = cipher
            .encrypt_in_place_detached(nonce, b"", &mut buffer)
            .map_err(|e| -> StoreError { format!("encrypt failed: {e}").into() })?;
        let mut out = Vec::with_capacity(12 + 16 + buffer.len());
        out.extend_from_slice(&iv);
        out.extend_from_slice(tag.as_slice());
        out.extend_from_slice(&buffer);
        write_private_file(&self.file_path, &out)?;
        Ok(())
    }

    async fn file_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let store = self.cached_read().await?;
        match store.get(key) {
            None => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err("invalid credential store value".into()),
        }
    }

    async fn file_set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        let mut store = self.cached_read().await?;
        store.insert(key.to_string(), Value::String(value.to_string()));
        self.write_store(&store).await?;
        self.invalidate();
        Ok(())
    }

    async fn file_delete(&self, key: &str) -> Result<(), StoreError> {
        let mut store = self.cached_read().await?;
        if store.remove(key).is_some() {
            self.write_store(&store).await?;
            self.invalidate();
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CredentialStore for FileCredentialStore {
    async fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.locked(|| self.file_get(key)).await
    }
    async fn set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        self.locked(|| self.file_set(key, value)).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.locked(|| self.file_delete(key)).await
    }
    async fn compare_exchange(
        &self,
        key: &str,
        expected: Option<&str>,
        replacement: &str,
    ) -> Result<bool, StoreError> {
        self.locked(|| async {
            let mut values = self.cached_read().await?;
            let current = match values.get(key) {
                None => None,
                Some(Value::String(value)) => Some(value.as_str()),
                Some(_) => return Err("invalid credential store value".into()),
            };
            if current != expected {
                return Ok(false);
            }
            values.insert(key.to_string(), Value::String(replacement.to_string()));
            self.write_store(&values).await?;
            self.invalidate();
            Ok(true)
        })
        .await
    }
}

/// Create-or-replace a file that is owner-only from the moment it exists, so
/// no window exposes the bytes under the umask default. An existing file keeps
/// its old mode across truncate, so the mode is re-asserted afterwards.
fn write_private_file(path: &str, bytes: &[u8]) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(bytes)?;
        set_permissions_blocking(path, 0o600);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

async fn set_mode(path: &str, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).await;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
}

#[cfg(unix)]
fn set_permissions_blocking(path: &str, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_file_store_uses_the_engine_config_home_before_any_io() {
        let _lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        let configured = std::env::temp_dir().join(format!(
            "klaus-credential-profile-contract-{}",
            uuid::Uuid::new_v4()
        ));
        crate::runtime_process::set_process_environment_var(
            "KLAUS_CONFIG_DIR",
            configured.to_str().unwrap(),
        );
        let store = FileCredentialStore::new();
        let expected = crate::fs::paths::config_home();
        match previous {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        // This assertion fails before the old constructor can touch the real
        // user profile. Constructing a credential store must perform no I/O.
        assert_eq!(store.dir, expected);
        assert!(!configured.exists());
        assert_eq!(
            std::path::Path::new(&store.file_path),
            configured.join("credentials.enc")
        );
        assert_eq!(
            std::path::Path::new(&store.key_path),
            configured.join("cred.key")
        );
    }

    #[test]
    fn default_file_store_keeps_the_default_profile_for_absent_or_empty_override() {
        let _lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR");
        let absent = FileCredentialStore::new();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", "");
        let empty = FileCredentialStore::new();
        match previous {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        #[allow(deprecated)]
        let expected = std::env::home_dir().unwrap().join(".klaus");
        assert_eq!(Path::new(&absent.dir), expected);
        assert_eq!(Path::new(&empty.dir), expected);
    }

    #[tokio::test]
    async fn in_memory_round_trips_and_deletes() {
        let store = InMemoryCredentialStore::new();
        assert_eq!(store.get("k").await.unwrap(), None);
        store.set("k", "v").await.unwrap();
        assert_eq!(store.get("k").await.unwrap().as_deref(), Some("v"));
        store.set("k", "v2").await.unwrap();
        assert_eq!(store.get("k").await.unwrap().as_deref(), Some("v2"));
        store.delete("k").await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), None);
    }

    fn scratch_store(name: &str) -> (std::path::PathBuf, FileCredentialStore) {
        let dir = std::env::temp_dir().join(format!(
            "klaus-cred-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = FileCredentialStore::at_config_root(&dir);
        (dir, store)
    }

    #[tokio::test]
    async fn file_store_encrypts_and_round_trips() {
        let (dir, store) = scratch_store("round-trip");
        assert_eq!(store.get("a").await.unwrap(), None);
        store.set("a", "secret").await.unwrap();
        store.set("b", "two").await.unwrap();
        assert_eq!(store.get("a").await.unwrap().as_deref(), Some("secret"));
        assert_eq!(store.get("b").await.unwrap().as_deref(), Some("two"));
        // The on-disk blob must not contain the plaintext secret.
        let raw = std::fs::read(&store.file_path).unwrap();
        assert!(!raw.windows(6).any(|w| w == b"secret"));
        store.delete("a").await.unwrap();
        assert_eq!(store.get("a").await.unwrap(), None);
        assert_eq!(store.get("b").await.unwrap().as_deref(), Some("two"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_store_reads_see_another_instance_writes_without_restarting() {
        let (dir, first) = scratch_store("cross-instance");
        let second = FileCredentialStore::at_config_root(&dir);
        first.set("oauth", "token-1").await.unwrap();
        assert_eq!(
            second.get("oauth").await.unwrap().as_deref(),
            Some("token-1")
        );
        // The first instance now holds a decrypted cache of the blob; a write
        // through the second instance must still reach it.
        second.set("oauth", "token-2").await.unwrap();
        assert_eq!(
            first.get("oauth").await.unwrap().as_deref(),
            Some("token-2")
        );
        second.delete("oauth").await.unwrap();
        assert_eq!(first.get("oauth").await.unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_store_cache_invalidates_when_the_blob_changes_under_the_same_length() {
        let (dir, store) = scratch_store("same-length-rewrite");
        store.set("oauth", "token-a").await.unwrap();
        assert_eq!(
            store.get("oauth").await.unwrap().as_deref(),
            Some("token-a")
        );
        // Rewrite the encrypted blob in place with different bytes of the same
        // length: only the modification time can tell the cache the bytes moved.
        let path = std::path::Path::new(&store.file_path);
        let mut raw = std::fs::read(path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        std::fs::write(path, &raw).unwrap();
        let error = store.get("oauth").await.unwrap_err().to_string();
        assert!(error.contains("decryption"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn conditional_writes_share_the_backend_boundary_with_set_and_delete() {
        let store = InMemoryCredentialStore::new();
        assert!(store.compare_exchange("oauth", None, "old").await.unwrap());
        store.set("oauth", "new-login").await.unwrap();
        assert!(!store
            .compare_exchange("oauth", Some("old"), "stale-refresh")
            .await
            .unwrap());
        assert_eq!(
            store.get("oauth").await.unwrap().as_deref(),
            Some("new-login")
        );
        store.delete("oauth").await.unwrap();
        assert!(!store
            .compare_exchange("oauth", Some("new-login"), "stale-refresh")
            .await
            .unwrap());
        assert!(store
            .compare_exchange("oauth", None, "next-login")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn file_store_instances_conditionally_write_without_losing_other_keys() {
        let (dir, first) = scratch_store("conditional");
        let second = FileCredentialStore::at_config_root(&dir);
        first.set("oauth", "old").await.unwrap();
        second.set("other", "unrelated").await.unwrap();
        second.set("oauth", "new-login").await.unwrap();
        assert!(!first
            .compare_exchange("oauth", Some("old"), "stale-refresh")
            .await
            .unwrap());
        assert!(first
            .compare_exchange("oauth", Some("new-login"), "new-rotation")
            .await
            .unwrap());
        assert_eq!(
            second.get("oauth").await.unwrap().as_deref(),
            Some("new-rotation")
        );
        assert_eq!(
            first.get("other").await.unwrap().as_deref(),
            Some("unrelated")
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn file_store_child_conditional_write() {
        let Some(root) = std::env::var_os("KLAUS_TEST_CREDENTIAL_CHILD_ROOT") else {
            return;
        };
        let mode = std::env::var("KLAUS_TEST_CREDENTIAL_CHILD_MODE").unwrap();
        let store = FileCredentialStore::at_config_root(&root);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            if mode == "conditional" {
                tokio::fs::write(Path::new(&root).join("child-ready"), b"ready")
                    .await
                    .unwrap();
                assert!(!store
                    .compare_exchange("oauth", Some("old"), "stale-refresh")
                    .await
                    .unwrap());
                assert_eq!(
                    store.get("oauth").await.unwrap().as_deref(),
                    Some("new-login")
                );
                tokio::fs::write(Path::new(&root).join("child-done"), b"done")
                    .await
                    .unwrap();
            } else {
                store.set(&mode, "child-value").await.unwrap();
            }
        });
    }

    fn credential_child(dir: &Path, mode: &str) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mcp::credential_store::tests::file_store_child_conditional_write",
                "--nocapture",
            ])
            .env("KLAUS_TEST_CREDENTIAL_CHILD_ROOT", dir)
            .env("KLAUS_TEST_CREDENTIAL_CHILD_MODE", mode)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn assert_credential_child(output: std::process::Output) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "child failed: {stdout}\n{stderr}");
        assert!(
            stdout.contains("1 passed"),
            "child test was not executed: {stdout}\n{stderr}"
        );
    }

    #[tokio::test]
    async fn file_store_cross_process_cas_preserves_new_login_and_parallel_map_writes() {
        let (dir, store) = scratch_store("process-cas");
        store.set("oauth", "old").await.unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join("credentials.write-lock"))
            .unwrap();
        assert_eq!(try_lock_exclusive(&lock).unwrap(), LockAttempt::Acquired);
        let child = credential_child(&dir, "conditional");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !dir.join("child-ready").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!dir.join("child-done").exists());
        // The parent owns the actual backend lock while replacing the login.
        // A separate process's CAS must wait, then reject its stale expectation.
        store.file_set("oauth", "new-login").await.unwrap();
        fs2::FileExt::unlock(&lock).unwrap();
        drop(lock);
        let output = tokio::task::spawn_blocking(move || child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert_credential_child(output);
        let children = (0..4)
            .map(|index| credential_child(&dir, &format!("peer-{index}")))
            .collect::<Vec<_>>();
        store.set("parent", "parent-value").await.unwrap();
        for child in children {
            let output = tokio::task::spawn_blocking(move || child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
            assert_credential_child(output);
        }
        assert_eq!(
            store.get("oauth").await.unwrap().as_deref(),
            Some("new-login")
        );
        assert_eq!(
            store.get("parent").await.unwrap().as_deref(),
            Some("parent-value")
        );
        for index in 0..4 {
            assert_eq!(
                store
                    .get(&format!("peer-{index}"))
                    .await
                    .unwrap()
                    .as_deref(),
                Some("child-value")
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn corrupted_file_store_is_not_treated_as_an_absent_cas_value() {
        let (dir, store) = scratch_store("corrupt-cas");
        store.set("oauth", "old").await.unwrap();
        std::fs::write(&store.file_path, b"invalid-encrypted-store").unwrap();
        assert!(store
            .compare_exchange("oauth", None, "new-login")
            .await
            .is_err());
        assert_eq!(
            std::fs::read(&store.file_path).unwrap().as_slice(),
            b"invalid-encrypted-store"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_store_creates_owner_only_files() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, store) = scratch_store("modes");
        store.set("a", "secret").await.unwrap();
        for path in [&store.file_path, &store.key_path] {
            let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "{path} must be owner-only");
        }
        let lock_mode = std::fs::metadata(dir.join("credentials.write-lock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(lock_mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
