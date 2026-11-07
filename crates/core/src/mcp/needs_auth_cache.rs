//! A small on-disk cache of which OAuth MCP servers recently needed auth, keyed
//! by the server key. It lets a connect skip a guaranteed-401 attempt for a
//! server known to need auth and with no stored token. Entries are JSON `{ts}`
//! and expire after [`NEEDS_AUTH_TTL_MS`]; writes are serialized by a lockfile so
//! concurrent connects don't corrupt the map.

use crate::fs::lockfile::{with_lock, LockOptions};
use serde_json::{Map, Value};
use std::path::Path;

/// How long a cached "needs auth" verdict is trusted.
pub const NEEDS_AUTH_TTL_MS: i64 = 15 * 60 * 1000;

/// A lockfile-serialized JSON map of server-key → timestamp.
pub struct NeedsAuthCache {
    file: String,
}

impl NeedsAuthCache {
    pub fn new(file: impl Into<String>) -> Self {
        Self { file: file.into() }
    }

    /// Whether `key` was marked needs-auth within the TTL.
    pub async fn is_fresh(&self, key: &str, ttl_ms: i64, now: i64) -> bool {
        let map = self.read().await;
        match map
            .get(key)
            .and_then(|e| e.get("ts"))
            .and_then(|t| t.as_i64())
        {
            Some(ts) => now - ts < ttl_ms,
            None => false,
        }
    }

    /// Record that `key` needs auth as of `now`.
    pub async fn mark(&self, key: &str, now: i64) {
        let key = key.to_string();
        self.mutate(move |map| {
            let mut entry = Map::new();
            entry.insert("ts".to_string(), Value::from(now));
            map.insert(key, Value::Object(entry));
        })
        .await;
    }

    /// Drop any needs-auth record for `key` (e.g. after a successful connect).
    pub async fn clear(&self, key: &str) {
        let key = key.to_string();
        self.mutate(move |map| {
            map.remove(&key);
        })
        .await;
    }

    async fn read(&self) -> Map<String, Value> {
        match tokio::fs::read_to_string(&self.file).await {
            Ok(raw) => match serde_json::from_str::<Value>(&raw) {
                Ok(Value::Object(map)) => map,
                _ => Map::new(),
            },
            Err(_) => Map::new(),
        }
    }

    async fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut Map<String, Value>),
    {
        if let Some(parent) = Path::new(&self.file).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let file = self.file.clone();
        // Advisory cache: a write failure must never break a connect.
        let _ = with_lock(
            &self.file,
            move || async move {
                let mut map = match tokio::fs::read_to_string(&file).await {
                    Ok(raw) => match serde_json::from_str::<Value>(&raw) {
                        Ok(Value::Object(m)) => m,
                        _ => Map::new(),
                    },
                    Err(_) => Map::new(),
                };
                f(&mut map);
                tokio::fs::write(&file, Value::Object(map).to_string()).await
            },
            LockOptions::default(),
        )
        .await;
    }
}

/// The default needs-auth cache file under `dir` (default `~/.klaus`).
pub fn default_needs_auth_cache(dir: Option<&str>) -> NeedsAuthCache {
    let base = match dir {
        Some(d) if !d.is_empty() => d.to_string(),
        _ => format!("{}/.klaus", home_dir()),
    };
    NeedsAuthCache::new(format!("{}/mcp-needs-auth.json", base))
}

fn home_dir() -> String {
    #[allow(deprecated)]
    std::env::home_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir(tag: &str) -> String {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let uniq = format!(
            "ae-mcp-nac-{}-{}-{}-{}",
            tag,
            std::process::id(),
            n,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn marks_reports_freshness_within_the_ttl_expires_and_clears() {
        let dir = temp_dir("nac");
        let cache = NeedsAuthCache::new(format!("{}/nac.json", dir));
        let now: i64 = 1_000_000_000_000;
        assert!(!cache.is_fresh("k", NEEDS_AUTH_TTL_MS, now).await);
        cache.mark("k", now).await;
        assert!(cache.is_fresh("k", NEEDS_AUTH_TTL_MS, now + 60_000).await);
        assert!(
            !cache
                .is_fresh("k", NEEDS_AUTH_TTL_MS, now + NEEDS_AUTH_TTL_MS + 1)
                .await
        );
        cache.clear("k").await;
        assert!(!cache.is_fresh("k", NEEDS_AUTH_TTL_MS, now).await);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
