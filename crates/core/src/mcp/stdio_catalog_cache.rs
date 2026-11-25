//! Persistent discovery catalog cache for local `stdio` MCP servers.
//!
//! Spawning every configured `stdio` MCP subprocess eagerly during session
//! startup pays child process creation, `initialize`, and `tools/list` latency
//! even when a turn never invokes that server's tools. Caching the discovered
//! catalog keyed by a fingerprint of the server declaration (and any local
//! command/script file metadata) allows warm sessions to register the server's
//! tool schemas, prompts, resources, and instructions immediately while keeping
//! the child process unspawned until a tool, prompt, or resource on that server
//! is actually requested.

use crate::fs::lockfile::{with_lock, LockOptions};
use crate::mcp::types::{
    McpResourceTemplateDescriptor, McpServerConfig, McpServerInfo, McpToolDescriptor,
};
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};
use crate::types::tool::{McpPromptDescriptor, McpResourceDescriptor};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// How long a cached `stdio` server catalog remains valid without a live refresh.
pub const STDIO_CATALOG_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Maximum number of server fingerprints retained in the cache file.
const MAX_STDIO_CATALOG_ENTRIES: usize = 128;

/// Cached discovery snapshot for a single `stdio` MCP server.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StdioCatalogSnapshot {
    pub tools: Vec<McpToolDescriptor>,
    pub resources: Vec<McpResourceDescriptor>,
    pub resource_templates: Vec<McpResourceTemplateDescriptor>,
    pub prompts: Vec<McpPromptDescriptor>,
    pub instructions: Option<RuntimeString>,
    pub server_info: Option<McpServerInfo>,
}

impl RuntimeEncode for StdioCatalogSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("tools", &self.tools)?;
        if !self.resources.is_empty() {
            object.field("resources", &self.resources)?;
        }
        if !self.resource_templates.is_empty() {
            object.field("resourceTemplates", &self.resource_templates)?;
        }
        if !self.prompts.is_empty() {
            object.field("prompts", &self.prompts)?;
        }
        if let Some(instructions) = &self.instructions {
            object.field("instructions", instructions)?;
        }
        if let Some(server_info) = &self.server_info {
            object.field("serverInfo", server_info)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for StdioCatalogSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tools: object.defaulted("tools", &[], Vec::new)?,
                resources: object.defaulted("resources", &[], Vec::new)?,
                resource_templates: object.defaulted("resourceTemplates", &[], Vec::new)?,
                prompts: object.defaulted("prompts", &[], Vec::new)?,
                instructions: object.optional("instructions", &[])?,
                server_info: object.optional("serverInfo", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone)]
struct StoredEntry {
    ts: i64,
    snapshot: StdioCatalogSnapshot,
}

impl RuntimeEncode for StoredEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("ts", &self.ts)?;
        object.field("snapshot", &self.snapshot)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for StoredEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                ts: object.required("ts", &[])?,
                snapshot: object.required("snapshot", &[])?,
            })
        })
    }
}

/// Lockfile-serialized JSON map of `fingerprint -> { ts, snapshot }`.
#[derive(Clone)]
pub struct StdioCatalogCache {
    file: String,
}

impl StdioCatalogCache {
    pub fn new(file: impl Into<String>) -> Self {
        Self { file: file.into() }
    }

    pub async fn get_fresh(
        &self,
        fingerprint: &str,
        ttl_ms: i64,
        now: i64,
    ) -> Option<StdioCatalogSnapshot> {
        let raw = tokio::fs::read_to_string(&self.file).await.ok()?;
        let parsed = runtime_json::parse(&RuntimeString::from(raw)).ok()?;
        let map = parsed.as_object()?;
        let entry_val = map.get(fingerprint)?;
        let entry: StoredEntry = runtime_codec::decode_value(entry_val).ok()?;
        if now.saturating_sub(entry.ts) < ttl_ms {
            Some(entry.snapshot)
        } else {
            None
        }
    }

    pub async fn put(&self, fingerprint: &str, snapshot: &StdioCatalogSnapshot, now: i64) {
        let Ok(encoded) = runtime_codec::encode_value(&StoredEntry {
            ts: now,
            snapshot: snapshot.clone(),
        }) else {
            return;
        };
        let key = RuntimeString::from(fingerprint);
        self.mutate(now, move |map| {
            map.insert(key, encoded);
        })
        .await;
    }

    pub async fn remove(&self, fingerprint: &str) {
        let key = fingerprint.to_string();
        self.mutate(0, move |map| {
            map.remove(key.as_str());
        })
        .await;
    }

    async fn mutate<F>(&self, now: i64, f: F)
    where
        F: FnOnce(&mut RuntimeObject),
    {
        if let Some(parent) = Path::new(&self.file).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let file = self.file.clone();
        let _ = with_lock(
            &self.file,
            move || async move {
                let mut map = match tokio::fs::read_to_string(&file).await {
                    Ok(raw) => runtime_json::parse(&RuntimeString::from(raw))
                        .ok()
                        .and_then(|v| v.as_object().cloned())
                        .unwrap_or_default(),
                    Err(_) => RuntimeObject::new(),
                };
                if now > 0 {
                    let expired: Vec<RuntimeString> = map
                        .entries()
                        .filter_map(|(k, value)| {
                            let fresh = value
                                .get("ts")
                                .and_then(RuntimeJson::as_i64)
                                .is_some_and(|ts| now.saturating_sub(ts) < STDIO_CATALOG_TTL_MS);
                            (!fresh).then(|| k.clone())
                        })
                        .collect();
                    for stale_key in expired {
                        map.remove(stale_key);
                    }
                }
                f(&mut map);
                if map.len() > MAX_STDIO_CATALOG_ENTRIES {
                    let mut timestamps: Vec<(RuntimeString, i64)> = map
                        .entries()
                        .map(|(k, v)| {
                            let ts = v.get("ts").and_then(RuntimeJson::as_i64).unwrap_or(0);
                            (k.clone(), ts)
                        })
                        .collect();
                    timestamps.sort_by_key(|(_, ts)| *ts);
                    let drop_count = timestamps.len().saturating_sub(MAX_STDIO_CATALOG_ENTRIES);
                    for (stale_key, _) in timestamps.into_iter().take(drop_count) {
                        map.remove(stale_key);
                    }
                }
                let serialized = runtime_json::stringify(&RuntimeJson::Object(map)).to_node_utf8();
                tokio::fs::write(&file, serialized).await
            },
            LockOptions::default(),
        )
        .await;
    }
}

/// Resolve the default `stdio` catalog cache file under `explicit_dir`,
/// `$KLAUS_CONFIG_DIR`, `fallback_lock_dir`, or `~/.klaus`.
pub fn default_stdio_catalog_cache(
    explicit_dir: Option<&str>,
    fallback_lock_dir: Option<&str>,
) -> StdioCatalogCache {
    let env_dir = std::env::var("KLAUS_CONFIG_DIR")
        .ok()
        .filter(|dir| !dir.trim().is_empty());
    let base = explicit_dir
        .filter(|dir| !dir.is_empty())
        .map(str::to_string)
        .or(env_dir)
        .or_else(|| {
            fallback_lock_dir
                .filter(|dir| !dir.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{}/.klaus", home_dir()));
    StdioCatalogCache::new(format!("{base}/mcp-stdio-catalog-cache.json"))
}

/// Compute a deterministic SHA-256 fingerprint for a `stdio` MCP declaration.
/// Includes any existing local executable or script file size + mtime so local
/// edits or package updates invalidate the cached catalog automatically.
pub fn stdio_catalog_fingerprint(
    config: &McpServerConfig,
    cwd: Option<&RuntimePath>,
) -> Option<String> {
    let McpServerConfig::Stdio {
        name,
        command,
        args,
        env,
        ..
    } = config
    else {
        return None;
    };

    let cwd_utf8 = cwd.and_then(|path: &RuntimePath| path.as_runtime_string().try_to_utf8().ok());
    let mut hasher = Sha256::new();
    hasher.update(b"klaus-mcp-stdio-catalog-v1\0");
    hasher.update(crate::mcp::client::PROTOCOL_VERSION.as_bytes());
    hasher.update(b"\0name:");
    hasher.update(name.as_bytes());
    hasher.update(b"\0cmd:");
    hasher.update(command.as_bytes());
    update_local_file_stamp(&mut hasher, command, cwd_utf8.as_deref());

    hasher.update(b"\0args:");
    if let Some(args) = args {
        for arg in args {
            hasher.update((arg.len() as u64).to_le_bytes());
            hasher.update(arg.as_bytes());
            update_local_file_stamp(&mut hasher, arg, cwd_utf8.as_deref());
        }
    }

    hasher.update(b"\0env:");
    if let Some(env) = env {
        if let Ok(encoded) = runtime_codec::encode_value(env) {
            hasher.update(runtime_json::stringify(&encoded).to_node_utf8());
        }
    }

    hasher.update(b"\0cwd:");
    if let Some(cwd) = cwd {
        hasher.update(cwd.as_runtime_string().to_node_utf8());
    }

    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Some(hex)
}

fn update_local_file_stamp(hasher: &mut Sha256, token: &str, cwd: Option<&str>) {
    if token.is_empty() || token.starts_with('-') {
        return;
    }
    let candidate = Path::new(token);
    let resolved: PathBuf = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else if token.contains('/') || token.contains('\\') {
        match cwd {
            Some(base) if !base.is_empty() => Path::new(base).join(candidate),
            _ => candidate.to_path_buf(),
        }
    } else {
        return;
    };
    let Ok(meta) = std::fs::metadata(&resolved) else {
        return;
    };
    if !meta.is_file() {
        return;
    }
    hasher.update(b"\0file-len:");
    hasher.update(meta.len().to_le_bytes());
    if let Ok(modified) = meta.modified() {
        if let Ok(since_epoch) = modified.duration_since(UNIX_EPOCH) {
            hasher.update(b"\0file-mtime:");
            hasher.update(since_epoch.as_nanos().to_le_bytes());
        }
    }
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
    use crate::mcp::types::McpServerOptions;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir(tag: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "klaus-mcp-stdio-cat-{tag}-{}-{n}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn round_trips_snapshot_and_honors_ttl_and_removal() {
        let dir = temp_dir("rt");
        let cache = StdioCatalogCache::new(
            dir.join("mcp-stdio-catalog-cache.json")
                .to_string_lossy()
                .into_owned(),
        );
        let snapshot = StdioCatalogSnapshot {
            tools: vec![McpToolDescriptor {
                name: "ping".into(),
                title: Some("Ping".into()),
                description: Some("Connectivity check".into()),
                input_schema: Some(
                    runtime_json::parse(&RuntimeString::from(r#"{"type":"object"}"#)).unwrap(),
                ),
                annotations: None,
                meta: None,
            }],
            resources: Vec::new(),
            resource_templates: Vec::new(),
            prompts: Vec::new(),
            instructions: Some("Use ping sparingly.".into()),
            server_info: Some(McpServerInfo {
                name: Some("demo".into()),
                version: Some("1.2.0".into()),
                title: None,
            }),
        };

        cache.put("fp-1", &snapshot, 1_000).await;
        assert_eq!(cache.get_fresh("fp-1", 5_000, 3_000).await, Some(snapshot));
        assert_eq!(cache.get_fresh("fp-1", 5_000, 7_000).await, None);

        cache.remove("fp-1").await;
        assert_eq!(cache.get_fresh("fp-1", 5_000, 2_000).await, None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn fingerprint_changes_when_script_file_or_declaration_changes() {
        let dir = temp_dir("fp");
        let script = dir.join("server.js");
        std::fs::write(&script, "console.log('v1');").unwrap();

        let make_cfg = |arg: &str| McpServerConfig::Stdio {
            name: "local".into(),
            command: "node".into(),
            args: Some(vec![script.to_string_lossy().into_owned(), arg.into()]),
            env: None,
            options: McpServerOptions::default(),
        };
        let cwd = RuntimePath::from(dir.to_string_lossy().into_owned());
        let fp1 = stdio_catalog_fingerprint(&make_cfg("--mode=a"), Some(&cwd)).unwrap();
        let fp2 = stdio_catalog_fingerprint(&make_cfg("--mode=b"), Some(&cwd)).unwrap();
        assert_ne!(fp1, fp2);

        std::fs::write(&script, "console.log('version-two-longer-payload');").unwrap();
        let fp3 = stdio_catalog_fingerprint(&make_cfg("--mode=a"), Some(&cwd)).unwrap();
        assert_ne!(fp1, fp3);

        let _ = std::fs::remove_dir_all(dir);
    }
}
