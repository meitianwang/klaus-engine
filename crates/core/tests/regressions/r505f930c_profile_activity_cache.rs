//! Defect: repeated profile reads replayed every transcript and delayed local usage.
//! Root cause: aggregate reads had no disposable, fingerprinted metrics cache.
//! Work item: profile activity task 505f930c-de04-4c14-a4db-478a90447800.

use std::path::PathBuf;

use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::profile_activity::ProfileActivityDay;
use klaus_agent_core::store::session_store::SessionStore;

struct History(PathBuf);

impl History {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("activity-cache-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn store(&self) -> JsonlSessionStore {
        JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(self.0.to_string_lossy().into_owned()),
            archive_dir: None,
        })
    }
}

impl Drop for History {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[cfg(unix)]
struct RestorePermissions {
    path: PathBuf,
    permissions: std::fs::Permissions,
}

#[cfg(unix)]
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.path, self.permissions.clone()).unwrap();
    }
}

#[tokio::test]
async fn profile_activity_cache_preserves_reordered_usage_and_distinct_utf16_request_ids() {
    let history = History::new();
    let file = history.0.join("history.jsonl");
    let original = br#"{"kind":"session_meta","schemaVersion":2,"meta":{"id":"history","createdAt":1792324800000,"cwd":"/fixture","model":"fixture"}}
{"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture","permissionMode":"default","turnId":"turn","startedAtMs":1792324800000}}
{"kind":"event","event":"turn_lifecycle","turnId":"turn","payload":{"status":"completed","startedAtMs":1792324800000,"finishedAtMs":1792324800500,"durationMs":500}}
{"message":{"usage":{"inputTokens":10,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"content":[{"type":"text","text":"nested \"model\": \"wrong\""}],"model":"fixture","uuid":"reordered","type" : "assistant"},"kind":"message"}
{"usage":{"inputTokens":5,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"requestId":"\ud800","assistantMessageId":"\udfff","model":"fixture","costUsd":0,"kind":"usage_ledger"}
{"kind":"usage_ledger","requestId":"\ud801","assistantMessageId":"next","model":"fixture","usage":{"inputTokens":7,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"costUsd":0}
{"kind":"usage_ledger","requestId":"w:[d800]","assistantMessageId":"scalar","model":"fixture","usage":{"inputTokens":11,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"costUsd":0}
{"kind":"usage_ledger","requestId":"\ud800","assistantMessageId":"duplicate","model":"fixture","usage":{"inputTokens":100,"outputTokens":0,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"costUsd":0}
"#;
    std::fs::write(&file, original).unwrap();
    let now = 1792411200000;
    let store = history.store();
    let first = store.profile_activity(now).await.unwrap();
    assert_eq!(first.total_tokens, 33);
    assert_eq!(first.peak_daily_tokens, 33);
    assert_eq!(first.longest_task_ms, 500);
    assert_eq!(first.longest_streak_days, 1);
    assert_eq!(first.current_streak_days, 1);
    assert_eq!(
        first.days,
        vec![ProfileActivityDay {
            date: "2026-10-18".into(),
            tokens: 33
        }]
    );
    assert!(!first.incomplete, "{first:?}");
    let cache = history.0.join(".profile-activity-cache.json");
    assert!(cache.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _restore = RestorePermissions {
            path: file.clone(),
            permissions: std::fs::metadata(&file).unwrap().permissions(),
        };
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(store.profile_activity(now).await.unwrap(), first);
        assert_eq!(history.store().profile_activity(now).await.unwrap(), first);
    }
    std::fs::remove_file(&cache).unwrap();
    assert_eq!(history.store().profile_activity(now).await.unwrap(), first);
    std::fs::write(&cache, b"{not valid JSON").unwrap();
    assert_eq!(history.store().profile_activity(now).await.unwrap(), first);
    assert_eq!(std::fs::read(&file).unwrap(), original);
    assert!(!history.0.join("history.jsonl.lock").exists());
}
