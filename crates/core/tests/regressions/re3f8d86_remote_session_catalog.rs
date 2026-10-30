//! Defect: every remote catalog page reopened every rollout header and reread 64 KiB.
//! Root cause: forced scans bypassed the header cache and pagination had no snapshot.
//! Fixing commit: e3f8d8612aa38c0217810f219e3b31642f311277.

use base64::Engine;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::rollout::{
    RolloutItem, SessionMeta, SessionRole, ROLLOUT_SCHEMA_VERSION,
};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::{parse_rollout_line, serialize_rollout_item};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore, ThreadPage};
use std::io::Write;
use std::path::{Path, PathBuf};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("klaus-catalog-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn store(dir: &Path) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}

fn write_header(dir: &Path, id: &str, title: &str, time: i64) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let meta = SessionMeta {
        id: id.into(),
        created_at: 1,
        cwd: "/fixture".into(),
        model: "fixture".into(),
        title: Some(title.into()),
        originator: None,
        cli_version: None,
        source: None,
        role: Some(SessionRole::Main),
        parent_thread_id: None,
        forked_from_id: None,
        git: None,
        host_metadata: None,
    };
    let path = dir.join(format!("1-{id}.jsonl"));
    std::fs::write(
        &path,
        serialize_rollout_item(&RolloutItem::SessionMeta {
            schema_version: ROLLOUT_SCHEMA_VERSION,
            meta,
        }) + "\n",
    )
    .unwrap();
    filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(time, 0)).unwrap();
    path
}

fn ids(page: &ThreadPage) -> Vec<String> {
    page.items
        .iter()
        .map(|row| row.id.try_to_utf8().unwrap())
        .collect()
}

#[tokio::test]
async fn remote_catalog_pages_do_no_repeated_io_and_cold_restart_reuses_validated_metadata() {
    let dir = Scratch::new();
    for i in 0..137 {
        let path = write_header(
            dir.path(),
            &format!("s{i:03}"),
            "fixture",
            1_700_000_000 + i,
        );
        if i == 136 {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(64 * 1024 * 1024)
                .unwrap();
        }
    }
    let reader = store(dir.path());
    let mut page = reader
        .list(ListOptions {
            limit: Some(17),
            ..Default::default()
        })
        .await
        .unwrap();
    let first = reader.catalog_io_stats().await;
    assert_eq!(first.directory_scans, 1);
    assert_eq!(first.header_reads, 137);
    assert!(
        first.header_bytes <= 137 * 4096,
        "read-ahead must stop at the first header newline: {first:?}"
    );
    let membership = page.membership_fingerprint.clone();
    let metadata = page.metadata_fingerprint.clone();
    let mut seen = ids(&page);
    while let Some(cursor) = page.next_cursor {
        page = reader
            .list(ListOptions {
                cursor: Some(cursor),
                limit: Some(17),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.membership_fingerprint, membership);
        assert_eq!(page.metadata_fingerprint, metadata);
        seen.extend(ids(&page));
    }
    assert_eq!(seen.len(), 137);
    assert_eq!(
        seen.iter().collect::<std::collections::HashSet<_>>().len(),
        137
    );
    assert_eq!(
        reader.catalog_io_stats().await,
        first,
        "continuation pages may not rescan names, stat files, or reopen headers"
    );
    reader.list(ListOptions::default()).await.unwrap();
    let refreshed = reader.catalog_io_stats().await;
    assert_eq!(refreshed.directory_scans, first.directory_scans + 1);
    assert_eq!(refreshed.metadata_reads, first.metadata_reads + 137);
    assert_eq!(refreshed.header_reads, first.header_reads);
    assert_eq!(refreshed.cache_writes, first.cache_writes);

    let cold = store(dir.path());
    let restarted = cold.list(ListOptions::default()).await.unwrap();
    assert_eq!(restarted.membership_fingerprint, membership);
    assert_eq!(restarted.metadata_fingerprint, metadata);
    assert_eq!(
        cold.catalog_io_stats().await.header_reads,
        0,
        "durable catalog avoids cold header hydration"
    );
    assert_eq!(cold.catalog_io_stats().await.metadata_reads, 137);
}

#[tokio::test]
async fn remote_catalog_snapshot_is_coherent_and_fresh_query_reconciles_external_membership() {
    let dir = Scratch::new();
    for (i, id) in ["a", "b", "c", "d"].into_iter().enumerate() {
        write_header(dir.path(), id, "original", 1_700_000_000 + i as i64);
    }
    let reader = store(dir.path());
    let first = reader
        .list(ListOptions {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&first), ["d", "c"]);
    let fingerprint = first.membership_fingerprint.clone();
    std::fs::remove_file(dir.path().join("1-b.jsonl")).unwrap();
    write_header(dir.path(), "e", "created", 1_700_000_005);
    let replacement = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(replacement.items.len(), 4);
    assert_ne!(
        replacement.membership_fingerprint, fingerprint,
        "delete plus create with equal counts must be detectable"
    );
    let archived = dir.path().join("archived");
    std::fs::create_dir_all(&archived).unwrap();
    std::fs::rename(dir.path().join("1-c.jsonl"), archived.join("1-c.jsonl")).unwrap();
    write_header(dir.path(), "a", "updated", 1_700_000_006);
    let second = reader
        .list(ListOptions {
            cursor: first.next_cursor,
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&second), ["b", "a"]);
    assert_eq!(second.items[1].title, Some(RuntimeString::from("original")));
    assert_eq!(second.membership_fingerprint, fingerprint);
    let fresh = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(ids(&fresh), ["a", "e", "d"]);
    let current = fresh.membership_fingerprint;
    write_header(dir.path(), "a", "renamed", 1_700_000_007);
    assert_eq!(
        reader
            .list(ListOptions::default())
            .await
            .unwrap()
            .membership_fingerprint,
        current,
        "content-only updates retain membership identity"
    );
    std::fs::rename(archived.join("1-c.jsonl"), dir.path().join("1-c.jsonl")).unwrap();
    assert!(ids(&reader.list(ListOptions::default()).await.unwrap()).contains(&"c".into()));
}

#[tokio::test]
async fn remote_catalog_summary_lookup_is_read_only_before_any_catalog_listing() {
    let dir = Scratch::new();
    let active = write_header(dir.path(), "active", "active", 1_700_000_000);
    let archived_dir = dir.path().join("archived");
    let archived = write_header(&archived_dir, "archived", "archived", 1_700_000_001);
    let active_bytes = std::fs::read(&active).unwrap();
    let archived_bytes = std::fs::read(&archived).unwrap();
    let reader = store(dir.path());
    assert_eq!(
        reader
            .read_session_summary("archived")
            .await
            .unwrap()
            .unwrap()
            .archived,
        Some(true)
    );
    assert!(reader
        .read_session_summary("absent")
        .await
        .unwrap()
        .is_none());
    assert_eq!(reader.catalog_io_stats().await.cache_writes, 0);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    assert_eq!(std::fs::read_dir(&archived_dir).unwrap().count(), 1);
    assert_eq!(std::fs::read(active).unwrap(), active_bytes);
    assert_eq!(std::fs::read(archived).unwrap(), archived_bytes);
}

#[tokio::test]
async fn remote_catalog_fingerprints_follow_eligible_archive_role_and_workspace_membership() {
    let dir = Scratch::new();
    write_header(dir.path(), "main", "main", 1_700_000_000);
    write_header(&dir.path().join("archived"), "old", "old", 1_700_000_001);
    let side_path = write_header(dir.path(), "side", "side", 1_700_000_002);
    let RolloutItem::SessionMeta {
        mut meta,
        schema_version,
    } = parse_rollout_line(std::fs::read_to_string(&side_path).unwrap().trim()).unwrap()
    else {
        panic!("fixture header")
    };
    meta.role = Some(SessionRole::SideTask);
    meta.cwd = "/other".into();
    std::fs::write(
        side_path,
        serialize_rollout_item(&RolloutItem::SessionMeta {
            schema_version,
            meta,
        }) + "\n",
    )
    .unwrap();
    let reader = store(dir.path());
    let main = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(ids(&main), ["main"]);
    let all_main = reader
        .list(ListOptions {
            include_archived: Some(true),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&all_main), ["old"]);
    assert_ne!(main.membership_fingerprint, all_main.membership_fingerprint);
    assert_ne!(main.metadata_fingerprint, all_main.metadata_fingerprint);
    let next = reader
        .list(ListOptions {
            include_archived: Some(true),
            cursor: all_main.next_cursor,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&next), ["main"]);
    assert_eq!(all_main.membership_fingerprint, next.membership_fingerprint);
    assert_eq!(all_main.metadata_fingerprint, next.metadata_fingerprint);
    let side = reader
        .list(ListOptions {
            role: Some(SessionRole::SideTask),
            cwd: Some("/other".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&side), ["side"]);
    assert_ne!(side.membership_fingerprint, main.membership_fingerprint);
    let outside = reader
        .list(ListOptions {
            cwd: Some("/other".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(outside.items.is_empty());
    assert_ne!(outside.membership_fingerprint, main.membership_fingerprint);
}

#[tokio::test]
async fn remote_catalog_target_read_is_metadata_only_and_revalidates_identity_and_archive_moves() {
    let dir = Scratch::new();
    let path = write_header(dir.path(), "target", "old", 1_700_000_000);
    let mut body = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    body.write_all(b"this transcript is deliberately invalid\n")
        .unwrap();
    body.set_len(64 * 1024 * 1024).unwrap();
    drop(body);
    let reader = store(dir.path());
    reader.list(ListOptions::default()).await.unwrap();
    let before = reader.catalog_io_stats().await;
    assert_eq!(
        reader
            .read_session_summary("target")
            .await
            .unwrap()
            .unwrap()
            .title,
        Some("old".into())
    );
    let after = reader.catalog_io_stats().await;
    assert_eq!(after.directory_scans, before.directory_scans);
    assert_eq!(after.header_reads, before.header_reads);
    assert_eq!(after.metadata_reads, before.metadata_reads + 1);
    // A replacement updates metadata without ever parsing the old invalid body.
    let replacement = write_header(dir.path(), "replacement", "new", 1_700_000_000);
    let encoded = std::fs::read_to_string(&replacement)
        .unwrap()
        .replace("replacement", "target");
    std::fs::write(&replacement, encoded).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(1_700_000_000, 0)).unwrap();
    assert_eq!(
        reader
            .read_session_summary("target")
            .await
            .unwrap()
            .unwrap()
            .title,
        Some("new".into())
    );
    let archive = dir.path().join("archived");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::rename(&path, archive.join("1-target.jsonl")).unwrap();
    assert_eq!(
        reader
            .read_session_summary("target")
            .await
            .unwrap()
            .unwrap()
            .archived,
        Some(true)
    );
    reader
        .list(ListOptions {
            include_archived: Some(true),
            ..Default::default()
        })
        .await
        .unwrap();
    let cold = store(dir.path());
    assert_eq!(
        cold.read_session_summary("target")
            .await
            .unwrap()
            .unwrap()
            .archived,
        Some(true)
    );
    assert_eq!(cold.catalog_io_stats().await.directory_scans, 0);
    assert_eq!(cold.catalog_io_stats().await.header_reads, 0);
    std::fs::rename(archive.join("1-target.jsonl"), &path).unwrap();
    assert_eq!(
        reader
            .read_session_summary("target")
            .await
            .unwrap()
            .unwrap()
            .archived,
        Some(false)
    );
    std::fs::remove_file(&path).unwrap();
    assert!(reader
        .read_session_summary("target")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn remote_catalog_failed_scan_does_not_publish_or_persist_partial_namespaces() {
    let dir = Scratch::new();
    write_header(dir.path(), "active", "original", 1_700_000_000);
    let archived = dir.path().join("archived");
    write_header(&archived, "saved", "original", 1_700_000_001);
    let reader = store(dir.path());
    let options = ListOptions {
        include_archived: Some(true),
        ..Default::default()
    };
    reader.list(options.clone()).await.unwrap();
    let cache = dir.path().join(".session-header-index-v1.json");
    let before = std::fs::read(&cache).unwrap();
    write_header(dir.path(), "active", "changed", 1_700_000_003);
    std::fs::create_dir(archived.join("broken.jsonl")).unwrap();
    assert!(reader.list(options.clone()).await.is_err());
    assert_eq!(
        std::fs::read(&cache).unwrap(),
        before,
        "failed archived scan must not commit active cache"
    );
    std::fs::remove_dir(archived.join("broken.jsonl")).unwrap();
    let page = reader.list(options).await.unwrap();
    assert_eq!(page.items[0].title, Some("changed".into()));
    assert_ne!(std::fs::read(cache).unwrap(), before);
}

#[tokio::test]
async fn remote_catalog_revalidates_equal_length_equal_mtime_replacements_and_corrupt_cache() {
    let dir = Scratch::new();
    let path = write_header(dir.path(), "same", "first", 1_700_000_000);
    let reader = store(dir.path());
    reader.list(ListOptions::default()).await.unwrap();
    let original = std::fs::read(&path).unwrap();
    let replacement = dir.path().join("replacement.tmp");
    let new = String::from_utf8(original.clone())
        .unwrap()
        .replace("first", "other");
    assert_eq!(new.len(), original.len());
    std::fs::write(&replacement, new).unwrap();
    filetime::set_file_mtime(
        &replacement,
        filetime::FileTime::from_unix_time(1_700_000_000, 0),
    )
    .unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    let first = reader.catalog_io_stats().await;
    assert_eq!(
        reader
            .read_session_summary("same")
            .await
            .unwrap()
            .unwrap()
            .title,
        Some("other".into())
    );
    assert_eq!(
        reader.catalog_io_stats().await.header_reads,
        first.header_reads + 1,
        "file identity must invalidate equal-sized equal-time replacement"
    );
    reader.list(ListOptions::default()).await.unwrap();
    let cold = store(dir.path());
    assert_eq!(
        cold.list(ListOptions::default()).await.unwrap().items[0].title,
        Some("other".into())
    );
    assert_eq!(
        cold.catalog_io_stats().await.header_reads,
        0,
        "target refresh must become durable at full reconciliation"
    );
    std::fs::write(
        dir.path().join(".session-header-index-v1.json"),
        "broken derived cache",
    )
    .unwrap();
    let rebuilt = store(dir.path());
    assert_eq!(
        rebuilt.list(ListOptions::default()).await.unwrap().items[0].title,
        Some("other".into())
    );
    assert_eq!(rebuilt.catalog_io_stats().await.header_reads, 1);
}

#[tokio::test]
async fn remote_catalog_metadata_fingerprint_tracks_complete_headers_not_transcript_or_stat_changes(
) {
    let dir = Scratch::new();
    let path = write_header(dir.path(), "a", "original", 1_700_000_000);
    write_header(dir.path(), "b", "second", 1_700_000_001);
    let reader = store(dir.path());
    let initial = reader
        .list(ListOptions {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let membership = initial.membership_fingerprint.clone();
    let metadata = initial.metadata_fingerprint.clone();
    assert!(metadata.is_some());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"arbitrary transcript bytes\n")
        .unwrap();
    let after_append = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(
        ids(&after_append)[0],
        "a",
        "body append changes recency ordering"
    );
    assert_eq!(after_append.membership_fingerprint, membership);
    assert_eq!(
        after_append.metadata_fingerprint, metadata,
        "body, size, stat identity and recency must not force metadata scans"
    );
    let cold = store(dir.path());
    assert_eq!(
        cold.list(ListOptions::default())
            .await
            .unwrap()
            .metadata_fingerprint,
        metadata
    );
    assert_eq!(cold.catalog_io_stats().await.header_reads, 0);

    let raw = std::fs::read_to_string(&path).unwrap();
    let RolloutItem::SessionMeta {
        mut meta,
        schema_version,
    } = parse_rollout_line(raw.lines().next().unwrap()).unwrap()
    else {
        panic!("fixture header")
    };
    let modified =
        filetime::FileTime::from_last_modification_time(&std::fs::metadata(&path).unwrap());
    let replace = |meta: &SessionMeta| {
        std::fs::write(
            &path,
            serialize_rollout_item(&RolloutItem::SessionMeta {
                schema_version,
                meta: meta.clone(),
            }) + "\n",
        )
        .unwrap();
        filetime::set_file_mtime(&path, modified).unwrap();
    };
    meta.host_metadata = Some(
        runtime_json::parse(&RuntimeString::from(
            r#"{"z":"\ud800","a":{"two":2,"one":1}}"#,
        ))
        .unwrap(),
    );
    replace(&meta);
    let attachment = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(attachment.membership_fingerprint, membership);
    assert_ne!(
        attachment.metadata_fingerprint, metadata,
        "external same-mtime metadata edit must trigger reconciliation"
    );
    let cursor_page = reader
        .list(ListOptions {
            cursor: initial.next_cursor,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        cursor_page.metadata_fingerprint, metadata,
        "an in-progress snapshot keeps its original metadata identity"
    );
    meta.host_metadata = Some(
        runtime_json::parse(&RuntimeString::from(
            r#"{"a":{"one":1,"two":2},"z":"\ud800"}"#,
        ))
        .unwrap(),
    );
    replace(&meta);
    let reordered = reader.list(ListOptions::default()).await.unwrap();
    assert_eq!(
        reordered.metadata_fingerprint, attachment.metadata_fingerprint,
        "recursive key order must not affect deterministic exact-runtime hashes"
    );
    meta.originator = Some("external-origin".into());
    replace(&meta);
    let complete = reader.list(ListOptions::default()).await.unwrap();
    assert_ne!(
        complete.metadata_fingerprint, reordered.metadata_fingerprint,
        "hash complete canonical metadata, including fields omitted from lightweight summaries"
    );
    meta.title = Some("renamed".into());
    meta.cwd = "/other-workspace".into();
    replace(&meta);
    let renamed = reader.list(ListOptions::default()).await.unwrap();
    assert_ne!(renamed.metadata_fingerprint, complete.metadata_fingerprint);
    assert_eq!(
        renamed
            .items
            .iter()
            .find(|row| row.id == "a")
            .unwrap()
            .updated_at,
        attachment.items[0].updated_at
    );
    assert_eq!(
        store(dir.path())
            .list(ListOptions::default())
            .await
            .unwrap()
            .metadata_fingerprint,
        renamed.metadata_fingerprint
    );
}

#[tokio::test]
async fn remote_catalog_cursors_reject_expired_or_mismatched_snapshots_and_upgrade_legacy() {
    let dir = Scratch::new();
    for (i, id) in ["a", "b", "c"].into_iter().enumerate() {
        write_header(dir.path(), id, "fixture", 1_700_000_000 + i as i64);
    }
    let reader = store(dir.path());
    let first = reader
        .list(ListOptions {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let cursor = first.next_cursor.unwrap();
    let mismatch = reader
        .list(ListOptions {
            cursor: Some(cursor.clone()),
            include_archived: Some(true),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(mismatch.kind(), std::io::ErrorKind::InvalidInput);
    let stale = store(dir.path())
        .list(ListOptions {
            cursor: Some(cursor.clone()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(stale.to_string().contains("cursor expired"));
    for _ in 0..16 {
        reader
            .list(ListOptions {
                limit: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    assert!(reader
        .list(ListOptions {
            cursor: Some(cursor),
            ..Default::default()
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("cursor expired"));
    let legacy = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(r#"{"updatedAt":1700000002000,"id":"c"}"#);
    let page = reader
        .list(ListOptions {
            cursor: Some(legacy.into()),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(ids(&page), ["b"]);
    assert!(page.next_cursor.is_some());
    assert!(reader
        .list(ListOptions {
            cursor: Some("garbage".into()),
            ..Default::default()
        })
        .await
        .is_err());
}
