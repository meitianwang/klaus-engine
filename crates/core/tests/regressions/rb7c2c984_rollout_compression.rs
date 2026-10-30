//! Broke: cold history occupied unbounded plain storage and compressed sources
//! were invisible to readers, continuation writers, and lifecycle operations.
//! Root cause: discovery and file access assumed one physical JSONL representation.
//! Native work item: b7c2c984-235c-4169-a129-beba4ca4ef77.

use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, UserMessage};
use klaus_agent_core::schemas::rollout::{SessionMeta, TurnContextSnapshot};
use klaus_agent_core::store::compression::{compressed_path, plain_path};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::search::SessionSearchOptions;
use klaus_agent_core::store::session_store::{
    ListOptions, LiveRollout, RolloutMigrationMode, RolloutMigrationRequest, SessionStore,
    SubAgentMeta, UnarchiveSessionStatus,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn meta(id: &str, created: i64) -> SessionMeta {
    SessionMeta {
        id: id.into(),
        created_at: created,
        cwd: "/tmp".into(),
        model: "compression-fixture".into(),
        title: Some(id.into()),
        originator: None,
        cli_version: None,
        source: None,
        role: None,
        parent_thread_id: None,
        forked_from_id: None,
        git: None,
        host_metadata: None,
    }
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}
fn user(id: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: id.into(),
        content: vec![text("durable compression search needle")],
        is_meta: None,
    })
}
fn assistant(id: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content: vec![text("complete original assistant")],
        model: "compression-fixture".into(),
        usage: None,
    })
}
fn context(id: &str) -> TurnContextSnapshot {
    TurnContextSnapshot {
        cwd: "/tmp".into(),
        model: "compression-fixture".into(),
        permission_mode: ExternalPermissionMode::Default,
        turn_id: Some(id.into()),
        started_at_ms: Some(1),
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        network_allowed: None,
        writable_roots: None,
    }
}
fn store(path: &Path) -> Arc<JsonlSessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(path.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}
fn cold(path: &Path) {
    filetime::set_file_mtime(
        path,
        filetime::FileTime::from_system_time(
            SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60),
        ),
    )
    .unwrap();
}
async fn closed(store: &JsonlSessionStore, root: &Path, id: &str, created: i64) -> PathBuf {
    let rollout = store.create(meta(id, created)).await.unwrap();
    for number in 0..3 {
        rollout
            .append_turn_context(context(&format!("{id}-turn-{number}")))
            .await;
        rollout
            .append_message(user(&format!("{id}-user-{number}")))
            .await;
        rollout
            .append_message(assistant(&format!("{id}-assistant-{number}")))
            .await;
    }
    rollout.shutdown().await.unwrap();
    root.join(format!("{created}-{id}.jsonl"))
}
async fn child(store: &JsonlSessionStore, id: &str) -> Box<dyn LiveRollout> {
    store
        .open_sub_agent(
            id,
            "child",
            SubAgentMeta {
                agent_type: None,
                model: "compression-fixture".into(),
                permission_mode: None,
                effort: None,
                created_at: 1,
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn child_ids_preserve_namespace_like_prefixes_and_extensions_across_compression() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    closed(&store, root.path(), "main", 1).await;
    let ids = ["A", "agent-A", "agent-agent-A"];
    let mut sources = Vec::new();
    for id in ids {
        let writer = store
            .open_sub_agent(
                "main",
                id,
                SubAgentMeta {
                    agent_type: None,
                    model: "compression-fixture".into(),
                    permission_mode: None,
                    effort: None,
                    created_at: 1,
                },
            )
            .await
            .unwrap();
        writer.append_message(user(id)).await;
        writer.shutdown().await.unwrap();
        let path = root
            .path()
            .join(format!("1-main/subagents/agent-{id}.jsonl"));
        cold(&path);
        sources.push((path.clone(), std::fs::read(path).unwrap()));
    }
    // Historical filenames may contain extensions that current writer IDs sanitize.
    let extension_path = root.path().join("1-main/subagents/agent-x.jsonl.jsonl");
    std::fs::write(&extension_path, &sources[0].1).unwrap();
    cold(&extension_path);
    sources.push((extension_path, sources[0].1.clone()));
    let mut expected = ids.map(str::to_owned).to_vec();
    expected.push("x.jsonl".to_owned());
    expected.sort();
    for representation in ["plain", "compressed", "coexisting"] {
        if representation == "compressed" {
            let report = store.compress_cold_rollouts().await.unwrap();
            assert_eq!(report.failed, 0);
            assert_eq!(report.compressed, sources.len() as u64);
        } else if representation == "coexisting" {
            for (path, contents) in &sources {
                std::fs::write(path, contents).unwrap();
                assert!(compressed_path(path).exists());
            }
        }
        let fresh = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.path().to_string_lossy().into_owned()),
            archive_dir: None,
        });
        let mut actual = fresh.list_sub_agents("main").await.unwrap();
        actual.sort();
        assert_eq!(actual, expected, "{representation}");
        for id in ids {
            assert_eq!(
                fresh.load_sub_agent("main", id).await.unwrap().messages,
                vec![user(id)],
                "{representation}: {id}"
            );
        }
    }
}

#[tokio::test]
async fn cold_main_archived_and_child_sources_are_lossless_and_reads_stay_noninvasive() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let main = closed(&store, root.path(), "main", 1).await;
    let _archived = closed(&store, root.path(), "archived", 2).await;
    let archived = PathBuf::from(store.archive("archived").await.unwrap());
    let hot = closed(&store, root.path(), "hot", 3).await;
    let writer = store.create(meta("active", 4)).await.unwrap();
    let active = root.path().join("4-active.jsonl");
    cold(&active);
    let child_writer = child(&store, "main").await;
    child_writer.append_message(user("child-user")).await;
    child_writer.shutdown().await.unwrap();
    let child_path = root.path().join("1-main/subagents/agent-child.jsonl");
    for path in [&main, &archived, &child_path] {
        cold(path);
    }
    let originals = std::fs::read(&main).unwrap();
    let mtime = std::fs::metadata(&main).unwrap().modified().unwrap();
    let snapshot = store.read_thread("main").await.unwrap();
    let report = store.compress_cold_rollouts().await.unwrap();
    assert_eq!(report.compressed, 3);
    assert_eq!(report.failed, 0);
    assert!(report.skipped_hot >= 1);
    assert!(report.skipped_active >= 1);
    for path in [&main, &archived, &child_path] {
        assert!(!path.exists());
        assert!(compressed_path(path).exists());
    }
    assert!(hot.exists());
    assert!(active.exists());
    assert_eq!(
        std::fs::metadata(compressed_path(&main))
            .unwrap()
            .modified()
            .unwrap(),
        mtime
    );
    let mut decoded = klaus_agent_core::store::compression::open_seekable(&main).unwrap();
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut decoded, &mut bytes).unwrap();
    assert_eq!(bytes, originals);
    assert_eq!(
        store.read_thread("main").await.unwrap().raw_items,
        snapshot.raw_items
    );
    assert_eq!(
        store.resume("main").await.unwrap().raw_items,
        snapshot.raw_items
    );
    assert_eq!(
        store
            .load_sub_agent("main", "child")
            .await
            .unwrap()
            .messages,
        vec![user("child-user")]
    );
    assert!(store
        .list(ListOptions {
            include_archived: Some(true),
            ..Default::default()
        })
        .await
        .unwrap()
        .items
        .iter()
        .any(|entry| entry.id == "main"));
    let hits = store
        .search(
            &SessionSearchOptions {
                query: "compression search needle".into(),
                limit: Some(20),
                include_archived: Some(true),
            },
            None,
            &AbortSignal::new(),
        )
        .await
        .unwrap();
    assert!(hits.items.iter().any(|hit| hit.thread.id == "main"));
    assert!(
        !main.exists(),
        "ordinary reads never restore a writable source"
    );
    assert_eq!(
        std::fs::metadata(compressed_path(&main))
            .unwrap()
            .modified()
            .unwrap(),
        mtime
    );
    assert_eq!(store.compress_cold_rollouts().await.unwrap().compressed, 0);
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_compression_transition_preserves_both_turn_and_message_cursors_and_reopen_appends() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let source = closed(&store, root.path(), "main", 1).await;
    let page = store
        .paged_thread_history("main", None, Some(1), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let cursor = page.page.next_cursor.unwrap();
    let turn_page = store
        .paged_turn_slice("main", "main-turn-0", None, Some(1), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let message_cursor = turn_page.page.next_cursor.unwrap();
    let next_before = store
        .paged_thread_history("main", Some(&cursor), Some(1), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let slice_before = store
        .paged_turn_slice(
            "main",
            "main-turn-0",
            Some(&message_cursor),
            Some(1),
            Some("asc"),
        )
        .await
        .unwrap()
        .unwrap();
    cold(&source);
    assert_eq!(store.compress_cold_rollouts().await.unwrap().compressed, 1);
    let next = store
        .paged_thread_history("main", Some(&cursor), Some(1), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let slice = store
        .paged_turn_slice(
            "main",
            "main-turn-0",
            Some(&message_cursor),
            Some(1),
            Some("asc"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.history, next_before.history);
    assert_eq!(slice.history, slice_before.history);
    assert!(!source.exists());
    let resumed = store.resume_runtime_for_write("main").await.unwrap();
    assert!(source.exists());
    assert!(!compressed_path(&source).exists());
    resumed.rollout.append_message(user("after-resume")).await;
    resumed.rollout.shutdown().await.unwrap();
    assert!(store.read_thread("main").await.unwrap().raw_items.iter().any(|item|matches!(item,klaus_agent_core::schemas::rollout::RolloutItem::Message{message} if message==&user("after-resume"))));
}

#[tokio::test]
async fn compressed_lifecycle_operations_do_not_leave_a_shadow_that_resurrects_deleted_history() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let source = closed(&store, root.path(), "main", 1).await;
    cold(&source);
    store.compress_cold_rollouts().await.unwrap();
    let archived = PathBuf::from(store.archive("main").await.unwrap());
    assert!(!compressed_path(&source).exists());
    cold(&archived);
    store.compress_cold_rollouts().await.unwrap();
    assert_eq!(
        store.unarchive("main").await.status,
        UnarchiveSessionStatus::Restored
    );
    assert!(!compressed_path(&archived).exists());
    cold(&source);
    store.compress_cold_rollouts().await.unwrap();
    store.rename("main", "renamed").await.unwrap();
    assert!(!compressed_path(&source).exists());
    let renamed = root.path().join("1-renamed.jsonl");
    assert_eq!(
        store.read_thread("renamed").await.unwrap().meta.id,
        "renamed"
    );
    cold(&renamed);
    store.compress_cold_rollouts().await.unwrap();
    store.delete("renamed").await.unwrap();
    assert!(!renamed.exists());
    assert!(!compressed_path(&renamed).exists());
    assert!(store
        .list(ListOptions {
            include_archived: Some(true),
            ..Default::default()
        })
        .await
        .unwrap()
        .items
        .is_empty());
}

#[tokio::test]
async fn an_active_child_holds_its_own_writer_lease_and_cold_maintenance_never_rewrites_it() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let _source = closed(&store, root.path(), "main", 1).await;
    let writer = child(&store, "main").await;
    writer.append_message(user("first")).await;
    writer.flush().await.unwrap();
    let source = root.path().join("1-main/subagents/agent-child.jsonl");
    cold(&source);
    let before = std::fs::read(&source).unwrap();
    let report = store.compress_cold_rollouts().await.unwrap();
    assert!(report.skipped_active >= 1);
    assert_eq!(std::fs::read(&source).unwrap(), before);
    assert!(!compressed_path(&source).exists());
    assert_eq!(
        store.archive("main").await.unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    writer.append_message(user("second")).await;
    writer.shutdown().await.unwrap();
    cold(&source);
    assert_eq!(store.compress_cold_rollouts().await.unwrap().compressed, 1);
    let reopened = child(&store, "main").await;
    reopened.append_message(user("third")).await;
    reopened.shutdown().await.unwrap();
    assert_eq!(
        store
            .load_sub_agent("main", "child")
            .await
            .unwrap()
            .messages,
        vec![user("first"), user("second"), user("third")]
    );
}

#[tokio::test]
async fn corrupt_sources_failed_publication_and_external_cancellation_preserve_originals() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let bad = root.path().join("1-invalid.jsonl");
    std::fs::write(&bad, b"{broken-json\n").unwrap();
    cold(&bad);
    let good = closed(&store, root.path(), "good", 2).await;
    cold(&good);
    let destination = compressed_path(&good);
    std::fs::create_dir(&destination).unwrap();
    let bytes = std::fs::read(&good).unwrap();
    let report = store.compress_cold_rollouts().await.unwrap();
    assert_eq!(report.failed, 2);
    assert_eq!(std::fs::read(&bad).unwrap(), b"{broken-json\n");
    assert_eq!(std::fs::read(&good).unwrap(), bytes);
    std::fs::remove_dir(&destination).unwrap();
    std::fs::write(&destination, b"not-zstd").unwrap();
    // A valid plain source wins over a corrupt compressed shadow.
    assert_eq!(store.read_thread("good").await.unwrap().meta.id, "good");
    std::fs::remove_file(&good).unwrap();
    assert!(store.read_thread("good").await.is_err());
    assert!(store.reopen("good").await.is_err());
    assert!(!good.exists());
    assert!(!root.path().join("2-good.jsonl.lock").exists());
    assert!(!std::fs::read_dir(root.path()).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".rollout-decode-")));
    assert_eq!(std::fs::read(&destination).unwrap(), b"not-zstd");
    let flag = Arc::new(AtomicBool::new(true));
    assert_eq!(
        store
            .compress_cold_rollouts_with_cancellation(flag.clone())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Interrupted
    );
    assert!(flag.load(Ordering::Acquire));
    assert!(!root.path().join(".compression-run.json").exists());
    assert!(!std::fs::read_dir(root.path()).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".rollout-compress-")));
}

#[tokio::test]
async fn recent_run_markers_block_overlap_and_expired_markers_do_not_hide_cold_work() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let source = closed(&store, root.path(), "main", 1).await;
    cold(&source);
    let marker = root.path().join(".compression-run.json");
    std::fs::write(&marker, b"{}").unwrap();
    assert!(store.compress_cold_rollouts().await.unwrap().busy);
    assert!(source.exists());
    filetime::set_file_mtime(
        &marker,
        filetime::FileTime::from_system_time(SystemTime::now() - Duration::from_secs(7 * 60 * 60)),
    )
    .unwrap();
    assert_eq!(store.compress_cold_rollouts().await.unwrap().compressed, 1);
    assert!(!marker.exists());
}

#[tokio::test]
async fn migration_rejects_old_turn_and_message_cursors_while_new_cursors_carry_the_epoch() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let source = closed(&store, root.path(), "main", 1).await;
    let old = store
        .paged_thread_history("main", None, Some(1), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap()
        .page
        .next_cursor
        .unwrap();
    let old_message = store
        .paged_turn_slice("main", "main-turn-0", None, Some(1), Some("asc"))
        .await
        .unwrap()
        .unwrap()
        .page
        .next_cursor
        .unwrap();
    let request = RolloutMigrationRequest {
        mode: RolloutMigrationMode::Apply,
        expected_source_revisions: None,
    };
    let report = store
        .migrate_rollouts(request, Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    assert!(!report.busy);
    assert!(report.files.iter().any(|value| value
        .get("status")
        .and_then(|value| value.as_string())
        .is_some_and(|status| status == "applied")));
    assert!(store
        .paged_thread_history("main", Some(&old), Some(1), Some("full"), Some("asc"))
        .await
        .is_err());
    assert!(store
        .paged_turn_slice(
            "main",
            "main-turn-0",
            Some(&old_message),
            Some(1),
            Some("asc")
        )
        .await
        .is_err());
    let page = store
        .paged_thread_history("main", None, Some(1), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    assert!(page
        .page
        .next_cursor
        .as_deref()
        .unwrap()
        .starts_with("migration:"));
    let next = store
        .paged_thread_history(
            "main",
            page.page.next_cursor.as_deref(),
            Some(1),
            Some("full"),
            Some("asc"),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(next
        .page
        .backwards_cursor
        .as_deref()
        .unwrap()
        .starts_with("migration:"));
    let slice = store
        .paged_turn_slice("main", "main-turn-0", None, Some(1), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    assert!(slice
        .page
        .next_cursor
        .as_deref()
        .unwrap()
        .starts_with("migration:"));
    let continued_slice = store
        .paged_turn_slice(
            "main",
            "main-turn-0",
            slice.page.next_cursor.as_deref(),
            Some(1),
            Some("asc"),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(continued_slice
        .page
        .backwards_cursor
        .as_deref()
        .unwrap()
        .starts_with("migration:"));
    cold(&source);
    store.compress_cold_rollouts().await.unwrap();
    assert!(store
        .paged_thread_history(
            "main",
            page.page.next_cursor.as_deref(),
            Some(1),
            Some("full"),
            Some("asc")
        )
        .await
        .is_ok());
    assert_eq!(plain_path(&compressed_path(&source)), source);
    // The report remains usable as the exact preview/apply revision payload.
    runtime_codec::encode_value(&report).unwrap();
    assert_ne!(
        RuntimeString::from(old),
        RuntimeString::from(page.page.next_cursor.unwrap())
    );
}

#[tokio::test]
async fn completing_maintenance_does_not_cancel_its_callers_shared_shutdown_signal() {
    let root = super::context_fixture::tempdir().unwrap();
    let store = store(root.path());
    let flag = Arc::new(AtomicBool::new(false));
    assert_eq!(
        store
            .compress_cold_rollouts_with_cancellation(flag.clone())
            .await
            .unwrap()
            .compressed,
        0
    );
    assert!(!flag.load(Ordering::Acquire));
    let report = store
        .migrate_rollouts(RolloutMigrationRequest::default(), flag.clone())
        .await
        .unwrap();
    assert!(!report.busy);
    assert!(!flag.load(Ordering::Acquire));
}

#[tokio::test]
async fn child_migration_uses_real_writer_leases_and_preserves_both_complete_and_incomplete_contexts(
) {
    use klaus_agent_core::engine::approval_capture::ReviewParentHistory;
    use klaus_agent_core::store::session_store::{AppendEventOptions, CompactionRecord};
    for complete in [false, true] {
        let root = super::context_fixture::tempdir().unwrap();
        let store = store(root.path());
        closed(&store, root.path(), "main", 1).await;
        let writer = child(&store, "main").await;
        writer.append_message(user("original-child-user")).await;
        writer
            .append_message(assistant("original-child-assistant"))
            .await;
        let checkpoint = ReviewParentHistory::default()
            .checkpoint()
            .to_runtime_value()
            .unwrap();
        for _ in 0..3 {
            writer
                .append_event(
                    "approval_parent_history".into(),
                    Some(AppendEventOptions {
                        turn_id: None,
                        payload: Some(checkpoint.clone()),
                    }),
                )
                .await;
        }
        writer
            .append_compaction(CompactionRecord {
                summary: "retained child context".into(),
                replaced_range: (0, 2),
                replacement_history: Some(vec![user("retained-child-user")]),
                source_message_ids: Some(vec![
                    "original-child-user".into(),
                    "original-child-assistant".into(),
                ]),
                approval_parent_history: complete.then_some(checkpoint),
            })
            .await;
        writer.flush().await.unwrap();
        let active = store
            .migrate_rollouts(
                RolloutMigrationRequest {
                    mode: RolloutMigrationMode::Apply,
                    expected_source_revisions: None,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
        assert!(active.files.iter().any(|row| row
            .get("path")
            .and_then(|value| value.as_string())
            .is_some_and(|path| path.contains("agent-child.jsonl"))
            && row
                .get("status")
                .and_then(|value| value.as_string())
                .is_some_and(|status| status == "conflict")));
        writer.shutdown().await.unwrap();
        let before = store.load_sub_agent("main", "child").await.unwrap();
        let result = store
            .migrate_rollouts(
                RolloutMigrationRequest {
                    mode: RolloutMigrationMode::Apply,
                    expected_source_revisions: None,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
        assert!(result.files.iter().any(|row| row
            .get("path")
            .and_then(|value| value.as_string())
            .is_some_and(|path| path.contains("agent-child.jsonl"))
            && row
                .get("status")
                .and_then(|value| value.as_string())
                .is_some_and(|status| status == "applied")));
        assert_eq!(
            store
                .load_sub_agent("main", "child")
                .await
                .unwrap()
                .messages,
            before.messages
        );
        assert!(!root
            .path()
            .join("1-main/subagents/agent-child.jsonl.migration.pending")
            .exists());
        let again = store
            .migrate_rollouts(
                RolloutMigrationRequest {
                    mode: RolloutMigrationMode::Apply,
                    expected_source_revisions: None,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
        assert!(again.files.iter().all(|row| row
            .get("status")
            .and_then(|value| value.as_string())
            .is_some_and(|status| status == "unchanged")));
    }
}
