//! Defect: obsolete approval checkpoints and revoked branches remained on disk.
//! Root cause: continuation recovery optimized reads without an explicit guarded rewrite.
//! Native work item: b7c2c984-235c-4169-a129-beba4ca4ef77.

use klaus_agent_core::engine::approval_capture::ReviewParentHistory;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::rollout_migration::{
    acknowledge, apply, pending, preview, recover, MigrationKind, MigrationStatus, RecoveryState,
};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use serde_json::{json, Value};
use std::fs::{File, FileTimes};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

struct Fixture {
    directory: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new(rows: &[Value]) -> Self {
        let directory =
            std::env::temp_dir().join(format!("klaus-rollout-migration-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("1700000000000-migration.jsonl");
        write_rows(&path, rows);
        Self { directory, path }
    }
    fn bytes(&self) -> Vec<u8> {
        std::fs::read(&self.path).unwrap()
    }
    fn files(&self) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(&self.directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn write_rows(path: &Path, rows: &[Value]) {
    std::fs::write(
        path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}
fn header() -> Value {
    json!({"kind":"session_meta","schemaVersion":3,"meta":{
        "id":"migration","createdAt":1700000000000_i64,"cwd":"/tmp","model":"fixture","title":"Original title",
        "hostMetadata":{"retained":"opaque"}
    }})
}
fn context(turn: &str) -> Value {
    json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"fixture","turnId":turn}})
}
fn user(id: &str, text: &str) -> Value {
    json!({"kind":"message","message":{"type":"user","uuid":id,"content":[{"type":"text","text":text}]}})
}
fn assistant(id: &str, text: &str) -> Value {
    json!({"kind":"message","message":{"type":"assistant","uuid":id,"model":"fixture","content":[{"type":"text","text":text}]}})
}
fn message(row: &Value) -> Message {
    match parse_rollout_line(&row.to_string()).unwrap() {
        RolloutItem::Message { message } => message,
        _ => panic!("message fixture"),
    }
}
fn checkpoint(history: &ReviewParentHistory) -> Value {
    let payload = history.checkpoint().to_runtime_value().unwrap();
    let payload: Value =
        serde_json::from_str(&runtime_json::stringify(&payload).try_to_utf8().unwrap()).unwrap();
    json!({"kind":"event","event":"approval_parent_history","payload":payload})
}
fn items(path: &Path) -> Vec<RolloutItem> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| parse_rollout_line(line).unwrap())
        .collect()
}
fn evidence(rows: &[RolloutItem]) -> (Vec<Message>, Value, Value) {
    let visible = project_thread_history(rows.to_vec());
    let visible: Value =
        serde_json::from_slice(&runtime_codec::encode_bytes(&visible).unwrap()).unwrap();
    let approval = ReviewParentHistory::from_rollout(rows)
        .unwrap()
        .checkpoint()
        .to_runtime_value()
        .unwrap();
    let approval =
        serde_json::from_str(&runtime_json::stringify(&approval).try_to_utf8().unwrap()).unwrap();
    (project_for_prompt(rows), visible, approval)
}

fn store(fixture: &Fixture) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(fixture.directory.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}

#[tokio::test]
async fn migration_preserves_persisted_user_deduplication_across_runtime_reopen_and_repeated_rewrites(
) {
    let kept = user("keep", "Inspect only.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&kept), "keep-turn").unwrap();
    let cp = checkpoint(&history);
    let mut meta = user("removed-meta", "Internal continuation.");
    meta["message"]["isMeta"] = json!(true);
    let fixture = Fixture::new(&[
        header(),
        context("keep-turn"),
        kept,
        context("removed-turn"),
        user("removed", "Publish."),
        meta,
        json!({"kind":"rollback","numTurns":1}),
        context("keep-turn"),
        cp.clone(),
    ]);
    let before = store(&fixture)
        .resume_runtime_for_write("migration")
        .await
        .unwrap();
    let expected = before.resume.persisted_user_uuids.clone();
    assert!(expected.contains(&RuntimeString::from("removed")));
    assert!(expected.contains(&RuntimeString::from("removed-meta")));
    before.rollout.shutdown().await.unwrap();
    let cancelled = AtomicBool::new(false);
    let first = preview(&fixture.path, MigrationKind::Main, &cancelled).unwrap();
    assert_eq!(first.report.removed_rollback_records, 1);
    apply(&fixture.path, &first, &cancelled).unwrap();
    acknowledge(&fixture.path).unwrap();
    let saved = items(&fixture.path);
    assert!(!saved.iter().any(|item| matches!(item,
        RolloutItem::Message {message:Message::User(user)} if user.uuid=="removed" || user.uuid=="removed-meta")));
    assert_eq!(
        project_for_prompt(&saved)
            .iter()
            .filter(|message| matches!(message,
        Message::User(user) if user.uuid=="removed" || user.uuid=="removed-meta"))
            .count(),
        0
    );
    let reopened = store(&fixture)
        .resume_runtime_for_write("migration")
        .await
        .unwrap();
    assert_eq!(reopened.resume.persisted_user_uuids, expected);
    reopened.rollout.shutdown().await.unwrap();

    // Another physical rewrite must carry IDs from the earlier migration marker,
    // even though their revoked originals no longer exist in the source.
    let mut file = File::options().append(true).open(&fixture.path).unwrap();
    writeln!(file, "{cp}").unwrap();
    writeln!(file, "{cp}").unwrap();
    drop(file);
    let second = preview(&fixture.path, MigrationKind::Main, &cancelled).unwrap();
    assert!(second.report.removed_approval_snapshots > 0);
    apply(&fixture.path, &second, &cancelled).unwrap();
    acknowledge(&fixture.path).unwrap();
    let final_resume = store(&fixture)
        .resume_runtime_for_write("migration")
        .await
        .unwrap();
    assert_eq!(final_resume.resume.persisted_user_uuids, expected);
    final_resume.rollout.shutdown().await.unwrap();
    let marker: Value = serde_json::from_str(
        std::fs::read_to_string(&fixture.path)
            .unwrap()
            .lines()
            .rev()
            .find(|line| line.contains("\"event\":\"rollout_migration\""))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        marker["payload"]["seenUserMessageIds"],
        json!(["keep", "removed", "removed-meta"])
    );
    assert_eq!(
        preview(&fixture.path, MigrationKind::Main, &cancelled)
            .unwrap()
            .report
            .status,
        MigrationStatus::Unchanged
    );
}

#[tokio::test]
async fn rollback_lifecycle_clocks_remain_available_to_lifetime_profile_statistics() {
    let kept = user("keep", "Inspect only.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&kept), "keep-turn").unwrap();
    let lifecycle = json!({"kind":"event","event":"turn_lifecycle","turnId":"removed-turn","payload":{
        "status":"completed","startedAtMs":1700000000000_i64,
        "finishedAtMs":1700000012000_i64,"durationMs":12000
    }});
    let fixture = Fixture::new(&[
        header(),
        context("keep-turn"),
        kept,
        context("removed-turn"),
        user("removed", "Publish."),
        lifecycle.clone(),
        json!({"kind":"rollback","numTurns":1}),
        context("keep-turn"),
        checkpoint(&history),
    ]);
    let now = 1700086400000_i64;
    let before = store(&fixture).profile_activity(now).await.unwrap();
    assert_eq!(before.longest_task_ms, 12000);
    let cancelled = AtomicBool::new(false);
    let plan = preview(&fixture.path, MigrationKind::Main, &cancelled).unwrap();
    assert_eq!(plan.report.removed_rollback_records, 0);
    assert!(plan
        .report
        .retained_reasons
        .iter()
        .any(|reason| reason == "retained_complex_rollback"));
    apply(&fixture.path, &plan, &cancelled).unwrap();
    acknowledge(&fixture.path).unwrap();
    let serialized_lifecycle = lifecycle.to_string();
    assert!(std::fs::read_to_string(&fixture.path)
        .unwrap()
        .lines()
        .any(|line| line == serialized_lifecycle.as_str()));
    assert_eq!(store(&fixture).profile_activity(now).await.unwrap(), before);
}

#[test]
fn preview_is_read_only_and_apply_preserves_history_header_sources_and_cursor_epoch() {
    let input = user("u1", "Inspect only. Do not publish.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&input), "turn-1").unwrap();
    let cp = checkpoint(&history);
    let fixture = Fixture::new(&[header(), context("turn-1"), input, cp.clone(), cp]);
    let before = fixture.bytes();
    let before_files = fixture.files();
    let before_evidence = evidence(&items(&fixture.path));
    let cancelled = AtomicBool::new(false);
    let plan = preview(&fixture.path, MigrationKind::Main, &cancelled).unwrap();
    assert_eq!(plan.report.status, MigrationStatus::Preview);
    assert_eq!(plan.report.removed_approval_snapshots, 1);
    assert_eq!(fixture.bytes(), before);
    assert_eq!(fixture.files(), before_files);
    assert!(!plan.report.cursor_epoch.is_empty());
    let result = apply(&fixture.path, &plan, &cancelled).unwrap();
    assert_eq!(result.status, MigrationStatus::Applied);
    assert!(result.needs_index_refresh);
    assert!(pending(&fixture.path).unwrap());
    assert_eq!(
        recover(&fixture.path, &cancelled).unwrap(),
        RecoveryState::NeedsIndexRefresh
    );
    let after = fixture.bytes();
    assert_ne!(after, before);
    assert_eq!(
        after.split(|byte| *byte == b'\n').next(),
        before.split(|byte| *byte == b'\n').next()
    );
    assert_eq!(evidence(&items(&fixture.path)), before_evidence);
    let marker: Value =
        serde_json::from_str(std::str::from_utf8(&after).unwrap().lines().last().unwrap()).unwrap();
    assert_eq!(marker["event"], "rollout_migration");
    assert_eq!(marker["payload"]["cursorEpoch"], result.cursor_epoch);
    acknowledge(&fixture.path).unwrap();
    assert!(!pending(&fixture.path).unwrap());
    let again = preview(&fixture.path, MigrationKind::Main, &cancelled).unwrap();
    assert_eq!(again.report.status, MigrationStatus::Unchanged);
    assert_eq!(again.report.cursor_epoch, result.cursor_epoch);
    assert_eq!(
        apply(&fixture.path, &again, &cancelled).unwrap().status,
        MigrationStatus::Unchanged
    );
    assert_eq!(fixture.bytes(), after);
}

#[test]
fn covered_snapshot_cleanup_keeps_unique_manual_approval_audit_anchor() {
    let mut historical = ReviewParentHistory::default();
    historical
        .record_manual_approval(
            &json!({"type":"command","command":"git status"}),
            "manual-turn",
        )
        .unwrap();
    let anchor = checkpoint(&historical);
    let empty = checkpoint(&ReviewParentHistory::default());
    let fixture = Fixture::new(&[
        header(),
        anchor.clone(),
        anchor.clone(),
        empty.clone(),
        empty,
    ]);
    let plan = preview(&fixture.path, MigrationKind::Main, &AtomicBool::new(false)).unwrap();
    assert_eq!(plan.report.removed_approval_snapshots, 2);
    assert!(plan
        .report
        .retained_reasons
        .iter()
        .any(|reason| reason == "retained_approval_audit_anchor"));
    let before = evidence(&items(&fixture.path));
    apply(&fixture.path, &plan, &AtomicBool::new(false)).unwrap();
    assert_eq!(evidence(&items(&fixture.path)), before);
    let serialized_anchor = anchor.to_string();
    assert_eq!(
        std::fs::read_to_string(&fixture.path)
            .unwrap()
            .lines()
            .filter(|line| *line == serialized_anchor.as_str())
            .count(),
        1
    );
}

#[test]
fn a_proven_revoked_branch_is_removed_without_reapplying_its_rollback() {
    let kept = user("keep", "Read only.");
    let kept_reply = assistant("reply-keep", "Read completed.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&kept), "keep-turn").unwrap();
    history.accept(&message(&kept_reply), "keep-turn").unwrap();
    let fixture = Fixture::new(&[
        header(),
        context("keep-turn"),
        kept,
        kept_reply,
        context("removed-turn"),
        user("removed", "Publish."),
        assistant("removed-reply", "Will publish."),
        json!({"kind":"rollback","numTurns":1}),
        context("keep-turn"),
        checkpoint(&history),
    ]);
    let before = evidence(&items(&fixture.path));
    let plan = preview(&fixture.path, MigrationKind::Main, &AtomicBool::new(false)).unwrap();
    assert_eq!(plan.report.removed_rollback_records, 1);
    assert!(plan.report.fully_normalized);
    apply(&fixture.path, &plan, &AtomicBool::new(false)).unwrap();
    assert!(!std::fs::read_to_string(&fixture.path)
        .unwrap()
        .contains("removed-reply"));
    assert_eq!(evidence(&items(&fixture.path)), before);
}

#[test]
fn revoked_typed_answers_remain_explicitly_unfinished_without_changing_authority() {
    let mut answer = user("typed", "Accepted response");
    answer["message"]["questionAnswer"] = json!({"requestId":"request","turnId":"turn","toolCallId":"question",
        "clientResponseId":"response","questions":[{"id":"scope","question":"Scope?"}],"answers":{"scope":"Never publish"}});
    let fixture = Fixture::new(&[
        header(),
        context("turn"),
        answer.clone(),
        json!({"kind":"rollback","numTurns":1}),
        checkpoint(&ReviewParentHistory::default()),
    ]);
    let before = evidence(&items(&fixture.path));
    let plan = preview(&fixture.path, MigrationKind::Main, &AtomicBool::new(false)).unwrap();
    assert!(!plan.report.fully_normalized);
    assert!(plan
        .report
        .retained_reasons
        .iter()
        .any(|reason| reason == "retained_complex_rollback"));
    apply(&fixture.path, &plan, &AtomicBool::new(false)).unwrap();
    assert!(std::fs::read_to_string(&fixture.path)
        .unwrap()
        .contains("Never publish"));
    assert_eq!(evidence(&items(&fixture.path)), before);
}

#[test]
fn active_writers_source_conflicts_and_cancellation_leave_source_unchanged() {
    let fixture = Fixture::new(&[header(), user("u", "Read only.")]);
    let flag = AtomicBool::new(false);
    let plan = preview(&fixture.path, MigrationKind::Main, &flag).unwrap();
    let lock = PathBuf::from(format!("{}.lock", fixture.path.display()));
    std::fs::write(&lock, format!("{}\n", std::process::id())).unwrap();
    let original = fixture.bytes();
    assert_eq!(
        apply(&fixture.path, &plan, &flag).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.bytes(), original);
    std::fs::remove_file(&lock).unwrap();
    assert_eq!(
        apply(&fixture.path, &plan, &AtomicBool::new(true))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Interrupted
    );
    assert_eq!(fixture.bytes(), original);
    assert!(!pending(&fixture.path).unwrap());
    let modified = std::fs::metadata(&fixture.path)
        .unwrap()
        .modified()
        .unwrap();
    let edited = String::from_utf8(original)
        .unwrap()
        .replace("Read only.", "Scan only.");
    std::fs::write(&fixture.path, edited.as_bytes()).unwrap();
    File::options()
        .write(true)
        .open(&fixture.path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    assert_eq!(
        apply(&fixture.path, &plan, &flag).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.bytes(), edited.as_bytes());
    assert!(!pending(&fixture.path).unwrap());
}

#[test]
fn malformed_rows_fail_without_loss_and_prepared_journals_recover_without_publication() {
    let fixture = Fixture::new(&[header()]);
    let before = fixture.bytes();
    let flag = AtomicBool::new(false);
    let plan = preview(&fixture.path, MigrationKind::Main, &flag).unwrap();
    let temporary = ".rollout-migration-interrupted.tmp";
    std::fs::write(fixture.directory.join(temporary), b"unpublished output").unwrap();
    let pending_path = PathBuf::from(format!("{}.migration.pending", fixture.path.display()));
    std::fs::write(&pending_path,json!({"version":1,"source":plan.report.source_revision,
        "outputSha256":"not-published","outputLength":18,"temporaryFile":temporary,"cursorEpoch":"interrupted"}).to_string()).unwrap();
    assert_eq!(
        recover(&fixture.path, &flag).unwrap(),
        RecoveryState::PreparedAbandoned
    );
    assert_eq!(fixture.bytes(), before);
    assert!(!pending_path.exists());
    assert!(!fixture.directory.join(temporary).exists());
    let mut file = File::options().append(true).open(&fixture.path).unwrap();
    file.write_all(b"{\"kind\":\"message\",broken\n").unwrap();
    drop(file);
    let malformed = fixture.bytes();
    assert_eq!(
        preview(&fixture.path, MigrationKind::Main, &flag)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.bytes(), malformed);
}

#[test]
fn subagent_trimming_requires_complete_recovery_and_preserves_human_usage_and_lifetime_sources() {
    let input = user("human", "Never publish.");
    let output = assistant("old-output", "Long tool investigation.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&input), "turn").unwrap();
    history.accept(&message(&output), "turn").unwrap();
    let approval = checkpoint(&history)["payload"].clone();
    let compact = json!({"kind":"compacted","summary":"Summary","replacedRange":[0,2],
        "replacementHistory":[{"type":"compact_boundary","uuid":"summary","summary":"Summary"}],
        "sourceMessageIds":["human","old-output"],"approvalParentHistory":approval});
    let fixture = Fixture::new(&[
        header(),
        context("turn"),
        input.clone(),
        output.clone(),
        compact.clone(),
    ]);
    let flag = AtomicBool::new(false);
    let before = project_for_prompt(&items(&fixture.path));
    let plan = preview(&fixture.path, MigrationKind::Subagent, &flag).unwrap();
    assert!(plan.report.truncated_audit);
    assert!(plan.report.removed_records > 0);
    assert_eq!(
        apply(&fixture.path, &plan, &flag).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(project_for_prompt(&items(&fixture.path)), before);
    let mut incomplete = compact;
    incomplete
        .as_object_mut()
        .unwrap()
        .remove("approvalParentHistory");
    let fixture = Fixture::new(&[header(), context("turn"), input, output, incomplete]);
    let plan = preview(&fixture.path, MigrationKind::Subagent, &flag).unwrap();
    assert!(!plan.report.truncated_audit);
    assert!(!plan.report.fully_normalized);
    assert!(plan
        .report
        .retained_reasons
        .iter()
        .any(|reason| reason == "retained_subagent_without_complete_checkpoint"));
    assert_eq!(plan.report.removed_records, 0);
}

#[test]
fn typed_usage_survives_a_revoked_assistant_while_uncovered_legacy_usage_blocks_removal() {
    let kept = user("keep", "Inspect only.");
    let mut history = ReviewParentHistory::default();
    history.accept(&message(&kept), "keep-turn").unwrap();
    let mut reply = assistant("billable", "Revoked assistant response.");
    let usage = json!({"inputTokens":10,"outputTokens":5,"cacheReadInputTokens":0,"cacheCreationInputTokens":0,"totalTokens":15});
    reply["message"]["usage"] = usage.clone();
    let ledger = json!({"kind":"usage_ledger","requestId":"request","assistantMessageId":"billable",
        "model":"fixture","usage":usage,"costUsd":0.005});
    for with_ledger in [false, true] {
        let mut rows = vec![
            header(),
            context("keep-turn"),
            kept.clone(),
            context("removed-turn"),
            user("removed", "Publish."),
            reply.clone(),
        ];
        if with_ledger {
            rows.push(ledger.clone());
        }
        rows.extend([
            json!({"kind":"rollback","numTurns":1}),
            context("keep-turn"),
            checkpoint(&history),
        ]);
        let fixture = Fixture::new(&rows);
        let before = evidence(&items(&fixture.path));
        let plan = preview(&fixture.path, MigrationKind::Main, &AtomicBool::new(false)).unwrap();
        assert_eq!(plan.report.removed_rollback_records, u64::from(with_ledger));
        apply(&fixture.path, &plan, &AtomicBool::new(false)).unwrap();
        let after = items(&fixture.path);
        assert_eq!(evidence(&after), before);
        if with_ledger {
            assert!(after
                .iter()
                .any(|item| matches!(item, RolloutItem::UsageLedger(_))));
            assert!(!after.iter().any(|item|matches!(item,RolloutItem::Message{message:Message::Assistant(assistant)} if assistant.uuid=="billable")));
        } else {
            assert!(after.iter().any(|item|matches!(item,RolloutItem::Message{message:Message::Assistant(assistant)} if assistant.uuid=="billable")));
        }
    }
}
