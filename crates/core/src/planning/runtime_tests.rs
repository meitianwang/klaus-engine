//! State-level planning contracts exercised without a model or client adapter.

use super::runtime::PlanningRuntime;
use super::types::{CollaborationMode, PlanningSnapshot, ProposedPlan};
use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-planning-runtime-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).expect("create isolated planning root");
        Self(path)
    }

    fn runtime(&self, thread_id: &str) -> PlanningRuntime {
        PlanningRuntime::new(thread_id.into(), Some(self.0.clone()), false, false)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn text(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("test text is valid UTF-8")
}

fn field(document: &RuntimeJson, name: &str) -> String {
    text(
        document
            .get(name)
            .and_then(RuntimeJson::as_str)
            .expect("document field is a string"),
    )
}

fn document_path(document: &RuntimeJson) -> PathBuf {
    PathBuf::from(field(document, "path"))
}

fn propose(runtime: &PlanningRuntime, id: &str, turn: &str, content: &str) -> ProposedPlan {
    let id = RuntimeString::from(id);
    let writing = runtime.start_plan(id.clone(), turn.into()).unwrap();
    assert_eq!(writing.status, "writing");
    assert!(writing.content.is_empty());
    let streamed = runtime.append_plan(&id, content).unwrap();
    assert_eq!(streamed.content, content);
    let complete = runtime.finish_plan(&id, true).unwrap();
    assert_eq!(complete.status, "proposed");
    complete
}

fn unchanged_after_rejection<T>(
    runtime: &PlanningRuntime,
    before: &PlanningSnapshot,
    result: Result<T, RuntimeString>,
) {
    assert!(result.is_err(), "invalid mutation must be rejected");
    assert_eq!(&runtime.snapshot().unwrap(), before);
}

fn only_snapshot_file(root: &Path) -> PathBuf {
    let mut paths = std::fs::read_dir(root.join("planning"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "one thread owns one snapshot");
    paths.pop().unwrap()
}

#[test]
fn collaboration_aliases_decode_to_the_two_supported_wire_modes() {
    for alias in ["default", "code", "pair_programming", "execute", "custom"] {
        assert_eq!(
            CollaborationMode::parse(alias).unwrap(),
            CollaborationMode::Default
        );
        let mode: CollaborationMode =
            runtime_codec::decode_value(&RuntimeJson::String(alias.into())).unwrap();
        assert_eq!(mode, CollaborationMode::Default);
        assert_eq!(
            runtime_codec::encode_value(&mode).unwrap(),
            RuntimeJson::String("default".into())
        );
    }
    assert_eq!(
        runtime_codec::decode_value::<CollaborationMode>(&RuntimeJson::String("plan".into()))
            .unwrap(),
        CollaborationMode::Plan
    );
    assert_eq!(
        runtime_codec::encode_value(&CollaborationMode::Plan).unwrap(),
        RuntimeJson::String("plan".into())
    );
    for unsupported in ["", "Plan", "review", " plan "] {
        assert!(CollaborationMode::parse(unsupported).is_err());
        assert!(
            runtime_codec::decode_value::<CollaborationMode>(&RuntimeJson::String(
                unsupported.into()
            ))
            .is_err()
        );
    }
}

#[test]
fn mode_changes_persist_and_stale_expected_revisions_do_not_mutate_state() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-mode");
    let initial = runtime.snapshot().unwrap();
    assert_eq!(initial.mode, CollaborationMode::Default);
    runtime
        .set_mode(CollaborationMode::Plan, Some(initial.revision))
        .unwrap();
    let planning = runtime.snapshot().unwrap();
    assert_eq!(planning.mode, CollaborationMode::Plan);
    assert!(planning.revision > initial.revision);
    unchanged_after_rejection(
        &runtime,
        &planning,
        runtime.set_mode(CollaborationMode::Default, Some(initial.revision)),
    );
    drop(runtime);

    let resumed = root.runtime("thread-mode");
    assert_eq!(resumed.snapshot().unwrap(), planning);
    resumed
        .set_mode(CollaborationMode::Default, Some(planning.revision))
        .unwrap();
    let saved = resumed.snapshot().unwrap();
    drop(resumed);
    let legacy_resume =
        PlanningRuntime::new("thread-mode".into(), Some(root.0.clone()), true, true);
    assert_eq!(legacy_resume.snapshot().unwrap(), saved);
    assert_eq!(legacy_resume.mode(), CollaborationMode::Default);
    assert!(legacy_resume.default_questions_enabled());
}

#[test]
fn legacy_planning_seeds_only_new_threads_and_turn_modes_change_at_admission() {
    let root = TestRoot::new();
    let runtime = PlanningRuntime::new("thread-legacy".into(), Some(root.0.clone()), true, false);
    assert_eq!(runtime.snapshot().unwrap().mode, CollaborationMode::Plan);
    assert!(!runtime.default_questions_enabled());
    runtime.begin_turn("turn-investigation".into());
    runtime.set_mode(CollaborationMode::Default, None).unwrap();
    assert_eq!(runtime.desired_mode(), CollaborationMode::Default);
    assert_eq!(runtime.mode(), CollaborationMode::Plan);
    runtime.begin_turn("turn-implementation".into());
    assert_eq!(runtime.mode(), CollaborationMode::Default);
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    assert_eq!(runtime.mode(), CollaborationMode::Default);
    runtime.begin_turn("turn-revision".into());
    assert_eq!(runtime.mode(), CollaborationMode::Plan);
}

#[test]
fn observers_receive_committed_snapshots_and_failed_persistence_does_not_publish() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-observed");
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&events);
    runtime.set_observer(Some(Arc::new(move |snapshot| {
        captured.lock().unwrap().push(snapshot);
    })));
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let committed = runtime.snapshot().unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        std::slice::from_ref(&committed)
    );
    let restored = root.runtime("thread-observed");
    assert_eq!(restored.snapshot().unwrap(), committed);
    drop(restored);

    std::fs::remove_dir_all(root.0.join("planning")).unwrap();
    std::fs::write(root.0.join("planning"), "occupied by a file").unwrap();
    unchanged_after_rejection(
        &runtime,
        &committed,
        runtime.set_mode(CollaborationMode::Default, Some(committed.revision)),
    );
    assert_eq!(events.lock().unwrap().as_slice(), &[committed]);
}

#[test]
fn streaming_plans_preserve_identity_content_source_turn_and_resume_lifecycle() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-stream");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let ready = propose(&runtime, "plan-ready", "turn-ready", "# Ready\n\nDo work.");
    let id: RuntimeString = "plan-interrupted".into();
    let writing = runtime
        .start_plan(id.clone(), "turn-writing".into())
        .unwrap();
    assert_eq!(writing.thread_id, "thread-stream");
    assert_eq!(writing.turn_id, "turn-writing");
    assert!(writing.revision > ready.revision);
    runtime.append_plan(&id, "# Draft\n").unwrap();
    runtime.append_plan(&id, "Inspect 🦀 first.").unwrap();
    drop(runtime);

    let resumed = root.runtime("thread-stream");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.mode, CollaborationMode::Plan);
    assert_eq!(snapshot.plans.len(), 2);
    assert_eq!(snapshot.plans[0], ready);
    let draft = &snapshot.plans[1];
    assert_eq!(draft.id, id);
    assert_eq!(draft.thread_id, "thread-stream");
    assert_eq!(draft.turn_id, "turn-writing");
    assert_eq!(draft.revision, writing.revision);
    assert_eq!(draft.content, "# Draft\nInspect 🦀 first.");
    assert_eq!(draft.status, "interrupted");
    unchanged_after_rejection(
        &resumed,
        &snapshot,
        resumed.accept(&id, draft.revision, "request-draft".into(), None),
    );
}

#[test]
fn interrupted_proposals_cannot_be_promoted_by_a_late_completion() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-interruption");
    let id: RuntimeString = "plan-interrupted".into();
    let writing = runtime
        .start_plan(id.clone(), "turn-interrupted".into())
        .unwrap();
    runtime
        .append_plan(&id, "# Draft\nUnfinished work")
        .unwrap();
    let interrupted = runtime.finish_plan(&id, false).unwrap();
    assert_eq!(interrupted.status, "interrupted");
    let snapshot = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.append_plan(&id, " after interruption"),
    );
    unchanged_after_rejection(&runtime, &snapshot, runtime.finish_plan(&id, true));
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.accept(&id, writing.revision, "request-interrupted".into(), None),
    );
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.read_document(&id, writing.revision),
    );
}

#[test]
fn complete_revisions_replace_prior_proposals_but_interrupted_revisions_do_not() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-revisions");
    let first = propose(&runtime, "plan-first", "turn-first", "# First\nKeep this.");
    let draft = runtime
        .start_plan("plan-draft".into(), "turn-draft".into())
        .unwrap();
    runtime.append_plan(&draft.id, "# Unfinished").unwrap();
    runtime.finish_plan(&draft.id, false).unwrap();
    assert_eq!(runtime.snapshot().unwrap().plans[0].status, "proposed");
    let document = runtime.read_document(&first.id, first.revision).unwrap();
    assert_eq!(field(&document, "content"), text(&first.content));

    let latest = propose(
        &runtime,
        "plan-latest",
        "turn-latest",
        "# Latest\nReplace all.",
    );
    assert!(latest.revision > draft.revision);
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.plans[0].status, "superseded");
    assert_eq!(snapshot.plans[1].status, "interrupted");
    assert_eq!(snapshot.plans[2].status, "proposed");
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.accept(&first.id, first.revision, "request-stale".into(), None),
    );
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.accept(
            &latest.id,
            first.revision,
            "request-wrong-revision".into(),
            None,
        ),
    );
    unchanged_after_rejection(
        &runtime,
        &snapshot,
        runtime.save_document(&first.id, first.revision, &"overwrite stale".into()),
    );
    assert_eq!(
        std::fs::read_to_string(document_path(&document)).unwrap(),
        text(&first.content)
    );
}

#[test]
fn documents_are_thread_owned_and_reopening_never_overwrites_editor_changes() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread/../editor");
    let plan = propose(
        &runtime,
        "plan/../../document",
        "turn-document",
        "# Original\nBody.",
    );
    let first = runtime.read_document(&plan.id, plan.revision).unwrap();
    let path = document_path(&first);
    assert!(path.starts_with(root.0.join("planning")));
    assert_eq!(path.file_name().unwrap(), "PLAN.md");
    assert_eq!(field(&first, "planId"), text(&plan.id));
    assert_eq!(
        runtime_codec::decode_value::<u64>(first.get("revision").unwrap()).unwrap(),
        plan.revision
    );
    let edited = "# Edited by the user\n\nPreserve the complete text.\n末尾\n";
    std::fs::write(&path, edited).unwrap();
    let reopened = runtime.read_document(&plan.id, plan.revision).unwrap();
    let reopened_revision =
        runtime_codec::decode_value::<u64>(reopened.get("revision").unwrap()).unwrap();
    assert!(reopened_revision > plan.revision);
    assert_eq!(document_path(&reopened), path);
    assert_eq!(field(&reopened, "content"), edited);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), edited);
    let reloaded = runtime.snapshot().unwrap();
    assert_eq!(reloaded.plans[0].content, edited);
    assert_eq!(reloaded.plans[0].revision, reopened_revision);
    unchanged_after_rejection(
        &runtime,
        &reloaded,
        runtime.read_document(&plan.id, plan.revision),
    );
    unchanged_after_rejection(
        &runtime,
        &reloaded,
        runtime.save_document(&plan.id, plan.revision, &"# Stale editor draft".into()),
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), edited);

    let second = propose(
        &runtime,
        "plan-second",
        "turn-second",
        "# Second\nFresh text.",
    );
    let second_document = runtime.read_document(&second.id, second.revision).unwrap();
    assert_ne!(document_path(&second_document), path);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), edited);
    drop(runtime);
    let resumed = root.runtime("thread/../editor");
    let restored = resumed.read_document(&second.id, second.revision).unwrap();
    assert_eq!(field(&restored, "content"), "# Second\nFresh text.");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), edited);
}

#[test]
fn saving_full_editor_text_is_used_for_acceptance_and_empty_saves_are_rejected() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-editor-save");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let plan = propose(
        &runtime,
        "plan-editor",
        "turn-editor",
        "# Original\nOld details.",
    );
    let edited: RuntimeString = "# Final plan\n\n1. Full change\n2. Verify\n".into();
    let saved = runtime
        .save_document(&plan.id, plan.revision, &edited)
        .unwrap();
    let revision = runtime_codec::decode_value::<u64>(saved.get("revision").unwrap()).unwrap();
    assert!(revision > plan.revision);
    assert_eq!(field(&saved, "content"), text(&edited));
    let before = runtime.snapshot().unwrap();
    assert_eq!(before.plans[0].content, edited);
    assert_eq!(before.plans[0].revision, revision);
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.save_document(&plan.id, revision, &RuntimeString::default()),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.save_document(&plan.id, plan.revision, &"# Stale editor draft".into()),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&plan.id, plan.revision, "request-stale-editor".into(), None),
    );
    assert_eq!(
        std::fs::read_to_string(document_path(&saved)).unwrap(),
        text(&edited)
    );

    let (admit, input) = runtime
        .accept(&plan.id, revision, "request-editor".into(), None)
        .unwrap();
    assert!(admit);
    assert_eq!(
        text(&input),
        format!("PLEASE IMPLEMENT THIS PLAN:\n{}", text(&edited))
    );
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.mode, CollaborationMode::Default);
    assert_eq!(snapshot.plans[0].content, edited);
    assert_eq!(snapshot.plans[0].status, "proposed");
    let receipt = snapshot.implementation.unwrap();
    assert_eq!(receipt.kind, "implementation");
    assert_eq!(receipt.admission, "pending");
}

#[test]
fn acceptance_loads_current_external_editor_text_and_keeps_thread_identity() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-live-editor");
    let plan = propose(
        &runtime,
        "plan-live-editor",
        "turn-live-editor",
        "# Original\nOld.",
    );
    let document = runtime.read_document(&plan.id, plan.revision).unwrap();
    let current = "# Current editor\n\nChanged outside the chat.\n";
    std::fs::write(document_path(&document), current).unwrap();
    let reviewed = runtime.read_document(&plan.id, plan.revision).unwrap();
    let revision = runtime_codec::decode_value::<u64>(reviewed.get("revision").unwrap()).unwrap();
    assert!(revision > plan.revision);
    assert_eq!(field(&reviewed, "content"), current);
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&plan.id, plan.revision, "request-stale-editor".into(), None),
    );
    let other = root.runtime("thread-unrelated");
    let unrelated = other.snapshot().unwrap();
    unchanged_after_rejection(
        &other,
        &unrelated,
        other.accept(&plan.id, revision, "request-cross-thread".into(), None),
    );
    let (admit, input) = runtime
        .accept(
            &plan.id,
            revision,
            "request-current-editor".into(),
            Some(current.into()),
        )
        .unwrap();
    assert!(admit);
    assert_eq!(
        text(&input),
        format!("PLEASE IMPLEMENT THIS PLAN:\n{current}")
    );
    assert_eq!(runtime.snapshot().unwrap().plans[0].content, current);
}

#[test]
fn acceptance_uses_structured_full_content_and_validates_request_identity() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-structured-content");
    let plan = propose(
        &runtime,
        "plan-structured",
        "turn-structured",
        "# Original\nBody.",
    );
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&plan.id, plan.revision, RuntimeString::default(), None),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(
            &plan.id,
            plan.revision,
            "request-empty-content".into(),
            Some(RuntimeString::default()),
        ),
    );
    let content: RuntimeString = "# Reviewed full text\n\nThis replaces every prior detail.".into();
    let (admit, input) = runtime
        .accept(
            &plan.id,
            plan.revision,
            "request-full-content".into(),
            Some(content.clone()),
        )
        .unwrap();
    assert!(admit);
    assert_eq!(
        text(&input),
        format!("PLEASE IMPLEMENT THIS PLAN:\n{}", text(&content))
    );
    assert_eq!(runtime.snapshot().unwrap().plans[0].content, content);
}

#[test]
fn pending_and_started_acceptance_receipts_prevent_duplicate_execution_across_resume() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-idempotent");
    let plan = propose(
        &runtime,
        "plan-idempotent",
        "turn-idempotent",
        "# Plan\nExecute once.",
    );
    let request: RuntimeString = "request-idempotent".into();
    let (admit, input) = runtime
        .accept(&plan.id, plan.revision, request.clone(), None)
        .unwrap();
    assert!(admit);
    let pending = runtime.snapshot().unwrap();
    assert_eq!(pending.plans[0].status, "proposed");
    assert_eq!(
        pending.implementation.as_ref().unwrap().admission,
        "pending"
    );
    let implementation_id = pending
        .implementation
        .as_ref()
        .unwrap()
        .implementation_id
        .clone();
    drop(runtime);

    let resumed = root.runtime("thread-idempotent");
    let restored = resumed.snapshot().unwrap();
    unchanged_after_rejection(
        &resumed,
        &restored,
        resumed.accept(
            &plan.id,
            plan.revision,
            request.clone(),
            Some("different retry text".into()),
        ),
    );
    let (duplicate, duplicate_input) = resumed
        .accept(&plan.id, plan.revision, request.clone(), None)
        .unwrap();
    assert!(!duplicate);
    assert_eq!(duplicate_input, input);
    assert_eq!(
        resumed
            .snapshot()
            .unwrap()
            .implementation
            .as_ref()
            .unwrap()
            .implementation_id,
        implementation_id
    );
    resumed
        .settle(&plan.id, plan.revision, &request, &"started".into())
        .unwrap();
    let started = resumed.snapshot().unwrap();
    assert_eq!(started.plans[0].status, "accepted");
    assert_eq!(
        started.implementation.as_ref().unwrap().admission,
        "started"
    );
    let (duplicate, duplicate_input) = resumed
        .accept(&plan.id, plan.revision, request.clone(), None)
        .unwrap();
    assert!(!duplicate);
    assert_eq!(duplicate_input, input);
    let before = resumed.snapshot().unwrap();
    unchanged_after_rejection(
        &resumed,
        &before,
        resumed.accept(&plan.id, plan.revision, "request-again".into(), None),
    );
    for outcome in ["failed", "cancelled"] {
        unchanged_after_rejection(
            &resumed,
            &before,
            resumed.settle(&plan.id, plan.revision, &request, &outcome.into()),
        );
    }
}

#[test]
fn a_pending_implementation_cannot_be_replaced_by_a_different_request() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-pending-owner");
    let plan = propose(
        &runtime,
        "plan-pending-owner",
        "turn-pending-owner",
        "# Plan\nOne admission.",
    );
    runtime
        .accept(&plan.id, plan.revision, "request-first".into(), None)
        .unwrap();
    let pending = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.accept(&plan.id, plan.revision, "request-second".into(), None),
    );
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.feedback(
            &plan.id,
            plan.revision,
            "request-feedback-race".into(),
            "Replace the pending implementation.".into(),
        ),
    );
}

#[test]
fn failed_and_cancelled_admissions_leave_a_retryable_reviewed_plan() {
    for outcome in ["failed", "cancelled"] {
        let root = TestRoot::new();
        let runtime = root.runtime("thread-retry");
        let plan = propose(
            &runtime,
            "plan-retry",
            "turn-retry",
            "# Plan\nRecover admission.",
        );
        let request: RuntimeString = "request-retry".into();
        let (_, first_input) = runtime
            .accept(&plan.id, plan.revision, request.clone(), None)
            .unwrap();
        runtime
            .settle(&plan.id, plan.revision, &request, &outcome.into())
            .unwrap();
        let failed = runtime.snapshot().unwrap();
        assert_eq!(failed.plans[0].status, "proposed");
        assert_eq!(failed.plans[0].content, plan.content);
        assert_eq!(failed.implementation.as_ref().unwrap().admission, outcome);
        drop(runtime);

        let resumed = root.runtime("thread-retry");
        let (admit, retry_input) = resumed
            .accept(&plan.id, plan.revision, request.clone(), None)
            .unwrap();
        assert!(admit);
        assert_eq!(retry_input, first_input);
        assert_eq!(
            resumed
                .snapshot()
                .unwrap()
                .implementation
                .as_ref()
                .unwrap()
                .admission,
            "pending"
        );
        resumed
            .settle(&plan.id, plan.revision, &request, &"started".into())
            .unwrap();
        assert_eq!(resumed.snapshot().unwrap().plans[0].status, "accepted");
    }
}

#[test]
fn stale_settlement_and_reused_request_revisions_cannot_mutate_newer_state() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-stale-receipt");
    let first = propose(&runtime, "plan-first", "turn-first", "# First\nOriginal.");
    let request: RuntimeString = "request-shared".into();
    runtime
        .accept(&first.id, first.revision, request.clone(), None)
        .unwrap();
    runtime
        .settle(&first.id, first.revision, &request, &"failed".into())
        .unwrap();
    let second = propose(&runtime, "plan-second", "turn-second", "# Second\nRevised.");
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&second.id, second.revision, request.clone(), None),
    );
    let next_request: RuntimeString = "request-new".into();
    runtime
        .accept(&second.id, second.revision, next_request.clone(), None)
        .unwrap();
    let pending = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.settle(&first.id, first.revision, &request, &"started".into()),
    );
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.settle(&second.id, first.revision, &next_request, &"started".into()),
    );
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.settle(
            &second.id,
            second.revision,
            &next_request,
            &"unknown".into(),
        ),
    );
}

#[test]
fn feedback_is_a_plan_turn_and_never_accepts_the_proposal() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-feedback");
    let plan = propose(
        &runtime,
        "plan-feedback",
        "turn-feedback",
        "# Plan\nReview this.",
    );
    let request: RuntimeString = "request-feedback".into();
    let (admit, input) = runtime
        .feedback(
            &plan.id,
            plan.revision,
            request.clone(),
            "Add recovery steps.".into(),
        )
        .unwrap();
    assert!(admit);
    assert_eq!(
        text(&input),
        "Plan feedback for the current proposed plan:\n\nAdd recovery steps."
    );
    let pending = runtime.snapshot().unwrap();
    assert_eq!(pending.mode, CollaborationMode::Plan);
    let mut reviewed = plan.clone();
    reviewed.review_pending = false;
    assert_eq!(pending.plans[0], reviewed);
    let receipt = pending.implementation.as_ref().unwrap();
    assert_eq!(receipt.kind, "feedback");
    assert_eq!(receipt.admission, "pending");
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.feedback(
            &plan.id,
            plan.revision,
            request.clone(),
            "Changed retry text.".into(),
        ),
    );
    let (duplicate, duplicate_input) = runtime
        .feedback(
            &plan.id,
            plan.revision,
            request.clone(),
            "Add recovery steps.".into(),
        )
        .unwrap();
    assert!(!duplicate);
    assert_eq!(duplicate_input, input);
    runtime
        .settle(&plan.id, plan.revision, &request, &"started".into())
        .unwrap();
    let answered = runtime.snapshot().unwrap();
    assert_eq!(answered.mode, CollaborationMode::Plan);
    assert_eq!(answered.plans[0], reviewed);
    assert_eq!(answered.implementation.unwrap().admission, "started");
}

#[test]
fn feedback_and_implementation_request_identities_cannot_be_reused_across_kinds() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-feedback-identity");
    let plan = propose(
        &runtime,
        "plan-feedback",
        "turn-feedback",
        "# Plan\nIdentity matters.",
    );
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.feedback(
            &plan.id,
            plan.revision,
            RuntimeString::default(),
            "Revise this.".into(),
        ),
    );
    let request: RuntimeString = "request-feedback".into();
    runtime
        .feedback(
            &plan.id,
            plan.revision,
            request.clone(),
            "Revise this.".into(),
        )
        .unwrap();
    let pending = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.accept(&plan.id, plan.revision, request, None),
    );

    let other = root.runtime("thread-implementation-identity");
    let other_plan = propose(
        &other,
        "plan-other",
        "turn-other",
        "# Other\nIdentity matters.",
    );
    let other_request: RuntimeString = "request-implementation".into();
    other
        .accept(
            &other_plan.id,
            other_plan.revision,
            other_request.clone(),
            None,
        )
        .unwrap();
    let pending = other.snapshot().unwrap();
    unchanged_after_rejection(
        &other,
        &pending,
        other.feedback(
            &other_plan.id,
            other_plan.revision,
            other_request,
            "Revision request.".into(),
        ),
    );
}

#[test]
fn cancelling_a_pending_request_preserves_the_reviewed_artifact_and_allows_retry() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-cancel");
    let plan = propose(
        &runtime,
        "plan-cancel",
        "turn-cancel",
        "# Plan\nKeep the review.",
    );
    let request: RuntimeString = "request-cancel".into();
    runtime
        .accept(&plan.id, plan.revision, request.clone(), None)
        .unwrap();
    runtime.cancel(&plan.id, plan.revision).unwrap();
    let cancelled = runtime.snapshot().unwrap();
    assert_eq!(cancelled.mode, CollaborationMode::Default);
    let mut reviewed = plan.clone();
    reviewed.review_pending = false;
    assert_eq!(cancelled.plans[0], reviewed);
    assert_eq!(
        cancelled.implementation.as_ref().unwrap().admission,
        "cancelled"
    );
    let (retry, _) = runtime
        .accept(&plan.id, plan.revision, request, None)
        .unwrap();
    assert!(retry);
    assert_eq!(runtime.snapshot().unwrap().plans[0].status, "proposed");
}

#[test]
fn terminal_admission_receipts_reject_late_start_without_an_explicit_retry() {
    for outcome in ["failed", "cancelled"] {
        let root = TestRoot::new();
        let runtime = root.runtime("thread-late-admission");
        let plan = propose(
            &runtime,
            "plan-late-admission",
            "turn-late-admission",
            "# Plan\nDo not admit stale work.",
        );
        let request: RuntimeString = "request-late-admission".into();
        runtime
            .accept(&plan.id, plan.revision, request.clone(), None)
            .unwrap();
        if outcome == "cancelled" {
            runtime.cancel(&plan.id, plan.revision).unwrap();
        } else {
            runtime
                .settle(&plan.id, plan.revision, &request, &outcome.into())
                .unwrap();
        }
        let terminal = runtime.snapshot().unwrap();
        unchanged_after_rejection(
            &runtime,
            &terminal,
            runtime.settle(&plan.id, plan.revision, &request, &"started".into()),
        );
        assert_eq!(terminal.plans[0].status, "proposed");
    }
}

#[test]
fn failed_editor_reads_leave_acceptance_recoverable_without_mutating_the_plan() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-editor-failure");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let plan = propose(
        &runtime,
        "plan-editor-failure",
        "turn-editor-failure",
        "# Plan\nKeep the full reviewed text.",
    );
    let document = runtime.read_document(&plan.id, plan.revision).unwrap();
    let path = document_path(&document);
    std::fs::remove_file(&path).unwrap();
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(
            &plan.id,
            plan.revision,
            "request-editor-failure".into(),
            None,
        ),
    );
    assert_eq!(before.mode, CollaborationMode::Plan);
    assert_eq!(before.plans[0].status, "proposed");
    assert!(before.implementation.is_none());
    let recovered = runtime.read_document(&plan.id, plan.revision).unwrap();
    assert_eq!(document_path(&recovered), path);
    assert_eq!(field(&recovered, "content"), text(&plan.content));
    let (admit, _) = runtime
        .accept(
            &plan.id,
            plan.revision,
            "request-editor-failure".into(),
            None,
        )
        .unwrap();
    assert!(admit);
}

#[test]
fn resumed_questions_expire_only_pending_requests_and_preserve_resolved_history() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-question-history");
    for (request_id, status) in [
        ("request-pending", "pending"),
        ("request-answered", "answered"),
        ("request-skipped", "skipped"),
        ("request-cancelled", "cancelled"),
    ] {
        let question = runtime_json::parse(&format!(
            "{{\"requestId\":\"{request_id}\",\"turnId\":\"turn-questions\",\"status\":\"{status}\"}}"
        ).into()).unwrap();
        runtime.register_question(question).unwrap();
    }
    drop(runtime);
    let resumed = root.runtime("thread-question-history");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.questions.len(), 4);
    for (question, expected) in
        snapshot
            .questions
            .iter()
            .zip(["expired", "answered", "skipped", "cancelled"])
    {
        assert_eq!(field(question, "status"), expected);
        assert_eq!(field(question, "turnId"), "turn-questions");
    }
}

#[test]
fn corrupt_persistence_is_reported_and_never_silently_replaced() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-corrupt");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    drop(runtime);
    let path = only_snapshot_file(&root.0);
    let corrupt = "{ an interrupted invalid state file";
    std::fs::write(&path, corrupt).unwrap();
    let resumed = root.runtime("thread-corrupt");
    assert!(resumed.snapshot().is_err());
    assert!(resumed.set_mode(CollaborationMode::Default, None).is_err());
    assert_eq!(std::fs::read_to_string(path).unwrap(), corrupt);
}

#[test]
fn ordinary_turn_claim_is_durable_and_duplicate_start_is_rejected_after_restart() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-durable-admission");
    let plan = propose(
        &runtime,
        "plan-durable",
        "turn-proposal",
        "# Plan\nPerform the work.",
    );
    let (_, input) = runtime
        .accept(&plan.id, plan.revision, "durable-request".into(), None)
        .unwrap();
    let receipt = runtime.snapshot().unwrap().implementation.unwrap();
    assert!(runtime
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(input.clone()))
        )
        .unwrap());
    drop(runtime);
    let resumed = root.runtime("thread-durable-admission");
    assert_eq!(
        resumed
            .snapshot()
            .unwrap()
            .implementation
            .unwrap()
            .admission,
        "started"
    );
    assert!(resumed
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(input))
        )
        .is_err());
    assert_eq!(resumed.snapshot().unwrap().plans[0].status, "accepted");
}

#[test]
fn pending_implementation_freezes_document_and_disk_conflicts_preserve_both_drafts() {
    let root = TestRoot::new();
    let runtime = root.runtime("thread-document-cas");
    let plan = propose(&runtime, "plan-cas", "turn-cas", "# Original");
    let document = runtime.read_document(&plan.id, plan.revision).unwrap();
    let path = document_path(&document);
    std::fs::write(&path, "# External change").unwrap();
    assert!(runtime
        .save_document_expected(
            &plan.id,
            plan.revision,
            &"# Unsaved editor".into(),
            Some(&plan.content)
        )
        .is_err());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "# External change");
    let current = runtime.read_document(&plan.id, plan.revision).unwrap();
    let revision = current.get("revision").unwrap().as_u64().unwrap();
    runtime
        .accept(&plan.id, revision, "cas-accept".into(), None)
        .unwrap();
    assert!(runtime
        .save_document(&plan.id, revision, &"# Late editor".into())
        .is_err());
}

#[test]
fn forked_planning_state_preserves_artifacts_without_sharing_admission_or_documents() {
    let root = TestRoot::new();
    let source = root.runtime("fork-source");
    source.set_mode(CollaborationMode::Plan, None).unwrap();
    let plan = propose(&source, "fork-plan", "fork-turn", "# Isolated fork plan");
    let source_document = source.read_document(&plan.id, plan.revision).unwrap();
    source
        .accept(&plan.id, plan.revision, "source-request".into(), None)
        .unwrap();
    let snapshot = source.snapshot().unwrap();
    let target = root.runtime("fork-target");
    let retained = std::collections::HashSet::from([plan.turn_id.clone()]);
    target.inherit_from(&snapshot, &retained).unwrap();
    let inherited = target.snapshot().unwrap();
    assert!(inherited.implementation.is_none());
    assert_eq!(inherited.plans[0].thread_id, "fork-target");
    assert!(inherited.plans[0].document_path.is_none());
    let target_document = target.read_document(&plan.id, plan.revision).unwrap();
    assert_ne!(
        document_path(&target_document),
        document_path(&source_document)
    );
    target
        .save_document(&plan.id, plan.revision, &"# Target edit".into())
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(document_path(&source_document)).unwrap(),
        "# Isolated fork plan"
    );
    assert_eq!(source.snapshot().unwrap(), snapshot);
}

#[test]
fn fork_at_a_cut_does_not_copy_later_or_pending_question_answers() {
    let root = TestRoot::new();
    let source = root.runtime("question-fork-source");
    for (id, turn, status) in [
        ("retained", "before-cut", "answered"),
        ("later", "after-cut", "answered"),
        ("waiting", "before-cut", "pending"),
    ] {
        source
            .register_question(
                runtime_json::parse(
                    &format!(
                        "{{\"requestId\":\"{id}\",\"turnId\":\"{turn}\",\"status\":\"{status}\"}}"
                    )
                    .into(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let target = root.runtime("question-fork-target");
    target
        .inherit_from(
            &source.snapshot().unwrap(),
            &std::collections::HashSet::from([RuntimeString::from("before-cut")]),
        )
        .unwrap();
    let inherited = target.snapshot().unwrap();
    assert_eq!(inherited.questions.len(), 1);
    assert_eq!(field(&inherited.questions[0], "requestId"), "retained");
}

#[test]
fn empty_finalized_plan_is_retained_only_as_an_interrupted_artifact() {
    let root = TestRoot::new();
    let runtime = root.runtime("empty-proposal");
    runtime
        .start_plan("empty-plan".into(), "empty-turn".into())
        .unwrap();
    runtime.append_plan(&"empty-plan".into(), " \n").unwrap();
    let plan = runtime.finish_plan(&"empty-plan".into(), true).unwrap();
    assert_eq!(plan.status, "interrupted");
    assert!(!plan.review_pending);
}

#[test]
fn acceptance_rejects_unobserved_disk_edits_and_admitted_history_stays_immutable() {
    let root = TestRoot::new();
    let runtime = root.runtime("immutable-reviewed-plan");
    let plan = propose(
        &runtime,
        "immutable-plan",
        "immutable-turn",
        "# Reviewed plan",
    );
    let document = runtime.read_document(&plan.id, plan.revision).unwrap();
    let path = document_path(&document);
    std::fs::write(&path, "# Unobserved external change").unwrap();
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&plan.id, plan.revision, "unobserved-request".into(), None),
    );
    std::fs::write(&path, text(&plan.content)).unwrap();
    let (_, input) = runtime
        .accept(&plan.id, plan.revision, "reviewed-request".into(), None)
        .unwrap();
    let receipt = runtime.snapshot().unwrap().implementation.unwrap();
    runtime
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(input)),
        )
        .unwrap();
    let admitted = runtime.snapshot().unwrap();
    std::fs::write(&path, "# New draft after admission").unwrap();
    unchanged_after_rejection(
        &runtime,
        &admitted,
        runtime.read_document(&plan.id, plan.revision),
    );
    assert_eq!(text(&admitted.plans[0].content), "# Reviewed plan");
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "# New draft after admission"
    );
}

#[test]
fn exact_utf16_append_and_scalar_bridge_survive_durable_interrupted_reload() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-draft");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let id: RuntimeString = "exact-unit-plan".into();
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    runtime.append_plan(&id, "# Exact\n").unwrap();
    runtime
        .append_plan_runtime(&id, &RuntimeString::from_utf16(vec![0xd800]))
        .unwrap();
    let after_scalar = runtime.append_plan(&id, "x").unwrap();
    assert_eq!(
        after_scalar.content,
        RuntimeString::from("# Exact\n").concat(&RuntimeString::from_utf16(vec![0xd800, 0x0078]))
    );
    runtime
        .append_plan_runtime(&id, &RuntimeString::from_utf16(vec![0xdc00, 0xd83e]))
        .unwrap();
    let expected = RuntimeString::from("# Exact\n").concat(&RuntimeString::from_utf16(vec![
        0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80,
    ]));
    assert_eq!(
        runtime
            .append_plan_runtime(&id, &RuntimeString::from_utf16(vec![0xdd80]))
            .unwrap()
            .content,
        expected
    );
    let saved: PlanningSnapshot = runtime_codec::decode_text(&RuntimeString::from(
        std::fs::read_to_string(only_snapshot_file(&root.0)).unwrap(),
    ))
    .unwrap();
    assert_eq!(saved.plans[0].content, expected);
    drop(runtime);
    let resumed = root.runtime("exact-unit-draft");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.mode, CollaborationMode::Plan);
    assert_eq!(snapshot.plans[0].content, expected);
    assert_eq!(snapshot.plans[0].status, "interrupted");
    unchanged_after_rejection(
        &resumed,
        &snapshot,
        resumed.append_plan_runtime(&id, &RuntimeString::from_utf16(vec![0xd800])),
    );
}

#[test]
fn exact_utf16_append_enforces_the_code_unit_cap_without_changing_rejected_disk_state() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-cap");
    let id: RuntimeString = "exact-unit-plan".into();
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    let mut units = vec![0x0078; 2_000_000];
    *units.last_mut().unwrap() = 0xd800;
    let expected = RuntimeString::from_utf16(units);
    let at_cap = runtime.append_plan_runtime(&id, &expected).unwrap();
    assert_eq!(at_cap.content, expected);
    assert_eq!(at_cap.content.len_utf16(), 2_000_000);
    let before = runtime.snapshot().unwrap();
    let path = only_snapshot_file(&root.0);
    let disk_before = std::fs::read(&path).unwrap();
    let error = runtime
        .append_plan_runtime(&id, &RuntimeString::from_utf16(vec![0xdc00]))
        .unwrap_err();
    assert_eq!(error, "Plan content exceeds 2000000 characters");
    assert_eq!(runtime.snapshot().unwrap(), before);
    assert_eq!(std::fs::read(&path).unwrap(), disk_before);
    assert_eq!(runtime.finish_plan(&id, true).unwrap().content, expected);
    drop(runtime);
    let resumed = root.runtime("exact-unit-cap");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.plans[0].status, "proposed");
    assert_eq!(snapshot.plans[0].content, expected);
    assert_eq!(snapshot.plans[0].content.as_utf16().last(), Some(&0xd800));
}

#[test]
fn exact_utf16_accepted_input_and_receipt_survive_claim_and_durable_reload() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-acceptance");
    runtime.set_mode(CollaborationMode::Plan, None).unwrap();
    let id: RuntimeString = "exact-unit-reviewed-plan".into();
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    let content = RuntimeString::from("# Exact\n").concat(&RuntimeString::from_utf16(vec![
        0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80,
    ]));
    runtime.append_plan_runtime(&id, &content).unwrap();
    let plan = runtime.finish_plan(&id, true).unwrap();
    let request: RuntimeString = "exact-unit-implementation-request".into();
    let (admit, input) = runtime
        .accept(&id, plan.revision, request.clone(), None)
        .unwrap();
    assert!(admit);
    let expected = RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&content);
    assert_eq!(input, expected);
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.mode, CollaborationMode::Default);
    assert_eq!(snapshot.plans[0].content, content);
    let receipt = snapshot.implementation.unwrap();
    assert_eq!(receipt.input, expected);
    assert_eq!(receipt.admission, "pending");
    assert_eq!(
        runtime
            .accept(&id, plan.revision, request, Some(content.clone()))
            .unwrap(),
        (false, expected.clone())
    );
    assert!(runtime
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(input))
        )
        .unwrap());
    drop(runtime);
    let resumed = root.runtime("exact-unit-acceptance");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.plans[0].content, content);
    assert_eq!(snapshot.plans[0].status, "accepted");
    let receipt = snapshot.implementation.unwrap();
    assert_eq!(receipt.input, expected);
    assert_eq!(receipt.admission, "started");
}

#[test]
fn reused_acceptance_identity_rejects_distinct_lone_units_without_mutating_authority() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-acceptance-mismatch");
    let id: RuntimeString = "exact-unit-reviewed-plan".into();
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    let prefix = RuntimeString::from("# Exact\n");
    let content = prefix.concat(&RuntimeString::from_utf16(vec![0xd800]));
    runtime.append_plan_runtime(&id, &content).unwrap();
    let plan = runtime.finish_plan(&id, true).unwrap();
    let request: RuntimeString = "exact-unit-implementation-request".into();
    assert!(
        runtime
            .accept(&id, plan.revision, request.clone(), Some(content.clone()))
            .unwrap()
            .0
    );
    let before = runtime.snapshot().unwrap();
    let disk_before = std::fs::read(only_snapshot_file(&root.0)).unwrap();
    for unit in [0xd801, 0xdc00, 0xfffd] {
        let changed = prefix.concat(&RuntimeString::from_utf16(vec![unit]));
        assert_ne!(changed, content);
        // These distinct exact strings share the filesystem UTF-8 projection.
        assert_eq!(changed.to_node_utf8(), content.to_node_utf8());
        let error = runtime
            .accept(&id, plan.revision, request.clone(), Some(changed))
            .unwrap_err();
        assert_eq!(
            error,
            "Implementation request identity was reused with changed content"
        );
        assert_eq!(runtime.snapshot().unwrap(), before);
        assert_eq!(
            std::fs::read(only_snapshot_file(&root.0)).unwrap(),
            disk_before
        );
    }
    assert!(
        !runtime
            .accept(&id, plan.revision, request, Some(content))
            .unwrap()
            .0
    );
}

#[test]
fn exact_utf16_feedback_receipt_rejects_projected_collisions_and_survives_reload() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-feedback");
    let plan = propose(
        &runtime,
        "exact-feedback-plan",
        "exact-feedback-turn",
        "# Plan",
    );
    let request: RuntimeString = "exact-feedback-request".into();
    let feedback = RuntimeString::from("# Revise\n").concat(&RuntimeString::from_utf16(vec![
        0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80,
    ]));
    let expected =
        RuntimeString::from("Plan feedback for the current proposed plan:\n\n").concat(&feedback);
    let (admit, input) = runtime
        .feedback(&plan.id, plan.revision, request.clone(), feedback.clone())
        .unwrap();
    assert!(admit);
    assert_eq!(input, expected);
    let before = runtime.snapshot().unwrap();
    assert_eq!(before.mode, CollaborationMode::Plan);
    assert_eq!(before.plans[0].status, "proposed");
    assert_eq!(before.implementation.as_ref().unwrap().input, expected);
    let disk_before = std::fs::read(only_snapshot_file(&root.0)).unwrap();
    let mut changed_units = feedback.as_utf16().to_vec();
    let lone = changed_units
        .iter()
        .position(|unit| *unit == 0xd800)
        .unwrap();
    changed_units[lone] = 0xd801;
    let changed = RuntimeString::from_utf16(changed_units);
    assert_ne!(changed, feedback);
    assert_eq!(changed.to_node_utf8(), feedback.to_node_utf8());
    let error = runtime
        .feedback(&plan.id, plan.revision, request.clone(), changed)
        .unwrap_err();
    assert_eq!(
        error,
        "Feedback request identity was reused with changed content"
    );
    assert_eq!(runtime.snapshot().unwrap(), before);
    assert_eq!(
        std::fs::read(only_snapshot_file(&root.0)).unwrap(),
        disk_before
    );
    assert_eq!(
        runtime
            .feedback(&plan.id, plan.revision, request, feedback)
            .unwrap(),
        (false, expected.clone())
    );
    drop(runtime);
    let resumed = root.runtime("exact-unit-feedback");
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.mode, CollaborationMode::Plan);
    assert_eq!(snapshot.plans[0].status, "proposed");
    let receipt = snapshot.implementation.unwrap();
    assert_eq!(receipt.input, expected);
    assert_eq!(receipt.admission, "pending");
    assert!(resumed
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(expected))
        )
        .unwrap());
    let snapshot = resumed.snapshot().unwrap();
    assert_eq!(snapshot.plans[0].status, "proposed");
    assert_eq!(snapshot.implementation.unwrap().admission, "started");
}

#[test]
fn materialized_utf8_projection_preserves_exact_document_revision_and_accepted_input() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-materialized-document");
    let id: RuntimeString = "exact-unit-materialized-plan".into();
    let content = RuntimeString::from("# Exact\n").concat(&RuntimeString::from_utf16(vec![
        0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80,
    ]));
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    runtime.append_plan_runtime(&id, &content).unwrap();
    let plan = runtime.finish_plan(&id, true).unwrap();
    let document = runtime.read_document(&id, plan.revision).unwrap();
    let path = document_path(&document);
    assert_eq!(
        document.get("content").and_then(RuntimeJson::as_str),
        Some(&content)
    );
    assert_eq!(
        document.get("revision").and_then(RuntimeJson::as_u64),
        Some(plan.revision)
    );
    assert_eq!(std::fs::read(&path).unwrap(), content.to_node_utf8());
    assert_eq!(runtime.snapshot().unwrap().plans[0].content, content);
    assert_eq!(runtime.snapshot().unwrap().plans[0].revision, plan.revision);
    let again = runtime.read_document(&id, plan.revision).unwrap();
    assert_eq!(again, document);
    drop(runtime);
    let resumed = root.runtime("exact-unit-materialized-document");
    assert_eq!(resumed.read_document(&id, plan.revision).unwrap(), document);
    let exact = resumed.snapshot().unwrap();
    assert_eq!(exact.plans[0].content, content);
    assert_eq!(exact.plans[0].revision, plan.revision);
    let projected = RuntimeString::from(std::fs::read_to_string(&path).unwrap());
    assert_ne!(projected, content);
    unchanged_after_rejection(
        &resumed,
        &exact,
        resumed.accept(
            &id,
            plan.revision,
            "projected-review".into(),
            Some(projected),
        ),
    );
    let (admit, input) = resumed
        .accept(
            &id,
            plan.revision,
            "exact-review".into(),
            Some(content.clone()),
        )
        .unwrap();
    assert!(admit);
    assert_eq!(
        input,
        RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&content)
    );
    assert_eq!(resumed.read_document(&id, plan.revision).unwrap(), document);
    let receipt = resumed.snapshot().unwrap().implementation.unwrap();
    assert!(resumed
        .claim_submission(
            &receipt.implementation_id,
            Some(&crate::engine::session::Input::Text(input))
        )
        .unwrap());
    assert_eq!(resumed.read_document(&id, plan.revision).unwrap(), document);
    let accepted = resumed.snapshot().unwrap();
    assert_eq!(accepted.plans[0].content, content);
    assert_eq!(accepted.plans[0].revision, plan.revision);
    assert_eq!(accepted.plans[0].status, "accepted");
}

#[test]
fn exact_unit_saves_keep_projected_readback_stable_and_reject_stale_revisions() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-saved-document");
    let plan = propose(
        &runtime,
        "exact-unit-saved-plan",
        "exact-unit-turn",
        "# Original",
    );
    let original = runtime.read_document(&plan.id, plan.revision).unwrap();
    let path = document_path(&original);
    let prefix = RuntimeString::from("# Saved\n");
    let first = prefix.concat(&RuntimeString::from_utf16(vec![0xd800]));
    let saved = runtime
        .save_document_expected(&plan.id, plan.revision, &first, Some(&plan.content))
        .unwrap();
    let first_revision = saved.get("revision").and_then(RuntimeJson::as_u64).unwrap();
    assert!(first_revision > plan.revision);
    assert_eq!(
        saved.get("content").and_then(RuntimeJson::as_str),
        Some(&first)
    );
    let projected_bytes = std::fs::read(&path).unwrap();
    assert_eq!(projected_bytes, first.to_node_utf8());
    assert_eq!(
        runtime.read_document(&plan.id, first_revision).unwrap(),
        saved
    );
    let second = prefix.concat(&RuntimeString::from_utf16(vec![0xd801]));
    assert_ne!(first, second);
    assert_eq!(first.to_node_utf8(), second.to_node_utf8());
    let saved = runtime
        .save_document_expected(&plan.id, first_revision, &second, Some(&first))
        .unwrap();
    let second_revision = saved.get("revision").and_then(RuntimeJson::as_u64).unwrap();
    assert!(second_revision > first_revision);
    assert_eq!(
        saved.get("content").and_then(RuntimeJson::as_str),
        Some(&second)
    );
    assert_eq!(std::fs::read(&path).unwrap(), projected_bytes);
    let before = runtime.snapshot().unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.read_document(&plan.id, first_revision),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.save_document_expected(
            &plan.id,
            first_revision,
            &"# Stale editor".into(),
            Some(&first),
        ),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&plan.id, first_revision, "stale-review".into(), Some(first)),
    );
    assert_eq!(std::fs::read(&path).unwrap(), projected_bytes);
    drop(runtime);
    let resumed = root.runtime("exact-unit-saved-document");
    assert_eq!(
        resumed.read_document(&plan.id, second_revision).unwrap(),
        saved
    );
    assert_eq!(resumed.snapshot().unwrap().plans[0].content, second);
    assert_eq!(
        resumed.snapshot().unwrap().plans[0].revision,
        second_revision
    );
    let (_, input) = resumed
        .accept(&plan.id, second_revision, "current-review".into(), None)
        .unwrap();
    assert_eq!(
        input,
        RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&second)
    );
}

#[test]
fn external_scalar_document_edits_update_once_and_keep_conflict_and_pending_guards() {
    let root = TestRoot::new();
    let runtime = root.runtime("exact-unit-external-document");
    let id: RuntimeString = "exact-unit-external-plan".into();
    let content = RuntimeString::from("# Exact\n").concat(&RuntimeString::from_utf16(vec![0xd800]));
    runtime
        .start_plan(id.clone(), "exact-unit-turn".into())
        .unwrap();
    runtime.append_plan_runtime(&id, &content).unwrap();
    let plan = runtime.finish_plan(&id, true).unwrap();
    let document = runtime.read_document(&id, plan.revision).unwrap();
    let path = document_path(&document);
    let before = runtime.snapshot().unwrap();
    let external = "# External scalar edit\nUnicode: \u{4e2d}\u{6587} \u{1f980}.\n";
    std::fs::write(&path, external).unwrap();
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.save_document_expected(
            &id,
            plan.revision,
            &"# Unsaved editor".into(),
            Some(&content),
        ),
    );
    unchanged_after_rejection(
        &runtime,
        &before,
        runtime.accept(&id, plan.revision, "stale-disk-review".into(), None),
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), external);
    let updated = runtime.read_document(&id, plan.revision).unwrap();
    let revision = updated
        .get("revision")
        .and_then(RuntimeJson::as_u64)
        .unwrap();
    assert_eq!(revision, plan.revision + 1);
    assert_eq!(
        updated.get("content").and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from(external))
    );
    let current = runtime.snapshot().unwrap();
    assert_eq!(current.plans[0].content, external);
    assert_eq!(current.plans[0].revision, revision);
    unchanged_after_rejection(
        &runtime,
        &current,
        runtime.read_document(&id, plan.revision),
    );
    unchanged_after_rejection(
        &runtime,
        &current,
        runtime.save_document(&id, plan.revision, &content),
    );
    unchanged_after_rejection(
        &runtime,
        &current,
        runtime.accept(&id, plan.revision, "stale-revision-review".into(), None),
    );
    assert_eq!(runtime.read_document(&id, revision).unwrap(), updated);
    let (_, input) = runtime
        .accept(
            &id,
            revision,
            "current-disk-review".into(),
            Some(external.into()),
        )
        .unwrap();
    assert_eq!(
        input,
        RuntimeString::from("PLEASE IMPLEMENT THIS PLAN:\n").concat(&external.into())
    );
    let pending = runtime.snapshot().unwrap();
    std::fs::write(&path, "# Later external edit").unwrap();
    unchanged_after_rejection(&runtime, &pending, runtime.read_document(&id, revision));
    unchanged_after_rejection(
        &runtime,
        &pending,
        runtime.save_document(&id, revision, &"# Late editor".into()),
    );
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "# Later external edit"
    );
}
