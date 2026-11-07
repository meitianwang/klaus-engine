//! Host-side orchestration for a unified, SCOPED rewind. The engine keeps
//! conversation rollback and filesystem checkpointing as separate MECHANISMS (the
//! engine provides mechanism, the host owns policy); this composes them into the
//! single "undo the last N turns, including their file edits" action exposed to
//! users.
//!
//! It maintains a per-user-turn working-tree checkpoint ledger: the host calls
//! [`RewindController::before_turn`] before each turn runs, then
//! [`RewindController::rewind`] to undo conversation and/or files together. Shared
//! by every surface so they behave identically (no duplicated logic).

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Message;
use crate::store::history_rewind::count_user_turns;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// A boxed dynamic error carrying a human-readable message via `Display` — the
/// unit of failure the rewind seam surfaces from the session's mechanisms. The
/// controller reports `err.to_string()` (equivalent to reading an `Error.message`).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The outcome of [`RewindSession::rollback`]: how many user turns the
/// conversation rollback dropped/kept, and the surviving message count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackResult {
    pub dropped_turns: usize,
    pub remaining_turns: usize,
    pub messages: usize,
}

/// The session mechanisms a [`RewindController`] composes. Implemented by the
/// engine session (via interior mutability): the controller holds a shared handle
/// and drives conversation rollback + filesystem checkpointing through it.
///
/// Method contracts (mirroring the session):
/// - `snapshot` returns `Ok(None)` when checkpointing is off / git is unavailable,
///   and `Err` on a git error (the controller treats an `Err` as "no checkpoint").
/// - `rollback` returns `Err` on a guard violation (e.g. a turn in progress); the
///   controller pre-validates `num_turns`, so only the turn-active guard can fire.
/// - `restore_checkpoint` returns `Err` when checkpointing is off or on a git error.
/// - `checkpoint_changed_files` is the read-only half of `restore_checkpoint`: same
///   failure modes, but nothing is restored.
/// - `record_checkpoint` is a no-op when checkpointing is off.
#[async_trait]
pub trait RewindSession: Send + Sync {
    /// The live conversation history (used to count user turns).
    fn history(&self) -> Vec<Message>;
    /// The resume-reconstructed per-user-turn checkpoint ledger (one entry per
    /// prior turn that recorded one).
    fn resumed_checkpoints(&self) -> Vec<Option<String>>;
    /// Snapshot the working tree, returning the restore hash (or `None` when
    /// checkpointing is off / git is unavailable).
    async fn snapshot(&self, label: &str) -> Result<Option<String>, BoxError>;
    /// Persist a per-turn checkpoint ledger entry (`None` when the snapshot could
    /// not be taken).
    async fn record_checkpoint(&self, hash: Option<String>);
    /// Roll the conversation back by `num_turns` user turns.
    async fn rollback(&self, num_turns: f64) -> Result<RollbackResult, BoxError>;
    /// Restore the working tree to a prior snapshot.
    async fn restore_checkpoint(&self, hash: &str) -> Result<(), BoxError>;
    /// Project-relative paths a restore of `hash` would touch. Changes nothing.
    async fn checkpoint_changed_files(&self, hash: &str) -> Result<Vec<String>, BoxError>;
}

/// Which halves of a rewind to undo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RewindScope {
    Conversation,
    Files,
    Both,
}

impl RuntimeEncode for RewindScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Conversation => "conversation",
            Self::Files => "files",
            Self::Both => "both",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for RewindScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("conversation", &[]),
                RuntimeEnumVariant::new("files", &[]),
                RuntimeEnumVariant::new("both", &[]),
            ],
            &[Self::Conversation, Self::Files, Self::Both],
        )
    }
}

/// The result of a [`RewindController::rewind`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewindOutcome {
    pub scope: RewindScope,
    /// User turns dropped from the conversation (0 when scope is files-only or on failure).
    pub dropped_turns: usize,
    /// User turns remaining after the rewind.
    pub remaining_turns: usize,
    /// Whether the working tree was restored to a prior checkpoint.
    pub files_restored: bool,
    /// Set when part of the rewind failed (checkpointing off, git error, or a guard).
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for RewindOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("scope", &self.scope)?;
        object.field("droppedTurns", &self.dropped_turns)?;
        object.field("remainingTurns", &self.remaining_turns)?;
        object.field("filesRestored", &self.files_restored)?;
        if let Some(error) = &self.error {
            object.field("error", error)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RewindOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                scope: object.required("scope", &[])?,
                dropped_turns: object.required("droppedTurns", &[])?,
                remaining_turns: object.required("remainingTurns", &[])?,
                files_restored: object.required("filesRestored", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// The result of a [`RewindController::preview`]: what a rewind WOULD do.
///
/// Exists because the file half of a rewind is destructive and irreversible, so a
/// host that asks the user to confirm has to be able to state the real damage
/// first. `can_rewind` false means the rewind cannot proceed at all (no snapshot
/// for that turn, checkpointing off, git missing) — the host should not offer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewindPreview {
    /// Whether the rewind can run. False leaves `files` empty and sets `error`.
    pub can_rewind: bool,
    /// Project-relative paths the file restore would touch.
    pub files: Vec<RuntimeString>,
    /// User turns the conversation rollback would drop. For a files-only rewind
    /// this is still the span being undone: the turn asked for plus every turn
    /// after it, because one restore takes the whole tree back.
    pub dropped_turns: usize,
    /// Why the rewind cannot run, when `can_rewind` is false.
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for RewindPreview {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("canRewind", &self.can_rewind)?;
        object.field("files", &self.files)?;
        object.field("droppedTurns", &self.dropped_turns)?;
        if let Some(error) = &self.error {
            object.field("error", error)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RewindPreview {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                can_rewind: object.required("canRewind", &[])?,
                files: object.required("files", &[])?,
                dropped_turns: object.required("droppedTurns", &[])?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// Composes conversation rollback and filesystem checkpointing into one scoped
/// rewind, over a shared [`RewindSession`] handle.
pub struct RewindController {
    session: Arc<dyn RewindSession>,
    /// Working-tree checkpoint per user turn (one entry per turn; `None` when checkpointing is off).
    snapshots: Vec<Option<String>>,
}

impl std::fmt::Debug for RewindController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RewindController")
            .field("snapshots", &self.snapshots)
            .finish_non_exhaustive()
    }
}

impl RewindController {
    pub fn new(session: Arc<dyn RewindSession>) -> Self {
        // Resume parity: seed from the ledger reconstructed from the rollout, so file rewind can
        // target turns from before a restart — not just seed nulls. Reconcile to the FULL user-turn
        // count `rollback` operates on: persisted checkpoints cover the most recent turns (older /
        // pre-feature turns recorded none), so pad the front with nulls, keeping index i aligned with
        // user turn i. Defensive: if there are somehow more checkpoints than turns, keep the last N.
        let persisted = session.resumed_checkpoints();
        let turns = count_user_turns(&session.history());
        let pad = turns.saturating_sub(persisted.len());
        let mut snapshots: Vec<Option<String>> = Vec::new();
        for _ in 0..pad {
            snapshots.push(None);
        }
        let start = persisted.len().saturating_sub(turns);
        snapshots.extend_from_slice(&persisted[start..]);
        RewindController { session, snapshots }
    }

    /// Capture the pre-turn working-tree checkpoint. Call BEFORE each turn runs, so a later rewind
    /// can restore files to the state entering the turn it rewinds to. `snapshot()` returns `None`
    /// when checkpointing is off / git is unavailable; an `Err` is also treated as "no checkpoint".
    pub async fn before_turn(&mut self) {
        let label = format!("turn {}", self.snapshots.len() + 1);
        let snap = self.session.snapshot(&label).await.unwrap_or(None);
        // Persist this ledger entry so a later resume can restore files to this turn (no-op when
        // checkpointing is off — the ledger only exists then).
        self.session.record_checkpoint(snap.clone()).await;
        self.snapshots.push(snap);
    }

    /// What a `rewind(num_turns, ..)` would do to the working tree, changing nothing.
    ///
    /// Resolves the SAME target turn and restore hash the rewind would, so the
    /// numbers a host shows in a confirmation prompt are the numbers it will get.
    /// Takes `&self`: not mutating the ledger is the point, not an accident.
    pub async fn preview(&self, num_turns: f64) -> RewindPreview {
        if !num_turns.is_finite() || num_turns.fract() != 0.0 || num_turns < 1.0 {
            return RewindPreview {
                can_rewind: false,
                files: Vec::new(),
                dropped_turns: 0,
                error: Some(RuntimeString::from("numTurns must be an integer >= 1")),
            };
        }
        let total = self.snapshots.len();
        let target = (total as f64 - num_turns).max(0.0) as usize;
        let dropped_turns = total.saturating_sub(target);
        let Some(hash) = self.snapshots.get(target).cloned().flatten() else {
            return RewindPreview {
                can_rewind: false,
                files: Vec::new(),
                dropped_turns,
                error: Some(RuntimeString::from(
                    "no file checkpoint to restore (checkpointing off or no prior snapshot)",
                )),
            };
        };
        match self.session.checkpoint_changed_files(&hash).await {
            Ok(files) => RewindPreview {
                can_rewind: true,
                files: files.into_iter().map(RuntimeString::from).collect(),
                dropped_turns,
                error: None,
            },
            Err(err) => RewindPreview {
                can_rewind: false,
                files: Vec::new(),
                dropped_turns,
                error: Some(RuntimeString::from(err.to_string())),
            },
        }
    }

    /// Rewind by `num_turns` user turns. Conversation is rolled back first (sync, guarded); files are
    /// restored to the checkpoint captured entering the target turn (async, may fail) — and for
    /// `Both`, the DESTRUCTIVE file restore is skipped if the conversation rollback failed, so the
    /// working tree is never reverted without the matching conversation rewind.
    ///
    /// MUST be called only when no turn is in flight (the caller serializes this against turns); the
    /// session's own turn-active guard is a backstop on the conversation half.
    pub async fn rewind(&mut self, num_turns: f64, scope: RewindScope) -> RewindOutcome {
        if !num_turns.is_finite() || num_turns.fract() != 0.0 || num_turns < 1.0 {
            return RewindOutcome {
                scope,
                dropped_turns: 0,
                remaining_turns: count_user_turns(&self.session.history()),
                files_restored: false,
                error: Some(RuntimeString::from("numTurns must be an integer >= 1")),
            };
        }

        let total = self.snapshots.len();
        let target = (total as f64 - num_turns).max(0.0) as usize;
        // Read the restore target BEFORE truncating the ledger (a conversation rollback drops it).
        let restore_hash = self.snapshots.get(target).cloned().flatten();

        let mut dropped_turns: usize = 0;
        let mut remaining_turns: usize = total;
        let mut files_restored = false;
        let mut error: Option<RuntimeString> = None;

        if scope == RewindScope::Conversation || scope == RewindScope::Both {
            match self.session.rollback(num_turns).await {
                Ok(r) => {
                    dropped_turns = r.dropped_turns;
                    remaining_turns = r.remaining_turns;
                    self.snapshots.truncate(target); // align the ledger to the kept turns
                }
                Err(err) => {
                    error = Some(RuntimeString::from(err.to_string()));
                }
            }
        }
        if scope == RewindScope::Files || (scope == RewindScope::Both && error.is_none()) {
            match restore_hash {
                Some(hash) => match self.session.restore_checkpoint(&hash).await {
                    Ok(()) => files_restored = true,
                    Err(err) => {
                        error = Some(match error {
                            Some(prev) => prev
                                .concat(&RuntimeString::from("; "))
                                .concat(&RuntimeString::from(err.to_string())),
                            None => RuntimeString::from(err.to_string()),
                        });
                    }
                },
                None => {
                    error = error.or_else(|| {
                        Some(RuntimeString::from(
                            "no file checkpoint to restore (checkpointing off or no prior snapshot)"
                        ))
                    });
                }
            }
        }

        RewindOutcome {
            scope,
            dropped_turns,
            remaining_turns,
            files_restored,
            error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::UserMessage;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    fn user_turn(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: id.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    /// A scriptable [`RewindSession`] test double that records every mechanism call.
    #[derive(Default)]
    struct FakeSession {
        history: Vec<Message>,
        resumed: Vec<Option<String>>,
        snapshot_script: Mutex<VecDeque<Result<Option<String>, String>>>,
        rollback_script: Mutex<VecDeque<Result<RollbackResult, String>>>,
        restore_script: Mutex<VecDeque<Result<(), String>>>,
        changed_files_script: Mutex<VecDeque<Result<Vec<String>, String>>>,
        snapshot_labels: Mutex<Vec<String>>,
        recorded: Mutex<Vec<Option<String>>>,
        rollback_calls: Mutex<Vec<f64>>,
        restore_calls: Mutex<Vec<String>>,
        changed_files_calls: Mutex<Vec<String>>,
    }

    impl FakeSession {
        fn new(history: Vec<Message>, resumed: Vec<Option<String>>) -> Self {
            FakeSession {
                history,
                resumed,
                ..Default::default()
            }
        }
        fn push_snapshot(&self, r: Result<Option<String>, String>) {
            self.snapshot_script.lock().unwrap().push_back(r);
        }
        fn push_rollback(&self, r: Result<RollbackResult, String>) {
            self.rollback_script.lock().unwrap().push_back(r);
        }
        fn push_restore(&self, r: Result<(), String>) {
            self.restore_script.lock().unwrap().push_back(r);
        }
        fn push_changed_files(&self, r: Result<Vec<String>, String>) {
            self.changed_files_script.lock().unwrap().push_back(r);
        }
    }

    #[async_trait]
    impl RewindSession for FakeSession {
        fn history(&self) -> Vec<Message> {
            self.history.clone()
        }
        fn resumed_checkpoints(&self) -> Vec<Option<String>> {
            self.resumed.clone()
        }
        async fn snapshot(&self, label: &str) -> Result<Option<String>, BoxError> {
            self.snapshot_labels.lock().unwrap().push(label.to_string());
            match self.snapshot_script.lock().unwrap().pop_front() {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(e.into()),
                None => Ok(None),
            }
        }
        async fn record_checkpoint(&self, hash: Option<String>) {
            self.recorded.lock().unwrap().push(hash);
        }
        async fn rollback(&self, num_turns: f64) -> Result<RollbackResult, BoxError> {
            self.rollback_calls.lock().unwrap().push(num_turns);
            match self.rollback_script.lock().unwrap().pop_front() {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(e.into()),
                None => Ok(RollbackResult {
                    dropped_turns: 0,
                    remaining_turns: 0,
                    messages: 0,
                }),
            }
        }
        async fn restore_checkpoint(&self, hash: &str) -> Result<(), BoxError> {
            self.restore_calls.lock().unwrap().push(hash.to_string());
            match self.restore_script.lock().unwrap().pop_front() {
                Some(Ok(())) => Ok(()),
                Some(Err(e)) => Err(e.into()),
                None => Ok(()),
            }
        }
        async fn checkpoint_changed_files(&self, hash: &str) -> Result<Vec<String>, BoxError> {
            self.changed_files_calls
                .lock()
                .unwrap()
                .push(hash.to_string());
            match self.changed_files_script.lock().unwrap().pop_front() {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(e.into()),
                None => Ok(Vec::new()),
            }
        }
    }

    #[tokio::test]
    async fn rejects_non_integer_or_below_one() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![],
        ));
        let mut ctrl = RewindController::new(fake.clone());
        for n in [0.0_f64, -3.0, 1.5, f64::NAN, f64::INFINITY] {
            let out = ctrl.rewind(n, RewindScope::Both).await;
            assert_eq!(
                out.error.as_ref(),
                Some(&RuntimeString::from("numTurns must be an integer >= 1"))
            );
            assert_eq!(out.dropped_turns, 0);
            assert_eq!(out.remaining_turns, 2);
            assert!(!out.files_restored);
            assert_eq!(out.scope, RewindScope::Both);
        }
        // The guard fails BEFORE touching either mechanism.
        assert!(fake.rollback_calls.lock().unwrap().is_empty());
        assert!(fake.restore_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn preview_reports_the_target_snapshot_without_touching_anything() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2"), user_turn("u3")],
            vec![Some("h1".into()), Some("h2".into()), Some("h3".into())],
        ));
        let ctrl = RewindController::new(fake.clone());
        fake.push_changed_files(Ok(vec!["src/a.rs".into(), "docs/b.md".into()]));

        let preview = ctrl.preview(2.0).await;
        assert!(preview.can_rewind);
        assert_eq!(
            preview.files,
            vec![
                RuntimeString::from("src/a.rs"),
                RuntimeString::from("docs/b.md")
            ]
        );
        // Undoing 2 turns takes the tree back to the snapshot entering turn 2, so
        // turns 2 and 3 are both inside the span the caller must be warned about.
        assert_eq!(preview.dropped_turns, 2);
        assert_eq!(preview.error, None);
        // It asked about the SAME hash a real rewind would restore.
        assert_eq!(*fake.changed_files_calls.lock().unwrap(), vec!["h2"]);
        // And it changed nothing: no rollback, no restore, ledger untouched — so a
        // real rewind afterwards still resolves the same target.
        assert!(fake.rollback_calls.lock().unwrap().is_empty());
        assert!(fake.restore_calls.lock().unwrap().is_empty());
        assert!(fake.recorded.lock().unwrap().is_empty());
        let mut ctrl = ctrl;
        fake.push_restore(Ok(()));
        let out = ctrl.rewind(2.0, RewindScope::Files).await;
        assert!(out.files_restored);
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["h2"]);
    }

    #[tokio::test]
    async fn preview_refuses_when_the_target_turn_has_no_snapshot() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![None, None],
        ));
        let ctrl = RewindController::new(fake.clone());

        let preview = ctrl.preview(1.0).await;
        assert!(!preview.can_rewind);
        assert!(preview.files.is_empty());
        assert_eq!(preview.dropped_turns, 1);
        assert_eq!(
            preview.error,
            Some(RuntimeString::from(
                "no file checkpoint to restore (checkpointing off or no prior snapshot)"
            ))
        );
        // Nothing to ask about, so the mechanism is not consulted at all.
        assert!(fake.changed_files_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn preview_surfaces_the_mechanisms_error_as_cannot_rewind() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1")],
            vec![Some("h1".into())],
        ));
        let ctrl = RewindController::new(fake.clone());
        fake.push_changed_files(Err("checkpointing is disabled".into()));

        let preview = ctrl.preview(1.0).await;
        assert!(!preview.can_rewind);
        assert!(preview.files.is_empty());
        assert_eq!(
            preview.error,
            Some(RuntimeString::from("checkpointing is disabled"))
        );
    }

    #[tokio::test]
    async fn preview_rejects_non_integer_or_below_one() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1")],
            vec![Some("h1".into())],
        ));
        let ctrl = RewindController::new(fake.clone());
        for n in [0.0_f64, -3.0, 1.5, f64::NAN, f64::INFINITY] {
            let preview = ctrl.preview(n).await;
            assert!(!preview.can_rewind);
            assert_eq!(
                preview.error.as_ref(),
                Some(&RuntimeString::from("numTurns must be an integer >= 1"))
            );
        }
        assert!(fake.changed_files_calls.lock().unwrap().is_empty());
    }

    #[test]
    fn rewind_outcome_codec_preserves_exact_error_text() {
        let error = RuntimeString::from_utf16([b'e' as u16, 0xd800]);
        let outcome = RewindOutcome {
            scope: RewindScope::Both,
            dropped_turns: 1,
            remaining_turns: 2,
            files_restored: false,
            error: Some(error.clone()),
        };
        let encoded = runtime_codec::encode_value(&outcome).unwrap();
        let decoded = runtime_codec::decode_value::<RewindOutcome>(&encoded).unwrap();

        assert_eq!(decoded, outcome);
        assert_eq!(decoded.error, Some(error));
    }

    #[tokio::test]
    async fn before_turn_snapshots_labels_and_records() {
        let fake = Arc::new(FakeSession::new(vec![], vec![]));
        fake.push_snapshot(Ok(Some("h1".to_string())));
        fake.push_snapshot(Err("git down".to_string())); // a throw is treated as no checkpoint
        let mut ctrl = RewindController::new(fake.clone());
        ctrl.before_turn().await;
        ctrl.before_turn().await;
        assert_eq!(
            *fake.snapshot_labels.lock().unwrap(),
            vec!["turn 1", "turn 2"]
        );
        // The ledger (and the persisted record) captures h1 then null.
        assert_eq!(
            *fake.recorded.lock().unwrap(),
            vec![Some("h1".to_string()), None]
        );
    }

    #[tokio::test]
    async fn both_rolls_back_then_restores_files() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![Some("c0".to_string()), Some("c1".to_string())],
        ));
        fake.push_rollback(Ok(RollbackResult {
            dropped_turns: 1,
            remaining_turns: 1,
            messages: 3,
        }));
        fake.push_restore(Ok(()));
        let mut ctrl = RewindController::new(fake.clone());
        let out = ctrl.rewind(1.0, RewindScope::Both).await;
        assert_eq!(out.dropped_turns, 1);
        assert_eq!(out.remaining_turns, 1);
        assert!(out.files_restored);
        assert_eq!(out.error, None);
        // target = 2 - 1 = 1 → restore hash is snapshots[1] = "c1".
        assert_eq!(*fake.rollback_calls.lock().unwrap(), vec![1.0]);
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["c1".to_string()]);
    }

    #[tokio::test]
    async fn both_skips_file_restore_when_rollback_fails() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![Some("c0".to_string()), Some("c1".to_string())],
        ));
        fake.push_rollback(Err(
            "cannot rollback while a turn is in progress".to_string()
        ));
        let mut ctrl = RewindController::new(fake.clone());
        let out = ctrl.rewind(1.0, RewindScope::Both).await;
        assert_eq!(
            out.error.as_ref(),
            Some(&RuntimeString::from(
                "cannot rollback while a turn is in progress"
            ))
        );
        assert!(!out.files_restored);
        assert_eq!(out.dropped_turns, 0);
        assert_eq!(out.remaining_turns, 2); // total, unchanged
                                            // The destructive restore is never attempted when the conversation half failed.
        assert!(fake.restore_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn files_only_restores_without_rollback() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![Some("c0".to_string()), Some("c1".to_string())],
        ));
        fake.push_restore(Ok(()));
        let mut ctrl = RewindController::new(fake.clone());
        let out = ctrl.rewind(1.0, RewindScope::Files).await;
        assert!(out.files_restored);
        assert_eq!(out.dropped_turns, 0);
        assert_eq!(out.remaining_turns, 2);
        assert_eq!(out.error, None);
        assert!(fake.rollback_calls.lock().unwrap().is_empty());
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["c1".to_string()]);
    }

    #[tokio::test]
    async fn files_only_without_a_checkpoint_errors() {
        // 3 turns, only 1 persisted checkpoint → seeded ledger [None, None, Some("c")].
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2"), user_turn("u3")],
            vec![Some("c".to_string())],
        ));
        let mut ctrl = RewindController::new(fake.clone());
        // rewind 3 → target 0 → snapshots[0] = None.
        let out = ctrl.rewind(3.0, RewindScope::Files).await;
        assert_eq!(
            out.error.as_ref(),
            Some(&RuntimeString::from(
                "no file checkpoint to restore (checkpointing off or no prior snapshot)"
            ))
        );
        assert!(!out.files_restored);
        assert!(fake.restore_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn seeds_ledger_padded_and_aligned_from_resumed_checkpoints() {
        // 3 turns, 1 persisted checkpoint: it aligns to the NEWEST turn (index 2).
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2"), user_turn("u3")],
            vec![Some("c".to_string())],
        ));
        fake.push_restore(Ok(()));
        let mut ctrl = RewindController::new(fake.clone());
        // rewind 1 → target 2 → snapshots[2] = "c".
        let out = ctrl.rewind(1.0, RewindScope::Files).await;
        assert!(out.files_restored);
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["c".to_string()]);
    }

    #[tokio::test]
    async fn keeps_last_checkpoints_when_more_persisted_than_turns() {
        // 1 turn but 3 persisted → keep only the last, aligned to that turn.
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1")],
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ],
        ));
        fake.push_restore(Ok(()));
        let mut ctrl = RewindController::new(fake.clone());
        let out = ctrl.rewind(1.0, RewindScope::Files).await;
        assert!(out.files_restored);
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["c".to_string()]);
    }

    #[tokio::test]
    async fn both_reports_restore_error_after_successful_rollback() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2")],
            vec![Some("c0".to_string()), Some("c1".to_string())],
        ));
        fake.push_rollback(Ok(RollbackResult {
            dropped_turns: 1,
            remaining_turns: 1,
            messages: 3,
        }));
        fake.push_restore(Err("git restore failed".to_string()));
        let mut ctrl = RewindController::new(fake.clone());
        let out = ctrl.rewind(1.0, RewindScope::Both).await;
        // The conversation half succeeded...
        assert_eq!(out.dropped_turns, 1);
        assert_eq!(out.remaining_turns, 1);
        // ...but the file half failed; the message is the raw restore error (no prior prefix).
        assert!(!out.files_restored);
        assert_eq!(
            out.error.as_ref(),
            Some(&RuntimeString::from("git restore failed"))
        );
    }

    #[tokio::test]
    async fn conversation_rollback_truncates_the_ledger() {
        let fake = Arc::new(FakeSession::new(
            vec![user_turn("u1"), user_turn("u2"), user_turn("u3")],
            vec![
                Some("c0".to_string()),
                Some("c1".to_string()),
                Some("c2".to_string()),
            ],
        ));
        fake.push_rollback(Ok(RollbackResult {
            dropped_turns: 1,
            remaining_turns: 2,
            messages: 6,
        }));
        let mut ctrl = RewindController::new(fake.clone());
        // Conversation-only: rolls back, truncates the ledger to target = 2, no file restore.
        let out = ctrl.rewind(1.0, RewindScope::Conversation).await;
        assert_eq!(out.dropped_turns, 1);
        assert_eq!(out.remaining_turns, 2);
        assert!(!out.files_restored);
        assert_eq!(out.error, None);
        assert!(fake.restore_calls.lock().unwrap().is_empty());
        // The ledger is now length 2; a following files rewind of 1 targets index 1.
        fake.push_restore(Ok(()));
        let out2 = ctrl.rewind(1.0, RewindScope::Files).await;
        assert!(out2.files_restored);
        assert_eq!(*fake.restore_calls.lock().unwrap(), vec!["c1".to_string()]);
    }
}
