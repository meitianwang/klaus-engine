//! Named queue methods coordinate durable input with the atomic turn gate.

use super::*;
use crate::app_server::queue_store::{self, Error, Store};
use crate::app_server::queue_wire::*;
use klaus_agent_protocol::server::{
    TurnSubmission, TurnSubmissionKind, TurnSubmissionMode, TurnSubmissionOutcome,
    TurnSubmissionRejection,
};
use serde::Serialize;

const REVISION_CONFLICT: i64 = -32014;
use queue_store::INTERRUPTED_REASON;

pub(super) type InterruptChange = Arc<Mutex<Option<(String, u64, bool)>>>;

/// Reserve publication capacity before entering a queue transaction. A committed
/// mutation never waits for the output stream while retaining its file lease.
struct MutationPublication<'a> {
    permits: mpsc::PermitIterator<'a, Outgoing>,
}
impl MutationPublication<'_> {
    fn send(&mut self, message: impl Into<Payload>) {
        self.permits
            .next()
            .expect("queue publication budget")
            .send(Outgoing {
                message: message.into(),
                cursor_operation: None,
                workbuddy_operation: None,
                provider_operation: None,
            });
    }
    fn answer(&mut self, id: &Value, result: &impl Serialize) -> Result<(), Error> {
        let raw =
            RawJson::from_serialize(result).map_err(|error| Error::Storage(error.to_string()))?;
        self.send(exact_payload::response(id, raw));
        Ok(())
    }
    fn changed(&mut self, state: &AppServerState, thread: &str, revision: u64, did_change: bool) {
        if let Some(params) = change_payload(state, thread, revision, did_change) {
            if !state
                .notification_opt_out
                .lock()
                .unwrap()
                .contains("thread/queue/changed")
            {
                self.send(wire::notification("thread/queue/changed", params));
            }
        }
    }
}

pub(super) async fn publish_interrupt_change(
    state: &AppServerState,
    handle: &SessionHandle,
    submission_id: &str,
) {
    let pending = handle
        .interrupt_changes
        .lock()
        .unwrap()
        .remove(submission_id);
    let change = pending.and_then(|pending| pending.lock().unwrap().take());
    if let Some((thread, revision, did_change)) = change {
        changed(state, &thread, revision, did_change).await;
    }
}

pub(super) async fn interrupt(
    state: Arc<AppServerState>,
    id: Value,
    params: wire::TurnInterruptParams,
) {
    let Some(handle) = state.session(&params.session_id) else {
        return send_unknown_session(&state, &id, &params.session_id).await;
    };
    let submission_id = params
        .submission_id
        .unwrap_or_else(|| state.next_submission_id("sub"));
    if submission_id.is_empty() {
        return send_invalid_params_message(&state, &id, "submissionId must not be empty").await;
    }
    if handle.turns.lock().unwrap().contains(&submission_id)
        || handle.waiters.lock().unwrap().contains_key(&submission_id)
        || handle
            .ordered_responses
            .lock()
            .unwrap()
            .contains_key(&submission_id)
    {
        return state
            .send(wire::error_response(
                &id,
                wire::DUPLICATE_SUBMISSION,
                "submissionId already in flight",
            ))
            .await;
    }
    let store = store_for(&state);
    let mut op = serde_json::json!({"type":"interrupt"});
    if let Some(origin) = params.origin {
        op["origin"] = serde_json::to_value(origin).expect("interrupt origin");
    }
    if let Some(expected) = params.expected_turn_id {
        op["expectedTurnId"] = expected.into();
    }
    let pending_change: InterruptChange = Arc::new(Mutex::new(None));
    let committed_change = pending_change.clone();
    let prepare = Box::new(move || {
        Box::pin(async move {
            let lock = store
                .lock(&params.session_id)
                .await
                .map_err(|error| format!("durable queue pause failed: {error}"))?;
            let commit: klaus_agent_protocol::server::InterruptCommit = Box::new(move || {
                let _lock = lock;
                let (_, revision, did_change) = store
                    .transact(&params.session_id, |document| {
                        let mut did_change = false;
                        for row in &mut document.rows {
                            if row.dispatching.is_none() && row.paused_reason.is_none() {
                                row.paused_reason = Some(INTERRUPTED_REASON.into());
                                did_change = true;
                            }
                        }
                        Ok(((), did_change))
                    })
                    .map_err(|error| format!("durable queue pause failed: {error}"))?;
                // Publication belongs to the async protocol pump after this commit.
                *committed_change.lock().unwrap() =
                    Some((params.session_id.clone(), revision, did_change));
                Ok(())
            });
            Ok(commit)
        })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<klaus_agent_protocol::server::InterruptCommit, String>,
                        > + Send,
                >,
            >
    });
    if let Err(error) = handle
        .ps
        .register_interrupt_commit(submission_id.clone(), prepare)
    {
        return state
            .send(wire::error_response(&id, wire::ENGINE_ERROR, &error))
            .await;
    }
    handle
        .interrupt_changes
        .lock()
        .unwrap()
        .insert(submission_id.clone(), pending_change);
    // Keep the original reader ordering relative to turn/start and controls.
    // The persistence preparation acquires no lock until this op is dispatched.
    if handle
        .op_tx
        .send(Payload::Runtime(exact_payload::object([
            ("id", submission_id.clone().into()),
            ("op", exact_payload::runtime(op)),
        ])))
        .is_err()
    {
        handle.ps.unregister_interrupt_commit(&submission_id);
        handle
            .interrupt_changes
            .lock()
            .unwrap()
            .remove(&submission_id);
        return state
            .send(wire::error_response(
                &id,
                wire::ENGINE_ERROR,
                "interrupt endpoint is closed",
            ))
            .await;
    }
    state
        .send(wire::response(
            &id,
            serde_json::json!({"submissionId":submission_id}),
        ))
        .await;
}

fn store_for(state: &AppServerState) -> Store {
    state.queue_store.clone()
}

pub(super) fn wrap_lifecycle(
    mut lifecycle: Option<ProtocolLifecycle>,
    store: Store,
) -> Option<ProtocolLifecycle> {
    if let Some(lifecycle) = &mut lifecycle {
        if let Some(delete) = lifecycle.delete_session.take() {
            lifecycle.delete_session = Some(Arc::new(move |thread| {
                let delete = delete.clone();
                let store = store.clone();
                Box::pin(async move {
                    let _lock = store
                        .lock(&thread)
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                    let result = delete(thread.clone()).await?;
                    // An already absent thread is also a successful cleanup.
                    // Clear receipts here, before the delete reply can escape.
                    store
                        .clear(&thread)
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                    Ok(result)
                })
            }));
        }
    }
    lifecycle
}

pub(super) async fn notify_deleted(state: &AppServerState, thread: &str) {
    if let Ok(document) = store_for(state).read(thread) {
        changed(state, thread, document.revision, true).await;
    }
}

/// The watcher retains one SQLite connection: PRAGMA data_version only reports
/// external commits relative to that same connection. Its task is explicitly
/// aborted and joined at connection teardown.
pub(super) fn watch(state: &Arc<AppServerState>) -> tokio::task::JoinHandle<()> {
    let weak = Arc::downgrade(state);
    let store = store_for(state);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut connection = None;
        let mut version = None;
        loop {
            interval.tick().await;
            let Some(state) = weak.upgrade() else {
                return;
            };
            if !state.initialized.load(Ordering::Acquire) {
                continue;
            }
            if connection.is_none() {
                connection = store.watch_connection().ok().flatten();
            }
            let Some(observer) = connection.as_ref() else {
                continue;
            };
            let Ok(current) = Store::data_version(observer) else {
                connection = None;
                version = None;
                continue;
            };
            if version == Some(current) {
                continue;
            }
            if let Ok(revisions) = Store::revisions(observer) {
                version = Some(current);
                for (thread, revision) in revisions {
                    changed(&state, &thread, revision, true).await;
                }
            } else {
                connection = None;
                version = None;
            }
        }
    })
}

enum Request {
    List(ThreadQueueListParams),
    Add(ThreadQueueAddParams),
    Update(ThreadQueueUpdateParams),
    Delete(ThreadQueueDeleteParams),
    Reorder(ThreadQueueReorderParams),
    Start(ThreadQueueStartParams),
    Pause(ThreadQueuePauseParams),
    Resume(ThreadQueueResumeParams),
    Import(ThreadQueueImportParams),
    Discard(ThreadQueueDiscardParams),
    Clear(ThreadQueueClearParams),
}

impl Request {
    fn thread(&self) -> &str {
        match self {
            Self::List(p) => &p.thread_id,
            Self::Add(p) => &p.thread_id,
            Self::Update(p) => &p.thread_id,
            Self::Delete(p) => &p.thread_id,
            Self::Reorder(p) => &p.thread_id,
            Self::Start(p) => &p.thread_id,
            Self::Pause(p) => &p.thread_id,
            Self::Resume(p) => &p.thread_id,
            Self::Import(p) => &p.thread_id,
            Self::Discard(p) => &p.thread_id,
            Self::Clear(p) => &p.thread_id,
        }
    }
}

pub(super) async fn handle(
    state: &Arc<AppServerState>,
    id: &Value,
    method: &str,
    params: &RawJson,
) -> bool {
    macro_rules! decode {
        ($variant:ident, $ty:ty) => {
            params.deserialize::<$ty>().map(Request::$variant)
        };
    }
    let request = match method {
        "thread/queue/list" => decode!(List, ThreadQueueListParams),
        "thread/queue/add" => decode!(Add, ThreadQueueAddParams),
        "thread/queue/update" => decode!(Update, ThreadQueueUpdateParams),
        "thread/queue/delete" => decode!(Delete, ThreadQueueDeleteParams),
        "thread/queue/reorder" => decode!(Reorder, ThreadQueueReorderParams),
        "thread/queue/start" => decode!(Start, ThreadQueueStartParams),
        "thread/queue/pause" => decode!(Pause, ThreadQueuePauseParams),
        "thread/queue/resume" => decode!(Resume, ThreadQueueResumeParams),
        "thread/queue/import" => decode!(Import, ThreadQueueImportParams),
        "thread/queue/clear" => decode!(Clear, ThreadQueueClearParams),
        "thread/queue/discard" => decode!(Discard, ThreadQueueDiscardParams),
        _ => return false,
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => {
            send_invalid_params_message(state, id, &error.to_string()).await;
            return true;
        }
    };
    if let Err(error) = process(state, id, request).await {
        send_error(state, id, error).await;
    }
    true
}

async fn send_error(state: &AppServerState, id: &Value, error: Error) {
    if let Error::Conflict { expected, actual } = error {
        state
            .send(serde_json::json!({"id":id,"error":{
                "code":REVISION_CONFLICT,"message":"queue revision conflict",
                "data":{"expectedRevision":expected,"revision":actual}
            }}))
            .await;
    } else {
        let code = if matches!(error, Error::Invalid(_)) {
            wire::INVALID_PARAMS
        } else {
            wire::ENGINE_ERROR
        };
        state
            .send(wire::error_response(id, code, &error.to_string()))
            .await;
    }
}

async fn answer(state: &AppServerState, id: &Value, result: &impl Serialize) -> Result<(), Error> {
    let raw = RawJson::from_serialize(result).map_err(|e| Error::Storage(e.to_string()))?;
    state.send(exact_payload::response(id, raw)).await;
    Ok(())
}

fn change_payload(
    state: &AppServerState,
    thread: &str,
    revision: u64,
    did_change: bool,
) -> Option<Value> {
    if !did_change {
        return None;
    }
    let mut revisions = state.queue_revisions.lock().unwrap();
    if revisions
        .get(thread)
        .is_some_and(|known| *known >= revision)
    {
        return None;
    }
    revisions.insert(thread.to_owned(), revision);
    Some(serde_json::json!({"threadId":thread,"revision":revision}))
}

async fn changed(state: &AppServerState, thread: &str, revision: u64, did_change: bool) {
    if let Some(params) = change_payload(state, thread, revision, did_change) {
        state.notify("thread/queue/changed", params).await;
    }
}

async fn process(state: &Arc<AppServerState>, id: &Value, request: Request) -> Result<(), Error> {
    let store = store_for(state);
    let thread = request.thread().to_owned();
    queue_store::nonempty(&thread, "threadId")?;
    if let Request::Add(params) = &request {
        if params.lookup_only {
            return answer(state, id, &store.lookup(params)?).await;
        }
    }
    if let Request::List(params) = request {
        let (result, revision) = if params.include_input {
            let result = store.list(params)?;
            let revision = result.revision;
            (
                RawJson::from_serialize(&result)
                    .map_err(|error| Error::Storage(error.to_string()))?,
                revision,
            )
        } else {
            store.list_projected(params)?
        };
        state
            .queue_revisions
            .lock()
            .unwrap()
            .entry(thread.clone())
            .and_modify(|known| *known = (*known).max(revision))
            .or_insert(revision);
        state.send(exact_payload::response(id, result)).await;
        return Ok(());
    }
    // This lock is independent of the turn serial chain. Lists bypass it, and
    // all metadata mutations wait only for another queue mutation/admission.
    let Ok(permits) = state.out_tx.reserve_many(3).await else {
        return Ok(());
    };
    let mut publication = MutationPublication { permits };
    let _lock = store.lock(&thread).await?;
    match request {
        Request::List(_) => unreachable!(),
        Request::Add(params) => {
            let (result, did_change) = store.add(params)?;
            publication.changed(state, &thread, result.revision, did_change);
            publication.answer(id, &result)
        }
        Request::Delete(params) => {
            let (result, did_change) = store.delete(params)?;
            publication.changed(state, &thread, result.revision, did_change);
            publication.answer(id, &result)
        }
        Request::Update(params) => {
            let include_input = params.include_input;
            let row_id = params.queued_submission_id.clone();
            let (_row, revision, did_change) = store.transact(&thread, |document| {
                document.check_revision(params.expected_revision)?;
                let index = document.row_index(&params.queued_submission_id)?;
                document.require_mutable(index)?;
                let row = &mut document.rows[index];
                if let Some(input) = params.input {
                    row.input = input;
                    row.input_omitted = false;
                }
                if let Some(metadata) = params.client_metadata {
                    row.client_metadata = Some(metadata);
                }
                if let Some(reason) = params.paused_reason {
                    row.paused_reason = reason;
                }
                Ok((row.clone(), true))
            })?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(
                id,
                &ThreadQueueItemResult {
                    queued_submission: store.entry(&thread, &row_id, include_input)?,
                    revision,
                    replayed: false,
                    receipt: None,
                    found: None,
                },
            )
        }
        Request::Reorder(params) => {
            let (_, revision, did_change) = store.transact(&thread, |document| {
                document.check_revision(params.expected_revision)?;
                let requested: HashSet<_> = params.queued_submission_ids.iter().collect();
                if requested.len() != params.queued_submission_ids.len()
                    || requested.len() != document.rows.len()
                    || document.rows.iter().any(|row| !requested.contains(&row.id))
                {
                    return Err(Error::Invalid(
                        "reorder must contain every queued submission exactly once".into(),
                    ));
                }
                let did_change = document
                    .rows
                    .iter()
                    .map(|row| &row.id)
                    .ne(params.queued_submission_ids.iter());
                let mut rows: HashMap<_, _> = std::mem::take(&mut document.rows)
                    .into_iter()
                    .map(|row| (row.id.clone(), row))
                    .collect();
                document.rows = params
                    .queued_submission_ids
                    .into_iter()
                    .map(|id| rows.remove(&id).unwrap())
                    .collect();
                Ok(((), did_change))
            })?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(id, &ThreadQueueMutationResult { revision })
        }
        Request::Pause(params) => {
            queue_store::nonempty(&params.reason, "reason")?;
            let (_, revision, did_change) = store.transact(&thread, |document| {
                document.check_revision(params.expected_revision)?;
                validate_selection(document, params.queued_submission_ids.as_deref())?;
                let mut did_change = false;
                for row in &mut document.rows {
                    if selected(row, params.queued_submission_ids.as_deref())
                        && row.dispatching.is_none()
                        && row.paused_reason.as_deref() != Some(&params.reason)
                    {
                        row.paused_reason = Some(params.reason.clone());
                        did_change = true;
                    }
                }
                Ok(((), did_change))
            })?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(id, &ThreadQueueMutationResult { revision })
        }
        Request::Resume(params) => {
            let (_, revision, did_change) = store.transact(&thread, |document| {
                document.check_revision(params.expected_revision)?;
                validate_selection(document, params.queued_submission_ids.as_deref())?;
                let mut did_change = false;
                for row in &mut document.rows {
                    if selected(row, params.queued_submission_ids.as_deref())
                        && row.dispatching.is_none()
                        && row.paused_reason.is_some()
                        && params
                            .reason
                            .as_ref()
                            .is_none_or(|reason| row.paused_reason.as_ref() == Some(reason))
                    {
                        row.paused_reason = None;
                        did_change = true;
                    }
                }
                Ok(((), did_change))
            })?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(id, &ThreadQueueMutationResult { revision })
        }
        Request::Import(params) => {
            let (revision, did_change) = store.import(params)?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(id, &ThreadQueueMutationResult { revision })
        }
        Request::Clear(params) => {
            let (result, did_change) = store.clear_rows(params)?;
            publication.changed(state, &thread, result.revision, did_change);
            publication.answer(id, &result)
        }
        Request::Discard(params) => {
            let (revision, did_change) = store.discard(&thread, params.expected_revision)?;
            publication.changed(state, &thread, revision, did_change);
            publication.answer(id, &ThreadQueueMutationResult { revision })
        }
        Request::Start(params) => start(state, id, &store, params, &mut publication).await,
    }
}

fn selected(row: &QueuedSubmission, ids: Option<&[String]>) -> bool {
    ids.is_none_or(|ids| ids.contains(&row.id))
}

fn validate_selection(
    document: &queue_store::Document,
    ids: Option<&[String]>,
) -> Result<(), Error> {
    if let Some(ids) = ids {
        for id in ids {
            document.row_index(id)?;
        }
    }
    Ok(())
}

fn reason(code: wire::TurnSubmitReasonCode, message: impl Into<String>) -> wire::TurnSubmitReason {
    wire::TurnSubmitReason {
        code,
        message: message.into(),
        active_turn_id: None,
        turn_kind: None,
    }
}

fn rejection(
    outcome: TurnSubmissionOutcome,
) -> (
    wire::TurnSubmitStatus,
    Option<String>,
    Option<wire::TurnSubmitReason>,
) {
    use wire::{TurnSubmitKind as Kind, TurnSubmitReasonCode as Code, TurnSubmitStatus as Status};
    match outcome {
        TurnSubmissionOutcome::Started { turn_id } => (Status::Started, Some(turn_id), None),
        TurnSubmissionOutcome::Steered { turn_id } => (Status::Steered, Some(turn_id), None),
        TurnSubmissionOutcome::NotSubmitted {
            code,
            message,
            active_turn_id,
            turn_kind,
        } => (
            Status::NotSubmitted,
            None,
            Some(wire::TurnSubmitReason {
                code: match code {
                    TurnSubmissionRejection::NoActiveTurn => Code::NoActiveTurn,
                    TurnSubmissionRejection::ExpectedTurnMismatch => Code::ExpectedTurnMismatch,
                    TurnSubmissionRejection::NotIdle => Code::NotIdle,
                    TurnSubmissionRejection::SessionClosing => Code::SessionClosing,
                    TurnSubmissionRejection::ActiveTurnNotSteerable => Code::ActiveTurnNotSteerable,
                    TurnSubmissionRejection::EmptyInput => Code::EmptyInput,
                    TurnSubmissionRejection::InvalidInput => Code::InvalidInput,
                },
                message,
                active_turn_id,
                turn_kind: turn_kind.map(|kind| match kind {
                    TurnSubmissionKind::Review => Kind::Review,
                    TurnSubmissionKind::Compact => Kind::Compact,
                }),
            }),
        ),
    }
}

async fn start(
    state: &Arc<AppServerState>,
    id: &Value,
    store: &Store,
    params: ThreadQueueStartParams,
    publication: &mut MutationPublication<'_>,
) -> Result<(), Error> {
    use wire::{TurnSubmitReasonCode as Code, TurnSubmitStatus as Status};
    let document = store.read_projected(&params.thread_id, false)?;
    document.check_revision(params.expected_revision)?;
    let index = if let Some(id) = &params.queued_submission_id {
        document.row_index(id)?
    } else if document.rows.is_empty() {
        return Err(Error::Invalid("thread queue is empty".into()));
    } else {
        0
    };
    let original = store
        .entry(&params.thread_id, &document.rows[index].id, true)?
        .ok_or_else(|| Error::Invalid("queued submission disappeared".into()))?;
    let observed_revision = document.revision;
    let submission_id = params
        .submission_id
        .clone()
        .unwrap_or_else(|| format!("queue-{}", uuid::Uuid::new_v4()));
    queue_store::nonempty(&submission_id, "submissionId")?;
    let mode = params.mode.unwrap_or(wire::TurnSubmitMode::StartIfIdle);
    let not_submitted = |reason| ThreadQueueStartResult {
        status: Status::NotSubmitted,
        submission_id: submission_id.clone(),
        turn_id: None,
        turn: None,
        reason: Some(reason),
        queued_submission: projected(original.clone(), params.include_input),
        revision: document.revision,
    };
    if params
        .expected_input_fingerprint
        .as_ref()
        .is_some_and(|v| v != &original.input_fingerprint)
        || params
            .expected_row_version
            .is_some_and(|v| v != original.row_version)
    {
        return publication.answer(
            id,
            &not_submitted(reason(Code::QueueEntryChanged, "queued submission changed")),
        );
    }
    if !matches!(
        mode,
        wire::TurnSubmitMode::StartIfIdle | wire::TurnSubmitMode::Steer
    ) {
        return publication.answer(
            id,
            &not_submitted(reason(
                Code::InvalidInput,
                "queue start requires start-if-idle or steer",
            )),
        );
    }
    if original.dispatching.is_some() && !params.retry_unconfirmed {
        return publication.answer(
            id,
            &not_submitted(reason(
                Code::AdmissionUnknown,
                "previous admission is unconfirmed; an explicit retry is required",
            )),
        );
    }
    if document
        .rows
        .iter()
        .any(|row| row.id != original.id && row.dispatching.is_some())
    {
        return publication.answer(
            id,
            &not_submitted(reason(
                Code::AdmissionUnknown,
                "another queued admission is unconfirmed",
            )),
        );
    }
    if original.dispatching.is_none() && original.paused_reason.is_some() {
        return publication.answer(
            id,
            &not_submitted(reason(Code::QueuePaused, "queued submission is paused")),
        );
    }
    let Some(handle) = state.session(&params.thread_id) else {
        return publication.answer(
            id,
            &not_submitted(reason(
                Code::SessionUnavailable,
                "thread has no live session; initialize it before starting queued input",
            )),
        );
    };
    // Do not rebuild scalar JSON: input and display metadata may carry exact
    // UTF-16 strings and arbitrary JSON number spelling.
    let mut op = exact_payload::object([
        ("type", "run".into()),
        (
            "input",
            RuntimeJson::Array(original.input.iter().map(|part| part.0.clone()).collect()),
        ),
    ]);
    if let Some(options) = params.options {
        op.as_object_mut().unwrap().insert("options", options.0);
    }
    let decoded = klaus_agent_core::runtime_values::runtime_codec::decode_value::<
        klaus_agent_protocol::types::OpKind,
    >(&op);
    let (mut input, options) = match decoded {
        Ok(klaus_agent_protocol::types::OpKind::Run { input, options }) => (input, options),
        Ok(_) => unreachable!(),
        Err(error) => {
            return publication.answer(
                id,
                &not_submitted(reason(Code::InvalidInput, error.to_string())),
            )
        }
    };
    if matches!(mode, wire::TurnSubmitMode::Steer) && params.delivery.as_deref() == Some("user") {
        use klaus_agent_core::engine::session::{Input, UserContentPart};
        let marker = UserContentPart::Text {
            text: "<!-- klaus-steered-message -->\n".into(),
        };
        match &mut input {
            Input::Parts(parts) => parts.insert(0, marker),
            Input::Text(text) => {
                *text = RuntimeString::from("<!-- klaus-steered-message -->\n").concat(text);
            }
        }
    }
    let delivery = params
        .delivery
        .map(|value| klaus_agent_core::runtime_values::runtime_codec::decode_value(&value.into()))
        .transpose()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let display_metadata = params.display_metadata.map(|value| value.0).or_else(|| {
        original
            .client_metadata
            .as_ref()
            .map(|value| exact_field(exact_field(&value.0, "item"), "displayMetadata").clone())
            .filter(|value| !matches!(value, RuntimeJson::Null))
    });
    let duplicate = {
        let mut turns = handle.turns.lock().unwrap();
        handle.waiters.lock().unwrap().contains_key(&submission_id)
            || handle
                .ordered_responses
                .lock()
                .unwrap()
                .contains_key(&submission_id)
            || !turns.insert(submission_id.clone())
    };
    if duplicate {
        return publication.answer(
            id,
            &not_submitted(reason(Code::InvalidInput, "submissionId already in flight")),
        );
    }
    let reserve = store.transact(&params.thread_id, |document| {
        document.check_revision(Some(observed_revision))?;
        if document
            .rows
            .iter()
            .any(|row| row.id != original.id && row.dispatching.is_some())
        {
            return Err(Error::Invalid(
                "another queued admission is unconfirmed".into(),
            ));
        }
        let index = document.row_index(&original.id)?;
        let row = &mut document.rows[index];
        if params
            .expected_input_fingerprint
            .as_ref()
            .is_some_and(|v| v != &row.input_fingerprint)
            || params
                .expected_row_version
                .is_some_and(|v| v != row.row_version)
        {
            return Err(Error::EntryChanged);
        }
        row.dispatching = Some(QueueDispatch {
            submission_id: submission_id.clone(),
            mode,
        });
        row.paused_reason = Some("admission-unknown".into());
        Ok(((), true))
    });
    let (_, revision, did_change) = match reserve {
        Ok(result) => result,
        Err(error) => {
            handle.turns.lock().unwrap().remove(&submission_id);
            if matches!(error, Error::EntryChanged) {
                return publication.answer(
                    id,
                    &not_submitted(reason(Code::QueueEntryChanged, "queued submission changed")),
                );
            }
            return Err(error);
        }
    };
    publication.changed(state, &params.thread_id, revision, did_change);
    let preparing = state.request_queues.preparing_session(&params.thread_id);
    let outcome = handle
        .ps
        .submit_turn(TurnSubmission {
            submission_id: submission_id.clone(),
            input,
            options,
            mode: match mode {
                wire::TurnSubmitMode::Steer => TurnSubmissionMode::Steer,
                _ => TurnSubmissionMode::StartIfIdle,
            },
            expected_turn_id: params.expected_turn_id,
            queue_message_id: params.queue_message_id,
            delivery,
            display_metadata,
        })
        .await;
    drop(preparing);
    let (status, turn_id, refusal) = rejection(outcome);
    if status != Status::Started {
        handle.turns.lock().unwrap().remove(&submission_id);
    }
    let (_, revision, did_change) = store.transact(&params.thread_id, |document| {
        let index = document.row_index(&original.id)?;
        if document.rows[index]
            .dispatching
            .as_ref()
            .map(|dispatch| &dispatch.submission_id)
            != Some(&submission_id)
        {
            return Err(Error::Storage(
                "queue dispatch identity changed before settlement".into(),
            ));
        }
        if status == Status::NotSubmitted {
            let version = document.rows[index].row_version;
            document.rows[index] = projected(original.clone(), false);
            document.rows[index].row_version = version;
            // Definite non-admission permits a future explicit start. Invalid
            // model input and closing sessions remain visible as paused rows.
            document.rows[index].dispatching = None;
            if original.dispatching.is_some() {
                document.rows[index].paused_reason = None;
            }
            if let Some(refusal) = &refusal {
                if matches!(
                    refusal.code,
                    Code::InvalidInput | Code::EmptyInput | Code::SessionClosing
                ) {
                    document.rows[index].paused_reason = Some(
                        match refusal.code {
                            Code::SessionClosing => "session-closing",
                            _ => "invalid-input",
                        }
                        .into(),
                    );
                }
            }
        } else {
            document.rows.remove(index);
            let mut receipt = document.receipt(
                original.clone(),
                if status == Status::Started {
                    "started"
                } else {
                    "steered"
                },
            );
            receipt.submission_id = Some(submission_id.clone());
            receipt.turn_id = turn_id.clone();
            document.remember(receipt, None);
        }
        Ok(((), true))
    })?;
    publication.changed(state, &params.thread_id, revision, did_change);
    publication.answer(
        id,
        &ThreadQueueStartResult {
            status,
            submission_id,
            turn: turn_id.as_ref().map(|id| QueueTurn {
                id: id.clone(),
                status: "inProgress".into(),
            }),
            turn_id,
            reason: refusal,
            queued_submission: projected(original, params.include_input),
            revision,
        },
    )
}

fn projected(mut row: QueuedSubmission, include: bool) -> QueuedSubmission {
    if !include {
        row.input.clear();
        row.input_omitted = true;
    }
    row
}
