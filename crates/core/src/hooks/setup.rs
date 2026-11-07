//! Correlated, durable Setup-hook lifecycle state.
//!
//! Setup differs from ordinary lifecycle hooks in two important ways: an
//! `init` pass may succeed only once before the first real run, and the context
//! it returns is consumed by the first model prompt exactly once.  The records
//! in this module are persisted as rollout events so resume can preserve both
//! boundaries instead of re-running initialization after a process restart.

use std::collections::{BTreeMap, BTreeSet};

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

pub const SETUP_LIFECYCLE_ROLLOUT_EVENT: &str = "setup_lifecycle";

fn decode_unit_enum<T: Copy>(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupTrigger {
    Init,
    Maintenance,
}

impl RuntimeEncode for SetupTrigger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Init => "init",
                Self::Maintenance => "maintenance",
            },
        )
    }
}

impl RuntimeDecode for SetupTrigger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("init", &[]),
                RuntimeEnumVariant::new("maintenance", &[]),
            ],
            &[Self::Init, Self::Maintenance],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupRunStatus {
    Started,
    Completed,
    Failed,
    /// The hook returned, but its terminal record could not be durably
    /// acknowledged. The init must not be re-executed in this session because
    /// its external side effects may already have occurred.
    PersistenceFailed,
    Interrupted,
    /// Recovery found a durable `started` record without a terminal record.
    /// Whether the external hook completed is unknowable, so init fails closed
    /// against duplicate side effects while maintenance remains available.
    OutcomeUnknown,
    AlreadyCompleted,
    TooLate,
    RequestConflict,
    Shutdown,
}

impl SetupRunStatus {
    /// Statuses written to the durable lifecycle log for an actually admitted
    /// Setup attempt. The remaining statuses are reply-only results and must
    /// never become authoritative run history.
    fn is_persisted_record(self) -> bool {
        matches!(
            self,
            Self::Started
                | Self::Completed
                | Self::Failed
                | Self::PersistenceFailed
                | Self::Interrupted
                | Self::OutcomeUnknown
        )
    }

    fn is_persisted_terminal(self) -> bool {
        self.is_persisted_record() && self != Self::Started
    }
}

impl RuntimeEncode for SetupRunStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Started => "started",
                Self::Completed => "completed",
                Self::Failed => "failed",
                Self::PersistenceFailed => "persistence_failed",
                Self::Interrupted => "interrupted",
                Self::OutcomeUnknown => "outcome_unknown",
                Self::AlreadyCompleted => "already_completed",
                Self::TooLate => "too_late",
                Self::RequestConflict => "request_conflict",
                Self::Shutdown => "shutdown",
            },
        )
    }
}

impl RuntimeDecode for SetupRunStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("started", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("persistence_failed", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
                RuntimeEnumVariant::new("outcome_unknown", &[]),
                RuntimeEnumVariant::new("already_completed", &[]),
                RuntimeEnumVariant::new("too_late", &[]),
                RuntimeEnumVariant::new("request_conflict", &[]),
                RuntimeEnumVariant::new("shutdown", &[]),
            ],
            &[
                Self::Started,
                Self::Completed,
                Self::Failed,
                Self::PersistenceFailed,
                Self::Interrupted,
                Self::OutcomeUnknown,
                Self::AlreadyCompleted,
                Self::TooLate,
                Self::RequestConflict,
                Self::Shutdown,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetupRunRequest {
    pub request_id: RuntimeString,
    pub trigger: SetupTrigger,
}

impl RuntimeEncode for SetupRunRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", &self.request_id)?;
        object.field("trigger", &self.trigger)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SetupRunRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.required("requestId", &[])?,
                trigger: object.required("trigger", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetupRunRecord {
    pub request_id: RuntimeString,
    pub session_id: RuntimeString,
    pub trigger: SetupTrigger,
    pub status: SetupRunStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub hook_count: u64,
    pub additional_context: Vec<RuntimeString>,
    pub system_messages: Vec<RuntimeString>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for SetupRunRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", &self.request_id)?;
        object.field("sessionId", &self.session_id)?;
        object.field("trigger", &self.trigger)?;
        object.field("status", &self.status)?;
        object.field("startedAt", &self.started_at)?;
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        object.field("hookCount", &self.hook_count)?;
        object.field("additionalContext", &self.additional_context)?;
        object.field("systemMessages", &self.system_messages)?;
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SetupRunRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.required("requestId", &[])?,
                session_id: object.required("sessionId", &[])?,
                trigger: object.required("trigger", &[])?,
                status: object.required("status", &[])?,
                started_at: object.required("startedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                hook_count: object.defaulted("hookCount", &[], || 0)?,
                additional_context: object.defaulted("additionalContext", &[], Vec::new)?,
                system_messages: object.defaulted("systemMessages", &[], Vec::new)?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetupRunResult {
    pub request_id: RuntimeString,
    pub trigger: SetupTrigger,
    pub status: SetupRunStatus,
    /// The completed init request whose cached result caused
    /// `already_completed`, if different from `request_id`.
    pub source_request_id: Option<RuntimeString>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub hook_count: u64,
    pub additional_context: Vec<RuntimeString>,
    pub system_messages: Vec<RuntimeString>,
    pub error: Option<RuntimeString>,
}

impl SetupRunResult {
    pub fn from_record(record: &SetupRunRecord) -> Self {
        Self {
            request_id: record.request_id.clone(),
            trigger: record.trigger,
            status: record.status,
            source_request_id: None,
            started_at: Some(record.started_at),
            ended_at: record.ended_at,
            hook_count: record.hook_count,
            additional_context: record.additional_context.clone(),
            system_messages: record.system_messages.clone(),
            error: record.error.clone(),
        }
    }

    pub fn terminal(
        request: &SetupRunRequest,
        status: SetupRunStatus,
        source_request_id: Option<RuntimeString>,
        error: impl Into<Option<RuntimeString>>,
    ) -> Self {
        Self {
            request_id: request.request_id.clone(),
            trigger: request.trigger,
            status,
            source_request_id,
            started_at: None,
            ended_at: None,
            hook_count: 0,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: error.into(),
        }
    }
}

impl RuntimeEncode for SetupRunResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("requestId", &self.request_id)?;
        object.field("trigger", &self.trigger)?;
        object.field("status", &self.status)?;
        if let Some(value) = &self.source_request_id {
            object.field("sourceRequestId", value)?;
        }
        if let Some(value) = self.started_at {
            object.field("startedAt", &value)?;
        }
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        object.field("hookCount", &self.hook_count)?;
        object.field("additionalContext", &self.additional_context)?;
        object.field("systemMessages", &self.system_messages)?;
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SetupRunResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                request_id: object.required("requestId", &[])?,
                trigger: object.required("trigger", &[])?,
                status: object.required("status", &[])?,
                source_request_id: object.optional("sourceRequestId", &[])?,
                started_at: object.optional("startedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                hook_count: object.defaulted("hookCount", &[], || 0)?,
                additional_context: object.defaulted("additionalContext", &[], Vec::new)?,
                system_messages: object.defaulted("systemMessages", &[], Vec::new)?,
                error: object.optional("error", &[])?,
            })
        })
    }
}

/// Durable phase of the one-time Setup context hand-off to the first provider
/// request. `released` is the only terminal that permits another independent
/// turn to claim the context; every other terminal fails closed against a
/// duplicate model-visible delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupContextAdmissionStatus {
    Started,
    Released,
    Consumed,
    OutcomeUnknown,
    PersistenceFailed,
}

impl SetupContextAdmissionStatus {
    fn is_terminal(self) -> bool {
        self != Self::Started
    }

    fn blocks_context_delivery(self) -> bool {
        !matches!(self, Self::Released)
    }
}

impl RuntimeEncode for SetupContextAdmissionStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Started => "started",
                Self::Released => "released",
                Self::Consumed => "consumed",
                Self::OutcomeUnknown => "outcome_unknown",
                Self::PersistenceFailed => "persistence_failed",
            },
        )
    }
}

impl RuntimeDecode for SetupContextAdmissionStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("started", &[]),
                RuntimeEnumVariant::new("released", &[]),
                RuntimeEnumVariant::new("consumed", &[]),
                RuntimeEnumVariant::new("outcome_unknown", &[]),
                RuntimeEnumVariant::new("persistence_failed", &[]),
            ],
            &[
                Self::Started,
                Self::Released,
                Self::Consumed,
                Self::OutcomeUnknown,
                Self::PersistenceFailed,
            ],
        )
    }
}

/// Correlated durable receipt for one claim of completed init context. The
/// `request_id` identifies the init run that produced the context, while the
/// independently unique `admission_id` distinguishes a released claim from a
/// later retry of that same context.
#[derive(Debug, Clone, PartialEq)]
pub struct SetupContextAdmissionRecord {
    pub admission_id: RuntimeString,
    pub request_id: RuntimeString,
    pub session_id: RuntimeString,
    pub status: SetupContextAdmissionStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub error: Option<RuntimeString>,
}

impl RuntimeEncode for SetupContextAdmissionRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("admissionId", &self.admission_id)?;
        object.field("requestId", &self.request_id)?;
        object.field("sessionId", &self.session_id)?;
        object.field("status", &self.status)?;
        object.field("startedAt", &self.started_at)?;
        if let Some(value) = self.ended_at {
            object.field("endedAt", &value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SetupContextAdmissionRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let record = Self {
                admission_id: object.required("admissionId", &[])?,
                request_id: object.required("requestId", &[])?,
                session_id: object.required("sessionId", &[])?,
                status: object.required("status", &[])?,
                started_at: object.required("startedAt", &[])?,
                ended_at: object.optional("endedAt", &[])?,
                error: object.optional("error", &[])?,
            };
            let valid_terminal_shape = if record.status == SetupContextAdmissionStatus::Started {
                record.ended_at.is_none()
            } else {
                record.ended_at.is_some()
            };
            if !valid_terminal_shape {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                    "Setup context admission started/terminal timestamps are inconsistent".into(),
                )));
            }
            Ok(record)
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetupLifecycleEvent {
    Run {
        run: SetupRunRecord,
    },
    InitClosed {
        session_id: RuntimeString,
        at: i64,
    },
    ContextConsumed {
        session_id: RuntimeString,
        request_id: RuntimeString,
        at: i64,
    },
    ContextAdmission {
        admission: SetupContextAdmissionRecord,
    },
}

impl RuntimeEncode for SetupLifecycleEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        let kind = match self {
            Self::Run { run } => {
                object.field("run", run)?;
                "run"
            }
            Self::InitClosed { session_id, at } => {
                object.field("sessionId", session_id)?;
                object.field("at", at)?;
                "init_closed"
            }
            Self::ContextConsumed {
                session_id,
                request_id,
                at,
            } => {
                object.field("sessionId", session_id)?;
                object.field("requestId", request_id)?;
                object.field("at", at)?;
                "context_consumed"
            }
            Self::ContextAdmission { admission } => {
                object.field("admission", admission)?;
                "context_admission"
            }
        };
        runtime_codec::encode_internal(out, "kind", &[], kind, &object.finish())
    }
}

impl RuntimeDecode for SetupLifecycleEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "kind",
            &[],
            &[
                RuntimeEnumVariant::new("run", &[]),
                RuntimeEnumVariant::new("init_closed", &[]),
                RuntimeEnumVariant::new("context_consumed", &[]),
                RuntimeEnumVariant::new("context_admission", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Run {
                    run: object.required("run", &[])?,
                }),
                1 => Ok(Self::InitClosed {
                    session_id: object.required("sessionId", &[])?,
                    at: object.required("at", &[])?,
                }),
                2 => Ok(Self::ContextConsumed {
                    session_id: object.required("sessionId", &[])?,
                    request_id: object.required("requestId", &[])?,
                    at: object.required("at", &[])?,
                }),
                3 => Ok(Self::ContextAdmission {
                    admission: object.required("admission", &[])?,
                }),
                _ => unreachable!("variant index is bounded"),
            },
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SetupLifecycleSnapshot {
    pub runs: Vec<SetupRunRecord>,
    /// Exact results for terminal maintenance requests whose full lifecycle
    /// records aged out of [`Self::runs`]. This durable index is intentionally
    /// not subject to the audit-display retention limit: a request ID remains
    /// an exactly-once boundary for the lifetime of the session, including
    /// after cold replay.
    pub idempotency_results: BTreeMap<RuntimeString, SetupRunResult>,
    pub init_closed_at: Option<i64>,
    /// Latest durable claim of completed init context. A released claim may be
    /// replaced by a new `started` claim; consumed/unknown/failure terminals
    /// are immutable global no-redelivery boundaries.
    pub context_admission: Option<SetupContextAdmissionRecord>,
    /// Legacy projection retained for old snapshots and protocol consumers.
    /// New `consumed` admission events populate it as well.
    pub context_consumed_request_id: Option<RuntimeString>,
    pub context_consumed_at: Option<i64>,
}

const MAX_MAINTENANCE_RUNS: usize = 64;

impl SetupLifecycleSnapshot {
    fn context_admission_matches_completed_init(
        &self,
        admission: &SetupContextAdmissionRecord,
    ) -> bool {
        self.completed_init().is_some_and(|run| {
            run.request_id == admission.request_id && run.session_id == admission.session_id
        })
    }

    fn project_consumed_admission(&mut self, admission: &SetupContextAdmissionRecord) {
        if admission.status != SetupContextAdmissionStatus::Consumed
            || self.context_consumed_at.is_some()
        {
            return;
        }
        self.context_consumed_request_id = Some(admission.request_id.clone());
        self.context_consumed_at = admission.ended_at;
    }

    fn apply_context_admission(&mut self, admission: &SetupContextAdmissionRecord) {
        if !self.context_admission_matches_completed_init(admission) {
            return;
        }

        let accepted = match self.context_admission.as_ref() {
            None => true,
            Some(existing) if existing == admission => false,
            Some(existing) => {
                let same_identity = existing.admission_id == admission.admission_id
                    && existing.request_id == admission.request_id
                    && existing.session_id == admission.session_id
                    && existing.started_at == admission.started_at;
                let legal_same_admission_transition = same_identity
                    && match (existing.status, admission.status) {
                        (SetupContextAdmissionStatus::Started, next) => next.is_terminal(),
                        (
                            SetupContextAdmissionStatus::Released
                            | SetupContextAdmissionStatus::Consumed
                            | SetupContextAdmissionStatus::OutcomeUnknown,
                            SetupContextAdmissionStatus::PersistenceFailed,
                        ) => existing.ended_at == admission.ended_at,
                        _ => false,
                    };
                let legal_reclaim = existing.status == SetupContextAdmissionStatus::Released
                    && admission.status == SetupContextAdmissionStatus::Started
                    && existing.admission_id != admission.admission_id
                    && existing.request_id == admission.request_id
                    && existing.session_id == admission.session_id;
                legal_same_admission_transition || legal_reclaim
            }
        };
        if !accepted {
            return;
        }

        self.context_admission = Some(admission.clone());
        self.project_consumed_admission(admission);
    }

    fn apply_legacy_context_consumed(
        &mut self,
        session_id: &RuntimeString,
        request_id: &RuntimeString,
        at: i64,
    ) {
        let valid_init = self
            .completed_init()
            .is_some_and(|run| run.request_id == *request_id && run.session_id == *session_id);
        if !valid_init {
            return;
        }

        let consumed = match self.context_admission.as_ref() {
            None => Some(SetupContextAdmissionRecord {
                admission_id: "legacy-context-consumed".into(),
                request_id: request_id.clone(),
                session_id: session_id.clone(),
                status: SetupContextAdmissionStatus::Consumed,
                started_at: at,
                ended_at: Some(at),
                error: None,
            }),
            Some(existing)
                if existing.status == SetupContextAdmissionStatus::Started
                    && existing.request_id == *request_id
                    && existing.session_id == *session_id =>
            {
                let mut consumed = existing.clone();
                consumed.status = SetupContextAdmissionStatus::Consumed;
                consumed.ended_at = Some(at);
                consumed.error = None;
                Some(consumed)
            }
            Some(existing) if existing.status == SetupContextAdmissionStatus::Consumed => None,
            Some(_) => return,
        };
        if let Some(consumed) = consumed {
            self.context_admission = Some(consumed);
        }
        if self.context_consumed_at.is_none() {
            self.context_consumed_request_id = Some(request_id.clone());
            self.context_consumed_at = Some(at);
        }
    }

    pub fn apply(&mut self, event: &SetupLifecycleEvent) {
        match event {
            SetupLifecycleEvent::Run { run } => {
                // Already-completed, gate, conflict, and shutdown statuses are
                // synchronous replies, not lifecycle transitions. Accepting
                // one here would let an unauthenticated reply poison a request
                // ID's durable exactly-once boundary.
                if !run.status.is_persisted_record() {
                    return;
                }

                if let Some(existing) = self
                    .runs
                    .iter_mut()
                    .find(|existing| existing.request_id == run.request_id)
                {
                    if existing == run {
                        return;
                    }

                    // Retained request identity and terminal execution
                    // evidence are first-writer-wins. The only legal durable
                    // transitions are Started -> terminal and the follow-up
                    // PersistenceFailed marker emitted when a terminal outcome
                    // cannot cross its durability barrier.
                    let same_identity = existing.session_id == run.session_id
                        && existing.trigger == run.trigger
                        && existing.started_at == run.started_at;
                    let legal_transition = same_identity
                        && match (existing.status, run.status) {
                            (SetupRunStatus::Started, next) => next.is_persisted_terminal(),
                            (
                                SetupRunStatus::Completed
                                | SetupRunStatus::Failed
                                | SetupRunStatus::Interrupted
                                | SetupRunStatus::OutcomeUnknown,
                                SetupRunStatus::PersistenceFailed,
                            ) => {
                                existing.ended_at == run.ended_at
                                    && existing.hook_count == run.hook_count
                                    && existing.system_messages == run.system_messages
                                    // Successful context is not consumable when
                                    // its terminal outcome was not acknowledged.
                                    && run.additional_context.is_empty()
                            }
                            _ => false,
                        };
                    if legal_transition {
                        *existing = run.clone();
                    }
                    return;
                }

                if self.idempotency_results.contains_key(&run.request_id) {
                    // A receipt is created only from a terminal record. There
                    // is no valid later transition for that request, so replay
                    // must be first-writer-wins even if an old buggy producer
                    // duplicated execution or the rollout contains a forged
                    // started/terminal event.
                    return;
                }

                self.runs.push(run.clone());
                while self
                    .runs
                    .iter()
                    .filter(|run| run.trigger == SetupTrigger::Maintenance)
                    .count()
                    > MAX_MAINTENANCE_RUNS
                {
                    let Some(index) = self.runs.iter().position(|candidate| {
                        candidate.trigger == SetupTrigger::Maintenance
                            && candidate.status != SetupRunStatus::Started
                    }) else {
                        // A `started` maintenance record represents an external
                        // effect whose outcome is not yet known. Never evict it
                        // merely to satisfy the display retention limit.
                        break;
                    };
                    let evicted = self.runs.remove(index);
                    self.idempotency_results.insert(
                        evicted.request_id.clone(),
                        SetupRunResult::from_record(&evicted),
                    );
                }
            }
            SetupLifecycleEvent::InitClosed { at, .. } => {
                if self.init_closed_at.is_none() {
                    self.init_closed_at = Some(*at);
                }
            }
            SetupLifecycleEvent::ContextConsumed {
                session_id,
                request_id,
                at,
            } => self.apply_legacy_context_consumed(session_id, request_id, *at),
            SetupLifecycleEvent::ContextAdmission { admission } => {
                self.apply_context_admission(admission)
            }
        }
    }

    pub fn run(&self, request_id: &RuntimeString) -> Option<&SetupRunRecord> {
        self.runs.iter().find(|run| &run.request_id == request_id)
    }

    /// Return the exact prior result for a correlated request, whether its full
    /// audit record is still retained or it has been compacted to an
    /// idempotency result receipt.
    pub fn request_result(&self, request_id: &RuntimeString) -> Option<SetupRunResult> {
        self.run(request_id)
            .map(SetupRunResult::from_record)
            .or_else(|| self.idempotency_results.get(request_id).cloned())
    }

    pub fn completed_init(&self) -> Option<&SetupRunRecord> {
        self.runs.iter().find(|run| {
            run.trigger == SetupTrigger::Init && run.status == SetupRunStatus::Completed
        })
    }

    /// An init that must close re-execution. A successful init supplies prompt
    /// context; persistence failure and crash-recovery uncertainty do not, but
    /// both may have performed external side effects already.
    pub fn closed_init(&self) -> Option<&SetupRunRecord> {
        self.runs.iter().find(|run| {
            run.trigger == SetupTrigger::Init
                && matches!(
                    run.status,
                    SetupRunStatus::Completed
                        | SetupRunStatus::PersistenceFailed
                        | SetupRunStatus::OutcomeUnknown
                )
        })
    }

    pub fn pending_init_context(&self) -> Option<&SetupRunRecord> {
        if self.context_consumed_at.is_some() {
            return None;
        }
        if self
            .context_admission
            .as_ref()
            .is_some_and(|admission| admission.status.blocks_context_delivery())
        {
            return None;
        }
        self.completed_init()
    }

    /// A legacy rollout with a turn but no Setup lifecycle records predates the
    /// contract.  It must still fail closed for init on resume.
    pub fn close_legacy_init(&mut self) {
        if self.init_closed_at.is_none() {
            self.init_closed_at = Some(0);
        }
    }

    /// Process-local Setup hooks cannot remain running after restart. Convert
    /// each persisted `started` record to an outcome-unknown terminal so init
    /// fails closed instead of risking duplicate external side effects.
    pub fn interrupt_nonterminal_for_recovery(&mut self, now: i64) -> Vec<SetupLifecycleEvent> {
        let mut transitions = Vec::new();
        for run in &mut self.runs {
            if run.status == SetupRunStatus::Started {
                run.status = SetupRunStatus::OutcomeUnknown;
                run.ended_at = Some(now);
                run.error = Some(
                    "Setup hook outcome is unknown because the session restarted before its terminal record became durable".into(),
                );
                transitions.push(SetupLifecycleEvent::Run { run: run.clone() });
            }
        }
        if let Some(admission) = self.context_admission.as_mut() {
            if admission.status == SetupContextAdmissionStatus::Started {
                admission.status = SetupContextAdmissionStatus::OutcomeUnknown;
                admission.ended_at = Some(now);
                admission.error = Some(
                    "Setup context delivery outcome is unknown because the session restarted after its claim became durable".into(),
                );
                transitions.push(SetupLifecycleEvent::ContextAdmission {
                    admission: admission.clone(),
                });
            }
        }
        transitions
    }
}

impl RuntimeEncode for SetupLifecycleSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("runs", &self.runs)?;
        object.field("idempotencyResults", &self.idempotency_results)?;
        if let Some(value) = self.init_closed_at {
            object.field("initClosedAt", &value)?;
        }
        if let Some(value) = &self.context_admission {
            object.field("contextAdmission", value)?;
        }
        if let Some(value) = &self.context_consumed_request_id {
            object.field("contextConsumedRequestId", value)?;
        }
        if let Some(value) = self.context_consumed_at {
            object.field("contextConsumedAt", &value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SetupLifecycleSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let snapshot = Self {
                runs: object.defaulted("runs", &[], Vec::new)?,
                idempotency_results: object.defaulted("idempotencyResults", &[], BTreeMap::new)?,
                init_closed_at: object.optional("initClosedAt", &[])?,
                context_admission: object.optional("contextAdmission", &[])?,
                context_consumed_request_id: object.optional("contextConsumedRequestId", &[])?,
                context_consumed_at: object.optional("contextConsumedAt", &[])?,
            };

            let invalid = |message: &'static str| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(message)))
            };
            let mut run_request_ids = BTreeSet::new();
            let mut maintenance_runs = 0usize;
            for run in &snapshot.runs {
                if !run.status.is_persisted_record() {
                    return Err(invalid(
                        "Setup lifecycle run status must be a persisted lifecycle status",
                    ));
                }
                if !run_request_ids.insert(run.request_id.clone()) {
                    return Err(invalid(
                        "Setup lifecycle runs must have unique requestId values",
                    ));
                }
                if run.trigger == SetupTrigger::Maintenance {
                    maintenance_runs += 1;
                }
            }
            if maintenance_runs > MAX_MAINTENANCE_RUNS {
                return Err(invalid(
                    "Setup lifecycle runs exceed the maintenance retention limit",
                ));
            }

            for (request_id, result) in &snapshot.idempotency_results {
                if request_id != &result.request_id {
                    return Err(invalid(
                        "Setup idempotency receipt key must equal its requestId",
                    ));
                }
                if result.trigger != SetupTrigger::Maintenance {
                    return Err(invalid(
                        "Setup idempotency receipt must describe a maintenance request",
                    ));
                }
                if !result.status.is_persisted_terminal() {
                    return Err(invalid(
                        "Setup idempotency receipt must contain a persisted terminal status",
                    ));
                }
                if result.source_request_id.is_some() {
                    return Err(invalid(
                        "Setup idempotency receipt cannot contain sourceRequestId",
                    ));
                }
                if result.started_at.is_none() || result.ended_at.is_none() {
                    return Err(invalid(
                        "Setup idempotency receipt must contain startedAt and endedAt",
                    ));
                }
                if run_request_ids.contains(request_id) {
                    return Err(invalid(
                        "Setup requestId cannot exist in both runs and idempotencyResults",
                    ));
                }
            }

            if snapshot.context_consumed_request_id.is_some()
                != snapshot.context_consumed_at.is_some()
            {
                return Err(invalid(
                    "Setup context consumed requestId and timestamp must appear together",
                ));
            }

            if let Some(admission) = &snapshot.context_admission {
                let matches_init = snapshot.completed_init().is_some_and(|run| {
                    run.request_id == admission.request_id && run.session_id == admission.session_id
                });
                if !matches_init {
                    return Err(invalid(
                        "Setup context admission must reference the completed init run",
                    ));
                }
                if admission.status == SetupContextAdmissionStatus::Consumed {
                    if snapshot.context_consumed_request_id.as_ref() != Some(&admission.request_id)
                        || snapshot.context_consumed_at != admission.ended_at
                    {
                        return Err(invalid(
                            "Setup consumed admission must match its legacy consumed projection",
                        ));
                    }
                } else if admission.status == SetupContextAdmissionStatus::PersistenceFailed
                    && snapshot.context_consumed_at.is_some()
                {
                    if snapshot.context_consumed_request_id.as_ref() != Some(&admission.request_id)
                        || snapshot.context_consumed_at != admission.ended_at
                    {
                        return Err(invalid(
                            "Setup failed consumed admission must preserve its legacy consumed projection",
                        ));
                    }
                } else if snapshot.context_consumed_at.is_some() {
                    return Err(invalid(
                        "Setup legacy consumed projection conflicts with context admission status",
                    ));
                }
            } else if let Some(request_id) = &snapshot.context_consumed_request_id {
                if snapshot
                    .completed_init()
                    .is_none_or(|run| &run.request_id != request_id)
                {
                    return Err(invalid(
                        "Setup legacy consumed context must reference the completed init run",
                    ));
                }
            }

            Ok(snapshot)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(trigger: SetupTrigger) -> SetupRunRecord {
        SetupRunRecord {
            request_id: "request-1".into(),
            session_id: "session-1".into(),
            trigger,
            status: SetupRunStatus::Started,
            started_at: 10,
            ended_at: None,
            hook_count: 1,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        }
    }

    fn completed_maintenance_result(request_id: &str) -> SetupRunResult {
        let mut run = started(SetupTrigger::Maintenance);
        run.request_id = request_id.into();
        run.status = SetupRunStatus::Completed;
        run.ended_at = Some(20);
        SetupRunResult::from_record(&run)
    }

    fn completed_init_snapshot() -> SetupLifecycleSnapshot {
        let mut snapshot = SetupLifecycleSnapshot::default();
        let mut init = started(SetupTrigger::Init);
        init.status = SetupRunStatus::Completed;
        init.ended_at = Some(20);
        init.additional_context = vec!["context".into()];
        snapshot.apply(&SetupLifecycleEvent::Run { run: init });
        snapshot
    }

    fn context_admission(
        admission_id: &str,
        status: SetupContextAdmissionStatus,
    ) -> SetupContextAdmissionRecord {
        SetupContextAdmissionRecord {
            admission_id: admission_id.into(),
            request_id: "request-1".into(),
            session_id: "session-1".into(),
            status,
            started_at: 30,
            ended_at: (status != SetupContextAdmissionStatus::Started).then_some(31),
            error: None,
        }
    }

    fn assert_snapshot_decode_rejected(snapshot: SetupLifecycleSnapshot) {
        let encoded = runtime_codec::encode_text(&snapshot).unwrap();
        let error = runtime_codec::decode_text::<SetupLifecycleSnapshot>(&encoded).unwrap_err();
        assert!(
            matches!(error.kind(), RuntimeCodecErrorKind::Custom(_)),
            "snapshot invariant failure must use the stable custom codec error category: {error}"
        );
    }

    #[test]
    fn lifecycle_events_and_results_have_stable_exact_codecs() {
        for status in [
            SetupRunStatus::Started,
            SetupRunStatus::Completed,
            SetupRunStatus::Failed,
            SetupRunStatus::PersistenceFailed,
            SetupRunStatus::Interrupted,
            SetupRunStatus::OutcomeUnknown,
            SetupRunStatus::AlreadyCompleted,
            SetupRunStatus::TooLate,
            SetupRunStatus::RequestConflict,
            SetupRunStatus::Shutdown,
        ] {
            let encoded = runtime_codec::encode_text(&status).unwrap();
            assert_eq!(
                runtime_codec::decode_text::<SetupRunStatus>(&encoded).unwrap(),
                status
            );
        }

        for status in [
            SetupContextAdmissionStatus::Started,
            SetupContextAdmissionStatus::Released,
            SetupContextAdmissionStatus::Consumed,
            SetupContextAdmissionStatus::OutcomeUnknown,
            SetupContextAdmissionStatus::PersistenceFailed,
        ] {
            let encoded = runtime_codec::encode_text(&status).unwrap();
            assert_eq!(
                runtime_codec::decode_text::<SetupContextAdmissionStatus>(&encoded).unwrap(),
                status
            );
        }

        let mut completed = started(SetupTrigger::Init);
        completed.status = SetupRunStatus::Completed;
        completed.ended_at = Some(20);
        completed.additional_context = vec!["context".into()];
        let event = SetupLifecycleEvent::Run {
            run: completed.clone(),
        };
        let encoded = runtime_codec::encode_text(&event).unwrap();
        assert!(encoded.try_to_utf8().unwrap().contains("init"));
        assert_eq!(
            runtime_codec::decode_text::<SetupLifecycleEvent>(&encoded).unwrap(),
            event
        );

        let result = SetupRunResult::from_record(&completed);
        let encoded = runtime_codec::encode_text(&result).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<SetupRunResult>(&encoded).unwrap(),
            result
        );

        let event = SetupLifecycleEvent::ContextAdmission {
            admission: context_admission("admission-1", SetupContextAdmissionStatus::Started),
        };
        let encoded = runtime_codec::encode_text(&event).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<SetupLifecycleEvent>(&encoded).unwrap(),
            event
        );
    }

    #[test]
    fn replay_preserves_success_close_and_one_time_context_consumption() {
        let mut snapshot = SetupLifecycleSnapshot::default();
        let mut completed = started(SetupTrigger::Init);
        completed.status = SetupRunStatus::Completed;
        completed.ended_at = Some(20);
        completed.additional_context = vec!["context".into()];
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: completed.clone(),
        });
        snapshot.apply(&SetupLifecycleEvent::InitClosed {
            session_id: "session-1".into(),
            at: 30,
        });
        assert_eq!(snapshot.pending_init_context(), Some(&completed));
        snapshot.apply(&SetupLifecycleEvent::ContextConsumed {
            session_id: "session-1".into(),
            request_id: "request-1".into(),
            at: 31,
        });
        assert!(snapshot.pending_init_context().is_none());
        assert_eq!(
            snapshot
                .context_admission
                .as_ref()
                .map(|admission| admission.status),
            Some(SetupContextAdmissionStatus::Consumed),
            "legacy context-consumed events must replay into the new fail-closed admission state"
        );

        let encoded = runtime_codec::encode_text(&snapshot).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<SetupLifecycleSnapshot>(&encoded).unwrap(),
            snapshot
        );
    }

    #[test]
    fn released_context_claim_can_be_reclaimed_but_consumed_is_immutable() {
        let mut snapshot = completed_init_snapshot();
        let first_started = context_admission("admission-1", SetupContextAdmissionStatus::Started);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: first_started.clone(),
        });
        assert!(snapshot.pending_init_context().is_none());

        let mut released = first_started;
        released.status = SetupContextAdmissionStatus::Released;
        released.ended_at = Some(31);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: released,
        });
        assert!(snapshot.pending_init_context().is_some());

        let second_started = context_admission("admission-2", SetupContextAdmissionStatus::Started);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: second_started.clone(),
        });
        assert!(snapshot.pending_init_context().is_none());

        let mut consumed = second_started;
        consumed.status = SetupContextAdmissionStatus::Consumed;
        consumed.ended_at = Some(32);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: consumed.clone(),
        });
        assert_eq!(snapshot.context_admission, Some(consumed.clone()));
        assert_eq!(snapshot.context_consumed_at, Some(32));

        let forged_reclaim = context_admission("admission-3", SetupContextAdmissionStatus::Started);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: forged_reclaim,
        });
        assert_eq!(snapshot.context_admission, Some(consumed));
        assert!(snapshot.pending_init_context().is_none());

        let mut persistence_failed = snapshot.context_admission.clone().unwrap();
        persistence_failed.status = SetupContextAdmissionStatus::PersistenceFailed;
        persistence_failed.error = Some("consumed receipt flush failed".into());
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: persistence_failed.clone(),
        });
        assert_eq!(snapshot.context_admission, Some(persistence_failed));
        assert_eq!(snapshot.context_consumed_at, Some(32));

        let encoded = runtime_codec::encode_text(&snapshot).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<SetupLifecycleSnapshot>(&encoded).unwrap(),
            snapshot
        );
    }

    #[test]
    fn context_admission_rejects_wrong_init_identity_and_forged_terminal_rewrites() {
        let mut snapshot = completed_init_snapshot();
        let mut wrong = context_admission("wrong", SetupContextAdmissionStatus::Started);
        wrong.request_id = "other-init".into();
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission { admission: wrong });
        assert!(snapshot.context_admission.is_none());

        let started = context_admission("admission-1", SetupContextAdmissionStatus::Started);
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: started.clone(),
        });
        let mut unknown = started;
        unknown.status = SetupContextAdmissionStatus::OutcomeUnknown;
        unknown.ended_at = Some(31);
        unknown.error = Some("provider outcome unknown".into());
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: unknown.clone(),
        });

        let mut late_consumed = unknown.clone();
        late_consumed.status = SetupContextAdmissionStatus::Consumed;
        late_consumed.error = None;
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: late_consumed,
        });
        assert_eq!(snapshot.context_admission, Some(unknown));
        assert!(snapshot.context_consumed_at.is_none());
        assert!(snapshot.pending_init_context().is_none());
    }

    #[test]
    fn maintenance_retention_compacts_only_terminal_records_to_exact_result_receipts() {
        let mut snapshot = SetupLifecycleSnapshot::default();
        let mut first_result = None;

        for index in 0..66 {
            let request_id = RuntimeString::from(format!("maintenance-{index}"));
            let mut run = started(SetupTrigger::Maintenance);
            run.request_id = request_id.clone();
            run.started_at = 100 + index;
            snapshot.apply(&SetupLifecycleEvent::Run { run: run.clone() });
            assert!(
                snapshot
                    .runs
                    .iter()
                    .filter(|run| run.trigger == SetupTrigger::Maintenance)
                    .count()
                    <= MAX_MAINTENANCE_RUNS,
                "serialized Setup admission must compact an old terminal before retaining a new inflight run"
            );
            assert_eq!(snapshot.run(&request_id), Some(&run));
            let inflight_encoded = runtime_codec::encode_text(&snapshot).unwrap();
            assert_eq!(
                runtime_codec::decode_text::<SetupLifecycleSnapshot>(&inflight_encoded).unwrap(),
                snapshot,
                "every snapshot reachable through serialized Setup admission must satisfy the decoded retention invariant"
            );

            run.status = SetupRunStatus::Completed;
            run.ended_at = Some(200 + index);
            run.additional_context = vec![RuntimeString::from(format!("context-{index}"))];
            if index == 0 {
                first_result = Some(SetupRunResult::from_record(&run));
            }
            snapshot.apply(&SetupLifecycleEvent::Run { run });
        }

        assert_eq!(
            snapshot
                .runs
                .iter()
                .filter(|run| run.trigger == SetupTrigger::Maintenance)
                .count(),
            MAX_MAINTENANCE_RUNS
        );
        assert!(snapshot
            .run(&RuntimeString::from("maintenance-0"))
            .is_none());
        assert_eq!(
            snapshot.request_result(&RuntimeString::from("maintenance-0")),
            first_result
        );
        assert_eq!(snapshot.idempotency_results.len(), 2);

        let retained_first = first_result.clone().unwrap();
        let mut forged = started(SetupTrigger::Maintenance);
        forged.request_id = "maintenance-0".into();
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: forged.clone(),
        });
        forged.status = SetupRunStatus::Failed;
        forged.ended_at = Some(999);
        forged.additional_context = vec!["forged replacement".into()];
        snapshot.apply(&SetupLifecycleEvent::Run { run: forged });
        let mut conflicting = started(SetupTrigger::Init);
        conflicting.request_id = "maintenance-0".into();
        snapshot.apply(&SetupLifecycleEvent::Run { run: conflicting });
        assert_eq!(
            snapshot.request_result(&RuntimeString::from("maintenance-0")),
            Some(retained_first),
            "a terminal receipt is immutable under duplicate or conflicting replay events"
        );

        let encoded = runtime_codec::encode_text(&snapshot).unwrap();
        assert_eq!(
            runtime_codec::decode_text::<SetupLifecycleSnapshot>(&encoded).unwrap(),
            snapshot,
            "cold snapshot codecs must preserve evicted request receipts"
        );

        let legacy = runtime_codec::decode_text::<SetupLifecycleSnapshot>(&RuntimeString::from(
            r#"{"runs":[]}"#,
        ))
        .unwrap();
        assert!(
            legacy.idempotency_results.is_empty(),
            "snapshots written before the receipt field must remain decodable"
        );
    }

    #[test]
    fn retained_runs_accept_only_authoritative_persisted_transitions() {
        for terminal_status in [SetupRunStatus::Completed, SetupRunStatus::Failed] {
            let mut snapshot = SetupLifecycleSnapshot::default();
            let initial = started(SetupTrigger::Maintenance);
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: initial.clone(),
            });
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: initial.clone(),
            });
            assert_eq!(snapshot.runs, vec![initial.clone()]);

            let mut wrong_identity = initial.clone();
            wrong_identity.session_id = "forged-session".into();
            wrong_identity.status = terminal_status;
            wrong_identity.ended_at = Some(20);
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: wrong_identity,
            });
            assert_eq!(snapshot.runs, vec![initial.clone()]);

            let mut terminal = initial.clone();
            terminal.status = terminal_status;
            terminal.ended_at = Some(20);
            terminal.hook_count = 3;
            terminal.additional_context = vec!["context".into()];
            terminal.system_messages = vec!["hook output".into()];
            if terminal_status == SetupRunStatus::Failed {
                terminal.error = Some("hook failed".into());
            }
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: terminal.clone(),
            });
            assert_eq!(snapshot.runs, vec![terminal.clone()]);

            for forged_status in [
                SetupRunStatus::Started,
                if terminal_status == SetupRunStatus::Completed {
                    SetupRunStatus::Failed
                } else {
                    SetupRunStatus::Completed
                },
                SetupRunStatus::AlreadyCompleted,
                SetupRunStatus::OutcomeUnknown,
            ] {
                let mut forged = terminal.clone();
                forged.status = forged_status;
                forged.error = Some("forged terminal replacement".into());
                snapshot.apply(&SetupLifecycleEvent::Run { run: forged });
                assert_eq!(
                    snapshot.runs,
                    vec![terminal.clone()],
                    "{forged_status:?} must not overwrite a retained {terminal_status:?} run"
                );
            }

            let mut forged_persistence_failure = terminal.clone();
            forged_persistence_failure.status = SetupRunStatus::PersistenceFailed;
            forged_persistence_failure.hook_count += 1;
            forged_persistence_failure.additional_context.clear();
            forged_persistence_failure.error = Some("forged evidence".into());
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: forged_persistence_failure,
            });
            assert_eq!(snapshot.runs, vec![terminal.clone()]);

            let mut persistence_failure = terminal.clone();
            persistence_failure.status = SetupRunStatus::PersistenceFailed;
            persistence_failure.additional_context.clear();
            persistence_failure.error = Some("durability barrier failed".into());
            snapshot.apply(&SetupLifecycleEvent::Run {
                run: persistence_failure.clone(),
            });
            assert_eq!(snapshot.runs, vec![persistence_failure.clone()]);
            assert_eq!(persistence_failure.ended_at, terminal.ended_at);
            assert_eq!(persistence_failure.hook_count, terminal.hook_count);
            assert_eq!(
                persistence_failure.system_messages,
                terminal.system_messages
            );

            let mut later_rewrite = persistence_failure.clone();
            later_rewrite.status = SetupRunStatus::Started;
            later_rewrite.ended_at = None;
            snapshot.apply(&SetupLifecycleEvent::Run { run: later_rewrite });
            assert_eq!(snapshot.runs, vec![persistence_failure]);
        }

        let mut snapshot = SetupLifecycleSnapshot::default();
        let mut reply_only = started(SetupTrigger::Maintenance);
        reply_only.request_id = "reply-only".into();
        reply_only.status = SetupRunStatus::Shutdown;
        snapshot.apply(&SetupLifecycleEvent::Run { run: reply_only });
        assert!(snapshot.runs.is_empty());
    }

    #[test]
    fn outcome_unknown_is_terminal_and_only_its_durability_failure_can_replace_it() {
        let mut snapshot = SetupLifecycleSnapshot::default();
        let initial = started(SetupTrigger::Init);
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: initial.clone(),
        });

        let mut unknown = initial.clone();
        unknown.status = SetupRunStatus::OutcomeUnknown;
        unknown.ended_at = Some(20);
        unknown.error = Some("effect may have completed".into());
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: unknown.clone(),
        });

        let mut late_completion = unknown.clone();
        late_completion.status = SetupRunStatus::Completed;
        late_completion.additional_context = vec!["late context".into()];
        late_completion.error = None;
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: late_completion,
        });
        assert_eq!(snapshot.runs, vec![unknown.clone()]);

        let mut persistence_failure = unknown.clone();
        persistence_failure.status = SetupRunStatus::PersistenceFailed;
        persistence_failure.additional_context.clear();
        persistence_failure.error = Some("durability barrier failed".into());
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: persistence_failure.clone(),
        });
        assert_eq!(snapshot.runs, vec![persistence_failure.clone()]);

        let mut later_completion = persistence_failure.clone();
        later_completion.status = SetupRunStatus::Completed;
        later_completion.error = None;
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: later_completion,
        });
        assert_eq!(snapshot.runs, vec![persistence_failure]);
    }

    #[test]
    fn snapshot_codec_rejects_ambiguous_or_forged_idempotency_state() {
        let duplicate = started(SetupTrigger::Init);
        assert_snapshot_decode_rejected(SetupLifecycleSnapshot {
            runs: vec![duplicate.clone(), duplicate],
            ..SetupLifecycleSnapshot::default()
        });

        let mut reply_only_run = started(SetupTrigger::Maintenance);
        reply_only_run.status = SetupRunStatus::RequestConflict;
        assert_snapshot_decode_rejected(SetupLifecycleSnapshot {
            runs: vec![reply_only_run],
            ..SetupLifecycleSnapshot::default()
        });

        let maintenance_runs = (0..=MAX_MAINTENANCE_RUNS)
            .map(|index| {
                let mut run = started(SetupTrigger::Maintenance);
                run.request_id = RuntimeString::from(format!("retained-{index}"));
                run
            })
            .collect();
        assert_snapshot_decode_rejected(SetupLifecycleSnapshot {
            runs: maintenance_runs,
            ..SetupLifecycleSnapshot::default()
        });

        let assert_invalid_receipt = |key: &str, result: SetupRunResult| {
            let mut idempotency_results = BTreeMap::new();
            idempotency_results.insert(key.into(), result);
            assert_snapshot_decode_rejected(SetupLifecycleSnapshot {
                idempotency_results,
                ..SetupLifecycleSnapshot::default()
            });
        };

        assert_invalid_receipt("map-key", completed_maintenance_result("result-request"));

        let mut init_receipt = completed_maintenance_result("init-receipt");
        init_receipt.trigger = SetupTrigger::Init;
        assert_invalid_receipt("init-receipt", init_receipt);

        for invalid_status in [
            SetupRunStatus::Started,
            SetupRunStatus::AlreadyCompleted,
            SetupRunStatus::Shutdown,
        ] {
            let mut receipt = completed_maintenance_result("invalid-status");
            receipt.status = invalid_status;
            assert_invalid_receipt("invalid-status", receipt);
        }

        let mut sourced_receipt = completed_maintenance_result("sourced");
        sourced_receipt.source_request_id = Some("source".into());
        assert_invalid_receipt("sourced", sourced_receipt);

        let mut missing_started_at = completed_maintenance_result("missing-started");
        missing_started_at.started_at = None;
        assert_invalid_receipt("missing-started", missing_started_at);

        let mut missing_ended_at = completed_maintenance_result("missing-ended");
        missing_ended_at.ended_at = None;
        assert_invalid_receipt("missing-ended", missing_ended_at);

        let overlapping_run = started(SetupTrigger::Maintenance);
        let mut idempotency_results = BTreeMap::new();
        idempotency_results.insert(
            overlapping_run.request_id.clone(),
            completed_maintenance_result("request-1"),
        );
        assert_snapshot_decode_rejected(SetupLifecycleSnapshot {
            runs: vec![overlapping_run],
            idempotency_results,
            ..SetupLifecycleSnapshot::default()
        });
    }

    #[test]
    fn recovery_makes_an_inflight_init_outcome_unknown_and_non_retryable() {
        let mut snapshot = SetupLifecycleSnapshot::default();
        snapshot.apply(&SetupLifecycleEvent::Run {
            run: started(SetupTrigger::Init),
        });
        let transitions = snapshot.interrupt_nonterminal_for_recovery(99);
        assert_eq!(transitions.len(), 1);
        assert_eq!(snapshot.runs[0].status, SetupRunStatus::OutcomeUnknown);
        assert_eq!(snapshot.runs[0].ended_at, Some(99));
        assert_eq!(snapshot.closed_init(), Some(&snapshot.runs[0]));
        assert!(snapshot.pending_init_context().is_none());
    }

    #[test]
    fn recovery_makes_a_bare_context_claim_outcome_unknown_before_redelivery() {
        let mut snapshot = completed_init_snapshot();
        snapshot.apply(&SetupLifecycleEvent::ContextAdmission {
            admission: context_admission(
                "admission-before-crash",
                SetupContextAdmissionStatus::Started,
            ),
        });

        let transitions = snapshot.interrupt_nonterminal_for_recovery(99);
        assert_eq!(transitions.len(), 1);
        let Some(admission) = snapshot.context_admission.as_ref() else {
            panic!("recovered context admission must remain visible");
        };
        assert_eq!(
            admission.status,
            SetupContextAdmissionStatus::OutcomeUnknown
        );
        assert_eq!(admission.ended_at, Some(99));
        assert!(snapshot.pending_init_context().is_none());
        assert!(matches!(
            &transitions[0],
            SetupLifecycleEvent::ContextAdmission { admission }
                if admission.status == SetupContextAdmissionStatus::OutcomeUnknown
        ));
    }

    #[test]
    fn snapshot_codec_rejects_inconsistent_context_admission_state() {
        let mut missing_terminal_time = completed_init_snapshot();
        let mut admission = context_admission(
            "missing-terminal-time",
            SetupContextAdmissionStatus::Consumed,
        );
        admission.ended_at = None;
        missing_terminal_time.context_admission = Some(admission);
        missing_terminal_time.context_consumed_request_id = Some("request-1".into());
        assert!(runtime_codec::decode_text::<SetupLifecycleSnapshot>(
            &runtime_codec::encode_text(&missing_terminal_time).unwrap()
        )
        .is_err());

        let mut wrong_init = completed_init_snapshot();
        let mut admission =
            context_admission("wrong-init", SetupContextAdmissionStatus::OutcomeUnknown);
        admission.request_id = "different-init".into();
        wrong_init.context_admission = Some(admission);
        assert_snapshot_decode_rejected(wrong_init);

        let mut missing_legacy_projection = completed_init_snapshot();
        missing_legacy_projection.context_admission = Some(context_admission(
            "consumed",
            SetupContextAdmissionStatus::Consumed,
        ));
        assert_snapshot_decode_rejected(missing_legacy_projection);
    }
}
