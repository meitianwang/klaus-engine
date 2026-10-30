//! Persisted and host-facing background-process lifecycle schemas.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use serde::{Deserialize, Serialize};

pub const RECOVERY_INTERRUPTED_REASON: &str =
    "background process was stopped because the parent session restarted";

fn decode_unit<T: Copy>(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackgroundProcessStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Stopped,
}

impl BackgroundProcessStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Stopped)
    }
}

impl RuntimeEncode for BackgroundProcessStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Pending => "pending",
                Self::Running => "running",
                Self::Paused => "paused",
                Self::Completed => "completed",
                Self::Failed => "failed",
                Self::Stopped => "stopped",
            },
        )
    }
}

impl RuntimeDecode for BackgroundProcessStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit(
            value,
            &[
                RuntimeEnumVariant::new("pending", &[]),
                RuntimeEnumVariant::new("running", &[]),
                RuntimeEnumVariant::new("paused", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("stopped", &[]),
            ],
            &[
                Self::Pending,
                Self::Running,
                Self::Paused,
                Self::Completed,
                Self::Failed,
                Self::Stopped,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundProcessRecord {
    pub shell_id: RuntimeString,
    pub command: RuntimeString,
    pub cwd: RuntimeString,
    pub status: BackgroundProcessStatus,
    pub started_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub termination_signal: Option<RuntimeString>,
    pub failure: Option<RuntimeString>,
    pub stop_requested: bool,
    pub output_start_offset: u64,
    pub output_next_offset: u64,
    pub dropped_bytes: u64,
    pub output_available: bool,
}

impl BackgroundProcessRecord {
    pub fn interrupted(mut self, now_ms: i64, output_available: bool) -> Self {
        self.status = BackgroundProcessStatus::Stopped;
        self.updated_at_ms = now_ms;
        self.completed_at_ms = Some(now_ms);
        self.failure = Some(RECOVERY_INTERRUPTED_REASON.into());
        self.stop_requested = true;
        self.output_available = output_available;
        self
    }
}

impl RuntimeEncode for BackgroundProcessRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("shellId", &self.shell_id)?;
        object.field("command", &self.command)?;
        object.field("cwd", &self.cwd)?;
        object.field("status", &self.status)?;
        object.field("startedAtMs", &self.started_at_ms)?;
        object.field("updatedAtMs", &self.updated_at_ms)?;
        object.field("completedAtMs", &self.completed_at_ms)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("terminationSignal", &self.termination_signal)?;
        object.field("failure", &self.failure)?;
        object.field("stopRequested", &self.stop_requested)?;
        object.field("outputStartOffset", &self.output_start_offset)?;
        object.field("outputNextOffset", &self.output_next_offset)?;
        object.field("droppedBytes", &self.dropped_bytes)?;
        object.field("outputAvailable", &self.output_available)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundProcessRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                shell_id: object.required("shellId", &[])?,
                command: object.required("command", &[])?,
                cwd: object.required("cwd", &[])?,
                status: object.required("status", &[])?,
                started_at_ms: object.required("startedAtMs", &[])?,
                updated_at_ms: object.required("updatedAtMs", &[])?,
                completed_at_ms: object.optional("completedAtMs", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                termination_signal: object.optional("terminationSignal", &[])?,
                failure: object.optional("failure", &[])?,
                stop_requested: object.required("stopRequested", &[])?,
                output_start_offset: object.required("outputStartOffset", &[])?,
                output_next_offset: object.required("outputNextOffset", &[])?,
                dropped_bytes: object.required("droppedBytes", &[])?,
                output_available: object.required("outputAvailable", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackgroundProcessSnapshot {
    pub processes: Vec<BackgroundProcessRecord>,
}

impl RuntimeEncode for BackgroundProcessSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("processes", &self.processes)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundProcessSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                processes: object.required("processes", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundProcessOutput {
    pub found: bool,
    pub output: RuntimeString,
    pub after_offset: u64,
    pub next_offset: u64,
    pub dropped_bytes: u64,
    pub status: Option<BackgroundProcessStatus>,
    pub exit_code: Option<i32>,
    pub eof: bool,
}

impl RuntimeEncode for BackgroundProcessOutput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("found", &self.found)?;
        object.field("output", &self.output)?;
        object.field("afterOffset", &self.after_offset)?;
        object.field("nextOffset", &self.next_offset)?;
        object.field("droppedBytes", &self.dropped_bytes)?;
        object.field("status", &self.status)?;
        object.field("exitCode", &self.exit_code)?;
        object.field("eof", &self.eof)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundProcessOutput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                found: object.required("found", &[])?,
                output: object.required("output", &[])?,
                after_offset: object.required("afterOffset", &[])?,
                next_offset: object.required("nextOffset", &[])?,
                dropped_bytes: object.required("droppedBytes", &[])?,
                status: object.optional("status", &[])?,
                exit_code: object.optional("exitCode", &[])?,
                eof: object.required("eof", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundProcessStopResult {
    pub found: bool,
    pub accepted: bool,
    pub process: Option<BackgroundProcessRecord>,
}

impl RuntimeEncode for BackgroundProcessStopResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("found", &self.found)?;
        object.field("accepted", &self.accepted)?;
        object.field("process", &self.process)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundProcessStopResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                found: object.required("found", &[])?,
                accepted: object.required("accepted", &[])?,
                process: object.optional("process", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundProcessEventSubtype {
    TaskStarted,
    TaskProgress,
    TaskNotification,
    TaskUpdated,
    BackgroundTasksChanged,
}

impl RuntimeEncode for BackgroundProcessEventSubtype {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::TaskStarted => "task_started",
                Self::TaskProgress => "task_progress",
                Self::TaskNotification => "task_notification",
                Self::TaskUpdated => "task_updated",
                Self::BackgroundTasksChanged => "background_tasks_changed",
            },
        )
    }
}

impl RuntimeDecode for BackgroundProcessEventSubtype {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit(
            value,
            &[
                RuntimeEnumVariant::new("task_started", &[]),
                RuntimeEnumVariant::new("task_progress", &[]),
                RuntimeEnumVariant::new("task_notification", &[]),
                RuntimeEnumVariant::new("task_updated", &[]),
                RuntimeEnumVariant::new("background_tasks_changed", &[]),
            ],
            &[
                Self::TaskStarted,
                Self::TaskProgress,
                Self::TaskNotification,
                Self::TaskUpdated,
                Self::BackgroundTasksChanged,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundProcessEvent {
    pub subtype: BackgroundProcessEventSubtype,
    pub shell_id: Option<RuntimeString>,
    pub process: Option<BackgroundProcessRecord>,
    pub processes: Option<Vec<BackgroundProcessRecord>>,
    pub output: Option<RuntimeString>,
    pub after_offset: Option<u64>,
    pub next_offset: Option<u64>,
    pub dropped_bytes: Option<u64>,
}

impl RuntimeEncode for BackgroundProcessEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("subtype", &self.subtype)?;
        if let Some(value) = &self.shell_id {
            object.field("shellId", value)?;
        }
        if let Some(value) = &self.process {
            object.field("process", value)?;
        }
        if let Some(value) = &self.processes {
            object.field("processes", value)?;
        }
        if let Some(value) = &self.output {
            object.field("output", value)?;
        }
        if let Some(value) = &self.after_offset {
            object.field("afterOffset", value)?;
        }
        if let Some(value) = &self.next_offset {
            object.field("nextOffset", value)?;
        }
        if let Some(value) = &self.dropped_bytes {
            object.field("droppedBytes", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundProcessEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                subtype: object.required("subtype", &[])?,
                shell_id: object.optional("shellId", &[])?,
                process: object.optional("process", &[])?,
                processes: object.optional("processes", &[])?,
                output: object.optional("output", &[])?,
                after_offset: object.optional("afterOffset", &[])?,
                next_offset: object.optional("nextOffset", &[])?,
                dropped_bytes: object.optional("droppedBytes", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_union_includes_paused_without_using_it_as_a_runtime_transition() {
        for (status, expected) in [
            (BackgroundProcessStatus::Pending, r#""pending""#),
            (BackgroundProcessStatus::Running, r#""running""#),
            (BackgroundProcessStatus::Paused, r#""paused""#),
            (BackgroundProcessStatus::Completed, r#""completed""#),
            (BackgroundProcessStatus::Failed, r#""failed""#),
            (BackgroundProcessStatus::Stopped, r#""stopped""#),
        ] {
            assert_eq!(
                runtime_codec::encode_text(&status)
                    .unwrap()
                    .try_to_utf8()
                    .unwrap(),
                expected
            );
        }
    }
}
