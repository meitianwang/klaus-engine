//! Delivery receipts for agent messages. These records are renderer metadata,
//! never model input and never inferred from a child's token/tool stream.

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNotification {
    pub notification_id: RuntimeString,
    pub source: RuntimeString,
    pub status: RuntimeString,
    pub agent_id: Option<RuntimeString>,
    pub agent_name: Option<RuntimeString>,
    pub task_id: Option<RuntimeString>,
    pub timestamp: i64,
    pub turn_id: Option<RuntimeString>,
}

impl RuntimeEncode for AgentNotification {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("notificationId", &self.notification_id)?;
        object.field("source", &self.source)?;
        object.field("status", &self.status)?;
        object.field("timestamp", &self.timestamp)?;
        if let Some(value) = &self.turn_id {
            object.field("turnId", value)?;
        }
        if let Some(value) = &self.agent_id {
            object.field("agentId", value)?;
        }
        if let Some(value) = &self.agent_name {
            object.field("agentName", value)?;
        }
        if let Some(value) = &self.task_id {
            object.field("taskId", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for AgentNotification {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let record = Self {
                notification_id: object.required("notificationId", &[])?,
                source: object.required("source", &[])?,
                status: object.required("status", &[])?,
                agent_id: object.optional("agentId", &[])?,
                agent_name: object.optional("agentName", &[])?,
                task_id: object.optional("taskId", &[])?,
                timestamp: object.required("timestamp", &[])?,
                turn_id: object.optional("turnId", &[])?,
            };
            if record.notification_id.is_empty()
                || !["background-task", "team-mailbox"]
                    .iter()
                    .any(|value| record.source == *value)
                || !["updated", "completed", "failed", "cancelled"]
                    .iter()
                    .any(|value| record.status == *value)
            {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                    "invalid agent notification receipt".into(),
                )));
            }
            Ok(record)
        })
    }
}
