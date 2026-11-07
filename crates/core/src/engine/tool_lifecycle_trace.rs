use crate::engine::code_mode::protocol::{
    CODE_MODE_CELL_COMPLETED_EVENT, CODE_MODE_CELL_TERMINATED_EVENT, CODE_MODE_CELL_YIELDED_EVENT,
};
use crate::runtime_values::{runtime_codec, RuntimeString};
use crate::schemas::rollout::RolloutItem;
use crate::types::events::{ToolLifecycleRecord, ToolLifecycleSource};
use crate::types::json::Json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeModeCellPhase {
    Yielded,
    Completed,
    Terminated,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodeModeCellRecord {
    pub phase: CodeModeCellPhase,
    pub cell_id: RuntimeString,
    pub runtime_id: Option<RuntimeString>,
    pub ok: Option<bool>,
    pub payload: Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodeModeCellTrace {
    pub cell_id: RuntimeString,
    pub runtime_id: Option<RuntimeString>,
    pub records: Vec<CodeModeCellRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLifecycleCallTrace {
    pub tool_use_id: RuntimeString,
    pub tool_name: RuntimeString,
    pub source: Option<ToolLifecycleSource>,
    pub records: Vec<ToolLifecycleRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLifecycleTrace {
    pub tool_calls: Vec<ToolLifecycleCallTrace>,
    pub code_mode_cells: Vec<CodeModeCellTrace>,
}

pub fn tool_lifecycle_records_from_rollout(items: &[RolloutItem]) -> Vec<ToolLifecycleRecord> {
    items
        .iter()
        .filter_map(|item| {
            let RolloutItem::Event(event) = item else {
                return None;
            };
            if event.event != "tool_lifecycle" {
                return None;
            }
            event
                .payload
                .as_ref()
                .and_then(|payload| runtime_codec::decode_value(payload).ok())
        })
        .collect()
}

pub fn tool_lifecycle_trace_from_rollout(items: &[RolloutItem]) -> ToolLifecycleTrace {
    ToolLifecycleTrace {
        tool_calls: tool_lifecycle_call_traces_from_rollout(items),
        code_mode_cells: code_mode_cell_traces_from_rollout(items),
    }
}

pub fn tool_lifecycle_call_traces_from_rollout(
    items: &[RolloutItem],
) -> Vec<ToolLifecycleCallTrace> {
    let mut traces: Vec<ToolLifecycleCallTrace> = Vec::new();
    for record in tool_lifecycle_records_from_rollout(items) {
        match traces
            .iter_mut()
            .find(|trace| trace.tool_use_id == record.tool_use_id)
        {
            Some(trace) => trace.records.push(record),
            None => traces.push(ToolLifecycleCallTrace {
                tool_use_id: record.tool_use_id.clone(),
                tool_name: record.tool_name.clone(),
                source: record.source.clone(),
                records: vec![record],
            }),
        }
    }
    traces
}

pub fn code_mode_cell_traces_from_rollout(items: &[RolloutItem]) -> Vec<CodeModeCellTrace> {
    let mut traces: Vec<CodeModeCellTrace> = Vec::new();
    for record in code_mode_cell_records_from_rollout(items) {
        match traces
            .iter_mut()
            .find(|trace| trace.cell_id == record.cell_id && trace.runtime_id == record.runtime_id)
        {
            Some(trace) => trace.records.push(record),
            None => traces.push(CodeModeCellTrace {
                cell_id: record.cell_id.clone(),
                runtime_id: record.runtime_id.clone(),
                records: vec![record],
            }),
        }
    }
    traces
}

pub fn code_mode_cell_records_from_rollout(items: &[RolloutItem]) -> Vec<CodeModeCellRecord> {
    items
        .iter()
        .filter_map(|item| {
            let RolloutItem::Event(event) = item else {
                return None;
            };
            let phase = if event.event == CODE_MODE_CELL_YIELDED_EVENT {
                CodeModeCellPhase::Yielded
            } else if event.event == CODE_MODE_CELL_COMPLETED_EVENT {
                CodeModeCellPhase::Completed
            } else if event.event == CODE_MODE_CELL_TERMINATED_EVENT {
                CodeModeCellPhase::Terminated
            } else {
                return None;
            };
            let payload = event.payload.as_ref()?;
            let object = payload.as_object()?;
            let cell_id = object
                .get("cellId")
                .or_else(|| object.get("cell_id"))
                .and_then(Json::as_str)
                .filter(|value| !value.is_empty())
                .cloned()?;
            let runtime_id = object
                .get("runtimeId")
                .or_else(|| object.get("runtime_id"))
                .and_then(Json::as_str)
                .filter(|value| !value.is_empty())
                .cloned();
            let ok = object.get("ok").and_then(Json::as_bool);
            Some(CodeModeCellRecord {
                phase,
                cell_id,
                runtime_id,
                ok,
                payload: payload.clone(),
            })
        })
        .collect()
}
