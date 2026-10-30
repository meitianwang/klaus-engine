use klaus_agent_core::engine::tool_lifecycle_trace::{
    code_mode_cell_traces_from_rollout, tool_lifecycle_call_traces_from_rollout,
    tool_lifecycle_records_from_rollout, tool_lifecycle_trace_from_rollout, CodeModeCellPhase,
};
use klaus_agent_core::runtime_values::{runtime_codec, ScalarJson};
use klaus_agent_core::schemas::rollout::{EventItem, RolloutItem};
use klaus_agent_core::types::events::{ToolLifecyclePhase, ToolLifecycleSourceKind};
use serde_json::json;

fn event(name: &str, payload: serde_json::Value) -> RolloutItem {
    let payload = ScalarJson::validate_static_schema(payload)
        .expect("tool lifecycle test fixtures contain only scalar JSON");
    RolloutItem::Event(EventItem {
        event: name.into(),
        turn_id: Some("turn-1".into()),
        payload: Some(runtime_codec::encode_value(&payload).unwrap()),
    })
}

#[test]
fn reconstructs_unified_trace_across_tool_classes_and_code_mode_cells() {
    let items = vec![
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "read-1",
                "toolName": "read",
                "phase": "requested",
                "source": { "kind": "builtin", "exposure": "direct" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "mcp-1",
                "toolName": "server_calc",
                "phase": "requested",
                "source": { "kind": "mcp", "name": "calc_server", "exposure": "deferred" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "workflow-1",
                "toolName": "workflow",
                "phase": "started",
                "source": { "kind": "workflow", "exposure": "direct" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "task-1",
                "toolName": "task",
                "phase": "started",
                "source": { "kind": "subagent", "exposure": "direct" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "bash-1",
                "toolName": "bash",
                "phase": "started",
                "source": { "kind": "shell", "exposure": "direct" }
            }),
        ),
        event(
            "code_mode_cell_yielded",
            json!({ "cellId": "cell-1", "runtimeId": "rt-1" }),
        ),
        event(
            "code_mode_cell_completed",
            json!({ "cellId": "cell-1", "runtimeId": "rt-1", "ok": false }),
        ),
    ];

    let trace = tool_lifecycle_trace_from_rollout(&items);

    let kinds = trace
        .tool_calls
        .iter()
        .map(|call| call.source.as_ref().unwrap().kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            ToolLifecycleSourceKind::Builtin,
            ToolLifecycleSourceKind::Mcp,
            ToolLifecycleSourceKind::Workflow,
            ToolLifecycleSourceKind::Subagent,
            ToolLifecycleSourceKind::Shell,
        ]
    );
    assert_eq!(trace.code_mode_cells.len(), 1);
    assert_eq!(trace.code_mode_cells[0].cell_id, "cell-1");
    assert_eq!(
        trace.code_mode_cells[0]
            .records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![CodeModeCellPhase::Yielded, CodeModeCellPhase::Completed]
    );
}

#[test]
fn groups_code_mode_cell_events_by_cell_and_runtime() {
    let items = vec![
        event(
            "code_mode_cell_yielded",
            json!({ "cellId": "1", "runtimeId": "old" }),
        ),
        event(
            "code_mode_cell_yielded",
            json!({ "cellId": "1", "runtimeId": "new" }),
        ),
        event(
            "code_mode_cell_completed",
            json!({ "cellId": "1", "runtimeId": "old", "ok": true }),
        ),
        event(
            "code_mode_cell_terminated",
            json!({ "cellId": "1", "runtimeId": "new" }),
        ),
        event("code_mode_cell_completed", json!({ "runtimeId": "broken" })),
        event("unrelated", json!({ "cellId": "2" })),
    ];

    let traces = code_mode_cell_traces_from_rollout(&items);

    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0].cell_id, "1");
    assert!(traces[0]
        .runtime_id
        .as_ref()
        .is_some_and(|value| value == "old"));
    assert_eq!(
        traces[0]
            .records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![CodeModeCellPhase::Yielded, CodeModeCellPhase::Completed]
    );
    assert_eq!(traces[0].records[1].ok, Some(true));

    assert!(traces[1]
        .runtime_id
        .as_ref()
        .is_some_and(|value| value == "new"));
    assert_eq!(
        traces[1]
            .records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![CodeModeCellPhase::Yielded, CodeModeCellPhase::Terminated]
    );
}

#[test]
fn groups_tool_lifecycle_records_by_first_seen_tool_use_id() {
    let items = vec![
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "a",
                "toolName": "bash",
                "phase": "requested",
                "source": { "kind": "shell", "exposure": "direct" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "b",
                "toolName": "server_calc",
                "phase": "requested",
                "source": {
                    "kind": "mcp",
                    "name": "calc_server",
                    "exposure": "deferred"
                }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "a",
                "toolName": "bash",
                "phase": "started",
                "source": { "kind": "shell", "exposure": "direct" }
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "b",
                "toolName": "server_calc",
                "phase": "completed",
                "source": {
                    "kind": "mcp",
                    "name": "calc_server",
                    "exposure": "deferred"
                },
                "isError": false
            }),
        ),
    ];

    let traces = tool_lifecycle_call_traces_from_rollout(&items);

    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0].tool_use_id, "a");
    assert_eq!(traces[0].tool_name, "bash");
    assert_eq!(
        traces[0].source.as_ref().unwrap().kind,
        ToolLifecycleSourceKind::Shell
    );
    assert_eq!(
        traces[0]
            .records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![ToolLifecyclePhase::Requested, ToolLifecyclePhase::Started]
    );

    assert_eq!(traces[1].tool_use_id, "b");
    assert_eq!(
        traces[1].source.as_ref().unwrap().kind,
        ToolLifecycleSourceKind::Mcp
    );
    assert_eq!(
        traces[1]
            .records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![ToolLifecyclePhase::Requested, ToolLifecyclePhase::Completed]
    );
}

#[test]
fn reconstructs_tool_lifecycle_records_from_raw_rollout_events() {
    let items = vec![
        event("other", json!({ "ignored": true })),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "mcp-1",
                "toolName": "server_calc",
                "phase": "requested",
                "source": {
                    "kind": "mcp",
                    "name": "calc_server",
                    "exposure": "deferred"
                },
                "arguments": {}
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "bad-1",
                "toolName": "bad",
                "phase": "not_a_phase"
            }),
        ),
        event(
            "tool_lifecycle",
            json!({
                "turnId": "turn-1",
                "toolUseId": "exec-1",
                "toolName": "exec",
                "phase": "started",
                "source": {
                    "kind": "code_mode",
                    "exposure": "direct"
                }
            }),
        ),
    ];

    let records = tool_lifecycle_records_from_rollout(&items);

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].tool_use_id, "mcp-1");
    assert_eq!(records[0].phase, ToolLifecyclePhase::Requested);
    let source = records[0].source.as_ref().unwrap();
    assert_eq!(source.kind, ToolLifecycleSourceKind::Mcp);
    assert!(source
        .name
        .as_ref()
        .is_some_and(|value| value == "calc_server"));
    assert!(source
        .exposure
        .as_ref()
        .is_some_and(|value| value == "deferred"));

    assert_eq!(records[1].tool_use_id, "exec-1");
    assert_eq!(records[1].phase, ToolLifecyclePhase::Started);
    assert_eq!(
        records[1].source.as_ref().unwrap().kind,
        ToolLifecycleSourceKind::CodeMode
    );
}
