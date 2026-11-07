//! Defect: the request-view tool-result budget replaces bodies only in the
//! request projection; the persisted history keeps the ORIGINAL bodies. A
//! resumed session that rebuilt its budget memory from the history alone would
//! see oversized bodies with no replacement record and either re-persist them
//! under a different body (breaking byte-identical re-apply) or freeze them
//! unreplaced (leaving the ceiling unenforced forever).
//! Root cause: nothing replayed the persisted `tool_result_budget` records on
//! resume.
//! Fixed by: store::projection::project_tool_result_budget_records, fed into
//! SessionInit on resume; ReplacementState::reconstruct freezes every
//! historical candidate id and restores the recorded bodies.

use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject};
use klaus_agent_core::schemas::rollout::{EventItem, RolloutItem};
use klaus_agent_core::store::projection::project_tool_result_budget_records;

fn budget_event(records: &[(&str, &str)]) -> RolloutItem {
    let mut payload = RuntimeObject::new();
    payload.insert(
        "records",
        RuntimeJson::Array(
            records
                .iter()
                .map(|(id, body)| {
                    RuntimeJson::Array(vec![
                        RuntimeJson::String((*id).into()),
                        RuntimeJson::String((*body).into()),
                    ])
                })
                .collect(),
        ),
    );
    RolloutItem::Event(EventItem {
        event: "tool_result_budget".into(),
        turn_id: None,
        payload: Some(RuntimeJson::Object(payload)),
    })
}

#[test]
fn resume_replays_persisted_replacement_records() {
    let items = vec![
        budget_event(&[("tu-1", "preview one")]),
        budget_event(&[("tu-2", "preview two")]),
    ];
    let records = project_tool_result_budget_records(&items);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0.try_to_utf8().unwrap(), "tu-1");
    assert_eq!(records[0].1.try_to_utf8().unwrap(), "preview one");
}

#[test]
fn a_later_record_for_the_same_id_wins() {
    let items = vec![
        budget_event(&[("tu-1", "stale body")]),
        budget_event(&[("tu-1", "refined body")]),
    ];
    let records = project_tool_result_budget_records(&items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1.try_to_utf8().unwrap(), "refined body");
}

#[test]
fn unrelated_events_and_malformed_payloads_are_ignored() {
    let mut payload = RuntimeObject::new();
    payload.insert("records", RuntimeJson::String("not an array".into()));
    let items = vec![
        RolloutItem::Event(EventItem {
            event: "tools_surfaced".into(),
            turn_id: None,
            payload: Some(RuntimeJson::Object({
                let mut other = RuntimeObject::new();
                other.insert("names", RuntimeJson::Array(Vec::new()));
                other
            })),
        }),
        RolloutItem::Event(EventItem {
            event: "tool_result_budget".into(),
            turn_id: None,
            payload: Some(RuntimeJson::Object(payload)),
        }),
    ];
    assert!(project_tool_result_budget_records(&items).is_empty());
}
