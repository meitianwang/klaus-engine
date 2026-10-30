//! Defect: a running background shell stranded an active goal before its blocked audit.
//! Root cause: automatic idle admission treated shell lifetime as foreground work.
//! Work item: plan-step-cefd6705-31ab-477b-a719-8600cdf8f2ad.

use std::sync::Arc;

use futures::StreamExt;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::schemas::background_process::BackgroundProcessStatus;
use klaus_agent_core::tools::builtins::bash::create_bash_tool;
use klaus_agent_core::tools::shell::Signal;
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};
use serde_json::{json, Value};

use super::goal_stream_fixture::{session_slot, text_events, usage, Fixture, WAIT};

struct ShellCleanup(Arc<Session>);
impl Drop for ShellCleanup {
    fn drop(&mut self) {
        for process in self.0.background_process_snapshot().processes {
            if !process.status.is_terminal() {
                self.0
                    .stop_background_process(&process.shell_id, Signal::Kill);
            }
        }
    }
}

fn tool_events(name: &str, input: Value) -> Vec<ModelStreamEvent> {
    let id: klaus_agent_core::runtime_values::RuntimeString =
        format!("background-goal-{name}").into();
    vec![
        ModelStreamEvent::MessageStart {
            model: "goal-stream-lifetime".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.clone(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.clone(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: usage(1, 1),
        },
    ]
}

fn assert_shell_running(session: &Session) {
    let snapshot = session.background_process_snapshot();
    assert_eq!(
        snapshot.processes.len(),
        1,
        "the real shell must be registered"
    );
    assert_eq!(
        snapshot.processes[0].status,
        BackgroundProcessStatus::Running,
        "goal continuation must happen before background shell completion"
    );
}

#[tokio::test]
async fn running_shell_allows_consecutive_goal_turns_until_model_reports_blocked() {
    let slot = session_slot();
    let fixture = Fixture::with_tools(&slot, vec![create_bash_tool()], |_, _, call| {
        let events = match call {
            0 => tool_events("bash", json!({
                "command": if cfg!(windows) { "ping -n 31 127.0.0.1 > NUL" } else { "exec sleep 30" },
                "runInBackground": true
            })),
            1 => text_events("External authorization is required: first goal turn.", usage(1, 1)),
            2 => text_events("The same authorization is still required: second goal turn.", usage(1, 1)),
            3 => tool_events("update_goal", json!({"status":"blocked"})),
            4 => text_events("The same blocker remains after three goal turns.", usage(1, 1)),
            _ => panic!("a blocked goal must not start another provider request"),
        };
        Box::pin(futures::stream::iter(events))
    }).await;
    let session = fixture.session.clone();
    let cleanup = ShellCleanup(session.clone());
    session
        .set_thread_goal(Some("Complete the authorized workflow.".into()), None, None)
        .await
        .unwrap();
    let first = session.run_streamed(
        Input::Text("Start the workflow.".into()),
        TurnOptions::default(),
    );
    tokio::time::timeout(WAIT, first.events.collect::<Vec<_>>())
        .await
        .unwrap();
    assert_shell_running(&session);
    let mut statuses = vec![session
        .thread_goal()
        .await
        .unwrap()
        .unwrap()
        .status
        .wire_name()];

    for goal_turn in 2..=3 {
        assert_eq!(
            session.thread_goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::Active
        );
        let next = session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("running background shell prevented goal turn {goal_turn}"));
        tokio::time::timeout(WAIT, next.events.collect::<Vec<_>>())
            .await
            .unwrap();
        assert_shell_running(&session);
        statuses.push(
            session
                .thread_goal()
                .await
                .unwrap()
                .unwrap()
                .status
                .wire_name(),
        );
    }
    let mut contract: Value = serde_json::from_str(include_str!(
        "../fixtures/goal-background-continuation.json"
    ))
    .unwrap();
    contract.as_object_mut().unwrap().remove("reason");
    assert_eq!(
        json!({
            "goalStatusesAfterTurns": statuses,
            "backgroundStatusAtBlocked": session.background_process_snapshot().processes[0].status,
            "automaticGoalTurns": statuses.len() - 1
        }),
        contract
    );
    assert_eq!(
        session.thread_goal().await.unwrap().unwrap().status,
        ThreadGoalStatus::Blocked
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 5);
    assert!(session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(fixture.requests.lock().unwrap().len(), 5);
    drop(cleanup);
    fixture.finish().await;
}
