//! Defect: an old model request could retire a goal edited by another runtime.
//! Root cause: request intent was process-local while same-generation edits were durable.
//! Work item 20261017: preserve user intent across concurrent runtime and saved writers.

use klaus_agent_core::goal::owned_resources::{GoalOwnedResourceFile, GoalOwnedResourcesInput};
use klaus_agent_core::goal::runtime::{GoalCreatePreconditions, GoalRequestIdentity};
use klaus_agent_core::goal::store;
use klaus_agent_core::goal::{
    GoalAccountingMode, GoalRuntime, GoalUpdate, GoalUpdateOutcome, ThreadGoal, ThreadGoalStatus,
};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use klaus_agent_core::runtime_values::RuntimeString;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

struct Home {
    path: PathBuf,
    previous: Option<String>,
}

impl Home {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-goal-request-intent-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        set_process_environment_var("KLAUS_CONFIG_DIR", &*path.to_string_lossy());
        Self { path, previous }
    }

    fn resources(&self, thread: &str, body: &[u8]) -> GoalOwnedResourcesInput {
        let preparation_id = uuid::Uuid::new_v4().to_string();
        let root = self
            .path
            .join("goal-objectives")
            .join(format!("{:x}", Sha256::digest(thread.as_bytes())))
            .join(&preparation_id);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("objective.md"), body).unwrap();
        GoalOwnedResourcesInput {
            preparation_id,
            files: vec![GoalOwnedResourceFile {
                name: "objective.md".into(),
                size_bytes: body.len() as u64,
                sha256: format!("{:x}", Sha256::digest(body)),
            }],
        }
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!("Retained goal request fixture: {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn runtime(thread: &str) -> Arc<GoalRuntime> {
    Arc::new(GoalRuntime::new(
        thread.into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(|| 1_000),
        None,
    ))
}

async fn start_request(runtime: &GoalRuntime, thread: &str) -> (ThreadGoal, GoalRequestIdentity) {
    let goal = store::get_goal(thread).await.unwrap().unwrap();
    runtime.on_turn_start().await.unwrap();
    let captured = runtime.request_identity();
    assert_eq!(captured.generation.as_ref(), Some(&goal.goal_id));
    (goal, captured)
}

async fn create_goal(runtime: &GoalRuntime, objective: &str) -> ThreadGoal {
    runtime
        .create_host_goal(objective.into(), None, None, None)
        .await
        .unwrap()
        .unwrap()
        .goal
}

async fn assert_stale_status_refused(
    runtime: &GoalRuntime,
    captured: &GoalRequestIdentity,
    expected: &ThreadGoal,
) {
    for status in [
        ThreadGoalStatus::Complete,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::Paused,
    ] {
        let result = runtime
            .update_goal_status_for_identity(status, captured)
            .await;
        assert!(
            matches!(result, GoalUpdateOutcome::Forbidden(_)),
            "a stale request must not change {status:?} after another writer's edit: {result:?}"
        );
        assert_eq!(runtime.goal().await.unwrap().as_ref(), Some(expected));
    }
}

#[tokio::test]
async fn another_runtime_same_generation_edit_rejects_old_model_status_updates() {
    let _home = Home::new();
    let first = runtime("external-runtime");
    let second = runtime("external-runtime");
    let initial = create_goal(&first, "Original objective").await;
    let (_, captured) = start_request(&first, "external-runtime").await;
    let edited = second
        .apply_host_update(GoalUpdate {
            objective: Some("Edited objective".into()),
            expected_goal_id: Some(initial.goal_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap()
        .goal;
    assert_eq!(
        edited.goal_id, initial.goal_id,
        "user edits retain identity"
    );
    assert_stale_status_refused(&first, &captured, &edited).await;
}

#[tokio::test]
async fn a_saved_writer_same_generation_edit_rejects_old_model_completion() {
    let _home = Home::new();
    let first = runtime("saved-writer");
    let initial = create_goal(&first, "Original objective").await;
    let (_, captured) = start_request(&first, "saved-writer").await;
    let edited = store::update_goal(
        "saved-writer",
        GoalUpdate {
            objective: Some("Saved objective".into()),
            expected_goal_id: Some(initial.goal_id.clone()),
            ..Default::default()
        },
        1_000,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(edited.goal_id, initial.goal_id);
    assert_stale_status_refused(&first, &captured, &edited).await;
}

#[tokio::test]
async fn changing_an_objective_back_does_not_restore_old_request_authority() {
    let _home = Home::new();
    let first = runtime("objective-aba");
    let second = runtime("objective-aba");
    let initial = create_goal(&first, "Original objective").await;
    let (_, captured) = start_request(&first, "objective-aba").await;
    for objective in ["Temporary objective", "Original objective"] {
        second
            .apply_host_update(GoalUpdate {
                objective: Some(objective.into()),
                expected_goal_id: Some(initial.goal_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .unwrap();
    }
    let current = first.goal().await.unwrap().unwrap();
    assert_eq!(
        current, initial,
        "the public goal can return to identical contents"
    );
    assert_stale_status_refused(&first, &captured, &current).await;
}

#[tokio::test]
async fn external_accounting_does_not_invalidate_a_current_request() {
    let _home = Home::new();
    let first = runtime("accounting-only");
    let initial = create_goal(&first, "Finish the authorized work").await;
    let (_, captured) = start_request(&first, "accounting-only").await;
    let before = store::get_record("accounting-only").await.unwrap();
    store::account_usage(
        "accounting-only",
        GoalAccountingMode::ActiveOnly,
        37,
        2,
        Some(initial.goal_id.clone()),
        1_000,
    )
    .await
    .unwrap();
    let after = store::get_record("accounting-only").await.unwrap();
    assert!(
        after.revision > before.revision,
        "the accounting write is durable"
    );
    let result = first
        .update_goal_status_for_identity(ThreadGoalStatus::Complete, &captured)
        .await;
    let GoalUpdateOutcome::Updated(completed) = result else {
        panic!("accounting alone must preserve request authority: {result:?}");
    };
    assert_eq!(completed.goal_id, initial.goal_id);
    assert_eq!(completed.tokens_used, 37);
    assert_eq!(completed.time_used_seconds, 2);
    assert_eq!(completed.status, ThreadGoalStatus::Complete);
}

#[tokio::test]
async fn external_control_change_prevents_an_old_provider_request_from_starting() {
    let _home = Home::new();
    let first = runtime("provider-admission");
    let second = runtime("provider-admission");
    create_goal(&first, "Original objective").await;
    let (_, captured) = start_request(&first, "provider-admission").await;
    second
        .apply_host_update(GoalUpdate {
            objective: Some("Edited before provider dispatch".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    let captured = Mutex::new(captured);
    let result = first
        .prepare_provider_request(&[], &captured, None, None)
        .await;
    assert!(
        result.is_err(),
        "a request that predates an external edit must fail admission"
    );
}

#[tokio::test]
async fn restored_owned_files_do_not_restore_old_read_authority() {
    let home = Home::new();
    let first = runtime("resource-aba");
    let second = runtime("resource-aba");
    let original_resources = home.resources("resource-aba", b"Original full objective\n");
    let temporary_resources = home.resources("resource-aba", b"Temporary full objective\n");
    let initial = first
        .create_host_goal_with_resources(
            "Original objective".into(),
            None,
            None,
            GoalCreatePreconditions::default(),
            Some(original_resources.clone()),
        )
        .await
        .unwrap()
        .unwrap()
        .goal;
    let (_, captured) = start_request(&first, "resource-aba").await;
    let original_capability = first
        .owned_read_resources(&captured)
        .await
        .unwrap()
        .expect("the original request may read its owned files");
    for (objective, resources) in [
        ("Temporary objective", temporary_resources),
        ("Original objective", original_resources),
    ] {
        second
            .apply_host_update_resources(
                GoalUpdate {
                    objective: Some(objective.into()),
                    expected_goal_id: Some(initial.goal_id.clone()),
                    ..Default::default()
                },
                Some(resources),
            )
            .await
            .unwrap()
            .unwrap();
    }
    let current = first.goal().await.unwrap().unwrap();
    assert_eq!(current.goal_id, initial.goal_id);
    assert_eq!(current.objective, RuntimeString::from("Original objective"));
    assert!(
        first
            .guard_owned_read(&captured, &original_capability)
            .await
            .is_err(),
        "restoring the same manifest must not revive an older request capability"
    );
}
