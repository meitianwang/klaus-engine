use super::*;
use crate::goal::owned_resources::{self, GoalOwnedResourcesInput};
use crate::goal::store::{GoalControlMutation, GoalRequestMutation};
use crate::goal::{GoalAccountingMode, GoalUpdate, ThreadGoal, ThreadGoalStatus};

struct Fixture {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
    home: std::path::PathBuf,
    thread_id: String,
    goal_id: RuntimeString,
    preparation_id: String,
    directory: std::path::PathBuf,
    files: Vec<GoalOwnedResourceFile>,
}

impl Fixture {
    fn new() -> Self {
        let lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        let home = std::env::temp_dir().join(format!("goal-preview-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home).unwrap();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &home);
        let thread_id = format!("objective-preview-{}", uuid::Uuid::new_v4());
        let preparation_id = uuid::Uuid::new_v4().to_string();
        let directory = home
            .join("goal-objectives")
            .join(format!("{:x}", Sha256::digest(thread_id.as_bytes())))
            .join(&preparation_id);
        std::fs::create_dir_all(&directory).unwrap();
        Self {
            _lock: lock,
            previous,
            home,
            thread_id,
            goal_id: uuid::Uuid::new_v4().to_string().into(),
            preparation_id,
            directory,
            files: Vec::new(),
        }
    }

    fn add(&mut self, name: &str, bytes: &[u8]) {
        std::fs::write(self.directory.join(name), bytes).unwrap();
        self.files.push(GoalOwnedResourceFile {
            name: name.into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        });
    }

    fn reference(&self, name: &str) -> RuntimeString {
        format!(
            "{OBJECTIVE_FILE_REFERENCE}{}",
            self.directory.join(name).display()
        )
        .into()
    }

    async fn install(&self, objective: RuntimeString) {
        let manifest = if self.files.is_empty() {
            None
        } else {
            Some(
                owned_resources::validate(
                    &self.thread_id,
                    &GoalOwnedResourcesInput {
                        preparation_id: self.preparation_id.clone(),
                        files: self.files.clone(),
                    },
                    &self.home,
                )
                .await
                .unwrap(),
            )
        };
        store::apply_goal_control(
            &self.thread_id,
            GoalControlMutation::Set(GoalRequestMutation::Create {
                goal: ThreadGoal {
                    thread_id: self.thread_id.clone().into(),
                    goal_id: self.goal_id.clone(),
                    objective,
                    status: ThreadGoalStatus::Paused,
                    token_budget: Some(500),
                    tokens_used: 0,
                    time_used_seconds: 0,
                    created_at_ms: 1,
                    updated_at_ms: 1,
                },
                expected_goal_id: None,
                expected_absent: true,
            }),
            None,
            None,
            Some(manifest),
        )
        .await
        .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn assert_conflict(error: io::Error) {
    assert!(
        error
            .to_string()
            .starts_with(super::super::service::GOAL_CONFLICT_PREFIX),
        "{error}"
    );
}

#[tokio::test]
async fn plain_objective_is_returned_without_a_preparation() {
    let fixture = Fixture::new();
    let objective = RuntimeString::from("A full inline objective\n包含所有细节 🦀");
    fixture.install(objective.clone()).await;
    assert_eq!(
        read_objective(&fixture.thread_id, &fixture.goal_id)
            .await
            .unwrap(),
        GoalObjectivePreview {
            goal_id: fixture.goal_id.clone(),
            objective,
        }
    );
}

#[tokio::test]
async fn full_utf8_objective_survives_source_owned_fork_and_source_clear() {
    let mut fixture = Fixture::new();
    let full = (0..160)
        .map(|index| {
            format!(
                "{index:03}: 全部目标细节与验证要求，以及完整的交互、底层实现和可靠恢复条件 🦀\n"
            )
        })
        .collect::<String>();
    assert!(full.chars().count() > 4_000);
    fixture.add("objective.md", full.as_bytes());
    fixture.install(fixture.reference("objective.md")).await;
    let target = format!("preview-fork-{}", uuid::Uuid::new_v4());
    let copied = store::copy_goal_deferred(&fixture.thread_id, &target)
        .await
        .unwrap()
        .unwrap();
    store::clear_goal(&fixture.thread_id).await.unwrap();
    let preview = read_objective(&target, &copied.goal_id).await.unwrap();
    assert_eq!(preview.goal_id, fixture.goal_id);
    assert_eq!(preview.objective, RuntimeString::from(full));
    assert!(
        store::get_record(&target)
            .await
            .unwrap()
            .continuation_deferred
    );
}

#[tokio::test]
async fn wrong_or_cleared_generation_cannot_read_even_a_valid_preparation() {
    let mut fixture = Fixture::new();
    fixture.add("objective.md", b"The complete owned objective");
    fixture.install(fixture.reference("objective.md")).await;
    assert_conflict(
        read_objective(&fixture.thread_id, &"different-generation".into())
            .await
            .unwrap_err(),
    );
    store::clear_goal(&fixture.thread_id).await.unwrap();
    assert_conflict(
        read_objective(&fixture.thread_id, &fixture.goal_id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn file_reference_text_does_not_grant_external_or_metadata_access() {
    for name in ["preparation.json", "admission.json", "paste-1.txt"] {
        let mut fixture = Fixture::new();
        fixture.add("objective.md", b"owned objective");
        std::fs::write(fixture.directory.join(name), b"private unrelated bytes").unwrap();
        let literal = fixture.reference(name);
        fixture.install(literal.clone()).await;
        assert_eq!(
            read_objective(&fixture.thread_id, &fixture.goal_id)
                .await
                .unwrap()
                .objective,
            literal
        );
    }
    let mut fixture = Fixture::new();
    fixture.add("objective.md", b"owned objective");
    let external = fixture.home.join("unrelated-secret.txt");
    std::fs::write(&external, b"private unrelated bytes").unwrap();
    let literal = RuntimeString::from(format!("{OBJECTIVE_FILE_REFERENCE}{}", external.display()));
    fixture.install(literal.clone()).await;
    assert_eq!(
        read_objective(&fixture.thread_id, &fixture.goal_id)
            .await
            .unwrap()
            .objective,
        literal
    );
}

#[tokio::test]
async fn objective_file_must_be_listed_in_the_current_manifest() {
    let mut fixture = Fixture::new();
    fixture.add("paste-1.txt", b"owned evidence");
    std::fs::write(fixture.directory.join("objective.md"), b"not authorized").unwrap();
    let literal = fixture.reference("objective.md");
    fixture.install(literal.clone()).await;
    assert_eq!(
        read_objective(&fixture.thread_id, &fixture.goal_id)
            .await
            .unwrap()
            .objective,
        literal
    );
}

#[tokio::test]
async fn changed_or_non_utf8_objective_bytes_are_rejected() {
    let mut fixture = Fixture::new();
    fixture.add("objective.md", b"owned objective");
    fixture.install(fixture.reference("objective.md")).await;
    std::fs::write(fixture.directory.join("objective.md"), b"other objective").unwrap();
    assert!(read_objective(&fixture.thread_id, &fixture.goal_id)
        .await
        .is_err());
    drop(fixture);

    let mut fixture = Fixture::new();
    fixture.add("objective.md", &[0xf0, 0x28, 0x8c, 0x28]);
    fixture.install(fixture.reference("objective.md")).await;
    assert!(read_objective(&fixture.thread_id, &fixture.goal_id)
        .await
        .unwrap_err()
        .to_string()
        .contains("not valid UTF-8"));
}

#[tokio::test]
async fn edit_then_restore_aba_rejects_the_already_read_snapshot() {
    let mut fixture = Fixture::new();
    fixture.add("objective.md", b"complete objective");
    let reference = fixture.reference("objective.md");
    fixture.install(reference.clone()).await;
    let binding = capture_binding(&fixture.thread_id, &fixture.goal_id)
        .await
        .unwrap();
    let objective = read_captured(&binding, &fixture.home).await.unwrap();
    for text in [RuntimeString::from("Changed during preview"), reference] {
        store::update_goal(
            &fixture.thread_id,
            GoalUpdate {
                objective: Some(text),
                expected_goal_id: Some(fixture.goal_id.clone()),
                ..Default::default()
            },
            2,
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_conflict(
        finish_read(&fixture.thread_id, binding, objective)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn clear_and_replacement_reject_the_already_read_snapshot() {
    #[derive(Clone, Copy)]
    enum Replacement {
        None,
        Fresh,
        Retired,
    }
    for replacement_kind in [Replacement::None, Replacement::Fresh, Replacement::Retired] {
        let fixture = Fixture::new();
        fixture.install("Original inline objective".into()).await;
        let binding = capture_binding(&fixture.thread_id, &fixture.goal_id)
            .await
            .unwrap();
        let objective = read_captured(&binding, &fixture.home).await.unwrap();
        store::clear_goal(&fixture.thread_id).await.unwrap();
        if !matches!(replacement_kind, Replacement::None) {
            let retired = matches!(replacement_kind, Replacement::Retired);
            let replacement_id = if retired {
                fixture.goal_id.clone()
            } else {
                uuid::Uuid::new_v4().to_string().into()
            };
            let replacement = ThreadGoal {
                thread_id: fixture.thread_id.clone().into(),
                goal_id: replacement_id.clone(),
                objective: "Replacement objective".into(),
                status: ThreadGoalStatus::Paused,
                token_budget: None,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at_ms: 3,
                updated_at_ms: 3,
            };
            let inserted = store::insert_goal(replacement).await;
            if retired {
                let error = inserted.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("goal generation was already used"),
                    "{error}"
                );
                assert!(store::get_record(&fixture.thread_id)
                    .await
                    .unwrap()
                    .goal
                    .is_none());
            } else {
                let replacement = inserted.unwrap().unwrap();
                assert_ne!(replacement.goal_id, fixture.goal_id);
                assert_eq!(replacement.goal_id, replacement_id);
            }
        }
        assert_conflict(
            finish_read(&fixture.thread_id, binding, objective)
                .await
                .unwrap_err(),
        );
    }
}

#[tokio::test]
async fn accounting_revision_does_not_invalidate_the_full_objective_preview() {
    let mut fixture = Fixture::new();
    fixture.add(
        "objective.md",
        b"All details remain visible during accounting",
    );
    fixture.install(fixture.reference("objective.md")).await;
    let binding = capture_binding(&fixture.thread_id, &fixture.goal_id)
        .await
        .unwrap();
    let before = store::get_record(&fixture.thread_id).await.unwrap();
    let objective = read_captured(&binding, &fixture.home).await.unwrap();
    store::account_usage(
        &fixture.thread_id,
        GoalAccountingMode::ActiveOrStopped,
        13,
        2,
        Some(fixture.goal_id.clone()),
        2,
    )
    .await
    .unwrap();
    let after = store::get_record(&fixture.thread_id).await.unwrap();
    assert!(after.revision > before.revision);
    assert_eq!(after.intent_revision, before.intent_revision);
    assert_eq!(after.goal.unwrap().tokens_used, 13);
    assert_eq!(
        finish_read(&fixture.thread_id, binding, objective)
            .await
            .unwrap()
            .objective,
        "All details remain visible during accounting"
    );
}
