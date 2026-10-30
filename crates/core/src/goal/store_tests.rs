use super::*;
use crate::goal::owned_resources::GoalOwnedResourceFile;
use crate::runtime_values::{runtime_codec, RuntimeObject};
use crate::test_support::CONFIG_ENV_LOCK;
use std::process::{Child, Command, Stdio};

struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<String>,
    home: PathBuf,
}

impl EnvGuard {
    fn new(tag: &str) -> Self {
        static SEQUENCE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let lock = CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        let home = std::env::temp_dir().join(format!(
            "klaus-goal-sqlite-{tag}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&home).unwrap();
        crate::runtime_process::set_process_environment_var(
            "KLAUS_CONFIG_DIR",
            home.to_str().unwrap(),
        );
        Self {
            _lock: lock,
            previous,
            home,
        }
    }

    fn legacy_path(&self, thread_id: &str) -> PathBuf {
        self.home
            .join("goals")
            .join(format!("{}.json", sanitize_path_component(thread_id)))
    }

    fn write_legacy(&self, thread_id: &str, bytes: &[u8]) {
        std::fs::create_dir_all(self.home.join("goals")).unwrap();
        std::fs::write(self.legacy_path(thread_id), bytes).unwrap();
    }

    fn child(&self, mode: &str, index: usize) -> Child {
        let log =
            std::fs::File::create(self.home.join(format!("child-{mode}-{index}.log"))).unwrap();
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "goal::store::tests::storage_process_worker",
                "--nocapture",
            ])
            .env("KLAUS_GOAL_STORAGE_CHILD", mode)
            .env("KLAUS_CONFIG_DIR", &self.home)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap()
    }

    fn stored_run(&self, thread_id: &str) -> Option<GoalRun> {
        let mut connection = open_database(&self.home).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let run = read_goal_run(&transaction, thread_id).unwrap();
        transaction.commit().unwrap();
        run
    }

    fn stored_wrapup(&self, thread_id: &str) -> Option<GoalBudgetWrapup> {
        let mut connection = open_database(&self.home).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let wrapup = read_budget_wrapup(&transaction, thread_id).unwrap();
        transaction.commit().unwrap();
        wrapup
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        std::fs::remove_dir_all(&self.home).unwrap();
    }
}

fn goal(thread: &str) -> ThreadGoal {
    ThreadGoal {
        thread_id: thread.into(),
        goal_id: "generation-1".into(),
        objective: "Finish the work".into(),
        status: ThreadGoalStatus::Active,
        token_budget: Some(1_000),
        tokens_used: 0,
        time_used_seconds: 0,
        created_at_ms: 10,
        updated_at_ms: 20,
    }
}

fn owned_resources(source: &str, preparation: &str) -> GoalOwnedResourcesManifest {
    GoalOwnedResourcesManifest {
        source_thread_id: source.to_owned(),
        preparation_id: preparation.to_owned(),
        files: vec![GoalOwnedResourceFile {
            name: "objective.md".to_owned(),
            size_bytes: 123,
            sha256: "ab".repeat(32),
        }],
    }
}

#[tokio::test]
async fn owned_resources_bind_to_generation_and_survive_same_generation_writes() {
    let _environment = EnvGuard::new("owned-resources-preserve");
    let initial = goal("t1");
    let resources = owned_resources("t1", "preparation-1");
    let bound = apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    assert_eq!(bound.record.revision, 1);
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources.clone())
    );
    assert!(get_goal_owned_resources("t1", &"stale-generation".into())
        .await
        .unwrap()
        .is_none());
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    account_usage("t1", GoalAccountingMode::ActiveOnly, 3, 2, None, 40)
        .await
        .unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Paused),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    let before_revoke = get_record("t1").await.unwrap();
    let revoked = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                expected_goal_id: Some(initial.goal_id.clone()),
                expected_revision: Some(before_revoke.revision),
                ..Default::default()
            },
            now_ms: 50,
        }),
        None,
        None,
        Some(None),
    )
    .await
    .unwrap();
    assert_eq!(revoked.record.goal, before_revoke.goal);
    assert_eq!(revoked.record.revision, before_revoke.revision + 1);
    assert!(get_goal_owned_resources("t1", &initial.goal_id)
        .await
        .unwrap()
        .is_none());
    assert!(revoked.record.continuation_deferred);
}

#[tokio::test]
async fn owned_resources_clear_and_replacement_revoke_generation_bindings() {
    let _environment = EnvGuard::new("owned-resources-clear");
    let mut original = goal("t1");
    original.status = ThreadGoalStatus::Complete;
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(original.clone())),
        None,
        None,
        Some(Some(owned_resources("t1", "preparation-1"))),
    )
    .await
    .unwrap();
    let mut replacement = goal("t1");
    replacement.goal_id = "generation-2".into();
    insert_goal(replacement.clone()).await.unwrap().unwrap();
    assert!(get_goal_owned_resources("t1", &replacement.goal_id)
        .await
        .unwrap()
        .is_none());
    assert!(get_goal_owned_resources("t1", &original.goal_id)
        .await
        .unwrap()
        .is_none());
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate::default(),
            now_ms: replacement.updated_at_ms,
        }),
        None,
        None,
        Some(Some(owned_resources("t1", "preparation-2"))),
    )
    .await
    .unwrap();
    clear_goal("t1").await.unwrap();
    assert!(get_goal_owned_resources("t1", &replacement.goal_id)
        .await
        .unwrap()
        .is_none());
    let error = apply_goal_control(
        "t1",
        GoalControlMutation::Clear {
            expected_goal_id: None,
            expected_revision: None,
            expected_intent_revision: None,
        },
        None,
        None,
        Some(Some(owned_resources("t1", "preparation-3"))),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn owned_resource_receipt_replay_never_rebinds_revoked_metadata() {
    let _environment = EnvGuard::new("owned-resources-receipt");
    let initial = goal("t1");
    let mutation = GoalControlMutation::Set(request_create(initial.clone()));
    let receipt = GoalControlReceipt {
        request_id: "request-1".to_owned(),
        fingerprint: "goal-with-prepared-resources".to_owned(),
    };
    let first = apply_goal_control(
        "t1",
        mutation.clone(),
        None,
        Some(receipt.clone()),
        Some(Some(owned_resources("t1", "preparation-1"))),
    )
    .await
    .unwrap();
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate::default(),
            now_ms: initial.updated_at_ms,
        }),
        None,
        None,
        Some(None),
    )
    .await
    .unwrap();
    let current = get_record("t1").await.unwrap();
    let replay = apply_goal_control(
        "t1",
        mutation,
        Some(pending_accounting(&initial.goal_id, 40, 2)),
        Some(receipt),
        Some(Some(owned_resources("t1", "preparation-1"))),
    )
    .await
    .unwrap();
    assert_eq!(replay.record, current);
    assert_eq!(replay.accounting, None);
    assert_eq!(
        replay.receipt,
        first.receipt.map(|receipt| GoalRequestOutcome {
            replayed: true,
            ..receipt
        })
    );
    assert!(get_goal_owned_resources("t1", &initial.goal_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(get_record("t1").await.unwrap(), current);
}

#[tokio::test]
async fn receipt_write_failure_rolls_back_resources_accounting_and_control() {
    let environment = EnvGuard::new("owned-resources-rollback");
    let initial = goal("t1");
    let resources = owned_resources("t1", "preparation-1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_resources_receipt BEFORE INSERT ON goal_request_receipts BEGIN SELECT RAISE(ABORT, 'forced owned-resource receipt failure'); END;").unwrap();
    let result = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                expected_goal_id: Some(initial.goal_id.clone()),
                expected_revision: Some(before.revision),
                ..Default::default()
            },
            now_ms: 50,
        }),
        Some(pending_accounting(&initial.goal_id, 1_000, 2)),
        Some(GoalControlReceipt {
            request_id: "request-1".to_owned(),
            fingerprint: "pause-with-new-resources".to_owned(),
        }),
        Some(Some(owned_resources("t1", "preparation-2"))),
    )
    .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    assert!(environment.stored_wrapup("t1").is_none());
    assert!(get_goal_request_receipt("t1", "request-1")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn concurrent_resource_edits_bind_only_the_winning_revision() {
    let _environment = EnvGuard::new("owned-resources-conflict");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let mutation = |label: &str| {
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                objective: Some(label.into()),
                expected_goal_id: Some(initial.goal_id.clone()),
                expected_revision: Some(before.revision),
                ..Default::default()
            },
            now_ms: 50,
        })
    };
    let (first, second) = tokio::join!(
        apply_goal_control(
            "t1",
            mutation("first"),
            None,
            None,
            Some(Some(owned_resources("t1", "first"))),
        ),
        apply_goal_control(
            "t1",
            mutation("second"),
            None,
            None,
            Some(Some(owned_resources("t1", "second"))),
        )
    );
    let (winner, loser) = match (first, second) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        _ => panic!("exactly one observed revision may bind its resources"),
    };
    assert!(loser.to_string().starts_with("GOAL_CONFLICT: "));
    let resources = get_goal_owned_resources("t1", &initial.goal_id)
        .await
        .unwrap()
        .unwrap();
    let current = get_record("t1").await.unwrap();
    assert_eq!(current, winner.record);
    assert_eq!(current.revision, before.revision + 1);
    assert_eq!(
        current.goal.unwrap().objective,
        RuntimeString::from(resources.preparation_id)
    );
}

#[tokio::test]
async fn fork_resource_snapshot_keeps_source_owner_after_source_clear() {
    let _environment = EnvGuard::new("owned-resources-fork");
    let original = goal("source");
    let resources = owned_resources("source", "preparation-1");
    apply_goal_control(
        "source",
        GoalControlMutation::Set(request_create(original.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    let snapshot = capture_goal_fork_snapshot("source").await.unwrap();
    clear_goal("source").await.unwrap();
    let copied = copy_goal_snapshot_deferred("target", &snapshot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(copied.thread_id, RuntimeString::from("target"));
    assert!(get_record("target").await.unwrap().continuation_deferred);
    assert_eq!(
        get_goal_owned_resources("target", &original.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    assert!(get_goal_owned_resources("source", &original.goal_id)
        .await
        .unwrap()
        .is_none());
    clear_goal("target").await.unwrap();
    assert!(get_goal_owned_resources("target", &original.goal_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn resource_manifest_is_exact_typed_metadata_and_corruption_is_an_error() {
    let environment = EnvGuard::new("owned-resources-corruption");
    let initial = goal("t1");
    let mut resources = owned_resources("source", "preparation-1");
    resources.files[0].name = "objective-\u{1f600}.md".to_owned();
    resources.files[0].size_bytes = u64::MAX;
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute(
            "UPDATE goal_owned_resources SET manifest = ?1 WHERE thread_id = 't1'",
            [b"{invalid owned resources".as_slice()],
        )
        .unwrap();
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT manifest FROM goal_owned_resources WHERE thread_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bytes, b"{invalid owned resources");
}

#[tokio::test]
async fn v5_upgrade_adds_resource_storage_without_rewriting_goal_or_receipt() {
    let environment = EnvGuard::new("owned-resources-migration");
    let initial = goal("t1");
    let receipt = apply_goal_request_once("t1", "request-1", "fp", request_create(initial.clone()))
        .await
        .unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("DROP TABLE goal_owned_resources; PRAGMA user_version = 5; CREATE TRIGGER reject_goal_rewrite BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT, 'migration must not rewrite goals'); END;").unwrap();
    assert!(get_goal_owned_resources("t1", &initial.goal_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert_eq!(
        get_goal_request_receipt("t1", "request-1").await.unwrap(),
        Some(GoalRequestOutcome {
            replayed: true,
            ..receipt
        })
    );
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        SCHEMA_VERSION
    );
}

fn legacy_bytes(goal: &ThreadGoal, deferred: bool) -> Vec<u8> {
    let mut record = RuntimeObject::new();
    record.insert("goal", runtime_codec::encode_value(goal).unwrap());
    record.insert("continuationDeferred", RuntimeJson::Bool(deferred));
    runtime_json::stringify(&RuntimeJson::Object(record)).to_node_utf8()
}

fn native_legacy_bytes(goal: &ThreadGoal, deferred: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "goal": {
            "threadId": goal.thread_id.try_to_utf8().unwrap(),
            "goalId": goal.goal_id.try_to_utf8().unwrap(),
            "objective": goal.objective.try_to_utf8().unwrap(),
            "status": goal.status.wire_name(),
            "tokenBudget": goal.token_budget,
            "tokensUsed": goal.tokens_used,
            "timeUsedSeconds": goal.time_used_seconds,
            "createdAtMs": goal.created_at_ms,
            "updatedAtMs": goal.updated_at_ms,
        },
        "continuationDeferred": deferred,
    }))
    .unwrap()
}

fn native_table_rows(connection: &Connection, table: &str) -> Vec<Vec<rusqlite::types::Value>> {
    let mut statement = connection
        .prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
        .unwrap();
    let columns = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| row.get(column))
                .collect::<Result<Vec<rusqlite::types::Value>, _>>()
        })
        .unwrap();
    rows.collect::<Result<Vec<_>, _>>().unwrap()
}

fn control_transaction_rows(connection: &Connection) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    [
        "goal_threads",
        "goal_intents",
        "goal_generations",
        "goals",
        "goal_request_receipts",
        "goal_continuation_deferrals",
        "goal_runs",
        "goal_budget_wrapups",
        "goal_owned_resources",
        "goal_thread_eligibility",
    ]
    .iter()
    .map(|table| native_table_rows(connection, table))
    .collect()
}

#[tokio::test]
async fn rejected_control_guard_leaves_all_sql_state_and_pending_usage_unchanged() {
    let environment = EnvGuard::new("control-guard-rejected");
    let initial = goal("t1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        Some(GoalControlReceipt {
            request_id: "creation".into(),
            fingerprint: "creation-fp".into(),
        }),
        Some(Some(owned_resources("t1", "preparation-1"))),
    )
    .await
    .unwrap();
    set_thread_goal_eligibility("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    let rows = control_transaction_rows(&connection);
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let entered_guard = Arc::clone(&entered);
    let guard = GoalControlGuard::new(move |_| {
        entered_guard.store(true, std::sync::atomic::Ordering::SeqCst);
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "goal control admission was revoked",
        ))
    });
    let result = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                expected_intent_revision: Some(before.intent_revision),
                control_guard: Some(guard),
                ..Default::default()
            },
            now_ms: 50,
        }),
        Some(pending_accounting(&initial.goal_id, 1_000, 3)),
        Some(GoalControlReceipt {
            request_id: "guarded-request".into(),
            fingerprint: "guarded-fp".into(),
        }),
        Some(Some(owned_resources("t1", "preparation-2"))),
    )
    .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(control_transaction_rows(&connection), rows);
    assert!(get_goal_request_receipt("t1", "guarded-request")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn rejected_direct_guard_does_not_confirm_or_mutate_prepared_legacy_import() {
    let environment = EnvGuard::new("control-guard-legacy");
    let original = goal("t1");
    let bytes = native_legacy_bytes(&original, true);
    environment.write_legacy("t1", &bytes);
    let guard = GoalControlGuard::new(|_| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "admission revoked",
        ))
    });
    assert_eq!(
        update_goal(
            "t1",
            GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                control_guard: Some(guard),
                ..Default::default()
            },
            50
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::PermissionDenied
    );
    let connection = open_database(&environment.home).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM goal_threads", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(std::fs::read(environment.legacy_path("t1")).unwrap(), bytes);
    assert_eq!(get_goal("t1").await.unwrap(), Some(original));
    assert!(get_record("t1").await.unwrap().continuation_deferred);
}

#[tokio::test]
async fn direct_goal_update_honors_guard_and_preserves_conflict_result_shape() {
    let _environment = EnvGuard::new("direct-control-guard");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let admission = Arc::new(std::sync::Mutex::new(()));
    let allowed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let captured_admission = Arc::clone(&admission);
    let captured_allowed = Arc::clone(&allowed);
    let guard = GoalControlGuard::new(move |operation| {
        let _admission = captured_admission.lock().unwrap();
        if !captured_allowed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "admission revoked",
            ));
        }
        operation()
    });
    let update = GoalUpdate {
        status: Some(ThreadGoalStatus::Paused),
        expected_intent_revision: Some(before.intent_revision),
        control_guard: Some(guard),
        ..Default::default()
    };
    assert_eq!(
        update_goal("t1", update.clone(), 50)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    allowed.store(true, std::sync::atomic::Ordering::SeqCst);
    let paused = update_goal("t1", update.clone(), 50)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = get_record("t1").await.unwrap();
    assert_eq!(paused.status, ThreadGoalStatus::Paused);
    assert_eq!(after.intent_revision, before.intent_revision + 1);
    assert_eq!(update_goal("t1", update, 60).await.unwrap(), Err(paused));
    assert_eq!(get_record("t1").await.unwrap(), after);
    assert!(admission.try_lock().is_ok());
}

#[tokio::test]
async fn control_guard_observes_sqlite_write_lease_before_entry_and_commit_before_release() {
    let environment = EnvGuard::new("control-guard-commit-boundary");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let expected_revision = before.revision + 1;
    let expected_intent = before.intent_revision + 1;
    let path = environment.home.join("goals.sqlite3");
    let admission = Arc::new(std::sync::Mutex::new(()));
    let captured_admission = Arc::clone(&admission);
    let committed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let captured_committed = Arc::clone(&committed);
    let guard = GoalControlGuard::new(move |operation| {
        let _admission = captured_admission.lock().unwrap();
        let probe = Connection::open(&path).unwrap();
        probe.busy_timeout(Duration::ZERO).unwrap();
        assert!(matches!(
            probe
                .execute_batch("BEGIN IMMEDIATE")
                .unwrap_err()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        ));
        let outcome = operation()?;
        assert_eq!(
            probe
                .query_row(
                    "SELECT tokens_used,time_used_seconds,status FROM goals WHERE thread_id='t1'",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?
                    ))
                )
                .unwrap(),
            (1_000, 3, "budget_limited".to_owned())
        );
        assert_eq!(
            probe
                .query_row(
                    "SELECT revision FROM goal_threads WHERE thread_id='t1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            expected_revision
        );
        assert_eq!(
            probe
                .query_row(
                    "SELECT intent_revision FROM goal_intents WHERE thread_id='t1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            expected_intent
        );
        assert_eq!(
            probe
                .query_row(
                    "SELECT count(*) FROM goal_request_receipts WHERE request_id='guarded-request'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            probe
                .query_row(
                    "SELECT delivered FROM goal_budget_wrapups WHERE thread_id='t1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let manifest: Vec<u8> = probe
            .query_row(
                "SELECT manifest FROM goal_owned_resources WHERE thread_id='t1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<GoalOwnedResourcesManifest>(&manifest).unwrap(),
            owned_resources("t1", "preparation-2")
        );
        assert!(captured_admission.try_lock().is_err());
        captured_committed.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(outcome)
    });
    let outcome = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                control_guard: Some(guard),
                ..Default::default()
            },
            now_ms: 50,
        }),
        Some(pending_accounting(&initial.goal_id, 1_000, 3)),
        Some(GoalControlReceipt {
            request_id: "guarded-request".into(),
            fingerprint: "guarded-fp".into(),
        }),
        Some(Some(owned_resources("t1", "preparation-2"))),
    )
    .await
    .unwrap();
    assert!(committed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(admission.try_lock().is_ok());
    assert_eq!(get_record("t1").await.unwrap(), outcome.record);
}

#[tokio::test]
async fn guarded_sql_failure_rolls_back_all_rows_before_releasing_admission() {
    let environment = EnvGuard::new("control-guard-sql-rollback");
    let initial = goal("t1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(owned_resources("t1", "preparation-1"))),
    )
    .await
    .unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_guarded_receipt BEFORE INSERT ON goal_request_receipts BEGIN SELECT RAISE(ABORT,'forced guarded receipt failure'); END;").unwrap();
    let rows = control_transaction_rows(&connection);
    let expected_rows = rows.clone();
    let path = environment.home.join("goals.sqlite3");
    let admission = Arc::new(std::sync::Mutex::new(()));
    let captured_admission = Arc::clone(&admission);
    let guard = GoalControlGuard::new(move |operation| {
        let _admission = captured_admission.lock().unwrap();
        let error = operation().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let probe = Connection::open(&path).unwrap();
        probe.busy_timeout(Duration::ZERO).unwrap();
        probe.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(control_transaction_rows(&probe), expected_rows);
        probe.execute_batch("ROLLBACK").unwrap();
        assert!(captured_admission.try_lock().is_err());
        Err(error)
    });
    assert_eq!(
        apply_goal_control(
            "t1",
            GoalControlMutation::Set(GoalRequestMutation::Update {
                update: GoalUpdate {
                    status: Some(ThreadGoalStatus::Paused),
                    control_guard: Some(guard),
                    ..Default::default()
                },
                now_ms: 50,
            }),
            Some(pending_accounting(&initial.goal_id, 1_000, 3)),
            Some(GoalControlReceipt {
                request_id: "guarded-request".into(),
                fingerprint: "guarded-fp".into(),
            }),
            Some(Some(owned_resources("t1", "preparation-2")))
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(control_transaction_rows(&connection), rows);
    assert!(admission.try_lock().is_ok());
}

#[tokio::test]
async fn sqlite_busy_wait_keeps_admission_available_and_rechecks_cancellation() {
    let environment = EnvGuard::new("control-guard-lock-order");
    let initial = goal("t1");
    insert_goal(initial).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let admission = Arc::new(std::sync::Mutex::new(()));
    let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (entered_sender, mut entered_receiver) = tokio::sync::oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_sender)));
    let captured_admission = Arc::clone(&admission);
    let captured_allowed = Arc::clone(&allowed);
    let guard = GoalControlGuard::new(move |operation| {
        let _admission = captured_admission.lock().unwrap();
        if let Some(sender) = entered.lock().unwrap().take() {
            sender.send(()).unwrap();
        }
        if !captured_allowed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "admission revoked while SQLite was busy",
            ));
        }
        operation()
    });
    let mut blocker = open_database(&environment.home).unwrap();
    let transaction = blocker
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let pending = tokio::spawn(async move {
        update_goal(
            "t1",
            GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                control_guard: Some(guard),
                ..Default::default()
            },
            50,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut entered_receiver)
            .await
            .is_err()
    );
    {
        let _admission = admission.try_lock().unwrap();
        allowed.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    transaction.commit().unwrap();
    assert_eq!(
        pending.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    entered_receiver.await.unwrap();
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(admission.try_lock().is_ok());
}

#[tokio::test]
async fn saved_eligibility_is_provider_free_explicit_and_independent_of_goal_import() {
    let environment = EnvGuard::new("saved-eligibility");
    environment.write_legacy("saved", b"{invalid legacy goal remains inspectable");
    assert_eq!(thread_goal_eligibility("saved").await.unwrap(), None);
    set_thread_goal_eligibility("saved", true).await.unwrap();
    assert_eq!(thread_goal_eligibility("saved").await.unwrap(), Some(true));
    let connection = open_database(&environment.home).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM goal_threads", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    set_thread_goal_eligibility("saved", false).await.unwrap();
    set_thread_goal_eligibility("saved", false).await.unwrap();
    assert_eq!(thread_goal_eligibility("saved").await.unwrap(), Some(false));
    assert_eq!(
        std::fs::read(environment.legacy_path("saved")).unwrap(),
        b"{invalid legacy goal remains inspectable"
    );
    drop(connection);
    let reopened = open_database(&environment.home).unwrap();
    let key = RuntimeString::from("saved").to_utf16le(BomPolicy::Omit);
    assert_eq!(
        reopened
            .query_row(
                "SELECT eligible FROM goal_thread_eligibility WHERE thread_key=?1",
                [key],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(thread_goal_eligibility("unknown").await.unwrap(), None);
}

#[tokio::test]
async fn eligibility_metadata_never_changes_goal_revisions_or_generation_state() {
    let environment = EnvGuard::new("eligibility-goal-independent");
    let initial = goal("t1");
    apply_goal_request_once("t1", "request-1", "fp", request_create(initial.clone()))
        .await
        .unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 3, None, 40)
        .await
        .unwrap();
    assert!(acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    let before = get_record("t1").await.unwrap();
    let run = environment.stored_run("t1");
    let wrapup = environment.stored_wrapup("t1");
    let receipt = get_goal_request_receipt("t1", "request-1").await.unwrap();
    for eligible in [false, true, true] {
        set_thread_goal_eligibility("t1", eligible).await.unwrap();
        assert_eq!(thread_goal_eligibility("t1").await.unwrap(), Some(eligible));
        assert_eq!(get_record("t1").await.unwrap(), before);
        assert_eq!(environment.stored_run("t1"), run);
        assert_eq!(environment.stored_wrapup("t1"), wrapup);
        assert_eq!(
            get_goal_request_receipt("t1", "request-1").await.unwrap(),
            receipt
        );
    }
    clear_goal("t1").await.unwrap();
    assert_eq!(thread_goal_eligibility("t1").await.unwrap(), Some(true));
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    insert_goal(next).await.unwrap().unwrap();
    assert_eq!(thread_goal_eligibility("t1").await.unwrap(), Some(true));
}

#[tokio::test]
async fn eligibility_keys_are_exact_and_forks_do_not_inherit_eligibility() {
    let _environment = EnvGuard::new("eligibility-keys");
    for (thread, eligible) in [
        ("one/two", true),
        ("one_two", false),
        ("thread\0suffix", true),
        ("线程😀", false),
    ] {
        set_thread_goal_eligibility(thread, eligible).await.unwrap();
    }
    for (thread, expected) in [
        ("one/two", true),
        ("one_two", false),
        ("thread\0suffix", true),
        ("线程😀", false),
    ] {
        assert_eq!(
            thread_goal_eligibility(thread).await.unwrap(),
            Some(expected)
        );
    }
    assert_eq!(
        set_thread_goal_eligibility("", true)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        thread_goal_eligibility("").await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    set_thread_goal_eligibility("source", true).await.unwrap();
    insert_goal(goal("source")).await.unwrap().unwrap();
    copy_goal_deferred("source", "target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread_goal_eligibility("target").await.unwrap(), None);
    assert!(get_goal("target").await.unwrap().is_some());
    set_thread_goal_eligibility("target", false).await.unwrap();
    assert_eq!(thread_goal_eligibility("source").await.unwrap(), Some(true));
    assert_eq!(
        thread_goal_eligibility("target").await.unwrap(),
        Some(false)
    );
}

#[tokio::test]
async fn v8_eligibility_upgrade_adds_unknown_marker_without_rewriting_native_goal() {
    let environment = EnvGuard::new("eligibility-schema-upgrade");
    let mut initial = goal("t1");
    initial.created_at_ms = i64::MIN;
    initial.updated_at_ms = i64::MAX;
    insert_goal(initial).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("DROP TABLE goal_thread_eligibility; PRAGMA user_version=8; CREATE TRIGGER reject_eligibility_goal_rewrite BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT,'eligibility migration must not rewrite goals'); END;").unwrap();
    assert_eq!(thread_goal_eligibility("t1").await.unwrap(), None);
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        SCHEMA_VERSION
    );
    set_thread_goal_eligibility("t1", false).await.unwrap();
    assert_eq!(thread_goal_eligibility("t1").await.unwrap(), Some(false));
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn corrupted_eligibility_is_an_error_and_never_assumed_true() {
    let environment = EnvGuard::new("eligibility-corrupt");
    set_thread_goal_eligibility("t1", false).await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .pragma_update(None, "ignore_check_constraints", "ON")
        .unwrap();
    connection
        .execute("UPDATE goal_thread_eligibility SET eligible=2", [])
        .unwrap();
    assert_eq!(
        thread_goal_eligibility("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        connection
            .query_row("SELECT eligible FROM goal_thread_eligibility", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn structured_update_checks_projected_public_revision_before_committing() {
    let environment = EnvGuard::new("json-public-revision");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute(
            "UPDATE goal_threads SET revision=?1 WHERE thread_id='t1'",
            [MAX_SAFE_JSON_REVISION],
        )
        .unwrap();
    let before = get_record("t1").await.unwrap();
    let update = |status| GoalUpdate {
        status: Some(status),
        require_safe_json: true,
        ..Default::default()
    };
    assert_eq!(
        update_goal(
            "t1",
            update(ThreadGoalStatus::Complete),
            initial.updated_at_ms
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    update_goal(
        "t1",
        update(ThreadGoalStatus::Active),
        initial.updated_at_ms,
    )
    .await
    .unwrap()
    .unwrap();
    let reasserted = get_record("t1").await.unwrap();
    assert_eq!(reasserted.revision, before.revision);
    assert_eq!(reasserted.intent_revision, before.intent_revision + 1);
    account_usage(
        "t1",
        GoalAccountingMode::ActiveOnly,
        1,
        0,
        None,
        initial.updated_at_ms,
    )
    .await
    .unwrap();
    let accounted = get_record("t1").await.unwrap();
    assert_eq!(accounted.revision, MAX_SAFE_JSON_REVISION + 1);
    assert_eq!(accounted.intent_revision, reasserted.intent_revision);
    assert_eq!(
        update_goal(
            "t1",
            update(ThreadGoalStatus::Complete),
            initial.updated_at_ms
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), accounted);
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        initial.updated_at_ms,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        get_record("t1").await.unwrap().revision,
        MAX_SAFE_JSON_REVISION + 2
    );
}

#[tokio::test]
async fn structured_control_checks_resource_and_pending_accounting_revision_changes() {
    let environment = EnvGuard::new("json-control-public-revision");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute(
            "UPDATE goal_threads SET revision=?1 WHERE thread_id='t1'",
            [MAX_SAFE_JSON_REVISION],
        )
        .unwrap();
    let before = get_record("t1").await.unwrap();
    let mutation = |now_ms| {
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                require_safe_json: true,
                ..Default::default()
            },
            now_ms,
        })
    };
    assert_eq!(
        apply_goal_control(
            "t1",
            mutation(initial.updated_at_ms),
            None,
            None,
            Some(Some(owned_resources("t1", "preparation-1")))
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(get_goal_owned_resources("t1", &initial.goal_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        apply_goal_control(
            "t1",
            mutation(40),
            Some(pending_accounting(&initial.goal_id, 1, 1)),
            None,
            None
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn signed_timestamp_extremes_survive_native_updates_receipts_forks_and_reopen() {
    let environment = EnvGuard::new("signed-native-timestamps");
    for (index, (created_at_ms, updated_at_ms)) in [(i64::MIN, i64::MAX), (i64::MAX, i64::MIN)]
        .into_iter()
        .enumerate()
    {
        let thread = format!("native-{index}");
        let mut original = goal(&thread);
        original.token_budget = None;
        original.tokens_used = 53;
        original.time_used_seconds = 89;
        original.created_at_ms = created_at_ms;
        original.updated_at_ms = updated_at_ms;
        let applied =
            apply_goal_request_once(&thread, "request-1", "fp", request_create(original.clone()))
                .await
                .unwrap();
        assert_eq!(get_goal(&thread).await.unwrap(), Some(original.clone()));
        let updated = update_goal(
            &thread,
            GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                ..Default::default()
            },
            created_at_ms,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(
            (updated.created_at_ms, updated.updated_at_ms),
            (created_at_ms, created_at_ms)
        );
        let charged = account_usage(
            &thread,
            GoalAccountingMode::ActiveOrStopped,
            3,
            2,
            Some(original.goal_id.clone()),
            i64::MIN,
        )
        .await
        .unwrap()
        .goal()
        .unwrap()
        .clone();
        assert_eq!(
            (charged.created_at_ms, charged.updated_at_ms),
            (created_at_ms, i64::MIN)
        );
        let target = format!("fork-{index}");
        let captured = capture_goal_fork_snapshot(&thread).await.unwrap();
        let copied = copy_goal_snapshot_deferred(&target, &captured)
            .await
            .unwrap()
            .unwrap();
        let mut expected = charged;
        expected.thread_id = target.as_str().into();
        assert_eq!(copied, expected);
        let connection = open_database(&environment.home).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT created_at_ms, updated_at_ms FROM goals WHERE thread_id=?1",
                    [&target],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            (created_at_ms, i64::MIN)
        );
        drop(connection);
        assert_eq!(get_goal(&target).await.unwrap(), Some(expected));
        assert_eq!(
            get_goal_request_receipt(&thread, "request-1")
                .await
                .unwrap(),
            Some(GoalRequestOutcome {
                replayed: true,
                ..applied
            })
        );
    }
}

#[tokio::test]
async fn legacy_migration_preserves_exact_native_integers_and_signed_timestamp_limits() {
    let environment = EnvGuard::new("signed-legacy-timestamps");
    for (thread, value) in [
        ("above-safe", 9_007_199_254_740_993),
        ("native-max", i64::MAX),
    ] {
        let mut original = goal(thread);
        original.status = ThreadGoalStatus::Complete;
        original.token_budget = Some(value);
        original.tokens_used = value;
        original.time_used_seconds = value;
        original.created_at_ms = if value == i64::MAX { i64::MIN } else { -value };
        original.updated_at_ms = value;
        let bytes = native_legacy_bytes(&original, true);
        environment.write_legacy(thread, &bytes);
        assert_eq!(get_goal(thread).await.unwrap(), Some(original.clone()));
        assert!(get_record(thread).await.unwrap().continuation_deferred);
        let connection = open_database(&environment.home).unwrap();
        let exact = connection.query_row(
            "SELECT token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms FROM goals WHERE thread_id=?1", [thread],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?)),
        ).unwrap();
        assert_eq!(
            exact,
            (
                original.token_budget,
                value,
                value,
                original.created_at_ms,
                value
            )
        );
        assert_eq!(
            std::fs::read(environment.legacy_path(thread)).unwrap(),
            bytes
        );
    }
    let mut unbounded = goal("unbounded");
    unbounded.token_budget = None;
    unbounded.created_at_ms = i64::MAX;
    unbounded.updated_at_ms = i64::MIN;
    let bytes = native_legacy_bytes(&unbounded, false);
    environment.write_legacy("unbounded", &bytes);
    assert_eq!(get_goal("unbounded").await.unwrap(), Some(unbounded));
    assert_eq!(
        std::fs::read(environment.legacy_path("unbounded")).unwrap(),
        bytes
    );
}

#[tokio::test]
async fn legacy_numeric_rejection_keeps_originals_and_migration_unconfirmed() {
    let environment = EnvGuard::new("signed-legacy-invalid");
    let original = goal("invalid");
    let valid: serde_json::Value =
        serde_json::from_slice(&native_legacy_bytes(&original, true)).unwrap();
    for field in [
        "tokenBudget",
        "tokensUsed",
        "timeUsedSeconds",
        "createdAtMs",
        "updatedAtMs",
    ] {
        for literal in [
            "9223372036854775808",
            "-9223372036854775809",
            "1.5",
            "true",
            "\"17\"",
        ] {
            let mut record = valid.clone();
            record["goal"][field] = serde_json::from_str(literal).unwrap();
            let bytes = serde_json::to_vec(&record).unwrap();
            environment.write_legacy("invalid", &bytes);
            assert_eq!(
                get_goal("invalid").await.unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{field}: {literal}"
            );
            assert_eq!(
                std::fs::read(environment.legacy_path("invalid")).unwrap(),
                bytes
            );
        }
    }
    let connection = open_database(&environment.home).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM goal_threads WHERE thread_id='invalid'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let mut fixed = original;
    fixed.created_at_ms = i64::MIN;
    fixed.updated_at_ms = i64::MAX;
    environment.write_legacy("invalid", &native_legacy_bytes(&fixed, true));
    assert_eq!(get_goal("invalid").await.unwrap(), Some(fixed));
}

#[tokio::test]
async fn signed_timestamps_leave_usage_and_budget_constraints_intact() {
    let environment = EnvGuard::new("signed-timestamps-invalid-usage");
    let mut original = goal("t1");
    original.created_at_ms = i64::MIN;
    original.updated_at_ms = i64::MAX;
    insert_goal(original).await.unwrap().unwrap();
    let connection = open_database(&environment.home).unwrap();
    for statement in [
        "UPDATE goals SET tokens_used=-1 WHERE thread_id='t1'",
        "UPDATE goals SET time_used_seconds=-1 WHERE thread_id='t1'",
        "UPDATE goals SET token_budget=0 WHERE thread_id='t1'",
    ] {
        assert_eq!(
            connection
                .execute(statement, [])
                .unwrap_err()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        );
    }
    let mut invalid = goal("invalid");
    invalid.created_at_ms = i64::MIN;
    invalid.updated_at_ms = i64::MAX;
    invalid.tokens_used = -1;
    assert_eq!(
        insert_goal(invalid).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(get_goal("invalid").await.unwrap().is_none());
}

#[tokio::test]
async fn structured_update_preflight_rejects_unsafe_native_result_before_any_mutation() {
    let _environment = EnvGuard::new("signed-timestamps-json-preflight");
    let mut native = goal("t1");
    native.token_budget = Some(i64::MAX);
    native.tokens_used = i64::MAX - 10;
    native.time_used_seconds = i64::MAX - 20;
    native.created_at_ms = i64::MIN;
    native.updated_at_ms = i64::MAX;
    insert_goal(native.clone()).await.unwrap().unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1, 1, None, i64::MAX)
        .await
        .unwrap();
    let before = get_record("t1").await.unwrap();
    let update = GoalUpdate {
        status: Some(ThreadGoalStatus::Complete),
        expected_goal_id: Some(native.goal_id.clone()),
        expected_intent_revision: Some(before.intent_revision),
        require_safe_json: true,
        ..Default::default()
    };
    assert_eq!(
        update_goal("t1", update, i64::MIN)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    let result = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            expected_goal_id: Some(native.goal_id),
            expected_intent_revision: Some(before.intent_revision),
            ..Default::default()
        },
        i64::MIN,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(
        (
            result.status,
            result.created_at_ms,
            result.updated_at_ms,
            result.tokens_used,
            result.time_used_seconds
        ),
        (
            ThreadGoalStatus::Complete,
            i64::MIN,
            i64::MIN,
            i64::MAX - 9,
            i64::MAX - 19
        )
    );
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.intent_revision, before.intent_revision + 1);
    assert_eq!(after.revision, before.revision + 1);
}

#[tokio::test]
async fn v7_signed_timestamp_migration_preserves_every_row_index_and_trigger() {
    let environment = EnvGuard::new("signed-timestamps-migration");
    let mut original = goal("t1");
    original.status = ThreadGoalStatus::BudgetLimited;
    original.tokens_used = 1_000;
    let resources = owned_resources("source", "preparation-1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(original.clone())),
        None,
        Some(GoalControlReceipt {
            request_id: "request-1".into(),
            fingerprint: "fp".into(),
        }),
        Some(Some(resources)),
    )
    .await
    .unwrap();
    assert!(begin_goal_run("t1", &original.goal_id, "owner-1")
        .await
        .unwrap());
    assert!(acknowledge_budget_wrapup("t1", &original.goal_id)
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(original.status),
            ..Default::default()
        },
        original.updated_at_ms,
    )
    .await
    .unwrap()
    .unwrap();
    insert_goal(goal("retired")).await.unwrap().unwrap();
    clear_goal("retired").await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    let old_schema = GOAL_TABLE_SCHEMA
        .replace(
            "CREATE TABLE IF NOT EXISTS goals (",
            "CREATE TABLE goals_v7 (",
        )
        .replace(
            "created_at_ms INTEGER NOT NULL,",
            "created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),",
        )
        .replace(
            "updated_at_ms INTEGER NOT NULL,",
            "updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),",
        );
    connection.execute_batch(&old_schema).unwrap();
    connection.execute_batch("INSERT INTO goals_v7 SELECT * FROM goals; DROP TABLE goals; ALTER TABLE goals_v7 RENAME TO goals; PRAGMA user_version=7;").unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    connection.execute_batch(
        "CREATE INDEX signed_goal_status ON goals(status,tokens_used);
         CREATE INDEX signed_deferral_generation ON goal_continuation_deferrals(goal_id);
         CREATE INDEX signed_run_owner ON goal_runs(owner_id);
         CREATE INDEX signed_wrapup_delivery ON goal_budget_wrapups(delivered);
         CREATE INDEX signed_resource_manifest ON goal_owned_resources(manifest);
         CREATE INDEX signed_private_intent ON goal_intents(intent_revision);
         CREATE INDEX signed_receipt_fingerprint ON goal_request_receipts(fingerprint);
         CREATE TRIGGER signed_goal_trigger BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT,'preserved goal trigger'); END;
         CREATE TRIGGER signed_resource_trigger BEFORE INSERT ON goal_owned_resources BEGIN SELECT RAISE(ABORT,'preserved resource trigger'); END;",
    ).unwrap();
    let tables = [
        "goal_threads",
        "goal_intents",
        "goal_generations",
        "goals",
        "goal_request_receipts",
        "goal_continuation_deferrals",
        "goal_runs",
        "goal_budget_wrapups",
        "goal_owned_resources",
    ];
    let rows: Vec<_> = tables
        .iter()
        .map(|table| native_table_rows(&connection, table))
        .collect();
    let objects = |connection: &Connection| {
        let mut query = connection.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name LIKE 'signed_%' ORDER BY name").unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    let schema_objects = objects(&connection);
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(objects(&connection), schema_objects);
    for (table, expected) in tables.iter().zip(rows) {
        assert_eq!(native_table_rows(&connection, table), expected, "{table}");
    }
    let migrated_schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='goals'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!migrated_schema.contains("created_at_ms >= 0"));
    assert!(!migrated_schema.contains("updated_at_ms >= 0"));
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        SCHEMA_VERSION
    );
    let foreign_key_failure: Option<String> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()
        .unwrap();
    assert_eq!(foreign_key_failure, None);
    connection
        .execute_batch("DROP TRIGGER signed_goal_trigger; DROP TRIGGER signed_resource_trigger;")
        .unwrap();
    let signed = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        i64::MIN,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(signed.updated_at_ms, i64::MIN);
}

#[tokio::test]
async fn durable_intent_ignores_accounting_recovery_and_run_bookkeeping() {
    let _environment = EnvGuard::new("intent-accounting");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let original_intent = get_record("t1").await.unwrap().intent_revision;
    assert_eq!(original_intent, 1);
    assert!(begin_goal_run("t1", &initial.goal_id, "old-owner")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 3, 2, None, 30)
        .await
        .unwrap();
    assert_eq!(
        get_record("t1").await.unwrap().intent_revision,
        original_intent
    );
    recover_goal_run("t1", "new-owner", 40).await.unwrap();
    let recovered = get_record("t1").await.unwrap();
    assert_eq!(recovered.intent_revision, original_intent);
    assert_eq!(
        recovered.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Paused
    );
    assert!(recovered.continuation_deferred);
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Active),
            expected_intent_revision: Some(original_intent),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap()
    .unwrap();
    let resumed = get_record("t1").await.unwrap();
    assert_eq!(resumed.intent_revision, original_intent + 1);
    assert!(begin_goal_run("t1", &initial.goal_id, "new-owner")
        .await
        .unwrap());
    account_usage("t1", GoalAccountingMode::ActiveOnly, 997, 2, None, 60)
        .await
        .unwrap();
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert!(acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    finish_goal_run("t1", &initial.goal_id, "new-owner")
        .await
        .unwrap();
    let limited = get_record("t1").await.unwrap();
    assert_eq!(limited.intent_revision, resumed.intent_revision);
    assert_eq!(
        limited.goal.unwrap().status,
        ThreadGoalStatus::BudgetLimited
    );
    assert!(limited.revision > resumed.revision);
}

#[tokio::test]
async fn private_intent_control_accepts_bookkeeping_and_its_own_pending_flush() {
    let _environment = EnvGuard::new("intent-control-own-flush");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let captured = get_record("t1").await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    account_usage("t1", GoalAccountingMode::ActiveOnly, 2, 1, None, 30)
        .await
        .unwrap();
    let before = get_record("t1").await.unwrap();
    assert!(before.revision > captured.revision);
    assert_eq!(before.intent_revision, captured.intent_revision);
    let outcome = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                expected_goal_id: Some(initial.goal_id.clone()),
                expected_intent_revision: Some(captured.intent_revision),
                ..Default::default()
            },
            now_ms: 50,
        }),
        Some(pending_accounting(&initial.goal_id, 5, 2)),
        None,
        None,
    )
    .await
    .unwrap();
    let stored = outcome.record.goal.as_ref().unwrap();
    assert_eq!(
        (stored.tokens_used, stored.time_used_seconds, stored.status),
        (7, 3, ThreadGoalStatus::Paused)
    );
    assert!(matches!(
        outcome.accounting,
        Some(GoalAccountingOutcome::Updated(_))
    ));
    assert_eq!(outcome.record.intent_revision, before.intent_revision + 1);
    assert_eq!(outcome.record.revision, before.revision + 1);
}

#[tokio::test]
async fn same_value_control_intent_advances_without_changing_public_goal_revision() {
    let _environment = EnvGuard::new("intent-same-value");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    for update in [
        GoalUpdate {
            objective: Some(initial.objective.clone()),
            ..Default::default()
        },
        GoalUpdate {
            status: Some(initial.status),
            ..Default::default()
        },
        GoalUpdate {
            token_budget: Some(initial.token_budget),
            ..Default::default()
        },
    ] {
        let before = get_record("t1").await.unwrap();
        update_goal("t1", update, initial.updated_at_ms)
            .await
            .unwrap()
            .unwrap();
        let after = get_record("t1").await.unwrap();
        assert_eq!(after.goal, before.goal);
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.intent_revision, before.intent_revision + 1);
    }
    let resources = owned_resources("t1", "preparation-1");
    for metadata in [None, Some(resources.clone()), Some(resources), None, None] {
        let before = get_record("t1").await.unwrap();
        let before_resources = get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap();
        let outcome = apply_goal_control(
            "t1",
            GoalControlMutation::Set(GoalRequestMutation::Update {
                update: GoalUpdate {
                    expected_intent_revision: Some(before.intent_revision),
                    ..Default::default()
                },
                now_ms: initial.updated_at_ms,
            }),
            None,
            None,
            Some(metadata.clone()),
        )
        .await
        .unwrap();
        assert_eq!(outcome.record.goal, before.goal);
        assert_eq!(outcome.record.intent_revision, before.intent_revision + 1);
        assert_eq!(
            outcome.record.revision,
            before.revision + i64::from(metadata != before_resources)
        );
        assert_eq!(
            get_goal_owned_resources("t1", &initial.goal_id)
                .await
                .unwrap(),
            metadata
        );
    }
    let before = get_record("t1").await.unwrap();
    update_goal("t1", GoalUpdate::default(), initial.updated_at_ms)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn private_intent_refuses_same_generation_status_aba_and_stale_clear() {
    let _environment = EnvGuard::new("intent-status-aba");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let captured = get_record("t1").await.unwrap();
    for status in [ThreadGoalStatus::Paused, ThreadGoalStatus::Active] {
        update_goal(
            "t1",
            GoalUpdate {
                status: Some(status),
                ..Default::default()
            },
            initial.updated_at_ms,
        )
        .await
        .unwrap()
        .unwrap();
    }
    let current = get_record("t1").await.unwrap();
    assert_eq!(current.goal, captured.goal);
    assert_eq!(current.intent_revision, captured.intent_revision + 2);
    let refused = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_intent_revision: Some(captured.intent_revision),
            ..Default::default()
        },
        40,
    )
    .await
    .unwrap();
    assert_eq!(refused, Err(initial.clone()));
    assert_eq!(
        clear_goal_if_intent_revision(
            "t1",
            Some(initial.goal_id.clone()),
            None,
            Some(captured.intent_revision)
        )
        .await
        .unwrap(),
        Err(initial)
    );
    assert_eq!(get_record("t1").await.unwrap(), current);
}

#[tokio::test]
async fn resource_aba_refuses_stale_intent_before_charging_or_binding() {
    let _environment = EnvGuard::new("intent-resource-aba");
    let initial = goal("t1");
    let resources = owned_resources("t1", "preparation-1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    let captured = get_record("t1").await.unwrap();
    for metadata in [None, Some(resources.clone())] {
        apply_goal_control(
            "t1",
            GoalControlMutation::Set(GoalRequestMutation::Update {
                update: GoalUpdate::default(),
                now_ms: initial.updated_at_ms,
            }),
            None,
            None,
            Some(metadata),
        )
        .await
        .unwrap();
    }
    let before = get_record("t1").await.unwrap();
    assert_eq!(before.goal, captured.goal);
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources.clone())
    );
    let error = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::Update {
            update: GoalUpdate {
                objective: Some("Stale authorized resource edit".into()),
                expected_goal_id: Some(initial.goal_id.clone()),
                expected_intent_revision: Some(captured.intent_revision),
                ..Default::default()
            },
            now_ms: 50,
        }),
        Some(pending_accounting(&initial.goal_id, 1_000, 3)),
        Some(GoalControlReceipt {
            request_id: "stale-resource-request".into(),
            fingerprint: "stale-resource-fp".into(),
        }),
        Some(Some(owned_resources("t1", "stale-preparation"))),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().starts_with("GOAL_CONFLICT: "));
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert!(get_goal_request_receipt("t1", "stale-resource-request")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn owned_resource_reads_atomically_check_intent_after_manifest_aba() {
    let environment = EnvGuard::new("owned-read-intent-aba");
    let initial = goal("t1");
    let resources = owned_resources("t1", "preparation-1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    let captured = get_record("t1").await.unwrap();
    assert_eq!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, captured.intent_revision)
            .await
            .unwrap(),
        Some(resources.clone())
    );
    for manifest in [None, Some(resources.clone())] {
        apply_goal_control(
            "t1",
            GoalControlMutation::Set(GoalRequestMutation::Update {
                update: GoalUpdate::default(),
                now_ms: initial.updated_at_ms,
            }),
            None,
            None,
            Some(manifest),
        )
        .await
        .unwrap();
    }
    let current = get_record("t1").await.unwrap();
    assert_eq!(current.goal, captured.goal);
    assert_eq!(current.intent_revision, captured.intent_revision + 2);
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources.clone())
    );
    assert!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, captured.intent_revision)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, current.intent_revision)
            .await
            .unwrap(),
        Some(resources)
    );
    assert!(get_goal_owned_resources_for_intent(
        "t1",
        &"stale-generation".into(),
        current.intent_revision
    )
    .await
    .unwrap()
    .is_none());
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute(
            "UPDATE goal_owned_resources SET manifest=?1 WHERE thread_id='t1'",
            [b"{corrupt current manifest".as_slice()],
        )
        .unwrap();
    assert!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, captured.intent_revision)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, current.intent_revision)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), current);
    clear_goal("t1").await.unwrap();
    let cleared = get_record("t1").await.unwrap();
    assert!(
        get_goal_owned_resources_for_intent("t1", &initial.goal_id, cleared.intent_revision)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(get_record("t1").await.unwrap(), cleared);
}

#[tokio::test]
async fn absent_clear_retains_intent_and_refuses_captured_model_create() {
    let _environment = EnvGuard::new("intent-absent-create");
    let captured = get_record("t1").await.unwrap();
    clear_goal("t1").await.unwrap();
    let cleared = get_record("t1").await.unwrap();
    assert_eq!(cleared.intent_revision, captured.intent_revision + 1);
    assert_eq!(
        insert_goal_intent_checked(goal("t1"), None, true, None, Some(captured.intent_revision))
            .await
            .unwrap(),
        Err(GoalInsertRejection::GenerationChanged(None))
    );
    let stale_clear =
        clear_goal_if_intent_revision("t1", None, None, Some(captured.intent_revision))
            .await
            .unwrap_err();
    assert!(stale_clear.to_string().starts_with("GOAL_CONFLICT: "));
    assert_eq!(get_record("t1").await.unwrap(), cleared);
    insert_goal_intent_checked(goal("t1"), None, true, None, Some(cleared.intent_revision))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        get_record("t1").await.unwrap().intent_revision,
        cleared.intent_revision + 1
    );
}

#[tokio::test]
async fn checked_create_compares_private_intent_when_public_revision_is_unchanged() {
    let _environment = EnvGuard::new("intent-checked-create");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::Complete;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let captured = get_record("t1").await.unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        initial.updated_at_ms,
    )
    .await
    .unwrap()
    .unwrap();
    let before = get_record("t1").await.unwrap();
    assert_eq!(before.revision, captured.revision);
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    let mutation = |expected_intent_revision| GoalRequestMutation::CreateChecked {
        goal: next.clone(),
        expected_goal_id: Some(initial.goal_id.clone()),
        expected_absent: false,
        expected_revision: Some(captured.revision),
        expected_intent_revision: Some(expected_intent_revision),
    };
    let error =
        apply_goal_request_once("t1", "request-1", "fp", mutation(captured.intent_revision))
            .await
            .unwrap_err();
    assert!(error.to_string().starts_with("GOAL_CONFLICT: "));
    assert_eq!(get_record("t1").await.unwrap(), before);
    let created =
        apply_goal_request_once("t1", "request-1", "fp", mutation(before.intent_revision))
            .await
            .unwrap();
    assert_eq!(created.goal, next);
    assert_eq!(
        get_record("t1").await.unwrap().intent_revision,
        before.intent_revision + 1
    );
}

#[tokio::test]
async fn intent_only_submission_rolls_back_on_receipt_failure_and_replay_is_read_only() {
    let environment = EnvGuard::new("intent-receipt-rollback");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_intent_receipt BEFORE INSERT ON goal_request_receipts BEGIN SELECT RAISE(ABORT, 'forced intent receipt failure'); END; CREATE TRIGGER reject_intent_goal_rewrite BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT, 'same-value intent must not rewrite goal'); END;").unwrap();
    let mutation = GoalRequestMutation::Update {
        update: GoalUpdate {
            status: Some(initial.status),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_intent_revision: Some(before.intent_revision),
            ..Default::default()
        },
        now_ms: initial.updated_at_ms,
    };
    assert_eq!(
        apply_goal_request_once("t1", "request-1", "fp", mutation.clone())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(get_goal_request_receipt("t1", "request-1")
        .await
        .unwrap()
        .is_none());
    connection
        .execute_batch("DROP TRIGGER reject_intent_receipt;")
        .unwrap();
    let receipt = apply_goal_request_once("t1", "request-1", "fp", mutation.clone())
        .await
        .unwrap();
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.intent_revision, before.intent_revision + 1);
    assert_eq!(receipt.revision, before.revision);
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(initial.status),
            ..Default::default()
        },
        initial.updated_at_ms,
    )
    .await
    .unwrap()
    .unwrap();
    let current = get_record("t1").await.unwrap();
    assert_eq!(
        apply_goal_request_once("t1", "request-1", "fp", mutation)
            .await
            .unwrap(),
        GoalRequestOutcome {
            replayed: true,
            ..receipt
        }
    );
    assert_eq!(get_record("t1").await.unwrap(), current);
}

#[tokio::test]
async fn same_value_intent_cas_has_one_winner_across_processes() {
    let environment = EnvGuard::new("intent-process-race");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("intent-status", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    assert!(environment.home.join("intent-status-owner").exists());
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.goal, before.goal);
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.intent_revision, before.intent_revision + 1);
}

#[tokio::test]
async fn v6_upgrade_initializes_intent_without_touching_goal_metadata_or_receipt() {
    let environment = EnvGuard::new("intent-schema-upgrade");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::BudgetLimited;
    initial.tokens_used = 1_100;
    let resources = owned_resources("t1", "preparation-1");
    let created = apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(initial.clone())),
        None,
        Some(GoalControlReceipt {
            request_id: "request-1".into(),
            fingerprint: "fp".into(),
        }),
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let wrapup = environment.stored_wrapup("t1");
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("DROP TABLE goal_intents; PRAGMA user_version = 6; CREATE TRIGGER reject_intent_migration_goal BEFORE INSERT ON goals BEGIN SELECT RAISE(ABORT, 'intent migration must preserve goal bytes'); END;").unwrap();
    let migrated = get_record("t1").await.unwrap();
    assert_eq!(
        migrated,
        GoalRecord {
            intent_revision: 0,
            ..before
        }
    );
    assert_eq!(
        get_goal_owned_resources("t1", &initial.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    assert_eq!(environment.stored_wrapup("t1"), wrapup);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert_eq!(
        get_goal_request_receipt("t1", "request-1").await.unwrap(),
        created.receipt.map(|receipt| GoalRequestOutcome {
            replayed: true,
            ..receipt
        })
    );
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        SCHEMA_VERSION
    );
}

#[tokio::test]
async fn missing_intent_row_and_exhausted_intent_counter_are_explicit_errors() {
    let environment = EnvGuard::new("intent-invalid");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute("DELETE FROM goal_intents WHERE thread_id='t1'", [])
        .unwrap();
    assert_eq!(
        get_record("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    connection
        .execute("INSERT INTO goal_intents VALUES ('t1', ?1)", [i64::MAX])
        .unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1, 1, None, 40)
        .await
        .unwrap();
    let before = get_record("t1").await.unwrap();
    assert_eq!(before.intent_revision, i64::MAX);
    assert_eq!(
        update_goal(
            "t1",
            GoalUpdate {
                status: Some(initial.status),
                ..Default::default()
            },
            50
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        clear_goal("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn revisions_survive_clear_and_new_generations() {
    let _environment = EnvGuard::new("revisions");
    assert_eq!(get_record("t1").await.unwrap(), GoalRecord::default());
    let initial = goal("t1");
    assert_eq!(
        insert_goal(initial.clone()).await.unwrap(),
        Ok(initial.clone())
    );
    assert_eq!(get_record("t1").await.unwrap().revision, 1);
    set_continuation_deferral("t1", true).await.unwrap();
    let deferred = get_record("t1").await.unwrap();
    assert!(deferred.continuation_deferred);
    assert_eq!(deferred.revision, 2);
    set_continuation_deferral("t1", true).await.unwrap();
    assert_eq!(
        get_record("t1").await.unwrap(),
        deferred,
        "no-op keeps revision"
    );
    assert_eq!(
        clear_goal_if("t1", Some("stale".into())).await.unwrap(),
        Err(initial.clone())
    );
    assert_eq!(get_record("t1").await.unwrap(), deferred);
    assert_eq!(clear_goal("t1").await.unwrap(), Some(initial));
    assert_eq!(
        get_record("t1").await.unwrap(),
        GoalRecord {
            revision: 3,
            intent_revision: 2,
            ..Default::default()
        }
    );
    clear_goal("t1").await.unwrap();
    assert_eq!(
        get_record("t1").await.unwrap().revision,
        4,
        "empty clear publishes revocation"
    );
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    insert_goal(next.clone()).await.unwrap().unwrap();
    assert_eq!(get_record("t1").await.unwrap().revision, 5);
    assert_eq!(get_goal("t1").await.unwrap(), Some(next));
}

#[tokio::test]
async fn stale_mutations_cannot_charge_clear_or_release_replacements() {
    let _environment = EnvGuard::new("stale");
    let mut finished = goal("t1");
    finished.status = ThreadGoalStatus::Complete;
    insert_goal(finished).await.unwrap().unwrap();
    let mut replacement = goal("t1");
    replacement.goal_id = "generation-2".into();
    insert_goal(replacement.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let refused = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            expected_goal_id: Some("generation-1".into()),
            ..Default::default()
        },
        99,
    )
    .await
    .unwrap();
    assert_eq!(refused, Err(replacement.clone()));
    assert_eq!(
        account_usage(
            "t1",
            GoalAccountingMode::ActiveOnly,
            1_000,
            8,
            Some("generation-1".into()),
            99
        )
        .await
        .unwrap(),
        GoalAccountingOutcome::Unchanged(Some(replacement.clone()))
    );
    assert_eq!(
        clear_goal_if("t1", Some("generation-1".into()))
            .await
            .unwrap(),
        Err(replacement.clone())
    );
    assert_eq!(
        set_continuation_deferral_if("t1", false, Some("generation-1".into()))
            .await
            .unwrap(),
        Err(replacement)
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    clear_goal("t1").await.unwrap();
    assert_eq!(
        set_continuation_deferral_if("t1", true, Some("generation-2".into()))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!has_continuation_deferral("t1").await.unwrap());
}

#[tokio::test]
async fn replacement_requires_completion_and_a_new_generation() {
    let _environment = EnvGuard::new("replacement");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert_eq!(
        insert_goal(initial.clone()).await.unwrap(),
        Err(GoalInsertRejection::Unfinished(initial))
    );
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        30,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        insert_goal(goal("t1")).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    set_continuation_deferral("t1", true).await.unwrap();
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    insert_goal(next).await.unwrap().unwrap();
    assert!(!has_continuation_deferral("t1").await.unwrap());
}

#[tokio::test]
async fn insertion_compares_the_observed_generation_in_its_transaction() {
    let _environment = EnvGuard::new("insert-guard");
    assert_eq!(
        insert_goal_if(goal("t1"), Some("missing".into()))
            .await
            .unwrap(),
        Err(GoalInsertRejection::GenerationChanged(None))
    );
    let mut finished = goal("t1");
    finished.status = ThreadGoalStatus::Complete;
    insert_goal(finished.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let mut replacement = goal("t1");
    replacement.goal_id = "generation-2".into();
    assert_eq!(
        insert_goal_if(replacement.clone(), Some("stale".into()))
            .await
            .unwrap(),
        Err(GoalInsertRejection::GenerationChanged(Some(finished)))
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        insert_goal_if(replacement.clone(), Some("generation-1".into()))
            .await
            .unwrap(),
        Ok(replacement)
    );
}

#[tokio::test]
async fn concurrent_same_generation_objective_edits_compare_revision_atomically() {
    let _environment = EnvGuard::new("revision-edit-race");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let first = GoalUpdate {
        objective: Some("First editor".into()),
        expected_goal_id: Some(initial.goal_id.clone()),
        expected_revision: Some(before.revision),
        ..Default::default()
    };
    let second = GoalUpdate {
        objective: Some("Second editor".into()),
        expected_goal_id: Some(initial.goal_id.clone()),
        expected_revision: Some(before.revision),
        ..Default::default()
    };
    let (first, second) = tokio::join!(update_goal("t1", first, 40), update_goal("t1", second, 50));
    let winner = match (first.unwrap(), second.unwrap()) {
        (Ok(Some(winner)), Err(current)) | (Err(current), Ok(Some(winner))) => {
            assert_eq!(winner, current);
            winner
        }
        results => panic!("one edit must win and the other must observe its revision: {results:?}"),
    };
    assert_eq!(winner.goal_id, initial.goal_id);
    assert_eq!(
        get_record("t1").await.unwrap(),
        GoalRecord {
            goal: Some(winner),
            continuation_deferred: false,
            intent_revision: before.intent_revision + 1,
            revision: before.revision + 1
        }
    );
}

#[tokio::test]
async fn stale_revision_status_and_clear_leave_current_goal_and_markers_unchanged() {
    let environment = EnvGuard::new("revision-status-clear");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let observed = get_record("t1").await.unwrap();
    let edited = update_goal(
        "t1",
        GoalUpdate {
            objective: Some("New objective".into()),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_revision: Some(observed.revision),
            ..Default::default()
        },
        40,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let before_refused = get_record("t1").await.unwrap();
    let refused = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Paused),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_revision: Some(observed.revision),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap();
    assert_eq!(refused, Err(edited.clone()));
    assert_eq!(
        clear_goal_if_revision("t1", Some(initial.goal_id.clone()), Some(observed.revision))
            .await
            .unwrap(),
        Err(edited.clone())
    );
    assert_eq!(get_record("t1").await.unwrap(), before_refused);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert_eq!(
        clear_goal_if_revision("t1", Some(initial.goal_id), Some(before_refused.revision))
            .await
            .unwrap(),
        Ok(Some(edited))
    );
    assert_eq!(environment.stored_run("t1"), None);
    assert!(!get_record("t1").await.unwrap().continuation_deferred);
}

#[tokio::test]
async fn absent_revision_guard_prevents_clear_and_create_aba() {
    let _environment = EnvGuard::new("revision-absent");
    let initial = get_record("t1").await.unwrap();
    clear_goal("t1").await.unwrap();
    let cleared = get_record("t1").await.unwrap();
    assert_eq!(
        update_goal(
            "t1",
            GoalUpdate {
                expected_revision: Some(initial.revision),
                ..Default::default()
            },
            50
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        clear_goal_if_revision("t1", None, Some(initial.revision))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        insert_goal_checked(goal("t1"), None, true, Some(initial.revision))
            .await
            .unwrap(),
        Err(GoalInsertRejection::GenerationChanged(None))
    );
    assert_eq!(get_record("t1").await.unwrap(), cleared);
    assert_eq!(
        insert_goal_checked(goal("t1"), None, true, Some(cleared.revision))
            .await
            .unwrap(),
        Ok(goal("t1"))
    );
}

#[tokio::test]
async fn request_update_revision_guard_rejects_stale_mutation_but_replays_original_receipt() {
    let _environment = EnvGuard::new("revision-request-update");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    let update = GoalUpdate {
        objective: Some("First submit".into()),
        expected_goal_id: Some(initial.goal_id.clone()),
        expected_revision: Some(observed.revision),
        ..Default::default()
    };
    let accepted = apply_goal_request_once(
        "t1",
        "request-1",
        "expected-revision-1",
        GoalRequestMutation::Update {
            update: update.clone(),
            now_ms: 40,
        },
    )
    .await
    .unwrap();
    let before_stale = get_record("t1").await.unwrap();
    let stale = GoalUpdate {
        status: Some(ThreadGoalStatus::Paused),
        expected_goal_id: Some(initial.goal_id),
        expected_revision: Some(observed.revision),
        ..Default::default()
    };
    assert_eq!(
        apply_goal_request_once(
            "t1",
            "request-2",
            "stale-pause-revision-1",
            GoalRequestMutation::Update {
                update: stale,
                now_ms: 50
            }
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(get_goal_request_receipt("t1", "request-2")
        .await
        .unwrap()
        .is_none());
    let replay = apply_goal_request_once(
        "t1",
        "request-1",
        "expected-revision-1",
        GoalRequestMutation::Update { update, now_ms: 60 },
    )
    .await
    .unwrap();
    assert_eq!(
        replay,
        GoalRequestOutcome {
            replayed: true,
            ..accepted
        }
    );
    assert_eq!(get_record("t1").await.unwrap(), before_stale);
}

#[tokio::test]
async fn checked_create_receipt_compares_finished_goal_revision_before_replacement() {
    let _environment = EnvGuard::new("revision-request-create");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::Complete;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("Updated completed objective".into()),
            ..Default::default()
        },
        40,
    )
    .await
    .unwrap()
    .unwrap();
    let current = get_record("t1").await.unwrap();
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    let stale = GoalRequestMutation::CreateChecked {
        goal: next.clone(),
        expected_goal_id: Some(initial.goal_id.clone()),
        expected_absent: false,
        expected_revision: Some(observed.revision),
        expected_intent_revision: None,
    };
    assert_eq!(
        apply_goal_request_once("t1", "stale-create", "revision-1", stale)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(get_record("t1").await.unwrap(), current);
    assert!(get_goal_request_receipt("t1", "stale-create")
        .await
        .unwrap()
        .is_none());
    let accepted = GoalRequestMutation::CreateChecked {
        goal: next.clone(),
        expected_goal_id: Some(initial.goal_id),
        expected_absent: false,
        expected_revision: Some(current.revision),
        expected_intent_revision: None,
    };
    assert_eq!(
        apply_goal_request_once("t1", "current-create", "revision-2", accepted)
            .await
            .unwrap()
            .goal,
        next
    );
}

#[tokio::test]
async fn cross_process_same_generation_revision_cas_has_one_editor_winner() {
    let environment = EnvGuard::new("revision-process-race");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("revision-edit", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    assert!(environment.home.join("revision-edit-owner").exists());
    let stored = get_record("t1").await.unwrap();
    assert_eq!(stored.revision, 2);
    assert!(stored
        .goal
        .unwrap()
        .objective
        .starts_with("Process editor "));
}

fn pending_accounting(goal_id: &RuntimeString, tokens: i64, seconds: i64) -> GoalControlAccounting {
    GoalControlAccounting {
        mode: GoalAccountingMode::ActiveOnly,
        goal_id: goal_id.clone(),
        token_delta: tokens,
        time_delta_seconds: seconds,
        now_ms: 40,
    }
}

fn checked_pause(goal_id: &RuntimeString, revision: i64) -> GoalControlMutation {
    GoalControlMutation::Set(GoalRequestMutation::Update {
        update: GoalUpdate {
            status: Some(ThreadGoalStatus::Paused),
            expected_goal_id: Some(goal_id.clone()),
            expected_revision: Some(revision),
            ..Default::default()
        },
        now_ms: 50,
    })
}

#[tokio::test]
async fn host_pause_accounts_own_pending_progress_without_invalidating_observed_revision() {
    let _environment = EnvGuard::new("control-own-flush");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    let outcome = apply_goal_control(
        "t1",
        checked_pause(&initial.goal_id, observed.revision),
        Some(pending_accounting(&initial.goal_id, 11, 3)),
        None,
        None,
    )
    .await
    .unwrap();
    let stored = outcome.record.goal.as_ref().unwrap();
    assert_eq!(
        (stored.status, stored.tokens_used, stored.time_used_seconds),
        (ThreadGoalStatus::Paused, 11, 3)
    );
    assert_eq!(outcome.record.revision, observed.revision + 1);
    assert!(
        matches!(outcome.accounting, Some(GoalAccountingOutcome::Updated(ref goal)) if goal.tokens_used == 11 && goal.time_used_seconds == 3)
    );
    assert_eq!(get_record("t1").await.unwrap(), outcome.record);
}

#[tokio::test]
async fn external_edit_rejects_host_control_before_accounting_any_pending_delta() {
    let environment = EnvGuard::new("control-external-intent");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let observed = get_record("t1").await.unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("External edit".into()),
            ..Default::default()
        },
        30,
    )
    .await
    .unwrap()
    .unwrap();
    let before = get_record("t1").await.unwrap();
    let error = apply_goal_control(
        "t1",
        checked_pause(&initial.goal_id, observed.revision),
        Some(pending_accounting(&initial.goal_id, 1_000, 3)),
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert!(error.to_string().starts_with("GOAL_CONFLICT: "));
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert_eq!(environment.stored_wrapup("t1"), None);
}

#[tokio::test]
async fn own_flush_budget_crossing_and_pause_commit_with_one_revision() {
    let _environment = EnvGuard::new("control-budget-pause");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    let outcome = apply_goal_control(
        "t1",
        checked_pause(&initial.goal_id, observed.revision),
        Some(pending_accounting(&initial.goal_id, 1_000, 3)),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome.record.revision, observed.revision + 1);
    assert_eq!(
        outcome.record.goal.unwrap().status,
        ThreadGoalStatus::BudgetLimited
    );
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
}

#[tokio::test]
async fn host_clear_accounts_final_usage_and_removes_all_generation_state_atomically() {
    let environment = EnvGuard::new("control-clear-flush");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let observed = get_record("t1").await.unwrap();
    let outcome = apply_goal_control(
        "t1",
        GoalControlMutation::Clear {
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_revision: Some(observed.revision),
            expected_intent_revision: None,
        },
        Some(pending_accounting(&initial.goal_id, 23, 4)),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(outcome.record.goal.is_none());
    assert_eq!(outcome.record.revision, observed.revision + 1);
    let removed = outcome.removed_goal.unwrap();
    assert_eq!((removed.tokens_used, removed.time_used_seconds), (23, 4));
    assert_eq!(environment.stored_run("t1"), None);
    assert_eq!(environment.stored_wrapup("t1"), None);
    assert!(!get_record("t1").await.unwrap().continuation_deferred);
}

#[tokio::test]
async fn checked_replacement_accounts_previous_goal_and_receipts_new_snapshot_atomically() {
    let _environment = EnvGuard::new("control-create-flush");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::Complete;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    let mut accounting = pending_accounting(&initial.goal_id, 19, 3);
    accounting.mode = GoalAccountingMode::ActiveOrComplete;
    let outcome = apply_goal_control(
        "t1",
        GoalControlMutation::Set(GoalRequestMutation::CreateChecked {
            goal: next.clone(),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_absent: false,
            expected_revision: Some(observed.revision),
            expected_intent_revision: None,
        }),
        Some(accounting),
        Some(GoalControlReceipt {
            request_id: "replace-1".into(),
            fingerprint: "replace-revision-1".into(),
        }),
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome.record.goal, Some(next.clone()));
    assert!(
        matches!(outcome.accounting, Some(GoalAccountingOutcome::Updated(ref goal)) if goal.goal_id == initial.goal_id && goal.tokens_used == 19)
    );
    assert_eq!(outcome.receipt.unwrap().goal, next);
    assert_eq!(outcome.record.revision, observed.revision + 1);
}

#[tokio::test]
async fn control_receipt_replay_never_recharges_its_supplied_accounting_snapshot() {
    let _environment = EnvGuard::new("control-receipt-replay");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let observed = get_record("t1").await.unwrap();
    let mutation = GoalControlMutation::Set(GoalRequestMutation::Update {
        update: GoalUpdate {
            objective: Some("Submitted edit".into()),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_revision: Some(observed.revision),
            ..Default::default()
        },
        now_ms: 50,
    });
    let receipt = GoalControlReceipt {
        request_id: "request-1".into(),
        fingerprint: "edit-revision-1".into(),
    };
    let first = apply_goal_control(
        "t1",
        mutation.clone(),
        Some(pending_accounting(&initial.goal_id, 10, 1)),
        Some(receipt.clone()),
        None,
    )
    .await
    .unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("Later edit".into()),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap();
    let current = get_record("t1").await.unwrap();
    let replay = apply_goal_control(
        "t1",
        mutation,
        Some(pending_accounting(&initial.goal_id, 10, 1)),
        Some(receipt),
        None,
    )
    .await
    .unwrap();
    assert_eq!(replay.accounting, None);
    assert_eq!(replay.record, current);
    assert_eq!(replay.record.goal.unwrap().tokens_used, 10);
    assert_eq!(
        replay.receipt.unwrap(),
        GoalRequestOutcome {
            replayed: true,
            ..first.receipt.unwrap()
        }
    );
}

#[tokio::test]
async fn control_receipt_failure_rolls_back_own_usage_intent_and_wrapup_obligation() {
    let environment = EnvGuard::new("control-receipt-rollback");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let observed = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_control_receipt BEFORE INSERT ON goal_request_receipts BEGIN SELECT RAISE(ABORT, 'forced combined write failure'); END;").unwrap();
    let mutation = GoalControlMutation::Set(GoalRequestMutation::Update {
        update: GoalUpdate {
            objective: Some("Submitted edit".into()),
            expected_goal_id: Some(initial.goal_id.clone()),
            expected_revision: Some(observed.revision),
            ..Default::default()
        },
        now_ms: 50,
    });
    let receipt = GoalControlReceipt {
        request_id: "request-1".into(),
        fingerprint: "edit-observed-revision".into(),
    };
    assert_eq!(
        apply_goal_control(
            "t1",
            mutation.clone(),
            Some(pending_accounting(&initial.goal_id, 1_000, 2)),
            Some(receipt.clone()),
            None,
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), observed);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert_eq!(environment.stored_wrapup("t1"), None);
    assert!(get_goal_request_receipt("t1", "request-1")
        .await
        .unwrap()
        .is_none());
    connection
        .execute_batch("DROP TRIGGER reject_control_receipt;")
        .unwrap();
    let retry = apply_goal_control(
        "t1",
        mutation,
        Some(pending_accounting(&initial.goal_id, 1_000, 2)),
        Some(receipt),
        None,
    )
    .await
    .unwrap();
    assert_eq!(retry.record.goal.unwrap().tokens_used, 1_000);
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
}

#[tokio::test]
async fn conditional_absent_clear_is_conflict_while_unconditional_clear_revokes() {
    let _environment = EnvGuard::new("control-clear-absent");
    clear_goal("t1").await.unwrap();
    let before = get_record("t1").await.unwrap();
    let error = clear_goal_if_revision("t1", Some("old-generation".into()), Some(before.revision))
        .await
        .unwrap_err();
    assert!(error.to_string().starts_with("GOAL_CONFLICT: "));
    assert_eq!(get_record("t1").await.unwrap(), before);
    let error = apply_goal_control(
        "t1",
        GoalControlMutation::Clear {
            expected_goal_id: Some("old-generation".into()),
            expected_revision: Some(before.revision),
            expected_intent_revision: None,
        },
        None,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().starts_with("GOAL_CONFLICT: "));
    let cleared = apply_goal_control(
        "t1",
        GoalControlMutation::Clear {
            expected_goal_id: None,
            expected_revision: Some(before.revision),
            expected_intent_revision: None,
        },
        None,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(cleared.record.revision, before.revision + 1);
}

#[tokio::test]
async fn provider_run_admission_allows_only_pending_or_owned_budget_wrapup() {
    let environment = EnvGuard::new("run-wrapup-admission");
    let mut limited = goal("limited");
    limited.status = ThreadGoalStatus::BudgetLimited;
    limited.tokens_used = 1_000;
    insert_goal(limited.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("limited", &limited.goal_id, "wrapup-owner")
        .await
        .unwrap());
    assert!(acknowledge_budget_wrapup("limited", &limited.goal_id)
        .await
        .unwrap());
    assert!(
        begin_goal_run("limited", &limited.goal_id, "wrapup-owner")
            .await
            .unwrap(),
        "the owned in-turn final response remains admitted"
    );
    assert!(!begin_goal_run("limited", &limited.goal_id, "other-owner")
        .await
        .unwrap());
    finish_goal_run("limited", &limited.goal_id, "wrapup-owner")
        .await
        .unwrap();
    assert!(
        !begin_goal_run("limited", &limited.goal_id, "wrapup-owner")
            .await
            .unwrap(),
        "delivered budget state cannot admit another run"
    );
    assert_eq!(environment.stored_run("limited"), None);
    let active = goal("stopped");
    insert_goal(active.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("stopped", &active.goal_id, "owner")
        .await
        .unwrap());
    for status in [
        ThreadGoalStatus::Paused,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
        ThreadGoalStatus::Complete,
    ] {
        update_goal(
            "stopped",
            GoalUpdate {
                status: Some(status),
                ..Default::default()
            },
            50,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !begin_goal_run("stopped", &active.goal_id, "owner")
                .await
                .unwrap(),
            "unrelated stopped state cannot admit a provider request"
        );
    }
}

#[tokio::test]
async fn concurrent_absent_guarded_creates_have_exactly_one_winner() {
    let _environment = EnvGuard::new("absent-insert-race");
    let mut first = goal("t1");
    first.status = ThreadGoalStatus::Complete;
    let mut second = first.clone();
    second.goal_id = "generation-2".into();
    second.objective = "A concurrent objective".into();
    // Completed goals allow ordinary replacement, so the absence guard alone
    // must prevent the second creator from overwriting the first one's result.
    let (first_result, second_result) = tokio::join!(
        insert_goal_guarded(first, None, true),
        insert_goal_guarded(second, None, true),
    );
    let winner = match (first_result.unwrap(), second_result.unwrap()) {
        (Ok(winner), Err(GoalInsertRejection::GenerationChanged(Some(current))))
        | (Err(GoalInsertRejection::GenerationChanged(Some(current))), Ok(winner)) => {
            assert_eq!(winner, current);
            winner
        }
        results => panic!("one creator must win and the other must observe it: {results:?}"),
    };
    assert_eq!(
        get_record("t1").await.unwrap(),
        GoalRecord {
            goal: Some(winner),
            continuation_deferred: false,
            intent_revision: 1,
            revision: 1,
        }
    );
}

fn request_create(goal: ThreadGoal) -> GoalRequestMutation {
    GoalRequestMutation::Create {
        goal,
        expected_goal_id: None,
        expected_absent: true,
    }
}

fn request_update() -> GoalRequestMutation {
    GoalRequestMutation::Update {
        update: GoalUpdate {
            objective: Some("Submitted once".into()),
            expected_goal_id: Some("generation-1".into()),
            ..Default::default()
        },
        now_ms: 40,
    }
}

#[tokio::test]
async fn stale_run_recovery_pauses_and_retains_independent_fork_deferral() {
    let environment = EnvGuard::new("run-recovery");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "old-owner")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    account_usage(
        "t1",
        GoalAccountingMode::ActiveOnly,
        5,
        1,
        Some(initial.goal_id.clone()),
        30,
    )
    .await
    .unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("Progress before restart".into()),
            ..Default::default()
        },
        35,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        environment.stored_run("t1"),
        Some(GoalRun {
            goal_id: initial.goal_id.clone(),
            owner_id: "old-owner".into()
        })
    );
    let before = get_record("t1").await.unwrap();
    assert!(!begin_goal_run("t1", &initial.goal_id, "new-owner")
        .await
        .unwrap());
    assert!(begin_goal_run("t1", &initial.goal_id, "old-owner")
        .await
        .unwrap());
    assert_eq!(recover_goal_run("t1", "old-owner", 40).await.unwrap(), None);
    assert_eq!(get_record("t1").await.unwrap(), before);
    let recovered = recover_goal_run("t1", "new-owner", 50)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            recovered.status,
            recovered.tokens_used,
            recovered.time_used_seconds,
            recovered.updated_at_ms
        ),
        (ThreadGoalStatus::Paused, 5, 1, 50)
    );
    assert_eq!(
        recovered.objective,
        RuntimeString::from("Progress before restart")
    );
    let after = get_record("t1").await.unwrap();
    assert!(after.continuation_deferred);
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(environment.stored_run("t1"), None);
    assert_eq!(recover_goal_run("t1", "new-owner", 60).await.unwrap(), None);
    assert!(!begin_goal_run("t1", &initial.goal_id, "new-owner")
        .await
        .unwrap());
    assert_eq!(get_record("t1").await.unwrap(), after);
}

#[tokio::test]
async fn persisted_run_recovery_preserves_counters_across_offline_wall_time() {
    let environment = EnvGuard::new("run-offline-recovery");
    let mut original = goal("t1");
    original.token_budget = Some(5_000);
    original.tokens_used = 987;
    original.time_used_seconds = 1_234;
    original.created_at_ms = 1_000;
    original.updated_at_ms = 2_000;
    let resources = owned_resources("t1", "preparation-1");
    apply_goal_control(
        "t1",
        GoalControlMutation::Set(request_create(original.clone())),
        None,
        None,
        Some(Some(resources.clone())),
    )
    .await
    .unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(environment.child("claim-run", 0).wait().unwrap().success());
    assert!(environment.home.join("run-claim-owner").exists());
    let persisted = get_record("t1").await.unwrap();
    assert_eq!(persisted.goal, Some(original.clone()));
    assert!(environment
        .stored_run("t1")
        .unwrap()
        .owner_id
        .starts_with("process-owner-"));
    let offline_duration_ms = 30 * 24 * 60 * 60 * 1_000_i64;
    let recovery_at_ms = original.updated_at_ms + offline_duration_ms;
    let recovered = recover_goal_run("t1", "restarted-session", recovery_at_ms)
        .await
        .unwrap()
        .unwrap();
    let mut expected = original;
    expected.status = ThreadGoalStatus::Paused;
    expected.updated_at_ms = recovery_at_ms;
    assert_eq!(recovered, expected);
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.goal, Some(expected.clone()));
    assert_eq!(after.revision, persisted.revision + 1);
    assert!(after.continuation_deferred);
    assert!(environment.stored_run("t1").is_none());
    assert_eq!(
        get_goal_owned_resources("t1", &expected.goal_id)
            .await
            .unwrap(),
        Some(resources)
    );
    assert_eq!(
        recover_goal_run(
            "t1",
            "another-restarted-session",
            recovery_at_ms + offline_duration_ms,
        )
        .await
        .unwrap(),
        None
    );
    assert!(
        !begin_goal_run("t1", &expected.goal_id, "restarted-session")
            .await
            .unwrap()
    );
    assert_eq!(get_record("t1").await.unwrap(), after);
}

#[tokio::test]
async fn run_finish_requires_both_owner_and_generation_and_is_idempotent() {
    let environment = EnvGuard::new("run-finish");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let before = get_record("t1").await.unwrap();
    finish_goal_run("t1", &initial.goal_id, "other-owner")
        .await
        .unwrap();
    finish_goal_run("t1", &"stale-generation".into(), "owner-1")
        .await
        .unwrap();
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    finish_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap();
    assert_eq!(environment.stored_run("t1"), None);
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    finish_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap();
    assert_eq!(get_record("t1").await.unwrap(), after);
}

#[tokio::test]
async fn run_clear_and_replacement_cascade_without_late_callback_resurrection() {
    let environment = EnvGuard::new("run-generation");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    clear_goal("t1").await.unwrap();
    assert_eq!(environment.stored_run("t1"), None);
    assert!(!begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    assert_eq!(recover_goal_run("t1", "new-owner", 50).await.unwrap(), None);
    assert!(get_goal("t1").await.unwrap().is_none());
    let mut second = goal("t1");
    second.goal_id = "generation-2".into();
    insert_goal(second.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &second.goal_id, "owner-2")
        .await
        .unwrap());
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-2");
    let mut third = goal("t1");
    third.goal_id = "generation-3".into();
    insert_goal(third.clone()).await.unwrap().unwrap();
    assert_eq!(environment.stored_run("t1"), None);
    assert!(begin_goal_run("t1", &third.goal_id, "owner-3")
        .await
        .unwrap());
    let before = get_record("t1").await.unwrap();
    finish_goal_run("t1", &second.goal_id, "owner-2")
        .await
        .unwrap();
    assert!(!begin_goal_run("t1", &second.goal_id, "owner-2")
        .await
        .unwrap());
    assert_eq!(recover_goal_run("t1", "owner-3", 70).await.unwrap(), None);
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-3");
}

#[tokio::test]
async fn stale_run_recovery_preserves_a_preexisting_budget_stop() {
    let environment = EnvGuard::new("run-budget-recovery");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "old-owner")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    account_usage(
        "t1",
        GoalAccountingMode::ActiveOnly,
        1_100,
        2,
        Some(initial.goal_id),
        40,
    )
    .await
    .unwrap();
    let before = get_record("t1").await.unwrap();
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "old-owner");
    let recovered = recover_goal_run("t1", "new-owner", 50)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            recovered.status,
            recovered.tokens_used,
            recovered.updated_at_ms
        ),
        (ThreadGoalStatus::BudgetLimited, 1_100, 40)
    );
    let after = get_record("t1").await.unwrap();
    assert_eq!(after.goal, before.goal);
    assert!(after.continuation_deferred);
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(environment.stored_run("t1"), None);
}

#[tokio::test]
async fn failed_run_recovery_rolls_back_marker_status_and_revision() {
    let environment = EnvGuard::new("run-recovery-rollback");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "old-owner")
        .await
        .unwrap());
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_recovery_revision BEFORE UPDATE ON goal_threads BEGIN SELECT RAISE(ABORT, 'forced recovery failure'); END;").unwrap();
    assert_eq!(
        recover_goal_run("t1", "new-owner", 50)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "old-owner");
    assert!(!begin_goal_run("t1", &initial.goal_id, "new-owner")
        .await
        .unwrap());
}

#[tokio::test]
async fn concurrent_run_owners_have_exactly_one_successful_claim() {
    let environment = EnvGuard::new("run-owner-race");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let (first, second) = tokio::join!(
        begin_goal_run("t1", &initial.goal_id, "owner-1"),
        begin_goal_run("t1", &initial.goal_id, "owner-2")
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(first, second);
    assert_eq!(get_record("t1").await.unwrap().revision, 2);
    let owner = if first { "owner-1" } else { "owner-2" };
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, owner);
}

#[tokio::test]
async fn run_claim_is_cross_process_and_recovery_after_exit_is_explicit() {
    let environment = EnvGuard::new("run-process-owner");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("claim-run", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    assert!(environment.home.join("run-claim-owner").exists());
    assert!(environment
        .stored_run("t1")
        .unwrap()
        .owner_id
        .starts_with("process-owner-"));
    let before = get_record("t1").await.unwrap();
    assert_eq!(
        before.goal.as_ref().unwrap().status,
        ThreadGoalStatus::Active,
        "process exit alone does not silently restart or mutate the goal"
    );
    let recovered = recover_goal_run("t1", "current-session-owner", 50)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, ThreadGoalStatus::Paused);
    assert!(get_record("t1").await.unwrap().continuation_deferred);
    assert_eq!(environment.stored_run("t1"), None);
}

#[tokio::test]
async fn schema_upgrade_adds_runs_without_mutating_goal_or_receipt() {
    let environment = EnvGuard::new("run-schema-upgrade");
    let original = apply_goal_request_once("t1", "request-1", "fp", request_create(goal("t1")))
        .await
        .unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    let goal_schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'goals'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute_batch(
            "DROP TABLE goal_owned_resources; DROP TABLE goal_runs; PRAGMA user_version = 3;",
        )
        .unwrap();
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        get_goal_request_receipt("t1", "request-1").await.unwrap(),
        Some(GoalRequestOutcome {
            replayed: true,
            ..original
        })
    );
    let upgraded_goal_schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'goals'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(goal_schema, upgraded_goal_schema);
    assert!(begin_goal_run("t1", &"generation-1".into(), "owner-1")
        .await
        .unwrap());
}

#[tokio::test]
async fn accounting_threshold_persists_pending_wrapup_without_consuming_it() {
    let environment = EnvGuard::new("budget-wrapup-threshold");
    let mut initial = goal("t1");
    initial.token_budget = Some(10);
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    account_usage(
        "t1",
        GoalAccountingMode::ActiveOnly,
        4,
        1,
        Some(initial.goal_id.clone()),
        30,
    )
    .await
    .unwrap();
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    // A root or descendant usage flush crosses the ceiling. Inspecting or
    // discarding the accounting outcome must not acknowledge the notice.
    account_usage(
        "t1",
        GoalAccountingMode::ActiveOnly,
        6,
        1,
        Some(initial.goal_id.clone()),
        40,
    )
    .await
    .unwrap();
    let threshold = get_record("t1").await.unwrap();
    assert_eq!(
        threshold.goal.as_ref().unwrap().status,
        ThreadGoalStatus::BudgetLimited
    );
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert_eq!(get_record("t1").await.unwrap(), threshold);
    assert_eq!(
        environment.stored_wrapup("t1"),
        Some(GoalBudgetWrapup {
            goal_id: initial.goal_id,
            delivered: false
        })
    );
}

#[tokio::test]
async fn delivered_wrapup_never_resets_on_repeated_charge_or_same_generation_edits() {
    let environment = EnvGuard::new("budget-wrapup-once");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 1, None, 30)
        .await
        .unwrap();
    let before_ack = get_record("t1").await.unwrap();
    assert!(acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    let after_ack = get_record("t1").await.unwrap();
    assert_eq!(after_ack.revision, before_ack.revision + 1);
    assert!(!acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    assert_eq!(get_record("t1").await.unwrap(), after_ack);
    account_usage("t1", GoalAccountingMode::ActiveOnly, 0, 0, None, 40)
        .await
        .unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 25, 1, None, 50)
        .await
        .unwrap();
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("Edited after delivery".into()),
            token_budget: Some(Some(2_000)),
            status: Some(ThreadGoalStatus::Active),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 1, None, 70)
        .await
        .unwrap();
    assert_eq!(
        get_goal("t1").await.unwrap().unwrap().status,
        ThreadGoalStatus::BudgetLimited
    );
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert!(environment.stored_wrapup("t1").unwrap().delivered);
}

#[tokio::test]
async fn concurrent_wrapup_acknowledgments_change_delivery_once() {
    let _environment = EnvGuard::new("budget-wrapup-ack-race");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::BudgetLimited;
    initial.tokens_used = 1_000;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let (first, second) = tokio::join!(
        acknowledge_budget_wrapup("t1", &initial.goal_id),
        acknowledge_budget_wrapup("t1", &initial.goal_id)
    );
    assert_ne!(first.unwrap(), second.unwrap());
    assert_eq!(
        get_record("t1").await.unwrap().revision,
        before.revision + 1
    );
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
}

#[tokio::test]
async fn stale_wrapup_acknowledgment_cannot_consume_replacement_or_resurrect_clear() {
    let environment = EnvGuard::new("budget-wrapup-generation");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::BudgetLimited;
    initial.tokens_used = 1_000;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    clear_goal("t1").await.unwrap();
    assert_eq!(environment.stored_wrapup("t1"), None);
    assert!(!acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    let mut next = initial.clone();
    next.goal_id = "generation-2".into();
    insert_goal(next.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    assert!(!acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(pending_budget_wrapup("t1", &next.goal_id).await.unwrap());
    update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap()
    .unwrap();
    let mut replacement = goal("t1");
    replacement.goal_id = "generation-3".into();
    insert_goal(replacement.clone()).await.unwrap().unwrap();
    assert_eq!(environment.stored_wrapup("t1"), None);
    assert!(!pending_budget_wrapup("t1", &replacement.goal_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn deferred_forks_preserve_pending_or_delivered_snapshot_independently() {
    let environment = EnvGuard::new("budget-wrapup-fork");
    let mut source = goal("source");
    source.status = ThreadGoalStatus::BudgetLimited;
    source.tokens_used = 1_000;
    insert_goal(source.clone()).await.unwrap().unwrap();
    copy_goal_deferred("source", "pending-fork")
        .await
        .unwrap()
        .unwrap();
    assert!(pending_budget_wrapup("pending-fork", &source.goal_id)
        .await
        .unwrap());
    assert!(
        get_record("pending-fork")
            .await
            .unwrap()
            .continuation_deferred
    );
    assert!(acknowledge_budget_wrapup("source", &source.goal_id)
        .await
        .unwrap());
    assert!(
        pending_budget_wrapup("pending-fork", &source.goal_id)
            .await
            .unwrap(),
        "source delivery does not consume a copied pending snapshot"
    );
    copy_goal_deferred("source", "delivered-fork")
        .await
        .unwrap()
        .unwrap();
    assert!(!pending_budget_wrapup("delivered-fork", &source.goal_id)
        .await
        .unwrap());
    assert!(
        environment
            .stored_wrapup("delivered-fork")
            .unwrap()
            .delivered
    );
    clear_goal("pending-fork").await.unwrap();
    assert_eq!(environment.stored_wrapup("pending-fork"), None);
    assert!(!pending_budget_wrapup("source", &source.goal_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn deferred_fork_snapshots_preserve_every_goal_field_for_all_six_statuses() {
    let environment = EnvGuard::new("fork-all-statuses");
    for (index, status) in [
        ThreadGoalStatus::Active,
        ThreadGoalStatus::Paused,
        ThreadGoalStatus::Blocked,
        ThreadGoalStatus::UsageLimited,
        ThreadGoalStatus::BudgetLimited,
        ThreadGoalStatus::Complete,
    ]
    .into_iter()
    .enumerate()
    {
        let label = status.wire_name();
        let source = format!("source-{label}");
        let mut original = goal(&source);
        original.goal_id = RuntimeString::from_utf16(vec![0x67, 0xd800, index as u16]);
        original.objective = RuntimeString::from_utf16(vec![
            0x41,
            0xdfff,
            0x0a,
            0xd83d,
            0xde00,
            0x42 + index as u16,
        ]);
        original.status = status;
        original.token_budget = (status != ThreadGoalStatus::UsageLimited).then_some(1_000);
        original.tokens_used = if status == ThreadGoalStatus::BudgetLimited {
            1_077
        } else {
            31 + index as i64
        };
        original.time_used_seconds = 789 + index as i64;
        original.created_at_ms = 10_000 + index as i64;
        original.updated_at_ms = 20_000 + index as i64;
        let resources = owned_resources(&source, &format!("preparation-{label}"));
        apply_goal_control(
            &source,
            GoalControlMutation::Set(request_create(original.clone())),
            None,
            None,
            Some(Some(resources.clone())),
        )
        .await
        .unwrap();
        if index % 2 == 0 {
            set_continuation_deferral(&source, true).await.unwrap();
        }
        let source_before = get_record(&source).await.unwrap();
        assert_eq!(source_before.goal, Some(original.clone()), "{label}");
        let snapshot = capture_goal_fork_snapshot(&source).await.unwrap();
        assert_eq!(snapshot.record, source_before, "{label}");
        let captured_target = format!("captured-{label}");
        let captured_goal = copy_goal_snapshot_deferred(&captured_target, &snapshot)
            .await
            .unwrap()
            .unwrap();
        let direct_target = format!("direct-{label}");
        let direct_goal = copy_goal_deferred(&source, &direct_target)
            .await
            .unwrap()
            .unwrap();
        for (target, copied) in [
            (captured_target, captured_goal),
            (direct_target, direct_goal),
        ] {
            let mut expected = original.clone();
            expected.thread_id = target.as_str().into();
            assert_eq!(copied, expected, "{label}: {target}");
            assert_eq!(
                get_record(&target).await.unwrap(),
                GoalRecord {
                    goal: Some(expected),
                    continuation_deferred: true,
                    intent_revision: 1,
                    revision: 1,
                },
                "{label}: {target}"
            );
            assert_eq!(
                get_goal_owned_resources(&target, &original.goal_id)
                    .await
                    .unwrap(),
                Some(resources.clone()),
                "{label}: {target}"
            );
            assert_eq!(environment.stored_run(&target), None, "{label}: {target}");
            assert_eq!(
                pending_budget_wrapup(&target, &original.goal_id)
                    .await
                    .unwrap(),
                status == ThreadGoalStatus::BudgetLimited,
                "{label}: {target}"
            );
        }
        assert_eq!(get_record(&source).await.unwrap(), source_before, "{label}");
    }
}

#[tokio::test]
async fn delayed_fork_uses_captured_goal_and_delivery_after_source_replacement() {
    let environment = EnvGuard::new("captured-fork-replacement");
    let mut original = goal("source");
    original.status = ThreadGoalStatus::BudgetLimited;
    original.tokens_used = 1_100;
    original.time_used_seconds = 77;
    insert_goal(original.clone()).await.unwrap().unwrap();
    assert!(acknowledge_budget_wrapup("source", &original.goal_id)
        .await
        .unwrap());
    let captured = capture_goal_fork_snapshot("source").await.unwrap();
    assert_eq!(captured.record.goal, Some(original.clone()));
    clear_goal("source").await.unwrap();
    let mut replacement = goal("source");
    replacement.goal_id = "generation-2".into();
    replacement.objective = "Replacement after capture".into();
    insert_goal(replacement).await.unwrap().unwrap();
    let source_before_copy = get_record("source").await.unwrap();
    let copied = copy_goal_snapshot_deferred("target", &captured)
        .await
        .unwrap()
        .unwrap();
    let mut expected = original;
    expected.thread_id = "target".into();
    assert_eq!(copied, expected);
    assert!(get_record("target").await.unwrap().continuation_deferred);
    assert!(!pending_budget_wrapup("target", &copied.goal_id)
        .await
        .unwrap());
    assert!(environment.stored_wrapup("target").unwrap().delivered);
    assert_eq!(get_record("source").await.unwrap(), source_before_copy);
    assert_eq!(
        copy_goal_snapshot_deferred("target", &captured)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
}

#[tokio::test]
async fn delayed_fork_preserves_pending_snapshot_after_source_delivery_and_edit() {
    let _environment = EnvGuard::new("captured-fork-pending");
    let mut original = goal("source");
    original.status = ThreadGoalStatus::BudgetLimited;
    original.tokens_used = 1_000;
    insert_goal(original.clone()).await.unwrap().unwrap();
    let captured = capture_goal_fork_snapshot("source").await.unwrap();
    assert!(acknowledge_budget_wrapup("source", &original.goal_id)
        .await
        .unwrap());
    update_goal(
        "source",
        GoalUpdate {
            objective: Some("Source edited after capture".into()),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap();
    let copied = copy_goal_snapshot_deferred("target", &captured)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(copied.objective, original.objective);
    assert!(pending_budget_wrapup("target", &original.goal_id)
        .await
        .unwrap());
    assert!(!pending_budget_wrapup("source", &original.goal_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn captured_fork_keeps_goal_state_and_does_not_inherit_source_run_owner() {
    let environment = EnvGuard::new("captured-fork-run");
    let original = goal("source");
    insert_goal(original.clone()).await.unwrap().unwrap();
    assert!(begin_goal_run("source", &original.goal_id, "source-owner")
        .await
        .unwrap());
    let captured = capture_goal_fork_snapshot("source").await.unwrap();
    update_goal(
        "source",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Paused),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap()
    .unwrap();
    let copied = copy_goal_snapshot_deferred("target", &captured)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(copied.status, ThreadGoalStatus::Active);
    assert_eq!(environment.stored_run("target"), None);
    assert!(get_record("target").await.unwrap().continuation_deferred);
    assert_eq!(
        environment.stored_run("source").unwrap().owner_id,
        "source-owner"
    );
}

#[tokio::test]
async fn wrapup_creation_failure_rolls_back_usage_status_run_and_revision() {
    let environment = EnvGuard::new("budget-wrapup-rollback");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_budget_wrapup BEFORE INSERT ON goal_budget_wrapups BEGIN SELECT RAISE(ABORT, 'forced wrap-up write failure'); END;").unwrap();
    assert_eq!(
        account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 2, None, 40)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    connection
        .execute_batch("DROP TRIGGER reject_budget_wrapup;")
        .unwrap();
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 2, None, 40)
        .await
        .unwrap();
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
}

#[tokio::test]
async fn failed_wrapup_acknowledgment_keeps_delivery_pending() {
    let environment = EnvGuard::new("budget-wrapup-ack-rollback");
    let mut initial = goal("t1");
    initial.status = ThreadGoalStatus::BudgetLimited;
    initial.tokens_used = 1_000;
    insert_goal(initial.clone()).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_budget_wrapup_ack BEFORE UPDATE OF delivered ON goal_budget_wrapups WHEN NEW.delivered = 1 BEGIN SELECT RAISE(ABORT, 'forced acknowledgment failure'); END;").unwrap();
    assert_eq!(
        acknowledge_budget_wrapup("t1", &initial.goal_id)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert_eq!(get_record("t1").await.unwrap(), before);
    connection
        .execute_batch("DROP TRIGGER reject_budget_wrapup_ack;")
        .unwrap();
    assert!(acknowledge_budget_wrapup("t1", &initial.goal_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn v4_budget_limited_goals_gain_pending_wrapups_without_losing_other_state() {
    let environment = EnvGuard::new("budget-wrapup-schema-upgrade");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    assert!(begin_goal_run("t1", &initial.goal_id, "owner-1")
        .await
        .unwrap());
    account_usage("t1", GoalAccountingMode::ActiveOnly, 1_000, 2, None, 40)
        .await
        .unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute_batch("DROP TABLE goal_owned_resources; DROP TABLE goal_budget_wrapups; PRAGMA user_version = 4;")
        .unwrap();
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(environment.stored_run("t1").unwrap().owner_id, "owner-1");
    assert!(get_record("t1").await.unwrap().continuation_deferred);
}

#[tokio::test]
async fn imported_limited_goal_or_missing_delivery_record_gets_durable_pending_state() {
    let environment = EnvGuard::new("budget-wrapup-legacy");
    let mut original = goal("t1");
    original.status = ThreadGoalStatus::BudgetLimited;
    original.tokens_used = 1_100;
    let bytes = legacy_bytes(&original, true);
    environment.write_legacy("t1", &bytes);
    assert!(pending_budget_wrapup("t1", &original.goal_id)
        .await
        .unwrap());
    assert_eq!(get_goal("t1").await.unwrap(), Some(original.clone()));
    assert_eq!(std::fs::read(environment.legacy_path("t1")).unwrap(), bytes);
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute("DELETE FROM goal_budget_wrapups WHERE thread_id = 't1'", [])
        .unwrap();
    let before = get_record("t1").await.unwrap();
    assert!(pending_budget_wrapup("t1", &original.goal_id)
        .await
        .unwrap());
    let seeded = get_record("t1").await.unwrap();
    assert_eq!(seeded.goal, before.goal);
    assert_eq!(seeded.revision, before.revision + 1);
    assert!(pending_budget_wrapup("t1", &original.goal_id)
        .await
        .unwrap());
    assert_eq!(get_record("t1").await.unwrap(), seeded);
}

#[tokio::test]
async fn budget_flush_survives_process_exit_and_acknowledges_once_across_processes() {
    let environment = EnvGuard::new("budget-wrapup-processes");
    let initial = goal("t1");
    insert_goal(initial.clone()).await.unwrap().unwrap();
    assert!(environment
        .child("budget-threshold-exit", 0)
        .wait()
        .unwrap()
        .success());
    assert_eq!(
        get_goal("t1").await.unwrap().unwrap().status,
        ThreadGoalStatus::BudgetLimited
    );
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    assert!(environment
        .child("inspect-budget-wrapup", 0)
        .wait()
        .unwrap()
        .success());
    assert!(pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("ack-budget-wrapup", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    assert!(environment
        .home
        .join("budget-wrapup-delivery-owner")
        .exists());
    assert!(!pending_budget_wrapup("t1", &initial.goal_id).await.unwrap());
}

#[tokio::test]
async fn request_receipt_replays_original_snapshot_after_progress_and_clear() {
    let _environment = EnvGuard::new("request-replay");
    let original = goal("t1");
    let first = apply_goal_request_once(
        "t1",
        "request-1",
        "semantic-1",
        request_create(original.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        first,
        GoalRequestOutcome {
            goal: original.clone(),
            revision: 1,
            replayed: false,
            fingerprint: "semantic-1".into()
        }
    );
    account_usage("t1", GoalAccountingMode::ActiveOnly, 17, 2, None, 50)
        .await
        .unwrap();
    update_goal(
        "t1",
        GoalUpdate {
            objective: Some("Edited after submission".into()),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap();
    clear_goal("t1").await.unwrap();
    let cleared = get_record("t1").await.unwrap();
    let mut generated_retry = original;
    generated_retry.goal_id = "fresh-retry-generation".into();
    generated_retry.created_at_ms = 99;
    generated_retry.updated_at_ms = 99;
    let replay = apply_goal_request_once(
        "t1",
        "request-1",
        "semantic-1",
        request_create(generated_retry),
    )
    .await
    .unwrap();
    assert_eq!(
        replay,
        GoalRequestOutcome {
            replayed: true,
            ..first
        }
    );
    assert_eq!(
        get_goal_request_receipt("t1", "request-1").await.unwrap(),
        Some(replay)
    );
    assert_eq!(
        get_record("t1").await.unwrap(),
        cleared,
        "replay cannot resurrect a cleared goal"
    );
}

#[tokio::test]
async fn request_update_shares_budget_precedence_and_rejects_changed_semantics() {
    let _environment = EnvGuard::new("request-update");
    let mut initial = goal("t1");
    initial.tokens_used = 900;
    insert_goal(initial).await.unwrap().unwrap();
    let update = GoalUpdate {
        status: Some(ThreadGoalStatus::Paused),
        token_budget: Some(Some(500)),
        expected_goal_id: Some("generation-1".into()),
        ..Default::default()
    };
    let result = apply_goal_request_once(
        "t1",
        "request-1",
        "semantic-1",
        GoalRequestMutation::Update {
            update: update.clone(),
            now_ms: 50,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        (result.goal.status, result.goal.tokens_used, result.revision),
        (ThreadGoalStatus::BudgetLimited, 900, 2)
    );
    let before = get_record("t1").await.unwrap();
    let replay = apply_goal_request_once(
        "t1",
        "request-1",
        "semantic-1",
        GoalRequestMutation::Update {
            update: update.clone(),
            now_ms: 99,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        replay,
        GoalRequestOutcome {
            replayed: true,
            ..result
        }
    );
    assert_eq!(
        apply_goal_request_once(
            "t1",
            "request-1",
            "semantic-changed",
            GoalRequestMutation::Update {
                update,
                now_ms: 100
            }
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn refused_request_mutations_leave_no_receipt() {
    let _environment = EnvGuard::new("request-rejections");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    assert_eq!(
        apply_goal_request_once("t1", "request-create", "fp", request_create(goal("t1")))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(get_goal_request_receipt("t1", "request-create")
        .await
        .unwrap()
        .is_none());
    let stale = GoalRequestMutation::Update {
        update: GoalUpdate {
            expected_goal_id: Some("stale".into()),
            ..Default::default()
        },
        now_ms: 60,
    };
    assert_eq!(
        apply_goal_request_once("t1", "request-update", "fp", stale)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(get_goal_request_receipt("t1", "request-update")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        apply_goal_request_once(
            "missing",
            "request-missing",
            "fp",
            GoalRequestMutation::Update {
                update: GoalUpdate::default(),
                now_ms: 40
            }
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::NotFound
    );
    assert!(get_goal_request_receipt("missing", "request-missing")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        apply_goal_request_once(
            "other-thread",
            "request-owner",
            "fp",
            request_create(goal("t1"))
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn receipt_failure_rolls_back_replacement_revision_and_generation_history() {
    let environment = EnvGuard::new("receipt-rollback");
    let mut original = goal("t1");
    original.status = ThreadGoalStatus::Complete;
    insert_goal(original).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_receipt BEFORE INSERT ON goal_request_receipts BEGIN SELECT RAISE(ABORT, 'forced receipt failure'); END;").unwrap();
    let mut replacement = goal("t1");
    replacement.goal_id = "generation-2".into();
    let mutation = GoalRequestMutation::Create {
        goal: replacement,
        expected_goal_id: Some("generation-1".into()),
        expected_absent: false,
    };
    assert_eq!(
        apply_goal_request_once("t1", "request-1", "fp", mutation.clone())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(get_goal_request_receipt("t1", "request-1")
        .await
        .unwrap()
        .is_none());
    let generations: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM goal_generations WHERE thread_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(generations, 1);
    connection
        .execute_batch("DROP TRIGGER reject_receipt;")
        .unwrap();
    let retried = apply_goal_request_once("t1", "request-1", "fp", mutation)
        .await
        .unwrap();
    assert_eq!(retried.revision, before.revision + 1);
    assert!(!retried.replayed);
}

#[tokio::test]
async fn concurrent_same_request_creates_return_one_original_result() {
    let _environment = EnvGuard::new("receipt-race");
    let mut first = goal("t1");
    first.status = ThreadGoalStatus::Complete;
    let mut retry = first.clone();
    retry.goal_id = "fresh-retry-generation".into();
    let (first, retry) = tokio::join!(
        apply_goal_request_once(
            "t1",
            "same-request",
            "same-semantics",
            request_create(first)
        ),
        apply_goal_request_once(
            "t1",
            "same-request",
            "same-semantics",
            request_create(retry)
        ),
    );
    let first = first.unwrap();
    let retry = retry.unwrap();
    assert_ne!(
        first.replayed, retry.replayed,
        "exactly one mutation is admitted"
    );
    assert_eq!(first.goal, retry.goal);
    assert_eq!((first.revision, retry.revision), (1, 1));
    assert_eq!(
        get_record("t1").await.unwrap(),
        GoalRecord {
            goal: Some(first.goal),
            continuation_deferred: false,
            intent_revision: 1,
            revision: 1
        }
    );
}

#[tokio::test]
async fn request_snapshot_preserves_exact_strings_and_full_i64_accounting() {
    let _environment = EnvGuard::new("receipt-exact");
    let mut original = goal("t1");
    original.goal_id = RuntimeString::from_utf16(vec![0x67, 0xd800]);
    original.objective = RuntimeString::from_utf16(vec![0x41, 0xdfff]);
    original.status = ThreadGoalStatus::Complete;
    original.token_budget = Some(i64::MAX);
    original.tokens_used = i64::MAX - 1;
    original.time_used_seconds = i64::MAX - 2;
    original.created_at_ms = i64::MAX - 3;
    original.updated_at_ms = i64::MAX - 4;
    let applied =
        apply_goal_request_once("t1", "request-1", "fp", request_create(original.clone()))
            .await
            .unwrap();
    let restored = get_goal_request_receipt("t1", "request-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.goal, original);
    assert_eq!(
        restored,
        GoalRequestOutcome {
            replayed: true,
            ..applied
        }
    );
}

#[tokio::test]
async fn corrupt_receipt_is_inspectable_and_never_reapplies_a_mutation() {
    let environment = EnvGuard::new("receipt-corruption");
    apply_goal_request_once("t1", "request-1", "fp", request_create(goal("t1")))
        .await
        .unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute(
            "UPDATE goal_request_receipts SET goal_snapshot = ?1",
            [b"corrupt snapshot".as_slice()],
        )
        .unwrap();
    assert_eq!(
        get_goal_request_receipt("t1", "request-1")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        apply_goal_request_once("t1", "request-1", "fp", request_update())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    let snapshot: Vec<u8> = connection
        .query_row(
            "SELECT goal_snapshot FROM goal_request_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(snapshot, b"corrupt snapshot");
}

#[tokio::test]
async fn missing_current_schema_tables_are_corruption_instead_of_empty_storage() {
    for table in [
        "goals",
        "goal_request_receipts",
        "goal_runs",
        "goal_budget_wrapups",
        "goal_owned_resources",
        "goal_intents",
        "goal_thread_eligibility",
    ] {
        let environment = EnvGuard::new("missing-schema-table");
        apply_goal_request_once("t1", "request-1", "fp", request_create(goal("t1")))
            .await
            .unwrap();
        let connection = open_database(&environment.home).unwrap();
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        connection
            .execute_batch(&format!("DROP TABLE {table}"))
            .unwrap();
        assert_eq!(
            get_goal("t1").await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            apply_goal_request_once("t1", "request-1", "fp", request_update())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !exists,
            "corrupt schema is preserved for recovery instead of silently recreated"
        );
    }
}

#[tokio::test]
async fn schema_upgrade_adds_receipts_without_rewriting_existing_goal() {
    let environment = EnvGuard::new("receipt-schema-upgrade");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .execute_batch(
            "DROP TABLE goal_owned_resources; DROP TABLE goal_budget_wrapups; DROP TABLE goal_runs; DROP TABLE goal_request_receipts; PRAGMA user_version = 1;",
        )
        .unwrap();
    assert!(get_goal_request_receipt("t1", "new-request")
        .await
        .unwrap()
        .is_none());
    assert_eq!(get_record("t1").await.unwrap(), before);
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}

#[tokio::test]
async fn duplicate_request_across_processes_has_one_mutation_owner() {
    let environment = EnvGuard::new("receipt-process-race");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("request-update", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    assert!(environment.home.join("request-mutation-owner").exists());
    let stored = get_record("t1").await.unwrap();
    assert_eq!(stored.revision, 2);
    assert_eq!(
        stored.goal.unwrap().objective,
        RuntimeString::from("Submitted once")
    );
    let receipt = get_goal_request_receipt("t1", "process-request")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((receipt.revision, receipt.replayed), (2, true));
}

#[tokio::test]
async fn process_exit_before_receipt_commit_rolls_back_both_halves() {
    let environment = EnvGuard::new("receipt-process-rollback");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    assert!(environment
        .child("exit-during-request", 0)
        .wait()
        .unwrap()
        .success());
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(get_goal_request_receipt("t1", "process-request")
        .await
        .unwrap()
        .is_none());
    let applied = apply_goal_request_once("t1", "process-request", "process-fp", request_update())
        .await
        .unwrap();
    assert_eq!((applied.revision, applied.replayed), (2, false));
}

#[tokio::test]
async fn lost_response_after_commit_replays_without_another_mutation() {
    let environment = EnvGuard::new("receipt-lost-response");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    assert!(environment
        .child("request-commit-exit", 0)
        .wait()
        .unwrap()
        .success());
    let before = get_record("t1").await.unwrap();
    assert_eq!(before.revision, 2);
    let replay = apply_goal_request_once("t1", "process-request", "process-fp", request_update())
        .await
        .unwrap();
    assert_eq!((replay.revision, replay.replayed), (2, true));
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn retired_generation_cannot_be_reused_after_clear() {
    let _environment = EnvGuard::new("generation-history");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    clear_goal("t1").await.unwrap();
    let cleared = get_record("t1").await.unwrap();
    assert_eq!(
        insert_goal(goal("t1")).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(get_record("t1").await.unwrap(), cleared);
    let mut next = goal("t1");
    next.goal_id = "generation-2".into();
    insert_goal(next.clone()).await.unwrap().unwrap();
    assert_eq!(
        account_usage(
            "t1",
            GoalAccountingMode::ActiveOnly,
            10,
            1,
            Some("generation-1".into()),
            80
        )
        .await
        .unwrap(),
        GoalAccountingOutcome::Unchanged(Some(next))
    );
}

#[tokio::test]
async fn accounting_and_budget_precedence_commit_together() {
    let _environment = EnvGuard::new("budget");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let outcome = account_usage("t1", GoalAccountingMode::ActiveOnly, 1_100, 6, None, 40)
        .await
        .unwrap();
    let charged = outcome.goal().unwrap();
    assert_eq!(
        (
            charged.tokens_used,
            charged.time_used_seconds,
            charged.status
        ),
        (1_100, 6, ThreadGoalStatus::BudgetLimited)
    );
    assert_eq!(charged.remaining_tokens(), Some(0));
    let paused = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Paused),
            ..Default::default()
        },
        50,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(paused.status, ThreadGoalStatus::BudgetLimited);
    let complete = update_goal(
        "t1",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Complete),
            ..Default::default()
        },
        60,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(complete.status, ThreadGoalStatus::Complete);
    account_usage("t1", GoalAccountingMode::ActiveOrComplete, 12, 1, None, 70)
        .await
        .unwrap();
    let stored = get_goal("t1").await.unwrap().unwrap();
    assert_eq!(
        (stored.status, stored.tokens_used),
        (ThreadGoalStatus::Complete, 1_112)
    );
}

#[tokio::test]
async fn budget_limit_survives_blocked_updates_but_preserves_distinct_outcomes() {
    let _environment = EnvGuard::new("blocked-budget-precedence");
    let mut exhausted = goal("t1");
    exhausted.status = ThreadGoalStatus::BudgetLimited;
    exhausted.tokens_used = 1_100;
    insert_goal(exhausted).await.unwrap().unwrap();
    let blocked = GoalUpdate {
        status: Some(ThreadGoalStatus::Blocked),
        expected_goal_id: Some("generation-1".into()),
        ..Default::default()
    };
    let direct = update_goal("t1", blocked.clone(), 50)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(direct.status, ThreadGoalStatus::BudgetLimited);
    let submitted = apply_goal_request_once(
        "t1",
        "blocked-request",
        "blocked-fp",
        GoalRequestMutation::Update {
            update: blocked,
            now_ms: 60,
        },
    )
    .await
    .unwrap();
    assert_eq!(submitted.goal.status, ThreadGoalStatus::BudgetLimited);
    for status in [ThreadGoalStatus::UsageLimited, ThreadGoalStatus::Complete] {
        let outcome = update_goal(
            "t1",
            GoalUpdate {
                status: Some(status),
                ..Default::default()
            },
            70,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(outcome.status, status);
    }
    let mut stopped = goal("t2");
    stopped.status = ThreadGoalStatus::Blocked;
    stopped.tokens_used = 900;
    insert_goal(stopped).await.unwrap().unwrap();
    let lower_budget = update_goal(
        "t2",
        GoalUpdate {
            token_budget: Some(Some(500)),
            ..Default::default()
        },
        80,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(
        lower_budget.status,
        ThreadGoalStatus::Blocked,
        "lowering an already blocked goal budget does not invent a prior budget stop"
    );
}

#[tokio::test]
async fn objective_limit_counts_code_points_for_insert_update_and_legacy_migration() {
    let environment = EnvGuard::new("objective-code-points");
    let mut maximum = goal("t1");
    maximum.objective = "😀".repeat(GOAL_OBJECTIVE_MAX_LENGTH).into();
    assert_eq!(maximum.objective.len_utf16(), GOAL_OBJECTIVE_MAX_LENGTH * 2);
    insert_goal(maximum.clone()).await.unwrap().unwrap();
    assert_eq!(get_goal("t1").await.unwrap(), Some(maximum.clone()));
    let before = get_record("t1").await.unwrap();
    assert_eq!(
        update_goal(
            "t1",
            GoalUpdate {
                objective: Some("😀".repeat(GOAL_OBJECTIVE_MAX_LENGTH + 1).into()),
                ..Default::default()
            },
            60
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
    maximum.thread_id = "legacy".into();
    let original = legacy_bytes(&maximum, true);
    environment.write_legacy("legacy", &original);
    assert_eq!(get_goal("legacy").await.unwrap(), Some(maximum));
    assert_eq!(
        std::fs::read(environment.legacy_path("legacy")).unwrap(),
        original
    );
    let mut oversized = goal("oversized");
    oversized.objective = "😀".repeat(GOAL_OBJECTIVE_MAX_LENGTH + 1).into();
    assert_eq!(
        insert_goal(oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn objective_limit_schema_upgrade_preserves_deferrals_history_and_receipts() {
    let environment = EnvGuard::new("objective-schema-upgrade");
    let original = apply_goal_request_once("t1", "request-1", "fp", request_create(goal("t1")))
        .await
        .unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let mut connection = open_database(&environment.home).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let old_schema = GOAL_TABLE_SCHEMA
        .replace(
            "CREATE TABLE IF NOT EXISTS goals (",
            "CREATE TABLE legacy_goals (",
        )
        .replace("16000", "8000");
    transaction.execute_batch(&old_schema).unwrap();
    transaction.execute_batch("INSERT INTO legacy_goals SELECT * FROM goals; DROP TABLE goal_owned_resources; DROP TABLE goal_budget_wrapups; DROP TABLE goal_runs; DROP TABLE goals; ALTER TABLE legacy_goals RENAME TO goals; PRAGMA user_version = 2;").unwrap();
    transaction.commit().unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert_eq!(
        get_goal_request_receipt("t1", "request-1").await.unwrap(),
        Some(GoalRequestOutcome {
            replayed: true,
            ..original
        })
    );
    let maximum: RuntimeString = "😀".repeat(GOAL_OBJECTIVE_MAX_LENGTH).into();
    let updated = update_goal(
        "t1",
        GoalUpdate {
            objective: Some(maximum.clone()),
            ..Default::default()
        },
        80,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(updated.objective, maximum);
    assert!(get_record("t1").await.unwrap().continuation_deferred);
    let generations: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM goal_generations WHERE thread_id = 't1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(generations, 1);
}

#[tokio::test]
async fn bounded_queue_preserves_all_concurrent_charges() {
    let _environment = EnvGuard::new("queue");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let results = futures::future::join_all((0..192).map(|_| async {
        account_usage(
            "t1",
            GoalAccountingMode::ActiveOnly,
            2,
            1,
            Some("generation-1".into()),
            40,
        )
        .await
    }))
    .await;
    assert!(results
        .into_iter()
        .all(|result| matches!(result.unwrap(), GoalAccountingOutcome::Updated(_))));
    let snapshot = get_record("t1").await.unwrap();
    let stored = snapshot.goal.unwrap();
    assert_eq!(
        (
            stored.tokens_used,
            stored.time_used_seconds,
            snapshot.revision
        ),
        (384, 192, 193)
    );
}

#[tokio::test]
async fn concurrent_processes_share_transactional_accounting() {
    let environment = EnvGuard::new("processes");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let children: Vec<_> = (0..4)
        .map(|index| environment.child("charge", index))
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    let snapshot = get_record("t1").await.unwrap();
    let stored = snapshot.goal.unwrap();
    assert_eq!(
        (
            stored.tokens_used,
            stored.time_used_seconds,
            snapshot.revision
        ),
        (100, 100, 101)
    );
}

#[tokio::test]
async fn process_exit_rolls_back_uncommitted_progress() {
    let environment = EnvGuard::new("crash");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    assert!(environment
        .child("exit-without-commit", 0)
        .wait()
        .unwrap()
        .success());
    assert_eq!(get_record("t1").await.unwrap(), before);
    set_continuation_deferral_if("t1", false, Some("generation-1".into()))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !get_record("t1").await.unwrap().continuation_deferred,
        "explicit recovery releases the retained marker"
    );
}

#[tokio::test]
async fn process_exit_during_migration_preserves_original_and_retries_import() {
    let environment = EnvGuard::new("migration-crash");
    let original = legacy_bytes(&goal("t1"), true);
    environment.write_legacy("t1", &original);
    assert!(environment
        .child("exit-during-migration", 0)
        .wait()
        .unwrap()
        .success());
    let connection = open_database(&environment.home).unwrap();
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM goal_threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "uncommitted import and confirmation marker both roll back"
    );
    assert_eq!(
        std::fs::read(environment.legacy_path("t1")).unwrap(),
        original
    );
    assert_eq!(
        get_record("t1").await.unwrap(),
        GoalRecord {
            goal: Some(goal("t1")),
            continuation_deferred: true,
            intent_revision: 1,
            revision: 1
        }
    );
}

#[test]
fn storage_process_worker() {
    let Ok(mode) = std::env::var("KLAUS_GOAL_STORAGE_CHILD") else {
        return;
    };
    if mode == "exit-without-commit" {
        let mut connection = open_database(Path::new(&config_home())).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                "UPDATE goals SET tokens_used = 999 WHERE thread_id = 't1'",
                [],
            )
            .unwrap();
        transaction
            .execute("DELETE FROM goal_continuation_deferrals", [])
            .unwrap();
        std::process::exit(0);
    }
    if mode == "exit-during-migration" {
        let root = PathBuf::from(config_home());
        let mut connection = open_database(&root).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        migrate_legacy(&transaction, &root, "t1").unwrap();
        std::process::exit(0);
    }
    if mode == "exit-during-request" {
        let root = PathBuf::from(config_home());
        let mut connection = open_database(&root).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let before = read_record(&transaction, "t1").unwrap();
        let GoalRequestMutation::Update { update, now_ms } = request_update() else {
            unreachable!()
        };
        let (mut after, applied) = update_record(before.clone(), update, now_ms, true).unwrap();
        let goal = applied.unwrap().unwrap();
        persist_changed_record(&transaction, "t1", &before, &mut after).unwrap();
        transaction.execute(
            "INSERT INTO goal_request_receipts (thread_id, request_id, fingerprint, revision, goal_snapshot) VALUES ('t1', 'process-request', 'process-fp', ?1, ?2)",
            params![after.revision, encode_request_snapshot(&goal)],
        ).unwrap();
        std::process::exit(0);
    }
    if mode == "intent-status" {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let update = GoalUpdate {
                    status: Some(ThreadGoalStatus::Active),
                    expected_goal_id: Some("generation-1".into()),
                    expected_intent_revision: Some(1),
                    ..Default::default()
                };
                match update_goal("t1", update, 20).await.unwrap() {
                    Ok(Some(_)) => {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(PathBuf::from(config_home()).join("intent-status-owner"))
                            .unwrap();
                    }
                    Err(_) => {}
                    Ok(None) => panic!("the shared goal must remain present"),
                }
            });
        return;
    }
    if mode == "revision-edit" {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let update = GoalUpdate {
                    objective: Some(format!("Process editor {}", std::process::id()).into()),
                    expected_goal_id: Some("generation-1".into()),
                    expected_revision: Some(1),
                    ..Default::default()
                };
                match update_goal("t1", update, 40).await.unwrap() {
                    Ok(Some(_)) => {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(PathBuf::from(config_home()).join("revision-edit-owner"))
                            .unwrap();
                    }
                    Err(_) => {}
                    Ok(None) => panic!("the shared goal must remain present"),
                }
            });
        return;
    }
    if mode == "budget-threshold-exit"
        || mode == "inspect-budget-wrapup"
        || mode == "ack-budget-wrapup"
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                match mode.as_str() {
                    "budget-threshold-exit" => {
                        let outcome = account_usage(
                            "t1",
                            GoalAccountingMode::ActiveOnly,
                            1_000,
                            2,
                            Some("generation-1".into()),
                            40,
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            outcome.goal().unwrap().status,
                            ThreadGoalStatus::BudgetLimited
                        );
                    }
                    "inspect-budget-wrapup" => {
                        assert!(pending_budget_wrapup("t1", &"generation-1".into())
                            .await
                            .unwrap())
                    }
                    "ack-budget-wrapup" => {
                        if acknowledge_budget_wrapup("t1", &"generation-1".into())
                            .await
                            .unwrap()
                        {
                            std::fs::OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(
                                    PathBuf::from(config_home())
                                        .join("budget-wrapup-delivery-owner"),
                                )
                                .unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            });
        if mode == "budget-threshold-exit" {
            std::process::exit(0);
        }
        return;
    }
    if mode == "claim-run" {
        let owner = format!("process-owner-{}", std::process::id());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                if begin_goal_run("t1", &"generation-1".into(), &owner)
                    .await
                    .unwrap()
                {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(PathBuf::from(config_home()).join("run-claim-owner"))
                        .unwrap();
                }
            });
        return;
    }
    if mode == "request-update" || mode == "request-commit-exit" {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let receipt = apply_goal_request_once(
                    "t1",
                    "process-request",
                    "process-fp",
                    request_update(),
                )
                .await
                .unwrap();
                if !receipt.replayed {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(PathBuf::from(config_home()).join("request-mutation-owner"))
                        .unwrap();
                }
            });
        if mode == "request-commit-exit" {
            std::process::exit(0);
        }
        return;
    }
    assert_eq!(mode, "charge");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for _ in 0..25 {
                assert!(matches!(
                    account_usage(
                        "t1",
                        GoalAccountingMode::ActiveOnly,
                        1,
                        1,
                        Some("generation-1".into()),
                        40
                    )
                    .await
                    .unwrap(),
                    GoalAccountingOutcome::Updated(_)
                ));
            }
        });
}

#[tokio::test]
async fn failed_insert_rolls_back_revision_goal_and_deferral() {
    let environment = EnvGuard::new("rollback");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    set_continuation_deferral("t1", true).await.unwrap();
    let before = get_record("t1").await.unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_charge BEFORE INSERT ON goals WHEN NEW.tokens_used = 7 BEGIN SELECT RAISE(ABORT, 'forced transactional failure'); END;").unwrap();
    assert_eq!(
        account_usage("t1", GoalAccountingMode::ActiveOnly, 7, 1, None, 80)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(get_record("t1").await.unwrap(), before);
}

#[tokio::test]
async fn transaction_panic_returns_an_error_and_worker_remains_usable() {
    let _environment = EnvGuard::new("worker-panic");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let before = get_record("t1").await.unwrap();
    let result: io::Result<()> =
        with_record("t1", |_| panic!("forced storage transaction panic")).await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(get_record("t1").await.unwrap(), before);
    assert!(matches!(
        account_usage("t1", GoalAccountingMode::ActiveOnly, 1, 1, None, 80)
            .await
            .unwrap(),
        GoalAccountingOutcome::Updated(_)
    ));
}

#[tokio::test]
async fn migration_preserves_exact_snapshot_and_original_then_runs_once() {
    let environment = EnvGuard::new("migration");
    let mut original = goal("t1");
    original.goal_id = RuntimeString::from_utf16(vec![0x67, 0xd800]);
    original.objective = RuntimeString::from_utf16(vec![0x41, 0xdfff, 0x0a, 0x42]);
    original.status = ThreadGoalStatus::UsageLimited;
    original.token_budget = None;
    original.tokens_used = 987;
    original.time_used_seconds = 432;
    original.created_at_ms = 123;
    original.updated_at_ms = 789;
    let bytes = legacy_bytes(&original, true);
    environment.write_legacy("t1", &bytes);
    let migrated = get_record("t1").await.unwrap();
    assert_eq!(
        migrated,
        GoalRecord {
            goal: Some(original.clone()),
            continuation_deferred: true,
            intent_revision: 1,
            revision: 1
        }
    );
    assert_eq!(std::fs::read(environment.legacy_path("t1")).unwrap(), bytes);
    environment.write_legacy("t1", b"{broken after confirmed import");
    assert_eq!(
        get_record("t1").await.unwrap(),
        migrated,
        "confirmed import no longer rereads JSON"
    );
    clear_goal("t1").await.unwrap();
    assert!(
        get_goal("t1").await.unwrap().is_none(),
        "retained JSON cannot resurrect a clear"
    );
}

#[tokio::test]
async fn corrupt_or_invalid_legacy_records_are_preserved_and_never_replaced() {
    let environment = EnvGuard::new("invalid-json");
    for (thread, bytes) in [
        ("syntax", b"{not-json".as_slice()),
        ("encoding", &[0xff][..]),
        ("shape", b"[]".as_slice()),
        ("goal-null", b"{\"goal\":null}".as_slice()),
        ("orphan", b"{\"continuationDeferred\":true}".as_slice()),
        ("deferral-type", b"{\"continuationDeferred\":1}".as_slice()),
    ] {
        environment.write_legacy(thread, bytes);
        assert_eq!(
            get_goal(thread).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "{thread}"
        );
        assert_eq!(
            insert_goal(goal(thread)).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "{thread}"
        );
        assert_eq!(
            std::fs::read(environment.legacy_path(thread)).unwrap(),
            bytes
        );
    }
    let mut mismatched = goal("other-thread");
    environment.write_legacy("mismatch", &legacy_bytes(&mismatched, false));
    assert_eq!(
        get_goal("mismatch").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    mismatched.thread_id = "negative".into();
    mismatched.tokens_used = -1;
    environment.write_legacy("negative", &legacy_bytes(&mismatched, false));
    assert_eq!(
        get_goal("negative").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    environment.write_legacy("syntax", &legacy_bytes(&goal("syntax"), false));
    assert_eq!(
        get_goal("syntax").await.unwrap(),
        Some(goal("syntax")),
        "failed import does not set its confirmation marker"
    );
}

#[tokio::test]
async fn migration_io_failure_is_not_missing_storage() {
    let environment = EnvGuard::new("migration-io");
    std::fs::create_dir_all(environment.legacy_path("directory")).unwrap();
    let error = get_goal("directory").await.unwrap_err();
    assert_ne!(error.kind(), io::ErrorKind::NotFound);
    assert!(error.to_string().contains("directory.json"));
    assert_eq!(get_goal("missing").await.unwrap(), None);
}

#[cfg(unix)]
#[tokio::test]
async fn permission_denied_legacy_file_remains_a_recovery_error() {
    use std::os::unix::fs::PermissionsExt;
    let environment = EnvGuard::new("permissions");
    let path = environment.legacy_path("t1");
    let bytes = legacy_bytes(&goal("t1"), false);
    environment.write_legacy("t1", &bytes);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    let actual_read = std::fs::read(&path);
    let result = get_goal("t1").await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        actual_read.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied,
        "fixture requires an unprivileged process"
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert_eq!(get_goal("t1").await.unwrap(), Some(goal("t1")));
}

#[tokio::test]
async fn corrupt_database_and_unsupported_schema_are_explicit_errors() {
    let environment = EnvGuard::new("corrupt-database");
    let path = environment.home.join("goals.sqlite3");
    let corrupt = b"inspectable invalid database bytes";
    std::fs::write(&path, corrupt).unwrap();
    assert_eq!(
        get_goal("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        insert_goal(goal("t1")).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
    std::fs::remove_file(&path).unwrap();
    let connection = open_database(&environment.home).unwrap();
    connection
        .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
        .unwrap();
    assert_eq!(
        get_goal("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn sqlite_constraints_protect_status_usage_and_deferral_ownership() {
    let environment = EnvGuard::new("schema");
    insert_goal(goal("t1")).await.unwrap().unwrap();
    let connection = open_database(&environment.home).unwrap();
    assert_eq!(
        connection
            .pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
            .unwrap(),
        5_000
    );
    for statement in [
        "UPDATE goals SET status = 'invalid' WHERE thread_id = 't1'",
        "UPDATE goals SET tokens_used = -1 WHERE thread_id = 't1'",
        "UPDATE goals SET token_budget = 0 WHERE thread_id = 't1'",
        "UPDATE goals SET objective = X'41' WHERE thread_id = 't1'",
        "UPDATE goals SET tokens_used = 1000 WHERE thread_id = 't1'",
        "UPDATE goal_threads SET legacy_migrated = 0 WHERE thread_id = 't1'",
        "INSERT INTO goal_continuation_deferrals (thread_id, goal_id) VALUES ('missing', X'6100')",
        "INSERT INTO goal_continuation_deferrals (thread_id, goal_id) VALUES ('t1', X'6100')",
    ] {
        assert_eq!(
            connection
                .execute(statement, [])
                .unwrap_err()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation),
            "{statement}"
        );
    }
    assert_eq!(get_goal("t1").await.unwrap(), Some(goal("t1")));
    connection
        .pragma_update(None, "ignore_check_constraints", "ON")
        .unwrap();
    connection
        .execute(
            "UPDATE goals SET tokens_used = -1 WHERE thread_id = 't1'",
            [],
        )
        .unwrap();
    assert_eq!(
        get_goal("t1").await.unwrap_err().kind(),
        io::ErrorKind::InvalidData,
        "tampered records are errors instead of missing goals"
    );
}

#[tokio::test]
async fn deferred_copy_preserves_every_goal_field_and_isolates_both_threads() {
    let environment = EnvGuard::new("fork");
    let mut original = goal("source");
    original.status = ThreadGoalStatus::Blocked;
    original.tokens_used = 456;
    original.time_used_seconds = 123;
    environment.write_legacy("source", &legacy_bytes(&original, true));
    let source_before = get_record("source").await.unwrap();
    assert!(get_goal("ordinary-fork").await.unwrap().is_none());
    let copied = copy_goal_deferred("source", "target")
        .await
        .unwrap()
        .unwrap();
    let mut expected = original.clone();
    expected.thread_id = "target".into();
    assert_eq!(copied, expected);
    assert_eq!(
        get_record("target").await.unwrap(),
        GoalRecord {
            goal: Some(expected.clone()),
            continuation_deferred: true,
            intent_revision: 1,
            revision: 1
        }
    );
    assert_eq!(get_record("source").await.unwrap(), source_before);
    assert_eq!(
        copy_goal_deferred("source", "target")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    set_continuation_deferral_if("target", false, Some(expected.goal_id.clone()))
        .await
        .unwrap()
        .unwrap();
    update_goal(
        "target",
        GoalUpdate {
            status: Some(ThreadGoalStatus::Active),
            ..Default::default()
        },
        100,
    )
    .await
    .unwrap()
    .unwrap();
    account_usage(
        "target",
        GoalAccountingMode::ActiveOnly,
        1,
        1,
        Some(expected.goal_id),
        101,
    )
    .await
    .unwrap();
    assert_eq!(get_record("source").await.unwrap(), source_before);
    assert!(!get_record("target").await.unwrap().continuation_deferred);
    clear_goal("target").await.unwrap();
    let cleared_target = get_record("target").await.unwrap();
    assert_eq!(
        copy_goal_deferred("source", "target")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput,
        "a retired target generation cannot be reintroduced by a fork"
    );
    assert_eq!(get_record("target").await.unwrap(), cleared_target);
}

#[tokio::test]
async fn deferred_copy_rolls_back_imports_when_target_is_invalid() {
    let environment = EnvGuard::new("fork-rollback");
    let original = legacy_bytes(&goal("source"), false);
    environment.write_legacy("source", &original);
    environment.write_legacy("target", b"broken");
    assert_eq!(
        copy_goal_deferred("source", "target")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let connection = open_database(&environment.home).unwrap();
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM goal_threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "source import and target import share a transaction"
    );
    assert_eq!(
        std::fs::read(environment.legacy_path("source")).unwrap(),
        original
    );
}

#[tokio::test]
async fn guarded_insert_rechecks_admission_after_sql_lease_and_preserves_native_integers() {
    let environment = EnvGuard::new("guarded-insert");
    let database = environment.home.join("goals.sqlite3");
    let admitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = GoalControlGuard::new({
        let admitted = admitted.clone();
        move |_operation| {
            // A second writer cannot acquire a lease: admission runs after
            // the authoritative transaction has finished its busy wait.
            let probe = Connection::open(&database).unwrap();
            probe.busy_timeout(Duration::ZERO).unwrap();
            let error = probe.execute_batch("BEGIN IMMEDIATE").unwrap_err();
            assert_eq!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseBusy)
            );
            admitted.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "captured request cancelled",
            ))
        }
    });
    let mut native = goal("t1");
    native.created_at_ms = i64::MIN;
    native.updated_at_ms = i64::MAX;
    let error =
        insert_goal_intent_guarded(native.clone(), None, true, Some(0), Some(0), Some(guard))
            .await
            .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(admitted.load(std::sync::atomic::Ordering::SeqCst));
    let connection = Connection::open(environment.home.join("goals.sqlite3")).unwrap();
    for table in ["goal_threads", "goal_intents", "goal_generations"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "rejected admission must not reserve {table}");
    }
    let absent = get_record("t1").await.unwrap();
    assert_eq!(absent.goal, None);
    assert_eq!((absent.revision, absent.intent_revision), (0, 0));
    assert_eq!(
        insert_goal_intent_checked(native.clone(), None, true, Some(0), Some(0))
            .await
            .unwrap()
            .unwrap(),
        native,
        "the native wrapper retains the full signed integer contract"
    );
    assert_eq!(get_goal("t1").await.unwrap(), Some(native));
}
