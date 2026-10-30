use super::*;

const SAFE: i64 = 9_007_199_254_740_991;

fn native_goal() -> ThreadGoal {
    ThreadGoal {
        thread_id: "numeric-goal".into(),
        goal_id: "native-generation".into(),
        objective: "Preserve native numeric state".into(),
        status: ThreadGoalStatus::Complete,
        token_budget: None,
        tokens_used: 250,
        time_used_seconds: 30,
        created_at_ms: 1,
        updated_at_ms: 2,
    }
}

type GoalNumericField = (&'static str, fn(&mut ThreadGoal, i64));

fn fields() -> [GoalNumericField; 5] {
    [
        ("tokensUsed", |goal, value| goal.tokens_used = value),
        ("timeUsedSeconds", |goal, value| {
            goal.time_used_seconds = value
        }),
        ("createdAtMs", |goal, value| goal.created_at_ms = value),
        ("updatedAtMs", |goal, value| goal.updated_at_ms = value),
        ("tokenBudget", |goal, value| goal.token_budget = Some(value)),
    ]
}

#[test]
fn every_model_goal_integer_accepts_both_exact_signed_boundaries() {
    for (_, set) in fields() {
        for value in [-SAFE, SAFE] {
            let mut goal = native_goal();
            set(&mut goal, value);
            assert!(validate_goal_result(&goal).is_ok(), "{goal:?}");
        }
    }
}

#[test]
fn unsafe_native_values_produce_explicit_errors_without_a_rounded_goal_payload() {
    for (field, set) in fields() {
        for value in [SAFE + 1, -SAFE - 1, i64::MAX, i64::MIN] {
            let mut goal = native_goal();
            set(&mut goal, value);
            let error = validate_goal_result(&goal).unwrap_err();
            assert!(error.contains(field), "{error}");
            assert!(error.contains(&value.to_string()), "{error}");
            let result = goal_result(&goal);
            assert_eq!(result.serializable.is_error, Some(true));
            assert_eq!(
                result.serializable.output.get("ok"),
                Some(&RuntimeJson::Bool(false))
            );
            assert!(result.serializable.output.get("goal").is_none());
            assert!(result.serializable.output.get("remainingTokens").is_none());
            assert!(result
                .serializable
                .output
                .get("completionBudgetReport")
                .is_none());
            let message = result
                .serializable
                .output
                .get("message")
                .and_then(RuntimeJson::as_str)
                .unwrap();
            assert!(message.contains(&value.to_string()));
        }
    }
}

#[test]
fn derived_remaining_tokens_must_also_fit_the_exact_integer_range() {
    let mut goal = native_goal();
    goal.token_budget = Some(SAFE);
    goal.tokens_used = -1;
    assert!(validate_goal_result(&goal)
        .unwrap_err()
        .contains("remainingTokens"));
    goal.tokens_used = 0;
    assert!(validate_goal_result(&goal).is_ok());
    let result = goal_result(&goal);
    assert_eq!(result.serializable.is_error, Some(false));
    assert_eq!(
        result
            .serializable
            .output
            .get("remainingTokens")
            .and_then(RuntimeJson::as_number)
            .map(|number| number.as_f64()),
        Some(SAFE as f64)
    );
}

struct ConfigGuard {
    previous: Option<std::ffi::OsString>,
    root: std::path::PathBuf,
    _lock: std::sync::MutexGuard<'static, ()>,
}
impl ConfigGuard {
    fn new() -> Self {
        let lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        let root =
            std::env::temp_dir().join(format!("klaus-goal-tools-numeric-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &root);
        Self {
            previous,
            root,
            _lock: lock,
        }
    }
}
impl Drop for ConfigGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", previous)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn get_goal_refuses_unsafe_model_json_while_native_storage_retains_the_exact_record() {
    let _config = ConfigGuard::new();
    for (index, (field, set)) in fields().into_iter().enumerate() {
        let mut goal = native_goal();
        goal.thread_id = format!("numeric-goal-{index}").into();
        set(&mut goal, i64::MAX);
        crate::goal::store::insert_goal(goal.clone())
            .await
            .unwrap()
            .unwrap();
        let thread = goal.thread_id.try_to_utf8().unwrap();
        assert_eq!(
            crate::goal::store::get_goal(&thread).await.unwrap(),
            Some(goal.clone())
        );
        let runtime = Arc::new(crate::goal::GoalRuntime::new(
            goal.thread_id.clone(),
            Arc::new(|| "unused-numeric-runtime-id".into()),
            Arc::new(|| 10),
            None,
        ));
        let mut ctx = crate::tools::builtins::test_ctx::make_ctx(".");
        ctx.goal = Some(runtime);
        let result = create_get_goal_tool()
            .call(RuntimeJson::Object(RuntimeObject::new()), &ctx, None)
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(true), "{field}");
        assert!(result.serializable.output.get("goal").is_none());
        let message = result
            .serializable
            .output
            .get("message")
            .and_then(RuntimeJson::as_str)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(message.contains(field), "{message}");
        assert!(message.contains(&i64::MAX.to_string()), "{message}");
        assert_eq!(
            crate::goal::store::get_goal(&thread).await.unwrap(),
            Some(goal)
        );
    }
}
