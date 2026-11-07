use super::*;

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "klaus-hooks-store-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn command_hook(command: &str) -> Value {
    serde_json::json!([{ "hooks": [{ "type": "command", "command": command }] }])
}

/// The roster the validator uses IS the engine's dispatch roster, so an event
/// the engine never fires cannot be saved. The host's own copy of this list
/// carried `PermissionRequest`, which the engine has never dispatched.
#[test]
fn the_event_roster_is_the_engines_own() {
    let names = hook_event_names();
    assert_eq!(names.len(), HOOK_EVENTS.len());
    assert!(names.contains(&"PreToolUse"));
    assert!(!names.contains(&"PermissionRequest"));

    let mut hooks = Map::new();
    hooks.insert("PermissionRequest".into(), command_hook("echo hi"));
    let error = validate(&hooks).expect_err("an undispatched event must not save");
    assert!(error.to_string().contains("PermissionRequest"), "{error}");
}

#[test]
fn a_malformed_group_names_the_event_and_the_problem() {
    let mut hooks = Map::new();
    hooks.insert(
        "PreToolUse".into(),
        serde_json::json!([{ "hooks": [{ "type": "command" }] }]),
    );
    let error = validate(&hooks).expect_err("a command hook needs a command");
    assert!(error.to_string().contains("PreToolUse"), "{error}");
    assert!(error.to_string().contains("command"), "{error}");
}

#[tokio::test]
async fn a_round_trip_keeps_the_file_shape() {
    let dir = temp_dir("roundtrip");
    let path = dir.join(HOOKS_FILE);

    let mut hooks = Map::new();
    hooks.insert("PreToolUse".into(), command_hook("echo before"));
    store(&path, &hooks).await.unwrap();

    assert_eq!(load(&path).await.unwrap(), hooks);
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.contains("\"PreToolUse\""), "{raw}");
    std::fs::remove_dir_all(&dir).ok();
}

/// Clearing the hooks removes the file: "no hooks" has to be one state on disk,
/// or a later reader has to know that `{}` and absent mean the same thing.
#[tokio::test]
async fn clearing_the_hooks_removes_the_file() {
    let dir = temp_dir("clear");
    let path = dir.join(HOOKS_FILE);
    let mut hooks = Map::new();
    hooks.insert("Stop".into(), command_hook("echo done"));
    store(&path, &hooks).await.unwrap();

    store(&path, &Map::new()).await.unwrap();
    assert!(!path.exists());
    assert_eq!(load(&path).await.unwrap(), Map::new());
    std::fs::remove_dir_all(&dir).ok();
}

/// A malformed file is surfaced rather than read as "no hooks", which would look
/// like the user's hooks had been deleted.
#[tokio::test]
async fn a_malformed_file_is_an_error() {
    let dir = temp_dir("malformed");
    let path = dir.join(HOOKS_FILE);
    std::fs::write(&path, "{ not json").unwrap();

    let error = load(&path).await.expect_err("malformed JSON must not load");
    assert_eq!(error.code(), "parse");
    std::fs::remove_dir_all(&dir).ok();
}

/// A rejected write leaves the file exactly as it was.
#[tokio::test]
async fn a_rejected_write_does_not_touch_the_file() {
    let dir = temp_dir("reject");
    let path = dir.join(HOOKS_FILE);
    let mut good = Map::new();
    good.insert("Stop".into(), command_hook("echo done"));
    store(&path, &good).await.unwrap();
    let before = std::fs::read_to_string(&path).unwrap();

    let mut bad = Map::new();
    bad.insert("NotAnEvent".into(), command_hook("echo nope"));
    store(&path, &bad)
        .await
        .expect_err("unknown event must not write");

    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    std::fs::remove_dir_all(&dir).ok();
}
