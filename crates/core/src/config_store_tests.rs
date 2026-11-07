use super::*;

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "klaus-config-store-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn upsert(key_path: &str, value: Value) -> ConfigEdit {
    ConfigEdit {
        key_path: key_path.to_string(),
        value,
        merge: MergeStrategy::Upsert,
    }
}

fn replace(key_path: &str, value: Value) -> ConfigEdit {
    ConfigEdit {
        key_path: key_path.to_string(),
        value,
        merge: MergeStrategy::Replace,
    }
}

fn delete(key_path: &str) -> ConfigEdit {
    ConfigEdit {
        key_path: key_path.to_string(),
        value: Value::Null,
        merge: MergeStrategy::Delete,
    }
}

#[tokio::test]
async fn legacy_sandbox_file_migrates_once_and_replays_notices_after_background_reads() {
    let dir = temp_dir("sandbox-migration");
    let path = dir.join(CONFIG_FILE);
    let original = "# retained project note\nmodel = \"custom-model\"\nsandbox = \"process\"\nsandboxNetworkAllowed = true\nadditionalWritableRoots = [\"/extra\"]\nrequireRealSandbox = false\nconfineReads = true\nadditionalReadableRoots = [\"/private\"]\n\n[desktop]\ncustom = \"retained\"\n";
    std::fs::write(&path, original).unwrap();
    let old_version = content_version(original.as_bytes());

    // Synchronous session assembly may be the first reader of this layer.
    let first = load_blocking(&path).unwrap();
    assert_ne!(first.version, old_version);
    assert_eq!(
        first.values["sandbox_mode"],
        Value::from("danger-full-access")
    );
    assert_eq!(
        first.values["sandbox_workspace_write"]["network_access"],
        Value::from(true)
    );
    assert_eq!(
        first.values["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!(["/extra"])
    );
    assert_eq!(first.values["model"], Value::from("custom-model"));
    assert_eq!(first.values["desktop"]["custom"], Value::from("retained"));
    assert!(!first.values.contains_key("confineReads"));
    assert!(!first.migration_warnings.is_empty());
    let migrated = std::fs::read_to_string(&path).unwrap();
    assert!(migrated.starts_with("# retained project note\n"));
    assert!(!migrated.contains("sandboxNetworkAllowed"));
    assert!(!migrated.contains("requireRealSandbox"));
    assert!(!migrated.contains("additionalReadableRoots"));

    let again = load(&path).await.unwrap();
    assert_eq!(again.version, first.version);
    assert_eq!(again.migration_warnings, first.migration_warnings);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), migrated);
    let error = apply_edits(
        &path,
        &[upsert("model", Value::from("stale"))],
        Some(&old_version),
    )
    .await
    .expect_err("the pre-migration version cannot overwrite the migration");
    assert_eq!(error.code(), "versionConflict");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), migrated);
    let updated = apply_edits(
        &path,
        &[upsert("model", Value::from("updated"))],
        Some(&again.version),
    )
    .await
    .unwrap();
    assert_eq!(
        updated.values["sandbox_mode"],
        Value::from("danger-full-access")
    );
    assert_eq!(updated.migration_warnings, first.migration_warnings);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn migration_keeps_raw_user_and_project_ownership_separate() {
    let dir = temp_dir("sandbox-layer-migration");
    let user = dir.join("user.toml");
    let project = project_config_path(&dir.join("project"));
    std::fs::create_dir_all(project.parent().unwrap()).unwrap();
    std::fs::write(&user, "sandbox = \"process\"\napprovalPolicy = \"never\"\n").unwrap();
    std::fs::write(&project, "sandboxMode = \"read-only\"\n").unwrap();
    let user_document = load(&user).await.unwrap();
    let project_document = load(&project).await.unwrap();
    assert!(!project_document.values.contains_key("approval_policy"));
    assert!(!user_document.values.contains_key("sandbox_workspace_write"));
    let view = resolve_view(&[
        user_document.engine_layer(ConfigGovernanceSource::User),
        project_document.engine_layer(ConfigGovernanceSource::Project),
    ])
    .unwrap();
    assert_eq!(view.resolved["sandbox_mode"], Value::from("read-only"));
    assert_eq!(view.sources["sandbox_mode"], Value::from("project"));
    assert_eq!(view.resolved["approval_policy"], Value::from("never"));
    assert_eq!(view.sources["approval_policy"], Value::from("user"));
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn canonical_writes_reject_retired_keys_and_nested_key_aliases_atomically() {
    let dir = temp_dir("sandbox-canonical-writes");
    let path = dir.join(CONFIG_FILE);
    let initial = "sandbox_mode = \"read-only\"\n";
    std::fs::write(&path, initial).unwrap();
    for (key, value) in [
        ("sandbox", Value::from("process")),
        ("sandboxMode", Value::from("danger-full-access")),
        ("approvalPolicy", Value::from("never")),
        ("requireRealSandbox", Value::from(false)),
        ("sandboxRetryOnDenial", Value::from(true)),
        ("confineReads", Value::from(false)),
        ("additionalWritableRoots", serde_json::json!(["/extra"])),
        ("additionalReadableRoots", serde_json::json!(["/private"])),
        ("sandboxNetworkAllowed", Value::from(true)),
        ("permissionMode", Value::from("bypassPermissions")),
        ("sandbox_workspace_write.networkAccess", Value::from(true)),
        (
            "sandbox_workspace_write",
            serde_json::json!({"writableRoots": ["/extra"]}),
        ),
    ] {
        let error = apply_edits(
            &path,
            &[
                upsert("model", Value::from("must-not-land")),
                upsert(key, value),
            ],
            None,
        )
        .await
        .expect_err("legacy writes must use canonical policy fields");
        assert_eq!(error.code(), "rejected", "{key}: {error}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), initial);
    }
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn canonical_keys_accept_supported_value_aliases_and_report_canonical_resolution() {
    for (key, alias, canonical) in [
        ("approvals_reviewer", "guardian_subagent", "auto_review"),
        ("approvals_reviewer", "automatic", "auto_review"),
        ("approval_policy", "on-failure", "on-request"),
    ] {
        let dir = temp_dir(alias);
        let path = dir.join(CONFIG_FILE);
        let written = apply_edits(&path, &[upsert(key, Value::from(alias))], None)
            .await
            .unwrap();
        let config =
            resolve_config_layers(vec![written.engine_layer(ConfigGovernanceSource::User)])
                .unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap()[key],
            Value::from(canonical)
        );
        assert!(written
            .migration_warnings
            .iter()
            .any(|warning| warning.code == "legacySandboxConfigMigrated"
                && warning.keys.contains(&key.to_string())));
        let reread = load(&path).await.unwrap();
        assert_eq!(reread.values[key], Value::from(canonical));
        let body = std::fs::read_to_string(&path).unwrap();
        let invalid = apply_edits(
            &path,
            &[upsert(key, Value::from("unknown-policy-tag"))],
            Some(&reread.version),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid.code(), "rejected");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
        std::fs::remove_dir_all(dir).ok();
    }
}

#[tokio::test]
async fn rejected_migration_leaves_the_legacy_file_unchanged() {
    let dir = temp_dir("sandbox-invalid-migration");
    let path = dir.join(CONFIG_FILE);
    for original in [
        "sandbox = \"invalid-backend\"\n",
        "additionalWritableRoots = \"not-a-list\"\n",
        "sandboxNetworkAllowed = \"not-a-boolean\"\n",
        "sandbox = \"process\"\n[sandbox_workspace_write]\nnetwork_access = \"not-a-boolean\"\n",
    ] {
        std::fs::write(&path, original).unwrap();
        assert!(load(&path).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    }
    std::fs::remove_dir_all(dir).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn sandbox_migration_writes_through_a_config_symlink() {
    let dir = temp_dir("sandbox-symlink-migration");
    let target = dir.join("owned.toml");
    let link = dir.join(CONFIG_FILE);
    std::fs::write(&target, "sandbox = \"auto\"\n").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let migrated = load(&link).await.unwrap();
    assert!(std::fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        migrated.values["sandbox_mode"],
        Value::from("workspace-write")
    );
    assert!(std::fs::read_to_string(target)
        .unwrap()
        .contains("sandbox_mode"));
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn stored_workspace_roots_keep_raw_text_and_resolve_per_config_directory() {
    let dir = temp_dir("workspace-root-context");
    let user = dir.join("user").join(CONFIG_FILE);
    let project = project_config_path(&dir.join("project"));
    std::fs::create_dir_all(user.parent().unwrap()).unwrap();
    std::fs::create_dir_all(project.parent().unwrap()).unwrap();
    let raw = "[sandbox_workspace_write]\nwritable_roots = [\"../shared\", \"\"]\n";
    std::fs::write(&user, raw).unwrap();
    std::fs::write(&project, raw).unwrap();
    let user_document = load(&user).await.unwrap();
    let project_document = load_blocking(&project).unwrap();
    for document in [&user_document, &project_document] {
        assert_eq!(
            document.values["sandbox_workspace_write"]["writable_roots"],
            serde_json::json!(["../shared", ""])
        );
        assert_eq!(document.version, content_version(raw.as_bytes()));
        assert!(document.migration_warnings.is_empty());
    }
    assert_eq!(
        user_document.engine_values()["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!([dir.join("shared"), dir.join("user")])
    );
    let view = resolve_view(&[
        user_document.engine_layer(ConfigGovernanceSource::User),
        project_document.engine_layer(ConfigGovernanceSource::Project),
    ])
    .unwrap();
    assert_eq!(
        view.resolved["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!([
            dir.join("project").join("shared"),
            dir.join("project").join(".klaus")
        ])
    );
    assert_eq!(
        view.sources["sandbox_workspace_write"],
        Value::from("project")
    );
    assert_eq!(std::fs::read_to_string(&user).unwrap(), raw);
    assert_eq!(std::fs::read_to_string(&project).unwrap(), raw);
    let written = apply_edits(
        &project,
        &[upsert(
            "sandbox_workspace_write.writable_roots",
            serde_json::json!(["../updated"]),
        )],
        Some(&project_document.version),
    )
    .await
    .unwrap();
    assert_eq!(
        written.values["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!(["../updated"])
    );
    assert_eq!(
        written.engine_values()["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!([dir.join("project").join("updated")])
    );
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn sandbox_control_migration_keeps_windows_flags_comments_and_table_order() {
    let dir = temp_dir("windows-feature-migration");
    let path = dir.join(CONFIG_FILE);
    let raw = "sandboxNetworkAllowed = true\n# retained note\n[features] # feature header\nexperimental_windows_sandbox = true\nelevated_windows_sandbox = true\nunrelated_entry   = { raw = \"retained\" } # inline note\n\n[desktop] # later product section\nappearance = \"dark\"\n\n# workspace note\n[sandbox_workspace_write] # workspace header\nexclude_slash_tmp = true\nunrelated_workspace = { raw = \"retained\" } # workspace inline\n";
    std::fs::write(&path, raw).unwrap();
    let old_version = content_version(raw.as_bytes());
    let first = load(&path).await.unwrap();
    assert_eq!(
        first.values["sandbox_workspace_write"]["network_access"],
        Value::from(true)
    );
    assert!(!first.values.contains_key("windows"));
    assert_eq!(
        first.values["features"],
        serde_json::json!({"experimental_windows_sandbox": true, "elevated_windows_sandbox": true, "unrelated_entry": {"raw": "retained"}})
    );
    assert_eq!(
        first.engine_values()["features"],
        serde_json::json!({"experimental_windows_sandbox": true, "elevated_windows_sandbox": true})
    );
    assert!(first
        .migration_warnings
        .iter()
        .any(|warning| warning.code == "unsupportedConfigFeatures"));
    assert!(first.migration_warnings.iter().any(|warning| warning
        .keys
        .contains(&"features.experimental_windows_sandbox".to_string())));
    assert_ne!(first.version, old_version);
    let body = std::fs::read_to_string(&path).unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let again = load_blocking(&path).unwrap();
    assert_eq!(again.version, first.version);
    assert_eq!(again.migration_warnings, first.migration_warnings);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        modified
    );
    assert!(body.starts_with("# retained note\n"));
    assert!(body.contains("[features] # feature header\n"));
    assert!(body.contains("# workspace note\n[sandbox_workspace_write] # workspace header\n"));
    assert!(body.contains("unrelated_workspace = { raw = \"retained\" } # workspace inline\n"));
    assert!(body.contains("unrelated_entry   = { raw = \"retained\" } # inline note\n"));
    assert!(body.contains("[desktop] # later product section\nappearance = \"dark\"\n"));
    assert!(body.find("[features]").unwrap() < body.find("[desktop]").unwrap());
    assert!(body.find("[desktop]").unwrap() < body.find("[sandbox_workspace_write]").unwrap());
    assert!(body.contains("experimental_windows_sandbox = true"));
    assert!(!body.contains("sandboxNetworkAllowed"));
    let stale = apply_edits(
        &path,
        &[upsert("windows.sandbox", Value::from("unelevated"))],
        Some(&old_version),
    )
    .await
    .unwrap_err();
    assert_eq!(stale.code(), "versionConflict");
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn deprecated_windows_feature_writes_preserve_both_boolean_values() {
    let dir = temp_dir("windows-feature-writes");
    let path = dir.join(CONFIG_FILE);
    for key in [
        "experimental_windows_sandbox",
        "enable_experimental_windows_sandbox",
        "elevated_windows_sandbox",
    ] {
        for enabled in [false, true] {
            let written = apply_edits(
                &path,
                &[upsert(&format!("features.{key}"), Value::from(enabled))],
                None,
            )
            .await
            .unwrap();
            assert_eq!(written.values["features"][key], Value::from(enabled));
            assert!(!written.values.contains_key("windows"));
            assert!(written
                .migration_warnings
                .iter()
                .any(|warning| warning.code == "deprecatedSandboxFeature"
                    && warning.keys.contains(&format!("features.{key}"))));
        }
    }
    let written = apply_edits(
        &path,
        &[
            replace(
                "features",
                serde_json::json!({"elevated_windows_sandbox": true, "unrelated_entry": true}),
            ),
            upsert("windows.sandbox", Value::from("unelevated")),
            upsert("windows.allow_mxc", Value::from(false)),
        ],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        written.values["features"],
        serde_json::json!({"elevated_windows_sandbox": true, "unrelated_entry": true})
    );
    assert_eq!(
        written.engine_values()["features"],
        serde_json::json!({"elevated_windows_sandbox": true})
    );
    let config =
        resolve_config_layers(vec![written.engine_layer(ConfigGovernanceSource::User)]).unwrap();
    assert_eq!(
        crate::sandbox::effective_windows_config(&config)
            .unwrap()
            .unwrap()
            .sandbox,
        Some(crate::schemas::config::WindowsSandboxMode::Unelevated)
    );
    let body = std::fs::read_to_string(&path).unwrap();
    let invalid = apply_edits(
        &path,
        &[upsert(
            "features.elevated_windows_sandbox",
            Value::from("true"),
        )],
        Some(&written.version),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code(), "rejected");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn windows_compatibility_flags_resolve_after_user_project_and_session_layers() {
    use crate::schemas::config::WindowsSandboxMode::{Elevated, Unelevated};
    struct Case {
        name: &'static str,
        user: &'static str,
        project: &'static str,
        session: Option<Value>,
        expected: Option<crate::schemas::config::WindowsSandboxMode>,
        source_key: &'static str,
        source: &'static str,
    }
    for case in [
        Case {
            name: "same-false",
            user: "[features]\nelevated_windows_sandbox = true\n",
            project: "[features]\nelevated_windows_sandbox = false\n",
            session: None,
            expected: None,
            source_key: "features.elevated_windows_sandbox",
            source: "project",
        },
        Case {
            name: "explicit-mode",
            user: "[windows]\nsandbox = \"unelevated\"\n",
            project: "[features]\nelevated_windows_sandbox = true\n",
            session: None,
            expected: Some(Unelevated),
            source_key: "windows.sandbox",
            source: "user",
        },
        Case {
            name: "different-flag",
            user: "[features]\nelevated_windows_sandbox = true\n",
            project: "[features]\nexperimental_windows_sandbox = false\n",
            session: None,
            expected: Some(Elevated),
            source_key: "features.elevated_windows_sandbox",
            source: "user",
        },
        Case {
            name: "default",
            user: "",
            project: "",
            session: None,
            expected: None,
            source_key: "features.elevated_windows_sandbox",
            source: "defaults",
        },
        Case {
            name: "alias-false",
            user: "[features]\nenable_experimental_windows_sandbox = true\n",
            project: "[features]\nenable_experimental_windows_sandbox = false\n",
            session: None,
            expected: None,
            source_key: "features.enable_experimental_windows_sandbox",
            source: "project",
        },
        Case {
            name: "independent-alias",
            user: "[features]\nexperimental_windows_sandbox = true\n",
            project: "[features]\nenable_experimental_windows_sandbox = false\n",
            session: None,
            expected: Some(Unelevated),
            source_key: "features.experimental_windows_sandbox",
            source: "user",
        },
        Case {
            name: "session-flag",
            user: "[features]\nelevated_windows_sandbox = true\n",
            project: "[features]\nelevated_windows_sandbox = false\n",
            session: Some(serde_json::json!({"features": {"elevated_windows_sandbox": true}})),
            expected: Some(Elevated),
            source_key: "features.elevated_windows_sandbox",
            source: "sessionFlags",
        },
    ] {
        let dir = temp_dir(case.name);
        let user = dir.join(CONFIG_FILE);
        let project = dir.join("project").join(".klaus").join(CONFIG_FILE);
        std::fs::create_dir_all(project.parent().unwrap()).unwrap();
        std::fs::write(&user, case.user).unwrap();
        std::fs::write(&project, case.project).unwrap();
        let user_document = load(&user).await.unwrap();
        let project_document = load(&project).await.unwrap();
        assert_eq!(std::fs::read_to_string(&user).unwrap(), case.user);
        assert_eq!(std::fs::read_to_string(&project).unwrap(), case.project);
        let mut layers = vec![
            user_document.engine_layer(ConfigGovernanceSource::User),
            project_document.engine_layer(ConfigGovernanceSource::Project),
        ];
        if let Some(session) = case.session {
            layers.push(
                config_layer_from_value(ConfigGovernanceSource::SessionFlags, session).unwrap(),
            );
        }
        let view = resolve_view(&layers).unwrap();
        assert_eq!(
            view.sources[case.source_key],
            Value::from(case.source),
            "{}",
            case.name
        );
        let config = resolve_config_layers(layers).unwrap();
        assert_eq!(
            crate::sandbox::effective_windows_config(&config)
                .unwrap()
                .and_then(|windows| windows.sandbox),
            case.expected,
            "{}",
            case.name
        );
        std::fs::remove_dir_all(dir).ok();
    }
}

#[tokio::test]
async fn sparse_sandbox_tables_keep_inherited_roots_tmp_controls_and_mxc_opt_out() {
    let dir = temp_dir("sparse-sandbox-tables");
    let user = dir.join(CONFIG_FILE);
    let project = dir.join("project").join(".klaus").join(CONFIG_FILE);
    std::fs::create_dir_all(project.parent().unwrap()).unwrap();
    let user_raw = "[sandbox_workspace_write]\nwritable_roots = [\"shared\"]\nexclude_tmpdir_env_var = true\nexclude_slash_tmp = true\n[windows]\nallow_mxc = false\n[features]\nelevated_windows_sandbox = true\n";
    let project_raw = "[sandbox_workspace_write]\nnetwork_access = true\n[windows]\nsandbox = \"unelevated\"\n[features]\nprefer_mxc = true\nexperimental_windows_sandbox = false\n";
    std::fs::write(&user, user_raw).unwrap();
    std::fs::write(&project, project_raw).unwrap();
    let user_document = load(&user).await.unwrap();
    let project_document = load(&project).await.unwrap();
    let layers = vec![
        user_document.engine_layer(ConfigGovernanceSource::User),
        project_document.engine_layer(ConfigGovernanceSource::Project),
    ];
    let view = resolve_view(&layers).unwrap();
    assert_eq!(
        view.resolved["sandbox_workspace_write"]["writable_roots"],
        serde_json::json!([dir.join("shared")])
    );
    assert_eq!(
        view.resolved["sandbox_workspace_write"]["network_access"],
        Value::from(true)
    );
    assert_eq!(
        view.resolved["sandbox_workspace_write"]["exclude_tmpdir_env_var"],
        Value::from(true)
    );
    assert_eq!(
        view.resolved["sandbox_workspace_write"]["exclude_slash_tmp"],
        Value::from(true)
    );
    assert_eq!(
        view.resolved["windows"],
        serde_json::json!({"sandbox": "unelevated", "allow_mxc": false})
    );
    for key in [
        "sandbox_workspace_write.writable_roots",
        "sandbox_workspace_write.exclude_tmpdir_env_var",
        "sandbox_workspace_write.exclude_slash_tmp",
        "windows.allow_mxc",
        "features.elevated_windows_sandbox",
    ] {
        assert_eq!(view.sources[key], Value::from("user"), "{key}");
    }
    for key in [
        "sandbox_workspace_write.network_access",
        "windows.sandbox",
        "features.prefer_mxc",
        "features.experimental_windows_sandbox",
    ] {
        assert_eq!(view.sources[key], Value::from("project"), "{key}");
    }
    assert_eq!(
        view.sources["sandbox_workspace_write"],
        Value::from("project")
    );
    let config = resolve_config_layers(layers.clone()).unwrap();
    assert_eq!(config.windows.as_ref().unwrap().allow_mxc, Some(false));
    assert_eq!(
        crate::sandbox::effective_windows_config(&config)
            .unwrap()
            .unwrap()
            .sandbox,
        Some(crate::schemas::config::WindowsSandboxMode::Unelevated)
    );
    let mut conflict = layers;
    conflict.push(
        config_layer_from_value(
            ConfigGovernanceSource::SessionFlags,
            serde_json::json!({"windows": {"sandbox": "mxc"}}),
        )
        .unwrap(),
    );
    assert_eq!(
        resolve_config_layers(conflict).unwrap_err(),
        "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false"
    );
    assert_eq!(std::fs::read_to_string(&user).unwrap(), user_raw);
    assert_eq!(std::fs::read_to_string(&project).unwrap(), project_raw);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn sandbox_preference_projects_only_the_supported_key_and_preserves_raw_features() {
    let dir = temp_dir("sandbox-preference");
    let path = dir.join(CONFIG_FILE);
    let raw = "# keep feature metadata\n[features]\nprefer_mxc = true\nunrelated_entry = { raw = \"retained\" }\n";
    std::fs::write(&path, raw).unwrap();
    let loaded = load(&path).await.unwrap();
    assert_eq!(loaded.version, content_version(raw.as_bytes()));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), raw);
    assert_eq!(
        loaded.values["features"],
        serde_json::json!({"prefer_mxc": true, "unrelated_entry": {"raw": "retained"}})
    );
    assert_eq!(
        loaded.engine_values()["features"],
        serde_json::json!({"prefer_mxc": true})
    );
    assert!(!loaded.values.contains_key("windows"));
    let config =
        resolve_config_layers(vec![loaded.engine_layer(ConfigGovernanceSource::User)]).unwrap();
    assert!(config.features.prefer_mxc);
    assert!(config.windows.is_none());
    assert!(loaded
        .migration_warnings
        .iter()
        .any(|warning| warning.code == "unsupportedConfigFeatures"
            && warning.keys == vec!["features.unrelated_entry".to_string()]));
    let written = apply_edits(
        &path,
        &[upsert("features.prefer_mxc", Value::from(false))],
        Some(&loaded.version),
    )
    .await
    .unwrap();
    assert_eq!(written.values["features"]["prefer_mxc"], Value::from(false));
    assert_eq!(
        written.values["features"]["unrelated_entry"],
        serde_json::json!({"raw": "retained"})
    );
    assert_eq!(
        written.engine_values()["features"],
        serde_json::json!({"prefer_mxc": false})
    );
    let body = std::fs::read_to_string(&path).unwrap();
    let invalid = apply_edits(
        &path,
        &[upsert("features.prefer_mxc", Value::from("true"))],
        Some(&written.version),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code(), "rejected");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
    let replaced = apply_edits(
        &path,
        &[replace("features", serde_json::json!({"prefer_mxc": true}))],
        Some(&written.version),
    )
    .await
    .unwrap();
    assert_eq!(
        replaced.engine_values()["features"],
        serde_json::json!({"prefer_mxc": true})
    );
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn explicit_disallowed_mxc_is_rejected_atomically_by_the_config_writer() {
    let dir = temp_dir("mxc-conflict");
    let path = dir.join(CONFIG_FILE);
    let raw = "model = \"model-x\"\n";
    std::fs::write(&path, raw).unwrap();
    let error = apply_edits(
        &path,
        &[upsert(
            "windows",
            serde_json::json!({"sandbox": "mxc", "allow_mxc": false}),
        )],
        Some(&content_version(raw.as_bytes())),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "rejected");
    assert!(error
        .to_string()
        .contains("windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false"));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), raw);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn legacy_landlock_preference_remains_raw_and_active_with_deprecation_notice() {
    let dir = temp_dir("legacy-landlock-preference");
    let path = dir.join(CONFIG_FILE);
    let raw = "# keep Linux preferences\n[features]\nuse_legacy_landlock = false\nuse_linux_sandbox_bwrap = true\n";
    std::fs::write(&path, raw).unwrap();
    let loaded = load(&path).await.unwrap();
    assert_eq!(loaded.version, content_version(raw.as_bytes()));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), raw);
    assert_eq!(
        loaded.values["features"],
        serde_json::json!({"use_legacy_landlock": false, "use_linux_sandbox_bwrap": true})
    );
    assert_eq!(
        loaded.engine_values()["features"],
        serde_json::json!({"use_legacy_landlock": false})
    );
    assert!(loaded
        .migration_warnings
        .iter()
        .any(|warning| warning.code == "deprecatedSandboxFeature"
            && warning.keys == vec!["features.use_legacy_landlock".to_string()]));
    assert!(loaded
        .migration_warnings
        .iter()
        .any(|warning| warning.code == "unsupportedConfigFeatures"
            && warning.keys == vec!["features.use_linux_sandbox_bwrap".to_string()]));
    let written = apply_edits(
        &path,
        &[upsert("features.use_legacy_landlock", Value::from(true))],
        Some(&loaded.version),
    )
    .await
    .unwrap();
    assert_eq!(
        written.values["features"]["use_linux_sandbox_bwrap"],
        Value::from(true)
    );
    let config =
        resolve_config_layers(vec![written.engine_layer(ConfigGovernanceSource::User)]).unwrap();
    assert!(config.features.use_legacy_landlock);
    let body = std::fs::read_to_string(&path).unwrap();
    let invalid = apply_edits(
        &path,
        &[upsert("features.use_legacy_landlock", Value::from("true"))],
        Some(&written.version),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code(), "rejected");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
    std::fs::remove_dir_all(dir).ok();
}

/// The whole reason this file is TOML: a settings toggle must not cost the user
/// the notes and the parked-out block they hand-wrote around it.
#[tokio::test]
async fn editing_one_key_keeps_comments_order_and_spacing() {
    let dir = temp_dir("preserve");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(
        &path,
        "# top of file\nmodel = \"claude\"\n\n# why this is off\nworkflow = false\n\n#OFF# [mcpServers.figma]\n#OFF# url = \"https://mcp.figma.com/mcp\"\n",
    )
    .unwrap();

    apply_edits(
        &path,
        &[upsert("maxToolConcurrency", Value::from(12))],
        None,
    )
    .await
    .unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        raw,
        "# top of file\nmodel = \"claude\"\n\n# why this is off\nworkflow = false\nmaxToolConcurrency = 12\n\n#OFF# [mcpServers.figma]\n#OFF# url = \"https://mcp.figma.com/mcp\"\n"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// A stale writer must lose, not overwrite: the settings page and a hand edit
/// both rewrite the same file.
#[tokio::test]
async fn a_stale_expected_version_is_refused() {
    let dir = temp_dir("conflict");
    let path = dir.join(CONFIG_FILE);

    let first = apply_edits(&path, &[upsert("maxToolConcurrency", Value::from(3))], None)
        .await
        .unwrap();
    // Someone else writes in between.
    apply_edits(&path, &[upsert("maxToolConcurrency", Value::from(4))], None)
        .await
        .unwrap();

    let error = apply_edits(
        &path,
        &[upsert("maxToolConcurrency", Value::from(5))],
        Some(&first.version),
    )
    .await
    .expect_err("a stale version must not write");
    assert_eq!(error.code(), "versionConflict");
    assert_eq!(
        load(&path).await.unwrap().values["maxToolConcurrency"],
        Value::from(4)
    );

    // The version the last successful write returned still writes.
    let current = load(&path).await.unwrap();
    apply_edits(
        &path,
        &[upsert("maxToolConcurrency", Value::from(5))],
        Some(&current.version),
    )
    .await
    .unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// A typo in an engine key is drift, not product data: it must fail loudly at
/// the write, and the file must be untouched afterwards.
#[tokio::test]
async fn an_unknown_engine_key_is_rejected_and_nothing_is_written() {
    let dir = temp_dir("unknown");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(&path, "maxToolConcurrency = 3\n").unwrap();

    let error = apply_edits(
        &path,
        &[upsert("maxToolConcurrencys", Value::from(9))],
        None,
    )
    .await
    .expect_err("an unknown key must not write");
    assert_eq!(error.code(), "rejected");
    assert!(error.to_string().contains("maxToolConcurrencys"), "{error}");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "maxToolConcurrency = 3\n"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The wrong type for a known key fails the same way, with the engine's own
/// validator doing the judging.
#[tokio::test]
async fn a_wrong_typed_value_is_rejected() {
    let dir = temp_dir("typed");
    let path = dir.join(CONFIG_FILE);

    let error = apply_edits(
        &path,
        &[upsert("maxToolConcurrency", Value::from("many"))],
        None,
    )
    .await
    .expect_err("a string maxToolConcurrency must not write");
    assert_eq!(error.code(), "rejected");
    assert!(!path.exists(), "a rejected write must not create the file");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn retired_max_turns_is_ignored_on_read_and_rejected_on_write() {
    let dir = temp_dir("retired-max-turns");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(&path, "maxTurns = 100\nmaxToolConcurrency = 4\n").unwrap();

    let document = load(&path).await.unwrap();
    let layer = document.engine_layer(ConfigGovernanceSource::User);
    assert_eq!(layer.values["maxToolConcurrency"], Value::from(4));
    assert!(!layer.values.contains_key("maxTurns"));

    let error = apply_edits(&path, &[upsert("maxTurns", Value::from(200))], None)
        .await
        .expect_err("retired maxTurns must not be written again");
    assert_eq!(error.code(), "rejected");
    assert!(error.to_string().contains("retired"));
    std::fs::remove_dir_all(&dir).ok();
}

/// Product sections are stored verbatim: the engine has no schema for a desktop
/// preference and must not invent one.
#[tokio::test]
async fn product_sections_are_stored_without_engine_validation() {
    let dir = temp_dir("product");
    let path = dir.join(CONFIG_FILE);

    let stored = apply_edits(
        &path,
        &[
            upsert("desktop.dockIconPreference", Value::from("hidden")),
            upsert("desktop.followUpQueueMode", Value::from("queue")),
            upsert(
                "projects.\"/Users/me/work.repo\".trustLevel",
                Value::from("trusted"),
            ),
        ],
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        stored.product_section("desktop")["dockIconPreference"],
        Value::from("hidden")
    );
    assert_eq!(
        stored.product_section("projects")["/Users/me/work.repo"]["trustLevel"],
        Value::from("trusted")
    );
    assert!(stored.engine_values().is_empty());
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .contains("[projects.\"/Users/me/work.repo\"]"));
    std::fs::remove_dir_all(&dir).ok();
}

/// Upsert merges into the table already there; replace throws it away. The
/// difference is the whole reason a settings page can write one nested field.
#[tokio::test]
async fn upsert_merges_a_nested_table_and_replace_overwrites_it() {
    let dir = temp_dir("merge");
    let path = dir.join(CONFIG_FILE);

    apply_edits(
        &path,
        &[upsert(
            "webSearch",
            serde_json::json!({ "mode": "client", "maxUses": 5 }),
        )],
        None,
    )
    .await
    .unwrap();

    let merged = apply_edits(
        &path,
        &[upsert(
            "webSearch",
            serde_json::json!({ "allowedDomains": ["example.com"] }),
        )],
        None,
    )
    .await
    .unwrap();
    assert_eq!(merged.values["webSearch"]["mode"], Value::from("client"));
    assert_eq!(merged.values["webSearch"]["maxUses"], Value::from(5));
    assert_eq!(
        merged.values["webSearch"]["allowedDomains"],
        serde_json::json!(["example.com"])
    );

    let replaced = apply_edits(
        &path,
        &[replace("webSearch", serde_json::json!({ "mode": "off" }))],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        replaced.values["webSearch"],
        serde_json::json!({"mode": "off"})
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Unsetting has to reach the same key path that set it, including a null value
/// standing in for a delete (the shape a batch write sends).
#[tokio::test]
async fn deleting_removes_the_key_by_strategy_or_by_null() {
    let dir = temp_dir("delete");
    let path = dir.join(CONFIG_FILE);

    apply_edits(
        &path,
        &[
            upsert("maxToolConcurrency", Value::from(7)),
            upsert("desktop.showUsage", Value::from(true)),
        ],
        None,
    )
    .await
    .unwrap();

    let after = apply_edits(
        &path,
        &[
            delete("maxToolConcurrency"),
            replace("desktop.showUsage", Value::Null),
        ],
        None,
    )
    .await
    .unwrap();
    assert!(!after.values.contains_key("maxToolConcurrency"));
    assert_eq!(after.product_section("desktop"), serde_json::json!({}));
    std::fs::remove_dir_all(&dir).ok();
}

/// A hand-written inline table stays addressable: writing inside it converts it
/// to a section rather than failing or clobbering its siblings.
#[tokio::test]
async fn writing_inside_an_inline_table_keeps_its_other_keys() {
    let dir = temp_dir("inline");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(
        &path,
        "[mcpServers]\nnode = { command = \"node\", args = [\"repl.js\"] }\n",
    )
    .unwrap();

    let stored = apply_edits(
        &path,
        &[upsert("mcpServers.node.env.PORT", Value::from("8080"))],
        None,
    )
    .await
    .unwrap();

    let node = &stored.product_section("mcpServers")["node"];
    assert_eq!(node["command"], Value::from("node"));
    assert_eq!(node["args"], serde_json::json!(["repl.js"]));
    assert_eq!(node["env"]["PORT"], Value::from("8080"));
    std::fs::remove_dir_all(&dir).ok();
}

/// Descending into a scalar is a caller mistake, and the message has to name the
/// key so a settings page can show it.
#[tokio::test]
async fn descending_into_a_scalar_is_a_key_path_error() {
    let dir = temp_dir("scalar");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(&path, "model = \"claude\"\n").unwrap();

    let error = apply_edits(&path, &[upsert("model.name", Value::from("x"))], None)
        .await
        .expect_err("a scalar cannot hold nested keys");
    assert_eq!(error.code(), "invalidKeyPath");
    assert!(error.to_string().contains("model"), "{error}");
    std::fs::remove_dir_all(&dir).ok();
}

/// A corrupt file is surfaced, not swallowed: starting on defaults would quietly
/// discard everything the user configured.
#[tokio::test]
async fn a_malformed_file_is_an_error_rather_than_empty_config() {
    let dir = temp_dir("malformed");
    let path = dir.join(CONFIG_FILE);
    std::fs::write(&path, "model = \n").unwrap();

    let error = load(&path).await.expect_err("malformed TOML must not load");
    assert_eq!(error.code(), "parse");
    std::fs::remove_dir_all(&dir).ok();
}

/// A missing file is the same state as an empty one, and the version it reports
/// is the one a first write can be made against.
#[tokio::test]
async fn a_missing_file_loads_as_an_empty_document() {
    let dir = temp_dir("missing");
    let path = dir.join(CONFIG_FILE);

    let document = load(&path).await.unwrap();
    assert!(document.values.is_empty());

    apply_edits(
        &path,
        &[upsert("maxToolConcurrency", Value::from(2))],
        Some(&document.version),
    )
    .await
    .unwrap();
    assert_eq!(
        load(&path).await.unwrap().values["maxToolConcurrency"],
        Value::from(2)
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The engine slice is what a session resolves, and product sections must stay
/// out of it or governance would reject the whole file as unknown keys.
#[tokio::test]
async fn the_engine_layer_carries_engine_keys_only() {
    let dir = temp_dir("layer");
    let path = dir.join(CONFIG_FILE);

    let stored = apply_edits(
        &path,
        &[
            upsert("maxToolConcurrency", Value::from(4)),
            upsert("desktop.theme", Value::from("dark")),
        ],
        None,
    )
    .await
    .unwrap();

    let layer = stored.engine_layer(ConfigGovernanceSource::User);
    assert_eq!(layer.source, ConfigGovernanceSource::User);
    assert_eq!(layer.values.len(), 1);
    assert_eq!(layer.values["maxToolConcurrency"], Value::from(4));
    std::fs::remove_dir_all(&dir).ok();
}

/// Writing through a symlink must land in the file it points at: a dotfiles repo
/// keeps the real config, and replacing the link would silently detach it.
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_config_is_written_through_to_its_target() {
    let dir = temp_dir("symlink");
    let real = dir.join("real-config.toml");
    let link = dir.join(CONFIG_FILE);
    std::fs::write(&real, "maxToolConcurrency = 1\n").unwrap();
    std::os::unix::fs::symlink(&real, &link).unwrap();

    apply_edits(&link, &[upsert("maxToolConcurrency", Value::from(6))], None)
        .await
        .unwrap();

    assert!(std::fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        std::fs::read_to_string(&real).unwrap(),
        "maxToolConcurrency = 6\n"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Quoted segments are the only way to address a key that contains dots, and the
/// path parser has to reject the shapes it cannot mean.
#[test]
fn key_paths_split_on_dots_outside_quotes() {
    assert_eq!(
        parse_key_path("webSearch.allowedDomains").unwrap(),
        vec!["webSearch".to_string(), "allowedDomains".to_string()]
    );
    assert_eq!(
        parse_key_path("plugins.\"browser@bundled\".enabled").unwrap(),
        vec![
            "plugins".to_string(),
            "browser@bundled".to_string(),
            "enabled".to_string(),
        ]
    );
    assert_eq!(
        parse_key_path("projects.\"/Users/me/a.b\"").unwrap(),
        vec!["projects".to_string(), "/Users/me/a.b".to_string()]
    );
    assert!(parse_key_path("").is_err());
    assert!(parse_key_path("a..b").is_err());
    assert!(parse_key_path("a.\"unterminated").is_err());
}
