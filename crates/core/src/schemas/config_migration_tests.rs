use super::*;
use crate::config_governance::ConfigGovernanceSource;
use crate::engine::session::{config_layer_from_value, resolve_config, resolve_config_layers};
use crate::runtime_values::RuntimeString;
use crate::schemas::config::{EngineConfig, SandboxWorkspaceWrite};
use crate::types::permission::{
    ApprovalPolicy, ApprovalsReviewer, ExternalPermissionMode, SandboxMode,
};

fn normalize(value: Value) -> (Map<String, Value>, Vec<ConfigMigrationWarning>) {
    let mut values = value.as_object().unwrap().clone();
    let warnings = normalize_sandbox_config(&mut values).unwrap();
    (values, warnings)
}

#[test]
fn old_backend_and_flat_controls_are_explicitly_migrated() {
    let (values, warnings) = normalize(json!({
        "sandbox": "process",
        "sandboxNetworkAllowed": true,
        "additionalWritableRoots": ["/work/extra"],
        "requireRealSandbox": false,
        "sandboxRetryOnDenial": true,
        "confineReads": true,
        "additionalReadableRoots": ["/private"]
    }));
    assert_eq!(values["sandbox_mode"], json!("danger-full-access"));
    assert_eq!(
        values["sandbox_workspace_write"],
        json!({
            "network_access": true, "writable_roots": ["/work/extra"]
        })
    );
    assert_eq!(values.len(), 2);
    let keys: Vec<_> = warnings
        .iter()
        .flat_map(|warning| warning.keys.clone())
        .collect();
    for key in [
        "sandbox",
        "sandboxNetworkAllowed",
        "additionalWritableRoots",
        "requireRealSandbox",
        "sandboxRetryOnDenial",
        "confineReads",
        "additionalReadableRoots",
    ] {
        assert!(keys.contains(&key.into()), "missing notice for {key}");
    }
    assert!(warnings
        .iter()
        .all(|warning| warning.code == "legacySandboxConfigMigrated"));
    let (_, repeated) = normalize(Value::Object(values));
    assert!(repeated.is_empty());
}

#[test]
fn canonical_policy_and_workspace_fields_win_over_legacy_inputs() {
    let (values, warnings) = normalize(json!({
        "approval_policy": "on-request", "approvalPolicy": "never",
        "sandbox_mode": "read-only", "sandboxMode": "danger-full-access",
        "approvals_reviewer": "user", "approvalsReviewer": "automatic",
        "sandbox": "process", "permissionMode": "bypassPermissions",
        "sandbox_workspace_write": {"network_access": false, "writable_roots": ["/canonical"]},
        "sandboxNetworkAllowed": true, "additionalWritableRoots": ["/legacy"]
    }));
    assert_eq!(values["approval_policy"], json!("on-request"));
    assert_eq!(values["sandbox_mode"], json!("read-only"));
    assert_eq!(values["approvals_reviewer"], json!("user"));
    assert_eq!(values["permissionMode"], json!("default"));
    assert_eq!(
        values["sandbox_workspace_write"]["network_access"],
        json!(false)
    );
    assert_eq!(
        values["sandbox_workspace_write"]["writable_roots"],
        json!(["/canonical"])
    );
    assert!(!warnings.is_empty());
}

#[test]
fn platform_backend_names_become_modes_without_a_backend_override() {
    for backend in [
        "auto",
        "seatbelt",
        "bubblewrap",
        "native-helper",
        "windows-helper",
        "windows-restricted-token",
    ] {
        let (values, warnings) = normalize(json!({"sandbox": backend}));
        assert_eq!(
            values,
            json!({"sandbox_mode": "workspace-write"})
                .as_object()
                .unwrap()
                .clone()
        );
        assert_eq!(warnings[0].keys, ["sandbox"]);
    }
}

#[test]
fn aliases_have_notices_and_workspace_defaults_are_exact() {
    let (values, warnings) = normalize(json!({
        "approvalPolicy": "on-failure", "approvalsReviewer": "guardian_subagent", "sandboxMode": "workspace-write",
        "sandboxWorkspaceWrite": {"writableRoots": ["/extra"], "networkAccess": true, "excludeTmpdirEnvVar": true, "excludeSlashTmp": true}
    }));
    assert_eq!(values["approval_policy"], json!("on-request"));
    assert_eq!(values["approvals_reviewer"], json!("auto_review"));
    assert_eq!(
        values["sandbox_workspace_write"],
        json!({"writable_roots": ["/extra"], "network_access": true, "exclude_tmpdir_env_var": true, "exclude_slash_tmp": true})
    );
    assert_eq!(warnings.len(), 3);
    assert_eq!(
        serde_json::to_value(SandboxWorkspaceWrite::default()).unwrap(),
        json!({
            "writable_roots": [], "network_access": false, "exclude_tmpdir_env_var": false, "exclude_slash_tmp": false
        })
    );
    assert_eq!(
        serde_json::from_value::<SandboxWorkspaceWrite>(json!({"future_extra": true})).unwrap(),
        SandboxWorkspaceWrite::default()
    );
    assert_eq!(
        crate::runtime_values::runtime_codec::decode_text::<SandboxWorkspaceWrite>(
            &crate::runtime_values::RuntimeString::from(r#"{"future_extra":true}"#),
        )
        .unwrap(),
        SandboxWorkspaceWrite::default()
    );
}

#[test]
fn legacy_presets_convert_only_without_explicit_policy_and_keep_plan_separate() {
    for (preset, approval, sandbox, reviewer) in [
        (
            "bypassPermissions",
            ApprovalPolicy::Never,
            SandboxMode::DangerFullAccess,
            ApprovalsReviewer::User,
        ),
        (
            "auto",
            ApprovalPolicy::OnRequest,
            SandboxMode::WorkspaceWrite,
            ApprovalsReviewer::Automatic,
        ),
        (
            "acceptEdits",
            ApprovalPolicy::OnRequest,
            SandboxMode::WorkspaceWrite,
            ApprovalsReviewer::User,
        ),
    ] {
        let config = resolve_config(json!({"permissionMode": preset})).unwrap();
        assert_eq!(config.approval_policy, Some(approval));
        assert_eq!(config.sandbox_mode, Some(sandbox));
        assert_eq!(config.approvals_reviewer, Some(reviewer));
        assert_eq!(config.permission_mode, ExternalPermissionMode::Default);
        let explicit =
            resolve_config(json!({"permissionMode": preset, "sandbox_mode": "read-only"})).unwrap();
        assert_eq!(explicit.sandbox_mode, Some(SandboxMode::ReadOnly));
        assert_eq!(explicit.approval_policy, Some(ApprovalPolicy::OnRequest));
    }
    let plan = resolve_config(json!({"permissionMode": "plan"})).unwrap();
    assert_eq!(plan.permission_mode, ExternalPermissionMode::Plan);
    assert_eq!(plan.sandbox_mode, Some(SandboxMode::ReadOnly));
}

#[test]
fn config_layers_normalize_before_governance_and_search_uses_the_canonical_mode() {
    let layers = vec![
        config_layer_from_value(ConfigGovernanceSource::User, json!({"sandbox": "process"}))
            .unwrap(),
        config_layer_from_value(
            ConfigGovernanceSource::Project,
            json!({"sandbox_mode": "read-only"}),
        )
        .unwrap(),
    ];
    let config = resolve_config_layers(layers).unwrap();
    assert_eq!(config.sandbox_mode, Some(SandboxMode::ReadOnly));
    assert_eq!(
        config.web_search.access,
        crate::schemas::config::WebSearchAccess::Cached
    );
    let full = resolve_config(json!({"sandbox_mode": "danger-full-access"})).unwrap();
    assert_eq!(
        full.web_search.access,
        crate::schemas::config::WebSearchAccess::Live
    );
    let defaults = resolve_config(json!({})).unwrap();
    assert_eq!(defaults.sandbox_mode, EngineConfig::default().sandbox_mode);
}

#[test]
fn explicit_defaults_do_not_narrow_granular_approval_policy_to_a_string() {
    let config = resolve_config(json!({
        "approval_policy": {"granular": {
            "sandbox_approval": true, "rules": false,
            "skill_approval": true, "request_permissions": false, "mcp_elicitations": true
        }}
    }))
    .unwrap();
    assert!(matches!(
        config.approval_policy,
        Some(ApprovalPolicy::Granular(_))
    ));
}

#[test]
fn workspace_root_ingress_requires_an_explicit_base_for_relative_inputs() {
    for root in ["", "relative/child", "~/child"] {
        assert!(
            serde_json::from_value::<SandboxWorkspaceWrite>(json!({
                "writable_roots": [root]
            }))
            .is_err(),
            "{root:?} requires a base"
        );
        let input = crate::runtime_values::RuntimeString::from(
            serde_json::to_string(&json!({"writable_roots": [root]})).unwrap(),
        );
        assert!(
            crate::runtime_values::runtime_codec::decode_text::<SandboxWorkspaceWrite>(&input)
                .is_err()
        );
    }
}

#[test]
fn workspace_roots_are_lexical_paths_without_an_existence_requirement() {
    let base = std::env::temp_dir().join("klaus-root-config-owner");
    let mut workspace = SandboxWorkspaceWrite {
        writable_roots: vec!["".into(), "./child/../target".into(), "../shared".into()],
        ..SandboxWorkspaceWrite::default()
    };
    workspace.normalize_writable_roots(Some(&base)).unwrap();
    assert_eq!(
        workspace.writable_roots,
        vec![
            RuntimeString::from(base.to_str().unwrap()),
            RuntimeString::from(base.join("target").to_str().unwrap()),
            RuntimeString::from(base.parent().unwrap().join("shared").to_str().unwrap()),
        ]
    );
    let dotted = base.join("nested").join("..").join("target");
    let parsed: SandboxWorkspaceWrite = serde_json::from_value(json!({
        "writable_roots": [dotted]
    }))
    .unwrap();
    assert_eq!(
        parsed.writable_roots,
        vec![RuntimeString::from(base.join("target").to_str().unwrap())]
    );
}

#[test]
fn path_normalization_preserves_raw_table_shape_and_uses_the_supplied_owner() {
    let base = std::env::temp_dir().join("klaus-config-owner");
    let mut values = json!({
        "sandbox_workspace_write": {"writable_roots": ["../shared", ""], "future_extra": true},
        "model": "existing-model"
    })
    .as_object()
    .unwrap()
    .clone();
    crate::schemas::config::normalize_sandbox_workspace_roots(&mut values, Some(&base)).unwrap();
    assert_eq!(
        values["sandbox_workspace_write"],
        json!({
            "writable_roots": [base.parent().unwrap().join("shared"), base],
            "future_extra": true
        })
    );
    assert_eq!(values["model"], json!("existing-model"));
}

#[test]
fn home_relative_config_roots_expand_only_with_explicit_base_context() {
    #[allow(deprecated)]
    let Some(home) = std::env::home_dir() else {
        return;
    };
    let base = std::env::temp_dir().join("klaus-config-owner");
    let mut workspace = SandboxWorkspaceWrite {
        writable_roots: vec!["~".into(), "~/extra".into()],
        ..SandboxWorkspaceWrite::default()
    };
    workspace.normalize_writable_roots(Some(&base)).unwrap();
    assert_eq!(
        workspace.writable_roots,
        vec![
            RuntimeString::from(home.to_str().unwrap()),
            RuntimeString::from(home.join("extra").to_str().unwrap()),
        ]
    );
}

#[cfg(unix)]
#[test]
fn absolute_roots_do_not_require_a_scalar_base_directory() {
    use std::os::unix::ffi::OsStringExt;
    let base = std::path::PathBuf::from(std::ffi::OsString::from_vec(b"/owner/\xff".to_vec()));
    let mut workspace = SandboxWorkspaceWrite {
        writable_roots: vec!["/absolute/../target".into()],
        ..SandboxWorkspaceWrite::default()
    };
    workspace.normalize_writable_roots(Some(&base)).unwrap();
    assert_eq!(
        workspace.writable_roots,
        vec![RuntimeString::from("/target")]
    );
    workspace.writable_roots = vec!["relative".into()];
    assert!(workspace.normalize_writable_roots(Some(&base)).is_err());
}

#[cfg(windows)]
#[test]
fn windows_workspace_roots_use_explicit_prefixes_without_drive_cwd_lookup() {
    let mut workspace = SandboxWorkspaceWrite {
        writable_roots: vec![
            r"D:child\file".into(),
            r"\rooted".into(),
            r"\\?\C:\base\..\target".into(),
        ],
        ..SandboxWorkspaceWrite::default()
    };
    workspace
        .normalize_writable_roots(Some(std::path::Path::new(r"C:\owner\config")))
        .unwrap();
    assert_eq!(
        workspace.writable_roots,
        vec![
            RuntimeString::from(r"D:\owner\config\child\file"),
            RuntimeString::from(r"C:\rooted"),
            RuntimeString::from(r"C:\target"),
        ]
    );
}

#[test]
fn windows_configuration_is_optional_and_uses_exact_field_and_enum_names() {
    use crate::schemas::config::{WindowsConfig, WindowsSandboxMode};
    assert!(EngineConfig::default().windows.is_none());
    assert!(resolve_config(json!({})).unwrap().windows.is_none());
    for (mode, name) in [
        (WindowsSandboxMode::Elevated, "elevated"),
        (WindowsSandboxMode::Unelevated, "unelevated"),
        (WindowsSandboxMode::Mxc, "mxc"),
    ] {
        let expected = WindowsConfig {
            sandbox: Some(mode),
            allow_mxc: Some(true),
        };
        let value = json!({"sandbox": name, "allow_mxc": true});
        assert_eq!(serde_json::to_value(&expected).unwrap(), value);
        let decoded = resolve_config(json!({"windows": value})).unwrap();
        assert_eq!(decoded.windows, Some(expected.clone()));
        let encoded = crate::runtime_values::runtime_codec::encode_value(&expected).unwrap();
        assert_eq!(
            crate::runtime_values::runtime_codec::decode_value::<WindowsConfig>(&encoded).unwrap(),
            expected
        );
    }
    assert_eq!(
        serde_json::from_value::<WindowsConfig>(json!({"future_extra": true})).unwrap(),
        WindowsConfig::default()
    );
    let unknown = RuntimeString::from(r#"{"future_extra":true}"#);
    assert_eq!(
        crate::runtime_values::runtime_codec::decode_text::<WindowsConfig>(&unknown).unwrap(),
        WindowsConfig::default()
    );
}

#[test]
fn deprecated_windows_flags_remain_sparse_and_keep_explicit_false() {
    use crate::schemas::config::WindowsSandboxMode;
    for (key, expected) in [
        (
            "experimental_windows_sandbox",
            WindowsSandboxMode::Unelevated,
        ),
        (
            "enable_experimental_windows_sandbox",
            WindowsSandboxMode::Unelevated,
        ),
        ("elevated_windows_sandbox", WindowsSandboxMode::Elevated),
    ] {
        for enabled in [false, true] {
            let mut features = Map::new();
            features.insert(key.into(), json!(enabled));
            let raw = json!({"features": features});
            let (values, warnings) = normalize(raw.clone());
            assert_eq!(Value::Object(values.clone()), raw);
            assert!(!values.contains_key("windows"));
            assert_eq!(warnings[0].code, "deprecatedSandboxFeature");
            assert_eq!(warnings[0].keys, vec![format!("features.{key}")]);
            let config = resolve_config(Value::Object(values)).unwrap();
            assert!(config.windows.is_none());
            assert_eq!(
                crate::sandbox::effective_windows_config(&config)
                    .unwrap()
                    .and_then(|windows| windows.sandbox),
                enabled.then_some(expected)
            );
        }
    }
    let (empty, warnings) = normalize(json!({}));
    assert!(!empty.contains_key("windows"));
    assert!(warnings.is_empty());
}

#[test]
fn explicit_windows_mode_wins_and_unimplemented_feature_data_is_retained_with_notice() {
    use crate::schemas::config::{WindowsConfig, WindowsSandboxMode};
    let (values, warnings) = normalize(json!({
        "windows": {"sandbox": "mxc", "allow_mxc": true, "future_extra": "retained"},
        "features": {"experimental_windows_sandbox": true, "elevated_windows_sandbox": true, "unrelated_entry": {"raw": "retained"}}
    }));
    assert_eq!(values["windows"]["sandbox"], json!("mxc"));
    assert_eq!(values["windows"]["allow_mxc"], json!(true));
    assert_eq!(values["windows"]["future_extra"], json!("retained"));
    assert_eq!(
        values["features"],
        json!({"experimental_windows_sandbox": true, "elevated_windows_sandbox": true, "unrelated_entry": {"raw": "retained"}})
    );
    assert!(warnings
        .iter()
        .any(|warning| warning.code == "unsupportedConfigFeatures"
            && warning.keys == vec!["features.unrelated_entry".to_string()]));
    let config = resolve_config(Value::Object(values)).unwrap();
    assert_eq!(
        config.windows,
        Some(WindowsConfig {
            sandbox: Some(WindowsSandboxMode::Mxc),
            allow_mxc: Some(true)
        })
    );
    let (values, _) = normalize(
        json!({"features": {"experimental_windows_sandbox": true, "elevated_windows_sandbox": true}}),
    );
    assert!(!values.contains_key("windows"));
    let config = resolve_config(Value::Object(values)).unwrap();
    assert_eq!(
        crate::sandbox::effective_windows_config(&config)
            .unwrap()
            .unwrap()
            .sandbox,
        Some(WindowsSandboxMode::Elevated)
    );
}

#[test]
fn explicit_mxc_disallow_conflict_is_rejected_for_every_filesystem_mode() {
    use crate::schemas::config::{WindowsConfig, WindowsSandboxMode};
    let message = "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false";
    for mode in ["read-only", "workspace-write", "danger-full-access"] {
        assert_eq!(
            resolve_config(json!({
                "sandbox_mode": mode,
                "windows": {"sandbox": "mxc", "allow_mxc": false},
                "features": {"prefer_mxc": true}
            }))
            .unwrap_err(),
            message
        );
    }
    let config = EngineConfig {
        windows: Some(WindowsConfig {
            sandbox: Some(WindowsSandboxMode::Mxc),
            allow_mxc: Some(false),
        }),
        ..EngineConfig::default()
    };
    assert_eq!(config.validate().unwrap_err(), message);
    for mode in ["elevated", "unelevated"] {
        assert!(resolve_config(json!({
            "windows": {"sandbox": mode, "allow_mxc": false},
            "features": {"prefer_mxc": true}
        }))
        .is_ok());
    }
}

#[test]
fn rollout_maintenance_features_are_opt_in_and_survive_governance_and_runtime_codec() {
    let defaults = resolve_config(json!({})).unwrap();
    assert!(!defaults.features.cold_rollout_compression);
    assert!(!defaults.features.background_rollout_migration);
    let enabled = resolve_config(json!({"features": {"cold_rollout_compression": true, "background_rollout_migration": true}})).unwrap();
    assert!(enabled.features.cold_rollout_compression);
    assert!(enabled.features.background_rollout_migration);
    let encoded = crate::runtime_values::runtime_codec::encode_value(&enabled.features).unwrap();
    assert_eq!(
        crate::runtime_values::runtime_codec::decode_value::<
            crate::schemas::config::SandboxFeatureConfig,
        >(&encoded)
        .unwrap(),
        enabled.features
    );
    assert!(resolve_config(json!({"features": {"cold_rollout_compression": "true"}})).is_err());
}

#[test]
fn supported_sandbox_preference_has_exact_default_and_runtime_codec() {
    use crate::schemas::config::SandboxFeatureConfig;
    assert!(!EngineConfig::default().features.prefer_mxc);
    let empty = resolve_config(json!({})).unwrap();
    assert!(!empty.features.prefer_mxc);
    assert!(empty.windows.is_none());
    let expected = SandboxFeatureConfig {
        prefer_mxc: true,
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&expected).unwrap(),
        json!({"cold_rollout_compression": false, "background_rollout_migration": false, "prefer_mxc": true, "windows_sandbox_service": false, "experimental_windows_sandbox": false, "enable_experimental_windows_sandbox": false, "elevated_windows_sandbox": false, "use_legacy_landlock": false})
    );
    let encoded = crate::runtime_values::runtime_codec::encode_value(&expected).unwrap();
    assert_eq!(
        crate::runtime_values::runtime_codec::decode_value::<SandboxFeatureConfig>(&encoded)
            .unwrap(),
        expected
    );
    let unknown = RuntimeString::from(r#"{"future_extra":true}"#);
    assert_eq!(
        crate::runtime_values::runtime_codec::decode_text::<SandboxFeatureConfig>(&unknown)
            .unwrap(),
        SandboxFeatureConfig::default()
    );
    assert!(resolve_config(json!({"features": {"prefer_mxc": "true"}})).is_err());
}

#[test]
fn supported_preference_is_active_while_other_feature_entries_remain_raw_with_notice() {
    let (values, warnings) = normalize(json!({"features": {
        "prefer_mxc": true,
        "elevated_windows_sandbox": true,
        "unrelated_entry": {"raw": "retained"}
    }}));
    assert_eq!(values["features"]["prefer_mxc"], json!(true));
    assert!(warnings
        .iter()
        .any(|warning| warning.code == "unsupportedConfigFeatures"
            && warning.keys == vec!["features.unrelated_entry".to_string()]));
    assert!(!warnings
        .iter()
        .any(|warning| warning.keys.contains(&"features.prefer_mxc".to_string())));
    let layer =
        config_layer_from_value(ConfigGovernanceSource::User, Value::Object(values)).unwrap();
    assert_eq!(
        layer.values["features"],
        json!({"prefer_mxc": true, "elevated_windows_sandbox": true})
    );
    let project = config_layer_from_value(
        ConfigGovernanceSource::Project,
        json!({"features": {"unrelated_entry": false}}),
    )
    .unwrap();
    assert!(!project.values.contains_key("features"));
    assert!(
        resolve_config_layers(vec![layer.clone(), project])
            .unwrap()
            .features
            .prefer_mxc
    );
    let project = config_layer_from_value(
        ConfigGovernanceSource::Project,
        json!({"features": {"prefer_mxc": false}}),
    )
    .unwrap();
    assert!(
        !resolve_config_layers(vec![layer, project])
            .unwrap()
            .features
            .prefer_mxc
    );
    let (_, warnings) = normalize(json!({"features": {"prefer_mxc": true}}));
    assert!(warnings.is_empty());
}

#[test]
fn legacy_landlock_preference_is_typed_and_deprecated_for_either_explicit_value() {
    use crate::schemas::config::SandboxFeatureConfig;
    assert!(!EngineConfig::default().features.use_legacy_landlock);
    for enabled in [false, true] {
        let raw = json!({"features": {
            "prefer_mxc": true,
            "use_legacy_landlock": enabled,
            "use_linux_sandbox_bwrap": true
        }});
        let (values, warnings) = normalize(raw.clone());
        assert_eq!(Value::Object(values.clone()), raw);
        assert!(warnings
            .iter()
            .any(|warning| warning.code == "deprecatedSandboxFeature"
                && warning.keys == vec!["features.use_legacy_landlock".to_string()]));
        assert!(warnings
            .iter()
            .any(|warning| warning.code == "unsupportedConfigFeatures"
                && warning.keys == vec!["features.use_linux_sandbox_bwrap".to_string()]));
        let layer =
            config_layer_from_value(ConfigGovernanceSource::User, Value::Object(values)).unwrap();
        assert_eq!(
            layer.values["features"],
            json!({"prefer_mxc": true, "use_legacy_landlock": enabled})
        );
        let config = resolve_config_layers(vec![layer]).unwrap();
        let expected = SandboxFeatureConfig {
            prefer_mxc: true,
            use_legacy_landlock: enabled,
            ..Default::default()
        };
        assert_eq!(config.features, expected);
        let encoded = crate::runtime_values::runtime_codec::encode_value(&expected).unwrap();
        assert_eq!(
            crate::runtime_values::runtime_codec::decode_value::<SandboxFeatureConfig>(&encoded)
                .unwrap(),
            expected
        );
    }
    assert!(resolve_config(json!({"features": {"use_legacy_landlock": "true"}})).is_err());
    let (_, warnings) = normalize(json!({"features": {"prefer_mxc": true}}));
    assert!(warnings.is_empty());
}
