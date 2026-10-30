use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// Decode a historical preset at an input boundary. Runtime decisions use only
/// the canonical policy fields; planning is migrated independently.
pub fn legacy_permission_policy(
    mode: crate::types::permission::ExternalPermissionMode,
) -> Option<(
    crate::types::permission::ApprovalPolicy,
    crate::types::permission::SandboxMode,
    crate::types::permission::ApprovalsReviewer,
)> {
    use crate::types::permission::{
        ApprovalPolicy, ApprovalsReviewer, ExternalPermissionMode, SandboxMode,
    };
    match mode {
        ExternalPermissionMode::BypassPermissions => Some((
            ApprovalPolicy::Never,
            SandboxMode::DangerFullAccess,
            ApprovalsReviewer::User,
        )),
        ExternalPermissionMode::Auto => Some((
            ApprovalPolicy::OnRequest,
            SandboxMode::WorkspaceWrite,
            ApprovalsReviewer::Automatic,
        )),
        ExternalPermissionMode::AcceptEdits => Some((
            ApprovalPolicy::OnRequest,
            SandboxMode::WorkspaceWrite,
            ApprovalsReviewer::User,
        )),
        ExternalPermissionMode::DontAsk => Some((
            ApprovalPolicy::Never,
            SandboxMode::ReadOnly,
            ApprovalsReviewer::User,
        )),
        ExternalPermissionMode::Default | ExternalPermissionMode::Plan => None,
    }
}

const LEGACY_WINDOWS_SANDBOX_FEATURE_KEYS: [&str; 3] = [
    "experimental_windows_sandbox",
    "enable_experimental_windows_sandbox",
    "elevated_windows_sandbox",
];

pub(crate) const SANDBOX_FEATURE_KEYS: &[&str] = &[
    "cold_rollout_compression",
    "background_rollout_migration",
    "prefer_mxc",
    "windows_sandbox_service",
    "experimental_windows_sandbox",
    "enable_experimental_windows_sandbox",
    "elevated_windows_sandbox",
    "use_legacy_landlock",
];

/// An explicit compatibility or deprecation notice for a configuration layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigMigrationWarning {
    pub code: String,
    pub message: String,
    pub keys: Vec<String>,
}

impl ConfigMigrationWarning {
    fn new(message: &str, keys: Vec<String>) -> Self {
        Self {
            code: "legacySandboxConfigMigrated".into(),
            message: message.into(),
            keys,
        }
    }
}

/// Normalize one raw layer before governance, without importing values from another layer.
/// Canonical policy and workspace fields always take precedence over legacy inputs.
pub fn normalize_sandbox_config(
    values: &mut Map<String, Value>,
) -> Result<Vec<ConfigMigrationWarning>, String> {
    let mut warnings = Vec::new();
    let mut aliases = Vec::new();
    for (old, canonical) in [
        ("approvalPolicy", "approval_policy"),
        ("approvalsReviewer", "approvals_reviewer"),
        ("sandboxMode", "sandbox_mode"),
        ("sandboxWorkspaceWrite", "sandbox_workspace_write"),
        ("permission_mode", "permissionMode"),
    ] {
        if let Some(value) = values.remove(old) {
            values.entry(canonical).or_insert(value);
            aliases.push(old.to_string());
        }
    }
    if let Some(workspace) = values.get_mut("sandbox_workspace_write") {
        if let Some(workspace) = workspace.as_object_mut() {
            for (old, canonical) in [
                ("writableRoots", "writable_roots"),
                ("networkAccess", "network_access"),
                ("excludeTmpdirEnvVar", "exclude_tmpdir_env_var"),
                ("excludeSlashTmp", "exclude_slash_tmp"),
            ] {
                if let Some(value) = workspace.remove(old) {
                    workspace.entry(canonical).or_insert(value);
                    aliases.push(format!("sandbox_workspace_write.{old}"));
                }
            }
        }
    }
    if !aliases.is_empty() {
        warnings.push(ConfigMigrationWarning::new(
            "Legacy permission and sandbox key spellings were converted to canonical names; existing canonical values were retained.",
            aliases,
        ));
    }
    for key in ["approval_policy", "approvals_reviewer", "sandbox_mode"] {
        if values.get(key).is_some_and(Value::is_null) {
            values.remove(key);
        }
    }
    for (key, legacy, canonical) in [
        ("approval_policy", "on-failure", "on-request"),
        ("approvals_reviewer", "guardian_subagent", "auto_review"),
        ("approvals_reviewer", "automatic", "auto_review"),
    ] {
        if values.get(key).and_then(Value::as_str) == Some(legacy) {
            values.insert(key.into(), json!(canonical));
            warnings.push(ConfigMigrationWarning::new(
                "A legacy permission policy value was converted to its canonical value.",
                vec![key.into()],
            ));
        }
    }
    let explicit_policy = ["approval_policy", "approvals_reviewer", "sandbox_mode"]
        .iter()
        .any(|key| values.contains_key(*key));
    let legacy_mode = values
        .get("permissionMode")
        .and_then(Value::as_str)
        .filter(|mode| ["auto", "acceptEdits", "bypassPermissions", "dontAsk"].contains(mode))
        .map(str::to_owned);
    if let Some(mode) = legacy_mode {
        if !explicit_policy {
            let (approval, sandbox, reviewer) = match mode.as_str() {
                "bypassPermissions" => ("never", "danger-full-access", "user"),
                "auto" => ("on-request", "workspace-write", "auto_review"),
                "acceptEdits" => ("on-request", "workspace-write", "user"),
                _ => ("never", "read-only", "user"),
            };
            values.insert("approval_policy".into(), json!(approval));
            values.insert("sandbox_mode".into(), json!(sandbox));
            values.insert("approvals_reviewer".into(), json!(reviewer));
        }
        // Plan and task state remain separate; retired presets cannot override policy.
        values.insert("permissionMode".into(), json!("default"));
        warnings.push(ConfigMigrationWarning::new(
            "The legacy permission preset was replaced by explicit policy fields when no policy was configured; Plan and task state were preserved.",
            vec!["permissionMode".into()],
        ));
    }
    if let Some(backend) = values.remove("sandbox") {
        let backend = backend
            .as_str()
            .ok_or_else(|| "legacy sandbox backend must be a string".to_string())?;
        if ![
            "process",
            "auto",
            "seatbelt",
            "bubblewrap",
            "native-helper",
            "windows-helper",
            "windows-restricted-token",
        ]
        .contains(&backend)
        {
            return Err(format!("unknown legacy sandbox backend '{backend}'"));
        }
        values.entry("sandbox_mode").or_insert_with(|| {
            json!(if backend == "process" {
                "danger-full-access"
            } else {
                "workspace-write"
            })
        });
        warnings.push(ConfigMigrationWarning::new(
            "The legacy sandbox backend was converted to a sandbox mode; platform backend selection is no longer configurable.",
            vec!["sandbox".into()],
        ));
    }
    let mut workspace_keys = Vec::new();
    for (old, canonical, array) in [
        ("additionalWritableRoots", "writable_roots", true),
        ("additional_writable_roots", "writable_roots", true),
        ("sandboxNetworkAllowed", "network_access", false),
        ("sandbox_network_allowed", "network_access", false),
    ] {
        if let Some(value) = values.remove(old) {
            if (array
                && !value
                    .as_array()
                    .is_some_and(|items| items.iter().all(Value::is_string)))
                || (!array && !value.is_boolean())
            {
                return Err(format!("legacy sandbox key '{old}' has an invalid value"));
            }
            let workspace = values
                .entry("sandbox_workspace_write")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(|| "sandbox_workspace_write must be an object".to_string())?;
            workspace.entry(canonical).or_insert(value);
            workspace_keys.push(old.to_string());
        }
    }
    if !workspace_keys.is_empty() {
        warnings.push(ConfigMigrationWarning::new(
            "Legacy writable roots and network access were moved into sandbox_workspace_write; existing workspace policy values were retained.",
            workspace_keys,
        ));
    }
    let mut retired = Vec::new();
    for key in [
        "requireRealSandbox",
        "require_real_sandbox",
        "sandboxRetryOnDenial",
        "sandbox_retry_on_denial",
        "confineReads",
        "confine_reads",
        "additionalReadableRoots",
        "additional_readable_roots",
    ] {
        if values.remove(key).is_some() {
            retired.push(key.to_string());
        }
    }
    if !retired.is_empty() {
        warnings.push(ConfigMigrationWarning::new(
            "Retired sandbox fallback, retry and read-confinement controls were removed; sandbox_mode now defines the execution boundary.",
            retired,
        ));
    }
    warnings.extend(normalize_windows_config(values)?);
    Ok(warnings)
}

fn normalize_windows_config(
    values: &mut Map<String, Value>,
) -> Result<Vec<ConfigMigrationWarning>, String> {
    let mut warnings = Vec::new();
    if values.get("windows").is_some_and(Value::is_null) {
        values.remove("windows");
    }
    if let Some(features) = values.get("features").and_then(Value::as_object) {
        let mut deprecated = Vec::new();
        for key in LEGACY_WINDOWS_SANDBOX_FEATURE_KEYS {
            if let Some(value) = features.get(key) {
                if !value.is_boolean() {
                    return Err(format!(
                        "Windows sandbox feature 'features.{key}' must be a boolean"
                    ));
                }
                deprecated.push(format!("features.{key}"));
            }
        }
        if !deprecated.is_empty() {
            warnings.push(ConfigMigrationWarning {
                code: "deprecatedSandboxFeature".into(),
                message: "Windows sandbox feature flags are deprecated; configure windows.sandbox for an explicit executor mode. Their values remain active after configuration layers are merged.".into(),
                keys: deprecated,
            });
        }
    }
    if let Some(features) = values.get("features") {
        if features
            .as_object()
            .is_some_and(|features| features.contains_key("use_legacy_landlock"))
        {
            warnings.push(ConfigMigrationWarning {
                code: "deprecatedSandboxFeature".into(),
                message: "features.use_legacy_landlock is deprecated; omit it to use the current Linux sandbox executor.".into(),
                keys: vec!["features.use_legacy_landlock".into()],
            });
        }
        let keys = match features.as_object() {
            Some(features) => features
                .keys()
                .filter(|key| !SANDBOX_FEATURE_KEYS.contains(&key.as_str()))
                .map(|key| format!("features.{key}"))
                .collect::<Vec<_>>(),
            None => vec!["features".into()],
        };
        if !keys.is_empty() {
            warnings.push(ConfigMigrationWarning {
                code: "unsupportedConfigFeatures".into(),
                message: "Feature entries were retained as raw configuration data and are not implemented or active in this engine.".into(),
                keys,
            });
        }
    }
    Ok(warnings)
}

/// Project the implemented sandbox preferences into an engine-owned config layer.
/// Other feature entries remain available in the raw configuration document.
pub(crate) fn retain_supported_sandbox_features(values: &mut Map<String, Value>) {
    let Some(features) = values.get_mut("features").and_then(Value::as_object_mut) else {
        return;
    };
    features.retain(|key, _| SANDBOX_FEATURE_KEYS.contains(&key.as_str()));
    if features.is_empty() {
        values.remove("features");
    }
}

#[cfg(test)]
#[path = "config_migration_tests.rs"]
mod tests;
