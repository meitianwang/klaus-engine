//! Pure config layering, schema validation, and managed lockout policy.

use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConfigGovernanceSource {
    Defaults,
    System,
    User,
    Project,
    Env,
    SessionFlags,
    Managed,
}

impl ConfigGovernanceSource {
    pub fn precedence(self) -> i16 {
        match self {
            Self::Defaults => 0,
            Self::System => 10,
            Self::User => 20,
            Self::Project => 25,
            Self::Env => 28,
            Self::SessionFlags => 30,
            Self::Managed => 50,
        }
    }

    /// Stable name for the wire, so a host can say WHERE a value came from
    /// without mirroring this enum's Rust spelling.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::System => "system",
            Self::User => "user",
            Self::Project => "project",
            Self::Env => "env",
            Self::SessionFlags => "sessionFlags",
            Self::Managed => "managed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigValueKind {
    Bool,
    Integer,
    Number,
    String,
    Array,
    Object,
    Any,
}

impl ConfigValueKind {
    fn matches(self, value: &Value) -> bool {
        match self {
            Self::Bool => value.is_boolean(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Number => value.is_number(),
            Self::String => value.is_string(),
            Self::Array => value.is_array(),
            Self::Object => value.is_object(),
            Self::Any => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigKeySpec {
    pub key: String,
    pub kind: ConfigValueKind,
    pub default_value: Option<Value>,
    pub managed_lockable: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigGovernanceSchema {
    keys: BTreeMap<String, ConfigKeySpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigGovernanceLayer {
    pub source: ConfigGovernanceSource,
    pub values: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigEffectiveEntry {
    pub value: Value,
    pub source: ConfigGovernanceSource,
    pub managed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigGovernanceDiagnostic {
    ManagedLockout {
        key: String,
        attempted_source: ConfigGovernanceSource,
        managed_source: ConfigGovernanceSource,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigGovernanceViolation {
    UnknownKey {
        key: String,
        source: ConfigGovernanceSource,
    },
    InvalidValue {
        key: String,
        source: ConfigGovernanceSource,
        expected: ConfigValueKind,
        actual: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigGovernanceError {
    pub violations: Vec<ConfigGovernanceViolation>,
}

/// The violations spelled out, in one place: every caller that turns a rejected
/// config into a message for a user (session start, a config write) has to say
/// the same thing about the same key.
impl std::fmt::Display for ConfigGovernanceViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKey { key, source } => {
                write!(f, "unknown engine config key '{key}' from {source:?}")
            }
            Self::InvalidValue {
                key,
                source,
                expected,
                actual,
            } => write!(
                f,
                "invalid engine config value for '{key}' from {source:?}: expected {expected:?}, got {actual}"
            ),
        }
    }
}

impl std::fmt::Display for ConfigGovernanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = self
            .violations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        f.write_str(&text)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GovernedConfig {
    entries: BTreeMap<String, ConfigEffectiveEntry>,
    diagnostics: Vec<ConfigGovernanceDiagnostic>,
    sandbox_field_sources: BTreeMap<String, ConfigGovernanceSource>,
}

impl ConfigKeySpec {
    pub fn new(key: impl Into<String>, kind: ConfigValueKind) -> Self {
        Self {
            key: key.into(),
            kind,
            default_value: None,
            managed_lockable: false,
        }
    }

    pub fn with_default(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn managed_lockable(mut self) -> Self {
        self.managed_lockable = true;
        self
    }
}

impl ConfigGovernanceSchema {
    pub fn new(keys: impl IntoIterator<Item = ConfigKeySpec>) -> Self {
        Self {
            keys: keys
                .into_iter()
                .map(|spec| (spec.key.clone(), spec))
                .collect(),
        }
    }

    pub fn spec(&self, key: &str) -> Option<&ConfigKeySpec> {
        self.keys.get(key)
    }
}

impl ConfigGovernanceLayer {
    pub fn new(
        source: ConfigGovernanceSource,
        values: impl IntoIterator<Item = (impl Into<String>, Value)>,
    ) -> Self {
        Self {
            source,
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        }
    }
}

impl GovernedConfig {
    pub fn get(&self, key: &str) -> Option<&ConfigEffectiveEntry> {
        self.entries.get(key)
    }

    /// Every resolved key with the layer that won it — what a settings page reads
    /// to mark a value it cannot change from where it is standing.
    pub fn entries(&self) -> impl Iterator<Item = (&String, &ConfigEffectiveEntry)> {
        self.entries.iter()
    }

    pub fn to_json_object(&self) -> serde_json::Map<String, Value> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    pub fn diagnostics(&self) -> &[ConfigGovernanceDiagnostic] {
        &self.diagnostics
    }

    /// Precise read origins for fields merged inside the supported sandbox tables.
    pub fn sandbox_field_sources(&self) -> impl Iterator<Item = (&String, ConfigGovernanceSource)> {
        self.sandbox_field_sources
            .iter()
            .map(|(key, source)| (key, *source))
    }
}

fn sandbox_table_fields(key: &str) -> Option<&'static [&'static str]> {
    match key {
        "features" => Some(crate::schemas::config::SANDBOX_FEATURE_KEYS),
        "windows" => Some(&["sandbox", "allow_mxc"]),
        "sandbox_workspace_write" => Some(&[
            "writable_roots",
            "network_access",
            "exclude_tmpdir_env_var",
            "exclude_slash_tmp",
        ]),
        _ => None,
    }
}

pub fn govern_config(
    schema: &ConfigGovernanceSchema,
    layers: &[ConfigGovernanceLayer],
) -> Result<GovernedConfig, ConfigGovernanceError> {
    let mut violations = Vec::new();

    for spec in schema.keys.values() {
        if let Some(default_value) = &spec.default_value {
            validate_value(
                spec,
                default_value,
                ConfigGovernanceSource::Defaults,
                &mut violations,
            );
        }
    }

    for layer in layers {
        for (key, value) in &layer.values {
            let Some(spec) = schema.spec(key) else {
                violations.push(ConfigGovernanceViolation::UnknownKey {
                    key: key.clone(),
                    source: layer.source,
                });
                continue;
            };
            validate_value(spec, value, layer.source, &mut violations);
        }
    }

    if !violations.is_empty() {
        return Err(ConfigGovernanceError { violations });
    }

    let mut entries = BTreeMap::new();
    let mut diagnostics = Vec::new();
    let mut managed_values = BTreeMap::new();
    let mut sandbox_field_sources = BTreeMap::new();

    for layer in layers {
        if layer.source != ConfigGovernanceSource::Managed {
            continue;
        }
        for (key, value) in &layer.values {
            managed_values.insert(key.clone(), value.clone());
        }
    }

    for spec in schema.keys.values() {
        if let Some(default_value) = &spec.default_value {
            if let (Some(fields), Some(values)) =
                (sandbox_table_fields(&spec.key), default_value.as_object())
            {
                for field in fields.iter().filter(|field| values.contains_key(**field)) {
                    sandbox_field_sources.insert(
                        format!("{}.{field}", spec.key),
                        ConfigGovernanceSource::Defaults,
                    );
                }
            }
            entries.insert(
                spec.key.clone(),
                ConfigEffectiveEntry {
                    value: default_value.clone(),
                    source: ConfigGovernanceSource::Defaults,
                    managed: false,
                },
            );
        }
    }

    let mut ordered_layers: Vec<(usize, &ConfigGovernanceLayer)> =
        layers.iter().enumerate().collect();
    ordered_layers.sort_by_key(|(index, layer)| (layer.source.precedence(), *index));

    for (_, layer) in ordered_layers {
        for (key, value) in &layer.values {
            let spec = schema
                .spec(key)
                .expect("layers were already validated against schema");
            if layer.source != ConfigGovernanceSource::Managed
                && spec.managed_lockable
                && managed_values
                    .get(key)
                    .is_some_and(|managed_value| managed_value != value)
            {
                diagnostics.push(ConfigGovernanceDiagnostic::ManagedLockout {
                    key: key.clone(),
                    attempted_source: layer.source,
                    managed_source: ConfigGovernanceSource::Managed,
                });
                continue;
            }

            let value = if let (Some(fields), Some(overlay)) =
                (sandbox_table_fields(key), value.as_object())
            {
                let mut merged = entries
                    .get(key)
                    .and_then(|entry| entry.value.as_object())
                    .cloned()
                    .unwrap_or_default();
                for (field, value) in overlay {
                    merged.insert(field.clone(), value.clone());
                    if fields.contains(&field.as_str()) {
                        sandbox_field_sources.insert(format!("{key}.{field}"), layer.source);
                    }
                }
                Value::Object(merged)
            } else {
                value.clone()
            };
            entries.insert(
                key.clone(),
                ConfigEffectiveEntry {
                    value,
                    source: layer.source,
                    managed: layer.source == ConfigGovernanceSource::Managed,
                },
            );
        }
    }

    Ok(GovernedConfig {
        entries,
        diagnostics,
        sandbox_field_sources,
    })
}

fn validate_value(
    spec: &ConfigKeySpec,
    value: &Value,
    source: ConfigGovernanceSource,
    violations: &mut Vec<ConfigGovernanceViolation>,
) {
    if !spec.kind.matches(value) {
        violations.push(ConfigGovernanceViolation::InvalidValue {
            key: spec.key.clone(),
            source,
            expected: spec.kind,
            actual: value_kind_name(value),
        });
    }
}

fn value_kind_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_schema() -> ConfigGovernanceSchema {
        ConfigGovernanceSchema::new([
            ConfigKeySpec::new("model", ConfigValueKind::String)
                .with_default(json!("claude-sonnet-4-6")),
            ConfigKeySpec::new("maxOutputTokens", ConfigValueKind::Integer)
                .with_default(json!(8192)),
            ConfigKeySpec::new("workflow", ConfigValueKind::Bool).with_default(json!(true)),
            ConfigKeySpec::new("permissionMode", ConfigValueKind::String)
                .with_default(json!("default"))
                .managed_lockable(),
        ])
    }

    fn entry<'a>(
        config: &'a GovernedConfig,
        key: &str,
    ) -> (&'a Value, ConfigGovernanceSource, bool) {
        let entry = config.get(key).expect("entry exists");
        (&entry.value, entry.source, entry.managed)
    }

    #[test]
    fn defaults_fill_missing_keys() {
        let governed = govern_config(&sample_schema(), &[]).expect("config resolves");

        assert_eq!(
            entry(&governed, "model"),
            (
                &json!("claude-sonnet-4-6"),
                ConfigGovernanceSource::Defaults,
                false,
            )
        );
        assert_eq!(
            entry(&governed, "maxOutputTokens"),
            (&json!(8192), ConfigGovernanceSource::Defaults, false)
        );
    }

    #[test]
    fn project_overrides_defaults() {
        let governed = govern_config(
            &sample_schema(),
            &[ConfigGovernanceLayer::new(
                ConfigGovernanceSource::Project,
                [("model", json!("project-model"))],
            )],
        )
        .expect("config resolves");

        assert_eq!(
            entry(&governed, "model"),
            (
                &json!("project-model"),
                ConfigGovernanceSource::Project,
                false,
            )
        );
    }

    #[test]
    fn env_overrides_project() {
        let governed = govern_config(
            &sample_schema(),
            &[
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::Env,
                    [("model", json!("env-model"))],
                ),
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::Project,
                    [("model", json!("project-model"))],
                ),
            ],
        )
        .expect("config resolves");

        assert_eq!(
            entry(&governed, "model"),
            (&json!("env-model"), ConfigGovernanceSource::Env, false)
        );
    }

    #[test]
    fn managed_values_lock_out_lower_sources() {
        let governed = govern_config(
            &sample_schema(),
            &[
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::Project,
                    [("permissionMode", json!("dangerous"))],
                ),
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::Managed,
                    [("permissionMode", json!("ask"))],
                ),
            ],
        )
        .expect("config resolves");

        assert_eq!(
            entry(&governed, "permissionMode"),
            (&json!("ask"), ConfigGovernanceSource::Managed, true)
        );
        assert_eq!(
            governed.diagnostics(),
            &[ConfigGovernanceDiagnostic::ManagedLockout {
                key: "permissionMode".to_string(),
                attempted_source: ConfigGovernanceSource::Project,
                managed_source: ConfigGovernanceSource::Managed,
            }]
        );
    }

    #[test]
    fn sandbox_table_fields_merge_only_after_existing_managed_lockout() {
        let schema = crate::schemas::config::engine_config_governance_schema();
        let governed = govern_config(
            &schema,
            &[
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::User,
                    [("features", json!({"elevated_windows_sandbox": true}))],
                ),
                ConfigGovernanceLayer::new(
                    ConfigGovernanceSource::Managed,
                    [("features", json!({"experimental_windows_sandbox": false}))],
                ),
            ],
        )
        .unwrap();
        assert_eq!(
            governed.get("features").unwrap().value["elevated_windows_sandbox"],
            json!(false)
        );
        assert_eq!(
            governed.diagnostics(),
            &[ConfigGovernanceDiagnostic::ManagedLockout {
                key: "features".into(),
                attempted_source: ConfigGovernanceSource::User,
                managed_source: ConfigGovernanceSource::Managed
            }]
        );
        let sources: BTreeMap<_, _> = governed
            .sandbox_field_sources()
            .map(|(key, source)| (key.clone(), source))
            .collect();
        assert_eq!(
            sources.get("features.elevated_windows_sandbox"),
            Some(&ConfigGovernanceSource::Defaults)
        );
        assert_eq!(
            sources.get("features.experimental_windows_sandbox"),
            Some(&ConfigGovernanceSource::Managed)
        );
    }

    #[test]
    fn invalid_config_value_is_reported() {
        let err = govern_config(
            &sample_schema(),
            &[ConfigGovernanceLayer::new(
                ConfigGovernanceSource::Project,
                [("maxOutputTokens", json!("many"))],
            )],
        )
        .expect_err("invalid value should fail");

        assert_eq!(
            err.violations,
            vec![ConfigGovernanceViolation::InvalidValue {
                key: "maxOutputTokens".to_string(),
                source: ConfigGovernanceSource::Project,
                expected: ConfigValueKind::Integer,
                actual: "string",
            }]
        );
    }

    #[test]
    fn unknown_key_is_schema_drift() {
        let err = govern_config(
            &sample_schema(),
            &[ConfigGovernanceLayer::new(
                ConfigGovernanceSource::Project,
                [("madeUpFeature", json!(true))],
            )],
        )
        .expect_err("unknown key should fail");

        assert_eq!(
            err.violations,
            vec![ConfigGovernanceViolation::UnknownKey {
                key: "madeUpFeature".to_string(),
                source: ConfigGovernanceSource::Project,
            }]
        );
    }
}
