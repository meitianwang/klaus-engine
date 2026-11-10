//! Engine configuration schema with layered defaults.

use crate::config_governance::{ConfigGovernanceSchema, ConfigKeySpec, ConfigValueKind};
use crate::network_proxy::NetworkProxyConfig;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, ConfigJson, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode,
    RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::effort::EffortValue;
use crate::types::permission::ExternalPermissionMode;
use crate::types::provider::{OutputVerbosity, ReasoningSummary};
use serde::{Deserialize, Serialize};

mod scalar_runtime_string {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::runtime_values::RuntimeString;

    fn to_scalar<E: serde::ser::Error>(value: &RuntimeString) -> Result<String, E> {
        value.try_to_utf8().map_err(E::custom)
    }

    pub fn serialize<S>(value: &RuntimeString, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        to_scalar::<S::Error>(value)?.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<RuntimeString, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(RuntimeString::from)
    }

    pub mod option {
        use super::*;

        pub fn serialize<S>(value: &Option<RuntimeString>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .as_ref()
                .map(to_scalar::<S::Error>)
                .transpose()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<RuntimeString>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<String>::deserialize(deserializer).map(|value| value.map(RuntimeString::from))
        }
    }

    pub mod vec {
        use super::*;

        pub fn serialize<S>(value: &[RuntimeString], serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .iter()
                .map(to_scalar::<S::Error>)
                .collect::<Result<Vec<_>, _>>()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<RuntimeString>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Vec::<String>::deserialize(deserializer)
                .map(|values| values.into_iter().map(RuntimeString::from).collect())
        }
    }

    pub mod option_vec {
        use super::*;

        pub fn serialize<S>(
            value: &Option<Vec<RuntimeString>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            value
                .as_ref()
                .map(|values| {
                    values
                        .iter()
                        .map(to_scalar::<S::Error>)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<RuntimeString>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<Vec<String>>::deserialize(deserializer).map(|values| {
                values.map(|values| values.into_iter().map(RuntimeString::from).collect())
            })
        }
    }
}

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Layered config precedence — higher number wins on deep merge.
pub struct ConfigSource;

impl ConfigSource {
    pub const SYSTEM: u8 = 0;
    pub const ENTERPRISE_MANAGED: u8 = 15;
    pub const USER: u8 = 20;
    pub const PROJECT: u8 = 25;
    pub const SESSION_FLAGS: u8 = 30;
}

pub const DEFAULT_MODEL: &str = "claude-sonnet-4-6";

/// How much context the model is told it has left.
///
/// The model cannot see the window it is filling, so without a signal it writes
/// as if space were unbounded and only learns otherwise when the harness compacts
/// underneath it. A per-turn remaining-token line lets it budget its own output
/// and shed history it no longer needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TotalTokensReminderMode {
    /// Say nothing.
    Off,
    /// Report an unbounded budget. Used to measure how the model behaves when it
    /// believes it can never run out.
    Infinite,
    /// Report a fixed, large budget that never moves — the control arm against
    /// which a live countdown is compared.
    Fixed,
    /// Report the real remaining room: context window minus current occupancy.
    Countdown,
    /// Report a long-horizon budget that keeps descending across compactions:
    /// tokens dropped by a compaction stay spent, so the number never rewinds when
    /// history is summarized away.
    PaddedCountdown,
}

/// The active input counted against the automatic compaction limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AutoCompactScope {
    /// Count the complete active request, including its initial prefix.
    #[default]
    Total,
    /// Count only input added after the current context window was established.
    BodyAfterPrefix,
}

/// The budget reported under [`TotalTokensReminderMode::Fixed`].
pub const FIXED_TOTAL_TOKENS: u64 = 5_000_000;

/// Additional filesystem and network access for workspace-write sessions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SandboxWorkspaceWrite {
    #[serde(
        serialize_with = "scalar_runtime_string::vec::serialize",
        deserialize_with = "sandbox_workspace_paths::deserialize_writable_roots"
    )]
    pub writable_roots: Vec<RuntimeString>,
    pub network_access: bool,
    pub exclude_tmpdir_env_var: bool,
    pub exclude_slash_tmp: bool,
}

impl RuntimeEncode for SandboxWorkspaceWrite {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("writable_roots", &self.writable_roots)?;
        object.field("network_access", &self.network_access)?;
        object.field("exclude_tmpdir_env_var", &self.exclude_tmpdir_env_var)?;
        object.field("exclude_slash_tmp", &self.exclude_slash_tmp)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SandboxWorkspaceWrite {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let mut config = Self {
                writable_roots: object.defaulted("writable_roots", &[], Vec::new)?,
                network_access: object.defaulted("network_access", &[], || false)?,
                exclude_tmpdir_env_var: object
                    .defaulted("exclude_tmpdir_env_var", &[], || false)?,
                exclude_slash_tmp: object.defaulted("exclude_slash_tmp", &[], || false)?,
            };
            config.normalize_writable_roots(None).map_err(|error| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(error)))
            })?;
            Ok(config)
        })
    }
}

#[path = "config_migration.rs"]
mod config_migration;
pub use config_migration::{
    legacy_permission_policy, normalize_sandbox_config, ConfigMigrationWarning,
};
pub(crate) use config_migration::{retain_supported_sandbox_features, SANDBOX_FEATURE_KEYS};
#[path = "sandbox_workspace_paths.rs"]
mod sandbox_workspace_paths;
pub use sandbox_workspace_paths::normalize_sandbox_workspace_roots;

/// The configured Windows executor mode, independent of filesystem permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WindowsSandboxMode {
    Elevated,
    Unelevated,
    Mxc,
}

impl RuntimeEncode for WindowsSandboxMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Elevated => "elevated",
                Self::Unelevated => "unelevated",
                Self::Mxc => "mxc",
            },
        )
    }
}

impl RuntimeDecode for WindowsSandboxMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("elevated", &[]),
                RuntimeEnumVariant::new("unelevated", &[]),
                RuntimeEnumVariant::new("mxc", &[]),
            ],
            &[Self::Elevated, Self::Unelevated, Self::Mxc],
        )
    }
}

/// Platform-specific Windows execution settings. Omission does not select a mode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowsConfig {
    pub sandbox: Option<WindowsSandboxMode>,
    pub allow_mxc: Option<bool>,
}

impl RuntimeEncode for WindowsConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sandbox", &self.sandbox)?;
        object.field("allow_mxc", &self.allow_mxc)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WindowsConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                sandbox: object.optional("sandbox", &[])?,
                allow_mxc: object.optional("allow_mxc", &[])?,
            })
        })
    }
}

/// Supported feature preferences; endpoint maintenance and sandbox selection are opt-in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SandboxFeatureConfig {
    /// Automatically compress cold stored histories without delaying endpoint startup.
    pub cold_rollout_compression: bool,
    /// Automatically migrate eligible stored histories to the current paged format.
    pub background_rollout_migration: bool,
    pub prefer_mxc: bool,
    pub windows_sandbox_service: bool,
    /// Deprecated Windows selection flags remain sparse until layers are merged.
    pub experimental_windows_sandbox: bool,
    pub enable_experimental_windows_sandbox: bool,
    pub elevated_windows_sandbox: bool,
    /// Deprecated Linux execution preference; explicit configuration emits a warning.
    pub use_legacy_landlock: bool,
}

impl RuntimeEncode for SandboxFeatureConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("cold_rollout_compression", &self.cold_rollout_compression)?;
        object.field(
            "background_rollout_migration",
            &self.background_rollout_migration,
        )?;
        object.field("prefer_mxc", &self.prefer_mxc)?;
        object.field("windows_sandbox_service", &self.windows_sandbox_service)?;
        object.field(
            "experimental_windows_sandbox",
            &self.experimental_windows_sandbox,
        )?;
        object.field(
            "enable_experimental_windows_sandbox",
            &self.enable_experimental_windows_sandbox,
        )?;
        object.field("elevated_windows_sandbox", &self.elevated_windows_sandbox)?;
        object.field("use_legacy_landlock", &self.use_legacy_landlock)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SandboxFeatureConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                cold_rollout_compression: object.defaulted(
                    "cold_rollout_compression",
                    &[],
                    || false,
                )?,
                background_rollout_migration: object.defaulted(
                    "background_rollout_migration",
                    &[],
                    || false,
                )?,
                prefer_mxc: object.defaulted("prefer_mxc", &[], || false)?,
                windows_sandbox_service: object.defaulted(
                    "windows_sandbox_service",
                    &[],
                    || false,
                )?,
                experimental_windows_sandbox: object.defaulted(
                    "experimental_windows_sandbox",
                    &[],
                    || false,
                )?,
                enable_experimental_windows_sandbox: object.defaulted(
                    "enable_experimental_windows_sandbox",
                    &[],
                    || false,
                )?,
                elevated_windows_sandbox: object.defaulted(
                    "elevated_windows_sandbox",
                    &[],
                    || false,
                )?,
                use_legacy_landlock: object.defaulted("use_legacy_landlock", &[], || false)?,
            })
        })
    }
}

/// Tiered memory/instruction loading toggles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MemoryConfig {
    pub enabled: bool,
    pub user: bool,
    pub project: bool,
    pub local: bool,
    pub allow_external_includes: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig {
            enabled: true,
            user: true,
            project: true,
            local: true,
            allow_external_includes: false,
        }
    }
}

/// Web-search policy mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSearchMode {
    Auto,
    Native,
    Client,
    Off,
    Disabled,
    Indexed,
    Cached,
    Live,
}

// The legacy mode tags remain decodable; access is resolved independently.
impl Serialize for WebSearchMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let tag = match self {
            Self::Auto => "auto",
            Self::Native => "native",
            Self::Client => "client",
            Self::Off => "off",
            Self::Disabled => "disabled",
            Self::Indexed => "indexed",
            Self::Cached => "cached",
            Self::Live => "live",
        };
        serializer.serialize_str(tag)
    }
}

impl<'de> Deserialize<'de> for WebSearchMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "auto" => Ok(Self::Auto),
            "native" => Ok(Self::Native),
            "client" => Ok(Self::Client),
            "off" => Ok(Self::Off),
            "disabled" => Ok(Self::Disabled),
            "indexed" => Ok(Self::Indexed),
            "cached" => Ok(Self::Cached),
            "live" => Ok(Self::Live),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "auto", "native", "client", "off", "disabled", "indexed", "cached", "live",
                ],
            )),
        }
    }
}

impl RuntimeEncode for WebSearchMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Auto => "auto",
            Self::Native => "native",
            Self::Client => "client",
            Self::Off => "off",
            Self::Disabled => "disabled",
            Self::Indexed => "indexed",
            Self::Cached => "cached",
            Self::Live => "live",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for WebSearchMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("native", &[]),
                RuntimeEnumVariant::new("client", &[]),
                RuntimeEnumVariant::new("off", &[]),
                RuntimeEnumVariant::new("disabled", &[]),
                RuntimeEnumVariant::new("indexed", &[]),
                RuntimeEnumVariant::new("cached", &[]),
                RuntimeEnumVariant::new("live", &[]),
            ],
            &[
                Self::Auto,
                Self::Native,
                Self::Client,
                Self::Off,
                Self::Disabled,
                Self::Indexed,
                Self::Cached,
                Self::Live,
            ],
        )
    }
}

/// Network freshness policy, independent from the selected search source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebSearchAccess {
    Disabled,
    Cached,
    Indexed,
    #[default]
    Live,
}

impl WebSearchAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Cached => "cached",
            Self::Indexed => "indexed",
            Self::Live => "live",
        }
    }
}

impl RuntimeEncode for WebSearchAccess {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.as_str())
    }
}

impl RuntimeDecode for WebSearchAccess {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("disabled", &[]),
                RuntimeEnumVariant::new("cached", &[]),
                RuntimeEnumVariant::new("indexed", &[]),
                RuntimeEnumVariant::new("live", &[]),
            ],
            &[Self::Disabled, Self::Cached, Self::Indexed, Self::Live],
        )
    }
}

/// The effective inference controls captured by newly admitted work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInferenceSettings {
    pub model_verbosity: Option<OutputVerbosity>,
    pub reasoning_summary: ReasoningSummary,
    pub web_search_access: WebSearchAccess,
}

/// Non-secret model choices published by the endpoint for delegated work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentModelOption {
    #[serde(with = "scalar_runtime_string")]
    pub value: RuntimeString,
    #[serde(with = "scalar_runtime_string")]
    pub label: RuntimeString,
}

impl RuntimeEncode for SubagentModelOption {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("value", &self.value)?;
        object.field("label", &self.label)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentModelOption {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                value: object.required("value", &[])?,
                label: object.required("label", &[])?,
            })
        })
    }
}

pub fn validate_subagent_model_options(options: &[SubagentModelOption]) -> Result<(), String> {
    if options.len() > 4096 {
        return Err("The subagent model directory contains too many entries.".into());
    }
    let mut seen = std::collections::BTreeSet::new();
    for option in options {
        let value = option
            .value
            .try_to_utf8()
            .map_err(|_| "Model IDs must be scalar text.")?;
        let label = option
            .label
            .try_to_utf8()
            .map_err(|_| "Model labels must be scalar text.")?;
        if !value
            .split_once(':')
            .is_some_and(|(source, model)| !source.is_empty() && !model.is_empty())
            || value.trim() != value
            || value.chars().any(char::is_control)
            || value.len() > 2048
            || label.trim().is_empty()
            || label.trim() != label
            || label.chars().any(char::is_control)
            || label.len() > 512
            || !seen.insert(value)
        {
            return Err("Each subagent model directory entry requires a unique source:modelId and a nonempty label.".into());
        }
    }
    Ok(())
}

/// Approximate user location forwarded onto a native web-search request.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSearchUserLocation {
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option"
    )]
    pub city: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option"
    )]
    pub region: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option"
    )]
    pub country: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option"
    )]
    pub timezone: Option<RuntimeString>,
}

impl RuntimeEncode for WebSearchUserLocation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.city {
            object.field("city", value)?;
        }
        if let Some(value) = &self.region {
            object.field("region", value)?;
        }
        if let Some(value) = &self.country {
            object.field("country", value)?;
        }
        if let Some(value) = &self.timezone {
            object.field("timezone", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WebSearchUserLocation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                city: object.optional("city", &[])?,
                region: object.optional("region", &[])?,
                country: object.optional("country", &[])?,
                timezone: object.optional("timezone", &[])?,
            })
        })
    }
}

/// Approximate amount of server-side search context to ask providers for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebSearchContextSize {
    Low,
    Medium,
    High,
}

impl RuntimeEncode for WebSearchContextSize {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for WebSearchContextSize {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("medium", &[]),
                RuntimeEnumVariant::new("high", &[]),
            ],
            &[Self::Low, Self::Medium, Self::High],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default, from = "WebSearchConfigWire")]
pub struct WebSearchConfig {
    pub access: WebSearchAccess,
    pub mode: WebSearchMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option_vec"
    )]
    pub allowed_domains: Option<Vec<RuntimeString>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option_vec"
    )]
    pub blocked_domains: Option<Vec<RuntimeString>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_location: Option<WebSearchUserLocation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_size: Option<WebSearchContextSize>,
    /// Which model subscription's own search service backs the client
    /// `web_search` tool (`codex`, `qoder`, `qoderwork`, `bailian`, `cursor`, `qwenwork`). Absent means the
    /// ordinary backend: the injected host backend or the engine browser.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct WebSearchConfigWire {
    mode: WebSearchMode,
    access: Option<WebSearchAccess>,
    max_uses: Option<u64>,
    allowed_domains: Option<Vec<String>>,
    blocked_domains: Option<Vec<String>>,
    user_location: Option<WebSearchUserLocation>,
    context_size: Option<WebSearchContextSize>,
    source: Option<String>,
}

impl Default for WebSearchConfigWire {
    fn default() -> Self {
        Self {
            mode: WebSearchMode::Client,
            access: None,
            max_uses: None,
            allowed_domains: None,
            blocked_domains: None,
            user_location: None,
            context_size: None,
            source: None,
        }
    }
}

fn legacy_search_access(mode: WebSearchMode) -> WebSearchAccess {
    match mode {
        WebSearchMode::Off | WebSearchMode::Disabled => WebSearchAccess::Disabled,
        WebSearchMode::Cached => WebSearchAccess::Cached,
        WebSearchMode::Indexed => WebSearchAccess::Indexed,
        _ => WebSearchAccess::Live,
    }
}

impl From<WebSearchConfigWire> for WebSearchConfig {
    fn from(value: WebSearchConfigWire) -> Self {
        let mode = effective_search_mode(value.mode, value.access, value.source.as_deref());
        Self {
            access: value
                .access
                .unwrap_or_else(|| legacy_search_access(value.mode)),
            mode,
            max_uses: value.max_uses,
            allowed_domains: value
                .allowed_domains
                .map(|domains| domains.into_iter().map(Into::into).collect()),
            blocked_domains: value
                .blocked_domains
                .map(|domains| domains.into_iter().map(Into::into).collect()),
            user_location: value.user_location,
            context_size: value.context_size,
            source: value.source,
        }
    }
}

fn effective_search_mode(
    mode: WebSearchMode,
    access: Option<WebSearchAccess>,
    source: Option<&str>,
) -> WebSearchMode {
    if access.is_some_and(|access| access != WebSearchAccess::Disabled)
        && matches!(mode, WebSearchMode::Off | WebSearchMode::Disabled)
    {
        if source == Some("native") {
            WebSearchMode::Native
        } else {
            WebSearchMode::Client
        }
    } else {
        mode
    }
}

impl Default for WebSearchConfig {
    fn default() -> Self {
        WebSearchConfig {
            access: WebSearchAccess::Live,
            mode: WebSearchMode::Client,
            max_uses: None,
            allowed_domains: None,
            blocked_domains: None,
            user_location: None,
            context_size: None,
            source: None,
        }
    }
}

impl WebSearchConfig {
    pub(crate) fn set_access(&mut self, access: WebSearchAccess) {
        self.mode = effective_search_mode(self.mode, Some(access), self.source.as_deref());
        self.access = access;
    }
}

impl RuntimeEncode for WebSearchConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mode", &self.mode)?;
        object.field("access", &self.access)?;
        if let Some(value) = &self.max_uses {
            object.field("maxUses", value)?;
        }
        if let Some(value) = &self.allowed_domains {
            object.field("allowedDomains", value)?;
        }
        if let Some(value) = &self.blocked_domains {
            object.field("blockedDomains", value)?;
        }
        if let Some(value) = &self.user_location {
            object.field("userLocation", value)?;
        }
        if let Some(value) = &self.context_size {
            object.field("contextSize", value)?;
        }
        if let Some(value) = &self.source {
            object.field("source", &RuntimeString::from(value.clone()))?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WebSearchConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let mode = object.defaulted("mode", &[], || WebSearchMode::Client)?;
            let access = object.optional("access", &[])?;
            let source = object
                .optional::<RuntimeString>("source", &[])?
                .map(|value| value.try_to_utf8().unwrap_or_default());
            Ok(Self {
                mode: effective_search_mode(mode, access, source.as_deref()),
                access: access.unwrap_or_else(|| legacy_search_access(mode)),
                max_uses: object.optional("maxUses", &[])?,
                allowed_domains: object.optional("allowedDomains", &[])?,
                blocked_domains: object.optional("blockedDomains", &[])?,
                user_location: object.optional("userLocation", &[])?,
                context_size: object.optional("contextSize", &[])?,
                source,
            })
        })
    }
}

/// Network egress domain policy for the `web_fetch` tool.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebFetchConfig {
    /// Explicit provider for page extraction. Absent or `builtin` uses local networking.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option_vec"
    )]
    pub allowed_domains: Option<Vec<RuntimeString>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        with = "scalar_runtime_string::option_vec"
    )]
    pub blocked_domains: Option<Vec<RuntimeString>>,
}

impl RuntimeEncode for WebFetchConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.source {
            object.field("source", &RuntimeString::from(value.clone()))?;
        }
        if let Some(value) = &self.allowed_domains {
            object.field("allowedDomains", value)?;
        }
        if let Some(value) = &self.blocked_domains {
            object.field("blockedDomains", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WebFetchConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                source: object
                    .optional::<RuntimeString>("source", &[])?
                    .map(|value| value.try_to_utf8().unwrap_or_default()),
                allowed_domains: object.optional("allowedDomains", &[])?,
                blocked_domains: object.optional("blockedDomains", &[])?,
            })
        })
    }
}

/// Skill-store (catalog) behavior for the read-only `skill_search` tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SkillCatalogConfig {
    pub enabled: bool,
    pub non_suspicious_only: bool,
}

impl Default for SkillCatalogConfig {
    fn default() -> Self {
        SkillCatalogConfig {
            enabled: true,
            non_suspicious_only: true,
        }
    }
}

/// MCP-server store behavior for the `mcp_search` + `request_mcp_install` tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct McpCatalogConfig {
    pub enabled: bool,
}

/// Automatic memory-consolidation thresholds shared with Desktop settings.
#[derive(Debug, Clone, PartialEq)]
pub struct DreamConfig {
    /// Minimum wall-clock interval in hours between successful consolidations.
    pub min_hours: f64,
    /// Minimum number of other touched sessions since the last consolidation.
    pub min_sessions: u32,
}

mod dream_config_serde {
    use super::DreamConfig;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &DreamConfig, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("DreamConfig", 2)?;
        state.serialize_field("minHours", &value.min_hours)?;
        state.serialize_field("minSessions", &value.min_sessions)?;
        state.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DreamConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("dream must be an object"))?;
        let mut config = DreamConfig::default();
        if let Some(value) = object.get("minHours") {
            config.min_hours = serde_json::from_value(value.clone()).map_err(D::Error::custom)?;
        }
        if let Some(value) = object.get("minSessions") {
            config.min_sessions =
                serde_json::from_value(value.clone()).map_err(D::Error::custom)?;
        }
        Ok(config)
    }
}

impl Default for DreamConfig {
    fn default() -> Self {
        Self {
            min_hours: 24.0,
            min_sessions: 5,
        }
    }
}

impl DreamConfig {
    pub const MIN_HOURS: f64 = 0.01;
    pub const MAX_HOURS: f64 = 720.0;
    pub const MIN_SESSIONS: u32 = 1;
    pub const MAX_SESSIONS: u32 = 100;

    pub fn validate(&self) -> Result<(), String> {
        if !self.min_hours.is_finite()
            || !(Self::MIN_HOURS..=Self::MAX_HOURS).contains(&self.min_hours)
        {
            return Err(format!(
                "dream.minHours must be a finite number between {} and {}",
                Self::MIN_HOURS,
                Self::MAX_HOURS
            ));
        }
        if !(Self::MIN_SESSIONS..=Self::MAX_SESSIONS).contains(&self.min_sessions) {
            return Err(format!(
                "dream.minSessions must be an integer between {} and {}",
                Self::MIN_SESSIONS,
                Self::MAX_SESSIONS
            ));
        }
        Ok(())
    }

    pub fn min_interval_ms(&self) -> i64 {
        (self.min_hours * 60.0 * 60.0 * 1000.0).round() as i64
    }
}

impl Default for McpCatalogConfig {
    fn default() -> Self {
        McpCatalogConfig { enabled: true }
    }
}

/// Which cleared tool calls also lose their INPUTS, not just their results
/// (API `clear_tool_inputs`). The wire accepts either a flag covering every
/// cleared call or an explicit tool-name list, so both forms are modeled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClearToolInputs {
    /// `true` clears the inputs of every cleared call; `false` keeps them all.
    All(bool),
    /// Clear inputs only for calls to these tools.
    Tools(#[serde(with = "scalar_runtime_string::vec")] Vec<RuntimeString>),
}

impl RuntimeEncode for ClearToolInputs {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            ClearToolInputs::All(all) => all.encode_runtime(out),
            ClearToolInputs::Tools(tools) => tools.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for ClearToolInputs {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Array(_) => Ok(ClearToolInputs::Tools(Vec::decode_runtime(value)?)),
            _ => Ok(ClearToolInputs::All(bool::decode_runtime(value)?)),
        }
    }
}

/// API-level context editing (Anthropic `context-management` beta). When set,
/// the provider clears aged `tool_use`/`tool_result` blocks server-side once the
/// prompt crosses `trigger_input_tokens`, preserving the most recent
/// `keep_tool_uses` calls. This is a wire-level complement to the client-side
/// micro-compaction: cleared blocks stop counting against the context window
/// AND stop being retransmitted. Neutral across providers — only the Anthropic
/// first-party adapter emits it; others ignore it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContextManagementConfig {
    /// Clear once the request reaches this many input tokens (API `trigger`).
    /// Omitted lets the server apply its own default threshold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_input_tokens: Option<u64>,
    /// Most-recent tool uses to preserve when clearing (API `keep`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_tool_uses: Option<u64>,
    /// Only clear when at least this many input tokens are reclaimable
    /// (API `clear_at_least`), avoiding churn that frees very little.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clear_at_least_input_tokens: Option<u64>,
    /// Also clear the tool-call INPUTS, not just their results
    /// (API `clear_tool_inputs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clear_tool_inputs: Option<ClearToolInputs>,
    /// Tool names whose calls are never cleared (API `exclude_tools`).
    #[serde(with = "scalar_runtime_string::vec")]
    pub exclude_tools: Vec<RuntimeString>,
}

impl RuntimeEncode for ContextManagementConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(trigger_input_tokens) = &self.trigger_input_tokens {
            object.field("triggerInputTokens", trigger_input_tokens)?;
        }
        if let Some(keep_tool_uses) = &self.keep_tool_uses {
            object.field("keepToolUses", keep_tool_uses)?;
        }
        if let Some(clear_at_least_input_tokens) = &self.clear_at_least_input_tokens {
            object.field("clearAtLeastInputTokens", clear_at_least_input_tokens)?;
        }
        if let Some(clear_tool_inputs) = &self.clear_tool_inputs {
            object.field("clearToolInputs", clear_tool_inputs)?;
        }
        if !self.exclude_tools.is_empty() {
            object.field("excludeTools", &self.exclude_tools)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ContextManagementConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                trigger_input_tokens: object.optional("triggerInputTokens", &[])?,
                keep_tool_uses: object.optional("keepToolUses", &[])?,
                clear_at_least_input_tokens: object.optional("clearAtLeastInputTokens", &[])?,
                clear_tool_inputs: object.optional("clearToolInputs", &[])?,
                exclude_tools: object.optional("excludeTools", &[])?.unwrap_or_default(),
            })
        })
    }
}

/// A model tier the engine's auxiliary/background work is routed through.
/// High = the strongest model the source offers, Low = the cheap/fast one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelTier {
    High,
    Medium,
    Low,
}

impl ModelTier {
    /// The tier a selection string names, if any. Only the tier names resolve;
    /// anything else is a concrete model id for the session's provider.
    pub fn from_selection(selection: &str) -> Option<Self> {
        match selection {
            "high" => Some(Self::High),
            "medium" => Some(Self::Medium),
            "low" => Some(Self::Low),
            _ => None,
        }
    }
}

/// The session's tier→model table. Sub-agents and agent definitions that
/// declare a tier token resolve their model from it by tier; a tier left unset
/// falls back to the session `model`. Named auxiliary features (title
/// generation, message prediction, memory maintenance) carry independent
/// feature targets instead, and follow the session's main model while unset.
/// Values are concrete model ids within the session's provider — the host
/// resolves per-source lineups to real ids before a session starts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ModelTiers {
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub high: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub medium: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub low: Option<RuntimeString>,
}

/// Engine config. Magic numbers (budgets, timeouts) live here, not scattered in
/// code. All have defaults so a zero-config session works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskResourceMode {
    Manual,
    AutoEdit,
    ScopedAuto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskResourceScope {
    pub task_id: String,
    pub mode: TaskResourceMode,
    #[serde(default)]
    pub network_allowed: bool,
    #[serde(with = "scalar_runtime_string::vec")]
    pub read_roots: Vec<RuntimeString>,
    #[serde(with = "scalar_runtime_string::vec")]
    pub write_roots: Vec<RuntimeString>,
}

impl TaskResourceScope {
    pub fn validate(&self) -> Result<(), String> {
        if self.task_id.is_empty() || self.task_id.len() > 256 || self.task_id.contains('\0') {
            return Err("taskResourceScope.taskId must identify one task".into());
        }
        if self.read_roots.len() + self.write_roots.len() > 128 {
            return Err("taskResourceScope has too many authorized directories".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct EngineConfig {
    #[serde(with = "scalar_runtime_string")]
    pub model: RuntimeString,
    /// The name the `# Environment` block reports as the model in charge.
    ///
    /// Hosts that serve a managed model line set this to the product name they
    /// show the user, so the session never recites an upstream vendor model id
    /// the host deliberately keeps to itself. Absent (every BYO source) means
    /// the environment block names `model`, exactly as before.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub model_display_name: Option<RuntimeString>,
    /// Auxiliary model slots by tier (see [`ModelTiers`]).
    pub model_tiers: ModelTiers,
    /// Non-secret host selection provenance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_source: Option<crate::types::events::InferenceSource>,
    /// Cap on one response's output tokens. `None` — the default — means "as much
    /// as the model accepts": the session resolves it from the model's own declared
    /// ceiling at turn assembly. A fixed small default was worse than it looks: the
    /// turn hit the cap, and the only way back was a second request with a doubled
    /// budget, which cost round trips and still stopped below what the model would
    /// have written in one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    /// Declared model output capacity captured with the inference binding. A
    /// smaller request budget must not reduce the context-management reservation.
    /// This is runtime state, never a persisted or host-configurable preference.
    #[serde(skip)]
    pub model_max_output_tokens: Option<u64>,
    /// Validated actual model capacity captured at the provider binding.
    /// A legacy declared override is capped by the provider's declared capacity.
    #[serde(skip)]
    pub model_context_window: Option<u64>,
    /// Captured model headroom policy; never a user-editable preference.
    #[serde(skip)]
    pub model_effective_context_window_percent: Option<u8>,
    /// Captured model automatic-compaction limit before user scope overrides.
    #[serde(skip)]
    pub model_auto_compact_token_limit: Option<u64>,
    /// Runtime provenance of an automatic-window override; never persisted.
    #[serde(skip)]
    pub auto_compact_window_source: Option<crate::context::compact::window::AutoWindowSource>,
    /// The output budget a truncation-recovery retry may ask for. Truncation
    /// means the response ran out of room, so the recovery ladder needs a rung
    /// ABOVE the budget that just failed; re-asking with the same number spends
    /// a whole response to land in the same place.
    ///
    /// `None` — the default — doubles the resolved budget for the turn, which is
    /// what the provider-side ceiling comment promises (`clamp_output_tokens`).
    /// The provider still clamps the request to the model's own declared
    /// ceiling, so a host that declares one keeps the ladder inside it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_retry_tokens: Option<u64>,
    /// Legacy input translated before session admission; never published as policy.
    #[serde(skip_serializing)]
    pub permission_mode: ExternalPermissionMode,
    #[serde(rename = "approval_policy", skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    #[serde(rename = "sandbox_mode", skip_serializing_if = "Option::is_none")]
    pub sandbox_mode: Option<crate::types::permission::SandboxMode>,
    #[serde(rename = "approvals_reviewer", skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    #[serde(rename = "sandbox_workspace_write")]
    pub sandbox_workspace_write: SandboxWorkspaceWrite,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows: Option<WindowsConfig>,
    pub features: SandboxFeatureConfig,
    /// Optional predictive review configuration. Omission keeps its legacy gate
    /// disabled; an explicit model-owned review policy is resolved independently.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_review: Option<crate::engine::approval_async::AsyncFeatureConfig>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub approval_review_policy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub approval_review_extra_policy: Option<RuntimeString>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub approval_review_policy_template: Option<RuntimeString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_review_transcript_mode: Option<crate::engine::approval_async::TranscriptFormat>,
    /// Custom directory for plan-mode plan files, relative to the session's
    /// project root. Ignored (with fallback) when it escapes the project root.
    /// Unset → `<config_home>/plans`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub plans_directory: Option<RuntimeString>,
    /// Raw reasoning selection. Child defaults, call fields and explicit role
    /// fields may replace it before the captured model resolves a request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<EffortValue>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option",
        alias = "default_subagent_model"
    )]
    pub default_subagent_model: Option<RuntimeString>,
    /// Allow explicit model choices from the endpoint's captured directory.
    pub subagent_model_catalog: bool,
    pub subagent_model_options: Vec<SubagentModelOption>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        alias = "default_subagent_reasoning_effort"
    )]
    pub default_subagent_reasoning_effort: Option<EffortValue>,
    /// Local multi-agent orchestration mode. This authorizes proactive
    /// delegation for the session; it is never a provider reasoning level and
    /// never rides a provider wire.
    pub ultra_mode: bool,
    /// None preserves the selected model's declared output-detail default.
    pub model_verbosity: Option<OutputVerbosity>,
    pub reasoning_summary: ReasoningSummary,
    /// Extended thinking for the main loop (default-on).
    pub thinking: bool,
    /// Codex subscription Fast mode. Other provider sources keep Standard mode.
    pub fast_mode: bool,
    /// Agentic task budget (Anthropic task-budgets beta). Optional; first-party
    /// + supported-model only, clamped to the 20k API minimum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_budget_tokens: Option<u64>,
    /// API-level context editing (Anthropic `context-management` beta). Optional;
    /// first-party Anthropic only. Complements client-side micro-compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_management: Option<ContextManagementConfig>,
    /// Manage prior-turn thinking blocks server-side: normally pin every thinking
    /// turn so no server-side policy sheds them, and once the session has gone
    /// cache-cold keep only the most recent turn (the aged ones are pure weight on
    /// a prefix that must be rewritten anyway). First-party Anthropic only, and
    /// only on requests that actually carry thinking.
    pub manage_thinking_context: bool,
    /// Prefix allow/deny lists, e.g. "Bash(git status)" or "Read".
    #[serde(with = "scalar_runtime_string::vec")]
    pub allow: Vec<RuntimeString>,
    #[serde(with = "scalar_runtime_string::vec")]
    pub deny: Vec<RuntimeString>,
    /// Rules that force the approval prompt even when the mode would allow the
    /// action outright. Same grammar as `allow`/`deny`; evaluated BEFORE allow,
    /// so `ask` on a narrow prefix still prompts under a broad allow rule.
    #[serde(default, with = "scalar_runtime_string::vec")]
    pub ask: Vec<RuntimeString>,
    /// Apply durable user-level allow/ask rules. Disable for isolated sessions
    /// that need fresh owner approval; stored deny rules always remain active.
    pub use_stored_permission_rules: bool,
    /// Immutable owner-approved resources for one isolated task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_resource_scope: Option<TaskResourceScope>,
    /// Managed proxy that routes sandboxed child-process egress through a
    /// policy-checking loopback proxy.
    pub network_proxy: NetworkProxyConfig,
    /// Harden the engine's own process on Agent construction.
    pub process_hardening: bool,

    // Context window accounting.
    /// Runtime kill switch for every compaction entry point, including manual
    /// requests. Bound from the process environment, never a stored preference.
    #[serde(skip)]
    pub compaction_disabled: bool,
    /// Enable automatic context compaction and overflow recovery.
    pub auto_compaction: bool,
    pub context_window: u64,
    /// Optional automatic-compaction token limit. Total scope caps it at 90% of
    /// the actual model window; body scope applies it after the retained prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_compact_window: Option<u64>,
    pub auto_compact_scope: AutoCompactScope,
    /// After an answer completes, compact at this percentage of usable input.
    /// Zero disables post-turn compaction.
    pub post_turn_compact_threshold_percent: u8,
    /// Maximum stream reconnection retries for a foreground compaction request.
    /// The initial request is separate from this retry budget.
    pub compaction_stream_max_retries: u64,
    /// Summary instructions used by automatic and manual full compaction.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub compact_prompt: Option<RuntimeString>,
    /// Retired input accepted when loading older settings; it has no effect.
    #[serde(skip_serializing)]
    pub output_reserve_tokens: u64,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub history_snip: bool,
    /// Retired input accepted when loading older settings; it has no effect.
    #[serde(skip_serializing)]
    pub compact_buffer_tokens: u64,
    /// Retired input accepted when loading older settings; it has no effect.
    #[serde(skip_serializing)]
    pub compact_threshold: Option<f64>,
    /// How much context the model is told it has left, injected as the last system
    /// part each turn so it never sits inside a cached prefix.
    ///
    /// Off by default: the line is a deliberate addition to the request shape, and
    /// the signed provider-behavior baseline defines the default shape, so it has
    /// to be switched on rather than assumed.
    pub total_tokens_reminder: TotalTokensReminderMode,
    /// The long-horizon budget reported under
    /// [`TotalTokensReminderMode::PaddedCountdown`].
    pub total_tokens_reminder_budget: u64,
    /// Re-anchor the optional padded countdown at each regular user turn.
    /// An absent value preserves session-wide accounting and existing wire data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens_reminder_after_user_turn: Option<bool>,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub compaction_self_check: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub speculative_compaction: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub persist_precomputed_compaction: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub compact_cold_cleanup: bool,
    /// Enable request-local context hints on supported main-session transports.
    pub context_hints: bool,
    /// Retired input accepted when loading older settings; it has no effect.
    #[serde(skip_serializing)]
    pub precompute_buffer_fraction: Option<f64>,
    /// Precompute the automatic-compaction summary in the background while
    /// the context is still inside the lead window before the compaction
    /// boundary. Only applies when automatic compaction is enabled; on by
    /// default, and setting it to `false` keeps the synchronous path.
    pub precompute_compaction: bool,
    /// Developer diagnostics: record how every model request settled (raw
    /// finish reason, stream framing, usage, timing) into
    /// `<config home>/logs/provider-trace.log`. Off by default; the provider
    /// layer re-reads the user config file per request so a settings toggle
    /// takes effect without restarting a session.
    pub provider_trace: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub early_microcompact: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub idle_microcompact: bool,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub idle_microcompact_gap_minutes: u64,
    /// Retired input accepted from older settings; context operations ignore it.
    #[serde(skip_serializing)]
    pub idle_microcompact_keep_recent: i64,

    /// Inject a start-of-session git snapshot into the system prompt.
    pub include_git_context: bool,

    /// Active output style NAME (the `outputStyle` setting).
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub output_style: Option<RuntimeString>,

    /// Tiered memory/instruction loading.
    pub memory: MemoryConfig,

    // Failure-semantics knobs (§0.2 / §0.3).
    pub max_tool_concurrency: u32,
    pub tool_timeout_ms: u64,
    /// Maximum observation window for a live background command, in milliseconds.
    #[serde(alias = "background_terminal_max_timeout")]
    pub background_terminal_max_timeout: u64,
    pub stream_idle_timeout_ms: u64,
    pub stream_total_timeout_ms: u64,
    /// `None` = wait forever; set for headless to avoid approval deadlock.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_timeout_ms: Option<u64>,
    pub max_aggregated_output_bytes: u64,
    pub max_tool_results_per_message_chars: u64,
    /// Enable frozen aggregate replacement of otherwise eligible tool results.
    pub aggregate_tool_results: bool,
    /// SESSION-CUMULATIVE spend ceiling in USD. Omit for no cap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_budget_usd: Option<f64>,
    pub max_retries: u32,
    /// Keep retrying connection-establishment failures on a dead network past
    /// the attempt cap (5s→60s capped backoff) instead of failing the turn.
    /// Connection recovery stays on for the whole turn.
    pub unbounded_network_retries: bool,
    /// Backup models to rotate to when the primary model is unusable.
    #[serde(with = "scalar_runtime_string::vec")]
    pub fallback_models: Vec<RuntimeString>,
    /// Behavioral loop detection.
    pub loop_detection: bool,
    /// Retained for older configuration documents; agent trees have no depth gate.
    pub max_sub_agent_depth: u32,
    /// Legacy child capacity. The total-thread setting takes precedence.
    pub max_concurrent_sub_agents: u32,
    /// Total configured threads, including the primary thread's reserved place.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_threads_per_session: Option<u32>,
    /// A supplied delegation hint overrides effort-dependent hints, including when empty.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "scalar_runtime_string::option"
    )]
    pub multi_agent_mode_hint_text: Option<RuntimeString>,

    /// Tool names removed from the default built-in tool set before
    /// assembly. Lets a host drop capabilities its product must not expose
    /// (e.g. shell execution) without replacing the whole tool set.
    #[serde(with = "scalar_runtime_string::vec")]
    pub disabled_tools: Vec<RuntimeString>,
    /// Deprecated compatibility key. Durable Task tools are always enabled.
    pub todo_v2: bool,
    /// Enable the worktree tools (EnterWorktree/ExitWorktree).
    pub worktree: bool,
    /// Enable filesystem checkpointing.
    pub checkpointing: bool,
    /// Capture the user's interactive login-shell environment once per session.
    pub shell_env_capture: bool,
    /// Retired compatibility field. Workflow is always available; false is ignored.
    pub workflow: bool,
    /// Enable the swarm tools.
    pub swarm: bool,
    /// Enable the scheduled-prompt tools (CronCreate/CronList/CronDelete) and
    /// the session cron scheduler.
    pub cron: bool,
    /// Enable goal mode: the `get_goal`/`create_goal`/`update_goal` tools plus
    /// the automatic continuation of an active goal after every turn.
    ///
    /// Off by default. An automatic continuation spends the user's tokens
    /// without them asking again, so it is opt-in per session.
    pub goal: bool,
    /// Offer optional structured questions during Default collaboration.
    pub request_user_input_default: bool,
    /// Optional complete collaboration instruction template overrides.
    pub collaboration_default_instructions: Option<String>,
    pub collaboration_plan_instructions: Option<String>,
    /// Optional system ceiling for one persistent goal. No limit is invented when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_goal_token_budget: Option<i64>,
    /// Warm provider transports at session startup: the DNS+TCP+TLS handshake
    /// is completed in the background and pooled, so the first model request
    /// reuses a warm connection. Misses degrade silently to a cold handshake.
    pub prewarm: bool,
    /// Retired: the `exec` / `wait` surface was replaced by the `REPL` tool.
    /// Accepted only so settings files written by older builds keep parsing;
    /// it is never serialized and has no effect.
    #[serde(rename = "codeMode", default, skip_serializing)]
    pub retired_code_mode: bool,
    /// Run code-mode cells in a host SUBPROCESS instead of in this process.
    ///
    /// The engine still owns the session: nested tool calls travel back over the
    /// host connection and cross the full permission + hook pipeline. When the
    /// host program cannot be launched this degrades to the in-process carrier,
    /// unless [`Self::code_mode_host_required`] forbids it.
    pub code_mode_host: bool,
    /// Refuse to run cells in this process when `code_mode_host` is set: a
    /// deployment that requires host isolation fails loudly instead of quietly
    /// executing cells locally.
    pub code_mode_host_required: bool,
    /// Forward a sub-agent's assistant TEXT up the spawning tool's progress
    /// stream.
    pub forward_subagent_text: bool,
    /// Enable the LSP tools + auto-surfaced diagnostics after edits.
    pub lsp: bool,
    /// Surface out-of-band file edits.
    pub edited_files: bool,
    /// Use the 1-hour prompt-cache TTL instead of the default 5 minutes.
    pub cache1h_ttl: bool,
    /// Master switch for per-agent persistent memory.
    pub agent_memory: bool,
    /// Enable LIVE per-turn memory recall.
    pub memory_recall: bool,
    /// Enable user-scope memory snapshot sync.
    pub agent_memory_snapshot: bool,
    /// Enable automatic memory EXTRACTION.
    pub auto_memory: bool,
    /// Enable automatic memory CONSOLIDATION.
    pub auto_dream: bool,
    /// Scheduling thresholds for automatic consolidation. Manual Dream does
    /// not consult these time/session-count gates.
    #[serde(with = "dream_config_serde")]
    pub dream: DreamConfig,
    /// Enable fork sub-agents.
    pub fork: bool,

    /// Web-search policy.
    pub web_search: WebSearchConfig,
    /// Network egress domain policy for the `web_fetch` tool.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_fetch: Option<WebFetchConfig>,
    /// Skill-store (catalog) behavior for the read-only `skill_search` tool.
    pub skill_catalog: SkillCatalogConfig,
    /// MCP-server store behavior for the `mcp_search` and `request_mcp_install` tools.
    pub mcp_catalog: McpCatalogConfig,
}

impl RuntimeEncode for EngineConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        ConfigJson::serialize_config(self)?.encode_runtime(out)
    }
}

impl RuntimeDecode for EngineConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let mut value = value.clone();
        if let RuntimeJson::Object(fields) = &mut value {
            if fields
                .get("effort")
                .and_then(RuntimeJson::as_string)
                .is_some_and(|level| level == "ultra")
                && !fields.contains_key("ultraMode")
            {
                fields.insert("ultraMode", RuntimeJson::Bool(true));
            }
        }
        let config: Self = ConfigJson::decode_runtime_document(&value)?
            .deserialize_config()
            .map_err(|error| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                    error.to_string(),
                )))
            })?;
        validate_subagent_model_options(&config.subagent_model_options)
            .map_err(|error| RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(error.into())))?;
        Ok(config)
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            model: DEFAULT_MODEL.into(),
            model_display_name: None,
            model_tiers: ModelTiers::default(),
            model_source: None,
            max_output_tokens: None,
            model_max_output_tokens: None,
            model_context_window: None,
            model_effective_context_window_percent: None,
            model_auto_compact_token_limit: None,
            auto_compact_window_source: None,
            max_output_retry_tokens: None,
            permission_mode: ExternalPermissionMode::Default,
            approval_policy: Some(crate::types::permission::ApprovalPolicy::OnRequest),
            sandbox_mode: Some(crate::types::permission::SandboxMode::ReadOnly),
            approvals_reviewer: Some(crate::types::permission::ApprovalsReviewer::User),
            sandbox_workspace_write: SandboxWorkspaceWrite::default(),
            windows: None,
            features: SandboxFeatureConfig::default(),
            approval_review: None,
            approval_review_policy: None,
            approval_review_extra_policy: None,
            approval_review_policy_template: None,
            approval_review_transcript_mode: None,
            plans_directory: None,
            effort: None,
            ultra_mode: false,
            default_subagent_model: None,
            subagent_model_catalog: false,
            subagent_model_options: Vec::new(),
            default_subagent_reasoning_effort: None,
            model_verbosity: None,
            reasoning_summary: ReasoningSummary::Auto,
            thinking: true,
            fast_mode: false,
            task_budget_tokens: None,
            context_management: None,
            allow: Vec::new(),
            deny: Vec::new(),
            ask: Vec::new(),
            use_stored_permission_rules: true,
            task_resource_scope: None,
            network_proxy: NetworkProxyConfig::default(),
            process_hardening: false,
            context_window: 200_000,
            compaction_disabled: false,
            auto_compaction: true,
            auto_compact_window: None,
            auto_compact_scope: AutoCompactScope::Total,
            post_turn_compact_threshold_percent: 0,
            compaction_stream_max_retries:
                crate::context::compact::retry::DEFAULT_STREAM_MAX_RETRIES,
            compact_prompt: None,
            output_reserve_tokens: 0,
            history_snip: false,
            compact_buffer_tokens: 0,
            compact_threshold: None,
            total_tokens_reminder: TotalTokensReminderMode::Off,
            total_tokens_reminder_budget: 15_000_000,
            total_tokens_reminder_after_user_turn: None,
            compaction_self_check: false,
            speculative_compaction: false,
            persist_precomputed_compaction: false,
            compact_cold_cleanup: false,
            context_hints: true,
            precompute_buffer_fraction: None,
            precompute_compaction: true,
            provider_trace: false,
            early_microcompact: false,
            idle_microcompact: false,
            idle_microcompact_gap_minutes: 60,
            idle_microcompact_keep_recent: 5,
            manage_thinking_context: true,
            include_git_context: true,
            output_style: None,
            memory: MemoryConfig::default(),
            max_tool_concurrency: 10,
            tool_timeout_ms: 120_000,
            background_terminal_max_timeout: 300_000,
            // `stream_idle_timeout_ms` must stay STRICTLY BELOW
            // `stream_total_timeout_ms`. The stream loop classifies a timeout by
            // `remaining_total <= idle_wait`, so at equality every timeout is
            // reported as the total deadline — which is not retryable — while a
            // genuine silent stream (an idle timeout, classified as Network) is
            // retryable. Equal values would therefore turn recoverable stalls
            // into hard failures, not merely make the idle bound unreachable.
            stream_idle_timeout_ms: 300_000,
            stream_total_timeout_ms: 1_200_000,
            approval_timeout_ms: None,
            max_aggregated_output_bytes: 256 * 1024,
            max_tool_results_per_message_chars: 200_000,
            aggregate_tool_results: false,
            max_budget_usd: None,
            max_retries: 5,
            unbounded_network_retries: true,
            fallback_models: Vec::new(),
            loop_detection: true,
            max_sub_agent_depth: 3,
            max_concurrent_sub_agents: 3,
            max_concurrent_threads_per_session: None,
            multi_agent_mode_hint_text: None,
            todo_v2: false,
            worktree: false,
            disabled_tools: Vec::new(),
            checkpointing: false,
            shell_env_capture: false,
            workflow: true,
            swarm: true,
            cron: true,
            goal: false,
            request_user_input_default: false,
            collaboration_default_instructions: None,
            collaboration_plan_instructions: None,
            maximum_goal_token_budget: None,
            prewarm: true,
            retired_code_mode: false,
            code_mode_host: false,
            code_mode_host_required: false,
            forward_subagent_text: false,
            lsp: false,
            edited_files: false,
            cache1h_ttl: false,
            agent_memory: true,
            memory_recall: false,
            agent_memory_snapshot: false,
            auto_memory: false,
            auto_dream: false,
            dream: DreamConfig::default(),
            fork: false,
            web_search: WebSearchConfig::default(),
            web_fetch: None,
            skill_catalog: SkillCatalogConfig::default(),
            mcp_catalog: McpCatalogConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Resident capacity is resolved from the configuration of each load request.
    pub fn effective_subagent_capacity(&self) -> usize {
        self.max_concurrent_threads_per_session
            .map(|total| total.saturating_sub(1) as usize)
            .unwrap_or(self.max_concurrent_sub_agents as usize)
    }

    /// The concrete model a tier runs on in this session: the tier-map entry,
    /// else the main model. Auxiliary work calls this so an unconfigured tier
    /// silently inherits rather than failing.
    pub fn model_for_tier(&self, tier: ModelTier) -> RuntimeString {
        let mapped = match tier {
            ModelTier::High => &self.model_tiers.high,
            ModelTier::Medium => &self.model_tiers.medium,
            ModelTier::Low => &self.model_tiers.low,
        };
        mapped
            .as_ref()
            .filter(|model| !model.is_empty())
            .cloned()
            .unwrap_or_else(|| self.model.clone())
    }

    /// Resolve a model selection that may name a tier. `high`/`medium`/`low`
    /// go through the tier map; an empty selection means the main model;
    /// anything else is already a concrete id and passes through unchanged.
    pub fn resolve_model_selection(&self, selection: &RuntimeString) -> RuntimeString {
        if selection.is_empty() {
            return self.model.clone();
        }
        let tier = selection
            .try_to_utf8()
            .ok()
            .and_then(|value| ModelTier::from_selection(&value));
        match tier {
            Some(tier) => self.model_for_tier(tier),
            None => selection.clone(),
        }
    }

    /// Enforce the cross-field dependency validations.
    ///
    /// The agent-memory family is NOT validated here: `agentMemory` is the
    /// subsystem's master switch, and `resolve_config_layers` turns its children
    /// off with it. Refusing "child on, master off" instead of gating it made a
    /// GUI that toggles the master a config error, because the child rows it
    /// wrote earlier are still in the file.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_threads_per_session == Some(0) {
            return Err("maxConcurrentThreadsPerSession must be at least 1".into());
        }
        validate_subagent_model_options(&self.subagent_model_options)?;
        if self.total_tokens_reminder_budget == 0 {
            return Err("totalTokensReminderBudget must be a positive integer".into());
        }
        if self.auto_compact_window == Some(0) {
            return Err("autoCompactWindow must be a positive integer".into());
        }
        if self.post_turn_compact_threshold_percent > 100 {
            return Err("postTurnCompactThresholdPercent must be between 0 and 100".into());
        }
        if self.compaction_stream_max_retries
            > crate::context::compact::retry::MAX_STREAM_MAX_RETRIES
        {
            return Err("compactionStreamMaxRetries must be between 0 and 100".into());
        }
        if self
            .maximum_goal_token_budget
            .is_some_and(|budget| budget <= 0)
        {
            return Err("maximumGoalTokenBudget must be a positive integer".into());
        }
        if let Some(scope) = &self.task_resource_scope {
            scope.validate()?;
        }
        self.sandbox_workspace_write
            .clone()
            .normalize_writable_roots(None)?;
        if self.windows.as_ref().is_some_and(|windows| {
            windows.sandbox == Some(WindowsSandboxMode::Mxc) && windows.allow_mxc == Some(false)
        }) {
            return Err(
                "windows.sandbox = \"mxc\" is not allowed when windows.allow_mxc = false".into(),
            );
        }
        self.dream.validate()?;
        if let Some(config) = self
            .approval_review
            .as_ref()
            .and_then(|value| value.local_config())
        {
            crate::engine::approval_async::AsyncReviewConfig::resolve(config.clone())?;
        }
        Ok(())
    }

    /// Normalize compatibility fields without replacing the resolved collaboration flag.
    pub fn normalize_legacy_ultra(&mut self) {
        let mut mode = Some(self.ultra_mode);
        crate::schemas::effort::normalize_legacy_ultra(&mut self.effort, &mut mode);
        self.ultra_mode = mode.unwrap_or_default();
    }
}

/// Fill the compatibility orchestration flag without rewriting raw effort.
pub fn normalize_legacy_ultra_fields(
    fields: &mut serde_json::Map<String, serde_json::Value>,
) -> bool {
    let is_legacy = fields
        .get("effort")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        == Some("ultra");
    if !is_legacy {
        return false;
    }
    if !fields.contains_key("ultraMode") {
        fields.insert("ultraMode".to_string(), serde_json::Value::Bool(true));
    }
    true
}

/// A fully-resolved engine config (an alias of `EngineConfig`).
pub type ResolvedConfig = EngineConfig;

/// The zero-config default.
pub fn default_config() -> ResolvedConfig {
    crate::engine::session::resolve_config_default()
}

/// The shape a key accepts at the KEY level, read off the default the struct
/// itself produces: a field defaulting to a bool only ever takes a bool, one
/// defaulting to an object only ever takes an object.
///
/// `Any` is for the keys serde omits when they are unset (the `Option` fields):
/// there is no default to read a shape from, so the shape stays serde's to
/// judge when the resolved document is decoded.
fn engine_config_value_kind(default_value: Option<&serde_json::Value>) -> ConfigValueKind {
    use serde_json::Value;

    match default_value {
        Some(Value::Bool(_)) => ConfigValueKind::Bool,
        Some(Value::Number(number)) if number.is_f64() => ConfigValueKind::Number,
        Some(Value::Number(_)) => ConfigValueKind::Integer,
        Some(Value::String(_)) => ConfigValueKind::String,
        Some(Value::Array(_)) => ConfigValueKind::Array,
        Some(Value::Object(_)) => ConfigValueKind::Object,
        Some(Value::Null) | None => ConfigValueKind::Any,
    }
}

/// Which config keys EXIST, straight from serde: the derived `Deserialize` for
/// [`EngineConfig`] hands its field list to `Deserializer::deserialize_struct`,
/// and [`FieldNameCapture`] is there to read that list off. So a field added to
/// the struct is governed the moment it exists.
///
/// This used to be a second hand-kept list, and it drifted: `modelTiers` lived
/// on the struct while governance called it an unknown key, which failed every
/// session start whose init carried a tier map.
fn engine_config_wire_keys() -> &'static [&'static str] {
    static KEYS: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
    KEYS.get_or_init(|| {
        let mut keys = Vec::new();
        // Always `Err`: the capture never builds a config, it only intercepts
        // the field list on the way in.
        let _ = EngineConfig::deserialize(FieldNameCapture { keys: &mut keys });
        assert!(
            !keys.is_empty(),
            "serde must expose EngineConfig's field list; a #[serde(flatten)] field \
             would route the derive through deserialize_map and hide it"
        );
        keys
    })
    .as_slice()
}

/// A deserializer that answers one question — which field names does serde
/// expect for this struct — and then stops.
struct FieldNameCapture<'a> {
    keys: &'a mut Vec<&'static str>,
}

/// The capture's only outcome: it read what it came for, so it stops here.
#[derive(Debug)]
struct FieldNamesCaptured;

impl std::fmt::Display for FieldNamesCaptured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EngineConfig field names captured")
    }
}

impl std::error::Error for FieldNamesCaptured {}

impl serde::de::Error for FieldNamesCaptured {
    fn custom<T: std::fmt::Display>(_message: T) -> Self {
        Self
    }
}

impl<'de> serde::Deserializer<'de> for FieldNameCapture<'_> {
    type Error = FieldNamesCaptured;

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.keys.extend_from_slice(fields);
        Err(FieldNamesCaptured)
    }

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(FieldNamesCaptured)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map enum identifier ignored_any
    }
}

/// Explicit shapes for optional fields and fields whose default has a narrower
/// shape than their full type.
///
/// Serde hands over the key list, not the types, so a field that serializes to
/// nothing when unset (`Option`) leaves no value to read a shape from. Naming those
/// here keeps a wrong value reported with its key and layer instead of surfacing as
/// a bare `invalid type: string "many", expected u64`. Forgetting an entry costs
/// only the shape check, never governance: the key is still governed, and serde
/// still rejects the value.
///
/// A key whose type admits several shapes stays `Any`; approval_policy needs an
/// explicit override because its default is a string and granular policy is an object.
const OPTIONAL_KEY_SHAPES: &[(&str, ConfigValueKind)] = &[
    ("totalTokensReminderAfterUserTurn", ConfigValueKind::Bool),
    ("maxOutputTokens", ConfigValueKind::Integer),
    ("autoCompactWindow", ConfigValueKind::Integer),
    ("compactPrompt", ConfigValueKind::String),
    ("outputReserveTokens", ConfigValueKind::Integer),
    ("compactBufferTokens", ConfigValueKind::Integer),
    ("precomputeBufferFraction", ConfigValueKind::Any),
    ("approval_policy", ConfigValueKind::Any),
    ("sandbox_mode", ConfigValueKind::String),
    ("approvals_reviewer", ConfigValueKind::String),
    ("windows", ConfigValueKind::Object),
    ("approvalReviewPolicy", ConfigValueKind::String),
    ("approvalReviewExtraPolicy", ConfigValueKind::String),
    ("approvalReviewPolicyTemplate", ConfigValueKind::String),
    ("approvalReviewTranscriptMode", ConfigValueKind::String),
    ("maximumGoalTokenBudget", ConfigValueKind::Integer),
];

pub fn engine_config_governance_schema() -> ConfigGovernanceSchema {
    let mut defaults = serde_json::to_value(EngineConfig::default())
        .expect("default EngineConfig serializes to a JSON object");
    // Access has a sandbox-dependent default. Keeping it absent here lets
    // config readers distinguish an explicit choice from inherited policy.
    defaults
        .get_mut("webSearch")
        .and_then(serde_json::Value::as_object_mut)
        .expect("default web search configuration is an object")
        .remove("access");
    let defaults = defaults
        .as_object()
        .expect("default EngineConfig serializes to a JSON object");

    ConfigGovernanceSchema::new(engine_config_wire_keys().iter().map(|key| {
        let default_value = defaults.get(*key);
        let kind = OPTIONAL_KEY_SHAPES
            .iter()
            .find(|(named, _)| named == key)
            .map(|(_, kind)| *kind)
            .unwrap_or_else(|| engine_config_value_kind(default_value));
        let mut spec = ConfigKeySpec::new(*key, kind).managed_lockable();
        if let Some(default_value) = default_value {
            spec = spec.with_default(default_value.clone());
        }
        spec
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_object_parses_to_defaults() {
        let cfg: EngineConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg, EngineConfig::default());
        assert_eq!(cfg.model, DEFAULT_MODEL);
        assert_eq!(
            cfg.max_output_tokens, None,
            "unset means the model's own ceiling, resolved per turn"
        );
        assert_eq!(
            cfg.max_output_retry_tokens, None,
            "unset means the truncation ladder doubles the resolved budget"
        );
        assert_eq!(
            cfg.approval_policy,
            Some(crate::types::permission::ApprovalPolicy::OnRequest)
        );
        assert_eq!(
            cfg.sandbox_mode,
            Some(crate::types::permission::SandboxMode::ReadOnly)
        );
        assert_eq!(
            cfg.approvals_reviewer,
            Some(crate::types::permission::ApprovalsReviewer::User)
        );
        assert_eq!(
            cfg.sandbox_workspace_write,
            SandboxWorkspaceWrite::default()
        );
        assert!(cfg.thinking);
        assert!(cfg.workflow);
        assert!(cfg.swarm);
        assert!(cfg.cron);
        assert!(cfg.agent_memory);
        assert!(!cfg.memory_recall);
        assert_eq!(cfg.compact_threshold, None);
        assert_eq!(cfg.compact_buffer_tokens, 0);
        assert_eq!(cfg.auto_compact_scope, AutoCompactScope::Total);
        assert_eq!(cfg.post_turn_compact_threshold_percent, 0);
        assert_eq!(cfg.compaction_stream_max_retries, 5);
        assert_eq!(cfg.compact_prompt, None);
        assert!(!cfg.history_snip);
        assert_eq!(cfg.total_tokens_reminder, TotalTokensReminderMode::Off);
        assert_eq!(cfg.total_tokens_reminder_budget, 15_000_000);
        assert!(!cfg.idle_microcompact);
        assert_eq!(cfg.idle_microcompact_gap_minutes, 60);
        assert_eq!(cfg.idle_microcompact_keep_recent, 5);
        assert!(cfg.precompute_compaction);
        assert!(cfg.manage_thinking_context);
        assert!(cfg.memory.enabled);
        assert!(cfg.mcp_catalog.enabled);
        assert_eq!(cfg.dream.min_hours, 24.0);
        assert_eq!(cfg.dream.min_sessions, 5);
        assert!(matches!(cfg.web_search.mode, WebSearchMode::Client));
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let cfg: EngineConfig = serde_json::from_value(json!({
            "model": "x",
            "memory": { "user": false },
            "mcpCatalog": { "enabled": false },
            "dream": { "minHours": 0.5 },
            "sandbox_mode": "workspace-write",
            "sandbox_workspace_write": { "network_access": true }
        }))
        .unwrap();
        assert_eq!(cfg.model, "x");
        assert_eq!(
            cfg.sandbox_mode,
            Some(crate::types::permission::SandboxMode::WorkspaceWrite)
        );
        assert!(cfg.sandbox_workspace_write.network_access);
        assert!(!cfg.sandbox_workspace_write.exclude_slash_tmp);
        assert!(!cfg.memory.user);
        assert!(!cfg.mcp_catalog.enabled);
        assert_eq!(cfg.dream.min_hours, 0.5);
        assert_eq!(cfg.dream.min_sessions, 5);
        // untouched nested defaults survive
        assert!(cfg.memory.enabled);
        assert!(cfg.memory.project);
    }

    #[test]
    fn context_management_parses_camel_case_and_defaults_to_absent() {
        let cfg: EngineConfig = serde_json::from_value(json!({})).unwrap();
        assert!(cfg.context_management.is_none());

        let cfg: EngineConfig = serde_json::from_value(json!({
            "contextManagement": {
                "triggerInputTokens": 30000,
                "keepToolUses": 3,
                "clearAtLeastInputTokens": 5000,
                "clearToolInputs": true,
                "excludeTools": ["web_search"]
            }
        }))
        .unwrap();
        let cm = cfg.context_management.expect("contextManagement parsed");
        assert_eq!(cm.trigger_input_tokens, Some(30_000));
        assert_eq!(cm.keep_tool_uses, Some(3));
        assert_eq!(cm.clear_at_least_input_tokens, Some(5_000));
        assert_eq!(cm.clear_tool_inputs, Some(ClearToolInputs::All(true)));
        assert_eq!(cm.exclude_tools, vec![RuntimeString::from("web_search")]);
    }

    /// Governance covers every field of the struct, because the key set IS the
    /// struct's. Regression guard for the tier map: `modelTiers` existed on
    /// `EngineConfig` while the schema called it an unknown key, so every
    /// session start whose init carried a tier map died with
    /// "unknown engine config key 'modelTiers' from Project".
    #[test]
    fn every_engine_config_field_is_governed() {
        let schema = engine_config_governance_schema();
        let defaults = serde_json::to_value(EngineConfig::default()).unwrap();
        for key in defaults.as_object().expect("config is an object").keys() {
            assert!(schema.spec(key).is_some(), "{key} is not governed");
        }
        // The fields serde skips when unset are absent from the serialized
        // defaults, so name them: they are exactly the ones a key set derived
        // from the defaults document would have dropped.
        for key in [
            "modelTiers",
            "contextManagement",
            "codeModeHost",
            "codeModeHostRequired",
            "effort",
            "taskBudgetTokens",
            "compactThreshold",
            "compactPrompt",
            "approvalTimeoutMs",
            "maxBudgetUsd",
            "webFetch",
        ] {
            assert!(schema.spec(key).is_some(), "{key} is not governed");
        }
    }

    /// The key-level shape is read off the struct's own default, so a wrong
    /// shape is still reported with its key and its layer instead of surfacing
    /// as a bare serde type error with no idea where the value came from.
    #[test]
    fn value_shapes_come_from_the_struct_defaults() {
        let schema = engine_config_governance_schema();
        let kind = |key: &str| schema.spec(key).expect("governed").kind;

        assert_eq!(kind("model"), ConfigValueKind::String);
        assert_eq!(kind("thinking"), ConfigValueKind::Bool);
        assert_eq!(kind("allow"), ConfigValueKind::Array);
        assert_eq!(kind("modelTiers"), ConfigValueKind::Object);
        // Serde omits an unset Option, so those keys have no default to read a
        // shape from and stay `Any` unless their type admits exactly one shape and
        // `OPTIONAL_KEY_SHAPES` names it.
        assert_eq!(kind("effort"), ConfigValueKind::Any);
        assert_eq!(kind("contextManagement"), ConfigValueKind::Any);
        assert_eq!(
            kind("maxOutputTokens"),
            ConfigValueKind::Integer,
            "unset means the model's ceiling, but a value the host DOES send is still a number"
        );

        let error = crate::engine::session::resolve_config(json!({
            "maxOutputTokens": "many",
        }))
        .expect_err("a wrong shape is rejected");
        assert!(error.contains("maxOutputTokens"), "{error}");
        assert!(error.contains("Integer"), "{error}");
    }

    /// The host's tier map resolves instead of being rejected — the session
    /// init the desktop shell and the app-server both send.
    #[test]
    fn host_model_tiers_resolve() {
        let cfg = crate::engine::session::resolve_config(json!({
            "modelTiers": { "high": "big-model", "low": "small-model" },
        }))
        .expect("a tier map is a governed key");
        assert_eq!(cfg.model_tiers.high, Some(RuntimeString::from("big-model")));
        assert_eq!(
            cfg.model_tiers.low,
            Some(RuntimeString::from("small-model"))
        );
        assert_eq!(cfg.model_tiers.medium, None);
    }

    /// The master switch gates its children instead of refusing them: a GUI that
    /// turns agent memory off leaves the child rows it wrote earlier in the file,
    /// and that must resolve to "subsystem off", not to a config error.
    #[test]
    fn agent_memory_master_switch_gates_its_children() {
        let off = crate::engine::session::resolve_config(json!({
            "agentMemory": false,
            "memoryRecall": true,
            "autoMemory": true,
            "autoDream": true,
        }))
        .expect("a disabled subsystem is not a config error");
        assert!(!off.memory_recall);
        assert!(!off.auto_memory);
        assert!(!off.auto_dream);

        let on = crate::engine::session::resolve_config(json!({
            "agentMemory": true,
            "memoryRecall": true,
        }))
        .expect("an enabled subsystem keeps its children");
        assert!(on.memory_recall);
    }

    #[test]
    fn direct_config_decode_rejects_retired_controls_and_implicit_aliases() {
        for (key, value) in [
            ("sandbox", json!("process")),
            ("requireRealSandbox", json!(true)),
            ("sandboxRetryOnDenial", json!(true)),
            ("confineReads", json!(true)),
            ("additionalWritableRoots", json!(["/tmp"])),
            ("additionalReadableRoots", json!(["/tmp"])),
            ("sandboxNetworkAllowed", json!(true)),
            ("approvalPolicy", json!("never")),
            ("approvalsReviewer", json!("user")),
            ("sandboxMode", json!("danger-full-access")),
        ] {
            let error = serde_json::from_value::<EngineConfig>(json!({key: value}))
                .expect_err("legacy inputs must use the explicit migration boundary");
            assert!(error.to_string().contains(key), "{error}");
        }
    }

    #[test]
    fn dream_config_validates_desktop_bounds_and_integer_session_counts() {
        for (hours, sessions) in [(0.01, 1), (720.0, 100)] {
            let cfg = EngineConfig {
                dream: DreamConfig {
                    min_hours: hours,
                    min_sessions: sessions,
                },
                ..EngineConfig::default()
            };
            assert!(cfg.validate().is_ok(), "{hours}h/{sessions} sessions");
        }

        for hours in [0.0, 720.01, f64::NAN, f64::INFINITY] {
            let cfg = EngineConfig {
                dream: DreamConfig {
                    min_hours: hours,
                    ..DreamConfig::default()
                },
                ..EngineConfig::default()
            };
            assert!(cfg.validate().is_err(), "invalid minHours {hours}");
        }

        for sessions in [0, 101] {
            let cfg = EngineConfig {
                dream: DreamConfig {
                    min_sessions: sessions,
                    ..DreamConfig::default()
                },
                ..EngineConfig::default()
            };
            assert!(cfg.validate().is_err(), "invalid minSessions {sessions}");
        }

        let fractional = serde_json::from_value::<EngineConfig>(json!({
            "dream": { "minSessions": 1.5 }
        }));
        assert!(fractional.is_err());
    }

    #[test]
    fn dream_interval_uses_validated_fractional_hours() {
        let dream = DreamConfig {
            min_hours: 0.01,
            min_sessions: 1,
        };
        assert_eq!(dream.min_interval_ms(), 36_000);
    }

    #[test]
    fn model_tiers_parse_camel_case_and_default_to_absent() {
        let cfg: EngineConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.model_tiers, ModelTiers::default());

        let cfg: EngineConfig = serde_json::from_value(json!({
            "modelTiers": { "high": "big-model", "low": "small-model" }
        }))
        .unwrap();
        assert_eq!(cfg.model_tiers.high, Some(RuntimeString::from("big-model")));
        assert_eq!(cfg.model_tiers.medium, None);
        assert_eq!(
            cfg.model_tiers.low,
            Some(RuntimeString::from("small-model"))
        );
    }

    #[test]
    fn model_for_tier_falls_back_to_the_main_model() {
        let mut cfg = EngineConfig {
            model: "main-model".into(),
            ..EngineConfig::default()
        };
        assert_eq!(cfg.model_for_tier(ModelTier::High), "main-model");
        assert_eq!(cfg.model_for_tier(ModelTier::Low), "main-model");

        cfg.model_tiers.low = Some("small-model".into());
        assert_eq!(cfg.model_for_tier(ModelTier::Low), "small-model");
        assert_eq!(cfg.model_for_tier(ModelTier::Medium), "main-model");

        // An empty entry is treated as unset.
        cfg.model_tiers.high = Some(RuntimeString::default());
        assert_eq!(cfg.model_for_tier(ModelTier::High), "main-model");
    }

    #[test]
    fn resolve_model_selection_maps_tiers() {
        let cfg = EngineConfig {
            model: "main-model".into(),
            model_tiers: ModelTiers {
                high: Some("tier-high".into()),
                low: Some("tier-low".into()),
                ..ModelTiers::default()
            },
            ..EngineConfig::default()
        };

        // Tiers resolve through the map.
        assert_eq!(cfg.resolve_model_selection(&"high".into()), "tier-high");
        assert_eq!(cfg.resolve_model_selection(&"low".into()), "tier-low");
        // An unconfigured tier falls back to the main model.
        assert_eq!(cfg.resolve_model_selection(&"medium".into()), "main-model");
        // Retired cc-era aliases are plain ids now, not tier names.
        assert_eq!(cfg.resolve_model_selection(&"opus".into()), "opus");
        assert_eq!(cfg.resolve_model_selection(&"sonnet".into()), "sonnet");
        assert_eq!(cfg.resolve_model_selection(&"haiku".into()), "haiku");
        // A concrete id passes through; an empty selection means the main model.
        assert_eq!(
            cfg.resolve_model_selection(&"some-other-model".into()),
            "some-other-model"
        );
        assert_eq!(
            cfg.resolve_model_selection(&RuntimeString::default()),
            "main-model"
        );
    }
}
