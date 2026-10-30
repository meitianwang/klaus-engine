// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Modified to load native administrator requirements and bind runtime review policy.

//! Administrator review requirements are loaded independently of ordinary configuration.

use super::approval_context::ManagedReviewPolicy;
use crate::types::permission::{ApprovalPolicy, SandboxMode};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use toml_edit::{DocumentMut, Item};

#[cfg(any(target_os = "macos", test))]
const PREFERENCES_DOMAIN: &str = "com.klaus.desktop";
#[cfg(any(target_os = "macos", test))]
const PREFERENCES_KEY: &str = "requirements_toml_base64";
#[cfg(any(target_os = "macos", test))]
const LEGACY_PREFERENCES_KEY: &str = "config_toml_base64";

/// A source admitted through an administrator-owned filesystem or native trust boundary.
/// This type deliberately has no wire decoder or configurable native path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedRequirementOrigin {
    SystemFile { path: PathBuf },
    LegacyManagedFile { path: PathBuf },
    TrustedBundle { id: String, name: String },
    ForcedPreference { domain: String, key: String },
}

impl std::fmt::Display for ManagedRequirementOrigin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SystemFile { path } | Self::LegacyManagedFile { path } => {
                write!(formatter, "{}", path.display())
            }
            Self::TrustedBundle { id, name } => write!(formatter, "trusted bundle {name} ({id})"),
            Self::ForcedPreference { domain, key } => write!(formatter, "{domain}:{key}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedReviewer {
    User,
    AutoReview,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedApprovalPolicy {
    OnRequest,
    Never,
    UnlessTrusted,
    Granular {
        sandbox_approval: bool,
        rules: bool,
        skill_approval: bool,
        request_permissions: bool,
        mcp_elicitations: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedSandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
    ExternalSandbox,
}

/// Source attribution follows effective values rather than a user-supplied managed label.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManagedReviewSources {
    pub policy: Option<ManagedRequirementOrigin>,
    pub extra_policy: Option<ManagedRequirementOrigin>,
    pub allowed_reviewers: Option<ManagedRequirementOrigin>,
    pub allowed_approval_policies: Option<ManagedRequirementOrigin>,
    pub allowed_sandbox_modes: Option<ManagedRequirementOrigin>,
    pub required_models: Vec<ManagedRequirementOrigin>,
    pub ignore_rules: Option<ManagedRequirementOrigin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRequirementLayerRecord {
    pub origin: ManagedRequirementOrigin,
    pub declared_review_fields: Vec<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedReviewRequirements {
    pub policy: ManagedReviewPolicy,
    pub required_models: Vec<String>,
    pub allows_user_reviewer: bool,
    /// Derived from the reviewer constraint; this is not an independent TOML setting.
    pub force_sync: bool,
    pub allowed_reviewers: Option<Vec<ManagedReviewer>>,
    pub allowed_approval_policies: Option<Vec<ManagedApprovalPolicy>>,
    pub allowed_sandbox_modes: Option<Vec<ManagedSandboxMode>>,
    pub ignore_rules: Option<Vec<String>>,
    pub sources: ManagedReviewSources,
    /// Layers remain ordered from lowest to highest precedence.
    pub layers: Vec<ManagedRequirementLayerRecord>,
}

impl Default for ManagedReviewRequirements {
    fn default() -> Self {
        Self {
            policy: ManagedReviewPolicy::default(),
            required_models: Vec::new(),
            allows_user_reviewer: true,
            force_sync: false,
            allowed_reviewers: None,
            allowed_approval_policies: None,
            allowed_sandbox_modes: None,
            ignore_rules: None,
            sources: ManagedReviewSources::default(),
            layers: Vec::new(),
        }
    }
}

impl ManagedReviewRequirements {
    /// A single valid provider namespace may prefix an exact, case-sensitive slug.
    pub fn requires_review_for_model(&self, action_model: &str) -> bool {
        let slug = match action_model.split_once('/') {
            Some((namespace, suffix))
                if !namespace.is_empty()
                    && !suffix.contains('/')
                    && namespace.chars().all(|character| {
                        character.is_ascii_alphanumeric() || character == '_' || character == '-'
                    }) =>
            {
                suffix
            }
            Some(_) => return false,
            None => action_model,
        };
        self.required_models.iter().any(|required| required == slug)
    }

    pub fn review_required(&self, action_model: &str) -> bool {
        self.force_sync || self.requires_review_for_model(action_model)
    }

    /// Apply at startup and to live changes. A protected model does not itself
    /// disallow selecting the user reviewer; it separately requires assessment.
    pub fn validate_reviewer(&self, reviewer: ManagedReviewer) -> io::Result<()> {
        if self
            .allowed_reviewers
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&reviewer))
        {
            let source = self
                .sources
                .allowed_reviewers
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "administrator requirements".to_string());
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("approvals reviewer {reviewer:?} is disallowed by {source}"),
            ));
        }
        Ok(())
    }

    /// The first constrained reviewer is the managed default when one is needed.
    pub fn preferred_reviewer(&self) -> Option<ManagedReviewer> {
        self.allowed_reviewers
            .as_ref()
            .and_then(|allowed| allowed.first().copied())
    }

    pub fn validate_approval_policy(&self, policy: ApprovalPolicy) -> io::Result<()> {
        let candidate = match policy {
            ApprovalPolicy::OnRequest => ManagedApprovalPolicy::OnRequest,
            ApprovalPolicy::Never => ManagedApprovalPolicy::Never,
            ApprovalPolicy::UnlessTrusted => ManagedApprovalPolicy::UnlessTrusted,
            ApprovalPolicy::Granular(config) => ManagedApprovalPolicy::Granular {
                sandbox_approval: config.sandbox_approval,
                rules: config.rules,
                skill_approval: config.skill_approval,
                request_permissions: config.request_permissions,
                mcp_elicitations: config.mcp_elicitations,
            },
        };
        if self
            .allowed_approval_policies
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&candidate))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "approval policy is disallowed by administrator requirements",
            ));
        }
        Ok(())
    }

    pub fn preferred_approval_policy(&self) -> Option<ManagedApprovalPolicy> {
        self.allowed_approval_policies
            .as_ref()
            .and_then(|allowed| allowed.first().copied())
    }

    pub fn validate_sandbox_mode(&self, mode: SandboxMode) -> io::Result<()> {
        let candidate = match mode {
            SandboxMode::ReadOnly => ManagedSandboxMode::ReadOnly,
            SandboxMode::WorkspaceWrite => ManagedSandboxMode::WorkspaceWrite,
            SandboxMode::DangerFullAccess => ManagedSandboxMode::DangerFullAccess,
        };
        if self
            .allowed_sandbox_modes
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&candidate))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sandbox mode is disallowed by administrator requirements",
            ));
        }
        Ok(())
    }
}

/// A host-trusted provider may add authenticated requirements bundles. Ordinary
/// session configuration cannot create this provider or replace native sources.
pub trait TrustedRequirementsBundleProvider: Send + Sync {
    fn load(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<TrustedBundleRequirements>>> + Send + '_>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedBundleRequirements {
    pub id: String,
    pub name: String,
    pub toml: String,
}

pub async fn load_managed_review_requirements() -> io::Result<ManagedReviewRequirements> {
    tokio::task::spawn_blocking(load_managed_review_requirements_sync)
        .await
        .map_err(|error| io::Error::other(format!("managed requirements load failed: {error}")))?
}

/// Synchronous admission for session construction before a runtime is active.
pub fn load_managed_review_requirements_sync() -> io::Result<ManagedReviewRequirements> {
    load_managed_review_requirements_with_bundles_sync(Vec::new())
}

/// Compose an already-admitted native provider snapshot with current administrator files.
pub fn load_managed_review_requirements_with_bundles_sync(
    bundles: Vec<TrustedBundleRequirements>,
) -> io::Result<ManagedReviewRequirements> {
    compose_native_layers(load_native_layers()?, bundles)
}

/// Reload on every policy admission. Errors must reach the caller; neither an
/// earlier successful snapshot nor an empty policy is a fallback for failed reads.
pub async fn load_managed_review_requirements_with_bundle(
    provider: Option<&dyn TrustedRequirementsBundleProvider>,
) -> io::Result<ManagedReviewRequirements> {
    // Trusted bundle acquisition precedes native reads, while its layer priority
    // remains above the system file and below forced preferences.
    let Some(provider) = provider else {
        return load_managed_review_requirements().await;
    };
    let bundles = provider.load().await?;
    let native = tokio::task::spawn_blocking(load_native_layers)
        .await
        .map_err(|error| {
            io::Error::other(format!("managed requirements load failed: {error}"))
        })??;
    compose_native_layers(native, bundles)
}

#[derive(Clone, Debug)]
struct LoadedLayer {
    origin: ManagedRequirementOrigin,
    toml: String,
}

#[derive(Default)]
struct ParsedLayer {
    policy: Option<String>,
    extra_policy: Option<String>,
    reviewers: Option<Vec<ManagedReviewer>>,
    approval_policies: Option<Vec<ManagedApprovalPolicy>>,
    sandbox_modes: Option<Vec<ManagedSandboxMode>>,
    models: Option<Vec<String>>,
    ignore_rules: Option<Vec<String>>,
    declared: Vec<&'static str>,
}

fn invalid(origin: &ManagedRequirementOrigin, message: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid managed review requirements from {origin}: {message}"),
    )
}

fn parse_string(
    item: Option<&Item>,
    field: &str,
    origin: &ManagedRequirementOrigin,
) -> io::Result<Option<String>> {
    item.map(|value| {
        value
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| invalid(origin, format!("{field} must be a string")))
    })
    .transpose()
}

fn parse_strings(
    item: Option<&Item>,
    field: &str,
    origin: &ManagedRequirementOrigin,
) -> io::Result<Option<Vec<String>>> {
    item.map(|value| {
        let array = value
            .as_array()
            .ok_or_else(|| invalid(origin, format!("{field} must be an array of strings")))?;
        array
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| invalid(origin, format!("{field} must contain only strings")))
            })
            .collect()
    })
    .transpose()
}

fn parse_approval_policy(
    item: &Item,
    origin: &ManagedRequirementOrigin,
) -> io::Result<ManagedApprovalPolicy> {
    match item.as_str() {
        Some("on-request" | "on-failure") => return Ok(ManagedApprovalPolicy::OnRequest),
        Some("never") => return Ok(ManagedApprovalPolicy::Never),
        Some("untrusted") => return Ok(ManagedApprovalPolicy::UnlessTrusted),
        Some(_) => return Err(invalid(origin, "unknown approval policy")),
        None => {}
    }
    let wrapper = item.as_table_like().ok_or_else(|| {
        invalid(
            origin,
            "approval policy must be a policy name or granular table",
        )
    })?;
    if wrapper.len() != 1 {
        return Err(invalid(
            origin,
            "granular approval policy must have one variant",
        ));
    }
    let granular = wrapper
        .get("granular")
        .and_then(Item::as_table_like)
        .ok_or_else(|| invalid(origin, "unknown approval policy variant"))?;
    let boolean = |field: &str, default: Option<bool>| -> io::Result<bool> {
        match granular.get(field) {
            Some(value) => value
                .as_bool()
                .ok_or_else(|| invalid(origin, format!("granular.{field} must be boolean"))),
            None => default.ok_or_else(|| invalid(origin, format!("granular.{field} is required"))),
        }
    };
    Ok(ManagedApprovalPolicy::Granular {
        sandbox_approval: boolean("sandbox_approval", None)?,
        rules: boolean("rules", None)?,
        skill_approval: boolean("skill_approval", Some(false))?,
        request_permissions: boolean("request_permissions", Some(false))?,
        mcp_elicitations: boolean("mcp_elicitations", None)?,
    })
}

fn parse_approval_policies(
    item: Option<&Item>,
    origin: &ManagedRequirementOrigin,
) -> io::Result<Option<Vec<ManagedApprovalPolicy>>> {
    item.map(|item| {
        if let Some(array) = item.as_array() {
            array
                .iter()
                .map(|value| parse_approval_policy(&Item::Value(value.clone()), origin))
                .collect()
        } else if let Some(tables) = item.as_array_of_tables() {
            tables
                .iter()
                .map(|table| parse_approval_policy(&Item::Table(table.clone()), origin))
                .collect()
        } else {
            Err(invalid(
                origin,
                "allowed_approval_policies must be an array",
            ))
        }
    })
    .transpose()
}

fn parse_sandbox_mode(
    value: &str,
    origin: &ManagedRequirementOrigin,
) -> io::Result<ManagedSandboxMode> {
    match value {
        "read-only" => Ok(ManagedSandboxMode::ReadOnly),
        "workspace-write" => Ok(ManagedSandboxMode::WorkspaceWrite),
        "danger-full-access" => Ok(ManagedSandboxMode::DangerFullAccess),
        "external-sandbox" => Ok(ManagedSandboxMode::ExternalSandbox),
        _ => Err(invalid(origin, "unknown sandbox mode")),
    }
}

fn parse_layer(layer: &LoadedLayer) -> io::Result<ParsedLayer> {
    let document = layer
        .toml
        .parse::<DocumentMut>()
        .map_err(|error| invalid(&layer.origin, error.message()))?;
    let mut parsed = ParsedLayer {
        policy: parse_string(
            document.get("guardian_policy_config"),
            "guardian_policy_config",
            &layer.origin,
        )?,
        extra_policy: parse_string(
            document.get("guardian_extra_policy"),
            "guardian_extra_policy",
            &layer.origin,
        )?,
        ..ParsedLayer::default()
    };
    for field in [
        "guardian_policy_config",
        "guardian_extra_policy",
        "allowed_approvals_reviewers",
        "allowed_approval_policies",
        "allowed_sandbox_modes",
    ] {
        if document.contains_key(field) {
            parsed.declared.push(field);
        }
    }
    parsed.reviewers = parse_strings(
        document.get("allowed_approvals_reviewers"),
        "allowed_approvals_reviewers",
        &layer.origin,
    )?
    .map(|values| {
        values
            .into_iter()
            .map(|value| match value.as_str() {
                "user" => Ok(ManagedReviewer::User),
                "auto_review" | "guardian_subagent" => Ok(ManagedReviewer::AutoReview),
                _ => Err(invalid(
                    &layer.origin,
                    "allowed_approvals_reviewers contains an unknown reviewer",
                )),
            })
            .collect::<io::Result<Vec<_>>>()
    })
    .transpose()?;
    parsed.approval_policies =
        parse_approval_policies(document.get("allowed_approval_policies"), &layer.origin)?;
    parsed.sandbox_modes = parse_strings(
        document.get("allowed_sandbox_modes"),
        "allowed_sandbox_modes",
        &layer.origin,
    )?
    .map(|values| {
        values
            .into_iter()
            .map(|value| parse_sandbox_mode(&value, &layer.origin))
            .collect::<io::Result<Vec<_>>>()
    })
    .transpose()?;
    if let Some(item) = document.get("auto_review") {
        let table = item
            .as_table_like()
            .ok_or_else(|| invalid(&layer.origin, "auto_review must be a table"))?;
        parsed.models = parse_strings(
            table.get("required_on_models"),
            "auto_review.required_on_models",
            &layer.origin,
        )?;
        parsed.ignore_rules = parse_strings(
            table.get("ignore_rules"),
            "auto_review.ignore_rules",
            &layer.origin,
        )?;
        if parsed.models.is_some() {
            parsed.declared.push("auto_review.required_on_models");
        }
        if parsed.ignore_rules.is_some() {
            parsed.declared.push("auto_review.ignore_rules");
        }
    }
    Ok(parsed)
}

#[cfg(test)]
fn compose_loaded_layers(
    system: Option<LoadedLayer>,
    bundles: Vec<TrustedBundleRequirements>,
    forced: Option<LoadedLayer>,
) -> io::Result<ManagedReviewRequirements> {
    compose_layers(
        system
            .into_iter()
            .chain(bundles.into_iter().map(|bundle| LoadedLayer {
                origin: ManagedRequirementOrigin::TrustedBundle {
                    id: bundle.id,
                    name: bundle.name,
                },
                toml: bundle.toml,
            }))
            .chain(forced),
    )
}

pub(crate) fn validate_trusted_bundle_requirements(
    bundles: Vec<TrustedBundleRequirements>,
) -> io::Result<()> {
    compose_layers(bundles.into_iter().map(|bundle| LoadedLayer {
        origin: ManagedRequirementOrigin::TrustedBundle {
            id: bundle.id,
            name: bundle.name,
        },
        toml: bundle.toml,
    }))
    .map(|_| ())
}

fn compose_layers(
    layers: impl IntoIterator<Item = LoadedLayer>,
) -> io::Result<ManagedReviewRequirements> {
    let layers = layers
        .into_iter()
        .map(|layer| parse_layer(&layer).map(|parsed| (layer.origin, parsed)))
        .collect::<io::Result<Vec<_>>>()?;
    let mut output = ManagedReviewRequirements::default();
    // Ordinary values replace in priority order, including explicit empty values.
    for (origin, layer) in &layers {
        if let Some(policy) = &layer.policy {
            output.policy.policy = Some(policy.clone());
            output.sources.policy = Some(origin.clone());
        }
        if let Some(policy) = &layer.extra_policy {
            output.policy.extra_policy = Some(policy.clone());
            output.sources.extra_policy = Some(origin.clone());
        }
        if let Some(reviewers) = &layer.reviewers {
            output.allowed_reviewers = Some(reviewers.clone());
            output.sources.allowed_reviewers = Some(origin.clone());
        }
        if let Some(policies) = &layer.approval_policies {
            output.allowed_approval_policies = Some(policies.clone());
            output.sources.allowed_approval_policies = Some(origin.clone());
        }
        if let Some(modes) = &layer.sandbox_modes {
            output.allowed_sandbox_modes = Some(modes.clone());
            output.sources.allowed_sandbox_modes = Some(origin.clone());
        }
        if let Some(rules) = &layer.ignore_rules {
            output.ignore_rules = Some(rules.clone());
            output.sources.ignore_rules = Some(origin.clone());
        }
        output.layers.push(ManagedRequirementLayerRecord {
            origin: origin.clone(),
            declared_review_fields: layer.declared.clone(),
        });
    }
    // Model requirements are a priority-ordered union; a higher layer cannot
    // remove protected slugs by supplying an empty or narrower list.
    for (origin, layer) in layers.iter().rev() {
        for slug in layer.models.iter().flatten() {
            if slug.trim().is_empty() || slug.trim() != slug || slug.contains('/') {
                return Err(invalid(
                    origin,
                    "auto_review.required_on_models must contain nonempty canonical model slugs without surrounding whitespace or '/'",
                ));
            }
            if !output.required_models.contains(slug) {
                output.required_models.push(slug.clone());
                if !output.sources.required_models.contains(origin) {
                    output.sources.required_models.push(origin.clone());
                }
            }
        }
    }
    if output.allowed_reviewers.as_ref().is_some_and(Vec::is_empty) {
        return Err(invalid(
            output
                .sources
                .allowed_reviewers
                .as_ref()
                .expect("reviewer source"),
            "allowed_approvals_reviewers must not be empty",
        ));
    }
    output.allows_user_reviewer = output
        .allowed_reviewers
        .as_ref()
        .is_none_or(|allowed| allowed.contains(&ManagedReviewer::User));
    output.force_sync = !output.allows_user_reviewer;
    if output
        .allowed_approval_policies
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        return Err(invalid(
            output
                .sources
                .allowed_approval_policies
                .as_ref()
                .expect("approval policy source"),
            "allowed_approval_policies must not be empty",
        ));
    }
    if output
        .allowed_sandbox_modes
        .as_ref()
        .is_some_and(|modes| !modes.contains(&ManagedSandboxMode::ReadOnly))
    {
        return Err(invalid(
            output
                .sources
                .allowed_sandbox_modes
                .as_ref()
                .expect("sandbox source"),
            "allowed_sandbox_modes must include read-only",
        ));
    }
    if output
        .policy
        .policy
        .as_ref()
        .is_some_and(|policy| policy.trim().is_empty())
    {
        output.policy.policy = None;
        output.sources.policy = None;
    }
    if output
        .policy
        .extra_policy
        .as_ref()
        .is_some_and(|policy| policy.trim().is_empty())
    {
        output.policy.extra_policy = None;
        output.sources.extra_policy = None;
    }
    Ok(output)
}

fn load_system_layer(path: &Path) -> io::Result<Option<LoadedLayer>> {
    match std::fs::read_to_string(path) {
        Ok(toml) => Ok(Some(LoadedLayer {
            origin: ManagedRequirementOrigin::SystemFile {
                path: path.to_path_buf(),
            },
            toml,
        })),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!(
                "failed to read managed requirements {}: {error}",
                path.display()
            ),
        )),
    }
}

struct NativeLayers {
    system: Option<LoadedLayer>,
    legacy_file: Option<LoadedLayer>,
    legacy_forced: Option<LoadedLayer>,
    forced: Option<LoadedLayer>,
}

fn compose_native_layers(
    native: NativeLayers,
    bundles: Vec<TrustedBundleRequirements>,
) -> io::Result<ManagedReviewRequirements> {
    compose_layers(
        native
            .system
            .into_iter()
            .chain(bundles.into_iter().map(|bundle| LoadedLayer {
                origin: ManagedRequirementOrigin::TrustedBundle {
                    id: bundle.id,
                    name: bundle.name,
                },
                toml: bundle.toml,
            }))
            .chain(native.legacy_file)
            .chain(native.legacy_forced)
            .chain(native.forced),
    )
}

#[cfg(any(unix, test))]
fn render_approval_policy(policy: ManagedApprovalPolicy) -> String {
    match policy {
        ManagedApprovalPolicy::OnRequest => "'on-request'".to_string(),
        ManagedApprovalPolicy::Never => "'never'".to_string(),
        ManagedApprovalPolicy::UnlessTrusted => "'untrusted'".to_string(),
        ManagedApprovalPolicy::Granular { sandbox_approval, rules, skill_approval, request_permissions, mcp_elicitations } => format!(
            "{{ granular = {{ sandbox_approval = {sandbox_approval}, rules = {rules}, skill_approval = {skill_approval}, request_permissions = {request_permissions}, mcp_elicitations = {mcp_elicitations} }} }}"
        ),
    }
}

#[cfg(any(unix, test))]
fn project_legacy_layer(layer: LoadedLayer) -> io::Result<LoadedLayer> {
    let document = layer
        .toml
        .parse::<DocumentMut>()
        .map_err(|error| invalid(&layer.origin, error.message()))?;
    let mut projected = String::new();
    if let Some(item) = document.get("approval_policy") {
        let policy = parse_approval_policy(item, &layer.origin)?;
        projected.push_str(&format!(
            "allowed_approval_policies = [{}]\n",
            render_approval_policy(policy)
        ));
    }
    if let Some(reviewer) = parse_string(
        document.get("approvals_reviewer"),
        "approvals_reviewer",
        &layer.origin,
    )? {
        let values = match reviewer.as_str() {
            "user" => "['user']",
            "auto_review" | "guardian_subagent" => "['auto_review', 'user']",
            _ => return Err(invalid(&layer.origin, "unknown approvals reviewer")),
        };
        projected.push_str(&format!("allowed_approvals_reviewers = {values}\n"));
    }
    if let Some(mode) = parse_string(document.get("sandbox_mode"), "sandbox_mode", &layer.origin)? {
        match parse_sandbox_mode(&mode, &layer.origin)? {
            ManagedSandboxMode::ReadOnly => {
                projected.push_str("allowed_sandbox_modes = ['read-only']\n")
            }
            ManagedSandboxMode::WorkspaceWrite => {
                projected.push_str("allowed_sandbox_modes = ['read-only', 'workspace-write']\n")
            }
            ManagedSandboxMode::DangerFullAccess => {
                projected.push_str("allowed_sandbox_modes = ['read-only', 'danger-full-access']\n")
            }
            ManagedSandboxMode::ExternalSandbox => {
                return Err(invalid(
                    &layer.origin,
                    "external-sandbox is not a native managed sandbox_mode",
                ))
            }
        }
    }
    Ok(LoadedLayer {
        origin: layer.origin,
        toml: projected,
    })
}

#[cfg(unix)]
fn legacy_managed_path() -> PathBuf {
    PathBuf::from("/etc/klaus/managed_config.toml")
}

#[cfg(unix)]
fn load_legacy_file(path: &Path) -> io::Result<Option<LoadedLayer>> {
    load_system_layer(path)?
        .map(|mut layer| {
            layer.origin = ManagedRequirementOrigin::LegacyManagedFile {
                path: path.to_path_buf(),
            };
            project_legacy_layer(layer)
        })
        .transpose()
}

fn load_native_layers() -> io::Result<NativeLayers> {
    // Forced preferences are checked first; failure must not fall through to a
    // lower-priority system file.
    let forced = load_forced_layer()?;
    let system = load_system_layer(&system_requirements_path())?;
    #[cfg(unix)]
    let legacy_file = load_legacy_file(&legacy_managed_path())?;
    // The deprecated user-home managed file was never a native administrator
    // source on Windows. Do not promote a user-writable file into requirements.
    #[cfg(not(unix))]
    let legacy_file = None;
    #[cfg(target_os = "macos")]
    let legacy_forced = load_forced_layer_for_key(LEGACY_PREFERENCES_KEY)?
        .map(project_legacy_layer)
        .transpose()?;
    #[cfg(not(target_os = "macos"))]
    let legacy_forced = None;
    Ok(NativeLayers {
        system,
        legacy_file,
        legacy_forced,
        forced,
    })
}

#[cfg(not(windows))]
fn system_requirements_path() -> PathBuf {
    PathBuf::from("/etc/klaus/requirements.toml")
}

#[cfg(windows)]
fn system_requirements_path() -> PathBuf {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStringExt;

    #[repr(C)]
    struct Guid {
        data1: u32,
        data2: u16,
        data3: u16,
        data4: [u8; 8],
    }
    #[link(name = "shell32")]
    unsafe extern "system" {
        fn SHGetKnownFolderPath(
            folder: *const Guid,
            flags: u32,
            token: *mut c_void,
            path: *mut *mut u16,
        ) -> i32;
    }
    #[link(name = "ole32")]
    unsafe extern "system" {
        fn CoTaskMemFree(memory: *const c_void);
    }
    let folder = Guid {
        data1: 0x62ab5d82,
        data2: 0xfdc1,
        data3: 0x4dc3,
        data4: [0xa9, 0xdd, 0x07, 0x0d, 0x1d, 0x49, 0x5d, 0x97],
    };
    let mut pointer: *mut u16 = std::ptr::null_mut();
    let result = unsafe { SHGetKnownFolderPath(&folder, 0, std::ptr::null_mut(), &mut pointer) };
    let base = if result >= 0 && !pointer.is_null() {
        let mut length = 0;
        unsafe {
            while *pointer.add(length) != 0 {
                length += 1;
            }
        }
        let path =
            std::ffi::OsString::from_wide(unsafe { std::slice::from_raw_parts(pointer, length) });
        PathBuf::from(path)
    } else {
        tracing::warn!("Could not resolve the system ProgramData directory; using C:\\ProgramData");
        PathBuf::from(r"C:\ProgramData")
    };
    if !pointer.is_null() {
        unsafe { CoTaskMemFree(pointer.cast()) };
    }
    base.join("Klaus").join("requirements.toml")
}

#[cfg(not(target_os = "macos"))]
fn load_forced_layer() -> io::Result<Option<LoadedLayer>> {
    Ok(None)
}

#[cfg(test)]
fn load_forced_layer_with<T>(
    is_forced: impl FnMut() -> bool,
    copy_value: impl FnOnce() -> Option<T>,
    decode_string: impl FnOnce(T) -> io::Result<String>,
) -> io::Result<Option<LoadedLayer>> {
    load_forced_value_with(PREFERENCES_KEY, is_forced, copy_value, decode_string)
}

#[cfg(any(target_os = "macos", test))]
fn load_forced_value_with<T>(
    key: &str,
    mut is_forced: impl FnMut() -> bool,
    copy_value: impl FnOnce() -> Option<T>,
    decode_string: impl FnOnce(T) -> io::Result<String>,
) -> io::Result<Option<LoadedLayer>> {
    if !is_forced() {
        return Ok(None);
    }
    let Some(value) = copy_value() else {
        return Ok(None);
    };
    if !is_forced() {
        return Ok(None);
    }
    let encoded = decode_string(value)?;
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim().as_bytes())
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "managed requirements must be base64",
            )
        })?;
    let toml = String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "managed requirements must decode as UTF-8",
        )
    })?;
    Ok(Some(LoadedLayer {
        origin: ManagedRequirementOrigin::ForcedPreference {
            domain: PREFERENCES_DOMAIN.to_string(),
            key: key.to_string(),
        },
        toml,
    }))
}

#[cfg(target_os = "macos")]
fn load_forced_layer() -> io::Result<Option<LoadedLayer>> {
    load_forced_layer_for_key(PREFERENCES_KEY)
}

#[cfg(target_os = "macos")]
fn load_forced_layer_for_key(key_name: &str) -> io::Result<Option<LoadedLayer>> {
    use std::ffi::c_void;
    type CfRef = *const c_void;
    #[repr(C)]
    struct CfRange {
        location: isize,
        length: isize,
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFStringCreateWithBytes(
            allocator: CfRef,
            bytes: *const u8,
            length: isize,
            encoding: u32,
            external: u8,
        ) -> CfRef;
        fn CFPreferencesCopyAppValue(key: CfRef, application: CfRef) -> CfRef;
        fn CFPreferencesAppValueIsForced(key: CfRef, application: CfRef) -> u8;
        fn CFGetTypeID(value: CfRef) -> usize;
        fn CFStringGetTypeID() -> usize;
        fn CFStringGetLength(value: CfRef) -> isize;
        fn CFStringGetCharacters(value: CfRef, range: CfRange, buffer: *mut u16);
        fn CFRelease(value: CfRef);
    }
    struct OwnedCf(CfRef);
    impl Drop for OwnedCf {
        fn drop(&mut self) {
            unsafe { CFRelease(self.0) };
        }
    }
    fn cf_string(value: &str) -> io::Result<OwnedCf> {
        let pointer = unsafe {
            CFStringCreateWithBytes(
                std::ptr::null(),
                value.as_ptr(),
                value.len() as isize,
                0x0800_0100,
                0,
            )
        };
        if pointer.is_null() {
            Err(io::Error::other(
                "failed to allocate managed preference key",
            ))
        } else {
            Ok(OwnedCf(pointer))
        }
    }
    let key = cf_string(key_name)?;
    let application = cf_string(PREFERENCES_DOMAIN)?;
    load_forced_value_with(
        key_name,
        || unsafe { CFPreferencesAppValueIsForced(key.0, application.0) != 0 },
        || {
            let pointer = unsafe { CFPreferencesCopyAppValue(key.0, application.0) };
            (!pointer.is_null()).then(|| OwnedCf(pointer))
        },
        |value| {
            if unsafe { CFGetTypeID(value.0) != CFStringGetTypeID() } {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("managed preference {PREFERENCES_DOMAIN}:{key_name} must be a string"),
                ));
            }
            let length = unsafe { CFStringGetLength(value.0) };
            let mut characters = vec![
                0;
                usize::try_from(length).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid managed preference string",
                    )
                })?
            ];
            unsafe {
                CFStringGetCharacters(
                    value.0,
                    CfRange {
                        location: 0,
                        length,
                    },
                    characters.as_mut_ptr(),
                );
            }
            String::from_utf16(&characters).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid managed preference string",
                )
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn system(contents: &str) -> LoadedLayer {
        LoadedLayer {
            origin: ManagedRequirementOrigin::SystemFile {
                path: PathBuf::from("/administrator/requirements.toml"),
            },
            toml: contents.to_string(),
        }
    }

    fn bundle(contents: &str) -> TrustedBundleRequirements {
        TrustedBundleRequirements {
            id: "organization".to_string(),
            name: "Administrator".to_string(),
            toml: contents.to_string(),
        }
    }

    fn forced(contents: &str) -> LoadedLayer {
        load_forced_layer_with(
            || true,
            || Some(base64::engine::general_purpose::STANDARD.encode(contents)),
            Ok,
        )
        .unwrap()
        .unwrap()
    }

    fn compose(contents: &str) -> io::Result<ManagedReviewRequirements> {
        compose_loaded_layers(Some(system(contents)), Vec::new(), None)
    }

    fn legacy(contents: &str) -> LoadedLayer {
        project_legacy_layer(LoadedLayer {
            origin: ManagedRequirementOrigin::LegacyManagedFile {
                path: PathBuf::from("/administrator/managed_config.toml"),
            },
            toml: contents.to_string(),
        })
        .unwrap()
    }

    #[test]
    fn legacy_projection_constrains_approval_and_sandbox_but_keeps_user_opt_out() {
        let requirements = compose_layers([legacy("approval_policy = 'never'\napprovals_reviewer = 'auto_review'\nsandbox_mode = 'workspace-write'")]).unwrap();
        assert_eq!(
            requirements.allowed_approval_policies,
            Some(vec![ManagedApprovalPolicy::Never])
        );
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::Never)
            .is_ok());
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::OnRequest)
            .is_err());
        assert_eq!(
            requirements.preferred_approval_policy(),
            Some(ManagedApprovalPolicy::Never)
        );
        assert_eq!(
            requirements.allowed_reviewers,
            Some(vec![ManagedReviewer::AutoReview, ManagedReviewer::User])
        );
        assert!(requirements.allows_user_reviewer);
        assert!(!requirements.force_sync);
        assert!(requirements
            .validate_sandbox_mode(SandboxMode::ReadOnly)
            .is_ok());
        assert!(requirements
            .validate_sandbox_mode(SandboxMode::WorkspaceWrite)
            .is_ok());
        assert!(requirements
            .validate_sandbox_mode(SandboxMode::DangerFullAccess)
            .is_err());
    }

    #[test]
    fn legacy_readonly_is_not_duplicated_and_unknown_settings_are_not_requirements() {
        let requirements = compose_layers([legacy("approvals_reviewer = 'user'\nsandbox_mode = 'read-only'\nguardian_policy_config = 'ordinary setting'\nmodel = 'example'")]).unwrap();
        assert_eq!(
            requirements.allowed_sandbox_modes,
            Some(vec![ManagedSandboxMode::ReadOnly])
        );
        assert!(requirements
            .validate_reviewer(ManagedReviewer::AutoReview)
            .is_err());
        assert_eq!(requirements.policy, ManagedReviewPolicy::default());
        assert!(requirements.required_models.is_empty());
    }

    #[test]
    fn native_priority_places_legacy_after_cloud_and_forced_requirements_last() {
        let native = NativeLayers {
            system: Some(system(
                "allowed_approvals_reviewers = ['user']\nallowed_approval_policies = ['never']",
            )),
            legacy_file: Some(legacy(
                "approvals_reviewer = 'auto_review'\napproval_policy = 'on-request'",
            )),
            legacy_forced: Some(legacy("approvals_reviewer = 'user'")),
            forced: Some(forced("allowed_approvals_reviewers = ['auto_review']")),
        };
        let requirements = compose_native_layers(
            native,
            vec![bundle(
                "allowed_approvals_reviewers = ['user']\nallowed_approval_policies = ['never']",
            )],
        )
        .unwrap();
        assert_eq!(
            requirements.allowed_approval_policies,
            Some(vec![ManagedApprovalPolicy::OnRequest])
        );
        assert_eq!(
            requirements.allowed_reviewers,
            Some(vec![ManagedReviewer::AutoReview])
        );
        assert!(requirements.force_sync);
        assert!(matches!(
            requirements.sources.allowed_approval_policies,
            Some(ManagedRequirementOrigin::LegacyManagedFile { .. })
        ));
        assert!(matches!(
            requirements.layers[1].origin,
            ManagedRequirementOrigin::TrustedBundle { .. }
        ));
        assert_eq!(requirements.layers.len(), 5);
    }

    #[test]
    fn managed_approval_and_sandbox_constraints_validate_final_composed_lists() {
        for contents in [
            "allowed_approval_policies = []",
            "allowed_sandbox_modes = []",
            "allowed_sandbox_modes = ['workspace-write']",
            "allowed_approval_policies = ['unknown']",
            "allowed_sandbox_modes = ['unknown']",
        ] {
            assert_eq!(
                compose(contents).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{contents}"
            );
        }
        let requirements = compose("allowed_approval_policies = ['untrusted', 'on-failure']\nallowed_sandbox_modes = ['read-only', 'external-sandbox']").unwrap();
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::OnRequest)
            .is_ok());
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::Never)
            .is_err());
        assert_eq!(
            requirements.preferred_approval_policy(),
            Some(ManagedApprovalPolicy::UnlessTrusted)
        );
        assert!(requirements
            .validate_sandbox_mode(SandboxMode::ReadOnly)
            .is_ok());
        assert!(requirements
            .validate_sandbox_mode(SandboxMode::DangerFullAccess)
            .is_err());
    }

    #[test]
    fn granular_constraints_retain_full_flags_without_mapping_to_a_broader_policy() {
        let requirements = compose("allowed_approval_policies = [{ granular = { sandbox_approval = false, rules = true, mcp_elicitations = false } }]").unwrap();
        assert_eq!(
            requirements.preferred_approval_policy(),
            Some(ManagedApprovalPolicy::Granular {
                sandbox_approval: false,
                rules: true,
                skill_approval: false,
                request_permissions: false,
                mcp_elicitations: false
            })
        );
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::OnRequest)
            .is_err());
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::Never)
            .is_err());
        let projected = legacy("[approval_policy.granular]\nsandbox_approval = true\nrules = false\nskill_approval = true\nrequest_permissions = true\nmcp_elicitations = true");
        assert_eq!(
            compose_layers([projected])
                .unwrap()
                .preferred_approval_policy(),
            Some(ManagedApprovalPolicy::Granular {
                sandbox_approval: true,
                rules: false,
                skill_approval: true,
                request_permissions: true,
                mcp_elicitations: true
            })
        );
        assert!(compose("allowed_approval_policies = [{ granular = { rules = true } }]").is_err());
    }

    #[test]
    fn native_granular_policy_constraint_compares_all_five_flags_exactly() {
        use crate::types::permission::GranularApprovalConfig;
        let requirements = compose("allowed_approval_policies = [{ granular = { sandbox_approval = false, rules = true, skill_approval = false, request_permissions = true, mcp_elicitations = false } }]").unwrap();
        let exact = GranularApprovalConfig {
            sandbox_approval: false,
            rules: true,
            skill_approval: false,
            request_permissions: true,
            mcp_elicitations: false,
        };
        assert!(requirements
            .validate_approval_policy(ApprovalPolicy::Granular(exact))
            .is_ok());
        for changed in [
            GranularApprovalConfig {
                sandbox_approval: true,
                ..exact
            },
            GranularApprovalConfig {
                rules: false,
                ..exact
            },
            GranularApprovalConfig {
                skill_approval: true,
                ..exact
            },
            GranularApprovalConfig {
                request_permissions: false,
                ..exact
            },
            GranularApprovalConfig {
                mcp_elicitations: true,
                ..exact
            },
        ] {
            assert!(requirements
                .validate_approval_policy(ApprovalPolicy::Granular(changed))
                .is_err());
        }
        let untrusted = compose("allowed_approval_policies = ['untrusted']").unwrap();
        assert!(untrusted
            .validate_approval_policy(ApprovalPolicy::UnlessTrusted)
            .is_ok());
        assert!(untrusted
            .validate_approval_policy(ApprovalPolicy::OnRequest)
            .is_err());
    }

    #[test]
    fn malformed_legacy_requirements_fail_before_any_higher_override() {
        for contents in [
            "approval_policy = 1",
            "approvals_reviewer = 'unknown'",
            "sandbox_mode = 'external-sandbox'",
            "[invalid",
        ] {
            let layer = LoadedLayer {
                origin: ManagedRequirementOrigin::LegacyManagedFile {
                    path: PathBuf::from("/administrator/managed_config.toml"),
                },
                toml: contents.to_string(),
            };
            assert_eq!(
                project_legacy_layer(layer).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{contents}"
            );
        }
    }

    #[test]
    fn legacy_preference_uses_its_own_forced_key_and_projection() {
        let layer = load_forced_value_with(
            LEGACY_PREFERENCES_KEY,
            || true,
            || {
                Some(
                    base64::engine::general_purpose::STANDARD
                        .encode("approvals_reviewer = 'guardian_subagent'"),
                )
            },
            Ok,
        )
        .unwrap()
        .unwrap();
        assert!(
            matches!(&layer.origin, ManagedRequirementOrigin::ForcedPreference { key, .. } if key == LEGACY_PREFERENCES_KEY)
        );
        let requirements = compose_layers([project_legacy_layer(layer).unwrap()]).unwrap();
        assert_eq!(
            requirements.allowed_reviewers,
            Some(vec![ManagedReviewer::AutoReview, ManagedReviewer::User])
        );
        let result = load_forced_value_with(
            LEGACY_PREFERENCES_KEY,
            || false,
            || -> Option<String> { panic!("unforced legacy preference must not be copied") },
            Ok,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn absent_requirements_allow_both_reviewers_without_forcing_sync() {
        let requirements = compose_loaded_layers(None, Vec::new(), None).unwrap();
        assert_eq!(requirements, ManagedReviewRequirements::default());
        assert!(requirements
            .validate_reviewer(ManagedReviewer::User)
            .is_ok());
        assert!(requirements
            .validate_reviewer(ManagedReviewer::AutoReview)
            .is_ok());
        assert!(!requirements.review_required("model"));
    }

    #[test]
    fn priority_replaces_policy_and_constraints_but_unions_protected_models() {
        let requirements = compose_loaded_layers(
            Some(system(
                "guardian_policy_config = 'system'\nguardian_extra_policy = 'extra'\nallowed_approvals_reviewers = ['user']\n[auto_review]\nrequired_on_models = ['low', 'same']\nignore_rules = ['old']",
            )),
            vec![bundle(
                "guardian_policy_config = 'bundle'\nallowed_approvals_reviewers = ['auto_review', 'user']\n[auto_review]\nrequired_on_models = ['middle', 'same']\nignore_rules = ['new']",
            )],
            Some(forced(
                "guardian_policy_config = ' forced '\nallowed_approvals_reviewers = ['guardian_subagent']\n[auto_review]\nrequired_on_models = ['high', 'same']",
            )),
        )
        .unwrap();
        assert_eq!(requirements.policy.policy.as_deref(), Some(" forced "));
        assert_eq!(requirements.policy.extra_policy.as_deref(), Some("extra"));
        assert_eq!(
            requirements.required_models,
            ["high", "same", "middle", "low"]
        );
        assert_eq!(requirements.ignore_rules, Some(vec!["new".to_string()]));
        assert!(!requirements.allows_user_reviewer);
        assert!(requirements.force_sync);
        assert_eq!(
            requirements.preferred_reviewer(),
            Some(ManagedReviewer::AutoReview)
        );
        assert_eq!(requirements.sources.required_models.len(), 3);
        assert!(matches!(
            requirements.sources.policy,
            Some(ManagedRequirementOrigin::ForcedPreference { .. })
        ));
        assert!(matches!(
            requirements.sources.ignore_rules,
            Some(ManagedRequirementOrigin::TrustedBundle { .. })
        ));
        assert!(matches!(
            requirements.layers[0].origin,
            ManagedRequirementOrigin::SystemFile { .. }
        ));
        assert!(matches!(
            requirements.layers[2].origin,
            ManagedRequirementOrigin::ForcedPreference { .. }
        ));
    }

    #[test]
    fn empty_higher_model_list_does_not_remove_protection() {
        let requirements = compose_loaded_layers(
            Some(system("[auto_review]\nrequired_on_models = ['protected']")),
            vec![bundle("[auto_review]\nrequired_on_models = []")],
            None,
        )
        .unwrap();
        assert_eq!(requirements.required_models, ["protected"]);
    }

    #[test]
    fn explicit_blank_higher_policy_clears_lower_managed_override() {
        let requirements = compose_loaded_layers(
            Some(system(
                "guardian_policy_config = 'lower'\nguardian_extra_policy = 'lower'",
            )),
            vec![bundle(
                "guardian_policy_config = ' '\nguardian_extra_policy = ''",
            )],
            None,
        )
        .unwrap();
        assert_eq!(requirements.policy, ManagedReviewPolicy::default());
        assert_eq!(requirements.sources.policy, None);
        assert_eq!(requirements.sources.extra_policy, None);
    }

    #[test]
    fn reviewer_constraint_and_model_requirement_are_independent() {
        let requirements = compose(
            "allowed_approvals_reviewers = ['user', 'auto_review']\n[auto_review]\nrequired_on_models = ['protected']",
        )
        .unwrap();
        assert!(requirements
            .validate_reviewer(ManagedReviewer::User)
            .is_ok());
        assert!(requirements.review_required("provider/protected"));
        assert!(!requirements.force_sync);
        assert_eq!(
            requirements.preferred_reviewer(),
            Some(ManagedReviewer::User)
        );
        let strict = compose("allowed_approvals_reviewers = ['auto_review']").unwrap();
        assert_eq!(
            strict
                .validate_reviewer(ManagedReviewer::User)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(strict.review_required("unprotected"));
    }

    #[test]
    fn slugs_match_exactly_after_one_valid_namespace() {
        let requirements =
            compose("[auto_review]\nrequired_on_models = ['model.v1', 'inside space']").unwrap();
        for model in [
            "model.v1",
            "provider/model.v1",
            "a_B-9/model.v1",
            "inside space",
        ] {
            assert!(requirements.requires_review_for_model(model), "{model}");
        }
        for model in [
            "Model.v1",
            "/model.v1",
            "p.x/model.v1",
            "p q/model.v1",
            "p/model.v1/x",
            "p//model.v1",
            " model.v1",
            "p/",
            "p:auth/model.v1",
        ] {
            assert!(!requirements.requires_review_for_model(model), "{model}");
        }
    }

    #[test]
    fn invalid_slugs_fail_even_under_a_higher_empty_list() {
        for slug in ["", " ", " model", "model ", "provider/model"] {
            let lower = format!("[auto_review]\nrequired_on_models = [{slug:?}]");
            let result = compose_loaded_layers(
                Some(system(&lower)),
                vec![bundle("[auto_review]\nrequired_on_models = []")],
                None,
            );
            assert_eq!(
                result.unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{slug:?}"
            );
        }
    }

    #[test]
    fn each_layer_is_schema_checked_before_higher_replacement() {
        for contents in [
            "guardian_policy_config = 1",
            "guardian_extra_policy = false",
            "allowed_approvals_reviewers = ['unknown']",
            "allowed_approvals_reviewers = [1]",
            "auto_review = false",
            "[auto_review]\nrequired_on_models = [1]",
            "[auto_review]\nignore_rules = 'bad'",
            "[auto_review",
        ] {
            let result = compose_loaded_layers(
                Some(system(contents)),
                vec![bundle(
                    "guardian_policy_config = 'higher'\nallowed_approvals_reviewers = ['user']\n[auto_review]\nrequired_on_models = []\nignore_rules = []",
                )],
                None,
            );
            assert_eq!(
                result.unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{contents}"
            );
        }
    }

    #[test]
    fn empty_final_reviewer_constraint_fails_but_overridden_empty_is_valid() {
        assert_eq!(
            compose("allowed_approvals_reviewers = []")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(compose_loaded_layers(
            Some(system("allowed_approvals_reviewers = []")),
            vec![bundle("allowed_approvals_reviewers = ['user']")],
            None
        )
        .is_ok());
    }

    #[test]
    fn ordinary_settings_and_unknown_fields_cannot_become_managed_review_policy() {
        let requirements = compose("approvals_reviewer = 'auto_review'\nmanaged = true\nforce_sync = true\nguardian_policy = 'untrusted'\n[auto_review]\nforce_sync = true\nunknown = true").unwrap();
        assert!(requirements.allows_user_reviewer);
        assert!(!requirements.force_sync);
        assert_eq!(requirements.policy, ManagedReviewPolicy::default());
    }

    #[test]
    fn inline_table_is_equivalent_and_empty_ignore_rules_replace() {
        let requirements = compose_loaded_layers(
            Some(system(
                "auto_review = {required_on_models = ['protected'], ignore_rules = ['old']}",
            )),
            vec![bundle("auto_review = {ignore_rules = []}")],
            None,
        )
        .unwrap();
        assert_eq!(requirements.required_models, ["protected"]);
        assert_eq!(requirements.ignore_rules, Some(Vec::new()));
    }

    #[test]
    fn unforced_preference_is_never_read() {
        let result = load_forced_layer_with(
            || false,
            || -> Option<String> { panic!("unforced read") },
            Ok,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn preference_losing_forced_status_is_not_decoded() {
        let mut checks = [true, false].into_iter();
        let result = load_forced_layer_with(
            || checks.next().unwrap(),
            || Some(()),
            |_| -> io::Result<String> { panic!("lost trust") },
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn absent_forced_value_does_not_need_a_second_trust_check() {
        let mut count = 0;
        assert!(load_forced_layer_with(
            || {
                count += 1;
                true
            },
            || None::<String>,
            Ok
        )
        .unwrap()
        .is_none());
        assert_eq!(count, 1);
    }

    #[test]
    fn forced_nonstring_invalid_base64_utf8_and_toml_fail_closed() {
        let invalid_type = load_forced_layer_with(
            || true,
            || Some(()),
            |_| {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "must be a string",
                ))
            },
        );
        assert_eq!(invalid_type.unwrap_err().kind(), io::ErrorKind::InvalidData);
        for encoded in [
            "not base64!".to_string(),
            base64::engine::general_purpose::STANDARD.encode([0xff]),
        ] {
            assert_eq!(
                load_forced_layer_with(|| true, || Some(encoded), Ok)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(
            compose_loaded_layers(None, Vec::new(), Some(forced("[broken")))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn forced_base64_is_trimmed_and_empty_decodes_to_empty_layer() {
        let encoded = format!(
            " \n{}\t",
            base64::engine::general_purpose::STANDARD.encode("guardian_policy_config = 'forced'")
        );
        let layer = load_forced_layer_with(|| true, || Some(encoded), Ok)
            .unwrap()
            .unwrap();
        assert_eq!(
            compose_loaded_layers(None, Vec::new(), Some(layer))
                .unwrap()
                .policy
                .policy
                .as_deref(),
            Some("forced")
        );
        let empty = load_forced_layer_with(|| true, || Some("  ".to_string()), Ok)
            .unwrap()
            .unwrap();
        assert!(empty.toml.is_empty());
    }

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "klaus-managed-requirements-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn system_file_missing_is_absent_but_other_io_and_encoding_errors_propagate() {
        let directory = TestDirectory::new();
        assert!(load_system_layer(&directory.0.join("missing"))
            .unwrap()
            .is_none());
        assert!(load_system_layer(&directory.0).is_err());
        let path = directory.0.join("requirements.toml");
        std::fs::write(&path, [0xff]).unwrap();
        assert_eq!(
            load_system_layer(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn reloading_bad_or_changed_native_file_never_reuses_a_successful_snapshot() {
        let directory = TestDirectory::new();
        let path = directory.0.join("requirements.toml");
        std::fs::write(&path, "guardian_policy_config = 'first'").unwrap();
        let first =
            compose_loaded_layers(load_system_layer(&path).unwrap(), Vec::new(), None).unwrap();
        assert_eq!(first.policy.policy.as_deref(), Some("first"));
        std::fs::write(&path, "[invalid").unwrap();
        assert!(
            compose_loaded_layers(load_system_layer(&path).unwrap(), Vec::new(), None).is_err()
        );
        std::fs::write(&path, "allowed_approvals_reviewers = ['auto_review']").unwrap();
        let second =
            compose_loaded_layers(load_system_layer(&path).unwrap(), Vec::new(), None).unwrap();
        assert!(second.force_sync);
        assert_ne!(first, second);
    }

    struct FailingBundle;
    impl TrustedRequirementsBundleProvider for FailingBundle {
        fn load(
            &self,
        ) -> Pin<Box<dyn Future<Output = io::Result<Vec<TrustedBundleRequirements>>> + Send + '_>>
        {
            Box::pin(async {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "trusted bundle unavailable",
                ))
            })
        }
    }

    #[tokio::test]
    async fn trusted_bundle_failure_propagates_without_loading_lower_fallback() {
        assert_eq!(
            load_managed_review_requirements_with_bundle(Some(&FailingBundle))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn native_path_is_fixed_outside_user_and_project_configuration() {
        assert_eq!(
            system_requirements_path(),
            PathBuf::from("/etc/klaus/requirements.toml")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn native_forced_preference_reader_returns_only_trusted_requirements() {
        // Calls the actual native API without mutating the user's preferences.
        // Administrators may have installed a real profile on this machine.
        if let Some(layer) = load_forced_layer().unwrap() {
            assert!(matches!(
                layer.origin,
                ManagedRequirementOrigin::ForcedPreference { .. }
            ));
            compose_loaded_layers(None, Vec::new(), Some(layer)).unwrap();
        }
    }
}
