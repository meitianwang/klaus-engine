use crate::runtime_values::{
    ConfigJson, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewMode {
    #[default]
    Disabled,
    Synchronous,
    Adaptive,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnscoredAction {
    Ignore,
    AgeScore,
    #[default]
    #[serde(other)]
    InvalidateScore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReviewScope {
    ComputerUse,
    Shell,
    FileChanges,
    Mcp,
    Network,
    Permissions,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelReviewPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computer_use: Option<ReviewMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<ReviewMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_changes: Option<ReviewMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<ReviewMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<ReviewMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<ReviewMode>,
    #[serde(default)]
    pub other_tools: ReviewMode,
    #[serde(default)]
    pub unscored_action: UnscoredAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_cua_call: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandboxed_exec_commands: Option<bool>,
}

impl RuntimeEncode for ModelReviewPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        ConfigJson::serialize_config(self)?.encode_runtime(out)
    }
}

impl RuntimeDecode for ModelReviewPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        super::config::decode_config(value)
    }
}

impl ModelReviewPolicy {
    pub fn review_mode(&self, scope: ReviewScope) -> ReviewMode {
        match scope {
            ReviewScope::ComputerUse => self.computer_use,
            ReviewScope::Shell => self.shell,
            ReviewScope::FileChanges => self.file_changes,
            ReviewScope::Mcp => self.mcp,
            ReviewScope::Network => self.network,
            ReviewScope::Permissions => self.permissions,
        }
        .unwrap_or_default()
    }

    pub fn scoring_enabled(&self) -> bool {
        [
            self.computer_use,
            self.shell,
            self.file_changes,
            self.mcp,
            self.network,
            self.permissions,
        ]
        .contains(&Some(ReviewMode::Adaptive))
    }

    pub fn disable_scoring(&mut self) {
        for mode in [
            &mut self.computer_use,
            &mut self.shell,
            &mut self.file_changes,
            &mut self.mcp,
            &mut self.network,
            &mut self.permissions,
        ] {
            if *mode == Some(ReviewMode::Adaptive) {
                *mode = Some(ReviewMode::Synchronous);
            }
        }
        self.other_tools = ReviewMode::Synchronous;
    }

    pub fn allows_initial_computer_use(&self) -> bool {
        self.initial_cua_call
            .unwrap_or(self.computer_use == Some(ReviewMode::Adaptive))
    }

    pub fn scores_tool(
        &self,
        scope: Option<ReviewScope>,
        default_namespace: bool,
        name: &str,
        explicitly_escalated: bool,
    ) -> bool {
        if scope.map_or(self.other_tools, |scope| self.review_mode(scope)) != ReviewMode::Adaptive {
            return false;
        }
        self.sandboxed_exec_commands.unwrap_or(true)
            || !default_namespace
            || name != "exec_command"
            || explicitly_escalated
    }
}

/// Resolves catalog policy independently from the optional legacy feature gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyLoader {
    default: ModelReviewPolicy,
    legacy_enabled: bool,
    force_synchronous_review: bool,
}

impl PolicyLoader {
    pub fn new(legacy: Option<&super::AsyncFeatureConfig>, force_synchronous_review: bool) -> Self {
        let scope = legacy.and_then(super::AsyncFeatureConfig::local_config);
        let scope = scope.and_then(|config| config.review_scope.as_ref());
        let computer_use_only = scope
            .and_then(|scope| scope.computer_use_only)
            .unwrap_or(true);
        let other = if computer_use_only {
            ReviewMode::Synchronous
        } else {
            ReviewMode::Adaptive
        };
        Self {
            default: ModelReviewPolicy {
                computer_use: Some(ReviewMode::Adaptive),
                shell: Some(other),
                file_changes: Some(other),
                mcp: Some(other),
                network: Some(other),
                permissions: Some(other),
                other_tools: other,
                unscored_action: if computer_use_only {
                    UnscoredAction::Ignore
                } else {
                    UnscoredAction::AgeScore
                },
                initial_cua_call: Some(computer_use_only),
                sandboxed_exec_commands: Some(
                    !computer_use_only
                        && scope
                            .and_then(|scope| scope.sandboxed_exec_commands)
                            .unwrap_or(false),
                ),
            },
            legacy_enabled: legacy
                .and_then(super::AsyncFeatureConfig::enabled)
                .unwrap_or(false),
            force_synchronous_review,
        }
    }

    pub fn resolve(
        &self,
        catalog: Option<&ModelReviewPolicy>,
        legacy_computer_use_required: Option<bool>,
    ) -> ModelReviewPolicy {
        let mut policy = catalog.cloned().unwrap_or_else(|| self.default.clone());
        if catalog.is_none()
            && policy.allows_initial_computer_use()
            && legacy_computer_use_required == Some(false)
        {
            policy.computer_use = Some(ReviewMode::Synchronous);
            policy.initial_cua_call = Some(false);
            policy.unscored_action = UnscoredAction::InvalidateScore;
        }
        if self.force_synchronous_review || (!self.legacy_enabled && catalog.is_none()) {
            policy.disable_scoring();
        }
        policy
    }
}
