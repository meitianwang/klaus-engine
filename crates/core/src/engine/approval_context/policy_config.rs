// Copyright 2025 OpenAI. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0
// Modified: policy selection accepts the host's resolved configuration and model metadata.

//! Resolves review policy after ordinary configuration layers and administrator requirements.

use super::history_types::ResponseItemEnvelope;
use super::model::{ContentItem, ContentItemKind, ContextualUserFragment, ResponseItem};
use super::{
    policy_instructions, TranscriptFormat, DEFAULT_COMPUTER_POLICY, DEFAULT_POLICY,
    DEFAULT_POLICY_TEMPLATE, REJECTION_INSTRUCTIONS, TIMEOUT_INSTRUCTIONS,
};

/// Effective ordinary settings. Overlay raw layers before normalizing blank overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReviewPolicyOptions {
    pub policy: Option<String>,
    pub extra_policy: Option<String>,
    pub policy_template: Option<String>,
    pub transcript_mode: Option<TranscriptFormat>,
}

impl ReviewPolicyOptions {
    /// Higher-priority ordinary values replace lower values, including explicit blanks.
    pub fn overlay(&self, higher: &Self) -> Self {
        Self {
            policy: higher.policy.clone().or_else(|| self.policy.clone()),
            extra_policy: higher
                .extra_policy
                .clone()
                .or_else(|| self.extra_policy.clone()),
            policy_template: higher
                .policy_template
                .clone()
                .or_else(|| self.policy_template.clone()),
            transcript_mode: higher.transcript_mode.or(self.transcript_mode),
        }
    }
}

/// Captured catalog messages for the selected review model. Empty strings remain literal overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelReviewMessages {
    pub policy: Option<String>,
    pub policy_template: Option<String>,
    pub node_repl_policy: Option<String>,
    pub rejection_instructions: Option<String>,
    pub timeout_instructions: Option<String>,
}

/// Independently loaded administrator requirements, already composed by the host.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManagedReviewPolicy {
    pub policy: Option<String>,
    pub extra_policy: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyOrigin {
    Bundled,
    Model,
    Configuration,
    Managed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComputerUseReviewMode {
    Disabled,
    Synchronous,
    Adaptive,
    Unknown,
}

/// Eligibility belongs to the captured parent model, independently of the review model's messages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ComputerUseReviewEligibility {
    pub catalog_policy_present: bool,
    pub computer_use: Option<ComputerUseReviewMode>,
    pub legacy_required: bool,
}

impl ComputerUseReviewEligibility {
    pub fn required(self) -> bool {
        if self.catalog_policy_present {
            !matches!(
                self.computer_use,
                None | Some(ComputerUseReviewMode::Disabled)
            )
        } else {
            self.legacy_required
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedReviewPolicy {
    pub policy: String,
    pub extra_policy: String,
    pub policy_template: String,
    pub transcript_mode: TranscriptFormat,
    pub node_repl_policy: String,
    pub rejection_instructions: String,
    pub timeout_instructions: String,
    pub policy_origin: PolicyOrigin,
    pub extra_policy_origin: PolicyOrigin,
    pub template_origin: PolicyOrigin,
    pub computer_use_eligibility: ComputerUseReviewEligibility,
}

fn normalized(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

impl ResolvedReviewPolicy {
    pub fn resolve(
        local: &ReviewPolicyOptions,
        model: &ModelReviewMessages,
        managed: &ManagedReviewPolicy,
        computer_use_eligibility: ComputerUseReviewEligibility,
    ) -> Self {
        let (policy, policy_origin) = match normalized(managed.policy.as_deref()) {
            Some(policy) => (policy, PolicyOrigin::Managed),
            None => match normalized(local.policy.as_deref()) {
                Some(policy) => (policy, PolicyOrigin::Configuration),
                None => match model.policy.as_deref() {
                    Some(policy) => (policy, PolicyOrigin::Model),
                    None => (DEFAULT_POLICY, PolicyOrigin::Bundled),
                },
            },
        };
        let (extra_policy, extra_policy_origin) = match normalized(managed.extra_policy.as_deref())
        {
            Some(policy) => (policy, PolicyOrigin::Managed),
            None => match normalized(local.extra_policy.as_deref()) {
                Some(policy) => (policy, PolicyOrigin::Configuration),
                None => ("", PolicyOrigin::Bundled),
            },
        };
        let (policy_template, template_origin) = match normalized(local.policy_template.as_deref())
        {
            Some(template) => (template, PolicyOrigin::Configuration),
            None => match model.policy_template.as_deref() {
                Some(template) => (template, PolicyOrigin::Model),
                None => (DEFAULT_POLICY_TEMPLATE, PolicyOrigin::Bundled),
            },
        };
        Self {
            policy: policy.to_owned(),
            extra_policy: extra_policy.to_owned(),
            policy_template: policy_template.to_owned(),
            transcript_mode: local.transcript_mode.unwrap_or(TranscriptFormat::Line),
            node_repl_policy: model
                .node_repl_policy
                .as_deref()
                .unwrap_or(DEFAULT_COMPUTER_POLICY)
                .to_owned(),
            rejection_instructions: model
                .rejection_instructions
                .as_deref()
                .unwrap_or(REJECTION_INSTRUCTIONS)
                .to_owned(),
            timeout_instructions: model
                .timeout_instructions
                .as_deref()
                .unwrap_or(TIMEOUT_INSTRUCTIONS)
                .to_owned(),
            policy_origin,
            extra_policy_origin,
            template_origin,
            computer_use_eligibility,
        }
    }

    pub fn instructions(&self, output_contract: &str) -> String {
        policy_instructions(
            self.transcript_mode,
            &self.policy,
            &self.extra_policy,
            &self.policy_template,
            output_contract,
        )
    }

    pub fn computer_policy_for_action(&self, is_computer_use: bool) -> Option<&str> {
        (self.computer_use_eligibility.required()
            && is_computer_use
            && !self.node_repl_policy.is_empty())
        .then_some(self.node_repl_policy.as_str())
    }

    /// Only an existing developer message with this exact body proves prior policy delivery.
    pub fn missing_computer_policy_message(
        &self,
        is_computer_use: bool,
        history: &[ResponseItemEnvelope],
    ) -> Option<ResponseItem> {
        let policy = self.computer_policy_for_action(is_computer_use)?;
        let delivered = history.iter().any(|envelope| matches!(&envelope.item,
            ResponseItem::Message { role, content, .. } if role == "developer" && content.iter().any(|content|
                matches!(content, ContentItem::InputText { text } if text == policy))));
        (!delivered).then(|| ContextualUserFragment::into(ComputerPolicy(policy)))
    }
}

struct ComputerPolicy<'a>(&'a str);

impl ContextualUserFragment for ComputerPolicy<'_> {
    fn role(&self) -> &'static str {
        "developer"
    }
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("guardian.node_repl_policy".to_owned())
    }
    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }
    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }
    fn body(&self) -> String {
        self.0.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(
        local: ReviewPolicyOptions,
        model: ModelReviewMessages,
        managed: ManagedReviewPolicy,
    ) -> ResolvedReviewPolicy {
        ResolvedReviewPolicy::resolve(
            &local,
            &model,
            &managed,
            ComputerUseReviewEligibility::default(),
        )
    }

    #[test]
    fn bundled_policy_and_messages_apply_when_all_overrides_are_absent() {
        let policy = resolved(
            ReviewPolicyOptions::default(),
            ModelReviewMessages::default(),
            ManagedReviewPolicy::default(),
        );
        assert_eq!(policy.policy, DEFAULT_POLICY);
        assert_eq!(policy.policy_template, DEFAULT_POLICY_TEMPLATE);
        assert_eq!(policy.node_repl_policy, DEFAULT_COMPUTER_POLICY);
        assert_eq!(policy.rejection_instructions, REJECTION_INSTRUCTIONS);
        assert_eq!(policy.timeout_instructions, TIMEOUT_INSTRUCTIONS);
        assert_eq!(policy.transcript_mode, TranscriptFormat::Line);
    }

    #[test]
    fn managed_policy_and_extra_override_ordinary_settings_and_model_policy() {
        let policy = resolved(
            ReviewPolicyOptions {
                policy: Some(" local ".to_owned()),
                extra_policy: Some(" local extra ".to_owned()),
                policy_template: Some(" LOCAL TEMPLATE ".to_owned()),
                transcript_mode: Some(TranscriptFormat::Json),
            },
            ModelReviewMessages {
                policy: Some("MODEL".to_owned()),
                policy_template: Some("MODEL TEMPLATE".to_owned()),
                ..Default::default()
            },
            ManagedReviewPolicy {
                policy: Some(" managed ".to_owned()),
                extra_policy: Some(" managed extra ".to_owned()),
            },
        );
        assert_eq!(policy.policy, "managed");
        assert_eq!(policy.extra_policy, "managed extra");
        assert_eq!(policy.policy_template, "LOCAL TEMPLATE");
        assert_eq!(policy.policy_origin, PolicyOrigin::Managed);
        assert_eq!(policy.template_origin, PolicyOrigin::Configuration);
        assert_eq!(policy.transcript_mode, TranscriptFormat::Json);
    }

    #[test]
    fn blank_local_and_managed_values_fall_back_but_model_literals_are_preserved() {
        let policy = resolved(
            ReviewPolicyOptions {
                policy: Some(" \n\t".to_owned()),
                extra_policy: Some(" \n".to_owned()),
                policy_template: Some(" \t".to_owned()),
                ..Default::default()
            },
            ModelReviewMessages {
                policy: Some("".to_owned()),
                policy_template: Some("  ".to_owned()),
                node_repl_policy: Some("".to_owned()),
                rejection_instructions: Some(" \n".to_owned()),
                timeout_instructions: Some("".to_owned()),
            },
            ManagedReviewPolicy {
                policy: Some(" \n".to_owned()),
                extra_policy: Some(" \t".to_owned()),
            },
        );
        assert_eq!(policy.policy, "");
        assert_eq!(policy.extra_policy, "");
        assert_eq!(policy.policy_template, "  ");
        assert_eq!(policy.node_repl_policy, "");
        assert_eq!(policy.rejection_instructions, " \n");
        assert_eq!(policy.timeout_instructions, "");
    }

    #[test]
    fn ordinary_layers_merge_before_blank_override_normalization() {
        let user = ReviewPolicyOptions {
            policy: Some("USER".to_owned()),
            extra_policy: Some("USER EXTRA".to_owned()),
            ..Default::default()
        };
        let project = ReviewPolicyOptions {
            policy: Some("  ".to_owned()),
            policy_template: Some("PROJECT".to_owned()),
            ..Default::default()
        };
        let policy = resolved(
            user.overlay(&project),
            ModelReviewMessages {
                policy: Some("MODEL".to_owned()),
                ..Default::default()
            },
            ManagedReviewPolicy::default(),
        );
        assert_eq!(policy.policy, "MODEL");
        assert_eq!(policy.extra_policy, "USER EXTRA");
        assert_eq!(policy.policy_template, "PROJECT");
    }

    #[test]
    fn catalog_scope_presence_replaces_the_legacy_eligibility_bit() {
        for legacy_required in [false, true] {
            for mode in [
                None,
                Some(ComputerUseReviewMode::Disabled),
                Some(ComputerUseReviewMode::Synchronous),
                Some(ComputerUseReviewMode::Adaptive),
                Some(ComputerUseReviewMode::Unknown),
            ] {
                assert_eq!(
                    ComputerUseReviewEligibility {
                        catalog_policy_present: true,
                        computer_use: mode,
                        legacy_required
                    }
                    .required(),
                    !matches!(mode, None | Some(ComputerUseReviewMode::Disabled))
                );
            }
            assert_eq!(
                ComputerUseReviewEligibility {
                    catalog_policy_present: false,
                    computer_use: Some(ComputerUseReviewMode::Disabled),
                    legacy_required
                }
                .required(),
                legacy_required
            );
        }
    }

    #[test]
    fn computer_policy_requires_matching_action_and_parent_model_eligibility() {
        let mut policy = resolved(
            ReviewPolicyOptions::default(),
            ModelReviewMessages {
                node_repl_policy: Some("CATALOG COMPUTER POLICY".to_owned()),
                ..Default::default()
            },
            ManagedReviewPolicy::default(),
        );
        assert!(policy.computer_policy_for_action(true).is_none());
        policy.computer_use_eligibility.legacy_required = true;
        assert!(policy.computer_policy_for_action(false).is_none());
        assert_eq!(
            policy.computer_policy_for_action(true),
            Some("CATALOG COMPUTER POLICY")
        );
        policy.node_repl_policy.clear();
        assert!(policy.computer_policy_for_action(true).is_none());
    }

    #[test]
    fn only_exact_developer_delivery_suppresses_computer_policy_injection() {
        let mut policy = resolved(
            ReviewPolicyOptions::default(),
            ModelReviewMessages::default(),
            ManagedReviewPolicy::default(),
        );
        policy.computer_use_eligibility.legacy_required = true;
        let original = policy.missing_computer_policy_message(true, &[]).unwrap();
        assert!(policy
            .missing_computer_policy_message(true, &[ResponseItemEnvelope::new(original.clone())])
            .is_none());
        let mut untrusted = original;
        let ResponseItem::Message { role, .. } = &mut untrusted else {
            panic!()
        };
        *role = "user".to_owned();
        assert!(policy
            .missing_computer_policy_message(true, &[ResponseItemEnvelope::new(untrusted)])
            .is_some());
    }
}
