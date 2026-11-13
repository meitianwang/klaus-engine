//! Admit an explicit source selection into a new child's own inference capture.

use super::{InferenceBundle, InferenceTarget, InferenceTargetState, Session};
use crate::runtime_values::RuntimeString;
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Endpoint-owned source resolution. Credentials and providers never cross the renderer bridge.
pub type SubagentTargetResolver = Arc<
    dyn Fn(String, String) -> BoxFuture<'static, Result<InferenceTargetState, String>>
        + Send
        + Sync,
>;

/// Reserved source prefixes shared by endpoint selection and child admission.
pub const MODEL_TARGET_SOURCES: &[&str] = &[
    "antigravity",
    "trae",
    "kiro",
    "subscription",
    "codex",
    "qoder",
    "qwenwork",
    "workbuddy",
    "cursor",
    "kimi",
    "bailian",
    "minimax",
    "zhipu",
    "ark",
    "custom",
    "local",
];

pub(super) enum SubagentInferenceError {
    Unavailable(String),
    PreparationFailed(String),
}

impl std::fmt::Display for SubagentInferenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::PreparationFailed(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl From<String> for SubagentInferenceError {
    fn from(message: String) -> Self {
        Self::PreparationFailed(message)
    }
}

impl From<&str> for SubagentInferenceError {
    fn from(message: &str) -> Self {
        Self::PreparationFailed(message.into())
    }
}

impl Session {
    /// Install during endpoint session construction, before the session is published.
    pub fn install_subagent_target_resolver(&self, resolver: SubagentTargetResolver) {
        *self.subagents.model_target_resolver.lock_recover() = Some(resolver);
    }

    pub(super) async fn resolve_subagent_inference(
        &self,
        captured: &Arc<InferenceBundle>,
        selection: &RuntimeString,
        signal: &AbortSignal,
    ) -> Result<Arc<InferenceBundle>, String> {
        self.prepare_subagent_inference(captured, selection, signal)
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn prepare_subagent_inference(
        &self,
        captured: &Arc<InferenceBundle>,
        selection: &RuntimeString,
        signal: &AbortSignal,
    ) -> Result<Arc<InferenceBundle>, SubagentInferenceError> {
        use SubagentInferenceError::{PreparationFailed, Unavailable};
        if signal.is_cancelled() || self.is_disposed() {
            return Err(PreparationFailed(
                "Sub-agent model preparation cancelled.".into(),
            ));
        }
        let text = selection.try_to_utf8().ok();
        let tier = captured
            .targets
            .as_ref()
            .and_then(|targets| match text.as_deref() {
                Some("high") => targets.high.as_ref(),
                Some("medium") => targets.medium.as_ref(),
                Some("low") => targets.low.as_ref(),
                _ => None,
            });
        if let Some(InferenceTargetState::Unavailable { code, message, .. }) = tier {
            return Err(Unavailable(format!(
                "Sub-agent model tier unavailable: {code}: {message}"
            )));
        }
        let Some((source, model_id)) = text.as_deref().and_then(|text| text.split_once(':')) else {
            return captured.for_selection(selection).map_err(Unavailable);
        };
        if !MODEL_TARGET_SOURCES.contains(&source) {
            return captured.for_selection(selection).map_err(Unavailable);
        }
        if source.is_empty()
            || source.trim() != source
            || model_id.is_empty()
            || model_id.trim() != model_id
            || selection.len_utf16() > 2048
            || text
                .as_ref()
                .is_some_and(|text| text.chars().any(char::is_control))
        {
            return Err(Unavailable(
                "A sub-agent model requires a nonempty source and concrete model ID.".into(),
            ));
        }
        if signal.is_cancelled() || self.is_disposed() {
            return Err("Sub-agent model preparation cancelled.".into());
        }

        // An already admitted source retains the parent's frozen provider and account.
        let transport_ids = !matches!(source, "custom" | "local");
        if let Some(current) = captured
            .config
            .model_source
            .as_ref()
            .filter(|current| current.source.as_deref() == Some(source))
        {
            let catalog_identity = current.model_id.as_deref() == Some(model_id)
                && captured
                    .source
                    .as_ref()
                    .is_some_and(|(current_source, current_id)| {
                        current_source == source && current_id == model_id
                    })
                && captured.config.model == captured.provider.info().default_model;
            if catalog_identity
                || transport_ids
                    && (current.model_id.as_deref() == Some(model_id)
                        || captured.config.model == model_id)
            {
                return Ok(captured.clone());
            }
            let published_identity =
                captured
                    .source
                    .as_ref()
                    .is_some_and(|(current_source, current_id)| {
                        current_source == source && current_id == model_id
                    });
            if transport_ids && published_identity {
                return captured
                    .for_selection(&captured.provider.info().default_model)
                    .map_err(Unavailable);
            }
        }
        let targets = captured.targets.as_ref();
        let mut unavailable = None;
        for target in [
            targets.and_then(|targets| targets.high.as_ref()),
            targets.and_then(|targets| targets.medium.as_ref()),
            targets.and_then(|targets| targets.low.as_ref()),
            captured.advisor_target.as_ref(),
            captured.feature_targets.title.as_ref(),
            captured.feature_targets.prediction.as_ref(),
            captured.feature_targets.memory.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            match target {
                InferenceTargetState::Ready(target)
                    if target.source == source
                        && (target.model_id == model_id
                            || transport_ids && target.model == model_id) =>
                {
                    return Ok(child_bundle(captured, target.clone()));
                }
                InferenceTargetState::Unavailable {
                    source: target_source,
                    model_id: target_model,
                    code,
                    message,
                } if target_source == source && target_model == model_id => {
                    unavailable = Some((code, message));
                }
                _ => {}
            }
        }
        if let Some((code, message)) = unavailable {
            return Err(Unavailable(format!(
                "Sub-agent model unavailable: {code}: {source}/{model_id}: {message}"
            )));
        }

        let resolver = self
            .subagents
            .model_target_resolver
            .lock_recover()
            .clone()
            .ok_or_else(|| {
                PreparationFailed(
                    "Source-qualified sub-agent models require an endpoint source resolver.".into(),
                )
            })?;
        let prepare = async {
            let target = resolver(source.to_string(), model_id.to_string()).await?;
            let mut target = match target {
                InferenceTargetState::Ready(target) => target,
                InferenceTargetState::Unavailable {
                    source: target_source,
                    model_id: target_model,
                    code,
                    message,
                } => {
                    if target_source != source || target_model != model_id {
                        return Err(PreparationFailed(
                            "Sub-agent model resolution returned a different source or model."
                                .into(),
                        ));
                    }
                    return Err(Unavailable(format!(
                        "Sub-agent model unavailable: {code}: {source}/{model_id}: {message}"
                    )));
                }
            };
            if target.source != source || target.model_id != model_id {
                return Err(PreparationFailed(
                    "Sub-agent model resolution returned a different source or model.".into(),
                ));
            }
            target.review_requirements = Some(
                self.prepare_review_requirements(target.provider.clone())
                    .await?,
            );
            Ok(child_bundle(captured, target))
        };
        let resolved = tokio::select! {
            biased;
            _ = signal.cancelled() => Err(PreparationFailed("Sub-agent model preparation cancelled.".into())),
            result = tokio::time::timeout(super::session_inference_part::MODEL_UPDATE_TIMEOUT, prepare) => {
                result.unwrap_or_else(|_| Err(PreparationFailed("Sub-agent model preparation timed out after 75 seconds.".into())))
            }
        }?;
        if self.is_disposed() || signal.is_cancelled() {
            return Err("Sub-agent model preparation cancelled.".into());
        }
        Ok(resolved)
    }
}

fn child_bundle(captured: &InferenceBundle, target: InferenceTarget) -> Arc<InferenceBundle> {
    let snapshot = captured.snapshot_for_target(None, Some(target), false);
    captured.bundle_for_target(snapshot, None).capture(&[])
}
