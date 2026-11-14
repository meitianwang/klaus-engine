//! Immutable inference capture and atomic publication; conversation ownership stays on Session.

use super::{
    FeatureTargets, InferencePublication, InferenceTarget, InferenceTargetState, InferenceTargets,
    PreparedReviewRequirements, Session, SharedDeps,
};
use crate::engine::deps::{production_deps, EngineDepsOverrides};
use crate::extensions::runtime_snapshot::{ExtensionSource, PricingRuntimeSnapshot};
use crate::observability::cost_tracker::{PricingTable, DEFAULT_PRICING};
use crate::schemas::config::{ModelTier, ResolvedConfig};
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::types::abort::AbortSignal;
use crate::types::events::{InferenceIdentity, InferenceSource};
use crate::types::provider::ModelProvider;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

pub(crate) const MODEL_UPDATE_TIMEOUT: Duration = Duration::from_secs(75);
#[derive(Clone)]
pub(crate) struct InferenceBundle {
    pub revision: u64,
    pub main_revision: u64,
    /// Identity of the publication that produced this bundle, so a host can
    /// reconcile a lost ACK instead of guessing from a bumped revision alone.
    pub change_id: Option<String>,
    pub provider: Arc<dyn ModelProvider>,
    pub review_cloud:
        Option<Arc<crate::engine::approval_requirements_cloud::CloudManagedRequirementsProvider>>,
    pub review_requirements: Arc<crate::engine::approval_requirements::ManagedReviewRequirements>,
    pub config: ResolvedConfig,
    pub context_window_override: Option<u64>,
    pub deps: SharedDeps,
    pub pricing: PricingRuntimeSnapshot,
    pub targets: Option<InferenceTargets>,
    pub advisor_target: Option<InferenceTargetState>,
    pub orchestrator_target: Option<InferenceTargetState>,
    pub orchestration_mode: bool,
    pub orchestrator_fallback_to_main: bool,
    pub feature_targets: FeatureTargets,
    pub source: Option<(String, String)>,
    /// Immutable rate table carried by the request that captured this bundle,
    /// so a later publication cannot reprice work already in flight.
    pub rates: PricingTable,
    /// Usable context window of the larger prior selection when switching down
    /// to a smaller model, without retaining the deselected provider.
    pub context_downshift: Option<Arc<ContextWindowDownshift>>,
}

pub(crate) struct ContextWindowDownshift {
    previous_usable_window: u64,
    completed: std::sync::atomic::AtomicBool,
}

impl ContextWindowDownshift {
    pub(crate) fn new(previous_usable_window: u64) -> Self {
        Self {
            previous_usable_window,
            completed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn previous_usable_window(&self) -> Option<u64> {
        (!self.completed.load(std::sync::atomic::Ordering::Acquire))
            .then_some(self.previous_usable_window)
    }

    pub(crate) fn complete(&self) {
        self.completed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

pub(crate) async fn prepare_review_requirements_for_provider(
    provider: Arc<dyn ModelProvider>,
    user_config_dir: Option<&crate::runtime_values::RuntimePath>,
) -> std::io::Result<PreparedReviewRequirements> {
    let cloud = provider.managed_review_cloud_auth_source().map(|auth| {
        let root = user_config_dir
            .map(|path| {
                std::path::PathBuf::from(
                    String::from_utf8(path.as_runtime_string().to_node_utf8())
                        .expect("Node path is UTF-8"),
                )
            })
            .unwrap_or_else(|| std::path::PathBuf::from(crate::fs::paths::config_home()));
        crate::engine::approval_requirements_cloud::CloudManagedRequirementsProvider::new(
            auth, root,
        )
    });
    let requirements = Arc::new(crate::engine::approval_requirements::load_managed_review_requirements_with_bundle(
        cloud.as_deref().map(|provider| provider as &dyn crate::engine::approval_requirements::TrustedRequirementsBundleProvider),
    ).await?);
    Ok(PreparedReviewRequirements {
        provider,
        cloud,
        requirements,
    })
}

impl InferenceBundle {
    fn downshift_to(&self, target: &ResolvedConfig) -> Option<Arc<ContextWindowDownshift>> {
        let previous_usable_window = self
            .context_downshift
            .as_ref()
            .and_then(|downshift| downshift.previous_usable_window())
            .unwrap_or_else(|| {
                crate::context::compact::policy::usable_context_window(&self.config)
            });
        let target_usable_window = crate::context::compact::policy::usable_context_window(target);
        if previous_usable_window <= target_usable_window {
            return None;
        }
        // Keep one high-water prior window across rapid, unused selections.
        Some(Arc::new(ContextWindowDownshift {
            previous_usable_window,
            completed: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    pub(crate) async fn load_review_requirements(
        &self,
    ) -> std::io::Result<Arc<crate::engine::approval_requirements::ManagedReviewRequirements>> {
        crate::engine::approval_requirements::load_managed_review_requirements_with_bundle(
            self.review_cloud.as_deref().map(|provider| provider as &dyn crate::engine::approval_requirements::TrustedRequirementsBundleProvider),
        ).await.map(Arc::new)
    }

    pub(crate) fn load_review_requirements_sync(
        &self,
    ) -> std::io::Result<Arc<crate::engine::approval_requirements::ManagedReviewRequirements>> {
        let bundles = self
            .review_cloud
            .as_deref()
            .map(|provider| provider.snapshot_sync().map_err(std::io::Error::from))
            .transpose()?
            .unwrap_or_default();
        crate::engine::approval_requirements::load_managed_review_requirements_with_bundles_sync(
            bundles,
        )
        .map(Arc::new)
    }
    pub(super) fn capture(
        self: &Arc<Self>,
        extra_models: &[crate::runtime_values::RuntimeString],
    ) -> Arc<Self> {
        let mut models = extra_models.to_vec();
        models.push(self.config.model.clone());
        models.push(self.provider.info().default_model.clone());
        models.extend(self.config.fallback_models.iter().cloned());
        models.extend(
            [ModelTier::High, ModelTier::Medium, ModelTier::Low]
                .map(|tier| self.config.model_for_tier(tier)),
        );
        // Independent targets are frozen by their own providers below. Their
        // model names do not enlarge the main provider's admitted model scope.
        models.sort();
        models.dedup();
        let mut captured = (**self).clone();
        captured.advisor_target = self.advisor_target.as_ref().map(|state| match state {
            InferenceTargetState::Ready(target) => InferenceTargetState::Ready(InferenceTarget {
                provider: crate::provider::model_catalog::capture_model_provider(
                    target.provider.clone(),
                    std::slice::from_ref(&target.model),
                ),
                ..target.clone()
            }),
            state => state.clone(),
        });
        captured.orchestrator_target = self.orchestrator_target.as_ref().map(|state| match state {
            InferenceTargetState::Ready(target) => InferenceTargetState::Ready(InferenceTarget {
                provider: crate::provider::model_catalog::capture_model_provider(
                    target.provider.clone(),
                    std::slice::from_ref(&target.model),
                ),
                ..target.clone()
            }),
            state => state.clone(),
        });
        captured.feature_targets = self.feature_targets.frozen();
        captured.provider =
            crate::provider::model_catalog::capture_model_provider(self.provider.clone(), &models);
        // An unset output ceiling means "follow the selected model's capability".
        // `config_for_model` resolves that to a concrete number, so freezing it
        // here would pin the OLD model's ceiling onto every later selection made
        // from this bundle — a tier or explicit switch would keep the previous
        // model's limit. Keep the dynamic form and let each selection resolve it
        // against the provider, which is itself frozen just above.
        let explicit_max_output = self.config.max_output_tokens;
        captured.config = captured.config_for_model(&self.config.model);
        if explicit_max_output.is_none() {
            captured.config.max_output_tokens = None;
        }
        // Keep injected transport/estimation seams; reject uncaptured models
        // before they can reach those seams or consult a newly refreshed catalog.
        let provider = captured.provider.clone();
        let call_model = self.deps.call_model.clone();
        captured.deps.call_model = Arc::new(move |mut request, signal| {
            if !provider.has_model_capabilities(&request.model) {
                return crate::provider::model_catalog::uncaptured_model_stream(&request.model);
            }
            request.bind_media_capabilities(&provider.capabilities(&request.model));
            call_model(request, signal)
        });
        if let Some(targets) = &self.targets {
            let freeze = |state: &Option<InferenceTargetState>| -> Option<InferenceTargetState> {
                match state {
                    Some(InferenceTargetState::Ready(target)) => {
                        Some(InferenceTargetState::Ready(InferenceTarget {
                            provider: crate::provider::model_catalog::capture_model_provider(
                                target.provider.clone(),
                                std::slice::from_ref(&target.model),
                            ),
                            ..target.clone()
                        }))
                    }
                    other => other.clone(),
                }
            };
            captured.targets = Some(InferenceTargets {
                high: freeze(&targets.high),
                medium: freeze(&targets.medium),
                low: freeze(&targets.low),
            });
        }
        Arc::new(captured)
    }

    /// Resolve a tier token or model selection against THIS captured bundle.
    ///
    /// Tier semantics belong to the shared `select()` implementation, so a tier
    /// target always resolves through the bundle that captured it: an in-flight
    /// step keeps its own targets, pricing and capabilities, and never picks up
    /// a publication that landed after it started. The returned bundle carries
    /// this capture's revision and change identity, so events and billing stay
    /// attributed to the work that actually ran.
    pub fn for_selection(
        self: &Arc<Self>,
        selection: &crate::runtime_values::RuntimeString,
    ) -> Result<Arc<Self>, String> {
        let tier = selection
            .try_to_utf8()
            .ok()
            .as_deref()
            .and_then(tier_name_for_selection);
        let target = self.select(Some(selection))?;
        Ok(self.bundle_for_target(target, tier))
    }

    /// The tier-token form of [`Self::for_selection`].
    pub fn for_tier(self: &Arc<Self>, tier: ModelTier) -> Result<Arc<Self>, String> {
        let name = tier_name(tier);
        self.for_selection(&crate::runtime_values::RuntimeString::from(name))
    }

    /// Rebuild a bundle around a resolved target without losing this capture's
    /// identity, rate table or frozen tier targets.
    pub(super) fn bundle_for_target(
        &self,
        target: super::TargetSnapshot,
        tier: Option<&str>,
    ) -> Arc<Self> {
        if target.target.is_none() && target.model == self.config.model && tier.is_none() {
            return Arc::new(self.clone());
        }
        let mut selected = self.clone();
        selected.provider = target.provider;
        selected.config = target.config;
        selected.deps = target.deps;
        // A tier target brings its own immutable rate table AND its own
        // source-qualified identity: attribution must name the target's source
        // and tier, not the main provider this bundle was captured with. A
        // main-provider resolution keeps this capture's identity.
        if let Some(tier_target) = &target.target {
            selected.context_window_override = None;
            let prepared = tier_target
                .review_requirements
                .as_ref()
                .expect("published target requirements were admitted");
            selected.review_cloud = prepared.cloud.clone();
            selected.review_requirements = prepared.requirements.clone();
            selected.rates = tier_target.pricing.clone();
            selected.source = Some((tier_target.source.clone(), tier_target.model_id.clone()));
            selected.config.model_source = Some(InferenceSource {
                source: Some(tier_target.source.clone()),
                model_id: Some(tier_target.model_id.clone()),
                tier: tier.map(str::to_string),
                selection: Some(format!("{}:{}", tier_target.source, tier_target.model_id)),
            });
            return Arc::new(selected);
        }
        if let Some(source) = selected.config.model_source.as_mut() {
            source.model_id = target.model.try_to_utf8().ok();
            source.selection = source
                .source
                .as_ref()
                .zip(source.model_id.as_ref())
                .map(|(source, model)| format!("{source}:{model}"));
        }
        Arc::new(selected)
    }

    pub fn config_for_model(&self, model: &crate::runtime_values::RuntimeString) -> ResolvedConfig {
        let mut config = self.config.clone();
        let capabilities = self.provider.capabilities(model);
        config.model = model.clone();
        config.model_max_output_tokens = Some(capabilities.max_output_tokens);
        config.model_effective_context_window_percent =
            Some(self.provider.effective_context_window_percent(model));
        config.model_auto_compact_token_limit = self.provider.model_auto_compact_token_limit(model);
        config.context_window = self
            .context_window_override
            .unwrap_or(capabilities.context_window);
        config.model_context_window = Some(config.context_window.min(capabilities.context_window));
        config.max_output_tokens = Some(
            config
                .max_output_tokens
                .unwrap_or(capabilities.max_output_tokens)
                .min(capabilities.max_output_tokens),
        );
        config
    }

    pub fn identity(&self, model: crate::runtime_values::RuntimeString) -> InferenceIdentity {
        let source = self.config.model_source.as_ref();
        let selection = if model == self.config.model {
            source.and_then(|source| source.selection.clone())
        } else {
            source
                .and_then(|source| source.source.as_ref())
                .zip(model.try_to_utf8().ok())
                .map(|(source, model)| format!("{source}:{model}"))
        };
        InferenceIdentity {
            revision: self.revision,
            change_id: self.change_id.clone().map(Into::into),
            model,
            source: source
                .and_then(|source| source.source.clone())
                .map(Into::into),
            tier: source
                .and_then(|source| source.tier.clone())
                .map(Into::into),
            selection: selection.map(Into::into),
        }
    }
}

struct PendingModelUpdate {
    change_id: String,
    started: Instant,
    error: Option<String>,
}

/// Which of a bundle's two revisions a validated input was checked against.
///
/// The main provider and the auxiliary tier targets bump independently: a
/// tier-only publication must not invalidate an input validated for the main
/// model, and a main publication must not be mistaken for a tier change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InputModelRevision {
    Main(u64),
    Tier(u64),
}

impl InputModelRevision {
    pub(super) fn matches(self, bundle: &InferenceBundle) -> bool {
        match self {
            Self::Main(revision) => revision == bundle.main_revision,
            Self::Tier(revision) => revision == bundle.revision,
        }
    }
}

#[derive(Default)]
pub(super) struct InferenceAdmission {
    pub active: usize,
    pub closed: bool,
    pending: Option<PendingModelUpdate>,
    commits: HashMap<String, (u64, String)>,
}

/// Ownership starts synchronously at work registration, not at its first poll.
pub(crate) struct InferenceActivityGuard {
    admission: Arc<Mutex<InferenceAdmission>>,
    pub bundle: Arc<InferenceBundle>,
    owner: Option<Arc<InferenceActivityGuard>>,
}

impl InferenceActivityGuard {
    pub fn with_bundle(self: &Arc<Self>, bundle: Arc<InferenceBundle>) -> Arc<Self> {
        Arc::new(Self {
            admission: self.admission.clone(),
            bundle,
            owner: Some(self.clone()),
        })
    }
}

impl Drop for InferenceActivityGuard {
    fn drop(&mut self) {
        if self.owner.is_none() {
            self.admission.lock_recover().active -= 1;
        }
    }
}

pub(crate) type InferenceSlot = Arc<Mutex<Option<Arc<InferenceActivityGuard>>>>;

impl Session {
    pub(crate) fn bundle_from_slot(
        slot: &InferenceSlot,
        initial: &Arc<InferenceBundle>,
    ) -> Arc<InferenceBundle> {
        slot.lock_recover()
            .as_ref()
            .map(|guard| guard.bundle.clone())
            .unwrap_or_else(|| initial.clone())
    }

    pub(super) fn inference_snapshot(&self) -> Arc<InferenceBundle> {
        self.inference.read_recover().clone()
    }

    pub(super) fn provider(&self) -> Arc<dyn ModelProvider> {
        self.inference_snapshot().provider.clone()
    }

    pub(super) fn inference_config(&self) -> ResolvedConfig {
        self.inference_snapshot().config.clone()
    }

    pub fn runtime_inference_settings(&self) -> crate::schemas::config::RuntimeInferenceSettings {
        let config = self.inference_config();
        crate::schemas::config::RuntimeInferenceSettings {
            model_verbosity: config.model_verbosity,
            reasoning_summary: config.reasoning_summary,
            web_search_access: config.web_search.access,
        }
    }

    /// Publish one immutable settings snapshot. In-flight requests retain the
    /// capture they started with; the next admitted step observes all changes.
    pub fn set_runtime_inference_settings(
        &self,
        model_verbosity: Option<Option<crate::types::provider::OutputVerbosity>>,
        reasoning_summary: Option<crate::types::provider::ReasoningSummary>,
        web_search_access: Option<crate::schemas::config::WebSearchAccess>,
    ) -> Result<crate::schemas::config::RuntimeInferenceSettings, String> {
        if model_verbosity.is_none() && reasoning_summary.is_none() && web_search_access.is_none() {
            return Ok(self.runtime_inference_settings());
        }
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".into());
        }
        if admission.pending.is_some() {
            return Err("model update is pending".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        if let Some(value) = model_verbosity {
            next.config.model_verbosity = value;
        }
        if let Some(value) = reasoning_summary {
            next.config.reasoning_summary = value;
        }
        if let Some(value) = web_search_access {
            next.config.web_search.set_access(value);
        }
        let settings = crate::schemas::config::RuntimeInferenceSettings {
            model_verbosity: next.config.model_verbosity,
            reasoning_summary: next.config.reasoning_summary,
            web_search_access: next.config.web_search.access,
        };
        if next.config == current.config {
            return Ok(settings);
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        *current = Arc::new(next);
        drop(current);
        drop(admission);
        self.inference_changed.notify_waiters();
        Ok(settings)
    }

    /// Publish the directory policy and model choices as one captured setting.
    pub fn set_subagent_model_settings(
        &self,
        catalog: Option<bool>,
        options: Option<Vec<crate::schemas::config::SubagentModelOption>>,
    ) -> Result<(bool, Vec<crate::schemas::config::SubagentModelOption>), String> {
        if catalog.is_none() && options.is_none() {
            let config = self.inference_config();
            return Ok((config.subagent_model_catalog, config.subagent_model_options));
        }
        if let Some(options) = &options {
            crate::schemas::config::validate_subagent_model_options(options)?;
        }
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".into());
        }
        if admission.pending.is_some() {
            return Err("model update is pending".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        if let Some(catalog) = catalog {
            next.config.subagent_model_catalog = catalog;
        }
        if let Some(options) = options {
            next.config.subagent_model_options = options;
        }
        let settings = (
            next.config.subagent_model_catalog,
            next.config.subagent_model_options.clone(),
        );
        if next.config == current.config {
            return Ok(settings);
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        *current = Arc::new(next);
        drop(current);
        drop(admission);
        self.inference_changed.notify_waiters();
        Ok(settings)
    }

    /// The live local orchestration mode.
    pub fn ultra_mode(&self) -> bool {
        self.inference_config().ultra_mode
    }

    /// Publish one immutable orchestration-mode snapshot. In-flight requests
    /// retain the capture they started with; the next admitted step observes
    /// the change, exactly like the other runtime inference settings.
    pub fn set_ultra_mode(&self, enabled: bool) -> Result<bool, String> {
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".into());
        }
        if admission.pending.is_some() {
            return Err("model update is pending".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        next.config.ultra_mode = enabled;
        if next.config == current.config {
            return Ok(next.config.ultra_mode);
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        *current = Arc::new(next);
        drop(current);
        drop(admission);
        self.inference_changed.notify_waiters();
        Ok(enabled)
    }

    pub fn runtime_approval_review(
        &self,
    ) -> Option<crate::engine::approval_async::AsyncFeatureConfig> {
        self.inference_config().approval_review
    }

    /// Replace the local review configuration while preserving active request captures.
    pub fn set_runtime_approval_review(
        &self,
        value: Option<crate::engine::approval_async::AsyncFeatureConfig>,
    ) -> Result<(), String> {
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".into());
        }
        if admission.pending.is_some() {
            return Err("model update is pending".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        next.config.approval_review = value;
        next.config.validate()?;
        if next.config == current.config {
            return Ok(());
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        *current = Arc::new(next);
        drop(current);
        drop(admission);
        self.approval_async.reset();
        self.inference_changed.notify_waiters();
        Ok(())
    }

    pub(super) fn begin_inference_activity(&self) -> Result<InferenceActivityGuard, String> {
        self.begin_inference_activity_for(None)
    }

    pub(super) fn begin_inference_activity_for(
        &self,
        model: Option<&crate::runtime_values::RuntimeString>,
    ) -> Result<InferenceActivityGuard, String> {
        let mut admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot start inference on a closing session".to_string());
        }
        if let Some(pending) = &admission.pending {
            return Err(pending
                .error
                .clone()
                .unwrap_or_else(|| "model update is pending".to_string()));
        }
        let bundle = self.capture_inference_bundle(model)?;
        admission.active = admission
            .active
            .checked_add(1)
            .ok_or_else(|| "inference admission overflow".to_string())?;
        Ok(InferenceActivityGuard {
            admission: self.inference_admission.clone(),
            bundle,
            owner: None,
        })
    }

    pub(crate) async fn capture_inference(
        &self,
        signal: &AbortSignal,
    ) -> Result<Arc<InferenceActivityGuard>, String> {
        self.capture_inference_for(signal, None).await
    }

    pub(crate) async fn capture_inference_for(
        &self,
        signal: &AbortSignal,
        model: Option<&crate::runtime_values::RuntimeString>,
    ) -> Result<Arc<InferenceActivityGuard>, String> {
        loop {
            // Register before inspecting the state to avoid a lost publication/close wakeup.
            let notified = self.inference_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = {
                let mut admission = self.inference_admission.lock_recover();
                if admission.closed || self.is_disposed() {
                    return Err("cannot start inference on a closing session".to_string());
                }
                if signal.is_cancelled() {
                    return Err("inference capture cancelled".to_string());
                }
                if let Some(pending) = &mut admission.pending {
                    if pending.started.elapsed() >= MODEL_UPDATE_TIMEOUT && pending.error.is_none()
                    {
                        pending.error = Some("model provider preparation timed out".to_string());
                    }
                    if let Some(error) = &pending.error {
                        return Err(error.clone());
                    }
                    pending.started + MODEL_UPDATE_TIMEOUT
                } else {
                    let bundle = self.capture_inference_bundle(model)?;
                    admission.active = admission
                        .active
                        .checked_add(1)
                        .ok_or_else(|| "inference admission overflow".to_string())?;
                    return Ok(Arc::new(InferenceActivityGuard {
                        admission: self.inference_admission.clone(),
                        bundle,
                        owner: None,
                    }));
                }
            };
            tokio::select! {
                biased;
                _ = signal.cancelled() => return Err("inference capture cancelled".to_string()),
                _ = notified => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }

    fn capture_inference_bundle(
        &self,
        model: Option<&crate::runtime_values::RuntimeString>,
    ) -> Result<Arc<InferenceBundle>, String> {
        // A legacy `set_model` STAGES its selection instead of rewriting the
        // bundle, and a tier token resolves through the bundle's own targets.
        // Either way the captured bundle must already name the model this work
        // will actually request, because consumers read the capture rather than
        // re-resolving against a live session.
        let effective = model.cloned().or_else(|| self.pending_model());
        let mut models: Vec<_> = effective.iter().cloned().collect();
        models.extend(self.pending_model());
        models.extend(
            self.current_agent_registry()
                .list()
                .into_iter()
                .filter_map(|agent| agent.model)
                .filter(|model| model != "inherit"),
        );
        let snapshot = self.inference_snapshot();
        let captured = snapshot.capture(&models);
        let resolved = match &effective {
            Some(effective) if *effective != captured.config.model => {
                // Fail closed: an unresolvable selection must not silently fall
                // back to the previously captured model.
                captured.for_selection(effective)?
            }
            _ => captured,
        };
        // The published compactor may hold live model facts even when the model
        // id did not change. Every work capture gets a summary transport bound
        // to this snapshot, including catalog-only capability updates.
        let mut rebound = (*resolved).clone();
        rebound.deps = rebound.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        Ok(Arc::new(rebound))
    }

    pub fn begin_model_update(
        &self,
        change_id: String,
        expected_revision: Option<u64>,
    ) -> Result<u64, String> {
        if change_id.is_empty() || change_id.len() > 256 {
            return Err(
                "model changeId must be a nonempty string of at most 256 bytes".to_string(),
            );
        }
        let mut admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".to_string());
        }
        let revision = self.inference_snapshot().revision;
        if let Some((committed, _)) = admission.commits.get(&change_id) {
            return Ok(*committed);
        }
        if admission
            .pending
            .as_ref()
            .is_some_and(|pending| pending.change_id == change_id)
        {
            return Ok(revision);
        }
        if expected_revision.is_some_and(|expected| expected != revision) {
            return Err("model reconfiguration revision conflict".to_string());
        }
        admission.pending = Some(PendingModelUpdate {
            change_id,
            started: Instant::now(),
            error: None,
        });
        drop(admission);
        self.inference_changed.notify_waiters();
        Ok(revision)
    }

    pub fn fail_model_update(&self, change_id: &str, error: String) {
        let mut admission = self.inference_admission.lock_recover();
        if let Some(pending) = &mut admission.pending {
            if pending.change_id == change_id {
                pending.error = Some(error);
            }
        }
        drop(admission);
        self.inference_changed.notify_waiters();
    }

    pub fn model_update_status(&self) -> (Option<String>, Option<String>, Option<String>) {
        let mut admission = self.inference_admission.lock_recover();
        if let Some(pending) = &mut admission.pending {
            if pending.error.is_none() && pending.started.elapsed() >= MODEL_UPDATE_TIMEOUT {
                pending.error = Some("model provider preparation timed out".to_string());
            }
        }
        (
            self.inference_snapshot().change_id.clone(),
            admission
                .pending
                .as_ref()
                .map(|pending| pending.change_id.clone()),
            admission
                .pending
                .as_ref()
                .and_then(|pending| pending.error.clone()),
        )
    }

    #[cfg(test)]
    pub(crate) fn review_requirements_test_snapshot(&self) -> Arc<InferenceBundle> {
        self.inference_snapshot()
    }

    pub fn model_revision(&self) -> u64 {
        self.inference_snapshot().revision
    }

    pub fn model_runtime(
        &self,
    ) -> (
        u64,
        ResolvedConfig,
        crate::types::provider::ProviderCapabilities,
    ) {
        let _admission = self.inference_admission.lock_recover();
        let bundle = self.inference_snapshot();
        let model = self
            .pending_model()
            .unwrap_or_else(|| bundle.config.model.clone());
        (
            bundle.revision,
            bundle.config_for_model(&model),
            bundle.provider.capabilities(&model),
        )
    }

    pub(super) fn set_legacy_model(
        &self,
        model: crate::runtime_values::RuntimeString,
        active: bool,
    ) -> Result<(), String> {
        let base = self.inference_snapshot();
        let previous = self.capture_inference_bundle(None)?;
        let selected = base.for_selection(&model)?;
        let requirements = selected
            .load_review_requirements_sync()
            .map_err(|error| error.to_string())?;
        let admission = self.inference_admission.lock_recover();
        if admission.closed || admission.pending.is_some() || self.is_disposed() {
            return Err("Cannot change the model while the session is closing or a model publication is pending.".into());
        }
        let turns = self.turn.active_stack.lock_recover();
        let mut current = self.inference.write_recover();
        if !Arc::ptr_eq(&current, &base) {
            return Err("Model changed while its managed requirements were prepared.".into());
        }
        let revision = current
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        let mut policy = self.runtime_policy.write_recover();
        let previous_policy = policy.state();
        let mut next_policy = previous_policy.clone();
        if requirements.requires_review_for_model(
            &String::from_utf8(selected.config.model.to_node_utf8()).expect("Node text is UTF-8"),
        ) {
            next_policy.approvals_reviewer =
                Some(crate::types::permission::ApprovalsReviewer::Automatic);
        }
        super::session_core_policy_part::validate_review_requirements(
            &requirements,
            &next_policy,
            &selected.config.model,
        )?;
        let mut next = (**current).clone();
        next.revision = revision;
        next.main_revision = revision;
        // A legacy switch STAGES the model rather than rewriting the bundle's
        // main model. Every consumer resolves the pending selection through the
        // captured bundle, which is what lets a tier-only publication, an
        // auxiliary snapshot and the compaction summary each resolve it against
        // their own captured targets and rebind their own transport. Writing
        // `config.model` here made that resolution a no-op, so the compaction
        // summary kept riding the model it was first pinned to.
        // The change is not host-published, so it clears any prior changeId.
        next.change_id = None;
        next.context_downshift = previous.downshift_to(&selected.config);
        self.mode.set_pending_model(Some(model.clone()));
        if active {
            if let Some(turn) = turns.last() {
                *turn.live_model_override.lock_recover() = Some(model);
            }
        }
        *current = Arc::new(next);
        if next_policy != previous_policy {
            next_policy.generation = previous_policy.generation.saturating_add(1);
            *policy = Arc::new(super::RuntimePolicySnapshot {
                state: next_policy,
                sandbox_backend: policy.sandbox_backend.clone(),
            });
        }
        Ok(())
    }

    pub fn reconfigure_inference(
        self: &Arc<Self>,
        provider: Arc<dyn ModelProvider>,
        selected: ResolvedConfig,
        context_window_override: Option<u64>,
        pricing: Option<PricingTable>,
        expected_revision: u64,
    ) -> Result<u64, String> {
        self.reconfigure_inference_with_change_id(
            provider,
            selected,
            context_window_override,
            pricing,
            expected_revision,
            None,
        )
    }

    pub fn reconfigure_inference_with_change_id(
        self: &Arc<Self>,
        provider: Arc<dyn ModelProvider>,
        selected: ResolvedConfig,
        context_window_override: Option<u64>,
        pricing: Option<PricingTable>,
        expected_revision: u64,
        change_id: Option<String>,
    ) -> Result<u64, String> {
        self.publish_inference(
            provider,
            selected,
            context_window_override,
            pricing,
            InferencePublication {
                expected_revision,
                change_id,
                targets: None,
                advisor_target: self.inference_snapshot().advisor_target.clone(),
                orchestrator_target: self.inference_snapshot().orchestrator_target.clone(),
                orchestration_mode: self.inference_snapshot().orchestration_mode,
                orchestrator_fallback_to_main: self
                    .inference_snapshot()
                    .orchestrator_fallback_to_main,
                feature_targets: self.inference_snapshot().feature_targets.clone(),
                source: None,
                rates: None,
                publication_key: None,
                review_requirements: None,
            },
        )
    }

    /// Admit the selected transport's account requirements before its atomic publication.
    pub async fn prepare_review_requirements(
        &self,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<PreparedReviewRequirements, String> {
        prepare_review_requirements_for_provider(provider, self.user_config_dir.as_ref())
            .await
            .map_err(|error| error.to_string())
    }

    /// Publish the main provider and every prepared auxiliary target atomically.
    /// `publication_key` is canonical wire input; only its digest is retained.
    pub fn reconfigure_inference_with_targets(
        self: &Arc<Self>,
        provider: Arc<dyn ModelProvider>,
        selected: ResolvedConfig,
        context_window_override: Option<u64>,
        pricing: Option<PricingTable>,
        publication: InferencePublication,
    ) -> Result<u64, String> {
        self.publish_inference(
            provider,
            selected,
            context_window_override,
            pricing,
            publication,
        )
    }

    fn publish_inference(
        self: &Arc<Self>,
        provider: Arc<dyn ModelProvider>,
        selected: ResolvedConfig,
        context_window_override: Option<u64>,
        pricing: Option<PricingTable>,
        publication: InferencePublication,
    ) -> Result<u64, String> {
        let InferencePublication {
            expected_revision,
            change_id,
            targets: mut publication_targets,
            mut advisor_target,
            mut orchestrator_target,
            orchestration_mode,
            orchestrator_fallback_to_main,
            mut feature_targets,
            source: publication_source,
            rates: publication_rates,
            publication_key,
            review_requirements: prepared_requirements,
        } = publication;
        let publication_rates_len = publication_rates.as_ref().map(|rates| rates.len());
        // Validate and bind expensive dependencies outside the publication lock.
        let fail_preparation = |error: String| {
            if let Some(id) = &change_id {
                self.fail_model_update(id, error.clone());
            }
            error
        };
        selected.validate().map_err(&fail_preparation)?;
        super::session_targets_part::prepare_targets_requirements_sync(
            &mut publication_targets,
            &mut advisor_target,
        )
        .map_err(&fail_preparation)?;
        super::session_targets_part::prepare_targets_requirements_sync(
            &mut None,
            &mut orchestrator_target,
        )
        .map_err(&fail_preparation)?;
        super::session_targets_part::prepare_feature_requirements_sync(&mut feature_targets)
            .map_err(&fail_preparation)?;
        let (review_cloud, requirements) = match prepared_requirements {
            Some(prepared) => {
                if !Arc::ptr_eq(&prepared.provider, &provider) {
                    return Err(fail_preparation(
                        "Prepared approval requirements belong to a different provider.".into(),
                    ));
                }
                (prepared.cloud, prepared.requirements)
            }
            None if provider.managed_review_cloud_auth_source().is_some() => {
                return Err(fail_preparation("The selected provider requires asynchronous approval requirements preparation.".into()));
            }
            None => (
                None,
                Arc::new(
                    crate::engine::approval_requirements::load_managed_review_requirements_sync()
                        .map_err(|error| fail_preparation(error.to_string()))?,
                ),
            ),
        };
        if selected.model.is_empty() {
            return Err(fail_preparation("model must not be empty".to_string()));
        }
        let base = self.inference_snapshot();
        let previous = self
            .capture_inference_bundle(None)
            .map_err(&fail_preparation)?;
        if previous.main_revision != base.main_revision {
            return Err(fail_preparation(
                "model reconfiguration revision conflict".into(),
            ));
        }
        let mut config = merge_inference_config(&base.config, &selected);
        let mut target_ids: Vec<String> = match &publication_targets {
            Some(targets) => [
                ("high", targets.high.as_ref()),
                ("medium", targets.medium.as_ref()),
                ("low", targets.low.as_ref()),
            ]
            .into_iter()
            .filter_map(|(tier, state)| state.map(|state| (tier, state)))
            .map(|(tier, state)| match state {
                InferenceTargetState::Ready(target) => {
                    format!("{tier}=ready:{}:{}", target.source, target.model_id)
                }
                InferenceTargetState::Unavailable {
                    source, model_id, ..
                } => format!("{tier}=unavailable:{source}:{model_id}"),
            })
            .collect(),
            None => Vec::new(),
        };
        target_ids.push(match &advisor_target {
            Some(InferenceTargetState::Ready(target)) => {
                format!("advisor=ready:{}:{}", target.source, target.model_id)
            }
            Some(InferenceTargetState::Unavailable {
                source,
                model_id,
                code,
                message,
            }) => format!("advisor=unavailable:{source}:{model_id}:{code}:{message}"),
            None => "advisor=disabled".to_string(),
        });
        target_ids.push(match &orchestrator_target {
            Some(InferenceTargetState::Ready(target)) => {
                format!("orchestrator=ready:{}:{}", target.source, target.model_id)
            }
            Some(InferenceTargetState::Unavailable {
                source,
                model_id,
                code,
                message,
            }) => format!("orchestrator=unavailable:{source}:{model_id}:{code}:{message}"),
            None => "orchestrator=medium".to_string(),
        });
        target_ids.push(format!("orchestration_mode={orchestration_mode}"));
        target_ids.push(format!(
            "orchestrator_fallback_to_main={orchestrator_fallback_to_main}"
        ));
        for (name, state) in feature_targets.slots() {
            target_ids.push(match state {
                Some(InferenceTargetState::Ready(target)) => {
                    format!("feature.{name}=ready:{}:{}", target.source, target.model_id)
                }
                Some(InferenceTargetState::Unavailable {
                    source,
                    model_id,
                    code,
                    message,
                }) => format!("feature.{name}=unavailable:{source}:{model_id}:{code}:{message}"),
                None => format!("feature.{name}=unset"),
            });
        }
        let fingerprint = digest(&format!(
            "{:?}|{:?}|{:?}|{:?}|{}|{:?}|{:?}|{}",
            selected,
            context_window_override,
            pricing,
            provider.info(),
            target_ids.join(","),
            publication_source,
            publication_rates_len,
            publication_key.unwrap_or_default()
        ));
        let compactor = base
            .deps
            .compactor
            .rebind_model(provider.clone(), &config)
            .map_err(&fail_preparation)?;
        let deps = SharedDeps::from_engine(production_deps(
            provider.clone(),
            EngineDepsOverrides {
                compactor: Some(compactor),
                uuid: Some(self.deps.uuid.clone()),
                now: Some(self.deps.now.clone()),
                ..Default::default()
            },
        ));
        let pricing_runtime = PricingRuntimeSnapshot {
            model_ids: pricing
                .as_ref()
                .unwrap_or(&DEFAULT_PRICING)
                .iter()
                .map(|(model, _)| model.clone().into())
                .collect(),
            source: if pricing.is_some() {
                ExtensionSource::session()
            } else {
                ExtensionSource::builtin()
            },
        };
        let rates = publication_rates
            .or(pricing)
            .unwrap_or_else(|| base.rates.clone());
        let next_targets = publication_targets.or_else(|| base.targets.clone());
        let next_source = publication_source.or_else(|| base.source.clone());
        // Bind the compaction summary BEFORE taking the publication lock: the
        // summary request rides the session's main model (an unavailable
        // configured tier degrades to it inside `select`), never a separate
        // auxiliary source. `summary_deps` is a no-op without targets, so a
        // main-only publication keeps the deps built above. The provisional
        // bundle carries only fields this method has already resolved;
        // `summary_deps` reads targets/source/rates/deps, not the revision, so
        // the placeholder ids do not matter.
        let summary_deps = InferenceBundle {
            revision: base.revision,
            main_revision: base.main_revision,
            change_id: None,
            provider: provider.clone(),
            review_cloud: review_cloud.clone(),
            review_requirements: requirements.clone(),
            config: config.clone(),
            context_window_override,
            deps: deps.clone(),
            pricing: pricing_runtime.clone(),
            targets: next_targets.clone(),
            advisor_target: advisor_target.clone(),
            orchestrator_target: orchestrator_target.clone(),
            orchestration_mode,
            orchestrator_fallback_to_main,
            feature_targets: feature_targets.clone(),
            source: next_source.clone(),
            rates: rates.clone(),
            context_downshift: None,
        }
        .summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )
        .map_err(&fail_preparation)?;
        let mut admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".to_string());
        }
        if let Some(id) = &change_id {
            if let Some((revision, prior)) = admission.commits.get(id) {
                return if prior == &fingerprint {
                    Ok(*revision)
                } else {
                    Err("model changeId reused with different configuration".to_string())
                };
            }
            let pending = admission
                .pending
                .as_ref()
                .filter(|pending| &pending.change_id == id)
                .ok_or_else(|| "model update was superseded or has not begun".to_string())?;
            if let Some(error) = &pending.error {
                return Err(error.clone());
            }
            if pending.started.elapsed() >= MODEL_UPDATE_TIMEOUT {
                return Err("model provider preparation timed out".to_string());
            }
        } else if admission.pending.is_some() {
            return Err("model update is pending; publication requires its changeId".to_string());
        }
        let mut current = self.inference.write_recover();
        if current.revision != expected_revision || !Arc::ptr_eq(&current, &base) {
            return Err("model reconfiguration revision conflict".to_string());
        }
        let mut policy = self.runtime_policy.write_recover();
        let previous_policy = policy.state();
        let mut next_policy = previous_policy.clone();
        if let Some(reviewer) = selected.approvals_reviewer {
            next_policy.approvals_reviewer = Some(reviewer);
        } else if selected.model != base.config.model
            && requirements.requires_review_for_model(
                &String::from_utf8(selected.model.to_node_utf8()).expect("Node text is UTF-8"),
            )
        {
            next_policy.approvals_reviewer =
                Some(crate::types::permission::ApprovalsReviewer::Automatic);
        }
        super::session_core_policy_part::validate_review_requirements(
            &requirements,
            &next_policy,
            &selected.model,
        )?;
        config.approvals_reviewer = next_policy.approvals_reviewer;
        let revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| "model revision overflow".to_string())?;
        let mut next = InferenceBundle {
            revision,
            main_revision: revision,
            change_id: change_id.clone(),
            provider,
            review_cloud,
            review_requirements: requirements,
            config,
            context_window_override,
            deps: summary_deps,
            pricing: pricing_runtime,
            targets: next_targets,
            advisor_target,
            orchestrator_target,
            orchestration_mode,
            orchestrator_fallback_to_main,
            feature_targets,
            source: next_source,
            rates,
            context_downshift: None,
        };
        next.context_downshift = previous.downshift_to(&next.config_for_model(&next.config.model));
        *current = Arc::new(next);
        if !orchestration_mode {
            self.clear_orchestration_fallback();
        }
        if next_policy != previous_policy {
            next_policy.generation = previous_policy.generation.saturating_add(1);
            *policy = Arc::new(super::RuntimePolicySnapshot {
                state: next_policy,
                sandbox_backend: policy.sandbox_backend.clone(),
            });
        }
        drop(policy);
        self.mode.set_pending_model(None);
        if let Some(id) = change_id {
            admission.commits.insert(id, (revision, fingerprint));
        }
        admission.pending = None;
        drop(current);
        drop(admission);
        self.inference_changed.notify_waiters();
        self.cancel_transport_prewarm();
        self.transport_prewarm_consumed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.schedule_transport_prewarm();
        Ok(revision)
    }

    pub(crate) fn restore_resumed_context_downshift(
        &self,
        raw_items: &[crate::schemas::rollout::RolloutItem],
    ) {
        use crate::schemas::rollout::RolloutItem;
        let mut current = self.inference.write_recover();
        let config = current.config_for_model(&current.config.model);
        let usable = crate::context::compact::policy::usable_context_window(&config);
        let history = self.history_snapshot();
        let estimated = crate::context::tokens::estimate_context_tokens(&history);
        let current_source = config
            .model_source
            .as_ref()
            .and_then(|source| source.source.clone())
            .map(crate::runtime_values::RuntimeString::from);
        let model_changed = raw_items
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::UsageLedger(entry) => Some(
                    entry.model != config.model
                        || entry.inference.as_ref().is_some_and(|identity| {
                            identity.source.is_some() && identity.source != current_source
                        }),
                ),
                RolloutItem::TurnContext { ctx } => Some(ctx.model != config.model),
                _ => None,
            })
            .unwrap_or(false);
        let prior_high_water = estimated.max(
            history
                .iter()
                .rev()
                .find_map(|message| match message {
                    crate::schemas::message::Message::Assistant(assistant) => assistant
                        .usage
                        .as_ref()
                        .map(|usage| {
                            usage.total_tokens.unwrap_or(
                                usage
                                    .input_tokens
                                    .saturating_add(usage.cache_read_input_tokens)
                                    .saturating_add(usage.cache_creation_input_tokens)
                                    .saturating_add(usage.output_tokens),
                            )
                        })
                        .filter(|tokens| *tokens > 0),
                    _ => None,
                })
                .unwrap_or(0),
        );
        if estimated >= usable || (model_changed && prior_high_water > usable) {
            let mut next = (**current).clone();
            next.context_downshift = Some(Arc::new(ContextWindowDownshift::new(
                prior_high_water
                    .saturating_add(1)
                    .max(usable.saturating_add(1)),
            )));
            *current = Arc::new(next);
        }
    }
}

fn tier_name(tier: ModelTier) -> &'static str {
    match tier {
        ModelTier::High => "high",
        ModelTier::Medium => "medium",
        ModelTier::Low => "low",
    }
}

fn tier_name_for_selection(selection: &str) -> Option<&'static str> {
    ModelTier::from_selection(selection).map(tier_name)
}

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn merge_inference_config(base: &ResolvedConfig, selected: &ResolvedConfig) -> ResolvedConfig {
    let mut config = base.clone();
    config.model = selected.model.clone();
    config.model_display_name = selected.model_display_name.clone();
    config.model_source = selected.model_source.clone();
    config.model_tiers = selected.model_tiers.clone();
    config.effort = selected.effort.clone();
    config.ultra_mode = selected.ultra_mode;
    config.model_verbosity = selected.model_verbosity;
    config.reasoning_summary = selected.reasoning_summary;
    config.web_search.set_access(selected.web_search.access);
    if selected.approval_review.is_some() {
        config.approval_review = selected.approval_review.clone();
    }
    config.thinking = selected.thinking;
    config.fast_mode = selected.fast_mode;
    config.context_window = selected.context_window;
    config.model_max_output_tokens = selected.model_max_output_tokens;
    config.model_context_window = selected.model_context_window;
    config.model_effective_context_window_percent = selected.model_effective_context_window_percent;
    config.model_auto_compact_token_limit = selected.model_auto_compact_token_limit;
    config.auto_compact_window_source = selected.auto_compact_window_source;
    config.max_output_tokens = selected.max_output_tokens;
    config.max_output_retry_tokens = selected.max_output_retry_tokens;
    config
}

#[cfg(test)]
mod media_capture_tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, RuntimeString};
    use crate::types::provider::{
        ImageLimits, ModelRequest, ModelStream, ModelStreamEvent, ProviderCapabilities,
        ProviderInfo,
    };
    use futures::StreamExt;

    struct MetadataProvider {
        info: ProviderInfo,
        capabilities: Mutex<HashMap<RuntimeString, ProviderCapabilities>>,
        fallback: ProviderCapabilities,
    }

    impl ModelProvider for MetadataProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }
        fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
            self.capabilities
                .lock()
                .unwrap()
                .get(model)
                .cloned()
                .unwrap_or_else(|| self.fallback.clone())
        }
        fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
            panic!("captured dependency must retain the injected transport")
        }
    }

    #[tokio::test]
    async fn injected_capture_binds_actual_request_model_and_keeps_frozen_media_metadata() {
        let fallback: ProviderCapabilities = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"streaming":true,"tools":true,"thinking":false,"structuredOutput":false,"webSearch":false,"promptCacheControl":false,"contextWindow":200000,"maxOutputTokens":8192,"nativePdf":true,"vision":true}"#,
        )).unwrap();
        let mut main = fallback.clone();
        main.image_limits = Some(
            runtime_codec::decode_text(&RuntimeString::from(
                r#"{"maxWidth":2000,"maxBase64Size":5242880}"#,
            ))
            .unwrap(),
        );
        main.max_media_blocks = std::num::NonZeroU32::new(600);
        let mut summary = fallback.clone();
        summary.image_limits = Some(
            runtime_codec::decode_text(&RuntimeString::from(
                r#"{"maxHeight":1200,"targetRawSize":512000}"#,
            ))
            .unwrap(),
        );
        summary.max_media_blocks = std::num::NonZeroU32::new(42);
        let provider = Arc::new(MetadataProvider {
            info: ProviderInfo {
                name: "metadata-fixture".into(),
                default_model: "main".into(),
                base_url: None,
                env_key: None,
            },
            capabilities: Mutex::new(HashMap::from([
                ("main".into(), main),
                ("summary".into(), summary),
            ])),
            fallback,
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let deps = SharedDeps::from_engine(production_deps(
            provider.clone(),
            EngineDepsOverrides {
                call_model: Some(Box::new(move |request, _| {
                    recorded.lock().unwrap().push(request);
                    Box::pin(futures::stream::empty())
                })),
                estimate_tokens: Some(Arc::new(|_| 77)),
                ..Default::default()
            },
        ));
        let mut config = crate::schemas::config::default_config();
        config.model = "main".into();
        let bundle = Arc::new(InferenceBundle {
            revision: 0,
            main_revision: 0,
            change_id: None,
            provider: provider.clone(),
            review_cloud: None,
            review_requirements: Arc::new(Default::default()),
            config,
            context_window_override: None,
            deps,
            pricing: PricingRuntimeSnapshot {
                model_ids: Vec::new(),
                source: ExtensionSource::builtin(),
            },
            targets: None,
            advisor_target: None,
            orchestrator_target: None,
            orchestration_mode: false,
            orchestrator_fallback_to_main: false,
            feature_targets: Default::default(),
            source: None,
            rates: DEFAULT_PRICING.clone(),
            context_downshift: None,
        });
        let models = ["summary".into(), "undeclared".into()];
        let captured = bundle.capture(&models);
        let refreshed = provider
            .capabilities
            .lock()
            .unwrap()
            .get_mut(&RuntimeString::from("main"))
            .map(|caps| {
                caps.image_limits.as_mut().unwrap().max_width = std::num::NonZeroU32::new(1_000);
                caps.max_media_blocks = std::num::NonZeroU32::new(100);
            });
        assert!(refreshed.is_some());
        let seed: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
            r#"{"model":"main","system":[],"messages":[],"tools":[],"maxOutputTokens":100,"imageLimits":{"maxWidth":3333},"maxMediaBlocks":600}"#,
        )).unwrap();
        for model in ["main", "summary", "undeclared"] {
            let mut request = seed.clone();
            request.model = model.into();
            let _ = (captured.deps.call_model)(request, AbortSignal::new())
                .collect::<Vec<_>>()
                .await;
        }
        let mut denied = seed.clone();
        denied.model = "forbidden-other".into();
        let events = (captured.deps.call_model)(denied, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.as_slice(),
            [ModelStreamEvent::Error { .. }]
        ));
        let next = bundle.capture(&models);
        let _ = (next.deps.call_model)(seed.clone(), AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert_eq!((captured.deps.estimate_tokens)(&seed), 77);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(
            requests[0].image_limits.unwrap().max_width.unwrap().get(),
            2_000
        );
        assert_eq!(requests[0].max_media_blocks.unwrap().get(), 600);
        assert_eq!(requests[1].image_limits.unwrap().max_width, None);
        assert_eq!(
            requests[1].image_limits.unwrap().max_height.unwrap().get(),
            1_200
        );
        assert_eq!(
            requests[1]
                .image_limits
                .unwrap()
                .target_raw_size
                .unwrap()
                .get(),
            512_000
        );
        assert_eq!(requests[1].max_media_blocks.unwrap().get(), 42);
        assert_eq!(requests[2].image_limits, Some(ImageLimits::default()));
        assert_eq!(requests[2].max_media_blocks.unwrap().get(), 100);
        assert_eq!(
            requests[3].image_limits.unwrap().max_width.unwrap().get(),
            1_000
        );
        assert_eq!(requests[3].max_media_blocks.unwrap().get(), 100);
    }
}
