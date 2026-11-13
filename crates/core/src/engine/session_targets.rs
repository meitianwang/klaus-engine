//! Immutable source-qualified inference targets. A missing slot follows the
//! main target, and a slot whose configured source or model is currently
//! unavailable degrades to the main target as well: an auxiliary path keeps
//! working on the conversation's own model instead of failing, and it never
//! silently borrows a different configured source.

use super::{InferenceBundle, InputModelRevision, Session, SharedCallModel, SharedDeps};
use crate::engine::deps::{production_deps, EngineDepsOverrides};
use crate::observability::cost_tracker::{PricingTable, DEFAULT_PRICING};
use crate::runtime_values::RuntimeString;
use crate::schemas::config::{ModelTier, ResolvedConfig};
use crate::store::receipt_broker::{reserve_receipt, ReceiptBroker, ReceiptSlot};
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::types::provider::ModelProvider;
use std::sync::Arc;

#[derive(Clone)]
pub struct InferenceTarget {
    pub source: String,
    pub model_id: String,
    pub model: RuntimeString,
    pub provider: Arc<dyn ModelProvider>,
    pub pricing: PricingTable,
    pub review_requirements: Option<super::PreparedReviewRequirements>,
}

#[derive(Clone)]
pub enum InferenceTargetState {
    Ready(InferenceTarget),
    Unavailable {
        source: String,
        model_id: String,
        code: String,
        message: String,
    },
}

#[derive(Clone, Default)]
pub struct InferenceTargets {
    pub high: Option<InferenceTargetState>,
    pub medium: Option<InferenceTargetState>,
    pub low: Option<InferenceTargetState>,
}

impl InferenceTargets {
    fn get(&self, tier: ModelTier) -> Option<&InferenceTargetState> {
        match tier {
            ModelTier::High => self.high.as_ref(),
            ModelTier::Medium => self.medium.as_ref(),
            ModelTier::Low => self.low.as_ref(),
        }
    }
}

/// A named auxiliary capability that can ride its own model target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FeatureSlot {
    Title,
    Prediction,
    Memory,
}

impl FeatureSlot {
    pub fn name(self) -> &'static str {
        match self {
            FeatureSlot::Title => "title",
            FeatureSlot::Prediction => "prediction",
            FeatureSlot::Memory => "memory",
        }
    }
}

/// Independent per-feature model targets.
///
/// A missing slot follows the session's main model, and a slot whose
/// configured source or model is currently unavailable degrades to the main
/// model as well: these are maintenance paths that keep working on the
/// conversation's own model instead of failing, and they never silently borrow
/// a different configured source.
#[derive(Clone, Default)]
pub struct FeatureTargets {
    pub title: Option<InferenceTargetState>,
    pub prediction: Option<InferenceTargetState>,
    pub memory: Option<InferenceTargetState>,
}

impl FeatureTargets {
    pub fn get(&self, slot: FeatureSlot) -> Option<&InferenceTargetState> {
        match slot {
            FeatureSlot::Title => self.title.as_ref(),
            FeatureSlot::Prediction => self.prediction.as_ref(),
            FeatureSlot::Memory => self.memory.as_ref(),
        }
    }

    pub fn slots(&self) -> [(&'static str, Option<&InferenceTargetState>); 3] {
        [
            ("title", self.title.as_ref()),
            ("prediction", self.prediction.as_ref()),
            ("memory", self.memory.as_ref()),
        ]
    }

    pub fn slots_mut(&mut self) -> [&mut Option<InferenceTargetState>; 3] {
        [&mut self.title, &mut self.prediction, &mut self.memory]
    }

    /// Freeze every ready slot's provider for an in-flight capture, mirroring
    /// the advisor and tier freezes.
    pub(super) fn frozen(&self) -> FeatureTargets {
        FeatureTargets {
            title: frozen_target_state(&self.title),
            prediction: frozen_target_state(&self.prediction),
            memory: frozen_target_state(&self.memory),
        }
    }
}

pub(super) fn frozen_target_state(
    state: &Option<InferenceTargetState>,
) -> Option<InferenceTargetState> {
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
}

#[derive(Clone)]
pub(crate) struct TargetSnapshot {
    pub revision: InputModelRevision,
    pub target: Option<InferenceTarget>,
    pub provider: Arc<dyn ModelProvider>,
    pub model: RuntimeString,
    pub config: ResolvedConfig,
    pub deps: SharedDeps,
}

impl InferenceBundle {
    pub(super) fn select(
        &self,
        selection: Option<&RuntimeString>,
    ) -> Result<TargetSnapshot, String> {
        let tier = match selection {
            Some(value) if value == "high" => Some(ModelTier::High),
            Some(value) if value == "medium" => Some(ModelTier::Medium),
            Some(value) if value == "low" => Some(ModelTier::Low),
            _ => None,
        };
        let target = match tier
            .and_then(|tier| self.targets.as_ref().and_then(|targets| targets.get(tier)))
        {
            Some(InferenceTargetState::Ready(target)) => Some(target.clone()),
            // A configured tier whose source or model is currently unavailable
            // (signed out, model retired, capability missing) degrades to the
            // main target: the auxiliary path still runs, on the conversation's
            // own model, rather than failing the whole request.
            Some(InferenceTargetState::Unavailable { .. }) => None,
            None => None,
        };
        let snapshot = self.snapshot_for_target(selection, target, tier.is_some());
        if !snapshot.provider.has_model_capabilities(&snapshot.model) {
            let error = crate::provider::model_catalog::uncaptured_model_error(&snapshot.model);
            return Err(error.message.try_to_utf8().unwrap_or_else(|_| {
                "The selected model is not bound to the captured inference provider.".to_string()
            }));
        }
        Ok(snapshot)
    }

    /// Resolve a named auxiliary feature against THIS captured bundle.
    ///
    /// A ready slot rides its own frozen provider, pricing and source
    /// identity; a missing or unavailable slot follows the session's main
    /// model, so a maintenance path never fails on auxiliary configuration.
    pub fn for_feature(self: &Arc<Self>, slot: FeatureSlot) -> Result<Arc<Self>, String> {
        let Some(InferenceTargetState::Ready(target)) = self.feature_targets.get(slot) else {
            return Ok(self.clone());
        };
        let snapshot = self.snapshot_for_target(None, Some(target.clone()), false);
        if !snapshot.provider.has_model_capabilities(&snapshot.model) {
            let error = crate::provider::model_catalog::uncaptured_model_error(&snapshot.model);
            return Err(error.message.try_to_utf8().unwrap_or_else(|_| {
                "The feature model is not bound to the captured inference provider.".to_string()
            }));
        }
        Ok(self.bundle_for_target(snapshot, None))
    }

    pub(super) fn snapshot_for_target(
        &self,
        selection: Option<&RuntimeString>,
        target: Option<InferenceTarget>,
        is_tier: bool,
    ) -> TargetSnapshot {
        let provider = target
            .as_ref()
            .map(|target| target.provider.clone())
            .unwrap_or_else(|| self.provider.clone());
        let model = target
            .as_ref()
            .map(|target| target.model.clone())
            .unwrap_or_else(|| {
                if self.targets.is_some() && is_tier {
                    self.config.model.clone()
                } else {
                    selection
                        .filter(|value| *value != "inherit")
                        .map(|value| self.config.resolve_model_selection(value))
                        .unwrap_or_else(|| self.config.model.clone())
                }
            });
        let capabilities = provider.capabilities(&model);
        let mut config = self.config.clone();
        config.model = model.clone();
        config.model_max_output_tokens = Some(capabilities.max_output_tokens);
        if target.is_some() {
            config.fallback_models.clear();
        }
        config.context_window = if target.is_some() {
            capabilities.context_window
        } else {
            self.context_window_override
                .unwrap_or(capabilities.context_window)
        };
        config.model_context_window = Some(config.context_window.min(capabilities.context_window));
        config.model_effective_context_window_percent =
            Some(provider.effective_context_window_percent(&model));
        config.model_auto_compact_token_limit = provider.model_auto_compact_token_limit(&model);
        config.max_output_tokens = Some(if target.is_some() {
            capabilities.max_output_tokens
        } else {
            config
                .max_output_tokens
                .unwrap_or(capabilities.max_output_tokens)
        });
        let deps = if target.is_some() {
            SharedDeps::from_engine(production_deps(
                provider.clone(),
                EngineDepsOverrides {
                    compactor: Some(self.deps.compactor.clone()),
                    uuid: Some(self.deps.uuid.clone()),
                    now: Some(self.deps.now.clone()),
                    ..Default::default()
                },
            ))
        } else {
            self.deps.clone()
        };
        TargetSnapshot {
            revision: if is_tier {
                InputModelRevision::Tier(self.revision)
            } else {
                InputModelRevision::Main(self.main_revision)
            },
            target,
            provider,
            model,
            config,
            deps,
        }
    }
}

pub(super) fn prepare_target_requirements_sync(target: &mut InferenceTarget) -> Result<(), String> {
    if let Some(prepared) = &target.review_requirements {
        if !Arc::ptr_eq(&prepared.provider, &target.provider) {
            return Err(
                "Prepared target approval requirements belong to a different provider.".into(),
            );
        }
        return Ok(());
    }
    if target.provider.managed_review_cloud_auth_source().is_some() {
        return Err(
            "The selected target requires asynchronous approval requirements preparation.".into(),
        );
    }
    target.review_requirements = Some(super::PreparedReviewRequirements {
        provider: target.provider.clone(),
        cloud: None,
        requirements: Arc::new(
            crate::engine::approval_requirements::load_managed_review_requirements_sync()
                .map_err(|error| error.to_string())?,
        ),
    });
    Ok(())
}

pub(super) fn prepare_targets_requirements_sync(
    targets: &mut Option<InferenceTargets>,
    advisor: &mut Option<InferenceTargetState>,
) -> Result<(), String> {
    let mut states = targets
        .as_mut()
        .map(|targets| vec![&mut targets.high, &mut targets.medium, &mut targets.low])
        .unwrap_or_default();
    states.push(advisor);
    for state in states {
        if let Some(InferenceTargetState::Ready(target)) = state {
            prepare_target_requirements_sync(target)?;
        }
    }
    Ok(())
}

pub(super) fn prepare_feature_requirements_sync(
    features: &mut FeatureTargets,
) -> Result<(), String> {
    for state in features.slots_mut() {
        if let Some(InferenceTargetState::Ready(target)) = state {
            prepare_target_requirements_sync(target)?;
        }
    }
    Ok(())
}

/// Degrade a ready slot whose asynchronous preparation failed.
///
/// Auxiliary paths keep the conversation usable: tiers and the advisor report
/// at use time, feature slots follow the main model, and session publication
/// never fails on them. Only the conversation's own model gates startup.
fn degrade_preparation_failure(
    slot: &mut Option<InferenceTargetState>,
    slot_name: &str,
    error: String,
) {
    let Some(InferenceTargetState::Ready(target)) = slot else {
        return;
    };
    let source = target.source.clone();
    let model_id = target.model_id.clone();
    eprintln!(
        "[session-targets] {slot_name} target preparation failed; degrading to unavailable: source={source} model={model_id} error={error}"
    );
    *slot = Some(InferenceTargetState::Unavailable {
        source,
        model_id,
        code: "target-preparation-failed".into(),
        message: error,
    });
}

impl Session {
    /// Prepare managed-review requirements for every ready slot concurrently.
    ///
    /// A slot whose preparation fails degrades to
    /// [`InferenceTargetState::Unavailable`] instead of failing the caller, so
    /// a revoked subscription login on an auxiliary source cannot block the
    /// conversation's own model.
    async fn prepare_slot_requirements(
        &self,
        slots: &mut [(&'static str, &mut Option<InferenceTargetState>)],
    ) {
        let results = futures::future::join_all(slots.iter().enumerate().filter_map(
            |(index, (_name, slot))| match slot {
                Some(InferenceTargetState::Ready(target)) => {
                    let provider = target.provider.clone();
                    Some(async move { (index, self.prepare_review_requirements(provider).await) })
                }
                _ => None,
            },
        ))
        .await;
        for (index, result) in results {
            let Some((name, slot)) = slots.get_mut(index) else {
                continue;
            };
            match result {
                Ok(requirements) => {
                    if let Some(InferenceTargetState::Ready(target)) = slot {
                        target.review_requirements = Some(requirements);
                    }
                }
                Err(error) => degrade_preparation_failure(slot, name, error),
            }
        }
    }

    /// Prepares each selected account before an auxiliary target can be published.
    ///
    /// Preparation failures degrade the affected slot instead of failing; see
    /// [`degrade_preparation_failure`].
    pub async fn prepare_inference_targets(
        &self,
        targets: &mut Option<InferenceTargets>,
        advisor: &mut Option<InferenceTargetState>,
    ) {
        let mut slots: Vec<(&'static str, &mut Option<InferenceTargetState>)> =
            match targets.as_mut() {
                Some(targets) => vec![
                    ("high", &mut targets.high),
                    ("medium", &mut targets.medium),
                    ("low", &mut targets.low),
                ],
                None => Vec::new(),
            };
        slots.push(("advisor", advisor));
        self.prepare_slot_requirements(&mut slots).await;
    }
    /// Prepares each configured feature account before its target can be published.
    ///
    /// Preparation failures degrade the affected slot instead of failing; see
    /// [`degrade_preparation_failure`].
    pub async fn prepare_feature_targets(&self, features: &mut FeatureTargets) {
        let mut slots: Vec<(&'static str, &mut Option<InferenceTargetState>)> = vec![
            ("title", &mut features.title),
            ("prediction", &mut features.prediction),
            ("memory", &mut features.memory),
        ];
        self.prepare_slot_requirements(&mut slots).await;
    }
    /// Tier-only changes replace the immutable bundle while active requests and
    /// running children retain their already captured provider and limits.
    pub fn reconfigure_tier_targets(
        &self,
        targets: InferenceTargets,
        expected_revision: u64,
    ) -> Result<u64, String> {
        let mut targets = Some(targets);
        prepare_targets_requirements_sync(&mut targets, &mut None)?;
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot reconfigure a closing session".into());
        }
        let mut current = self.inference.write_recover();
        if current.revision != expected_revision {
            return Err("model reconfiguration revision conflict".into());
        }
        let mut next = (**current).clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or("model revision overflow")?;
        next.targets = targets;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        let revision = next.revision;
        *current = Arc::new(next);
        Ok(revision)
    }

    /// Called before publication; initial target preparation does not increment
    /// the existing model revision.
    pub fn install_inference_targets(
        &self,
        mut targets: Option<InferenceTargets>,
        source: Option<(String, String)>,
        pricing: Option<PricingTable>,
    ) -> Result<(), String> {
        prepare_targets_requirements_sync(&mut targets, &mut None)?;
        let _admission = self.inference_admission.lock_recover();
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        next.targets = targets;
        next.source = source;
        next.rates = pricing.unwrap_or_else(|| DEFAULT_PRICING.clone());
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        *current = Arc::new(next);
        Ok(())
    }

    /// Install before publishing a newly created session. Live changes use
    /// atomic inference publication.
    pub fn install_feature_targets(&self, mut features: FeatureTargets) -> Result<(), String> {
        prepare_feature_requirements_sync(&mut features)?;
        let admission = self.inference_admission.lock_recover();
        if admission.closed || self.is_disposed() {
            return Err("cannot configure feature model targets on a closing session".into());
        }
        let mut current = self.inference.write_recover();
        let mut next = (**current).clone();
        next.feature_targets = features;
        *current = Arc::new(next);
        Ok(())
    }

    /// The explicit child selection for a feature slot: the ready target's
    /// source-qualified identity (which the frozen-slot scan reuses), or
    /// "inherit" so an unset or unavailable slot keeps the child on the
    /// session's main model, exactly as the memory model setting promises.
    pub(super) fn feature_model_selection(&self, slot: FeatureSlot) -> RuntimeString {
        match self.inference_snapshot().feature_targets.get(slot) {
            Some(InferenceTargetState::Ready(target)) => {
                RuntimeString::from(format!("{}:{}", target.source, target.model_id))
            }
            _ => RuntimeString::from("inherit"),
        }
    }

    pub(super) fn target_for_selection(
        &self,
        selection: Option<&RuntimeString>,
    ) -> Result<TargetSnapshot, String> {
        self.auxiliary_inference_snapshot()?.select(selection)
    }

    pub(super) fn auxiliary_inference_snapshot(&self) -> Result<Arc<InferenceBundle>, String> {
        self.auxiliary_bundle_for(self.inference_snapshot())
    }

    /// Apply the auxiliary adjustment to a bundle the caller already captured.
    ///
    /// A pending main-model switch changes which model the compaction SUMMARY
    /// rides, and the summary transport is bound inside the compactor rather
    /// than passed per call, so the rebinding has to happen here. Doing it on
    /// the caller's captured bundle keeps a compaction that started before a
    /// publication on the configuration it captured.
    pub(super) fn auxiliary_bundle_for(
        &self,
        current: Arc<InferenceBundle>,
    ) -> Result<Arc<InferenceBundle>, String> {
        let Some(model) = self.pending_model().filter(|_| current.targets.is_some()) else {
            return Ok(current);
        };
        let model = current.config.resolve_model_selection(&model);
        if model == current.config.model {
            return Ok(current);
        }
        let mut next = (*current).clone();
        next.config.model = model;
        next.deps = next.summary_deps(
            self.cost.clone(),
            self.persistence.usage_receipts().cloned(),
        )?;
        Ok(Arc::new(next))
    }

    pub(super) fn target_for_turn(
        &self,
        opts: &super::TurnOptions,
    ) -> Result<TargetSnapshot, String> {
        if opts.model.is_none()
            && self.pending_model().is_some()
            && self.inference_snapshot().targets.is_some()
        {
            return self.auxiliary_inference_snapshot()?.select(None);
        }
        let selection = opts
            .model
            .clone()
            .or_else(|| self.pending_model())
            .or_else(|| {
                self.root_agent_definition
                    .as_ref()
                    .and_then(|definition| definition.model.clone())
            });
        let selection = selection.or_else(|| {
            self.inference_snapshot()
                .targets
                .is_none()
                .then(|| self.effective_turn_model(opts))
        });
        self.target_for_selection(selection.as_ref())
    }

    pub fn model_tier_targets_runtime(&self) -> Option<serde_json::Value> {
        let bundle = self.inference_snapshot();
        let targets = bundle.targets.as_ref()?;
        let mut values = serde_json::Map::new();
        for (name, tier) in [
            ("high", ModelTier::High),
            ("medium", ModelTier::Medium),
            ("low", ModelTier::Low),
        ] {
            let value = match targets.get(tier) {
                None => serde_json::Value::Null,
                Some(InferenceTargetState::Unavailable {
                    source,
                    model_id,
                    code,
                    message,
                }) => {
                    serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":code,"message":message})
                }
                Some(InferenceTargetState::Ready(target)) => {
                    serde_json::json!({"type":"ready","source":target.source,"modelId":target.model_id,"model":target.model.try_to_utf8().ok(),"capabilities":target.provider.capabilities(&target.model)})
                }
            };
            values.insert(name.into(), value);
        }
        Some(serde_json::Value::Object(values))
    }

    /// Non-secret per-feature target status; a null slot follows the main model.
    pub fn feature_targets_runtime(&self) -> serde_json::Value {
        let features = &self.inference_snapshot().feature_targets;
        let mut values = serde_json::Map::new();
        for (name, state) in features.slots() {
            let value = match state {
                None => serde_json::Value::Null,
                Some(InferenceTargetState::Unavailable {
                    source,
                    model_id,
                    code,
                    message,
                }) => {
                    serde_json::json!({"type":"unavailable","source":source,"modelId":model_id,"code":code,"message":message})
                }
                Some(InferenceTargetState::Ready(target)) => {
                    serde_json::json!({"type":"ready","source":target.source,"modelId":target.model_id,"model":target.model.try_to_utf8().ok(),"capabilities":target.provider.capabilities(&target.model)})
                }
            };
            values.insert(name.to_string(), value);
        }
        serde_json::Value::Object(values)
    }
}

impl InferenceBundle {
    pub(super) fn summary_deps(
        &self,
        cost: Arc<std::sync::Mutex<crate::observability::cost_tracker::CostTracker>>,
        broker: Option<Arc<ReceiptBroker>>,
    ) -> Result<SharedDeps, String> {
        // The compaction summary rides the session's main model, the same way a
        // main-loop turn does: tier targets configure auxiliary paths only, and
        // an unavailable configured tier degrades to the main model inside
        // `select` rather than failing the summary.
        let summary = self.select(None)?;
        let identity = summary
            .target
            .as_ref()
            .map(|target| (target.source.clone(), target.model_id.clone()))
            .or_else(|| self.source.clone())
            .unwrap_or_else(|| {
                (
                    summary
                        .provider
                        .info()
                        .name
                        .try_to_utf8()
                        .unwrap_or_default(),
                    summary.model.try_to_utf8().unwrap_or_default(),
                )
            });
        let pricing = summary
            .target
            .as_ref()
            .map(|target| target.pricing.clone())
            .unwrap_or_else(|| self.rates.clone());
        let uuid = self.deps.uuid.clone();
        let sink = Arc::new(
            move |model: &RuntimeString,
                  usage: &crate::schemas::message::Usage,
                  slot: Option<ReceiptSlot>| {
                let id = uuid();
                let record = cost.lock_recover().record_target(
                    id.clone(),
                    id,
                    (&identity.0, &identity.1),
                    &model.try_to_utf8().unwrap_or_default(),
                    usage,
                    &pricing,
                );
                if let Some(slot) = slot {
                    slot.commit(record);
                }
            },
        );
        let (provider, config) = (
            Arc::new(SummaryProvider {
                inner: summary.provider,
                call_model: self.deps.call_model.clone(),
                sink,
                broker,
            }) as Arc<dyn ModelProvider>,
            summary.config,
        );
        let compactor = self.deps.compactor.rebind_summary(provider, &config)?;
        let mut deps = self.deps.clone();
        if let Some(compactor) = compactor {
            deps.compactor = compactor;
        }
        Ok(deps)
    }
}

type SummaryUsageSink =
    Arc<dyn Fn(&RuntimeString, &crate::schemas::message::Usage, Option<ReceiptSlot>) + Send + Sync>;

struct SummaryUsageReceipt {
    model: RuntimeString,
    actual: Option<crate::schemas::message::Usage>,
    sink: SummaryUsageSink,
    slot: Option<ReceiptSlot>,
}

impl Drop for SummaryUsageReceipt {
    fn drop(&mut self) {
        if let Some(usage) = self.actual.take() {
            (self.sink)(&self.model, &usage, self.slot.take());
        }
    }
}

pub(super) fn usage_admission_error(error: std::io::Error) -> crate::types::errors::ProviderError {
    crate::types::errors::ProviderError {
        kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
        message: format!("Cannot reserve usage receipt persistence: {error}").into(),
        status: None,
        code: Some("usage_receipt_admission_failed".into()),
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    }
}

struct SummaryProvider {
    inner: Arc<dyn ModelProvider>,
    call_model: SharedCallModel,
    sink: SummaryUsageSink,
    broker: Option<Arc<ReceiptBroker>>,
}

struct NativeSummaryReceipt {
    model: RuntimeString,
    actual: Arc<std::sync::Mutex<Option<crate::schemas::message::Usage>>>,
    sink: SummaryUsageSink,
    slot: Option<ReceiptSlot>,
}

impl Drop for NativeSummaryReceipt {
    fn drop(&mut self) {
        if let Some(usage) = self.actual.lock_recover().take() {
            (self.sink)(&self.model, &usage, self.slot.take());
        }
    }
}

#[async_trait::async_trait]
impl ModelProvider for SummaryProvider {
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        model: &RuntimeString,
        signal: &crate::types::abort::AbortSignal,
    ) -> Result<bool, crate::types::errors::ProviderError> {
        if checkpoint
            .comp_hash
            .as_ref()
            .zip(self.compaction_compatibility_hash(model))
            .is_some_and(|(saved, current)| saved != &current)
        {
            return Ok(false);
        }
        self.inner
            .native_checkpoint_compatibility(checkpoint, model, signal)
            .await
    }

    fn effective_context_window_percent(&self, model: &RuntimeString) -> u8 {
        self.inner.effective_context_window_percent(model)
    }
    fn model_auto_compact_token_limit(&self, model: &RuntimeString) -> Option<u64> {
        self.inner.model_auto_compact_token_limit(model)
    }

    fn native_compaction_context(
        &self,
        model: &RuntimeString,
    ) -> Option<crate::types::provider::NativeCompactionContext> {
        if self.remote_compaction_support(model)
            == crate::types::provider::RemoteCompactionSupport::Unsupported
        {
            return None;
        }
        self.inner
            .native_compaction_context(model)
            .map(|mut context| {
                context.identity = format!(
                    "{:?}:{:?}",
                    context.identity,
                    self.compaction_compatibility_hash(model)
                )
                .into();
                context
            })
    }

    fn remote_compaction_support(
        &self,
        model: &RuntimeString,
    ) -> crate::types::provider::RemoteCompactionSupport {
        self.inner.remote_compaction_support(model)
    }
    fn compaction_compatibility_hash(&self, model: &RuntimeString) -> Option<RuntimeString> {
        self.inner.compaction_compatibility_hash(model)
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        self.inner.compaction_stream_max_retries()
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.inner.supports_compaction_model_fallback()
    }
    async fn compact_remote(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        let model = request.model.clone();
        let slot = reserve_receipt(self.broker.as_ref(), &signal)
            .await
            .map_err(usage_admission_error)?;
        let result = self.inner.compact_remote(request, signal).await?;
        if let Some(usage) = &result.usage {
            (self.sink)(&model, usage, slot);
        }
        Ok(result)
    }

    async fn compact_remote_with_idle_timeout(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
        idle_ms: u64,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        self.compact_remote_with_usage(request, signal, idle_ms, Arc::new(|_| {}), None)
            .await
    }

    async fn compact_remote_with_usage(
        &self,
        request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
        idle_ms: u64,
        observer: crate::types::provider::NativeCompactionUsageObserver,
        expected_scope: Option<RuntimeString>,
    ) -> Result<crate::types::provider::RemoteCompactionResult, crate::types::errors::ProviderError>
    {
        let slot = reserve_receipt(self.broker.as_ref(), &signal)
            .await
            .map_err(usage_admission_error)?;
        let actual = Arc::new(std::sync::Mutex::new(
            None::<crate::schemas::message::Usage>,
        ));
        let receipt = NativeSummaryReceipt {
            model: request.model.clone(),
            actual: actual.clone(),
            sink: self.sink.clone(),
            slot,
        };
        let result = self
            .inner
            .compact_remote_with_usage(
                request,
                signal,
                idle_ms,
                Arc::new(move |usage| {
                    observer(usage.clone());
                    let mut recorded = actual.lock_recover();
                    if let Some(total) = recorded.as_mut() {
                        total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
                        total.output_tokens =
                            total.output_tokens.saturating_add(usage.output_tokens);
                        total.cache_read_input_tokens = total
                            .cache_read_input_tokens
                            .saturating_add(usage.cache_read_input_tokens);
                        total.cache_creation_input_tokens = total
                            .cache_creation_input_tokens
                            .saturating_add(usage.cache_creation_input_tokens);
                        total.total_tokens = match (total.total_tokens, usage.total_tokens) {
                            (Some(a), Some(b)) => Some(a.saturating_add(b)),
                            _ => None,
                        };
                    } else {
                        *recorded = Some(usage);
                    }
                }),
                expected_scope,
            )
            .await;
        drop(receipt);
        result
    }
    fn info(&self) -> &crate::types::provider::ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> crate::types::provider::ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn capture_capabilities(
        &self,
        models: &[RuntimeString],
    ) -> std::collections::HashMap<RuntimeString, crate::types::provider::ProviderCapabilities>
    {
        self.inner.capture_capabilities(models)
    }
    fn has_model_capabilities(&self, model: &RuntimeString) -> bool {
        self.inner.has_model_capabilities(model)
    }
    fn stream(
        &self,
        mut request: crate::types::provider::ModelRequest,
        signal: crate::types::abort::AbortSignal,
    ) -> crate::types::provider::ModelStream {
        use futures::StreamExt;
        let model = request.model.clone();
        if !self.inner.has_model_capabilities(&model) {
            return crate::provider::model_catalog::uncaptured_model_stream(&model);
        }
        let capabilities = self.inner.capabilities(&model);
        request.bind_media_capabilities(&capabilities);
        if !capabilities.vision {
            crate::engine::query::request::omit_request_images(&mut request.messages);
        }
        if !capabilities.native_pdf {
            crate::engine::query::request::omit_historical_request_documents(
                &mut request.messages,
                false,
            );
        }
        crate::engine::query::tool_dispatch::gate_request_images(&mut request);
        let sink = self.sink.clone();
        let call_model = self.call_model.clone();
        let broker = self.broker.clone();
        Box::pin(async_stream::stream! {
            let slot = match reserve_receipt(broker.as_ref(), &signal).await {
                Ok(slot) => slot,
                Err(error) => {
                    yield crate::types::provider::ModelStreamEvent::Error { error: usage_admission_error(error) };
                    return;
                }
            };
            let mut receipt = SummaryUsageReceipt { model, sink, slot, actual: None };
            let mut stream = call_model(request, signal);
            while let Some(event) = stream.next().await {
                match &event {
                    crate::types::provider::ModelStreamEvent::UsageSnapshot { usage }
                    | crate::types::provider::ModelStreamEvent::MessageStop { usage, .. } => receipt.actual = Some(usage.clone()),
                    _ => {}
                }
                yield event;
            }
        })
    }
}

#[cfg(test)]
mod native_summary_usage_tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, runtime_json};
    use crate::schemas::message::{NativeCompactionCheckpoint, Usage};
    use crate::types::abort::AbortSignal;
    use crate::types::errors::ProviderError;
    use crate::types::provider::{
        ModelRequest, ModelStream, NativeCompactionUsageObserver, ProviderCapabilities,
        ProviderInfo, RemoteCompactionResult,
    };

    struct MeteredNativeProvider {
        info: ProviderInfo,
        pending: Option<Arc<tokio::sync::Notify>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl ModelProvider for MeteredNativeProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }
        fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
            panic!("native compaction must not request text capabilities")
        }
        fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
            panic!("native compaction must not request a text summary")
        }
        async fn compact_remote_with_usage(
            &self,
            _: ModelRequest,
            _: AbortSignal,
            _: u64,
            observer: NativeCompactionUsageObserver,
            _: Option<RuntimeString>,
        ) -> Result<RemoteCompactionResult, ProviderError> {
            for (input, output) in [(30, 7), (100, 12)] {
                observer(Usage {
                    input_tokens: input,
                    output_tokens: output,
                    cache_read_input_tokens: 4,
                    cache_creation_input_tokens: 2,
                    total_tokens: Some(input + output + 6),
                });
            }
            if let Some(ready) = &self.pending {
                ready.notify_one();
                futures::future::pending::<()>().await;
            }
            if self.fail {
                return Err(usage_admission_error(std::io::Error::other("fixture")));
            }
            Ok(RemoteCompactionResult {
                checkpoint: NativeCompactionCheckpoint {
                    source: "fixture".into(),
                    auth_scope: "fixture".into(),
                    model: "selected".into(),
                    comp_hash: None,
                    response_id: "fixture".into(),
                    encrypted_content: "opaque".into(),
                    retained_input: Vec::new(),
                    retained_input_metadata: Vec::new(),
                },
                usage: None,
            })
        }
    }

    #[tokio::test]
    async fn native_summary_receipts_include_retries_on_success_failure_and_future_drop() {
        for mode in 0..3 {
            let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink_records = recorded.clone();
            let ready = Arc::new(tokio::sync::Notify::new());
            let provider = SummaryProvider {
                inner: Arc::new(MeteredNativeProvider {
                    info: ProviderInfo {
                        name: "fixture".into(),
                        base_url: None,
                        env_key: None,
                        default_model: "selected".into(),
                    },
                    pending: (mode == 2).then(|| ready.clone()),
                    fail: mode == 1,
                }),
                call_model: Arc::new(|_, _| panic!("text transport must not be called")),
                sink: Arc::new(move |model, usage, slot| {
                    assert_eq!(model, &RuntimeString::from("selected"));
                    assert!(slot.is_none());
                    sink_records.lock_recover().push(usage.clone());
                }),
                broker: None,
            };
            let request: ModelRequest = runtime_codec::decode_value(
                &runtime_json::parse(&RuntimeString::from(
                    r#"{"model":"selected","system":[],"messages":[],"tools":[],"maxOutputTokens":1024}"#,
                ))
                .unwrap(),
            )
            .unwrap();
            let task = tokio::spawn(async move {
                provider
                    .compact_remote_with_idle_timeout(request, AbortSignal::new(), 100)
                    .await
            });
            if mode == 2 {
                ready.notified().await;
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(task.await.unwrap().is_err(), mode == 1);
            }
            assert_eq!(
                *recorded.lock_recover(),
                vec![Usage {
                    input_tokens: 130,
                    output_tokens: 19,
                    cache_read_input_tokens: 8,
                    cache_creation_input_tokens: 4,
                    total_tokens: Some(161),
                }],
                "each logical compaction must persist all attempt usage exactly once"
            );
        }
    }
}
