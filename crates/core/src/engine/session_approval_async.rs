//! Background action classification using the owning session's model transport.

use super::session_approval_review_part::{ReviewAuthorization, ReviewEvidenceSnapshot};
use super::Session;
use crate::engine::approval_async as scoring;
use crate::engine::approval_context as evidence;
use crate::provider::approval_classifier_transport::{
    create_classifier_transport, ApprovalClassifierTransport, ClassifierAttribution,
    ClassifierAuthRecovery, ClassifierRequest, ClassifierTransportEvent,
};
use crate::provider::review_target::capture_review_model_target_with_effort;
use crate::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeObject, RuntimeString,
};
use crate::schemas::content::{ContentBlock, ThinkingBlock};
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::schemas::message::{AssistantMessage, Message, Usage};
use crate::store::session_store::AppendEventOptions;
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderErrorKind;
use crate::types::events::EngineEvent;
use crate::types::permission::{
    ApprovalActionCallSource, ApprovalActionMetadata, ApprovalActionSource, ApprovalKind,
    ApprovalRequest, ApprovalReviewContext, ApprovalsReviewer, SandboxMode,
};
use crate::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStreamEvent,
    ReasoningSummary, ThinkingContextPolicy, ToolChoice,
};
use futures::StreamExt;
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::TaskTracker;

type ReuseKey = scoring::ConversationReuseKey<ReviewAuthorization, String, RuntimeJson>;
type RetainedBackend = scoring::ConversationBackend<ReuseKey, RetainedHistory>;
type Reservation = scoring::ConversationReservation<ReuseKey, RetainedHistory>;
type Envelope = evidence::history_types::ResponseItemEnvelope;

struct ClassifierUsageReceipt<'a> {
    session: &'a Session,
    capture: &'a ApprovalReviewContext,
    actual: Option<Usage>,
    slot: Option<crate::store::receipt_broker::ReceiptSlot>,
}

impl Drop for ClassifierUsageReceipt<'_> {
    fn drop(&mut self) {
        if let Some(usage) = self.actual.take() {
            self.session
                .record_classifier_usage(self.capture, usage, self.slot.take());
        }
    }
}

#[derive(Clone)]
struct RetainedHistory {
    native_input: Vec<RuntimeJson>,
    annotated: Vec<Envelope>,
    tokens: usize,
}

struct RuntimeState {
    configuration: Option<RuntimeString>,
    generation: Arc<()>,
    cancellation: AbortSignal,
    retained: Option<Arc<RetainedBackend>>,
    transport: Option<Arc<dyn ApprovalClassifierTransport>>,
    prewarmed: bool,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            configuration: None,
            generation: Arc::new(()),
            cancellation: AbortSignal::new(),
            retained: None,
            transport: None,
            prewarmed: false,
        }
    }
}

pub(crate) struct ApprovalAsyncRuntime {
    progress: Arc<scoring::ScoreProgress<ReviewAuthorization>>,
    sampler: Arc<scoring::ClassifierSampler>,
    state: Mutex<RuntimeState>,
    tasks: TaskTracker,
    shutdown: AbortSignal,
}

impl Default for ApprovalAsyncRuntime {
    fn default() -> Self {
        Self {
            progress: Arc::default(),
            sampler: Arc::default(),
            state: Mutex::default(),
            tasks: TaskTracker::new(),
            shutdown: AbortSignal::new(),
        }
    }
}

impl ApprovalAsyncRuntime {
    pub(crate) fn reset(&self) {
        let old = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.cancellation.cancel();
            std::mem::take(&mut *state)
        };
        if let Some(transport) = old.transport {
            self.tasks.spawn(async move {
                transport.shutdown().await;
            });
        }
        self.progress.reset();
    }

    fn disable_configuration(&self, key: RuntimeString) {
        let old = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.configuration.as_ref() == Some(&key) && state.transport.is_none() {
                return;
            }
            state.cancellation.cancel();
            std::mem::replace(
                &mut *state,
                RuntimeState {
                    configuration: Some(key),
                    ..Default::default()
                },
            )
        };
        self.progress.reset();
        if let Some(transport) = old.transport {
            self.tasks.spawn(async move {
                transport.shutdown().await;
            });
        }
    }

    fn configuration_is_current(&self, key: &RuntimeString) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.configuration.as_ref() == Some(key) && !state.cancellation.is_cancelled()
    }

    fn bind_configuration(
        &self,
        settings: &ResolvedSettings,
        attribution: ClassifierAttribution,
        should_prewarm: bool,
    ) -> BoundRuntime {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.configuration.as_ref() != Some(&settings.configuration_key) {
            state.cancellation.cancel();
            if let Some(old) = state.transport.take() {
                self.tasks.spawn(async move {
                    old.shutdown().await;
                });
            }
            *state = RuntimeState {
                configuration: Some(settings.configuration_key.clone()),
                ..RuntimeState::default()
            };
            self.progress.reset();
            if let Some(mut auth) = settings.classifier.classifier_auth_changes() {
                let initial_owner = auth.borrow_and_update().owner_generation;
                let cancellation = state.cancellation.clone();
                let progress = self.progress.clone();
                self.tasks.spawn(async move {
                    loop {
                        let changed = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => return,
                            changed = auth.changed() => changed,
                        };
                        if changed.is_err() {
                            return;
                        }
                        if auth.borrow_and_update().owner_generation != initial_owner {
                            progress.reset();
                            cancellation.cancel();
                            return;
                        }
                    }
                });
            }
        }
        if settings.config.classifier_mode == scoring::AsyncClassifierMode::Conversation
            && state.retained.is_none()
        {
            state.retained = Some(Arc::new(RetainedBackend::default()));
        }
        let transport = state
            .transport
            .get_or_insert_with(|| {
                create_classifier_transport(settings.classifier.clone(), attribution)
            })
            .clone();
        if should_prewarm && !state.prewarmed && !self.shutdown.is_cancelled() {
            state.prewarmed = true;
            let warming = transport.clone();
            self.tasks.spawn(async move {
                warming.prewarm().await;
            });
        }
        BoundRuntime {
            generation: Arc::downgrade(&state.generation),
            cancellation: state.cancellation.child_token(),
            backend: state.retained.clone(),
            transport,
        }
    }

    async fn shutdown(&self) {
        self.shutdown.cancel();
        self.reset();
        self.sampler.shutdown().await;
        self.tasks.close();
        self.tasks.wait().await;
    }
}

struct BoundRuntime {
    generation: Weak<()>,
    cancellation: AbortSignal,
    backend: Option<Arc<RetainedBackend>>,
    transport: Arc<dyn ApprovalClassifierTransport>,
}

struct ResolvedSettings {
    policy: scoring::ModelReviewPolicy,
    config: scoring::AsyncReviewConfig,
    classifier: Arc<dyn ModelProvider>,
    classifier_metadata: ModelCapabilities,
    max_input_tokens: usize,
    instructions: String,
    required_for_model: bool,
    configuration_key: RuntimeString,
}

struct ClassificationContext {
    capture: ApprovalReviewContext,
    request: ApprovalRequest,
    settings: ResolvedSettings,
    bound: BoundRuntime,
    authorization: ReviewAuthorization,
    snapshot: ReviewEvidenceSnapshot,
}

fn node_text(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node projection is UTF-8")
}

fn utf8(value: &RuntimeString) -> Result<String, RuntimeString> {
    value
        .try_to_utf8()
        .map_err(|_| RuntimeString::from("Review context contains invalid text."))
}

fn optional_utf8(value: Option<&RuntimeString>) -> Result<Option<String>, RuntimeString> {
    value.map(utf8).transpose()
}

fn model_message(
    messages: Option<&RuntimeJson>,
    key: &str,
) -> Result<Option<String>, RuntimeString> {
    messages
        .and_then(|messages| {
            messages
                .get("auto_review")
                .or_else(|| messages.get("autoReview"))
        })
        .and_then(|messages| messages.get(key))
        .and_then(RuntimeJson::as_string)
        .map(utf8)
        .transpose()
}

fn async_model_policy_messages(
    parent: &ModelCapabilities,
    catalog: &[(RuntimeString, ModelCapabilities)],
    preferred: &RuntimeString,
) -> Result<evidence::ModelReviewMessages, RuntimeString> {
    let wanted = parent
        .auto_review_model_override
        .as_ref()
        .unwrap_or(preferred);
    let selected = catalog
        .iter()
        .find(|(model, _)| model == wanted)
        .map(|(_, metadata)| metadata);
    let metadata = selected.or_else(|| {
        parent
            .auto_review_model_override
            .is_none()
            .then_some(parent)
    });
    Ok(evidence::ModelReviewMessages {
        policy: model_message(
            metadata.and_then(|metadata| metadata.model_messages.as_ref()),
            "policy",
        )?,
        ..Default::default()
    })
}

fn resolve_settings(
    session: &Session,
    capture: &ApprovalReviewContext,
) -> Result<ResolvedSettings, RuntimeString> {
    let accounting = capture
        .accounting
        .clone()
        .unwrap_or_else(|| session.inference_snapshot());
    let requirements = accounting
        .load_review_requirements_sync()
        .map_err(|error| RuntimeString::from(error.to_string()))?;
    let required_for_model = requirements.requires_review_for_model(&utf8(&capture.request.model)?);
    let provider = capture
        .provider
        .clone()
        .ok_or_else(|| RuntimeString::from("The classifier model provider is unavailable."))?;
    let parent_metadata = provider.model_metadata(&capture.request.model);
    let local = capture
        .config
        .approval_review
        .as_ref()
        .and_then(scoring::AsyncFeatureConfig::local_config)
        .cloned()
        .unwrap_or_default();
    let defaults = parent_metadata
        .guardian_v2
        .as_ref()
        .or_else(|| {
            parent_metadata
                .model_messages
                .as_ref()
                .and_then(|messages| {
                    messages
                        .get("guardian_v2")
                        .or_else(|| messages.get("guardianV2"))
                })
        })
        .map(runtime_codec::decode_value::<scoring::AsyncModelConfig>)
        .transpose()
        .map_err(|_| RuntimeString::from("The classifier model settings are invalid."))?;
    let mut config = scoring::AsyncReviewConfig::resolve(local)
        .and_then(|config| config.with_model_defaults(defaults.as_ref()))
        .map_err(RuntimeString::from)?;
    for warning in config.deprecation_warnings() {
        tracing::warn!(warning, "Deprecated approval classifier setting");
    }
    if let Some(format) = capture.config.approval_review_transcript_mode {
        config.transcript.format = format;
    }
    let catalog = parent_metadata
        .guardian
        .as_ref()
        .map(runtime_codec::decode_value::<scoring::ModelReviewPolicy>)
        .transpose()
        .map_err(|_| RuntimeString::from("The model review policy is invalid."))?;
    let policy = scoring::PolicyLoader::new(
        capture.config.approval_review.as_ref(),
        requirements.force_sync,
    )
    .resolve(
        catalog.as_ref(),
        Some(parent_metadata.node_repl_auto_review_required),
    );
    let configured_policy = optional_utf8(capture.config.approval_review_policy.as_ref())?
        .is_some_and(|policy| !policy.trim().is_empty())
        || requirements
            .policy
            .policy
            .as_ref()
            .is_some_and(|policy| !policy.trim().is_empty());
    let review_messages = if configured_policy {
        evidence::ModelReviewMessages::default()
    } else {
        async_model_policy_messages(
            &parent_metadata,
            &provider.offline_model_catalog(),
            &provider.approval_review_preferred_model(),
        )?
    };
    let review_policy = evidence::ResolvedReviewPolicy::resolve(
        &evidence::ReviewPolicyOptions {
            policy: optional_utf8(capture.config.approval_review_policy.as_ref())?,
            extra_policy: optional_utf8(capture.config.approval_review_extra_policy.as_ref())?,
            policy_template: optional_utf8(
                capture.config.approval_review_policy_template.as_ref(),
            )?,
            transcript_mode: Some(context_format(config.transcript.format)),
        },
        &review_messages,
        &requirements.policy,
        evidence::ComputerUseReviewEligibility::default(),
    );
    let selected_instructions =
        evidence::select_classifier_instructions(config.classifier_instructions.as_deref());
    let combined_policy = match review_policy.extra_policy.trim() {
        "" => review_policy.policy.clone(),
        extra => format!("{}\n\n{extra}", review_policy.policy),
    };
    let instructions = evidence::classifier_instructions(
        context_format(config.transcript.format),
        selected_instructions.as_ref(),
        &combined_policy,
        scoring::CLASSIFICATION_OUTPUT_INSTRUCTIONS,
        config.max_classifier_instruction_tokens,
    );
    let model = RuntimeString::from(scoring::CLASSIFIER_MODEL);
    let classifier_metadata = provider
        .offline_model_catalog()
        .into_iter()
        .find(|(id, _)| id == &model)
        .map(|(_, metadata)| metadata);
    let max_input_tokens =
        classifier_metadata
            .as_ref()
            .map_or(evidence::DEFAULT_MAX_INPUT_TOKENS, |metadata| {
                evidence::effective_input_token_limit(
                    &evidence::model::ModelInfo {
                        context_window: metadata
                            .context_window
                            .and_then(|window| i64::try_from(window).ok()),
                        used_fallback_model_metadata: metadata.used_fallback_model_metadata,
                        effective_context_window_percent: metadata
                            .effective_context_window_percent
                            .unwrap_or(95),
                    },
                    None,
                )
            });
    let classifier_metadata = classifier_metadata.unwrap_or_default();
    let classifier = capture_review_model_target_with_effort(
        provider,
        model,
        classifier_metadata.clone(),
        Some(config.reasoning_effort.clone().into()),
        3,
    );
    let mut key = RuntimeObject::new();
    key.insert(
        "config",
        runtime_codec::encode_value(&capture.config)
            .map_err(|_| RuntimeString::from("Cannot encode classifier configuration."))?,
    );
    key.insert("model", RuntimeJson::String(capture.request.model.clone()));
    key.insert(
        "classifierMetadata",
        runtime_codec::encode_value(&classifier_metadata)
            .map_err(|_| RuntimeString::from("Cannot encode classifier metadata."))?,
    );
    key.insert(
        "metadata",
        runtime_codec::encode_value(&parent_metadata)
            .map_err(|_| RuntimeString::from("Cannot encode classifier model metadata."))?,
    );
    key.insert(
        "requirements",
        RuntimeJson::String(format!("{requirements:?}").into()),
    );
    if let Some(auth) = classifier.classifier_auth_changes() {
        key.insert(
            "authOwnerGeneration",
            RuntimeJson::from(auth.borrow().owner_generation),
        );
    }
    key.insert(
        "instructions",
        RuntimeJson::String(instructions.clone().into()),
    );
    let configuration_key = runtime_codec::encode_text(&RuntimeJson::Object(key))
        .map_err(|_| RuntimeString::from("Cannot encode classifier identity."))?;
    Ok(ResolvedSettings {
        policy,
        config,
        classifier,
        classifier_metadata,
        max_input_tokens,
        instructions,
        required_for_model,
        configuration_key,
    })
}

fn shell_permissions(request: &ApprovalRequest, permission: &str) -> bool {
    request
        .input
        .get("sandboxPermissions")
        .or_else(|| request.input.get("sandbox_permissions"))
        .and_then(RuntimeJson::as_string)
        .is_some_and(|value| value == permission)
}

fn context_format(format: scoring::TranscriptFormat) -> evidence::TranscriptFormat {
    match format {
        scoring::TranscriptFormat::Line => evidence::TranscriptFormat::Line,
        scoring::TranscriptFormat::Json => evidence::TranscriptFormat::Json,
    }
}

fn profile(config: &scoring::AsyncReviewConfig) -> evidence::ContextProfile {
    let mut profile = evidence::ContextProfile::asynchronous();
    profile.transcript_format = context_format(config.transcript.format);
    profile.include_images = config.transcript.include_images;
    profile.transcript.options.include_tool_calls = config
        .transcript
        .sources
        .contains(&scoring::TranscriptSource::ToolCalls);
    profile.transcript.options.include_tool_outputs = config
        .transcript
        .sources
        .contains(&scoring::TranscriptSource::ToolOutputs);
    profile.transcript.options.include_reasoning = config
        .transcript
        .sources
        .contains(&scoring::TranscriptSource::Reasoning);
    profile.transcript.entry_limits.message_tokens = config.transcript.max_message_entry_tokens;
    profile.transcript.entry_limits.tool_tokens = config.transcript.max_tool_entry_tokens;
    profile.transcript.entry_limits.node_repl_output_tokens =
        config.transcript.max_tool_entry_tokens;
    profile.retention.max_message_transcript_tokens =
        config.transcript.max_message_transcript_tokens;
    profile.retention.max_tool_transcript_tokens = config.transcript.max_tool_transcript_tokens;
    profile.retention.max_recent_non_user_entries = config.transcript.max_recent_non_user_entries;
    profile
}

fn attribution(session: &Session, capture: &ApprovalReviewContext) -> ClassifierAttribution {
    ClassifierAttribution {
        session_id: node_text(&session.id),
        parent_thread_id: node_text(&session.id),
        parent_turn_id: node_text(&capture.turn_id),
        turn_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    }
}

fn native_json(
    item: &evidence::model::ResponseItem,
) -> Result<RuntimeJson, scoring::ClassifierError> {
    let text = serde_json::to_string(item).map_err(|_| scoring::ClassifierError::InvalidOutput)?;
    runtime_json::parse(&text.into()).map_err(|_| scoring::ClassifierError::InvalidOutput)
}

fn typed_item(item: &RuntimeJson) -> Option<evidence::model::ResponseItem> {
    serde_json::from_slice(&runtime_json::stringify(item).to_node_utf8()).ok()
}

fn native_tokens(item: &RuntimeJson) -> Result<usize, scoring::ClassifierError> {
    let typed = typed_item(item).ok_or(scoring::ClassifierError::InvalidOutput)?;
    Ok(evidence::estimate_input_tokens_from_serialized_len(
        &typed,
        runtime_json::stringify(item).to_node_utf8().len(),
    ))
}

fn select_compaction(
    snapshot: &ReviewEvidenceSnapshot,
    settings: &ResolvedSettings,
) -> Result<scoring::SelectedCompaction<RuntimeJson>, scoring::ClassifierError> {
    // This history owns original review evidence independently of model compaction.
    // Opaque checkpoints require a host-attested compatibility identity before reuse.
    if snapshot.history.items.iter().any(|item| {
        matches!(
            item.item,
            evidence::model::ResponseItem::Compaction { .. }
                | evidence::model::ResponseItem::ContextCompaction { .. }
        )
    }) {
        return Err(scoring::ClassifierError::IncompatibleCompaction);
    }
    let classifier_hash = settings
        .classifier_metadata
        .comp_hash
        .as_ref()
        .map(utf8)
        .transpose()
        .map_err(|_| scoring::ClassifierError::IncompatibleCompaction)?;
    scoring::select_parent_compaction(
        snapshot.context_mode,
        &settings.config,
        None,
        None,
        classifier_hash.as_deref(),
    )
    .map_err(|_| scoring::ClassifierError::IncompatibleCompaction)
}

fn normalize_new_item(mut item: RuntimeJson) -> RuntimeJson {
    if let Some(object) = item.as_object_mut() {
        let kind = object
            .get("type")
            .and_then(RuntimeJson::as_string)
            .map(node_text)
            .unwrap_or_default();
        if object.get("id").is_none_or(|id| id == &RuntimeJson::Null)
            && matches!(
                kind.as_str(),
                "message"
                    | "agent_message"
                    | "reasoning"
                    | "compaction"
                    | "context_compaction"
                    | "additional_tools"
            )
        {
            let prefix = if kind == "reasoning" {
                "rs"
            } else if kind.contains("compaction") {
                "cmp"
            } else if kind == "additional_tools" {
                "at"
            } else {
                "msg"
            };
            object.insert(
                "id",
                RuntimeJson::String(format!("{prefix}_{}", uuid::Uuid::new_v4()).into()),
            );
        }
        if kind == "message" {
            if let Some(content) = object
                .get_mut("content")
                .and_then(RuntimeJson::as_array_mut)
            {
                for content in content {
                    if content
                        .get("type")
                        .and_then(RuntimeJson::as_string)
                        .is_some_and(|kind| kind == "input_image")
                    {
                        if let Some(content) = content.as_object_mut() {
                            content.remove("detail");
                        }
                    }
                }
            }
        }
    }
    item
}

fn setup_input(instructions: &str) -> Result<Vec<RuntimeJson>, scoring::ClassifierError> {
    let mut tools = RuntimeObject::new();
    tools.insert("type", RuntimeJson::String("additional_tools".into()));
    tools.insert("role", RuntimeJson::String("developer".into()));
    tools.insert("tools", RuntimeJson::Array(Vec::new()));
    let instruction = evidence::model::ResponseItem::Message {
        id: None,
        status: None,
        encrypted_content: None,
        role: "developer".to_owned(),
        content: vec![evidence::model::ContentItem::InputText {
            text: instructions.to_owned(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    Ok(vec![
        normalize_new_item(RuntimeJson::Object(tools)),
        normalize_new_item(native_json(&instruction)?),
    ])
}

struct PreparedClassification {
    native_input: Vec<RuntimeJson>,
    annotated: Vec<Envelope>,
    tokens: usize,
    cursor: scoring::TranscriptCursor,
}

fn collect_context(
    snapshot: &ReviewEvidenceSnapshot,
    config: &scoring::AsyncReviewConfig,
    action: &evidence::PlannedAction,
) -> Result<evidence::CollectedContext, scoring::ClassifierError> {
    let profile = profile(config);
    evidence::default_registry()
        .prepare(&evidence::SectionInput {
            target: evidence::ContextTarget::Async,
            history: &snapshot.history,
            transcript: &profile.transcript,
            root_conversation: &snapshot.root_conversation,
            trusted_user_answers: &snapshot.trusted_user_answers,
            planned_action: Some(action),
            permissions: Some(&snapshot.permissions),
            previous_reviews: snapshot.previous_reviews.as_ref(),
            trusted_tool: snapshot.trusted_tool.as_ref(),
            trusted_skill_paths: &snapshot.trusted_skill_paths,
            images: Some(evidence::TranscriptImageInput {
                enabled: profile.include_images,
                include_tool_outputs: profile.transcript.options.include_tool_outputs,
                node_repl_images: &[],
            }),
            node_repl: None,
        })
        .map_err(|_| scoring::ClassifierError::InputTooLarge)
}

fn prepare_classification(
    collected: evidence::CollectedContext,
    snapshot: &ReviewEvidenceSnapshot,
    settings: &ResolvedSettings,
    previous: Option<&scoring::CompletedConversation<ReuseKey, RetainedHistory>>,
) -> Result<PreparedClassification, scoring::ClassifierError> {
    let mut collected = collected;
    let history = previous
        .map(|previous| previous.history.annotated.as_slice())
        .unwrap_or_default();
    collected.retain_new_reviews(history);
    let mode = previous.map_or(evidence::TranscriptMode::Full, |previous| {
        evidence::TranscriptMode::Delta {
            cursor: evidence::TranscriptCursor {
                parent_history_version: previous.cursor.parent_history_version,
                transcript_entry_count: previous.cursor.transcript_entry_count,
            },
        }
    });
    let (selection, cursor) =
        mode.select(collected.transcript_entries(), snapshot.history_generation);
    let (entries, offset, presentation) = match selection {
        evidence::TranscriptSelection::Full(entries) => {
            (entries, 0, evidence::ContextPresentation::Async)
        }
        evidence::TranscriptSelection::Delta { entries, offset } => {
            (entries, offset, evidence::ContextPresentation::AsyncDelta)
        }
    };
    let transcript = profile(&settings.config).prepare_transcript(entries, offset);
    let mut composed = collected
        .compose(presentation, transcript)
        .map_err(|_| scoring::ClassifierError::InputTooLarge)?;
    if settings.config.classifier_mode == scoring::AsyncClassifierMode::Snapshot {
        composed.deduplicate_transcript_instructions();
    } else {
        composed.retain_new_instructions(history);
        if previous.is_some() {
            composed.retain_images(|image, _| !history.iter().any(|envelope| matches!(&envelope.item,
                evidence::model::ResponseItem::Message { content, .. } if content.iter().any(|content| matches!(content,
                    evidence::model::ContentItem::InputImage { image: retained, .. } if retained == image)))));
        }
    }
    let suffix = composed.into_annotated_messages();
    let mut native_input = match previous {
        Some(previous) => previous.history.native_input.clone(),
        None => setup_input(&settings.instructions)?,
    };
    let mut annotated = previous
        .map(|previous| previous.history.annotated.clone())
        .unwrap_or_default();
    let mut tokens = previous
        .map(|previous| previous.history.tokens)
        .unwrap_or_else(|| {
            native_input
                .iter()
                .map(|item| native_tokens(item).unwrap_or(usize::MAX))
                .fold(0usize, usize::saturating_add)
        });
    for mut envelope in suffix {
        let item = normalize_new_item(native_json(&envelope.item)?);
        if let Some(normalized) = typed_item(&item) {
            envelope.item = normalized;
        }
        tokens = tokens.saturating_add(native_tokens(&item)?);
        native_input.push(item);
        annotated.push(envelope);
    }
    if tokens > settings.max_input_tokens.saturating_sub(256) {
        return Err(scoring::ClassifierError::InputTooLarge);
    }
    Ok(PreparedClassification {
        native_input,
        annotated,
        tokens,
        cursor: scoring::TranscriptCursor {
            parent_history_version: cursor.parent_history_version,
            transcript_entry_count: cursor.transcript_entry_count,
        },
    })
}

fn projected_request(
    capture: &ApprovalReviewContext,
    settings: &ResolvedSettings,
    input: &[RuntimeJson],
    cache_key: RuntimeString,
) -> Result<ModelRequest, scoring::ClassifierError> {
    let mut request = capture.request.clone();
    request.model = scoring::CLASSIFIER_MODEL.into();
    request.system.clear();
    request.messages.clear();
    request.tools.clear();
    request.tool_choice = Some(ToolChoice::None);
    request.max_output_tokens = settings
        .classifier_metadata
        .clamp_output_tokens(capture.request.max_output_tokens);
    request.task_budget = None;
    request.output_schema = None;
    request.web_search = None;
    request.thinking = None;
    request.effort = Some(EffortValue::Level(EffortLevel::Low));
    request.model_verbosity = None;
    request.reasoning_summary = ReasoningSummary::None;
    request.skip_cache_write = Some(true);
    request.cache_ttl = None;
    request.cache_key = Some(cache_key);
    request.cache_domain = None;
    request.context_management = None;
    request.thinking_context = Some(ThinkingContextPolicy::KeepAll);
    for native in input {
        let Some(item) = typed_item(native) else {
            continue;
        };
        match item {
            evidence::model::ResponseItem::AdditionalTools { .. } => {}
            evidence::model::ResponseItem::Reasoning {
                id,
                summary,
                encrypted_content,
                signature,
                ..
            } => {
                request.messages.push(Message::Assistant(AssistantMessage {
                    uuid: id
                        .clone()
                        .map(|id| id.0)
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
                        .into(),
                    model: request.model.clone(),
                    usage: None,
                    content: vec![ContentBlock::Thinking(ThinkingBlock {
                        text: summary
                            .into_iter()
                            .map(|summary| match summary {
                                evidence::model::ReasoningItemReasoningSummary::SummaryText {
                                    text,
                                } => text,
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                            .into(),
                        signature: signature.map(Into::into),
                        encrypted_content: encrypted_content.map(Into::into),
                        message_id: id.map(|id| id.0.into()),
                    })],
                }));
            }
            evidence::model::ResponseItem::Message { .. } => request.messages.extend(
                evidence::model_messages(&[Envelope::new(item)], &mut request.system)
                    .map_err(|_| scoring::ClassifierError::InvalidOutput)?,
            ),
            _ => {}
        }
    }
    request.bind_media_capabilities(&settings.classifier.capabilities(&request.model));
    Ok(request)
}

fn output_item(item: RuntimeJson) -> scoring::SamplingEvent<RuntimeJson> {
    let decoded = typed_item(&item);
    let tokens = native_tokens(&item).unwrap_or(usize::MAX);
    let (valid_for_retention, assistant_text) = match decoded {
        Some(evidence::model::ResponseItem::Message { role, content, .. })
            if role == "assistant" =>
        {
            let mut text = String::new();
            let mut valid = true;
            for content in content {
                match content {
                    evidence::model::ContentItem::OutputText { text: part, .. } => {
                        text.push_str(&part)
                    }
                    _ => valid = false,
                }
            }
            (valid, Some(text))
        }
        Some(evidence::model::ResponseItem::Reasoning {
            encrypted_content: Some(content),
            ..
        }) => (!content.is_empty(), None),
        _ => (false, None),
    };
    scoring::SamplingEvent::OutputItem {
        item,
        tokens,
        valid_for_retention,
        assistant_text,
    }
}

fn classifier_error_is_terminal(error: &crate::types::errors::ProviderError) -> bool {
    matches!(
        error.kind,
        ProviderErrorKind::PromptTooLong
            | ProviderErrorKind::RequestTooLarge
            | ProviderErrorKind::Quota
            | ProviderErrorKind::MaxOutput
            | ProviderErrorKind::InvalidRequest
    ) || error.code.as_ref().is_some_and(|code| {
        matches!(
            node_text(code).as_str(),
            "response_incomplete"
                | "invalid_prompt"
                | "invalid_request_error"
                | "cyber_policy"
                | "bio_policy"
                | "bio_policy_violation"
                | "misalignment_policy_violation"
                | "usage_not_included"
                | "rate_limit"
        )
    })
}

fn classifier_error_has_retryable_category(error: &crate::types::errors::ProviderError) -> bool {
    matches!(
        error.kind,
        ProviderErrorKind::Network
            | ProviderErrorKind::Timeout
            | ProviderErrorKind::Overloaded
            | ProviderErrorKind::RateLimit
            | ProviderErrorKind::Withheld
    )
}

fn classifier_error_retryable(error: &crate::types::errors::ProviderError) -> bool {
    !classifier_error_is_terminal(error)
        && error.kind != ProviderErrorKind::Auth
        && (classifier_error_has_retryable_category(error)
            || error
                .status
                .is_some_and(|status| status >= 500 || status == 429))
}

async fn retry_classifier_after_failure(
    error: &crate::types::errors::ProviderError,
    auth_recovery: &mut Option<Box<dyn ClassifierAuthRecovery>>,
    retries: &mut usize,
) -> bool {
    if error.status == Some(401)
        && !classifier_error_is_terminal(error)
        && !classifier_error_has_retryable_category(error)
    {
        return match auth_recovery.as_mut() {
            Some(recovery) => recovery.next().await,
            None => false,
        };
    }
    if *retries < 2 && classifier_error_retryable(error) {
        *retries += 1;
        return true;
    }
    false
}

fn classification_record(
    call_id: &RuntimeString,
    action: &RuntimeString,
    sampled_at: SystemTime,
    label: &str,
) -> Result<RuntimeJson, scoring::ClassifierError> {
    let action =
        runtime_json::parse(action).map_err(|_| scoring::ClassifierError::InvalidOutput)?;
    let risk = match label {
        "low" => 0.0,
        "high" => 1.0,
        _ => return Err(scoring::ClassifierError::InvalidOutput),
    };
    let mut scores = RuntimeObject::new();
    scores.insert("action_risk", RuntimeJson::from(risk));
    let mut payload = RuntimeObject::new();
    payload.insert("callId", RuntimeJson::String(call_id.clone()));
    payload.insert("action", action);
    payload.insert("scores", RuntimeJson::Object(scores));
    payload.insert(
        "sampledAt",
        RuntimeJson::String(
            chrono::DateTime::<chrono::Utc>::from(sampled_at)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
                .into(),
        ),
    );
    Ok(RuntimeJson::Object(payload))
}

async fn pending_classification_result(
    score: oneshot::Receiver<Result<String, scoring::ClassifierError>>,
    pending: scoring::PendingScore<ReviewAuthorization>,
    cancellation: AbortSignal,
    shutdown: AbortSignal,
) -> Result<String, scoring::ClassifierError> {
    tokio::select! {
        biased;
        () = pending.expired() => Err(scoring::ClassifierError::Superseded),
        () = cancellation.cancelled() => Err(scoring::ClassifierError::Superseded),
        () = shutdown.cancelled() => Err(scoring::ClassifierError::Superseded),
        score = score => score.unwrap_or(Err(scoring::ClassifierError::Superseded)),
    }
}

async fn deliver_event(
    sender: &mpsc::Sender<scoring::SamplingEvent<RuntimeJson>>,
    event: scoring::SamplingEvent<RuntimeJson>,
    abort: &AbortSignal,
    delivering: &mut bool,
) {
    if *delivering {
        *delivering = tokio::select! {
            biased;
            () = abort.cancelled() => false,
            sent = sender.send(event) => sent.is_ok(),
        };
    }
}

impl Session {
    pub(crate) async fn shutdown_approval_async(&self) {
        self.approval_async.shutdown().await;
    }

    pub(crate) fn finish_approval_action(&self, id: &RuntimeString) {
        if let Ok(id) = id.try_to_utf8() {
            self.approval_async.progress.finish(&id);
        }
    }

    pub(crate) fn prewarm_approval_async(&self, capture: &ApprovalReviewContext) {
        if let Ok(settings) = resolve_settings(self, capture) {
            let policy = self.runtime_policy_state();
            let enabled = settings.policy.scoring_enabled()
                && policy.approvals_reviewer == Some(ApprovalsReviewer::Automatic)
                && policy.sandbox_mode != Some(SandboxMode::DangerFullAccess);
            if enabled {
                self.approval_async
                    .bind_configuration(&settings, attribution(self, capture), true);
            } else if !settings.policy.scoring_enabled() {
                self.approval_async
                    .disable_configuration(settings.configuration_key);
            }
        }
    }

    pub(crate) fn observe_approval_action(
        self: &Arc<Self>,
        capture: ApprovalReviewContext,
        request: ApprovalRequest,
        metadata: ApprovalActionMetadata,
    ) {
        if self.approval_async.shutdown.is_cancelled() {
            return;
        }
        let settings = match resolve_settings(self, &capture) {
            Ok(settings) => settings,
            Err(message) => {
                (capture.emit)(EngineEvent::ApprovalReviewWarning {
                    message,
                    kind: Some("classification_configuration".into()),
                });
                return;
            }
        };
        if !settings.policy.scoring_enabled() {
            self.approval_async
                .disable_configuration(settings.configuration_key);
            return;
        }
        let live_policy = self.runtime_policy_state();
        let full_access = live_policy.sandbox_mode == Some(SandboxMode::DangerFullAccess);
        let user_reviewer = live_policy.approvals_reviewer != Some(ApprovalsReviewer::Automatic);
        let bound = self.approval_async.bind_configuration(
            &settings,
            attribution(self, &capture),
            !full_access && !user_reviewer,
        );
        let Ok(id) = request.id.try_to_utf8() else {
            return;
        };
        let Ok(name) = metadata.registered_tool_name.try_to_utf8() else {
            return;
        };
        let originating = metadata
            .originating_item_id
            .as_ref()
            .and_then(|id| id.try_to_utf8().ok());
        let wrapper = match &metadata.call_source {
            ApprovalActionCallSource::Direct => None,
            ApprovalActionCallSource::CodeMode {
                originating_tool_call_id,
                ..
            } => originating_tool_call_id.try_to_utf8().ok(),
        };
        let origin = match &metadata.call_source {
            ApprovalActionCallSource::Direct => scoring::ToolOrigin::Direct,
            _ => scoring::ToolOrigin::Nested {
                wrapper_item_id: wrapper.as_deref(),
            },
        };
        let scope = if request.kind == ApprovalKind::Network {
            Some(scoring::ReviewScope::Network)
        } else {
            metadata.review_scope()
        };
        let is_js = metadata.is_computer_use_js_execution();
        let default_namespace = matches!(&metadata.source, ApprovalActionSource::Builtin);
        let observation = scoring::ToolObservation {
            call_id: &id,
            originating_item_id: originating.as_deref(),
            origin,
            is_code_wrapper: metadata.is_code_wrapper(),
        };
        let observed = scoring::ObservationInput {
            action: observation,
            default_namespace,
            tool_name: &name,
            scope,
            explicitly_escalated: shell_permissions(&request, "require_escalated"),
            with_additional_permissions: shell_permissions(&request, "with_additional_permissions"),
            computer_use_js_execution: is_js,
            full_access,
            user_reviewer,
            synchronous_review_required_for_model: settings.required_for_model,
        };
        let sampled_at = SystemTime::now();
        let (index, reservation) = if let Some(backend) = bound.backend.as_ref().filter(|_| {
            settings.policy.scores_tool(
                scope,
                default_namespace,
                &name,
                observed.explicitly_escalated,
            )
        }) {
            if is_js {
                self.approval_async.progress.observe_js_execution();
            }
            let (index, reservation) =
                backend.reserve(|| self.approval_async.progress.observe(observation));
            if full_access || user_reviewer {
                self.approval_async.progress.invalidate(index);
                return;
            }
            if settings.required_for_model
                && !(scope == Some(scoring::ReviewScope::ComputerUse)
                    && settings.policy.allows_initial_computer_use())
            {
                self.approval_async.progress.clear_score();
                return;
            }
            (index, reservation.map(Some))
        } else {
            match scoring::observe_action(&self.approval_async.progress, &settings.policy, observed)
            {
                scoring::ObservationDisposition::Classify { index } => (index, Ok(None)),
                _ => return,
            }
        };
        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(_) => {
                self.approval_async.progress.invalidate(index);
                self.approval_async.progress.fail_closed(sampled_at);
                return;
            }
        };
        let planned_action = match scoring::render_action(
            metadata.registered_tool_name.clone(),
            request.input.clone(),
            settings.config.max_action_tokens,
        ) {
            Ok(action) => action,
            Err(_) => {
                self.approval_async.progress.mark_oversized(&id, index);
                self.approval_async.progress.fail_closed(sampled_at);
                return;
            }
        };
        let authorization = self.approval_authorization(&capture);
        let mut snapshot = match self.review_evidence_snapshot(&capture) {
            Ok(snapshot) => snapshot,
            Err(message) => {
                self.approval_async.progress.fail_closed(sampled_at);
                (capture.emit)(EngineEvent::ApprovalReviewWarning {
                    message,
                    kind: Some("classification_context".into()),
                });
                return;
            }
        };
        let session = Arc::clone(self);
        self.approval_async.tasks.spawn(async move {
            snapshot.trusted_tool =
                crate::mcp::approval_provenance::trusted_tool_context(&metadata).await;
            session
                .run_approval_classification(
                    ClassificationContext {
                        capture,
                        request,
                        settings,
                        bound,
                        authorization,
                        snapshot,
                    },
                    reservation,
                    planned_action,
                    sampled_at,
                    index,
                )
                .await;
        });
    }

    async fn run_approval_classification(
        self: Arc<Self>,
        context: ClassificationContext,
        reservation: Option<Reservation>,
        planned_action: RuntimeString,
        sampled_at: SystemTime,
        index: usize,
    ) {
        let pending = scoring::PendingScore {
            progress: self.approval_async.progress.clone(),
            index,
            max_lag: context.settings.config.max_tool_call_lag,
        };
        if pending.is_expired() || context.bound.cancellation.is_cancelled() || self.is_disposed() {
            return;
        }
        let action = evidence::PlannedAction {
            json: node_text(&planned_action),
            kind: evidence::PlannedActionKind::Command,
            reason: None,
            tool_descriptions: None,
        };
        let result = self
            .prepare_and_sample(&context, reservation, &action, pending, sampled_at, index)
            .await;
        if !matches!(result, Err(scoring::ClassifierError::Superseded)) {
            let publication = scoring::publish_classification(
                &self.approval_async.progress,
                result.as_deref(),
                context.authorization.clone(),
                sampled_at,
                index,
                self.approval_authorization(&context.capture) == context.authorization,
                context.bound.generation.strong_count() > 0
                    && !context.bound.cancellation.is_cancelled(),
            );
            if publication == scoring::ClassificationPublication::Failed {
                (context.capture.emit)(EngineEvent::ApprovalReviewWarning { message: "Background action classification could not supply reusable approval evidence.".into(), kind: Some("classification_failed".into()) });
            }
            if publication == scoring::ClassificationPublication::Published
                && context.settings.config.persist_scores
            {
                self.persist_classification(
                    &context.capture,
                    &context.request,
                    &planned_action,
                    sampled_at,
                    result
                        .as_deref()
                        .expect("Only a valid classification can be published"),
                )
                .await;
            }
        }
    }

    async fn prepare_and_sample(
        self: &Arc<Self>,
        context: &ClassificationContext,
        reservation: Option<Reservation>,
        action: &evidence::PlannedAction,
        pending: scoring::PendingScore<ReviewAuthorization>,
        sampled_at: SystemTime,
        index: usize,
    ) -> Result<String, scoring::ClassifierError> {
        let ClassificationContext {
            capture,
            request,
            settings,
            bound,
            authorization,
            snapshot,
        } = context;
        let selected_compaction = select_compaction(snapshot, settings)?;
        let collected = collect_context(snapshot, &settings.config, action)?;
        let cursor = scoring::TranscriptCursor {
            parent_history_version: snapshot.history_generation,
            transcript_entry_count: collected.transcript_entries().len(),
        };
        let key = ReuseKey {
            authorization: authorization.clone(),
            instructions: settings.instructions.clone(),
            parent_compaction: selected_compaction.item,
        };
        let prepared = if let Some(reservation) = reservation.as_ref() {
            reservation
                .prepare(
                    &key,
                    cursor,
                    settings.config.async_classifier_conversation_token_limit,
                    settings.max_input_tokens,
                    |previous| {
                        prepare_classification(collected.clone(), snapshot, settings, previous)
                            .ok()
                            .map(|prepared| {
                                let tokens = prepared.tokens;
                                (prepared, tokens)
                            })
                    },
                )
                .map(|(prepared, _)| prepared)
                .ok_or(scoring::ClassifierError::InputTooLarge)?
        } else {
            prepare_classification(collected, snapshot, settings, None)?
        };
        let model_request = projected_request(
            capture,
            settings,
            &prepared.native_input,
            self.id.concat(&":approval-classifier".into()),
        )?;
        let classifier_request = ClassifierRequest {
            request: model_request,
            attribution: attribution(self, capture),
            retain_history: reservation.is_some(),
            native_input: Some(prepared.native_input.clone()),
        };
        let mut admission = if reservation.is_some() {
            self.approval_async.sampler.admit_retained()
        } else {
            self.approval_async.sampler.admit()
        };
        tokio::select! {
            biased;
            () = pending.expired() => return Err(scoring::ClassifierError::Superseded),
            () = bound.cancellation.cancelled() => return Err(scoring::ClassifierError::Superseded),
            () = self.approval_async.shutdown.cancelled() => return Err(scoring::ClassifierError::Superseded),
            acquired = admission.acquire() => acquired?,
        }
        let abort = AbortSignal::new();
        let (events, done) = self.normalized_classifier_stream(
            capture.clone(),
            bound.transport.clone(),
            classifier_request,
            abort.clone(),
            settings
                .max_input_tokens
                .saturating_sub(prepared.tokens)
                .saturating_sub(256),
        );
        let publish_pending = pending.clone();
        let publish_shutdown = self.approval_async.shutdown.clone();
        let shutdown = self.approval_async.shutdown.clone();
        let cancellation = bound.cancellation.clone();
        let expiry = async move {
            tokio::select! { () = pending.expired() => {}, () = shutdown.cancelled() => {}, () = cancellation.cancelled() => {} }
        };
        if let Some(reservation) = reservation {
            let (ready, scored) = oneshot::channel();
            let current_session = self.clone();
            let current_capture = capture.clone();
            let current_authorization = authorization.clone();
            let generation = bound.generation.clone();
            let cancellation = bound.cancellation.clone();
            let config = settings.config.clone();
            let request = request.clone();
            let action_json = action.json.clone();
            let completed = admission.sample_retained(
                events,
                Some(ready),
                settings
                    .max_input_tokens
                    .saturating_sub(prepared.tokens)
                    .saturating_sub(256),
                expiry,
                move || async move {
                    abort.cancel();
                    let _ = done.await;
                },
            );
            let publish = async move {
                let output = pending_classification_result(
                    scored,
                    publish_pending,
                    cancellation.clone(),
                    publish_shutdown,
                )
                .await;
                let result = scoring::publish_classification(
                    &current_session.approval_async.progress,
                    output.as_deref(),
                    current_authorization.clone(),
                    sampled_at,
                    index,
                    current_session.approval_authorization(&current_capture)
                        == current_authorization,
                    generation.strong_count() > 0 && !cancellation.is_cancelled(),
                );
                if result == scoring::ClassificationPublication::Failed {
                    (current_capture.emit)(EngineEvent::ApprovalReviewWarning { message: "Background action classification could not supply reusable approval evidence.".into(), kind: Some("classification_failed".into()) });
                }
                if result == scoring::ClassificationPublication::Published && config.persist_scores
                {
                    current_session
                        .persist_classification(
                            &current_capture,
                            &request,
                            &action_json.into(),
                            sampled_at,
                            output
                                .as_deref()
                                .expect("Only a valid classification can be published"),
                        )
                        .await;
                }
            };
            let (completed, ()) = tokio::join!(completed, publish);
            if let Some(output) = completed.retained_output {
                let mut retained = RetainedHistory {
                    native_input: prepared.native_input,
                    annotated: prepared.annotated,
                    tokens: prepared.tokens,
                };
                for item in output {
                    if let Some(typed) = typed_item(&item) {
                        retained.tokens = retained
                            .tokens
                            .saturating_add(native_tokens(&item).unwrap_or(usize::MAX));
                        retained.annotated.push(Envelope::new(typed));
                    }
                    retained.native_input.push(item);
                }
                reservation.commit(key, retained, prepared.cursor, || {
                    bound.generation.strong_count() > 0
                        && !bound.cancellation.is_cancelled()
                        && self.approval_authorization(capture) == *authorization
                });
            }
            // Retained mode publishes once as soon as the first complete classifier value is available.
            return Err(scoring::ClassifierError::Superseded);
        }
        admission
            .sample_snapshot(events, expiry, move || async move {
                abort.cancel();
                let _ = done.await;
            })
            .await
    }

    fn normalized_classifier_stream(
        self: &Arc<Self>,
        capture: ApprovalReviewContext,
        transport: Arc<dyn ApprovalClassifierTransport>,
        request: ClassifierRequest,
        abort: AbortSignal,
        retention_budget: usize,
    ) -> (scoring::SamplingStream<RuntimeJson>, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(1);
        let (finished, done) = oneshot::channel();
        let session = self.clone();
        self.approval_async.tasks.spawn(async move {
            let mut retries = 0;
            let mut auth_recovery = transport.auth_recovery(&abort).await;
            let mut early_output = false;
            let mut delivering = true;
            loop {
                if abort.is_cancelled() {
                    break;
                }
                let slot = match session.reserve_usage_receipt(&abort).await {
                    Ok(slot) => slot,
                    Err(error) => {
                        tracing::warn!(%error, "Classifier usage admission failed");
                        deliver_event(
                            &sender,
                            scoring::SamplingEvent::Error(scoring::ClassifierError::Transport),
                            &abort,
                            &mut delivering,
                        ).await;
                        break;
                    }
                };
                let mut usage = ClassifierUsageReceipt {
                    session: &session,
                    capture: &capture,
                    actual: None,
                    slot,
                };
                let mut raw = transport.stream(request.clone(), abort.clone());
                let mut completed = false;
                let mut terminal_error = None;
                let mut invalid_output = false;
                let mut raw_items = false;
                let mut text = String::new();
                let mut text_overflow = false;
                let mut text_id: Option<RuntimeString> = None;
                let mut fallback_reasoning = Vec::new();
                let mut fallback_reasoning_tokens = 0usize;
                let mut fallback_reasoning_valid = true;
                while let Some(event) = raw.next().await {
                    let normalized = match event {
                        ClassifierTransportEvent::OutputItem(item) => {
                            raw_items = true;
                            fallback_reasoning.clear();
                            Some(output_item(item))
                        }
                        ClassifierTransportEvent::Model(ModelStreamEvent::TextMessage {
                            id,
                            ..
                        }) => {
                            text_id = Some(id);
                            None
                        }
                        ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta {
                            text: delta,
                        }) => match delta.try_to_utf8() {
                            Ok(delta) => {
                                if text.len().saturating_add(delta.len())
                                    <= scoring::MAX_CLASSIFIER_OUTPUT_BYTES
                                {
                                    text.push_str(&delta);
                                } else {
                                    text_overflow = true;
                                }
                                early_output |= !delta.is_empty();
                                Some(scoring::SamplingEvent::OutputTextDelta(delta))
                            }
                            Err(_) => {
                                invalid_output = true;
                                Some(scoring::SamplingEvent::Error(
                                    scoring::ClassifierError::InvalidOutput,
                                ))
                            }
                        },
                        ClassifierTransportEvent::Model(ModelStreamEvent::ThinkingBlock {
                            text,
                            is_summary,
                            signature,
                            encrypted_content,
                        }) if !raw_items => {
                            let item = evidence::model::ResponseItem::Reasoning {
                                id: None,
                                summary: if is_summary { vec![evidence::model::ReasoningItemReasoningSummary::SummaryText { text: node_text(&text) }] } else { Vec::new() },
                                content: (!is_summary && !text.is_empty()).then(|| vec![evidence::model::ReasoningItemContent::ReasoningText { text: node_text(&text) }]),
                                encrypted_content: encrypted_content.map(|value| node_text(&value)),
                                signature: signature.map(|value| node_text(&value)),
                            };
                            let tokens = evidence::estimate_input_tokens(&item);
                            if tokens > 10_000
                                || fallback_reasoning_tokens.saturating_add(tokens)
                                    > retention_budget
                            {
                                fallback_reasoning_valid = false;
                            } else if let Ok(item) = native_json(&item) {
                                fallback_reasoning_tokens =
                                    fallback_reasoning_tokens.saturating_add(tokens);
                                fallback_reasoning.push(normalize_new_item(item));
                            }
                            None
                        }
                        ClassifierTransportEvent::Model(ModelStreamEvent::UsageSnapshot {
                            usage: actual,
                        }) => {
                            usage.actual = Some(actual);
                            None
                        }
                        ClassifierTransportEvent::Model(ModelStreamEvent::MessageStop {
                            stop_reason,
                            usage: actual,
                        }) => {
                            usage.actual = Some(actual);
                            completed = matches!(
                                stop_reason,
                                ModelStopReason::EndTurn | ModelStopReason::StopSequence
                            );
                            invalid_output |= !completed;
                            None
                        }
                        ClassifierTransportEvent::Model(ModelStreamEvent::Error { error }) => {
                            terminal_error = Some(error);
                            None
                        }
                        ClassifierTransportEvent::Model(
                            ModelStreamEvent::ToolUseStart { .. }
                            | ModelStreamEvent::ServerSearchQuery { .. },
                        ) => {
                            invalid_output = true;
                            Some(scoring::SamplingEvent::Error(
                                scoring::ClassifierError::InvalidOutput,
                            ))
                        }
                        _ => None,
                    };
                    if let Some(normalized) = normalized {
                        deliver_event(&sender, normalized, &abort, &mut delivering).await;
                    }
                }
                drop(usage);
                if let Some(error) = terminal_error {
                    let may_retry = !early_output
                        && !abort.is_cancelled()
                        && !transport.request_retries_managed();
                    let retry = may_retry
                        && retry_classifier_after_failure(
                            &error,
                            &mut auth_recovery,
                            &mut retries,
                        )
                        .await;
                    if retry {
                        deliver_event(
                            &sender,
                            scoring::SamplingEvent::Restart,
                            &abort,
                            &mut delivering,
                        )
                        .await;
                        continue;
                    }
                    deliver_event(
                        &sender,
                        scoring::SamplingEvent::Error(scoring::ClassifierError::Transport),
                        &abort,
                        &mut delivering,
                    )
                    .await;
                    break;
                }
                if !raw_items && completed && !invalid_output && !text_overflow && !text.is_empty()
                {
                    for item in fallback_reasoning {
                        deliver_event(&sender, output_item(item), &abort, &mut delivering).await;
                    }
                    if !fallback_reasoning_valid {
                        deliver_event(
                            &sender,
                            scoring::SamplingEvent::OutputItem {
                                item: RuntimeJson::Null,
                                tokens: 0,
                                valid_for_retention: false,
                                assistant_text: None,
                            },
                            &abort,
                            &mut delivering,
                        )
                        .await;
                    }
                    let item = evidence::model::ResponseItem::Message {
                        id: Some(
                            text_id
                                .map(|id| node_text(&id))
                                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
                                .into(),
                        ),
                        status: None,
                        encrypted_content: None,
                        role: "assistant".to_owned(),
                        content: vec![evidence::model::ContentItem::OutputText {
                            text,
                            annotations: Vec::new(),
                        }],
                        phase: None,
                        internal_chat_message_metadata_passthrough: None,
                    };
                    if let Ok(item) = native_json(&item) {
                        deliver_event(&sender, output_item(item), &abort, &mut delivering).await;
                    }
                }
                let event = if completed && !invalid_output && (raw_items || !text_overflow) {
                    scoring::SamplingEvent::Completed
                } else if text_overflow && !raw_items {
                    scoring::SamplingEvent::Error(scoring::ClassifierError::OutputTooLarge)
                } else {
                    scoring::SamplingEvent::Error(scoring::ClassifierError::Transport)
                };
                deliver_event(&sender, event, &abort, &mut delivering).await;
                break;
            }
            let _ = finished.send(());
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|event| (event, receiver))
        });
        (Box::pin(stream), done)
    }

    fn record_classifier_usage(
        &self,
        capture: &ApprovalReviewContext,
        usage: Usage,
        slot: Option<crate::store::receipt_broker::ReceiptSlot>,
    ) {
        if let Some(accounting) = capture.accounting.as_ref() {
            self.record_reserved_assistant_usage_for(
                &AssistantMessage {
                    uuid: format!("classification-usage-{}", uuid::Uuid::new_v4()).into(),
                    model: scoring::CLASSIFIER_MODEL.into(),
                    content: Vec::new(),
                    usage: Some(usage),
                },
                accounting,
                slot,
            );
        } else {
            tracing::warn!(
                "Classifier usage could not be attributed to the captured inference configuration"
            );
        }
    }

    async fn persist_classification(
        &self,
        capture: &ApprovalReviewContext,
        request: &ApprovalRequest,
        action: &RuntimeString,
        sampled_at: SystemTime,
        label: &str,
    ) {
        if let Some(rollout) = self.persistence.rollout() {
            match classification_record(&request.id, action, sampled_at, label) {
                Ok(payload) => {
                    rollout
                        .append_event(
                            "approval_classification".into(),
                            Some(AppendEventOptions {
                                turn_id: Some(capture.turn_id.clone()),
                                payload: Some(payload),
                            }),
                        )
                        .await
                }
                Err(error) => tracing::warn!(%error, "Could not persist a classification result"),
            }
        }
    }

    pub(crate) async fn approval_cached_evidence(
        &self,
        capture: &ApprovalReviewContext,
        request: &ApprovalRequest,
        action: &RuntimeJson,
        require_fresh: bool,
        metadata: &ApprovalActionMetadata,
    ) -> Result<scoring::CachedApproval, scoring::ReviewFallbackReason> {
        if require_fresh {
            return Err(scoring::ReviewFallbackReason::FreshRequired);
        }
        let settings = resolve_settings(self, capture)
            .map_err(|_| scoring::ReviewFallbackReason::ScoringFailure)?;
        if !self
            .approval_async
            .configuration_is_current(&settings.configuration_key)
        {
            return Err(scoring::ReviewFallbackReason::AuthorizationChanged);
        }
        let category = (if request.kind == ApprovalKind::Network {
            Some(scoring::ReviewScope::Network)
        } else {
            metadata.review_scope()
        })
        .ok_or(scoring::ReviewFallbackReason::Policy)?;
        if settings.policy.review_mode(category) != scoring::ReviewMode::Adaptive {
            return Err(scoring::ReviewFallbackReason::Policy);
        }
        let snapshot = self
            .review_evidence_snapshot(capture)
            .map_err(|_| scoring::ReviewFallbackReason::ScoringFailure)?;
        let current = self.approval_authorization(capture);
        let id = request.id.try_to_utf8().ok();
        let action_fits =
            scoring::render_review_action(action, settings.config.max_action_tokens).is_ok();
        let compaction = select_compaction(&snapshot, &settings);
        scoring::cached_evidence(
            &self.approval_async.progress,
            &settings.config,
            &settings.policy,
            scoring::CacheCheck {
                call_id: id.as_deref(),
                category,
                action_fits,
                compaction_compatible: compaction.is_ok(),
                is_computer_use_js: metadata.is_computer_use_js_execution(),
                current_authorization: Some(&current),
                retained_context_complete: snapshot.retained_context_complete,
            },
        )
    }

    pub(crate) async fn wait_approval_cache(
        self: Arc<Self>,
        capture: ApprovalReviewContext,
        request: ApprovalRequest,
        action: RuntimeJson,
        metadata: ApprovalActionMetadata,
    ) {
        let progress = Arc::clone(&self.approval_async.progress);
        scoring::wait_for_cached_approval(&progress, || {
            self.approval_cached_evidence(&capture, &request, &action, false, &metadata)
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_a_full_normalization_channel_unblocks_transport_draining() {
        let (sender, _receiver) = mpsc::channel(1);
        sender
            .send(scoring::SamplingEvent::OutputTextDelta("low".to_owned()))
            .await
            .unwrap();
        let abort = AbortSignal::new();
        abort.cancel();
        let mut delivering = true;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            deliver_event(
                &sender,
                scoring::SamplingEvent::Completed,
                &abort,
                &mut delivering,
            ),
        )
        .await
        .unwrap();
        assert!(!delivering);
    }

    #[test]
    fn retained_native_output_keeps_original_encrypted_reasoning_and_ids() {
        let original = runtime_json::parse(&RuntimeString::from(
            r#"{"type":"reasoning","id":"rs_original","summary":[],"encrypted_content":"opaque"}"#,
        ))
        .unwrap();
        let scoring::SamplingEvent::OutputItem {
            item,
            valid_for_retention,
            assistant_text,
            ..
        } = output_item(original.clone())
        else {
            panic!("A native output item must remain an output item");
        };
        assert_eq!(item, original);
        assert!(valid_for_retention);
        assert_eq!(assistant_text, None);
        let invalid = runtime_json::parse(&RuntimeString::from(
            r#"{"type":"function_call","name":"write","arguments":"{}","call_id":"call"}"#,
        ))
        .unwrap();
        let scoring::SamplingEvent::OutputItem {
            valid_for_retention,
            ..
        } = output_item(invalid)
        else {
            panic!("A native output item must remain an output item");
        };
        assert!(!valid_for_retention);
    }

    #[test]
    fn quota_failures_with_rate_limit_status_are_terminal() {
        let error = crate::types::errors::ProviderError {
            kind: ProviderErrorKind::Quota,
            message: "Quota exhausted".into(),
            status: Some(429),
            code: Some("insufficient_quota".into()),
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms: None,
            request_id: None,
        };
        assert!(!classifier_error_retryable(&error));
        assert!(classifier_error_retryable(
            &crate::types::errors::ProviderError {
                kind: ProviderErrorKind::RateLimit,
                code: Some("rate_limit_exceeded".into()),
                ..error
            }
        ));
    }
    #[tokio::test]
    async fn authentication_recovery_has_its_own_finite_episode_without_sampling_retries() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Recovery {
            remaining: usize,
            succeeds: bool,
            actions: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ClassifierAuthRecovery for Recovery {
            async fn next(&mut self) -> bool {
                if self.remaining == 0 {
                    return false;
                }
                self.remaining -= 1;
                self.actions.fetch_add(1, Ordering::SeqCst);
                if !self.succeeds {
                    self.remaining = 0;
                }
                self.succeeds
            }
        }

        let auth_error = crate::types::errors::ProviderError {
            kind: ProviderErrorKind::Auth,
            message: "Unauthorized".into(),
            status: Some(401),
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms: None,
            request_id: None,
        };
        let actions = Arc::new(AtomicUsize::new(0));
        let mut recovery: Option<Box<dyn ClassifierAuthRecovery>> = Some(Box::new(Recovery {
            remaining: 2,
            succeeds: true,
            actions: actions.clone(),
        }));
        let mut retries = 0;
        for _ in 0..2 {
            assert!(retry_classifier_after_failure(&auth_error, &mut recovery, &mut retries).await);
        }
        assert!(!retry_classifier_after_failure(&auth_error, &mut recovery, &mut retries).await);
        assert_eq!(actions.load(Ordering::SeqCst), 2);
        assert_eq!(retries, 0);

        let network_error = crate::types::errors::ProviderError {
            kind: ProviderErrorKind::Network,
            status: None,
            ..auth_error.clone()
        };
        for _ in 0..2 {
            assert!(
                retry_classifier_after_failure(&network_error, &mut recovery, &mut retries).await
            );
        }
        assert!(!retry_classifier_after_failure(&network_error, &mut recovery, &mut retries).await);
        assert_eq!(retries, 2);

        let mut failed: Option<Box<dyn ClassifierAuthRecovery>> = Some(Box::new(Recovery {
            remaining: 2,
            succeeds: false,
            actions: actions.clone(),
        }));
        assert!(!retry_classifier_after_failure(&auth_error, &mut failed, &mut retries).await);
        assert_eq!(actions.load(Ordering::SeqCst), 3);
        assert_eq!(retries, 2);

        let quota = crate::types::errors::ProviderError {
            kind: ProviderErrorKind::Quota,
            code: Some("insufficient_quota".into()),
            ..auth_error
        };
        assert!(!retry_classifier_after_failure(&quota, &mut recovery, &mut retries).await);
        assert_eq!(actions.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn async_policy_metadata_fallback_distinguishes_an_explicit_missing_override() {
        let parent_messages = runtime_json::parse(&RuntimeString::from(
            r#"{"auto_review":{"policy":"parent-policy"}}"#,
        ))
        .unwrap();
        let preferred_messages = runtime_json::parse(&RuntimeString::from(
            r#"{"auto_review":{"policy":"preferred-policy"}}"#,
        ))
        .unwrap();
        let parent = ModelCapabilities {
            model_messages: Some(parent_messages),
            ..Default::default()
        };
        let preferred = RuntimeString::from("review-model");
        assert_eq!(
            async_model_policy_messages(&parent, &[], &preferred)
                .unwrap()
                .policy
                .as_deref(),
            Some("parent-policy")
        );
        let catalog = vec![(
            preferred.clone(),
            ModelCapabilities {
                model_messages: Some(preferred_messages),
                ..Default::default()
            },
        )];
        assert_eq!(
            async_model_policy_messages(&parent, &catalog, &preferred)
                .unwrap()
                .policy
                .as_deref(),
            Some("preferred-policy")
        );
        let explicit_missing = ModelCapabilities {
            auto_review_model_override: Some("missing-review-model".into()),
            ..parent
        };
        assert_eq!(
            async_model_policy_messages(&explicit_missing, &catalog, &preferred)
                .unwrap()
                .policy,
            None
        );
    }
    #[tokio::test]
    async fn expired_observation_wins_a_simultaneously_ready_retained_label() {
        let progress = Arc::new(scoring::ScoreProgress::<ReviewAuthorization>::default());
        let action = |call_id| scoring::ToolObservation {
            call_id,
            originating_item_id: None,
            origin: scoring::ToolOrigin::Direct,
            is_code_wrapper: false,
        };
        let index = progress.observe(action("classified"));
        let pending = scoring::PendingScore {
            progress: progress.clone(),
            index,
            max_lag: 0,
        };
        progress.observe(action("newer"));
        let (ready, score) = oneshot::channel();
        ready.send(Ok("low".to_owned())).unwrap();
        assert_eq!(
            pending_classification_result(score, pending, AbortSignal::new(), AbortSignal::new())
                .await,
            Err(scoring::ClassifierError::Superseded)
        );
    }
    #[test]
    fn persisted_classification_keeps_structured_action_scores_and_sample_precision() {
        let action = RuntimeString::from(r#"{"tool":"registered","arguments":{"path":"file"}}"#);
        let time = SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(1_000_000_001);
        let record = classification_record(&"call".into(), &action, time, "low").unwrap();
        assert_eq!(
            record.get("action"),
            Some(&runtime_json::parse(&action).unwrap())
        );
        assert_eq!(
            record
                .get("scores")
                .and_then(|scores| scores.get("action_risk")),
            Some(&RuntimeJson::from(0.0))
        );
        assert_eq!(
            record.get("sampledAt").and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("1970-01-01T00:00:01.000000001Z"))
        );
    }
}
