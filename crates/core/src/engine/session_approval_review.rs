//! Stateful, isolated permission assessments with captured authorization and tool evidence.

use super::session_approval_query_part::{
    ApprovalQueryInput, ApprovalQueryOutcome, ApprovalQueryState,
};
use super::{InferenceBundle, RuntimePolicyState, Session};
use crate::engine::approval_context as context;
use crate::engine::approval_context::{
    ContextProfile, HistoryTruncation, PlannedAction, PlannedActionKind, RequestBudget,
    TranscriptCursor, TranscriptMode,
};
use crate::engine::approval_review_support as review;
use crate::engine::approval_review_support::{ReviewerRequest, ReviewerSession};
use crate::permission::permission_update::SessionPermissionState;
use crate::provider::review_target::{resolve_approval_review_target, ResolvedReviewTarget};
use crate::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use crate::schemas::message::Message;
use crate::sync_ext::{MutexExt, RwLockExt};
use crate::types::abort::AbortSignal;
use crate::types::approval_review::*;
use crate::types::events::EngineEvent;
use crate::types::permission::{
    ApprovalActionMetadata, ApprovalRequest, ApprovalReviewContext, ApprovalReviewResponse,
    ApprovalReviewer, ApprovalsReviewer,
};
use crate::types::provider::SystemPromptPart;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use tokio::time::Instant;

#[derive(Clone, PartialEq)]
pub(crate) struct ReviewAuthorization {
    pub policy: RuntimePolicyState,
    pub rules: SessionPermissionState,
    pub model: RuntimeString,
    pub config: RuntimeString,
    pub user_context: RuntimeString,
    pub manual_revision: u64,
    pub history_generation: u64,
    pub account: Option<String>,
    pub inference_revision: u64,
    pub mcp_revision: u64,
}

pub(crate) struct ReviewEvidenceSnapshot {
    pub history: context::MessageHistory,
    pub root_conversation: Vec<context::RootMessage>,
    pub trusted_user_answers: Vec<String>,
    pub permissions: context::PermissionContext,
    pub previous_reviews: Option<context::PreviousReviews>,
    pub trusted_tool: Option<context::TrustedTool>,
    pub trusted_skill_paths: Vec<String>,
    pub history_generation: u64,
    pub context_mode: crate::engine::approval_async::ContextMode,
    pub retained_context_complete: bool,
}

pub(super) struct ApprovalReviewRuntime {
    pub tasks: Arc<review::ReviewerTasks>,
    pool: review::ReviewerPool<ReviewConversation>,
    pub history: Mutex<crate::engine::approval_capture::ReviewParentHistory>,
    pub(super) events: Mutex<Vec<ApprovalReviewEvent>>,
    pub(super) breaker: Mutex<review::DenialCircuitBreaker>,
    manual_revision: AtomicU64,
    assistant_orders: Mutex<std::collections::HashMap<RuntimeString, u64>>,
    completed_evidence: Mutex<Vec<CompletedReviewEvidence>>,
    skills: Mutex<
        std::collections::HashMap<
            Option<RuntimeString>,
            crate::engine::approval_skill_evidence::InvokedSkillEvidence,
        >,
    >,
}

pub(crate) struct InvokedSkillScope {
    runtime: Weak<ApprovalReviewRuntime>,
    agent_id: RuntimeString,
}
impl Drop for InvokedSkillScope {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime
                .skills
                .lock_recover()
                .remove(&Some(self.agent_id.clone()));
        }
    }
}

struct CompletedReviewEvidence {
    authorization: ReviewAuthorization,
    completed_at_ms: i64,
    review: context::PreviousReview,
}

struct ReviewSetup {
    parent: Weak<Session>,
    target: ResolvedReviewTarget,
    capture: ApprovalReviewContext,
    accounting: Arc<InferenceBundle>,
    system: Vec<SystemPromptPart>,
    policy: context::ResolvedReviewPolicy,
    cancellation: AbortSignal,
}

#[derive(Clone, Default)]
struct ReviewCheckpoint {
    history: Vec<Message>,
    delivery: context::ReviewMessageDelivery,
}

struct ReviewConversation {
    context: RuntimeString,
    state: Arc<ApprovalQueryState>,
    conversation: Mutex<review::ConversationState<ReviewCheckpoint, TranscriptCursor>>,
    history: Mutex<Vec<Message>>,
    delivery: Mutex<context::ReviewMessageDelivery>,
    cancellation: AbortSignal,
}

impl ReviewerSession for ReviewConversation {
    type Setup = ReviewSetup;
    type Context = RuntimeString;
    type Snapshot = review::ConversationCheckpoint<ReviewCheckpoint, TranscriptCursor>;
    fn context(&self) -> &RuntimeString {
        &self.context
    }
    async fn snapshot(&self) -> Option<Self::Snapshot> {
        self.conversation.lock_recover().snapshot().cloned()
    }
}

impl ApprovalReviewRuntime {
    pub(super) fn new(
        checkpoint: Option<&crate::engine::approval_capture::ReviewParentCheckpoint>,
        initial_reviews: Option<Vec<ApprovalReviewEvent>>,
    ) -> Arc<Self> {
        let tasks = Arc::new(review::ReviewerTasks::default());
        let owner = tasks.clone();
        let pool = review::ReviewerPool::new(
            tasks.clone(),
            move |setup: Arc<ReviewSetup>, key, _, snapshot, cancellation| {
                let owner = owner.clone();
                Box::pin(async move {
                    let parent = setup
                        .parent
                        .upgrade()
                        .ok_or_else(|| "The parent conversation closed.".to_string())?;
                    if cancellation.is_cancelled()
                        || setup.cancellation.is_cancelled()
                        || parent.is_disposed()
                    {
                        return Err("Approval review was cancelled before startup.".into());
                    }
                    let state = ApprovalQueryState::new(
                        &parent,
                        &setup.capture.config,
                        &setup.target.metadata,
                    )
                    .map_err(|error| error.message.to_node_utf8_string())?;
                    let (conversation, history) = match snapshot {
                        Some(snapshot) => review::ConversationState::fork(snapshot),
                        None => (
                            review::ConversationState::default(),
                            ReviewCheckpoint::default(),
                        ),
                    };
                    let worker_state = state.clone();
                    let worker_cancellation = cancellation.clone();
                    owner.tasks.spawn(async move {
                    worker_cancellation.cancelled().await;
                    if let Err(error) = worker_state.shutdown().await {
                        tracing::warn!(error = %error.to_node_utf8_string(), "Approval review resources did not close cleanly");
                    }
                });
                    Ok(ReviewConversation {
                        context: key,
                        state,
                        conversation: Mutex::new(conversation),
                        history: Mutex::new(history.history),
                        delivery: Mutex::new(history.delivery),
                        cancellation,
                    })
                })
            },
        );
        Arc::new(Self {
            tasks,
            pool,
            history: Mutex::new(
                checkpoint
                    .map(crate::engine::approval_capture::ReviewParentHistory::restore)
                    .unwrap_or_default(),
            ),
            assistant_orders: Mutex::new(Default::default()),
            completed_evidence: Mutex::new(Vec::new()),
            skills: Mutex::new(Default::default()),
            events: Mutex::new(initial_reviews.unwrap_or_default()),
            breaker: Mutex::new(Default::default()),
            manual_revision: AtomicU64::new(0),
        })
    }
}

struct ModelApprovalReviewer {
    session: Weak<Session>,
    bundle: Arc<InferenceBundle>,
    safety_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    capture: RwLock<Option<ApprovalReviewContext>>,
    metadata: Mutex<std::collections::HashMap<RuntimeString, ApprovalActionMetadata>>,
}

impl Session {
    pub(crate) fn record_invoked_skill(
        &self,
        agent_id: Option<RuntimeString>,
        turn_id: &RuntimeString,
        path: &RuntimeString,
    ) {
        let home = self
            .user_config_dir
            .as_ref()
            .map(|path| std::path::PathBuf::from(path.as_runtime_string().to_node_utf8_string()))
            .unwrap_or_else(|| std::path::PathBuf::from(crate::fs::paths::config_home()));
        self.approval_review
            .skills
            .lock_recover()
            .entry(agent_id)
            .or_default()
            .record(
                &turn_id.to_node_utf8_string(),
                &path.to_node_utf8_string(),
                &home,
                std::env::home_dir().as_deref(),
            );
    }

    pub(crate) fn invoked_skill_scope(&self, agent_id: RuntimeString) -> InvokedSkillScope {
        InvokedSkillScope {
            runtime: Arc::downgrade(&self.approval_review),
            agent_id,
        }
    }

    pub(crate) fn approval_reviewer_for_turn(
        self: &Arc<Self>,
        bundle: Arc<InferenceBundle>,
        safety_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    ) -> Arc<dyn ApprovalReviewer> {
        Arc::new(ModelApprovalReviewer {
            session: Arc::downgrade(self),
            bundle,
            safety_reviewer,
            capture: RwLock::new(None),
            metadata: Mutex::new(Default::default()),
        })
    }

    pub(crate) async fn capture_review_message(&self, message: &Message, turn_id: &RuntimeString) {
        let rollout = self.persistence.rollout();
        let prepared = match rollout {
            Some(rollout) => match rollout.prepare_append().await {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    tracing::warn!(%error, "Review evidence persistence admission is closed");
                    return;
                }
            },
            None => None,
        };
        if let Some(record) = self.capture_review_message_record(message, turn_id) {
            if let Some(prepared) = prepared {
                prepared.commit(record);
            }
        }
    }

    /// Build evidence inside an already owned admission operation. Callers must
    /// retain the returned record before exposing a cancellable operation.
    pub(crate) fn capture_review_message_record(
        &self,
        message: &Message,
        turn_id: &RuntimeString,
    ) -> Option<crate::schemas::rollout::RolloutItem> {
        let id = match message {
            Message::User(message) => Some(&message.uuid),
            Message::Assistant(message) => Some(&message.uuid),
            Message::ToolResult(message) => Some(&message.uuid),
            _ => None,
        };
        let order = id.and_then(|id| {
            self.approval_review
                .assistant_orders
                .lock_recover()
                .remove(id)
        });
        // Writer capacity is owned before entering the synchronous history
        // mutation. Checkpoints enter the same admission lane, so neither can
        // overtake the other's committed evidence.
        let result = {
            let mut history = self.approval_review.history.lock_recover();
            history
                .admit(message, turn_id, order)
                .and_then(|admission| admission.map(|item| item.to_runtime_value()).transpose())
        };
        match result {
            Ok(Some(payload)) => Some(crate::schemas::rollout::RolloutItem::Event(
                crate::schemas::rollout::EventItem {
                    event: crate::engine::approval_capture::APPROVAL_MESSAGE_ADMITTED_EVENT.into(),
                    turn_id: Some(turn_id.clone()),
                    payload: Some(payload),
                },
            )),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(error=%error.to_node_utf8_string(), "Original review evidence could not be captured");
                None
            }
        }
    }

    pub(crate) async fn shutdown_approval_review(&self) {
        self.approval_review.pool.shutdown().await;
    }

    pub fn approval_review_history(&self) -> Vec<ApprovalReviewEvent> {
        self.approval_review.events.lock_recover().clone()
    }
    pub fn list_approval_review_denials(&self) -> Vec<ApprovalReviewEvent> {
        self.approval_review
            .events
            .lock_recover()
            .iter()
            .rev()
            .filter(|event| event.status == ApprovalReviewStatus::Denied)
            .take(10)
            .cloned()
            .collect()
    }

    pub async fn approve_approval_review_denial(
        &self,
        event: ApprovalReviewEvent,
    ) -> Result<(), RuntimeString> {
        if self.is_disposed() {
            return Err("The conversation is closing.".into());
        }
        if event.status != ApprovalReviewStatus::Denied {
            tracing::warn!("Only a denied review can receive explicit action approval");
            return Ok(());
        }
        let action = runtime_codec::encode_value(&event.action)
            .map_err(|_| RuntimeString::from("Cannot encode the denied action."))?;
        let action: serde_json::Value = serde_json::from_slice(
            &crate::runtime_values::runtime_json::stringify(&action).to_node_utf8(),
        )
        .map_err(|error| RuntimeString::from(error.to_string()))?;
        let mut preview = crate::engine::approval_capture::ReviewParentHistory::default();
        let item = preview.record_manual_approval(&action, &event.turn_id.to_node_utf8_string())?;
        let text = match item.item {
            context::model::ResponseItem::Message { content, .. } => content
                .into_iter()
                .filter_map(|item| match item {
                    context::model::ContentItem::InputText { text } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
            _ => return Err("Cannot prepare explicit action approval.".into()),
        };
        self.conversation
            .queue
            .enqueue_durable(crate::engine::message_queue::QueuedMessage {
                value: crate::engine::message_queue::QueuedValue::Text(text.into()),
                mode: "approval-context".into(),
                priority: Some(crate::engine::message_queue::QueuePriority::Now),
                uuid: Some((self.deps.uuid)().into()),
                session_id: Some(self.id.clone()),
                is_meta: Some(true),
                origin: Some("approval-review".into()),
                agent_id: None,
                trigger_turn: Some(false),
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: Some(crate::engine::message_queue::SteerDelivery::User),
                display_metadata: None,
            })
            .await?;
        let prepared = match self.persistence.rollout() {
            Some(rollout) => Some(
                rollout
                    .prepare_append()
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        let payload = {
            let mut history = self.approval_review.history.lock_recover();
            let envelope =
                history.record_manual_approval(&action, &event.turn_id.to_node_utf8_string())?;
            crate::engine::approval_capture::manual_action_payload(&envelope)?
        };
        if let Some(prepared) = prepared {
            prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                crate::schemas::rollout::EventItem {
                    event: crate::engine::approval_capture::APPROVAL_MANUAL_ACTION_EVENT.into(),
                    turn_id: Some(event.turn_id.clone()),
                    payload: Some(payload),
                },
            ));
        }
        self.approval_review
            .manual_revision
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn publish_approval_review(
        &self,
        capture: &ApprovalReviewContext,
        event: ApprovalReviewEvent,
    ) {
        let rollout = self.persistence.rollout();
        let prepared = match rollout {
            Some(rollout) => match rollout.prepare_append().await {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    tracing::warn!(%error, "Review result persistence admission is closed");
                    return;
                }
            },
            None => None,
        };
        {
            let mut events = self.approval_review.events.lock_recover();
            if let Some(previous) = events.iter_mut().find(|previous| previous.id == event.id) {
                *previous = event.clone();
            } else {
                events.push(event.clone());
            }
        }
        let options = crate::store::session_store::AppendEventOptions {
            turn_id: Some(event.turn_id.clone()),
            payload: runtime_codec::encode_value(&event).ok(),
        };
        if let Some(prepared) = prepared {
            prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                crate::schemas::rollout::EventItem {
                    event: "approval_review".into(),
                    turn_id: options.turn_id,
                    payload: options.payload,
                },
            ));
        }
        (capture.emit)(EngineEvent::ApprovalReview { review: event });
    }

    pub(crate) async fn persist_review_checkpoint(&self) {
        if let Some(rollout) = self.persistence.rollout() {
            let prepared = match rollout.prepare_append().await {
                Ok(prepared) => prepared,
                Err(error) => {
                    tracing::warn!(%error, "Review checkpoint persistence admission is closed");
                    return;
                }
            };
            if let Some(payload) = self.review_parent_checkpoint_payload() {
                let event = crate::engine::approval_capture::APPROVAL_PARENT_HISTORY_EVENT.into();
                prepared.commit(crate::schemas::rollout::RolloutItem::Event(
                    crate::schemas::rollout::EventItem {
                        event,
                        turn_id: None,
                        payload: Some(payload),
                    },
                ));
            }
        }
    }

    pub(crate) fn review_parent_checkpoint_payload(&self) -> Option<RuntimeJson> {
        let checkpoint = self.approval_review.history.lock_recover().checkpoint();
        match checkpoint.to_runtime_value() {
            Ok(payload) => Some(payload),
            Err(error) => {
                tracing::warn!(error = %error.to_node_utf8_string(), "Review checkpoint could not be encoded");
                None
            }
        }
    }

    /// Enqueue the complete checkpoint before another admission can enter its suffix.
    pub(crate) async fn persist_review_compaction(
        &self,
        mut record: crate::store::session_store::CompactionRecord,
    ) {
        if let Some(rollout) = self.persistence.rollout() {
            let prepared = match rollout.prepare_append().await {
                Ok(prepared) => prepared,
                Err(error) => {
                    tracing::warn!(%error, "Review checkpoint persistence admission is closed");
                    return;
                }
            };
            record.approval_parent_history = match self
                .approval_review
                .history
                .lock_recover()
                .checkpoint()
                .to_runtime_value()
            {
                Ok(payload) => Some(payload),
                Err(error) => {
                    tracing::warn!(error = %error.to_node_utf8_string(), "Review compaction checkpoint could not be encoded");
                    None
                }
            };
            prepared.commit(crate::schemas::rollout::RolloutItem::Compacted(
                crate::schemas::rollout::CompactedItem {
                    summary: record.summary,
                    replaced_range: record.replaced_range,
                    replacement_history: record.replacement_history,
                    source_message_ids: record.source_message_ids,
                    approval_parent_history: record.approval_parent_history,
                },
            ));
        }
    }

    pub(crate) fn review_evidence_snapshot(
        &self,
        capture: &ApprovalReviewContext,
    ) -> Result<ReviewEvidenceSnapshot, RuntimeString> {
        let root = self.approval_review.history.lock_recover().snapshot();
        root.ensure_representable()?;
        let child = capture.agent_id.is_some();
        let snapshot = if child {
            let mut state = crate::engine::approval_capture::ReviewParentHistory::default();
            state.ingest(&capture.history, &capture.turn_id.to_node_utf8_string())?;
            state.snapshot()
        } else {
            let mut state = self.approval_review.history.lock_recover();
            state.ingest(&capture.history, &capture.turn_id.to_node_utf8_string())?;
            state.snapshot()
        };
        snapshot.ensure_representable()?;
        let history = context::MessageHistory {
            items: snapshot.items,
            retained: snapshot.retained,
        };
        let mut retained_context_complete = history.retained.user_messages_complete()
            && history.retained.verified_answers_complete();
        let root_projection = context::root_conversation_snapshot(&context::MessageHistory {
            items: root.items,
            retained: root.retained.clone(),
        });
        let root_conversation = if child {
            retained_context_complete &= root_projection.retained_context_complete;
            root_projection.messages
        } else {
            Vec::new()
        };
        let verified = context::render_verified_answers(if child {
            &root.retained
        } else {
            &history.retained
        });
        retained_context_complete &= verified.complete;
        let trusted_user_answers = verified.fragments;
        let permissions = context::PermissionContext {
            environment_id: Some(self.id.to_node_utf8_string()),
            denied_paths: Vec::new(),
            denied_globs: Vec::new(),
        };
        let authorization = self.approval_authorization(capture);
        let previous_reviews = {
            let records = self.approval_review.completed_evidence.lock_recover();
            let reviews = records
                .iter()
                .filter(|record| record.authorization == authorization)
                .map(|record| record.review.clone())
                .collect::<Vec<_>>();
            if reviews.is_empty() {
                None
            } else {
                Some(
                    context::PreviousReviews::try_from_fragments(reviews)
                        .map_err(|error| RuntimeString::from(error.to_string()))?,
                )
            }
        };
        Ok(ReviewEvidenceSnapshot {
            history,
            root_conversation,
            trusted_user_answers,
            permissions,
            previous_reviews,
            trusted_tool: None,
            trusted_skill_paths: {
                let skills = self.approval_review.skills.lock_recover();
                let local = skills
                    .get(&capture.agent_id)
                    .map(|evidence| evidence.paths_for_turn(&capture.turn_id.to_node_utf8_string()))
                    .unwrap_or_default();
                let root = if child {
                    root_projection
                        .latest_user_turn_id
                        .as_deref()
                        .and_then(|turn| {
                            skills
                                .get(&None)
                                .map(|evidence| evidence.paths_for_turn(turn))
                        })
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                crate::engine::approval_skill_evidence::merge_invoked_skill_paths(&local, &root)
            },
            history_generation: snapshot.generation,
            context_mode: crate::engine::approval_async::ContextMode::Independent,
            retained_context_complete,
        })
    }

    pub(crate) fn approval_authorization(
        &self,
        capture: &ApprovalReviewContext,
    ) -> ReviewAuthorization {
        let mut source = RuntimeObject::new();
        source.insert(
            "system",
            runtime_codec::encode_value(&capture.canonical_system).unwrap_or(RuntimeJson::Null),
        );
        let snapshot = self.approval_review.history.lock_recover().snapshot();
        let authorities = snapshot.items.iter().filter(|item| matches!(&item.item, context::model::ResponseItem::Message { role, .. } if role == "user" || role == "developer")).collect::<Vec<_>>();
        let authorities = serde_json::to_vec(&authorities).unwrap_or_default();
        source.insert("users", RuntimeJson::String(digest(&authorities).into()));
        source.insert(
            "revision",
            RuntimeJson::from(snapshot.user_message_revision),
        );
        let config = self.inference_config();
        ReviewAuthorization {
            policy: self.runtime_policy_state(),
            rules: self.permission_engine.rule_snapshot(),
            model: capture.request.model.clone(),
            config: {
                let accounting = capture
                    .accounting
                    .clone()
                    .unwrap_or_else(|| self.inference_snapshot());
                let requirements = accounting.load_review_requirements_sync();
                digest(format!("{:?}|{:?}", config, requirements).as_bytes()).into()
            },
            user_context: digest(
                &crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(source))
                    .to_node_utf8(),
            )
            .into(),
            manual_revision: self.approval_review.manual_revision.load(Ordering::Acquire),
            history_generation: self
                .approval_review
                .history
                .lock_recover()
                .snapshot()
                .generation,
            inference_revision: self.inference_snapshot().revision,
            mcp_revision: self.mcp_registration_revision(),
            account: capture
                .provider
                .as_ref()
                .and_then(|provider| provider.credential_account_id()),
        }
    }

    async fn make_review_setup(
        self: &Arc<Self>,
        capture: ApprovalReviewContext,
        accounting: Arc<InferenceBundle>,
        signal: AbortSignal,
    ) -> Result<(Arc<ReviewSetup>, RuntimeString), review::ReviewError> {
        let requirements = accounting
            .load_review_requirements()
            .await
            .map_err(review::ReviewError::prompt_build)?;
        let target = resolve_approval_review_target(
            capture
                .provider
                .clone()
                .unwrap_or_else(|| accounting.provider.clone()),
            &capture.request.model,
            capture.request.effort.clone(),
        );
        let messages = target
            .metadata
            .model_messages
            .as_ref()
            .and_then(|messages| messages.get("auto_review"));
        let message = |key: &str| {
            messages
                .and_then(|messages| messages.get(key))
                .and_then(RuntimeJson::as_str)
                .map(NodeText::to_node_utf8_string)
        };
        let model_messages = context::ModelReviewMessages {
            policy: message("policy"),
            policy_template: message("policy_template"),
            node_repl_policy: message("node_repl_policy"),
            rejection_instructions: message("rejection_instructions"),
            timeout_instructions: message("timeout_instructions"),
        };
        let local = context::ReviewPolicyOptions {
            policy: capture
                .config
                .approval_review_policy
                .as_ref()
                .map(NodeText::to_node_utf8_string),
            extra_policy: capture
                .config
                .approval_review_extra_policy
                .as_ref()
                .map(NodeText::to_node_utf8_string),
            policy_template: capture
                .config
                .approval_review_policy_template
                .as_ref()
                .map(NodeText::to_node_utf8_string),
            transcript_mode: capture
                .config
                .approval_review_transcript_mode
                .or_else(|| {
                    capture
                        .config
                        .approval_review
                        .as_ref()
                        .and_then(crate::engine::approval_async::AsyncFeatureConfig::local_config)
                        .and_then(|config| config.transcript_mode)
                })
                .map(|format| match format {
                    crate::engine::approval_async::TranscriptFormat::Line => {
                        context::TranscriptFormat::Line
                    }
                    crate::engine::approval_async::TranscriptFormat::Json => {
                        context::TranscriptFormat::Json
                    }
                }),
        };
        let parent_metadata = capture
            .provider
            .as_ref()
            .unwrap_or(&accounting.provider)
            .model_metadata(&capture.request.model);
        let policy_mode = parent_metadata
            .guardian
            .as_ref()
            .and_then(|policy| policy.get("computer_use"))
            .and_then(RuntimeJson::as_str)
            .map(|mode| match mode.to_node_utf8_string().as_str() {
                "disabled" => context::ComputerUseReviewMode::Disabled,
                "synchronous" => context::ComputerUseReviewMode::Synchronous,
                "adaptive" => context::ComputerUseReviewMode::Adaptive,
                _ => context::ComputerUseReviewMode::Unknown,
            });
        let eligibility = context::ComputerUseReviewEligibility {
            catalog_policy_present: parent_metadata.guardian.is_some(),
            computer_use: policy_mode,
            legacy_required: parent_metadata.node_repl_auto_review_required,
        };
        let policy = context::ResolvedReviewPolicy::resolve(
            &local,
            &model_messages,
            &requirements.policy,
            eligibility,
        );
        let text = policy.instructions(review::output_contract_prompt());
        let mut system = vec![SystemPromptPart {
            name: "approval-review-policy".into(),
            text: text.into(),
            cacheable: Some(true),
            cache_scope: None,
        }];
        system.extend(
            capture
                .canonical_system
                .iter()
                .filter(|part| {
                    matches!(
                        part.name.to_node_utf8_string().as_str(),
                        "developer" | "user" | "thread" | "project" | "env" | "personality"
                    ) || (part.name == "memory" && part.cacheable == Some(true))
                })
                .cloned(),
        );
        let authorization = self.approval_authorization(&capture);
        let mut key = RuntimeObject::new();
        key.insert("model", RuntimeJson::String(target.model.clone()));
        key.insert(
            "agent",
            capture
                .agent_id
                .clone()
                .map(RuntimeJson::String)
                .unwrap_or(RuntimeJson::Null),
        );
        key.insert(
            "system",
            runtime_codec::encode_value(&system).unwrap_or(RuntimeJson::Null),
        );
        key.insert(
            "policyGeneration",
            RuntimeJson::from(authorization.policy.generation),
        );
        key.insert("config", RuntimeJson::String(authorization.config));
        key.insert(
            "rules",
            RuntimeJson::String(format!("{:?}", authorization.rules).into()),
        );
        key.insert(
            "provider",
            RuntimeJson::String(format!("{:?}", target.provider.info()).into()),
        );
        key.insert(
            "cwd",
            RuntimeJson::String(self.cwd_now().into_runtime_string()),
        );
        key.insert(
            "historyGeneration",
            RuntimeJson::from(authorization.history_generation),
        );
        key.insert(
            "metadata",
            runtime_codec::encode_value(&target.metadata).unwrap_or(RuntimeJson::Null),
        );
        key.insert(
            "account",
            authorization
                .account
                .map(RuntimeJson::from)
                .unwrap_or(RuntimeJson::Null),
        );
        let key = digest(
            &crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(key))
                .to_node_utf8(),
        )
        .into();
        Ok((
            Arc::new(ReviewSetup {
                parent: Arc::downgrade(self),
                target,
                capture,
                accounting,
                system,
                policy,
                cancellation: signal,
            }),
            key,
        ))
    }
}

fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[async_trait::async_trait]
impl ApprovalReviewer for ModelApprovalReviewer {
    fn is_automatic(&self) -> bool {
        true
    }
    fn ignore_allow_prefix_rules(&self) -> bool {
        let capture = self.capture.read_recover();
        let model = capture
            .as_ref()
            .map(|capture| &capture.request.model)
            .unwrap_or(&self.bundle.config.model);
        let provider = capture
            .as_ref()
            .and_then(|capture| capture.provider.as_ref())
            .unwrap_or(&self.bundle.provider);
        provider
            .model_metadata(model)
            .model_specialty
            .as_ref()
            .is_some_and(|specialty| specialty == "cyber")
            || capture
                .as_ref()
                .and_then(|capture| capture.accounting.as_ref())
                .unwrap_or(&self.bundle)
                .load_review_requirements_sync()
                .map(|requirements| {
                    requirements
                        .ignore_rules
                        .as_ref()
                        .is_some_and(|models| models.contains(&model.to_node_utf8_string()))
                })
                .unwrap_or(true)
    }
    fn requires_review(&self) -> bool {
        let capture = self.capture.read_recover();
        let model = capture
            .as_ref()
            .map(|capture| &capture.request.model)
            .unwrap_or(&self.bundle.config.model);
        capture
            .as_ref()
            .and_then(|capture| capture.accounting.as_ref())
            .unwrap_or(&self.bundle)
            .load_review_requirements_sync()
            .map(|requirements| requirements.review_required(&model.to_node_utf8_string()))
            .unwrap_or(true)
    }
    fn observe_context(&self, mut capture: ApprovalReviewContext) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        if capture.provider.is_none() {
            capture.provider = Some(self.bundle.provider.clone());
        }
        if capture.accounting.is_none() {
            capture.accounting = Some(self.bundle.clone());
        }
        let _ = session.review_evidence_snapshot(&capture);
        session.prewarm_approval_async(&capture);
        *self.capture.write_recover() = Some(capture.clone());
        if session.runtime_policy_state().approvals_reviewer == Some(ApprovalsReviewer::Automatic)
            || self.requires_review()
        {
            let owner = session.approval_review.clone();
            let accounting = capture
                .accounting
                .clone()
                .unwrap_or_else(|| self.bundle.clone());
            owner.tasks.tasks.spawn({
                let owner = owner.clone();
                async move {
                    let signal = owner.tasks.cancellation.child_token();
                    if let Ok((setup, key)) =
                        session.make_review_setup(capture, accounting, signal).await
                    {
                        if owner.pool.trunk().await.is_none() {
                            let _ = owner.pool.prewarm(setup, key).await;
                        }
                    }
                }
            });
        }
    }
    fn assistant_started(&self, id: &RuntimeString) {
        if let Some(session) = self.session.upgrade() {
            let order = session
                .approval_review
                .history
                .lock_recover()
                .reserve_order();
            session
                .approval_review
                .assistant_orders
                .lock_recover()
                .insert(id.clone(), order);
        }
    }
    fn observe_action_with_metadata(
        &self,
        request: ApprovalRequest,
        metadata: ApprovalActionMetadata,
    ) {
        self.set_action_metadata(&request.id, metadata.clone());
        if let (Some(session), Some(capture)) =
            (self.session.upgrade(), self.capture.read_recover().clone())
        {
            session.observe_approval_action(capture, request, metadata);
        }
    }
    fn set_action_metadata(&self, id: &RuntimeString, mut metadata: ApprovalActionMetadata) {
        let mut actions = self.metadata.lock_recover();
        if let Some(previous) = actions.get(id) {
            metadata.call_source = previous.call_source.clone();
            if metadata.originating_item_id.is_none() {
                metadata.originating_item_id = previous.originating_item_id.clone();
            }
            if metadata.network_trigger.is_none() {
                metadata.network_trigger = previous.network_trigger.clone();
            }
        }
        actions.insert(id.clone(), metadata);
    }
    fn finish_action(&self, id: &RuntimeString) {
        self.metadata.lock_recover().remove(id);
        if let Some(session) = self.session.upgrade() {
            session.finish_approval_action(id);
        }
    }
    async fn review_for_user(
        &self,
        request: ApprovalRequest,
        signal: AbortSignal,
    ) -> ApprovalReviewResponse {
        match &self.safety_reviewer {
            Some(reviewer) => reviewer.review(request, signal).await,
            None => ApprovalReviewResponse::Allow,
        }
    }
    async fn review(
        &self,
        request: ApprovalRequest,
        signal: AbortSignal,
    ) -> ApprovalReviewResponse {
        let Some(session) = self.session.upgrade() else {
            return deny("The conversation closed before approval review completed.");
        };
        if !self.requires_review()
            && session.runtime_policy_state().approvals_reviewer
                != Some(ApprovalsReviewer::Automatic)
        {
            return ApprovalReviewResponse::Allow;
        }
        let Some(capture) = self.capture.read_recover().clone() else {
            return deny("The original action context is unavailable; automatic approval review could not be completed.");
        };
        let Some(metadata) = self.metadata.lock_recover().get(&request.id).cloned() else {
            return deny("The registered action metadata is unavailable; automatic approval review could not be completed.");
        };
        let accounting = capture
            .accounting
            .clone()
            .unwrap_or_else(|| self.bundle.clone());
        let required = self.requires_review();
        let task = {
            let admission = session.inference_admission.lock_recover();
            if admission.closed {
                return deny("The conversation closed before approval review started.");
            }
            let owner = session.clone();
            // An assessment owns its terminal receipt even when the caller's
            // tool cancellation grace ends before private-query cleanup.
            session.approval_review.tasks.tasks.spawn(async move {
                owner
                    .assess_approval(accounting, capture, request, metadata, signal, required)
                    .await
            })
        };
        task.await.unwrap_or_else(|_| {
            deny("Approval review ended unexpectedly; the action was not executed.")
        })
    }
}

fn deny(message: impl Into<RuntimeString>) -> ApprovalReviewResponse {
    ApprovalReviewResponse::Deny {
        message: Some(message.into()),
    }
}

struct ReviewAttempt {
    parent: Arc<Session>,
    setup: Arc<ReviewSetup>,
    key: RuntimeString,
    request: ApprovalRequest,
    action: RuntimeJson,
    action_kind: crate::engine::approval_action::PreparedActionKind,
    computer_use: bool,
    descriptions: Option<RuntimeString>,
    authorization: ReviewAuthorization,
    deadline: Instant,
    cancellation: AbortSignal,
    fresh: bool,
}

impl ReviewerRequest for ReviewAttempt {
    type Session = ReviewConversation;
    fn setup(&self) -> Arc<ReviewSetup> {
        self.setup.clone()
    }
    fn context(&self, _: Option<&ReviewConversation>) -> RuntimeString {
        self.key.clone()
    }
    fn requires_fresh_session(&self) -> bool {
        self.fresh
    }
    fn deadline(&self) -> Instant {
        self.deadline
    }
    fn cancellation(&self) -> Option<&AbortSignal> {
        Some(&self.cancellation)
    }
    async fn run(
        &self,
        session: &ReviewConversation,
        _: review::ReviewSessionKind,
    ) -> review::ReviewSessionResult {
        let reusable = std::sync::atomic::AtomicBool::new(false);
        let run = async {
            let evidence = match self.parent.review_evidence_snapshot(&self.setup.capture) {
                Ok(evidence) => evidence,
                Err(error) => {
                    return review::ReviewSessionOutcome::PromptBuildFailed(
                        error.to_node_utf8_string(),
                    )
                }
            };
            let mut profile = ContextProfile::synchronous();
            if self.setup.capture.config.approval_review_transcript_mode
                == Some(crate::engine::approval_async::TranscriptFormat::Json)
            {
                profile.transcript_format = context::TranscriptFormat::Json;
            }
            let mut history = session.history.lock_recover().clone();
            let cursor = session.conversation.lock_recover().cursor();
            let mode = cursor.map_or(TranscriptMode::Full, |cursor| TranscriptMode::Delta {
                cursor,
            });
            let rendered = match runtime_codec::encode_text(&self.action).and_then(|text| {
                text.try_to_utf8().map_err(|_| {
                    crate::runtime_values::RuntimeCodecError::new(
                        crate::runtime_values::RuntimeCodecErrorKind::Custom(
                            "Review evidence contains invalid Unicode.".into(),
                        ),
                    )
                })
            }) {
                Ok(text) => text,
                Err(_) => {
                    return review::ReviewSessionOutcome::PromptBuildFailed(
                        "Cannot encode the complete reviewed action.".into(),
                    )
                }
            };
            let kind = match self.action_kind {
                crate::engine::approval_action::PreparedActionKind::Command => {
                    PlannedActionKind::Command
                }
                crate::engine::approval_action::PreparedActionKind::TerminalInput => {
                    PlannedActionKind::TerminalInput
                }
                crate::engine::approval_action::PreparedActionKind::Network { has_trigger } => {
                    PlannedActionKind::Network { has_trigger }
                }
            };
            let mut planned = match PlannedAction::for_request(
                rendered,
                kind,
                Some(&self.request.message.to_node_utf8_string()),
                None,
                None,
                Some(&self.parent.id.to_node_utf8_string()),
            ) {
                Ok(planned) => planned,
                Err(error) => {
                    return review::ReviewSessionOutcome::PromptBuildFailed(error.to_string())
                }
            };
            planned.tool_descriptions = self
                .descriptions
                .as_ref()
                .map(NodeText::to_node_utf8_string);
            let mut system = self.setup.system.clone();
            if let Some(policy) = self
                .setup
                .policy
                .computer_policy_for_action(self.computer_use)
            {
                system.push(SystemPromptPart {
                    name: "approval-review-computer-policy".into(),
                    text: policy.into(),
                    cacheable: Some(true),
                    cache_scope: None,
                });
            }
            let budget_request = crate::types::provider::ModelRequest {
                model: self.setup.target.model.clone(),
                image_limits: None,
                max_media_blocks: None,
                supports_image_detail_original: self
                    .setup
                    .target
                    .metadata
                    .supports_image_detail_original,
                system: system.clone(),
                messages: history.clone(),
                tools: session.state.tool_specs(&self.setup.target.capabilities),
                tool_choice: None,
                max_output_tokens: self.setup.target.capabilities.max_output_tokens,
                task_budget: None,
                output_schema: Some(
                    crate::runtime_values::runtime_json::parse(
                        &review::output_schema().to_string().into(),
                    )
                    .expect("The review schema is valid JSON"),
                ),
                web_search: None,
                thinking: None,
                effort: self.setup.target.effort.clone(),
                skip_cache_write: None,
                cache_ttl: None,
                cache_key: None,
                context_management: None,
                context_hints: None,
                thinking_context: None,
                model_verbosity: None,
                reasoning_summary: self.setup.capture.config.reasoning_summary,
            };
            let estimated_existing = match self
                .setup
                .target
                .provider
                .approval_request_tokens(&budget_request)
            {
                Ok(tokens) => tokens,
                Err(error) => {
                    return review::ReviewSessionOutcome::PromptBuildFailed(
                        error.to_node_utf8_string(),
                    )
                }
            };
            let window = context::effective_input_token_limit(
                &context::model::ModelInfo {
                    context_window: Some(
                        self.setup
                            .target
                            .capabilities
                            .context_window
                            .min(i64::MAX as u64) as i64,
                    ),
                    used_fallback_model_metadata: self
                        .setup
                        .target
                        .metadata
                        .used_fallback_model_metadata,
                    effective_context_window_percent: self
                        .setup
                        .target
                        .metadata
                        .effective_context_window_percent
                        .unwrap_or(95),
                },
                (self.setup.target.model == self.setup.capture.request.model)
                    .then_some(self.setup.accounting.context_window_override)
                    .flatten()
                    .and_then(|window| i64::try_from(window).ok()),
            ) as u64;
            let mut delivery = session.delivery.lock_recover().clone();
            delivery.retain_history(&history);
            let prepared = match context::prepare_for_messages_with_history(
                context::PrepareReviewInput {
                    canonical_system: &system,
                    history: &evidence.history,
                    root_conversation: &evidence.root_conversation,
                    trusted_user_answers: &evidence.trusted_user_answers,
                    permissions: Some(&evidence.permissions),
                    action: &planned,
                    previous_reviews: evidence.previous_reviews.as_ref(),
                    trusted_tool: evidence.trusted_tool.as_ref(),
                    trusted_skill_paths: &evidence.trusted_skill_paths,
                    node_repl: None,
                    profile,
                    mode,
                    parent_history_version: evidence.history_generation,
                    session_id: &self.parent.id.to_node_utf8_string(),
                    budget: RequestBudget {
                        max_input_tokens: window.saturating_sub(256) as usize,
                        existing_context_tokens: estimated_existing as usize,
                    },
                    history_truncation: HistoryTruncation::Allow,
                    independent_snapshot: history.is_empty(),
                },
                &history,
                &delivery,
            ) {
                Ok(prepared) => prepared,
                Err(context::SectionError::EvidenceLimitExceeded { .. }) => {
                    return review::ReviewSessionOutcome::InputBudgetExceeded
                }
                Err(error) => {
                    return review::ReviewSessionOutcome::PromptBuildFailed(error.to_string())
                }
            };
            let protected_message_ids = std::collections::HashSet::new();
            delivery.extend(prepared.delivery.clone());
            let restorer = Arc::new(Mutex::new(context::RetainedMessageRestorer::new(
                &prepared.retained_context,
                &delivery,
            )));
            let restore_retained: super::ApprovalQueryRestore = {
                let restorer = restorer.clone();
                let provider = self.setup.target.provider.clone();
                let maximum = window.saturating_sub(256) as usize;
                Arc::new(move |request| {
                    let baseline = provider
                        .approval_request_tokens(request)
                        .map_err(super::ApprovalQueryRestoreError::Invalid)?;
                    let additions = restorer
                        .lock_recover()
                        .prepare(
                            &request.messages,
                            RequestBudget {
                                max_input_tokens: maximum,
                                existing_context_tokens: baseline,
                            },
                        )
                        .map_err(|error| match error {
                            context::SectionError::EvidenceLimitExceeded { .. } => {
                                super::ApprovalQueryRestoreError::BudgetExceeded {
                                    estimated_tokens: baseline.saturating_add(1),
                                    max_input_tokens: maximum,
                                }
                            }
                            error => {
                                super::ApprovalQueryRestoreError::Invalid(error.to_string().into())
                            }
                        })?;
                    Ok(super::ApprovalQueryRestoration {
                        messages: additions.messages,
                        system: additions.system,
                    })
                })
            };
            history.extend(prepared.messages);
            let mut config = self.setup.capture.config.clone();
            config.model = self.setup.target.model.clone();
            config.effort = self.setup.target.effort.clone();
            let query_signal = self.cancellation.child_token();
            let monitor_done = AbortSignal::new();
            let monitor_guard = monitor_done.clone().drop_guard();
            let worker_cancellation = session.cancellation.clone();
            let parent_cancellation = self.parent.approval_review.tasks.cancellation.clone();
            let linked_query_signal = query_signal.clone();
            self.parent.approval_review.tasks.tasks.spawn(async move {
                tokio::select! {
                    () = monitor_done.cancelled() => {},
                    () = worker_cancellation.cancelled() => linked_query_signal.cancel(),
                    () = parent_cancellation.cancelled() => linked_query_signal.cancel(),
                }
            });
            let result = self
                .parent
                .run_approval_query(ApprovalQueryInput {
                    state: session.state.clone(),
                    provider: self.setup.target.provider.clone(),
                    accounting: self.setup.accounting.clone(),
                    config,
                    model: self.setup.target.model.clone(),
                    effort: self.setup.target.effort.clone(),
                    system: prepared.system,
                    history,
                    protected_message_ids,
                    restore_retained: Some(restore_retained),
                    max_input_tokens: window.saturating_sub(256) as usize,
                    schema: crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                        review::output_schema().to_string(),
                    ))
                    .expect("The review schema is valid JSON"),
                    thread_id: self.parent.id.concat(&":approval-review".into()),
                    turn_id: (self.parent.deps.uuid)().into(),
                    cache_key: self.parent.id.concat(&":approval-review".into()),
                    signal: query_signal,
                    deadline: self.deadline,
                })
                .await;
            drop(monitor_guard);
            tracing::debug!(usage = ?result.usage, "Approval review query completed");
            reusable.store(result.reusable && result.turn_completed, Ordering::Release);
            match result.outcome {
                ApprovalQueryOutcome::Completed => {
                    let current = self.parent.approval_authorization(&self.setup.capture);
                    if current != self.authorization {
                        return if current.policy != self.authorization.policy
                            || current.rules != self.authorization.rules
                            || current.model != self.authorization.model
                            || current.config != self.authorization.config
                            || current.history_generation != self.authorization.history_generation
                            || current.account != self.authorization.account
                            || current.inference_revision != self.authorization.inference_revision
                            || current.mcp_revision != self.authorization.mcp_revision
                        {
                            review::ReviewSessionOutcome::Aborted
                        } else {
                            review::ReviewSessionOutcome::StaleAuthorization
                        };
                    }
                    if result.final_text.as_ref().is_some_and(|text| {
                        review::parse_assessment(&text.to_node_utf8_string()).is_ok()
                    }) && result.reusable
                        && result.turn_completed
                        && !self.cancellation.is_cancelled()
                        && !session.cancellation.is_cancelled()
                    {
                        *session.history.lock_recover() = result.history.clone();
                        let mut conversation = session.conversation.lock_recover();
                        conversation.complete_review(prepared.transcript_cursor);
                        let delivery = restorer
                            .lock_recover()
                            .delivery_for_history(&result.history);
                        *session.delivery.lock_recover() = delivery.clone();
                        conversation.commit_snapshot(ReviewCheckpoint {
                            history: result.history,
                            delivery,
                        });
                    }
                    review::ReviewSessionOutcome::Completed(Ok(result
                        .final_text
                        .map(|text| text.to_node_utf8_string())))
                }
                ApprovalQueryOutcome::Failed(error) => {
                    review::ReviewSessionOutcome::SessionFailed {
                        error: error.message.to_node_utf8_string(),
                        error_info: session_error_info(&error),
                        retry_at: error
                            .retry_after_ms
                            .map(|ms| Instant::now() + std::time::Duration::from_millis(ms)),
                    }
                }
                ApprovalQueryOutcome::InputBudgetExceeded(budget) => {
                    tracing::debug!(
                        estimated_tokens = budget.estimated_tokens,
                        max_input_tokens = budget.max_input_tokens,
                        "Approval review input remains over budget after recovery"
                    );
                    review::ReviewSessionOutcome::InputBudgetExceeded
                }
                ApprovalQueryOutcome::TimedOut => review::ReviewSessionOutcome::TimedOut,
                ApprovalQueryOutcome::Aborted => review::ReviewSessionOutcome::Aborted,
            }
        }
        .await;
        let disposition = match run {
            review::ReviewSessionOutcome::Completed(Ok(Some(ref text)))
                if reusable.load(Ordering::Acquire) && review::parse_assessment(text).is_ok() =>
            {
                review::SessionDisposition::Reusable
            }
            review::ReviewSessionOutcome::Completed(_)
            | review::ReviewSessionOutcome::StaleAuthorization
                if reusable.load(Ordering::Acquire) =>
            {
                review::SessionDisposition::Rollback
            }
            _ => review::SessionDisposition::Discard,
        };
        review::ReviewSessionResult {
            outcome: run,
            disposition,
            analytics: review::ReviewAnalytics::default(),
        }
    }
}

fn session_error_info(
    error: &crate::types::errors::EngineError,
) -> Option<review::SessionErrorInfo> {
    review::SessionErrorInfo::from_engine_error(error)
}

impl Session {
    async fn assess_approval(
        self: &Arc<Self>,
        accounting: Arc<InferenceBundle>,
        capture: ApprovalReviewContext,
        request: ApprovalRequest,
        metadata: ApprovalActionMetadata,
        signal: AbortSignal,
        required: bool,
    ) -> ApprovalReviewResponse {
        let deadline = Instant::now() + review::REVIEW_TIMEOUT;
        let prepared_action = match crate::engine::approval_action::prepare_action(
            &request,
            &metadata,
            &self.cwd_now().into_runtime_string(),
        ) {
            Ok(action) => action,
            Err(error) => return deny(error),
        };
        let typed_action = prepared_action.action;
        let action = prepared_action.planned_action;
        let action_kind = prepared_action.kind;
        let descriptions = prepared_action.tool_descriptions;
        let computer_use = metadata.is_computer_use_js_execution();
        let escalated = request
            .input
            .get("sandboxPermissions")
            .or_else(|| request.input.get("sandbox_permissions"))
            .is_some_and(|value| {
                value
                    .as_str()
                    .is_some_and(|value| value == "require_escalated")
            });
        let fresh = escalated || required;
        let prepared_setup = review::run_before_review_deadline(
            deadline,
            Some(&signal),
            self.make_review_setup(capture.clone(), accounting.clone(), signal.clone()),
        )
        .await;
        let (initial_setup, initial_key) = match prepared_setup {
            Ok(Ok(setup)) => setup,
            Ok(Err(error)) => {
                let message = format!("Automatic approval review preparation failed: {error}. The action was not executed.");
                (capture.emit)(EngineEvent::ApprovalReviewWarning {
                    message: message.clone().into(),
                    kind: None,
                });
                return deny(message);
            }
            Err(review::ReviewSessionOutcome::TimedOut) => {
                return deny(context::TIMEOUT_INSTRUCTIONS)
            }
            Err(_) => return deny("Approval review was cancelled before preparation completed."),
        };
        let cached = self
            .approval_cached_evidence(&capture, &request, &action, fresh, &metadata)
            .await;
        let reason = cached
            .as_ref()
            .err()
            .copied()
            .unwrap_or(crate::engine::approval_async::ReviewFallbackReason::Policy);
        let review_reason = Some(match reason {
            crate::engine::approval_async::ReviewFallbackReason::Policy => {
                ApprovalReviewReason::Policy
            }
            crate::engine::approval_async::ReviewFallbackReason::FreshRequired => {
                ApprovalReviewReason::FreshRequired
            }
            crate::engine::approval_async::ReviewFallbackReason::MissingScore => {
                ApprovalReviewReason::MissingScore
            }
            crate::engine::approval_async::ReviewFallbackReason::StaleScore => {
                ApprovalReviewReason::StaleScore
            }
            crate::engine::approval_async::ReviewFallbackReason::InvalidScore => {
                ApprovalReviewReason::InvalidScore
            }
            crate::engine::approval_async::ReviewFallbackReason::IncompatibleCompaction => {
                ApprovalReviewReason::IncompatibleCompaction
            }
            crate::engine::approval_async::ReviewFallbackReason::ElevatedRisk => {
                ApprovalReviewReason::ElevatedRisk
            }
            crate::engine::approval_async::ReviewFallbackReason::ScoringFailure => {
                ApprovalReviewReason::ScoringFailure
            }
            crate::engine::approval_async::ReviewFallbackReason::AuthorizationChanged => {
                ApprovalReviewReason::AuthorizationChanged
            }
        });
        let mut event = ApprovalReviewEvent {
            model_context: Some(ApprovalReviewModelContext {
                model_slug: initial_setup.target.model.clone(),
                reasoning_effort: initial_setup.target.raw_effort.clone(),
            }),
            review_reason,
            id: (self.deps.uuid)().into(),
            target_item_id: Some(request.id.clone()),
            plugin_id: None,
            script_path: None,
            turn_id: capture.turn_id.clone(),
            started_at_ms: (self.deps.now)(),
            completed_at_ms: None,
            status: ApprovalReviewStatus::InProgress,
            risk_level: None,
            user_authorization: None,
            rationale: None,
            decision_source: None,
            action: typed_action,
        };
        if matches!(
            reason,
            crate::engine::approval_async::ReviewFallbackReason::ElevatedRisk
                | crate::engine::approval_async::ReviewFallbackReason::StaleScore
                | crate::engine::approval_async::ReviewFallbackReason::IncompatibleCompaction
                | crate::engine::approval_async::ReviewFallbackReason::ScoringFailure
                | crate::engine::approval_async::ReviewFallbackReason::AuthorizationChanged
        ) {
            (capture.emit)(EngineEvent::ApprovalReviewStrictRequired {
                turn_id: capture.turn_id.clone(),
                started_at_ms: event.started_at_ms,
            });
        }
        self.publish_approval_review(&capture, event.clone()).await;
        let (outcome, completed_authorization) = if cached.is_ok() {
            (review::ReviewOutcome::CachedApproval, None)
        } else {
            let review_signal = signal.child_token();
            let mut attempt = 0;
            let descriptions = descriptions.clone();
            let mut first_setup = Some((initial_setup.clone(), initial_key));
            let synchronous = review::run_with_retry(
                review::ReviewSessionLimits {
                    max_attempts: review::MAX_REVIEW_ATTEMPTS,
                    deadline,
                },
                Some(&review_signal),
                |_| {
                    attempt += 1;
                    let capture = capture.clone();
                    let request = request.clone();
                    let action = action.clone();
                    let accounting = accounting.clone();
                    let signal = review_signal.clone();
                    let descriptions = descriptions.clone();
                    let first_setup = first_setup.take();
                    async move {
                        let preparation = match first_setup {
                            Some(setup) => Ok(setup),
                            None => {
                                self.make_review_setup(capture.clone(), accounting, signal.clone())
                                    .await
                            }
                        };
                        let (setup, key) = match preparation {
                            Ok(setup) => setup,
                            Err(error) => {
                                return (
                                    review::ReviewOutcome::Error(error),
                                    review::ReviewAnalytics::default(),
                                    None::<ReviewAuthorization>,
                                )
                            }
                        };
                        let authorization = self.approval_authorization(&capture);
                        let (outcome, analytics) = self
                            .approval_review
                            .pool
                            .review(ReviewAttempt {
                                parent: self.clone(),
                                setup,
                                key,
                                request,
                                action,
                                action_kind,
                                computer_use,
                                descriptions,
                                authorization: authorization.clone(),
                                deadline,
                                cancellation: signal,
                                fresh: attempt > 1,
                            })
                            .await;
                        tracing::debug!(attempt_count=analytics.attempt_count, session_kind=?analytics.session_kind, "Approval review attempt completed");
                        (outcome.into(), analytics, Some(authorization))
                    }
                },
            );
            let waiting = async {
                if fresh {
                    std::future::pending::<()>().await;
                } else {
                    self.clone()
                        .wait_approval_cache(
                            capture.clone(),
                            request.clone(),
                            action.clone(),
                            metadata.clone(),
                        )
                        .await;
                }
            };
            match crate::engine::approval_async::race_review(
                synchronous,
                waiting,
                &review_signal,
                &signal,
                deadline,
                || async {
                    !signal.is_cancelled()
                        && (required
                            || self.runtime_policy_state().approvals_reviewer
                                == Some(ApprovalsReviewer::Automatic))
                        && self
                            .approval_cached_evidence(&capture, &request, &action, fresh, &metadata)
                            .await
                            .is_ok()
                },
            )
            .await
            {
                crate::engine::approval_async::ReviewRace::Synchronous((
                    outcome,
                    _,
                    authorization,
                )) => (outcome, authorization),
                crate::engine::approval_async::ReviewRace::Cached { .. } => {
                    (review::ReviewOutcome::CachedApproval, None)
                }
                crate::engine::approval_async::ReviewRace::Aborted { .. } => (
                    review::ReviewOutcome::Error(review::ReviewError::Cancelled),
                    None,
                ),
                crate::engine::approval_async::ReviewRace::TimedOut { .. } => (
                    review::ReviewOutcome::Error(review::ReviewError::Timeout),
                    None,
                ),
            }
        };
        event.completed_at_ms = Some((self.deps.now)());
        let mut warning = None;
        let mut policy_denied = false;
        let cached_outcome = matches!(&outcome, review::ReviewOutcome::CachedApproval);
        let response = match outcome {
            review::ReviewOutcome::CachedApproval => {
                event.status = ApprovalReviewStatus::Approved;
                event.rationale =
                    Some("Current low-risk classification approved this action.".into());
                ApprovalReviewResponse::Approved { message: None }
            }
            review::ReviewOutcome::Completed(assessment) => {
                let approved = assessment.outcome == review::AssessmentOutcome::Allow;
                policy_denied = !approved;
                event.status = if approved {
                    ApprovalReviewStatus::Approved
                } else {
                    ApprovalReviewStatus::Denied
                };
                event.risk_level = Some(match assessment.risk_level {
                    review::RiskLevel::Low => ApprovalReviewRiskLevel::Low,
                    review::RiskLevel::Medium => ApprovalReviewRiskLevel::Medium,
                    review::RiskLevel::High => ApprovalReviewRiskLevel::High,
                    review::RiskLevel::Critical => ApprovalReviewRiskLevel::Critical,
                });
                event.user_authorization = Some(match assessment.user_authorization {
                    review::UserAuthorization::Unknown => ApprovalReviewUserAuthorization::Unknown,
                    review::UserAuthorization::Low => ApprovalReviewUserAuthorization::Low,
                    review::UserAuthorization::Medium => ApprovalReviewUserAuthorization::Medium,
                    review::UserAuthorization::High => ApprovalReviewUserAuthorization::High,
                });
                event.rationale = Some(assessment.rationale.clone().into());
                event.decision_source = Some(ApprovalReviewDecisionSource::Agent);
                warning = Some(format!(
                    "Automatic approval review {} (risk: {:?}, authorization: {:?}): {}",
                    if approved { "approved" } else { "denied" },
                    assessment.risk_level,
                    assessment.user_authorization,
                    assessment.rationale
                ));
                if approved {
                    ApprovalReviewResponse::Approved { message: None }
                } else {
                    deny(context::rejection_feedback(
                        &assessment.rationale,
                        &initial_setup.policy.rejection_instructions,
                    ))
                }
            }
            review::ReviewOutcome::Error(review::ReviewError::InputBudgetExceeded) if !required => {
                event.status = ApprovalReviewStatus::Aborted;
                warning = Some("Automatic approval review exceeded its input budget; user approval is required.".into());
                ApprovalReviewResponse::AskUser {
                    message: warning.clone().map(Into::into),
                }
            }
            review::ReviewOutcome::Error(review::ReviewError::Cancelled) => {
                event.status = ApprovalReviewStatus::Aborted;
                deny("Approval review was cancelled.")
            }
            review::ReviewOutcome::Error(review::ReviewError::Timeout) => {
                event.status = ApprovalReviewStatus::TimedOut;
                event.rationale = Some(initial_setup.policy.timeout_instructions.clone().into());
                warning = Some(initial_setup.policy.timeout_instructions.clone());
                deny(initial_setup.policy.timeout_instructions.clone())
            }
            review::ReviewOutcome::Error(error) => {
                event.status = ApprovalReviewStatus::Denied;
                let message = format!("Automatic approval review failed: {error}. The action was not executed because automatic approval review could not be completed. This is a review failure, not a determination that the action is unsafe. Do not bypass the approval check; resolve the error or ask the user for guidance.");
                event.rationale = Some(message.clone().into());
                warning = Some(message.clone());
                deny(message)
            }
        };
        if let Some(authorization) = completed_authorization
            .filter(|_| event.decision_source == Some(ApprovalReviewDecisionSource::Agent))
        {
            let completed_at_ms = event.completed_at_ms.unwrap_or_default();
            let correlation = serde_json::json!({ "review_id": event.id.to_node_utf8_string(), "turn_id": event.turn_id.to_node_utf8_string(), "target_item_id": event.target_item_id.as_ref().map(NodeText::to_node_utf8_string), "completed_at_ms": completed_at_ms });
            let as_json = |value: RuntimeJson| {
                serde_json::from_slice::<serde_json::Value>(
                    &crate::runtime_values::runtime_json::stringify(&value).to_node_utf8(),
                )
                .unwrap_or(serde_json::Value::Null)
            };
            let decision = serde_json::json!({ "status": as_json(runtime_codec::encode_value(&event.status).unwrap_or(RuntimeJson::Null)), "risk_level": as_json(runtime_codec::encode_value(&event.risk_level).unwrap_or(RuntimeJson::Null)), "user_authorization": as_json(runtime_codec::encode_value(&event.user_authorization).unwrap_or(RuntimeJson::Null)) });
            let action =
                crate::runtime_values::runtime_json::stringify(&action).to_node_utf8_string();
            let rationale = event.rationale.as_ref().map(NodeText::to_node_utf8_string);
            let rendered = context::render_review_evidence(context::ReviewEvidence {
                correlation: &correlation,
                decision: &decision,
                action: &action,
                rationale: rationale.as_deref(),
            });
            let mut records = self.approval_review.completed_evidence.lock_recover();
            records.push(CompletedReviewEvidence {
                authorization,
                completed_at_ms,
                review: context::PreviousReview {
                    id: context::model::ResponseItemId(format!(
                        "approval_review_{}",
                        (self.deps.uuid)()
                    )),
                    fragment: rendered.body,
                },
            });
            records.sort_by_key(|record| record.completed_at_ms);
            let excess = records.len().saturating_sub(context::MAX_PREVIOUS_REVIEWS);
            records.drain(..excess);
        }
        if let Some(message) = warning {
            (capture.emit)(EngineEvent::ApprovalReviewWarning {
                message: message.into(),
                kind: None,
            });
        }
        let breaker = if cached_outcome {
            review::DenialCircuitBreakerAction::Continue
        } else {
            let mut breaker = self.approval_review.breaker.lock_recover();
            if policy_denied {
                breaker.record_denial(
                    &capture.turn_id.to_node_utf8_string(),
                    if capture.provider.as_ref().is_some_and(|provider| {
                        provider
                            .model_metadata(&capture.request.model)
                            .model_specialty
                            .as_ref()
                            .is_some_and(|specialty| specialty == "cyber")
                    }) {
                        review::DenialCircuitBreakerPolicy::CyberModel
                    } else {
                        review::DenialCircuitBreakerPolicy::Standard
                    },
                )
            } else {
                breaker.record_non_denial(&capture.turn_id.to_node_utf8_string());
                review::DenialCircuitBreakerAction::Continue
            }
        };
        self.publish_approval_review(&capture, event).await;
        if let review::DenialCircuitBreakerAction::InterruptTurn { .. } = breaker {
            (capture.emit)(EngineEvent::ApprovalReviewWarning { message: "Automatic approval review denied too many actions in this turn. Stop and request user guidance.".into(), kind: Some("tooManyDenials".into()) });
            signal.cancel();
        }
        response
    }
}

trait NodeText {
    fn to_node_utf8_string(&self) -> String;
}
impl NodeText for RuntimeString {
    fn to_node_utf8_string(&self) -> String {
        String::from_utf8(self.to_node_utf8()).expect("Node projection is UTF-8")
    }
}
