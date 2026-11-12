//! Background precomputation of the automatic-compaction checkpoint.
//!
//! While the context approaches the automatic-compaction boundary, a detached
//! task produces the summary checkpoint ahead of time so the turn that finally
//! crosses the boundary applies it without waiting for a model call. The
//! background pass never emits user-visible events on its own; everything the
//! user can observe happens when the checkpoint is applied on the synchronous
//! path, in the same order that path produces today.
//!
//! A precomputed checkpoint is applied only while it provably matches the live
//! history: its boundary message is still present, every message the summary
//! retained still exists, the summary model is still the selected model, and
//! the history neither grew nor shrank beyond the staleness bounds. Every
//! other case falls back to the synchronous compaction unchanged.

use super::compaction::query_message_uuid;
use crate::context::compact::outcome::CompactionNoopReason;
use crate::context::tokens::estimate_context_tokens;
use crate::engine::deps::{CompactionResult, Compactor, NowFn};
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::message::Message;
use crate::types::abort::AbortSignal;
use crate::types::provider::ModelRequest;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

/// Consecutive counted background failures after which the agent stops arming.
const REARM_FAILURE_CAP: u32 = 3;
/// History growth (tokens) beyond which a ready checkpoint counts as stale.
const GROWTH_LIMIT_TOKENS: i128 = 150_000;

/// Runs the PreCompact hook inside the background task. The notifications it
/// produced are replayed when the checkpoint is applied so their visible
/// position matches the synchronous path.
pub type PrecomputeHook = Arc<
    dyn Fn(AbortSignal) -> futures::future::BoxFuture<'static, PrecomputeHookOutcome> + Send + Sync,
>;

/// What the PreCompact hook run at arm time produced.
pub struct PrecomputeHookOutcome {
    pub blocked: bool,
    pub notifications: Vec<RuntimeString>,
    pub instructions: Option<RuntimeString>,
}

/// Everything the detached summary task needs, captured at the arm site.
pub struct PrecomputeArm {
    /// Registry key: the agent identity within the owning session.
    pub agent_key: RuntimeString,
    /// Compactable history snapshot (protected messages excluded).
    pub snapshot: Vec<Message>,
    /// Prefix-aware full-request estimate for the maybe-compact path.
    pub estimated_tokens: u64,
    /// Messages-only estimate recorded for the staleness bounds.
    pub pre_compact_tokens: u64,
    /// The model the summary is produced with; a later switch invalidates.
    pub model: RuntimeString,
    pub config: ResolvedConfig,
    /// Live request snapshot carrying the cacheable prefix into the summary.
    pub request: ModelRequest,
    /// Independent compactor fork; never shares slots with the foreground one.
    pub compactor: Arc<dyn Compactor>,
    pub hook: Option<PrecomputeHook>,
    pub now: NowFn,
}

struct ReadyPayload {
    result: CompactionResult,
    staged_output: Vec<Message>,
    hook_notifications: Vec<RuntimeString>,
    ready_at_ms: i64,
}

enum EntryState {
    Pending,
    Ready(ReadyPayload),
    Failed,
    /// The hook blocked the run, or the checkpoint was consumed/invalidated.
    Retired,
}

/// One armed background checkpoint.
pub struct PrecomputeEntry {
    agent_key: RuntimeString,
    /// The last message the summary covered; consumption re-locates it in the
    /// live history instead of trusting indices.
    precomputed_at_uuid: RuntimeString,
    pre_compact_tokens: u64,
    model: RuntimeString,
    native: Option<NativeCandidate>,
    started_at_ms: i64,
    /// Independent cancellation: a turn abort never kills the background task,
    /// but session teardown (registry drop) does.
    signal: AbortSignal,
    state: Mutex<EntryState>,
    settled: tokio::sync::watch::Sender<bool>,
}

struct NativeCandidate {
    context: RuntimeString,
    source: Option<crate::types::events::InferenceSource>,
    prefix_len: usize,
    prefix_hash: [u8; 32],
    system: Vec<crate::types::provider::SystemPromptPart>,
    tools: Vec<crate::types::provider::ToolSpec>,
    native_deferred_tools: Option<bool>,
    deferred_tools: Vec<crate::types::provider::ToolSpec>,
}

fn history_hash(messages: &[Message]) -> Option<[u8; 32]> {
    let encoded = crate::runtime_values::runtime_codec::encode_text(&messages.to_vec()).ok()?;
    let mut hash = Sha256::new();
    for unit in encoded.as_utf16() {
        hash.update(unit.to_le_bytes());
    }
    Some(hash.finalize().into())
}

fn closed_tool_exchanges(messages: &[Message]) -> bool {
    use crate::schemas::content::ContentBlock;
    let mut pending = HashSet::new();
    for message in messages {
        match message {
            Message::Assistant(assistant) => {
                for block in &assistant.content {
                    if let ContentBlock::ToolUse(call) = block {
                        pending.insert(call.id.clone());
                    }
                }
            }
            Message::ToolResult(result) => {
                pending.remove(&result.tool_use_id);
            }
            Message::User(user) => {
                for block in &user.content {
                    if let ContentBlock::ToolResult(result) = block {
                        pending.remove(&result.tool_use_id);
                    }
                }
            }
            _ => {}
        }
    }
    pending.is_empty()
}

/// The checkpoint plus everything needed to apply it on the synchronous path.
#[derive(Debug)]
pub struct PrecomputedSwap {
    /// Compacted replacement history: the precomputed checkpoint followed by
    /// every live message that arrived after its boundary.
    pub result: CompactionResult,
    pub hook_notifications: Vec<RuntimeString>,
    pub staged_output: Vec<Message>,
    /// Arm-to-ready duration of the background summary.
    pub lead_ms: i64,
    /// Arm-to-apply duration.
    pub total_ms: i64,
    pub messages_since: usize,
    pub borrowed: bool,
}

#[derive(Debug)]
pub enum PrecomputeConsume {
    /// No usable checkpoint; the caller runs the synchronous compaction.
    Miss,
    /// The turn was cancelled while waiting for an in-flight checkpoint. The
    /// entry stays armed for a later turn.
    Aborted,
    Hit(Box<PrecomputedSwap>),
}

/// Session-scoped table of armed background checkpoints, keyed by agent.
#[derive(Default)]
pub struct PrecomputeRegistry {
    entries: Mutex<HashMap<RuntimeString, Arc<PrecomputeEntry>>>,
    failures: Mutex<HashMap<RuntimeString, u32>>,
}

impl Drop for PrecomputeRegistry {
    fn drop(&mut self) {
        // Session teardown cancels every in-flight background summary.
        let entries = self
            .entries
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in entries.values() {
            entry.signal.cancel();
        }
    }
}

impl PrecomputeRegistry {
    /// Arm one background checkpoint unless an entry is already in flight, the
    /// agent hit its failure cap, or the snapshot has no boundary uuid.
    pub fn arm(self: &Arc<Self>, arm: PrecomputeArm) -> bool {
        let native = if arm.compactor.native_summary_active() {
            let Some(context) = arm.compactor.native_precompute_context() else {
                return false;
            };
            let Some(prefix_hash) = history_hash(&arm.snapshot) else {
                return false;
            };
            if !closed_tool_exchanges(&arm.snapshot) {
                return false;
            }
            Some(NativeCandidate {
                context,
                prefix_hash,
                prefix_len: arm.snapshot.len(),
                source: arm.config.model_source.clone(),
                system: arm.request.system.clone(),
                tools: arm.request.tools.clone(),
                native_deferred_tools: arm.request.native_deferred_tools,
                deferred_tools: arm.request.deferred_tools.clone(),
            })
        } else {
            None
        };
        if self
            .failures
            .lock()
            .unwrap()
            .get(&arm.agent_key)
            .copied()
            .unwrap_or(0)
            >= REARM_FAILURE_CAP
        {
            tracing::debug!("compaction precompute rearm capped");
            return false;
        }
        let Some(boundary_uuid) = arm
            .snapshot
            .last()
            .map(|message| query_message_uuid(message).clone())
        else {
            return false;
        };
        if boundary_uuid.is_empty() {
            return false;
        }
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(existing) = entries.get(&arm.agent_key) {
                let replaceable = matches!(
                    *existing.state.lock().unwrap(),
                    EntryState::Failed | EntryState::Retired
                );
                if !replaceable {
                    return false;
                }
                existing.signal.cancel();
                entries.remove(&arm.agent_key);
            }
        }
        let (settled, _initial) = tokio::sync::watch::channel(false);
        let entry = Arc::new(PrecomputeEntry {
            agent_key: arm.agent_key.clone(),
            precomputed_at_uuid: boundary_uuid,
            pre_compact_tokens: arm.pre_compact_tokens,
            model: arm.model.clone(),
            native,
            started_at_ms: (arm.now)(),
            signal: AbortSignal::new(),
            state: Mutex::new(EntryState::Pending),
            settled,
        });
        self.entries
            .lock()
            .unwrap()
            .insert(entry.agent_key.clone(), entry.clone());
        tracing::info!(
            tokens = arm.pre_compact_tokens,
            "compaction precompute armed"
        );
        tokio::spawn(run_precompute(Arc::downgrade(self), entry, arm));
        true
    }

    /// Try to apply the armed checkpoint for this agent or, failing that, a
    /// borrowable one from another agent in the same session. A borrow only
    /// succeeds when the other entry's boundary uuid exists in this history.
    #[cfg(test)]
    pub async fn consume(
        &self,
        agent_key: &RuntimeString,
        messages_now: &[Message],
        config: &ResolvedConfig,
        turn_signal: &AbortSignal,
        now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> PrecomputeConsume {
        self.consume_checked(agent_key, messages_now, config, turn_signal, now, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn consume_native(
        &self,
        agent_key: &RuntimeString,
        messages_now: &[Message],
        config: &ResolvedConfig,
        turn_signal: &AbortSignal,
        now: &(dyn Fn() -> i64 + Send + Sync),
        compactor: &dyn Compactor,
        request: &ModelRequest,
    ) -> PrecomputeConsume {
        self.consume_checked(
            agent_key,
            messages_now,
            config,
            turn_signal,
            now,
            Some((compactor, request)),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_checked(
        &self,
        agent_key: &RuntimeString,
        messages_now: &[Message],
        config: &ResolvedConfig,
        turn_signal: &AbortSignal,
        now: &(dyn Fn() -> i64 + Send + Sync),
        current: Option<(&dyn Compactor, &ModelRequest)>,
    ) -> PrecomputeConsume {
        let candidates: Vec<(Arc<PrecomputeEntry>, bool)> = {
            let entries = self.entries.lock().unwrap();
            let mut list = Vec::with_capacity(entries.len());
            if let Some(own) = entries.get(agent_key) {
                list.push((own.clone(), false));
            }
            for (key, entry) in entries.iter() {
                if key != agent_key {
                    list.push((entry.clone(), true));
                }
            }
            list
        };
        for (entry, borrowed) in candidates {
            if borrowed && entry.native.is_some() {
                continue;
            }
            match self
                .consume_entry(
                    &entry,
                    borrowed,
                    messages_now,
                    config,
                    turn_signal,
                    now,
                    current,
                )
                .await
            {
                PrecomputeConsume::Miss => continue,
                other => return other,
            }
        }
        PrecomputeConsume::Miss
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_entry(
        &self,
        entry: &Arc<PrecomputeEntry>,
        borrowed: bool,
        messages_now: &[Message],
        config: &ResolvedConfig,
        turn_signal: &AbortSignal,
        now: &(dyn Fn() -> i64 + Send + Sync),
        current: Option<(&dyn Compactor, &ModelRequest)>,
    ) -> PrecomputeConsume {
        // Staleness bounds, checked before any wait: a checkpoint whose
        // boundary no longer exists can never be applied.
        if entry.model != config.model {
            self.invalidate(entry, "model_changed");
            return PrecomputeConsume::Miss;
        }
        let native_is_current = || {
            entry.native.as_ref().is_none_or(|native| {
                current.is_some_and(|(compactor, request)| {
                    compactor.native_precompute_context().as_ref() == Some(&native.context)
                        && config.model_source == native.source
                        && request.system == native.system
                        && request.tools == native.tools
                        && request.deferred_tools == native.deferred_tools
                        && request.native_deferred_tools == native.native_deferred_tools
                        && messages_now.len() >= native.prefix_len
                        && history_hash(&messages_now[..native.prefix_len])
                            == Some(native.prefix_hash)
                })
            })
        };
        if !native_is_current() {
            self.invalidate(entry, "native_context_changed");
            return PrecomputeConsume::Miss;
        }
        let Some(boundary_index) = messages_now
            .iter()
            .position(|message| query_message_uuid(message) == &entry.precomputed_at_uuid)
        else {
            self.invalidate(entry, "boundary_uuid_missing");
            return PrecomputeConsume::Miss;
        };
        let est_now = estimate_context_tokens(messages_now);
        let delta = i128::from(est_now) - i128::from(entry.pre_compact_tokens);
        if delta > GROWTH_LIMIT_TOKENS {
            self.invalidate(entry, "grew_too_much");
            return PrecomputeConsume::Miss;
        }
        if delta < -(i128::from(entry.pre_compact_tokens) / 2) {
            self.invalidate(entry, "shrank_too_much");
            return PrecomputeConsume::Miss;
        }
        // Wait for an in-flight summary; a cancelled turn leaves the entry
        // armed for a later turn instead of discarding the work.
        {
            let mut settled = entry.settled.subscribe();
            while !*settled.borrow() {
                tokio::select! {
                    biased;
                    _ = turn_signal.cancelled() => return PrecomputeConsume::Aborted,
                    _ = entry.signal.cancelled() => return PrecomputeConsume::Miss,
                    changed = settled.changed() => {
                        if changed.is_err() {
                            return PrecomputeConsume::Miss;
                        }
                    }
                }
            }
        }
        if !native_is_current() {
            self.invalidate(entry, "native_context_changed_while_waiting");
            return PrecomputeConsume::Miss;
        }
        let payload = {
            let mut state = entry.state.lock().unwrap();
            match std::mem::replace(&mut *state, EntryState::Retired) {
                EntryState::Ready(payload) => payload,
                // Failed entries stay listed so the next arm replaces them;
                // the caller falls back to the synchronous path.
                other => {
                    *state = other;
                    return PrecomputeConsume::Miss;
                }
            }
        };
        if entry.native.is_some() && !payload.result.messages.iter().any(|message| {
            matches!(
                message, Message::CompactBoundary(boundary) if boundary.native_compaction.is_some()
            )
        }) {
            self.invalidate(entry, "native_checkpoint_missing");
            return PrecomputeConsume::Miss;
        }
        // The retained tail must survive verbatim: a rewind that removed any
        // of its messages invalidates the checkpoint.
        let live_uuids: HashSet<RuntimeString> = messages_now
            .iter()
            .map(query_message_uuid)
            .cloned()
            .collect();
        let retained_missing = payload.result.messages.iter().any(|message| {
            !matches!(message, Message::CompactBoundary(_))
                && !live_uuids.contains(query_message_uuid(message))
        });
        // Another compaction after the arm point rewrote the window; its own
        // checkpoint must not be double-stacked by this splice.
        let after = &messages_now[boundary_index + 1..];
        let history_rewritten = after
            .iter()
            .any(|message| matches!(message, Message::CompactBoundary(_)));
        let Some(boundary_pos) = payload
            .result
            .messages
            .iter()
            .rposition(|message| matches!(message, Message::CompactBoundary(_)))
        else {
            self.invalidate(entry, "checkpoint_missing");
            return PrecomputeConsume::Miss;
        };
        if retained_missing || history_rewritten {
            self.invalidate(
                entry,
                if retained_missing {
                    "preserve_uuid_missing"
                } else {
                    "history_rewritten"
                },
            );
            return PrecomputeConsume::Miss;
        }
        let mut spliced = payload.result.messages;
        spliced.splice(boundary_pos + 1..boundary_pos + 1, after.iter().cloned());
        if entry.native.is_some() {
            let fixed = current.map_or(0, |(_, request)| {
                super::request::fixed_prefix_tokens(request)
            });
            let status = crate::context::compact::policy::context_window_status(
                config,
                estimate_context_tokens(&spliced).saturating_add(fixed),
                None,
            );
            if status.token_limit_reached {
                self.invalidate(entry, "native_checkpoint_still_over_budget");
                return PrecomputeConsume::Miss;
            }
        }
        let freed = est_now.saturating_sub(estimate_context_tokens(&spliced));
        let lead_ms = payload.ready_at_ms.saturating_sub(entry.started_at_ms);
        let total_ms = now().saturating_sub(entry.started_at_ms);
        let messages_since = after.len();
        let result = CompactionResult {
            messages: spliced,
            freed_tokens: freed,
            compacted: true,
            reason: None,
            diagnostics: payload.result.diagnostics,
        };
        self.remove_listed(entry);
        PrecomputeConsume::Hit(Box::new(PrecomputedSwap {
            result,
            hook_notifications: payload.hook_notifications,
            staged_output: payload.staged_output,
            lead_ms,
            total_ms,
            messages_since,
            borrowed,
        }))
    }

    fn invalidate(&self, entry: &Arc<PrecomputeEntry>, reason: &'static str) {
        self.remove_listed(entry);
        tracing::debug!(reason, "compaction precompute invalidated");
    }

    fn remove_listed(&self, entry: &Arc<PrecomputeEntry>) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(listed) = entries.get(&entry.agent_key) {
            if Arc::ptr_eq(listed, entry) {
                entries.remove(&entry.agent_key);
            }
        }
    }
}

async fn run_precompute(
    registry: Weak<PrecomputeRegistry>,
    entry: Arc<PrecomputeEntry>,
    arm: PrecomputeArm,
) {
    let started = entry.started_at_ms;
    let mut instructions: Option<RuntimeString> = None;
    let mut notifications: Vec<RuntimeString> = Vec::new();
    if let Some(hook) = &arm.hook {
        let outcome = tokio::select! {
            biased;
            _ = entry.signal.cancelled() => return,
            outcome = hook(entry.signal.clone()) => outcome,
        };
        if entry.signal.is_cancelled() {
            return;
        }
        if outcome.blocked {
            *entry.state.lock().unwrap() = EntryState::Retired;
            entry.settled.send_replace(true);
            tracing::debug!("compaction precompute discarded by the hook");
            return;
        }
        notifications = outcome.notifications;
        instructions = outcome.instructions;
    }
    arm.compactor.prepare_request(&arm.request, &entry.signal);
    let staged = Arc::new(Mutex::new(Vec::<Message>::new()));
    arm.compactor.bind_output_observer(Some({
        let staged = staged.clone();
        Arc::new(move |message| {
            staged.lock().unwrap().push(message);
            Box::pin(futures::future::ready(()))
        })
    }));
    // Mirror the foreground dispatch so the background pass runs exactly the
    // operation the synchronous path would run at the boundary.
    let result = if arm.compactor.reactive_enabled() {
        arm.compactor
            .threshold_compact(&arm.snapshot, &arm.config, instructions.as_ref())
            .await
    } else {
        arm.compactor
            .maybe_compact(
                &arm.snapshot,
                arm.estimated_tokens,
                &arm.config,
                instructions.as_ref(),
            )
            .await
    };
    arm.compactor.bind_output_observer(None);
    if entry.signal.is_cancelled() {
        return;
    }
    let staged_output = std::mem::take(&mut *staged.lock().unwrap());
    if result.compacted && result.has_new_checkpoint(&arm.snapshot) {
        let ready_at_ms = (arm.now)();
        *entry.state.lock().unwrap() = EntryState::Ready(ReadyPayload {
            result,
            staged_output,
            hook_notifications: notifications,
            ready_at_ms,
        });
        if let Some(registry) = registry.upgrade() {
            // The cap counts consecutive failures; a success resets it.
            registry.failures.lock().unwrap().remove(&entry.agent_key);
        }
        tracing::info!(
            duration_ms = ready_at_ms.saturating_sub(started),
            "compaction precompute ready"
        );
    } else {
        let message = result
            .diagnostics
            .iter()
            .filter_map(|diagnostic| diagnostic.message.clone())
            .next()
            .unwrap_or_else(|| {
                RuntimeString::from("The background compaction summary did not complete.")
            });
        // A no-progress or disabled-compactor outcome is not a strike against
        // the rearm budget; genuine summary failures are.
        let countable = !matches!(
            result.reason,
            Some(CompactionNoopReason::NoProgress) | Some(CompactionNoopReason::CompactorDisabled)
        );
        tracing::debug!(
            countable,
            message = message.try_to_utf8().unwrap_or_default(),
            "compaction precompute failed"
        );
        *entry.state.lock().unwrap() = EntryState::Failed;
        if countable {
            if let Some(registry) = registry.upgrade() {
                let mut failures = registry.failures.lock().unwrap();
                let count = failures.entry(entry.agent_key.clone()).or_insert(0);
                *count += 1;
                if *count >= REARM_FAILURE_CAP {
                    tracing::info!("compaction precompute rearm capped after repeated failures");
                }
            }
        }
    }
    entry.settled.send_replace(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::config::default_config;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{CompactBoundaryMessage, UserMessage};

    fn user(uuid: &str) -> Message {
        Message::User(UserMessage {
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "continue the work".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
            question_answer: None,
        })
    }

    fn boundary(uuid: &str, summary: RuntimeString) -> Message {
        Message::CompactBoundary(CompactBoundaryMessage {
            uuid: uuid.into(),
            summary,
            native_compaction: None,
        })
    }

    struct ScriptedCompactor {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        outcome: ScriptedOutcome,
    }

    enum ScriptedOutcome {
        Summary(RuntimeString),
        Failure,
    }

    #[async_trait::async_trait]
    impl Compactor for ScriptedCompactor {
        async fn maybe_compact(
            &self,
            messages: &[Message],
            _estimated_tokens: u64,
            _config: &ResolvedConfig,
            _custom_instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            self.threshold_compact(messages, _config, _custom_instructions)
                .await
        }

        async fn reactive_compact(
            &self,
            messages: &[Message],
            config: &ResolvedConfig,
            custom_instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            self.threshold_compact(messages, config, custom_instructions)
                .await
        }

        async fn threshold_compact(
            &self,
            messages: &[Message],
            _config: &ResolvedConfig,
            _custom_instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match &self.outcome {
                ScriptedOutcome::Summary(summary) => CompactionResult {
                    messages: vec![boundary("bg-boundary", summary.clone())],
                    freed_tokens: 10,
                    compacted: true,
                    reason: None,
                    diagnostics: Vec::new(),
                },
                ScriptedOutcome::Failure => CompactionResult {
                    messages: messages.to_vec(),
                    freed_tokens: 0,
                    compacted: false,
                    reason: None,
                    diagnostics: Vec::new(),
                },
            }
        }
    }

    fn arm_request(
        registry_compactor: Arc<dyn Compactor>,
        snapshot: Vec<Message>,
    ) -> PrecomputeArm {
        PrecomputeArm {
            agent_key: "main".into(),
            pre_compact_tokens: estimate_context_tokens(&snapshot),
            estimated_tokens: estimate_context_tokens(&snapshot),
            snapshot,
            model: default_config().model.clone(),
            config: default_config(),
            request: ModelRequest {
                supports_image_detail_original: false,
                model_verbosity: None,
                reasoning_summary: crate::types::provider::ReasoningSummary::default(),
                model: default_config().model.clone(),
                image_limits: None,
                max_media_blocks: None,
                system: Vec::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                native_deferred_tools: None,
                deferred_tools: Vec::new(),
                tool_choice: None,
                max_output_tokens: 100,
                task_budget: None,
                output_schema: None,
                web_search: None,
                thinking: None,
                effort: None,
                skip_cache_write: None,
                cache_ttl: None,
                cache_domain: None,
                cache_key: None,
                context_management: None,
                context_hints: None,
                thinking_context: None,
            },
            compactor: registry_compactor,
            hook: None,
            now: Arc::new(|| 0),
        }
    }

    async fn settle(registry: &PrecomputeRegistry, messages: &[Message]) -> PrecomputeConsume {
        // consume() waits for an in-flight entry; a Miss after the wait means
        // the entry failed. Used by tests as the settle barrier.
        registry
            .consume(
                &"main".into(),
                messages,
                &default_config(),
                &AbortSignal::new(),
                &|| 0,
            )
            .await
    }

    struct NativeScript {
        owner: std::sync::atomic::AtomicUsize,
        gate: Option<Arc<tokio::sync::Notify>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl Compactor for NativeScript {
        fn native_summary_active(&self) -> bool {
            true
        }
        fn native_precompute_context(&self) -> Option<RuntimeString> {
            Some(
                format!(
                    "source-account-model-{}",
                    self.owner.load(std::sync::atomic::Ordering::SeqCst)
                )
                .into(),
            )
        }
        async fn maybe_compact(
            &self,
            messages: &[Message],
            _: u64,
            config: &ResolvedConfig,
            instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            self.threshold_compact(messages, config, instructions).await
        }
        async fn reactive_compact(
            &self,
            messages: &[Message],
            config: &ResolvedConfig,
            instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            self.threshold_compact(messages, config, instructions).await
        }
        async fn threshold_compact(
            &self,
            messages: &[Message],
            _: &ResolvedConfig,
            _: Option<&RuntimeString>,
        ) -> CompactionResult {
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            CompactionResult {
                messages: if self.fail {
                    messages.to_vec()
                } else {
                    vec![Message::CompactBoundary(CompactBoundaryMessage {
                        uuid: "native-background".into(),
                        summary: RuntimeString::default(),
                        native_compaction: Some(
                            crate::schemas::message::NativeCompactionCheckpoint {
                                source: "fixture-source".into(),
                                auth_scope: "fixture-account".into(),
                                model: default_config().model,
                                comp_hash: None,
                                response_id: "fixture-response".into(),
                                encrypted_content: "opaque-context".into(),
                                retained_input: Vec::new(),
                                retained_input_metadata: Vec::new(),
                            },
                        ),
                    })]
                },
                freed_tokens: 0,
                compacted: !self.fail,
                reason: None,
                diagnostics: Vec::new(),
            }
        }
    }

    fn native_script() -> Arc<NativeScript> {
        Arc::new(NativeScript {
            owner: std::sync::atomic::AtomicUsize::new(0),
            gate: None,
            fail: false,
        })
    }

    #[tokio::test]
    async fn native_precompute_waits_for_a_complete_tool_exchange() {
        use crate::schemas::content::ToolUseBlock;
        use crate::schemas::message::{AssistantMessage, ToolResultMessage};
        let registry = Arc::new(PrecomputeRegistry::default());
        let mut messages = vec![Message::Assistant(AssistantMessage {
            uuid: "call-message".into(),
            model: default_config().model,
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "call".into(),
                name: "read".into(),
                input: crate::runtime_values::RuntimeJson::Object(
                    crate::runtime_values::RuntimeObject::new(),
                ),
                tool_kind: None,
                thought_signature: None,
            })],
        })];
        assert!(!registry.arm(arm_request(native_script(), messages.clone())));
        messages.push(Message::ToolResult(ToolResultMessage {
            uuid: "result-message".into(),
            tool_use_id: "call".into(),
            content: Vec::new(),
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }));
        assert!(registry.arm(arm_request(native_script(), messages)));
    }

    #[tokio::test]
    async fn cancelled_turn_leaves_native_candidate_for_the_next_turn() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let gate = Arc::new(tokio::sync::Notify::new());
        let compactor = Arc::new(NativeScript {
            owner: std::sync::atomic::AtomicUsize::new(0),
            gate: Some(gate.clone()),
            fail: false,
        });
        let snapshot = vec![user("old")];
        let arm = arm_request(compactor.clone(), snapshot.clone());
        let request = arm.request.clone();
        assert!(registry.arm(arm));
        let cancelled = AbortSignal::new();
        cancelled.cancel();
        assert!(matches!(
            registry
                .consume_native(
                    &"main".into(),
                    &snapshot,
                    &default_config(),
                    &cancelled,
                    &|| 0,
                    compactor.as_ref(),
                    &request
                )
                .await,
            PrecomputeConsume::Aborted
        ));
        gate.notify_one();
        assert!(matches!(
            registry
                .consume_native(
                    &"main".into(),
                    &snapshot,
                    &default_config(),
                    &AbortSignal::new(),
                    &|| 0,
                    compactor.as_ref(),
                    &request
                )
                .await,
            PrecomputeConsume::Hit(_)
        ));
    }

    #[tokio::test]
    async fn native_candidate_preserves_live_suffix_and_opaque_checkpoint() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let compactor = native_script();
        let snapshot = vec![user("old")];
        let arm = arm_request(compactor.clone(), snapshot.clone());
        let request = arm.request.clone();
        assert!(registry.arm(arm));
        let mut live = snapshot;
        live.push(user("steer"));
        let PrecomputeConsume::Hit(swap) = registry
            .consume_native(
                &"main".into(),
                &live,
                &default_config(),
                &AbortSignal::new(),
                &|| 0,
                compactor.as_ref(),
                &request,
            )
            .await
        else {
            panic!("expected native checkpoint adoption");
        };
        assert_eq!(swap.result.messages[1..], live[1..]);
        let Message::CompactBoundary(boundary) = &swap.result.messages[0] else {
            panic!("expected native boundary");
        };
        assert_eq!(
            boundary
                .native_compaction
                .as_ref()
                .unwrap()
                .encrypted_content,
            RuntimeString::from("opaque-context")
        );
        let encoded =
            crate::runtime_values::runtime_codec::encode_text(&swap.result.messages).unwrap();
        let restored: Vec<Message> =
            crate::runtime_values::runtime_codec::decode_text(&encoded).unwrap();
        assert_eq!(restored, swap.result.messages);
    }

    #[tokio::test]
    async fn native_candidate_rejects_same_id_rewrites_and_changed_request_context() {
        for mutation in 0..4 {
            let registry = Arc::new(PrecomputeRegistry::default());
            let compactor = native_script();
            let snapshot = vec![user("old")];
            let arm = arm_request(compactor.clone(), snapshot.clone());
            let mut request = arm.request.clone();
            assert!(registry.arm(arm));
            let mut live = snapshot;
            let mut config = default_config();
            match mutation {
                0 => {
                    let Message::User(user) = &mut live[0] else {
                        unreachable!()
                    };
                    user.content.clear();
                }
                1 => {
                    compactor
                        .owner
                        .store(1, std::sync::atomic::Ordering::SeqCst);
                }
                2 => {
                    request
                        .system
                        .push(crate::types::provider::SystemPromptPart {
                            name: "mode".into(),
                            text: "new-mode".into(),
                            cacheable: None,
                            cache_scope: None,
                        });
                }
                _ => {
                    config.model = "changed-model".into();
                }
            }
            assert!(
                matches!(
                    registry
                        .consume_native(
                            &"main".into(),
                            &live,
                            &config,
                            &AbortSignal::new(),
                            &|| 0,
                            compactor.as_ref(),
                            &request
                        )
                        .await,
                    PrecomputeConsume::Miss
                ),
                "mutation {mutation}"
            );
        }
    }

    #[tokio::test]
    async fn native_candidate_rechecks_owner_after_wait_without_blocking_other_work() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let gate = Arc::new(tokio::sync::Notify::new());
        let compactor = Arc::new(NativeScript {
            owner: std::sync::atomic::AtomicUsize::new(0),
            gate: Some(gate.clone()),
            fail: false,
        });
        let snapshot = vec![user("old")];
        let arm = arm_request(compactor.clone(), snapshot.clone());
        let request = arm.request.clone();
        assert!(registry.arm(arm));
        let waiting = tokio::spawn({
            let registry = registry.clone();
            let compactor = compactor.clone();
            async move {
                registry
                    .consume_native(
                        &"main".into(),
                        &snapshot,
                        &default_config(),
                        &AbortSignal::new(),
                        &|| 0,
                        compactor.as_ref(),
                        &request,
                    )
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        compactor
            .owner
            .store(1, std::sync::atomic::Ordering::SeqCst);
        gate.notify_one();
        assert!(matches!(waiting.await.unwrap(), PrecomputeConsume::Miss));
    }

    #[tokio::test]
    async fn native_candidates_are_not_borrowed_and_over_budget_results_fall_back() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let compactor = native_script();
        let snapshot = vec![user("old")];
        let arm = arm_request(compactor.clone(), snapshot.clone());
        let request = arm.request.clone();
        assert!(registry.arm(arm));
        assert!(matches!(
            registry
                .consume_native(
                    &"child".into(),
                    &snapshot,
                    &default_config(),
                    &AbortSignal::new(),
                    &|| 0,
                    compactor.as_ref(),
                    &request
                )
                .await,
            PrecomputeConsume::Miss
        ));
        let mut live = snapshot;
        let mut huge = user("new");
        let Message::User(user) = &mut huge else {
            unreachable!()
        };
        let ContentBlock::Text(text) = &mut user.content[0] else {
            unreachable!()
        };
        text.text = "x".repeat(10_000).into();
        live.push(huge);
        let mut config = default_config();
        config.context_window = 1_000;
        assert!(matches!(
            registry
                .consume_native(
                    &"main".into(),
                    &live,
                    &config,
                    &AbortSignal::new(),
                    &|| 0,
                    compactor.as_ref(),
                    &request
                )
                .await,
            PrecomputeConsume::Miss
        ));
    }

    #[tokio::test]
    async fn native_precompute_keeps_failed_work_out_of_history() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let compactor = Arc::new(NativeScript {
            owner: std::sync::atomic::AtomicUsize::new(0),
            gate: None,
            fail: true,
        });
        let snapshot = vec![user("old")];
        let arm = arm_request(compactor.clone(), snapshot.clone());
        let request = arm.request.clone();
        assert!(registry.arm(arm));
        assert!(matches!(
            registry
                .consume_native(
                    &"main".into(),
                    &snapshot,
                    &default_config(),
                    &AbortSignal::new(),
                    &|| 0,
                    compactor.as_ref(),
                    &request
                )
                .await,
            PrecomputeConsume::Miss
        ));
        assert_eq!(snapshot, vec![user("old")]);
    }

    #[tokio::test]
    async fn ready_checkpoint_splices_messages_after_the_boundary() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = Arc::new(PrecomputeRegistry::default());
        let snapshot = vec![user("u1"), user("u2")];
        assert!(registry.arm(arm_request(
            Arc::new(ScriptedCompactor {
                calls: calls.clone(),
                outcome: ScriptedOutcome::Summary("PRECOMPUTED".into()),
            }),
            snapshot.clone(),
        )));
        // History grew past the arm point: the boundary is u2, so a1/u3 follow.
        let assistant = Message::Assistant(crate::schemas::message::AssistantMessage {
            uuid: "a1".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "done".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: None,
        });
        let live = vec![user("u1"), user("u2"), assistant.clone(), user("u3")];
        match settle(&registry, &live).await {
            PrecomputeConsume::Hit(swap) => {
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                let uuids: Vec<String> = swap
                    .result
                    .messages
                    .iter()
                    .map(|message| query_message_uuid(message).try_to_utf8().unwrap())
                    .collect();
                assert_eq!(
                    uuids,
                    vec![
                        "bg-boundary".to_string(),
                        "a1".to_string(),
                        "u3".to_string()
                    ]
                );
                assert!(swap.result.compacted);
                assert_eq!(swap.messages_since, 2);
                assert!(!swap.borrowed);
            }
            other => panic!("expected hit, got {other:?}"),
        }
        // Consumed entries are removed.
        assert!(!registry
            .entries
            .lock()
            .unwrap()
            .contains_key(&RuntimeString::from("main")));
    }

    #[tokio::test]
    async fn missing_boundary_uuid_falls_back() {
        let registry = Arc::new(PrecomputeRegistry::default());
        registry.arm(arm_request(
            Arc::new(ScriptedCompactor {
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                outcome: ScriptedOutcome::Summary("S".into()),
            }),
            vec![user("u1")],
        ));
        // Live history no longer contains the boundary uuid.
        let live = vec![user("x1"), user("x2")];
        assert!(matches!(
            settle(&registry, &live).await,
            PrecomputeConsume::Miss
        ));
        assert!(!registry
            .entries
            .lock()
            .unwrap()
            .contains_key(&RuntimeString::from("main")));
    }

    #[tokio::test]
    async fn model_switch_invalidates_the_entry() {
        let registry = Arc::new(PrecomputeRegistry::default());
        registry.arm(arm_request(
            Arc::new(ScriptedCompactor {
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                outcome: ScriptedOutcome::Summary("S".into()),
            }),
            vec![user("u1")],
        ));
        let mut config = default_config();
        config.model = "another-model".into();
        let outcome = registry
            .consume(
                &"main".into(),
                &[user("u1")],
                &config,
                &AbortSignal::new(),
                &|| 0,
            )
            .await;
        assert!(matches!(outcome, PrecomputeConsume::Miss));
        assert!(!registry
            .entries
            .lock()
            .unwrap()
            .contains_key(&RuntimeString::from("main")));
    }

    #[tokio::test]
    async fn repeated_failures_stop_rearming() {
        let registry = Arc::new(PrecomputeRegistry::default());
        for round in 0..3 {
            assert!(
                registry.arm(arm_request(
                    Arc::new(ScriptedCompactor {
                        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                        outcome: ScriptedOutcome::Failure,
                    }),
                    vec![user("u1")],
                )),
                "round {round} should arm"
            );
            // Wait for the failure to settle; the entry stays listed as failed.
            assert!(matches!(
                settle(&registry, &[user("u1")]).await,
                PrecomputeConsume::Miss
            ));
        }
        // Fourth arm is refused: the consecutive-failure cap is reached.
        assert!(!registry.arm(arm_request(
            Arc::new(ScriptedCompactor {
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                outcome: ScriptedOutcome::Failure,
            }),
            vec![user("u1")],
        )));
    }

    #[tokio::test]
    async fn borrowed_entry_requires_the_boundary_in_the_borrower_history() {
        let registry = Arc::new(PrecomputeRegistry::default());
        let mut arm = arm_request(
            Arc::new(ScriptedCompactor {
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                outcome: ScriptedOutcome::Summary("S".into()),
            }),
            vec![user("shared-u1")],
        );
        arm.agent_key = "agent-a".into();
        registry.arm(arm);
        // Borrower whose history shares the boundary uuid: hit, marked borrowed.
        match registry
            .consume(
                &"agent-b".into(),
                &[user("shared-u1"), user("b-only")],
                &default_config(),
                &AbortSignal::new(),
                &|| 0,
            )
            .await
        {
            PrecomputeConsume::Hit(swap) => {
                assert!(swap.borrowed);
                assert_eq!(swap.messages_since, 1);
            }
            other => panic!("expected borrowed hit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn registry_drop_cancels_armed_entries() {
        let registry = Arc::new(PrecomputeRegistry::default());
        registry.arm(arm_request(
            Arc::new(ScriptedCompactor {
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                outcome: ScriptedOutcome::Summary("S".into()),
            }),
            vec![user("u1")],
        ));
        let entry = registry
            .entries
            .lock()
            .unwrap()
            .get(&RuntimeString::from("main"))
            .cloned()
            .expect("armed");
        drop(registry);
        assert!(entry.signal.is_cancelled());
    }

    struct BlockingCompactor;

    #[async_trait::async_trait]
    impl Compactor for BlockingCompactor {
        async fn maybe_compact(
            &self,
            _messages: &[Message],
            _estimated_tokens: u64,
            _config: &ResolvedConfig,
            _custom_instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            std::future::pending::<CompactionResult>().await
        }

        async fn reactive_compact(
            &self,
            _messages: &[Message],
            _config: &ResolvedConfig,
            _custom_instructions: Option<&RuntimeString>,
        ) -> CompactionResult {
            std::future::pending::<CompactionResult>().await
        }
    }

    #[tokio::test]
    async fn aborted_wait_keeps_the_entry_armed() {
        let registry = Arc::new(PrecomputeRegistry::default());
        registry.arm(arm_request(Arc::new(BlockingCompactor), vec![user("u1")]));
        let cancelled = AbortSignal::new();
        cancelled.cancel();
        let outcome = registry
            .consume(
                &"main".into(),
                &[user("u1")],
                &default_config(),
                &cancelled,
                &|| 0,
            )
            .await;
        // A cancelled turn stops waiting but leaves the in-flight checkpoint
        // armed so a later turn can still consume it.
        assert!(matches!(outcome, PrecomputeConsume::Aborted));
        assert!(registry
            .entries
            .lock()
            .unwrap()
            .contains_key(&RuntimeString::from("main")));
    }
}
