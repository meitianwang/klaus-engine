//! Cost-gated sliding-TTL prompt-cache keep-alive for idle interactive sessions.
//!
//! When a user pauses for several minutes to inspect diffs or review an answer
//! after a turn that warmed a substantial prompt cache (`>= 16,384` cached
//! input tokens), a 5-minute provider cache TTL would otherwise expire right
//! before their follow-up prompt — forcing a full `1.25x` cold cache rewrite
//! and multi-second prefill stall.
//!
//! Replaying the completed turn's exact admitted `ModelRequest` prefix with
//! minimal `max_output_tokens` refreshes the server-side cache entry at `0.1x`
//! cache-read cost without mutating session history, rollout persistence, or UI
//! events. Strictly bounded to at most two idle refreshes per user turn and
//! cancelled immediately on foreground turn admission, manual compaction, model
//! switch, or session shutdown.

use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::context::runtime_reminders::{is_antigravity_source, is_prefix_cached_compat_source};
use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::schemas::rollout::SessionRole;
use crate::sync_ext::MutexExt;
use crate::types::provider::{
    CacheTtl, ModelProvider, ModelRequest, ModelStreamEvent, ThinkingConfig,
};

use super::{InferenceBundle, InferenceSlot, Session};

/// Minimum cached prefix size (`cache_read_input_tokens + cache_creation_input_tokens`)
/// where a `0.1x` keep-alive read comfortably pays for itself against a cold rewrite.
pub(crate) const MIN_KEEPALIVE_CACHED_TOKENS: u64 = 16_384;

/// Refresh cadence ahead of standard 5-minute sliding prompt-cache eviction.
pub(crate) const FIVE_MIN_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(240);

/// Refresh cadence ahead of extended 1-hour prompt-cache eviction.
pub(crate) const ONE_HOUR_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(3_300);

/// Hard ceiling on consecutive idle refreshes per user turn so an unattended
/// window never loops indefinitely.
pub(crate) const MAX_CONSECUTIVE_KEEPALIVES: u8 = 2;

/// Per-ping wall-clock timeout; failures silently stop the idle loop.
pub(crate) const KEEPALIVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

const MIN_EXPLICIT_THINKING_BUDGET_TOKENS: u64 = 1_024;

pub(crate) struct PromptCacheKeepAlive {
    task: JoinHandle<()>,
    cancel: CancellationToken,
}

impl PromptCacheKeepAlive {
    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

impl Drop for PromptCacheKeepAlive {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

pub(crate) fn keepalive_interval_for_request(req: &ModelRequest) -> Duration {
    match req.cache_ttl {
        Some(CacheTtl::OneHour) => ONE_HOUR_KEEPALIVE_INTERVAL,
        None => FIVE_MIN_KEEPALIVE_INTERVAL,
    }
}

/// Build a prefix-identical keep-alive request from the completed turn's last
/// wire `ModelRequest`, minimizing output generation while preserving every
/// field that participates in provider prompt-cache key derivation.
pub(crate) fn prepare_keepalive_request(source: &ModelRequest) -> ModelRequest {
    let mut ping = source.clone();
    ping.skip_cache_write = None;
    ping.output_schema = None;
    ping.task_budget = None;
    match ping.thinking {
        Some(ThinkingConfig::Enabled {
            budget_tokens: Some(_),
        }) => {
            ping.thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: Some(MIN_EXPLICIT_THINKING_BUDGET_TOKENS),
            });
            ping.max_output_tokens = MIN_EXPLICIT_THINKING_BUDGET_TOKENS + 1;
        }
        _ => {
            ping.max_output_tokens = 1;
        }
    }
    ping
}

pub(crate) fn is_keepalive_eligible_bundle(
    bundle: &InferenceBundle,
    source_request: &ModelRequest,
    warm_tokens: u64,
) -> bool {
    if source_request.messages.is_empty()
        || source_request.skip_cache_write == Some(true)
        || warm_tokens < MIN_KEEPALIVE_CACHED_TOKENS
    {
        return false;
    }
    if is_antigravity_source(&bundle.config)
        || bundle
            .config
            .model_source
            .as_ref()
            .and_then(|source| source.source.as_deref())
            == Some("codex")
    {
        return false;
    }
    if !bundle.provider.has_prewarm_endpoints() {
        return false;
    }
    let model = &bundle.config.model;
    let caps = bundle.provider.capabilities(model);
    let info = bundle.provider.info();
    let profile = resolve_provider_behavior_profile(Some(info), model);
    caps.prompt_cache_control
        || profile.family == Some("openai")
        || is_prefix_cached_compat_source(&bundle.config, Some(info))
}

async fn run_single_keepalive_ping(
    provider: &Arc<dyn ModelProvider>,
    request: ModelRequest,
    cancel: &CancellationToken,
) -> Option<u64> {
    let ping_signal = cancel.child_token();
    let outcome = tokio::time::timeout(KEEPALIVE_REQUEST_TIMEOUT, async {
        let mut stream = provider.stream(request, ping_signal.clone());
        let mut terminal_warm_tokens: Option<u64> = None;
        while let Some(ev) = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                ping_signal.cancel();
                return None;
            }
            next = stream.next() => next,
        } {
            match ev {
                ModelStreamEvent::MessageStop { usage, .. } => {
                    terminal_warm_tokens = Some(
                        usage
                            .cache_read_input_tokens
                            .saturating_add(usage.cache_creation_input_tokens),
                    );
                }
                ModelStreamEvent::Error { .. } | ModelStreamEvent::RetryScheduled { .. } => {
                    ping_signal.cancel();
                    return None;
                }
                _ => {}
            }
        }
        terminal_warm_tokens
    })
    .await;
    match outcome {
        Ok(Some(warm)) => Some(warm),
        _ => {
            ping_signal.cancel();
            None
        }
    }
}

impl Session {
    pub(crate) fn cancel_cache_keepalive(&self) {
        if let Some(active) = self.cache_keepalive.lock_recover().take() {
            active.cancel();
        }
    }

    pub(crate) fn arm_cache_keepalive_after_turn(
        self: &Arc<Self>,
        slot: &InferenceSlot,
        initial: &Arc<InferenceBundle>,
        last_wire_request: Option<ModelRequest>,
        warm_tokens: u64,
    ) {
        self.cancel_cache_keepalive();
        if !self.config.prewarm || self.lifecycle.is_disposed() {
            return;
        }
        if self.meta.role == Some(SessionRole::SideTask) {
            return;
        }
        let Some(source_request) = last_wire_request else {
            return;
        };
        let bundle = Session::bundle_from_slot(slot, initial);
        if !is_keepalive_eligible_bundle(&bundle, &source_request, warm_tokens) {
            return;
        }
        let revision = bundle.revision;
        if self.model_revision() != revision {
            return;
        }
        let interval = keepalive_interval_for_request(&source_request);
        let first_deadline = tokio::time::Instant::now() + interval;
        let ping_template = prepare_keepalive_request(&source_request);
        let provider = bundle.provider.clone();
        let weak_session: Weak<Session> = Arc::downgrade(self);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();

        let task = tokio::spawn(async move {
            let mut next_deadline = first_deadline;
            for round in 0..MAX_CONSECUTIVE_KEEPALIVES {
                tokio::select! {
                    biased;
                    _ = worker_cancel.cancelled() => return,
                    _ = tokio::time::sleep_until(next_deadline) => {}
                }
                let Some(session) = weak_session.upgrade() else {
                    return;
                };
                if session.lifecycle.is_disposed()
                    || session.model_revision() != revision
                    || !session.turn.active_stack.lock_recover().is_empty()
                {
                    return;
                }
                drop(session);

                let Some(refreshed_warm_tokens) =
                    run_single_keepalive_ping(&provider, ping_template.clone(), &worker_cancel)
                        .await
                else {
                    debug!(
                        round = round + 1,
                        "prompt cache keep-alive: stopped after failed or cancelled ping"
                    );
                    return;
                };

                let Some(session) = weak_session.upgrade() else {
                    return;
                };
                if session.lifecycle.is_disposed() || session.model_revision() != revision {
                    return;
                }
                session.turn.mark_response_at_ms((session.deps.now)());
                debug!(
                    round = round + 1,
                    refreshed_warm_tokens, "prompt cache keep-alive: refreshed sliding cache ttl"
                );
                if refreshed_warm_tokens < MIN_KEEPALIVE_CACHED_TOKENS {
                    return;
                }
                next_deadline = tokio::time::Instant::now() + interval;
            }
        });

        let mut guard = self.cache_keepalive.lock_recover();
        if !self.lifecycle.is_disposed() && self.model_revision() == revision {
            *guard = Some(PromptCacheKeepAlive { task, cancel });
        } else {
            cancel.cancel();
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{Message, UserMessage};
    use crate::types::provider::SystemPromptPart;

    fn base_request() -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model: "claude-sonnet-4-6".into(),
            image_limits: None,
            max_media_blocks: None,
            system: vec![SystemPromptPart {
                name: "base".into(),
                text: "You are a helpful coding assistant.".into(),
                cacheable: Some(true),
                cache_scope: None,
            }],
            messages: vec![Message::User(UserMessage {
                question_answer: None,
                uuid: "u-1".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "Inspect cache prefix".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            })],
            tools: Vec::new(),
            native_deferred_tools: None,
            deferred_tools: Vec::new(),
            surfaced_deferred_names: Vec::new(),
            wire_caps: None,
            frozen_tools: None,
            frozen_instructions: None,
            tool_inventory_events: Vec::new(),
            tool_choice: None,
            max_output_tokens: 16_384,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: Some(ThinkingConfig::Adaptive),
            effort: None,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            skip_cache_write: Some(true),
            cache_ttl: None,
            cache_key: Some("session-cache-key".into()),
            cache_domain: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[test]
    fn prepare_keepalive_request_preserves_prefix_and_clamps_output_budget() {
        let mut adaptive = base_request();
        adaptive.skip_cache_write = None;
        let ping = prepare_keepalive_request(&adaptive);
        assert_eq!(ping.max_output_tokens, 1);
        assert_eq!(ping.skip_cache_write, None);
        assert_eq!(ping.system, adaptive.system);
        assert_eq!(ping.messages, adaptive.messages);
        assert_eq!(ping.cache_key, adaptive.cache_key);
        assert_eq!(
            keepalive_interval_for_request(&adaptive),
            FIVE_MIN_KEEPALIVE_INTERVAL
        );

        let mut explicit_budget = base_request();
        explicit_budget.cache_ttl = Some(CacheTtl::OneHour);
        explicit_budget.thinking = Some(ThinkingConfig::Enabled {
            budget_tokens: Some(8_192),
        });
        let budget_ping = prepare_keepalive_request(&explicit_budget);
        assert_eq!(
            budget_ping.thinking,
            Some(ThinkingConfig::Enabled {
                budget_tokens: Some(1_024)
            })
        );
        assert_eq!(budget_ping.max_output_tokens, 1_025);
        assert_eq!(
            keepalive_interval_for_request(&explicit_budget),
            ONE_HOUR_KEEPALIVE_INTERVAL
        );
    }
}
