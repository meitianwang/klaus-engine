//! Cache-miss attribution: diff each request's prefix fingerprint against the
//! previous request on the same cache-routing identity, name the cause, and
//! track the prompt epoch.
//!
//! The usage ledger says how much was read from cache but never why a request
//! missed. This module answers "who moved the prefix": every request's
//! fingerprint is compared segment by segment against the stored snapshot for
//! its routing key, and the moved segments become a reason list in the
//! request's lifecycle record. When no local segment moved, the gap since the
//! previous request decides between a TTL expiry and a server-side cause.
//!
//! Attribution is advisory and must never block or fail a request: a missing
//! snapshot degrades to `cold_start`, and the registry is a bounded in-memory
//! map. The lifecycle record carrying these fields is already persisted to the
//! timing log, which is the exportable trail; no prompt content enters it
//! (only digests, counts and reason names).

use super::prefix_fingerprint::PrefixFingerprint;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default prompt-cache lifetime on the wires (short retention).
const TTL_SHORT_MS: u64 = 5 * 60 * 1000;
/// Long-retention prompt-cache lifetime.
const TTL_LONG_MS: u64 = 60 * 60 * 1000;
/// Coarse bound on tracked routing identities; attribution is best-effort.
const MAX_ENTRIES: usize = 64;

#[derive(Clone)]
pub struct PreviousSnapshot {
    pub fingerprint: PrefixFingerprint,
    pub at_ms: u64,
    pub epoch: u64,
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<String, (u64, PreviousSnapshot)>,
    next_seq: u64,
}

fn registry() -> &'static Mutex<RegistryState> {
    static REGISTRY: std::sync::LazyLock<Mutex<RegistryState>> =
        std::sync::LazyLock::new(|| Mutex::new(RegistryState::default()));
    &REGISTRY
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Attribute one request against the stored snapshot for `routing_key`, then
/// store this request as the new snapshot. Returns the lifecycle-record fields.
pub fn attribute(
    routing_key: Option<&str>,
    retention_long: bool,
    current: &PrefixFingerprint,
) -> Value {
    let key = routing_key.unwrap_or("default").to_string();
    let now = now_ms();
    let mut state = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = state.entries.remove(&key).map(|(_, snapshot)| snapshot);
    let (reasons, continues_prefix, epoch) =
        classify(previous.as_ref(), current, now, retention_long);
    if state.entries.len() >= MAX_ENTRIES {
        if let Some(oldest_key) = state
            .entries
            .iter()
            .min_by_key(|(_, (seq, _))| *seq)
            .map(|(k, _)| k.clone())
        {
            state.entries.remove(&oldest_key);
        }
    }
    state.next_seq = state.next_seq.saturating_add(1);
    let seq = state.next_seq;
    state.entries.insert(
        key,
        (
            seq,
            PreviousSnapshot {
                fingerprint: current.clone(),
                at_ms: now,
                epoch,
            },
        ),
    );
    json!({
        "prefixMissReasons": reasons,
        "prefixContinuesPrefix": continues_prefix,
        "prefixPromptEpoch": epoch,
        "prefixSinceLastMs": previous.map(|snapshot| now.saturating_sub(snapshot.at_ms)),
    })
}

/// Pure classification, split out so the reason vocabulary is unit-testable
/// without the registry or the clock.
pub(crate) fn classify(
    previous: Option<&PreviousSnapshot>,
    current: &PrefixFingerprint,
    now_ms: u64,
    retention_long: bool,
) -> (Vec<&'static str>, bool, u64) {
    let Some(previous) = previous else {
        return (vec!["cold_start"], false, 0);
    };
    let prev = &previous.fingerprint;
    let mut reasons: Vec<&'static str> = Vec::new();
    let mut epoch = previous.epoch;

    // A model switch re-signs thinking blocks and re-resolves every wire
    // setting: a new prompt epoch by definition.
    if prev.model != current.model {
        reasons.push("model_changed");
        epoch += 1;
    }

    // History: shrink = compaction; same count with a moved digest = in-place
    // rewrite; growth checks exact per-prefix digests for multi-message turns
    // (assistant tool_use + tool_result) with a 1-message fallback.
    let continues_prefix = current.history_messages > prev.history_messages
        && ((prev.history_messages == current.history_messages.saturating_sub(1)
            && prev.history_hash == current.history_prefix_hash)
            || prev
                .history_messages
                .checked_sub(1)
                .and_then(|idx| current.history_prefix_hashes.get(idx))
                .is_some_and(|prefix| prefix == &prev.history_hash));
    if current.history_messages < prev.history_messages {
        reasons.push("messages_rewritten");
        epoch += 1;
    } else if current.history_messages == prev.history_messages {
        if current.history_hash != prev.history_hash {
            reasons.push("messages_rewritten");
            epoch += 1;
        }
    } else if !continues_prefix {
        reasons.push("history_prefix_changed");
    }

    // The inventory stream and newly unlocked native-deferred registrations stay
    // outside the cached top-level breakpoint: only baseline or immediate moves
    // count as `tools_changed`.
    let top_level_moved = match (&prev.frozen_tools_hash, &current.frozen_tools_hash) {
        (Some(before), Some(after)) => before != after,
        _ => {
            prev.immediate_tools_hash != current.immediate_tools_hash
                || (prev.tools_hash != current.tools_hash
                    && !(prev.defer_unlock_active || current.defer_unlock_active))
        }
    };
    if top_level_moved {
        reasons.push("tools_changed");
    }
    if prev.stable_system_hash != current.stable_system_hash {
        reasons.push("system_changed");
    }
    if prev.effort_hash != current.effort_hash {
        reasons.push("effort_changed");
    }
    if prev.thinking_hash != current.thinking_hash {
        reasons.push("thinking_changed");
    }
    if prev.retention_hash != current.retention_hash {
        reasons.push("cache_scope_or_ttl_changed");
    }
    if prev.defer_hash != current.defer_hash {
        reasons.push("defer_loading_changed");
    }
    if prev.wire_caps_hash != current.wire_caps_hash {
        reasons.push("betas_changed");
    }

    if reasons.is_empty() && !continues_prefix && current.history_messages > prev.history_messages {
        // Growth without continuation was already named above; nothing to add.
    }
    if reasons.is_empty() {
        let elapsed = now_ms.saturating_sub(previous.at_ms);
        let ttl = if retention_long {
            TTL_LONG_MS
        } else {
            TTL_SHORT_MS
        };
        if elapsed > ttl {
            reasons.push(if retention_long {
                "ttl_expired_1h"
            } else {
                "ttl_expired_5m"
            });
        } else {
            reasons.push("likely_server_side");
        }
    }
    (reasons, continues_prefix, epoch)
}

/// Record a protocol downgrade rebuild (a rejected native tool shape folded
/// back into the plain inventory). One prefix rebuild is paid per downgrade;
/// this entry is what makes that rebuild attributable in the ledger.
pub fn record_downgrade_rebuild(protocol: &str, model: &str, status: Option<u16>) {
    crate::observability::provider_diagnostics::record_named(
        crate::observability::provider_diagnostics::TIMING_LOG_FILE_NAME,
        "downgrade_rebuild",
        json!({
            "protocol": protocol,
            "model": model,
            "status": status,
            "reason": "downgrade_rebuild",
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint() -> PrefixFingerprint {
        PrefixFingerprint::of(&crate::observability::prefix_fingerprint::tests::request())
    }

    fn snapshot(fingerprint: PrefixFingerprint, at_ms: u64, epoch: u64) -> PreviousSnapshot {
        PreviousSnapshot {
            fingerprint,
            at_ms,
            epoch,
        }
    }

    #[test]
    fn first_request_is_cold_start() {
        let (reasons, continues, epoch) = classify(None, &fingerprint(), 1000, false);
        assert_eq!(reasons, vec!["cold_start"]);
        assert!(!continues);
        assert_eq!(epoch, 0);
    }

    #[test]
    fn identical_prefix_within_ttl_is_server_side() {
        let current = fingerprint();
        // Simulate the previous request being the current one minus its last
        // message: its full digest equals the current prefix digest, so the
        // cached region continued.
        let mut previous_fp = fingerprint();
        previous_fp.history_hash = current.history_prefix_hash.clone();
        previous_fp.history_messages = current.history_messages - 1;
        let previous = snapshot(previous_fp, 1000, 3);
        let (reasons, continues, epoch) = classify(Some(&previous), &current, 2000, false);
        assert_eq!(reasons, vec!["likely_server_side"]);
        assert!(continues);
        assert_eq!(epoch, 3);

        // A byte-identical resend (nothing appended) does not "continue": the
        // prefix digest still excludes the last message.
        let previous = snapshot(fingerprint(), 1000, 3);
        let (reasons, continues, epoch) = classify(Some(&previous), &current, 2000, false);
        assert_eq!(reasons, vec!["likely_server_side"]);
        assert!(!continues);
        assert_eq!(epoch, 3);
    }

    #[test]
    fn identical_prefix_past_ttl_is_expiry() {
        let current = fingerprint();
        let previous = snapshot(fingerprint(), 1000, 0);
        let (reasons, _, _) = classify(Some(&previous), &current, 1000 + TTL_SHORT_MS + 1, false);
        assert_eq!(reasons, vec!["ttl_expired_5m"]);
        let previous = snapshot(fingerprint(), 1000, 0);
        let (reasons, _, _) = classify(Some(&previous), &current, 1000 + TTL_LONG_MS + 1, true);
        assert_eq!(reasons, vec!["ttl_expired_1h"]);
    }

    #[test]
    fn moved_segments_name_their_reasons() {
        let mut current = fingerprint();
        current.tools_hash = "moved".into();
        current.stable_system_hash = "moved".into();
        current.effort_hash = "moved".into();
        current.thinking_hash = "moved".into();
        current.retention_hash = "moved".into();
        current.defer_hash = "moved".into();
        current.wire_caps_hash = "moved".into();
        let previous = snapshot(fingerprint(), 1000, 0);
        let (reasons, _, _) = classify(Some(&previous), &current, 2000, false);
        assert_eq!(
            reasons,
            vec![
                "tools_changed",
                "system_changed",
                "effort_changed",
                "thinking_changed",
                "cache_scope_or_ttl_changed",
                "defer_loading_changed",
                "betas_changed",
            ]
        );
    }

    #[test]
    fn frozen_baseline_shields_the_stream_from_tool_reasons() {
        // Under a frozen top level, an inventory-stream append must not read as
        // tools_changed; only a moved baseline does.
        let mut base = fingerprint();
        base.frozen_tools_hash = Some("frozen".into());
        let mut current = base.clone();
        current.inventory_events = 1;
        current.inventory_events_hash = "event".into();
        let previous = snapshot(base.clone(), 1000, 0);
        let (reasons, _, _) = classify(Some(&previous), &current, 2000, false);
        assert_eq!(reasons, vec!["likely_server_side"]);

        let mut rebuilt = base.clone();
        rebuilt.frozen_tools_hash = Some("rebased".into());
        let previous = snapshot(base, 1000, 0);
        let (reasons, _, _) = classify(Some(&previous), &rebuilt, 2000, false);
        assert_eq!(reasons, vec!["tools_changed"]);
    }

    #[test]
    fn model_switch_and_history_shrink_bump_the_epoch() {
        let mut current = fingerprint();
        current.model = "other-model".into();
        current.history_messages = 0;
        let mut previous_fp = fingerprint();
        previous_fp.history_messages = 4;
        let previous = snapshot(previous_fp, 1000, 7);
        let (reasons, _, epoch) = classify(Some(&previous), &current, 2000, false);
        assert!(reasons.contains(&"model_changed"));
        assert!(reasons.contains(&"messages_rewritten"));
        assert_eq!(epoch, 9);
    }

    #[test]
    fn multi_message_turn_continues_prefix_and_deferred_unlock_does_not_claim_tools_changed() {
        let mut req1 = crate::observability::prefix_fingerprint::tests::request();
        req1.native_deferred_tools = Some(true);
        let fp1 = PrefixFingerprint::of(&req1);

        let mut req2 = req1.clone();
        req2.messages
            .push(crate::schemas::message::Message::Assistant(
                crate::schemas::message::AssistantMessage {
                    uuid: "a-turn-2".into(),
                    content: vec![crate::schemas::content::ContentBlock::Text(
                        crate::schemas::content::TextBlock {
                            text: "searching".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        },
                    )],
                    model: "claude-opus-5-5".into(),
                    usage: None,
                },
            ));
        req2.messages
            .push(crate::schemas::message::Message::ToolResult(
                crate::schemas::message::ToolResultMessage {
                    uuid: "tr-turn-2".into(),
                    tool_use_id: "call-1".into(),
                    content: Vec::new(),
                    is_error: false,
                    tool_kind: None,
                    native_tool_output: None,
                    output: None,
                    extra_user_blocks: None,
                },
            ));
        let mut unlocked_spec = req2.tools[0].clone();
        unlocked_spec.name = "unlocked_deferred".into();
        req2.tools.push(unlocked_spec);
        req2.surfaced_deferred_names = vec!["unlocked_deferred".into()];
        let fp2 = PrefixFingerprint::of(&req2);

        let previous = snapshot(fp1, 1000, 0);
        let (reasons, continues, _) = classify(Some(&previous), &fp2, 2000, false);
        assert!(continues, "multi-message append must continue prefix");
        assert_eq!(reasons, vec!["likely_server_side"]);
    }

    #[test]
    fn registry_evicts_oldest_entry_instead_of_clearing_all_sessions() {
        let mut base = crate::observability::prefix_fingerprint::tests::request();
        base.messages
            .push(crate::schemas::message::Message::Assistant(
                crate::schemas::message::AssistantMessage {
                    uuid: "a-evict".into(),
                    content: Vec::new(),
                    model: "claude-opus-5-5".into(),
                    usage: None,
                },
            ));
        let fp = PrefixFingerprint::of(&base);
        let hot_key = "attr-evict-hot";
        let oldest_key = "attr-evict-oldest";
        let _ = attribute(Some(oldest_key), false, &fp);
        for i in 0..MAX_ENTRIES {
            let _ = attribute(Some(hot_key), false, &fp);
            let k = format!("attr-evict-fill-{i}");
            let _ = attribute(Some(&k), false, &fp);
        }
        let follow = attribute(Some(hot_key), false, &fp);
        assert_eq!(
            follow["prefixMissReasons"],
            serde_json::json!(["likely_server_side"]),
            "active key must survive unrelated churn beyond MAX_ENTRIES"
        );
    }
}
