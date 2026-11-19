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

fn registry() -> &'static Mutex<HashMap<String, PreviousSnapshot>> {
    static REGISTRY: std::sync::LazyLock<Mutex<HashMap<String, PreviousSnapshot>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
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
    let mut registry = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = registry.remove(&key);
    let (reasons, continues_prefix, epoch) =
        classify(previous.as_ref(), current, now, retention_long);
    if registry.len() >= MAX_ENTRIES {
        registry.clear();
    }
    registry.insert(
        key,
        PreviousSnapshot {
            fingerprint: current.clone(),
            at_ms: now,
            epoch,
        },
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
    // rewrite; growth that does not continue the previous full digest = the
    // cached prefix itself moved.
    let continues_prefix = prev.history_hash == current.history_prefix_hash;
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

    // The inventory stream moving is NOT a bust under a frozen top level: only
    // the segment the cached prefix actually carries counts.
    let top_level_moved = match (&prev.frozen_tools_hash, &current.frozen_tools_hash) {
        (Some(before), Some(after)) => before != after,
        _ => prev.tools_hash != current.tools_hash,
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
}
