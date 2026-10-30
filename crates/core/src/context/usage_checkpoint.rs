//! Durable cumulative context accounting, separate from model-visible history.

use crate::context::total_tokens_reminder::PaddedCountdownSpend;
use crate::runtime_values::{RuntimeJson, RuntimeObject};
use crate::schemas::rollout::RolloutItem;

pub const CONTEXT_USAGE_ROLLOUT_EVENT: &str = "context_usage_checkpoint";

/// Decimal strings retain exact counters independently of the JSON number range.
pub fn encode_checkpoint(spend: PaddedCountdownSpend) -> RuntimeJson {
    let mut payload = RuntimeObject::new();
    payload.insert("version", RuntimeJson::String("1".into()));
    payload.insert(
        "retiredTokens",
        RuntimeJson::String(spend.base_tokens.to_string().into()),
    );
    payload.insert(
        "highWaterTokens",
        RuntimeJson::String(spend.high_water_tokens.to_string().into()),
    );
    RuntimeJson::Object(payload)
}

pub fn decode_checkpoint(payload: &RuntimeJson) -> Option<PaddedCountdownSpend> {
    if payload.get("version")?.as_str()? != "1" {
        return None;
    }
    let counter = |key| {
        let text = payload.get(key)?.as_str()?.try_to_utf8().ok()?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        text.parse::<u64>().ok()
    };
    Some(PaddedCountdownSpend {
        base_tokens: counter("retiredTokens")?,
        high_water_tokens: counter("highWaterTokens")?,
    })
}

/// Checkpoints contain cumulative totals. Replayed records must never be added.
pub fn checkpoint_from_rollout(items: &[RolloutItem]) -> PaddedCountdownSpend {
    let mut spend = PaddedCountdownSpend::default();
    for item in items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != CONTEXT_USAGE_ROLLOUT_EVENT {
            continue;
        }
        let Some(snapshot) = event.payload.as_ref().and_then(decode_checkpoint) else {
            continue;
        };
        spend.base_tokens = spend.base_tokens.max(snapshot.base_tokens);
        spend.high_water_tokens = spend.high_water_tokens.max(snapshot.high_water_tokens);
    }
    spend
}
