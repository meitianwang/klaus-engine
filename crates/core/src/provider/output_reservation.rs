//! Request-local recovery when a provider rejects the reserved output budget.

use std::sync::LazyLock;

use regex::Regex;

use crate::provider::classify::RawProviderError;
use crate::runtime_values::RuntimeJson as Value;

const OUTPUT_HEADROOM: u64 = 1_000;
const MIN_OUTPUT_TOKENS: u64 = 3_000;

static OUTPUT_CONTEXT_OVERFLOW: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"input length and `max_tokens` exceed context limit:\s*(\d+)\s*\+\s*(\d+)\s*>\s*(\d+)",
    )
    .expect("valid output reservation overflow pattern")
});

/// Reduce only an output limit already present on the wire. A strict decrease
/// and a usable output floor prevent repeated rejections from spinning.
pub(crate) fn reduce_output_reservation(body: &mut Value, error: &RawProviderError) -> Option<u64> {
    if error.status != Some(400) {
        return None;
    }
    let captures = OUTPUT_CONTEXT_OVERFLOW.captures(&error.message)?;
    let input = captures.get(1)?.as_str().parse::<u64>().ok()?;
    let _rejected_output = captures.get(2)?.as_str().parse::<u64>().ok()?;
    let window = captures.get(3)?.as_str().parse::<u64>().ok()?;
    let adjusted = window.saturating_sub(input).saturating_sub(OUTPUT_HEADROOM);
    if adjusted < MIN_OUTPUT_TOKENS {
        return None;
    }

    let mut fields = ["max_tokens", "max_completion_tokens", "max_output_tokens"]
        .into_iter()
        .filter_map(|field| {
            body.get(field)
                .and_then(Value::as_u64)
                .map(|cap| (field, cap))
        });
    let (field, current) = fields.next()?;
    if fields.next().is_some() || adjusted >= current {
        return None;
    }
    body[field] = Value::from(adjusted);
    let thinking_budget = body
        .get("thinking")
        .filter(|thinking| {
            thinking
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "enabled")
        })
        .and_then(|thinking| thinking.get("budget_tokens"))
        .and_then(Value::as_u64);
    if let Some(budget) = thinking_budget {
        body["thinking"]["budget_tokens"] = Value::from(budget.min(adjusted - 1));
    }
    Some(adjusted)
}
