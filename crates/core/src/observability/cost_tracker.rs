//! In-memory token/USD cost accounting, partitioned by model.

use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::message::Usage;
use crate::schemas::rollout::UsageLedgerItem;
use std::sync::LazyLock;

/// USD price per 1,000,000 tokens, broken out by token class.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelPricing {
    pub input_per_m_tok: f64,
    pub output_per_m_tok: f64,
    pub cache_read_per_m_tok: f64,
    pub cache_write_per_m_tok: f64,
}

impl RuntimeEncode for ModelPricing {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            input_per_m_tok,
            output_per_m_tok,
            cache_read_per_m_tok,
            cache_write_per_m_tok,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("inputPerMTok", input_per_m_tok)?;
        object.field("outputPerMTok", output_per_m_tok)?;
        object.field("cacheReadPerMTok", cache_read_per_m_tok)?;
        object.field("cacheWritePerMTok", cache_write_per_m_tok)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ModelPricing {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                input_per_m_tok: object.required("inputPerMTok", &[])?,
                output_per_m_tok: object.required("outputPerMTok", &[])?,
                cache_read_per_m_tok: object.required("cacheReadPerMTok", &[])?,
                cache_write_per_m_tok: object.required("cacheWritePerMTok", &[])?,
            })
        })
    }
}

/// An ordered pricing table keyed by canonical model-name substring. Order IS
/// precedence — entries are specific-first within each family, so the first key
/// that appears (case-insensitively) inside a model id wins.
pub type PricingTable = Vec<(String, ModelPricing)>;

const fn pricing(input: f64, output: f64, cache_read: f64, cache_write: f64) -> ModelPricing {
    ModelPricing {
        input_per_m_tok: input,
        output_per_m_tok: output,
        cache_read_per_m_tok: cache_read,
        cache_write_per_m_tok: cache_write,
    }
}

/// Pricing keyed by canonical model-name substring, ordered SPECIFIC-FIRST.
///
/// Non-Anthropic rates are PUBLIC LIST PRICES (USD / 1M tokens) maintained here.
/// They drift as vendors reprice — a host that needs exact accounting should
/// inject its own table via `CostTracker::with_pricing` rather than rely on
/// these defaults.
///
/// Order is not cosmetic: [`price_for_in`] takes the FIRST key contained in the
/// model id, so a family entry placed above a specific one silently prices a whole
/// generation at another generation's rates. The Claude block therefore runs from
/// the most specific id to the bare family name, and each family's fallback is the
/// CURRENT generation's rate — a model this build has never heard of should inherit
/// today's price, not a retired one.
///
/// Every rate here is the vendor's published short-context list price, with the
/// 5-minute cache-write column as `cache_write` (the published tables split 5m and
/// 1h writes; this struct carries one). Haiku 5.5's long-prompt tier is selected
/// per request below. A subscription source that knows better injects its own
/// table instead (see the Codex resolver).
pub static DEFAULT_PRICING: LazyLock<PricingTable> = LazyLock::new(|| {
    vec![
        // ---- Anthropic (Claude) ----
        // Fable: $10/$50. Cache reads are 0.025x base input on 5.1, 0.1x on 5.
        (
            "claude-fable-5-1".to_string(),
            pricing(10.0, 50.0, 0.25, 12.5),
        ),
        ("claude-fable-5".to_string(), pricing(10.0, 50.0, 1.0, 12.5)),
        ("claude-fable".to_string(), pricing(10.0, 50.0, 0.25, 12.5)),
        // Opus 5.5 cut Opus pricing to $4/$20 and the cache-read multiplier to
        // 0.05x; 4.5 through 5.0 all sit at $5/$25, and 4/4.1 at $15/$75.
        ("claude-opus-5-5".to_string(), pricing(4.0, 20.0, 0.2, 5.0)),
        ("claude-opus-5".to_string(), pricing(5.0, 25.0, 0.5, 6.25)),
        ("claude-opus-4-8".to_string(), pricing(5.0, 25.0, 0.5, 6.25)),
        ("claude-opus-4-7".to_string(), pricing(5.0, 25.0, 0.5, 6.25)),
        ("claude-opus-4-6".to_string(), pricing(5.0, 25.0, 0.5, 6.25)),
        ("claude-opus-4-5".to_string(), pricing(5.0, 25.0, 0.5, 6.25)),
        (
            "claude-opus-4-1".to_string(),
            pricing(15.0, 75.0, 1.5, 18.75),
        ),
        ("claude-opus-4".to_string(), pricing(15.0, 75.0, 1.5, 18.75)),
        ("claude-opus".to_string(), pricing(4.0, 20.0, 0.2, 5.0)),
        // Sonnet 5.5 keeps the 5.0 base price but halves the cache-read rate.
        (
            "claude-sonnet-5-5".to_string(),
            pricing(2.0, 10.0, 0.1, 2.5),
        ),
        ("claude-sonnet-5".to_string(), pricing(2.0, 10.0, 0.2, 2.5)),
        (
            "claude-sonnet-4-6".to_string(),
            pricing(3.0, 15.0, 0.3, 3.75),
        ),
        (
            "claude-sonnet-4-5".to_string(),
            pricing(3.0, 15.0, 0.3, 3.75),
        ),
        ("claude-sonnet-4".to_string(), pricing(3.0, 15.0, 0.3, 3.75)),
        ("claude-sonnet".to_string(), pricing(2.0, 10.0, 0.1, 2.5)),
        // Haiku 5.5 lowers the short-context rate; older sessions retain theirs.
        (
            "claude-haiku-5-5".to_string(),
            pricing(0.1, 0.5, 0.01, 0.125),
        ),
        ("claude-haiku-4-5".to_string(), pricing(1.0, 5.0, 0.1, 1.25)),
        ("claude-haiku-3-5".to_string(), pricing(0.8, 4.0, 0.08, 1.0)),
        ("claude-haiku".to_string(), pricing(0.1, 0.5, 0.01, 0.125)),
        // ---- OpenAI (public list price) ----
        ("gpt-4o-mini".to_string(), pricing(0.15, 0.6, 0.075, 0.15)),
        ("gpt-4o".to_string(), pricing(2.5, 10.0, 1.25, 2.5)),
        ("gpt-4.1-nano".to_string(), pricing(0.1, 0.4, 0.025, 0.1)),
        ("gpt-4.1-mini".to_string(), pricing(0.4, 1.6, 0.1, 0.4)),
        ("gpt-4.1".to_string(), pricing(2.0, 8.0, 0.5, 2.0)),
        ("o3-mini".to_string(), pricing(1.1, 4.4, 0.55, 1.1)),
        ("o1-mini".to_string(), pricing(1.1, 4.4, 0.55, 1.1)),
        ("o1".to_string(), pricing(15.0, 60.0, 7.5, 15.0)),
        // ---- Google (Gemini, public list price) ----
        (
            "gemini-2.5-flash-lite".to_string(),
            pricing(0.1, 0.4, 0.025, 0.1),
        ),
        (
            "gemini-2.5-flash".to_string(),
            pricing(0.3, 2.5, 0.075, 0.3),
        ),
        (
            "gemini-2.5-pro".to_string(),
            pricing(1.25, 10.0, 0.31, 1.25),
        ),
        ("gemini".to_string(), pricing(1.25, 10.0, 0.31, 1.25)),
    ]
});

/// Used when no pricing entry matches the model. Zero — an UNKNOWN/unpriced
/// model contributes $0 rather than being silently charged at a wrong rate.
pub const FALLBACK_PRICING: ModelPricing = pricing(0.0, 0.0, 0.0, 0.0);

/// First entry in `table` whose key is a (case-insensitive) substring of
/// `model`, else `fallback`. Order within `table` is precedence
/// (specific-first).
pub fn price_for_in(
    model: &str,
    table: &[(String, ModelPricing)],
    fallback: ModelPricing,
) -> ModelPricing {
    let haystack = model.to_lowercase();
    for (key, p) in table {
        if haystack.contains(&key.to_lowercase()) {
            return *p;
        }
    }
    fallback
}

/// First `DEFAULT_PRICING` entry whose key is a substring of `model`, else the
/// fallback.
pub fn price_for(model: &str) -> ModelPricing {
    price_for_in(model, &DEFAULT_PRICING, FALLBACK_PRICING)
}

fn price_for_request_in(
    model: &str,
    usage: &Usage,
    table: &PricingTable,
    fallback: ModelPricing,
) -> ModelPricing {
    let base = price_for_in(model, table, fallback);
    // The high-context rate applies to the entire request, including cached
    // tokens. Only the built-in table gets this rule; injected rates remain the
    // caller's decision.
    if model.eq_ignore_ascii_case("claude-haiku-5-5")
        && table == &*DEFAULT_PRICING
        && usage
            .input_tokens
            .saturating_add(usage.cache_read_input_tokens)
            .saturating_add(usage.cache_creation_input_tokens)
            > 100_000
    {
        pricing(0.5, 2.5, 0.05, 0.625)
    } else {
        base
    }
}

/// Accumulated usage and computed cost for a single model.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelUsageTotals {
    pub model: RuntimeString,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cost_usd: f64,
    pub requests: u64,
}

impl RuntimeEncode for ModelUsageTotals {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("model", &self.model)?;
        object.field("inputTokens", &self.input_tokens)?;
        object.field("outputTokens", &self.output_tokens)?;
        object.field("cacheReadInputTokens", &self.cache_read_input_tokens)?;
        object.field(
            "cacheCreationInputTokens",
            &self.cache_creation_input_tokens,
        )?;
        object.field("costUsd", &self.cost_usd)?;
        object.field("requests", &self.requests)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ModelUsageTotals {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model: object.required("model", &[])?,
                input_tokens: object.required("inputTokens", &[])?,
                output_tokens: object.required("outputTokens", &[])?,
                cache_read_input_tokens: object.required("cacheReadInputTokens", &[])?,
                cache_creation_input_tokens: object.required("cacheCreationInputTokens", &[])?,
                cost_usd: object.required("costUsd", &[])?,
                requests: object.required("requests", &[])?,
            })
        })
    }
}

/// Token usage summed across all models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TotalUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
}

impl RuntimeEncode for TotalUsage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("inputTokens", input_tokens)?;
        object.field("outputTokens", output_tokens)?;
        object.field("cacheReadInputTokens", cache_read_input_tokens)?;
        object.field("cacheCreationInputTokens", cache_creation_input_tokens)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for TotalUsage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                input_tokens: object.required("inputTokens", &[])?,
                output_tokens: object.required("outputTokens", &[])?,
                cache_read_input_tokens: object.required("cacheReadInputTokens", &[])?,
                cache_creation_input_tokens: object.required("cacheCreationInputTokens", &[])?,
            })
        })
    }
}

/// A per-model snapshot entry: the running totals plus that model's hit rate.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelUsageSnapshot {
    pub totals: ModelUsageTotals,
    pub cache_hit_rate: f64,
}

impl RuntimeEncode for ModelUsageSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("model", &self.totals.model)?;
        object.field("inputTokens", &self.totals.input_tokens)?;
        object.field("outputTokens", &self.totals.output_tokens)?;
        object.field("cacheReadInputTokens", &self.totals.cache_read_input_tokens)?;
        object.field(
            "cacheCreationInputTokens",
            &self.totals.cache_creation_input_tokens,
        )?;
        object.field("costUsd", &self.totals.cost_usd)?;
        object.field("requests", &self.totals.requests)?;
        object.field("cacheHitRate", &self.cache_hit_rate)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ModelUsageSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                totals: ModelUsageTotals {
                    model: object.required("model", &[])?,
                    input_tokens: object.required("inputTokens", &[])?,
                    output_tokens: object.required("outputTokens", &[])?,
                    cache_read_input_tokens: object.required("cacheReadInputTokens", &[])?,
                    cache_creation_input_tokens: object
                        .required("cacheCreationInputTokens", &[])?,
                    cost_usd: object.required("costUsd", &[])?,
                    requests: object.required("requests", &[])?,
                },
                cache_hit_rate: object.required("cacheHitRate", &[])?,
            })
        })
    }
}

/// A one-call cache-effectiveness snapshot for hosts.
#[derive(Debug, Clone, PartialEq)]
pub struct CostSnapshot {
    pub total_cost_usd: f64,
    pub cache_hit_rate: f64,
    pub models: Vec<ModelUsageSnapshot>,
}

impl RuntimeEncode for CostSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("totalCostUsd", &self.total_cost_usd)?;
        object.field("cacheHitRate", &self.cache_hit_rate)?;
        object.field("models", &self.models)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CostSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                total_cost_usd: object.required("totalCostUsd", &[])?,
                cache_hit_rate: object.required("cacheHitRate", &[])?,
                models: object.required("models", &[])?,
            })
        })
    }
}

/// Cache reads as a fraction of all prompt tokens (input + read + creation), in
/// 0..1. 0 when there are no prompt tokens.
pub fn cache_hit_rate_of(
    input_tokens: u64,
    cache_read_input_tokens: u64,
    cache_creation_input_tokens: u64,
) -> f64 {
    let total = input_tokens + cache_read_input_tokens + cache_creation_input_tokens;
    if total == 0 {
        0.0
    } else {
        cache_read_input_tokens as f64 / total as f64
    }
}

fn cost_of(usage: &Usage, pricing: &ModelPricing) -> f64 {
    (usage.input_tokens as f64 / 1e6) * pricing.input_per_m_tok
        + (usage.output_tokens as f64 / 1e6) * pricing.output_per_m_tok
        + (usage.cache_read_input_tokens as f64 / 1e6) * pricing.cache_read_per_m_tok
        + (usage.cache_creation_input_tokens as f64 / 1e6) * pricing.cache_write_per_m_tok
}

/// In-memory accumulator of token usage and USD cost, partitioned by model. Cost
/// is computed at `add` time from the model's pricing so later pricing changes do
/// not retroactively alter already-recorded totals.
pub struct CostTracker {
    // A Vec preserves first-seen model order (mirrors the JS Map insertion
    // order that `by_model` and iteration rely on).
    totals: Vec<ModelUsageTotals>,
    pricing: PricingTable,
    fallback: ModelPricing,
}

impl Default for CostTracker {
    fn default() -> Self {
        CostTracker::new()
    }
}

impl CostTracker {
    /// A tracker using [`DEFAULT_PRICING`] and the zero [`FALLBACK_PRICING`].
    pub fn new() -> Self {
        CostTracker {
            totals: Vec::new(),
            pricing: DEFAULT_PRICING.clone(),
            fallback: FALLBACK_PRICING,
        }
    }

    /// A tracker with an injected pricing table (and the zero fallback).
    pub fn with_pricing(pricing: PricingTable) -> Self {
        CostTracker {
            totals: Vec::new(),
            pricing,
            fallback: FALLBACK_PRICING,
        }
    }

    /// A tracker with an injected pricing table and fallback.
    pub fn with_pricing_and_fallback(pricing: PricingTable, fallback: ModelPricing) -> Self {
        CostTracker {
            totals: Vec::new(),
            pricing,
            fallback,
        }
    }

    /// Replace rates for future records without recalculating historical spend.
    pub fn replace_pricing(&mut self, pricing: Option<PricingTable>) {
        self.pricing = pricing.unwrap_or_else(|| DEFAULT_PRICING.clone());
        self.fallback = FALLBACK_PRICING;
    }

    /// Fold one provider usage report for `model` into the running totals.
    pub fn add(&mut self, model: &str, usage: &Usage) {
        let cost = cost_of(
            usage,
            &price_for_request_in(model, usage, &self.pricing, self.fallback),
        );
        self.add_with_cost(model, usage, cost);
    }

    /// Record one completed provider request and return its immutable billing
    /// record for append-only persistence.
    pub fn record(
        &mut self,
        request_id: impl Into<RuntimeString>,
        assistant_message_id: impl Into<RuntimeString>,
        model: &str,
        usage: &Usage,
    ) -> UsageLedgerItem {
        self.record_with_pricing(
            request_id,
            assistant_message_id,
            model,
            usage,
            &self.pricing.clone(),
        )
    }

    pub fn record_with_pricing(
        &mut self,
        request_id: impl Into<RuntimeString>,
        assistant_message_id: impl Into<RuntimeString>,
        model: &str,
        usage: &Usage,
        pricing: &PricingTable,
    ) -> UsageLedgerItem {
        self.record_attributed(
            request_id,
            assistant_message_id,
            model,
            usage,
            pricing,
            UsageAttribution::default(),
        )
    }

    /// Record one completed request against the pricing table that captured it.
    ///
    /// `model` is the wire model name and drives the price lookup. The optional
    /// attribution carries the request's non-secret identity and, when the
    /// request rode an auxiliary target, the source-qualified display name that
    /// keeps two providers sharing one wire model id from aggregating into a
    /// single ledger row.
    pub fn record_attributed(
        &mut self,
        request_id: impl Into<RuntimeString>,
        assistant_message_id: impl Into<RuntimeString>,
        model: &str,
        usage: &Usage,
        pricing: &PricingTable,
        attribution: UsageAttribution<'_>,
    ) -> UsageLedgerItem {
        let record = UsageLedgerItem {
            inference: attribution.inference,
            request_id: request_id.into(),
            assistant_message_id: assistant_message_id.into(),
            model: attribution.ledger_model.unwrap_or(model).into(),
            usage: usage.clone(),
            cost_usd: cost_of(
                usage,
                &price_for_request_in(model, usage, pricing, self.fallback),
            ),
        };
        self.restore(&record);
        record
    }

    /// The source-qualified ledger name for a bundle that carries one, so an
    /// auxiliary target's usage never aggregates into the main model's row.
    pub fn source_qualified_ledger_model(
        bundle_source: Option<&(String, String)>,
    ) -> Option<String> {
        bundle_source.map(|(source, model_id)| {
            format!(
                "{}:{}",
                escape_identity_part(source),
                escape_identity_part(model_id)
            )
        })
    }

    /// Keep catalog identities separate even when providers use the same wire
    /// model name. Rates are captured by the request, not read from a live map.
    pub fn record_target(
        &mut self,
        request_id: impl Into<RuntimeString>,
        assistant_message_id: impl Into<RuntimeString>,
        identity: (&str, &str),
        model: &str,
        usage: &Usage,
        pricing: &PricingTable,
    ) -> UsageLedgerItem {
        let record = UsageLedgerItem {
            inference: None,
            request_id: request_id.into(),
            assistant_message_id: assistant_message_id.into(),
            model: format!(
                "{}:{}",
                escape_identity_part(identity.0),
                escape_identity_part(identity.1)
            )
            .into(),
            usage: usage.clone(),
            cost_usd: cost_of(
                usage,
                &price_for_request_in(model, usage, pricing, FALLBACK_PRICING),
            ),
        };
        self.restore(&record);
        record
    }

    /// Restore a persisted provider request without consulting the current
    /// pricing table.
    pub fn restore(&mut self, record: &UsageLedgerItem) {
        let model = record
            .model
            .try_to_utf8()
            .expect("persisted provider model ids are scalar strings");
        self.add_with_cost(&model, &record.usage, record.cost_usd);
    }

    fn add_with_cost(&mut self, model: &str, usage: &Usage, cost: f64) {
        let entry = match self.totals.iter_mut().find(|e| e.model == model) {
            Some(e) => e,
            None => {
                self.totals.push(ModelUsageTotals {
                    model: model.into(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cost_usd: 0.0,
                    requests: 0,
                });
                self.totals.last_mut().unwrap()
            }
        };
        entry.input_tokens += usage.input_tokens;
        entry.output_tokens += usage.output_tokens;
        entry.cache_read_input_tokens += usage.cache_read_input_tokens;
        entry.cache_creation_input_tokens += usage.cache_creation_input_tokens;
        entry.cost_usd += cost;
        entry.requests += 1;
    }

    /// Total USD cost summed across all models.
    pub fn total_cost_usd(&self) -> f64 {
        self.totals.iter().map(|e| e.cost_usd).sum()
    }

    /// Token usage summed across all models.
    pub fn total_usage(&self) -> TotalUsage {
        let mut acc = TotalUsage::default();
        for e in &self.totals {
            acc.input_tokens += e.input_tokens;
            acc.output_tokens += e.output_tokens;
            acc.cache_read_input_tokens += e.cache_read_input_tokens;
            acc.cache_creation_input_tokens += e.cache_creation_input_tokens;
        }
        acc
    }

    /// Cache hit rate across ALL prompt tokens processed.
    pub fn cache_hit_rate(&self) -> f64 {
        let u = self.total_usage();
        cache_hit_rate_of(
            u.input_tokens,
            u.cache_read_input_tokens,
            u.cache_creation_input_tokens,
        )
    }

    /// Per-model totals (a fresh copy of each entry).
    pub fn by_model(&self) -> Vec<ModelUsageTotals> {
        self.totals.clone()
    }

    /// Immutable view of total cost, the cache hit rate (overall + per model),
    /// and the per-model usage breakdown.
    pub fn snapshot(&self) -> CostSnapshot {
        CostSnapshot {
            total_cost_usd: self.total_cost_usd(),
            cache_hit_rate: self.cache_hit_rate(),
            models: self
                .by_model()
                .into_iter()
                .map(|m| {
                    let rate = cache_hit_rate_of(
                        m.input_tokens,
                        m.cache_read_input_tokens,
                        m.cache_creation_input_tokens,
                    );
                    ModelUsageSnapshot {
                        totals: m,
                        cache_hit_rate: rate,
                    }
                })
                .collect(),
        }
    }

    /// Drop all accumulated totals.
    pub fn reset(&mut self) {
        self.totals.clear();
    }
}

/// Optional attribution for one billing record.
///
/// Grouped into a struct because both halves are optional and independent: an
/// event needs the capture identity, while a ledger row for an auxiliary target
/// needs the source-qualified display name. Keeping them positional made the
/// recorder exceed its argument budget.
#[derive(Default)]
pub struct UsageAttribution<'a> {
    /// The non-secret identity of the inference capture that produced the request.
    pub inference: Option<crate::types::events::InferenceIdentity>,
    /// Display name override for the ledger row; `None` keeps the wire model name.
    pub ledger_model: Option<&'a str>,
}

fn escape_identity_part(value: &str) -> String {
    value.replace('%', "%25").replace(':', "%3A")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(input: u64, output: u64, read: u64, creation: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: read,
            cache_creation_input_tokens: creation,
            total_tokens: None,
        }
    }

    #[test]
    fn global_model_tiers_price_identity_encoding_is_unambiguous_and_restorable() {
        let mut tracker = CostTracker::new();
        let rates = |price| {
            vec![(
                "same-name".to_string(),
                ModelPricing {
                    input_per_m_tok: price,
                    output_per_m_tok: 0.0,
                    cache_read_per_m_tok: 0.0,
                    cache_write_per_m_tok: 0.0,
                },
            )]
        };
        let first = tracker.record_target(
            "r1",
            "a1",
            ("x", "y:z"),
            "same-name",
            &usage(1_000_000, 0, 0, 0),
            &rates(2.0),
        );
        let second = tracker.record_target(
            "r2",
            "a2",
            ("x:y", "z"),
            "same-name",
            &usage(1_000_000, 0, 0, 0),
            &rates(7.0),
        );
        assert_ne!(first.model, second.model);
        assert_eq!(tracker.snapshot().models.len(), 2);
        assert_eq!(tracker.snapshot().total_cost_usd, 9.0);
        let mut restored = CostTracker::new();
        restored.restore(&first);
        restored.restore(&second);
        assert_eq!(restored.snapshot(), tracker.snapshot());
    }

    #[test]
    fn cache_hit_rate_reads_over_all_prompt_tokens() {
        assert_eq!(cache_hit_rate_of(10, 90, 0), 0.9);
        assert_eq!(cache_hit_rate_of(10, 0, 90), 0.0);
        assert!((cache_hit_rate_of(20, 60, 20) - 0.6).abs() < 1e-10);
        assert_eq!(cache_hit_rate_of(0, 0, 0), 0.0);
    }

    #[test]
    fn overall_hit_rate_with_creation_in_denominator() {
        let mut t = CostTracker::new();
        t.add("claude-sonnet-4-6", &usage(10, 0, 0, 90));
        assert_eq!(t.cache_hit_rate(), 0.0);
        t.add("claude-sonnet-4-6", &usage(10, 0, 90, 0));
        assert!((t.cache_hit_rate() - 0.45).abs() < 1e-10);
    }

    #[test]
    fn price_for_matches_specific_first() {
        assert_eq!(price_for("gpt-4o-mini-2024-07-18").input_per_m_tok, 0.15);
        assert_eq!(price_for("gpt-4o-2024-08-06").input_per_m_tok, 2.5);
        assert_eq!(price_for("gpt-4.1-mini").input_per_m_tok, 0.4);
        assert_eq!(price_for("gpt-4.1-nano").input_per_m_tok, 0.1);
        assert_eq!(price_for("gpt-4.1").input_per_m_tok, 2.0);
        assert_eq!(price_for("claude-sonnet-4-6").output_per_m_tok, 15.0);
        assert_eq!(price_for("claude-opus-4-8").output_per_m_tok, 25.0);
        assert_eq!(price_for("gemini-2.5-flash-lite").input_per_m_tok, 0.1);
        assert_eq!(price_for("gemini-2.5-flash").input_per_m_tok, 0.3);
        assert_eq!(price_for("gemini-2.5-pro").input_per_m_tok, 1.25);
        assert_eq!(price_for("gemini-3-pro-preview").input_per_m_tok, 1.25);
    }

    /// Every model the subscription can currently run has to hit its OWN row.
    ///
    /// This is the guard the previous table lacked: the keys were bare family
    /// substrings carrying one generation's rates, so a newer model inherited an
    /// older generation's price (Opus 5 billed at Opus 4.1's $15/$75), and Fable —
    /// which had no key at all — fell through to the zero fallback and billed a
    /// $10/$50 model as free. Asserting each id's full four rates pins the exact
    /// row, so reordering or collapsing a family entry fails here rather than
    /// silently re-pricing.
    #[test]
    fn every_current_claude_model_prices_at_its_published_rate() {
        for (model, input, output, cache_read, cache_write) in [
            // The lineup the subscription actually resolves to.
            ("claude-fable-5-1", 10.0, 50.0, 0.25, 12.5),
            ("claude-opus-5-5", 4.0, 20.0, 0.2, 5.0),
            ("claude-sonnet-5-5", 2.0, 10.0, 0.1, 2.5),
            ("claude-haiku-5-5", 0.1, 0.5, 0.01, 0.125),
            // Older ids a stored session can still name. These must keep their own
            // generation's rates: a resumed thread re-prices on replay only if the
            // table moved under it.
            ("claude-fable-5", 10.0, 50.0, 1.0, 12.5),
            ("claude-opus-5", 5.0, 25.0, 0.5, 6.25),
            ("claude-opus-4-8", 5.0, 25.0, 0.5, 6.25),
            ("claude-opus-4-1", 15.0, 75.0, 1.5, 18.75),
            ("claude-sonnet-5", 2.0, 10.0, 0.2, 2.5),
            ("claude-sonnet-4-6", 3.0, 15.0, 0.3, 3.75),
            ("claude-haiku-4-5", 1.0, 5.0, 0.1, 1.25),
            ("claude-haiku-3-5", 0.8, 4.0, 0.08, 1.0),
        ] {
            let p = price_for(model);
            assert_eq!(p.input_per_m_tok, input, "{model} input");
            assert_eq!(p.output_per_m_tok, output, "{model} output");
            assert_eq!(p.cache_read_per_m_tok, cache_read, "{model} cache read");
            assert_eq!(p.cache_write_per_m_tok, cache_write, "{model} cache write");
        }
    }

    #[test]
    fn haiku_55_prices_each_request_by_its_full_prompt_length() {
        let model = "claude-haiku-5-5";
        let short = pricing(0.1, 0.5, 0.01, 0.125);
        let long = pricing(0.5, 2.5, 0.05, 0.625);
        let at_threshold = usage(100_000, 1_000, 0, 0);
        let over_threshold = usage(100_001, 1_000, 0, 0);
        let cache_crosses_threshold = usage(0, 1_000, 100_000, 1);
        assert_eq!(
            price_for_request_in(model, &at_threshold, &DEFAULT_PRICING, FALLBACK_PRICING),
            short
        );
        for tokens in [&over_threshold, &cache_crosses_threshold] {
            assert_eq!(
                price_for_request_in(model, tokens, &DEFAULT_PRICING, FALLBACK_PRICING),
                long
            );
        }

        let mut tracker = CostTracker::new();
        let record = tracker.record("request", "message", model, &cache_crosses_threshold);
        let expected = cost_of(&cache_crosses_threshold, &long);
        assert!((record.cost_usd - expected).abs() < 1e-12);
        assert!((tracker.total_cost_usd() - expected).abs() < 1e-12);
        let mut added = CostTracker::new();
        added.add(model, &over_threshold);
        assert!((added.total_cost_usd() - cost_of(&over_threshold, &long)).abs() < 1e-12);
        let targeted = added.record_target(
            "target-request",
            "target-message",
            ("subscription", "haiku"),
            model,
            &over_threshold,
            &DEFAULT_PRICING,
        );
        assert!((targeted.cost_usd - cost_of(&over_threshold, &long)).abs() < 1e-12);

        let custom = vec![(model.to_string(), pricing(7.0, 8.0, 9.0, 10.0))];
        let override_record = tracker.record_with_pricing(
            "custom-request",
            "custom-message",
            model,
            &over_threshold,
            &custom,
        );
        assert!((override_record.cost_usd - cost_of(&over_threshold, &custom[0].1)).abs() < 1e-12);
    }

    /// A bare family name is the fallback for an id this build does not know, and
    /// it has to price at the CURRENT generation rather than a retired one: an
    /// unknown-but-newer model billed at an old rate is a wrong number nobody sees.
    #[test]
    fn an_unknown_family_member_inherits_the_current_generation_rate() {
        assert_eq!(price_for("claude-fable-9").input_per_m_tok, 10.0);
        assert_eq!(price_for("claude-opus-9").input_per_m_tok, 4.0);
        assert_eq!(price_for("claude-sonnet-9").input_per_m_tok, 2.0);
        assert_eq!(price_for("claude-haiku-9").input_per_m_tok, 0.1);
    }

    #[test]
    fn unmatched_model_gets_zero_fallback() {
        let p = price_for("some-unknown-llm");
        assert_eq!(p.input_per_m_tok, 0.0);
        assert_eq!(p.output_per_m_tok, 0.0);
    }

    #[test]
    fn restore_uses_recorded_cost_instead_of_current_pricing() {
        let usage = usage(1_000_000, 0, 0, 0);
        let mut original =
            CostTracker::with_pricing(vec![("model-a".to_string(), pricing(2.0, 0.0, 0.0, 0.0))]);
        let record = original.record("req-1", "assistant-1", "model-a", &usage);
        assert_eq!(record.cost_usd, 2.0);

        let mut resumed =
            CostTracker::with_pricing(vec![("model-a".to_string(), pricing(100.0, 0.0, 0.0, 0.0))]);
        resumed.restore(&record);
        assert_eq!(resumed.total_cost_usd(), 2.0);
        assert_eq!(resumed.by_model()[0].requests, 1);
    }

    #[test]
    fn price_for_in_honors_injected_table() {
        let table = vec![("my-model".to_string(), pricing(7.0, 9.0, 1.0, 2.0))];
        let fallback = pricing(1.0, 1.0, 1.0, 1.0);
        assert_eq!(
            price_for_in("acme/my-model-v2", &table, fallback).input_per_m_tok,
            7.0
        );
        assert_eq!(price_for_in("other", &table, fallback).input_per_m_tok, 1.0);
    }

    #[test]
    fn unpriced_model_zero_cost_tokens_accumulate() {
        let mut t = CostTracker::new();
        t.add("some-unknown-llm", &usage(1_000_000, 1_000_000, 0, 0));
        assert_eq!(t.total_cost_usd(), 0.0);
        assert_eq!(t.by_model()[0].input_tokens, 1_000_000);
    }

    #[test]
    fn injected_table_prices_custom_model() {
        let mut t =
            CostTracker::with_pricing(vec![("acme-llm".to_string(), pricing(2.0, 10.0, 0.5, 0.0))]);
        t.add("acme-llm-v2", &usage(1_000_000, 0, 0, 0));
        assert!((t.total_cost_usd() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn snapshot_exposes_overall_and_per_model_hit_rate() {
        let mut t = CostTracker::new();
        t.add("claude-sonnet-4-6", &usage(10, 0, 90, 0));
        t.add("claude-haiku-4-5", &usage(100, 0, 0, 0));
        let snap = t.snapshot();
        assert!((snap.cache_hit_rate - 90.0 / 200.0).abs() < 1e-10);
        let sonnet = snap
            .models
            .iter()
            .find(|m| m.totals.model == "claude-sonnet-4-6")
            .unwrap();
        let haiku = snap
            .models
            .iter()
            .find(|m| m.totals.model == "claude-haiku-4-5")
            .unwrap();
        assert!((sonnet.cache_hit_rate - 0.9).abs() < 1e-10);
        assert_eq!(haiku.cache_hit_rate, 0.0);
    }

    #[test]
    fn cost_snapshot_runtime_codec_preserves_exact_model_string() {
        let snapshot = CostSnapshot {
            total_cost_usd: 1.25,
            cache_hit_rate: 0.5,
            models: vec![ModelUsageSnapshot {
                totals: ModelUsageTotals {
                    model: RuntimeString::from_utf16(vec![0xd800, u16::from(b'x')]),
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_read_input_tokens: 3,
                    cache_creation_input_tokens: 4,
                    cost_usd: 1.25,
                    requests: 1,
                },
                cache_hit_rate: 0.375,
            }],
        };

        let encoded = runtime_codec::encode_value(&snapshot).expect("encode cost snapshot");
        let decoded = CostSnapshot::decode_runtime(&encoded).expect("decode cost snapshot");

        assert_eq!(decoded, snapshot);
    }
}
