//! Typed consumer API over resolved prompt fragments.
//!
//! Business consumers never read raw catalog JSON. They build
//! [`ModelFragments`] from the model id and the source declaration riding
//! with the model metadata, then call the typed accessors. The declaration
//! may arrive in the legacy per-consumer shape (nested `multi_agent` /
//! `auto_review` / `guardian_v2` objects) or in the slot-keyed shape
//! (`{ "slots": { "<slot id>": { "text": … } } }`); both normalize into the
//! same declared resolution layer, preserving the original consumer
//! semantics of each field (notably: an empty string on a disableable
//! delegation slot disables the part, while an empty review text stays a
//! literal override).

use crate::runtime_values::{RuntimeJson, RuntimeString};

use super::catalog::SlotEntry;
use super::resolve::{self, PromptCatalogKey, ResolvedFragments};
use super::slots::{
    SlotSpec, SLOT_ADVISOR_GUIDANCE, SLOT_ADVISOR_ROLE, SLOT_ADVISOR_SUMMARY, SLOT_AGENT_CHILD,
    SLOT_AGENT_MODE_EXPLICIT, SLOT_AGENT_MODE_HINT, SLOT_AGENT_MODE_PROACTIVE, SLOT_AGENT_ROOT,
    SLOT_BASE_INSTRUCTIONS, SLOT_COMPACT_LEAN_SYSTEM, SLOT_COMPACT_SUMMARY_TEMPLATE,
    SLOT_MEMORY_CONSOLIDATION_QUALITY, SLOT_MEMORY_LEARNING_QUALITY, SLOT_MODE_DEFAULT,
    SLOT_MODE_PLAN, SLOT_ORCHESTRATION_GUIDANCE, SLOT_RESPONSE_COMMENTARY, SLOT_REVIEW_GUARDIAN_V2,
    SLOT_TITLE_INSTRUCTIONS,
};

/// The fragments bound to one inference: catalog layers plus the source
/// declaration, resolved leaf-by-leaf.
#[derive(Clone, Debug)]
pub struct ModelFragments {
    resolved: ResolvedFragments,
}

impl ModelFragments {
    /// Resolve for a model id, deriving its family for family-keyed defaults
    /// and the family pack.
    pub fn for_model(model: &RuntimeString, declared: Option<&RuntimeJson>) -> Self {
        let family =
            crate::provider::behavior::resolve_provider_behavior_profile(None, model).family;
        Self::for_key(&PromptCatalogKey::for_model(model), family, declared)
    }

    /// Resolve with an already-known model family (request paths that carry
    /// the behavior profile pass it directly). No access-route information is
    /// available, so only wildcard bindings can match.
    pub fn for_family(family: Option<&str>, declared: Option<&RuntimeJson>) -> Self {
        let key = PromptCatalogKey {
            source_kind: RuntimeString::from(""),
            source_scope: RuntimeString::from(""),
            model_key: RuntimeString::from(""),
            variant_key: RuntimeString::from("standard"),
        };
        Self::for_key(&key, family, declared)
    }

    /// Resolve for a model on a known access route (`sourceKind` from the
    /// session's inference source). Route-scoped bindings only match when the
    /// source kind is present.
    pub fn for_route(
        model: &RuntimeString,
        source_kind: Option<&str>,
        declared: Option<&RuntimeJson>,
    ) -> Self {
        let family =
            crate::provider::behavior::resolve_provider_behavior_profile(None, model).family;
        let key = PromptCatalogKey {
            source_kind: RuntimeString::from(source_kind.unwrap_or("")),
            source_scope: RuntimeString::from(""),
            model_key: model.clone(),
            variant_key: RuntimeString::from("standard"),
        };
        Self::for_key(&key, family, declared)
    }

    /// Resolve for a session/feature configuration: the model plus, when the
    /// session recorded one, its access-route source kind.
    pub fn for_config(
        config: &crate::schemas::config::ResolvedConfig,
        declared: Option<&RuntimeJson>,
    ) -> Self {
        let source_kind = config
            .model_source
            .as_ref()
            .and_then(|source| source.source.as_deref());
        Self::for_route(&config.model, source_kind, declared)
    }

    /// Resolve with a pre-derived family and a known route/model. Request
    /// paths that already carry the behavior profile use this to avoid
    /// re-deriving the family.
    pub fn for_route_with_family(
        model: &RuntimeString,
        source_kind: Option<&str>,
        family: Option<&str>,
        declared: Option<&RuntimeJson>,
    ) -> Self {
        let key = PromptCatalogKey {
            source_kind: RuntimeString::from(source_kind.unwrap_or("")),
            source_scope: RuntimeString::from(""),
            model_key: model.clone(),
            variant_key: RuntimeString::from("standard"),
        };
        Self::for_key(&key, family, declared)
    }

    /// Full-form resolution for callers that know the access route.
    pub fn for_key(
        key: &PromptCatalogKey,
        family: Option<&str>,
        declared: Option<&RuntimeJson>,
    ) -> Self {
        let catalog = super::active();
        Self {
            resolved: resolve::resolve(&catalog, key, family, declared),
        }
    }

    pub fn resolved(&self) -> &ResolvedFragments {
        &self.resolved
    }

    /// Base model instructions (required slot; always non-empty).
    pub fn base_instructions(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_BASE_INSTRUCTIONS)
    }

    /// Intermediate-commentary policy text; `None` when the route disabled
    /// the progress channel.
    pub fn commentary(&self) -> Option<&RuntimeString> {
        self.resolved.text(SLOT_RESPONSE_COMMENTARY)
    }

    /// Collaboration-mode template (required slot; the bundled default is the
    /// shipped template). User configuration overrides stay at the consumer:
    /// they outrank the catalog by design.
    pub fn collaboration_template(&self, plan: bool) -> RuntimeString {
        self.resolved.required_text(if plan {
            SLOT_MODE_PLAN
        } else {
            SLOT_MODE_DEFAULT
        })
    }

    /// Delegation role text for the current agent; `None` when disabled, in
    /// which case the consumer omits the part.
    pub fn agent_role(&self, is_child: bool) -> Option<&RuntimeString> {
        self.resolved.text(if is_child {
            SLOT_AGENT_CHILD
        } else {
            SLOT_AGENT_ROOT
        })
    }

    /// Effective delegation-mode text: the configured hint outranks the
    /// catalog hint, which outranks the explicit/proactive slot. A disabled
    /// hint slot suppresses the mode text entirely (the pre-catalog consumer
    /// treated an explicitly cleared hint as "no mode text"), and a disabled
    /// mode slot likewise yields `None` so the consumer omits the part.
    pub fn agent_mode(
        &self,
        ultra_mode: bool,
        configured_hint: Option<&RuntimeString>,
    ) -> Option<RuntimeString> {
        if let Some(hint) = configured_hint {
            return Some(hint.clone());
        }
        if self.resolved.is_disabled(SLOT_AGENT_MODE_HINT) {
            return None;
        }
        if let Some(hint) = self.resolved.text(SLOT_AGENT_MODE_HINT) {
            return Some(hint.clone());
        }
        self.resolved
            .text(if ultra_mode {
                SLOT_AGENT_MODE_PROACTIVE
            } else {
                SLOT_AGENT_MODE_EXPLICIT
            })
            .cloned()
    }

    /// Review-model text slots. Empty strings are returned as-is: the review
    /// policy module treats them as literal overrides.
    pub fn review_text(&self, slot: &'static str) -> Option<&RuntimeString> {
        self.resolved.text(slot)
    }

    /// Structured classifier settings declared through the fragment channel.
    pub fn guardian_v2(&self) -> Option<&RuntimeJson> {
        self.resolved.json(SLOT_REVIEW_GUARDIAN_V2)
    }

    // ---- Specialized inference purposes ----
    // Each binds to the model that actually receives that request.

    /// Title-generation instructions (required slot).
    pub fn title_instructions(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_TITLE_INSTRUCTIONS)
    }

    /// Advisor-tool guidance shown to the calling model (required slot).
    pub fn advisor_guidance(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_ADVISOR_GUIDANCE)
    }

    /// Consultation role of the advisor model's own request (required slot).
    pub fn advisor_role(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_ADVISOR_ROLE)
    }

    /// Context-summarizer instructions for advisor consultations (required).
    pub fn advisor_summary(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_ADVISOR_SUMMARY)
    }

    /// Orchestration-mode guidance for the planning model (required slot).
    pub fn orchestration_guidance(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_ORCHESTRATION_GUIDANCE)
    }

    /// Compaction summary template (required slot). A user-configured
    /// `compact_prompt` outranks this at the consumer.
    pub fn compact_summary_template(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_COMPACT_SUMMARY_TEMPLATE)
    }

    /// Stripped-fallback system line for overflowed compaction requests.
    pub fn compact_lean_system(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_COMPACT_LEAN_SYSTEM)
    }

    /// Quality bar for the memory learning/maintenance fork.
    pub fn memory_learning_quality(&self) -> RuntimeString {
        self.resolved.required_text(SLOT_MEMORY_LEARNING_QUALITY)
    }

    /// Quality bar for memory consolidation.
    pub fn memory_consolidation_quality(&self) -> RuntimeString {
        self.resolved
            .required_text(SLOT_MEMORY_CONSOLIDATION_QUALITY)
    }
}

/// Legacy declaration path → registered slot id, for the shapes existing
/// sources ship today.
fn legacy_text_slot(path: &[&str]) -> Option<(&'static str, bool)> {
    // Returns (slot id, disableable-empty semantics).
    let joined = path.join(".");
    match joined.as_str() {
        "multi_agent.role.root" | "multiAgent.role.root" => Some((SLOT_AGENT_ROOT, true)),
        "multi_agent.role.subagent" | "multiAgent.role.subagent" => Some((SLOT_AGENT_CHILD, true)),
        "multi_agent.mode.hint_text"
        | "multi_agent.mode.hintText"
        | "multiAgent.mode.hint_text"
        | "multiAgent.mode.hintText" => Some((SLOT_AGENT_MODE_HINT, true)),
        "multi_agent.mode.explicit" | "multiAgent.mode.explicit" => {
            Some((SLOT_AGENT_MODE_EXPLICIT, true))
        }
        "multi_agent.mode.proactive" | "multiAgent.mode.proactive" => {
            Some((SLOT_AGENT_MODE_PROACTIVE, true))
        }
        "auto_review.policy" => Some((super::slots::SLOT_REVIEW_POLICY, false)),
        "auto_review.policy_template" => Some((super::slots::SLOT_REVIEW_POLICY_TEMPLATE, false)),
        "auto_review.node_repl_policy" => Some((super::slots::SLOT_REVIEW_NODE_REPL_POLICY, false)),
        "auto_review.rejection_instructions" => {
            Some((super::slots::SLOT_REVIEW_REJECTION_INSTRUCTIONS, false))
        }
        "auto_review.timeout_instructions" => {
            Some((super::slots::SLOT_REVIEW_TIMEOUT_INSTRUCTIONS, false))
        }
        _ => None,
    }
}

/// Normalize a source declaration into slot entries for the declared
/// resolution layer. Accepts the slot-keyed form and the legacy nested form;
/// unknown keys are ignored (they may belong to other consumers or future
/// slots).
pub fn normalize_declared(declared: &RuntimeJson) -> Vec<(RuntimeString, SlotEntry)> {
    let Some(root) = declared.as_object() else {
        return Vec::new();
    };

    // Slot-keyed form: { "slots": { "<slot>": { "text" | "json" | "disabled" } | null } }
    if let Some(slots) = root.get("slots").and_then(RuntimeJson::as_object) {
        let mut entries = Vec::with_capacity(slots.len());
        for (slot, value) in slots.entries() {
            let entry = match value {
                RuntimeJson::Null => SlotEntry::Inherit,
                RuntimeJson::Object(entry) => {
                    if entry.get("disabled").and_then(RuntimeJson::as_bool) == Some(true) {
                        SlotEntry::Disabled
                    } else if let Some(text) = entry.get("text").and_then(RuntimeJson::as_string) {
                        SlotEntry::Text(text.clone())
                    } else if let Some(json) = entry.get("json") {
                        SlotEntry::Json(json.clone())
                    } else {
                        continue;
                    }
                }
                RuntimeJson::String(text) => SlotEntry::Text(text.clone()),
                _ => continue,
            };
            entries.push((slot.clone(), entry));
        }
        return entries;
    }

    // Legacy nested form.
    let mut entries = Vec::new();
    for (key, value) in root.entries() {
        let Some(value_object) = value.as_object() else {
            // guardian_v2 may arrive as a direct structured value.
            let key_utf8 = key.try_to_utf8().unwrap_or_default();
            if key_utf8 == "guardian_v2" || key_utf8 == "guardianV2" {
                entries.push((
                    RuntimeString::from(SLOT_REVIEW_GUARDIAN_V2),
                    SlotEntry::Json(value.clone()),
                ));
            }
            continue;
        };
        let key_utf8 = key.try_to_utf8().unwrap_or_default();
        match key_utf8.as_str() {
            "multi_agent" | "multiAgent" => {
                collect_legacy_group("multi_agent", value_object, &mut entries);
            }
            "auto_review" | "autoReview" => {
                collect_legacy_group("auto_review", value_object, &mut entries);
            }
            "guardian_v2" | "guardianV2" => {
                entries.push((
                    RuntimeString::from(SLOT_REVIEW_GUARDIAN_V2),
                    SlotEntry::Json(RuntimeJson::Object(value_object.clone())),
                ));
            }
            _ => {}
        }
    }
    entries
}

fn collect_legacy_group(
    group: &str,
    object: &crate::runtime_values::RuntimeObject,
    entries: &mut Vec<(RuntimeString, SlotEntry)>,
) {
    for (key, value) in object.entries() {
        let key_utf8 = key.try_to_utf8().unwrap_or_default();
        match value {
            RuntimeJson::Object(nested) => {
                // One more level: multi_agent.mode.{hint_text,explicit,proactive}
                for (nested_key, nested_value) in nested.entries() {
                    let nested_utf8 = nested_key.try_to_utf8().unwrap_or_default();
                    push_legacy_leaf(group, &key_utf8, &nested_utf8, nested_value, entries);
                }
            }
            other => push_legacy_leaf(group, &key_utf8, "", other, entries),
        }
    }
}

fn push_legacy_leaf(
    group: &str,
    key: &str,
    nested_key: &str,
    value: &RuntimeJson,
    entries: &mut Vec<(RuntimeString, SlotEntry)>,
) {
    let path: Vec<&str> = if nested_key.is_empty() {
        vec![group, key]
    } else {
        vec![group, key, nested_key]
    };
    let Some((slot, empty_disables)) = legacy_text_slot(&path) else {
        return;
    };
    let Some(text) = value.as_string() else {
        return;
    };
    let entry = if text.is_empty() && empty_disables {
        // Pre-catalog consumers skipped empty delegation texts entirely;
        // normalization preserves that as an explicit disable.
        SlotEntry::Disabled
    } else {
        SlotEntry::Text(text.clone())
    };
    entries.push((RuntimeString::from(slot), entry));
}

/// The spec of a registered slot, re-exported for consumers that validate
/// their own inputs.
pub fn slot(slot_id: &str) -> Option<&'static SlotSpec> {
    super::slots::slot_spec(slot_id)
}

/// Deep leaf merge of two source declarations: `overlay` leaves win, `base`
/// leaves the overlay does not mention survive, and an explicit `null` in the
/// overlay means "inherit" (keeps the base leaf). Non-object values replace
/// wholesale. This replaces the former whole-object replacement, where a
/// partial declaration silently dropped every other fragment of the lower
/// layer.
pub fn merge_declared_leaves(
    base: Option<RuntimeJson>,
    overlay: Option<RuntimeJson>,
) -> Option<RuntimeJson> {
    match (base, overlay) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value),
        (Some(base), Some(overlay)) => Some(merge_values(&base, &overlay)),
    }
}

fn merge_values(base: &RuntimeJson, overlay: &RuntimeJson) -> RuntimeJson {
    let (Some(base_object), Some(overlay_object)) = (base.as_object(), overlay.as_object()) else {
        return overlay.clone();
    };
    let mut merged = base_object.clone();
    for (key, value) in overlay_object.entries() {
        if value.is_null() {
            // Explicit null inherits the lower layer.
            continue;
        }
        match merged.get(&key) {
            Some(existing) => {
                let merged_leaf = merge_values(existing, value);
                merged.insert(key, merged_leaf);
            }
            None => {
                merged.insert(key, value.clone());
            }
        }
    }
    RuntimeJson::Object(merged)
}
