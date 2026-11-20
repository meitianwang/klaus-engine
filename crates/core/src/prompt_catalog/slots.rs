//! Engine-owned slot specifications for the model prompt catalog.
//!
//! Every catalog-visible prompt slot is registered here with its value kind,
//! requiredness, disableability, authority class and size cap. Catalog data
//! may only fill slots this registry declares, and only in the shapes it
//! allows: an unknown slot id, a wrong value kind, `disabled` on a
//! non-disableable slot or oversized text makes the whole candidate catalog
//! revision invalid (see [`super::catalog`]).
//!
//! The registry also owns the bottom resolution layer: the engine defaults
//! that reproduce the bundled offline behavior byte-for-byte when no catalog
//! profile, binding override or source declaration supplies a slot. Required
//! slots always resolve to valid content through this layer.
//!
//! High-authority slots carry the stable global prefix and the collaboration
//! authority text. The bundled package may set them; a distributed revision
//! may not until publisher-signature verification ships (see [`super::store`]).

use crate::runtime_values::RuntimeString;

/// Value shape a slot accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotValueKind {
    /// Plain instruction text.
    Text,
    /// Structured configuration consumed by a typed decoder (not prompt text).
    Json,
}

/// One registered prompt slot.
#[derive(Clone, Copy, Debug)]
pub struct SlotSpec {
    /// Stable semantic slot id (dotted, lowercase).
    pub id: &'static str,
    pub kind: SlotValueKind,
    /// A required slot always resolves to valid content; `disabled` entries
    /// for it are rejected at catalog validation time.
    pub required: bool,
    /// Whether a catalog layer may switch the slot off (`disabled: true`).
    pub disableable: bool,
    /// Whether the slot carries stable-prefix or collaboration-authority
    /// text that unsigned distributed catalogs may not override.
    pub high_authority: bool,
    /// Maximum resolved text length in bytes (text slots) or serialized JSON
    /// size in bytes (json slots).
    pub max_bytes: usize,
}

pub const SLOT_BASE_INSTRUCTIONS: &str = "base.instructions";
pub const SLOT_RESPONSE_COMMENTARY: &str = "response.commentary";
pub const SLOT_MODE_DEFAULT: &str = "mode.default";
pub const SLOT_MODE_PLAN: &str = "mode.plan";
pub const SLOT_AGENT_ROOT: &str = "agent.root";
pub const SLOT_AGENT_CHILD: &str = "agent.child";
pub const SLOT_AGENT_MODE_EXPLICIT: &str = "agent.mode.explicit";
pub const SLOT_AGENT_MODE_PROACTIVE: &str = "agent.mode.proactive";
pub const SLOT_AGENT_MODE_HINT: &str = "agent.mode.hint";
pub const SLOT_REVIEW_POLICY: &str = "review.policy";
pub const SLOT_REVIEW_POLICY_TEMPLATE: &str = "review.policy_template";
pub const SLOT_REVIEW_NODE_REPL_POLICY: &str = "review.node_repl_policy";
pub const SLOT_REVIEW_REJECTION_INSTRUCTIONS: &str = "review.rejection_instructions";
pub const SLOT_REVIEW_TIMEOUT_INSTRUCTIONS: &str = "review.timeout_instructions";
pub const SLOT_REVIEW_GUARDIAN_V2: &str = "review.guardian_v2";
pub const SLOT_TITLE_INSTRUCTIONS: &str = "title.instructions";
pub const SLOT_ADVISOR_GUIDANCE: &str = "advisor.guidance";
pub const SLOT_ADVISOR_ROLE: &str = "advisor.role";
pub const SLOT_ADVISOR_SUMMARY: &str = "advisor.summary";
pub const SLOT_ORCHESTRATION_GUIDANCE: &str = "orchestration.guidance";
pub const SLOT_COMPACT_SUMMARY_TEMPLATE: &str = "compact.summary_template";
pub const SLOT_COMPACT_LEAN_SYSTEM: &str = "compact.lean_system";
pub const SLOT_MEMORY_LEARNING_QUALITY: &str = "memory.learning_quality";
pub const SLOT_MEMORY_CONSOLIDATION_QUALITY: &str = "memory.consolidation_quality";

/// Text slot cap: generous headroom over the bundled defaults, small enough
/// that a corrupt or malicious catalog cannot smuggle unbounded prompt bytes.
const TEXT_CAP: usize = 256 * 1024;
const SMALL_TEXT_CAP: usize = 32 * 1024;
const JSON_CAP: usize = 16 * 1024;

const fn text_slot(
    id: &'static str,
    required: bool,
    disableable: bool,
    high_authority: bool,
    max_bytes: usize,
) -> SlotSpec {
    SlotSpec {
        id,
        kind: SlotValueKind::Text,
        required,
        disableable,
        high_authority,
        max_bytes,
    }
}

/// All registered slots, lowest-first declaration order.
pub const SLOTS: &[SlotSpec] = &[
    text_slot(SLOT_BASE_INSTRUCTIONS, true, false, true, TEXT_CAP),
    text_slot(SLOT_RESPONSE_COMMENTARY, false, true, true, SMALL_TEXT_CAP),
    text_slot(SLOT_MODE_DEFAULT, true, false, true, TEXT_CAP),
    text_slot(SLOT_MODE_PLAN, true, false, true, TEXT_CAP),
    text_slot(SLOT_AGENT_ROOT, false, true, false, SMALL_TEXT_CAP),
    text_slot(SLOT_AGENT_CHILD, false, true, false, SMALL_TEXT_CAP),
    text_slot(SLOT_AGENT_MODE_EXPLICIT, false, true, false, SMALL_TEXT_CAP),
    text_slot(
        SLOT_AGENT_MODE_PROACTIVE,
        false,
        true,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(SLOT_AGENT_MODE_HINT, false, true, false, SMALL_TEXT_CAP),
    text_slot(SLOT_REVIEW_POLICY, false, false, false, SMALL_TEXT_CAP),
    text_slot(
        SLOT_REVIEW_POLICY_TEMPLATE,
        false,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(
        SLOT_REVIEW_NODE_REPL_POLICY,
        false,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(
        SLOT_REVIEW_REJECTION_INSTRUCTIONS,
        false,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(
        SLOT_REVIEW_TIMEOUT_INSTRUCTIONS,
        false,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    SlotSpec {
        id: SLOT_REVIEW_GUARDIAN_V2,
        kind: SlotValueKind::Json,
        required: false,
        disableable: true,
        high_authority: false,
        max_bytes: JSON_CAP,
    },
    // Specialized inference purposes: each request binds the fragments of the
    // model that actually receives it.
    text_slot(SLOT_TITLE_INSTRUCTIONS, true, false, false, SMALL_TEXT_CAP),
    text_slot(SLOT_ADVISOR_GUIDANCE, true, false, true, SMALL_TEXT_CAP),
    text_slot(SLOT_ADVISOR_ROLE, true, false, false, SMALL_TEXT_CAP),
    text_slot(SLOT_ADVISOR_SUMMARY, true, false, false, SMALL_TEXT_CAP),
    text_slot(
        SLOT_ORCHESTRATION_GUIDANCE,
        true,
        false,
        true,
        SMALL_TEXT_CAP,
    ),
    text_slot(
        SLOT_COMPACT_SUMMARY_TEMPLATE,
        true,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(SLOT_COMPACT_LEAN_SYSTEM, true, false, false, SMALL_TEXT_CAP),
    text_slot(
        SLOT_MEMORY_LEARNING_QUALITY,
        true,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
    text_slot(
        SLOT_MEMORY_CONSOLIDATION_QUALITY,
        true,
        false,
        false,
        SMALL_TEXT_CAP,
    ),
];

/// Look up the specification of a registered slot id.
pub fn slot_spec(id: &str) -> Option<&'static SlotSpec> {
    SLOTS.iter().find(|spec| spec.id == id)
}

/// The engine-default value of a slot: the bottom resolution layer that keeps
/// the offline behavior byte-identical when no catalog data applies.
///
/// `family` is the resolved model family (`"claude"`, `"gemini"`, …) for the
/// few slots whose bundled default is family-keyed: families whose wire
/// protocol tags assistant messages with a message phase keep the stock
/// progress-update policy (tagged updates ride the phase channel and the
/// client collapses them once the final answer lands); untagged families get
/// a brevity discipline instead, because every visible line renders as answer
/// text there. Slots without an engine default (the review texts, whose
/// bundled fallbacks live in the review policy module, and the delegation
/// hint) return `None`.
///
/// Defaults are interned once: resolution runs per request and clones must
/// stay reference-count bumps, not repeated UTF-16 conversions of the large
/// instruction bodies.
pub fn engine_default(slot: &str, family: Option<&str>) -> Option<&'static RuntimeString> {
    macro_rules! interned {
        ($text:expr) => {{
            static VALUE: std::sync::OnceLock<RuntimeString> = std::sync::OnceLock::new();
            VALUE.get_or_init(|| RuntimeString::from($text))
        }};
    }
    let value: &'static RuntimeString = match slot {
        SLOT_BASE_INSTRUCTIONS => interned!(crate::context::system_prompt::BASE_INSTRUCTIONS),
        SLOT_RESPONSE_COMMENTARY => match family {
            Some("claude") => {
                interned!(crate::context::system_prompt::COMMENTARY_UNTAGGED_BRIEF_UPDATES)
            }
            Some("gemini") => {
                interned!(crate::context::system_prompt::COMMENTARY_UNTAGGED_THINKING_CHANNEL)
            }
            _ => interned!(crate::context::system_prompt::COMMENTARY_PHASE_TAGGED),
        },
        SLOT_MODE_DEFAULT => interned!(include_str!("../planning/prompts/default.md")),
        SLOT_MODE_PLAN => interned!(include_str!("../planning/prompts/plan.md")),
        SLOT_AGENT_ROOT => interned!(crate::context::multi_agent::ROOT_ROLE),
        SLOT_AGENT_CHILD => interned!(crate::context::multi_agent::CHILD_ROLE),
        SLOT_AGENT_MODE_EXPLICIT => interned!(crate::context::multi_agent::EXPLICIT_MODE),
        SLOT_AGENT_MODE_PROACTIVE => interned!(crate::context::multi_agent::PROACTIVE_MODE),
        SLOT_TITLE_INSTRUCTIONS => interned!(crate::engine::session::SESSION_TITLE_PROMPT),
        SLOT_ADVISOR_GUIDANCE => interned!(crate::context::system_prompt::ADVISOR_GUIDANCE),
        SLOT_ADVISOR_ROLE => interned!(crate::engine::session::ADVISOR_INSTRUCTIONS),
        SLOT_ADVISOR_SUMMARY => interned!(crate::engine::session::SUMMARY_INSTRUCTIONS),
        SLOT_ORCHESTRATION_GUIDANCE => {
            interned!(crate::context::system_prompt::ORCHESTRATION_GUIDANCE)
        }
        SLOT_COMPACT_SUMMARY_TEMPLATE => {
            interned!(crate::context::compact::full_compact::SUMMARY_PROMPT)
        }
        SLOT_COMPACT_LEAN_SYSTEM => {
            interned!(crate::context::compact::full_compact::LEAN_SUMMARY_SYSTEM_PROMPT)
        }
        SLOT_MEMORY_LEARNING_QUALITY => {
            interned!(include_str!("../memory/prompts/learning_quality.md"))
        }
        SLOT_MEMORY_CONSOLIDATION_QUALITY => {
            interned!(include_str!("../memory/prompts/consolidation_quality.md"))
        }
        _ => return None,
    };
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_slot_has_a_unique_dotted_id() {
        let mut seen = std::collections::HashSet::new();
        for spec in SLOTS {
            assert!(seen.insert(spec.id), "duplicate slot id {}", spec.id);
            assert!(
                spec.id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'),
                "slot id {} is not lowercase dotted",
                spec.id
            );
            assert!(
                spec.id.contains('.'),
                "slot id {} is not namespaced",
                spec.id
            );
            assert!(spec.max_bytes > 0);
        }
    }

    #[test]
    fn required_slots_always_have_engine_defaults() {
        for spec in SLOTS {
            if spec.required {
                assert!(
                    engine_default(spec.id, None).is_some(),
                    "required slot {} has no engine default",
                    spec.id
                );
            }
        }
    }

    #[test]
    fn engine_default_respects_slot_registration() {
        assert!(slot_spec(SLOT_BASE_INSTRUCTIONS).is_some());
        assert!(slot_spec("base.instructions_template").is_none());
        assert!(engine_default("unknown.slot", None).is_none());
    }

    #[test]
    fn commentary_default_is_family_keyed() {
        let claude = engine_default(SLOT_RESPONSE_COMMENTARY, Some("claude")).unwrap();
        let gemini = engine_default(SLOT_RESPONSE_COMMENTARY, Some("gemini")).unwrap();
        let other = engine_default(SLOT_RESPONSE_COMMENTARY, None).unwrap();
        assert_ne!(claude, other);
        assert_ne!(gemini, other);
        assert_eq!(
            other,
            engine_default(SLOT_RESPONSE_COMMENTARY, Some("gpt")).unwrap()
        );
    }

    #[test]
    fn high_authority_slots_cover_the_stable_prefix_and_collaboration_authority() {
        for id in [
            SLOT_BASE_INSTRUCTIONS,
            SLOT_RESPONSE_COMMENTARY,
            SLOT_MODE_DEFAULT,
            SLOT_MODE_PLAN,
            SLOT_ADVISOR_GUIDANCE,
            SLOT_ORCHESTRATION_GUIDANCE,
        ] {
            assert!(
                slot_spec(id).unwrap().high_authority,
                "{id} enters the stable prefix and must be high-authority"
            );
        }
        for id in [
            SLOT_AGENT_ROOT,
            SLOT_REVIEW_POLICY,
            SLOT_TITLE_INSTRUCTIONS,
            SLOT_COMPACT_SUMMARY_TEMPLATE,
        ] {
            assert!(
                !slot_spec(id).unwrap().high_authority,
                "{id} is model/purpose-scoped and stays tunable"
            );
        }
    }
}
