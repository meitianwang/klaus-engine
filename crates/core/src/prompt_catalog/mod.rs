//! Model prompt catalog: versioned, per-model prompt fragments resolved
//! leaf-by-leaf across engine defaults, shared model packs, access-route
//! bindings and source declarations.
//!
//! The catalog expresses "how this model should be addressed in this
//! scenario"; the engine still decides whether the scenario holds, who
//! receives a fragment and when. Slot semantics (who may write a slot, its
//! value kind, requiredness, size caps) are owned by the engine
//! ([`slots`]); catalog data may only select content the slot registry
//! allows.
//!
//! Phase 1 wiring: the bundled offline catalog ([`bundled`]) plus the engine
//! defaults reproduce the shipped behavior byte-for-byte; consumers obtain
//! fragments through [`ModelFragments`] instead of reading raw catalog JSON.

use std::sync::OnceLock;

use crate::runtime_values::{RuntimeJson, RuntimeString};

pub mod catalog;
pub mod fragments;
pub mod resolve;
pub mod slots;

#[cfg(test)]
mod tests;

pub use catalog::{CatalogBinding, CatalogError, CatalogProfile, PromptCatalog, SlotEntry};
pub use fragments::{merge_declared_leaves, normalize_declared, ModelFragments};
pub use resolve::{
    FragmentLayer, PromptCatalogKey, ResolvedFragments, ResolvedSlot, SlotProvenance, SlotValue,
};
pub use slots::{slot_spec, SlotSpec, SlotValueKind, SLOTS};

/// The bundled offline catalog, parsed and validated once.
///
/// A bundled catalog that fails validation is a build defect (pinned by
/// tests); at runtime the engine degrades to an empty catalog, which resolves
/// every slot from the engine defaults — the same bytes the shipped prompts
/// carry.
pub fn bundled() -> &'static PromptCatalog {
    static BUNDLED: OnceLock<PromptCatalog> = OnceLock::new();
    BUNDLED.get_or_init(|| {
        let text = RuntimeString::from(include_str!("bundled.json"));
        match PromptCatalog::parse(&text) {
            Ok(catalog) => catalog,
            Err(error) => {
                tracing::error!(
                    %error,
                    "bundled prompt catalog failed validation; falling back to engine defaults"
                );
                PromptCatalog::empty("bundled-invalid")
            }
        }
    })
}

/// Serialized size estimate of a structured slot value, in UTF-16 code units.
pub(crate) fn json_len(value: &RuntimeJson) -> usize {
    crate::runtime_values::runtime_json::stringify(value).len_utf16()
}
