//! Layered slot resolution: engine defaults, catalog profiles, binding
//! overrides and source declarations merge leaf-by-leaf into the fragments
//! bound to one inference.
//!
//! Layer order (lowest first), matching the catalog design:
//!
//! 1. engine defaults — the bundled offline behavior (slot registry);
//! 2. the generic pack (`klaus/default`);
//! 3. the family pack (`family/<name>`) when the model family is known;
//! 4. the bound model pack: the binding's profile with its inheritance chain
//!    expanded, ancestors first;
//! 5. the binding's route-local overrides;
//! 6. the source declaration riding with the model metadata (legacy
//!    `model_messages` shapes or the slot-keyed form), normalized by
//!    [`super::fragments`].
//!
//! Every layer writes slot leaves only; a layer that does not mention a slot
//! inherits it. `null` means inherit everywhere. Invalid declared entries
//! fall back to the next-lower valid value and are logged, never fatal.

use std::collections::HashMap;

use crate::runtime_values::{RuntimeJson, RuntimeString};

use super::catalog::{
    CatalogBinding, CatalogProfile, PromptCatalog, SlotEntry, FAMILY_PROFILE_PREFIX,
};
use super::slots::{slot_spec, SlotSpec, SlotValueKind, SLOTS};

/// Which access route a resolution applies to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptCatalogKey {
    /// Source kind (`codex`, `subscription`, `cursor`, `custom`, …). Empty
    /// means "route not identified".
    pub source_kind: RuntimeString,
    /// Local, non-sensitive scope id of the account/endpoint configuration.
    pub source_scope: RuntimeString,
    /// The actual model id this inference sends to.
    pub model_key: RuntimeString,
    /// Normalized in-source variant; `standard` when the route has none.
    pub variant_key: RuntimeString,
}

impl PromptCatalogKey {
    pub fn new(
        source_kind: impl Into<RuntimeString>,
        source_scope: impl Into<RuntimeString>,
        model_key: impl Into<RuntimeString>,
        variant_key: impl Into<RuntimeString>,
    ) -> Self {
        Self {
            source_kind: source_kind.into(),
            source_scope: source_scope.into(),
            model_key: model_key.into(),
            variant_key: variant_key.into(),
        }
    }

    /// A key that only identifies the model: no route information, standard
    /// variant. Matches bindings written with `*` wildcards on the route
    /// fields, and never matches route-specific bindings.
    pub fn for_model(model_key: &RuntimeString) -> Self {
        Self {
            source_kind: RuntimeString::from(""),
            source_scope: RuntimeString::from(""),
            model_key: model_key.clone(),
            variant_key: RuntimeString::from("standard"),
        }
    }
}

/// The model identity without an in-source selection suffix.
///
/// Aggregating routes identify one selection as `<model>[k=v,…]` and may add a
/// trailing `@<mode>` tag after it. Catalog packs describe the model, so a key
/// that carries those decorations still resolves the pack of the same model.
/// The undecorated key is only consulted when no binding matches the exact
/// key, which keeps route- or variant-specific bindings authoritative.
pub fn canonical_model_key(model: &RuntimeString) -> RuntimeString {
    let units = model.as_utf16();
    let mut end = units.len();
    if let Some(at) = units.iter().rposition(|unit| *unit == u16::from(b'@')) {
        if at > 0 && at + 1 < units.len() && units[at - 1] == u16::from(b']') {
            end = at;
        }
    }
    if end > 0 && units[end - 1] == u16::from(b']') {
        if let Some(open) = units[..end - 1]
            .iter()
            .rposition(|unit| *unit == u16::from(b'['))
        {
            end = open;
        }
    }
    if end == units.len() {
        model.clone()
    } else {
        model.slice_utf16(..end)
    }
}

/// The resolution layer a slot value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentLayer {
    EngineDefault,
    BaseProfile,
    FamilyProfile,
    ModelProfile,
    BindingOverride,
    Declared,
}

impl FragmentLayer {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EngineDefault => "engine-default",
            Self::BaseProfile => "base-profile",
            Self::FamilyProfile => "family-profile",
            Self::ModelProfile => "model-profile",
            Self::BindingOverride => "binding-override",
            Self::Declared => "declared",
        }
    }
}

/// Where a resolved slot value came from, for diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotProvenance {
    pub layer: FragmentLayer,
    /// Profile id, fragment id or `declared`, depending on the layer.
    pub origin: RuntimeString,
}

/// One resolved slot value.
#[derive(Clone, Debug, PartialEq)]
pub enum SlotValue {
    Text(RuntimeString),
    Json(RuntimeJson),
    Disabled,
}

#[derive(Clone, Debug)]
pub struct ResolvedSlot {
    pub value: SlotValue,
    pub provenance: SlotProvenance,
}

/// The effective fragment set bound to one inference.
#[derive(Clone, Debug)]
pub struct ResolvedFragments {
    slots: HashMap<&'static str, ResolvedSlot>,
    pub catalog_revision: RuntimeString,
    pub profile_ref: Option<RuntimeString>,
}

impl ResolvedFragments {
    /// Text of a slot; `None` when the slot is disabled, absent or holds a
    /// structured value.
    pub fn text(&self, slot: &str) -> Option<&RuntimeString> {
        match &self.slots.get(slot_spec_id(slot)?)?.value {
            SlotValue::Text(text) => Some(text),
            _ => None,
        }
    }

    /// Structured value of a slot.
    pub fn json(&self, slot: &str) -> Option<&RuntimeJson> {
        let id = slot_spec_id(slot)?;
        match &self.slots.get(id)?.value {
            SlotValue::Json(value) => Some(value),
            _ => None,
        }
    }

    pub fn is_disabled(&self, slot: &str) -> bool {
        let Some(id) = slot_spec_id(slot) else {
            return false;
        };
        matches!(
            self.slots.get(id).map(|slot| &slot.value),
            Some(SlotValue::Disabled)
        )
    }

    pub fn provenance(&self, slot: &str) -> Option<&SlotProvenance> {
        let id = slot_spec_id(slot)?;
        self.slots.get(id).map(|slot| &slot.provenance)
    }

    /// Text of a required slot. Required slots always carry valid content
    /// from the engine-default layer up; a disabled or structured value can
    /// only appear through a catalog bug, which validation rejects.
    pub fn required_text(&self, slot: &str) -> RuntimeString {
        if let Some(text) = self.text(slot) {
            return text.clone();
        }
        super::slots::engine_default(slot, None)
            .cloned()
            .unwrap_or_default()
    }

    /// Safe per-slot diagnostic summary: layer, origin and value size. Never
    /// includes credential material; text bodies are summarized by length.
    pub fn describe(&self) -> Vec<(&'static str, FragmentLayer, RuntimeString, usize)> {
        let mut rows = Vec::new();
        for spec in SLOTS {
            let Some(slot) = self.slots.get(spec.id) else {
                continue;
            };
            let size = match &slot.value {
                SlotValue::Text(text) => text.len_utf16(),
                SlotValue::Json(value) => super::json_len(value),
                SlotValue::Disabled => 0,
            };
            rows.push((
                spec.id,
                slot.provenance.layer,
                slot.provenance.origin.clone(),
                size,
            ));
        }
        rows
    }
}

fn slot_spec_id(slot: &str) -> Option<&'static str> {
    slot_spec(slot).map(|spec| spec.id)
}

struct Resolution {
    slots: HashMap<&'static str, ResolvedSlot>,
}

impl Resolution {
    fn new() -> Self {
        Self {
            slots: HashMap::new(),
        }
    }

    fn set(&mut self, spec: &'static SlotSpec, value: SlotValue, provenance: SlotProvenance) {
        self.slots
            .insert(spec.id, ResolvedSlot { value, provenance });
    }

    /// Apply one catalog entry list (a profile body or binding overrides).
    /// Entries are already validated against the slot registry. An untrusted
    /// (unsigned distributed) revision never reaches high-authority slots.
    fn apply_entries(
        &mut self,
        catalog: &PromptCatalog,
        entries: &[(RuntimeString, SlotEntry)],
        layer: FragmentLayer,
        origin: &RuntimeString,
    ) {
        for (slot, entry) in entries {
            let Some(spec) =
                slot_spec_id(slot.try_to_utf8().ok().as_deref().unwrap_or("")).and_then(slot_spec)
            else {
                continue;
            };
            if !catalog.trusted && spec.high_authority && !matches!(entry, SlotEntry::Inherit) {
                tracing::debug!(
                    slot = spec.id,
                    revision = %catalog.revision_utf8(),
                    "prompt catalog: unsigned distributed revision may not override a high-authority slot"
                );
                continue;
            }
            match entry {
                SlotEntry::Inherit => {}
                SlotEntry::Disabled => self.set(
                    spec,
                    SlotValue::Disabled,
                    SlotProvenance {
                        layer,
                        origin: origin.clone(),
                    },
                ),
                SlotEntry::Ref(id) => {
                    if let Some(text) = catalog.fragment(id) {
                        self.set(
                            spec,
                            SlotValue::Text(text.clone()),
                            SlotProvenance {
                                layer,
                                origin: id.clone(),
                            },
                        );
                    }
                }
                SlotEntry::Text(text) => self.set(
                    spec,
                    SlotValue::Text(text.clone()),
                    SlotProvenance {
                        layer,
                        origin: origin.clone(),
                    },
                ),
                SlotEntry::Json(value) => self.set(
                    spec,
                    SlotValue::Json(value.clone()),
                    SlotProvenance {
                        layer,
                        origin: origin.clone(),
                    },
                ),
            }
        }
    }

    /// Apply one declared (runtime, untrusted-shape) entry with tolerance:
    /// an invalid entry keeps the next-lower valid value and is logged.
    fn apply_declared(&mut self, slot: &str, entry: SlotEntry) {
        let Some(spec) = slot_spec_id(slot).and_then(slot_spec) else {
            tracing::debug!(
                slot,
                "prompt catalog: declared entry for unregistered slot ignored"
            );
            return;
        };
        match entry {
            SlotEntry::Inherit => {}
            SlotEntry::Disabled => {
                if spec.disableable {
                    self.set(
                        spec,
                        SlotValue::Disabled,
                        SlotProvenance {
                            layer: FragmentLayer::Declared,
                            origin: RuntimeString::from("declared"),
                        },
                    );
                } else {
                    tracing::debug!(
                        slot = spec.id,
                        "prompt catalog: declared disable of a non-disableable slot ignored"
                    );
                }
            }
            SlotEntry::Ref(_) => {
                tracing::debug!(
                    slot = spec.id,
                    "prompt catalog: declared fragment references are not resolvable at runtime"
                );
            }
            SlotEntry::Text(text) => {
                if spec.kind != SlotValueKind::Text {
                    tracing::debug!(
                        slot = spec.id,
                        "prompt catalog: declared text for a structured slot ignored"
                    );
                    return;
                }
                let len = text
                    .try_to_utf8()
                    .map(|value| value.len())
                    .unwrap_or_else(|_| text.len_utf16());
                if len > spec.max_bytes {
                    tracing::debug!(
                        slot = spec.id,
                        len,
                        "prompt catalog: oversized declared text ignored"
                    );
                    return;
                }
                self.set(
                    spec,
                    SlotValue::Text(text),
                    SlotProvenance {
                        layer: FragmentLayer::Declared,
                        origin: RuntimeString::from("declared"),
                    },
                );
            }
            SlotEntry::Json(value) => {
                if spec.kind != SlotValueKind::Json {
                    tracing::debug!(
                        slot = spec.id,
                        "prompt catalog: declared structured value for a text slot ignored"
                    );
                    return;
                }
                if super::json_len(&value) > spec.max_bytes {
                    tracing::debug!(
                        slot = spec.id,
                        "prompt catalog: oversized declared structured value ignored"
                    );
                    return;
                }
                self.set(
                    spec,
                    SlotValue::Json(value),
                    SlotProvenance {
                        layer: FragmentLayer::Declared,
                        origin: RuntimeString::from("declared"),
                    },
                );
            }
        }
    }
}

/// Resolve the effective fragments for one inference.
///
/// `family` is the resolved model family for family-keyed engine defaults and
/// the `family/<name>` pack; `declared` is the source declaration riding with
/// the model metadata (any shape; normalized by [`super::fragments`]).
pub fn resolve(
    catalog: &PromptCatalog,
    key: &PromptCatalogKey,
    family: Option<&str>,
    declared: Option<&RuntimeJson>,
) -> ResolvedFragments {
    let mut resolution = Resolution::new();

    // 1. Engine defaults.
    for spec in SLOTS {
        if let Some(text) = super::slots::engine_default(spec.id, family) {
            resolution.set(
                spec,
                SlotValue::Text(text.clone()),
                SlotProvenance {
                    layer: FragmentLayer::EngineDefault,
                    origin: RuntimeString::from(""),
                },
            );
        }
    }

    // 2. Generic pack.
    let base_id = RuntimeString::from(super::catalog::BASE_PROFILE);
    if let Some(profile) = catalog.profile(&base_id) {
        resolution.apply_entries(
            catalog,
            &profile.model_messages,
            FragmentLayer::BaseProfile,
            &profile.id,
        );
    }

    // 3. Family pack (only fragments that explicitly cover the family).
    if let Some(family) = family {
        let family_id = RuntimeString::from(format!("{FAMILY_PROFILE_PREFIX}{family}"));
        if let Some(profile) = catalog.profile(&family_id) {
            resolution.apply_entries(
                catalog,
                &profile.model_messages,
                FragmentLayer::FamilyProfile,
                &profile.id,
            );
        }
    }

    // 4 + 5. Bound model pack (inheritance chain, ancestors first) and the
    // route-local overrides.
    let binding = catalog.binding(key);
    let profile_ref = binding.map(|binding| binding.profile_ref.clone());
    if let Some(binding) = binding {
        apply_binding(catalog, binding, &mut resolution);
    }

    // 6. Source declaration.
    if let Some(declared) = declared {
        for (slot, entry) in super::fragments::normalize_declared(declared) {
            resolution.apply_declared(slot.try_to_utf8().ok().as_deref().unwrap_or(""), entry);
        }
    }

    ResolvedFragments {
        slots: resolution.slots,
        catalog_revision: catalog.revision.clone(),
        profile_ref,
    }
}

fn apply_binding(catalog: &PromptCatalog, binding: &CatalogBinding, resolution: &mut Resolution) {
    for profile in catalog.profile_chain(&binding.profile_ref) {
        apply_profile(catalog, profile, resolution);
    }
    resolution.apply_entries(
        catalog,
        &binding.overrides,
        FragmentLayer::BindingOverride,
        &binding.profile_ref,
    );
}

fn apply_profile(catalog: &PromptCatalog, profile: &CatalogProfile, resolution: &mut Resolution) {
    // A chain that passes through the generic or a family pack must not
    // double-apply it at a higher layer; those layers already ran.
    let id = profile.id.try_to_utf8().unwrap_or_default();
    let layer = if id == super::catalog::BASE_PROFILE || id.starts_with(FAMILY_PROFILE_PREFIX) {
        return;
    } else {
        FragmentLayer::ModelProfile
    };
    resolution.apply_entries(catalog, &profile.model_messages, layer, &profile.id);
}
