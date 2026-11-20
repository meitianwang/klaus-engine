//! Prompt-catalog document: wire schema, decoding and whole-document
//! validation.
//!
//! A catalog is one JSON document with four top-level members:
//!
//! ```json
//! {
//!   "schemaVersion": 2,
//!   "revision": "catalog-r13",
//!   "fragments": { "base-v3": "…" },
//!   "profiles": {
//!     "klaus/default": { "modelMessages": { "base.instructions": { "ref": "base-v3" } } },
//!     "fixture/model-a": { "extends": "klaus/default", "modelMessages": { … } }
//!   },
//!   "bindings": [
//!     { "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-a",
//!       "variantKey": "standard", "profileRef": "fixture/model-a",
//!       "overrides": { "response.commentary": { "disabled": true } } }
//!   ]
//! }
//! ```
//!
//! Validation is all-or-nothing: any unknown slot, unresolvable reference,
//! inheritance cycle, duplicate binding key, wrong value kind, oversized text
//! or `disabled` on a non-disableable slot rejects the ENTIRE candidate
//! revision, so a partially understood document can never activate. This is
//! the atomic-activation rule the offline bundled catalog and any future
//! distributed catalog share — both go through the same parser.

use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};

use super::slots::{slot_spec, SlotValueKind};

/// Profile id of the generic pack every request inherits from.
pub const BASE_PROFILE: &str = "klaus/default";
/// Prefix of family-scoped profile ids (`family/<name>`).
pub const FAMILY_PROFILE_PREFIX: &str = "family/";

/// The supported wire schema version.
pub const SCHEMA_VERSION: u32 = 2;

/// One declared value of a slot inside a profile or a binding override.
#[derive(Clone, Debug, PartialEq)]
pub enum SlotEntry {
    /// Absent or `null`: inherit from the next-lower layer.
    Inherit,
    /// `{ "disabled": true }`: switch the slot off (disableable slots only).
    Disabled,
    /// `{ "ref": "<fragment id>" }`: fragment-backed text, resolved at
    /// validation time into [`SlotEntry::Text`] is NOT done here; the resolver
    /// dereferences through [`PromptCatalog::fragment`].
    Ref(RuntimeString),
    /// `{ "text": "…" }`: inline text.
    Text(RuntimeString),
    /// `{ "json": … }`: structured slot value.
    Json(RuntimeJson),
}

/// A shared model pack: an optional inheritance parent plus slot entries.
#[derive(Clone, Debug)]
pub struct CatalogProfile {
    pub id: RuntimeString,
    pub extends: Option<RuntimeString>,
    pub model_messages: Vec<(RuntimeString, SlotEntry)>,
}

/// One access route's binding: which profile a (source, scope, model,
/// variant) key resolves to, plus route-local overrides.
#[derive(Clone, Debug)]
pub struct CatalogBinding {
    pub source_kind: RuntimeString,
    pub source_scope: RuntimeString,
    pub model_key: RuntimeString,
    pub variant_key: RuntimeString,
    pub profile_ref: RuntimeString,
    pub overrides: Vec<(RuntimeString, SlotEntry)>,
}

impl CatalogBinding {
    /// A binding field matches when it equals the request key or when the
    /// binding declares the `*` wildcard.
    fn field_matches(bound: &RuntimeString, actual: &RuntimeString) -> bool {
        bound == "*" || bound == actual
    }

    pub fn matches(&self, key: &super::resolve::PromptCatalogKey) -> bool {
        Self::field_matches(&self.source_kind, &key.source_kind)
            && Self::field_matches(&self.source_scope, &key.source_scope)
            && Self::field_matches(&self.model_key, &key.model_key)
            && Self::field_matches(&self.variant_key, &key.variant_key)
    }
}

/// A decoded, fully validated catalog revision.
#[derive(Clone, Debug)]
pub struct PromptCatalog {
    pub schema_version: u32,
    pub revision: RuntimeString,
    /// Whether this revision may override high-authority slots (the stable
    /// global prefix and collaboration templates). The bundled package is
    /// trusted by build; a distributed revision stays untrusted until
    /// publisher-signature verification ships, so it can tune model-specific
    /// slots but never rewrite the shared prefix.
    pub trusted: bool,
    fragments: RuntimeObject,
    profiles: Vec<CatalogProfile>,
    bindings: Vec<CatalogBinding>,
}

/// Whole-document rejection reason. The message never quotes catalog text
/// bodies; it names the offending member only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogError(pub String);

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

fn err(message: impl Into<String>) -> CatalogError {
    CatalogError(message.into())
}

fn key_utf8(key: &RuntimeString) -> String {
    key.try_to_utf8()
        .unwrap_or_else(|_| "<non-scalar>".to_owned())
}

fn text_len(value: &RuntimeString) -> usize {
    value
        .try_to_utf8()
        .map(|text| text.len())
        .unwrap_or_else(|_| value.len_utf16())
}

impl PromptCatalog {
    /// Parse and validate one catalog document. Any validation failure
    /// rejects the whole revision.
    pub fn decode(value: &RuntimeJson) -> Result<Self, CatalogError> {
        let root = value
            .as_object()
            .ok_or_else(|| err("catalog root must be an object"))?;
        let schema_version = match root.get("schemaVersion") {
            Some(RuntimeJson::Number(number)) => number.as_f64() as u32,
            _ => return Err(err("catalog schemaVersion must be a number")),
        };
        if schema_version != SCHEMA_VERSION {
            return Err(err(format!(
                "unsupported catalog schemaVersion {schema_version} (expected {SCHEMA_VERSION})"
            )));
        }
        let revision = root
            .get("revision")
            .and_then(RuntimeJson::as_string)
            .filter(|revision| !revision.is_empty())
            .cloned()
            .ok_or_else(|| err("catalog revision must be a non-empty string"))?;

        let mut fragments = RuntimeObject::new();
        if let Some(entries) = root.get("fragments") {
            let object = entries
                .as_object()
                .ok_or_else(|| err("catalog fragments must be an object"))?;
            for (id, value) in object.entries() {
                let text = value
                    .as_string()
                    .ok_or_else(|| err(format!("fragment {} must be a string", key_utf8(&id))))?
                    .clone();
                fragments.insert(id, RuntimeJson::String(text));
            }
        }

        let mut profiles = Vec::new();
        if let Some(entries) = root.get("profiles") {
            let object = entries
                .as_object()
                .ok_or_else(|| err("catalog profiles must be an object"))?;
            for (id, value) in object.entries() {
                let profile = Self::decode_profile(id.clone(), value, &fragments)?;
                profiles.push(profile);
            }
        }

        let mut bindings = Vec::new();
        if let Some(entries) = root.get("bindings") {
            let array = entries
                .as_array()
                .ok_or_else(|| err("catalog bindings must be an array"))?;
            for (index, value) in array.iter().enumerate() {
                bindings.push(Self::decode_binding(index, value, &profiles, &fragments)?);
            }
        }

        let catalog = Self {
            schema_version,
            revision,
            trusted: false,
            fragments,
            profiles,
            bindings,
        };
        catalog.validate_inheritance()?;
        Ok(catalog)
    }

    /// Mark this revision trusted (allowed to override high-authority slots).
    /// Only the build-bundled package and, once publisher signatures ship,
    /// signature-verified distributed revisions may call this.
    pub fn with_trust(mut self, trusted: bool) -> Self {
        self.trusted = trusted;
        self
    }

    /// Parse a catalog from its JSON text form.
    pub fn parse(text: &RuntimeString) -> Result<Self, CatalogError> {
        let value = runtime_json::parse(text)
            .map_err(|error| err(format!("catalog is not valid JSON: {error:?}")))?;
        Self::decode(&value)
    }

    fn decode_profile(
        id: RuntimeString,
        value: &RuntimeJson,
        fragments: &RuntimeObject,
    ) -> Result<CatalogProfile, CatalogError> {
        let object = value
            .as_object()
            .ok_or_else(|| err(format!("profile {} must be an object", key_utf8(&id))))?;
        let extends = match object.get("extends") {
            None | Some(RuntimeJson::Null) => None,
            Some(value) => Some(
                value
                    .as_string()
                    .ok_or_else(|| {
                        err(format!(
                            "profile {} extends must be a string",
                            key_utf8(&id)
                        ))
                    })?
                    .clone(),
            ),
        };
        let model_messages = Self::decode_entries(&id, object.get("modelMessages"), fragments)?;
        Ok(CatalogProfile {
            id,
            extends,
            model_messages,
        })
    }

    fn decode_binding(
        index: usize,
        value: &RuntimeJson,
        profiles: &[CatalogProfile],
        fragments: &RuntimeObject,
    ) -> Result<CatalogBinding, CatalogError> {
        let object = value
            .as_object()
            .ok_or_else(|| err(format!("binding[{index}] must be an object")))?;
        let field = |name: &str, aliases: &[&str]| -> Result<RuntimeString, CatalogError> {
            let mut lookup = Vec::with_capacity(aliases.len() + 1);
            lookup.push(name);
            lookup.extend(aliases.iter().copied());
            for key in lookup {
                if let Some(value) = object.get(key).and_then(RuntimeJson::as_string) {
                    return Ok(value.clone());
                }
            }
            Err(err(format!("binding[{index}] missing {name}")))
        };
        let binding_id = RuntimeString::from(format!("binding[{index}]"));
        let profile_ref = field("profileRef", &["profile_ref"])?;
        if !profiles.iter().any(|profile| profile.id == profile_ref) {
            return Err(err(format!(
                "binding[{index}] references unknown profile {}",
                key_utf8(&profile_ref)
            )));
        }
        let overrides = Self::decode_entries(&binding_id, object.get("overrides"), fragments)?;
        Ok(CatalogBinding {
            source_kind: field("sourceKind", &["source_kind"])?,
            source_scope: field("sourceScope", &["source_scope"])?,
            model_key: field("modelKey", &["model_key"])?,
            variant_key: field("variantKey", &["variant_key"])?,
            profile_ref,
            overrides,
        })
    }

    fn decode_entries(
        owner: &RuntimeString,
        value: Option<&RuntimeJson>,
        fragments: &RuntimeObject,
    ) -> Result<Vec<(RuntimeString, SlotEntry)>, CatalogError> {
        let Some(value) = value else {
            return Ok(Vec::new());
        };
        let object = value.as_object().ok_or_else(|| {
            err(format!(
                "{} modelMessages must be an object",
                key_utf8(owner)
            ))
        })?;
        let owner_utf8 = key_utf8(owner);
        let mut entries = Vec::with_capacity(object.len());
        for (slot, value) in object.entries() {
            let slot_utf8 = key_utf8(&slot);
            let Some(spec) = slot_spec(slot_utf8.as_str()) else {
                return Err(err(format!(
                    "{owner_utf8} references unregistered slot {slot_utf8}"
                )));
            };
            let entry = match value {
                RuntimeJson::Null => SlotEntry::Inherit,
                RuntimeJson::Object(entry) => {
                    if entry.get("disabled").and_then(RuntimeJson::as_bool) == Some(true) {
                        if entry.len() != 1 {
                            return Err(err(format!(
                                "{owner_utf8} slot {slot_utf8} combines disabled with other keys"
                            )));
                        }
                        SlotEntry::Disabled
                    } else if let Some(id) = entry.get("ref").and_then(RuntimeJson::as_string) {
                        if entry.len() != 1 {
                            return Err(err(format!(
                                "{owner_utf8} slot {slot_utf8} combines ref with other keys"
                            )));
                        }
                        SlotEntry::Ref(id.clone())
                    } else if let Some(text) = entry.get("text").and_then(RuntimeJson::as_string) {
                        if entry.len() != 1 {
                            return Err(err(format!(
                                "{owner_utf8} slot {slot_utf8} combines text with other keys"
                            )));
                        }
                        SlotEntry::Text(text.clone())
                    } else if let Some(json) = entry.get("json") {
                        if entry.len() != 1 {
                            return Err(err(format!(
                                "{owner_utf8} slot {slot_utf8} combines json with other keys"
                            )));
                        }
                        SlotEntry::Json(json.clone())
                    } else {
                        return Err(err(format!(
                            "{owner_utf8} slot {slot_utf8} has an unrecognized entry shape"
                        )));
                    }
                }
                _ => {
                    return Err(err(format!(
                        "{owner_utf8} slot {slot_utf8} must be null or an object"
                    )))
                }
            };
            Self::validate_entry(&owner_utf8, &slot_utf8, spec, &entry, fragments)?;
            entries.push((slot.clone(), entry));
        }
        Ok(entries)
    }

    fn validate_entry(
        owner: &str,
        slot: &str,
        spec: &super::slots::SlotSpec,
        entry: &SlotEntry,
        fragments: &RuntimeObject,
    ) -> Result<(), CatalogError> {
        match entry {
            SlotEntry::Inherit => Ok(()),
            SlotEntry::Disabled => {
                if spec.disableable {
                    Ok(())
                } else {
                    Err(err(format!("{owner} disables non-disableable slot {slot}")))
                }
            }
            SlotEntry::Ref(id) => {
                if spec.kind != SlotValueKind::Text {
                    return Err(err(format!(
                        "{owner} slot {slot} does not accept text references"
                    )));
                }
                let text = fragments
                    .get(id)
                    .and_then(RuntimeJson::as_string)
                    .ok_or_else(|| {
                        err(format!(
                            "{owner} slot {slot} references unknown fragment {}",
                            key_utf8(id)
                        ))
                    })?;
                Self::validate_len(owner, slot, spec, text_len(text))
            }
            SlotEntry::Text(text) => {
                if spec.kind != SlotValueKind::Text {
                    return Err(err(format!(
                        "{owner} slot {slot} does not accept inline text"
                    )));
                }
                Self::validate_len(owner, slot, spec, text_len(text))
            }
            SlotEntry::Json(value) => {
                if spec.kind != SlotValueKind::Json {
                    return Err(err(format!(
                        "{owner} slot {slot} does not accept structured values"
                    )));
                }
                let serialized = runtime_json::stringify(value);
                Self::validate_len(owner, slot, spec, serialized.len_utf16())
            }
        }
    }

    fn validate_len(
        owner: &str,
        slot: &str,
        spec: &super::slots::SlotSpec,
        len: usize,
    ) -> Result<(), CatalogError> {
        if len <= spec.max_bytes {
            Ok(())
        } else {
            Err(err(format!(
                "{owner} slot {slot} exceeds the {max}-byte cap ({len} bytes)",
                max = spec.max_bytes
            )))
        }
    }

    /// Inheritance chains must be finite: every `extends` target exists and no
    /// chain revisits a profile.
    fn validate_inheritance(&self) -> Result<(), CatalogError> {
        for profile in &self.profiles {
            let mut seen = vec![profile.id.clone()];
            let mut current = profile;
            while let Some(parent_id) = &current.extends {
                let Some(parent) = self.profile(parent_id) else {
                    return Err(err(format!(
                        "profile {} extends unknown profile {}",
                        key_utf8(&current.id),
                        key_utf8(parent_id)
                    )));
                };
                if seen.iter().any(|id| id == parent_id) {
                    return Err(err(format!(
                        "profile {} has a cyclic inheritance chain",
                        key_utf8(&profile.id)
                    )));
                }
                seen.push(parent_id.clone());
                current = parent;
            }
        }
        // Duplicate binding keys are a same-layer conflict.
        for (index, binding) in self.bindings.iter().enumerate() {
            for other in &self.bindings[index + 1..] {
                let same = |a: &RuntimeString, b: &RuntimeString| a == b;
                if same(&binding.source_kind, &other.source_kind)
                    && same(&binding.source_scope, &other.source_scope)
                    && same(&binding.model_key, &other.model_key)
                    && same(&binding.variant_key, &other.variant_key)
                {
                    return Err(err(format!(
                        "bindings[{index}] and a later binding declare the same key"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Revision label for diagnostics (never quotes fragment bodies).
    pub fn revision_utf8(&self) -> String {
        self.revision.try_to_utf8().unwrap_or_default()
    }

    pub fn profile(&self, id: &RuntimeString) -> Option<&CatalogProfile> {
        self.profiles.iter().find(|profile| &profile.id == id)
    }

    pub fn fragment(&self, id: &RuntimeString) -> Option<&RuntimeString> {
        self.fragments.get(id).and_then(RuntimeJson::as_string)
    }

    /// The inheritance chain of a profile, ancestors first and the profile
    /// itself last. Decode-time validation guarantees this terminates.
    pub fn profile_chain(&self, id: &RuntimeString) -> Vec<&CatalogProfile> {
        let mut chain = Vec::new();
        let mut current = self.profile(id);
        while let Some(profile) = current {
            chain.push(profile);
            current = profile.extends.as_ref().and_then(|id| self.profile(id));
        }
        chain.reverse();
        chain
    }

    /// The binding whose key matches, if any. First match wins; decode-time
    /// validation rejects duplicate identical keys.
    pub fn binding(&self, key: &super::resolve::PromptCatalogKey) -> Option<&CatalogBinding> {
        self.bindings.iter().find(|binding| binding.matches(key))
    }

    /// An empty but valid catalog: no profiles beyond nothing, no bindings.
    /// Resolution against it yields exactly the engine defaults.
    pub fn empty(revision: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            revision: RuntimeString::from(revision),
            trusted: true,
            fragments: RuntimeObject::new(),
            profiles: Vec::new(),
            bindings: Vec::new(),
        }
    }
}
