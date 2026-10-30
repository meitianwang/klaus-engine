use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use indexmap::map;
use indexmap::IndexMap;

use crate::runtime_values::{
    RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeJson, RuntimeObject, RuntimeString,
};

const MAX_ARRAY_INDEX: u32 = u32::MAX - 1;
const NODE_V8_COVERAGE: &str = "NODE_V8_COVERAGE";

/// A string-only JavaScript environment object with exact property order.
///
/// The storage is intentionally private: callers can mutate it only with exact
/// [`RuntimeString`] keys and values and can observe it only in ECMAScript
/// property-enumeration order.
#[derive(Clone)]
pub struct RuntimeEnv {
    array_indices: BTreeMap<u32, RuntimeString>,
    string_properties: IndexMap<RuntimeString, RuntimeString>,
}

impl RuntimeEnv {
    /// Construct an empty JavaScript environment object.
    pub fn new() -> Self {
        Self {
            array_indices: BTreeMap::new(),
            string_properties: IndexMap::new(),
        }
    }

    /// Number of exact logical properties.
    pub fn len(&self) -> usize {
        self.array_indices.len() + self.string_properties.len()
    }

    /// Whether the environment has no logical properties.
    pub fn is_empty(&self) -> bool {
        self.array_indices.is_empty() && self.string_properties.is_empty()
    }

    /// Whether this object has the exact logical key as an own property.
    pub fn contains_key(&self, key: &RuntimeString) -> bool {
        if let Some(index) = array_index(key) {
            self.array_indices.contains_key(&index)
        } else {
            self.string_properties.contains_key(key)
        }
    }

    /// Return the exact logical value assigned to `key`.
    pub fn get(&self, key: &RuntimeString) -> Option<&RuntimeString> {
        if let Some(index) = array_index(key) {
            self.array_indices.get(&index)
        } else {
            self.string_properties.get(key)
        }
    }

    /// Assign an exact string value using JavaScript object semantics.
    ///
    /// Overwrite retains an ordinary property's first-insertion slot. Array
    /// index keys are always enumerated in ascending numeric order.
    pub fn insert(&mut self, key: RuntimeString, value: RuntimeString) -> Option<RuntimeString> {
        if let Some(index) = array_index(&key) {
            self.array_indices.insert(index, value)
        } else {
            self.string_properties.insert(key, value)
        }
    }

    /// Delete an exact own property and return its prior value.
    pub fn remove(&mut self, key: &RuntimeString) -> Option<RuntimeString> {
        if let Some(index) = array_index(key) {
            self.array_indices.remove(&index)
        } else {
            self.string_properties.shift_remove(key)
        }
    }

    /// Assign all properties from `overlay` in its ECMAScript enumeration order.
    pub fn overlay(&mut self, overlay: &Self) {
        for (key, value) in overlay.entries() {
            self.insert(key, value.clone());
        }
    }

    /// Enumerate numeric indices followed by ordinary first-insertion slots.
    pub fn entries(&self) -> RuntimeEnvEntries<'_> {
        RuntimeEnvEntries {
            array_indices: self.array_indices.iter(),
            string_properties: self.string_properties.iter(),
        }
    }

    /// Borrow the two backing key domains without formatting array indices.
    ///
    /// This is restricted to the unix terminal process-image adapter
    /// (`runtime_process::unix`), which must keep every allocation explicit and
    /// fallible. Public logical iteration stays in ECMAScript string-key form
    /// through [`Self::entries`].
    #[cfg(unix)]
    pub(super) fn raw_entries(&self) -> RuntimeEnvRawEntries<'_> {
        RuntimeEnvRawEntries {
            array_indices: self.array_indices.iter(),
            string_properties: self.string_properties.iter(),
        }
    }
}

impl Default for RuntimeEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeEncode for RuntimeEnv {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObject::new();
        for (key, value) in self.entries() {
            object.insert(key, RuntimeJson::String(value.clone()));
        }
        out.emit(RuntimeJson::Object(object))
    }
}

impl RuntimeDecode for RuntimeEnv {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(value) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        let mut environment = Self::new();
        for (key, value) in value.entries() {
            let RuntimeJson::String(value) = value else {
                return Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedString).prepend(key),
                );
            };
            environment.insert(key, value.clone());
        }
        Ok(environment)
    }
}

impl PartialEq for RuntimeEnv {
    fn eq(&self, other: &Self) -> bool {
        self.entries().eq(other.entries())
    }
}

impl Eq for RuntimeEnv {}

impl Hash for RuntimeEnv {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.len().hash(state);
        for (key, value) in self.entries() {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl fmt::Debug for RuntimeEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeEnv")
            .field("property_count", &self.len())
            .finish_non_exhaustive()
    }
}

/// Normalize an optional child environment against the observable parent.
///
/// An omitted child clones the parent. An explicit child keeps its own exact
/// properties, then inherits a non-empty parent `NODE_V8_COVERAGE` only when it
/// has no exact own property of that name. In particular, an explicit empty
/// value suppresses propagation.
pub fn normalize_child_env(parent: &RuntimeEnv, explicit: Option<&RuntimeEnv>) -> RuntimeEnv {
    let Some(explicit) = explicit else {
        return parent.clone();
    };

    let mut normalized = explicit.clone();
    let coverage_key = RuntimeString::from_utf8(NODE_V8_COVERAGE);
    if !normalized.contains_key(&coverage_key) {
        if let Some(parent_value) = parent.get(&coverage_key).filter(|value| !value.is_empty()) {
            normalized.insert(coverage_key, parent_value.clone());
        }
    }
    normalized
}

/// Iterator over an environment's exact ECMAScript property order.
pub struct RuntimeEnvEntries<'a> {
    array_indices: btree_map::Iter<'a, u32, RuntimeString>,
    string_properties: map::Iter<'a, RuntimeString, RuntimeString>,
}

impl<'a> Iterator for RuntimeEnvEntries<'a> {
    type Item = (RuntimeString, &'a RuntimeString);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((index, value)) = self.array_indices.next() {
            return Some((RuntimeString::from_utf8(&index.to_string()), value));
        }
        self.string_properties
            .next()
            .map(|(key, value)| (key.clone(), value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.array_indices.len() + self.string_properties.len();
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RuntimeEnvEntries<'_> {}

#[cfg(unix)]
pub(super) enum RuntimeEnvRawKey<'a> {
    ArrayIndex(u32),
    String(&'a RuntimeString),
}

#[cfg(unix)]
pub(super) struct RuntimeEnvRawEntries<'a> {
    array_indices: btree_map::Iter<'a, u32, RuntimeString>,
    string_properties: map::Iter<'a, RuntimeString, RuntimeString>,
}

#[cfg(unix)]
impl<'a> Iterator for RuntimeEnvRawEntries<'a> {
    type Item = (RuntimeEnvRawKey<'a>, &'a RuntimeString);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((index, value)) = self.array_indices.next() {
            return Some((RuntimeEnvRawKey::ArrayIndex(*index), value));
        }
        self.string_properties
            .next()
            .map(|(key, value)| (RuntimeEnvRawKey::String(key), value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.array_indices.len() + self.string_properties.len();
        (remaining, Some(remaining))
    }
}

#[cfg(unix)]
impl ExactSizeIterator for RuntimeEnvRawEntries<'_> {}

fn array_index(key: &RuntimeString) -> Option<u32> {
    let units = key.as_utf16();
    if units.is_empty() || (units.len() > 1 && units[0] == u16::from(b'0')) {
        return None;
    }

    let mut value = 0_u32;
    for unit in units.iter().copied() {
        if !(u16::from(b'0')..=u16::from(b'9')).contains(&unit) {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(u32::from(unit - u16::from(b'0')))?;
        if value > MAX_ARRAY_INDEX {
            return None;
        }
    }
    Some(value)
}
