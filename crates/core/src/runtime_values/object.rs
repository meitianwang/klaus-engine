use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use indexmap::map;
use indexmap::IndexMap;

use super::{RuntimeJson, RuntimeString};

const MAX_ARRAY_INDEX: u32 = u32::MAX - 1;

/// JavaScript object storage with exact ECMAScript observable property order.
#[derive(Clone, Default)]
pub struct RuntimeObject {
    array_indices: BTreeMap<u32, RuntimeJson>,
    string_properties: IndexMap<RuntimeString, RuntimeJson>,
}

impl RuntimeObject {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.array_indices.len() + self.string_properties.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array_indices.is_empty() && self.string_properties.is_empty()
    }

    pub fn insert(
        &mut self,
        key: impl Into<RuntimeString>,
        value: RuntimeJson,
    ) -> Option<RuntimeJson> {
        let key = key.into();
        if let Some(index) = array_index(&key) {
            self.array_indices.insert(index, value)
        } else {
            self.string_properties.insert(key, value)
        }
    }

    pub fn get(&self, key: impl Into<RuntimeString>) -> Option<&RuntimeJson> {
        let key = key.into();
        if let Some(index) = array_index(&key) {
            self.array_indices.get(&index)
        } else {
            self.string_properties.get(&key)
        }
    }

    pub fn get_mut(&mut self, key: impl Into<RuntimeString>) -> Option<&mut RuntimeJson> {
        let key = key.into();
        if let Some(index) = array_index(&key) {
            self.array_indices.get_mut(&index)
        } else {
            self.string_properties.get_mut(&key)
        }
    }

    pub fn contains_key(&self, key: impl Into<RuntimeString>) -> bool {
        self.get(key).is_some()
    }

    pub fn remove(&mut self, key: impl Into<RuntimeString>) -> Option<RuntimeJson> {
        let key = key.into();
        if let Some(index) = array_index(&key) {
            self.array_indices.remove(&index)
        } else {
            self.string_properties.shift_remove(&key)
        }
    }

    pub fn entries(&self) -> RuntimeObjectEntries<'_> {
        RuntimeObjectEntries {
            array_indices: self.array_indices.iter(),
            string_properties: self.string_properties.iter(),
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = RuntimeString> + '_ {
        self.array_indices
            .keys()
            .map(|index| RuntimeString::from(index.to_string()))
            .chain(self.string_properties.keys().cloned())
    }

    pub fn values(&self) -> impl Iterator<Item = &RuntimeJson> + '_ {
        self.array_indices
            .values()
            .chain(self.string_properties.values())
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut RuntimeJson> + '_ {
        self.array_indices
            .values_mut()
            .chain(self.string_properties.values_mut())
    }

    pub fn semantic_eq_unordered(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.entries().all(|(key, value)| {
                other
                    .get(key)
                    .is_some_and(|other| value.semantic_eq_unordered(other))
            })
    }

    pub(crate) fn drain_values_into(&mut self, pending: &mut Vec<RuntimeJson>) {
        pending.extend(std::mem::take(&mut self.array_indices).into_values());
        pending.extend(std::mem::take(&mut self.string_properties).into_values());
    }
}

impl PartialEq for RuntimeObject {
    fn eq(&self, other: &Self) -> bool {
        self.entries().eq(other.entries())
    }
}

impl Eq for RuntimeObject {}

impl Hash for RuntimeObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.len().hash(state);
        for (key, value) in self.entries() {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl fmt::Debug for RuntimeObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.entries()).finish()
    }
}

/// Iterator over array indices followed by ordinary first-insertion properties.
pub struct RuntimeObjectEntries<'a> {
    array_indices: btree_map::Iter<'a, u32, RuntimeJson>,
    string_properties: map::Iter<'a, RuntimeString, RuntimeJson>,
}

impl<'a> Iterator for RuntimeObjectEntries<'a> {
    type Item = (RuntimeString, &'a RuntimeJson);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((index, value)) = self.array_indices.next() {
            return Some((RuntimeString::from(index.to_string()), value));
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

impl ExactSizeIterator for RuntimeObjectEntries<'_> {}

impl<'a> IntoIterator for &'a RuntimeObject {
    type Item = (RuntimeString, &'a RuntimeJson);
    type IntoIter = RuntimeObjectEntries<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries()
    }
}

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
