use std::borrow::Cow;
use std::fmt;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

/// Exact JavaScript string identity as an immutable UTF-16 code-unit sequence.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeString(Arc<[u16]>);

/// Whether [`RuntimeString::to_utf16le`] emits a leading U+FEFF code unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BomPolicy {
    /// Emit only the value's code units.
    Omit,
    /// Prefix the value with a little-endian U+FEFF BOM.
    Emit,
}

/// A fallible scalar conversion encountered an unpaired UTF-16 surrogate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonScalarStringError {
    index: usize,
    code_unit: u16,
}

impl NonScalarStringError {
    /// Zero-based UTF-16 index of the unpaired surrogate.
    pub fn index(self) -> usize {
        self.index
    }

    /// The unpaired surrogate code unit.
    pub fn code_unit(self) -> u16 {
        self.code_unit
    }
}

impl fmt::Display for NonScalarStringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unpaired UTF-16 surrogate \\u{:04x} at code-unit index {}",
            self.code_unit, self.index
        )
    }
}

impl std::error::Error for NonScalarStringError {}

impl RuntimeString {
    /// Construct from an ordinary Unicode-scalar UTF-8 string.
    pub fn from_utf8(value: &str) -> Self {
        Self(Arc::from(value.encode_utf16().collect::<Vec<_>>()))
    }

    /// Construct from arbitrary JavaScript UTF-16 code units.
    pub fn from_utf16(value: impl Into<Vec<u16>>) -> Self {
        Self(Arc::from(value.into()))
    }

    /// Borrow the authoritative UTF-16 code-unit identity.
    pub fn as_utf16(&self) -> &[u16] {
        &self.0
    }

    /// JavaScript `String.length`.
    pub fn len_utf16(&self) -> usize {
        self.0.len()
    }

    /// Whether this value has no code units.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Test an ordinary Unicode-scalar prefix without projecting this value to
    /// UTF-8. The comparison is performed directly over UTF-16 code units, so
    /// a lone surrogate elsewhere in the value remains untouched.
    pub fn starts_with(&self, prefix: &str) -> bool {
        let prefix = prefix.encode_utf16().collect::<Vec<_>>();
        self.0.starts_with(&prefix)
    }

    /// Test an ordinary Unicode-scalar suffix directly over UTF-16 code units.
    pub fn ends_with(&self, suffix: &str) -> bool {
        let suffix = suffix.encode_utf16().collect::<Vec<_>>();
        self.0.ends_with(&suffix)
    }

    /// Search for an ordinary Unicode-scalar substring directly over UTF-16
    /// code units.
    pub fn contains(&self, needle: &str) -> bool {
        let needle = needle.encode_utf16().collect::<Vec<_>>();
        needle.is_empty()
            || self
                .0
                .windows(needle.len())
                .any(|window| window == needle.as_slice())
    }

    /// Replace every non-overlapping ordinary Unicode-scalar match while
    /// preserving every unmatched UTF-16 code unit exactly.
    pub fn replace(&self, from: &str, to: &str) -> Self {
        let from = from.encode_utf16().collect::<Vec<_>>();
        let to = to.encode_utf16().collect::<Vec<_>>();
        if from.is_empty() {
            let mut units = Vec::with_capacity(self.0.len() + (self.0.len() + 1) * to.len());
            units.extend_from_slice(&to);
            for unit in self.0.iter().copied() {
                units.push(unit);
                units.extend_from_slice(&to);
            }
            return Self::from_utf16(units);
        }

        let mut units = Vec::with_capacity(self.0.len());
        let mut index = 0;
        while index < self.0.len() {
            if self.0[index..].starts_with(&from) {
                units.extend_from_slice(&to);
                index += from.len();
            } else {
                units.push(self.0[index]);
                index += 1;
            }
        }
        Self::from_utf16(units)
    }

    /// Slice exact UTF-16 code units, including through a valid surrogate pair.
    pub fn slice_utf16(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.saturating_add(1),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => self.0.len(),
        };
        let start = start.min(self.0.len());
        let end = end.min(self.0.len()).max(start);
        Self(Arc::from(&self.0[start..end]))
    }

    /// Concatenate exact code units without normalizing surrogate boundaries.
    pub fn concat(&self, other: &Self) -> Self {
        let mut units = Vec::with_capacity(self.0.len() + other.0.len());
        units.extend_from_slice(&self.0);
        units.extend_from_slice(&other.0);
        Self(Arc::from(units))
    }

    /// Iterate JavaScript code points while retaining unpaired surrogate values.
    pub fn code_points(&self) -> RuntimeCodePoints<'_> {
        RuntimeCodePoints {
            units: &self.0,
            index: 0,
        }
    }

    /// Convert only when every code unit participates in a Unicode scalar.
    pub fn try_to_utf8(&self) -> Result<String, NonScalarStringError> {
        String::from_utf16(&self.0).map_err(|_| {
            first_unpaired_surrogate(&self.0).expect("from_utf16 failed on an unpaired surrogate")
        })
    }

    /// Apply Node/V8 UTF-8 projection, replacing each unpaired surrogate once.
    pub fn to_node_utf8(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.0.len());
        for point in self.code_points() {
            let scalar = char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER);
            let mut bytes = [0; 4];
            output.extend_from_slice(scalar.encode_utf8(&mut bytes).as_bytes());
        }
        output
    }

    /// Encode the exact code units as little-endian bytes.
    pub fn to_utf16le(&self, bom: BomPolicy) -> Vec<u8> {
        let prefix_units = usize::from(matches!(bom, BomPolicy::Emit));
        let mut output = Vec::with_capacity((self.0.len() + prefix_units) * 2);
        if matches!(bom, BomPolicy::Emit) {
            output.extend_from_slice(&0xfeff_u16.to_le_bytes());
        }
        for unit in self.0.iter().copied() {
            output.extend_from_slice(&unit.to_le_bytes());
        }
        output
    }
}

impl fmt::Debug for RuntimeString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RuntimeString(\"")?;
        let mut index = 0;
        while index < self.0.len() {
            let unit = self.0[index];
            if is_high_surrogate(unit)
                && self.0.get(index + 1).copied().is_some_and(is_low_surrogate)
            {
                let low = self.0[index + 1];
                let point = combine_surrogates(unit, low);
                for escaped in char::from_u32(point)
                    .expect("valid surrogate pair")
                    .escape_debug()
                {
                    formatter.write_fmt(format_args!("{escaped}"))?;
                }
                index += 2;
            } else if is_surrogate(unit) {
                write!(formatter, "\\u{unit:04x}")?;
                index += 1;
            } else {
                for escaped in char::from_u32(u32::from(unit))
                    .expect("non-surrogate u16 is a Unicode scalar")
                    .escape_debug()
                {
                    formatter.write_fmt(format_args!("{escaped}"))?;
                }
                index += 1;
            }
        }
        formatter.write_str("\")")
    }
}

impl From<&str> for RuntimeString {
    fn from(value: &str) -> Self {
        Self::from_utf8(value)
    }
}

impl From<String> for RuntimeString {
    fn from(value: String) -> Self {
        Self::from_utf8(&value)
    }
}

impl From<&String> for RuntimeString {
    fn from(value: &String) -> Self {
        Self::from_utf8(value)
    }
}

impl From<&RuntimeString> for RuntimeString {
    fn from(value: &RuntimeString) -> Self {
        value.clone()
    }
}

impl<'a> From<Cow<'a, str>> for RuntimeString {
    fn from(value: Cow<'a, str>) -> Self {
        Self::from_utf8(&value)
    }
}

impl PartialEq<str> for RuntimeString {
    fn eq(&self, other: &str) -> bool {
        self.0.iter().copied().eq(other.encode_utf16())
    }
}

impl PartialEq<&str> for RuntimeString {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<String> for RuntimeString {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<RuntimeString> for str {
    fn eq(&self, other: &RuntimeString) -> bool {
        other == self
    }
}

impl PartialEq<RuntimeString> for &str {
    fn eq(&self, other: &RuntimeString) -> bool {
        other == *self
    }
}

impl PartialEq<RuntimeString> for String {
    fn eq(&self, other: &RuntimeString) -> bool {
        other == self.as_str()
    }
}

/// Iterator returned by [`RuntimeString::code_points`].
#[derive(Clone, Debug)]
pub struct RuntimeCodePoints<'a> {
    units: &'a [u16],
    index: usize,
}

impl Iterator for RuntimeCodePoints<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let first = *self.units.get(self.index)?;
        self.index += 1;
        if is_high_surrogate(first) {
            if let Some(second) = self
                .units
                .get(self.index)
                .copied()
                .filter(|u| is_low_surrogate(*u))
            {
                self.index += 1;
                return Some(combine_surrogates(first, second));
            }
        }
        Some(u32::from(first))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.units.len().saturating_sub(self.index)))
    }
}

fn first_unpaired_surrogate(units: &[u16]) -> Option<NonScalarStringError> {
    let mut index = 0;
    while index < units.len() {
        let unit = units[index];
        if is_high_surrogate(unit) {
            if units.get(index + 1).copied().is_some_and(is_low_surrogate) {
                index += 2;
                continue;
            }
            return Some(NonScalarStringError {
                index,
                code_unit: unit,
            });
        }
        if is_low_surrogate(unit) {
            return Some(NonScalarStringError {
                index,
                code_unit: unit,
            });
        }
        index += 1;
    }
    None
}

fn is_surrogate(unit: u16) -> bool {
    (0xd800..=0xdfff).contains(&unit)
}

fn is_high_surrogate(unit: u16) -> bool {
    (0xd800..=0xdbff).contains(&unit)
}

fn is_low_surrogate(unit: u16) -> bool {
    (0xdc00..=0xdfff).contains(&unit)
}

fn combine_surrogates(high: u16, low: u16) -> u32 {
    0x1_0000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00)
}
