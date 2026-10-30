use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use super::parser::RuntimeJsonError;
use super::{runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString};

/// Closed, stable typed-runtime codec failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeCodecErrorKind {
    EncoderAlreadySet,
    EncoderMissingValue,
    MissingField,
    DuplicateField,
    UnknownField,
    ExpectedNull,
    ExpectedBool,
    ExpectedNumber,
    ExpectedString,
    ExpectedArray,
    ExpectedObject,
    NonFiniteInteger,
    NonIntegralInteger,
    IntegerOutOfRange,
    InvalidEnumShape,
    UnknownEnumVariant,
    AmbiguousEnumVariant,
    NoMatchingUntaggedVariant,
    InvalidJson(RuntimeJsonError),
    Custom(RuntimeString),
}

/// Typed-runtime codec failure with an exact JSON-pointer path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCodecError {
    path: Vec<RuntimeString>,
    kind: RuntimeCodecErrorKind,
}

impl RuntimeCodecError {
    pub fn new(kind: RuntimeCodecErrorKind) -> Self {
        Self {
            path: Vec::new(),
            kind,
        }
    }

    pub fn kind(&self) -> &RuntimeCodecErrorKind {
        &self.kind
    }

    pub fn path(&self) -> &[RuntimeString] {
        &self.path
    }

    pub fn pointer(&self) -> RuntimeString {
        let mut units = Vec::new();
        for segment in &self.path {
            units.push(u16::from(b'/'));
            for unit in segment.as_utf16().iter().copied() {
                match unit {
                    unit if unit == u16::from(b'~') => {
                        units.extend([u16::from(b'~'), u16::from(b'0')]);
                    }
                    unit if unit == u16::from(b'/') => {
                        units.extend([u16::from(b'~'), u16::from(b'1')]);
                    }
                    unit => units.push(unit),
                }
            }
        }
        RuntimeString::from_utf16(units)
    }

    pub fn prepend(mut self, segment: impl Into<RuntimeString>) -> Self {
        self.path.insert(0, segment.into());
        self
    }
}

impl fmt::Display for RuntimeCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Custom messages are human-authored; print them verbatim instead of
        // through the Debug wrapper so callers can match on their content.
        match &self.kind {
            RuntimeCodecErrorKind::Custom(message) => match message.try_to_utf8() {
                Ok(message) => write!(formatter, "runtime codec {message} at ")?,
                Err(_) => write!(formatter, "runtime codec {message:?} at ")?,
            },
            kind => write!(formatter, "runtime codec {kind:?} at ")?,
        }
        let pointer = self.pointer();
        if pointer.is_empty() {
            formatter.write_str("<root>")
        } else {
            match pointer.try_to_utf8() {
                Ok(pointer) => formatter.write_str(&pointer),
                Err(_) => write!(formatter, "{pointer:?}"),
            }
        }
    }
}

impl std::error::Error for RuntimeCodecError {}

/// Explicit exact-runtime DTO encoder.
pub trait RuntimeEncode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError>;
}

/// Explicit exact-runtime DTO decoder.
pub trait RuntimeDecode: Sized {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError>;
}

/// One declaration-ordered enum tag and its accepted decode-only aliases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEnumVariant<'a> {
    canonical: &'a str,
    aliases: &'a [&'a str],
}

impl<'a> RuntimeEnumVariant<'a> {
    /// Define the one canonical encode tag and every accepted decode alias.
    pub const fn new(canonical: &'a str, aliases: &'a [&'a str]) -> Self {
        Self { canonical, aliases }
    }

    /// Return the only tag emitted for this variant.
    pub const fn canonical(self) -> &'a str {
        self.canonical
    }

    fn matches(self, tag: &RuntimeString) -> bool {
        runtime_tag_eq(tag, self.canonical)
            || self.aliases.iter().any(|alias| runtime_tag_eq(tag, alias))
    }
}

/// Materialized shape of an externally tagged enum after exact tag resolution.
#[derive(Clone, Copy, Debug)]
pub enum RuntimeExternalVariant<'a> {
    /// A canonical or aliased string selected a unit variant.
    Unit { variant: usize },
    /// A one-property object selected a payload-bearing variant.
    Payload {
        variant: usize,
        value: &'a RuntimeJson,
    },
}

/// One ordered attempt used by an untagged enum decoder.
pub type RuntimeUntaggedVariantDecoder<T> = fn(&RuntimeJson) -> Result<T, RuntimeCodecError>;

/// A typed map with exact [`RuntimeString`] keys and `RuntimeObject` ordering.
#[derive(Clone)]
pub struct RuntimeMap<V> {
    order: RuntimeObject,
    values: HashMap<RuntimeString, V>,
}

impl<V> RuntimeMap<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn insert(&mut self, key: RuntimeString, value: V) -> Option<V> {
        self.order.insert(key.clone(), RuntimeJson::Null);
        self.values.insert(key, value)
    }

    pub fn get(&self, key: &RuntimeString) -> Option<&V> {
        self.values.get(key)
    }

    pub fn remove(&mut self, key: &RuntimeString) -> Option<V> {
        self.order.remove(key.clone());
        self.values.remove(key)
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = (RuntimeString, &V)> + '_ {
        self.order.entries().map(|(key, _)| {
            let value = self
                .values
                .get(&key)
                .expect("runtime map order and value storage remain synchronized");
            (key, value)
        })
    }
}

impl<V> Default for RuntimeMap<V> {
    fn default() -> Self {
        Self {
            order: RuntimeObject::new(),
            values: HashMap::new(),
        }
    }
}

impl<V: PartialEq> PartialEq for RuntimeMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.entries().eq(other.entries())
    }
}

impl<V: Eq> Eq for RuntimeMap<V> {}

impl<V: fmt::Debug> fmt::Debug for RuntimeMap<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.entries()).finish()
    }
}

/// Single-value target used by explicit [`RuntimeEncode`] implementations.
#[derive(Debug, Default)]
pub struct RuntimeEncoder {
    value: Option<RuntimeJson>,
    poisoned: bool,
}

impl RuntimeEncoder {
    pub fn emit(&mut self, value: RuntimeJson) -> Result<(), RuntimeCodecError> {
        if self.poisoned || self.value.is_some() {
            self.poisoned = true;
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::EncoderAlreadySet,
            ));
        }
        self.value = Some(value);
        Ok(())
    }

    fn finish(self) -> Result<RuntimeJson, RuntimeCodecError> {
        if self.poisoned {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::EncoderAlreadySet,
            ));
        }
        self.value
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::EncoderMissingValue))
    }
}

pub fn encode_value<T: RuntimeEncode + ?Sized>(
    value: &T,
) -> Result<RuntimeJson, RuntimeCodecError> {
    let mut encoder = RuntimeEncoder::default();
    value.encode_runtime(&mut encoder)?;
    encoder.finish()
}

pub fn encode_text<T: RuntimeEncode + ?Sized>(
    value: &T,
) -> Result<RuntimeString, RuntimeCodecError> {
    encode_value(value).map(|value| runtime_json::stringify(&value))
}

pub fn encode_bytes<T: RuntimeEncode + ?Sized>(value: &T) -> Result<Vec<u8>, RuntimeCodecError> {
    encode_text(value).map(|value| value.to_node_utf8())
}

pub fn decode_value<T: RuntimeDecode>(value: &RuntimeJson) -> Result<T, RuntimeCodecError> {
    T::decode_runtime(value)
}

pub fn decode_text<T: RuntimeDecode>(value: &RuntimeString) -> Result<T, RuntimeCodecError> {
    let value = runtime_json::parse(value)
        .map_err(|error| RuntimeCodecError::new(RuntimeCodecErrorKind::InvalidJson(error)))?;
    decode_value(&value)
}

/// Decode an object through one scope that always enforces its unknown-field
/// policy after the handwritten/declarative field body succeeds.
pub fn decode_object<T>(
    value: &RuntimeJson,
    deny_unknown: bool,
    decode: impl FnOnce(&mut RuntimeObjectDecoder<'_>) -> Result<T, RuntimeCodecError>,
) -> Result<T, RuntimeCodecError> {
    let mut object = RuntimeObjectDecoder::new(value, deny_unknown)?;
    let decoded = decode(&mut object)?;
    object.finish()?;
    Ok(decoded)
}

/// Encode an externally tagged unit variant as its canonical string.
pub fn encode_external_unit(
    out: &mut RuntimeEncoder,
    canonical_tag: &str,
) -> Result<(), RuntimeCodecError> {
    out.emit(RuntimeJson::String(RuntimeString::from(canonical_tag)))
}

/// Encode an externally tagged payload as an exact one-property object.
pub fn encode_external_payload<T: RuntimeEncode + ?Sized>(
    out: &mut RuntimeEncoder,
    canonical_tag: &str,
    payload: &T,
) -> Result<(), RuntimeCodecError> {
    let tag = RuntimeString::from(canonical_tag);
    let payload = encode_value(payload).map_err(|error| error.prepend(tag.clone()))?;
    let mut object = RuntimeObject::new();
    object.insert(tag, payload);
    out.emit(RuntimeJson::Object(object))
}

/// Decode the exact string or single-key object shape of an external enum.
pub fn decode_external<'value, T>(
    value: &'value RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    decode: impl FnOnce(RuntimeExternalVariant<'value>) -> Result<T, RuntimeCodecError>,
) -> Result<T, RuntimeCodecError> {
    match value {
        RuntimeJson::String(tag) => {
            let variant = resolve_enum_variant(tag, variants)?;
            decode(RuntimeExternalVariant::Unit { variant })
        }
        RuntimeJson::Object(object) => {
            let mut entries = object.entries();
            let Some((tag, value)) = entries.next() else {
                return Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::InvalidEnumShape,
                ));
            };
            if entries.next().is_some() {
                return Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::InvalidEnumShape,
                ));
            }
            let variant =
                resolve_enum_variant(&tag, variants).map_err(|error| error.prepend(tag.clone()))?;
            decode(RuntimeExternalVariant::Payload { variant, value })
                .map_err(|error| error.prepend(variants[variant].canonical()))
        }
        _ => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    }
}

/// Encode an internally tagged variant with the tag assignment before fields.
pub fn encode_internal(
    out: &mut RuntimeEncoder,
    tag_field: &str,
    tag_aliases: &[&str],
    canonical_tag: &str,
    fields: &RuntimeObject,
) -> Result<(), RuntimeCodecError> {
    for field_name in std::iter::once(tag_field).chain(tag_aliases.iter().copied()) {
        if fields.get(field_name).is_some() {
            return Err(field_error(
                tag_field,
                RuntimeCodecErrorKind::DuplicateField,
            ));
        }
    }

    let mut object = RuntimeObject::new();
    object.insert(
        RuntimeString::from(tag_field),
        RuntimeJson::String(RuntimeString::from(canonical_tag)),
    );
    for (key, value) in fields.entries() {
        object.insert(key, value.clone());
    }
    out.emit(RuntimeJson::Object(object))
}

/// Decode an internally tagged object and finalize its unknown-field policy.
pub fn decode_internal<T>(
    value: &RuntimeJson,
    deny_unknown: bool,
    tag_field: &str,
    tag_aliases: &[&str],
    variants: &[RuntimeEnumVariant<'_>],
    decode: impl FnOnce(usize, &mut RuntimeObjectDecoder<'_>) -> Result<T, RuntimeCodecError>,
) -> Result<T, RuntimeCodecError> {
    decode_object(value, deny_unknown, |object| {
        let tag: RuntimeString = object.required(tag_field, tag_aliases)?;
        let variant =
            resolve_enum_variant(&tag, variants).map_err(|error| error.prepend(tag_field))?;
        decode(variant, object)
    })
}

/// Encode an adjacently tagged payload with tag followed by content.
#[allow(clippy::too_many_arguments)]
pub fn encode_adjacent_payload<T: RuntimeEncode + ?Sized>(
    out: &mut RuntimeEncoder,
    tag_field: &str,
    tag_aliases: &[&str],
    content_field: &str,
    content_aliases: &[&str],
    canonical_tag: &str,
    payload: &T,
) -> Result<(), RuntimeCodecError> {
    reject_overlapping_enum_fields(tag_field, tag_aliases, content_field, content_aliases)?;
    let payload = encode_value(payload).map_err(|error| error.prepend(content_field))?;
    let mut object = RuntimeObject::new();
    object.insert(
        RuntimeString::from(tag_field),
        RuntimeJson::String(RuntimeString::from(canonical_tag)),
    );
    object.insert(RuntimeString::from(content_field), payload);
    out.emit(RuntimeJson::Object(object))
}

/// Encode the explicit omitted-content form of an adjacent unit variant.
pub fn encode_adjacent_unit(
    out: &mut RuntimeEncoder,
    tag_field: &str,
    tag_aliases: &[&str],
    content_field: &str,
    content_aliases: &[&str],
    canonical_tag: &str,
) -> Result<(), RuntimeCodecError> {
    reject_overlapping_enum_fields(tag_field, tag_aliases, content_field, content_aliases)?;
    let mut object = RuntimeObject::new();
    object.insert(
        RuntimeString::from(tag_field),
        RuntimeJson::String(RuntimeString::from(canonical_tag)),
    );
    out.emit(RuntimeJson::Object(object))
}

/// Decode an adjacent enum with an explicit allowlist for omitted unit content.
#[allow(clippy::too_many_arguments)]
pub fn decode_adjacent<T>(
    value: &RuntimeJson,
    deny_unknown: bool,
    tag_field: &str,
    tag_aliases: &[&str],
    content_field: &str,
    content_aliases: &[&str],
    variants: &[RuntimeEnumVariant<'_>],
    omitted_content_variants: &[usize],
    decode: impl FnOnce(usize, Option<&RuntimeJson>) -> Result<T, RuntimeCodecError>,
) -> Result<T, RuntimeCodecError> {
    reject_overlapping_enum_fields(tag_field, tag_aliases, content_field, content_aliases)?;
    decode_object(value, deny_unknown, |object| {
        let tag: RuntimeString = object.required(tag_field, tag_aliases)?;
        let variant =
            resolve_enum_variant(&tag, variants).map_err(|error| error.prepend(tag_field))?;
        let content = object.field_value(content_field, content_aliases)?;
        if content.is_none() && !omitted_content_variants.contains(&variant) {
            return Err(field_error(
                content_field,
                RuntimeCodecErrorKind::MissingField,
            ));
        }
        decode(variant, content).map_err(|error| error.prepend(content_field))
    })
}

/// Try untagged variants in declaration order against the same immutable value.
pub fn decode_untagged<T>(
    value: &RuntimeJson,
    variants: &[RuntimeUntaggedVariantDecoder<T>],
) -> Result<T, RuntimeCodecError> {
    for decode in variants {
        if let Ok(decoded) = decode(value) {
            return Ok(decoded);
        }
    }
    Err(RuntimeCodecError::new(
        RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
    ))
}

fn resolve_enum_variant(
    tag: &RuntimeString,
    variants: &[RuntimeEnumVariant<'_>],
) -> Result<usize, RuntimeCodecError> {
    let mut found = None;
    for (index, variant) in variants.iter().copied().enumerate() {
        if variant.matches(tag) {
            if found.is_some() {
                return Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::AmbiguousEnumVariant,
                ));
            }
            found = Some(index);
        }
    }
    found.ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant))
}

fn runtime_tag_eq(tag: &RuntimeString, candidate: &str) -> bool {
    tag.as_utf16().iter().copied().eq(candidate.encode_utf16())
}

fn reject_overlapping_enum_fields(
    first_canonical: &str,
    first_aliases: &[&str],
    second_canonical: &str,
    second_aliases: &[&str],
) -> Result<(), RuntimeCodecError> {
    for first in std::iter::once(first_canonical).chain(first_aliases.iter().copied()) {
        for second in std::iter::once(second_canonical).chain(second_aliases.iter().copied()) {
            if first == second {
                return Err(field_error(
                    second_canonical,
                    RuntimeCodecErrorKind::DuplicateField,
                ));
            }
        }
    }
    Ok(())
}

/// Declaration-ordered object builder for handwritten/declarative DTO codecs.
#[derive(Debug, Default)]
pub struct RuntimeObjectEncoder {
    value: RuntimeObject,
}

impl RuntimeObjectEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field<T: RuntimeEncode + ?Sized>(
        &mut self,
        name: impl Into<RuntimeString>,
        value: &T,
    ) -> Result<(), RuntimeCodecError> {
        let name = name.into();
        let value = encode_value(value).map_err(|error| error.prepend(name.clone()))?;
        self.value.insert(name, value);
        Ok(())
    }

    pub fn field_value(&mut self, name: impl Into<RuntimeString>, value: RuntimeJson) {
        self.value.insert(name, value);
    }

    pub fn flatten<T: RuntimeEncode + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), RuntimeCodecError> {
        let value = encode_value(value)?;
        let RuntimeJson::Object(value) = &value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        for (key, value) in value.entries() {
            self.value.insert(key, value.clone());
        }
        Ok(())
    }

    pub fn finish(self) -> RuntimeObject {
        self.value
    }
}

/// Contract-driven field resolver over a fully materialized runtime object.
pub struct RuntimeObjectDecoder<'a> {
    value: &'a RuntimeObject,
    consumed: HashSet<RuntimeString>,
    deny_unknown: bool,
}

impl<'a> RuntimeObjectDecoder<'a> {
    fn new(value: &'a RuntimeJson, deny_unknown: bool) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(value) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        Ok(Self {
            value,
            consumed: HashSet::new(),
            deny_unknown,
        })
    }

    pub fn required<T: RuntimeDecode>(
        &mut self,
        canonical: &str,
        aliases: &[&str],
    ) -> Result<T, RuntimeCodecError> {
        let value = self
            .field_value(canonical, aliases)?
            .ok_or_else(|| field_error(canonical, RuntimeCodecErrorKind::MissingField))?;
        T::decode_runtime(value).map_err(|error| error.prepend(canonical))
    }

    pub fn optional<T: RuntimeDecode>(
        &mut self,
        canonical: &str,
        aliases: &[&str],
    ) -> Result<Option<T>, RuntimeCodecError> {
        match self.field_value(canonical, aliases)? {
            None | Some(RuntimeJson::Null) => Ok(None),
            Some(value) => T::decode_runtime(value)
                .map(Some)
                .map_err(|error| error.prepend(canonical)),
        }
    }

    /// Distinguish a missing field from explicit JSON null at mutation boundaries.
    pub fn optional_nullable<T: RuntimeDecode>(
        &mut self,
        canonical: &str,
        aliases: &[&str],
    ) -> Result<Option<Option<T>>, RuntimeCodecError> {
        match self.field_value(canonical, aliases)? {
            None => Ok(None),
            Some(RuntimeJson::Null) => Ok(Some(None)),
            Some(value) => T::decode_runtime(value)
                .map(|value| Some(Some(value)))
                .map_err(|error| error.prepend(canonical)),
        }
    }

    pub fn defaulted<T: RuntimeDecode>(
        &mut self,
        canonical: &str,
        aliases: &[&str],
        default: impl FnOnce() -> T,
    ) -> Result<T, RuntimeCodecError> {
        match self.field_value(canonical, aliases)? {
            None => Ok(default()),
            Some(value) => T::decode_runtime(value).map_err(|error| error.prepend(canonical)),
        }
    }

    /// Decode flattened DTO fields against this exact shared consumed-key set.
    pub fn flatten_dto<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, RuntimeCodecError>,
    ) -> Result<T, RuntimeCodecError> {
        decode(self)
    }

    /// Decode every still-unconsumed field as one exact runtime-key map.
    pub fn flatten_map<V: RuntimeDecode>(&mut self) -> Result<RuntimeMap<V>, RuntimeCodecError> {
        let mut remaining = RuntimeMap::new();
        for (key, value) in self.value.entries() {
            if self.consumed.contains(&key) {
                continue;
            }
            let decoded = V::decode_runtime(value).map_err(|error| error.prepend(key.clone()))?;
            self.consumed.insert(key.clone());
            remaining.insert(key, decoded);
        }
        Ok(remaining)
    }

    pub fn take_remaining(&mut self) -> RuntimeObject {
        let mut remaining = RuntimeObject::new();
        for (key, value) in self.value.entries() {
            if self.consumed.insert(key.clone()) {
                remaining.insert(key, value.clone());
            }
        }
        remaining
    }

    fn finish(self) -> Result<(), RuntimeCodecError> {
        if self.deny_unknown {
            if let Some((key, _)) = self
                .value
                .entries()
                .find(|(key, _)| !self.consumed.contains(key))
            {
                return Err(
                    RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownField).prepend(key)
                );
            }
        }
        Ok(())
    }

    fn field_value(
        &mut self,
        canonical: &str,
        aliases: &[&str],
    ) -> Result<Option<&'a RuntimeJson>, RuntimeCodecError> {
        let mut found = None;
        for name in std::iter::once(canonical).chain(aliases.iter().copied()) {
            let key = RuntimeString::from(name);
            if let Some(value) = self.value.get(key.clone()) {
                if self.consumed.contains(&key) || found.is_some() {
                    return Err(field_error(
                        canonical,
                        RuntimeCodecErrorKind::DuplicateField,
                    ));
                }
                found = Some((key, value));
            }
        }
        if let Some((key, value)) = found {
            self.consumed.insert(key);
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

fn field_error(name: &str, kind: RuntimeCodecErrorKind) -> RuntimeCodecError {
    RuntimeCodecError::new(kind).prepend(name)
}

macro_rules! impl_direct_codec {
    ($type:ty, $variant:ident, $expected:ident) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                out.emit(RuntimeJson::$variant(self.clone()))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                match value {
                    RuntimeJson::$variant(value) => Ok(value.clone()),
                    _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::$expected)),
                }
            }
        }
    };
}

impl_direct_codec!(RuntimeString, String, ExpectedString);
impl_direct_codec!(RuntimeObject, Object, ExpectedObject);

impl RuntimeEncode for str {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        RuntimeString::from(self).encode_runtime(out)
    }
}

impl RuntimeEncode for String {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.as_str().encode_runtime(out)
    }
}

impl<T: RuntimeEncode + ?Sized> RuntimeEncode for &T {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        (*self).encode_runtime(out)
    }
}

impl<V: RuntimeEncode> RuntimeEncode for RuntimeMap<V> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObject::new();
        for (key, value) in self.entries() {
            let encoded = encode_value(value).map_err(|error| error.prepend(key.clone()))?;
            object.insert(key, encoded);
        }
        out.emit(RuntimeJson::Object(object))
    }
}

impl<V: RuntimeDecode> RuntimeDecode for RuntimeMap<V> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(object) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        let mut decoded = Self::new();
        for (key, value) in object.entries() {
            let value = V::decode_runtime(value).map_err(|error| error.prepend(key.clone()))?;
            decoded.insert(key, value);
        }
        Ok(decoded)
    }
}

impl<V: RuntimeEncode> RuntimeEncode for BTreeMap<RuntimeString, V> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObject::new();
        for (key, value) in self {
            let encoded = encode_value(value).map_err(|error| error.prepend(key.clone()))?;
            object.insert(key.clone(), encoded);
        }
        out.emit(RuntimeJson::Object(object))
    }
}

impl<V: RuntimeDecode> RuntimeDecode for BTreeMap<RuntimeString, V> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Object(object) = value else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedObject,
            ));
        };
        object
            .entries()
            .map(|(key, value)| {
                V::decode_runtime(value)
                    .map(|value| (key.clone(), value))
                    .map_err(|error| error.prepend(key))
            })
            .collect()
    }
}

impl<T: RuntimeEncode + ?Sized> RuntimeEncode for Box<T> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.as_ref().encode_runtime(out)
    }
}

impl<T: RuntimeDecode> RuntimeDecode for Box<T> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        T::decode_runtime(value).map(Box::new)
    }
}

impl RuntimeEncode for RuntimeJson {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(self.clone())
    }
}

impl RuntimeDecode for RuntimeJson {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        Ok(value.clone())
    }
}

impl RuntimeEncode for bool {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(RuntimeJson::Bool(*self))
    }
}

impl RuntimeDecode for bool {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Bool(value) => Ok(*value),
            _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedBool)),
        }
    }
}

impl RuntimeEncode for () {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(RuntimeJson::Null)
    }
}

impl RuntimeDecode for () {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Null => Ok(()),
            _ => Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedNull)),
        }
    }
}

impl<T: RuntimeEncode> RuntimeEncode for Option<T> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Some(value) => value.encode_runtime(out),
            None => out.emit(RuntimeJson::Null),
        }
    }
}

impl<T: RuntimeDecode> RuntimeDecode for Option<T> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Null => Ok(None),
            value => T::decode_runtime(value).map(Some),
        }
    }
}

impl<T: RuntimeEncode> RuntimeEncode for Vec<T> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let values = self
            .iter()
            .enumerate()
            .map(|(index, value)| {
                encode_value(value).map_err(|error| error.prepend(index.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        out.emit(RuntimeJson::Array(values))
    }
}

impl<T: RuntimeDecode> RuntimeDecode for Vec<T> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let RuntimeJson::Array(values) = value else {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::ExpectedArray));
        };
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                T::decode_runtime(value).map_err(|error| error.prepend(index.to_string()))
            })
            .collect()
    }
}

impl RuntimeEncode for RuntimeNumber {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        out.emit(RuntimeJson::Number(*self))
    }
}

impl RuntimeDecode for RuntimeNumber {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::Number(value) => Ok(*value),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::ExpectedNumber,
            )),
        }
    }
}

impl RuntimeEncode for f64 {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        RuntimeNumber::from_f64(*self).encode_runtime(out)
    }
}

impl RuntimeDecode for f64 {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        RuntimeNumber::decode_runtime(value).map(RuntimeNumber::as_f64)
    }
}

macro_rules! impl_integer_codec {
    (signed: $($signed:ty),+; unsigned: $($unsigned:ty),+ $(;)?) => {
        $(
            impl RuntimeEncode for $signed {
                fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                    RuntimeNumber::from(*self).encode_runtime(out)
                }
            }

            impl RuntimeDecode for $signed {
                fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                    let value = integer_number(value)?;
                    let bound = 2_f64.powi((<$signed>::BITS - 1) as i32);
                    if value < -bound || value >= bound {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::IntegerOutOfRange));
                    }
                    Ok(value as $signed)
                }
            }
        )+
        $(
            impl RuntimeEncode for $unsigned {
                fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                    RuntimeNumber::from(*self).encode_runtime(out)
                }
            }

            impl RuntimeDecode for $unsigned {
                fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                    let value = integer_number(value)?;
                    let upper = 2_f64.powi(<$unsigned>::BITS as i32);
                    if value < 0.0 || value >= upper {
                        return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::IntegerOutOfRange));
                    }
                    Ok(value as $unsigned)
                }
            }
        )+
    };
}

impl_integer_codec!(
    signed: i8, i16, i32, i64, i128, isize;
    unsigned: u8, u16, u32, u64, u128, usize;
);

fn integer_number(value: &RuntimeJson) -> Result<f64, RuntimeCodecError> {
    let value = RuntimeNumber::decode_runtime(value)?.as_f64();
    if !value.is_finite() {
        return Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::NonFiniteInteger,
        ));
    }
    if value.fract() != 0.0 {
        return Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::NonIntegralInteger,
        ));
    }
    Ok(value)
}
