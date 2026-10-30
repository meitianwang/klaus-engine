//! Memory record types.

use serde::{Deserialize, Serialize};

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// The kind of memory a record holds, used for filtering and prompt framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryType {
    User,
    Feedback,
    Project,
    Reference,
}

impl RuntimeEncode for MemoryType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::User => "user",
            Self::Feedback => "feedback",
            Self::Project => "project",
            Self::Reference => "reference",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for MemoryType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("feedback", &[]),
                RuntimeEnumVariant::new("project", &[]),
                RuntimeEnumVariant::new("reference", &[]),
            ],
            &[Self::User, Self::Feedback, Self::Project, Self::Reference],
        )
    }
}

impl MemoryType {
    /// The wire/string form (`user` | `feedback` | `project` | `reference`).
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryType::User => "user",
            MemoryType::Feedback => "feedback",
            MemoryType::Project => "project",
            MemoryType::Reference => "reference",
        }
    }
}

/// A single durable memory, persisted as a markdown file with YAML-ish
/// frontmatter. `path` (absolute on-disk location) and `mtime_ms` (last-modified
/// time, for freshness/staleness surfacing) are populated by
/// the store on read; neither is persisted into the frontmatter itself.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub memory_type: MemoryType,
    pub body: RuntimeString,
    pub path: RuntimeString,
    pub mtime_ms: i64,
}

impl RuntimeEncode for MemoryRecord {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        object.field("type", &self.memory_type)?;
        object.field("body", &self.body)?;
        object.field("path", &self.path)?;
        object.field("mtimeMs", &self.mtime_ms)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemoryRecord {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                memory_type: object.required("type", &[])?,
                body: object.required("body", &[])?,
                path: object.required("path", &[])?,
                mtime_ms: object.required("mtimeMs", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_record_codec_preserves_order_and_non_scalar_utf16() {
        let lone_high = RuntimeString::from_utf16([0xd800]);
        let lone_low = RuntimeString::from_utf16([0xdc00]);
        let record = MemoryRecord {
            name: lone_high.clone(),
            description: RuntimeString::from("description"),
            memory_type: MemoryType::Reference,
            body: lone_low.clone(),
            path: RuntimeString::from("/memory/item.md"),
            mtime_ms: 42,
        };

        let encoded = runtime_codec::encode_text(&record).expect("encode exact memory record");
        assert_eq!(
            encoded.as_utf16(),
            RuntimeString::from_utf16(
                r#"{"name":"\ud800","description":"description","type":"reference","body":"\udc00","path":"/memory/item.md","mtimeMs":42}"#
                    .encode_utf16()
                    .collect::<Vec<_>>(),
            )
            .as_utf16(),
        );
        let decoded: MemoryRecord =
            runtime_codec::decode_text(&encoded).expect("decode exact memory record");
        assert_eq!(decoded, record);
        assert_eq!(decoded.name, lone_high);
        assert_eq!(decoded.body, lone_low);
    }
}
