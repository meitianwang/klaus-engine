//! Exact JavaScript runtime-value foundations.
//!
//! These types preserve JavaScript UTF-16 string identity, binary64 number
//! identity, and ECMAScript object property order without passing through
//! `String` or `serde_json::Value`. They are the authoritative carriers for
//! runtime-derived values across the workspace.
//!
//! # Compile-fail API boundary guards
//!
//! `RuntimeString` deliberately has no `Deref<Target = str>` escape hatch:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_str_deref<T: std::ops::Deref<Target = str>>(_: &T) {}
//!
//! requires_str_deref(&RuntimeString::from_utf8("exact"));
//! ```
//!
//! It also cannot be borrowed through `AsRef<str>`:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_str_as_ref<T: AsRef<str>>(_: &T) {}
//!
//! requires_str_as_ref(&RuntimeString::from_utf8("exact"));
//! ```
//!
//! No implicit owned-scalar conversion is available:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_string_conversion<T: Into<String>>(_: T) {}
//!
//! requires_string_conversion(RuntimeString::from_utf8("exact"));
//! ```
//!
//! Formatting cannot silently project an exact runtime string to a scalar
//! string:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_display<T: std::fmt::Display>(_: &T) {}
//!
//! requires_display(&RuntimeString::from_utf8("exact"));
//! ```
//!
//! Serde is not an exact-runtime serialization path:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeString;
//!
//! fn requires_serde<T: serde::Serialize>(_: &T) {}
//!
//! requires_serde(&RuntimeString::from_utf8("exact"));
//! ```
//!
//! The same boundary prevents an exact `RuntimeJson` tree from entering
//! `serde_json`:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimeJson;
//!
//! let _ = serde_json::to_value(RuntimeJson::Null).unwrap();
//! ```
//!
//! Scalar-key maps never become exact runtime maps implicitly:
//!
//! ```compile_fail,E0277
//! use std::collections::BTreeMap;
//! use klaus_agent_core::runtime_values::runtime_codec;
//!
//! let values = BTreeMap::<String, u64>::new();
//! let _ = runtime_codec::encode_value(&values);
//! ```
//!
//! Object decoders cannot be constructed directly because unknown-field
//! finalization must not be optional:
//!
//! ```compile_fail,E0624
//! use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObjectDecoder};
//!
//! let _ = RuntimeObjectDecoder::new(&RuntimeJson::Null, true);
//! ```
//!
//! A runtime path cannot become a Rust path implicitly:
//!
//! ```compile_fail,E0277
//! use std::path::PathBuf;
//! use klaus_agent_core::runtime_values::RuntimePath;
//!
//! fn requires_path<T: Into<PathBuf>>(_: T) {}
//!
//! requires_path(RuntimePath::from("logical/path"));
//! ```
//!
//! It cannot be borrowed through `AsRef<Path>` either:
//!
//! ```compile_fail,E0277
//! use std::path::Path;
//! use klaus_agent_core::runtime_values::RuntimePath;
//!
//! fn requires_path<T: AsRef<Path>>(_: &T) {}
//!
//! requires_path(&RuntimePath::from("logical/path"));
//! ```
//!
//! Formatting cannot silently project a runtime path:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_values::RuntimePath;
//!
//! fn requires_display<T: std::fmt::Display>(_: &T) {}
//!
//! requires_display(&RuntimePath::from("logical/path"));
//! ```
//!
//! Nor can it construct the private native-host process identity:
//!
//! ```compile_fail,E0277
//! use klaus_agent_core::runtime_process::NativeOsString;
//! use klaus_agent_core::runtime_values::RuntimePath;
//!
//! fn requires_native<T: Into<NativeOsString>>(_: T) {}
//!
//! requires_native(RuntimePath::from("logical/path"));
//! ```

mod codec;
mod decoder;
mod json;
mod number;
mod object;
mod parser;
mod path;
mod scalar;
mod string;
mod writer;

pub use codec::{
    RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode, RuntimeEncoder,
    RuntimeMap, RuntimeObjectDecoder, RuntimeObjectEncoder,
};
pub use decoder::{
    CanonicalRuntimeJsonError, CanonicalRuntimeJsonUtf8, NodeBufferUtf8, NodeChunkUtf8,
    NodeStringDecoderUtf8, StrictScalarUtf8, Utf8BomPolicy, Utf8DecodeError, Utf8DecodeErrorKind,
    WhatwgUtf8,
};
pub use json::RuntimeJson;
pub use number::RuntimeNumber;
pub use object::{RuntimeObject, RuntimeObjectEntries};
pub use path::{Node20FsPathIngress, RuntimePath};
pub use scalar::{
    try_to_scalar_json, ConfigJson, ProvenanceJson, ScalarJson, ScalarJsonError,
    ScalarJsonErrorKind,
};
pub use string::{BomPolicy, NonScalarStringError, RuntimeCodePoints, RuntimeString};

/// Exact JSON parser and writer for runtime values.
pub mod runtime_json {
    pub use super::parser::{parse, RuntimeJsonError, RuntimeJsonErrorKind};
    pub use super::writer::{stringify, stringify_pretty, write_to};
}

/// Typed exact-runtime DTO codec entry points.
pub mod runtime_codec {
    pub use super::codec::{
        decode_adjacent, decode_external, decode_internal, decode_object, decode_text,
        decode_untagged, decode_value, encode_adjacent_payload, encode_adjacent_unit, encode_bytes,
        encode_external_payload, encode_external_unit, encode_internal, encode_text, encode_value,
        RuntimeEnumVariant, RuntimeExternalVariant, RuntimeUntaggedVariantDecoder,
    };
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::{BomPolicy, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString};

    fn hash_of(value: &RuntimeObject) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn runtime_string_order_is_unsigned_utf16_lexicographic() {
        let astral = RuntimeString::from_utf16(vec![0xd800, 0xdc00]);
        let bmp = RuntimeString::from_utf16(vec![0xe000]);
        assert!(
            astral < bmp,
            "code-unit order differs from code-point order"
        );
        assert!(RuntimeString::from_utf16(vec![0xd800]) < RuntimeString::from_utf16(vec![0xdc00]));
    }

    #[test]
    fn runtime_string_utf16le_bom_policy_is_exact() {
        let value = RuntimeString::from_utf16(vec![0x0041, 0xd800]);
        assert_eq!(value.to_utf16le(BomPolicy::Omit), [0x41, 0x00, 0x00, 0xd8]);
        assert_eq!(
            value.to_utf16le(BomPolicy::Emit),
            [0xff, 0xfe, 0x41, 0x00, 0x00, 0xd8]
        );
    }

    #[test]
    fn runtime_string_debug_visibly_escapes_non_scalar_and_control_units() {
        let value = RuntimeString::from_utf16(vec![0xd800, b'\n'.into(), 0xdc00]);
        assert_eq!(format!("{value:?}"), r#"RuntimeString("\ud800\n\udc00")"#);
    }

    #[test]
    fn runtime_object_structural_order_and_unordered_semantics_are_distinct() {
        let mut left = RuntimeObject::new();
        left.insert("a", RuntimeJson::from(1));
        left.insert("b", RuntimeJson::from(2));
        let mut right = RuntimeObject::new();
        right.insert("b", RuntimeJson::from(2));
        right.insert("a", RuntimeJson::from(1));

        assert_ne!(left, right);
        assert_ne!(hash_of(&left), hash_of(&right));
        assert!(left.semantic_eq_unordered(&right));
    }

    #[test]
    fn runtime_number_numeric_comparison_is_not_total() {
        let nan = RuntimeNumber::from_f64(f64::NAN);
        assert_eq!(nan.numeric_cmp(RuntimeNumber::from_f64(1.0)), None);
        assert_eq!(
            RuntimeNumber::from_f64(-0.0).numeric_cmp(RuntimeNumber::from_f64(0.0)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            RuntimeNumber::from_f64(1.0).numeric_cmp(RuntimeNumber::from_f64(2.0)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn runtime_json_consuming_accessors_preserve_owned_payloads_with_iterative_drop() {
        let string = RuntimeString::from_utf16(vec![0xd800]);
        assert_eq!(
            RuntimeJson::String(string.clone()).into_string().unwrap(),
            string
        );

        let array = vec![RuntimeJson::from(1), RuntimeJson::from(2)];
        assert_eq!(
            RuntimeJson::Array(array.clone()).into_array().unwrap(),
            array
        );

        let mut object = RuntimeObject::new();
        object.insert("a", RuntimeJson::from(1));
        assert_eq!(
            RuntimeJson::Object(object.clone()).into_object().unwrap(),
            object
        );
    }
}
