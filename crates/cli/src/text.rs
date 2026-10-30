//! Crossing between the engine's exact runtime values and ordinary Rust strings.
//!
//! The engine models every wire string as [`RuntimeString`] (UTF-16 code units,
//! unpaired surrogates preserved) because that is what a JavaScript host can
//! round-trip. A terminal cannot: it takes bytes. So every value that reaches
//! the screen passes through [`text`], which applies the same lossy projection
//! Node applies when it writes a string to a pipe. Nothing here goes the other
//! way lossily — `&str` into `RuntimeString` is exact.

use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeDecode, RuntimeEncode, RuntimeJson, RuntimeNumber,
    RuntimeObject, RuntimeString,
};
use klaus_agent_protocol::types::{self as protocol_types, Input};

use crate::error::{CliError, CliResult};
use klaus_agent_server::app_server::runtime_wire::RuntimeValue;

/// A wire string as printable text, using Node's UTF-8 projection (an unpaired
/// surrogate becomes one replacement character).
pub fn text(value: &RuntimeString) -> String {
    String::from_utf8_lossy(&value.to_node_utf8()).into_owned()
}

/// The same projection for an optional field, with a caller-chosen stand-in.
pub fn text_or(value: Option<&RuntimeString>, fallback: &str) -> String {
    value.map(text).unwrap_or_else(|| fallback.to_string())
}

/// A JSON value as one compact line.
pub fn json_line(value: &RuntimeJson) -> String {
    text(&runtime_json::stringify(value))
}

/// A JSON value indented for reading.
pub fn json_pretty(value: &RuntimeJson) -> String {
    text(&runtime_json::stringify_pretty(value))
}

/// Encode a wire DTO to its exact JSON form.
pub fn encode<T: RuntimeEncode + ?Sized>(value: &T) -> CliResult<RuntimeJson> {
    runtime_codec::encode_value(value).map_err(CliError::from)
}

/// Encode runtime content for a typed named-facade field without projecting it.
pub fn to_json<T: RuntimeEncode + ?Sized>(value: &T) -> CliResult<RuntimeValue> {
    Ok(RuntimeValue(encode(value)?))
}

/// The protocol owns the string-or-parts shape of ordinary input.
pub fn input(input: &Input) -> CliResult<RuntimeValue> {
    Ok(RuntimeValue(
        protocol_types::encode_input(input).map_err(CliError::from)?,
    ))
}

/// Validate an endpoint-owned scalar configuration document. Runtime content
/// uses `to_json`; it must never cross this separate configuration validator.
pub fn scalar_json<T: RuntimeEncode + ?Sized>(value: &T) -> CliResult<serde_json::Value> {
    let exact = encode(value)?;
    let scalar = klaus_agent_core::runtime_values::try_to_scalar_json(&exact)
        .map_err(|error| CliError::Codec(error.to_string()))?;
    let raw = runtime_codec::encode_bytes(&scalar).map_err(CliError::from)?;
    serde_json::from_slice(&raw).map_err(|error| CliError::Codec(error.to_string()))
}

/// A wire enum's tag, for a facade param that carries the choice as a bare
/// string rather than as JSON.
///
/// Goes through the codec for the same reason [`to_json`] does — how each
/// variant is spelled is the engine's to decide — and refuses anything that does
/// not encode to a string, because a param declared as one cannot carry it.
pub fn tag<T: RuntimeEncode + ?Sized>(value: &T) -> CliResult<String> {
    // Matched by reference: `RuntimeJson` implements `Drop`, so the tag cannot be
    // moved out of the variant.
    match &encode(value)? {
        RuntimeJson::String(tag) => tag
            .try_to_utf8()
            .map_err(|error| CliError::Codec(error.to_string())),
        other => Err(CliError::Codec(format!(
            "expected a wire tag, not {}",
            json_line(other)
        ))),
    }
}

/// Read a declared facade result as a runtime value for protocol decoding or
/// human formatting. Machine output serializes the declared result directly so
/// scalar control numbers retain their original integer representation.
pub fn from_wire<T: serde::Serialize>(value: &T) -> CliResult<RuntimeJson> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| CliError::Codec(format!("cannot encode a declared result: {error}")))?;
    runtime_json::parse(&RuntimeString::from(encoded))
        .map_err(|error| CliError::Codec(format!("cannot re-read a declared result: {error}")))
}

/// Read a typed facade value through its exact serialized form. Scoped facade
/// content adapters retain runtime strings; scalar control DTOs keep serde's
/// original integer and field validation before they reach this boundary.
pub fn decode<T: RuntimeDecode>(value: &impl serde::Serialize) -> CliResult<T> {
    let raw = serde_json::to_string(value).map_err(|error| CliError::Codec(error.to_string()))?;
    runtime_codec::decode_text(&RuntimeString::from(raw)).map_err(CliError::from)
}

/// Parse a JSON argument typed on the command line.
pub fn parse_json(raw: &str) -> CliResult<RuntimeJson> {
    runtime_json::parse(&RuntimeString::from(raw))
        .map_err(|error| CliError::usage(format!("invalid JSON argument: {error}")))
}

/// Build a JSON object from already-encoded fields, in the order given. Used to
/// wrap a CLI-composed result (a discovery listing, a status line) so `--json`
/// still emits one object per invocation instead of a bare array.
pub fn object(fields: Vec<(&str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(RuntimeString::from(key), value);
    }
    RuntimeJson::Object(object)
}

/// A JSON string value.
pub fn string(value: &str) -> RuntimeJson {
    RuntimeJson::String(RuntimeString::from(value))
}

/// A JSON boolean value.
pub fn boolean(value: bool) -> RuntimeJson {
    RuntimeJson::Bool(value)
}

/// A JSON array value.
pub fn array(items: Vec<RuntimeJson>) -> RuntimeJson {
    RuntimeJson::Array(items)
}

/// A JSON number value. Counts and byte sizes only, so an `f64` round-trip is
/// exact for anything the CLI reports.
pub fn number(value: u64) -> RuntimeJson {
    RuntimeJson::Number(RuntimeNumber::from_f64(value as f64))
}

/// Read one field out of a decoded JSON object, if it is an object at all.
pub fn field<'a>(value: &'a RuntimeJson, key: &str) -> Option<&'a RuntimeJson> {
    match value {
        RuntimeJson::Object(object) => object.get(RuntimeString::from(key)),
        _ => None,
    }
}
