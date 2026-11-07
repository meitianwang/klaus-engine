//! Recursive JSON value schema.
//!
//! Tool inputs and MCP results are validated / round-tripped through this on the
//! rollout + wire boundary. Inputs are small, so the recursion cost is
//! negligible.
//!
//! A schema library would validate this at runtime; in Rust the
//! equivalent guarantee is provided by the exact runtime parser. It preserves
//! JavaScript UTF-16 strings, number semantics, and object order without a
//! `serde_json::Value` round trip.

use crate::runtime_values::{runtime_json, RuntimeString};
use crate::types::json::Json;

/// Parse and validate a JSON value from raw text. Returns the recursive
/// [`Json`] value, or the parse error for the wire boundary to surface.
pub fn parse_json_value(input: &RuntimeString) -> Result<Json, runtime_json::RuntimeJsonError> {
    runtime_json::parse(input)
}

/// Validate an already-parsed value round-trips as JSON. This is always true for
/// an exact [`Json`]; kept as an explicit no-op boundary check at value-validation
/// call sites.
pub fn validate_json_value(value: Json) -> Json {
    value
}
