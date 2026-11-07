//! JSON value type — what crosses the rollout/wire boundary.

use serde_json::{Map, Value};

use crate::runtime_values::{runtime_codec, RuntimeJson, ScalarJson};

/// JSON value type — what crosses the rollout/wire boundary.
///
/// Temporary Task 30 compatibility name for the exact runtime JSON carrier.
///
/// This alias deliberately has no Serde implementation: runtime ingress and
/// egress must use the exact codec rather than an implicit serde round trip.
pub type Json = RuntimeJson;

/// A JSON object (`{ [key: string]: Json }`).
pub type JsonObject = Map<String, Value>;

/// A JSON-Schema-shaped object (draft 2020-12 subset). We keep this structural
/// rather than coupling to a specific validator's return type, so providers and
/// MCP tools can both supply schemas. Normalization per provider lives in
/// `tools/tool_schema.rs`.
///
/// This shape is represented as a plain JSON object with an optional `type`
/// field, read via [`schema_type`].
pub type JsonSchema = Map<String, Value>;

/// Lift one engine-owned, statically validated scalar schema into the exact
/// runtime carrier used at every model/provider/tool boundary. This is an
/// explicit scalar-only seam: dynamic or externally supplied schemas must
/// already be [`RuntimeJson`] and never pass through this function.
pub fn runtime_schema_from_static(schema: &JsonSchema) -> RuntimeJson {
    let schema = ScalarJson::validate_static_schema(Value::Object(schema.clone()))
        .expect("native tool schemas contain only scalar JSON");
    runtime_codec::encode_value(&schema)
        .expect("validated native tool schema has an exact runtime encoding")
}

/// Read the optional `type` discriminator of a [`JsonSchema`] — the optional
/// `type` field of the schema object, if present.
pub fn schema_type(schema: &JsonSchema) -> Option<&str> {
    schema.get("type").and_then(Value::as_str)
}
