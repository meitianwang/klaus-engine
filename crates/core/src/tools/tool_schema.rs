//! The validation-schema layer: turn a schema definition into the provider-ready
//! JSON Schema the model sees.
//!
//! A tool declares its input as a JSON Schema value; this module normalizes that
//! value into the exact shape every provider accepts.

use crate::runtime_values::{RuntimeJson, RuntimeObject};
use crate::types::json::Json;

/// Produce a provider-ready JSON Schema from a schema definition. This runs on
/// the hot path (every tool, every turn), so it stays cheap: strip the keywords
/// providers reject and guarantee an object root.
pub fn to_tool_json_schema(schema: &Json) -> RuntimeObject {
    let raw = schema.as_object().cloned().unwrap_or_default();
    normalize_for_provider(raw)
}

/// Strip keywords providers reject and ensure an object root. Applied to both
/// engine-declared and MCP-supplied schemas.
pub fn normalize_for_provider(mut schema: RuntimeObject) -> RuntimeObject {
    // Drop the dialect marker — providers reject `$schema`.
    schema.remove("$schema");
    // Guarantee a top-level object schema when the definition omits `type`.
    if !schema.contains_key("type") {
        schema.insert("type", RuntimeJson::String("object".into()));
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            crate::runtime_values::runtime_json::parse(
                &crate::runtime_values::RuntimeString::from(stringify!($($json)+)),
            )
            .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    #[test]
    fn strips_the_dialect_marker_and_keeps_the_rest() {
        let raw = exact_json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": { "path": { "type": "string" } },
            "required": ["path"],
            "additionalProperties": false
        });
        let out = to_tool_json_schema(&raw);
        assert_eq!(
            RuntimeJson::Object(out),
            exact_json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn defaults_type_to_object_when_absent() {
        let raw = exact_json!({ "properties": {} });
        let out = to_tool_json_schema(&raw);
        assert_eq!(out.get("type"), Some(&RuntimeJson::from("object")));
        // The rest of the schema is preserved verbatim.
        assert_eq!(
            RuntimeJson::Object(out),
            exact_json!({ "properties": {}, "type": "object" })
        );
    }

    #[test]
    fn keeps_an_explicit_non_object_type() {
        let raw = exact_json!({ "type": "string", "minLength": 1 });
        let out = normalize_for_provider(raw.as_object().unwrap().clone());
        assert_eq!(out.get("type"), Some(&RuntimeJson::from("string")));
        assert_eq!(out.get("minLength"), Some(&RuntimeJson::from(1_u64)));
    }

    #[test]
    fn a_null_type_is_left_untouched() {
        // A present `type` key (even a null value) is not the absent case, so the
        // object-root default does not fire.
        let raw = exact_json!({ "type": null });
        let out = normalize_for_provider(raw.as_object().unwrap().clone());
        assert_eq!(out.get("type"), Some(&RuntimeJson::Null));
    }

    #[test]
    fn a_non_object_value_normalizes_to_an_empty_object_schema() {
        let out = to_tool_json_schema(&RuntimeJson::Bool(true));
        assert_eq!(RuntimeJson::Object(out), exact_json!({ "type": "object" }));
    }

    #[test]
    fn normalization_preserves_non_scalar_property_names_and_values() {
        let raw = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"$schema":"drop-me","properties":{"\ud800":{"const":"\udc00"},"\ufffd":{}}}"#,
        ))
        .unwrap();
        let out = RuntimeJson::Object(to_tool_json_schema(&raw));

        assert_eq!(
            out,
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                r#"{"properties":{"\ud800":{"const":"\udc00"},"�":{}},"type":"object"}"#,
            ))
            .unwrap()
        );
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&out),
            RuntimeString::from(
                r#"{"properties":{"\ud800":{"const":"\udc00"},"�":{}},"type":"object"}"#,
            )
        );
    }
}
