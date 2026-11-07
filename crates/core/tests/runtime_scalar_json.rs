use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, try_to_scalar_json, ConfigJson, ProvenanceJson, RuntimeJson,
    RuntimeNumber, RuntimeObject, RuntimeString, ScalarJson, ScalarJsonErrorKind,
};
use klaus_agent_core::schemas::config::{McpCatalogConfig, MemoryConfig, SkillCatalogConfig};
use klaus_agent_core::types::permission::SandboxMode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::fmt::Debug;

fn assert_config_scalar_wire<T>(value: &T, expected: &str)
where
    T: Debug + DeserializeOwned + PartialEq + Serialize,
{
    let serde_value = serde_json::to_value(value).unwrap();
    let config = ConfigJson::validate_config_document(serde_value).unwrap();
    assert_eq!(
        runtime_codec::encode_bytes(&config).unwrap(),
        expected.as_bytes()
    );
    assert_eq!(
        runtime_codec::encode_value(&config).unwrap(),
        runtime_json::parse(&RuntimeString::from(expected)).unwrap()
    );
    assert_eq!(serde_json::from_str::<T>(expected).unwrap(), *value);
}

fn bool_wire(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

#[test]
fn named_scalar_boundaries_convert_to_runtime_without_a_general_from_escape_hatch() {
    let schema = ScalarJson::validate_static_schema(json!({
        "type": "object",
        "wide": 9_007_199_254_740_993_u64,
    }))
    .unwrap();
    let config = ConfigJson::validate_config_document(json!({"enabled": true})).unwrap();
    let provenance =
        ProvenanceJson::validate_provenance_document(json!({"sha256": "abc"})).unwrap();

    let encoded = runtime_codec::encode_value(&schema).unwrap();
    assert_eq!(
        encoded,
        RuntimeJson::Object({
            let mut object = RuntimeObject::new();
            object.insert("type", RuntimeJson::from("object"));
            object.insert("wide", RuntimeJson::from(9_007_199_254_740_992_f64));
            object
        })
    );
    assert_eq!(
        runtime_codec::encode_text(&config).unwrap(),
        RuntimeString::from(r#"{"enabled":true}"#)
    );
    assert_eq!(
        runtime_codec::encode_text(&provenance).unwrap(),
        RuntimeString::from(r#"{"sha256":"abc"}"#)
    );
}

#[test]
fn canonical_sandbox_mode_round_trips_every_tag() {
    for (value, expected) in [
        (SandboxMode::ReadOnly, r#""read-only""#),
        (SandboxMode::WorkspaceWrite, r#""workspace-write""#),
        (SandboxMode::DangerFullAccess, r#""danger-full-access""#),
    ] {
        assert_config_scalar_wire(&value, expected);
    }
}

#[test]
fn scalar_only_memory_config_round_trips_every_bool_combination() {
    for mask in 0_u8..32 {
        let value = MemoryConfig {
            enabled: mask & 1 != 0,
            user: mask & 2 != 0,
            project: mask & 4 != 0,
            local: mask & 8 != 0,
            allow_external_includes: mask & 16 != 0,
        };
        let expected = format!(
            "{{\"enabled\":{},\"user\":{},\"project\":{},\"local\":{},\"allowExternalIncludes\":{}}}",
            bool_wire(value.enabled),
            bool_wire(value.user),
            bool_wire(value.project),
            bool_wire(value.local),
            bool_wire(value.allow_external_includes),
        );
        assert_config_scalar_wire(&value, &expected);
    }
}

#[test]
fn scalar_only_skill_catalog_config_round_trips_every_bool_combination() {
    for mask in 0_u8..4 {
        let value = SkillCatalogConfig {
            enabled: mask & 1 != 0,
            non_suspicious_only: mask & 2 != 0,
        };
        let expected = format!(
            "{{\"enabled\":{},\"nonSuspiciousOnly\":{}}}",
            bool_wire(value.enabled),
            bool_wire(value.non_suspicious_only),
        );
        assert_config_scalar_wire(&value, &expected);
    }
}

#[test]
fn scalar_only_mcp_catalog_config_round_trips_every_bool_combination() {
    for enabled in [false, true] {
        let value = McpCatalogConfig { enabled };
        let expected = format!("{{\"enabled\":{}}}", bool_wire(value.enabled));
        assert_config_scalar_wire(&value, &expected);
    }
}

#[test]
fn scalar_only_config_conversion_rejects_an_exact_lone_surrogate() {
    let mut object = RuntimeObject::new();
    object.insert(
        "memory",
        RuntimeJson::String(RuntimeString::from_utf16(vec![0xd800])),
    );
    let error = try_to_scalar_json(&RuntimeJson::Object(object)).unwrap_err();
    assert_eq!(error.kind(), ScalarJsonErrorKind::NonScalarString);
    assert_eq!(error.path(), &[RuntimeString::from("memory")]);
}

#[test]
fn runtime_to_scalar_json_rejects_identity_changing_values() {
    assert!(
        try_to_scalar_json(&RuntimeJson::String(RuntimeString::from_utf16(vec![
            0xd800
        ])))
        .is_err()
    );
    assert!(
        try_to_scalar_json(&RuntimeJson::Number(RuntimeNumber::from_f64(f64::INFINITY))).is_err()
    );

    let negative_zero = RuntimeJson::Number(RuntimeNumber::from_f64(-0.0));
    let scalar = try_to_scalar_json(&negative_zero).unwrap();
    let round_trip = runtime_codec::encode_value(&scalar).unwrap();
    assert_eq!(round_trip, negative_zero);
}

#[test]
fn scalar_conversion_preserves_a_non_scalar_object_key_in_the_error_path() {
    let key = RuntimeString::from_utf16(vec![0xd800]);
    let mut object = RuntimeObject::new();
    object.insert(key.clone(), RuntimeJson::Null);
    let error = try_to_scalar_json(&RuntimeJson::Object(object)).unwrap_err();
    assert_eq!(error.kind(), ScalarJsonErrorKind::NonScalarString);
    assert_eq!(error.path(), &[key]);
}
