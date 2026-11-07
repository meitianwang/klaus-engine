//! Output-level wire-schema golden. This is the checked-in schema
//! baseline that replaces the whole-file source fingerprints: it projects the
//! live serde scan of every serialized type into a deterministic wire-schema
//! description (Rust field name -> wire field name for both directions, enum
//! variants and their tags/aliases, encode/decode codec category, adapter
//! bindings, tagging and skip/default policy) and compares it byte-for-byte
//! with the checked-in golden. A field rename, a type added/removed from the
//! wire, or a codec-category drift changes this projection and turns the test
//! red. Re-record with `UPDATE_GOLDEN=1 cargo test -p klaus-agent-core \
//! --test runtime_wire_schema_golden`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

#[path = "support/runtime_wire_ast.rs"]
mod runtime_wire_ast;

use runtime_wire_ast::{scan_repository, FieldContract, SourceContract, VariantContract};

const GOLDEN: &str = include_str!("fixtures/runtime/runtime-wire-schema-golden.json");

/// One serialized type projected down to its wire-observable schema.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireSchema {
    kind: String,
    shape: String,
    can_encode: bool,
    can_decode: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    rename_serialize: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rename_deserialize: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rename_all_serialize: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rename_all_deserialize: Option<String>,
    deny_unknown_fields: bool,
    transparent: bool,
    container_default: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    deserialize_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deserialize_try_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    serialize_into: Option<String>,
    tagging_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tagging_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tagging_content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<WireField>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    variants: Vec<WireVariant>,
}

/// A struct or variant field projected to its wire name mapping and policy.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireField {
    #[serde(skip_serializing_if = "Option::is_none")]
    rust_name: Option<String>,
    rust_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_serialize: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_deserialize: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    aliases: Vec<String>,
    default: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_function: Option<String>,
    skip_serialize: bool,
    skip_deserialize: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_serialize_if: Option<String>,
    flatten: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    serialize_with: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deserialize_with: Option<String>,
}

/// An enum variant projected to its wire tag mapping, aliases and payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireVariant {
    rust_name: String,
    shape: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_serialize: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_deserialize: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    aliases: Vec<String>,
    other: bool,
    rust_default: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<WireField>,
}

fn project_field(field: &FieldContract) -> WireField {
    let semantics = &field.serde_semantics;
    WireField {
        rust_name: field.rust_name.clone(),
        rust_type: field.rust_type.clone(),
        wire_serialize: semantics
            .canonical_name
            .as_ref()
            .map(|name| name.serialize.clone()),
        wire_deserialize: semantics
            .canonical_name
            .as_ref()
            .map(|name| name.deserialize.clone()),
        aliases: semantics.aliases.clone(),
        default: semantics.default.enabled,
        default_function: semantics.default.function.clone(),
        skip_serialize: semantics.skip.serialize,
        skip_deserialize: semantics.skip.deserialize,
        skip_serialize_if: semantics.skip.serialize_if.clone(),
        flatten: semantics.flatten,
        serialize_with: semantics.adapters.effective_serialize_with.clone(),
        deserialize_with: semantics.adapters.effective_deserialize_with.clone(),
    }
}

fn project_variant(variant: &VariantContract) -> WireVariant {
    let semantics = &variant.serde_semantics;
    WireVariant {
        rust_name: variant.rust_name.clone(),
        shape: variant.shape.clone(),
        wire_serialize: semantics
            .canonical_name
            .as_ref()
            .map(|name| name.serialize.clone()),
        wire_deserialize: semantics
            .canonical_name
            .as_ref()
            .map(|name| name.deserialize.clone()),
        aliases: semantics.aliases.clone(),
        other: semantics.other,
        rust_default: semantics.rust_default,
        fields: variant.fields.iter().map(project_field).collect(),
    }
}

fn project(contract: &SourceContract) -> WireSchema {
    let semantics = &contract.serde_semantics;
    WireSchema {
        kind: contract.kind.clone(),
        shape: contract.shape.clone(),
        can_encode: contract.can_encode(),
        can_decode: contract.can_decode(),
        rename_serialize: semantics.rename.serialize.clone(),
        rename_deserialize: semantics.rename.deserialize.clone(),
        rename_all_serialize: semantics.rename_all.serialize.clone(),
        rename_all_deserialize: semantics.rename_all.deserialize.clone(),
        deny_unknown_fields: semantics.deny_unknown_fields,
        transparent: semantics.transparent,
        container_default: semantics.default.enabled,
        deserialize_from: semantics.deserialize_from.clone(),
        deserialize_try_from: semantics.deserialize_try_from.clone(),
        serialize_into: semantics.serialize_into.clone(),
        tagging_kind: semantics.tagging.kind.clone(),
        tagging_tag: semantics.tagging.tag.clone(),
        tagging_content: semantics.tagging.content.clone(),
        fields: contract.fields.iter().map(project_field).collect(),
        variants: contract.variants.iter().map(project_variant).collect(),
    }
}

fn build_schema(root: &Path) -> BTreeMap<String, WireSchema> {
    scan_repository(root)
        .expect("the serde scanner must resolve the workspace")
        .iter()
        .map(|(symbol_key, contract)| (symbol_key.clone(), project(contract)))
        .collect()
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

#[test]
fn runtime_wire_schema_matches_the_output_level_golden() {
    let root = repository_root();
    let schema = build_schema(&root);

    // Robustness invariants that hold independently of the golden file.
    assert!(
        !schema.is_empty(),
        "the wire-schema projection must cover the serialized surface"
    );
    for (symbol_key, entry) in &schema {
        assert!(
            entry.kind == "struct" || entry.kind == "enum",
            "{symbol_key} projected an unexpected kind {}",
            entry.kind
        );
        assert!(
            entry.can_encode || entry.can_decode,
            "{symbol_key} is serialized in neither direction"
        );
    }

    let mut actual = serde_json::to_string_pretty(&schema).expect("schema serializes");
    actual.push('\n');

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/runtime/runtime-wire-schema-golden.json");
        std::fs::write(path, actual).expect("golden write succeeds");
        return;
    }

    assert_eq!(
        actual, GOLDEN,
        "wire schema drifted from the golden baseline; re-record with \
         UPDATE_GOLDEN=1 cargo test -p klaus-agent-core --test runtime_wire_schema_golden"
    );
}

// ---------------------------------------------------------------------------
// Autopsy test: prove the wall actually turns red on a wire rename.
// ---------------------------------------------------------------------------

#[test]
fn wire_field_rename_turns_the_schema_golden_red() {
    let root = repository_root();
    let mut schema = build_schema(&root);
    let baseline = serde_json::to_string_pretty(&schema).expect("schema serializes");

    // Rename one registered type's wire field exactly the way a source-level
    // #[serde(rename)] drift would surface through the scanner.
    let entry = schema
        .values_mut()
        .find(|entry| {
            entry
                .fields
                .iter()
                .any(|field| field.wire_serialize.is_some())
        })
        .expect("a serialized struct with wire field names");
    let field = entry
        .fields
        .iter_mut()
        .find(|field| field.wire_serialize.is_some())
        .expect("field presence was checked");
    let renamed = format!("{}Renamed", field.wire_serialize.clone().expect("present"));
    field.wire_serialize = Some(renamed);

    let mutated = serde_json::to_string_pretty(&schema).expect("mutated schema serializes");
    assert_ne!(
        mutated, baseline,
        "a wire field rename must change the schema projection"
    );
    assert_ne!(
        format!("{mutated}\n"),
        GOLDEN,
        "a wire field rename must no longer match the checked-in schema golden"
    );
}

#[test]
fn container_conversion_type_drift_changes_the_wire_projection() {
    let project_source = |source: &str| {
        let facts = runtime_wire_ast::scan_source("wire.rs", source).unwrap();
        assert_eq!(facts.len(), 1);
        let contract = facts.into_values().next().unwrap();
        serde_json::to_value(project(&contract)).unwrap()
    };
    let plain = project_source(
        "#[derive(serde::Serialize, serde::Deserialize)] struct Plain { value: String }",
    );
    for key in ["deserializeFrom", "deserializeTryFrom", "serializeInto"] {
        assert!(plain.get(key).is_none());
    }

    let source = r#"
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(from = "Input", into = "Output")]
        struct Converted { value: String }
    "#;
    let converted = project_source(source);
    assert_eq!(converted["deserializeFrom"], "Input");
    assert_eq!(converted["serializeInto"], "Output");
    assert!(converted.get("deserializeTryFrom").is_none());
    assert_ne!(
        project_source(&source.replace("Input", "OtherInput")),
        converted
    );
    assert_ne!(
        project_source(&source.replace("Output", "OtherOutput")),
        converted
    );

    let fallible_source = r#"
        #[derive(serde::Deserialize)]
        #[serde(try_from = "CheckedInput")]
        struct Checked { value: String }
    "#;
    let fallible = project_source(fallible_source);
    assert_eq!(fallible["deserializeTryFrom"], "CheckedInput");
    assert!(fallible.get("deserializeFrom").is_none());
    assert!(fallible.get("serializeInto").is_none());
    assert_ne!(
        project_source(&fallible_source.replace("CheckedInput", "OtherCheckedInput")),
        fallible
    );
}
