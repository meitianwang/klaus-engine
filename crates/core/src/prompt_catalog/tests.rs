//! Contract tests for the model prompt catalog.
//!
//! The scenarios mirror the catalog design's acceptance matrix: shared model
//! packs across routes, single-leaf updates that must not disturb sibling
//! leaves, opaque routes falling back to the generic pack, the
//! missing/cleared/disabled/invalid distinctions, whole-document rejection of
//! corrupt revisions, and leaf-wise merging of source declarations.

use crate::prompt_catalog::resolve::{resolve, FragmentLayer};
use crate::prompt_catalog::{
    catalog::CatalogError, ModelFragments, PromptCatalog, PromptCatalogKey,
};
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeString};

use crate::prompt_catalog::slots::{
    SLOT_AGENT_CHILD, SLOT_AGENT_MODE_EXPLICIT, SLOT_AGENT_MODE_HINT, SLOT_AGENT_MODE_PROACTIVE,
    SLOT_AGENT_ROOT, SLOT_BASE_INSTRUCTIONS, SLOT_RESPONSE_COMMENTARY, SLOT_REVIEW_GUARDIAN_V2,
    SLOT_REVIEW_POLICY, SLOT_REVIEW_REJECTION_INSTRUCTIONS,
};

fn parse(text: &str) -> PromptCatalog {
    // Fixture catalogs stand in for verified published packages: trusted, so
    // they may exercise high-authority slots. The untrusted path (distributed
    // revisions) has its own tests.
    PromptCatalog::parse(&RuntimeString::from(text))
        .expect("fixture catalog must be valid")
        .with_trust(true)
}

fn parse_err(text: &str) -> CatalogError {
    PromptCatalog::parse(&RuntimeString::from(text)).expect_err("catalog must be rejected")
}

fn declared(text: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(text)).unwrap()
}

fn key(
    source_kind: &str,
    source_scope: &str,
    model_key: &str,
    variant_key: &str,
) -> PromptCatalogKey {
    PromptCatalogKey::new(source_kind, source_scope, model_key, variant_key)
}

fn text_of(fragments: &crate::prompt_catalog::ResolvedFragments, slot: &str) -> Option<String> {
    fragments
        .text(slot)
        .and_then(|text| text.try_to_utf8().ok())
}

/// The spec's synthetic catalog: two models share a base instruction but pick
/// different delegation-role texts; one model is reachable through two routes
/// and one route disables the progress channel; an opaque auto tier stays on
/// the generic pack.
const FIXTURE: &str = r#"{
  "schemaVersion": 2,
  "revision": "catalog-r13",
  "fragments": {
    "base-v3": "Complete the task using the tools available in this request.",
    "progress-default": "Use the progress channel for updates during longer tasks.",
    "root-default": "Coordinate only the workers available in this session.",
    "root-a": "Give each worker a bounded task and integrate its findings.",
    "root-b": "Check outstanding assignments before delegating another task."
  },
  "profiles": {
    "klaus/default": {
      "modelMessages": {
        "base.instructions": { "ref": "base-v3" },
        "agent.root": { "ref": "root-default" },
        "response.commentary": { "ref": "progress-default" }
      }
    },
    "fixture/model-a": {
      "extends": "klaus/default",
      "modelMessages": { "agent.root": { "ref": "root-a" } }
    },
    "fixture/model-b": {
      "extends": "klaus/default",
      "modelMessages": { "agent.root": { "ref": "root-b" } }
    }
  },
  "bindings": [
    {
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-a",
      "variantKey": "standard", "profileRef": "fixture/model-a"
    },
    {
      "sourceKind": "cursor", "sourceScope": "fixture-cursor", "modelKey": "mapped-a",
      "variantKey": "standard", "profileRef": "fixture/model-a",
      "overrides": { "response.commentary": { "disabled": true } }
    },
    {
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-b",
      "variantKey": "standard", "profileRef": "fixture/model-b"
    },
    {
      "sourceKind": "workbuddy", "sourceScope": "fixture-auto", "modelKey": "balanced-model",
      "variantKey": "auto", "profileRef": "klaus/default"
    }
  ]
}"#;

#[test]
fn bundled_offline_catalog_is_valid_and_resolves_to_engine_defaults() {
    let catalog = crate::prompt_catalog::bundled();
    assert_eq!(
        catalog.schema_version,
        crate::prompt_catalog::catalog::SCHEMA_VERSION
    );
    assert!(!catalog.revision.is_empty());

    let model = RuntimeString::from("gpt-4o");
    let fragments = ModelFragments::for_model(&model, None);
    // Required slots resolve from the engine defaults (gpt-4o carries no model pack).
    assert_eq!(
        fragments.base_instructions(),
        RuntimeString::from(crate::context::system_prompt::BASE_INSTRUCTIONS)
    );
    assert_eq!(
        fragments.collaboration_template(false),
        RuntimeString::from(include_str!("../planning/prompts/default.md"))
    );
    assert_eq!(
        fragments.collaboration_template(true),
        RuntimeString::from(include_str!("../planning/prompts/plan.md"))
    );
    // Family-keyed commentary defaults survive the migration.
    let claude = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-4-5"), None);
    assert_eq!(
        claude.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_UNTAGGED_BRIEF_UPDATES
        ))
    );
    let gemini = ModelFragments::for_model(&RuntimeString::from("gemini-2.5-pro"), None);
    assert_eq!(
        gemini.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_UNTAGGED_THINKING_CHANNEL
        ))
    );
    assert_eq!(
        fragments.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_PHASE_TAGGED
        ))
    );
    // Delegation defaults.
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from(crate::context::multi_agent::ROOT_ROLE))
    );
    assert_eq!(
        fragments.agent_role(true),
        Some(&RuntimeString::from(
            crate::context::multi_agent::CHILD_ROLE
        ))
    );
    assert_eq!(
        fragments.agent_mode(true, None),
        Some(RuntimeString::from(
            crate::context::multi_agent::PROACTIVE_MODE
        ))
    );
    assert_eq!(
        fragments.agent_mode(false, None),
        Some(RuntimeString::from(
            crate::context::multi_agent::EXPLICIT_MODE
        ))
    );
    // Review slots have no engine default: the policy module owns the bundled
    // fallbacks.
    assert!(fragments.review_text(SLOT_REVIEW_POLICY).is_none());
    assert!(fragments.guardian_v2().is_none());
}

#[test]
fn two_models_share_base_and_differ_only_in_their_own_leaf() {
    let catalog = parse(FIXTURE);
    let a = resolve(
        &catalog,
        &key("custom", "fixture-api", "model-a", "standard"),
        None,
        None,
    );
    let b = resolve(
        &catalog,
        &key("custom", "fixture-api", "model-b", "standard"),
        None,
        None,
    );
    // Shared base content.
    assert_eq!(
        text_of(&a, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("Complete the task using the tools available in this request.")
    );
    assert_eq!(
        text_of(&a, SLOT_BASE_INSTRUCTIONS),
        text_of(&b, SLOT_BASE_INSTRUCTIONS)
    );
    // Per-model role leaves differ.
    assert_eq!(
        text_of(&a, SLOT_AGENT_ROOT).as_deref(),
        Some("Give each worker a bounded task and integrate its findings.")
    );
    assert_eq!(
        text_of(&b, SLOT_AGENT_ROOT).as_deref(),
        Some("Check outstanding assignments before delegating another task.")
    );
    assert_eq!(a.catalog_revision, RuntimeString::from("catalog-r13"));
    assert_eq!(a.profile_ref, Some(RuntimeString::from("fixture/model-a")));
}

#[test]
fn single_leaf_update_leaves_sibling_leaves_and_other_models_intact() {
    // Revision N.
    let before = parse(FIXTURE);
    let a_before = resolve(
        &before,
        &key("custom", "fixture-api", "model-a", "standard"),
        None,
        None,
    );
    let b_before = resolve(
        &before,
        &key("custom", "fixture-api", "model-b", "standard"),
        None,
        None,
    );
    // Revision N+1 only rewrites model-a's role fragment.
    let after = parse(&FIXTURE.replace(
        "\"root-a\": \"Give each worker a bounded task and integrate its findings.\"",
        "\"root-a\": \"Hand each worker one bounded task.\"",
    ));
    let a_after = resolve(
        &after,
        &key("custom", "fixture-api", "model-a", "standard"),
        None,
        None,
    );
    let b_after = resolve(
        &after,
        &key("custom", "fixture-api", "model-b", "standard"),
        None,
        None,
    );
    assert_eq!(
        text_of(&a_after, SLOT_AGENT_ROOT).as_deref(),
        Some("Hand each worker one bounded task.")
    );
    // Every other leaf of A is unchanged.
    assert_eq!(
        text_of(&a_before, SLOT_BASE_INSTRUCTIONS),
        text_of(&a_after, SLOT_BASE_INSTRUCTIONS)
    );
    assert_eq!(
        text_of(&a_before, SLOT_RESPONSE_COMMENTARY),
        text_of(&a_after, SLOT_RESPONSE_COMMENTARY)
    );
    // Model B is untouched.
    assert_eq!(
        text_of(&b_before, SLOT_AGENT_ROOT),
        text_of(&b_after, SLOT_AGENT_ROOT)
    );
    assert_eq!(
        text_of(&b_before, SLOT_BASE_INSTRUCTIONS),
        text_of(&b_after, SLOT_BASE_INSTRUCTIONS)
    );
}

#[test]
fn same_model_across_routes_shares_the_pack_and_keeps_route_differences() {
    let catalog = parse(FIXTURE);
    let direct = resolve(
        &catalog,
        &key("custom", "fixture-api", "model-a", "standard"),
        None,
        None,
    );
    let aggregated = resolve(
        &catalog,
        &key("cursor", "fixture-cursor", "mapped-a", "standard"),
        None,
        None,
    );
    // Shared pack content is identical on both routes.
    assert_eq!(
        text_of(&direct, SLOT_BASE_INSTRUCTIONS),
        text_of(&aggregated, SLOT_BASE_INSTRUCTIONS)
    );
    assert_eq!(
        text_of(&direct, SLOT_AGENT_ROOT),
        text_of(&aggregated, SLOT_AGENT_ROOT)
    );
    // The aggregated route disabled its progress channel; the direct route keeps it.
    assert!(!direct.is_disabled(SLOT_RESPONSE_COMMENTARY));
    assert!(aggregated.is_disabled(SLOT_RESPONSE_COMMENTARY));
    assert_eq!(aggregated.text(SLOT_RESPONSE_COMMENTARY), None);
    assert_eq!(
        aggregated
            .provenance(SLOT_RESPONSE_COMMENTARY)
            .map(|p| p.layer),
        Some(FragmentLayer::BindingOverride)
    );
}

#[test]
fn opaque_auto_tier_resolves_the_generic_pack() {
    let catalog = parse(FIXTURE);
    let auto = resolve(
        &catalog,
        &key("workbuddy", "fixture-auto", "balanced-model", "auto"),
        None,
        None,
    );
    assert_eq!(auto.profile_ref, Some(RuntimeString::from("klaus/default")));
    assert_eq!(
        text_of(&auto, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("Complete the task using the tools available in this request.")
    );
    assert_eq!(
        text_of(&auto, SLOT_AGENT_ROOT).as_deref(),
        Some("Coordinate only the workers available in this session.")
    );
    // A route with no binding at all falls back to base + engine defaults.
    let unbound = resolve(
        &catalog,
        &key("custom", "fixture-api", "never-bound", "standard"),
        None,
        None,
    );
    assert!(unbound.profile_ref.is_none());
    assert_eq!(
        text_of(&unbound, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("Complete the task using the tools available in this request.")
    );
    // Unbound slots below the base profile keep engine defaults.
    assert_eq!(
        text_of(&unbound, SLOT_AGENT_MODE_EXPLICIT),
        Some(crate::context::multi_agent::EXPLICIT_MODE.to_owned())
    );
}

#[test]
fn wildcard_binding_fields_match_any_route_value() {
    let catalog = parse(&FIXTURE.replace(
        r#"{
      "sourceKind": "workbuddy", "sourceScope": "fixture-auto", "modelKey": "balanced-model",
      "variantKey": "auto", "profileRef": "klaus/default"
    }"#,
        r#"{
      "sourceKind": "*", "sourceScope": "*", "modelKey": "anywhere-model",
      "variantKey": "*", "profileRef": "fixture/model-b"
    }"#,
    ));
    for (kind, scope, variant) in [
        ("custom", "one", "standard"),
        ("cursor", "two", "max"),
        ("", "", "standard"),
    ] {
        let resolved = resolve(
            &catalog,
            &key(kind, scope, "anywhere-model", variant),
            None,
            None,
        );
        assert_eq!(
            text_of(&resolved, SLOT_AGENT_ROOT).as_deref(),
            Some("Check outstanding assignments before delegating another task."),
            "wildcard binding must match {kind}/{scope}/{variant}"
        );
    }
}

#[test]
fn family_pack_applies_between_generic_pack_and_model_pack() {
    let catalog = parse(
        &FIXTURE
            .replace(
                "\"fixture/model-a\": {",
                r#""family/claude": {
      "extends": "klaus/default",
      "modelMessages": { "agent.child": { "text": "claude child wording" } }
    },
    "fixture/claude-model": {
      "extends": "family/claude",
      "modelMessages": { "agent.root": { "text": "claude model root" } }
    },
    "fixture/model-a": {"#,
            )
            .replace(
                r#"{
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-a",
      "variantKey": "standard", "profileRef": "fixture/model-a"
    }"#,
                r#"{
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-a",
      "variantKey": "standard", "profileRef": "fixture/model-a"
    },
    {
      "sourceKind": "subscription", "sourceScope": "fixture-claude", "modelKey": "claude-x",
      "variantKey": "standard", "profileRef": "fixture/claude-model"
    }"#,
            ),
    );
    // Bound claude model: model pack wins over family pack over generic pack.
    let bound = resolve(
        &catalog,
        &key("subscription", "fixture-claude", "claude-x", "standard"),
        Some("claude"),
        None,
    );
    assert_eq!(
        text_of(&bound, SLOT_AGENT_ROOT).as_deref(),
        Some("claude model root")
    );
    assert_eq!(
        text_of(&bound, SLOT_AGENT_CHILD).as_deref(),
        Some("claude child wording")
    );
    assert_eq!(
        text_of(&bound, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("Complete the task using the tools available in this request.")
    );
    // Unbound model with a known family still receives the family fragments.
    let unbound_family = resolve(
        &catalog,
        &key("", "", "some-claude", "standard"),
        Some("claude"),
        None,
    );
    assert_eq!(
        text_of(&unbound_family, SLOT_AGENT_CHILD).as_deref(),
        Some("claude child wording")
    );
    assert_eq!(
        text_of(&unbound_family, SLOT_AGENT_ROOT).as_deref(),
        Some("Coordinate only the workers available in this session.")
    );
    // A different family never sees claude-scoped fragments.
    let other_family = resolve(
        &catalog,
        &key("", "", "some-claude", "standard"),
        Some("gemini"),
        None,
    );
    assert_eq!(
        text_of(&other_family, SLOT_AGENT_CHILD),
        Some(crate::context::multi_agent::CHILD_ROLE.to_owned())
    );
}

#[test]
fn declared_layer_wins_over_catalog_and_engine_defaults() {
    let catalog = parse(FIXTURE);
    let declared_value = declared(
        r#"{"multi_agent":{"role":{"root":"declared root"},"mode":{"proactive":"declared proactive"}}}"#,
    );
    let resolved = resolve(
        &catalog,
        &key("custom", "fixture-api", "model-a", "standard"),
        None,
        Some(&declared_value),
    );
    assert_eq!(
        text_of(&resolved, SLOT_AGENT_ROOT).as_deref(),
        Some("declared root")
    );
    assert_eq!(
        text_of(&resolved, SLOT_AGENT_MODE_PROACTIVE).as_deref(),
        Some("declared proactive")
    );
    // Untouched slots keep their catalog values.
    assert_eq!(
        text_of(&resolved, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("Complete the task using the tools available in this request.")
    );
    assert_eq!(
        resolved.provenance(SLOT_AGENT_ROOT).map(|p| p.layer),
        Some(FragmentLayer::Declared)
    );
    assert_eq!(
        resolved.provenance(SLOT_BASE_INSTRUCTIONS).map(|p| p.layer),
        Some(FragmentLayer::BaseProfile)
    );
}

#[test]
fn legacy_declared_shapes_normalize_with_original_consumer_semantics() {
    // Shadow oracle: the exact values the pre-catalog ad-hoc readers produced.
    let messages = declared(
        r#"{"multi_agent":{"role":{"root":"root base","subagent":"child base"},"mode":{"proactive":" custom\nmode ","explicit":"off"}}}"#,
    );
    let fragments = ModelFragments::for_family(None, Some(&messages));
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from("root base"))
    );
    assert_eq!(
        fragments.agent_role(true),
        Some(&RuntimeString::from("child base"))
    );
    assert_eq!(
        fragments.agent_mode(true, None),
        Some(RuntimeString::from(" custom\nmode "))
    );
    assert_eq!(
        fragments.agent_mode(false, None),
        Some(RuntimeString::from("off"))
    );

    // camelCase aliases resolve identically.
    let aliases = declared(
        r#"{"multiAgent":{"role":{"root":"alias root"},"mode":{"hintText":"alias hint"}}}"#,
    );
    let fragments = ModelFragments::for_family(None, Some(&aliases));
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from("alias root"))
    );
    assert_eq!(
        fragments.agent_mode(false, None),
        Some(RuntimeString::from("alias hint"))
    );

    // Empty delegation texts disable their parts (legacy skip semantics).
    let emptied = declared(r#"{"multi_agent":{"role":{"root":""},"mode":{"proactive":""}}}"#);
    let fragments = ModelFragments::for_family(None, Some(&emptied));
    assert_eq!(fragments.agent_role(false), None);
    assert_eq!(fragments.agent_mode(true, None), None);
    // The sibling explicit slot survives the emptied proactive leaf.
    assert_eq!(
        fragments.agent_mode(false, None),
        Some(RuntimeString::from(
            crate::context::multi_agent::EXPLICIT_MODE
        ))
    );

    // An explicitly cleared hint suppresses the mode text entirely, matching
    // the legacy short-circuit.
    let cleared_hint =
        declared(r#"{"multi_agent":{"mode":{"hint_text":"","proactive":"active"}}}"#);
    let fragments = ModelFragments::for_family(None, Some(&cleared_hint));
    assert_eq!(fragments.agent_mode(true, None), None);

    // Empty review texts stay literal overrides (not disables).
    let review = declared(r#"{"auto_review":{"policy":"","rejection_instructions":"no"}}"#);
    let fragments = ModelFragments::for_family(None, Some(&review));
    assert_eq!(
        fragments.review_text(SLOT_REVIEW_POLICY),
        Some(&RuntimeString::from(""))
    );
    assert_eq!(
        fragments.review_text(SLOT_REVIEW_REJECTION_INSTRUCTIONS),
        Some(&RuntimeString::from("no"))
    );

    // Structured guardian settings pass through unchanged.
    let guardian = declared(r#"{"guardian_v2":{"risk_score_template":"t","threshold":7}}"#);
    let fragments = ModelFragments::for_family(None, Some(&guardian));
    let value = fragments.guardian_v2().expect("guardian_v2 survives");
    assert_eq!(
        value
            .get("risk_score_template")
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("t"))
    );
    // camelCase alias.
    let guardian_alias = declared(r#"{"guardianV2":{"a":1}}"#);
    let fragments = ModelFragments::for_family(None, Some(&guardian_alias));
    assert!(fragments.guardian_v2().is_some());

    // Non-string leaves are ignored, keeping the engine default.
    let junk = declared(r#"{"multi_agent":{"role":{"root":42},"mode":{"explicit":true}}}"#);
    let fragments = ModelFragments::for_family(None, Some(&junk));
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from(crate::context::multi_agent::ROOT_ROLE))
    );
    assert_eq!(
        fragments.agent_mode(false, None),
        Some(RuntimeString::from(
            crate::context::multi_agent::EXPLICIT_MODE
        ))
    );
}

#[test]
fn slot_keyed_declared_form_supports_text_json_and_disabled() {
    let value = declared(
        r#"{"slots":{
            "agent.root": {"text":"slot-keyed root"},
            "response.commentary": {"disabled":true},
            "review.guardian_v2": {"json":{"threshold":3}},
            "agent.child": null,
            "unknown.slot": {"text":"ignored"}
        }}"#,
    );
    let fragments = ModelFragments::for_family(None, Some(&value));
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from("slot-keyed root"))
    );
    assert!(fragments.resolved().is_disabled(SLOT_RESPONSE_COMMENTARY));
    assert!(fragments.commentary().is_none());
    assert!(fragments.guardian_v2().is_some());
    // null inherits the engine default.
    assert_eq!(
        fragments.agent_role(true),
        Some(&RuntimeString::from(
            crate::context::multi_agent::CHILD_ROLE
        ))
    );
    // A declared disable of a non-disableable slot is ignored, not fatal.
    let value = declared(r#"{"slots":{"base.instructions":{"disabled":true}}}"#);
    let fragments = ModelFragments::for_family(None, Some(&value));
    assert_eq!(
        fragments.base_instructions(),
        RuntimeString::from(crate::context::system_prompt::BASE_INSTRUCTIONS)
    );
    // Oversized declared text falls back to the next valid value.
    let big = "x".repeat(
        crate::prompt_catalog::slots::slot_spec(SLOT_AGENT_MODE_HINT)
            .unwrap()
            .max_bytes
            + 1,
    );
    let value = declared(&format!(
        r#"{{"slots":{{"agent.mode.hint":{{"text":"{big}"}}}}}}"#
    ));
    let fragments = ModelFragments::for_family(None, Some(&value));
    // The oversized entry is ignored: the slot keeps no declared value.
    assert!(fragments.resolved().text(SLOT_AGENT_MODE_HINT).is_none());
    assert!(fragments
        .resolved()
        .provenance(SLOT_AGENT_MODE_HINT)
        .is_none());
}

#[test]
fn corrupt_catalogs_are_rejected_whole() {
    // Unknown slot id.
    let error = parse_err(&FIXTURE.replace("\"agent.root\"", "\"agent.unknown\""));
    assert!(error.0.contains("unregistered slot"), "{error}");
    // Unresolvable fragment reference.
    let error = parse_err(&FIXTURE.replace("\"ref\": \"root-a\"", "\"ref\": \"root-missing\""));
    assert!(error.0.contains("unknown fragment"), "{error}");
    // Inheritance cycle.
    let cyclic = r#"{
      "schemaVersion": 2,
      "revision": "r",
      "profiles": {
        "a": { "extends": "b", "modelMessages": {} },
        "b": { "extends": "a", "modelMessages": {} }
      },
      "bindings": []
    }"#;
    let error = parse_err(cyclic);
    assert!(error.0.contains("cyclic"), "{error}");
    // Extends an unknown profile.
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{"a":{"extends":"missing","modelMessages":{}}},"bindings":[]}"#,
    );
    assert!(error.0.contains("unknown profile"), "{error}");
    // Duplicate binding keys are a same-layer conflict.
    let error = parse_err(&FIXTURE.replace(
        r#"{
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-b",
      "variantKey": "standard", "profileRef": "fixture/model-b"
    }"#,
        r#"{
      "sourceKind": "custom", "sourceScope": "fixture-api", "modelKey": "model-a",
      "variantKey": "standard", "profileRef": "fixture/model-b"
    }"#,
    ));
    assert!(error.0.contains("same key"), "{error}");
    // disabled on a non-disableable slot.
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{"klaus/default":{"modelMessages":{"base.instructions":{"disabled":true}}}},"bindings":[]}"#,
    );
    assert!(error.0.contains("non-disableable"), "{error}");
    // Wrong schema version.
    let error = parse_err(&FIXTURE.replace("\"schemaVersion\": 2", "\"schemaVersion\": 1"));
    assert!(error.0.contains("schemaVersion"), "{error}");
    // Oversized fragment text.
    let big = "x".repeat(300 * 1024);
    let error = parse_err(
        &r#"{"schemaVersion":2,"revision":"r","fragments":{"big":"BIG"},"profiles":{"klaus/default":{"modelMessages":{"response.commentary":{"ref":"big"}}}},"bindings":[]}"#
            .replace("BIG", &big),
    );
    assert!(error.0.contains("exceeds"), "{error}");
    // Text entry on a structured slot and vice versa.
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{"klaus/default":{"modelMessages":{"review.guardian_v2":{"text":"no"}}}},"bindings":[]}"#,
    );
    assert!(error.0.contains("inline text"), "{error}");
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{"klaus/default":{"modelMessages":{"base.instructions":{"json":{}}}}},"bindings":[]}"#,
    );
    assert!(error.0.contains("structured"), "{error}");
    // Binding to an unknown profile.
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{},"bindings":[{"sourceKind":"custom","sourceScope":"s","modelKey":"m","variantKey":"standard","profileRef":"nope"}]}"#,
    );
    assert!(error.0.contains("unknown profile"), "{error}");
    // Entry shapes that mix keys are rejected.
    let error = parse_err(
        r#"{"schemaVersion":2,"revision":"r","profiles":{"klaus/default":{"modelMessages":{"agent.root":{"text":"a","disabled":true}}}},"bindings":[]}"#,
    );
    assert!(error.0.contains("combines"), "{error}");
    // Missing revision.
    let error = parse_err(r#"{"schemaVersion":2,"profiles":{},"bindings":[]}"#);
    assert!(error.0.contains("revision"), "{error}");
}

#[test]
fn leaf_merge_keeps_unmentioned_fragments_of_the_lower_layer() {
    use crate::prompt_catalog::merge_declared_leaves;
    let catalog_side = declared(
        r#"{"multi_agent":{"role":{"root":"catalog root","subagent":"catalog child"},"mode":{"explicit":"catalog explicit"}},"auto_review":{"policy":"catalog policy"}}"#,
    );
    let declared_side = declared(r#"{"multi_agent":{"role":{"root":"declared root"}}}"#);
    let merged = merge_declared_leaves(Some(catalog_side), Some(declared_side)).unwrap();
    // The declared leaf wins.
    assert_eq!(
        merged
            .get("multi_agent")
            .and_then(|m| m.get("role"))
            .and_then(|r| r.get("root"))
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("declared root"))
    );
    // The unmentioned sibling leaves survive — the regression the
    // whole-object replacement caused.
    assert_eq!(
        merged
            .get("multi_agent")
            .and_then(|m| m.get("role"))
            .and_then(|r| r.get("subagent"))
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("catalog child"))
    );
    assert_eq!(
        merged
            .get("multi_agent")
            .and_then(|m| m.get("mode"))
            .and_then(|r| r.get("explicit"))
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("catalog explicit"))
    );
    assert_eq!(
        merged
            .get("auto_review")
            .and_then(|r| r.get("policy"))
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("catalog policy"))
    );

    // Explicit null inherits the lower layer.
    let base = declared(r#"{"multi_agent":{"role":{"root":"base"}}}"#);
    let overlay = declared(r#"{"multi_agent":{"role":{"root":null}}}"#);
    let merged = merge_declared_leaves(Some(base), Some(overlay)).unwrap();
    assert_eq!(
        merged
            .get("multi_agent")
            .and_then(|m| m.get("role"))
            .and_then(|r| r.get("root"))
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("base"))
    );

    // Scalar overlays replace wholesale.
    let base = declared(r#"{"guardian_v2":{"a":1}}"#);
    let overlay = declared(r#"{"guardian_v2":"off"}"#);
    let merged = merge_declared_leaves(Some(base), Some(overlay)).unwrap();
    assert_eq!(
        merged.get("guardian_v2").and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("off"))
    );

    // None handling.
    assert!(merge_declared_leaves(None, None).is_none());
    let only = declared(r#"{"a":1}"#);
    assert_eq!(
        merge_declared_leaves(Some(only.clone()), None),
        Some(only.clone())
    );
    assert_eq!(merge_declared_leaves(None, Some(only.clone())), Some(only));
}

#[test]
fn resolution_provenance_names_the_winning_layer_for_every_slot() {
    let catalog = parse(FIXTURE);
    let resolved = resolve(
        &catalog,
        &key("cursor", "fixture-cursor", "mapped-a", "standard"),
        None,
        Some(&declared(r#"{"auto_review":{"policy":"declared"}}"#)),
    );
    let provenance = |slot: &str| {
        resolved
            .provenance(slot)
            .map(|p| (p.layer, p.origin.clone()))
    };
    assert_eq!(
        provenance(SLOT_BASE_INSTRUCTIONS),
        Some((FragmentLayer::BaseProfile, RuntimeString::from("base-v3")))
    );
    assert_eq!(
        provenance(SLOT_AGENT_ROOT),
        Some((FragmentLayer::ModelProfile, RuntimeString::from("root-a")))
    );
    assert_eq!(
        provenance(SLOT_RESPONSE_COMMENTARY).map(|(layer, _)| layer),
        Some(FragmentLayer::BindingOverride)
    );
    assert_eq!(
        provenance(SLOT_REVIEW_POLICY).map(|(layer, _)| layer),
        Some(FragmentLayer::Declared)
    );
    assert_eq!(
        provenance(SLOT_AGENT_MODE_EXPLICIT).map(|(layer, _)| layer),
        Some(FragmentLayer::EngineDefault)
    );
    // The diagnostic summary covers every resolved slot without leaking text.
    let rows = resolved.describe();
    assert!(rows
        .iter()
        .any(|(slot, _, _, _)| *slot == SLOT_BASE_INSTRUCTIONS));
    assert!(rows.iter().all(|(_, _, _, size)| *size < 512 * 1024));
}

#[test]
fn hint_precedence_keeps_configured_hint_on_top() {
    let messages = declared(
        r#"{"multi_agent":{"mode":{"hint_text":"catalog","proactive":"active","explicit":"inactive"}}}"#,
    );
    let fragments = ModelFragments::for_family(None, Some(&messages));
    let configured = RuntimeString::from("configured");
    assert_eq!(
        fragments.agent_mode(true, Some(&configured)),
        Some(RuntimeString::from("configured"))
    );
    assert_eq!(
        fragments.agent_mode(true, None),
        Some(RuntimeString::from("catalog"))
    );
    // An empty configured hint short-circuits to an empty text, which the
    // consumer renders as no part (legacy semantics).
    assert_eq!(
        fragments.agent_mode(true, Some(&RuntimeString::default())),
        Some(RuntimeString::default())
    );
}

#[test]
fn structured_guardian_declaration_resolves_through_the_fragment_channel() {
    let messages =
        declared(r#"{"guardian_v2":{"classifier_model":"gpt-x","threshold_basis_points":5000}}"#);
    let fragments = ModelFragments::for_family(None, Some(&messages));
    let value = fragments.guardian_v2().expect("guardian_v2");
    assert_eq!(
        value
            .get("classifier_model")
            .and_then(RuntimeJson::as_string),
        Some(&RuntimeString::from("gpt-x"))
    );
    assert_eq!(
        fragments
            .resolved()
            .provenance(SLOT_REVIEW_GUARDIAN_V2)
            .map(|p| p.layer),
        Some(FragmentLayer::Declared)
    );
}

#[test]
fn child_role_reads_its_own_leaf() {
    let messages = declared(r#"{"multi_agent":{"role":{"subagent":"child text"}}}"#);
    let fragments = ModelFragments::for_family(None, Some(&messages));
    assert_eq!(
        fragments.agent_role(true),
        Some(&RuntimeString::from("child text"))
    );
    // The root leaf keeps its engine default.
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from(crate::context::multi_agent::ROOT_ROLE))
    );
    assert_eq!(
        fragments
            .resolved()
            .provenance(SLOT_AGENT_CHILD)
            .map(|p| p.layer),
        Some(FragmentLayer::Declared)
    );
    assert_eq!(
        fragments
            .resolved()
            .provenance(SLOT_AGENT_ROOT)
            .map(|p| p.layer),
        Some(FragmentLayer::EngineDefault)
    );
    // The hint slot has no engine default and no declaration: it stays absent.
    assert!(fragments
        .resolved()
        .provenance(SLOT_AGENT_MODE_HINT)
        .is_none());
}

// ---- Phase 2: bundled data, source routes, trust tiers, refresh chain ----

#[test]
fn bundled_commentary_flows_from_catalog_data_with_identical_bytes() {
    let claude = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-4-5"), None);
    assert_eq!(
        claude.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_UNTAGGED_BRIEF_UPDATES
        ))
    );
    assert_eq!(
        claude
            .resolved()
            .provenance(SLOT_RESPONSE_COMMENTARY)
            .map(|p| (p.layer, p.origin.clone())),
        Some((
            FragmentLayer::FamilyProfile,
            RuntimeString::from("commentary-untagged-brief")
        ))
    );

    let gemini = ModelFragments::for_model(&RuntimeString::from("gemini-2.5-pro"), None);
    assert_eq!(
        gemini.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_UNTAGGED_THINKING_CHANNEL
        ))
    );

    // The generic pack carries the phase-tagged text for every other model.
    for model in ["gpt-4o", "qwen3-max", "deepseek-v3.2", "unknown-model"] {
        let fragments = ModelFragments::for_model(&RuntimeString::from(model), None);
        assert_eq!(
            fragments.commentary(),
            Some(&RuntimeString::from(
                crate::context::system_prompt::COMMENTARY_PHASE_TAGGED
            )),
            "{model} must resolve the generic-pack commentary"
        );
    }
    // Required slots still resolve to the engine baseline.
    let any = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-4-5"), None);
    assert_eq!(
        any.base_instructions(),
        RuntimeString::from(crate::context::system_prompt::BASE_INSTRUCTIONS)
    );
    assert_eq!(
        crate::prompt_catalog::bundled().revision,
        RuntimeString::from("bundled-r6")
    );
}

#[test]
fn purpose_slots_resolve_their_engine_defaults() {
    let fragments = ModelFragments::for_model(&RuntimeString::from("gpt-5.6-sol"), None);
    assert_eq!(
        fragments.title_instructions(),
        RuntimeString::from(crate::engine::session::SESSION_TITLE_PROMPT)
    );
    assert_eq!(
        fragments.advisor_guidance(),
        RuntimeString::from(crate::context::system_prompt::ADVISOR_GUIDANCE)
    );
    assert_eq!(
        fragments.advisor_role(),
        RuntimeString::from(crate::engine::session::ADVISOR_INSTRUCTIONS)
    );
    assert_eq!(
        fragments.advisor_summary(),
        RuntimeString::from(crate::engine::session::SUMMARY_INSTRUCTIONS)
    );
    assert_eq!(
        fragments.orchestration_guidance(),
        RuntimeString::from(crate::context::system_prompt::ORCHESTRATION_GUIDANCE)
    );
    assert_eq!(
        fragments.compact_summary_template(),
        RuntimeString::from(crate::context::compact::full_compact::SUMMARY_PROMPT)
    );
    assert_eq!(
        fragments.compact_lean_system(),
        RuntimeString::from(crate::context::compact::full_compact::LEAN_SUMMARY_SYSTEM_PROMPT)
    );
    assert_eq!(
        fragments.memory_learning_quality(),
        RuntimeString::from(include_str!("../memory/prompts/learning_quality.md"))
    );
    assert_eq!(
        fragments.memory_consolidation_quality(),
        RuntimeString::from(include_str!("../memory/prompts/consolidation_quality.md"))
    );
}

/// Two routes of one confirmed model share the model pack; each keeps its own
/// channel differences (spec acceptance: direct + aggregated same-model).
const SOURCE_ROUTE_FIXTURE: &str = r#"{
  "schemaVersion": 2,
  "revision": "routes-r1",
  "fragments": {
    "claude-root": "Coordinate the work across the workers this session declares."
  },
  "profiles": {
    "klaus/default": { "modelMessages": {} },
    "claude/sonnet-x": {
      "extends": "klaus/default",
      "modelMessages": { "agent.root": { "ref": "claude-root" } }
    }
  },
  "bindings": [
    {
      "sourceKind": "subscription", "sourceScope": "*", "modelKey": "claude-sonnet-x",
      "variantKey": "standard", "profileRef": "claude/sonnet-x"
    },
    {
      "sourceKind": "cursor", "sourceScope": "*", "modelKey": "mapped:claude-sonnet-x",
      "variantKey": "standard", "profileRef": "claude/sonnet-x",
      "overrides": { "response.commentary": { "disabled": true } }
    }
  ]
}"#;

#[test]
fn source_scoped_routes_share_the_pack_and_keep_channel_differences() {
    let catalog = parse(SOURCE_ROUTE_FIXTURE);
    let direct = resolve(
        &catalog,
        &key("subscription", "", "claude-sonnet-x", "standard"),
        Some("claude"),
        None,
    );
    let aggregated = resolve(
        &catalog,
        &key("cursor", "", "mapped:claude-sonnet-x", "standard"),
        Some("claude"),
        None,
    );
    // Shared model-pack content on both routes.
    assert_eq!(
        text_of(&direct, SLOT_AGENT_ROOT).as_deref(),
        Some("Coordinate the work across the workers this session declares.")
    );
    assert_eq!(
        text_of(&direct, SLOT_AGENT_ROOT),
        text_of(&aggregated, SLOT_AGENT_ROOT)
    );
    // Channel difference: the aggregated route disabled its progress channel.
    assert!(!direct.is_disabled(SLOT_RESPONSE_COMMENTARY));
    assert!(aggregated.is_disabled(SLOT_RESPONSE_COMMENTARY));

    // A source declaration rides only its own route.
    let declared_value = declared(r#"{"auto_review":{"policy":"subscription-only policy"}}"#);
    let direct_declared = resolve(
        &catalog,
        &key("subscription", "", "claude-sonnet-x", "standard"),
        Some("claude"),
        Some(&declared_value),
    );
    let aggregated_plain = resolve(
        &catalog,
        &key("cursor", "", "mapped:claude-sonnet-x", "standard"),
        Some("claude"),
        None,
    );
    assert_eq!(
        text_of(&direct_declared, SLOT_REVIEW_POLICY).as_deref(),
        Some("subscription-only policy")
    );
    assert!(text_of(&aggregated_plain, SLOT_REVIEW_POLICY).is_none());

    // An unrelated source never matches the route bindings.
    let other = resolve(
        &catalog,
        &key("kiro", "", "claude-sonnet-x", "standard"),
        Some("claude"),
        None,
    );
    assert!(other.profile_ref.is_none());
    assert_eq!(
        text_of(&other, SLOT_AGENT_ROOT),
        Some(crate::context::multi_agent::ROOT_ROLE.to_owned())
    );
}

#[test]
fn untrusted_revisions_cannot_override_high_authority_slots() {
    let document = r#"{
      "schemaVersion": 2,
      "revision": "distributed-r1",
      "fragments": { "rogue-base": "REWRITTEN BASE", "tuned-root": "tuned delegation role" },
      "profiles": {
        "klaus/default": {
          "modelMessages": {
            "base.instructions": { "ref": "rogue-base" },
            "response.commentary": { "text": "REWRITTEN COMMENTARY" },
            "agent.root": { "ref": "tuned-root" },
            "review.policy": { "text": "tuned review policy" }
          }
        }
      },
      "bindings": []
    }"#;
    // Unsigned (distributed) revision: high-authority slots keep the engine
    // baseline; model/purpose-scoped slots apply.
    let untrusted =
        PromptCatalog::parse(&RuntimeString::from(document)).expect("fixture must be valid");
    assert!(!untrusted.trusted);
    let resolved = resolve(
        &untrusted,
        &key("", "", "any-model", "standard"),
        None,
        None,
    );
    assert_eq!(
        text_of(&resolved, SLOT_BASE_INSTRUCTIONS),
        Some(crate::context::system_prompt::BASE_INSTRUCTIONS.to_owned())
    );
    assert_eq!(
        text_of(&resolved, SLOT_RESPONSE_COMMENTARY),
        Some(crate::context::system_prompt::COMMENTARY_PHASE_TAGGED.to_owned())
    );
    assert_eq!(
        text_of(&resolved, SLOT_AGENT_ROOT).as_deref(),
        Some("tuned delegation role")
    );
    assert_eq!(
        text_of(&resolved, SLOT_REVIEW_POLICY).as_deref(),
        Some("tuned review policy")
    );

    // The same revision, trusted (bundled or signature-verified), applies
    // every slot.
    let trusted = untrusted.clone().with_trust(true);
    let resolved = resolve(&trusted, &key("", "", "any-model", "standard"), None, None);
    assert_eq!(
        text_of(&resolved, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("REWRITTEN BASE")
    );
    assert_eq!(
        text_of(&resolved, SLOT_RESPONSE_COMMENTARY).as_deref(),
        Some("REWRITTEN COMMENTARY")
    );
}

#[test]
fn for_route_binds_the_source_key_through_the_active_store() {
    use crate::prompt_catalog::store::PromptCatalogStore;
    let store = PromptCatalogStore::global();
    let bundled_before = store.active();
    store.install(parse(SOURCE_ROUTE_FIXTURE).with_trust(true));

    let direct = ModelFragments::for_route(
        &RuntimeString::from("claude-sonnet-x"),
        Some("subscription"),
        None,
    );
    assert_eq!(
        direct.agent_role(false),
        Some(&RuntimeString::from(
            "Coordinate the work across the workers this session declares."
        ))
    );
    // Without the source kind the route binding cannot match.
    let routed_out = ModelFragments::for_route(&RuntimeString::from("claude-sonnet-x"), None, None);
    assert_eq!(
        routed_out.agent_role(false),
        Some(&RuntimeString::from(crate::context::multi_agent::ROOT_ROLE))
    );

    store.install((*bundled_before).clone());
    assert_eq!(store.active().revision, RuntimeString::from("bundled-r6"));
}

mod store_refresh {
    use crate::prompt_catalog::store::{CatalogRefresh, PromptCatalogStore};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "prompt-catalog-{}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().into_owned()
    }

    async fn serve_scripted(listener: TcpListener, responses: Vec<String>) -> Vec<String> {
        let mut heads = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                let read = socket.read(&mut byte).await.unwrap();
                assert_ne!(read, 0, "client closed before completing the request");
                head.push(byte[0]);
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            heads.push(String::from_utf8(head).unwrap());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        heads
    }

    fn document(revision: &str) -> String {
        format!(
            r#"{{"schemaVersion":2,"revision":"{revision}","fragments":{{}},"profiles":{{"klaus/default":{{"modelMessages":{{}}}}}},"bindings":[]}}"#
        )
    }

    fn ok_response(body: &str, etag: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: {etag}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[tokio::test]
    async fn refresh_activates_valid_revisions_and_keeps_current_on_invalid() {
        let dir = temp_dir();
        let store = PromptCatalogStore::load(Some(&dir));
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "bundled-r6"
        );

        let v1 = document("distributed-v1");
        let invalid = r#"{"schemaVersion":99,"revision":"bad"}"#.to_string();
        let v2 = document("distributed-v2");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_scripted(
            listener,
            vec![
                ok_response(&v1, "\"e1\""),
                ok_response(&invalid, "\"e2\""),
                ok_response(&v2, "\"e3\""),
                "HTTP/1.1 304 Not Modified\r\nETag: \"e3\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            ],
        ));
        let url = format!("http://{address}/prompt-catalog.json");
        let http = reqwest::Client::new();

        // A valid revision activates atomically.
        assert_eq!(
            store.refresh(&http, &url).await.unwrap(),
            CatalogRefresh::Updated
        );
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "distributed-v1"
        );

        // An invalid candidate never activates; the current revision stays.
        assert!(store.refresh(&http, &url).await.is_err());
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "distributed-v1"
        );

        // The next valid revision replaces it, then a 304 only revalidates.
        assert_eq!(
            store.refresh(&http, &url).await.unwrap(),
            CatalogRefresh::Updated
        );
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "distributed-v2"
        );
        assert_eq!(
            store.refresh(&http, &url).await.unwrap(),
            CatalogRefresh::NotModified
        );
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "distributed-v2"
        );

        let heads = server.await.unwrap();
        // Conditional requests carry the stored ETag after the first fetch
        // (hyper writes request header names lowercased).
        assert!(!heads[0].to_ascii_lowercase().contains("if-none-match"));
        assert!(heads[1]
            .to_ascii_lowercase()
            .contains("if-none-match: \"e1\""));
        assert!(heads[3]
            .to_ascii_lowercase()
            .contains("if-none-match: \"e3\""));

        // Offline restart: the cached revision activates without a network
        // round-trip, and it stays untrusted (no high-authority overrides).
        let reloaded = PromptCatalogStore::load(Some(&dir));
        assert_eq!(
            reloaded.active().revision.try_to_utf8().unwrap().as_str(),
            "distributed-v2"
        );
        assert!(!reloaded.active().trusted);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn concurrent_refreshes_publish_complete_revisions_only() {
        // Two refreshers race the same store: each validates its own
        // candidate and the store only ever holds one complete revision —
        // readers never observe a half-applied document.
        let dir = temp_dir();
        let store = std::sync::Arc::new(PromptCatalogStore::load(Some(&dir)));
        let va = document("concurrent-a");
        let vb = document("concurrent-b");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_scripted(
            listener,
            vec![ok_response(&va, "\"ea\""), ok_response(&vb, "\"eb\"")],
        ));
        let url = format!("http://{address}/prompt-catalog.json");
        let http = reqwest::Client::new();

        let (r1, r2) = tokio::join!(store.refresh(&http, &url), store.refresh(&http, &url));
        assert!(
            r1.is_ok() && r2.is_ok(),
            "both refreshes must complete: {r1:?} {r2:?}"
        );
        let active = store.active();
        // Whichever revision won, it is complete and self-consistent.
        assert!(
            active.revision.try_to_utf8().unwrap().as_str() == "concurrent-a"
                || active.revision.try_to_utf8().unwrap().as_str() == "concurrent-b"
        );
        assert_eq!(active.schema_version, 2);
        server.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn corrupt_cache_falls_back_to_the_bundled_package() {
        let dir = temp_dir();
        std::fs::write(
            format!("{dir}/prompt-catalog.json"),
            r#"{"etag":"x","checkedAtMs":1,"document":{"schemaVersion":2,"revision":"broken","profiles":{"p":{"extends":"missing"}}}}"#,
        )
        .unwrap();
        let store = PromptCatalogStore::load(Some(&dir));
        assert_eq!(
            store.active().revision.try_to_utf8().unwrap().as_str(),
            "bundled-r6"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}

// ---- Bundled model packs: per-model base prompts synced from the published
// ---- subscription catalog data (converted into Klaus slot vocabulary).

#[test]
fn bundled_model_packs_resolve_per_model_and_share_across_routes() {
    let sol = ModelFragments::for_model(&RuntimeString::from("gpt-6.2-sol"), None);
    let astra = ModelFragments::for_model(&RuntimeString::from("gpt-6.2-astra"), None);
    let five_six = ModelFragments::for_model(&RuntimeString::from("gpt-5.6-sol"), None);
    let five_six_terra = ModelFragments::for_model(&RuntimeString::from("gpt-5.6-terra"), None);

    // Each bound model resolves its own base pack instead of the generic
    // engine default, and sibling models of one generation still differ.
    let generic = RuntimeString::from(crate::context::system_prompt::BASE_INSTRUCTIONS);
    assert_ne!(sol.base_instructions(), generic);
    assert_ne!(astra.base_instructions(), generic);
    assert_ne!(sol.base_instructions(), astra.base_instructions());
    // Models sharing one template share one fragment.
    assert_eq!(
        five_six_terra.base_instructions(),
        five_six.base_instructions()
    );
    assert_eq!(
        sol.resolved()
            .provenance(SLOT_BASE_INSTRUCTIONS)
            .map(|p| p.layer),
        Some(FragmentLayer::ModelProfile)
    );

    // Same confirmed model through another route resolves the same pack
    // (sourceKind "*" bindings): cross-entry sharing with per-model identity.
    // Aggregating routes carry the selected parameters inside the model key, so
    // the decorated key must reach the same pack as the bare model id.
    for selection in [
        "gpt-6.2-sol",
        "gpt-6.2-sol[context=1m,fast=false,reasoning=high]@max",
        "gpt-5.6-sol[context=272k,fast=true,reasoning=none]",
    ] {
        let via_other_route =
            ModelFragments::for_route(&RuntimeString::from(selection), Some("cursor"), None);
        let expected = if selection.starts_with("gpt-6.2-sol") {
            sol.base_instructions()
        } else {
            five_six.base_instructions()
        };
        assert_eq!(
            via_other_route.base_instructions(),
            expected,
            "decorated selection {selection} must resolve its model pack"
        );
    }

    // Commentary follows the model's own group text, not the generic pack.
    assert_ne!(
        sol.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_PHASE_TAGGED
        ))
    );
    assert_eq!(sol.commentary(), astra.commentary());
    assert_ne!(sol.commentary(), five_six.commentary());

    // Unbound and non-GPT models keep the generic resolution.
    let unbound = ModelFragments::for_model(&RuntimeString::from("gpt-4o"), None);
    assert_eq!(unbound.base_instructions(), generic);
    assert_eq!(
        unbound.commentary(),
        Some(&RuntimeString::from(
            crate::context::system_prompt::COMMENTARY_PHASE_TAGGED
        ))
    );
    let claude = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-4-5"), None);
    assert_eq!(claude.base_instructions(), generic);

    // Model packs leave the other slots on their inherited layers.
    assert_eq!(
        sol.collaboration_template(false),
        RuntimeString::from(include_str!("../planning/prompts/default.md"))
    );
    assert_eq!(
        sol.title_instructions(),
        RuntimeString::from(crate::engine::session::SESSION_TITLE_PROMPT)
    );
}

#[test]
fn model_pack_base_keeps_klaus_identity_and_tool_vocabulary() {
    let sol = ModelFragments::for_model(&RuntimeString::from("gpt-6.2-sol"), None);
    let base = sol.base_instructions().try_to_utf8().unwrap();
    // Identity and product surface are Klaus's own.
    assert!(base.starts_with("You are Klaus."));
    for banned in [
        "functions.",
        "{{",
        "ImageGen",
        "exec_command",
        "$CODEX_HOME",
        "skills.read",
        "list_mcp_resource_templates",
    ] {
        assert!(
            !base.contains(banned),
            "converted base still contains {banned}"
        );
    }
    for expected in [
        "ask_user_async",
        "`skill`",
        "# Apps (Connectors)",
        "`generate_image`",
    ] {
        assert!(base.contains(expected), "converted base misses {expected}");
    }
}

#[test]
fn bundled_claude_model_packs_resolve_per_model_and_keep_model_sections() {
    let generic = RuntimeString::from(crate::context::system_prompt::BASE_INSTRUCTIONS);
    let fable = ModelFragments::for_model(&RuntimeString::from("claude-fable-5-2"), None);
    let opus = ModelFragments::for_model(&RuntimeString::from("claude-opus-5-5"), None);
    let sonnet = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-5-5"), None);
    let haiku = ModelFragments::for_model(&RuntimeString::from("claude-haiku-5-5"), None);

    for (model, fragments) in [
        ("claude-fable-5-2", &fable),
        ("claude-opus-5-5", &opus),
        ("claude-sonnet-5-5", &sonnet),
        ("claude-haiku-5-5", &haiku),
    ] {
        assert_ne!(fragments.base_instructions(), generic);
        let base = fragments.base_instructions().try_to_utf8().unwrap();
        assert!(base.starts_with("You are Klaus."));
        assert!(
            base.contains(&format!("The exact model ID is {model}.")),
            "{model} pack lost its model identity line"
        );
        assert!(base.contains("# Harness"));
        assert!(base.contains("# Session-specific guidance"));
        assert!(base.contains("# Using skills"));
        assert!(base.contains("# Apps (Connectors)"));
        assert!(base.contains("# Plugins"));
        // Lean packs keep the capable-model prompt small instead of carrying
        // the full generic final-answer and tool-discipline sections.
        assert!(
            base.len() <= 12 * 1024,
            "{model} lean pack grew past its budget: {} bytes",
            base.len()
        );
        assert!(!base.contains("### Final answer structure and style guidelines"));
        assert_eq!(
            fragments.commentary(),
            Some(&RuntimeString::from(
                crate::context::system_prompt::COMMENTARY_UNTAGGED_BRIEF_UPDATES
            )),
            "{model} must keep the Claude progress-channel policy"
        );
        for banned in [
            "Claude Code",
            "Anthropic's official CLI",
            "CLAUDE.md",
            "AgentTool",
            "functions.",
        ] {
            assert!(
                !base.contains(banned),
                "{model} pack still contains product token {banned}"
            );
        }
    }

    let fable_base = fable.base_instructions().try_to_utf8().unwrap();
    assert!(fable_base.contains("# Finishing work"));
    assert!(fable_base.contains("# Memory, notes and feedback"));
    assert!(fable_base.contains("# Original games"));

    let opus_base = opus.base_instructions().try_to_utf8().unwrap();
    assert!(opus_base.contains("# Original games"));
    assert!(!opus_base.contains("# Finishing work"));
    assert!(!opus_base.contains("# Early stopping"));

    let sonnet_base = sonnet.base_instructions().try_to_utf8().unwrap();
    assert!(!sonnet_base.contains("# Finishing work"));
    assert!(!sonnet_base.contains("# Early stopping"));
    assert!(!sonnet_base.contains("# Original games"));

    let haiku_base = haiku.base_instructions().try_to_utf8().unwrap();
    assert!(haiku_base.contains("# Early stopping"));
    assert!(!haiku_base.contains("# Finishing work"));

    assert_ne!(
        fable.base_instructions(),
        opus.base_instructions(),
        "model bundles must not collapse to one shared base"
    );
    assert_ne!(opus.base_instructions(), sonnet.base_instructions());
    assert_ne!(sonnet.base_instructions(), haiku.base_instructions());

    // Aggregating routes may decorate the model key; the same Claude model
    // still resolves its pack.
    let decorated = ModelFragments::for_route(
        &RuntimeString::from("claude-opus-5-5[context=1m]@max"),
        Some("cursor"),
        None,
    );
    assert_eq!(decorated.base_instructions(), opus.base_instructions());

    // A legacy Claude id with no current menu entry keeps the generic pack.
    let legacy = ModelFragments::for_model(&RuntimeString::from("claude-sonnet-4-5"), None);
    assert_eq!(legacy.base_instructions(), generic);
}

#[test]
fn decorated_model_key_prefers_exact_binding_and_falls_back_to_identity() {
    let catalog = parse(
        r#"{
          "schemaVersion": 2,
          "revision": "catalog-decorated",
          "fragments": {
            "base-generic": "generic pack",
            "base-canonical": "canonical pack",
            "base-exact": "exact decorated pack"
          },
          "profiles": {
            "klaus/default": {
              "modelMessages": { "base.instructions": { "ref": "base-generic" } }
            },
            "fixture/canonical": {
              "extends": "klaus/default",
              "modelMessages": { "base.instructions": { "ref": "base-canonical" } }
            },
            "fixture/exact": {
              "extends": "klaus/default",
              "modelMessages": { "base.instructions": { "ref": "base-exact" } }
            }
          },
          "bindings": [
            {
              "sourceKind": "cursor", "sourceScope": "*", "modelKey": "mapped-a",
              "variantKey": "standard", "profileRef": "fixture/canonical"
            },
            {
              "sourceKind": "cursor", "sourceScope": "*",
              "modelKey": "mapped-a[context=1m]@max",
              "variantKey": "standard", "profileRef": "fixture/exact"
            }
          ]
        }"#,
    );
    let exact = resolve(
        &catalog,
        &key("cursor", "", "mapped-a[context=1m]@max", "standard"),
        None,
        None,
    );
    assert_eq!(
        text_of(&exact, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("exact decorated pack")
    );
    let canonical = resolve(
        &catalog,
        &key("cursor", "", "mapped-a[context=272k]", "standard"),
        None,
        None,
    );
    assert_eq!(
        text_of(&canonical, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("canonical pack")
    );
    let unbound = resolve(
        &catalog,
        &key("cursor", "", "other[context=272k]@max", "standard"),
        None,
        None,
    );
    assert_eq!(
        text_of(&unbound, SLOT_BASE_INSTRUCTIONS).as_deref(),
        Some("generic pack")
    );
}

#[test]
fn declared_layer_still_wins_over_a_model_pack() {
    let declared_value = declared(r#"{"multi_agent":{"role":{"root":"declared root override"}}}"#);
    let fragments =
        ModelFragments::for_model(&RuntimeString::from("gpt-6.2-sol"), Some(&declared_value));
    assert_eq!(
        fragments.agent_role(false),
        Some(&RuntimeString::from("declared root override"))
    );
    // The pack's base stays untouched by the declaration.
    assert_eq!(
        fragments
            .resolved()
            .provenance(SLOT_BASE_INSTRUCTIONS)
            .map(|p| p.layer),
        Some(FragmentLayer::ModelProfile)
    );
}
