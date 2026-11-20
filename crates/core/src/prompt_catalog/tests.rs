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
    PromptCatalog::parse(&RuntimeString::from(text)).expect("fixture catalog must be valid")
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

    let model = RuntimeString::from("gpt-5.6-sol");
    let fragments = ModelFragments::for_model(&model, None);
    // Required slots resolve from the engine defaults.
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
