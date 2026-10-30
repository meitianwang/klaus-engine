//! Defect: synthetic search access defaults could override sandbox-derived access.
//! Root cause: default serialization erased whether access was explicitly configured.
//! Work item: 20261018 independent search access resolution and provenance.

use klaus_agent_core::config_governance::{ConfigGovernanceLayer, ConfigGovernanceSource};
use klaus_agent_core::config_store::resolve_view;
use klaus_agent_core::engine::session::{
    config_layer_from_value, resolve_config, resolve_config_default, resolve_config_layers,
};
use klaus_agent_core::schemas::config::{WebSearchAccess, WebSearchConfig, WebSearchMode};
use serde_json::{json, Value};

fn layer(source: ConfigGovernanceSource, value: Value) -> ConfigGovernanceLayer {
    config_layer_from_value(source, value).unwrap()
}

#[test]
fn omitted_access_uses_canonical_defaults_and_tracks_independent_sandbox_modes() {
    assert_eq!(WebSearchConfig::default().access, WebSearchAccess::Live);
    assert_eq!(
        resolve_config(json!({})).unwrap().web_search.access,
        WebSearchAccess::Cached
    );
    assert_eq!(resolve_config_default(), resolve_config(json!({})).unwrap());
    for (sandbox, expected) in [
        ("read-only", WebSearchAccess::Cached),
        ("workspace-write", WebSearchAccess::Cached),
        ("danger-full-access", WebSearchAccess::Live),
    ] {
        for search in [
            json!({}),
            json!({"source":"codex"}),
            json!({"mode":"native"}),
        ] {
            let config =
                resolve_config(json!({"sandbox_mode":sandbox,"webSearch":search})).unwrap();
            assert_eq!(
                config.web_search.access, expected,
                "sandbox={sandbox}, search={search}"
            );
        }
    }
}

#[test]
fn explicit_access_overrides_every_sandbox_and_reenables_legacy_disabled_modes() {
    for sandbox in ["read-only", "workspace-write", "danger-full-access"] {
        for access in [
            WebSearchAccess::Disabled,
            WebSearchAccess::Cached,
            WebSearchAccess::Indexed,
            WebSearchAccess::Live,
        ] {
            for mode in ["off", "disabled", "cached", "indexed", "live"] {
                for source in [None, Some("codex")] {
                    let mut search = json!({"mode":mode,"access":access.as_str()});
                    if let Some(source) = source {
                        search["source"] = json!(source);
                    }
                    let config =
                        resolve_config(json!({"sandbox_mode":sandbox,"webSearch":search})).unwrap();
                    assert_eq!(
                        config.web_search.access, access,
                        "sandbox={sandbox}, search={search}"
                    );
                    assert_eq!(config.web_search.source.as_deref(), source);
                    if access != WebSearchAccess::Disabled {
                        assert!(
                            !matches!(
                                config.web_search.mode,
                                WebSearchMode::Off | WebSearchMode::Disabled
                            ),
                            "an explicit enabled access must expose search: {search}, mode={:?}",
                            config.web_search.mode
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn explicit_legacy_freshness_modes_keep_their_intent_without_a_new_access_value() {
    for sandbox in ["read-only", "workspace-write", "danger-full-access"] {
        for (mode, expected) in [
            ("off", WebSearchAccess::Disabled),
            ("disabled", WebSearchAccess::Disabled),
            ("cached", WebSearchAccess::Cached),
            ("indexed", WebSearchAccess::Indexed),
            ("live", WebSearchAccess::Live),
        ] {
            let config =
                resolve_config(json!({"sandbox_mode":sandbox,"webSearch":{"mode":mode}})).unwrap();
            assert_eq!(
                config.web_search.access, expected,
                "sandbox={sandbox}, legacy mode={mode}"
            );
        }
    }
}

#[test]
fn raw_config_view_leaves_derived_access_absent_and_reports_actual_sources() {
    let defaults = resolve_view(&[]).unwrap();
    assert!(defaults
        .resolved
        .get("webSearch")
        .unwrap()
        .get("access")
        .is_none());
    assert_eq!(defaults.sources.get("webSearch"), Some(&json!("defaults")));

    let layers = vec![
        layer(
            ConfigGovernanceSource::User,
            json!({"sandbox_mode":"danger-full-access","webSearch":{"source":"codex"}}),
        ),
        layer(
            ConfigGovernanceSource::Project,
            json!({"sandbox_mode":"read-only"}),
        ),
        layer(
            ConfigGovernanceSource::SessionFlags,
            json!({"reasoningSummary":"detailed"}),
        ),
    ];
    let view = resolve_view(&layers).unwrap();
    assert_eq!(
        view.resolved.get("webSearch"),
        Some(&json!({"source":"codex"}))
    );
    assert_eq!(view.sources.get("webSearch"), Some(&json!("user")));
    assert_eq!(view.sources.get("sandbox_mode"), Some(&json!("project")));
    assert_eq!(
        view.sources.get("reasoningSummary"),
        Some(&json!("sessionFlags"))
    );
    let config = resolve_config_layers(layers).unwrap();
    assert_eq!(config.web_search.access, WebSearchAccess::Cached);
    assert_eq!(config.web_search.source.as_deref(), Some("codex"));
}

#[test]
fn lower_layer_explicit_access_survives_a_higher_layer_sandbox_change() {
    for (access, expected) in [
        ("live", WebSearchAccess::Live),
        ("cached", WebSearchAccess::Cached),
        ("indexed", WebSearchAccess::Indexed),
        ("disabled", WebSearchAccess::Disabled),
    ] {
        let layers = vec![
            layer(
                ConfigGovernanceSource::User,
                json!({"webSearch":{"mode":"off","access":access}}),
            ),
            layer(
                ConfigGovernanceSource::Project,
                json!({"sandbox_mode":"workspace-write"}),
            ),
        ];
        let view = resolve_view(&layers).unwrap();
        assert_eq!(view.sources.get("webSearch"), Some(&json!("user")));
        assert_eq!(view.resolved["webSearch"]["access"], json!(access));
        let config = resolve_config_layers(layers).unwrap();
        assert_eq!(config.web_search.access, expected);
        if expected != WebSearchAccess::Disabled {
            assert!(matches!(
                config.web_search.mode,
                WebSearchMode::Client | WebSearchMode::Native
            ));
        }
    }
}
