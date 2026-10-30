//! Defect: tool guidance contradicted shared workflow and advertised invalid PDF ranges.
//! Root cause: independently authored descriptions retained obsolete behavioral rules.
//! Work item: 20261016 consistent tool instruction contracts.

use klaus_agent_core::provider::behavior::{
    render_tool_specs_for_behavior, resolve_provider_behavior_profile,
};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::tools::builtins::index::{default_builtins, BuiltinsOptions};
use klaus_agent_core::tools::pool::{find_tool, to_tool_spec};
use klaus_agent_core::types::provider::ProviderInfo;

#[test]
fn file_tool_guidance_preserves_meaningful_verification_and_valid_pdf_ranges() {
    let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
    let read = to_tool_spec(find_tool(&builtins, "read").unwrap().as_ref());
    assert!(read.description.contains("PDF"));
    assert!(!read
        .description
        .contains("do not re-read a file you just edited to verify"));
    for provider in ["anthropic", "openai", "unknown"] {
        let profile = resolve_provider_behavior_profile(
            Some(&ProviderInfo {
                name: provider.into(),
                base_url: None,
                env_key: None,
                default_model: "policy-fixture".into(),
            }),
            &"policy-fixture".into(),
        );
        let rendered = render_tool_specs_for_behavior(std::slice::from_ref(&read), &profile);
        let pages = rendered[0]
            .input_schema
            .pointer("/properties/pages/description")
            .and_then(|value| value.as_str())
            .unwrap();
        assert!(pages.contains("bounded page range"));
        assert!(pages.contains("Open-ended ranges are not supported"));
        assert!(!pages.contains("\"3-\""));
    }
    let search = to_tool_spec(find_tool(&builtins, "grep").unwrap().as_ref());
    assert!(!search.description.contains("never run grep or rg"));
    assert!(search.description.contains("shell rg"));
}

#[test]
fn search_and_workflow_tools_defer_delegation_to_the_current_mode() {
    let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
    for name in ["glob", "workflow"] {
        let spec = to_tool_spec(find_tool(&builtins, name).unwrap().as_ref());
        assert!(spec.description.contains("current collaboration mode"));
        assert!(!spec.description.contains("standing authorization"));
        assert!(!spec.description.contains("ask whether to run one"));
    }
}

#[test]
fn scheduled_prompts_preserve_scope_and_notification_intent_without_duplicate_jobs() {
    let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
    let create = to_tool_spec(find_tool(&builtins, "CronCreate").unwrap().as_ref());
    for required in [
        "Inspect CronList",
        "Creation always adds a new job",
        "unless periodic updates were explicitly requested",
        "stay quiet while the state is unchanged",
        "human-readable instructions",
        "session-only",
    ] {
        assert!(
            create.description.contains(required),
            "missing scheduling policy: {required}"
        );
    }
    let prompt = create
        .input_schema
        .pointer("/properties/prompt/description")
        .and_then(|value| value.as_str())
        .unwrap();
    assert!(prompt.contains("authorized task scope"));
    assert!(prompt.contains("notification intent"));
}
