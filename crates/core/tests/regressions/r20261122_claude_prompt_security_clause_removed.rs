//! Defect: selecting Claude Opus 5.5 or Sonnet 5.5 through the Antigravity
//! model source failed immediately with an upstream `429 RESOURCE_EXHAUSTED`
//! rejection even when account quota was unused.
//! Root cause: bundled Claude base prompt fragments carried a static dual-use
//! security paragraph (`Assist with authorized security testing`, `C2
//! frameworks, credential testing, exploit development`) in `systemInstruction`
//! that triggered deterministic upstream gateway rejection.
//! Fix: remove that paragraph from every bundled Claude base prompt fragment
//! while keeping model identity and section spacing intact.

use klaus_agent_core::context::system_prompt::{build_system_prompt, SystemPromptInput};
use klaus_agent_core::engine::session::resolve_config;
use klaus_agent_core::prompt_catalog::ModelFragments;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use serde_json::json;

const REMOVED_PHRASES: &[&str] = &[
    "Assist with authorized security testing",
    "Dual-use security tools",
    "C2 frameworks, credential testing, exploit development",
];

#[test]
fn bundled_claude_fragments_and_assembled_system_prompts_omit_removed_security_clause() {
    for (source, model) in [
        ("antigravity", "claude-opus-5-5-medium"),
        ("antigravity", "claude-opus-5-5-high"),
        ("antigravity", "claude-sonnet-5-5-medium"),
        ("subscription", "claude-3-5-sonnet"),
        ("subscription", "claude-fable-5"),
        ("subscription", "claude-fable-5-1"),
        ("subscription", "claude-fable-5-2"),
        ("subscription", "claude-opus-4-5"),
        ("subscription", "claude-opus-4-6"),
        ("subscription", "claude-opus-4-7"),
        ("subscription", "claude-opus-4-8"),
        ("subscription", "claude-opus-5"),
        ("subscription", "claude-opus-5-5"),
        ("subscription", "claude-sonnet-3-7"),
        ("subscription", "claude-sonnet-4"),
        ("subscription", "claude-sonnet-4-5"),
        ("subscription", "claude-sonnet-4-6"),
        ("subscription", "claude-sonnet-5"),
        ("subscription", "claude-sonnet-5-5"),
        ("subscription", "claude-haiku-4-5"),
        ("subscription", "claude-haiku-5-5"),
    ] {
        let route_fragments =
            ModelFragments::for_route(&RuntimeString::from(model), Some(source), None);
        let base_text = route_fragments
            .base_instructions()
            .try_to_utf8()
            .expect("utf8 base instructions");
        assert!(
            base_text.starts_with("You are Klaus."),
            "{source}:{model} lost leading identity"
        );
        assert!(
            base_text.contains("# Harness"),
            "{source}:{model} lost # Harness section"
        );
        let head = &base_text[..base_text.len().min(500)];
        assert!(
            !head.contains("\n\n\n"),
            "{source}:{model} left consecutive blank lines at splice site"
        );
        for phrase in REMOVED_PHRASES {
            assert!(
                !base_text.contains(phrase),
                "{source}:{model} base instructions still contain removed phrase: {phrase}"
            );
        }

        let config = resolve_config(json!({
            "model": model,
            "modelSource": {
                "source": source,
                "modelId": model,
                "selection": format!("{source}:{model}"),
            }
        }))
        .expect("resolve config");
        let parts = build_system_prompt(&SystemPromptInput {
            cwd: RuntimePath::from("/workspace/project"),
            config,
            now_ms: Some(1_700_000_000_000),
            ..SystemPromptInput::default()
        });
        let full_prompt: String = parts
            .iter()
            .map(|part| part.text.try_to_utf8().expect("utf8 part"))
            .collect::<Vec<_>>()
            .join("\n\n");
        for phrase in REMOVED_PHRASES {
            assert!(
                !full_prompt.contains(phrase),
                "assembled system prompt for {source}:{model} still contains {phrase}"
            );
        }
    }
}
