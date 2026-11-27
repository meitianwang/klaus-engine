//! Defect: Gemini 4 sessions (especially `antigravity:gemini-4-carbon-high`
//! after cross-account thought-block stripping) emitted fragmented per-step
//! chatter and spilled spoken inner monologue into visible assistant text.
//! Root cause: Gemini base/commentary prompt fragments mandated visible
//! progress messages between routine tool calls, lacked a guard keeping the
//! thinking channel active when prior turns contain no thinking blocks, and
//! omitted explicit `model/gemini-4` bindings for the 14 tiered Gemini 4 IDs.
//! Fix: stay silent between routine tool calls, explicitly require continuing
//! to use the thinking channel and never thinking out loud in visible text
//! even when prior turns lack thinking blocks, and bind all 14 tiered Gemini 4
//! model IDs (`-low`/`-medium`/`-high`/`-xhigh`) to `model/gemini-4`.

use klaus_agent_core::context::system_prompt::{build_system_prompt, SystemPromptInput};
use klaus_agent_core::engine::session::resolve_config;
use klaus_agent_core::prompt_catalog::ModelFragments;
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use serde_json::json;

const TIERED_GEMINI_4_MODELS: &[&str] = &[
    "gemini-4-carbon-low",
    "gemini-4-carbon-medium",
    "gemini-4-carbon-high",
    "gemini-4-argon-low",
    "gemini-4-argon-medium",
    "gemini-4-argon-high",
    "gemini-4-hydrogen-low",
    "gemini-4-hydrogen-medium",
    "gemini-4-hydrogen-high",
    "gemini-4-hydrogen-xhigh",
    "gemini-4-neon-low",
    "gemini-4-neon-medium",
    "gemini-4-neon-high",
    "gemini-4-neon-xhigh",
];

#[test]
fn tiered_gemini4_routes_resolve_gemini4_pack_and_forbid_visible_self_talk() {
    for model in TIERED_GEMINI_4_MODELS {
        let route_fragments =
            ModelFragments::for_route(&RuntimeString::from(*model), Some("antigravity"), None);
        let base_text = route_fragments
            .base_instructions()
            .try_to_utf8()
            .expect("utf8 base instructions");
        assert!(
            base_text
                .contains("always respond in the language of the user (defaulting to English)"),
            "{model} did not resolve to model/gemini-4 base instructions"
        );
        assert!(
            !base_text.contains("You share progress updates in messages between tool calls."),
            "{model} base instructions still mandate per-tool progress updates"
        );

        let config = resolve_config(json!({
            "model": model,
            "modelSource": {
                "source": "antigravity",
                "modelId": model,
                "selection": format!("antigravity:{model}"),
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
        assert!(
            full_prompt.contains(
                "Continue using your thinking channel even if earlier assistant turns in the conversation contain no thinking blocks."
            ),
            "assembled prompt for {model} lacks post-switch thinking continuity guard"
        );
        assert!(
            full_prompt
                .contains("Even on steps where you emit no thinking block, never think out loud in visible text"),
            "assembled prompt for {model} lacks visible self-talk prohibition"
        );
        assert!(
            full_prompt.contains("After that, stay silent between routine tool calls"),
            "assembled prompt for {model} lacks routine tool-call silence rule"
        );
        assert!(
            !full_prompt.contains("You share progress updates in messages between tool calls."),
            "assembled prompt for {model} still contains per-step progress obligation"
        );
    }
}
