//! Built-in sub-agent roles every engine ships by default — a generic worker and
//! a couple of read-only specialists. Without these, a fresh engine would have no
//! agents until the host registers some, so `task` could only spawn an
//! undifferentiated clone of the parent. The generic `general-purpose` worker is
//! also the role a type-less `task` delegation resolves to, so the "just dispatch
//! a worker" path produces an agent with its own system prompt rather than a bare
//! parent clone.
//!
//! Prompts here are ORIGINAL to this engine, written from each role's purpose.

use std::collections::HashSet;
use std::sync::LazyLock;

use crate::engine::agent_definitions::AgentDefinition;
use crate::runtime_values::RuntimeString;
use crate::schemas::effort::{EffortLevel, EffortValue};

/// The role name a type-less `task` delegation resolves to.
pub const GENERAL_PURPOSE_AGENT: &str = "general-purpose";

/// Planning assignments cannot restart after completion. Exploration supports
/// explicit follow-up turns under the same frozen role and creation context.
pub(crate) fn is_one_shot_builtin_role(identity: Option<&RuntimeString>) -> bool {
    identity.is_some_and(|identity| identity == "klaus:builtin:plan:1")
}

/// Tools a read-only role (Explore / Plan) may NOT use: the file-mutating tools,
/// and the sub-agent spawner. The shell stays available,
/// since searching a repo (ripgrep / find) needs it; the prompt forbids using it
/// to mutate.
const READ_ONLY_DENY: [&str; 5] = ["write", "edit", "multi_edit", "apply_patch", "task"];

/// The read-only denylist as an owned tool-name list.
fn read_only_deny() -> Vec<RuntimeString> {
    READ_ONLY_DENY
        .iter()
        .map(|s| RuntimeString::from(*s))
        .collect()
}

/// The built-in agent roles, in their contract order.
pub static BUILTIN_AGENTS: LazyLock<Vec<AgentDefinition>> = LazyLock::new(build_builtin_agents);

fn build_builtin_agents() -> Vec<AgentDefinition> {
    vec![
        AgentDefinition {
            name: GENERAL_PURPOSE_AGENT.into(),
            description:
                "General-purpose worker for self-contained, multi-step subtasks — investigation, implementation, or verification. Specify the owned modules or responsibility, deliverables, relevant context, and dependencies or handoffs with other tasks. The default role when a task is delegated without a specific subagent_type. The caller must select a model tier or concrete model for every new task."
                    .into(),
            // `tools` omitted → inherits the parent's full set.
            prompt: Some(
                [
                    "You are a general-purpose sub-agent. A parent agent has handed you a self-contained subtask. Complete it end to end using the tools actually available to you.",
                    "",
                    "- Resolve discoverable facts autonomously. State reasonable assumptions; report a blocker when a consequential missing decision cannot be established from the assigned context.",
                    "- When the relevant implementation is unknown, start with a broad search and narrow to the responsible files and call sites. If the first search is inconclusive, vary the terms, naming conventions, and locations rather than repeating the same query.",
                    "- Own the assigned modules or responsibility and deliverables. Respect work owned by other agents, adapt to concurrent changes, and report interface dependencies or handoffs to the parent instead of silently expanding your assignment.",
                    "- Validate the result with the smallest relevant checks. Distinguish checks that passed, checks that failed, and checks you did not run; never claim verification from an intended command alone.",
                    "- Your final message must stand alone: state what you completed, the result and supporting evidence, what you verified and its outcome, and any unfinished work, uncertainty, or handoff the parent needs. Match the report's detail to the assignment.",
                    "- Be thorough but focused: gather what the task needs, then stop. Do not expand scope beyond what was asked.",
                ]
                .join("\n")
                .into(),
            ),
            ..Default::default()
        },
        AgentDefinition {
            name: "Explore".into(),
            builtin_identity: Some("klaus:builtin:explore:1".into()),
            description:
                "Focused code-location agent for finding files, symbols, definitions, and call sites. Specify a targeted lookup, cross-directory search, or exhaustive inventory within an explicit scope. It reads relevant excerpts and reports evidence and coverage; use general-purpose for complex reviews, completeness audits, or open-ended root-cause analysis. Explicit follow-ups can reuse its findings and original search context."
                    .into(),
            disallowed_tools: Some(read_only_deny()),
            prompt: Some(
                [
                    "You are a read-only exploration sub-agent. Your job is to LOCATE and REPORT — not to change anything.",
                    "",
                    "- Match the requested breadth: a targeted lookup finds a specific definition or reference; a cross-directory search checks relevant roots and naming variants; an exhaustive inventory enumerates matches within the explicitly assigned scope. If breadth is unspecified, choose what the question needs and state your coverage.",
                    "- Find the files, symbols, definitions, and call sites relevant to the request. Search freely with grep/glob/ls and read only the excerpts you need — avoid reading whole files when a span will do.",
                    "- Search file patterns to locate candidates, search contents to identify symbols or references, then read enough surrounding context to support each finding. Batch independent searches or reads when possible, and change strategy when a query is inconclusive.",
                    "- You MUST NOT modify the workspace. A shell is available for searching (ripgrep, find) only; never use it to write, move, or delete files.",
                    "- Complex reviews, completeness audits, and open-ended root-cause analysis belong to general-purpose. Return useful locations and explain the remaining investigation rather than presenting a partial search as a completed audit.",
                    "- Report findings with precise file locations and supporting excerpts where useful. State the searched scope, exclusions, truncation, and unresolved questions. A query with no matches is evidence about that query and scope, not proof that something does not exist elsewhere.",
                    "- For an explicit follow-up, reuse prior findings and target the missing references or context. Recheck evidence that may have changed. Keep the original search scope and role constraints; ask the parent to delegate a separate task when the new question requires a different assignment.",
                    "- Finish each assigned lookup or follow-up with a focused report, then stop. Further work requires another explicit direction from the parent.",
                ]
                .join("\n")
                .into(),
            ),
            ..Default::default()
        },
        AgentDefinition {
            name: "Plan".into(),
            builtin_identity: Some("klaus:builtin:plan:1".into()),
            description:
                "Read-only architect agent for a decision-complete implementation plan: behavior and interface changes, material trade-offs, acceptance checks, and an executable sequence where needed. Use when the current collaboration mode permits delegation and separate planning would help the assigned task."
                    .into(),
            disallowed_tools: Some(read_only_deny()),
            // Architecture judgment is the most quality-sensitive delegation:
            // the high tier is the requested quality bar (an unconfigured or
            // unavailable tier resolves to the parent session's main model).
            model: Some("high".into()),
            prompt: Some(
                [
                    "You are a software-architect sub-agent. Given a task, you DESIGN an implementation plan — you do not implement it.",
                    "",
                    "- Investigate the codebase read-only first, so the plan is grounded in how things actually work today rather than how they might.",
                    "- You MUST NOT modify any files. A shell is available for reading and searching only.",
                    "- Return a decision-complete implementation plan: the recommended behavior and interface changes, an executable sequence where needed, acceptance checks, and material assumptions. Name files or symbols only when they prevent ambiguity.",
                    "- Call out risks, unknowns, and anything that needs a decision before coding starts.",
                ]
                .join("\n")
                .into(),
            ),
            ..Default::default()
        },
        AgentDefinition {
            name: "researcher".into(),
            description:
                "Web-research worker: given ONE focused question, runs web searches, reads the most promising pages, and returns a self-contained findings summary with an explicit Sources list (title + URL). The deep-research skill dispatches these in parallel — one per sub-question — so raw page text stays out of the orchestrator context."
                    .into(),
            // Tight allow-list: search + read only. Excludes `task`, so a researcher CANNOT recurse.
            tools: Some(vec![
                "web_search".into(),
                "web_fetch".into(),
                "read".into(),
                "think".into(),
            ]),
            // Research synthesis has a quality bar (cross-checked citations),
            // but not the main loop's: the medium tier is the requested quality
            // bar (an unconfigured or unavailable tier resolves to the parent
            // session's main model).
            model: Some("medium".into()),
            effort: Some(EffortValue::Level(EffortLevel::Medium)),
            prompt: Some(
                [
                    "You are a focused web-research sub-agent. You are given one question. Investigate it and return a self-contained final answer with the relevant evidence; progress and tool activity do not replace that result.",
                    "",
                    "Method:",
                    "- Use targeted queries suited to the question. Research independent facets in parallel when helpful, then refine searches based on the evidence.",
                    "- Use web_fetch to read the FULL text of the most promising results; do not answer from snippets alone when a claim is load-bearing. Prefer primary/authoritative sources.",
                    "- Cross-check any surprising or contested claim against a second independent source before asserting it. If sources conflict, say so rather than silently picking one.",
                    "",
                    "Follow the assignment's explicit output schema or format, including a structured JSON result when requested. Preserve source evidence in the requested fields. The reporting defaults below apply only when the assignment does not specify another format.",
                    "",
                    "Default output (plain text, no preamble):",
                    "1. A direct findings summary answering the question. Cite EVERY fact taken from search inline as [N], where N is the id web_search printed for that result (e.g. \"revenue was $391B[2]\"). Use ONLY ids web_search actually showed you — never invent a citation id.",
                    "2. A \"Sources:\" section — one line per id you cited, as \"[N] Title — URL\".",
                    "Never fabricate a URL, quote, statistic, or citation id. If you could not establish something, say what is still unknown. Resolve discoverable facts yourself and state uncertainty when an important decision remains unresolved.",
                ]
                .join("\n")
                .into(),
            ),
            ..Default::default()
        },
    ]
}

/// Merge the built-in roles with host-supplied ones. A host definition WINS on a
/// name collision, so `general-purpose`/`Explore`/`Plan` can be customized or
/// replaced. When `include_builtins` is false, only the host agents are returned.
///
/// Order is a contract: `[non-overridden built-ins, then host agents]`. An
/// overridden role keeps the host's position, not the built-in's. This order is
/// what surfaces in the `task` tool's role listing and `agentType` enum, so treat
/// it as stable.
pub fn resolve_agent_definitions(
    user_agents: Option<Vec<AgentDefinition>>,
    include_builtins: bool,
) -> Vec<AgentDefinition> {
    let user = user_agents
        .unwrap_or_default()
        .into_iter()
        .map(|mut definition| {
            definition.builtin_identity = None;
            definition
        })
        .collect::<Vec<_>>();
    if !include_builtins {
        return user;
    }
    let user_names: HashSet<RuntimeString> = user.iter().map(|a| a.name.clone()).collect();
    let mut out: Vec<AgentDefinition> = BUILTIN_AGENTS
        .iter()
        .filter(|b| !user_names.contains(&b.name))
        .cloned()
        .collect();
    out.extend(user);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str) -> AgentDefinition {
        AgentDefinition {
            name: name.into(),
            description: format!("{name} desc").into(),
            ..Default::default()
        }
    }

    #[test]
    fn ships_the_four_default_roles_in_order() {
        let names = BUILTIN_AGENTS
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                RuntimeString::from("general-purpose"),
                RuntimeString::from("Explore"),
                RuntimeString::from("Plan"),
                RuntimeString::from("researcher")
            ]
        );
    }

    #[test]
    fn read_only_roles_deny_the_mutating_tools() {
        let explore = BUILTIN_AGENTS.iter().find(|a| a.name == "Explore").unwrap();
        assert_eq!(
            explore.disallowed_tools,
            Some(vec![
                "write".into(),
                "edit".into(),
                "multi_edit".into(),
                "apply_patch".into(),
                "task".into(),
            ])
        );
    }

    #[test]
    fn researcher_has_a_tight_allowlist() {
        let r = BUILTIN_AGENTS
            .iter()
            .find(|a| a.name == "researcher")
            .unwrap();
        assert_eq!(
            r.tools,
            Some(vec![
                "web_search".into(),
                "web_fetch".into(),
                "read".into(),
                "think".into(),
            ])
        );
        assert_eq!(r.effort, Some(EffortValue::Level(EffortLevel::Medium)));
    }

    #[test]
    fn builtin_roles_keep_inherited_or_explicit_model_selections() {
        // General-purpose and exploration keep the captured parent selection.
        // Other declared role models resolve against the session's tier map.
        let tier_of = |name: &str| {
            BUILTIN_AGENTS
                .iter()
                .find(|a| a.name == name)
                .unwrap()
                .model
                .clone()
        };
        assert_eq!(tier_of("general-purpose"), None);
        assert_eq!(tier_of("Explore"), None);
        assert_eq!(tier_of("Plan"), Some(RuntimeString::from("high")));
        assert_eq!(tier_of("researcher"), Some(RuntimeString::from("medium")));
    }

    #[test]
    fn resolve_merges_builtins_then_host_with_host_winning() {
        let out = resolve_agent_definitions(Some(vec![def("custom")]), true);
        let names = out
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        // Non-overridden built-ins first, then host agents.
        assert_eq!(
            names,
            vec![
                RuntimeString::from("general-purpose"),
                RuntimeString::from("Explore"),
                RuntimeString::from("Plan"),
                RuntimeString::from("researcher"),
                RuntimeString::from("custom")
            ]
        );
    }

    #[test]
    fn resolve_lets_a_host_override_a_builtin_keeping_the_host_position() {
        let mut override_plan = def("Plan");
        override_plan.description = "my plan".into();
        let out = resolve_agent_definitions(Some(vec![def("extra"), override_plan]), true);
        let names = out
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        // Built-in `Plan` is filtered out; the host's `Plan` arrives at its host position.
        assert_eq!(
            names,
            vec![
                RuntimeString::from("general-purpose"),
                RuntimeString::from("Explore"),
                RuntimeString::from("researcher"),
                RuntimeString::from("extra"),
                RuntimeString::from("Plan")
            ]
        );
        assert_eq!(
            out.iter().find(|a| a.name == "Plan").unwrap().description,
            "my plan"
        );
    }

    #[test]
    fn resolve_without_builtins_returns_only_host_agents() {
        let out = resolve_agent_definitions(Some(vec![def("only")]), false);
        let names = out
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![RuntimeString::from("only")]);

        // None + no builtins → empty.
        assert!(resolve_agent_definitions(None, false).is_empty());
    }
}
