//! Built-in workflows the engine ships by default — the workflow analogue of the
//! built-in agents and the built-in research skill. Gated by the same
//! include-defaults switch a host uses for the batteries-included agents: a host
//! that opts out of batteries drops these.
//!
//! `deep-research-strict` is the HIGH-ASSURANCE research tier. The model-driven
//! research SKILL is the everyday tier: the model itself decides coverage and
//! follows prompt instructions to cite — fast, adaptive, but the citation
//! discipline is only as good as the model's compliance. This WORKFLOW instead
//! makes citation integrity CODE-ENFORCED, for output that will be shown to
//! others or treated as fact:
//!
//!   Gate 1 (existence) — researchers return STRUCTURED claims `{text,
//!     sourceUrl, quote}`; code dedupes the sources into a canonical `[N]` list,
//!     and after synthesis code strips any `[N]` in the prose that is not in that
//!     list. A fabricated citation cannot reach the final report — the model
//!     can't emit an id that code didn't mint.
//!   Gate 2 (support) — every claim is independently re-checked by a separate
//!     verifier agent (re-reads the source) before it is allowed into synthesis;
//!     unsupported / unverifiable claims are dropped. This catches the "real URL
//!     but the page doesn't actually say that" failure that existence alone
//!     misses.
//!
//! The result: every citation in the report points to a real source that was
//! independently confirmed to support at least one cited claim. The cost is more
//! agents (one verifier per claim) — the deliberate price of the assurance tier.

use crate::runtime_values::RuntimeString;
use crate::tools::builtins::workflow::WorkflowRef;

/// The orchestration script, shipped as plain JavaScript.
const DEEP_RESEARCH_STRICT_SCRIPT: &str = include_str!("deep_research_strict.js");

/// Built-in workflows shipped with the engine (merged with host workflows).
pub fn builtin_workflows() -> Vec<WorkflowRef> {
    vec![WorkflowRef {
        name: "deep-research-strict".into(),
        description: Some(
            "High-assurance deep research with code-enforced citation integrity (structured sources + per-claim adversarial verification + deterministic citation audit). Heavier than the deep-research skill; use when a citation must never be fabricated or misattributed."
                .into(),
        ),
        script: DEEP_RESEARCH_STRICT_SCRIPT.into(),
    }]
}

/// Merge built-in workflows with host-supplied ones. A host workflow WINS on a
/// name collision (so a host can override/replace `deep-research-strict`).
/// Order: [non-overridden built-ins, then host]. When `include_builtins` is
/// false, only the host workflows are returned.
pub fn resolve_workflows(
    host_workflows: Option<Vec<WorkflowRef>>,
    include_builtins: bool,
) -> Vec<WorkflowRef> {
    let host = host_workflows.unwrap_or_default();
    if !include_builtins {
        return host;
    }
    let host_names: std::collections::HashSet<RuntimeString> =
        host.iter().map(|w| w.name.clone()).collect();
    let mut out: Vec<WorkflowRef> = builtin_workflows()
        .into_iter()
        .filter(|b| !host_names.contains(&b.name))
        .collect();
    out.extend(host);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::flow::is_flow_source;
    use crate::engine::js_runtime::NodeJsWorkflowScriptRunner;
    use crate::engine::workflow::meta::parse_workflow;
    use crate::engine::workflow::runtime::{run_workflow, WorkflowRunInput, WorkflowRuntimeDeps};
    use crate::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeObject};
    use crate::types::abort::AbortSignal;
    use crate::types::errors::TerminalReason;
    use crate::types::events::{EngineEvent, ThreadItem};
    use crate::types::tool::{EngineEventStream, SpawnSubAgent, SpawnSubAgentOptions};
    use std::sync::Arc;

    macro_rules! exact_json {
        (null) => {
            RuntimeJson::Null
        };
        ([ $($value:tt),* $(,)? ]) => {
            RuntimeJson::Array(vec![$(exact_json!($value)),*])
        };
        ({ $($key:literal : $value:tt),* $(,)? }) => {{
            let mut object = RuntimeObject::new();
            $(object.insert($key, exact_json!($value));)*
            RuntimeJson::Object(object)
        }};
        ($value:expr) => {
            runtime_codec::encode_value(&$value)
                .expect("exact built-in workflow JSON expression must encode")
        };
    }

    #[test]
    fn builtin_script_is_a_valid_javascript_workflow_and_names_match() {
        let built = builtin_workflows();
        assert_eq!(built.len(), 1);
        assert_eq!(built[0].name, "deep-research-strict");
        assert!(!is_flow_source(&built[0].script));
        assert!(built[0].script.starts_with("export const meta"));
        let parsed = parse_workflow(&built[0].script).unwrap();
        assert_eq!(parsed.meta.name, "deep-research-strict");
        assert_eq!(parsed.meta.phases.as_ref().unwrap().len(), 4);
    }

    fn strict_research_spawn() -> SpawnSubAgent {
        Arc::new(|opts: SpawnSubAgentOptions| {
            let prompt = opts.prompt;
            let text = if prompt.contains("Break this research question") {
                runtime_json::stringify(&exact_json!({ "subquestions": ["alpha", "beta"] }))
            } else if prompt.contains("Research this question") && prompt.contains("alpha") {
                runtime_json::stringify(&exact_json!({
                    "claims": [
                        {
                            "text": "supported alpha fact",
                            "sourceUrl": "https://example.com/report/#section",
                            "sourceTitle": "Example Report",
                            "quote": "alpha support"
                        },
                        {
                            "text": "unsupported alpha fact",
                            "sourceUrl": "https://example.com/other",
                            "sourceTitle": "Other",
                            "quote": "weak"
                        }
                    ]
                }))
            } else if prompt.contains("Research this question") && prompt.contains("beta") {
                runtime_json::stringify(&exact_json!({
                    "claims": [
                        {
                            "text": "supported beta fact",
                            "sourceUrl": "https://example.com/report/",
                            "sourceTitle": "Example Report",
                            "quote": "beta support"
                        }
                    ]
                }))
            } else if prompt.contains("Decide whether the SOURCE actually supports the CLAIM")
                && prompt.contains("unsupported alpha fact")
            {
                runtime_json::stringify(&exact_json!({
                    "supported": false,
                    "reason": "source does not support it"
                }))
            } else if prompt.contains("Decide whether the SOURCE actually supports the CLAIM") {
                runtime_json::stringify(&exact_json!({
                    "supported": true,
                    "reason": "source supports it"
                }))
            } else if prompt.contains("Write a clear, conclusion-first report") {
                RuntimeString::from(
                    "The answer rests on supported facts [1]. This fabricated citation should vanish [99].",
                )
            } else {
                panic!("unexpected prompt: {prompt:?}");
            };
            let events = vec![
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        phase: None,
                        id: "m".into(),
                        text,
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "bw".into(),
                    usage: None,
                    stop_reason: TerminalReason::Completed,
                },
            ];
            Box::pin(futures::stream::iter(events)) as EngineEventStream
        })
    }

    #[tokio::test]
    async fn deep_research_strict_enforces_verification_and_citation_audit() {
        // The bundled deep-research script is raw JavaScript, so the runner hands it
        // to a real Node subprocess. Without Node there is nothing to run it in, and
        // the spawn failure would read as a defect in the workflow rather than as a
        // missing interpreter.
        if !crate::testing::node_interpreter_present() {
            println!(
                "skipping the raw-JavaScript workflow check: no `node` on PATH (set \
                 KLAUS_NODE, or install Node.js)"
            );
            return;
        }
        let workflow = builtin_workflows().pop().unwrap();
        let result = run_workflow(
            WorkflowRunInput {
                script: workflow.script,
                args: Some(exact_json!("What happened?")),
                resume_from_run_id: None,
                budget_tokens: None,
            },
            WorkflowRuntimeDeps {
                process_context: crate::runtime_process::test_runtime_process_context(),
                spawn_workflow_agent: strict_research_spawn(),
                journal: None,
                emit: None,
                resolve_workflow: None,
                signal: AbortSignal::new(),
                max_concurrency: Some(8),
            },
            Arc::new(NodeJsWorkflowScriptRunner),
        )
        .await
        .unwrap();

        assert_eq!(result.agents_spawned, 7);
        assert_eq!(
            result.result.get("question"),
            Some(&exact_json!("What happened?"))
        );
        assert_eq!(
            result.result.pointer("/audit"),
            Some(&exact_json!({
                "subQuestions": 2,
                "claimsFound": 3,
                "verified": 2,
                "dropped": 1,
                "sources": 1,
                "strippedCitations": 1
            }))
        );
        assert_eq!(
            result.result.pointer("/sources/0/id"),
            Some(&exact_json!(1))
        );
        assert_eq!(
            result.result.pointer("/sources/0/url"),
            Some(&exact_json!("https://example.com/report/#section"))
        );
        let report = result
            .result
            .get("report")
            .and_then(RuntimeJson::as_string)
            .unwrap();
        assert!(report.contains("[1]"));
        assert!(!report.contains("[99]"));
        assert!(report.contains("Sources:\n[1] Example Report"));
    }

    #[test]
    fn resolve_includes_builtins_by_default() {
        let out = resolve_workflows(None, true);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "deep-research-strict");
    }

    #[test]
    fn resolve_excludes_builtins_when_disabled() {
        let host = vec![WorkflowRef {
            name: "custom".into(),
            description: None,
            script: r#"{"meta":{"name":"custom","description":"d"},"body":[]}"#.into(),
        }];
        let out = resolve_workflows(Some(host), false);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "custom");
    }

    #[test]
    fn host_workflow_overrides_builtin_by_name() {
        let host = vec![WorkflowRef {
            name: "deep-research-strict".into(),
            description: Some("host override".into()),
            script: r#"{"meta":{"name":"deep-research-strict","description":"d"},"body":[]}"#
                .into(),
        }];
        let out = resolve_workflows(Some(host), true);
        // The built-in is filtered out; only the host copy survives.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "deep-research-strict");
        assert_eq!(out[0].description.as_ref(), Some(&"host override".into()));
    }

    #[test]
    fn non_colliding_host_workflows_follow_builtins() {
        let host = vec![WorkflowRef {
            name: "extra".into(),
            description: None,
            script: r#"{"meta":{"name":"extra","description":"d"},"body":[]}"#.into(),
        }];
        let out = resolve_workflows(Some(host), true);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "deep-research-strict");
        assert_eq!(out[1].name, "extra");
    }
}
