//! Defect: invoked skill receipts lost expanded arguments and the exact fork prompt.
//! Root cause: execution recorded source paths without the content actually dispatched.
//! Work item: 20261020 preserve expanded inline and delegated skill receipts.

use klaus_agent_core::extensions::skills::loader::{Skill, SkillContext};
use klaus_agent_core::extensions::skills::skill_tool::create_skill_tool;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::tool::{ForegroundSubAgentRun, SkillContextMod};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn skill_receipts_capture_the_expanded_inline_body_and_the_exact_fork_prompt() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut context =
        super::r20261015_browser_diagnostics_and_targets::make_ctx("/tmp", process.clone());
    let receipts = Arc::new(Mutex::new(Vec::<SkillContextMod>::new()));
    let observed = receipts.clone();
    context.apply_skill_context = Some(Box::new(move |change| {
        observed.lock().unwrap().push(change)
    }));
    let shell_calls = Arc::new(AtomicUsize::new(0));
    let observed = shell_calls.clone();
    context.run_bash = Some(Arc::new(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok("captured-shell-output".to_owned()) })
    }));
    let prompt = "Run $ARGUMENTS at ${KLAUS_SKILL_DIR}: !`printf ignored`";
    let inline = create_skill_tool(move || {
        vec![Skill {
            name: "check".into(),
            path: "/skills/check/SKILL.md".into(),
            body: prompt.into(),
            ..Default::default()
        }]
    });
    let input =
        || runtime_json::parse(&r#"{"name":"check","args":"expanded-argument"}"#.into()).unwrap();
    let output = inline.call(input(), &context, None).await.unwrap();
    assert_ne!(output.serializable.is_error, Some(true));
    let expanded =
        output
            .serializable
            .model_content
            .iter()
            .fold(RuntimeString::default(), |body, block| match block {
                OutputBlock::Text(text) => body.concat(&text.text),
                _ => body,
            });
    assert!(expanded.contains("expanded-argument"));
    assert!(expanded.contains("/skills/check"));
    assert!(expanded.contains("captured-shell-output"));
    assert!(!expanded.contains("!`"));
    assert_eq!(
        receipts.lock().unwrap()[0].invoked_skill_content.as_ref(),
        Some(&expanded)
    );
    assert_eq!(
        receipts.lock().unwrap()[0].invoked_skill_path.as_ref(),
        Some(&RuntimeString::from("/skills/check/SKILL.md"))
    );
    assert_eq!(
        shell_calls.load(Ordering::SeqCst),
        1,
        "recording the expanded receipt must not execute the shell again"
    );

    let dispatched = Arc::new(Mutex::new(Vec::new()));
    let observed = dispatched.clone();
    context.spawn_sub_agent = Some(Arc::new(move |options| {
        observed.lock().unwrap().push(options.prompt);
        Box::pin(std::future::ready(Ok(ForegroundSubAgentRun {
            agent_id: "child".into(),
            events: Box::pin(futures::stream::iter(vec![EngineEvent::TurnCompleted {
                turn_id: "child-turn".into(),
                usage: None,
                stop_reason: TerminalReason::Completed,
            }])),
        })))
    }));
    let fork = create_skill_tool(move || {
        vec![Skill {
            name: "check".into(),
            path: "/skills/check/SKILL.md".into(),
            body: prompt.into(),
            context: Some(SkillContext::Fork),
            ..Default::default()
        }]
    });
    assert_ne!(
        fork.call(input(), &context, None)
            .await
            .unwrap()
            .serializable
            .is_error,
        Some(true)
    );
    assert_eq!(
        receipts.lock().unwrap()[1].invoked_skill_content.as_ref(),
        dispatched.lock().unwrap().first()
    );
    assert_eq!(shell_calls.load(Ordering::SeqCst), 2);
    assert_eq!(SkillContextMod::default().invoked_skill_content, None);
    process.shutdown().await.unwrap();
}
