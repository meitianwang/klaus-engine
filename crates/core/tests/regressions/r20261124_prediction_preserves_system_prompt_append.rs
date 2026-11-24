//! Defect: composer suggestions missed the upstream prompt cache on every
//! desktop session even seconds after a warm main turn with >97% cache hit.
//! Root cause: `Session::run_prediction_query` passed `None` for the `append`
//! parameter of `assemble_turn_system_prompt_parts`, dropping the host's
//! per-turn `system_prompt_append` part and mutating the serialized
//! `systemInstruction` prefix on every prediction request.
//! Work item: 20261124 prediction preserves system_prompt_append.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::json;

use super::r20261117_antigravity_gemini_prompt_cache::{
    start_antigravity_gemini_session_with_overrides, GeminiFixtureOverrides, ScriptedGeminiClient,
    ScriptedReply,
};

const START_MS: i64 = 1_800_000_000_000;
const MINUTE_MS: i64 = 60 * 1_000;

fn controllable_clock() -> (Arc<AtomicI64>, klaus_agent_core::engine::deps::NowFn) {
    let clock = Arc::new(AtomicI64::new(START_MS));
    let clock_fn = clock.clone();
    let now: klaus_agent_core::engine::deps::NowFn =
        Arc::new(move || clock_fn.load(Ordering::SeqCst));
    (clock, now)
}

#[tokio::test]
async fn antigravity_prediction_keeps_wire_system_instruction_identical_with_append() {
    let (clock, now) = controllable_clock();
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("First reply."),
        ScriptedReply::Text(r#"{"suggestion":"Verify the fix"}"#),
        ScriptedReply::Text("Second reply."),
        ScriptedReply::Text(r#"{"suggestion":"Push the change"}"#),
    ]);
    let (_dir, session) = start_antigravity_gemini_session_with_overrides(
        client.clone(),
        Vec::new(),
        200_000,
        None,
        None,
        GeminiFixtureOverrides {
            config: Some(json!({ "thinking": true, "effort": "high" })),
            now: Some(now),
            thinking_level: true,
        },
    );

    let host_append =
        RuntimeString::from("### Subagent task titles\nUse concise localized task titles.");
    let first = session
        .run(
            Input::Text("Analyze the cache metrics.".into()),
            TurnOptions {
                system_prompt_append: Some(host_append.clone()),
                ..TurnOptions::default()
            },
        )
        .await;
    assert!(first.error.is_none(), "{:?}", first.error);
    let first_source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("completed first turn")
        .try_to_utf8()
        .unwrap();

    clock.fetch_add(MINUTE_MS, Ordering::SeqCst);
    let first_suggestion = session
        .predict_message_with_signal(
            "Suggest the next message.".into(),
            first_source,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        first_suggestion,
        Some(RuntimeString::from("Verify the fix"))
    );

    // A later turn without `system_prompt_append` clears the retained value so
    // the subsequent prediction still matches that turn's wire prefix.
    clock.fetch_add(MINUTE_MS, Ordering::SeqCst);
    let second = session
        .run(
            Input::Text("Now run without host append.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(second.error.is_none(), "{:?}", second.error);
    let second_source = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("completed second turn")
        .try_to_utf8()
        .unwrap();

    clock.fetch_add(MINUTE_MS, Ordering::SeqCst);
    let second_suggestion = session
        .predict_message_with_signal(
            "Suggest the next message.".into(),
            second_source,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        second_suggestion,
        Some(RuntimeString::from("Push the change"))
    );

    let calls = client.taken();
    assert_eq!(calls.len(), 4);
    assert_eq!(
        calls[1].wire["systemInstruction"], calls[0].wire["systemInstruction"],
        "prediction must send the exact same wire systemInstruction as the main turn"
    );
    let first_pred_system = calls[1].wire["systemInstruction"].to_string();
    assert!(
        first_pred_system.contains("Use concise localized task titles."),
        "prediction wire systemInstruction missing host append: {first_pred_system}"
    );

    assert_eq!(
        calls[3].wire["systemInstruction"], calls[2].wire["systemInstruction"],
        "prediction must match the latest turn once system_prompt_append is cleared"
    );
    let second_pred_system = calls[3].wire["systemInstruction"].to_string();
    assert!(
        !second_pred_system.contains("Use concise localized task titles."),
        "prediction wire systemInstruction retained stale append: {second_pred_system}"
    );

    session.dispose().await;
}
