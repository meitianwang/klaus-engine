//! Defect: replayed tool artifacts could overwrite other output, and retired
//! replacement decisions outlived the history that owned them.
//! Root cause: spill writes replaced existing files and replay imported every id.
//! Work item: 01a1588f preserve complete tool output and retained decision ownership.

use std::collections::{HashMap, HashSet};

use klaus_agent_core::context::tool_result_budget::{
    decide, settle, ReplacementState, BUDGET_REPLACEMENT_TAG,
};
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::schemas::content::{
    ContentBlock, ImageBlock, OutputBlock, TextBlock, ToolResultBlock,
};
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, UserMessage};
use klaus_agent_core::tools::result_storage::{
    maybe_spill_result, normalize_successful_tool_output, output_blocks_text, plan_spill_result,
    SpillOptions,
};

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn content(value: impl Into<RuntimeString>) -> Vec<OutputBlock> {
    vec![OutputBlock::Text(text(value))]
}

fn options(dir: &std::path::Path, id: &str) -> SpillOptions {
    SpillOptions {
        max_chars: 2_000.0,
        dir: RuntimePath::from(dir.to_str().unwrap()),
        id: id.into(),
    }
}

fn result(id: &str, body: impl Into<RuntimeString>) -> Message {
    Message::ToolResult(ToolResultMessage {
        uuid: format!("result-{id}").into(),
        tool_use_id: id.into(),
        content: content(body),
        is_error: false,
        output: None,
        extra_user_blocks: None,
        tool_kind: None,
        native_tool_output: None,
    })
}

#[test]
fn spill_previews_use_a_late_newline_without_splitting_utf16_accounting() {
    let body = format!("{}\n{}", "😀".repeat(600), "b".repeat(2_000));
    let plan = plan_spill_result(
        &content(body),
        &options(std::path::Path::new("artifacts"), "a"),
    )
    .unwrap();
    assert_eq!(plan.full_chars, 3_201);
    assert_eq!(plan.preview, RuntimeString::from("😀".repeat(600)));

    let halfway = format!("{}\n{}", "x".repeat(1_000), "y".repeat(2_000));
    let plan = plan_spill_result(
        &content(halfway.clone()),
        &options(std::path::Path::new("artifacts"), "b"),
    )
    .unwrap();
    assert_eq!(
        plan.preview,
        RuntimeString::from(halfway).slice_utf16(..2_000)
    );

    let mut small = options(std::path::Path::new("artifacts"), "c");
    small.max_chars = 7.0;
    let plan = plan_spill_result(&content("😀😀\nabcdef"), &small).unwrap();
    assert_eq!(plan.preview, "😀😀");
}

#[tokio::test]
async fn existing_spill_artifacts_are_reused_only_for_the_same_complete_bytes() {
    let dir = super::context_fixture::tempdir().unwrap();
    let original = content("a".repeat(5_000));
    let first = maybe_spill_result(original.clone(), options(dir.path(), "same"), |p, _, _| {
        p.clone()
    })
    .await;
    assert!(first.spilled_path.is_some());
    let path = dir.path().join("same.txt");
    assert_eq!(std::fs::read(&path).unwrap(), vec![b'a'; 5_000]);

    let reused =
        maybe_spill_result(original, options(dir.path(), "same"), |p, _, _| p.clone()).await;
    assert_eq!(reused.spilled_path, first.spilled_path);

    let different = content("b".repeat(5_000));
    let collision =
        maybe_spill_result(different.clone(), options(dir.path(), "same"), |_, _, _| {
            panic!("different existing bytes cannot be advertised as this output")
        })
        .await;
    assert_eq!(collision.content, different);
    assert!(collision.spilled_path.is_none());
    assert_eq!(std::fs::read(&path).unwrap(), vec![b'a'; 5_000]);

    let occupied = dir.path().join("directory.txt");
    std::fs::create_dir(&occupied).unwrap();
    let preserved = content("c".repeat(5_000));
    let collision = maybe_spill_result(
        preserved.clone(),
        options(dir.path(), "directory"),
        |_, _, _| panic!("an existing directory is not a persisted result"),
    )
    .await;
    assert_eq!(collision.content, preserved);
    assert!(collision.spilled_path.is_none());
    assert!(occupied.is_dir());
}

#[tokio::test]
async fn simultaneous_different_outputs_never_share_an_incorrect_artifact_reference() {
    let dir = super::context_fixture::tempdir().unwrap();
    let left = content("l".repeat(8_000));
    let right = content("r".repeat(8_000));
    let (left_result, right_result) = tokio::join!(
        maybe_spill_result(left.clone(), options(dir.path(), "race"), |p, _, _| p
            .clone()),
        maybe_spill_result(right.clone(), options(dir.path(), "race"), |p, _, _| p
            .clone()),
    );
    assert_eq!(
        usize::from(left_result.spilled_path.is_some())
            + usize::from(right_result.spilled_path.is_some()),
        1,
    );
    let persisted = std::fs::read(dir.path().join("race.txt")).unwrap();
    for (full, observed) in [(left, left_result), (right, right_result)] {
        if observed.spilled_path.is_some() {
            assert_eq!(persisted, output_blocks_text(&full).to_node_utf8());
        } else {
            assert_eq!(observed.content, full);
        }
    }
}

#[test]
fn successful_empty_text_is_explicit_while_media_and_real_text_are_preserved() {
    for empty in [Vec::new(), content(""), content(" \t\n\u{feff}\u{3000}")] {
        let output = normalize_successful_tool_output(empty, &"inspect".into());
        assert_eq!(
            output_blocks_text(&output),
            "inspect completed without output."
        );
    }
    let visible = content("\u{0085}");
    assert_eq!(
        normalize_successful_tool_output(visible.clone(), &"inspect".into()),
        visible,
    );
    let media = vec![OutputBlock::Image(ImageBlock {
        mime_type: "image/png".into(),
        base64: "AAAA".into(),
        detail: None,
        source_path: None,
    })];
    assert_eq!(
        normalize_successful_tool_output(media.clone(), &"inspect".into()),
        media,
    );
}

#[test]
fn resumed_replacements_keep_retained_markers_and_nested_results_but_drop_retired_ids() {
    let retained: RuntimeString = format!("{BUDGET_REPLACEMENT_TAG} retained.txt").into();
    let nested: RuntimeString = format!("{BUDGET_REPLACEMENT_TAG} nested.txt").into();
    let history = vec![
        result("kept", retained.clone()),
        Message::User(UserMessage {
            uuid: "nested-carrier".into(),
            content: vec![ContentBlock::ToolResult(ToolResultBlock {
                tool_use_id: "nested".into(),
                content: content(nested.clone()),
                is_error: Some(false),
            })],
            is_meta: None,
            question_answer: None,
        }),
    ];
    let state = ReplacementState::reconstruct(
        &history,
        vec![
            ("retired".into(), "retired body".into()),
            ("nested".into(), nested.clone()),
            ("kept".into(), retained.clone()),
        ],
    );
    assert_eq!(
        state.records(),
        vec![("kept".into(), retained.clone()), ("nested".into(), nested)],
    );
    let raw_retained = vec![result("kept", "freshly reconstructed full output")];
    let decision = decide(
        &raw_retained,
        &state,
        1,
        &HashSet::new(),
        &HashMap::new(),
        None,
    );
    assert_eq!(decision.reapply, vec![("kept".into(), retained)]);
    assert!(decision.to_persist.is_empty());

    let mut fork = state.clone();
    settle(
        &mut fork,
        &[result("child", "large child output")],
        &[("child".into(), "child artifact".into())],
    );
    assert_eq!(state.records().len(), 2);
    assert_eq!(fork.records().len(), 3);
}
