//! Defect: every side-task send failed with an orphaned inherited tool call.
//! Root cause: the context slice ended at the last assistant, before its tool results.
//! Work item 20261004: inherit the latest completed parent turn with paired tools.

use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::types::provider::{ModelRequest, ModelStopReason, ModelStreamEvent};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn context(id: &str) -> Value {
    json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":id,"startedAtMs":1700000000100_i64}})
}

fn user(id: &str) -> Value {
    json!({"kind":"message","message":{"type":"user","uuid":id,"content":[{"type":"text","text":"parent prompt"}]}})
}

fn assistant(id: &str, tools: bool) -> Value {
    let mut content = vec![json!({"type":"text","text":"parent answer"})];
    if tools {
        content.push(
            json!({"type":"tool_use","id":"parent-call","name":"bash","input":{"command":"true"}}),
        );
    }
    json!({"kind":"message","message":{"type":"assistant","uuid":id,"content":content,"model":"m"}})
}

fn result() -> Value {
    json!({"kind":"message","message":{"type":"tool_result","uuid":"parent-result","toolUseId":"parent-call","content":[{"type":"text","text":"actual parent output"}],"isError":false}})
}

fn completed(turn: &str, final_message: &str) -> Value {
    json!({"kind":"event","event":"turn_lifecycle","turnId":turn,"payload":{"status":"completed","finalMessageId":final_message,"startedAtMs":1700000000100_i64,"finishedAtMs":1700000000200_i64,"durationMs":100}})
}

fn ids(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match message {
            Message::User(message) => &message.uuid,
            Message::Assistant(message) => &message.uuid,
            Message::ToolResult(message) => &message.uuid,
            Message::System(message) => &message.uuid,
            Message::CompactBoundary(message) => &message.uuid,
        })
        .map(|id| id.try_to_utf8().unwrap())
        .collect()
}

async fn check(rows: Vec<Value>, expected: &[&str]) {
    check_request(rows, expected, "side question").await;
}

async fn check_request(rows: Vec<Value>, expected: &[&str], submitted_text: &str) {
    let root = std::env::temp_dir().join(format!(
        "klaus-side-task-context-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut all = vec![
        json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"parent","createdAt":1700000000000_i64,"cwd":root,"model":"m","role":"main"}}),
    ];
    all.extend(rows);
    for row in &all {
        assert!(parse_rollout_line(&row.to_string()).is_some(), "{row}");
    }
    let path = root.join("1700000000000-parent.jsonl");
    let source = all
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&path, &source).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.config = Some(
        json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}}),
    );
    options.deps.call_model = Some(Box::new({
        let requests = requests.clone();
        move |request, _| {
            requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart { model: "m".into() },
                ModelStreamEvent::TextDelta {
                    text: "side answer".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        }
    }));
    let agent = Agent::new(options);
    let side = agent
        .create_side_task(store, "parent", StartSessionOptions::default())
        .await
        .unwrap();
    assert_eq!(
        ids(&side.history()),
        expected,
        "inherited context must use a complete turn"
    );
    assert!(
        side.visible_history().is_empty(),
        "parent prefix stays hidden"
    );
    side.run(Input::Text(submitted_text.into()), TurnOptions::default())
        .await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "first side message reaches the provider");
        let history = &requests[0].messages;
        assert_eq!(ids(&history[..expected.len()]), expected);
        assert!(
            history
                .iter()
                .any(|message| matches!(message, Message::User(user)
                if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
                    if text.text == submitted_text)))),
            "the side provider must receive the explicit user payload unchanged"
        );
        for message in history {
            if let Message::Assistant(assistant) = message {
                for block in &assistant.content {
                    if let ContentBlock::ToolUse(call) = block {
                        assert!(history.iter().any(|message| matches!(message, Message::ToolResult(result) if result.tool_use_id == call.id)), "orphaned call {}", call.id.try_to_utf8().unwrap());
                    }
                }
            }
        }
    }
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        source,
        "parent transcript is unchanged"
    );
    assert_eq!(
        side.visible_history().len(),
        2,
        "side question and answer remain visible"
    );
    side.dispose().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn side_task_skips_active_tools_and_commentary() {
    for tail in [
        vec![assistant("active-tool", true)],
        vec![assistant("active-tool", true), result()],
        vec![assistant("active-commentary", false)],
    ] {
        let mut rows = vec![
            context("complete"),
            user("complete-user"),
            assistant("complete-answer", false),
            completed("complete", "complete-answer"),
            context("active"),
            user("active-user"),
        ];
        rows.extend(tail);
        check(rows, &["complete-user", "complete-answer"]).await;
    }
}

#[tokio::test]
async fn side_task_keeps_completed_tool_results_after_the_final_message() {
    check(
        vec![
            context("complete"),
            user("complete-user"),
            assistant("final-with-tool", true),
            completed("complete", "final-with-tool"),
            result(),
        ],
        &["complete-user", "final-with-tool", "parent-result"],
    )
    .await;
}

#[tokio::test]
async fn side_task_with_only_an_active_parent_has_no_inherited_tools() {
    check(
        vec![
            context("active"),
            user("active-user"),
            assistant("active-tool", true),
        ],
        &[],
    )
    .await;
}

#[tokio::test]
async fn side_task_receives_selected_streaming_text_before_parent_completion() {
    // A selected live delta need not exist in the persisted parent history.
    // It belongs to the side task's new user message, not its inherited prefix.
    let selected_request = "# Selected text:\n\n## Selection 1\nCurrent streamed sentence, not yet persisted in parent history.\n\n## My request:\n⟦Selection 1⟧Explain this sentence.\n";
    check_request(
        vec![
            context("active"),
            user("active-user"),
            assistant("active-tool", true),
        ],
        &[],
        selected_request,
    )
    .await;
}

#[tokio::test]
async fn legacy_side_task_requires_a_tool_free_final_answer() {
    check(
        vec![
            user("complete-user"),
            assistant("complete-answer", false),
            user("active-user"),
            assistant("active-tool", true),
            result(),
        ],
        &["complete-user", "complete-answer"],
    )
    .await;
    check(
        vec![
            user("complete-user"),
            assistant("tool-call", true),
            result(),
            assistant("complete-answer", false),
        ],
        &[
            "complete-user",
            "tool-call",
            "parent-result",
            "complete-answer",
        ],
    )
    .await;
}
