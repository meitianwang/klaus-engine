//! Defect: incomplete streamed tool arguments became empty calls and repeated until halted.
//! Root cause: finalization cached a JSON parse fallback as a successful parse.
//! Work item: 20261016 streamed argument validation and bounded output recovery.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::tools::builtins::{
    ls::create_ls_tool, read::create_read_tool, write::create_write_tool,
};
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelRequest, ModelStopReason, ModelStreamEvent,
};
use serde_json::json;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Call {
    tool: &'static str,
    arguments: String,
    stop: ModelStopReason,
    at_cap: bool,
}

fn write(arguments: &str, stop: ModelStopReason, at_cap: bool) -> Call {
    Call {
        tool: "write",
        arguments: arguments.into(),
        stop,
        at_cap,
    }
}

struct Run {
    directory: Directory,
    requests: Vec<ModelRequest>,
    events: Vec<EngineEvent>,
}

async fn run(calls: Vec<Call>) -> Run {
    run_with_budget(calls, 1000).await
}

async fn run_with_budget(calls: Vec<Call>, output_budget: u64) -> Run {
    let directory = Directory(
        std::env::temp_dir().join(format!("klaus-tool-arguments-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&directory.0).unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("fixture-key".into()),
        capabilities: Some(ModelCapabilities {
            tools: Some(true),
            max_output_tokens: Some(16_384),
            ..Default::default()
        }),
        ..Default::default()
    }));
    options.include_default_agents = Some(false);
    options.tools = Some(vec![
        create_write_tool(),
        create_read_tool(),
        create_ls_tool(),
    ]);
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captured = requests.clone();
    let script = Mutex::new(VecDeque::from(calls));
    options.deps.call_model = Some(Box::new(move |request, _| {
        let mut requests = captured.lock().unwrap();
        let id = format!("call-{}", requests.len());
        let call = script.lock().unwrap().pop_front();
        let mut events = Vec::new();
        let (stop_reason, output_tokens) = if let Some(call) = call {
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: id.clone().into(),
                    name: call.tool.into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: id.clone().into(),
                    partial_json: call.arguments.into(),
                },
                // A provider finalizes the tool even when its JSON is incomplete.
                ModelStreamEvent::ToolUseStop { id: id.into() },
            ]);
            (
                call.stop,
                if call.at_cap {
                    request.max_output_tokens
                } else {
                    20
                },
            )
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Finished.".into(),
            });
            (ModelStopReason::EndTurn, 10)
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: Usage {
                input_tokens: 100,
                output_tokens,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        requests.push(request);
        Box::pin(futures::stream::iter(events))
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.0.to_string_lossy().into_owned().into()),
        config: Some(json!({
            "model":"argument-fixture", "prewarm":false, "goal":false,
            "thinking":false, "autoMemory":false, "autoDream":false,
            "memoryRecall":false, "checkpointing":false,
            "speculativeCompaction":false, "earlyMicrocompact":false,
            "idleMicrocompact":false, "compactionSelfCheck":false,
            "loopDetection":true, "maxOutputTokens":output_budget
        })),
        ..Default::default()
    });
    let events = tokio::time::timeout(
        Duration::from_secs(15),
        session
            .run_streamed(
                Input::Text("Write the complete document in bounded chunks.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await;
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let requests = requests.lock().unwrap().clone();
    Run {
        directory,
        requests,
        events: events.expect("argument recovery must be bounded"),
    }
}

fn result<'a>(request: &'a ModelRequest, id: &str) -> &'a ToolResultMessage {
    request
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result),
            _ => None,
        })
        .expect("the next request must contain the failed call's paired result")
}

fn text(result: &ToolResultMessage) -> String {
    result
        .content
        .iter()
        .filter_map(|block| match block {
            OutputBlock::Text(text) => {
                Some(String::from_utf8_lossy(&text.text.to_node_utf8()).into_owned())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_completed(run: &Run) {
    assert!(
        !run.events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{:?}",
        run.events
    );
    assert!(run
        .events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
}

fn started(run: &Run, id: &str) -> bool {
    run.events.iter().any(|event| matches!(event,
        EngineEvent::ItemStarted { item: ThreadItem::ToolCall { id: actual, .. }, .. } if actual == id))
}

#[tokio::test]
async fn malformed_finalized_write_is_dropped_without_touching_disk() {
    let run = run(vec![write(
        r#"{"path":"partial.txt","content":"prefix"#,
        ModelStopReason::ToolUse,
        false,
    )])
    .await;
    let failed = result(&run.requests[1], "call-0");
    assert!(failed.is_error);
    assert!(
        text(failed).contains("arguments arrived incomplete"),
        "{}",
        text(failed)
    );
    assert!(!run.directory.0.join("partial.txt").exists());
    assert!(!started(&run, "call-0"));
    assert_eq!(
        run.requests[0].max_output_tokens,
        run.requests[1].max_output_tokens
    );
    assert_completed(&run);
}

#[tokio::test]
async fn finalized_write_at_confirmed_limit_is_salvaged_then_appended() {
    let run = run(vec![
        write(r#"{"path":"document.txt","content":"first\nsecond"#, ModelStopReason::MaxTokens, true),
        write(r#"{"path":"document.txt","content":"\nlast","append":true,"continuation_context":"first\nsecond"}"#, ModelStopReason::ToolUse, false),
    ]).await;
    let recovered = result(&run.requests[1], "call-0");
    assert!(!recovered.is_error, "{}", text(recovered));
    assert!(text(recovered).contains("append: true"));
    assert!(text(recovered).contains("first\nsecond"));
    assert!(!result(&run.requests[2], "call-1").is_error);
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("document.txt")).unwrap(),
        "first\nsecond\nlast"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn misreported_limit_retries_safely_then_requests_smaller_output() {
    let run = run(vec![
        write(
            r#"{"path":"partial.txt","content":"prefix"#,
            ModelStopReason::ToolUse,
            true,
        ),
        write(
            r#"{"path":"partial.txt","content":"prefix"#,
            ModelStopReason::ToolUse,
            true,
        ),
        write(
            r#"{"path":"complete.txt","content":"small complete chunk"}"#,
            ModelStopReason::ToolUse,
            false,
        ),
        write(
            r#"{"path":"complete.txt","content":"\nremaining chunk","append":true,"continuation_context":"small complete chunk"}"#,
            ModelStopReason::ToolUse,
            false,
        ),
    ])
    .await;
    assert!(
        !run.directory.0.join("partial.txt").exists(),
        "usage alone must never authorize a partial write"
    );
    assert!(!started(&run, "call-0"));
    assert!(!started(&run, "call-1"));
    assert!(text(result(&run.requests[1], "call-0")).contains("increase the token limit"));
    assert!(text(result(&run.requests[2], "call-1")).contains("reduce the output content by half"));
    assert!(text(result(&run.requests[2], "call-1")).contains("without omitting content"));
    assert!(text(result(&run.requests[2], "call-1")).contains("append: true"));
    assert_eq!(run.requests[1].max_output_tokens, 2000);
    assert_eq!(run.requests[2].max_output_tokens, 2000);
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("complete.txt")).unwrap(),
        "small complete chunk\nremaining chunk"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn provider_ceiling_still_requests_smaller_output_after_budget_retry() {
    let run = run_with_budget(
        vec![
            write(
                r#"{"path":"partial.txt","content":"prefix"#,
                ModelStopReason::ToolUse,
                true,
            ),
            write(
                r#"{"path":"partial.txt","content":"prefix"#,
                ModelStopReason::ToolUse,
                true,
            ),
        ],
        16_384,
    )
    .await;
    assert!(!run.directory.0.join("partial.txt").exists());
    assert_eq!(run.requests[1].max_output_tokens, 16_384);
    assert!(text(result(&run.requests[2], "call-1")).contains("reduce the output content by half"));
    assert_completed(&run);
}

#[tokio::test]
async fn truncated_paths_and_write_options_are_never_salvaged() {
    let run = run(vec![
        write(
            r#"{"content":"complete","path":"wrong-prefix"#,
            ModelStopReason::MaxTokens,
            true,
        ),
        write(
            r#"{"path":"unsafe.txt","content":"complete","append":tr"#,
            ModelStopReason::MaxTokens,
            true,
        ),
    ])
    .await;
    assert!(!run.directory.0.join("wrong-prefix").exists());
    assert!(!run.directory.0.join("unsafe.txt").exists());
    for index in 0..2 {
        let id = format!("call-{index}");
        assert!(result(&run.requests[index + 1], &id).is_error);
        assert!(!started(&run, &id));
    }
    assert_completed(&run);
}

#[tokio::test]
async fn truncated_content_without_a_write_mode_never_overwrites_an_existing_file() {
    let run = run(vec![
        write(
            r#"{"path":"document.txt","content":"first"}"#,
            ModelStopReason::ToolUse,
            false,
        ),
        write(
            r#"{"path":"document.txt","content":"second"#,
            ModelStopReason::MaxTokens,
            true,
        ),
    ])
    .await;
    assert!(result(&run.requests[2], "call-1").is_error);
    assert!(!started(&run, "call-1"));
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("document.txt")).unwrap(),
        "first"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn explicit_write_modes_before_truncated_content_are_preserved() {
    let run = run(vec![
        write(r#"{"path":"document.txt","content":"first"}"#, ModelStopReason::ToolUse, false),
        write(r#"{"path":"document.txt","append":true,"continuation_context":"first","content":"\nsecond"#, ModelStopReason::MaxTokens, true),
        write(r#"{"path":"document.txt","append":false,"content":"replacement"#, ModelStopReason::MaxTokens, true),
    ]).await;
    let appended = result(&run.requests[2], "call-1");
    assert!(!appended.is_error, "{}", text(appended));
    assert!(text(appended).contains("first\nsecond"));
    assert!(!result(&run.requests[3], "call-2").is_error);
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("document.txt")).unwrap(),
        "replacement"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn complete_write_at_budget_is_not_retried_or_split() {
    let run = run(vec![write(
        r#"{"path":"complete.txt","content":"complete"}"#,
        ModelStopReason::ToolUse,
        true,
    )])
    .await;
    assert!(!result(&run.requests[1], "call-0").is_error);
    assert_eq!(
        run.requests[0].max_output_tokens,
        run.requests[1].max_output_tokens
    );
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("complete.txt")).unwrap(),
        "complete"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn empty_write_payloads_at_the_limit_retry_without_dispatching() {
    let run = run(vec![
        write("{}", ModelStopReason::ToolUse, true),
        write("", ModelStopReason::ToolUse, true),
        write(
            r#"{"path":"recovered.txt","content":"complete document"}"#,
            ModelStopReason::ToolUse,
            false,
        ),
    ])
    .await;
    assert!(!started(&run, "call-0"));
    assert!(!started(&run, "call-1"));
    assert!(text(result(&run.requests[2], "call-1")).contains("reduce the output content by half"));
    assert_eq!(
        std::fs::read_to_string(run.directory.0.join("recovered.txt")).unwrap(),
        "complete document"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn invalid_escape_is_not_written_even_at_output_limit() {
    let run = run(vec![write(
        r#"{"path":"unsafe.txt","content":"prefix\q"#,
        ModelStopReason::MaxTokens,
        true,
    )])
    .await;
    assert!(result(&run.requests[1], "call-0").is_error);
    assert!(!run.directory.0.join("unsafe.txt").exists());
    assert!(!started(&run, "call-0"));
    assert_eq!(run.requests[1].max_output_tokens, 2000);
    assert_completed(&run);
}

#[tokio::test]
async fn legitimate_empty_arguments_remain_valid_for_no_argument_tools() {
    let run = run(["", "{}"]
        .into_iter()
        .map(|arguments| Call {
            tool: "ls",
            arguments: arguments.into(),
            stop: ModelStopReason::ToolUse,
            at_cap: false,
        })
        .collect())
    .await;
    for index in 0..2 {
        let id = format!("call-{index}");
        assert!(!result(&run.requests[index + 1], &id).is_error);
        assert!(started(&run, &id));
    }
    assert_completed(&run);
}

#[tokio::test]
async fn malformed_read_only_call_never_prestarts() {
    let run = run(vec![Call {
        tool: "read",
        arguments: r#"{"path":"incomplete"#.into(),
        stop: ModelStopReason::ToolUse,
        at_cap: false,
    }])
    .await;
    assert!(text(result(&run.requests[1], "call-0")).contains("arguments arrived incomplete"));
    assert!(
        !started(&run, "call-0"),
        "a malformed read must not execute before argument validation"
    );
    assert_completed(&run);
}

#[tokio::test]
async fn different_malformed_prefixes_do_not_count_as_identical_empty_calls() {
    let run = run((0..6)
        .map(|index| {
            write(
                &format!(r#"{{"path":"partial-{index}.txt","content":"prefix"#),
                ModelStopReason::ToolUse,
                false,
            )
        })
        .collect())
    .await;
    assert_eq!(run.requests.len(), 7);
    for index in 0..6 {
        assert!(!started(&run, &format!("call-{index}")));
    }
    assert_completed(&run);
}

#[tokio::test]
async fn identical_malformed_prefixes_still_have_bounded_loop_protection() {
    let run = run((0..8)
        .map(|_| {
            write(
                r#"{"path":"partial.txt","content":"prefix"#,
                ModelStopReason::ToolUse,
                false,
            )
        })
        .collect())
    .await;
    assert!(
        run.requests.len() <= 6,
        "identical malformed requests must remain bounded"
    );
    assert!(run.events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("stuck in a loop"))));
    assert!(!run.directory.0.join("partial.txt").exists());
}
