use super::RetainedToolTasks;
use crate::types::events::ToolDomainEvent;

#[path = "subagent_mailbox_tests.rs"]
mod subagent_mailbox_tests;

fn immediate_message_observer(
    observer: impl Fn(Message) + Send + Sync + 'static,
) -> super::OnMessage {
    let observer = Arc::new(observer);
    Box::new(move |message| {
        let observer = observer.clone();
        Box::pin(async move {
            observer(message);
            Ok(())
        })
    })
}

fn immediate_compaction_observer(
    observer: impl Fn(super::CompactionRecord) + Send + Sync + 'static,
) -> super::OnCompaction {
    let observer = Arc::new(observer);
    Box::new(move |record| {
        let observer = observer.clone();
        Box::pin(async move {
            observer(record);
            Ok(())
        })
    })
}

fn expected_collaboration_part(key: &str) -> crate::types::provider::SystemPromptPart {
    let fixture = crate::runtime_values::runtime_json::parse(&RuntimeString::from(include_str!(
        "../../../tests/fixtures/collaboration-policy-vectors.json"
    )))
    .expect("collaboration policy vectors must parse");
    crate::runtime_values::runtime_codec::decode_value(
        fixture.get(key).expect("pinned collaboration policy part"),
    )
    .expect("pinned collaboration policy part must decode")
}

#[tokio::test]
async fn inference_queue_freezes_identity_before_the_next_step_and_drain() {
    use crate::engine::async_queue::AsyncQueue;
    use crate::types::events::InferenceIdentity;

    let queue = super::EventQueue::new(Arc::new(AsyncQueue::new()));
    let identity = |revision, source: &str, model: &str| InferenceIdentity {
        revision,
        change_id: Some(format!("change-{revision}").into()),
        model: model.into(),
        source: Some(source.into()),
        tier: Some("high".into()),
        selection: Some("explicit".into()),
    };
    let a = identity(1, "byo", "model-a");
    let b = identity(2, "official", "model-b");
    queue.bind(Some(a.clone()));
    queue.push(EngineEvent::StreamRequestStart { inference: None });
    queue.push(EngineEvent::TextDelta {
        text: "A".into(),
        message_id: Some("message-a".into()),
        phase: None,
        inference: None,
    });
    queue.bind(Some(b.clone()));
    queue.push(EngineEvent::TextDelta {
        text: "B".into(),
        message_id: Some("message-b".into()),
        phase: None,
        inference: None,
    });
    queue.push(EngineEvent::ItemUpdated {
        item: ThreadItem::AgentMessage {
            id: "message-a".into(),
            text: "A".into(),
            phase: None,
        },
        inference: Some(a.clone()),
    });
    queue.push(EngineEvent::ItemStarted {
        item: ThreadItem::ToolCall {
            id: "model-call".into(),
            tool_name: "test-tool".into(),
            arguments: RuntimeJson::Null,
            result: None,
            error: None,
            status: ItemStatus::InProgress,
        },
        inference: None,
    });
    queue.push(EngineEvent::ItemCompleted {
        item: ThreadItem::CommandExecution {
            id: "tool".into(),
            command: "true".into(),
            aggregated_output: "".into(),
            exit_code: Some(0),
            status: ItemStatus::Completed,
        },
        inference: None,
    });
    queue.push(EngineEvent::TurnCompleted {
        turn_id: "turn".into(),
        usage: None,
        stop_reason: TerminalReason::Completed,
    });
    queue.close();
    assert_eq!(queue.next().await.unwrap().inference(), Some(&a));
    assert_eq!(queue.next().await.unwrap().inference(), Some(&a));
    assert_eq!(queue.next().await.unwrap().inference(), Some(&b));
    assert_eq!(queue.next().await.unwrap().inference(), Some(&a));
    assert_eq!(queue.next().await.unwrap().inference(), Some(&b));
    assert!(queue.next().await.unwrap().inference().is_none());
    assert!(queue.next().await.unwrap().inference().is_none());
    assert!(queue.next().await.is_none());
}

#[tokio::test]
async fn raw_query_fixture_keeps_inference_absent() {
    let deps = deps_with(scripted_call_model(vec![vec![
        ev::message_start(None),
        ev::text("fixture"),
        ev::stop(None, None),
    ]]));
    let events: Vec<_> = query(mk_params(vec![user_msg("hello")], Vec::new()), deps)
        .collect()
        .await;
    assert!(events.iter().all(|event| event.inference().is_none()));
}

use super::build_tool_catalog;
use super::compaction::inject_running_agent_reminder;
use super::compaction::inject_surfaced_tools_reminder;
use super::is_valid_runtime_json;
use super::join_runtime_strings;
use super::permission_denial_message;
use super::prepare_model_request;
use super::query_message_uuid;
use super::render_failed_tool_result_content;
use super::render_interrupted_tool_result_content;
use super::render_query_envelope_producer;
use super::render_query_owned_system_parts;
use super::render_stop_hook_continuation;
use super::render_structured_output_retry;
use super::render_successful_tool_result_content;
use super::render_token_budget_continuation;
use super::request::build_model_request;
use super::restore_compaction_protected;
use super::result_trim::render_spilled_tool_result;
use super::result_trim::render_trimmed_tool_result;
use super::review_approval_before_prompt;
use super::safe_is_concurrency_safe;
use super::safe_is_read_only;
use super::safe_json_text;
use super::safe_runtime_json_parse;
use super::split_compaction_protected;
use super::successful_result_provider_metadata;
use super::text_out;
use super::text_out_exact;
use super::tool_lifecycle_source_for_tool;
use super::turn::query;
use super::unknown_tool_message;
use super::visible_tool_specs;
use super::OnProviderRequestAdmitted;
use super::ProviderCallAdmission;
use super::ProviderCallAdmissionReceipt;
use super::QueryEnvelopeProducer;
use super::QueryParams;
use super::ResolvedToolCall;
use super::SystemPromptResolvable;
use super::TurnOverride;
use crate::engine::deps::EngineDeps;
use crate::engine::message_queue::MessageQueue;
use crate::engine::message_queue::QueuePriority;
use crate::engine::message_queue::QueuedMessage;
use crate::engine::message_queue::{QueueItemState, QueueSnapshotStatus, QueuedValue};
use crate::hooks::bus::HookBus;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookInput;
use crate::hooks::types::{Hook, HookDecision, HookFn, HookHandler, HookOutcome, HookResult};
use crate::permission::engine::PermissionDecision;
use crate::permission::engine::PermissionEngine;
use crate::permission::engine::StaticPermissionEngine;
use crate::provider::behavior::render_tool_specs_for_behavior;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::provider::behavior::{
    resolve_provider_schema_strategy, ProviderPromptStyle, ProviderSchemaProvider,
};
use crate::provider::observability::ProviderDecisionEventKind;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::runtime_json;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::TotalTokensReminderMode;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::OutputBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::content::ToolUseBlock;
use crate::schemas::effort::EffortValue;
use crate::schemas::message::AssistantMessage;
use crate::schemas::message::CompactBoundaryMessage;
use crate::schemas::message::Message;
use crate::schemas::message::ToolResultMessage;
use crate::schemas::message::UserMessage;
use crate::testing::{ev, scripted_call_model, ScriptedTurns};
use crate::tools::build_tool::text_result;
use crate::tools::builtins::grep::create_grep_tool;
use crate::tools::builtins::swarm_tools::create_send_message_tool;
use crate::tools::builtins::task::{create_task_tool, AgentDefinition, TaskToolOptions};
use crate::tools::builtins::web_fetch::{create_web_fetch_tool, WebFetchToolOptions};
use crate::tools::builtins::web_fetch_redirect::{FetchError, FetchResponse, Fetcher};
use crate::tools::pool::to_tool_spec;
use crate::tools::pool::to_tool_specs;
use crate::tools::result_storage::output_blocks_text;
use crate::types::abort::AbortSignal;
use crate::types::errors::EngineErrorCode;
use crate::types::errors::InputValidationErrorCode;
use crate::types::errors::ProviderError;
use crate::types::errors::ProviderErrorKind;
use crate::types::errors::TerminalReason;
use crate::types::errors::ToolError;
use crate::types::errors::ToolErrorKind;
use crate::types::events::EngineEvent;
use crate::types::events::ItemError;
use crate::types::events::ItemStatus;
use crate::types::events::ThreadItem;
use crate::types::events::ToolLifecyclePhase;
use crate::types::events::ToolLifecycleRecord;
use crate::types::events::ToolLifecycleSourceKind;
use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
use crate::types::json::Json;
use crate::types::json::JsonSchema;
use crate::types::permission::ApprovalHandler;
use crate::types::permission::ApprovalKind;
use crate::types::permission::ApprovalRequest;
use crate::types::permission::ApprovalResponse;
use crate::types::permission::PermissionContext;
use crate::types::permission::PermissionReason;
use crate::types::provider::CacheTtl;
use crate::types::provider::ModelRequest;
use crate::types::provider::ModelStreamEvent;
use crate::types::provider::ProviderInfo;
use crate::types::provider::ProviderToolCallKind;
use crate::types::provider::SystemPromptPart;
use crate::types::provider::ThinkingConfig;
use crate::types::provider::ToolChoice;
use crate::types::provider::ToolInputFormat;
use crate::types::provider::{ModelStopReason, ModelStream};
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use crate::types::tool::BackgroundTaskRegistry;
use crate::types::tool::BackgroundTaskStatus;
use crate::types::tool::ElicitHandler;
use crate::types::tool::ElicitRequest;
use crate::types::tool::ElicitResponse;
use crate::types::tool::Tool;
use crate::types::tool::ToolCatalogEntry;
use crate::types::tool::ToolContext;
use crate::types::tool::ToolExposure;
use crate::types::tool::ValidationResult;
use crate::types::tool::{ToolProgress, ToolResult};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

macro_rules! runtime_json {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

fn exact_test_string(value: &RuntimeString) -> RuntimeString {
    value.clone()
}

fn runtime_json_string_is(value: Option<&RuntimeJson>, expected: &str) -> bool {
    value
        .and_then(RuntimeJson::as_str)
        .is_some_and(|value| value == expected)
}

fn test_provider_profile(model: &str) -> ProviderBehaviorProfile {
    crate::provider::behavior::resolve_provider_behavior_profile(None, &model.into())
}

#[test]
fn safe_runtime_json_parse_recursively_uses_ecmascript_number_semantics() {
    let parsed = safe_runtime_json_parse(&RuntimeString::from(
        r#"{"root":9007199254740993,"nested":[-0,{"wide":9007199254740993}]}"#,
    ));
    assert_eq!(parsed["root"], RuntimeJson::from(9_007_199_254_740_992_u64));
    assert_eq!(
        parsed["nested"][1]["wide"],
        RuntimeJson::from(9_007_199_254_740_992_u64)
    );
    let negative_zero = parsed["nested"][0].as_f64().unwrap();
    assert_eq!(negative_zero, 0.0);
    assert!(negative_zero.is_sign_negative());
}

#[test]
fn structured_output_validation_accepts_lone_surrogate_json() {
    assert!(is_valid_runtime_json(&RuntimeString::from(
        r#"{"high":"\ud800","low":"\udc00"}"#
    )));
}

#[test]
fn safe_json_text_pretty_prints_surrogates_without_internal_markers() {
    let parsed = runtime_json::parse(&RuntimeString::from(
        r#"{"high":"\ud800","nested":["\udc00",{"replacement":"\ufffd"}]}"#,
    ))
    .unwrap();

    let rendered = safe_json_text(&parsed);

    assert_eq!(
        rendered,
        "{\n  \"high\": \"\\ud800\",\n  \"nested\": [\n    \"\\udc00\",\n    {\n      \"replacement\": \"�\"\n    }\n  ]\n}"
    );
    assert!(!rendered.contains("\u{fdd0}"));
}

#[test]
fn query_behavior_envelopes_preserve_unpaired_surrogates_exactly() {
    let profile = test_provider_profile("gpt-5");
    let body = RuntimeString::from_utf16(vec![b'x'.into(), 0xd800, b'y'.into()]);
    let rendered = render_query_envelope_producer(
        QueryEnvelopeProducer::StopHookContinuation,
        body.clone(),
        &profile,
    );
    let expected = RuntimeString::from("# System Reminder\n\n").concat(&body);
    assert_eq!(rendered, expected);
    assert!(rendered.try_to_utf8().is_err());

    let tool_name = RuntimeString::from_utf16(vec![b't'.into(), 0xdc00, b'z'.into()]);
    let rendered = first_text(render_failed_tool_result_content(
        vec![text_out_exact(body.clone())],
        &tool_name,
        &profile,
    ));
    let expected = RuntimeString::from("Tool error from `")
        .concat(&tool_name)
        .concat(&RuntimeString::from("`:\n"))
        .concat(&body);
    assert_eq!(rendered, expected);
    assert!(rendered.try_to_utf8().is_err());
}

struct EnvelopeProducerCase {
    name: &'static str,
    render: Box<dyn Fn(&ProviderBehaviorProfile) -> RuntimeString>,
    anthropic: &'static str,
    openai: &'static str,
    generic: &'static str,
}

fn first_text(content: Vec<OutputBlock>) -> RuntimeString {
    content
        .into_iter()
        .find_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text),
            OutputBlock::Image(_) => None,
        })
        .unwrap_or_default()
}

fn last_text(content: Vec<OutputBlock>) -> RuntimeString {
    content
        .into_iter()
        .rev()
        .find_map(|block| match block {
            OutputBlock::Text(text) => Some(text.text),
            OutputBlock::Image(_) => None,
        })
        .unwrap_or_default()
}

fn projected_system_part(
    name: &str,
    text: impl Into<RuntimeString>,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    render_query_owned_system_parts(
        &[SystemPromptPart {
            name: name.into(),
            text: text.into(),
            cacheable: None,
            cache_scope: None,
        }],
        profile,
    )[0]
    .text
    .clone()
}

fn envelope_producer_cases() -> Vec<EnvelopeProducerCase> {
    const MEMORY: &str = "This memory was saved 2 days ago. It is a point-in-time note, not live state — any file paths, line numbers, or code details it cites may be out of date. Verify against the current code before relying on it as fact.";
    const LOOP: &str = "A potential loop was detected (repetitive actions). Stop and take a step back — confirm you are making real forward progress toward the user's goal. If not, change your approach rather than repeating the same tool calls or text.";
    const TOKEN: &str = "You are at ~45% of the token target for this task (450 of 1000 output tokens). Continue productive work toward the full requested outcome while work remains. The target is a minimum for productive work on the requested task, not a reason to pad the answer or invent unrequested work. If the task is complete, report the outcome with the detail the request requires.";
    const BACKGROUND: &str = "Background task \"inspect logs\" completed. Read its result with task_output (or its output file); do not re-spawn it.";
    const READ: &str = "The file empty.txt exists but is empty — there is nothing to read.";
    const WRITE: &str = crate::tools::builtins::write::WRITE_REPORT_NUDGE;
    const EDITED: &str = "These files changed on disk since you last read them (edited by the user or a tool such as a formatter/linter). The shown content is current and the change was intentional — take it into account and do not revert it unless the user asks, and do not mention this notice to the user. Modified regions, by current line number:\n\nsrc/lib.rs:\n     1\tnew";
    const STRUCTURED: &str = "Your previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences.";

    vec![
        EnvelopeProducerCase {
            name: "hook additional context",
            render: Box::new(|profile| {
                projected_system_part("hook-context", "hook says stay narrow", profile)
            }),
            anthropic: "<system-reminder>\nhook says stay narrow\n</system-reminder>",
            openai: "# System Reminder\n\nhook says stay narrow",
            generic: "hook says stay narrow",
        },
        EnvelopeProducerCase {
            name: "memory freshness",
            render: Box::new(|profile| projected_system_part("reminder", MEMORY, profile)),
            anthropic: "<system-reminder>\nThis memory was saved 2 days ago. It is a point-in-time note, not live state — any file paths, line numbers, or code details it cites may be out of date. Verify against the current code before relying on it as fact.\n</system-reminder>",
            openai: "# System Reminder\n\nThis memory was saved 2 days ago. It is a point-in-time note, not live state — any file paths, line numbers, or code details it cites may be out of date. Verify against the current code before relying on it as fact.",
            generic: MEMORY,
        },
        EnvelopeProducerCase {
            name: "loop reminder",
            render: Box::new(|profile| projected_system_part("loop-recovery", LOOP, profile)),
            anthropic: "<system-reminder>\nA potential loop was detected (repetitive actions). Stop and take a step back — confirm you are making real forward progress toward the user's goal. If not, change your approach rather than repeating the same tool calls or text.\n</system-reminder>",
            openai: "# System Reminder\n\nA potential loop was detected (repetitive actions). Stop and take a step back — confirm you are making real forward progress toward the user's goal. If not, change your approach rather than repeating the same tool calls or text.",
            generic: LOOP,
        },
        EnvelopeProducerCase {
            name: "token-budget continuation",
            render: Box::new(|profile| render_token_budget_continuation(45, 450, 1000, profile)),
            anthropic: "<system-reminder>\nYou are at ~45% of the token target for this task (450 of 1000 output tokens). Continue productive work toward the full requested outcome while work remains. The target is a minimum for productive work on the requested task, not a reason to pad the answer or invent unrequested work. If the task is complete, report the outcome with the detail the request requires.\n</system-reminder>",
            openai: "# System Reminder\n\nYou are at ~45% of the token target for this task (450 of 1000 output tokens). Continue productive work toward the full requested outcome while work remains. The target is a minimum for productive work on the requested task, not a reason to pad the answer or invent unrequested work. If the task is complete, report the outcome with the detail the request requires.",
            generic: TOKEN,
        },
        EnvelopeProducerCase {
            name: "permission denial",
            render: Box::new(|profile| {
                first_text(render_failed_tool_result_content(
                    vec![text_out_exact(permission_denial_message(
                        "blocked by policy",
                    ))],
                    &"Read".into(),
                    profile,
                ))
            }),
            anthropic: "<tool_use_error tool=\"Read\">\nPermission denied: blocked by policy\n</tool_use_error>",
            openai: "Tool error from `Read`:\nPermission denied: blocked by policy",
            generic: "Permission denied: blocked by policy",
        },
        EnvelopeProducerCase {
            name: "unknown tool",
            render: Box::new(|profile| {
                first_text(render_failed_tool_result_content(
                    vec![text_out_exact(unknown_tool_message(&"missing_tool".into()))],
                    &"missing_tool".into(),
                    profile,
                ))
            }),
            anthropic: "<tool_use_error tool=\"missing_tool\">\nUnknown tool: missing_tool\n</tool_use_error>",
            openai: "Tool error from `missing_tool`:\nUnknown tool: missing_tool",
            generic: "Unknown tool: missing_tool",
        },
        EnvelopeProducerCase {
            name: "failing tool",
            render: Box::new(|profile| {
                first_text(render_failed_tool_result_content(
                    vec![text_out("boom failed".to_string())],
                    &"Write".into(),
                    profile,
                ))
            }),
            anthropic: "<tool_use_error tool=\"Write\">\nboom failed\n</tool_use_error>",
            openai: "Tool error from `Write`:\nboom failed",
            generic: "boom failed",
        },
        EnvelopeProducerCase {
            name: "sub-agent report",
            render: Box::new(|profile| {
                first_text(render_successful_tool_result_content(
                    vec![text_out("child answer".to_string())],
                    &"Task".into(),
                    profile,
                ))
            }),
            anthropic: "<subagent_report tool=\"Task\">\nchild answer\n</subagent_report>",
            openai: "# Sub-Agent Report\n\nchild answer",
            generic: "child answer",
        },
        EnvelopeProducerCase {
            name: "background completion",
            render: Box::new(|profile| {
                crate::engine::session::render_background_completion_notice_for_profile(
                    &"inspect logs".into(),
                    "completed",
                    profile,
                )
            }),
            anthropic: "<system-reminder>\nBackground task \"inspect logs\" completed. Read its result with task_output (or its output file); do not re-spawn it.\n</system-reminder>",
            openai: "# System Reminder\n\nBackground task \"inspect logs\" completed. Read its result with task_output (or its output file); do not re-spawn it.",
            generic: BACKGROUND,
        },
        EnvelopeProducerCase {
            name: "command output",
            render: Box::new(|profile| {
                first_text(render_successful_tool_result_content(
                    vec![text_out("stdout text".to_string())],
                    &"Bash".into(),
                    profile,
                ))
            }),
            anthropic: "<command_output tool=\"Bash\">\nstdout text\n</command_output>",
            openai: "# Command Output\n\nstdout text",
            generic: "stdout text",
        },
        EnvelopeProducerCase {
            name: "read advisory",
            render: Box::new(|profile| {
                let result = crate::tools::builtins::read::read_advisory_result(
                    Json::Null,
                    READ.to_string(),
                );
                first_text(render_successful_tool_result_content(
                    result.serializable.model_content,
                    &"Read".into(),
                    profile,
                ))
            }),
            anthropic: "<system-reminder>\nThe file empty.txt exists but is empty — there is nothing to read.\n</system-reminder>",
            openai: "# System Reminder\n\nThe file empty.txt exists but is empty — there is nothing to read.",
            generic: READ,
        },
        EnvelopeProducerCase {
            name: "write nudge",
            render: Box::new(|profile| {
                let result = crate::tools::builtins::write::write_success_result(
                    Json::Null,
                    "Created report.md (10 bytes)".into(),
                    true,
                );
                last_text(render_successful_tool_result_content(
                    result.serializable.model_content,
                    &"Write".into(),
                    profile,
                ))
            }),
            anthropic: "<system-reminder>\nSub-agents should usually return their findings as text in the final response rather than writing them to a report file. The file was written, but prefer including this content directly in your response.\n</system-reminder>",
            openai: "# System Reminder\n\nSub-agents should usually return their findings as text in the final response rather than writing them to a report file. The file was written, but prefer including this content directly in your response.",
            generic: WRITE,
        },
        EnvelopeProducerCase {
            name: "edited-files diff",
            render: Box::new(|profile| {
                let snippet = crate::context::edited_files_diff::diff_snippet(
                    &"old".into(),
                    &"new".into(),
                    crate::context::edited_files_diff::DiffSnippetOptions::default(),
                );
                let body = RuntimeString::from(
                        "These files changed on disk since you last read them (edited by the user or a tool such as a formatter/linter). The shown content is current and the change was intentional — take it into account and do not revert it unless the user asks, and do not mention this notice to the user. Modified regions, by current line number:\n\nsrc/lib.rs:\n",
                    )
                    .concat(&snippet);
                projected_system_part("edited-files", body, profile)
            }),
            anthropic: "<diff_output tool=\"edited_text_file_diff\">\nThese files changed on disk since you last read them (edited by the user or a tool such as a formatter/linter). The shown content is current and the change was intentional — take it into account and do not revert it unless the user asks, and do not mention this notice to the user. Modified regions, by current line number:\n\nsrc/lib.rs:\n     1\tnew\n</diff_output>",
            openai: "# Diff Output\n\nThese files changed on disk since you last read them (edited by the user or a tool such as a formatter/linter). The shown content is current and the change was intentional — take it into account and do not revert it unless the user asks, and do not mention this notice to the user. Modified regions, by current line number:\n\nsrc/lib.rs:\n     1\tnew",
            generic: EDITED,
        },
        EnvelopeProducerCase {
            name: "compact boundary",
            render: Box::new(|profile| match profile.prompt_style {
                ProviderPromptStyle::AnthropicReminder => {
                    crate::provider::anthropic::render_anthropic_compact_boundary(
                        "summary", profile,
                    )
                    .into()
                }
                ProviderPromptStyle::OpenAiDeveloper => RuntimeString::from(
                    crate::provider::openai::render_openai_compact_boundary("summary", profile),
                ),
                ProviderPromptStyle::Generic => RuntimeString::from(
                    crate::provider::gemini::render_gemini_compact_boundary("summary", profile),
                ),
            }),
            anthropic: "<system-reminder>\nEarlier conversation summary:\nsummary\n</system-reminder>",
            openai: "# Earlier Conversation Summary\n\nsummary",
            generic: "[Earlier conversation summarized]\nsummary",
        },
        EnvelopeProducerCase {
            name: "structured-output retry",
            render: Box::new(render_structured_output_retry),
            anthropic: "<system-reminder>\nYour previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences.\n</system-reminder>",
            openai: "# System Reminder\n\nYour previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences.",
            generic: STRUCTURED,
        },
        EnvelopeProducerCase {
            name: "Stop-hook continuation",
            render: Box::new(|profile| {
                render_stop_hook_continuation("Please continue.".into(), profile)
            }),
            anthropic: "<system-reminder>\nPlease continue.\n</system-reminder>",
            openai: "# System Reminder\n\nPlease continue.",
            generic: "Please continue.",
        },
    ]
}

#[test]
fn all_provider_envelope_producers_match_golden_text() {
    let cases = envelope_producer_cases();
    assert_eq!(cases.len(), 16);
    let anthropic = test_provider_profile("claude-sonnet-4-6");
    let openai = test_provider_profile("gpt-5");
    let generic = test_provider_profile("model-x");
    for case in cases {
        assert_eq!((case.render)(&anthropic), case.anthropic, "{}", case.name);
        assert_eq!((case.render)(&openai), case.openai, "{}", case.name);
        assert_eq!((case.render)(&generic), case.generic, "{}", case.name);
    }
}

#[test]
fn spill_and_trim_formatters_match_fixed_ts_text_exactly() {
    let anthropic = test_provider_profile("claude-sonnet-4-6");
    let openai = test_provider_profile("gpt-5");
    let generic = test_provider_profile("model-x");
    let preview = RuntimeString::from("preview");
    let path = RuntimePath::from("/tmp/out");
    let kept = RuntimeString::from("kept");

    assert_eq!(
        render_spilled_tool_result(&preview, 42, &path, &anthropic),
        "preview\n\n<system-reminder>\nTool output was truncated. The full 42 chars were saved to /tmp/out; read it only if the omitted content is needed.\n</system-reminder>"
    );
    assert_eq!(
        render_spilled_tool_result(&preview, 42, &path, &openai),
        "preview\n\n# Truncated Tool Output\n\nThe full output is 42 chars and was saved to /tmp/out. Read that file only if the omitted content is needed for the task."
    );
    assert_eq!(
        render_spilled_tool_result(&preview, 42, &path, &generic),
        "preview\n\n… [output truncated; full 42 chars saved to /tmp/out] …"
    );

    const TRIM_BODY: &str = "Omitted 7 chars from this tool result to fit the context budget. Re-run or narrow the tool call if you need the missing details.";
    assert_eq!(
        render_trimmed_tool_result(&kept, 7, &anthropic),
        format!("kept\n\n<system-reminder>\n{TRIM_BODY}\n</system-reminder>")
    );
    assert_eq!(
        render_trimmed_tool_result(&kept, 7, &openai),
        format!("kept\n\n# Context-Budget Trim\n\n{TRIM_BODY}")
    );
    assert_eq!(
        render_trimmed_tool_result(&kept, 7, &generic),
        "kept\n… [trimmed 7 chars to fit the context budget] …"
    );
}

#[test]
fn interrupted_tool_result_uses_provider_error_envelope() {
    let anthropic = test_provider_profile("claude-sonnet-4-6");
    let openai = test_provider_profile("gpt-5");
    let generic = test_provider_profile("model-x");
    assert_eq!(
        first_text(render_interrupted_tool_result_content(
            &"Read".into(),
            &anthropic
        )),
        "<tool_use_error tool=\"Read\">\nTool execution was interrupted.\n</tool_use_error>"
    );
    assert_eq!(
        first_text(render_interrupted_tool_result_content(
            &"Read".into(),
            &openai
        )),
        "Tool error from `Read`:\nTool execution was interrupted."
    );
    assert_eq!(
        first_text(render_interrupted_tool_result_content(
            &"Read".into(),
            &generic
        )),
        "Tool execution was interrupted."
    );
}

#[test]
fn typed_result_hints_are_consumed_once_and_write_keeps_two_blocks() {
    let cases = [
        (
            test_provider_profile("claude-sonnet-4-6"),
            "<system-reminder>\n",
        ),
        (test_provider_profile("gpt-5"), "# System Reminder\n\n"),
        (
            test_provider_profile("model-x"),
            crate::tools::builtins::write::WRITE_REPORT_NUDGE,
        ),
    ];
    for (profile, expected_nudge_prefix) in cases {
        let result = crate::tools::builtins::write::write_success_result(
            Json::Null,
            "Created report.md (10 bytes)".into(),
            true,
        );
        let rendered = render_successful_tool_result_content(
            result.serializable.model_content,
            &"Write".into(),
            &profile,
        );
        assert_eq!(rendered.len(), 2);
        let OutputBlock::Text(summary) = &rendered[0] else {
            panic!("expected write summary")
        };
        let OutputBlock::Text(nudge) = &rendered[1] else {
            panic!("expected write nudge")
        };
        assert_eq!(summary.text, "Created report.md (10 bytes)");
        assert!(nudge.text.starts_with(expected_nudge_prefix));
        assert!(summary.harness_envelope.is_none());
        assert!(nudge.harness_envelope.is_none());
    }
}

struct NoopFileState;
impl FileStateCache for NoopFileState {
    fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
        None
    }
    fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
    fn has(&self, _path: &RuntimePath) -> bool {
        false
    }
    fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
        Vec::new()
    }
    fn delete(&self, _path: &RuntimePath) {}
    fn clear(&self) {}
    fn clone_box(&self) -> Box<dyn FileStateCache> {
        Box::new(NoopFileState)
    }
}

struct NoopSandbox;
impl SandboxBackend for NoopSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Process
    }
    fn is_available(&self) -> bool {
        false
    }
    fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
        SpawnSpec {
            file: req.argv.first().cloned().unwrap_or_default(),
            args: req.argv.iter().skip(1).cloned().collect(),
            cwd: req.cwd.clone(),
            env: RuntimeEnv::new(),
        }
    }
}

fn make_test_ctx(cwd: &str) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            crate::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: crate::runtime_process::test_runtime_process_context(),
        cwd: cwd.into(),
        signal: AbortSignal::new(),
        file_state: Arc::new(NoopFileState),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(NoopSandbox),
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        emit: std::sync::Arc::new(|_| Box::pin(async {})),
        native_pdf: None,
        vision: None,
        shell_env: None,
        depth: None,
        forward_subagent_text: None,
        spawn_sub_agent: None,
        spawn_background_agent: None,
        spawn_workflow_agent: None,
        surface_tools: None,
        activate_skill_paths: None,
        tool_catalog: None,
        elicit: None,
        plan_file: None,
        apply_skill_context: None,
        run_bash: None,
        mcp_clients: None,
        background_tasks: None,
        task_list_id: None,
        task_plan: None,
        worktree: None,
        workflow_journal: None,
        swarm: None,
        cron: None,
        goal: None,
        goal_generation: None,
        goal_owned_resources: None,
        lsp: None,
        browser: None,
        browser_tool_mapping: None,
        run_hook: None,
        invoke_tool: None,
        advisor: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
    }
}

/// A permission engine that allows everything.
struct AllowAll;
#[async_trait]
impl PermissionEngine for AllowAll {
    async fn evaluate(
        &self,
        _tool: &dyn Tool,
        input: &Json,
        _ctx: &ToolContext,
    ) -> PermissionDecision {
        PermissionDecision::Allow {
            input: input.clone(),
            reason: PermissionReason::Default,
        }
    }
}

struct AskAll;
#[async_trait]
impl PermissionEngine for AskAll {
    async fn evaluate(
        &self,
        _tool: &dyn Tool,
        input: &Json,
        _ctx: &ToolContext,
    ) -> PermissionDecision {
        PermissionDecision::Ask {
            message: "Allow this tool?".to_string(),
            input: input.clone(),
            reason: PermissionReason::Default,
            suggestions: None,
        }
    }
}

struct CountingApprovalHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ApprovalHandler for CountingApprovalHandler {
    async fn approve(&self, _req: ApprovalRequest, _signal: AbortSignal) -> ApprovalResponse {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: None,
        }
    }
}

struct RecordingApprovalHandler {
    requests: Arc<Mutex<Vec<ApprovalRequest>>>,
}

#[async_trait]
impl ApprovalHandler for RecordingApprovalHandler {
    async fn approve(&self, req: ApprovalRequest, _signal: AbortSignal) -> ApprovalResponse {
        self.requests.lock().unwrap().push(req);
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: None,
        }
    }
}

#[derive(Clone)]
enum RecordingPermissionResponse {
    Allow,
    AllowRewrite(Json),
    Ask(Json),
    Deny,
}

struct RecordingPermission {
    seen: Arc<Mutex<Vec<(RuntimeString, Json)>>>,
    response: RecordingPermissionResponse,
}

#[async_trait]
impl PermissionEngine for RecordingPermission {
    async fn evaluate(
        &self,
        tool: &dyn Tool,
        input: &Json,
        _ctx: &ToolContext,
    ) -> PermissionDecision {
        self.seen
            .lock()
            .unwrap()
            .push((tool.name().clone(), input.clone()));
        match &self.response {
            RecordingPermissionResponse::Allow => PermissionDecision::Allow {
                input: input.clone(),
                reason: PermissionReason::Default,
            },
            RecordingPermissionResponse::AllowRewrite(updated) => PermissionDecision::Allow {
                input: updated.clone(),
                reason: PermissionReason::Default,
            },
            RecordingPermissionResponse::Ask(updated) => PermissionDecision::Ask {
                message: "Allow identity tool?".to_string(),
                input: updated.clone(),
                reason: PermissionReason::Default,
                suggestions: None,
            },
            RecordingPermissionResponse::Deny => PermissionDecision::Deny {
                message: "policy denied".to_string(),
                reason: PermissionReason::Default,
            },
        }
    }
}

struct StaticApproval {
    requests: Arc<Mutex<Vec<ApprovalRequest>>>,
    response: ApprovalResponse,
}

#[async_trait]
impl ApprovalHandler for StaticApproval {
    async fn approve(&self, request: ApprovalRequest, _signal: AbortSignal) -> ApprovalResponse {
        self.requests.lock().unwrap().push(request);
        self.response.clone()
    }
}

struct RecordingHook {
    seen: Arc<Mutex<Vec<HookInput>>>,
    outcome: HookOutcome,
}

static PANIC_HOOK_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct DropAction(Option<Box<dyn FnOnce() + Send>>);

impl Drop for DropAction {
    fn drop(&mut self) {
        if let Some(action) = self.0.take() {
            action();
        }
    }
}

struct OneShotBlockingHook {
    calls: AtomicUsize,
}

#[async_trait]
impl HookFn for RecordingHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        self.seen.lock().unwrap().push(input);
        self.outcome.clone()
    }
}

#[async_trait]
impl HookFn for OneShotBlockingHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            HookOutcome {
                decision: Some(HookDecision::Block),
                additional_context: Some("continue from hook".into()),
                ..Default::default()
            }
        } else {
            HookOutcome::default()
        }
    }
}

fn recording_hook(
    event: HookEvent,
    seen: Arc<Mutex<Vec<HookInput>>>,
    outcome: HookOutcome,
) -> Hook {
    Hook {
        event,
        matcher: None,
        handler: HookHandler::Fn(Arc::new(RecordingHook { seen, outcome })),
        is_async: false,
    }
}

struct StaticWebFetchFetcher;

#[async_trait]
impl Fetcher for StaticWebFetchFetcher {
    async fn fetch(
        &self,
        _url: &str,
        _headers: &[(String, String)],
        _manual_redirect: bool,
        _signal: &AbortSignal,
    ) -> Result<FetchResponse, FetchError> {
        Ok(FetchResponse::ready(
            200,
            "",
            vec![("content-type".to_string(), "text/plain".to_string())],
            "ok",
        ))
    }
}

fn approval_review_request() -> ApprovalRequest {
    ApprovalRequest {
        id: "approval-1".into(),
        kind: ApprovalKind::Tool,
        tool_name: "bash".into(),
        input: runtime_json!({ "command": "rm -rf target/tmp" }),
        message: "Allow bash?".into(),
        suggestions: None,
    }
}

struct StaticApprovalReviewer {
    response: crate::types::permission::ApprovalReviewResponse,
}

#[async_trait]
impl crate::types::permission::ApprovalReviewer for StaticApprovalReviewer {
    async fn review(
        &self,
        _req: ApprovalRequest,
        _signal: AbortSignal,
    ) -> crate::types::permission::ApprovalReviewResponse {
        self.response.clone()
    }
}

struct HangingApprovalReviewer;

#[async_trait]
impl crate::types::permission::ApprovalReviewer for HangingApprovalReviewer {
    async fn review(
        &self,
        _req: ApprovalRequest,
        _signal: AbortSignal,
    ) -> crate::types::permission::ApprovalReviewResponse {
        tokio::time::sleep(Duration::from_millis(50)).await;
        crate::types::permission::ApprovalReviewResponse::Allow
    }
}

struct PanickingApprovalReviewer;

#[async_trait]
impl crate::types::permission::ApprovalReviewer for PanickingApprovalReviewer {
    async fn review(
        &self,
        _req: ApprovalRequest,
        _signal: AbortSignal,
    ) -> crate::types::permission::ApprovalReviewResponse {
        panic!("approval reviewer panic");
    }
}

/// The POSIX rendering the engine reports for a host path, for expectations about paths it
/// hands the model.
fn model_facing_path(path: &std::path::Path) -> String {
    let path = path.to_str().expect("test path is a scalar host path");
    if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path.to_string()
    }
}

#[tokio::test]
async fn approval_reviewer_allows_approval_prompt_to_continue() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(StaticApprovalReviewer {
            response: crate::types::permission::ApprovalReviewResponse::Allow,
        });

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(100),
    )
    .await;

    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn approval_reviewer_denies_before_approval_prompt() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(StaticApprovalReviewer {
            response: crate::types::permission::ApprovalReviewResponse::Deny {
                message: Some("dangerous command".into()),
            },
        });

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(100),
    )
    .await;

    assert_eq!(result, Err(RuntimeString::from("dangerous command")));
}

#[tokio::test]
async fn approval_reviewer_unavailable_fails_closed() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(StaticApprovalReviewer {
            response: crate::types::permission::ApprovalReviewResponse::Unavailable {
                message: Some("review service unavailable".into()),
            },
        });

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(100),
    )
    .await;

    assert_eq!(
        result,
        Err(RuntimeString::from("review service unavailable"))
    );
}

#[tokio::test]
async fn approval_reviewer_timeout_fails_closed() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(HangingApprovalReviewer);

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(1),
    )
    .await;

    assert_eq!(
        result,
        Err(RuntimeString::from("Approval reviewer timed out; denied."))
    );
}

#[tokio::test]
async fn approval_reviewer_error_fails_closed() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(StaticApprovalReviewer {
            response: crate::types::permission::ApprovalReviewResponse::Error { message: None },
        });

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(100),
    )
    .await;

    assert_eq!(
        result,
        Err(RuntimeString::from("Approval reviewer failed; denied."))
    );
}

#[tokio::test]
async fn approval_reviewer_panic_fails_closed() {
    let reviewer: Arc<dyn crate::types::permission::ApprovalReviewer> =
        Arc::new(PanickingApprovalReviewer);

    let result = review_approval_before_prompt(
        Some(reviewer.as_ref()),
        approval_review_request(),
        AbortSignal::new(),
        Some(100),
    )
    .await;

    assert_eq!(
        result,
        Err(RuntimeString::from("Approval reviewer failed; denied."))
    );
}

#[tokio::test]
async fn approval_reviewer_denies_query_path_before_host_prompt() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "echo", &runtime_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool {
        schema: JsonSchema::new(),
    })];
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mut params = mk_params(vec![user_msg("run echo")], tools);
    params.permission_engine = Arc::new(AskAll);
    params.approval_handler = Some(Arc::new(CountingApprovalHandler {
        calls: Arc::clone(&handler_calls),
    }));
    params.approval_reviewer = Some(Arc::new(StaticApprovalReviewer {
        response: crate::types::permission::ApprovalReviewResponse::Deny {
            message: Some("blocked by reviewer".into()),
        },
    }));

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert!(!evs
        .iter()
        .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemUpdated { inference: _,
            item: ThreadItem::ToolCall {
                tool_name,
                status: ItemStatus::Failed,
                error: Some(ItemError { message }),
                ..
            }
        } if tool_name == "echo" && message == "blocked by reviewer"
    )));
}

#[tokio::test]
async fn network_access_approval_event_allows_web_fetch() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "web_fetch",
        &runtime_json!({ "url": "https://example.com/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![create_web_fetch_tool(WebFetchToolOptions {
        fetch_impl: Some(Arc::new(StaticWebFetchFetcher)),
        ..Default::default()
    })];
    let mut params = mk_params(vec![user_msg("fetch example")], tools);
    params.approval_handler = Some(Arc::new(RecordingApprovalHandler {
        requests: Arc::clone(&requests),
    }));
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ApprovalRequested {
            request: ApprovalRequest {
                kind: ApprovalKind::Network,
                tool_name,
                ..
            }
        } if tool_name == "web_fetch"
    )));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].kind, ApprovalKind::Network);
    assert_eq!(requests[0].tool_name, "web_fetch");
    assert!(requests[0]
        .input
        .get("host")
        .and_then(Json::as_str)
        .is_some_and(|host| host == "example.com"));
    let records = records.lock().unwrap().clone();
    let approved = records
        .iter()
        .find(|record| record.phase == ToolLifecyclePhase::Approved)
        .unwrap();
    assert_eq!(approved.approval_kind, Some(ApprovalKind::Network));
    assert!(approved
        .arguments
        .as_ref()
        .and_then(|value| value.get("host"))
        .and_then(Json::as_str)
        .is_some_and(|host| host == "example.com"));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, status: ItemStatus::Completed, .. }
        } if tool_name == "web_fetch"
    )));
}

#[tokio::test]
async fn network_access_without_approval_handler_fails_closed() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "web_fetch",
        &runtime_json!({ "url": "https://example.com/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![create_web_fetch_tool(WebFetchToolOptions {
        fetch_impl: Some(Arc::new(StaticWebFetchFetcher)),
        ..Default::default()
    })];
    let params = mk_params(vec![user_msg("fetch example")], tools);

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs
        .iter()
        .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })));
    assert!(!evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, status: ItemStatus::Completed, .. }
        } if tool_name == "web_fetch"
    )));
}

#[tokio::test]
async fn permitted_network_access_needs_no_approval_handler() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "web_fetch",
        &runtime_json!({ "url": "https://example.com/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![create_web_fetch_tool(WebFetchToolOptions {
        fetch_impl: Some(Arc::new(StaticWebFetchFetcher)),
        ..Default::default()
    })];
    let mut params = mk_params(vec![user_msg("fetch example")], tools);
    params.tool_context_base = Arc::new(|| {
        let mut ctx = make_test_ctx("/tmp");
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx.network_allowed = Some(true);
        ctx
    });

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(!evs
        .iter()
        .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, status: ItemStatus::Completed, .. }
        } if tool_name == "web_fetch"
    )));
}

#[tokio::test]
async fn never_policy_denies_network_escalation_without_prompting() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "web_fetch",
        &runtime_json!({ "url": "https://example.com/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![create_web_fetch_tool(WebFetchToolOptions {
        fetch_impl: Some(Arc::new(StaticWebFetchFetcher)),
        ..Default::default()
    })];
    let mut params = mk_params(vec![user_msg("fetch example")], tools);
    params.tool_context_base = Arc::new(|| {
        let mut ctx = make_test_ctx("/tmp");
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::ReadOnly);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx
    });

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(!evs
        .iter()
        .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })));
    assert!(!evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, status: ItemStatus::Completed, .. }
        } if tool_name == "web_fetch"
    )));
}

struct NoopBrowserExecutor;

#[async_trait]
unsafe impl crate::browser::BrowserExecutor for NoopBrowserExecutor {
    async fn execute(
        &self,
        _request: crate::browser::BrowserRuntimeRequest,
        _signal: AbortSignal,
        _emit: crate::browser::BrowserEventSink,
    ) -> Result<crate::browser::BrowserRuntimeResponse, crate::browser::BrowserRuntimeError> {
        Ok(crate::browser::BrowserRuntimeResponse::default())
    }
}

const BROWSER_WRITE_TOOLS: [&str; 5] = [
    "browser_select_browser",
    "browser_claim_tab",
    "browser_click",
    "browser_resize",
    "browser_tabs",
];

fn browser_write_query(
    sandbox_mode: crate::types::permission::SandboxMode,
    approval_policy: crate::types::permission::ApprovalPolicy,
) -> (QueryParams, EngineDeps) {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "select-1",
        "browser_select_browser",
        &runtime_json!({ "deviceId": "chrome:test" }),
    ));
    first.extend(exact_tool_use_events(
        "claim-1",
        "browser_claim_tab",
        &runtime_json!({ "browserId": "chrome:test", "tabId": 1 }),
    ));
    first.extend(exact_tool_use_events(
        "click-1",
        "browser_click",
        &runtime_json!({ "selector": "#buy" }),
    ));
    first.extend(exact_tool_use_events(
        "resize-1",
        "browser_resize",
        &runtime_json!({ "viewport": { "width": 800, "height": 600 } }),
    ));
    first.extend(exact_tool_use_events(
        "tabs-1",
        "browser_tabs",
        &runtime_json!({ "action": "new", "url": "https://example.com/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let deps = deps_with(scripted_call_model(vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ]));
    let tools: Vec<Arc<dyn Tool>> = crate::tools::builtins::browser::create_browser_tools()
        .into_iter()
        .filter(|tool| BROWSER_WRITE_TOOLS.iter().any(|name| tool.name() == *name))
        .collect();
    let mut params = mk_params(vec![user_msg("do browser writes")], tools);
    params.permission_engine = Arc::new(StaticPermissionEngine::new(&params.config));
    params.tool_context_base = Arc::new(move || {
        let mut ctx = make_test_ctx("/tmp");
        ctx.sandbox_mode = Some(sandbox_mode);
        ctx.approval_policy = Some(approval_policy);
        ctx.network_allowed = Some(true);
        ctx.browser =
            Some(unsafe { crate::browser::attest_spawn_free(Arc::new(NoopBrowserExecutor)) });
        ctx
    });
    (params, deps)
}

fn assert_browser_writes_completed_without_approval_error(evs: &[EngineEvent]) {
    assert!(
        !evs.iter()
            .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })),
        "a bypass session must not raise an approval card: {evs:?}"
    );
    for tool_name in BROWSER_WRITE_TOOLS {
        assert!(
            evs.iter().any(|e| matches!(
                e,
                EngineEvent::ItemCompleted {
                    item: ThreadItem::ToolCall {
                        tool_name: name,
                        status: ItemStatus::Completed,
                        error: None,
                        ..
                    },
                    ..
                } if name == tool_name
            )),
            "{tool_name} did not complete successfully: {evs:?}"
        );
    }
    assert!(
        !evs.iter().any(|e| matches!(
            e,
            EngineEvent::ItemUpdated {
                item: ThreadItem::ToolCall {
                    error: Some(ItemError { message, .. }),
                    ..
                },
                ..
            } if message.contains("approval policy is never")
        )),
        "the bypass session must not be rejected by the approval policy: {evs:?}"
    );
}

#[tokio::test]
async fn bypass_permissions_grants_browser_writes_without_prompting() {
    let (params, deps) = browser_write_query(
        crate::types::permission::SandboxMode::DangerFullAccess,
        crate::types::permission::ApprovalPolicy::Never,
    );
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;
    assert_browser_writes_completed_without_approval_error(&evs);
}

#[tokio::test]
async fn default_permissions_still_prompt_for_browser_writes() {
    let (mut params, deps) = browser_write_query(
        crate::types::permission::SandboxMode::WorkspaceWrite,
        crate::types::permission::ApprovalPolicy::OnRequest,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    params.approval_handler = Some(Arc::new(CountingApprovalHandler {
        calls: calls.clone(),
    }));
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(
        evs.iter()
            .any(|e| matches!(e, EngineEvent::ApprovalRequested { .. })),
        "the default preset must still show the approval card: {evs:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        BROWSER_WRITE_TOOLS.len(),
        "each browser write must reach the approval handler"
    );
}

struct EchoTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for EchoTool {
    fn static_name(&self) -> &str {
        "echo"
    }
    fn static_description(&self) -> &str {
        "echo tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(input, "echoed".to_string(), false))
    }
}

struct PanickingSafetyTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for PanickingSafetyTool {
    fn static_name(&self) -> &str {
        "panicking_safety"
    }

    fn static_description(&self) -> &str {
        "panics from tool scheduling predicates"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        panic!("concurrency predicate failure")
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        panic!("read-only predicate failure")
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        unreachable!("predicate regression does not execute the tool")
    }
}

#[test]
fn scheduling_predicate_panics_fail_closed_like_fixed_ts_exceptions() {
    let tool = PanickingSafetyTool {
        schema: JsonSchema::new(),
    };
    assert!(!safe_is_read_only(&tool, &runtime_json!({})));
    assert!(!safe_is_concurrency_safe(&tool, &runtime_json!({})));
}

struct CatalogProbeTool {
    schema: JsonSchema,
    catalogs: Arc<Mutex<Vec<Vec<ToolCatalogEntry>>>>,
}

#[async_trait]
impl Tool for CatalogProbeTool {
    fn static_name(&self) -> &str {
        "catalog_probe"
    }

    fn static_description(&self) -> &str {
        "capture the real query tool catalog"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let catalog = ctx
            .tool_catalog
            .as_ref()
            .expect("query installs tool_catalog before Tool::call")();
        self.catalogs.lock().unwrap().push(catalog);
        Ok(text_result(
            Json::Null,
            "catalog captured".to_string(),
            false,
        ))
    }
}

struct ProgressTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for ProgressTool {
    fn static_name(&self) -> &str {
        "progress"
    }
    fn static_description(&self) -> &str {
        "progress tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        if let Some(cb) = on_progress {
            cb(crate::types::events::ToolDomainEvent::Status {
                message: "halfway".into(),
            })
            .await;
        }
        Ok(text_result(Json::Null, "done".to_string(), false))
    }
}

struct CancelTurnTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for CancelTurnTool {
    fn static_name(&self) -> &str {
        "cancel_turn"
    }
    fn static_description(&self) -> &str {
        "cancel turn tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        ctx.signal.cancel();
        Ok(text_result(Json::Null, "cancelled".to_string(), false))
    }
}

struct PendingTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for PendingTool {
    fn static_name(&self) -> &str {
        "pending"
    }
    fn static_description(&self) -> &str {
        "pending tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, "pending ran".to_string(), false))
    }
}

struct ContextEmitTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for ContextEmitTool {
    fn static_name(&self) -> &str {
        "context_emit"
    }
    fn static_description(&self) -> &str {
        "context emit tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        (ctx.emit)(crate::types::events::ToolDomainEvent::Status {
            message: "from-context".into(),
        })
        .await;
        Ok(text_result(Json::Null, "done".to_string(), false))
    }
}

struct NestedInvokeTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for NestedInvokeTool {
    fn static_name(&self) -> &str {
        "nested_invoke"
    }
    fn static_description(&self) -> &str {
        "nested invoke tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let invoke_tool = ctx.invoke_tool.as_ref().unwrap();
        let nested = invoke_tool("echo".into(), runtime_json!({ "nested": true })).await?;
        assert_eq!(nested.output, Some(runtime_json!({ "nested": true })));
        Ok(text_result(
            Json::Null,
            format!("nested done: {} item(s)", nested.content.len()),
            nested.is_error,
        ))
    }
}

struct FatalNestedTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for FatalNestedTool {
    fn static_name(&self) -> &str {
        "fatal_nested"
    }

    fn static_description(&self) -> &str {
        "always fails fatally when invoked as a nested tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::fatal("nested fatal failure"))
    }
}

struct FatalNestedInvoker {
    name: &'static str,
    schema: JsonSchema,
    catch: bool,
}

#[async_trait]
impl Tool for FatalNestedInvoker {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "invokes a fatal nested tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let invoke_tool = ctx.invoke_tool.as_ref().unwrap();
        let nested = invoke_tool("fatal_nested".into(), runtime_json!({})).await;
        if self.catch {
            let error = nested.expect_err("fatal nested invoke must reject");
            assert_eq!(error.kind, ToolErrorKind::Fatal);
            return Ok(text_result(
                Json::Null,
                RuntimeString::from("caught: ").concat(&error.message),
                false,
            ));
        }
        nested?;
        unreachable!("fatal nested invoke cannot resolve")
    }
}

struct GatedProgressTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for GatedProgressTool {
    fn static_name(&self) -> &str {
        "gated_progress"
    }
    fn static_description(&self) -> &str {
        "gated progress tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        if let Some(cb) = on_progress {
            cb(crate::types::events::ToolDomainEvent::Status {
                message: "approved-progress".into(),
            })
            .await;
        }
        Ok(text_result(Json::Null, "done".to_string(), false))
    }
}

struct SourceTool {
    name: &'static str,
    schema: JsonSchema,
    mcp_server: Option<&'static str>,
    exposure: ToolExposure,
}

struct RawRecordingTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
}

struct RecordingThinkTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
}

struct ResultShapeTool {
    name: &'static str,
    schema: JsonSchema,
    text: String,
    max_chars: Option<u64>,
    output: Json,
    extra_user_blocks: Option<Vec<ContentBlock>>,
}

struct RecordingFacadeTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
}

#[derive(Clone, Copy)]
enum IdentityToolOutcome {
    Ok,
    OkIsError,
    NonfatalErr,
    Fatal,
}

struct IdentityTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
    validation_error: Option<&'static str>,
    outcome: IdentityToolOutcome,
}

#[derive(Clone, Copy)]
enum ParallelFatalRole {
    Fatal,
    Sibling,
}

struct ParallelFatalTool {
    name: &'static str,
    schema: JsonSchema,
    role: ParallelFatalRole,
    started: Arc<tokio::sync::Barrier>,
    sibling_completed: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[derive(Clone, Copy)]
enum CompletionOrderFatalRole {
    BlockingSibling,
    Fatal,
}

struct CompletionOrderFatalTool {
    name: &'static str,
    schema: JsonSchema,
    role: CompletionOrderFatalRole,
    started: Arc<tokio::sync::Barrier>,
    sibling_release: Arc<tokio::sync::Notify>,
    sibling_completed: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Copy)]
enum CompletionOrderSuccessRole {
    SlowFirst,
    FastSecond,
}

struct CompletionOrderSuccessTool {
    name: &'static str,
    schema: JsonSchema,
    role: CompletionOrderSuccessRole,
    started: Arc<tokio::sync::Barrier>,
    fast_completed: Arc<tokio::sync::Notify>,
    completion_order: Arc<Mutex<Vec<&'static str>>>,
}

struct AdmissionOrderTool {
    name: &'static str,
    schema: JsonSchema,
    admitted: Arc<Mutex<Vec<&'static str>>>,
    fatal: bool,
    completion: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait]
impl Tool for AdmissionOrderTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "ordered-admission test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.admitted.lock().unwrap().push(self.name);
        if let Some(completion) = &self.completion {
            completion.notify_one();
        }
        if self.fatal {
            return Err(ToolError::fatal("queued fifo fatal failure"));
        }
        tokio::task::yield_now().await;
        Ok(text_result(Json::Null, self.name.to_string(), false))
    }
}

#[async_trait]
impl Tool for ParallelFatalTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "parallel fatal lifecycle test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.started.wait().await;
        match self.role {
            ParallelFatalRole::Fatal => Err(ToolError::fatal("parallel fatal failure")),
            ParallelFatalRole::Sibling => {
                let completed = self.sibling_completed.lock().unwrap().take();
                tokio::time::sleep(Duration::from_millis(50)).await;
                if let Some(completed) = completed {
                    let _ = completed.send(());
                }
                Ok(text_result(
                    Json::Null,
                    "sibling completed".to_string(),
                    false,
                ))
            }
        }
    }
}

#[async_trait]
impl Tool for CompletionOrderFatalTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "completion-order fatal test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.started.wait().await;
        match self.role {
            CompletionOrderFatalRole::BlockingSibling => {
                self.sibling_release.notified().await;
                self.sibling_completed.notify_one();
                Ok(text_result(
                    Json::Null,
                    "blocking sibling completed".to_string(),
                    false,
                ))
            }
            CompletionOrderFatalRole::Fatal => {
                Err(ToolError::fatal("completion-order fatal failure"))
            }
        }
    }
}

#[async_trait]
impl Tool for CompletionOrderSuccessTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "completion-order success test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.started.wait().await;
        let label = match self.role {
            CompletionOrderSuccessRole::SlowFirst => {
                self.fast_completed.notified().await;
                tokio::time::sleep(Duration::from_millis(25)).await;
                "slow-first"
            }
            CompletionOrderSuccessRole::FastSecond => {
                self.fast_completed.notify_one();
                "fast-second"
            }
        };
        self.completion_order.lock().unwrap().push(label);
        Ok(text_result(Json::Null, label.to_string(), false))
    }
}

struct MidApprovalIdentityTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
    approvals: Arc<Mutex<Vec<bool>>>,
}

struct TrustedInternalRecorder {
    schema: JsonSchema,
    seen: Arc<Mutex<Vec<Json>>>,
}

#[async_trait]
impl Tool for TrustedInternalRecorder {
    fn static_name(&self) -> &str {
        "spawn_agent"
    }

    fn static_description(&self) -> &str {
        "internal tool whose name collides with an OpenAI facade"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input.clone());
        Ok(text_result(input, "trusted internal".to_string(), false))
    }
}

struct TrustedNestedInvoker {
    schema: JsonSchema,
    trusted_input: Json,
}

#[async_trait]
impl Tool for TrustedNestedInvoker {
    fn static_name(&self) -> &str {
        "nested_internal"
    }

    fn static_description(&self) -> &str {
        "invoke a trusted internal tool name"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let result = ctx
            .invoke_tool
            .as_ref()
            .expect("invoke_tool context callback")(
            "spawn_agent".into(),
            self.trusted_input.clone(),
        )
        .await?;
        Ok(text_result(
            result.output.unwrap_or(Json::Null),
            "nested complete".to_string(),
            result.is_error,
        ))
    }
}

#[async_trait]
impl Tool for MidApprovalIdentityTool {
    fn static_name(&self) -> &str {
        "task"
    }

    fn static_description(&self) -> &str {
        "request approval after permission rewriting"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input.clone());
        let approved = ctx
            .request_approval
            .as_ref()
            .expect("request_approval context callback")(
            RuntimeString::from("approve effective input"),
        )
        .await;
        self.approvals.lock().unwrap().push(approved);
        Ok(text_result(input, "approved".to_string(), !approved))
    }
}

#[async_trait]
impl Tool for IdentityTool {
    fn static_name(&self) -> &str {
        "task"
    }

    fn static_description(&self) -> &str {
        "resolved identity test tool"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn validate_input(&self, _input: &Json, _ctx: &ToolContext) -> Option<ValidationResult> {
        self.validation_error.map(|message| ValidationResult::Err {
            message: message.to_string(),
            error_code: None,
        })
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input.clone());
        match self.outcome {
            IdentityToolOutcome::Ok => Ok(text_result(input, "identity ok".to_string(), false)),
            IdentityToolOutcome::OkIsError => {
                Ok(text_result(input, "identity error".to_string(), true))
            }
            IdentityToolOutcome::NonfatalErr => Err(ToolError::new("identity tool failed")),
            IdentityToolOutcome::Fatal => Err(ToolError::fatal("identity fatal failure")),
        }
    }
}

#[async_trait]
impl Tool for RecordingFacadeTool {
    fn static_name(&self) -> &str {
        "task"
    }

    fn static_description(&self) -> &str {
        "record task"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input);
        Ok(text_result(Json::Null, "done".to_string(), false))
    }
}

#[async_trait]
impl Tool for RawRecordingTool {
    fn static_name(&self) -> &str {
        "apply_patch"
    }

    fn static_description(&self) -> &str {
        "apply an exact raw patch"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn input_kind(&self) -> crate::types::tool::ToolInputKind {
        crate::types::tool::ToolInputKind::Freeform {
            format: "text".into(),
        }
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input);
        Ok(text_result(Json::Null, "patch applied".to_owned(), false))
    }
}

#[async_trait]
impl Tool for RecordingThinkTool {
    fn static_name(&self) -> &str {
        "think"
    }

    fn static_description(&self) -> &str {
        "record the exact input that reaches Think execution"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.seen.lock().unwrap().push(input.clone());
        Ok(text_result(input, "Thought recorded.".to_string(), false))
    }
}

fn recording_think_tool(seen: Arc<Mutex<Vec<Json>>>) -> Arc<dyn Tool> {
    let schema = crate::tools::builtins::think::create_think_tool()
        .static_input_schema()
        .clone();
    Arc::new(RecordingThinkTool { schema, seen })
}

#[async_trait]
impl Tool for ResultShapeTool {
    fn static_name(&self) -> &str {
        self.name
    }

    fn static_description(&self) -> &str {
        "shape a result for provider pipeline tests"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        self.max_chars
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let mut result = text_result(self.output.clone(), self.text.clone(), false);
        result.serializable.extra_user_blocks = self.extra_user_blocks.clone();
        Ok(result)
    }
}

struct NativeSearchRecordingTool {
    schema: JsonSchema,
    output: Json,
}

#[async_trait]
impl Tool for NativeSearchRecordingTool {
    fn static_name(&self) -> &str {
        "tool_search"
    }

    fn static_description(&self) -> &str {
        "search the deferred tool catalog"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        match input.get("mode").and_then(Json::as_str) {
            Some(mode) if mode == "ok_error" => Ok(text_result(
                self.output.clone(),
                "search reported an error".to_owned(),
                true,
            )),
            Some(mode) if mode == "err" => Err(ToolError::new("search tool failed")),
            _ => Ok(text_result(
                self.output.clone(),
                "search complete".to_owned(),
                false,
            )),
        }
    }
}

fn native_search_output_fixture() -> Json {
    runtime_json!({"tools":[
        {"type":"function","name":"read","description":"Read","inputSchema":{"type":"object"}},
        {"type":"namespace","name":"calendar","tools":[
            {"type":"function","name":"events","description":"Events","parameters":{"type":"object"}}
        ]}
    ]})
}

#[test]
fn catalog_uses_internal_key_and_primary_rendered_openai_spec() {
    let tool = create_send_message_tool();
    let profile = test_provider_profile("gpt-5");

    let catalog = build_tool_catalog(std::slice::from_ref(&tool), &profile);

    assert_eq!(catalog.len(), 1);
    let entry = &catalog[0];
    assert_eq!(entry.name, "SendMessage");
    assert_eq!(entry.exposure, ToolExposure::Direct);
    assert!(entry.search_hint.as_ref().is_some_and(
        |hint| hint == &RuntimeString::from("queue a message to a subagent or teammate")
    ));
    let primary = entry.spec.as_ref().expect("rendered primary spec");
    assert_eq!(primary.name, "send_message");
    assert_eq!(entry.description, primary.description);
    assert_ne!(entry.description, tool.description());

    let rendered = render_tool_specs_for_behavior(&[to_tool_spec(tool.as_ref())], &profile);
    assert_eq!(rendered.len(), 2);
    assert_eq!(entry.spec.as_ref(), rendered.first());
    assert_eq!(rendered[1].name, "followup_task");
}

#[test]
fn model_visibility_filters_exposure_and_uses_internal_surface_name() {
    let source = |name, exposure| {
        Arc::new(SourceTool {
            name,
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure,
        }) as Arc<dyn Tool>
    };
    let tools = vec![
        source("read", ToolExposure::Direct),
        source("bridge", ToolExposure::DirectModelOnly),
        source("later", ToolExposure::Deferred),
        source("secret", ToolExposure::Hidden),
        source("SendMessage", ToolExposure::Deferred),
    ];
    let precomputed = to_tool_specs(&tools);

    let names = |surfaced: &HashSet<RuntimeString>| {
        visible_tool_specs(&tools, surfaced, &precomputed)
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&HashSet::new()), vec!["read", "bridge"]);

    let surfaced_visible_name = HashSet::from([RuntimeString::from("send_message")]);
    assert_eq!(
        names(&surfaced_visible_name),
        vec!["read", "bridge"],
        "provider-visible facade names must not surface internal tools"
    );

    let surfaced_internal_names = HashSet::from([
        RuntimeString::from("later"),
        RuntimeString::from("SendMessage"),
    ]);
    assert_eq!(
        names(&surfaced_internal_names),
        vec!["read", "bridge", "later", "SendMessage"]
    );
}

/// Surfacing a deferred tool must not disturb the tools already in the request.
///
/// The tools array leads the cached prefix, so a surfaced tool placed at its
/// sorted position shifts every tool behind it and costs the entire prefix —
/// system and history included. This pins the append-only contract: whatever a
/// later `tool_search` finds, the specs the session already cached stay
/// byte-identical and in the same order, with the newcomer after them.
#[test]
fn surfacing_a_deferred_tool_appends_instead_of_inserting_mid_array() {
    let source = |name, exposure| {
        Arc::new(SourceTool {
            name,
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure,
        }) as Arc<dyn Tool>
    };
    // Pool order is sorted by name, so the deferred tool sorts FIRST here. Under
    // an insert-at-sorted-position rule it would land ahead of `read`, moving
    // every already-cached tool; appended, `read` and `write` keep their bytes.
    let tools = vec![
        source("aaa_late", ToolExposure::Deferred),
        source("read", ToolExposure::Direct),
        source("write", ToolExposure::Direct),
    ];
    let precomputed = to_tool_specs(&tools);
    let names = |surfaced: &HashSet<RuntimeString>| {
        visible_tool_specs(&tools, surfaced, &precomputed)
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>()
    };

    let before = names(&HashSet::new());
    assert_eq!(before, vec!["read", "write"]);

    let after = names(&HashSet::from([RuntimeString::from("aaa_late")]));
    assert_eq!(
        after,
        vec!["read", "write", "aaa_late"],
        "the surfaced tool belongs after the cached prefix, not inside it"
    );
    assert_eq!(
        &after[..before.len()],
        before.as_slice(),
        "the already-cached tools must keep their order and identity"
    );
}

/// Two requests that surfaced the same set must emit the same order.
///
/// The surfaced set is a `HashSet`, whose iteration order is unspecified, so a
/// request that read it directly would reshuffle the tools array between calls
/// with nothing visible having changed — the same prefix invalidation as a
/// mid-array insert. The order must come from the pool alone, which is sorted by
/// name, so these two pools with different arrangements of the same deferred
/// tools prove the emitted order follows the pool and not the set.
#[test]
fn surfaced_tool_order_follows_the_pool_not_the_hash_set() {
    let source = |name, exposure| {
        Arc::new(SourceTool {
            name,
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure,
        }) as Arc<dyn Tool>
    };
    // One surfaced set, shared by both pools. It is only ever queried with
    // `contains`, never iterated.
    let surfaced = HashSet::from([
        RuntimeString::from("mcp_zeta"),
        RuntimeString::from("mcp_alpha"),
        RuntimeString::from("mcp_mid"),
    ]);

    // The production shape: a pool already sorted by name.
    let sorted_pool = vec![
        source("mcp_alpha", ToolExposure::Deferred),
        source("mcp_mid", ToolExposure::Deferred),
        source("mcp_zeta", ToolExposure::Deferred),
        source("read", ToolExposure::Direct),
    ];
    let sorted_names = visible_tool_specs(&sorted_pool, &surfaced, &to_tool_specs(&sorted_pool))
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    assert_eq!(
        sorted_names,
        vec!["read", "mcp_alpha", "mcp_mid", "mcp_zeta"],
        "always-visible tools lead, surfaced ones follow in pool order"
    );

    // A differently arranged pool of the same tools: the emitted order must
    // follow this arrangement, which is only possible if the set was not iterated.
    let shuffled_pool = vec![
        source("read", ToolExposure::Direct),
        source("mcp_zeta", ToolExposure::Deferred),
        source("mcp_alpha", ToolExposure::Deferred),
        source("mcp_mid", ToolExposure::Deferred),
    ];
    let shuffled_names =
        visible_tool_specs(&shuffled_pool, &surfaced, &to_tool_specs(&shuffled_pool))
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
    assert_eq!(
        shuffled_names,
        vec!["read", "mcp_zeta", "mcp_alpha", "mcp_mid"]
    );
}

#[test]
fn with_deferred_tools_only_converts_direct_exposure() {
    let source = |name, exposure| {
        Arc::new(SourceTool {
            name,
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure,
        }) as Arc<dyn Tool>
    };
    let direct = source("read", ToolExposure::Direct);
    let model_only = source("bridge", ToolExposure::DirectModelOnly);
    let deferred = source("later", ToolExposure::Deferred);
    let hidden = source("secret", ToolExposure::Hidden);

    let wrapped = crate::tools::builtins::tool_search::with_deferred_tools(
        vec![
            Arc::clone(&direct),
            Arc::clone(&model_only),
            Arc::clone(&deferred),
            Arc::clone(&hidden),
        ],
        Some(0),
    );

    assert_eq!(wrapped[0].name(), "tool_search");
    assert_eq!(wrapped[1].exposure(), ToolExposure::Deferred);
    assert!(!Arc::ptr_eq(&wrapped[1], &direct));
    assert_eq!(wrapped[2].exposure(), ToolExposure::DirectModelOnly);
    assert!(Arc::ptr_eq(&wrapped[2], &model_only));
    assert_eq!(wrapped[3].exposure(), ToolExposure::Deferred);
    assert!(Arc::ptr_eq(&wrapped[3], &deferred));
    assert_eq!(wrapped[4].exposure(), ToolExposure::Hidden);
    assert!(Arc::ptr_eq(&wrapped[4], &hidden));
}

#[async_trait]
impl Tool for SourceTool {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "source test tool"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn exposure(&self) -> ToolExposure {
        self.exposure
    }
    fn static_mcp_server(&self) -> Option<&str> {
        self.mcp_server
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, "done".to_string(), false))
    }
}

fn seq_uuid() -> Arc<dyn Fn() -> String + Send + Sync> {
    let c = Arc::new(AtomicUsize::new(0));
    Arc::new(move || format!("id-{}", c.fetch_add(1, Ordering::SeqCst)))
}

fn deps_with(call_model: crate::types::provider::CallModel) -> EngineDeps {
    EngineDeps {
        provider_id: "test-provider".into(),
        call_model,
        estimate_tokens: Arc::new(|_| 0),
        compactor: crate::engine::deps::noop_compactor(),
        uuid: seq_uuid(),
        now: Arc::new(|| 0),
    }
}

fn user_msg(text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: "u0".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

struct StaticBackgroundTasks {
    tasks: Vec<crate::types::tool::BackgroundTaskHandle>,
}

#[async_trait]
impl BackgroundTaskRegistry for StaticBackgroundTasks {
    fn get(&self, agent_id: &RuntimeString) -> Option<crate::types::tool::BackgroundTaskHandle> {
        self.tasks
            .iter()
            .find(|task| &task.agent_id == agent_id)
            .cloned()
    }

    fn list(&self) -> Vec<crate::types::tool::BackgroundTaskHandle> {
        self.tasks.clone()
    }

    fn stop(&self, _agent_id: &RuntimeString) -> bool {
        false
    }

    async fn wait_for_idle(&self) {}
}

fn compact_boundary() -> Message {
    Message::CompactBoundary(crate::schemas::message::CompactBoundaryMessage {
        native_compaction: None,
        uuid: "compact-1".into(),
        summary: "older context".into(),
    })
}

fn text_block_text(block: &ContentBlock) -> &RuntimeString {
    match block {
        ContentBlock::Text(text) => &text.text,
        _ => panic!("expected text block"),
    }
}

struct TestProviderCallAdmission {
    events: Arc<Mutex<Vec<&'static str>>>,
    before_result: Result<(), RuntimeString>,
    returned_receipt: Arc<dyn Fn() -> ProviderCallAdmissionReceipt + Send + Sync + 'static>,
    panicked_receipt: Arc<dyn Fn() -> ProviderCallAdmissionReceipt + Send + Sync + 'static>,
}

impl ProviderCallAdmission for TestProviderCallAdmission {
    fn before_call(&self) -> Result<(), RuntimeString> {
        self.events.lock().unwrap().push("before_call");
        self.before_result.clone()
    }

    fn call_returned(&self) -> ProviderCallAdmissionReceipt {
        self.events.lock().unwrap().push("call_returned");
        (self.returned_receipt)()
    }

    fn call_panicked(&self) -> ProviderCallAdmissionReceipt {
        self.events.lock().unwrap().push("call_panicked");
        (self.panicked_receipt)()
    }
}

fn ready_provider_admission_receipt() -> ProviderCallAdmissionReceipt {
    Box::pin(async { Ok(()) })
}

#[test]
fn compaction_excludes_protected_messages_and_restores_them_before_the_turn_user() {
    let message = |uuid: &str, text: &str, is_meta: bool| {
        Message::User(UserMessage {
            question_answer: None,
            uuid: uuid.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: is_meta.then_some(true),
        })
    };
    let protected_id = RuntimeString::from("reference");
    let protected_ids = HashSet::from([protected_id.clone()]);
    let messages = vec![
        message("old", "old conversation", false),
        message("reference", "untrusted reference", true),
        message("current", "current request", false),
    ];

    let (compactable, protected) = split_compaction_protected(&messages, &protected_ids);
    assert_eq!(
        compactable
            .iter()
            .map(query_message_uuid)
            .cloned()
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("old"), RuntimeString::from("current")]
    );
    let restored = restore_compaction_protected(
        vec![
            compact_boundary(),
            message("current", "current request", false),
        ],
        protected,
        Some(&RuntimeString::from("current")),
    );
    assert_eq!(
        restored
            .iter()
            .map(query_message_uuid)
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            RuntimeString::from("compact-1"),
            protected_id,
            RuntimeString::from("current"),
        ]
    );
}

#[test]
fn running_agent_reminder_uses_context_manager_reinsertion() {
    let registry: Arc<dyn BackgroundTaskRegistry> = Arc::new(StaticBackgroundTasks {
        tasks: vec![crate::types::tool::BackgroundTaskHandle {
            agent_id: "bg-1".into(),
            output_file: "/tmp/bg-1.txt".into(),
            status: BackgroundTaskStatus::Running,
            description: Some("inspect logs".into()),
            partial: None,
        }],
    });

    let rendered = inject_running_agent_reminder(
        vec![compact_boundary()],
        Some(&registry),
        &|| "ctx-1".to_string(),
        &test_provider_profile("claude-sonnet-4-6"),
    );

    assert_eq!(rendered.len(), 2);
    let Message::User(user) = &rendered[1] else {
        panic!("expected inserted user message");
    };
    assert_eq!(user.uuid, "ctx-1");
    assert_eq!(user.is_meta, Some(true));
    let text = text_block_text(&user.content[0]);
    assert!(text.starts_with("<system-reminder>\nThese background tasks"));
    assert!(text.contains("bg-1"));
    assert!(text.contains("task_output"));
}

#[test]
fn surfaced_tools_reminder_uses_context_manager_reinsertion() {
    let tool = Arc::new(SourceTool {
        name: "extra_tool",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Deferred,
    }) as Arc<dyn Tool>;
    let mut surfaced = HashSet::new();
    surfaced.insert(RuntimeString::from("extra_tool"));

    let rendered = inject_surfaced_tools_reminder(
        vec![user_msg("question")],
        &[tool],
        &surfaced,
        &|| "unused".to_string(),
        &test_provider_profile("claude-sonnet-4-6"),
    );

    let Message::User(user) = &rendered[0] else {
        panic!("expected trailing user message");
    };
    assert_eq!(user.is_meta, None);
    assert_eq!(user.content.len(), 2);
    let text = text_block_text(&user.content[1]);
    assert!(text.starts_with("<system-reminder>\nThese tools"));
    assert!(text.contains("extra_tool"));
}

fn mk_params(messages: Vec<Message>, tools: Vec<Arc<dyn Tool>>) -> QueryParams {
    let specs = to_tool_specs(&tools);
    QueryParams {
        retained_tool_tasks: None,
        prepare_request_usage: None,
        multi_agent_instructions: false,
        thread_id: "thread-1".into(),
        turn_id: "turn-1".into(),
        model: "fake-model".into(),
        provider_info: None,
        messages,
        compaction_protected_message_ids: HashSet::new(),
        context_window: None,
        compaction_history: None,
        recover_compaction_history: None,
        compaction_output: None,
        tools,
        tool_specs: specs,
        refresh_tools: None,
        refresh_tools_async: None,
        agent_tool_policy: None,
        system_prompt: SystemPromptResolvable::Parts(vec![SystemPromptPart {
            name: "sys".into(),
            text: "you are a test".into(),
            cacheable: None,
            cache_scope: None,
        }]),
        config: crate::schemas::config::default_config(),
        max_turns: None,
        cost_so_far: None,
        tool_context_base: Arc::new(|| make_test_ctx("/tmp")),
        permission_engine: Arc::new(AllowAll),
        hook_bus: None,
        signal: AbortSignal::new(),
        approval_handler: None,
        approval_reviewer: None,
        output_schema: None,
        token_budget: None,
        goal: None,
        goal_request_identity: None,
        goal_pending_input: None,
        goal_execution_enabled: None,
        goal_usage: None,
        web_search: None,
        on_message: None,
        message_rollout: None,
        on_permission_update: None,
        on_compaction: None,
        surfaced_tools: None,
        on_surface_tools: None,
        on_tool_lifecycle: None,
        provider_call_admission: None,
        on_provider_request_admitted: None,
        emit_thread_started: true,
        reminder_providers: Vec::new(),
        edited_files_reminder: None,
        effort: None,
        ultra_mode: false,
        thinking: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        plan_file_path: None,
        message_queue: None,
        input_pdf_bytes: 0,
        input_pdf_documents: 0,
        validate_native_pdf: None,
        agent_id: None,
        agent_type: None,
        context_hint_state: None,
        tool_result_budget_state: None,
        on_tool_result_budget_records: None,
        thinking_context_cold: false,
        padded_countdown_spend: Default::default(),
        task_countdown_anchor: None,
        live_model_override: None,
    }
}

#[tokio::test]
async fn missing_fixed_capture_fails_without_calling_legacy_transport() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    let deps = deps_with(Box::new(move |_, _| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(futures::stream::empty())
    }));
    let capture = super::InferenceCapture {
        slot: Arc::new(Mutex::new(None)),
        next: None,
        model_override: None,
        effort_override: None,
        ultra_override: None,
    };
    let events =
        super::query_with_inference(mk_params(vec![user_msg("hi")], Vec::new()), deps, capture)
            .collect::<Vec<_>>()
            .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        matches!(events.last(), Some(EngineEvent::TurnFailed { error, .. })
        if error.message.contains("missing its admitted guard"))
    );
}

#[tokio::test]
async fn failed_main_capture_does_not_send_with_legacy_transport() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    let deps = deps_with(Box::new(move |_, _| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(futures::stream::empty())
    }));
    let slot = Arc::new(Mutex::new(None));
    let capture = super::InferenceCapture {
        slot: slot.clone(),
        next: Some(Arc::new(|_| {
            Box::pin(async { Err("model update failed".to_string()) })
        })),
        model_override: None,
        effort_override: None,
        ultra_override: None,
    };
    let events =
        super::query_with_inference(mk_params(vec![user_msg("hi")], Vec::new()), deps, capture)
            .collect::<Vec<_>>()
            .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(slot.lock().unwrap().is_none());
    assert!(
        matches!(events.last(), Some(EngineEvent::TurnFailed { error, .. })
        if error.message == "model update failed")
    );
}

#[tokio::test]
async fn cancelled_main_capture_settles_as_aborted_without_transport() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    let signal = params.signal.clone();
    params.emit_thread_started = false;
    let capture = super::InferenceCapture {
        slot: Arc::new(Mutex::new(None)),
        next: Some(Arc::new(|_| Box::pin(futures::future::pending()))),
        model_override: None,
        effort_override: None,
        ultra_override: None,
    };
    let task = tokio::spawn(async move {
        super::query_with_inference(
            params,
            deps_with(Box::new(|_, _| {
                panic!("cancelled capture called transport")
            })),
            capture,
        )
        .collect::<Vec<_>>()
        .await
    });
    signal.cancel();
    let events = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("capture cancellation must wake the query")
        .unwrap();
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        })
    ));
}

#[test]
fn captured_request_planning_uses_captured_provider_config_and_explicit_effort() {
    let mut params = mk_params(vec![user_msg("request")], Vec::new());
    params.model = "stale-model".into();
    params.effort = Some(EffortValue::Number(1));
    params.thinking = None;
    params.config.task_budget_tokens = Some(100);
    params.config.manage_thinking_context = false;
    params.provider_info = Some(ProviderInfo {
        name: "anthropic".into(),
        base_url: None,
        env_key: None,
        default_model: "stale-model".into(),
    });
    params.thinking_context_cold = true;
    params.cache_key = Some("stable-thread".into());
    let mut captured_config = params.config.clone();
    captured_config.model = "gpt-5".into();
    captured_config.task_budget_tokens = Some(777);
    captured_config.manage_thinking_context = true;
    let provider = ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    };
    let turn_override = TurnOverride {
        model: "gpt-5".into(),
        effort: Some(EffortValue::Number(9)),
        ultra_mode: false,
    };
    let prepared = super::request::prepare_with_inference(
        &params.messages,
        &params,
        super::request::RequestInference {
            config: &captured_config,
            provider_info: Some(&provider),
            thinking: Some(ThinkingConfig::Adaptive),
        },
        &[],
        2048,
        &[],
        Some(&turn_override),
    );
    assert_eq!(
        prepared.profile.prompt_style,
        ProviderPromptStyle::OpenAiDeveloper
    );
    assert_eq!(prepared.request.model, "gpt-5");
    assert_eq!(prepared.request.effort, Some(EffortValue::Number(9)));
    assert_eq!(prepared.request.thinking, Some(ThinkingConfig::Adaptive));
    assert_eq!(prepared.request.task_budget, Some(777));
    assert_eq!(
        prepared.request.thinking_context,
        Some(crate::types::provider::ThinkingContextPolicy::KeepAll),
    );
    assert_eq!(prepared.request.cache_key, params.cache_key);
    assert_eq!(
        params.model, "stale-model",
        "planning does not mutate QueryParams"
    );
    assert_eq!(params.config.task_budget_tokens, Some(100));
}

#[test]
fn blind_request_projection_omits_all_image_shapes_but_keeps_canonical_history() {
    let image = crate::schemas::content::ImageBlock {
        mime_type: "image/png".into(),
        base64: "opaque-image".into(),
        detail: None,
        source_path: None,
    };
    let document = crate::schemas::content::DocumentBlock {
        mime_type: "application/pdf".into(),
        base64: "opaque-pdf".into(),
        filename: None,
    };
    let history = vec![
        Message::User(UserMessage {
            question_answer: None,
            uuid: "image-user".into(),
            content: vec![
                ContentBlock::Image(image.clone()),
                ContentBlock::Document(document.clone()),
            ],
            is_meta: None,
        }),
        Message::Assistant(crate::schemas::message::AssistantMessage {
            uuid: "image-assistant".into(),
            model: "old-vision".into(),
            usage: None,
            content: vec![ContentBlock::Image(image.clone())],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "image-result".into(),
            tool_use_id: "tool-id".into(),
            content: vec![
                OutputBlock::Image(image.clone()),
                OutputBlock::Image(image.clone()),
            ],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: Some(vec![
                ContentBlock::Image(image),
                ContentBlock::Document(document),
            ]),
        }),
    ];
    let mut projected = history.clone();
    super::request::omit_request_images(&mut projected);
    let Message::User(user) = &projected[0] else {
        panic!("expected user")
    };
    assert!(matches!(user.content[0], ContentBlock::Document(_)));
    assert!(text_block_text(&user.content[1]).contains("1 image was omitted"));
    let Message::Assistant(assistant) = &projected[1] else {
        panic!("expected assistant")
    };
    assert_eq!(assistant.content.len(), 1);
    assert!(text_block_text(&assistant.content[0]).contains("cannot understand images"));
    let Message::ToolResult(result) = &projected[2] else {
        panic!("expected tool result")
    };
    assert_eq!(result.content.len(), 1);
    assert!(output_blocks_text(&result.content).contains("Images were omitted"));
    let extra = result.extra_user_blocks.as_ref().unwrap();
    assert!(matches!(extra[0], ContentBlock::Document(_)));
    assert!(text_block_text(&extra[1]).contains("1 image was omitted"));
    assert!(
        matches!(&history[0], Message::User(user) if matches!(user.content[0], ContentBlock::Image(_)))
    );
    let once = projected.clone();
    super::request::omit_request_images(&mut projected);
    assert_eq!(
        projected, once,
        "retry projection must not duplicate omission notes"
    );
}

#[tokio::test]
async fn main_query_has_no_default_turn_limit() {
    let turns: ScriptedTurns = (0..101)
        .map(|index| {
            let mut events = vec![ev::message_start(Some("gpt-5"))];
            events.extend(exact_tool_use_events(
                &format!("loop-{index}"),
                "task",
                &runtime_json!({}),
            ));
            events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            events
        })
        .chain(std::iter::once(vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ]))
        .collect();
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(Vec::new())),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let mut params = mk_params(vec![user_msg("keep going")], vec![tool]);
    params.max_turns = None;
    params.config.loop_detection = false;

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::AgentMessage { text, .. }
        } if text == "done"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::TurnLimitExhausted,
            ..
        }
    )));
}

#[tokio::test]
async fn turn_cap_reports_its_own_terminal_reason() {
    let turns: ScriptedTurns = (0..3)
        .map(|index| {
            let mut events = vec![ev::message_start(Some("gpt-5"))];
            events.extend(exact_tool_use_events(
                &format!("loop-{index}"),
                "task",
                &runtime_json!({}),
            ));
            events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            events
        })
        .collect();
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(Vec::new())),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let mut params = mk_params(vec![user_msg("keep going")], vec![tool]);
    params.max_turns = Some(2);
    params.config.loop_detection = false;

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::TurnLimitExhausted,
            ..
        }
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::SpendLimitExhausted,
            ..
        }
    )));
}

#[tokio::test]
async fn spend_cap_reports_its_own_terminal_reason() {
    let turns: ScriptedTurns = vec![{
        let mut events = vec![ev::message_start(Some("gpt-5"))];
        events.extend(exact_tool_use_events("loop-0", "task", &runtime_json!({})));
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        events
    }];
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(Vec::new())),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let mut params = mk_params(vec![user_msg("keep going")], vec![tool]);
    params.config.loop_detection = false;
    params.config.max_budget_usd = Some(1.0);
    params.cost_so_far = Some(Box::new(|| 10.0));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::SpendLimitExhausted,
            ..
        }
    )));
}

#[test]
fn resolved_call_keeps_visible_and_initial_input_immutable() {
    let visible = ToolUseBlock {
        id: "call-1".into(),
        name: "spawn_agent".into(),
        input: runtime_json!({
            "task_name": "inspect",
            "message": "inspect the engine",
            "fork_turns": "none"
        }),
        thought_signature: None,
        tool_kind: None,
    };
    let profile = test_provider_profile("gpt-5");
    let mut call = ResolvedToolCall::new(visible.clone(), profile);

    assert_eq!(call.visible, visible);
    assert_eq!(call.internal_name, "task");
    assert_eq!(
        call.initial_internal_input,
        runtime_json!({
            "description": "inspect",
            "name": "inspect",
            "prompt": "inspect the engine",
            "fork_turns":"none"
        })
    );

    call.replace_effective_input(runtime_json!({
        "description": "approved",
        "prompt": "run"
    }));

    assert_eq!(
        call.visible.input["task_name"],
        RuntimeJson::from("inspect")
    );
    assert_eq!(
        call.initial_internal_input["description"],
        RuntimeJson::from("inspect")
    );
    assert_eq!(
        call.effective_internal_input["description"],
        RuntimeJson::from("approved")
    );
}

#[tokio::test]
async fn facade_prestart_executes_internal_call_once_but_persists_visible_call() {
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tool = Arc::new(RecordingFacadeTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&seen),
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "call-1",
        "spawn_agent",
        &runtime_json!({"task_name":"audit","message":"inspect"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("delegate")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![runtime_json!({"description":"audit","name":"audit","prompt":"inspect"})]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::ItemStarted { inference: _,
                    item: ThreadItem::ToolCall { tool_name, .. }
                } if tool_name == "task"
            ))
            .count(),
        1
    );
    let history = persisted.lock().unwrap();
    assert!(history.iter().any(|message| matches!(
        message,
        Message::Assistant(assistant) if assistant.content.iter().any(|block| {
            matches!(
                block,
                ContentBlock::ToolUse(call) if call.name == "spawn_agent"
            )
        })
    )));
    assert!(!history.iter().any(|message| matches!(
        message,
        Message::Assistant(assistant) if assistant.content.iter().any(|block| {
            matches!(block, ContentBlock::ToolUse(call) if call.name == "task")
        })
    )));
}

#[tokio::test]
async fn later_prestarted_read_can_make_earlier_model_order_write_fresh() {
    let root = std::env::temp_dir().join(format!(
        "klaus-query-prestart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("status.txt");
    std::fs::write(&path, "state=old\n").unwrap();
    let call_count = Arc::new(AtomicUsize::new(0));
    let read_completed = Arc::new(tokio::sync::Notify::new());
    let deps = deps_with(Box::new({
        let call_count = Arc::clone(&call_count);
        let read_completed = Arc::clone(&read_completed);
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let read_completed = Arc::clone(&read_completed);
            if call_count.fetch_add(1, Ordering::SeqCst) == 0 {
                Box::pin(async_stream::stream! {
                    yield ev::message_start(None);
                    for event in exact_tool_use_events(
                        "write-first",
                        "write",
                        &runtime_json!({"path":"status.txt","content":"state=new\n"}),
                    ) {
                        yield event;
                    }
                    for event in exact_tool_use_events(
                        "read-second",
                        "read",
                        &runtime_json!({"path":"status.txt"}),
                    ) {
                        yield event;
                    }
                    // The later read-only call is prestarted at ToolUseStop.
                    // Its lifecycle callback runs after the terminal event is
                    // queued, so this is a deterministic dispatch boundary.
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        read_completed.notified(),
                    )
                    .await
                    .expect("prestarted read must complete before stream stop");
                    yield ev::stop(Some(ModelStopReason::ToolUse), None);
                })
            } else {
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("done"),
                    ev::stop(None, None),
                ]))
            }
        }
    }));
    let tools = vec![
        crate::tools::builtins::write::create_write_tool(),
        crate::tools::builtins::read::create_read_tool(),
    ];
    let file_state = Arc::new(crate::tools::file_state::InMemoryFileStateCache::new());
    let cwd = root
        .to_str()
        .expect("test root is a scalar host path")
        .to_owned();
    let mut params = mk_params(vec![user_msg("write, then read")], tools);
    params.tool_context_base = Arc::new(move || {
        let mut ctx = make_test_ctx(&cwd);
        ctx.file_state = file_state.clone();
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx.network_allowed = Some(true);
        ctx
    });
    let read_completed_sink = Arc::clone(&read_completed);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        if record.tool_use_id == "read-second" && record.phase == ToolLifecyclePhase::Completed {
            read_completed_sink.notify_one();
        };
        Box::pin(async { Ok(()) })
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;
    let serialized = events
        .iter()
        .map(|event| runtime_codec::encode_value(event).unwrap())
        .collect::<Vec<_>>();
    let read_terminal = serialized
        .iter()
        .position(|event| {
            runtime_json_string_is(event.get("type"), "item.completed")
                && runtime_json_string_is(event.pointer("/item/status"), "completed")
                && runtime_json_string_is(event.pointer("/item/id"), "read-second")
        })
        .unwrap();
    let write_start = serialized
        .iter()
        .position(|event| {
            runtime_json_string_is(event.get("type"), "item.started")
                && runtime_json_string_is(event.pointer("/item/id"), "write-first")
        })
        .unwrap();
    assert!(read_terminal < write_start);
    assert_eq!(std::fs::read_to_string(path).unwrap(), "state=new\n");
    assert!(serialized.iter().any(|event| {
        runtime_json_string_is(event.get("type"), "item.completed")
            && runtime_json_string_is(event.pointer("/item/id"), "write-first")
            && runtime_json_string_is(event.pointer("/item/status"), "completed")
    }));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn tool_stop_resolution_is_first_wins_and_assembly_resolves_omitted_stop() {
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tool = Arc::new(RecordingFacadeTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&seen),
    }) as Arc<dyn Tool>;
    let first = vec![
        ev::message_start(Some("gpt-5")),
        ModelStreamEvent::ToolUseStart {
            id: "duplicate".into(),
            name: "spawn_agent".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "duplicate".into(),
            partial_json: r#"{"task_name":"first","message":"before"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "duplicate".into(),
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "duplicate".into(),
            partial_json: r#"{"task_name":"late","message":"after"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "duplicate".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: "omitted".into(),
            name: "spawn_agent".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "omitted".into(),
            partial_json: r#"{"task_name":"second","message":"assembly"}"#.into(),
        },
        ev::stop(Some(ModelStopReason::ToolUse), None),
    ];
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let mut params = mk_params(vec![user_msg("delegate twice")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert!(seen.contains(&runtime_json!({"description":"first","name":"first","prompt":"before"})));
    assert!(
        seen.contains(&runtime_json!({"description":"second","name":"second","prompt":"assembly"}))
    );
    assert!(!seen
        .iter()
        .any(|input| { runtime_json_string_is(input.get("description"), "late") }));

    for id in ["duplicate", "omitted"] {
        assert_eq!(
            lifecycle
                .lock()
                .unwrap()
                .iter()
                .filter(|record| {
                    record.tool_use_id == id && record.phase == ToolLifecyclePhase::Requested
                })
                .count(),
            1,
            "{id} requested count"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    EngineEvent::ItemStarted { inference: _,
                        item: ThreadItem::ToolCall { id: event_id, .. }
                    } if event_id == id
                ))
                .count(),
            1,
            "{id} started count"
        );
    }

    let history = persisted.lock().unwrap();
    let calls = history
        .iter()
        .find_map(|message| match message {
            Message::Assistant(assistant)
                if assistant
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse(_))) =>
            {
                Some(
                    assistant
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::ToolUse(call) => Some(call.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|call| exact_test_string(&call.id))
            .collect::<Vec<_>>(),
        vec![
            RuntimeString::from("duplicate"),
            RuntimeString::from("omitted"),
        ]
    );
    assert_eq!(calls[0].name, "spawn_agent");
    assert_eq!(
        calls[0].input,
        runtime_json!({"task_name":"first","message":"before"})
    );
    assert_eq!(calls[1].name, "spawn_agent");
    assert_eq!(
        calls[1].input,
        runtime_json!({"task_name":"second","message":"assembly"})
    );
}

#[tokio::test]
async fn duplicate_tool_use_start_keeps_first_assembly_and_dispatches_once() {
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tool = Arc::new(RecordingFacadeTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&seen),
    }) as Arc<dyn Tool>;
    let first = vec![
        ev::message_start(Some("gpt-5")),
        ModelStreamEvent::ToolUseStart {
            id: "duplicate-start".into(),
            name: "spawn_agent".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "duplicate-start".into(),
            partial_json: r#"{"task_name":"first","message":"before"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "duplicate-start".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: "duplicate-start".into(),
            name: "spawn_agent".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "duplicate-start".into(),
            partial_json: r#"{"task_name":"late","message":"after"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "duplicate-start".into(),
        },
        ev::stop(Some(ModelStopReason::ToolUse), None),
    ];
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("delegate")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![runtime_json!({"description":"first","name":"first","prompt":"before"})]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::ItemStarted { inference: _,
                    item: ThreadItem::ToolCall { id, .. }
                } if id == "duplicate-start"
            ))
            .count(),
        1
    );
    let persisted = persisted.lock().unwrap();
    let calls = persisted
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::ToolUse(call) if call.id == "duplicate-start" => Some(call),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "spawn_agent");
    assert_eq!(
        calls[0].input,
        runtime_json!({"task_name":"first","message":"before"})
    );
}

#[tokio::test]
async fn a_stop_mid_stream_persists_the_text_the_model_already_produced() {
    // The assistant message is normally recorded once the stream assembles. A stop
    // returns before that, so without an explicit record the partial reply the user
    // is looking at exists only in the client's live layer and disappears the first
    // time the transcript is read back from history.
    let turn_signal = AbortSignal::new();
    let deps = deps_with(Box::new({
        let turn_signal = turn_signal.clone();
        move |_request: ModelRequest, _attempt: AbortSignal| -> ModelStream {
            let turn_signal = turn_signal.clone();
            Box::pin(async_stream::stream! {
                yield ev::message_start(None);
                yield ModelStreamEvent::TextDelta { text: "half an ans".into() };
                turn_signal.cancel();
                yield ModelStreamEvent::Error {
                    error: ProviderError {
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false, credential_recovery: None,
                        kind: ProviderErrorKind::Network,
                        message: "stream closed".into(),
                        status: None,
                        retry_after_ms: None,
                        request_id: None,
                    },
                };
            })
        }
    }));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("go")], Vec::new());
    params.signal = turn_signal;
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        })
    ));
    let history = persisted.lock().unwrap();
    let texts = history
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(texts, vec![RuntimeString::from("half an ans")]);
    // A tool call the stop cut off has no result to pair with, so it must NOT be
    // recorded: a dangling tool_use is rejected on the next request.
    assert!(!history.iter().any(|message| matches!(
        message,
        Message::Assistant(assistant)
            if assistant.content.iter().any(|block| matches!(block, ContentBlock::ToolUse(_)))
    )));
}

#[tokio::test]
async fn cancelled_omitted_stop_remains_aborted_after_repeated_tool_calls() {
    let turn_signal = AbortSignal::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let turn_signal = turn_signal.clone();
        let call_count = Arc::clone(&call_count);
        move |_request: ModelRequest, _attempt: AbortSignal| -> ModelStream {
            if call_count.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut events = vec![ev::message_start(None)];
                for index in 0..5 {
                    events.extend(exact_tool_use_events(
                        &format!("loop-{index}"),
                        "missing_loop_tool",
                        &runtime_json!({}),
                    ));
                }
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                Box::pin(futures::stream::iter(events))
            } else {
                let turn_signal = turn_signal.clone();
                Box::pin(async_stream::stream! {
                    yield ev::message_start(None);
                    yield ModelStreamEvent::ToolUseStart {
                        id: "cancelled-omitted".into(),
                        name: "missing_loop_tool".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    };
                    yield ModelStreamEvent::ToolUseInputDelta {
                        id: "cancelled-omitted".into(),
                        partial_json: "{}".into(),
                    };
                    turn_signal.cancel();
                    yield ev::stop(Some(ModelStopReason::ToolUse), None);
                })
            }
        }
    }));
    let mut params = mk_params(vec![user_msg("repeat then cancel")], Vec::new());
    params.signal = turn_signal;
    params.config.loop_detection = true;

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::LoopDetected
    )));
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        })
    ));
}

#[tokio::test]
async fn repeated_wait_calls_continue_to_a_following_inspection() {
    let wait_round = |prefix: &str, count: usize| {
        let mut events = vec![ev::message_start(None)];
        for index in 0..count {
            events.extend(exact_tool_use_events(
                &format!("{prefix}-{index}"),
                "browser_computer",
                &runtime_json!({ "action": "wait", "duration": 2 }),
            ));
        }
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        events
    };
    let mut inspect = vec![ev::message_start(None)];
    inspect.extend(exact_tool_use_events(
        "inspect-page",
        "browser_read_page",
        &runtime_json!({}),
    ));
    inspect.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("finish the document")], Vec::new());
    params.config.loop_detection = true;

    let events = query(
        params,
        deps_with(scripted_call_model(vec![
            wait_round("first", 1),
            wait_round("second", 2),
            wait_round("third", 2),
            wait_round("after-nudge", 1),
            inspect,
            vec![
                ev::message_start(None),
                ev::text("The page was inspected with a different method."),
                ev::stop(Some(ModelStopReason::EndTurn), None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. } if error.code == EngineErrorCode::LoopDetected
    )));
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn repeated_tool_calls_persist_every_result_and_complete() {
    let repeated_turn = |prefix: &str| {
        let mut events = vec![ev::message_start(None)];
        for index in 0..5 {
            events.extend(exact_tool_use_events(
                &format!("{prefix}-{index}"),
                "missing_loop_tool",
                &runtime_json!({}),
            ));
        }
        events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        events
    };
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("repeat twice")], Vec::new());
    params.config.loop_detection = true;
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(
        params,
        deps_with(scripted_call_model(vec![
            repeated_turn("first"),
            repeated_turn("second"),
            vec![
                ev::message_start(None),
                ev::text("All tool results were received."),
                ev::stop(Some(ModelStopReason::EndTurn), None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::LoopDetected
    )));
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        })
    ));
    assert!(!events.iter().any(|event| matches!(event, EngineEvent::Notification { message, .. } if message.contains("Possible loop detected"))));
    let persisted = persisted.lock().unwrap();
    for index in 0..5 {
        let id = format!("second-{index}");
        assert_eq!(
            persisted
                .iter()
                .filter(|message| matches!(
                    message,
                    Message::ToolResult(result) if result.tool_use_id == id
                ))
                .count(),
            1,
            "orphan protection for {id}"
        );
    }
}

#[derive(Clone)]
struct ResolvedIdentityCase {
    name: &'static str,
    pre_hook_outcome: HookOutcome,
    permission_response: RecordingPermissionResponse,
    approval_response: Option<ApprovalResponse>,
    validation_error: Option<&'static str>,
    include_tool: bool,
    tool_outcome: IdentityToolOutcome,
    expected_pre_hook: Option<Json>,
    expected_permission: Option<Json>,
    expected_execution: Option<Json>,
    expected_post_hook: Option<(HookEvent, Json)>,
    expected_approval: Option<Json>,
    expected_terminal_status: ItemStatus,
    expected_terminal_error: Option<&'static str>,
}

#[derive(Debug)]
struct ResolvedIdentityTrace {
    started_count: usize,
    started: (RuntimeString, Json),
    terminal_count: usize,
    terminal: (RuntimeString, Json, ItemStatus, Option<RuntimeString>),
    pre_hook: Option<(RuntimeString, Json)>,
    permission: Option<(RuntimeString, Json)>,
    execution: Option<Json>,
    post_hook: Option<(HookEvent, RuntimeString, Json)>,
    approval: Option<(RuntimeString, Json)>,
    lifecycle: Vec<(ToolLifecyclePhase, RuntimeString, Option<Json>)>,
    persisted_visible_call: ToolUseBlock,
    persisted_result: Option<ToolResultMessage>,
}

fn resolved_identity_cases() -> Vec<ResolvedIdentityCase> {
    let initial = runtime_json!({"description":"inspect","name":"inspect","prompt":"initial"});
    let hook_rewrite = runtime_json!({"description":"hook","prompt":"effective"});
    let permission_rewrite = runtime_json!({"description":"permission","prompt":"effective"});
    let approval_candidate = runtime_json!({"description":"approval","prompt":"candidate"});
    let approval_rewrite = runtime_json!({"description":"approval","prompt":"effective"});

    vec![
        ResolvedIdentityCase {
            name: "plain",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(initial.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, initial.clone())),
            expected_approval: None,
            expected_terminal_status: ItemStatus::Completed,
            expected_terminal_error: None,
        },
        ResolvedIdentityCase {
            name: "pre_hook_rewrite",
            pre_hook_outcome: HookOutcome {
                updated_input: Some(hook_rewrite.clone()),
                ..HookOutcome::default()
            },
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(hook_rewrite.clone()),
            expected_execution: Some(hook_rewrite.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, hook_rewrite)),
            expected_approval: None,
            expected_terminal_status: ItemStatus::Completed,
            expected_terminal_error: None,
        },
        ResolvedIdentityCase {
            name: "permission_rewrite",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::AllowRewrite(
                permission_rewrite.clone(),
            ),
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(permission_rewrite.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, permission_rewrite)),
            expected_approval: None,
            expected_terminal_status: ItemStatus::Completed,
            expected_terminal_error: None,
        },
        ResolvedIdentityCase {
            name: "approval_rewrite",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Ask(approval_candidate.clone()),
            approval_response: Some(ApprovalResponse::Allow {
                updated_input: Some(approval_rewrite.clone()),
                updated_permissions: None,
            }),
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(approval_rewrite.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, approval_rewrite)),
            expected_approval: Some(approval_candidate.clone()),
            expected_terminal_status: ItemStatus::Completed,
            expected_terminal_error: None,
        },
        ResolvedIdentityCase {
            name: "approval_nullish_fallback",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Ask(approval_candidate.clone()),
            approval_response: Some(ApprovalResponse::Allow {
                updated_input: Some(Json::Null),
                updated_permissions: None,
            }),
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(approval_candidate.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, approval_candidate.clone())),
            expected_approval: Some(approval_candidate),
            expected_terminal_status: ItemStatus::Completed,
            expected_terminal_error: None,
        },
        ResolvedIdentityCase {
            name: "permission_deny",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Deny,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: None,
            expected_post_hook: None,
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("Permission denied: policy denied"),
        },
        ResolvedIdentityCase {
            name: "hook_block",
            pre_hook_outcome: HookOutcome {
                decision: Some(HookDecision::Block),
                permission_reason: Some("hook blocked".into()),
                ..HookOutcome::default()
            },
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: None,
            expected_execution: None,
            expected_post_hook: None,
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("hook blocked"),
        },
        ResolvedIdentityCase {
            name: "hook_abort",
            pre_hook_outcome: HookOutcome {
                result: Some(HookResult::FailedAbort),
                system_message: Some("hook aborted".into()),
                ..HookOutcome::default()
            },
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: None,
            expected_execution: None,
            expected_post_hook: None,
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("hook aborted"),
        },
        ResolvedIdentityCase {
            name: "validation_failure",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: Some("identity validation failed"),
            include_tool: true,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: None,
            expected_permission: None,
            expected_execution: None,
            expected_post_hook: None,
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("identity validation failed"),
        },
        ResolvedIdentityCase {
            name: "unknown_tool",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: false,
            tool_outcome: IdentityToolOutcome::Ok,
            expected_pre_hook: None,
            expected_permission: None,
            expected_execution: None,
            expected_post_hook: None,
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("Unknown tool: task"),
        },
        ResolvedIdentityCase {
            name: "ok_is_error",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::OkIsError,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(initial.clone()),
            expected_post_hook: Some((HookEvent::PostToolUse, initial.clone())),
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("identity error"),
        },
        ResolvedIdentityCase {
            name: "nonfatal_err",
            pre_hook_outcome: HookOutcome::default(),
            permission_response: RecordingPermissionResponse::Allow,
            approval_response: None,
            validation_error: None,
            include_tool: true,
            tool_outcome: IdentityToolOutcome::NonfatalErr,
            expected_pre_hook: Some(initial.clone()),
            expected_permission: Some(initial.clone()),
            expected_execution: Some(initial.clone()),
            expected_post_hook: Some((HookEvent::PostToolUseFailure, initial)),
            expected_approval: None,
            expected_terminal_status: ItemStatus::Failed,
            expected_terminal_error: Some("identity tool failed"),
        },
    ]
}

async fn run_resolved_identity_case(case: &ResolvedIdentityCase) -> ResolvedIdentityTrace {
    let execution = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tools: Vec<Arc<dyn Tool>> = if case.include_tool {
        vec![Arc::new(IdentityTool {
            schema: JsonSchema::new(),
            seen: Arc::clone(&execution),
            validation_error: case.validation_error,
            outcome: case.tool_outcome,
        })]
    } else {
        Vec::new()
    };

    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let hook_inputs = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let approval_requests = Arc::new(Mutex::new(Vec::<ApprovalRequest>::new()));
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let visible_call = ToolUseBlock {
        id: "call-1".into(),
        name: "spawn_agent".into(),
        input: runtime_json!({"task_name":"inspect","message":"initial"}),
        thought_signature: None,
        tool_kind: None,
    };

    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "call-1",
        "spawn_agent",
        &visible_call.input,
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];

    let mut params = mk_params(vec![user_msg("delegate")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: case.permission_response.clone(),
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hook_inputs),
                case.pre_hook_outcome.clone(),
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hook_inputs),
                HookOutcome::default(),
            ),
            recording_hook(
                HookEvent::PostToolUseFailure,
                Arc::clone(&hook_inputs),
                HookOutcome::default(),
            ),
        ],
    )));
    if let Some(response) = case.approval_response.clone() {
        params.approval_handler = Some(Arc::new(StaticApproval {
            requests: Arc::clone(&approval_requests),
            response,
        }));
    }
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    let started = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::ItemStarted {
                inference: _,
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        arguments,
                        ..
                    },
            } if id == &visible_call.id => Some((exact_test_string(tool_name), arguments.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let terminal = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::ItemCompleted {
                inference: _,
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        arguments,
                        status,
                        error,
                        ..
                    },
            }
            | EngineEvent::ItemUpdated {
                inference: _,
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        arguments,
                        status,
                        error,
                        ..
                    },
            } if id == &visible_call.id => Some((
                exact_test_string(tool_name),
                arguments.clone(),
                *status,
                error
                    .as_ref()
                    .map(|error| exact_test_string(&error.message)),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();

    let hook_inputs = hook_inputs.lock().unwrap().clone();
    let hook_value = |event| {
        hook_inputs
            .iter()
            .find(|input| input.event == event)
            .map(|input| {
                (
                    exact_test_string(input.tool_name.as_ref().unwrap()),
                    input.tool_input.clone().unwrap(),
                )
            })
    };
    let pre_hook = hook_value(HookEvent::PreToolUse);
    let post_hook = [HookEvent::PostToolUse, HookEvent::PostToolUseFailure]
        .into_iter()
        .find_map(|event| hook_value(event).map(|(name, input)| (event, name, input)));
    let permission = permission.lock().unwrap().first().cloned();
    let execution = execution.lock().unwrap().first().cloned();
    let approval = approval_requests
        .lock()
        .unwrap()
        .first()
        .map(|request| (exact_test_string(&request.tool_name), request.input.clone()));
    let lifecycle = lifecycle
        .lock()
        .unwrap()
        .iter()
        .map(|record| {
            (
                record.phase,
                exact_test_string(&record.tool_name),
                record.arguments.clone(),
            )
        })
        .collect();
    let persisted = persisted.lock().unwrap();
    let persisted_visible_call = persisted
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .find_map(|block| match block {
            ContentBlock::ToolUse(call) if call.id == visible_call.id => Some(call.clone()),
            _ => None,
        })
        .unwrap();
    let persisted_result = persisted.iter().find_map(|message| match message {
        Message::ToolResult(result) if result.tool_use_id == visible_call.id => {
            Some(result.clone())
        }
        _ => None,
    });

    ResolvedIdentityTrace {
        started_count: started.len(),
        started: started.into_iter().next().unwrap(),
        terminal_count: terminal.len(),
        terminal: terminal.into_iter().next().unwrap(),
        pre_hook,
        permission,
        execution,
        post_hook,
        approval,
        lifecycle,
        persisted_visible_call,
        persisted_result,
    }
}

#[tokio::test]
async fn resolved_call_event_identity_matrix() {
    let cases = resolved_identity_cases();
    assert_eq!(cases.len(), 12);
    let initial = runtime_json!({"description":"inspect","name":"inspect","prompt":"initial"});
    let visible_call = ToolUseBlock {
        id: "call-1".into(),
        name: "spawn_agent".into(),
        input: runtime_json!({"task_name":"inspect","message":"initial"}),
        thought_signature: None,
        tool_kind: None,
    };

    for case in &cases {
        let trace = run_resolved_identity_case(case).await;
        assert_eq!(trace.started_count, 1, "{} started count", case.name);
        assert_eq!(
            trace.started,
            (RuntimeString::from("task"), initial.clone()),
            "{} started identity",
            case.name
        );
        assert_eq!(trace.terminal_count, 1, "{} terminal count", case.name);
        assert_eq!(
            trace.terminal,
            (
                RuntimeString::from("task"),
                initial.clone(),
                case.expected_terminal_status,
                case.expected_terminal_error.map(RuntimeString::from),
            ),
            "{} terminal identity",
            case.name
        );
        assert_eq!(
            trace.pre_hook,
            case.expected_pre_hook
                .clone()
                .map(|input| (RuntimeString::from("task"), input)),
            "{} pre-hook identity",
            case.name
        );
        assert_eq!(
            trace.permission,
            case.expected_permission
                .clone()
                .map(|input| (RuntimeString::from("task"), input)),
            "{} permission identity",
            case.name
        );
        assert_eq!(
            trace.execution, case.expected_execution,
            "{} execution identity",
            case.name
        );
        assert_eq!(
            trace.post_hook,
            case.expected_post_hook
                .clone()
                .map(|(event, input)| (event, "task".into(), input)),
            "{} post-hook identity",
            case.name
        );
        assert_eq!(
            trace.approval,
            case.expected_approval
                .clone()
                .map(|input| (RuntimeString::from("task"), input)),
            "{} approval identity",
            case.name
        );
        assert!(
            trace
                .lifecycle
                .iter()
                .all(|(_, tool_name, _)| tool_name == "task"),
            "{} lifecycle tool identity: {:?}",
            case.name,
            trace.lifecycle
        );
        for phase in [ToolLifecyclePhase::Requested, ToolLifecyclePhase::Started] {
            assert_eq!(
                trace
                    .lifecycle
                    .iter()
                    .find(|(actual, _, _)| *actual == phase)
                    .and_then(|(_, _, arguments)| arguments.clone()),
                Some(initial.clone()),
                "{} {phase:?} lifecycle arguments",
                case.name
            );
        }
        if case.expected_terminal_status == ItemStatus::Failed {
            assert_eq!(
                trace
                    .lifecycle
                    .iter()
                    .rev()
                    .find(|(phase, _, _)| *phase == ToolLifecyclePhase::Failed)
                    .and_then(|(_, _, arguments)| arguments.clone()),
                Some(initial.clone()),
                "{} failed lifecycle arguments",
                case.name
            );
        }
        if let Some(expected) = case
            .expected_execution
            .as_ref()
            .filter(|_| case.expected_approval.is_some())
        {
            assert_eq!(
                trace
                    .lifecycle
                    .iter()
                    .find(|(phase, _, _)| *phase == ToolLifecyclePhase::Approved)
                    .and_then(|(_, _, arguments)| arguments.as_ref()),
                Some(expected),
                "{} approved lifecycle arguments",
                case.name
            );
        }
        assert_eq!(
            trace.persisted_visible_call, visible_call,
            "{} visible history identity",
            case.name
        );
    }
}

#[tokio::test]
async fn real_query_error_producers_render_exactly_with_visible_name_and_request_profile() {
    let expected = [
        (
            "permission_deny",
            "Tool error from `spawn_agent`:\nPermission denied: policy denied",
        ),
        (
            "unknown_tool",
            "Tool error from `spawn_agent`:\nUnknown tool: task",
        ),
        (
            "ok_is_error",
            "Tool error from `spawn_agent`:\nidentity error",
        ),
        (
            "nonfatal_err",
            "Tool error from `spawn_agent`:\nidentity tool failed",
        ),
    ];

    for (case_name, exact_text) in expected {
        let case = resolved_identity_cases()
            .into_iter()
            .find(|case| case.name == case_name)
            .unwrap();
        let trace = run_resolved_identity_case(&case).await;
        let result = trace.persisted_result.expect("persisted tool result");
        assert!(result.is_error, "{case_name} is_error");
        assert_eq!(first_text(result.content), exact_text, "{case_name}");
    }
}

#[tokio::test]
async fn real_query_subagent_success_renders_with_visible_name_and_request_profile() {
    let case = resolved_identity_cases()
        .into_iter()
        .find(|case| case.name == "plain")
        .unwrap();

    let trace = run_resolved_identity_case(&case).await;

    let result = trace.persisted_result.expect("persisted tool result");
    assert!(!result.is_error);
    assert_eq!(
        first_text(result.content),
        "# Sub-Agent Report\n\nidentity ok"
    );
}

#[tokio::test]
async fn real_query_command_success_renders_with_visible_name_and_request_profile() {
    let tool = Arc::new(SourceTool {
        name: "exec_command",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Direct,
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "command-1",
        "exec_command",
        &runtime_json!({"cmd":"pwd"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("run pwd")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    let persisted = persisted.lock().unwrap();
    let result = persisted_tool_result(&persisted, "command-1");
    assert!(!result.is_error);
    assert_eq!(first_text(result.content), "# Command Output\n\ndone");
}

#[tokio::test]
async fn read_result_over_default_limit_remains_unspilled() {
    let raw = "😀".repeat(25_001);
    assert_eq!(raw.encode_utf16().count(), 50_002);
    let spill_path = std::path::Path::new("/tmp/.klaus/tool-results/read-no-spill.txt");
    std::fs::remove_file(spill_path).ok();
    let tool = Arc::new(ResultShapeTool {
        name: "read",
        schema: JsonSchema::new(),
        text: raw.clone(),
        max_chars: None,
        output: runtime_json!({"path":"large.txt"}),
        extra_user_blocks: None,
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "read-no-spill",
        "read",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("read the large file")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.config.aggregate_tool_results = true;
    params.config.max_tool_results_per_message_chars = 1_000_000;
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(
        params,
        deps_with(scripted_call_model(vec![
            first,
            vec![
                ev::message_start(Some("gpt-5")),
                ev::text("done"),
                ev::stop(None, None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    let result = persisted_tool_result(&persisted.lock().unwrap(), "read-no-spill");
    assert_eq!(output_blocks_text(&result.content), raw);
    assert!(!spill_path.exists());
}

#[tokio::test]
async fn tool_result_override_spills_and_apply_patch_success_remains_plain() {
    let spill_path = std::path::Path::new("/tmp/.klaus/tool-results/override-result.txt");
    std::fs::remove_file(spill_path).ok();
    let raw_seen = Arc::new(Mutex::new(Vec::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(ResultShapeTool {
            name: "limited_output",
            schema: JsonSchema::new(),
            text: "x".repeat(101),
            max_chars: Some(10),
            output: runtime_json!({"limited":true}),
            extra_user_blocks: None,
        }),
        Arc::new(RawRecordingTool {
            schema: JsonSchema::new(),
            seen: Arc::clone(&raw_seen),
        }),
    ];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "override-result",
        "limited_output",
        &runtime_json!({}),
    ));
    first.extend([
        ModelStreamEvent::ToolUseStart {
            id: "patch-result".into(),
            name: "apply_patch".into(),
            thought_signature: None,
            tool_kind: Some(ProviderToolCallKind::Custom),
            input_format: Some(ToolInputFormat::Raw),
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "patch-result".into(),
            partial_json: "*** Begin Patch\n*** End Patch".into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "patch-result".into(),
        },
    ]);
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("shape both results")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.config.aggregate_tool_results = true;
    params.config.max_tool_results_per_message_chars = 1_000_000;
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(
        params,
        deps_with(scripted_call_model(vec![
            first,
            vec![
                ev::message_start(Some("gpt-5")),
                ev::text("done"),
                ev::stop(None, None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    let persisted = persisted.lock().unwrap();
    let limited = persisted_tool_result(&persisted, "override-result");
    let limited_text = output_blocks_text(&limited.content);
    assert_eq!(
        limited_text,
        format!(
            "xxxxxxxxxx\n\n# Truncated Tool Output\n\nThe full output is 101 chars and was saved to {}. Read that file only if the omitted content is needed for the task.",
            // The spill file is joined in the host dialect and reported POSIX-rendered,
            // which is the shape the engine promises the model on every host.
            model_facing_path(spill_path)
        )
    );
    assert_eq!(
        std::fs::read_to_string(spill_path).unwrap(),
        "x".repeat(101)
    );
    let patch = persisted_tool_result(&persisted, "patch-result");
    assert_eq!(output_blocks_text(&patch.content), "patch applied");
    assert!(!output_blocks_text(&patch.content).contains("Command Output"));
    assert!(!output_blocks_text(&patch.content).contains("Sub-Agent Report"));
    assert_eq!(
        *raw_seen.lock().unwrap(),
        vec![runtime_json!({"patch":"*** Begin Patch\n*** End Patch"})]
    );
    std::fs::remove_file(spill_path).ok();
}

#[tokio::test]
async fn steering_persists_only_final_transformed_tool_results_once() {
    let raw = "😀".repeat(25_001);
    let spill_path = std::path::Path::new("/tmp/.klaus/tool-results/command-pipeline.txt");
    std::fs::remove_file(spill_path).ok();
    let native_output = runtime_json!({"tools":[{"name":"calendar"}]});
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(ResultShapeTool {
            name: "exec_command",
            schema: JsonSchema::new(),
            text: "pre-hook command output".to_string(),
            max_chars: None,
            output: runtime_json!({"stdout":"stable"}),
            extra_user_blocks: None,
        }),
        Arc::new(ResultShapeTool {
            name: "tool_search",
            schema: JsonSchema::new(),
            text: "search shaped".to_string(),
            max_chars: None,
            output: native_output.clone(),
            extra_user_blocks: Some(vec![ContentBlock::Text(TextBlock {
                text: "attached context".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]),
        }),
    ];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "command-pipeline",
        "exec_command",
        &runtime_json!({"cmd":"large"}),
    ));
    first.extend(provider_tool_use_events(
        "search-pipeline",
        "tool_search",
        &runtime_json!({"query":"calendar"}),
        Some(ProviderToolCallKind::ToolSearch),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let scripted = Arc::new(Mutex::new(vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ]));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let scripted = Arc::clone(&scripted);
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            let events = scripted.lock().unwrap().remove(0);
            Box::pin(futures::stream::iter(events))
        }
    }));
    let queue = Arc::new(MessageQueue::new());
    queue
        .enqueue(QueuedMessage {
            trigger_turn: None,
            value: QueuedValue::Text("steer after tools".into()),
            mode: "prompt".into(),
            priority: Some(QueuePriority::Next),
            uuid: Some("steer-1".into()),
            session_id: Some("thread-queue-test".into()),
            is_meta: None,
            origin: Some("user".into()),
            agent_id: None,
            correlation_id: Some("steer-correlation".into()),
            pdf_bytes: 0,
            pdf_documents: 0,
            delivery: None,
            display_metadata: None,
        })
        .unwrap();
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("run and search")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.config.aggregate_tool_results = true;
    params.config.max_tool_results_per_message_chars = 1_000;
    params.message_queue = Some(queue);
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![Hook {
            event: HookEvent::PostToolUse,
            matcher: Some("exec_command".to_string()),
            handler: HookHandler::Fn(Arc::new(RecordingHook {
                seen: Arc::new(Mutex::new(Vec::new())),
                outcome: HookOutcome {
                    updated_tool_output: Some(Json::String(raw.clone().into())),
                    ..Default::default()
                },
            })),
            is_async: false,
        }],
    )));
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps).collect::<Vec<_>>().await;

    assert_eq!(std::fs::read_to_string(spill_path).unwrap(), raw);
    let persisted = persisted.lock().unwrap();
    let results = persisted
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    for id in ["command-pipeline", "search-pipeline"] {
        assert_eq!(
            results
                .iter()
                .filter(|result| result.tool_use_id == id)
                .count(),
            1,
            "{id} must be persisted only after final shaping"
        );
    }
    let command = results
        .iter()
        .copied()
        .find(|result| result.tool_use_id == "command-pipeline")
        .unwrap();
    let command_text = output_blocks_text(&command.content);
    let rendered_before_trim = format!(
        "# Command Output\n\n{}\n\n# Truncated Tool Output\n\nThe full output is 50002 chars and was saved to {}. Read that file only if the omitted content is needed for the task.",
        "😀".repeat(1_000),
        spill_path
            .to_str()
            .expect("test spill path is a scalar host path")
    );
    assert_eq!(command_text, rendered_before_trim);
    assert_eq!(command.output, Some(runtime_json!({"stdout":"stable"})));
    let search = results
        .iter()
        .copied()
        .find(|result| result.tool_use_id == "search-pipeline")
        .unwrap();
    assert!(output_blocks_text(&search.content).contains("steer after tools"));
    assert_eq!(search.tool_kind, Some(ProviderToolCallKind::ToolSearch));
    assert_eq!(search.native_tool_output, Some(native_output.clone()));
    assert_eq!(search.output, Some(native_output));
    assert_eq!(
        search.extra_user_blocks,
        Some(vec![ContentBlock::Text(TextBlock {
            text: "attached context".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })])
    );

    let requests = requests.lock().unwrap();
    let next_results = requests[1]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(next_results, results);
    std::fs::remove_file(spill_path).ok();
}

async fn run_steering_queue_durability_failure(
    failed_checkpoint: usize,
) -> (
    Arc<MessageQueue>,
    Vec<EngineEvent>,
    Vec<Message>,
    usize,
    usize,
) {
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ResultShapeTool {
        name: "exec_command",
        schema: JsonSchema::new(),
        text: "tool output".to_string(),
        max_chars: None,
        output: runtime_json!({"stdout":"stable"}),
        extra_user_blocks: None,
    })];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "queue-durability-tool",
        "exec_command",
        &runtime_json!({"cmd":"true"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let requests = Arc::new(AtomicUsize::new(0));
    let scripted = Arc::new(Mutex::new(vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("must not be requested after queue failure"),
            ev::stop(None, None),
        ],
    ]));
    let deps = deps_with(Box::new({
        let requests = requests.clone();
        let scripted = scripted.clone();
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(scripted.lock().unwrap().remove(0)))
        }
    }));

    // Establish one authoritative pending item before installing the
    // deterministic failing rollout seam. Query then owns checkpoint 1
    // (reserve) and checkpoint 2 (durable consumption).
    let queue = Arc::new(MessageQueue::for_session("query-queue-failure".into()));
    queue
        .enqueue(QueuedMessage {
            trigger_turn: None,
            value: QueuedValue::Text("durable queued input".into()),
            mode: "prompt".into(),
            priority: Some(QueuePriority::Next),
            uuid: Some("query-queue-item".into()),
            session_id: Some("query-queue-failure".into()),
            is_meta: None,
            origin: Some("user".into()),
            agent_id: None,
            correlation_id: Some("query-queue-correlation".into()),
            pdf_bytes: 0,
            pdf_documents: 0,
            delivery: None,
            display_metadata: None,
        })
        .unwrap();
    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sink_calls_for_receipt = sink_calls.clone();
    queue.set_persistence_sink(Arc::new(move |_| {
        let call = sink_calls_for_receipt.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            if call == failed_checkpoint {
                Err(RuntimeString::from(format!(
                    "forced query queue checkpoint {call} failure"
                )))
            } else {
                Ok(())
            }
        })
    }));

    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("run one tool")], tools);
    params.model = "gpt-5".into();
    params.message_queue = Some(queue.clone());
    let persisted_for_sink = persisted.clone();
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_for_sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;
    let persisted = persisted.lock().unwrap().clone();
    (
        queue,
        events,
        persisted,
        sink_calls.load(Ordering::SeqCst),
        requests.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn steering_reserve_durability_failure_fails_turn_without_input_ack() {
    let (queue, events, persisted, sink_calls, requests) =
        run_steering_queue_durability_failure(1).await;

    assert_eq!(sink_calls, 1);
    assert_eq!(requests, 1);
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::InputApplied { .. })));
    let failure = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => error.message.try_to_utf8().ok(),
            _ => None,
        })
        .expect("reserve durability failure must terminate the turn");
    assert!(failure.contains("reserving queued input"), "{failure}");
    assert!(!persisted
        .iter()
        .any(|message| matches!(message, Message::ToolResult(_))));

    let snapshot = queue.queue_snapshot();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.status, QueueSnapshotStatus::PersistenceFailed);
    assert_eq!(snapshot.items.len(), 1);
    assert_eq!(snapshot.items[0].state, QueueItemState::Pending);
    assert_eq!(
        snapshot.items[0].message.value,
        QueuedValue::Text("durable queued input".into())
    );
}

#[tokio::test]
async fn steering_consume_durability_failure_never_emits_input_applied() {
    let (queue, events, persisted, sink_calls, requests) =
        run_steering_queue_durability_failure(2).await;

    assert_eq!(sink_calls, 2);
    assert_eq!(requests, 1);
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::InputApplied { .. })));
    let failure = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => error.message.try_to_utf8().ok(),
            _ => None,
        })
        .expect("consume durability failure must terminate the turn");
    assert!(
        failure.contains("committing queued input consumption"),
        "{failure}"
    );
    assert!(persisted.iter().any(|message| matches!(
        message,
        Message::ToolResult(result) if result.tool_use_id == "queue-durability-tool"
    )));

    let snapshot = queue.queue_snapshot();
    assert_eq!(snapshot.revision, 2);
    assert_eq!(snapshot.status, QueueSnapshotStatus::PersistenceFailed);
    assert_eq!(snapshot.items.len(), 1);
    assert_eq!(snapshot.items[0].state, QueueItemState::Reserved);
    assert_eq!(
        snapshot.items[0].message.value,
        QueuedValue::Text("durable queued input".into())
    );
}

fn provider_tool_use_events(
    id: &str,
    name: &str,
    input: &Json,
    tool_kind: Option<ProviderToolCallKind>,
) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}

fn exact_tool_use_events(id: &str, name: &str, input: &Json) -> Vec<ModelStreamEvent> {
    provider_tool_use_events(id, name, input, None)
}

/// A tool call whose arguments arrive as RAW text rather than a valid JSON
/// document — what a provider stream looks like when the output ceiling cut the
/// call off mid-argument.
///
/// Includes finalization: adapters can send `ToolUseStop` even when the arguments
/// are incomplete. The original parse failure must survive that event.
fn raw_tool_use_events(id: &str, name: &str, raw_arguments: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: RuntimeString::from(raw_arguments),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}

fn persisted_tool_call(messages: &[Message], id: &str) -> ToolUseBlock {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .find_map(|block| match block {
            ContentBlock::ToolUse(call) if call.id == id => Some(call.clone()),
            _ => None,
        })
        .unwrap()
}

fn persisted_tool_result(messages: &[Message], id: &str) -> ToolResultMessage {
    messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result.clone()),
            _ => None,
        })
        .unwrap()
}

struct CancelledFacadeTrace {
    events: Vec<EngineEvent>,
    lifecycle: Vec<ToolLifecycleRecord>,
    messages: Vec<Message>,
    identity_seen: Vec<Json>,
}

async fn run_cancelled_facade_and_search() -> CancelledFacadeTrace {
    let identity_seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(CancelTurnTool {
            schema: JsonSchema::new(),
        }),
        Arc::new(IdentityTool {
            schema: JsonSchema::new(),
            seen: Arc::clone(&identity_seen),
            validation_error: None,
            outcome: IdentityToolOutcome::Ok,
        }),
        Arc::new(NativeSearchRecordingTool {
            schema: JsonSchema::new(),
            output: native_search_output_fixture(),
        }),
    ];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(provider_tool_use_events(
        "cancel",
        "cancel_turn",
        &runtime_json!({}),
        None,
    ));
    first.extend(provider_tool_use_events(
        "facade",
        "spawn_agent",
        &runtime_json!({"task_name":"queued","message":"later"}),
        None,
    ));
    first.extend(provider_tool_use_events(
        "search-cancel",
        "tool_search",
        &runtime_json!({"query":"calendar"}),
        Some(ProviderToolCallKind::ToolSearch),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let mut params = mk_params(vec![user_msg("cancel pending calls")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let message_sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        message_sink.lock().unwrap().push(message);
    }));
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;
    let lifecycle = lifecycle.lock().unwrap().clone();
    let messages = messages.lock().unwrap().clone();
    let identity_seen = identity_seen.lock().unwrap().clone();
    CancelledFacadeTrace {
        events,
        lifecycle,
        messages,
        identity_seen,
    }
}

#[tokio::test]
async fn cancelled_unstarted_facade_call_keeps_internal_lifecycle_and_explicit_result_kind() {
    let trace = run_cancelled_facade_and_search().await;
    let initial = runtime_json!({"description":"queued","name":"queued","prompt":"later"});
    let facade_lifecycle = trace
        .lifecycle
        .iter()
        .filter(|record| record.tool_use_id == "facade")
        .collect::<Vec<_>>();

    assert_eq!(
        facade_lifecycle
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![ToolLifecyclePhase::Requested, ToolLifecyclePhase::Cancelled]
    );
    assert!(facade_lifecycle.iter().all(|record| {
        record.tool_name == "task" && record.arguments.as_ref() == Some(&initial)
    }));
    assert!(trace.identity_seen.is_empty());
    for id in ["facade", "search-cancel"] {
        assert!(!trace.events.iter().any(|event| matches!(
            event,
            EngineEvent::ItemStarted { inference: _,
                item: ThreadItem::ToolCall { id: event_id, .. }
            } if event_id == id
        )));
    }

    let visible = persisted_tool_call(&trace.messages, "facade");
    assert_eq!(visible.name, "spawn_agent");
    assert_eq!(
        visible.input,
        runtime_json!({"task_name":"queued","message":"later"})
    );
    let facade_result = persisted_tool_result(&trace.messages, "facade");
    assert!(facade_result.is_error);
    assert_eq!(facade_result.tool_kind, None);
    assert_eq!(facade_result.native_tool_output, None);
    assert_eq!(facade_result.output, None);

    let search_result = persisted_tool_result(&trace.messages, "search-cancel");
    assert!(search_result.is_error);
    assert_eq!(
        search_result.tool_kind,
        Some(ProviderToolCallKind::ToolSearch)
    );
    assert_eq!(search_result.native_tool_output, None);
    assert_eq!(search_result.output, None);
}

#[tokio::test]
async fn real_query_interrupted_result_renders_with_visible_name_and_request_profile() {
    let trace = run_cancelled_facade_and_search().await;

    let result = persisted_tool_result(&trace.messages, "facade");
    assert!(result.is_error);
    assert_eq!(
        first_text(result.content),
        "Tool error from `spawn_agent`:\nTool execution was interrupted."
    );
}

#[tokio::test]
async fn approval_denial_keeps_internal_identity_and_visible_history() {
    let initial = runtime_json!({"description":"inspect","name":"inspect","prompt":"initial"});
    let candidate = runtime_json!({"description":"approval","prompt":"candidate"});
    let case = ResolvedIdentityCase {
        name: "approval_denial",
        pre_hook_outcome: HookOutcome::default(),
        permission_response: RecordingPermissionResponse::Ask(candidate.clone()),
        approval_response: Some(ApprovalResponse::Deny {
            message: Some("approval denied".into()),
        }),
        validation_error: None,
        include_tool: true,
        tool_outcome: IdentityToolOutcome::Ok,
        expected_pre_hook: Some(initial.clone()),
        expected_permission: Some(initial.clone()),
        expected_execution: None,
        expected_post_hook: None,
        expected_approval: Some(candidate.clone()),
        expected_terminal_status: ItemStatus::Failed,
        expected_terminal_error: Some("approval denied"),
    };

    let trace = run_resolved_identity_case(&case).await;

    assert_eq!(
        trace.started,
        (RuntimeString::from("task"), initial.clone())
    );
    assert_eq!(
        trace.terminal,
        (
            RuntimeString::from("task"),
            initial.clone(),
            ItemStatus::Failed,
            Some(RuntimeString::from("approval denied")),
        )
    );
    assert_eq!(
        trace.approval,
        Some((RuntimeString::from("task"), candidate))
    );
    assert_eq!(trace.execution, None);
    assert_eq!(
        trace
            .lifecycle
            .iter()
            .map(|(phase, name, arguments)| (*phase, name.clone(), arguments.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                ToolLifecyclePhase::Requested,
                RuntimeString::from("task"),
                Some(initial.clone()),
            ),
            (
                ToolLifecyclePhase::Started,
                RuntimeString::from("task"),
                Some(initial.clone()),
            ),
            (
                ToolLifecyclePhase::Failed,
                RuntimeString::from("task"),
                Some(initial),
            ),
        ]
    );
    assert_eq!(trace.persisted_visible_call.name, "spawn_agent");
    assert_eq!(
        trace.persisted_visible_call.input,
        runtime_json!({"task_name":"inspect","message":"initial"})
    );
}

#[tokio::test]
async fn fatal_tool_error_keeps_internal_start_and_visible_history() {
    let execution = Arc::new(Mutex::new(Vec::<Json>::new()));
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&execution),
        validation_error: None,
        outcome: IdentityToolOutcome::Fatal,
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "fatal-call",
        "spawn_agent",
        &runtime_json!({"task_name":"fatal","message":"fail"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("delegate")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let _panic_hook_lock = PANIC_HOOK_TEST_LOCK.lock().await;
    let fatal_panic_hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_calls = Arc::clone(&fatal_panic_hook_calls);
    let previous_hook = Arc::new(Mutex::new(Some(std::panic::take_hook())));
    let delegated_hook = Arc::clone(&previous_hook);
    std::panic::set_hook(Box::new(move |info| {
        if info.payload().downcast_ref::<ToolError>().is_some() {
            hook_calls.fetch_add(1, Ordering::SeqCst);
        } else if let Some(previous) = delegated_hook.lock().unwrap().as_ref() {
            previous(info);
        }
    }));
    let restore_hook = Arc::clone(&previous_hook);
    let _restore_hook = DropAction(Some(Box::new(move || {
        let _installed_hook = std::panic::take_hook();
        if let Some(previous) = restore_hook.lock().unwrap().take() {
            std::panic::set_hook(previous);
        }
    })));
    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;
    let initial = runtime_json!({"description":"fatal","name":"fatal","prompt":"fail"});

    assert_eq!(
        fatal_panic_hook_calls.load(Ordering::SeqCst),
        0,
        "a normal fatal ToolError must not invoke Rust's panic hook"
    );
    assert_eq!(*execution.lock().unwrap(), vec![initial.clone()]);
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted { inference: _,
            item: ThreadItem::ToolCall {
                id,
                tool_name,
                arguments,
                ..
            }
        } if id == "fatal-call" && tool_name == "task" && arguments == &initial
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemUpdated { inference: _,
            item: ThreadItem::ToolCall { id, .. }
        } | EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { id, .. }
        } if id == "fatal-call"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::Internal
                && error.message == "identity fatal failure"
    )));
    let fatal = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .expect("fatal tool error must fail the turn");
    let request_completed_at = events
        .iter()
        .position(|event| {
            matches!(event,
                EngineEvent::ProviderDecision { event, .. }
                    if event.kind == ProviderDecisionEventKind::RequestCompleted
            )
        })
        .expect("completed provider usage must survive a fatal tool error");
    let turn_failed_at = events
        .iter()
        .position(|event| matches!(event, EngineEvent::TurnFailed { .. }))
        .unwrap();
    assert!(request_completed_at < turn_failed_at);
    assert!(!events.iter().any(|event| matches!(event,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RequestFailed
    )));
    assert_eq!(
        fatal.cause.as_ref().map(ToString::to_string),
        Some("identity fatal failure".to_string())
    );
    assert_eq!(
        lifecycle
            .lock()
            .unwrap()
            .iter()
            .map(|record| (
                record.phase,
                exact_test_string(&record.tool_name),
                record.arguments.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                ToolLifecyclePhase::Requested,
                RuntimeString::from("task"),
                Some(initial.clone()),
            ),
            (
                ToolLifecyclePhase::Started,
                RuntimeString::from("task"),
                Some(initial),
            ),
        ]
    );
    let persisted = persisted.lock().unwrap();
    let visible = persisted_tool_call(&persisted, "fatal-call");
    assert_eq!(visible.name, "spawn_agent");
    assert_eq!(
        visible.input,
        runtime_json!({"task_name":"fatal","message":"fail"})
    );
    assert!(!persisted.iter().any(|message| matches!(
        message,
        Message::ToolResult(result) if result.tool_use_id == "fatal-call"
    )));
}

#[tokio::test]
async fn pre_tool_hook_null_rewrite_is_a_fatal_permission_error() {
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let hook_inputs = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "grep-null-rewrite",
        "grep",
        &runtime_json!({ "pattern": "needle" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let mut params = mk_params(
        vec![user_msg("search")],
        vec![create_grep_tool() as Arc<dyn Tool>],
    );
    params.permission_engine = Arc::new(StaticPermissionEngine::new(&params.config));
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PreToolUse,
            Arc::clone(&hook_inputs),
            HookOutcome {
                updated_input: Some(Json::Null),
                ..HookOutcome::default()
            },
        )],
    )));
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(hook_inputs.lock().unwrap().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted { inference: _,
            item: ThreadItem::ToolCall { id, .. }
        } if id == "grep-null-rewrite"
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemUpdated { inference: _,
            item: ThreadItem::ToolCall { id, .. }
        } | EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { id, .. }
        } if id == "grep-null-rewrite"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::Internal
                && error.message == "Cannot read properties of null (reading 'command')"
                && error.cause.as_ref().map(ToString::to_string).as_deref()
                    == Some("Cannot read properties of null (reading 'command')")
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    assert!(!persisted.lock().unwrap().iter().any(|message| matches!(
        message,
        Message::ToolResult(result) if result.tool_use_id == "grep-null-rewrite"
    )));
}

#[tokio::test]
async fn caught_nested_fatal_rejection_allows_outer_tool_to_complete() {
    let failures = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(FatalNestedInvoker {
            name: "catch_nested_fatal",
            schema: JsonSchema::new(),
            catch: true,
        }),
        Arc::new(FatalNestedTool {
            schema: JsonSchema::new(),
        }),
    ];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "outer-call",
        "catch_nested_fatal",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let second = vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ];
    let mut params = mk_params(vec![user_msg("catch nested fatal")], tools);
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PostToolUseFailure,
            Arc::clone(&failures),
            HookOutcome::default(),
        )],
    )));

    let events = query(params, deps_with(scripted_call_model(vec![first, second])))
        .collect::<Vec<_>>()
        .await;

    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, .. }
        } if tool_name == "catch_nested_fatal"
    )));
    assert_eq!(
        failures
            .lock()
            .unwrap()
            .iter()
            .map(|input| input.tool_name.as_ref().map(exact_test_string))
            .collect::<Vec<_>>(),
        vec![Some(RuntimeString::from("fatal_nested"))]
    );
}

#[tokio::test]
async fn uncaught_nested_fatal_rejection_runs_inner_and_outer_failure_hooks() {
    let failures = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(FatalNestedInvoker {
            name: "propagate_nested_fatal",
            schema: JsonSchema::new(),
            catch: false,
        }),
        Arc::new(FatalNestedTool {
            schema: JsonSchema::new(),
        }),
    ];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "outer-call",
        "propagate_nested_fatal",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("propagate nested fatal")], tools);
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PostToolUseFailure,
            Arc::clone(&failures),
            HookOutcome::default(),
        )],
    )));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::Internal
                && error.message == "nested fatal failure"
    )));
    assert_eq!(
        failures
            .lock()
            .unwrap()
            .iter()
            .map(|input| input.tool_name.as_ref().map(exact_test_string))
            .collect::<Vec<_>>(),
        vec![
            Some(RuntimeString::from("fatal_nested")),
            Some(RuntimeString::from("propagate_nested_fatal"))
        ]
    );
}

#[tokio::test]
async fn fatal_parallel_call_does_not_cancel_an_already_started_sibling() {
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    let sibling_completed = Arc::new(Mutex::new(Some(completed_tx)));
    let tools = vec![
        Arc::new(ParallelFatalTool {
            name: "parallel_fatal",
            schema: JsonSchema::new(),
            role: ParallelFatalRole::Fatal,
            started: Arc::clone(&started),
            sibling_completed: Arc::clone(&sibling_completed),
        }) as Arc<dyn Tool>,
        Arc::new(ParallelFatalTool {
            name: "parallel_sibling",
            schema: JsonSchema::new(),
            role: ParallelFatalRole::Sibling,
            started,
            sibling_completed,
        }) as Arc<dyn Tool>,
    ];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "parallel-fatal-call",
        "parallel_fatal",
        &runtime_json!({}),
    ));
    first.extend(exact_tool_use_events(
        "parallel-sibling-call",
        "parallel_sibling",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let events = query(
        mk_params(vec![user_msg("run both")], tools),
        deps_with(scripted_call_model(vec![first])),
    )
    .collect::<Vec<_>>()
    .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::Internal
                && error.message == "parallel fatal failure"
    )));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), completed_rx)
            .await
            .expect("started sibling must finish after Promise.all-style rejection"),
        Ok(())
    );
}

#[tokio::test]
async fn later_fatal_parallel_call_rejects_before_earlier_sibling_finishes() {
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let sibling_release = Arc::new(tokio::sync::Notify::new());
    let sibling_completed = Arc::new(tokio::sync::Notify::new());
    let tools = vec![
        Arc::new(CompletionOrderFatalTool {
            name: "completion_order_sibling",
            schema: JsonSchema::new(),
            role: CompletionOrderFatalRole::BlockingSibling,
            started: Arc::clone(&started),
            sibling_release: Arc::clone(&sibling_release),
            sibling_completed: Arc::clone(&sibling_completed),
        }) as Arc<dyn Tool>,
        Arc::new(CompletionOrderFatalTool {
            name: "completion_order_fatal",
            schema: JsonSchema::new(),
            role: CompletionOrderFatalRole::Fatal,
            started,
            sibling_release: Arc::clone(&sibling_release),
            sibling_completed: Arc::clone(&sibling_completed),
        }) as Arc<dyn Tool>,
    ];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "blocking-sibling-call",
        "completion_order_sibling",
        &runtime_json!({}),
    ));
    first.extend(exact_tool_use_events(
        "later-fatal-call",
        "completion_order_fatal",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let mut events = query(
        mk_params(vec![user_msg("run both")], tools),
        deps_with(scripted_call_model(vec![first])),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(event) = events.next().await {
            if matches!(
                event,
                EngineEvent::TurnFailed { error, .. }
                    if error.code == EngineErrorCode::Internal
                        && error.message == "completion-order fatal failure"
            ) {
                return;
            }
        }
        panic!("query closed before surfacing the later fatal call");
    })
    .await
    .expect("later fatal call must reject before the earlier sibling is released");

    sibling_release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), sibling_completed.notified())
        .await
        .expect("already-started sibling must remain detached and complete after rejection");
}

#[tokio::test]
async fn parallel_success_results_remain_in_model_order_when_completion_is_reversed() {
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let fast_completed = Arc::new(tokio::sync::Notify::new());
    let completion_order = Arc::new(Mutex::new(Vec::new()));
    let tools = vec![
        Arc::new(CompletionOrderSuccessTool {
            name: "completion_order_slow",
            schema: JsonSchema::new(),
            role: CompletionOrderSuccessRole::SlowFirst,
            started: Arc::clone(&started),
            fast_completed: Arc::clone(&fast_completed),
            completion_order: Arc::clone(&completion_order),
        }) as Arc<dyn Tool>,
        Arc::new(CompletionOrderSuccessTool {
            name: "completion_order_fast",
            schema: JsonSchema::new(),
            role: CompletionOrderSuccessRole::FastSecond,
            started,
            fast_completed,
            completion_order: Arc::clone(&completion_order),
        }) as Arc<dyn Tool>,
    ];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "slow-first-call",
        "completion_order_slow",
        &runtime_json!({}),
    ));
    first.extend(exact_tool_use_events(
        "fast-second-call",
        "completion_order_fast",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("run both")], tools);
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let events = query(
        params,
        deps_with(scripted_call_model(vec![
            first,
            vec![
                ev::message_start(None),
                ev::text("done"),
                ev::stop(None, None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(
        *completion_order.lock().unwrap(),
        vec!["fast-second", "slow-first"]
    );
    let persisted_result_order = persisted
        .lock()
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(exact_test_string(&result.tool_use_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        persisted_result_order,
        vec!["slow-first-call", "fast-second-call"]
    );
    assert!(matches!(
        events.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_parallel_admission_is_fifo_in_model_order() {
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let tools = ["fifo_first", "fifo_second", "fifo_third"]
        .into_iter()
        .map(|name| {
            Arc::new(AdmissionOrderTool {
                name,
                schema: JsonSchema::new(),
                admitted: Arc::clone(&admitted),
                fatal: false,
                completion: None,
            }) as Arc<dyn Tool>
        })
        .collect::<Vec<_>>();
    let mut first = vec![ev::message_start(None)];
    for (id, name) in [
        ("fifo-call-1", "fifo_first"),
        ("fifo-call-2", "fifo_second"),
        ("fifo-call-3", "fifo_third"),
    ] {
        let input = if id == "fifo-call-1" {
            runtime_json!({"test_admission_delay_ms": 25})
        } else {
            runtime_json!({})
        };
        first.extend(exact_tool_use_events(id, name, &input));
    }
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("run in order")], tools);
    params.config.max_tool_concurrency = 1;

    let _events = query(
        params,
        deps_with(scripted_call_model(vec![
            first,
            vec![
                ev::message_start(None),
                ev::text("done"),
                ev::stop(None, None),
            ],
        ])),
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(
        *admitted.lock().unwrap(),
        vec!["fifo_first", "fifo_second", "fifo_third"]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_rejection_does_not_cancel_fifo_queued_siblings() {
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let queued_tail_completed = Arc::new(tokio::sync::Notify::new());
    let tools = ["queued_fatal", "queued_second", "queued_third"]
        .into_iter()
        .map(|name| {
            Arc::new(AdmissionOrderTool {
                name,
                schema: JsonSchema::new(),
                admitted: Arc::clone(&admitted),
                fatal: name == "queued_fatal",
                completion: (name == "queued_third").then(|| Arc::clone(&queued_tail_completed)),
            }) as Arc<dyn Tool>
        })
        .collect::<Vec<_>>();
    let mut first = vec![ev::message_start(None)];
    for (id, name) in [
        ("queued-call-1", "queued_fatal"),
        ("queued-call-2", "queued_second"),
        ("queued-call-3", "queued_third"),
    ] {
        first.extend(exact_tool_use_events(id, name, &runtime_json!({})));
    }
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("run queued calls")], tools);
    params.config.max_tool_concurrency = 1;

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::Internal
                && error.message == "queued fifo fatal failure"
    )));
    tokio::time::timeout(Duration::from_secs(2), queued_tail_completed.notified())
        .await
        .expect("a fatal rejection must detach, not cancel, queued siblings");
    assert_eq!(
        *admitted.lock().unwrap(),
        vec!["queued_fatal", "queued_second", "queued_third"]
    );
}

#[tokio::test]
async fn tool_internal_approval_uses_final_effective_input() {
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let approvals = Arc::new(Mutex::new(Vec::<bool>::new()));
    let approval_requests = Arc::new(Mutex::new(Vec::<ApprovalRequest>::new()));
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let tool = Arc::new(MidApprovalIdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&seen),
        approvals: Arc::clone(&approvals),
    }) as Arc<dyn Tool>;
    let final_effective = runtime_json!({"description":"permission","prompt":"effective"});
    let permission_seen = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "mid-approval",
        "spawn_agent",
        &runtime_json!({"task_name":"inspect","message":"initial"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let mut params = mk_params(vec![user_msg("delegate")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission_seen),
        response: RecordingPermissionResponse::AllowRewrite(final_effective.clone()),
    });
    params.approval_handler = Some(Arc::new(StaticApproval {
        requests: Arc::clone(&approval_requests),
        response: ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: None,
        },
    }));
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        *permission_seen.lock().unwrap(),
        vec![(
            RuntimeString::from("task"),
            runtime_json!({"description":"inspect","name":"inspect","prompt":"initial"})
        )]
    );
    assert_eq!(*seen.lock().unwrap(), vec![final_effective.clone()]);
    assert_eq!(*approvals.lock().unwrap(), vec![true]);
    let requests = approval_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tool_name, "task");
    assert_eq!(requests[0].input, final_effective);
    assert!(lifecycle.lock().unwrap().iter().any(|record| {
        record.tool_use_id == "mid-approval"
            && record.tool_name == "task"
            && record.phase == ToolLifecyclePhase::Approved
            && record.arguments.as_ref() == Some(&requests[0].input)
    }));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall {
                id,
                tool_name,
                arguments,
                ..
            }
        } if id == "mid-approval"
            && tool_name == "task"
            && arguments == &runtime_json!({"description":"inspect","name":"inspect","prompt":"initial"})
    )));
    let persisted = persisted.lock().unwrap();
    let visible = persisted_tool_call(&persisted, "mid-approval");
    assert_eq!(visible.name, "spawn_agent");
    assert_eq!(
        visible.input,
        runtime_json!({"task_name":"inspect","message":"initial"})
    );
}

#[tokio::test]
async fn nested_internal_invoke_is_not_provider_normalized_again() {
    let trusted_input = runtime_json!({
        "task_name": "trusted-internal",
        "message": "do not normalize",
        "opaque": {"value": 7}
    });
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(TrustedNestedInvoker {
            schema: JsonSchema::new(),
            trusted_input: trusted_input.clone(),
        }),
        Arc::new(TrustedInternalRecorder {
            schema: JsonSchema::new(),
            seen: Arc::clone(&seen),
        }),
    ];
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "outer-call",
        "nested_internal",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let mut params = mk_params(vec![user_msg("invoke nested")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(*seen.lock().unwrap(), vec![trusted_input.clone()]);
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted { inference: _,
            item: ThreadItem::ToolCall {
                tool_name,
                arguments,
                ..
            }
        } if tool_name == "spawn_agent" && arguments == &trusted_input
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall {
                tool_name,
                arguments,
                status: ItemStatus::Completed,
                ..
            }
        } if tool_name == "spawn_agent" && arguments == &trusted_input
    )));
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle
            .iter()
            .filter(|record| record.tool_name == "spawn_agent")
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![
            ToolLifecyclePhase::Requested,
            ToolLifecyclePhase::Started,
            ToolLifecyclePhase::Completed,
        ]
    );
    assert!(lifecycle
        .iter()
        .filter(|record| record.tool_name == "spawn_agent")
        .take(2)
        .all(|record| record.arguments.as_ref() == Some(&trusted_input)));
    assert!(!lifecycle.iter().any(|record| record.tool_name == "task"));
}

#[test]
fn tool_search_success_metadata_falls_back_from_visible_name() {
    let profile = test_provider_profile("fake-model");
    let call = ResolvedToolCall::new(
        ToolUseBlock {
            id: "legacy-search".into(),
            name: "tool_search".into(),
            input: runtime_json!({"query":"calendar"}),
            thought_signature: None,
            tool_kind: None,
        },
        profile.clone(),
    );
    let output = native_search_output_fixture();
    assert_eq!(
        successful_result_provider_metadata(&call, &output),
        (
            Some(crate::types::provider::ProviderToolCallKind::ToolSearch),
            Some(output),
        )
    );

    let explicit_custom = ResolvedToolCall::new(
        ToolUseBlock {
            id: "custom-search".into(),
            name: "tool_search".into(),
            input: runtime_json!({"query":"calendar"}),
            thought_signature: None,
            tool_kind: Some(ProviderToolCallKind::Custom),
        },
        profile,
    );
    assert_eq!(
        successful_result_provider_metadata(&explicit_custom, &native_search_output_fixture()),
        (Some(ProviderToolCallKind::Custom), None)
    );
}

#[tokio::test]
async fn tool_search_result_metadata_covers_four_paths() {
    let output = native_search_output_fixture();
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(NativeSearchRecordingTool {
        schema: JsonSchema::new(),
        output: output.clone(),
    })];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(provider_tool_use_events(
        "search-ok",
        "tool_search",
        &runtime_json!({"mode":"ok"}),
        None,
    ));
    first.extend(provider_tool_use_events(
        "search-ok-error",
        "tool_search",
        &runtime_json!({"mode":"ok_error"}),
        None,
    ));
    first.extend(provider_tool_use_events(
        "search-err",
        "tool_search",
        &runtime_json!({"mode":"err"}),
        Some(ProviderToolCallKind::ToolSearch),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("search outcomes")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;
    let messages = messages.lock().unwrap().clone();

    let ok = persisted_tool_result(&messages, "search-ok");
    assert!(!ok.is_error);
    assert_eq!(ok.tool_kind, Some(ProviderToolCallKind::ToolSearch));
    assert_eq!(ok.native_tool_output, Some(output.clone()));
    assert_eq!(ok.output, Some(output.clone()));

    let ok_is_error = persisted_tool_result(&messages, "search-ok-error");
    assert!(ok_is_error.is_error);
    assert_eq!(
        ok_is_error.tool_kind,
        Some(ProviderToolCallKind::ToolSearch)
    );
    assert_eq!(ok_is_error.native_tool_output, Some(output.clone()));
    assert_eq!(ok_is_error.output, Some(output));

    let nonfatal_err = persisted_tool_result(&messages, "search-err");
    assert!(nonfatal_err.is_error);
    assert_eq!(
        nonfatal_err.tool_kind,
        Some(ProviderToolCallKind::ToolSearch)
    );
    assert_eq!(nonfatal_err.native_tool_output, None);
    assert_eq!(nonfatal_err.output, None);

    let cancelled = run_cancelled_facade_and_search().await;
    let cancelled = persisted_tool_result(&cancelled.messages, "search-cancel");
    assert!(cancelled.is_error);
    assert_eq!(cancelled.tool_kind, Some(ProviderToolCallKind::ToolSearch));
    assert_eq!(cancelled.native_tool_output, None);
    assert_eq!(cancelled.output, None);
}

#[tokio::test]
async fn think_schema_parser_rejects_before_hooks_and_permission() {
    let executed = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tools = vec![recording_think_tool(Arc::clone(&executed))];
    let cases = [
        (
            "think-missing",
            runtime_json!({}),
            "Invalid arguments: thought: Invalid input: expected string, received undefined",
        ),
        (
            "think-number",
            runtime_json!({ "thought": 7 }),
            "Invalid arguments: thought: Invalid input: expected string, received number",
        ),
    ];
    let mut first = vec![ev::message_start(None)];
    for (id, input, _) in &cases {
        first.extend(exact_tool_use_events(id, "think", input));
    }
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("reject invalid thoughts")], tools);
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Allow,
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
        ],
    )));
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert!(hooks.lock().unwrap().is_empty());
    assert!(permission.lock().unwrap().is_empty());
    assert!(executed.lock().unwrap().is_empty());
    let messages = messages.lock().unwrap();
    for (id, _, expected) in cases {
        let result = persisted_tool_result(&messages, id);
        assert!(result.is_error, "dispatcher accepted {id}");
        assert_eq!(output_blocks_text(&result.content), expected, "{id}");
        assert!(events.iter().any(|event| matches!(
            event,
            EngineEvent::ItemCompleted { inference: _,
                item: ThreadItem::ToolCall {
                    id: event_id,
                    status: ItemStatus::Failed,
                    error: Some(error),
                    ..
                }
            } | EngineEvent::ItemUpdated { inference: _,
                item: ThreadItem::ToolCall {
                    id: event_id,
                    status: ItemStatus::Failed,
                    error: Some(error),
                    ..
                }
            } if event_id == id && error.message == expected
        )));
    }
}

#[tokio::test]
async fn think_schema_parser_strips_unknowns_before_policy() {
    let raw = runtime_json!({ "thought": "step", "extra": "sentinel" });
    let parsed = runtime_json!({ "thought": "step" });
    let executed = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tools = vec![recording_think_tool(Arc::clone(&executed))];
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("think-sanitized", "think", &raw));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("sanitize the thought")], tools);
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Allow,
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
        ],
    )));
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted { inference: _,
            item: ThreadItem::ToolCall { id, arguments, .. }
        } if id == "think-sanitized" && arguments == &raw
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { id, arguments, .. }
        } if id == "think-sanitized" && arguments == &raw
    )));
    assert_eq!(
        *permission.lock().unwrap(),
        vec![(RuntimeString::from("think"), parsed.clone())]
    );
    assert_eq!(*executed.lock().unwrap(), vec![parsed.clone()]);
    let hooks = hooks.lock().unwrap();
    assert_eq!(hooks.len(), 2);
    assert!(hooks
        .iter()
        .all(|input| input.tool_input.as_ref() == Some(&parsed)));
    assert_eq!(
        persisted_tool_result(&messages.lock().unwrap(), "think-sanitized").output,
        Some(parsed)
    );
}

#[tokio::test]
async fn default_parser_runs_once_and_does_not_reparse_hook_rewrite() {
    let tool = create_task_tool(TaskToolOptions::default());
    let initial = runtime_json!({
        "description": "delegate once",
        "prompt": "initial prompt",
        "fork_turns":"9007199254740993",
        "extra": "strip-before-hook"
    });
    let hook_rewrite = runtime_json!({
        "description": "delegate once",
        "prompt": "hook prompt",
        "fork_turns": "all",
        "run_in_background": "false"
    });
    let approval_rewrite = runtime_json!({
        "description": "delegate once",
        "prompt": "approval prompt",
        "fork_turns": "all",
        "run_in_background": "false"
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("task-rewrite", "task", &initial));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(None),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let approvals = Arc::new(Mutex::new(Vec::<ApprovalRequest>::new()));
    let foreground_spawned = Arc::new(Mutex::new(Vec::new()));
    let background_spawned = Arc::new(Mutex::new(Vec::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("trusted rewrite")], vec![tool]);
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Ask(hook_rewrite.clone()),
    });
    params.approval_handler = Some(Arc::new(StaticApproval {
        requests: Arc::clone(&approvals),
        response: ApprovalResponse::Allow {
            updated_input: Some(approval_rewrite.clone()),
            updated_permissions: None,
        },
    }));
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hooks),
                HookOutcome {
                    updated_input: Some(hook_rewrite.clone()),
                    ..HookOutcome::default()
                },
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
        ],
    )));
    let foreground_sink = Arc::clone(&foreground_spawned);
    let background_sink = Arc::clone(&background_spawned);
    params.tool_context_base = Arc::new(move || {
        let mut ctx = make_test_ctx("/tmp");
        let foreground_sink = Arc::clone(&foreground_sink);
        ctx.spawn_sub_agent = Some(Arc::new(move |options| {
            foreground_sink.lock().unwrap().push(options);
            Box::pin(std::future::ready(Ok(
                crate::types::tool::ForegroundSubAgentRun {
                    agent_id: "child-1".into(),
                    events: Box::pin(futures::stream::empty()),
                },
            )))
        }));
        let background_sink = Arc::clone(&background_sink);
        ctx.spawn_background_agent = Some(Arc::new(move |options| {
            background_sink.lock().unwrap().push(options);
            Box::pin(std::future::ready(Ok(
                crate::types::tool::SpawnBackgroundAgentResult {
                    agent_id: "background-child-1".into(),
                    output_file: "/tmp/background-child-1.out".into(),
                },
            )))
        }));
        ctx
    });
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        *permission.lock().unwrap(),
        vec![(RuntimeString::from("task"), hook_rewrite.clone())]
    );
    assert_eq!(approvals.lock().unwrap()[0].input, hook_rewrite);
    assert!(
        foreground_spawned.lock().unwrap().is_empty(),
        "a truthy trusted rewrite must not use the foreground callback"
    );
    let spawned = background_spawned.lock().unwrap();
    assert_eq!(spawned.len(), 1, "trusted rewrite did not reach Task spawn");
    assert_eq!(spawned[0].prompt, "approval prompt");
    assert_eq!(
        spawned[0].fork_context,
        Some(crate::types::tool::ForkContext::All)
    );
    let hooks = hooks.lock().unwrap();
    let pre = hooks
        .iter()
        .find(|input| input.event == HookEvent::PreToolUse)
        .unwrap();
    assert_eq!(
        pre.tool_input,
        Some(runtime_json!({
            "description": "delegate once",
            "prompt": "initial prompt",
            "fork_turns":"9007199254740993"
        }))
    );
    let post = hooks
        .iter()
        .find(|input| input.event == HookEvent::PostToolUse)
        .unwrap();
    assert_eq!(post.tool_input, Some(approval_rewrite));
    assert!(!persisted_tool_result(&messages.lock().unwrap(), "task-rewrite").is_error);
}

#[tokio::test]
async fn trusted_task_agent_type_to_primitive_failure_uses_failure_hook_only() {
    let tool = create_task_tool(TaskToolOptions {
        model_catalog: false,
        model_options: Vec::new(),
        agents: vec![AgentDefinition {
            name: "reviewer".into(),
            ..Default::default()
        }],
    });
    let initial = runtime_json!({
        "description": "delegate",
        "prompt": "initial",
        "agentType": "reviewer"
    });
    let rewritten = runtime_json!({
        "description": "delegate",
        "prompt": "trusted rewrite",
        "agentType": { "toString": null }
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("task-to-primitive", "task", &initial));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let second = vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let spawn_count = Arc::new(AtomicUsize::new(0));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("trusted rewrite")], vec![tool]);
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hooks),
                HookOutcome {
                    updated_input: Some(rewritten.clone()),
                    ..HookOutcome::default()
                },
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
            recording_hook(
                HookEvent::PostToolUseFailure,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
        ],
    )));
    let spawn_count_for_ctx = Arc::clone(&spawn_count);
    params.tool_context_base = Arc::new(move || {
        let mut ctx = make_test_ctx("/tmp");
        let spawn_count = Arc::clone(&spawn_count_for_ctx);
        ctx.spawn_sub_agent = Some(Arc::new(move |_options| {
            spawn_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Ok(
                crate::types::tool::ForegroundSubAgentRun {
                    agent_id: "unexpected".into(),
                    events: Box::pin(futures::stream::empty()),
                },
            )))
        }));
        ctx
    });
    let messages_sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        messages_sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(vec![first, second])))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(spawn_count.load(Ordering::SeqCst), 0);
    let hooks = hooks.lock().unwrap();
    assert!(hooks
        .iter()
        .any(|input| input.event == HookEvent::PreToolUse));
    assert!(!hooks
        .iter()
        .any(|input| input.event == HookEvent::PostToolUse));
    let failure = hooks
        .iter()
        .find(|input| input.event == HookEvent::PostToolUseFailure)
        .expect("PostToolUseFailure hook");
    assert_eq!(failure.tool_input.as_ref(), Some(&rewritten));
    assert_eq!(
        failure.error.as_ref().map(exact_test_string),
        Some(RuntimeString::from(
            "Cannot convert object to primitive value"
        ))
    );
    let result = persisted_tool_result(&messages.lock().unwrap(), "task-to-primitive");
    assert!(result.is_error);
    assert!(
        output_blocks_text(&result.content).contains("Cannot convert object to primitive value")
    );
}

#[tokio::test]
async fn tool_search_schema_errors_survive_the_real_dispatcher() {
    let tools = vec![crate::tools::builtins::tool_search::create_tool_search_tool(None)];
    let cases = [
        (
            "search-missing-query",
            runtime_json!({}),
            "Invalid arguments: query: Invalid input: expected string, received undefined",
        ),
        (
            "search-numeric-query",
            runtime_json!({ "query": 7 }),
            "Invalid arguments: query: Invalid input: expected string, received number",
        ),
    ];
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    for (id, input, _) in &cases {
        first.extend(provider_tool_use_events(
            id,
            "tool_search",
            input,
            Some(ProviderToolCallKind::ToolSearch),
        ));
    }
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("invalid search inputs")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;
    let messages = messages.lock().unwrap();
    for (id, _, expected) in cases {
        let result = persisted_tool_result(&messages, id);
        assert!(result.is_error, "dispatcher accepted {id}");
        assert_eq!(
            output_blocks_text(&result.content),
            format!("Tool error from `tool_search`:\n{expected}"),
            "{id}"
        );
        assert_eq!(result.tool_kind, Some(ProviderToolCallKind::ToolSearch));
        assert_eq!(result.native_tool_output, None);
        assert!(
            events.iter().any(|event| matches!(
                event,
                EngineEvent::ItemCompleted { inference: _,
                    item: ThreadItem::ToolCall {
                        id: event_id,
                        status: ItemStatus::Failed,
                        error: Some(error),
                        ..
                    }
                } | EngineEvent::ItemUpdated { inference: _,
                    item: ThreadItem::ToolCall {
                        id: event_id,
                        status: ItemStatus::Failed,
                        error: Some(error),
                        ..
                    }
                } if event_id == id && error.message == expected
            )),
            "missing raw validation terminal event for {id}"
        );
    }
}

#[tokio::test]
async fn tool_search_parser_strips_unknowns_and_canonicalizes_before_policy() {
    let tools = vec![crate::tools::builtins::tool_search::create_tool_search_tool(None)];
    let raw = runtime_json::parse(&RuntimeString::from(
        r#"{"query":"nothing","limit":1e0,"extra":"sentinel"}"#,
    ))
    .unwrap();
    let started_expected = runtime_json!({ "query": "nothing", "limit": 1, "extra": "sentinel" });
    let expected = runtime_json!({ "query": "nothing", "limit": 1 });
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(provider_tool_use_events(
        "search-sanitized",
        "tool_search",
        &raw,
        Some(ProviderToolCallKind::ToolSearch),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("sanitize search")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Allow,
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            recording_hook(
                HookEvent::PreToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
            recording_hook(
                HookEvent::PostToolUse,
                Arc::clone(&hooks),
                HookOutcome::default(),
            ),
        ],
    )));
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted { inference: _,
            item: ThreadItem::ToolCall { id, arguments, .. }
        } if id == "search-sanitized" && arguments == &started_expected
    )));
    let hooks = hooks.lock().unwrap();
    assert_eq!(
        hooks
            .iter()
            .find(|input| input.event == HookEvent::PreToolUse)
            .and_then(|input| input.tool_input.clone()),
        Some(expected.clone())
    );
    assert_eq!(
        *permission.lock().unwrap(),
        vec![(RuntimeString::from("tool_search"), expected)]
    );
    assert!(!persisted_tool_result(&messages.lock().unwrap(), "search-sanitized").is_error);
}

#[tokio::test]
async fn hook_rewritten_input_is_not_schema_parsed_a_second_time() {
    let tools = vec![
        crate::tools::builtins::tool_search::create_tool_search_tool(None),
        Arc::new(SourceTool {
            name: "nothing_tool",
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure: ToolExposure::Deferred,
        }) as Arc<dyn Tool>,
    ];
    let initial = runtime_json!({ "query": "nothing", "limit": 1 });
    let hook_rewrite = runtime_json!({
        "query": "nothing",
        "limit": 0,
        "hook_only": "trusted rewrite"
    });
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(provider_tool_use_events(
        "search-hook-rewrite",
        "tool_search",
        &initial,
        Some(ProviderToolCallKind::ToolSearch),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("rewrite search")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Allow,
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PreToolUse,
            Arc::clone(&hooks),
            HookOutcome {
                updated_input: Some(hook_rewrite.clone()),
                ..HookOutcome::default()
            },
        )],
    )));
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        *permission.lock().unwrap(),
        vec![(RuntimeString::from("tool_search"), hook_rewrite)]
    );
    let result = persisted_tool_result(&messages.lock().unwrap(), "search-hook-rewrite");
    assert!(
        !result.is_error,
        "trusted Hook rewrite was schema-parsed twice"
    );
    assert_eq!(
        result.native_tool_output,
        Some(runtime_json!({ "matched": [], "tools": [] }))
    );
}

#[tokio::test]
async fn invalid_openai_fork_turns_fails_before_hooks_and_permission() {
    let tools = vec![crate::tools::builtins::task::create_task_tool(
        crate::tools::builtins::task::TaskToolOptions::default(),
    )];
    let visible = runtime_json!({
        "task_name": "unsafe context",
        "message": "must fail before policy",
        "fork_turns": "0"
    });
    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "fork-unsafe",
        "spawn_agent",
        &visible,
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let hooks = Arc::new(Mutex::new(Vec::<HookInput>::new()));
    let permission = Arc::new(Mutex::new(Vec::<(RuntimeString, Json)>::new()));
    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("invalid fork")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.permission_engine = Arc::new(RecordingPermission {
        seen: Arc::clone(&permission),
        response: RecordingPermissionResponse::Allow,
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PreToolUse,
            Arc::clone(&hooks),
            HookOutcome::default(),
        )],
    )));
    let sink = Arc::clone(&messages);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps_with(scripted_call_model(turns)))
        .collect::<Vec<_>>()
        .await;
    assert!(hooks.lock().unwrap().is_empty());
    assert!(permission.lock().unwrap().is_empty());
    let result = persisted_tool_result(&messages.lock().unwrap(), "fork-unsafe");
    assert!(result.is_error);
    assert_eq!(
        output_blocks_text(&result.content),
        concat!(
            "Tool error from `spawn_agent`:\n",
            "Invalid arguments: fork_turns must be `none` or `all`"
        )
    );
}

#[tokio::test]
async fn parsed_lone_surrogate_grep_path_reaches_node_utf8_stat_and_execution() {
    let root = std::env::temp_dir().join(format!(
        "klaus-query-grep-surrogate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("\u{fffd}"), "needle\n").unwrap();

    let first = vec![
        ev::message_start(None),
        ModelStreamEvent::ToolUseStart {
            id: "grep-surrogate-path".into(),
            name: "grep".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "grep-surrogate-path".into(),
            partial_json: r#"{"pattern":"needle","path":"\ud800"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "grep-surrogate-path".into(),
        },
        ev::stop(Some(ModelStopReason::ToolUse), None),
    ];
    let second = vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ];
    let mut params = mk_params(
        vec![user_msg("search the surrogate path")],
        vec![create_grep_tool() as Arc<dyn Tool>],
    );
    let context_root = root
        .to_str()
        .expect("test context root is a scalar host path")
        .to_owned();
    params.tool_context_base = Arc::new(move || make_test_ctx(&context_root));

    let events = query(params, deps_with(scripted_call_model(vec![first, second])))
        .collect::<Vec<_>>()
        .await;

    // Surface the tool's own error when this fails: a bare boolean assertion here
    // leaves a flake with no evidence of what the grep call actually reported.
    let surrogate_call = events.iter().find_map(|event| match event {
        EngineEvent::ItemCompleted {
            inference: _,
            item: ThreadItem::ToolCall { id, error, .. },
        } if id == "grep-surrogate-path" => Some(error.clone()),
        _ => None,
    });
    assert_eq!(
        surrogate_call,
        Some(None),
        "grep on a lone-surrogate path must complete without a tool error"
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn query_persists_call_and_result_kind_plus_native_search_output() {
    let raw_seen = Arc::new(Mutex::new(Vec::new()));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(RawRecordingTool {
            schema: JsonSchema::new(),
            seen: Arc::clone(&raw_seen),
        }),
        Arc::new(NativeSearchRecordingTool {
            schema: JsonSchema::new(),
            output: native_search_output_fixture(),
        }),
    ];
    let first = vec![
        ev::message_start(Some("gpt-5")),
        ModelStreamEvent::ToolUseStart {
            id: "c1".into(),
            name: "apply_patch".into(),
            thought_signature: None,
            tool_kind: Some(crate::types::provider::ProviderToolCallKind::Custom),
            input_format: Some(crate::types::provider::ToolInputFormat::Raw),
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "c1".into(),
            partial_json: "*** Begin Patch\n*** End Patch".into(),
        },
        ModelStreamEvent::ToolUseStop { id: "c1".into() },
        ModelStreamEvent::ToolUseStart {
            id: "s1".into(),
            name: "tool_search".into(),
            thought_signature: None,
            tool_kind: Some(crate::types::provider::ProviderToolCallKind::ToolSearch),
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "s1".into(),
            partial_json: r#"{"query":"calendar"}"#.into(),
        },
        ModelStreamEvent::ToolUseStop { id: "s1".into() },
        ev::stop(Some(ModelStopReason::ToolUse), None),
    ];
    let turns = vec![
        first,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ];
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("patch then search")], tools);
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events: Vec<EngineEvent> = query(params, deps_with(scripted_call_model(turns)))
        .collect()
        .await;

    assert_eq!(
        *raw_seen.lock().unwrap(),
        vec![runtime_json!("*** Begin Patch\n*** End Patch")]
    );
    let history = persisted.lock().unwrap();
    let calls: Vec<&ToolUseBlock> = history
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::ToolUse(call) => Some(call),
            _ => None,
        })
        .collect();
    let custom = calls.iter().copied().find(|call| call.id == "c1").unwrap();
    assert_eq!(
        custom.input,
        runtime_json!("*** Begin Patch\n*** End Patch")
    );
    assert_eq!(
        custom.tool_kind,
        Some(crate::types::provider::ProviderToolCallKind::Custom)
    );
    assert!(runtime_codec::encode_value(custom)
        .unwrap()
        .get("inputFormat")
        .is_none());

    let results: Vec<&ToolResultMessage> = history
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result),
            _ => None,
        })
        .collect();
    let custom_result = results
        .iter()
        .copied()
        .find(|result| result.tool_use_id == "c1")
        .unwrap();
    assert_eq!(
        custom_result.tool_kind,
        Some(crate::types::provider::ProviderToolCallKind::Custom)
    );
    assert_eq!(custom_result.native_tool_output, None);

    let search_result = results
        .iter()
        .copied()
        .find(|result| result.tool_use_id == "s1")
        .unwrap();
    assert_eq!(
        search_result.tool_kind,
        Some(crate::types::provider::ProviderToolCallKind::ToolSearch)
    );
    assert_eq!(
        search_result.native_tool_output,
        Some(native_search_output_fixture())
    );
}

#[test]
fn request_projection_uses_the_effective_model_and_keeps_qwen_generic() {
    let task = create_task_tool(TaskToolOptions::default());
    let mut params = mk_params(vec![user_msg("delegate this")], vec![task]);
    params.multi_agent_instructions = true;
    params.model = "qwen-max".into();
    params.provider_info = Some(crate::types::provider::ProviderInfo {
        name: "openai-compatible".into(),
        base_url: None,
        env_key: None,
        default_model: "qwen-max".into(),
    });
    params.effort = Some(EffortValue::Number(3));
    params.config.task_budget_tokens = Some(4_096);
    params.config.context_management = Some(crate::schemas::config::ContextManagementConfig {
        trigger_input_tokens: Some(30_000),
        keep_tool_uses: Some(3),
        ..Default::default()
    });
    params.output_schema = Some(runtime_json!({
        "type": "object",
        "properties": {
            "answer": { "type": "string" }
        }
    }));
    params.web_search = Some(crate::types::provider::WebSearchRequest {
        max_uses: Some(2),
        ..Default::default()
    });
    params.thinking = Some(ThinkingConfig::Adaptive);
    params.skip_cache_write = Some(true);
    params.cache_ttl = Some(CacheTtl::OneHour);
    params.cache_key = Some("projection-cache".into());

    let base_system = vec![SystemPromptPart {
        name: "base".into(),
        text: "Canonical base instructions.".into(),
        cacheable: Some(true),
        cache_scope: None,
    }];
    let internal_task_specs = params.tool_specs.clone();
    let generic = prepare_model_request(
        &params.messages,
        &params,
        &base_system,
        1_234,
        &internal_task_specs,
        None,
    );

    assert_eq!(generic.profile.prompt_style, ProviderPromptStyle::Generic);
    assert_eq!(
        resolve_provider_schema_strategy(&generic.profile).provider,
        ProviderSchemaProvider::OpenAi,
        "OpenAI-compatible schema capability must stay separate from Qwen facade behavior"
    );
    assert_eq!(generic.request.model, "qwen-max");
    assert_eq!(generic.request.effort, Some(EffortValue::Number(3)));
    assert_eq!(
        generic.request.system,
        vec![
            base_system[0].clone(),
            crate::context::instruction_policy::permission_instructions(
                &(params.tool_context_base)(),
                params.approval_handler.is_some(),
            ),
            expected_collaboration_part("root_role"),
            expected_collaboration_part("explicit_mode")
        ]
    );
    assert_eq!(
        generic
            .request
            .tools
            .iter()
            .map(|tool| exact_test_string(&tool.name))
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("task")]
    );
    assert_eq!(
        build_model_request(
            &params.messages,
            &params,
            &base_system,
            1_234,
            &internal_task_specs,
            None,
        ),
        generic.request
    );

    let turn_override = TurnOverride {
        model: "gpt-5".into(),
        effort: Some(EffortValue::Number(9)),
        ultra_mode: false,
    };
    let openai = prepare_model_request(
        &params.messages,
        &params,
        &base_system,
        1_234,
        &internal_task_specs,
        Some(&turn_override),
    );

    assert_eq!(
        openai.profile.prompt_style,
        ProviderPromptStyle::OpenAiDeveloper
    );
    assert_eq!(openai.request.model, "gpt-5");
    assert_eq!(openai.request.effort, Some(EffortValue::Number(9)));
    assert_eq!(openai.request.system[0], base_system[0]);
    assert_eq!(openai.request.system[1], generic.request.system[1]);
    assert_eq!(
        openai.request.system[2],
        expected_collaboration_part("root_role")
    );
    assert_eq!(
        openai.request.system[3],
        expected_collaboration_part("explicit_mode")
    );
    assert_eq!(openai.request.system.len(), 5);
    let behavior = openai.request.system.last().unwrap();
    assert_eq!(behavior.name, "provider-behavior:openai-developer");
    assert!(behavior.text.starts_with("# Provider Behavior\n\n"));
    assert_eq!(
        openai
            .request
            .tools
            .iter()
            .map(|tool| exact_test_string(&tool.name))
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("spawn_agent")]
    );
    assert_eq!(
        build_model_request(
            &params.messages,
            &params,
            &base_system,
            1_234,
            &internal_task_specs,
            Some(&turn_override),
        ),
        openai.request
    );
    assert_eq!(internal_task_specs[0].name, "task");

    for request in [&generic.request, &openai.request] {
        assert_eq!(request.messages, params.messages);
        assert_eq!(request.tool_choice, Some(ToolChoice::Auto));
        assert_eq!(request.max_output_tokens, 1_234);
        assert_eq!(request.task_budget, params.config.task_budget_tokens);
        assert_eq!(
            request.context_management,
            params.config.context_management.clone()
        );
        assert_eq!(request.output_schema, params.output_schema.clone());
        assert_eq!(request.web_search, params.web_search);
        assert_eq!(request.thinking, params.thinking);
        assert_eq!(request.skip_cache_write, Some(true));
        assert_eq!(request.cache_ttl, params.cache_ttl);
        assert_eq!(request.cache_key, params.cache_key.clone());
    }
}

#[test]
fn request_projection_preserves_exact_runtime_output_schema() {
    let mut params = mk_params(vec![user_msg("structured")], Vec::new());
    let key = RuntimeString::from_utf16(vec![b'k'.into(), 0xd800]);
    let value = RuntimeString::from_utf16(vec![b'v'.into(), 0xdc00]);
    let mut object = RuntimeObject::new();
    object.insert(key.clone(), RuntimeJson::String(value.clone()));
    let schema = RuntimeJson::Object(object);
    params.output_schema = Some(schema.clone());

    let prepared = prepare_model_request(&params.messages, &params, &[], 128, &[], None);

    assert_eq!(prepared.request.output_schema, Some(schema));
    let projected = prepared
        .request
        .output_schema
        .as_ref()
        .and_then(RuntimeJson::as_object)
        .and_then(|object| object.get(key))
        .and_then(RuntimeJson::as_str)
        .expect("surrogate-bearing property must remain present");
    assert_eq!(projected, &value);
    assert!(projected.try_to_utf8().is_err());
}

#[tokio::test]
async fn text_turn_emits_started_deltas_item_and_completed() {
    let turns: ScriptedTurns = vec![vec![
        ev::message_start(None),
        ev::text("hi"),
        ev::text(" there"),
        ev::stop(None, None),
    ]];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(matches!(
        evs.first(),
        Some(EngineEvent::ThreadStarted { .. })
    ));
    assert!(evs
        .iter()
        .any(|e| matches!(e, EngineEvent::TurnStarted { turn_id } if turn_id == "turn-1")));
    let text = join_runtime_strings(
        &evs.iter()
            .filter_map(|e| match e {
                EngineEvent::TextDelta { text, .. } => Some(exact_test_string(text)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        "",
    );
    assert_eq!(text, "hi there");
    assert!(evs.iter().any(|e| matches!(
            e,
            EngineEvent::ItemCompleted { inference: _, item: ThreadItem::AgentMessage { text, .. } } if text == "hi there"
        )));
    assert!(matches!(
        evs.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        })
    ));
}

/// A response that finishes normally while carrying nothing at all: no text, no
/// tool call. On screen that is the request followed by silence, so the turn is
/// settled as a failure that names it instead of a completion with nothing in it.
#[tokio::test]
async fn a_turn_that_produced_no_content_fails_instead_of_completing_silently() {
    let turns: ScriptedTurns = vec![vec![ev::message_start(None), ev::stop(None, None)]];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(!evs
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    let failure = evs
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .expect("an empty turn settles as a failure");
    assert_eq!(failure.code, EngineErrorCode::ProviderError);
    assert_eq!(
        failure.message,
        "the model returned no content for this turn"
    );
}

/// Thinking alone is not an answer either: the reasoning box is shown, the reply
/// is not, and the turn has to say so rather than read as finished. The model
/// gets one empty-closing retry; if it still returns only thinking, the turn
/// fails.
#[tokio::test]
async fn a_turn_with_only_thinking_is_retried_once_then_fails_as_contentless() {
    let turns: ScriptedTurns = vec![
        vec![
            ev::message_start(None),
            ev::thinking("weighing it up"),
            ev::stop(None, None),
        ],
        vec![
            ev::message_start(None),
            ev::thinking("still only reasoning"),
            ev::stop(None, None),
        ],
    ];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(
        evs.iter().any(
            |event| matches!(event, EngineEvent::Notification { level: _, message }
                if message.contains("empty response"))
        ),
        "the retry path must announce itself: {evs:?}"
    );
}

#[tokio::test]
async fn tool_only_assistant_segment_emits_no_agent_message() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "echo-1",
        "echo",
        &runtime_json!({ "value": 1 }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let second = vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ];
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut params = mk_params(
        vec![user_msg("use echo")],
        vec![Arc::new(EchoTool {
            schema: JsonSchema::new(),
        })],
    );
    let captured_cb = Arc::clone(&captured);
    params.on_message = Some(immediate_message_observer(move |message| {
        captured_cb.lock().unwrap().push(message);
    }));

    let events: Vec<EngineEvent> =
        query(params, deps_with(scripted_call_model(vec![first, second])))
            .collect()
            .await;
    let starts = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event, EngineEvent::StreamRequestStart { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(starts.len(), 2);
    assert!(!events[starts[0] + 1..starts[1]].iter().any(|event| {
        matches!(
            event,
            EngineEvent::ItemCompleted {
                inference: _,
                item: ThreadItem::AgentMessage { .. }
            }
        )
    }));
    let messages = captured.lock().unwrap();
    let first_assistant = messages
        .iter()
        .find_map(|message| match message {
            Message::Assistant(assistant) => Some(assistant),
            _ => None,
        })
        .unwrap();
    assert!(first_assistant
        .content
        .iter()
        .all(|block| !matches!(block, ContentBlock::Text(_))));
    assert!(first_assistant
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolUse(_))));
}

#[tokio::test]
async fn multi_text_block_assistant_segment_emits_one_concatenated_agent_message() {
    let mut first = vec![ev::message_start(None), ev::text("alpha")];
    first.extend(exact_tool_use_events(
        "echo-1",
        "echo",
        &runtime_json!({ "value": 1 }),
    ));
    first.push(ev::text("beta"));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let second = vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ];
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut params = mk_params(
        vec![user_msg("use echo")],
        vec![Arc::new(EchoTool {
            schema: JsonSchema::new(),
        })],
    );
    let captured_cb = Arc::clone(&captured);
    params.on_message = Some(immediate_message_observer(move |message| {
        captured_cb.lock().unwrap().push(message);
    }));

    let events: Vec<EngineEvent> =
        query(params, deps_with(scripted_call_model(vec![first, second])))
            .collect()
            .await;
    let starts = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event, EngineEvent::StreamRequestStart { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    let messages = captured.lock().unwrap();
    let first_assistant = messages
        .iter()
        .find_map(|message| match message {
            Message::Assistant(assistant) => Some(assistant),
            _ => None,
        })
        .unwrap();
    let text_blocks = first_assistant
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(exact_test_string(&text.text)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        text_blocks,
        vec![RuntimeString::from("alpha"), RuntimeString::from("beta")]
    );
    let segment = &events[starts[0] + 1..starts[1]];
    let delta_text = join_runtime_strings(
        &segment
            .iter()
            .filter_map(|event| match event {
                EngineEvent::TextDelta { text, .. } => Some(exact_test_string(text)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        "",
    );
    assert_eq!(delta_text, "alphabeta");
    let items = segment
        .iter()
        .filter_map(|event| match event {
            EngineEvent::ItemCompleted {
                inference: _,
                item: ThreadItem::AgentMessage { id, text, .. },
            } => Some((exact_test_string(id), exact_test_string(text))),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        items,
        vec![(
            exact_test_string(&first_assistant.uuid),
            RuntimeString::from("alphabeta"),
        )]
    );
}

#[tokio::test]
async fn refresh_tools_installs_search_for_new_deferred_surface() {
    let request_tools: Arc<Mutex<Vec<Vec<RuntimeString>>>> = Arc::new(Mutex::new(Vec::new()));
    let call_count = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let request_tools = request_tools.clone();
        move |request: ModelRequest, signal: AbortSignal| -> ModelStream {
            request_tools.lock().unwrap().push(
                request
                    .tools
                    .iter()
                    .map(|tool| exact_test_string(tool.name()))
                    .collect(),
            );
            let index = call_count.fetch_add(1, Ordering::SeqCst);
            let events = if index == 0 {
                let mut events = vec![ev::message_start(None)];
                events.extend(exact_tool_use_events(
                    "echo-1",
                    "echo",
                    &runtime_json!({ "value": 1 }),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                events
            } else {
                vec![
                    ev::message_start(None),
                    ev::text("done"),
                    ev::stop(None, None),
                ]
            };
            Box::pin(
                futures::stream::iter(events)
                    .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
            )
        }
    }));
    let echo = Arc::new(EchoTool {
        schema: JsonSchema::new(),
    }) as Arc<dyn Tool>;
    let deferred = Arc::new(SourceTool {
        name: "late_lookup",
        schema: JsonSchema::new(),
        mcp_server: Some("late-server"),
        exposure: ToolExposure::Deferred,
    }) as Arc<dyn Tool>;
    let mut params = mk_params(vec![user_msg("refresh tools")], vec![echo.clone()]);
    params.refresh_tools = Some(Box::new(move || vec![echo.clone(), deferred.clone()]));

    let _: Vec<EngineEvent> = query(params, deps).collect().await;

    let request_tools = request_tools.lock().unwrap();
    assert_eq!(request_tools.len(), 2);
    assert_eq!(request_tools[0], vec!["echo"]);
    assert!(request_tools[1].iter().any(|name| name == "tool_search"));
    assert!(!request_tools[1].iter().any(|name| name == "late_lookup"));
}

#[tokio::test]
async fn async_refresh_replaces_same_name_tool_before_the_next_model_step() {
    let descriptions = Arc::new(Mutex::new(Vec::<Vec<RuntimeString>>::new()));
    let prompts = Arc::new(Mutex::new(Vec::<Vec<RuntimeString>>::new()));
    let requests = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let descriptions = descriptions.clone();
        let prompts = prompts.clone();
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            descriptions.lock().unwrap().push(
                request
                    .tools
                    .iter()
                    .map(|tool| exact_test_string(&tool.description))
                    .collect(),
            );
            prompts.lock().unwrap().push(
                request
                    .system
                    .iter()
                    .map(|part| part.text.clone())
                    .collect(),
            );
            let events = if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut events = vec![ev::message_start(None)];
                events.extend(exact_tool_use_events(
                    "echo-1",
                    "echo",
                    &runtime_json!({ "value": 1 }),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                events
            } else {
                vec![
                    ev::message_start(None),
                    ev::text("done"),
                    ev::stop(None, None),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }));
    let original = Arc::new(EchoTool {
        schema: JsonSchema::new(),
    }) as Arc<dyn Tool>;
    let replacement = Arc::new(SourceTool {
        name: "echo",
        schema: JsonSchema::new(),
        mcp_server: Some("plugin-server"),
        exposure: ToolExposure::Direct,
    }) as Arc<dyn Tool>;
    let refreshed = Arc::new(AtomicUsize::new(0));
    let mut params = mk_params(vec![user_msg("refresh")], vec![original.clone()]);
    let prompt_version = Arc::new(AtomicUsize::new(0));
    params.system_prompt = SystemPromptResolvable::Fn(Arc::new(move || {
        let version = prompt_version.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            vec![SystemPromptPart {
                name: "mcp".into(),
                text: RuntimeString::from(format!("MCP generation {version}")),
                cacheable: None,
                cache_scope: None,
            }]
        })
    }));
    params.refresh_tools_async = Some(Box::new(move || {
        let refresh_system_prompt = refreshed.fetch_add(1, Ordering::SeqCst) > 0;
        let tool = if refresh_system_prompt {
            replacement.clone()
        } else {
            original.clone()
        };
        Box::pin(async move {
            Ok(super::AsyncToolRefresh {
                tools: vec![tool],
                refresh_system_prompt,
            })
        })
    }));
    let permissions = crate::context::instruction_policy::permission_instructions(
        &(params.tool_context_base)(),
        params.approval_handler.is_some(),
    )
    .text;
    let events = query(params, deps).collect::<Vec<_>>().await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert_eq!(
        *descriptions.lock().unwrap(),
        vec![
            vec![RuntimeString::from("echo tool")],
            vec![RuntimeString::from("source test tool")],
        ]
    );
    assert_eq!(
        *prompts.lock().unwrap(),
        vec![
            vec![RuntimeString::from("MCP generation 0"), permissions.clone()],
            vec![RuntimeString::from("MCP generation 1"), permissions],
        ]
    );
}

#[tokio::test]
async fn root_agent_definition_policy_survives_refresh_and_native_search() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let captured = requests.clone();
    let deps = deps_with(Box::new(move |request, _signal| {
        let mut requests = captured.lock().unwrap();
        requests.push(request);
        let mut events = vec![ev::message_start(None)];
        if requests.len() == 1 {
            events.extend(exact_tool_use_events(
                "echo-1",
                "echo",
                &runtime_json!({"value":1}),
            ));
            events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        } else {
            events.push(ev::text("done"));
            events.push(ev::stop(None, None));
        }
        Box::pin(futures::stream::iter(events))
    }));
    let echo = Arc::new(EchoTool {
        schema: JsonSchema::new(),
    }) as Arc<dyn Tool>;
    let deferred = Arc::new(SourceTool {
        name: "late_lookup",
        schema: JsonSchema::new(),
        mcp_server: Some("late-server"),
        exposure: ToolExposure::Deferred,
    }) as Arc<dyn Tool>;
    let forbidden = Arc::new(SourceTool {
        name: "forbidden",
        schema: JsonSchema::new(),
        mcp_server: Some("late-server"),
        exposure: ToolExposure::Direct,
    }) as Arc<dyn Tool>;
    // The initial pool models a lazy MCP merge after the Session's first filter.
    let mut params = mk_params(
        vec![user_msg("refresh")],
        vec![echo.clone(), forbidden.clone()],
    );
    params.agent_tool_policy = Some(crate::engine::agent_definitions::AgentDefinition {
        tools: Some(vec![
            "echo".into(),
            "late_lookup".into(),
            "forbidden".into(),
        ]),
        disallowed_tools: Some(vec!["forbidden".into()]),
        ..Default::default()
    });
    params.web_search = Some(crate::types::provider::WebSearchRequest::default());
    params.refresh_tools = Some(Box::new(move || {
        vec![echo.clone(), deferred.clone(), forbidden.clone()]
    }));
    let events = query(params, deps).collect::<Vec<_>>().await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(
            request
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>(),
            vec!["echo"]
        );
        assert!(request.web_search.is_none());
    }
}

#[tokio::test]
async fn root_agent_definition_policy_blocks_direct_and_nested_tool_execution() {
    for name in ["echo", "nested_invoke"] {
        let mut first = vec![ev::message_start(None)];
        first.extend(exact_tool_use_events(
            "blocked-call",
            name,
            &runtime_json!({}),
        ));
        first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        let mut params = mk_params(
            vec![user_msg("try forbidden tool")],
            vec![
                Arc::new(EchoTool {
                    schema: JsonSchema::new(),
                }),
                Arc::new(NestedInvokeTool {
                    schema: JsonSchema::new(),
                }),
            ],
        );
        params.agent_tool_policy = Some(crate::engine::agent_definitions::AgentDefinition {
            disallowed_tools: Some(vec!["echo".into()]),
            ..Default::default()
        });
        let records = Arc::new(Mutex::new(Vec::new()));
        let captured = records.clone();
        params.on_tool_lifecycle = Some(Box::new(move |record| {
            captured.lock().unwrap().push(record);
            Box::pin(async { Ok(()) })
        }));
        let _events = query(
            params,
            deps_with(scripted_call_model(vec![
                first,
                vec![
                    ev::message_start(None),
                    ev::text("done"),
                    ev::stop(None, None),
                ],
            ])),
        )
        .collect::<Vec<_>>()
        .await;
        let records = records.lock().unwrap();
        assert!(records.iter().any(|record| record.tool_use_id == "blocked-call" && record.is_error == Some(true)), "{name}: {records:?}");
        assert!(
            !records
                .iter()
                .any(|record| record.tool_name == "echo"
                    && record.phase == ToolLifecyclePhase::Started),
            "{name}: {records:?}"
        );
    }
}

#[tokio::test]
async fn provider_decision_events_are_emitted_for_model_requests() {
    let turns: ScriptedTurns = vec![vec![
        ev::message_start(None),
        ev::text("hi"),
        ev::stop(None, Some(crate::testing::make_usage(Some(3), Some(2)))),
    ]];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;
    let decisions: Vec<_> = evs
        .iter()
        .filter_map(|e| match e {
            EngineEvent::ProviderDecision { event, .. } => Some(event),
            _ => None,
        })
        .collect();

    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[0].kind, ProviderDecisionEventKind::RequestStarted);
    assert_eq!(decisions[0].provider_id, "test-provider");
    assert_eq!(decisions[0].model, "fake-model");
    assert_eq!(
        decisions[1].kind,
        ProviderDecisionEventKind::RequestCompleted
    );
    assert_eq!(
        decisions[1].usage.as_ref().map(|u| u.output_tokens),
        Some(2)
    );
}

#[tokio::test]
async fn establishment_retries_surface_as_decision_events_and_notifications() {
    // A provider-internal establishment retry arrives as a stream event and
    // must surface to hosts — structured (ProviderDecision) AND human-readable
    // (Notification) — without disturbing the assembled assistant message.
    let retry_error = ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind: ProviderErrorKind::Overloaded,
        message: "upstream capacity".into(),
        status: Some(529),
        retry_after_ms: Some(1_200),
        request_id: Some("req-establish-retry".into()),
    };
    let turns: ScriptedTurns = vec![vec![
        ModelStreamEvent::RetryScheduled {
            attempt: 1,
            max_attempts: 3,
            delay_ms: 1_200,
            error: retry_error,
            network_wait: false,
        },
        ev::message_start(None),
        ev::text("hi"),
        ev::stop(None, Some(crate::testing::make_usage(Some(3), Some(2)))),
    ]];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RetryScheduled
                && event.attempt == 1
                && event.delay_ms == Some(1_200)
                && event.provider_id == "test-provider"
                && event
                    .debug
                    .request_id
                    .as_ref()
                    .is_some_and(|request_id| request_id == "req-establish-retry")
    )));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::Notification { level: crate::types::events::NotificationLevel::Warn, message }
            if message.contains("retry 2/3 in 1200ms")
    )));
    // The retry report never leaks into the assembled assistant output.
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _, item: ThreadItem::AgentMessage { text, .. } }
            if text == "hi"
    )));
}

#[tokio::test]
async fn stream_breaks_retry_with_a_reconnecting_notice_and_stream_phase() {
    // A mid-stream break is the engine loop's retry budget: it tombstones the
    // partial reply, re-samples, and reports a structured decision carrying the
    // stream phase plus the configured budget, alongside a "Reconnecting... n/m"
    // notice (debug builds keep the first notice visible).
    let stream_error = ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind: ProviderErrorKind::Network,
        message: "stream closed".into(),
        status: None,
        retry_after_ms: None,
        request_id: None,
    };
    let turns: ScriptedTurns = vec![
        vec![
            ev::message_start(None),
            ev::text("half an ans"),
            ModelStreamEvent::Error {
                error: stream_error,
            },
        ],
        vec![
            ev::message_start(None),
            ev::text("hi"),
            ev::stop(None, Some(crate::testing::make_usage(Some(3), Some(2)))),
        ],
    ];
    let deps = deps_with(scripted_call_model(turns));
    let params = mk_params(vec![user_msg("hello")], Vec::new());
    let budget = params.config.max_retries;
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RetryScheduled
                && event.phase == Some(crate::provider::observability::RetryPhase::Stream)
                && event.max_attempts == Some(budget)
                && event.attempt == 1
    )));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::Notification { level: crate::types::events::NotificationLevel::Warn, message }
            if message == &format!("Reconnecting... 1/{budget}")
    )));
    // The partial reply is withdrawn before the re-sample.
    assert!(evs
        .iter()
        .any(|e| matches!(e, EngineEvent::Tombstone { .. })));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _, item: ThreadItem::AgentMessage { text, .. } }
            if text == "hi"
    )));
}

#[tokio::test]
async fn provider_decision_events_record_model_fallbacks() {
    let err = ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind: ProviderErrorKind::Unknown,
        message: "model not found: fake-model".into(),
        status: Some(404),
        retry_after_ms: None,
        request_id: Some("req-missing-model".into()),
    };
    let turns: ScriptedTurns = vec![
        vec![ev::message_start(None), ev::error(err)],
        vec![
            ev::message_start(None),
            ev::text("backup ok"),
            ev::stop(None, Some(crate::testing::make_usage(Some(1), Some(4)))),
        ],
    ];
    let deps = deps_with(scripted_call_model(turns));
    let mut params = mk_params(vec![user_msg("hello")], Vec::new());
    params.config.fallback_models = vec!["backup-model".into()];

    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::ModelFallback
                && event.provider_id == "test-provider"
                && event.model == "fake-model"
                && event.next_model.as_ref().is_some_and(|model| model == "backup-model")
                && event
                    .debug
                    .request_id
                    .as_ref()
                    .is_some_and(|request_id| request_id == "req-missing-model")
    )));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RequestCompleted
                && event.model == "backup-model"
                && event.usage.as_ref().map(|u| u.output_tokens) == Some(4)
    )));
}

#[tokio::test]
async fn model_fallback_rebuilds_provider_behavior_profile() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let call_count = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        let call_count = Arc::clone(&call_count);
        move |request: ModelRequest, signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            let events = if call_count.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![
                    ev::message_start(Some("gpt-5")),
                    ev::error(ProviderError {
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                        kind: ProviderErrorKind::Overloaded,
                        message: "overloaded".into(),
                        status: Some(529),
                        retry_after_ms: None,
                        request_id: None,
                    }),
                ]
            } else {
                vec![
                    ev::message_start(Some("claude-sonnet")),
                    ev::text("done"),
                    ev::stop(None, None),
                ]
            };
            Box::pin(
                futures::stream::iter(events)
                    .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
            )
        }
    }));
    let task = create_task_tool(TaskToolOptions::default());
    let mut params = mk_params(vec![user_msg("delegate")], vec![task]);
    params.model = "gpt-5".into();
    params.provider_info = Some(crate::types::provider::ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.config.max_retries = 0;
    params.config.fallback_models = vec!["claude-sonnet".into()];

    let _: Vec<EngineEvent> = query(params, deps).collect().await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]
            .tools
            .iter()
            .map(|tool| exact_test_string(&tool.name))
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("spawn_agent")]
    );
    assert_eq!(
        requests[1]
            .tools
            .iter()
            .map(|tool| exact_test_string(&tool.name))
            .collect::<Vec<_>>(),
        vec![RuntimeString::from("Task")]
    );
    assert!(requests[0]
        .system
        .iter()
        .any(|part| part.name == "provider-behavior:openai-developer"));
    assert!(!requests[0]
        .system
        .iter()
        .any(|part| part.name == "provider-behavior:anthropic-reminder"));
    assert!(requests[1].system.iter().any(|part| {
        part.name == "provider-behavior:anthropic-reminder"
            && part
                .text
                .starts_with("<system-reminder>\nHarness guidance:")
    }));
    assert!(!requests[1]
        .system
        .iter()
        .any(|part| part.name == "provider-behavior:openai-developer"));
}

#[tokio::test]
async fn mixed_family_fallback_resolves_each_attempt_with_its_prepared_profile() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let request_models = Arc::new(Mutex::new(Vec::<RuntimeString>::new()));
    let deps = deps_with(Box::new({
        let call_count = Arc::clone(&call_count);
        let request_models = Arc::clone(&request_models);
        move |request: ModelRequest, signal: AbortSignal| -> ModelStream {
            let model = exact_test_string(&request.model);
            request_models.lock().unwrap().push(model.clone());
            let event_model = if model == "gpt-5" {
                "gpt-5"
            } else if model == "claude-sonnet" {
                "claude-sonnet"
            } else {
                "fake-model"
            };
            let mut events = vec![ev::message_start(Some(event_model))];
            match call_count.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    events.extend(exact_tool_use_events(
                        "same-call",
                        "spawn_agent",
                        &runtime_json!({"task_name":"primary","message":"openai"}),
                    ));
                    events.push(ev::error(ProviderError {
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                        kind: ProviderErrorKind::Unknown,
                        message: "model not found: gpt-5".into(),
                        status: Some(404),
                        retry_after_ms: None,
                        request_id: Some("missing-gpt".into()),
                    }));
                }
                1 => {
                    events.extend(exact_tool_use_events(
                        "same-call",
                        "Task",
                        &runtime_json!({"description":"backup","prompt":"anthropic"}),
                    ));
                    events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                }
                _ => {
                    events.push(ev::text("done"));
                    events.push(ev::stop(None, None));
                }
            }
            Box::pin(
                futures::stream::iter(events)
                    .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
            )
        }
    }));
    let seen = Arc::new(Mutex::new(Vec::<Json>::new()));
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::clone(&seen),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let lifecycle = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("delegate")], vec![tool]);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.config.max_retries = 0;
    params.config.fallback_models = vec!["claude-sonnet".into()];
    let lifecycle_sink = Arc::clone(&lifecycle);
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        lifecycle_sink.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));
    let persisted_sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        persisted_sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert_eq!(
        *request_models.lock().unwrap(),
        vec![
            RuntimeString::from("gpt-5"),
            RuntimeString::from("claude-sonnet"),
            RuntimeString::from("claude-sonnet"),
        ]
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![runtime_json!({"description":"backup","prompt":"anthropic"})]
    );
    assert_eq!(
        lifecycle
            .lock()
            .unwrap()
            .iter()
            .filter(|record| {
                record.tool_use_id == "same-call" && record.phase == ToolLifecyclePhase::Requested
            })
            .map(|record| {
                (
                    exact_test_string(&record.tool_name),
                    record.arguments.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                RuntimeString::from("task"),
                Some(runtime_json!({"description":"primary","name":"primary","prompt":"openai"}))
            ),
            (
                RuntimeString::from("task"),
                Some(runtime_json!({"description":"backup","prompt":"anthropic"}))
            ),
        ]
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::ItemStarted {
                    inference: _,
                    item:
                        ThreadItem::ToolCall {
                            id,
                            tool_name,
                            arguments,
                            ..
                        },
                } if id == "same-call" => Some((exact_test_string(tool_name), arguments.clone(),)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![(
            RuntimeString::from("task"),
            runtime_json!({"description":"backup","prompt":"anthropic"})
        )]
    );
    let persisted = persisted.lock().unwrap();
    let visible = persisted_tool_call(&persisted, "same-call");
    assert_eq!(visible.name, "Task");
    assert_eq!(
        visible.input,
        runtime_json!({"description":"backup","prompt":"anthropic"})
    );
    assert!(!persisted.iter().any(|message| matches!(
        message,
        Message::Assistant(assistant) if assistant.content.iter().any(|block| matches!(
            block,
            ContentBlock::ToolUse(call)
                if call.id == "same-call" && call.name == "spawn_agent"
        ))
    )));
}

/// A continuation the query created is stored as RENDERED text, so a provider
/// fallback mid-turn does not rewrite it into the new family's style. The vehicle is
/// the structured-output retry, one of the query-owned continuations.
#[tokio::test]
async fn mixed_family_fallback_keeps_the_created_query_continuation_envelope() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let call_count = Arc::clone(&call_count);
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request.clone());
            let events = match call_count.fetch_add(1, Ordering::SeqCst) {
                0 => vec![
                    ev::message_start(Some("gpt-5")),
                    ev::text("not json"),
                    ev::stop(None, None),
                ],
                1 => vec![ev::error(ProviderError {
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    kind: ProviderErrorKind::Unknown,
                    message: "model not found: gpt-5".into(),
                    status: Some(404),
                    retry_after_ms: None,
                    request_id: Some("missing-gpt".into()),
                })],
                _ => vec![
                    ev::message_start(Some("claude-sonnet")),
                    ev::text("{}"),
                    ev::stop(None, None),
                ],
            };
            Box::pin(futures::stream::iter(events))
        }
    }));
    let mut params = mk_params(vec![user_msg("continue across fallback")], Vec::new());
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.output_schema = Some(runtime_json!({"type": "object"}));
    params.config.max_retries = 0;
    params.config.fallback_models = vec!["claude-sonnet".into()];

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|request| exact_test_string(&request.model))
            .collect::<Vec<_>>(),
        vec![
            RuntimeString::from("gpt-5"),
            RuntimeString::from("gpt-5"),
            RuntimeString::from("claude-sonnet"),
        ]
    );
    let continuation_text = |request: &ModelRequest| {
        request
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::User(user) if user.is_meta == Some(true) => {
                    user.content.iter().find_map(|block| match block {
                        ContentBlock::Text(text) => Some(text.text.clone()),
                        _ => None,
                    })
                }
                _ => None,
            })
            .expect("continuation user message")
    };
    const BODY: &str = "Your previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences.";
    assert_eq!(
        continuation_text(&requests[1]),
        format!("# System Reminder\n\n{BODY}")
    );
    assert_eq!(
        continuation_text(&requests[2]),
        format!("# System Reminder\n\n{BODY}"),
        "the engine stores the continuation as rendered text when it is created; a later in-request fallback does not rewrite persisted history"
    );
}

/// A truncated response ends the turn and says so. The budget already is what the
/// model accepts, so there is nothing to raise and re-asking would spend another
/// response to land in the same place; what the user needs is to be told the answer
/// A response cut off with visible text on screen is continued rather than
/// settled: the answer exists but stops mid-sentence, so the loop asks for the
/// rest. Once the ladder is spent the truncation is written INTO the message
/// body, which is what makes it survive a reload.
#[tokio::test]
async fn a_truncated_response_continues_then_footnotes_the_body() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            // Every response cuts off mid-answer, so the ladder is walked to
            // exhaustion.
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("gpt-5")),
                ev::text("partial"),
                ev::stop(Some(ModelStopReason::MaxTokens), None),
            ]))
        }
    }));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("finish the answer")], Vec::new());
    params.model = "gpt-5".into();
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;

    // One original request plus one per continuation rung.
    assert_eq!(
        requests.lock().unwrap().len(),
        1 + super::MAX_TRUNCATION_CONTINUATIONS as usize,
        "each truncation asks for the rest until the ladder is spent"
    );
    // The exhausted ladder does not fail a turn whose answer is on screen.
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "a truncated answer that was continued still settles as a completion"
    );
    // The footnote is part of the persisted message body, not a side notice.
    let persisted = persisted.lock().unwrap();
    let last_assistant = persisted
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::Assistant(assistant) => Some(assistant),
            _ => None,
        })
        .expect("an assistant message was persisted");
    let body = last_assistant
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .fold(RuntimeString::default(), |joined, text| {
            joined.concat(&text)
        });
    let body = String::from_utf8_lossy(&body.to_node_utf8()).into_owned();
    assert!(
        body.contains("Output was truncated due to token limit"),
        "the persisted answer carries the truncation footnote: {body:?}"
    );
    // The live screen renders assistant text from deltas, not from the completed
    // item, so the footnote has to arrive there too — otherwise it would only
    // appear after a reload, and the reader watching the turn would still see a
    // sentence that simply stops.
    let streamed_footnote = events.iter().any(|event| match event {
        EngineEvent::TextDelta { text, .. } => String::from_utf8_lossy(&text.to_node_utf8())
            .contains("Output was truncated due to token limit"),
        _ => false,
    });
    assert!(
        streamed_footnote,
        "the footnote also streams live, so it is visible without a reload"
    );
}

#[tokio::test]
async fn approval_native_end_turn_false_continues_without_synthetic_input() {
    use crate::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
    use crate::types::provider::ModelProvider;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (header_end, length) = loop {
                let mut chunk = [0; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
                assert!(
                    request.len() <= 1024 * 1024,
                    "fixture request must remain bounded"
                );
                if let Some(end) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + length {
                        break (end, length);
                    }
                }
            };
            bodies.push(
                crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                    std::str::from_utf8(&request[header_end..header_end + length]).unwrap(),
                ))
                .unwrap(),
            );
            let text = if index == 0 {
                "Inspecting."
            } else {
                r#"{"outcome":"allow"}"#
            };
            let mut item = runtime_json!({"id":"message", "type":"message", "role":"assistant", "phase":"final_answer", "content":[]});
            item.as_object_mut().unwrap().insert(
                "phase",
                RuntimeJson::from(if index == 0 {
                    "commentary"
                } else {
                    "final_answer"
                }),
            );
            let mut content = runtime_json!({"type":"output_text", "text":""});
            content
                .as_object_mut()
                .unwrap()
                .insert("text", RuntimeJson::from(text));
            let mut done = item.clone();
            done.as_object_mut()
                .unwrap()
                .insert("content", RuntimeJson::Array(vec![content]));
            let mut complete = runtime_json!({"type":"response.completed", "response":{"id":"response", "output":[]}});
            complete
                .get_mut("response")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("end_turn", RuntimeJson::Bool(index == 1));
            let mut added = runtime_json!({"type":"response.output_item.added", "output_index":0});
            added.as_object_mut().unwrap().insert("item", item);
            let mut finished =
                runtime_json!({"type":"response.output_item.done", "output_index":0});
            finished.as_object_mut().unwrap().insert("item", done);
            let mut delta = runtime_json!({"type":"response.output_text.delta", "item_id":"message", "output_index":0, "content_index":0, "delta":""});
            delta
                .as_object_mut()
                .unwrap()
                .insert("delta", RuntimeJson::from(text));
            let stream = [added, delta, finished, complete]
                .into_iter()
                .map(|event| {
                    format!(
                        "data: {}\n\n",
                        crate::runtime_values::runtime_json::stringify(&event)
                            .try_to_utf8()
                            .unwrap()
                    )
                })
                .collect::<String>();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{stream}", stream.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        bodies
    });
    let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("fixture".into()),
        base_url: Some(format!("http://{address}/v1")),
        api: Some(OpenAiApi::Responses),
        ..Default::default()
    }));
    let model = RuntimeString::from("fake-model");
    let metadata = provider.model_metadata(&model);
    let provider =
        crate::provider::review_target::capture_review_model_target(provider, model, metadata);
    let deps = deps_with(Box::new(move |request, signal| {
        provider.stream(request, signal)
    }));
    let mut params = mk_params(vec![user_msg("Assess this action")], Vec::new());
    params.agent_type = Some("approval-review".into());
    params.config.max_retries = 0;
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        super::query_isolated(params, deps).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let bodies = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bodies.len(), 2);
    let second = bodies[1]["input"].as_array().unwrap();
    assert_eq!(
        second
            .iter()
            .filter(|item| item
                .get("role")
                .and_then(RuntimeJson::as_str)
                .is_some_and(|role| role == "user"))
            .count(),
        1
    );
    assert!(second.iter().any(|item| item
        .get("role")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|role| role == "assistant")));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
}

#[tokio::test]
async fn approval_stream_reconnects_once_without_retrying_establishment() {
    use crate::types::errors::ProviderFailurePhase;
    for streaming in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let deps = deps_with(Box::new({
            let calls = calls.clone();
            move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                calls.fetch_add(1, Ordering::SeqCst);
                let mut error = provider_error(ProviderErrorKind::Network, "Disconnected fixture");
                error.status = None;
                let error = if streaming {
                    error.mark_stream_interrupted()
                } else {
                    error
                };
                assert_eq!(
                    error.failure_phase(),
                    if streaming {
                        ProviderFailurePhase::Streaming
                    } else {
                        ProviderFailurePhase::Establishment
                    }
                );
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::error(error),
                ]))
            }
        }));
        let mut params = mk_params(vec![user_msg("Assess the action")], Vec::new());
        params.agent_type = Some("approval-review".into());
        params.config.max_retries = 17;
        let events = super::query_isolated(params, deps)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), if streaming { 2 } else { 1 });
        assert!(events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    }
}

#[tokio::test]
async fn approval_empty_completion_and_output_limit_do_not_synthesize_recoveries() {
    for reason in [ModelStopReason::EndTurn, ModelStopReason::MaxTokens] {
        let calls = Arc::new(AtomicUsize::new(0));
        let deps = deps_with(Box::new({
            let calls = calls.clone();
            move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::stop(Some(reason), None),
                ]))
            }
        }));
        let mut params = mk_params(vec![user_msg("Assess the action")], Vec::new());
        params.agent_type = Some("approval-review".into());
        let events = super::query_isolated(params, deps)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            reason == ModelStopReason::MaxTokens
        );
        assert!(!events.iter().any(|event| matches!(event, EngineEvent::Notification { message, .. } if message.contains("re-sampling") || message.contains("close or continue"))));
    }
}

#[tokio::test]
async fn approval_input_budget_blocks_the_actual_provider_boundary() {
    let calls = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let calls = calls.clone();
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("an over-budget request must not reach the provider factory");
        }
    }));
    let mut params = mk_params(vec![user_msg("Assess the action")], Vec::new());
    params.output_schema = Some(runtime_json!({"type":"object"}));
    params
        .output_schema
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("description", RuntimeJson::from("x".repeat(16_384)));
    let history = Arc::new(Mutex::new(params.messages.clone()));
    let admission = crate::engine::session::ApprovalQueryAdmission::new(
        1024,
        &HashSet::new(),
        history,
        Arc::new(crate::testing::fake_provider()),
        None,
    );
    params.provider_call_admission = Some(admission.clone());
    let events = super::query_isolated(params, deps)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let rejected = admission
        .rejected()
        .expect("the local budget failure remains typed");
    assert!(rejected.estimated_tokens > rejected.max_input_tokens);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
}

#[tokio::test]
async fn approval_authority_survives_a_tool_continuation_and_real_compaction() {
    let authority = "ONLY_APPROVED_ACTION_IS_INSPECTING_THE_LOCAL_WORKSPACE";
    let mut messages = overflow_history();
    if let Message::User(user) = &mut messages[0] {
        user.is_meta = Some(true);
        user.content = vec![ContentBlock::Text(TextBlock {
            text: authority.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })];
    }
    let original = messages[0].clone();
    let protected = HashSet::new();
    let restores = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let held = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut deps = deps_with(Box::new({
        let requests = requests.clone();
        let held = held.clone();
        let release = release.clone();
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let index = {
                let mut requests = requests.lock().unwrap();
                let index = requests.len();
                assert!(index < 4);
                requests.push(request);
                index
            };
            match index {
                0 => {
                    let mut events = vec![ev::message_start(None)];
                    events.extend(exact_tool_use_events(
                        "inspect-1",
                        "read",
                        &runtime_json!({}),
                    ));
                    events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                    Box::pin(futures::stream::iter(events))
                }
                1 => Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::error(provider_error(
                        ProviderErrorKind::PromptTooLong,
                        "input exceeds the context window",
                    )),
                ])),
                2 => {
                    let held = held.clone();
                    let release = release.clone();
                    Box::pin(async_stream::stream! {
                        held.notify_one(); release.notified().await;
                        yield ev::message_start(None);
                        for event in exact_tool_use_events("inspect-2", "read", &runtime_json!({})) { yield event; }
                        yield ev::stop(Some(ModelStopReason::ToolUse), None);
                    })
                }
                _ => Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("{\"outcome\":\"allow\"}"),
                    ev::stop(None, None),
                ])),
            }
        }
    }));
    let rescued = Arc::new(Mutex::new(false));
    deps.compactor = Arc::new(OverflowRescueCompactor {
        rescued: rescued.clone(),
        recover: true,
    });
    let tool = Arc::new(SourceTool {
        name: "read",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Direct,
    }) as Arc<dyn Tool>;
    let mut params = mk_params(messages.clone(), vec![tool]);
    params.agent_type = Some("approval-review".into());
    params.output_schema = Some(
        runtime_json!({"type":"object", "properties":{"outcome":{"type":"string"}}, "required":["outcome"], "additionalProperties":false}),
    );
    params.compaction_protected_message_ids = protected.clone();
    let history = Arc::new(Mutex::new(messages));
    let admission = crate::engine::session::ApprovalQueryAdmission::new(
        128_000,
        &protected,
        history.clone(),
        Arc::new(crate::testing::fake_provider()),
        Some(Arc::new({
            let original = original.clone();
            let restores = restores.clone();
            move |request| {
                let messages = if request.messages.iter().any(|message| message == &original) {
                    Vec::new()
                } else {
                    restores.fetch_add(1, Ordering::SeqCst);
                    vec![original.clone()]
                };
                Ok(crate::engine::session::ApprovalQueryRestoration {
                    messages,
                    system: Vec::new(),
                })
            }
        })),
    );
    params.provider_call_admission = Some(admission.clone());
    params.on_message = Some({
        let history = history.clone();
        immediate_message_observer(move |message| history.lock().unwrap().push(message))
    });
    params.on_compaction = Some({
        let history = history.clone();
        immediate_compaction_observer(move |record| {
            *history.lock().unwrap() = record.replacement_history
        })
    });
    let mut query = tokio::spawn(async move {
        super::query_isolated(params, deps)
            .collect::<Vec<_>>()
            .await
    });
    tokio::select! {
        _ = held.notified() => {},
        ended = &mut query => panic!("query ended before its protected continuation was held: {ended:?}"),
    }
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let actual = &requests[2];
        assert!(actual.messages.iter().any(|message| message == &original));
        assert!(actual.tools.iter().any(|tool| tool.name == "read"));
        assert!(actual.output_schema.is_some());
    }
    assert!(*rescued.lock().unwrap());
    release.notify_one();
    let events = query.await.unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(restores.load(Ordering::SeqCst), 1);
    let final_request = &requests[3];
    let authority_index = final_request
        .messages
        .iter()
        .position(|message| message == &original)
        .unwrap();
    let call_index = final_request.messages.iter().position(|message| matches!(message, Message::Assistant(message) if message.content.iter().any(|block| matches!(block, ContentBlock::ToolUse(call) if call.id == "inspect-2")))).unwrap();
    assert!(
        authority_index < call_index,
        "admitted authority must enter query history before the next tool round"
    );
    assert!(admission.rejected().is_none());
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn approval_query_leaves_tolerant_output_parsing_to_its_owner() {
    let calls = Arc::new(AtomicUsize::new(0));
    let deps = deps_with(Box::new({
        let calls = calls.clone();
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            assert_eq!(
                calls.fetch_add(1, Ordering::SeqCst),
                0,
                "the query must not resample this assessment"
            );
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("assessment-model")),
                ev::text("Assessment: {\"outcome\":\"allow\"}"),
                ev::stop(None, None),
            ]))
        }
    }));
    let mut params = mk_params(vec![user_msg("Assess the pending action")], Vec::new());
    params.agent_type = Some("approval-review".into());
    params.output_schema = Some(runtime_json!({
        "type":"object", "properties":{"outcome":{"type":"string"}},
        "required":["outcome"], "additionalProperties":false
    }));
    let events = super::query_isolated(params, deps)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::ItemCompleted {
        item: ThreadItem::AgentMessage {text, ..}, ..
    } if text == "Assessment: {\"outcome\":\"allow\"}")));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn structured_output_retry_persists_one_meta_nudge_into_next_history() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let scripted = Arc::new(Mutex::new(vec![
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("not json"),
            ev::stop(None, None),
        ],
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("{}"),
            ev::stop(None, None),
        ],
    ]));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        let scripted = Arc::clone(&scripted);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(scripted.lock().unwrap().remove(0)))
        }
    }));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("return json")], Vec::new());
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.output_schema = Some(runtime_json!({}));
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let persisted = persisted.lock().unwrap();
    let meta = persisted
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.is_meta == Some(true) => Some(user),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(meta.len(), 1);
    assert_eq!(
        text_block_text(&meta[0].content[0]),
        "# System Reminder\n\nYour previous response was not valid JSON. Respond again with ONLY a single valid JSON value matching the required schema — no prose, no code fences."
    );
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .filter(|message| matches!(
                message,
                Message::User(user) if user.is_meta == Some(true)
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn stop_and_subagent_stop_each_persist_one_meta_nudge_into_next_history() {
    for (agent_id, event) in [
        (None, HookEvent::Stop),
        (
            Some(RuntimeString::from("agent-1")),
            HookEvent::SubagentStop,
        ),
    ] {
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let scripted = Arc::new(Mutex::new(vec![
            vec![
                ev::message_start(Some("gpt-5")),
                ev::text("first"),
                ev::stop(None, None),
            ],
            vec![
                ev::message_start(Some("gpt-5")),
                ev::text("done"),
                ev::stop(None, None),
            ],
        ]));
        let deps = deps_with(Box::new({
            let requests = Arc::clone(&requests);
            let scripted = Arc::clone(&scripted);
            move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(request);
                Box::pin(futures::stream::iter(scripted.lock().unwrap().remove(0)))
            }
        }));
        let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
        let mut params = mk_params(vec![user_msg("finish")], Vec::new());
        params.model = "gpt-5".into();
        params.provider_info = Some(ProviderInfo {
            name: "openai".into(),
            base_url: None,
            env_key: None,
            default_model: "gpt-5".into(),
        });
        params.agent_id = agent_id;
        params.agent_type = Some("worker".into());
        params.hook_bus = Some(Arc::new(HookBus::new(
            crate::runtime_process::test_runtime_process_context(),
            vec![Hook {
                event,
                matcher: None,
                handler: HookHandler::Fn(Arc::new(OneShotBlockingHook {
                    calls: AtomicUsize::new(0),
                })),
                is_async: false,
            }],
        )));
        let sink = Arc::clone(&persisted);
        params.on_message = Some(immediate_message_observer(move |message| {
            sink.lock().unwrap().push(message);
        }));

        let _events = query(params, deps).collect::<Vec<_>>().await;

        let persisted = persisted.lock().unwrap();
        let meta = persisted
            .iter()
            .filter_map(|message| match message {
                Message::User(user) if user.is_meta == Some(true) => Some(user),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(meta.len(), 1, "{event:?}");
        assert_eq!(
            text_block_text(&meta[0].content[0]),
            "# System Reminder\n\ncontinue from hook"
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[1]
                .messages
                .iter()
                .filter(|message| matches!(
                    message,
                    Message::User(user) if user.is_meta == Some(true)
                ))
                .count(),
            1,
            "{event:?}"
        );
    }
}

#[tokio::test]
async fn token_budget_continuation_persists_one_meta_nudge_into_next_history() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let scripted = Arc::new(Mutex::new(vec![
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("first"),
            ev::stop(None, Some(crate::testing::make_usage(None, Some(100)))),
        ],
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, Some(crate::testing::make_usage(None, Some(900)))),
        ],
    ]));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        let scripted = Arc::clone(&scripted);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(scripted.lock().unwrap().remove(0)))
        }
    }));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("use the budget")], Vec::new());
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });
    params.token_budget = Some(1_000);
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let persisted = persisted.lock().unwrap();
    let meta = persisted
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.is_meta == Some(true) => Some(user),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(meta.len(), 1);
    assert!(matches!(&meta[0].content[0], ContentBlock::Text(text)
        if text.harness_envelope == Some(crate::schemas::content::HarnessEnvelope::SystemReminder)));
    assert!(
        text_block_text(&meta[0].content[0]).starts_with("# System Reminder\n\nYou are at ~10%")
    );
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests[1]
            .messages
            .iter()
            .filter(|message| matches!(
                message,
                Message::User(user) if user.is_meta == Some(true)
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn model_fallback_revalidates_native_pdf_before_provider_call() {
    let err = ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind: ProviderErrorKind::Unknown,
        message: "model not found: fake-model".into(),
        status: Some(404),
        retry_after_ms: None,
        request_id: None,
    };
    let deps = deps_with(scripted_call_model(vec![vec![
        ev::message_start(None),
        ev::error(err),
    ]]));
    let mut params = mk_params(
        vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "pdf-user".into(),
            content: vec![ContentBlock::Document(
                crate::schemas::content::DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "JVBERi0xLjcK".into(),
                    filename: Some("report.pdf".into()),
                },
            )],
            is_meta: None,
        })],
        Vec::new(),
    );
    params.config.fallback_models = vec!["text-only-model".into()];
    params.validate_native_pdf = Some(Arc::new(|model| {
        if model == "text-only-model" {
            Err(crate::types::errors::InputValidationError::new(
                crate::types::errors::InputValidationErrorCode::UnsupportedInput,
                "text-only model rejects native PDF",
            ))
        } else {
            Ok(())
        }
    }));

    let events: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.code == EngineErrorCode::InvalidInput
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::InputRejected {
            correlation_id: None,
            detail,
            ..
        } if detail.code == InputValidationErrorCode::UnsupportedInput
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RequestCompleted
                && event.model == "text-only-model"
    )));
}

#[tokio::test]
async fn tool_call_dispatches_then_completes() {
    // Turn 1: the model calls `echo`; turn 2 (script exhausted → fallback) ends.
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "echo", &runtime_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns: ScriptedTurns = vec![first];

    let deps = deps_with(scripted_call_model(turns));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool {
        schema: JsonSchema::new(),
    })];
    let params = mk_params(vec![user_msg("run echo")], tools);
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
            e,
            EngineEvent::ItemStarted { inference: _, item: ThreadItem::ToolCall { tool_name, .. } } if tool_name == "echo"
        )));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall { tool_name, status: ItemStatus::Completed, .. }
        } if tool_name == "echo"
    )));
    assert!(matches!(
        evs.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn real_query_wires_primary_rendered_openai_catalog_into_tool_context() {
    let catalogs = Arc::new(Mutex::new(Vec::<Vec<ToolCatalogEntry>>::new()));
    let probe = Arc::new(CatalogProbeTool {
        schema: JsonSchema::new(),
        catalogs: Arc::clone(&catalogs),
    }) as Arc<dyn Tool>;
    let send_message = create_send_message_tool();
    let tools = vec![probe, Arc::clone(&send_message)];

    let mut first = vec![ev::message_start(Some("gpt-5"))];
    first.extend(exact_tool_use_events(
        "catalog-call",
        "catalog_probe",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("inspect the catalog")], tools);
    params.model = "gpt-5".into();
    params.provider_info = Some(ProviderInfo {
        name: "openai".into(),
        base_url: None,
        env_key: None,
        default_model: "gpt-5".into(),
    });

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemCompleted { inference: _,
            item: ThreadItem::ToolCall {
                id,
                tool_name,
                status: ItemStatus::Completed,
                ..
            }
        } if id == "catalog-call" && tool_name == "catalog_probe"
    )));
    let catalogs = catalogs.lock().unwrap();
    assert_eq!(catalogs.len(), 1);
    let catalog = &catalogs[0];
    let internal_entries = catalog
        .iter()
        .filter(|entry| entry.name == "SendMessage")
        .collect::<Vec<_>>();
    assert_eq!(internal_entries.len(), 1);
    let entry = internal_entries[0];
    let primary = entry.spec.as_ref().expect("provider-rendered primary spec");
    assert_eq!(primary.name, "send_message");
    assert_eq!(entry.description, primary.description);
    assert!(entry.description.contains("Call `send_message`"));
    assert!(entry
        .search_hint
        .as_ref()
        .is_some_and(|value| value == "queue a message to a subagent or teammate"));
    assert_eq!(entry.exposure, ToolExposure::Direct);
    assert!(!catalog.iter().any(|catalog_entry| {
        catalog_entry.name == "followup_task"
            || catalog_entry
                .spec
                .as_ref()
                .is_some_and(|spec| spec.name == "followup_task")
    }));
}

#[tokio::test]
async fn tool_lifecycle_callback_records_success_order() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "echo",
        &runtime_json!({"value": 1}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let turns: ScriptedTurns = vec![first];

    let deps = deps_with(scripted_call_model(turns));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool {
        schema: JsonSchema::new(),
    })];
    let mut params = mk_params(vec![user_msg("run echo")], tools);
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    assert_eq!(
        records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![
            ToolLifecyclePhase::Requested,
            ToolLifecyclePhase::Started,
            ToolLifecyclePhase::Completed,
        ]
    );
    assert!(records.iter().all(|record| {
        record.turn_id == "turn-1" && record.tool_use_id == "t1" && record.tool_name == "echo"
    }));
    assert_eq!(records[0].arguments, Some(runtime_json!({"value": 1})));
    assert_eq!(records[2].is_error, Some(false));
}

#[tokio::test]
async fn tool_lifecycle_callback_records_cancelled_unstarted_tools() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "cancel_turn",
        &runtime_json!({}),
    ));
    first.extend(exact_tool_use_events("t2", "pending", &runtime_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(CancelTurnTool {
            schema: JsonSchema::new(),
        }),
        Arc::new(EchoTool {
            schema: JsonSchema::new(),
        }),
        Arc::new(PendingTool {
            schema: JsonSchema::new(),
        }),
    ];
    let mut params = mk_params(vec![user_msg("run echo")], tools);
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    let t2_phases = records
        .iter()
        .filter(|record| record.tool_use_id == "t2")
        .map(|record| record.phase)
        .collect::<Vec<_>>();
    assert_eq!(
        t2_phases,
        vec![ToolLifecyclePhase::Requested, ToolLifecyclePhase::Cancelled]
    );
}

#[tokio::test]
async fn tool_lifecycle_callback_records_failed_tools() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "missing_tool",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let mut params = mk_params(vec![user_msg("run missing")], Vec::new());
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    assert_eq!(
        records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![
            ToolLifecyclePhase::Requested,
            ToolLifecyclePhase::Started,
            ToolLifecyclePhase::Failed,
        ]
    );
    assert_eq!(records[2].is_error, Some(true));
    assert!(records[2]
        .error
        .as_ref()
        .is_some_and(|value| value == "Unknown tool: missing_tool"));
}

#[tokio::test]
async fn tool_lifecycle_callback_records_approval_and_progress() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "gated_progress",
        &runtime_json!({"step": 1}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(GatedProgressTool {
        schema: JsonSchema::new(),
    })];
    let mut params = mk_params(vec![user_msg("run gated progress")], tools);
    params.permission_engine = Arc::new(AskAll);
    params.approval_handler = Some(Arc::new(CountingApprovalHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    }));
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    assert_eq!(
        records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![
            ToolLifecyclePhase::Requested,
            ToolLifecyclePhase::Started,
            ToolLifecyclePhase::Approved,
            ToolLifecyclePhase::Progress,
            ToolLifecyclePhase::Completed,
        ]
    );
    let approved = records
        .iter()
        .find(|record| record.phase == ToolLifecyclePhase::Approved)
        .unwrap();
    assert_eq!(approved.approval_kind, Some(ApprovalKind::Tool));
    assert_eq!(approved.arguments, Some(runtime_json!({"step": 1})));

    let progress = records
        .iter()
        .find(|record| record.phase == ToolLifecyclePhase::Progress)
        .unwrap();
    assert_eq!(
        progress.event,
        Some(runtime_json!({ "type": "status", "message": "approved-progress" }))
    );
}

#[tokio::test]
async fn tool_lifecycle_callback_records_context_emit_progress() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "context_emit",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ContextEmitTool {
        schema: JsonSchema::new(),
    })];
    let mut params = mk_params(vec![user_msg("run context emit")], tools);
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    let progress = records
        .iter()
        .find(|record| record.phase == ToolLifecyclePhase::Progress)
        .unwrap();
    assert_eq!(
        progress.event,
        Some(runtime_json!({ "type": "status", "message": "from-context" }))
    );
}

#[tokio::test]
async fn tool_lifecycle_callback_records_requested_for_nested_invoked_tools() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "nested_invoke",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![
        Arc::new(NestedInvokeTool {
            schema: JsonSchema::new(),
        }),
        Arc::new(EchoTool {
            schema: JsonSchema::new(),
        }),
    ];
    let mut params = mk_params(vec![user_msg("run nested invoke")], tools);
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    let echo_records = records
        .iter()
        .filter(|record| record.tool_name == "echo")
        .collect::<Vec<_>>();
    assert_eq!(
        echo_records
            .iter()
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        vec![
            ToolLifecyclePhase::Requested,
            ToolLifecyclePhase::Started,
            ToolLifecyclePhase::Completed,
        ]
    );
    assert_eq!(
        echo_records[0].arguments.as_ref(),
        Some(&runtime_json!({ "nested": true }))
    );
    assert_eq!(
        echo_records[0].source.as_ref().unwrap().kind,
        ToolLifecycleSourceKind::Host
    );
}

#[test]
fn tool_lifecycle_source_classifies_tool_kinds() {
    let host = SourceTool {
        name: "host_custom",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Direct,
    };
    assert_eq!(
        tool_lifecycle_source_for_tool(&host).kind,
        ToolLifecycleSourceKind::Host
    );

    let mcp = SourceTool {
        name: "server_calc",
        schema: JsonSchema::new(),
        mcp_server: Some("calc_server"),
        exposure: ToolExposure::Deferred,
    };
    let source = tool_lifecycle_source_for_tool(&mcp);
    assert_eq!(source.kind, ToolLifecycleSourceKind::Mcp);
    assert!(source
        .name
        .as_ref()
        .is_some_and(|value| value == "calc_server"));
    assert!(source
        .exposure
        .as_ref()
        .is_some_and(|value| value == "deferred"));

    for (name, expected) in [
        ("bash", ToolLifecycleSourceKind::Shell),
        ("exec", ToolLifecycleSourceKind::CodeMode),
        ("workflow", ToolLifecycleSourceKind::Workflow),
        ("task", ToolLifecycleSourceKind::Subagent),
        ("browser_navigate", ToolLifecycleSourceKind::Browser),
    ] {
        let tool = SourceTool {
            name,
            schema: JsonSchema::new(),
            mcp_server: None,
            exposure: ToolExposure::Direct,
        };
        assert_eq!(tool_lifecycle_source_for_tool(&tool).kind, expected);
    }
}

#[tokio::test]
async fn tool_lifecycle_callback_records_source_metadata_on_requested() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "server_calc",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(SourceTool {
        name: "server_calc",
        schema: JsonSchema::new(),
        mcp_server: Some("calc_server"),
        exposure: ToolExposure::Deferred,
    })];
    let mut params = mk_params(vec![user_msg("run server calc")], tools);
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_cb = records.clone();
    params.on_tool_lifecycle = Some(Box::new(move |record| {
        records_cb.lock().unwrap().push(record);
        Box::pin(async { Ok(()) })
    }));

    let _evs: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = records.lock().unwrap().clone();
    let requested = records
        .iter()
        .find(|record| record.phase == ToolLifecyclePhase::Requested)
        .unwrap();
    let source = requested.source.as_ref().unwrap();
    assert_eq!(source.kind, ToolLifecycleSourceKind::Mcp);
    assert!(source
        .name
        .as_ref()
        .is_some_and(|value| value == "calc_server"));
    assert!(source
        .exposure
        .as_ref()
        .is_some_and(|value| value == "deferred"));
}

struct WriterGateTool {
    schema: JsonSchema,
    concurrent: bool,
    read_only: bool,
    effects: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for WriterGateTool {
    fn static_name(&self) -> &str {
        "writer_gate_tool"
    }
    fn static_description(&self) -> &str {
        "Records one effect before reporting progress"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        self.concurrent
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        self.read_only
    }
    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        let event = ToolDomainEvent::Status {
            message: "effect accepted".into(),
        };
        if self.read_only {
            (ctx.emit)(event).await;
        } else if let Some(progress) = on_progress {
            progress(event).await;
        }
        Ok(text_result(
            Json::Null,
            "effect completed".to_string(),
            false,
        ))
    }
}

#[tokio::test]
async fn dropping_query_retains_serial_parallel_and_prestarted_tool_persistence() {
    for (concurrent, read_only, count) in [(false, false, 1u32), (true, false, 2), (true, true, 1)]
    {
        let effects = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let records = Arc::new(Mutex::new(Vec::<ToolLifecycleRecord>::new()));
        let canonical = Arc::new(Mutex::new(Vec::<Message>::new()));
        let tasks = Arc::new(RetainedToolTasks::new());
        let mut first = vec![ev::message_start(None)];
        for index in 0..count {
            first.extend(exact_tool_use_events(
                &format!("writer-{index}"),
                "writer_gate_tool",
                &runtime_json!({}),
            ));
        }
        first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        let tool = Arc::new(WriterGateTool {
            schema: JsonSchema::new(),
            concurrent,
            read_only,
            effects: effects.clone(),
        }) as Arc<dyn Tool>;
        let mut params = mk_params(vec![user_msg("perform owned effects")], vec![tool]);
        params.retained_tool_tasks = Some(tasks.clone());
        let signal = params.signal.clone();
        params.on_message = Some(Box::new({
            let canonical = canonical.clone();
            move |message| {
                canonical.lock().unwrap().push(message);
                Box::pin(async { Ok(()) })
            }
        }));
        params.on_tool_lifecycle = Some(Box::new({
            let entered = entered.clone();
            let release = release.clone();
            let records = records.clone();
            move |record| {
                let entered = entered.clone();
                let release = release.clone();
                let records = records.clone();
                Box::pin(async move {
                    if record.phase == ToolLifecyclePhase::Progress {
                        entered.add_permits(1);
                        release.acquire_owned().await.unwrap().forget();
                    }
                    records.lock().unwrap().push(record);
                    Ok(())
                })
            }
        }));
        let mut stream = query(params, deps_with(scripted_call_model(vec![first])));
        assert!(stream.next().await.is_some());
        tokio::time::timeout(Duration::from_secs(2), entered.acquire_many_owned(count))
            .await
            .expect("tools reach the persistence boundary")
            .unwrap()
            .forget();
        assert_eq!(effects.load(Ordering::SeqCst), count as usize);
        if !read_only {
            assert!(
                canonical
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|message| { matches!(message, Message::Assistant(_)) }),
                "the dispatched exchange already has a canonical assistant"
            );
        }
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(5), stream.next()).await
        {
            assert!(
                !matches!(
                    event,
                    EngineEvent::ToolEvent { .. }
                        | EngineEvent::ItemCompleted {
                            item: ThreadItem::ToolCall { .. },
                            ..
                        }
                ),
                "progress and completion must follow persistence admission"
            );
        }
        assert!(records
            .lock()
            .unwrap()
            .iter()
            .all(|record| record.phase != ToolLifecyclePhase::Progress
                && record.phase != ToolLifecyclePhase::Completed));
        drop(stream);
        assert!(
            signal.is_cancelled(),
            "dropping the stream must stop its tools"
        );
        tasks.close();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), tasks.wait())
                .await
                .is_err(),
            "accepted effects must survive the pre-execution cancellation grace"
        );
        release.add_permits(count as usize);
        tokio::time::timeout(Duration::from_secs(2), tasks.wait())
            .await
            .expect("accepted tool persistence settles after capacity returns")
            .unwrap();
        let records = records.lock().unwrap();
        for index in 0..count {
            let id = RuntimeString::from(format!("writer-{index}"));
            let phases = records
                .iter()
                .filter(|record| record.tool_use_id == id)
                .map(|record| record.phase)
                .collect::<Vec<_>>();
            assert_eq!(
                phases,
                vec![
                    ToolLifecyclePhase::Requested,
                    ToolLifecyclePhase::Started,
                    ToolLifecyclePhase::Progress,
                    ToolLifecyclePhase::Completed
                ]
            );
        }
        assert_eq!(effects.load(Ordering::SeqCst), count as usize);
        if !read_only {
            let canonical = canonical.lock().unwrap();
            for index in 0..count {
                let id = RuntimeString::from(format!("writer-{index}"));
                assert_eq!(canonical.iter().filter(|message| {
                    matches!(message, Message::ToolResult(result) if result.tool_use_id == id)
                }).count(), 1, "a committed tool exchange retains its canonical result receiver");
            }
        }
    }
}

struct CancellationProbeTool {
    schema: JsonSchema,
    entered: Arc<tokio::sync::Semaphore>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
    progress: bool,
}

#[async_trait]
impl Tool for CancellationProbeTool {
    fn static_name(&self) -> &str {
        "cancellation_probe"
    }
    fn static_description(&self) -> &str {
        "Waits independently of its cancellation signal"
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let dropped = self.dropped.clone();
        let _drop = DropAction(Some(Box::new(move || {
            dropped.store(true, Ordering::Release);
        })));
        self.entered.add_permits(1);
        if self.progress {
            (ctx.emit)(ToolDomainEvent::Status {
                message: "accepted progress".into(),
            })
            .await;
        }
        futures::future::pending().await
    }
}

#[tokio::test]
async fn cancellation_releases_a_stalled_tool_after_accepted_progress_drains() {
    for progress in [false, true] {
        let tasks = Arc::new(RetainedToolTasks::new());
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let writer_entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let records = Arc::new(Mutex::new(Vec::new()));
        let tool = Arc::new(CancellationProbeTool {
            schema: JsonSchema::new(),
            entered: entered.clone(),
            dropped: dropped.clone(),
            progress,
        }) as Arc<dyn Tool>;
        let mut first = vec![ev::message_start(None)];
        first.extend(exact_tool_use_events(
            "cancel-probe",
            "cancellation_probe",
            &runtime_json!({}),
        ));
        first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        let mut params = mk_params(vec![user_msg("cancel this operation")], vec![tool]);
        params.retained_tool_tasks = Some(tasks.clone());
        params.on_tool_lifecycle = Some(Box::new({
            let writer_entered = writer_entered.clone();
            let release = release.clone();
            let records = records.clone();
            move |record| {
                let entered = writer_entered.clone();
                let release = release.clone();
                let records = records.clone();
                Box::pin(async move {
                    if record.phase == ToolLifecyclePhase::Progress {
                        entered.add_permits(1);
                        release.acquire_owned().await.unwrap().forget();
                    }
                    records.lock().unwrap().push(record.phase);
                    Ok(())
                })
            }
        }));
        let mut stream = query(params, deps_with(scripted_call_model(vec![first])));
        assert!(stream.next().await.is_some());
        tokio::time::timeout(Duration::from_secs(2), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if progress {
            tokio::time::timeout(Duration::from_secs(2), writer_entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        drop(stream);
        tasks.close();
        if progress {
            assert!(
                tokio::time::timeout(Duration::from_millis(150), tasks.wait())
                    .await
                    .is_err()
            );
            assert!(!dropped.load(Ordering::Acquire));
            release.add_permits(1);
        }
        tokio::time::timeout(Duration::from_secs(2), tasks.wait())
            .await
            .expect("accepted writes cannot retain a stalled tool after draining")
            .unwrap();
        assert!(dropped.load(Ordering::Acquire));
        let records = records.lock().unwrap();
        assert!(records.contains(&ToolLifecyclePhase::Cancelled));
        assert!(!records.contains(&ToolLifecyclePhase::Completed));
        if progress {
            assert!(
                records
                    .iter()
                    .position(|phase| *phase == ToolLifecyclePhase::Progress)
                    .unwrap()
                    < records
                        .iter()
                        .position(|phase| *phase == ToolLifecyclePhase::Cancelled)
                        .unwrap()
            );
        }
    }
}

#[tokio::test]
async fn closed_lifecycle_admission_prevents_effects_and_success_publication() {
    let effects = Arc::new(AtomicUsize::new(0));
    let tasks = Arc::new(RetainedToolTasks::new());
    let tool = Arc::new(WriterGateTool {
        schema: JsonSchema::new(),
        concurrent: false,
        read_only: false,
        effects: effects.clone(),
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "closed-start",
        "writer_gate_tool",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("perform effect")], vec![tool]);
    params.retained_tool_tasks = Some(tasks.clone());
    params.max_turns = Some(1);
    params.on_tool_lifecycle = Some(Box::new(|record| {
        Box::pin(async move {
            if record.phase == ToolLifecyclePhase::Started {
                Err(ToolError::fatal("writer is closed before tool execution"))
            } else {
                Ok(())
            }
        })
    }));
    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::ItemStarted {
            item: ThreadItem::ToolCall { .. },
            ..
        } | EngineEvent::ItemCompleted {
            item: ThreadItem::ToolCall { .. },
            ..
        }
    )));
    assert!(tasks
        .close_and_wait()
        .await
        .unwrap_err()
        .contains("writer is closed"));
}

#[tokio::test]
async fn failed_terminal_persistence_keeps_the_completed_physical_result() {
    let effects = Arc::new(AtomicUsize::new(0));
    let tasks = Arc::new(RetainedToolTasks::new());
    let tool = Arc::new(WriterGateTool {
        schema: JsonSchema::new(),
        concurrent: false,
        read_only: false,
        effects: effects.clone(),
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "closed-terminal",
        "writer_gate_tool",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(vec![user_msg("perform effect once")], vec![tool]);
    params.retained_tool_tasks = Some(tasks.clone());
    params.max_turns = Some(1);
    params.on_tool_lifecycle = Some(Box::new(|record| {
        Box::pin(async move {
            if record.phase == ToolLifecyclePhase::Completed {
                Err(ToolError::fatal("writer closed after the effect"))
            } else {
                Ok(())
            }
        })
    }));
    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(events.iter().any(|event| matches!(event,
        EngineEvent::ItemUpdated { item: ThreadItem::ToolCall {
            id, result: Some(Json::Null), error: Some(error), status: ItemStatus::Failed, ..
        }, .. } if id == "closed-terminal"
            && error.message.contains("effects may already be complete")
            && error.message.contains("do not repeat")
    )));
    assert!(!events.iter().any(|event| matches!(event,
        EngineEvent::ItemCompleted { item: ThreadItem::ToolCall { id, .. }, .. }
            if id == "closed-terminal"
    )));
    assert!(tasks
        .close_and_wait()
        .await
        .unwrap_err()
        .contains("writer closed after the effect"));
}

#[tokio::test]
async fn failed_surface_persistence_refuses_the_tool_search_result() {
    use crate::tools::builtins::tool_search::create_tool_search_tool;
    let calls = Arc::new(AtomicUsize::new(0));
    let tasks = Arc::new(RetainedToolTasks::new());
    let deferred = Arc::new(SourceTool {
        name: "deferred_probe",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Deferred,
    }) as Arc<dyn Tool>;
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "surface-closed",
        "tool_search",
        &runtime_json!({"query":"select:deferred_probe"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut params = mk_params(
        vec![user_msg("surface deferred tool")],
        vec![create_tool_search_tool(None), deferred],
    );
    params.max_turns = Some(1);
    params.retained_tool_tasks = Some(tasks.clone());
    params.on_surface_tools = Some(Box::new({
        let calls = calls.clone();
        move |_names| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ToolError::fatal("surface writer is closed"))
            })
        }
    }));
    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!events.iter().any(|event| matches!(event,
        EngineEvent::ItemCompleted { item: ThreadItem::ToolCall { id, .. }, .. }
            if id == "surface-closed"
    )));
    assert!(tasks
        .close_and_wait()
        .await
        .unwrap_err()
        .contains("surface writer is closed"));
}

#[tokio::test]
async fn dropping_an_unpolled_query_cancels_its_original_driver() {
    let tasks = Arc::new(RetainedToolTasks::new());
    let mut params = mk_params(vec![user_msg("stop before polling")], Vec::new());
    params.retained_tool_tasks = Some(tasks.clone());
    let signal = params.signal.clone();
    let stream = query(params, deps_with(scripted_call_model(Vec::new())));
    drop(stream);
    assert!(signal.is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), tasks.close_and_wait())
        .await
        .expect("the aborted driver releases its registration")
        .unwrap();
}

#[tokio::test]
async fn dropping_query_retains_an_accepted_message_callback_until_it_finishes() {
    let tasks = Arc::new(RetainedToolTasks::new());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let mut params = mk_params(vec![user_msg("keep the accepted response")], Vec::new());
    params.retained_tool_tasks = Some(tasks.clone());
    let signal = params.signal.clone();
    params.on_message = Some(Box::new({
        let entered = entered.clone();
        let release = release.clone();
        let persisted = persisted.clone();
        move |message| {
            let entered = entered.clone();
            let release = release.clone();
            let persisted = persisted.clone();
            Box::pin(async move {
                if matches!(&message, Message::Assistant(_)) {
                    entered.add_permits(1);
                    release.acquire_owned().await.unwrap().forget();
                }
                persisted.lock().unwrap().push(message);
                Ok(())
            })
        }
    }));
    let mut stream = query(
        params,
        deps_with(scripted_call_model(vec![vec![
            ev::message_start(None),
            ev::text("accepted response"),
            ev::stop(None, None),
        ]])),
    );
    assert!(stream.next().await.is_some());
    tokio::time::timeout(Duration::from_secs(2), entered.acquire_owned())
        .await
        .expect("the accepted message reaches persistence")
        .unwrap()
        .forget();
    drop(stream);
    assert!(signal.is_cancelled());
    tasks.close();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), tasks.wait())
            .await
            .is_err()
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), tasks.wait())
        .await
        .expect("the original driver finishes its accepted callback")
        .unwrap();
    assert!(persisted
        .lock()
        .unwrap()
        .iter()
        .any(|message| matches!(message,
            Message::Assistant(message) if message.content.iter().any(|block| matches!(block,
                ContentBlock::Text(text) if text.text == "accepted response"
            ))
        )));
}

#[tokio::test]
async fn message_persistence_failure_closes_success_publication_once() {
    let tasks = Arc::new(RetainedToolTasks::new());
    let mut params = mk_params(vec![user_msg("retain the response failure")], Vec::new());
    params.retained_tool_tasks = Some(tasks.clone());
    let signal = params.signal.clone();
    params.on_message = Some(Box::new(|_message| {
        Box::pin(async { Err(RuntimeString::from("response writer is closed")) })
    }));
    let events = query(
        params,
        deps_with(scripted_call_model(vec![vec![
            ev::message_start(None),
            ev::text("accepted response"),
            ev::stop(None, None),
        ]])),
    )
    .collect::<Vec<_>>()
    .await;
    assert!(signal.is_cancelled());
    let failures = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].message.contains("Message persistence failed"));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted { .. }
            | EngineEvent::ItemCompleted {
                item: ThreadItem::AgentMessage { .. },
                ..
            }
    )));
    assert!(tasks
        .close_and_wait()
        .await
        .unwrap_err()
        .contains("response writer is closed"));
}

#[tokio::test]
async fn tool_progress_callback_emits_tool_events() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "progress", &runtime_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ProgressTool {
        schema: JsonSchema::new(),
    })];
    let params = mk_params(vec![user_msg("run progress")], tools);
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ToolEvent {
            turn_id,
            tool_use_id,
            event: crate::types::events::ToolDomainEvent::Status { message },
        } if turn_id == "turn-1" && tool_use_id == "t1" && message == "halfway"
    )));
}

#[tokio::test]
async fn tool_context_emit_callback_emits_tool_events_for_the_current_turn() {
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "context_emit",
        &runtime_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let deps = deps_with(scripted_call_model(vec![first]));
    let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ContextEmitTool {
        schema: JsonSchema::new(),
    })];
    let params = mk_params(vec![user_msg("run context emit")], tools);
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;

    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ToolEvent {
            turn_id,
            tool_use_id,
            event: crate::types::events::ToolDomainEvent::Status { message },
        } if turn_id == "turn-1" && tool_use_id == "t1" && message == "from-context"
    )));
}

#[tokio::test]
async fn provider_pre_call_admission_failure_never_invokes_provider_factory() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let provider_calls_for_factory = provider_calls.clone();
    let call_model: crate::types::provider::CallModel = Box::new(move |_, _| {
        provider_calls_for_factory.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::empty())
    });
    let admission = Arc::new(TestProviderCallAdmission {
        events: events.clone(),
        before_result: Err("registry is closing".into()),
        returned_receipt: Arc::new(ready_provider_admission_receipt),
        panicked_receipt: Arc::new(ready_provider_admission_receipt),
    });
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.provider_call_admission = Some(admission);

    let emitted: Vec<EngineEvent> = query(params, deps_with(call_model)).collect().await;

    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(events.lock().unwrap().as_slice(), &["before_call"]);
    assert!(emitted.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.message.try_to_utf8().unwrap().contains("registry is closing")
    )));
}

#[tokio::test]
async fn provider_return_signal_precedes_callback_and_both_receipts_are_polled() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let provider_receipt_polled = Arc::new(tokio::sync::Notify::new());
    let request_receipt_polled = Arc::new(tokio::sync::Notify::new());
    let provider_receipt_polled_for_provider = provider_receipt_polled.clone();
    let request_receipt_polled_for_provider = request_receipt_polled.clone();
    let admission = Arc::new(TestProviderCallAdmission {
        events: events.clone(),
        before_result: Ok(()),
        returned_receipt: Arc::new(move || {
            let provider_polled = provider_receipt_polled_for_provider.clone();
            let request_polled = request_receipt_polled_for_provider.clone();
            Box::pin(async move {
                provider_polled.notify_one();
                request_polled.notified().await;
                Ok(())
            })
        }),
        panicked_receipt: Arc::new(ready_provider_admission_receipt),
    });

    let events_for_factory = events.clone();
    let call_model: crate::types::provider::CallModel = Box::new(move |_, _| {
        events_for_factory.lock().unwrap().push("call_model");
        // The turn has to answer with something: a response carrying no content
        // at all settles as a FAILED turn, which would make the completion
        // assertion below test empty-turn handling instead of admission order.
        Box::pin(futures::stream::iter(vec![
            ev::message_start(None),
            ev::text("ok"),
            ev::stop(None, None),
        ]))
    });
    let events_for_callback = events.clone();
    let provider_receipt_polled_for_callback = provider_receipt_polled.clone();
    let request_receipt_polled_for_callback = request_receipt_polled.clone();
    let callback: OnProviderRequestAdmitted = Arc::new(move || {
        events_for_callback.lock().unwrap().push("request_callback");
        let provider_polled = provider_receipt_polled_for_callback.clone();
        let request_polled = request_receipt_polled_for_callback.clone();
        Box::pin(async move {
            request_polled.notify_one();
            provider_polled.notified().await;
            Ok(())
        })
    });
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.provider_call_admission = Some(admission);
    params.on_provider_request_admitted = Some(callback);

    let emitted: Vec<EngineEvent> = query(params, deps_with(call_model)).collect().await;

    assert_eq!(
        events.lock().unwrap().as_slice(),
        &[
            "before_call",
            "call_model",
            "call_returned",
            "request_callback"
        ]
    );
    assert!(emitted.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn provider_factory_panic_is_caught_and_persists_outcome_unknown() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_factory = events.clone();
    let call_model: crate::types::provider::CallModel = Box::new(move |_, _| {
        events_for_factory.lock().unwrap().push("call_model");
        panic!("provider factory exploded")
    });
    let admission = Arc::new(TestProviderCallAdmission {
        events: events.clone(),
        before_result: Ok(()),
        returned_receipt: Arc::new(ready_provider_admission_receipt),
        panicked_receipt: Arc::new(ready_provider_admission_receipt),
    });
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.provider_call_admission = Some(admission);

    let emitted: Vec<EngineEvent> = query(params, deps_with(call_model)).collect().await;

    assert_eq!(
        events.lock().unwrap().as_slice(),
        &["before_call", "call_model", "call_panicked"]
    );
    assert!(emitted.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.message.try_to_utf8().unwrap().contains("provider factory exploded")
    )));
}

#[tokio::test]
async fn already_aborted_turn_completes_aborted() {
    let deps = deps_with(scripted_call_model(vec![vec![
        ev::message_start(None),
        ev::stop(None, None),
    ]]));
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    let signal = AbortSignal::new();
    signal.cancel();
    params.signal = signal;
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;
    assert!(matches!(
        evs.last(),
        Some(EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        })
    ));
}

#[tokio::test]
async fn provider_error_becomes_turn_failed() {
    let err = ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind: ProviderErrorKind::InvalidRequest,
        message: "bad request".into(),
        status: Some(400),
        retry_after_ms: None,
        request_id: Some("req-9".into()),
    };
    let deps = deps_with(scripted_call_model(vec![vec![
        ev::message_start(None),
        ev::error(err),
    ]]));
    let params = mk_params(vec![user_msg("hi")], Vec::new());
    let evs: Vec<EngineEvent> = query(params, deps).collect().await;
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::TurnFailed { error, .. } if error.code == EngineErrorCode::ProviderError
            && error.message == "bad request"
            && error.request_id.as_ref().is_some_and(|value| value == "req-9")
    )));
    assert!(evs.iter().any(|e| matches!(
        e,
        EngineEvent::ProviderDecision { event, .. }
            if event.kind == ProviderDecisionEventKind::RequestFailed
                && event.provider_id == "test-provider"
                && event.model == "fake-model"
                && event.debug.status == Some(400)
                && event.debug.request_id.as_ref().is_some_and(|value| value == "req-9")
    )));
}

// ---------------------------------------------------------------------------
// Server-side context editing: the prior-turn thinking declaration.
// ---------------------------------------------------------------------------

/// The thinking policy the request builder resolves for `params`.
fn resolved_thinking_context(
    params: &QueryParams,
) -> Option<crate::types::provider::ThinkingContextPolicy> {
    prepare_model_request(&params.messages, params, &[], 128, &[], None)
        .request
        .thinking_context
}

#[tokio::test]
async fn thinking_context_pins_every_turn_while_the_cache_is_warm() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.thinking = Some(ThinkingConfig::Adaptive);
    assert_eq!(
        resolved_thinking_context(&params),
        Some(crate::types::provider::ThinkingContextPolicy::KeepAll),
        "prior-turn reasoning is pinned so no server default sheds it"
    );
}

#[tokio::test]
async fn thinking_context_pins_every_turn_after_an_idle_gap() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.thinking = Some(ThinkingConfig::Adaptive);
    params.thinking_context_cold = true;
    assert_eq!(
        resolved_thinking_context(&params),
        Some(crate::types::provider::ThinkingContextPolicy::KeepAll),
    );
}

#[tokio::test]
async fn thinking_context_is_absent_when_the_turn_has_no_thinking() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.thinking = None;
    params.thinking_context_cold = true;
    assert_eq!(resolved_thinking_context(&params), None);
}

#[tokio::test]
async fn thinking_context_is_absent_when_management_is_switched_off() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.thinking = Some(ThinkingConfig::Adaptive);
    params.config.manage_thinking_context = false;
    assert_eq!(resolved_thinking_context(&params), None);
}

#[tokio::test]
async fn tool_use_context_editing_reaches_the_request_from_config() {
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    params.config.context_management = Some(crate::schemas::config::ContextManagementConfig {
        keep_tool_uses: Some(4),
        ..Default::default()
    });
    let request = prepare_model_request(&params.messages, &params, &[], 128, &[], None).request;
    assert_eq!(
        request
            .context_management
            .as_ref()
            .and_then(|cm| cm.keep_tool_uses),
        Some(4)
    );
}

// ---------------------------------------------------------------------------
// The remaining-context line handed to the model.
// ---------------------------------------------------------------------------

/// The `total-tokens` system part of the first captured request, if present.
fn total_tokens_line(requests: &Arc<Mutex<Vec<ModelRequest>>>) -> Option<RuntimeString> {
    requests.lock().unwrap()[0]
        .system
        .iter()
        .find(|part| part.name == "total-tokens")
        .map(|part| part.text.clone())
}

/// Run one single-response turn and hand back the captured requests.
async fn run_reminder_turn(
    configure: impl FnOnce(&mut QueryParams),
) -> Arc<Mutex<Vec<ModelRequest>>> {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            Box::pin(futures::stream::iter(vec![
                ev::message_start(None),
                ev::text("done"),
                ev::stop(None, None),
            ]))
        }
    }));
    let mut params = mk_params(vec![user_msg("hi")], Vec::new());
    configure(&mut params);
    let _: Vec<EngineEvent> = query(params, deps).collect().await;
    requests
}

#[tokio::test]
async fn the_model_is_told_how_much_context_is_left() {
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Countdown;
        params.config.context_window = 200_000;
    })
    .await;
    let line = total_tokens_line(&requests).expect("the countdown line is present");
    let text = exact_test_string(&line);
    assert!(
        text.as_utf16()
            .starts_with(RuntimeString::from("<total_tokens>").as_utf16()),
        "the line is the remaining-context envelope"
    );
    assert!(
        text.as_utf16()
            .ends_with(RuntimeString::from(" tokens left</total_tokens>").as_utf16()),
        "the line is the remaining-context envelope"
    );
}

#[tokio::test]
async fn the_remaining_context_line_is_the_last_system_part() {
    // It changes nearly every turn, so anywhere earlier it would invalidate the
    // cached prefix on every single request.
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Countdown;
    })
    .await;
    let system = &requests.lock().unwrap()[0].system;
    assert_eq!(
        system.last().map(|part| part.name.clone()),
        Some(RuntimeString::from("total-tokens")),
        "the volatile part must sit after every cache breakpoint"
    );
    // `Some(false)` is the explicit volatile marker, not merely an absence of a
    // marker: it is what tells a wire with no cache breakpoints to keep this part
    // out of the system string it caches. Both readings satisfy the invariant
    // under test — a breakpoint is only ever claimed by `Some(true)` — so assert
    // the marker that carries the intent.
    assert_eq!(
        system.last().and_then(|part| part.cacheable),
        Some(false),
        "the volatile part must not itself become a breakpoint"
    );
}

#[tokio::test]
async fn the_remaining_context_line_is_absent_when_switched_off() {
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Off;
    })
    .await;
    assert_eq!(total_tokens_line(&requests), None);
}

#[tokio::test]
async fn an_infinite_budget_is_reported_verbatim() {
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Infinite;
    })
    .await;
    assert_eq!(
        total_tokens_line(&requests).map(|line| exact_test_string(&line)),
        Some(RuntimeString::from(
            "<total_tokens>Infinite tokens left</total_tokens>"
        ))
    );
}

#[tokio::test]
async fn a_fixed_budget_ignores_actual_occupancy() {
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Fixed;
        params.config.context_window = 200_000;
    })
    .await;
    assert_eq!(
        total_tokens_line(&requests).map(|line| exact_test_string(&line)),
        Some(RuntimeString::from(
            "<total_tokens>5000000 tokens left</total_tokens>"
        ))
    );
}

#[tokio::test]
async fn a_padded_countdown_measures_against_the_long_horizon_budget() {
    let requests = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::PaddedCountdown;
        params.config.total_tokens_reminder_budget = 1_000_000;
        params.padded_countdown_spend =
            crate::context::total_tokens_reminder::PaddedCountdownSpend {
                base_tokens: 400_000,
                high_water_tokens: 400_000,
            };
    })
    .await;
    let line = total_tokens_line(&requests).expect("the padded countdown line is present");
    let text = exact_test_string(&line);
    // 1_000_000 − (400_000 retired + the tiny live occupancy) stays just under 600k.
    assert!(
        text.as_utf16()
            .starts_with(RuntimeString::from("<total_tokens>59").as_utf16()),
        "the retired tokens stay spent: {text:?}"
    );
}

/// A PreToolUse hook that returns `approved` must NOT answer a permission Ask.
///
/// Defect (2026-09-18): plan mode began ASKing for shell commands it cannot prove
/// read-only (research needs them; an interpreter's read-only-ness is undecidable
/// statically). The Ask carried `PermissionReason::Mode`, which was absent from
/// `ask_wins_over_approval` in `tool_dispatch`, so a hook with `approved: true`
/// auto-allowed it and the command ran with nobody asked — silently defeating
/// "read-only until the plan is approved". Hooks are third-party config
/// (`~/.klaus/hooks.json`), so this was reachable without any code change.
///
/// The gate now treats `Mode` like `Rule` and `SafetyCheck`: a gate that asked for
/// a human is answered by a human. Plan mode later moved its unprovable-command
/// Ask to an auto-allow under a read-only sandbox, so this test now drives the
/// same gate through a content-specific ASK rule, which plan mode deliberately
/// keeps as a human decision point.
#[tokio::test]
async fn a_pre_tool_hook_cannot_approve_a_plan_mode_bash_ask() {
    let approvals = Arc::new(Mutex::new(Vec::<ApprovalRequest>::new()));
    let hook_inputs = Arc::new(Mutex::new(Vec::<HookInput>::new()));

    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "plan-bash-ambiguous",
        "bash",
        // A content-specific ASK rule keeps plan mode's human decision point,
        // which is what the hook must not answer.
        &runtime_json!({ "command": "grep -rn foo src/" }),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));

    let mut params = mk_params(
        vec![user_msg("research the codebase")],
        vec![crate::tools::builtins::bash::create_bash_tool()],
    );
    params.permission_engine = Arc::new(StaticPermissionEngine::new(&params.config));
    // The session is planning, so the plan-mode gate is live, and the user's own
    // ask rule names this command.
    params.tool_context_base = Arc::new(|| {
        let mut ctx = make_test_ctx("/repo");
        ctx.planning_read_only = true;
        ctx.permission.ask_rules = vec![crate::types::permission::PermissionRule {
            tool_name: crate::runtime_values::RuntimeString::from("bash"),
            argument_prefix: Some(crate::runtime_values::RuntimeString::from("grep")),
            tool_name_prefix: None,
        }];
        ctx
    });
    params.hook_bus = Some(Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![recording_hook(
            HookEvent::PreToolUse,
            Arc::clone(&hook_inputs),
            HookOutcome {
                // The hook tries to pre-approve the call.
                decision: Some(HookDecision::Approve),
                ..HookOutcome::default()
            },
        )],
    )));
    // A human is present and DENIES. If the hook could bypass the gate, this
    // handler would never be consulted and the command would run.
    params.approval_handler = Some(Arc::new(StaticApproval {
        requests: Arc::clone(&approvals),
        response: ApprovalResponse::Deny {
            message: Some("no writes while planning".into()),
        },
    }));

    let events = query(params, deps_with(scripted_call_model(vec![first])))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(hook_inputs.lock().unwrap().len(), 1, "the hook ran once");
    let asked = approvals.lock().unwrap();
    assert_eq!(
        asked.len(),
        1,
        "a plan-mode Ask must reach the human even when a hook pre-approved it; \
         got {:?}",
        asked.iter().map(|r| &r.tool_name).collect::<Vec<_>>()
    );
    assert!(
        events.iter().any(|event| matches!(event, EngineEvent::ItemUpdated { inference: _, item } | EngineEvent::ItemCompleted { inference: _, item, .. }
            if matches!(item, ThreadItem::ToolCall { tool_name, error, .. }
                if tool_name.try_to_utf8().ok().as_deref() == Some("bash") && error.is_some()))),
        "the denied command must not have executed"
    );
}

/// A `write` the output ceiling cut off mid-argument is SALVAGED rather than
/// dropped: the part that arrived is written, and the result carries the append
/// protocol with the file's actual tail so the model can finish the file.
///
/// This is the case where salvaging is safe — a file's contents cut short can be
/// completed — and it is why the gate is per-tool rather than per-truncation.
#[tokio::test]
async fn a_truncated_write_is_salvaged_and_offers_the_append_protocol() {
    let root = std::env::temp_dir().join(format!(
        "klaus-query-salvage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let cwd = root
        .to_str()
        .expect("test root is a scalar host path")
        .to_owned();
    let file_state = Arc::new(crate::tools::file_state::InMemoryFileStateCache::new());
    // The call's argument JSON is cut off inside `content`: a complete `path`,
    // then a string that was still being written.
    let mut truncated_write = vec![ev::message_start(Some("gpt-5"))];
    truncated_write.extend(raw_tool_use_events(
        "write-1",
        "write",
        r#"{"path":"big.txt","content":"first line\nsecond line"#,
    ));
    truncated_write.push(ev::stop(Some(ModelStopReason::MaxTokens), None));
    let deps = deps_with(scripted_call_model(vec![
        truncated_write,
        vec![
            ev::message_start(Some("gpt-5")),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ]));
    let tools = vec![crate::tools::builtins::write::create_write_tool()];
    let mut params = mk_params(vec![user_msg("write a big file")], tools);
    params.model = "gpt-5".into();
    params.config.loop_detection = false;
    let state = Arc::clone(&file_state);
    params.tool_context_base = Arc::new(move || {
        let mut ctx = make_test_ctx(&cwd);
        ctx.file_state = state.clone();
        ctx.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
        ctx.approval_policy = Some(crate::types::permission::ApprovalPolicy::Never);
        ctx.network_allowed = Some(true);
        ctx
    });
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let events = query(params, deps).collect::<Vec<_>>().await;

    let persisted = persisted.lock().unwrap();
    let result = persisted_tool_result(&persisted, "write-1");
    assert!(
        !result.is_error,
        "a salvaged write succeeded; it did not report a dropped call. Result text: {}",
        String::from_utf8_lossy(&first_text(result.content.clone()).to_node_utf8())
    );
    // The salvaged part reached the disk, and the tool ran rather than failing.
    // Written verbatim: the recovered content has no trailing newline, because the
    // model's string was cut before it wrote one, and inventing one would shift
    // every later append.
    assert_eq!(
        std::fs::read_to_string(root.join("big.txt")).unwrap(),
        "first line\nsecond line",
        "the recovered content is written, not discarded"
    );
    // The append protocol is carried ON the tool result, with the file's real
    // tail, so a resumed session keeps tool_use and tool_result adjacent.
    let instruction = last_text(result.content.clone());
    let instruction = String::from_utf8_lossy(&instruction.to_node_utf8()).into_owned();
    assert!(
        instruction.contains("append: true"),
        "the result tells the model how to finish the file: {instruction}"
    );
    assert!(
        instruction.contains("first line\nsecond line"),
        "the continuation_context is the file's actual tail: {instruction}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "a salvaged write is not a turn failure"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A truncated call to a tool that is NOT `write` stays dropped: the arguments
/// are unparseable, and a value cut short mid-string is not something to act on.
/// The retry instruction rides on that dropped result, and the next request asks
/// for a larger budget.
#[tokio::test]
async fn a_truncated_non_write_call_retries_with_a_larger_budget() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(Vec::new())),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let is_first = requests.lock().unwrap().is_empty();
            requests.lock().unwrap().push(request);
            if is_first {
                let mut events = vec![ev::message_start(Some("task"))];
                events.extend(raw_tool_use_events(
                    "call-1",
                    "task",
                    r#"{"prompt":"a long task descripti"#,
                ));
                events.push(ev::stop(Some(ModelStopReason::MaxTokens), None));
                Box::pin(futures::stream::iter(events))
            } else {
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(Some("task")),
                    ev::text("done"),
                    ev::stop(None, None),
                ]))
            }
        }
    }));
    let mut params = mk_params(vec![user_msg("start the task")], vec![tool]);
    params.model = "task".into();
    params.config.loop_detection = false;
    // A small declared budget makes the ladder's doubling observable.
    params.config.max_output_tokens = Some(1_000);
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let persisted = persisted.lock().unwrap();
    let result = persisted_tool_result(&persisted, "call-1");
    assert!(result.is_error, "the call stayed dropped");
    let instruction = last_text(result.content.clone());
    let instruction = String::from_utf8_lossy(&instruction.to_node_utf8()).into_owned();
    assert!(
        instruction.contains("increase the token limit"),
        "the retry instruction rides on the dropped result: {instruction}"
    );
    assert!(
        instruction.contains("task"),
        "the instruction names the call to reissue: {instruction}"
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2, "the loop re-requested");
    assert_eq!(
        requests[0].max_output_tokens, 1_000,
        "the first request used the declared budget"
    );
    assert_eq!(
        requests[1].max_output_tokens, 2_000,
        "the retry climbed to the doubled rung"
    );
}

/// The tool-over-limit ladder has two rungs, and they are deliberately different:
/// raising the budget only helps while there is room left to raise it, so the
/// second attempt asks for HALF the output instead of repeating a request that
/// already failed at the larger budget.
#[tokio::test]
async fn a_second_truncated_call_asks_for_half_instead_of_more_room() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let tool = Arc::new(IdentityTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(Vec::new())),
        validation_error: None,
        outcome: IdentityToolOutcome::Ok,
    }) as Arc<dyn Tool>;
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let index = requests.lock().unwrap().len();
            requests.lock().unwrap().push(request);
            if index < 2 {
                // Two consecutive calls cut off mid-argument.
                let mut events = vec![ev::message_start(Some("task"))];
                events.extend(raw_tool_use_events(
                    &format!("call-{index}"),
                    "task",
                    r#"{"prompt":"a long task descripti"#,
                ));
                events.push(ev::stop(Some(ModelStopReason::MaxTokens), None));
                Box::pin(futures::stream::iter(events))
            } else {
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(Some("task")),
                    ev::text("done"),
                    ev::stop(None, None),
                ]))
            }
        }
    }));
    let mut params = mk_params(vec![user_msg("start the task")], vec![tool]);
    params.model = "task".into();
    params.config.loop_detection = false;
    params.config.max_output_tokens = Some(1_000);
    let persisted = Arc::new(Mutex::new(Vec::<Message>::new()));
    let sink = Arc::clone(&persisted);
    params.on_message = Some(immediate_message_observer(move |message| {
        sink.lock().unwrap().push(message);
    }));

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let persisted = persisted.lock().unwrap();
    let first = persisted_tool_result(&persisted, "call-0");
    let first_text =
        String::from_utf8_lossy(&last_text(first.content.clone()).to_node_utf8()).into_owned();
    assert!(
        first_text.contains("increase the token limit"),
        "the first rung asks for more room: {first_text}"
    );
    let second = persisted_tool_result(&persisted, "call-1");
    let second_text =
        String::from_utf8_lossy(&last_text(second.content.clone()).to_node_utf8()).into_owned();
    assert!(
        second_text.contains("reduce the output content by half"),
        "the second rung asks for less output, not more room: {second_text}"
    );
    assert!(
        !second_text.contains("increase the token limit"),
        "repeating the failed request would land in the same place"
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "two retries then a completion");
    assert_eq!(requests[0].max_output_tokens, 1_000);
    assert_eq!(
        requests[1].max_output_tokens, 2_000,
        "the first rung climbed the budget"
    );
    assert_eq!(
        requests[2].max_output_tokens, 2_000,
        "the second rung does NOT climb again: asking for half contradicts a bigger budget"
    );
}

/// An explicit `maxOutputRetryTokens` replaces the doubling default, which is how
/// a host whose model refuses an oversized request keeps the ladder inside it.
#[tokio::test]
async fn a_configured_retry_rung_replaces_the_doubling_default() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let is_first = requests.lock().unwrap().is_empty();
            requests.lock().unwrap().push(request);
            let mut events = vec![ev::message_start(Some("gpt-5"))];
            if is_first {
                events.push(ev::text("partial"));
                events.push(ev::stop(Some(ModelStopReason::MaxTokens), None));
            } else {
                events.push(ev::text("finished"));
                events.push(ev::stop(None, None));
            }
            Box::pin(futures::stream::iter(events))
        }
    }));
    let mut params = mk_params(vec![user_msg("answer")], Vec::new());
    params.model = "gpt-5".into();
    params.config.max_output_tokens = Some(4_000);
    params.config.max_output_retry_tokens = Some(5_000);

    let _events = query(params, deps).collect::<Vec<_>>().await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].max_output_tokens, 4_000);
    assert_eq!(
        requests[1].max_output_tokens, 5_000,
        "the configured rung is used instead of doubling to 8_000"
    );
}

/// With no elicitation handler there is nobody to ask, so an exhausted
/// reasoning-over-limit ladder settles as a FAILURE rather than looping forever
/// or reporting success for a turn that produced no answer.
#[tokio::test]
async fn an_exhausted_reasoning_over_limit_ladder_fails_without_a_prompt() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let requests = Arc::clone(&requests);
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            requests.lock().unwrap().push(request);
            // Reasoning only, every time: the budget is spent with nothing
            // visible ever arriving.
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("gpt-5")),
                ev::thinking("thinking, never surfaced"),
                ev::stop(Some(ModelStopReason::MaxTokens), None),
            ]))
        }
    }));
    let mut params = mk_params(vec![user_msg("answer")], Vec::new());
    params.model = "gpt-5".into();
    params.config.loop_detection = false;

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert_eq!(
        requests.lock().unwrap().len(),
        1 + super::MAX_EMPTY_CONTENT_RETRIES as usize,
        "the reminder ladder is walked, then it stops"
    );
    let failure = events.iter().find_map(|event| match event {
        EngineEvent::TurnFailed { error, .. } => {
            Some(error.message.try_to_utf8().unwrap_or_default())
        }
        _ => None,
    });
    assert!(
        failure.is_some(),
        "no handler to ask means the turn fails rather than looping"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "a turn with no visible answer is never a completion"
    );
}

/// The reasoning-over-limit ladder parks on the user once it is spent, and the
/// answer decides the turn: "keep retrying" resets the ladder and asks again,
/// "stop" fails the turn. This is the mechanism that keeps a turn which produced
/// no visible answer from either looping forever or silently reporting success.
#[tokio::test]
async fn reasoning_over_limit_blocks_on_the_user_and_honours_the_answer() {
    for (answer, expect_retry, expect_completed) in [
        ("Keep retrying", true, true),
        ("Stop", false, false),
        ("something unrelated", false, false),
    ] {
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let elicits = Arc::new(Mutex::new(Vec::<ElicitRequest>::new()));
        let answer_text = RuntimeString::from(answer);
        let tool_context_base = {
            let elicits = Arc::clone(&elicits);
            Arc::new(move || {
                let mut ctx = make_test_ctx("/tmp");
                ctx.elicit = Some(Arc::new(ScriptedReasoningElicit {
                    elicits: Arc::clone(&elicits),
                    answer: answer_text.clone(),
                }));
                ctx
            })
        };
        let deps = deps_with(Box::new({
            let requests = Arc::clone(&requests);
            let elicits = Arc::clone(&elicits);
            move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                let index = requests.lock().unwrap().len();
                requests.lock().unwrap().push(request);
                // Reasoning only, forever — until the user's answer ends it. With
                // "keep retrying" the ladder resets, so the script has to run past
                // one full ladder; answer with real text once the user has been
                // asked AND a fresh ladder has been walked, so the retry path has
                // somewhere to land.
                let asked = elicits.lock().unwrap().len();
                let mut events = vec![ev::message_start(Some("gpt-5"))];
                if asked > 0 && index > asked + super::MAX_EMPTY_CONTENT_RETRIES as usize {
                    events.push(ev::text("finally visible"));
                    events.push(ev::stop(None, None));
                } else {
                    events.push(ev::thinking("reasoning that never surfaces"));
                    events.push(ev::stop(Some(ModelStopReason::MaxTokens), None));
                }
                Box::pin(futures::stream::iter(events))
            }
        }));
        let mut params = mk_params(vec![user_msg("answer")], Vec::new());
        params.model = "gpt-5".into();
        params.config.loop_detection = false;
        params.tool_context_base = tool_context_base;

        let events = query(params, deps).collect::<Vec<_>>().await;

        assert_eq!(
            elicits.lock().unwrap().len(),
            1,
            "{answer}: the user is asked exactly once, after the ladder is spent"
        );
        let question = &elicits.lock().unwrap()[0];
        assert_eq!(question.attachments_supported, None);
        let asked = String::from_utf8_lossy(&question.prompt.to_node_utf8()).into_owned();
        assert!(
            asked.contains("output budget") && asked.contains("reasoning"),
            "{answer}: the question says what actually happened: {asked}"
        );
        let request_count = requests.lock().unwrap().len();
        // A retry walks one more full ladder before the script answers with text;
        // a stop ends on the ladder that was already spent.
        assert_eq!(
            request_count > 1 + super::MAX_EMPTY_CONTENT_RETRIES as usize,
            expect_retry,
            "{answer}: requests={request_count}"
        );
        let completed = events.iter().any(|event| {
            matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )
        });
        assert_eq!(completed, expect_completed, "{answer}");
        if !expect_retry {
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
                "{answer}: an unrecognized or stop answer fails the turn rather than looping"
            );
        }
    }
}

/// A declined prompt reads as "stop", not as "keep going": failing a turn is
/// recoverable, looping with no visible output is not.
#[tokio::test]
async fn a_declined_reasoning_over_limit_prompt_fails_the_turn() {
    let elicits = Arc::new(Mutex::new(Vec::<ElicitRequest>::new()));
    let tool_context_base = {
        let elicits = Arc::clone(&elicits);
        Arc::new(move || {
            let mut ctx = make_test_ctx("/tmp");
            ctx.elicit = Some(Arc::new(DecliningReasoningElicit {
                elicits: Arc::clone(&elicits),
            }));
            ctx
        })
    };
    let deps = deps_with(Box::new(
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("gpt-5")),
                ev::thinking("reasoning only"),
                ev::stop(Some(ModelStopReason::MaxTokens), None),
            ]))
        },
    ));
    let mut params = mk_params(vec![user_msg("answer")], Vec::new());
    params.model = "gpt-5".into();
    params.config.loop_detection = false;
    params.tool_context_base = tool_context_base;

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert_eq!(elicits.lock().unwrap().len(), 1, "the user was asked");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "a declined prompt fails the turn"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "a turn with no visible answer never reports success"
    );
}

/// A non-interactive session has nobody to answer, so the ladder settles as a
/// failure instead of hanging the turn on a prompt no one will see.
#[tokio::test]
async fn reasoning_over_limit_settles_when_prompts_are_avoided() {
    let tool_context_base = Arc::new(|| {
        let mut ctx = make_test_ctx("/tmp");
        ctx.permission.should_avoid_prompts = true;
        ctx
    });
    let deps = deps_with(Box::new(
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            Box::pin(futures::stream::iter(vec![
                ev::message_start(Some("gpt-5")),
                ev::thinking("reasoning only"),
                ev::stop(Some(ModelStopReason::MaxTokens), None),
            ]))
        },
    ));
    let mut params = mk_params(vec![user_msg("answer")], Vec::new());
    params.model = "gpt-5".into();
    params.config.loop_detection = false;
    params.tool_context_base = tool_context_base;

    let events = query(params, deps).collect::<Vec<_>>().await;

    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "with no user available the turn fails instead of waiting"
    );
}

/// Returns one structured answer for the reasoning-over-limit question.
struct ScriptedReasoningElicit {
    elicits: Arc<Mutex<Vec<ElicitRequest>>>,
    answer: RuntimeString,
}

#[async_trait::async_trait]
impl ElicitHandler for ScriptedReasoningElicit {
    async fn elicit(&self, request: ElicitRequest, _signal: AbortSignal) -> ElicitResponse {
        let key = request.prompt.clone();
        self.elicits.lock().unwrap().push(request);
        let mut answers = crate::runtime_values::RuntimeMap::new();
        answers.insert(key, self.answer.clone());
        ElicitResponse::Answers {
            answers,
            attachments: None,
        }
    }
}

/// Declines every prompt.
struct DecliningReasoningElicit {
    elicits: Arc<Mutex<Vec<ElicitRequest>>>,
}

#[async_trait::async_trait]
impl ElicitHandler for DecliningReasoningElicit {
    async fn elicit(&self, request: ElicitRequest, _signal: AbortSignal) -> ElicitResponse {
        self.elicits.lock().unwrap().push(request);
        ElicitResponse::Decline { reason: None }
    }
}

// ---------------------------------------------------------------------------
// Overflow recovery layers (2026-10-03): media-size rejections ride the same
// reactive-compaction rescue as a 413, and the per-message tool-result budget
// enforces its ceiling on the request view (old history included).
// ---------------------------------------------------------------------------

struct OverflowRescueCompactor {
    rescued: Arc<Mutex<bool>>,
    recover: bool,
}

#[async_trait]
impl crate::engine::deps::Compactor for OverflowRescueCompactor {
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &crate::schemas::config::ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        crate::engine::deps::CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        _messages: &[Message],
        _config: &crate::schemas::config::ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        *self.rescued.lock().unwrap() = true;
        if self.recover {
            crate::engine::deps::CompactionResult {
                messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                    native_compaction: None,
                    uuid: "boundary-1".into(),
                    summary: "summary".into(),
                })],
                freed_tokens: 1_000,
                compacted: true,
                reason: None,
                diagnostics: Vec::new(),
            }
        } else {
            crate::engine::deps::CompactionResult {
                messages: Vec::new(),
                freed_tokens: 0,
                compacted: false,
                reason: Some(crate::context::compact::CompactionNoopReason::NoProgress),
                diagnostics: Vec::new(),
            }
        }
    }
}

fn provider_error(kind: ProviderErrorKind, message: &str) -> ProviderError {
    ProviderError {
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        kind,
        message: message.into(),
        status: Some(400),
        retry_after_ms: None,
        request_id: None,
    }
}

fn overflow_history() -> Vec<Message> {
    let mut earlier = user_msg("Preserve earlier task constraints.");
    if let Message::User(user) = &mut earlier {
        user.uuid = "overflow-earlier-user".into();
    }
    vec![
        earlier,
        Message::Assistant(crate::schemas::message::AssistantMessage {
            uuid: "overflow-earlier-assistant".into(),
            model: "test-model".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Earlier complete response.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            usage: None,
        }),
        user_msg("work"),
    ]
}

async fn run_overflow_turn(
    first_error: ProviderError,
    recover: bool,
    configure: impl FnOnce(&mut QueryParams),
) -> (Arc<Mutex<bool>>, Vec<EngineEvent>) {
    let rescued = Arc::new(Mutex::new(false));
    let mut deps = deps_with(scripted_call_model(vec![
        vec![ev::message_start(None), ev::error(first_error)],
        vec![
            ev::message_start(None),
            ev::text("done"),
            ev::stop(None, None),
        ],
    ]));
    deps.compactor = Arc::new(OverflowRescueCompactor {
        rescued: Arc::clone(&rescued),
        recover,
    });
    let mut params = mk_params(overflow_history(), Vec::new());
    configure(&mut params);
    let events: Vec<EngineEvent> = query(params, deps).collect().await;
    (rescued, events)
}

#[tokio::test]
async fn missing_media_origins_do_not_trigger_summary_or_recreate_a_request() {
    let (rescued, events) = run_overflow_turn(
        provider_error(
            ProviderErrorKind::InvalidRequest,
            "image exceeds the maximum allowed size of 5 MB",
        ),
        true,
        |_| {},
    )
    .await;
    assert!(!*rescued.lock().unwrap());
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::StreamRequestStart { .. }))
            .count(),
        1,
    );
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnFailed { error, .. }
            if error.provider_kind == Some(ProviderErrorKind::InvalidRequest)
                && error.message == "image exceeds the maximum allowed size of 5 MB"
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn media_size_rejection_rides_the_reactive_rescue_and_recovers() {
    let (rescued, events) = run_overflow_turn(
        provider_error(
            ProviderErrorKind::RequestTooLarge,
            "image exceeds the maximum allowed size of 5 MB",
        ),
        true,
        |_| {},
    )
    .await;
    assert!(
        *rescued.lock().unwrap(),
        "the media rejection must trigger the rescue"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "a successful rescue lets the turn finish normally"
    );
}

#[tokio::test]
async fn media_size_rejection_surfaces_the_provider_wording_when_the_rescue_fails() {
    let (rescued, events) = run_overflow_turn(
        provider_error(
            ProviderErrorKind::RequestTooLarge,
            "image exceeds the maximum allowed size of 5 MB",
        ),
        false,
        |_| {},
    )
    .await;
    assert!(*rescued.lock().unwrap());
    let failure = events.iter().find_map(|event| match event {
        EngineEvent::TurnFailed { error, .. } => error.message.try_to_utf8().ok(),
        _ => None,
    });
    assert_eq!(
        failure.as_deref(),
        Some("image exceeds the maximum allowed size of 5 MB"),
        "a failed media rescue surfaces the provider's own rejection, not a context-window claim"
    );
}

#[tokio::test]
async fn a_second_media_rejection_after_a_rescue_fails_without_spiraling() {
    let rescued = Arc::new(Mutex::new(false));
    let mut deps = deps_with(scripted_call_model(vec![
        vec![
            ev::message_start(None),
            ev::error(provider_error(
                ProviderErrorKind::RequestTooLarge,
                "request payload: image exceeds the maximum size",
            )),
        ],
        vec![
            ev::message_start(None),
            ev::error(provider_error(
                ProviderErrorKind::RequestTooLarge,
                "request payload: image exceeds the maximum size",
            )),
        ],
    ]));
    deps.compactor = Arc::new(OverflowRescueCompactor {
        rescued: Arc::clone(&rescued),
        recover: true,
    });
    let params = mk_params(overflow_history(), Vec::new());
    let events: Vec<EngineEvent> = query(params, deps).collect().await;
    assert!(
        *rescued.lock().unwrap(),
        "exactly one rescue attempt is allowed"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "the second rejection fails the turn instead of compacting again"
    );
}

#[tokio::test]
async fn prompt_too_long_keeps_its_terminal_reason_after_a_failed_rescue() {
    let (_rescued, events) = run_overflow_turn(
        provider_error(
            ProviderErrorKind::PromptTooLong,
            "prompt is too long: 300000 tokens > 200000 maximum",
        ),
        false,
        |_| {},
    )
    .await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnFailed { error, .. }
                if error.code == EngineErrorCode::PromptTooLong
                    && error.provider_kind == Some(ProviderErrorKind::PromptTooLong)
                    && error.message == "prompt is too long: 300000 tokens > 200000 maximum"
        )),
        "the typed overflow failure keeps its provider kind and original wording"
    );
}

#[tokio::test]
async fn request_view_budget_persists_an_over_budget_historical_result() {
    let recorded: Arc<Mutex<Vec<(RuntimeString, RuntimeString)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let body = format!("{}TAILMARKER", "A".repeat(290_000));
    let history = vec![
        user_msg("work"),
        Message::Assistant(AssistantMessage {
            uuid: "a1".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "tu-old".into(),
                name: "bash".into(),
                input: crate::runtime_values::RuntimeJson::Null,
                tool_kind: None,
                thought_signature: None,
            })],
            model: "fake-model".into(),
            usage: None,
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "tr-old".into(),
            tool_use_id: "tu-old".into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: body.clone().into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ];
    let mut deps = deps_with(scripted_call_model(vec![vec![
        ev::message_start(None),
        ev::text("done"),
        ev::stop(None, None),
    ]]));
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    deps.call_model = {
        let requests = Arc::clone(&requests);
        Box::new(
            move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(request);
                Box::pin(futures::stream::iter(vec![
                    ev::message_start(None),
                    ev::text("done"),
                    ev::stop(None, None),
                ]))
            },
        )
    };
    let state = Arc::new(Mutex::new(
        crate::context::tool_result_budget::ReplacementState::default(),
    ));
    let mut params = mk_params(history, Vec::new());
    params.config.aggregate_tool_results = true;
    params.config.max_tool_results_per_message_chars = 100_000;
    params.tool_result_budget_state = Some(Arc::clone(&state));
    params.on_tool_result_budget_records = {
        let recorded = Arc::clone(&recorded);
        Some(Arc::new(move |records| {
            recorded.lock().unwrap().extend(records);
            Box::pin(async { Ok(()) })
        }))
    };
    let _: Vec<EngineEvent> = query(params, deps).collect().await;

    let records = recorded.lock().unwrap().clone();
    assert_eq!(
        records.len(),
        1,
        "the oversized historical result is persisted once"
    );
    assert_eq!(records[0].0, RuntimeString::from("tu-old"));
    let sent = requests.lock().unwrap();
    let sent_text = sent
        .iter()
        .flat_map(|request| request.messages.clone())
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "tu-old" => Some(
                crate::tools::result_storage::output_blocks_text(&result.content),
            ),
            _ => None,
        })
        .expect("the tool result rides the request");
    let sent_text = sent_text.try_to_utf8().unwrap();
    assert!(
        !sent_text.contains("TAILMARKER"),
        "the model sees the preview, not the full body"
    );
    assert!(
        sent_text.len() < 10_000,
        "the replacement is a preview, not the body"
    );
    assert!(
        records[0].1.starts_with(&body[..100]),
        "the record keeps the exact body the model saw"
    );
}

struct ReviewBudgetCompactor {
    mode: u8,
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl crate::engine::deps::Compactor for ReviewBudgetCompactor {
    fn reactive_enabled(&self) -> bool {
        self.mode != 3
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated: u64,
        _config: &crate::schemas::config::ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        crate::engine::deps::CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &crate::schemas::config::ResolvedConfig,
        _instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.mode == 2 {
            let mut error = provider_error(ProviderErrorKind::Network, "summary connection reset");
            error.status = Some(503);
            error.code = Some("summary_reset".into());
            error.request_id = Some("summary-request-1".into());
            return crate::engine::deps::CompactionResult {
                messages: messages.to_vec(),
                freed_tokens: 0,
                compacted: false,
                reason: Some(crate::context::compact::CompactionNoopReason::NoProgress),
                diagnostics: vec![
                    crate::context::compact::CompactionDiagnostic::from_provider(
                        crate::context::compact::CompactionDiagnosticStage::Summary,
                        crate::context::compact::CompactionDiagnosticKind::ProviderError,
                        error,
                    ),
                ],
            };
        }
        crate::engine::deps::CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: format!("review-budget-summary-{attempt}").into(),
                summary: if matches!(self.mode, 1 | 6) {
                    "x".repeat(20_000).into()
                } else {
                    "Inspection result summarized.".into()
                },
            })],
            freed_tokens: 5_000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[tokio::test]
async fn approval_local_budget_compacts_once_then_rechecks_action_and_complete_request() {
    for mode in 0..=6 {
        let action =
            user_msg("CURRENT_ACTION: inspect the requested workspace command, with no writes.");
        let mut messages = overflow_history();
        messages.pop();
        messages.push(action.clone());
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let mut deps = deps_with(Box::new({
            let requests = requests.clone();
            move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                let index = {
                    let mut requests = requests.lock().unwrap();
                    let index = requests.len();
                    requests.push(request);
                    index
                };
                let maximum_calls = match mode {
                    4 => 3,
                    5 => 5,
                    _ => 2,
                };
                assert!(
                    index < maximum_calls,
                    "budget recovery cannot create another assessment attempt"
                );
                let mut events = vec![ev::message_start(None)];
                if (mode == 5 && matches!(index, 1 | 3)) || (mode == 6 && index == 0) {
                    events.push(ev::error(provider_error(
                        ProviderErrorKind::PromptTooLong,
                        "input exceeds the context window",
                    )));
                } else if index == 0 || (mode == 4 && index == 1) || (mode == 5 && index == 2) {
                    events.extend(exact_tool_use_events(
                        "budget-inspection",
                        "read",
                        &runtime_json!({}),
                    ));
                    events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
                } else {
                    events.push(ev::text("{\"outcome\":\"allow\"}"));
                    events.push(ev::stop(None, None));
                }
                Box::pin(futures::stream::iter(events))
            }
        }));
        let attempts = Arc::new(AtomicUsize::new(0));
        deps.compactor = Arc::new(ReviewBudgetCompactor {
            mode,
            attempts: attempts.clone(),
        });
        let tool: Arc<dyn Tool> = Arc::new(ResultShapeTool {
            name: "read",
            schema: JsonSchema::new(),
            text: "e".repeat(20_000),
            max_chars: Some(100_000),
            output: Json::Null,
            extra_user_blocks: None,
        });
        let mut params = mk_params(messages.clone(), vec![tool]);
        params.agent_type = Some("approval-review".into());
        params.system_prompt = SystemPromptResolvable::Parts(vec![SystemPromptPart {
            name: "approval-review-policy".into(),
            text: "Keep the exact workspace authority.".into(),
            cacheable: Some(true),
            cache_scope: None,
        }]);
        params.output_schema = Some(runtime_json!({
            "type":"object", "properties":{"outcome":{"type":"string","enum":["allow","deny"]}},
            "required":["outcome"], "additionalProperties":false
        }));
        let maximum_input = if mode == 5 { 128_000 } else { 4_000 };
        let admission = crate::engine::session::ApprovalQueryAdmission::new(
            maximum_input,
            &HashSet::new(),
            Arc::new(Mutex::new(messages)),
            Arc::new(crate::testing::fake_provider()),
            None,
        );
        params.provider_call_admission = Some(admission.clone());
        let events: Vec<_> = super::query_isolated(params, deps).collect().await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            if matches!(mode, 4 | 5) {
                2
            } else {
                usize::from(mode != 3)
            },
            "mode {mode}"
        );
        let requests = requests.lock().unwrap();
        if matches!(mode, 0 | 4 | 5) {
            assert_eq!(
                requests.len(),
                match mode {
                    4 => 3,
                    5 => 5,
                    _ => 2,
                }
            );
            let actual = requests.last().unwrap();
            assert!(actual.messages.iter().any(|message| message == &action));
            assert!(actual
                .messages
                .iter()
                .any(|message| matches!(message, Message::CompactBoundary(_))));
            assert!(actual
                .system
                .iter()
                .any(|part| part.text == "Keep the exact workspace authority."));
            assert!(actual.tools.iter().any(|tool| tool.name == "read"));
            assert!(actual.output_schema.is_some());
            assert!(
                crate::engine::session::estimate_approval_request_tokens(actual).unwrap()
                    <= maximum_input
            );
            assert!(admission.rejected().is_none());
            assert!(events.iter().any(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )));
        } else {
            assert_eq!(requests.len(), 1);
            let error = events
                .iter()
                .find_map(|event| match event {
                    EngineEvent::TurnFailed { error, .. } => Some(error),
                    _ => None,
                })
                .expect("an unrecoverable request must fail closed");
            if mode == 2 {
                assert!(
                    admission.rejected().is_none(),
                    "a summary transport failure cannot become a local budget fallback"
                );
                let provider = error
                    .provider_error()
                    .expect("summary failure identity must remain typed");
                assert_eq!(provider.kind, ProviderErrorKind::Network);
                assert_eq!(provider.status, Some(503));
                assert_eq!(provider.code.as_ref().unwrap(), "summary_reset");
                assert_eq!(provider.request_id.as_ref().unwrap(), "summary-request-1");
            } else {
                assert!(
                    admission.rejected().is_some(),
                    "local exhaustion remains eligible for its typed budget disposition"
                );
            }
        }
    }
}

#[test]
fn approval_compaction_preserves_recent_original_text_with_a_shared_twenty_thousand_token_limit() {
    let mut older = user_msg(&"o".repeat(80_000));
    if let Message::User(user) = &mut older {
        user.uuid = "older-user".into();
    }
    let action = user_msg("CURRENT_ACTION: do exactly the requested inspection.");
    let original = vec![older, action.clone()];
    let mut compacted = crate::context::compact::full_compact::retained_user_messages(&original);
    compacted.push(compact_boundary());
    assert_eq!(compacted[1], action);
    assert!(matches!(
        compacted.last().unwrap(),
        Message::CompactBoundary(_)
    ));
    let Message::User(older) = &compacted[0] else {
        panic!("expected bounded older text")
    };
    assert_eq!(older.uuid, "older-user");
    assert!(text_block_text(&older.content[0]).contains("tokens truncated"));
    assert!(text_block_text(&older.content[0]).len_utf16() < 80_000);
}

#[tokio::test]
async fn approval_repeated_inspection_has_no_generic_loop_nudge_or_turn_limit() {
    let original = user_msg("Assess this pending workspace action.");
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let deps = deps_with(Box::new({
        let requests = requests.clone();
        move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            let index = {
                let mut requests = requests.lock().unwrap();
                let index = requests.len();
                requests.push(request);
                index
            };
            assert!(index <= 7);
            let mut events = vec![ev::message_start(None)];
            if index < 7 {
                events.extend(exact_tool_use_events(
                    &format!("inspection-{index}"),
                    "read",
                    &runtime_json!({}),
                ));
                events.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            } else {
                events.push(ev::text("{\"outcome\":\"allow\"}"));
                events.push(ev::stop(None, None));
            }
            Box::pin(futures::stream::iter(events))
        }
    }));
    let tool: Arc<dyn Tool> = Arc::new(SourceTool {
        name: "read",
        schema: JsonSchema::new(),
        mcp_server: None,
        exposure: ToolExposure::Direct,
    });
    let mut params = mk_params(vec![original.clone()], vec![tool]);
    params.agent_type = Some("approval-review".into());
    params.config.loop_detection = true;
    let events: Vec<_> = super::query_isolated(params, deps).collect().await;
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 8);
    for request in requests.iter() {
        assert_eq!(
            request
                .messages
                .iter()
                .filter(|message| matches!(message, Message::User(_)))
                .collect::<Vec<_>>(),
            vec![&original]
        );
    }
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Completed,
            ..
        }
    )));
}

#[tokio::test]
async fn task_reminder_is_projected_after_user_only_when_enabled() {
    let enabled = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::PaddedCountdown;
        params.config.total_tokens_reminder_after_user_turn = Some(true);
        params.config.total_tokens_reminder_budget = 10_000;
    })
    .await;
    {
        let enabled = enabled.lock().unwrap();
        let Message::User(reminder) = enabled[0].messages.last().unwrap() else {
            panic!("missing reminder carrier")
        };
        assert_eq!(reminder.is_meta, Some(true));
        assert!(reminder
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text)
            if text.text.contains("<total_tokens>10000 tokens left</total_tokens>"))));
    }
    let ordinary = run_reminder_turn(|params| {
        params.config.total_tokens_reminder = TotalTokensReminderMode::Fixed;
    })
    .await;
    assert!(!ordinary.lock().unwrap()[0].messages.iter().any(|message| matches!(message,
        Message::User(user) if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
            if text.phase.as_ref().is_some_and(|phase| phase == "total_tokens_reminder"))))));
}

struct UnexpectedDisabledCompaction;
#[async_trait::async_trait]
impl crate::engine::deps::Compactor for UnexpectedDisabledCompaction {
    async fn maybe_compact(
        &self,
        _: &[Message],
        _: u64,
        _: &crate::schemas::config::ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        panic!("global disable must skip proactive summary")
    }
    async fn reactive_compact(
        &self,
        _: &[Message],
        _: &crate::schemas::config::ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        panic!("global disable must skip overflow summary")
    }
}

#[tokio::test]
async fn runtime_global_disable_skips_every_query_compaction_path() {
    for pressure in [false, true] {
        let mut params = mk_params(
            vec![Message::User(crate::schemas::message::UserMessage {
                uuid: "disabled-user".into(),
                content: vec![ContentBlock::Text(crate::schemas::content::TextBlock {
                    text: "x".repeat(400_000).into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
                question_answer: None,
            })],
            Vec::new(),
        );
        params.config.compaction_disabled = true;
        params.config.auto_compaction = true;
        params.config.context_window = 200_000;
        params.config.auto_compact_window = Some(50_000);
        params.config.output_reserve_tokens = 0;
        params.config.max_retries = 0;
        params.config.loop_detection = false;
        params.config.idle_microcompact = true;
        params.config.early_microcompact = true;
        params.config.speculative_compaction = true;
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut deps = deps_with(Box::new(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(if pressure {
                vec![ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::PromptTooLong,
                        message: "The request is too long.".into(),
                        status: Some(400),
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                        retry_after_ms: None,
                        request_id: None,
                    },
                }]
            } else {
                vec![
                    ev::message_start(None),
                    ev::text("Completed."),
                    ev::stop(None, None),
                ]
            }))
        }));
        deps.compactor = Arc::new(UnexpectedDisabledCompaction);
        deps.now = Arc::new(|| 7_200_000);
        let events: Vec<_> = query(params, deps).collect().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            EngineEvent::Compaction { .. } | EngineEvent::CompactionProgress { .. }
        )));
        assert_eq!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            pressure
        );
    }
}
