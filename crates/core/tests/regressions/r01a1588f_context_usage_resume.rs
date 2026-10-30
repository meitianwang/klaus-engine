//! Defect: compaction usage counters were lost on resume or charged again when
//! cumulative checkpoint events were replayed, and history repair retired usage.
//! Root cause: accounting lifetime was tied to a live query instead of the
//! cumulative session checkpoint, with no distinct nonretiring repair path.
//! Work item 01a1588f: replay durable counters once across real session resumes.

use klaus_agent_core::context::tokens::{
    estimate_context_tokens_with_prefix, estimate_prefix_tokens,
};
use klaus_agent_core::context::usage_checkpoint::{
    checkpoint_from_rollout, decode_checkpoint, CONTEXT_USAGE_ROLLOUT_EVENT,
};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::schemas::rollout::{EventItem, RolloutItem};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::{AppendEventOptions, SessionStore};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

const BUDGET: u64 = 1_000_000;

fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn user(id: &str, value: impl Into<RuntimeString>, is_meta: bool) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content: vec![text(value)],
        is_meta: is_meta.then_some(true),
        question_answer: None,
    })
}

fn assistant(id: &str, usage: Option<Usage>) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content: vec![text("Earlier progress remains relevant. ".repeat(2_000))],
        model: "usage-fixture-model".into(),
        usage,
    })
}

struct Provider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let summary = request.skip_cache_write == Some(true);
        self.requests.lock().unwrap().push(request);
        let response = if summary {
            RuntimeString::from("<summary>Preserve the active task and continue from the latest evidence.</summary>")
        } else {
            RuntimeString::from("Completed this fixture step with detailed progress. ".repeat(300))
        };
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta { text: response },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: if summary { 100 } else { 9_000 },
                    output_tokens: if summary { 10 } else { 4_000 },
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn usage_events(items: &[RolloutItem]) -> Vec<EventItem> {
    items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == CONTEXT_USAGE_ROLLOUT_EVENT => {
                Some(event.clone())
            }
            _ => None,
        })
        .collect()
}

fn reported_remaining(request: &ModelRequest) -> u64 {
    request
        .system
        .iter()
        .find(|part| part.name == "total-tokens")
        .expect("enabled reminder is present")
        .text
        .try_to_utf8()
        .unwrap()
        .trim_start_matches("<total_tokens>")
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn repeated_compactions_replay_cumulative_usage_once_and_repair_retires_nothing() {
    let directory = super::context_fixture::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "usage-checkpoint-fixture".into(),
            default_model: "usage-fixture-model".into(),
            base_url: None,
            env_key: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.include_default_agents = Some(false);
    options.tools = Some(Vec::new());
    options.config = Some(json!({
        "model":"usage-fixture-model", "maxOutputTokens":8192,
        "thinking":false, "prewarm":false, "autoCompaction":false,
        "totalTokensReminder":"padded-countdown", "totalTokensReminderBudget":BUDGET,
        "totalTokensReminderAfterUserTurn":false,
        "autoMemory":false, "autoDream":false, "memoryRecall":false,
        "speculativeCompaction":false, "earlyMicrocompact":false,
        "idleMicrocompact":false, "compactionSelfCheck":false,
        "permissionMode":"bypassPermissions", "sandbox":"process",
        "sandboxNetworkAllowed":true, "requireRealSandbox":false,
        "sandboxRetryOnDenial":false, "loopDetection":false
    }));
    let agent = Agent::new(options);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            directory
                .path()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        archive_dir: None,
    }));
    let start = || StartSessionOptions {
        cwd: Some(directory.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
        system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
        ..Default::default()
    };
    let mut initial = start();
    initial.initial_history = Some(vec![
        user("original-task", "a".repeat(16_000), false),
        assistant("earlier-response", None),
        assistant("latest-response", None),
        user("active-task", "Continue the current task.", false),
    ]);
    let session = agent.open_session(store.clone(), initial).await.unwrap();
    let first = session.compact(None).await.unwrap();
    assert!(first.compacted && first.before_tokens > first.after_tokens);
    session.flush_rollout().await.unwrap();
    let id = session.id.try_to_utf8().unwrap();
    let first_saved = store.read_thread(&id).await.unwrap();
    let first_spend = checkpoint_from_rollout(&first_saved.raw_items);
    let first_delta = first.before_tokens - first.after_tokens;
    assert_eq!(first_spend.base_tokens, first_delta);

    let completed = session
        .run(
            Input::Text("b".repeat(16_000).into()),
            TurnOptions::default(),
        )
        .await;
    assert!(completed.error.is_none(), "{:?}", completed.error);
    let second = session.compact(None).await.unwrap();
    assert!(second.compacted && second.before_tokens > second.after_tokens);
    let retired = first_delta + second.before_tokens - second.after_tokens;
    assert!(retired > first_delta);
    session.flush_rollout().await.unwrap();
    let saved = store.read_thread(&id).await.unwrap();
    let cumulative = checkpoint_from_rollout(&saved.raw_items);
    assert_eq!(cumulative.base_tokens, retired);
    assert_eq!(project_for_prompt(&saved.raw_items), session.history());
    let checkpoints = usage_events(&saved.raw_items);
    assert!(checkpoints.len() >= 2);
    for event in &checkpoints {
        let payload = event.payload.as_ref().unwrap();
        assert!(
            matches!(payload.get("version"), Some(RuntimeJson::String(version)) if version == "1")
        );
        for key in ["retiredTokens", "highWaterTokens"] {
            assert!(
                matches!(payload.get(key), Some(RuntimeJson::String(value)) if value.try_to_utf8().unwrap().bytes().all(|byte| byte.is_ascii_digit()))
            );
        }
        assert!(decode_checkpoint(payload).is_some());
    }
    session.shutdown_and_wait().await.unwrap();

    let replay = store.resume_for_write(&id).await.unwrap();
    for event in [
        checkpoints.last().unwrap(),
        &checkpoints[0],
        checkpoints.last().unwrap(),
    ] {
        replay
            .rollout
            .append_event(
                event.event.clone(),
                Some(AppendEventOptions {
                    turn_id: event.turn_id.clone(),
                    payload: event.payload.clone(),
                }),
            )
            .await;
    }
    for index in 0..4 {
        replay
            .rollout
            .append_message(user(
                &format!("request-workspace-instructions-{index}"),
                "Obsolete request-only workspace projection. ".repeat(100),
                true,
            ))
            .await;
    }
    replay
        .rollout
        .append_message(assistant(
            "obsolete-inflated-usage",
            Some(Usage {
                input_tokens: 500_000,
                output_tokens: 10,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            }),
        ))
        .await;
    replay.rollout.flush().await.unwrap();
    replay.rollout.shutdown().await.unwrap();
    drop(replay);
    let duplicated = store.read_thread(&id).await.unwrap();
    assert_eq!(checkpoint_from_rollout(&duplicated.raw_items), cumulative);

    let resumed = agent
        .resume_session(store.clone(), &id, start())
        .await
        .unwrap();
    let completed = resumed
        .run(
            Input::Text("Continue after restarting.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(completed.error.is_none(), "{:?}", completed.error);
    let request = provider.requests.lock().unwrap().last().unwrap().clone();
    let system = request
        .system
        .iter()
        .filter(|part| part.name != "total-tokens")
        .cloned()
        .collect::<Vec<_>>();
    let current = estimate_context_tokens_with_prefix(
        &request.messages,
        estimate_prefix_tokens(&system, &request.tools),
    );
    let expected_remaining = BUDGET.saturating_sub(cumulative.total_with(current));
    assert_eq!(reported_remaining(&request), expected_remaining);
    let prompt = runtime_json::stringify(&runtime_codec::encode_value(&request.messages).unwrap());
    assert!(!prompt.contains(CONTEXT_USAGE_ROLLOUT_EVENT));
    let live = resumed.history();
    assert!(!live.iter().any(|message| matches!(message, Message::User(user) if user.is_meta == Some(true) && user.uuid.starts_with("request-workspace-instructions-"))));
    resumed.flush_rollout().await.unwrap();
    let repaired = store.read_thread(&id).await.unwrap();
    assert_eq!(
        checkpoint_from_rollout(&repaired.raw_items).base_tokens,
        retired
    );
    assert_eq!(project_for_prompt(&repaired.raw_items), live);
    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
