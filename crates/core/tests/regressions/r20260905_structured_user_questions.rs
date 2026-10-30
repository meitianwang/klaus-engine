//! Defect: ask_user accepted only a single string question and lost rich choices.
//! Root cause: the tool schema and elicitation carriers omitted the question batch.
//! Fixed by: 2026-09-05 structured user question alignment work item.
//! Attachment follow-up: host answers lost images because the elicitation response
//! had no attachment carrier or validation before constructing the tool result.
//! Fixed by: 2026-10-01 ask_user attachment support work item.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeMap, RuntimePath, RuntimeString,
};
use klaus_agent_core::sandbox::process::ProcessSandbox;
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::ask_user::create_ask_user_tool;
use klaus_agent_core::tools::file_state::InMemoryFileStateCache;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{ExternalPermissionMode, PermissionContext};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{ElicitHandler, ElicitRequest, ElicitResponse, ToolContext};

fn parse(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(source)).unwrap()
}

fn question() -> RuntimeJson {
    parse(
        r#"{"questions":[{"question":"Which layout?","header":"Layout","options":[{"label":"Dense","description":"More rows"},{"label":"Spacious","description":"More space","preview":"a\nb"}],"multiSelect":false}]}"#,
    )
}

#[test]
fn accepts_question_batches_and_rejects_unanswerable_choices() {
    let tool = create_ask_user_tool();
    let valid = question();
    assert_eq!(tool.parse_input(&valid).unwrap(), valid);
    let cases = [
        r#"{"questions":[]}"#,
        r#"{"questions":[{"question":"Pick","header":"1234567890123","options":[{"label":"A","description":"a"},{"label":"B","description":"b"}],"multiSelect":false}]}"#,
        r#"{"questions":[{"question":"Pick","header":"Pick","options":[{"label":"A","description":"a"}],"multiSelect":false}]}"#,
        r#"{"questions":[{"question":"Pick","header":"Pick","options":[{"label":"A","description":"a"},{"label":"A","description":"b"}],"multiSelect":false}]}"#,
        r#"{"questions":[{"question":"   ","header":"Pick","options":[{"label":"A","description":"a"},{"label":"B","description":"b"}],"multiSelect":false}]}"#,
    ];
    for source in cases {
        assert!(
            tool.parse_input(&parse(source)).is_err(),
            "accepted {source}"
        );
    }
    let mut duplicate = valid.clone();
    let list = duplicate
        .as_object_mut()
        .unwrap()
        .get_mut("questions")
        .unwrap()
        .as_array_mut()
        .unwrap();
    list.push(list[0].clone());
    assert!(tool.parse_input(&duplicate).is_err());
}

pub(super) struct RecordingElicit {
    pub(super) response: ElicitResponse,
    pub(super) requests: Mutex<Vec<ElicitRequest>>,
    pub(super) cancel_before_return: bool,
}

#[async_trait]
impl ElicitHandler for RecordingElicit {
    async fn elicit(&self, request: ElicitRequest, signal: AbortSignal) -> ElicitResponse {
        self.requests.lock().unwrap().push(request);
        if self.cancel_before_return {
            signal.cancel();
        }
        self.response.clone()
    }
}

pub(super) fn context(
    process: RuntimeProcessContext,
    handler: Arc<RecordingElicit>,
) -> ToolContext {
    ToolContext {
        async_question: None,
        background_processes: Arc::new(
            klaus_agent_core::command_runtime::background::BackgroundCommandRegistry::new(),
        ),
        process_context: process,
        cwd: RuntimePath::from("/tmp"),
        signal: AbortSignal::new(),
        file_state: Arc::new(InMemoryFileStateCache::new()),
        permission: PermissionContext {
            allow_rules: Vec::new(),
            deny_rules: Vec::new(),
            ask_rules: Vec::new(),
            additional_writable_roots: Vec::new(),
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: false,
        },
        sandbox: Arc::new(ProcessSandbox),
        planning_read_only: false,
        approval_policy: None,
        sandbox_mode: None,
        approvals_reviewer: None,
        web_search_access: None,
        network_allowed: None,
        sandbox_retry_on_denial: None,
        request_approval: None,
        request_network_access: None,
        enqueue_background_notice: None,
        record_rollout_event: None,
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
        elicit: Some(handler),
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
    }
}

fn image(mime: &str, base64: &str) -> ContentBlock {
    ContentBlock::Image(ImageBlock {
        mime_type: mime.into(),
        base64: base64.into(),
        detail: None,
        source_path: None,
    })
}

#[tokio::test]
async fn public_question_tool_delivers_images_in_question_order_and_keeps_text_unchanged() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let tool = create_ask_user_tool();
    let input = parse(
        r#"{"questions":[{"question":"Which layout?","header":"Layout","options":[{"label":"Dense","description":"More rows"},{"label":"Spacious","description":"More space"}],"multiSelect":false},{"question":"Which files?","header":"Files","options":[{"label":"Images","description":"Pictures"},{"label":"PDF","description":"Path reference"}],"multiSelect":true}],"answers":{"Which layout?":"MODEL GUESS"}}"#,
    );
    let mut answers = RuntimeMap::new();
    answers.insert("Which layout?".into(), "  Spacious, custom  ".into());
    answers.insert("Which files?".into(), "PDF: /tmp/notes.pdf".into());
    let layout_images = vec![image("image/png", "AQID"), image("image/jpeg", "BAUG")];
    let file_images = vec![image("image/gif", "BwgJ"), image("image/webp", "CgsM")];
    let expected_images = [layout_images.clone(), file_images.clone()].concat();
    let mut attachments = RuntimeMap::new();
    attachments.insert("Which files?".into(), file_images);
    attachments.insert("Which layout?".into(), layout_images);
    let mut output = input.as_object().unwrap().clone();
    output.insert("answers", runtime_codec::encode_value(&answers).unwrap());
    let expected_output = RuntimeJson::Object(output);
    for supplied in [
        None,
        Some(Box::new(RuntimeMap::new())),
        Some(Box::new(attachments)),
    ] {
        let expected_blocks = supplied
            .as_ref()
            .filter(|attachments| !attachments.is_empty())
            .map(|_| expected_images.clone());
        let handler = Arc::new(RecordingElicit {
            response: ElicitResponse::Answers {
                answers: answers.clone(),
                attachments: supplied,
            },
            requests: Mutex::new(Vec::new()),
            cancel_before_return: false,
        });
        let ctx = context(process.clone(), handler.clone());
        let parsed = tool.parse_input(&input).unwrap();
        let result = tool.call(parsed, &ctx, None).await.unwrap().serializable;
        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.output, expected_output);
        assert_eq!(result.extra_user_blocks, expected_blocks);
        let [OutputBlock::Text(text)] = result.model_content.as_slice() else {
            panic!("images must not be embedded in the tool result text");
        };
        assert_eq!(runtime_json::parse(&text.text).unwrap(), expected_output);
        let requests = handler.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].attachments_supported, Some(true));
        let expected_questions = input.get("questions").unwrap().as_array().unwrap();
        assert_eq!(
            requests[0].questions.as_ref().unwrap(),
            &expected_questions
                .iter()
                .map(|question| question.as_object().unwrap().clone())
                .collect::<Vec<_>>()
        );
    }
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_question_tool_discards_answers_and_images_after_host_cancellation() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut answers = RuntimeMap::new();
    answers.insert("Which layout?".into(), "Spacious".into());
    let mut attachments = RuntimeMap::new();
    attachments.insert("Which layout?".into(), vec![image("image/png", "AQID")]);
    let handler = Arc::new(RecordingElicit {
        response: ElicitResponse::Answers {
            answers,
            attachments: Some(Box::new(attachments)),
        },
        requests: Mutex::new(Vec::new()),
        cancel_before_return: true,
    });
    let ctx = context(process.clone(), handler.clone());
    let tool = create_ask_user_tool();
    let input = tool.parse_input(&question()).unwrap();
    let result = tool.call(input, &ctx, None).await.unwrap().serializable;
    assert_eq!(handler.requests.lock().unwrap().len(), 1);
    assert!(ctx.signal.is_cancelled());
    assert_eq!(result.extra_user_blocks, None);
    assert_eq!(result.output.get("answers"), Some(&parse("{}")));
    let [OutputBlock::Text(text)] = result.model_content.as_slice() else {
        panic!("cancelled answers must not deliver image blocks");
    };
    assert_eq!(text.text, RuntimeString::from("User declined to answer."));
    process.shutdown().await.unwrap();
}

struct QuestionImageProvider {
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for QuestionImageProvider {
    fn info(&self) -> &ProviderInfo {
        static INFO: std::sync::OnceLock<ProviderInfo> = std::sync::OnceLock::new();
        INFO.get_or_init(|| ProviderInfo {
            name: "question-image-regression".into(),
            base_url: None,
            env_key: None,
            default_model: "question-image-regression".into(),
        })
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let first = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len() == 1
        };
        let usage = Usage {
            input_tokens: 100,
            output_tokens: 10,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        };
        if first {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::ToolUseStart {
                    id: "question-call".into(),
                    name: "ask_user".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "question-call".into(),
                    partial_json: runtime_json::stringify(&question()),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "question-call".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::ToolUse,
                    usage,
                },
            ]));
        }
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "Image received.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage,
            },
        ]))
    }
}

#[tokio::test]
async fn public_agent_delivers_question_images_to_the_next_model_request() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-question-image-regression-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let picture = image("image/png", "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==");
    let mut answers = RuntimeMap::new();
    answers.insert("Which layout?".into(), "Screenshot attached".into());
    let mut attachments = RuntimeMap::new();
    attachments.insert("Which layout?".into(), vec![picture.clone()]);
    let handler = Arc::new(RecordingElicit {
        response: ElicitResponse::Answers {
            answers,
            attachments: Some(Box::new(attachments)),
        },
        requests: Mutex::new(Vec::new()),
        cancel_before_return: false,
    });
    let provider = Arc::new(QuestionImageProvider {
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = serde_json::json!("question-image-regression");
    options.config.as_mut().unwrap()["agentMemory"] = serde_json::json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = serde_json::json!(false);
    options.tools = Some(vec![create_ask_user_tool()]);
    options.elicit_handler = Some(handler.clone());
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let result = session
        .run(
            Input::Text("Ask for a screenshot.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        handler.requests.lock().unwrap()[0].attachments_supported,
        Some(true)
    );
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let tool_result = requests[1]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id == "question-call" => {
                    Some(result)
                }
                _ => None,
            })
            .expect("the next model request retains the question tool result");
        assert_eq!(
            tool_result
                .output
                .as_ref()
                .and_then(|output| output.get("answers"))
                .and_then(|answers| answers.get("Which layout?")),
            Some(&parse(r#""Screenshot attached""#))
        );
        let images: Vec<ContentBlock> = requests[1]
            .messages
            .iter()
            .flat_map(|message| match message {
                Message::ToolResult(result) => {
                    result.extra_user_blocks.as_deref().unwrap_or_default()
                }
                _ => &[],
            })
            .filter(|block| matches!(block, ContentBlock::Image(_)))
            .cloned()
            .collect();
        assert_eq!(
            images,
            vec![picture],
            "the next provider call receives the real image, not just a filename"
        );
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
