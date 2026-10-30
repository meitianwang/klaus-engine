//! Defect: scoped children inherited browser mappings after losing the host operations behind their native tools.
//! Root cause: filtering lost implementation dependencies; cold continuations must rebuild them from the current authorized context.
//! Work item: 2026-10-15 child browser host dependencies.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::browser::{
    BrowserHostToolMapping, BrowserOperation, BrowserRuntimeRequest, BrowserRuntimeResponse,
    BrowserTabsAction, BrowserTarget,
};
use klaus_agent_core::capabilities::{RuntimeCapabilityRoot, SelectedRuntimeCapabilityRoot};
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SubagentMessageRequest, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const CHILD_PROMPT: &str = "scoped-browser-child-directive";
const NATIVE_TOOLS: [&str; 4] = [
    "browser_tabs",
    "browser_navigate",
    "browser_download",
    "browser_list_connected_browsers",
];

macro_rules! runtime_json {
    ($($value:tt)*) => {
        runtime_json::parse(&RuntimeString::from(json!($($value)*).to_string())).unwrap()
    };
}

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn tool_call(name: &str, input: serde_json::Value) -> ModelStream {
    let id = RuntimeString::from(format!("call-{name}"));
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "browser-child-fixture".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.clone(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.clone(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: usage(),
        },
    ]))
}

fn finish() -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "browser-child-fixture".into(),
        },
        ModelStreamEvent::TextDelta {
            text: "Browser checks completed.".into(),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(),
        },
    ]))
}

fn result<'a>(request: &'a ModelRequest, id: &str) -> &'a ToolResultMessage {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result),
            _ => None,
        })
        .expect("scripted tool result must be visible to the next model turn")
}

fn expect_error(result: &ToolResultMessage, expected: &str) {
    assert!(result.is_error, "expected rejection: {expected}");
    assert!(
        result.content.iter().any(|block| {
            matches!(block, OutputBlock::Text(text) if text.text.contains(expected))
        }),
        "expected rejection {expected:?}, got {:?}",
        result.content
    );
}

fn expect_hidden_host_rejection(result: &ToolResultMessage) {
    assert!(result.is_error, "a forged hidden-host call must fail");
    // A hidden executor is absent from the admitted pool or retained behind its role gate.
    assert!(
        result
            .content
            .iter()
            .any(|block| matches!(block, OutputBlock::Text(text)
            if text.text == "Unknown tool: mcp__selected__tabs"
                || text.text == "The selected agent definition does not permit this tool")),
        "expected an explicit hidden-host admission rejection, got {:?}",
        result.content
    );
}

struct BrowserChildProvider {
    info: ProviderInfo,
    spawned: AtomicUsize,
    tabs_only: bool,
    denied: bool,
    swap_owner: bool,
    root_policy: bool,
    child_step: AtomicUsize,
    completed: Arc<AtomicBool>,
    cold: bool,
    host_calls: Arc<AtomicUsize>,
    expected_error: Option<&'static str>,
}

impl ModelProvider for BrowserChildProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
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
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if self.root_policy {
            assert!(
                request
                    .tools
                    .iter()
                    .all(|tool| !tool.name.starts_with("mcp__")),
                "root filtering retains internal dispatch without exposing host dependencies to the model"
            );
        }
        let is_child=request.messages.iter().any(|message|match message {Message::User(user)=>user.content.iter().any(|block|matches!(block,ContentBlock::Text(text) if text.text.contains(CHILD_PROMPT))),_=>false});
        if is_child {
            assert!(
                request
                    .tools
                    .iter()
                    .all(|tool| !tool.name.starts_with("mcp__")),
                "host dependencies stay hidden from the model tool surface"
            );
            if self.tabs_only && !self.swap_owner {
                assert!(request
                    .tools
                    .iter()
                    .filter(|tool| tool.name.starts_with("browser_"))
                    .all(|tool| tool.name == "browser_tabs"));
            }
            let step = self.child_step.fetch_add(1, Ordering::SeqCst);
            if step == 0 {
                let mut request = BrowserRuntimeRequest::new(
                    BrowserOperation::Tabs {
                        action: BrowserTabsAction::List,
                        url: None,
                        name: None,
                    },
                    BrowserTarget::default(),
                    Some(5000),
                );
                request
                    .metadata
                    .insert("toolName".into(), Json::from("browser_tabs"));
                let input = runtime_codec::encode_value(&request).unwrap();
                return tool_call(
                    "mcp__selected__tabs",
                    serde_json::from_str(&runtime_json::stringify(&input).try_to_utf8().unwrap())
                        .unwrap(),
                );
            }
            if step == 1 {
                expect_hidden_host_rejection(result(&request, "call-mcp__selected__tabs"));
                assert_eq!(
                    self.host_calls.load(Ordering::SeqCst),
                    0,
                    "a model cannot reach the hidden executor by forging native request metadata"
                );
            }
            let step = step - 1;
            if self.tabs_only {
                if step.is_multiple_of(2) {
                    return tool_call("browser_tabs", json!({"action":"list"}));
                }
                let previous = result(&request, "call-browser_tabs");
                assert_eq!(
                    previous.is_error, self.denied,
                    "explicit host denials must remain effective"
                );
                if let Some(expected) = self.expected_error {
                    expect_error(previous, expected);
                }
                if !self.denied {
                    assert_eq!(
                        previous.output.as_ref().unwrap().get("ok"),
                        Some(&Json::Bool(true)),
                        "the current browser adapter must return a successful native result"
                    );
                }
                assert_eq!(
                    self.host_calls.load(Ordering::SeqCst),
                    if self.denied { 0 } else { step.div_ceil(2) },
                    "only successful native calls may reach the host executor"
                );
                if self.denied || step == 3 || self.cold {
                    self.completed.store(true, Ordering::SeqCst);
                }
                return finish();
            }
            if step > 0 {
                let previous = result(&request, &format!("call-{}", NATIVE_TOOLS[step - 1]));
                assert!(
                    !previous.is_error,
                    "native child browser dispatch must reach its host: {:?}",
                    previous.content
                );
                assert_eq!(
                    previous.output.as_ref().unwrap().get("ok"),
                    Some(&Json::Bool(true))
                );
            }
            match step {
                0 => return tool_call("browser_tabs", json!({"action":"list"})),
                1 => return tool_call("browser_navigate", json!({"url":"https://app.test/form"})),
                2 => {
                    return tool_call(
                        "browser_download",
                        json!({"url":"https://app.test/report.pdf","filename":"report.pdf"}),
                    );
                }
                3 => return tool_call("browser_list_connected_browsers", json!({})),
                4 => {
                    self.completed.store(true, Ordering::SeqCst);
                    return finish();
                }
                _ => panic!("unexpected child continuation"),
            }
        }
        let stage = self.spawned.fetch_add(1, Ordering::SeqCst);
        if stage == 0 {
            return tool_call(
                "task",
                json!({"model": "low", "description":"Run scoped browser checks","prompt":CHILD_PROMPT,"subagent_type":"browser-worker","run_in_background":false}),
            );
        }
        if stage == 1 && self.tabs_only && !self.denied && !self.cold {
            let id = result(&request, "call-task")
                .output
                .as_ref()
                .unwrap()
                .get("agentId")
                .unwrap()
                .as_string()
                .unwrap()
                .try_to_utf8()
                .unwrap();
            return tool_call(
                "task",
                json!({"description":"Resume browser checks","prompt":CHILD_PROMPT,"resume":id}),
            );
        }
        finish()
    }
}

struct BrowserHost {
    name: String,
    calls: Arc<AtomicUsize>,
    identity: Option<&'static str>,
}
#[async_trait]
impl Tool for BrowserHost {
    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.identity.map(RuntimeString::from)
    }
    fn static_name(&self) -> &str {
        &self.name
    }
    fn static_description(&self) -> &str {
        "Browser host fixture"
    }
    fn input_schema(&self) -> Json {
        runtime_json!({"type":"object"})
    }
    fn static_mcp_server(&self) -> Option<&str> {
        Some("selected")
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let request: BrowserRuntimeRequest = runtime_codec::decode_value(&input).unwrap();
        let canonical = match request.operation {
            BrowserOperation::Tabs { .. } => "browser_tabs",
            BrowserOperation::Navigate { .. } => "browser_navigate",
            BrowserOperation::Download { .. } => "browser_download",
            BrowserOperation::ListConnectedBrowsers { .. } => "browser_list_connected_browsers",
            _ => panic!("ungranted operation reached host"),
        };
        assert_eq!(
            request.metadata.get(&RuntimeString::from("toolName")),
            Some(&Json::from(canonical))
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            runtime_codec::encode_value(&BrowserRuntimeResponse {
                message: Some("Browser fixture completed.".into()),
                ..Default::default()
            })
            .unwrap(),
            "Browser fixture completed.",
            false,
        ))
    }
}

struct ReplaceBrowserOwner {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl HookFn for ReplaceBrowserOwner {
    async fn run(&self, _: HookInput) -> HookOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut request = BrowserRuntimeRequest::new(
            BrowserOperation::Navigate {
                url: "https://app.test/rewritten".into(),
                wait_until: None,
                viewport: None,
            },
            BrowserTarget::default(),
            Some(5000),
        );
        request
            .metadata
            .insert("toolName".into(), Json::from("browser_navigate"));
        HookOutcome {
            updated_input: Some(runtime_codec::encode_value(&request).unwrap()),
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn scoped_browser_child_dispatches_granted_tools_after_snapshot_restore_and_respects_denials()
{
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    for scenario in [
        "tabs-resume",
        "all-native",
        "host-disallowed",
        "host-disabled",
        "root-policy-tabs",
        "hook-owner",
        "root-host-disallowed",
        "root-native-disallowed",
        "native-disabled",
    ] {
        let mut mapping = BrowserHostToolMapping::for_mcp_server("selected");
        let swap_owner = scenario == "hook-owner";
        if swap_owner {
            mapping.navigate = mapping.tabs.clone();
        }
        let hook_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let tabs_only = scenario != "all-native";
        let root_policy = scenario.starts_with("root-");
        let denied = scenario.starts_with("host-")
            || scenario.ends_with("disallowed")
            || scenario == "native-disabled"
            || swap_owner;
        let mut options = agent_options(process.clone());
        options.config.as_mut().unwrap()["prewarm"] = json!(false);
        if scenario == "all-native" {
            options.config.as_mut().unwrap()["allow"] = json!(["browser_download"]);
        }
        if scenario == "host-disabled" {
            options.config.as_mut().unwrap()["disabledTools"] = json!([mapping.tabs.clone()]);
        }
        if scenario == "native-disabled" {
            options.config.as_mut().unwrap()["disabledTools"] = json!(["browser_tabs"]);
        }
        options.tools = None;
        if swap_owner {
            options.hooks = vec![Hook {
                event: HookEvent::PreToolUse,
                matcher: Some(mapping.tabs.clone()),
                handler: HookHandler::Fn(Arc::new(ReplaceBrowserOwner {
                    calls: hook_calls.clone(),
                })),
                is_async: false,
            }];
        }
        let provider = Arc::new(BrowserChildProvider {
            info: ProviderInfo {
                name: "browser-child-fixture".into(),
                default_model: "browser-child-fixture".into(),
                base_url: None,
                env_key: None,
            },
            spawned: AtomicUsize::new(0),
            tabs_only,
            denied,
            swap_owner,
            root_policy,
            child_step: AtomicUsize::new(0),
            completed: completed.clone(),
            cold: false,
            host_calls: calls.clone(),
            expected_error: None,
        });
        options.provider = provider.clone();
        options.mcp_tools = Some(
            [
                mapping.tabs.clone(),
                mapping.navigate.clone(),
                mapping.download.clone(),
                mapping.list_connected_browsers.clone(),
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|name| {
                Arc::new(BrowserHost {
                    name,
                    calls: calls.clone(),
                    identity: None,
                }) as Arc<dyn Tool>
            })
            .collect(),
        );
        options.capability_roots = vec![
            RuntimeCapabilityRoot::host("selected").with_browser_tool_mapping(mapping.clone())
        ];
        options.include_default_agents = Some(false);
        options.agents = Some(vec![AgentDefinition {
            name: "browser-worker".into(),
            description: "Scoped browser fixture".into(),
            tools: Some(if swap_owner {
                vec!["browser_tabs".into(), "browser_navigate".into()]
            } else if tabs_only {
                vec!["browser_tabs".into()]
            } else {
                NATIVE_TOOLS.iter().map(|name| (*name).into()).collect()
            }),
            disallowed_tools: if scenario == "host-disallowed" {
                Some(vec![mapping.tabs.clone().into()])
            } else {
                None
            },
            ..Default::default()
        }]);
        let directory =
            std::env::temp_dir().join(format!("klaus-browser-child-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let agent = Agent::new(options);
        let session = agent.start_session(StartSessionOptions {
            agent_definition_name: if root_policy {
                Some("browser-root".into())
            } else {
                None
            },
            agents: if root_policy {
                Some(vec![AgentDefinition {
                    name: "browser-root".into(),
                    tools: Some(vec!["task".into(), "browser_tabs".into()]),
                    disallowed_tools: match scenario {
                        "root-host-disallowed" => Some(vec![mapping.tabs.clone().into()]),
                        "root-native-disallowed" => Some(vec!["browser_tabs".into()]),
                        _ => None,
                    },
                    ..Default::default()
                }])
            } else {
                None
            },
            cwd: Some(directory.to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
            ..Default::default()
        });
        let mut events = session
            .run_streamed(
                Input::Text("Run the scoped browser checks".into()),
                TurnOptions::default(),
            )
            .events;
        let mut observed = Vec::new();
        while let Some(event) = events.next().await {
            observed.push(format!("{event:?}"));
        }
        let completion = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !completed.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        session.shutdown_and_wait().await.unwrap();
        let _ = std::fs::remove_dir_all(directory);
        assert!(
            completion.is_ok() && completed.load(Ordering::SeqCst),
            "{scenario}: parent={}, child={}, calls={}\n{}",
            provider.spawned.load(Ordering::SeqCst),
            provider.child_step.load(Ordering::SeqCst),
            calls.load(Ordering::SeqCst),
            observed.join("\n")
        );
        assert_eq!(
            hook_calls.load(Ordering::SeqCst),
            usize::from(swap_owner),
            "{scenario}: hook must traverse the actual dispatch pipeline"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if denied {
                0
            } else if tabs_only {
                2
            } else {
                4
            },
            "{scenario}"
        );
    }
    process.shutdown().await.unwrap();
}

fn cold_options(
    process: RuntimeProcessContext,
    calls: Arc<AtomicUsize>,
    completed: Arc<AtomicBool>,
    mapping: BrowserHostToolMapping,
    identity: Option<&'static str>,
    scenario: &str,
) -> AgentOptions {
    let mut options = agent_options(process);
    options.tools = None;
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    match scenario {
        "host-disabled" => {
            options.config.as_mut().unwrap()["disabledTools"] = json!([mapping.tabs.clone()]);
        }
        "native-disabled" => {
            options.config.as_mut().unwrap()["disabledTools"] = json!(["browser_tabs"]);
        }
        _ => {}
    }
    let expected_error = match scenario {
        "host-disallowed" | "host-disabled" => Some("Unknown tool: mcp__selected__tabs"),
        "native-disabled" | "native-disallowed" => Some("Unknown tool: browser_tabs"),
        "hook-owner" => Some("The selected agent definition does not permit this tool"),
        _ => None,
    };
    options.provider = Arc::new(BrowserChildProvider {
        info: ProviderInfo {
            name: "browser-child-fixture".into(),
            default_model: "browser-child-fixture".into(),
            base_url: None,
            env_key: None,
        },
        spawned: AtomicUsize::new(0),
        tabs_only: true,
        denied: expected_error.is_some(),
        swap_owner: scenario == "hook-owner",
        root_policy: false,
        child_step: AtomicUsize::new(0),
        completed,
        cold: true,
        host_calls: calls.clone(),
        expected_error,
    });
    options.mcp_tools = Some(vec![Arc::new(BrowserHost {
        name: mapping.tabs.clone(),
        calls,
        identity,
    })]);
    options.capability_roots =
        vec![RuntimeCapabilityRoot::host("selected").with_browser_tool_mapping(mapping.clone())];
    options.include_default_agents = Some(false);
    options.agents = Some(vec![AgentDefinition {
        name: "browser-worker".into(),
        tools: Some(if scenario == "hook-owner" {
            vec!["browser_tabs".into(), "browser_navigate".into()]
        } else {
            vec!["browser_tabs".into()]
        }),
        disallowed_tools: match scenario {
            "host-disallowed" => Some(vec![mapping.tabs.into()]),
            "native-disallowed" => Some(vec!["browser_tabs".into()]),
            _ => None,
        },
        ..Default::default()
    }]);
    options
}

#[tokio::test]
async fn browser_child_cold_reopen_uses_current_context_and_respects_denials() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    for scenario in [
        "recover",
        "nested-root",
        "mcp-reregister-no-tool-identity",
        "mcp-reregister-changed-tool-identity",
        "mapping-changed",
        "host-disallowed",
        "native-disallowed",
        "host-disabled",
        "native-disabled",
        "hook-owner",
    ] {
        eprintln!("Cold browser fixture scenario: {scenario}");
        let directory =
            std::env::temp_dir().join(format!("klaus-browser-cold-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let store_options = JsonlSessionStoreOptions {
            dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
            ..Default::default()
        };
        let start = || StartSessionOptions {
            id: Some("browser-cold-root".into()),
            cwd: Some(directory.to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
            agent_definition_name: (scenario == "nested-root").then(|| "browser-root".into()),
            agents: (scenario == "nested-root").then(|| {
                vec![AgentDefinition {
                    name: "browser-root".into(),
                    tools: Some(vec!["task".into(), "browser_tabs".into()]),
                    ..Default::default()
                }]
            }),
            ..Default::default()
        };
        let mut mapping = BrowserHostToolMapping::for_mcp_server("selected");
        if scenario == "hook-owner" {
            mapping.navigate = mapping.tabs.clone();
        }
        // Re-register the same admitted MCP server without changing its selected root.
        // Per-tool persistence identity is separate from native browser runtime authority.
        let identity = (scenario != "mcp-reregister-no-tool-identity")
            .then_some("fixture:mcp-tool:selected:v1");
        let calls = Arc::new(AtomicUsize::new(0));
        let warm_completed = Arc::new(AtomicBool::new(false));
        let store = Arc::new(JsonlSessionStore::new(store_options.clone()));
        let warm = Agent::new(cold_options(
            process.clone(),
            calls.clone(),
            warm_completed.clone(),
            mapping.clone(),
            identity,
            "recover",
        ))
        .open_session(store.clone(), start())
        .await
        .unwrap();
        let mut events = warm
            .run_streamed(
                Input::Text("Run browser checks".into()),
                TurnOptions::default(),
            )
            .events;
        while events.next().await.is_some() {}
        assert!(
            warm_completed.load(Ordering::SeqCst),
            "{scenario}: warm provider must check the hidden rejection and native result"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "{scenario}: warm native calls must execute"
        );
        let child: RuntimeString = warm
            .background_agent_summaries()
            .into_iter()
            .next()
            .expect("ordinary worker handle")
            .id
            .into();
        warm.flush_rollout().await.unwrap();
        warm.shutdown_and_wait().await.unwrap();
        let loaded = store
            .read_sub_agent("browser-cold-root", &child.try_to_utf8().unwrap())
            .await
            .unwrap();
        let creation = loaded
            .raw_items
            .iter()
            .find_map(|item| match item {
                klaus_agent_core::schemas::rollout::RolloutItem::Event(event)
                    if event.event == "ordinary_subagent_creation" =>
                {
                    event.payload.as_ref()
                }
                _ => None,
            })
            .expect("durable creation authority");
        assert_eq!(
            creation.get("version"),
            Some(&runtime_json!(6)),
            "{scenario}"
        );
        let capture = creation
            .get("runtimeCapabilityCapture")
            .expect("durable selected root capture");
        assert_eq!(
            capture.get("selectedRoots"),
            Some(
                &runtime_codec::encode_value(&vec![SelectedRuntimeCapabilityRoot::host(
                    "selected"
                )])
                .unwrap()
            ),
            "{scenario}: MCP re-registration must preserve the admitted selected root"
        );
        let evidence = capture
            .get("evidence")
            .expect("durable capability evidence");
        assert_eq!(
            evidence.get("browserKind"),
            Some(&Json::from("mapping")),
            "{scenario}: this fixture uses MCP mapping rather than a native browser runtime"
        );
        let bindings = evidence
            .get("browserBindings")
            .expect("durable browser bindings");
        assert_eq!(
            bindings.get("browser_tabs"),
            Some(&Json::from(mapping.tabs.as_str()))
        );
        assert_eq!(
            bindings.get("browser_navigate"),
            Some(&Json::from(mapping.navigate.as_str()))
        );
        let declarations = creation.get("tools").unwrap().as_array().unwrap();
        for (name, source) in [("browser_tabs", "native"), (mapping.tabs.as_str(), "mcp")] {
            let declaration = declarations
                .iter()
                .find(|tool| tool.get("name") == Some(&Json::from(name)))
                .expect("browser tool declaration");
            assert_eq!(
                declaration,
                &runtime_json!({"name": name, "source": source}),
                "{scenario}: v5 stores declarations without historical runtime fingerprints"
            );
        }
        assert!(
            !runtime_json::stringify(creation).contains("fixture:mcp-tool:selected:v1"),
            "per-tool MCP persistence identities stay outside the durable creation record"
        );
        drop(warm);
        drop(store);

        let mut current_mapping = mapping;
        if scenario == "mapping-changed" {
            current_mapping.navigate = current_mapping.tabs.clone();
        }
        let completed = Arc::new(AtomicBool::new(false));
        let current_calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = Arc::new(AtomicUsize::new(0));
        let mut options = cold_options(
            process.clone(),
            current_calls.clone(),
            completed.clone(),
            current_mapping.clone(),
            if scenario == "mcp-reregister-changed-tool-identity" {
                Some("fixture:mcp-tool:selected:v2")
            } else {
                identity
            },
            scenario,
        );
        if scenario == "hook-owner" {
            options.hooks = vec![Hook {
                event: HookEvent::PreToolUse,
                matcher: Some(current_mapping.tabs.clone()),
                handler: HookHandler::Fn(Arc::new(ReplaceBrowserOwner {
                    calls: hook_calls.clone(),
                })),
                is_async: false,
            }];
        }
        let cold = Agent::new(options)
            .resume_session(
                Arc::new(JsonlSessionStore::new(store_options)),
                "browser-cold-root",
                start(),
            )
            .await
            .unwrap();
        let (receipt_sender, mut receipts) = tokio::sync::mpsc::unbounded_channel();
        cold.set_agent_notification_sink(Some(Arc::new(move |receipt| {
            let _ = receipt_sender.send(receipt.clone());
        })));
        let outcome = cold
            .send_subagent_message(SubagentMessageRequest {
                target: child.clone(),
                message: CHILD_PROMPT.into(),
                summary: "Continue persisted browser checks".into(),
                sender_agent_id: None,
                user_initiated: false,
                trigger_turn: true,
            })
            .await;
        let outcome = outcome.expect("stored ordinary child must remain addressable");
        if scenario == "mapping-changed" {
            assert_eq!(
                outcome.expect_err("changed saved capability bindings must refuse cold loading"),
                RuntimeString::from(
                    "The original host capability roots are unavailable or changed."
                )
            );
            assert!(
                !completed.load(Ordering::SeqCst),
                "changed saved bindings must be rejected before the provider completes a turn"
            );
        } else {
            assert!(
                outcome
                    .expect("the saved root and current MCP policy must restore the child")
                    .resumed,
                "{scenario}: declarations must restore before current dispatch policy is enforced"
            );
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let receipt = receipts.recv().await.expect("completion receipt channel");
                    if receipt.agent_id.as_ref() == Some(&child) && receipt.status == "completed" {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            assert!(
                completed.load(Ordering::SeqCst),
                "{scenario}: cold provider must check actual hidden and native tool results"
            );
        }
        cold.shutdown_and_wait().await.unwrap();
        let denied = matches!(
            scenario,
            "mapping-changed"
                | "host-disallowed"
                | "native-disallowed"
                | "host-disabled"
                | "native-disabled"
                | "hook-owner"
        );
        assert_eq!(
            hook_calls.load(Ordering::SeqCst),
            usize::from(scenario == "hook-owner"),
            "{scenario}: owner replacement must traverse the actual dispatch pipeline"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "{scenario}: cold continuation must not reuse the previous host executor"
        );
        assert_eq!(
            current_calls.load(Ordering::SeqCst),
            usize::from(!denied),
            "{scenario}: only authorized native dispatch may reach the current host executor"
        );
        let output = std::env::temp_dir().join(format!(
            "klaus-agent-bg-{}.txt",
            child.try_to_utf8().unwrap()
        ));
        if output.exists() {
            std::fs::remove_file(output).unwrap();
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    process.shutdown().await.unwrap();
}
