//! Deterministic protocol wire-format scenario drivers producing golden regression traces.

use std::collections::BTreeMap;

use klaus_agent_core::engine::message_queue::{
    QueueItemState, QueueMutationResult, QueueMutationStatus, QueuePriority, QueueSnapshot,
    QueueSnapshotItem, QueueSnapshotStatus, QueueTerminalState, QueuedMessage, QueuedValue,
};
use klaus_agent_core::engine::rewind::{RewindOutcome, RewindScope};
use klaus_agent_core::engine::subagent_lifecycle::{
    SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentSnapshot, SubagentStatus,
};
use klaus_agent_core::extensions::plugins::installed::{InstalledPluginRecord, InstalledPlugins};
use klaus_agent_core::extensions::plugins::loader::{
    PluginDisabledEntry, PluginLoadIssue, PluginLoadIssueKind,
};
use klaus_agent_core::extensions::plugins::runtime::{
    PluginCapabilitySummary, PluginPermissionScope, PluginRuntimeEntry, PluginRuntimeSnapshot,
};
use klaus_agent_core::extensions::runtime_snapshot::{
    EffectiveExtension, EffectiveHook, ExtensionRuntimeSnapshot, ExtensionSource, HookHandlerKind,
    PricingRuntimeSnapshot,
};
use klaus_agent_core::hooks::setup::{
    SetupContextAdmissionRecord, SetupContextAdmissionStatus, SetupLifecycleSnapshot,
    SetupRunRecord, SetupRunResult, SetupRunStatus, SetupTrigger,
};
use klaus_agent_core::hooks::types::HookEvent;
use klaus_agent_core::mcp::types::McpConnectionState;
use klaus_agent_core::memory::agent_memory::{AgentMemoryMergeChoice, AgentMemoryScope};
use klaus_agent_core::memory::maintenance::{
    AutoDreamRun, AutoMemoryRun, AutoMemoryRunReason, MemoryMaintenanceEvent,
    MemoryMaintenanceQueryErrorCode, MemoryMaintenanceQueryErrorDetail, MemoryMaintenanceRunKind,
    MemoryMaintenanceSnapshot, MemoryMaintenanceStatus, MemoryMaintenanceTrigger,
    MemoryMaintenanceTurn,
};
use klaus_agent_core::memory::snapshot_decision::{
    AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotDecisionRequest,
    AgentMemorySnapshotDecisionSource, AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff,
    AgentMemorySnapshotDiffEntry, AgentMemorySnapshotDiffKind,
};
use klaus_agent_core::observability::cost_tracker::{
    CostSnapshot, ModelUsageSnapshot, ModelUsageTotals,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeEncode, RuntimeJson, RuntimeString,
};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::store::session_store::{
    ThreadSummary, UnarchiveSessionResult, UnarchiveSessionStatus,
};
use klaus_agent_core::tasks::task_store::{
    Task, TaskListChange, TaskListCursorStatus, TaskListSnapshot, TaskStatus,
};
use klaus_agent_core::types::errors::{
    EngineError, EngineErrorCode, InputValidationErrorCode, InputValidationErrorDetail,
    TerminalReason,
};
use klaus_agent_core::types::events::{
    CompactionMode, FileChange, FileChangeKind, FileChangeStatus, ItemError, ItemStatus,
    NotificationLevel, ThreadItem, ToolDomainEvent,
};
use klaus_agent_core::types::permission::{
    ApprovalKind, ApprovalRequest, ExternalPermissionMode, PermissionBehavior,
    PermissionDestination, PermissionRule, PermissionUpdate,
};
use klaus_agent_core::types::tool::{McpPromptArgument, ToolExposure};
use serde_json::{json, Value};

use crate::types::{
    parse_op, AgentMemorySnapshotDecisionQueryResult, BackgroundAgentSummary, ElicitRequestWire,
    Elicitation, InstalledPluginMutationResult, McpAuthStatus, McpAuthorizationRequestWire,
    McpAuthorizationResolvedWire, McpAuthorizationRevokedWire, McpAuthorizationStatusWire,
    McpElicitationAction, McpElicitationEvent, McpElicitationRequest, McpElicitationResolvedWire,
    McpPromptRuntimeSnapshot, McpResourceRuntimeSnapshot, McpResourceTemplateDescriptor,
    McpRuntimeResult, McpRuntimeScope, McpRuntimeSnapshot, McpServerInfo, McpServerRuntimeSnapshot,
    McpServerStatusUpdate, McpStartupFailureReason, McpToolRuntimeSnapshot, PluginActivationScope,
    ProtocolEvent, ResolvedRuntimeCapabilityRoot, RunResult, RuntimeCapabilityDescriptor,
    RuntimeCapabilityName, RuntimeCapabilityResolution, RuntimeCapabilitySource,
    SelectedRuntimeCapabilityRoot, ShutdownCompletion, SteerResult,
};

/// Generate the stable protocol/event wire-format regression trace.
pub fn protocol_wire_trace() -> Value {
    let events = protocol_events();
    json!({
        "scenario": "protocol-wire",
        "protocolEvents": events
            .iter()
            .map(runtime_encoded_value)
            .collect::<Vec<_>>(),
        "opValidation": op_validation_trace(),
    })
}

fn protocol_events() -> Vec<ProtocolEvent> {
    let usage = Usage {
        input_tokens: 12,
        output_tokens: 3,
        cache_read_input_tokens: 4,
        cache_creation_input_tokens: 5,
        total_tokens: Some(24),
    };
    let file_change = FileChange {
        turn_diff: None,
        path: "src/app.ts".into(),
        kind: FileChangeKind::Modify,
        diff: Some("@@ -1 +1\n-old\n+new".into()),
        added: Some(1),
        removed: Some(1),
    };
    let add_rules = PermissionUpdate::AddRules {
        rules: vec![PermissionRule {
            tool_name: "bash".into(),
            argument_prefix: Some("git status".into()),
            tool_name_prefix: None,
        }],
        behavior: PermissionBehavior::Allow,
        destination: PermissionDestination::ProjectSettings,
    };
    let set_mode = PermissionUpdate::SetMode {
        mode: ExternalPermissionMode::AcceptEdits,
        destination: PermissionDestination::Session,
    };
    let approval_request = ApprovalRequest {
        id: "apr_1".into(),
        kind: ApprovalKind::Tool,
        tool_name: "write".into(),
        input: runtime_value(json!({ "path": "src/app.ts", "content": "new" })),
        message: "Allow write?".into(),
        suggestions: Some(vec![add_rules.clone(), set_mode.clone()]),
    };

    let engine_events = vec![
        event(
            klaus_agent_core::types::events::EngineEvent::ThreadStarted {
                thread_id: "thread_1".into(),
            },
        ),
        event(klaus_agent_core::types::events::EngineEvent::TurnStarted {
            turn_id: "turn_1".into(),
        }),
        event(klaus_agent_core::types::events::EngineEvent::StreamRequestStart { inference: None }),
        event(klaus_agent_core::types::events::EngineEvent::TextDelta {
            inference: None,
            message_id: None,
            phase: None,
            text: "hello".into(),
        }),
        event(
            klaus_agent_core::types::events::EngineEvent::ThinkingDelta {
                message_id: None,
                inference: None,
                text: "thinking".into(),
            },
        ),
        event(klaus_agent_core::types::events::EngineEvent::ItemStarted {
            inference: None,
            item: ThreadItem::CommandExecution {
                id: "cmd_1".into(),
                command: "echo hi".into(),
                aggregated_output: RuntimeString::default(),
                exit_code: None,
                status: ItemStatus::InProgress,
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ItemUpdated {
            inference: None,
            item: ThreadItem::CommandExecution {
                id: "cmd_1".into(),
                command: "echo hi".into(),
                aggregated_output: "hi\n".into(),
                exit_code: Some(0),
                status: ItemStatus::Completed,
            },
        }),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::AgentMessage {
                    phase: None,
                    id: "msg_1".into(),
                    text: "final answer".into(),
                },
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::Reasoning {
                    id: "reason_1".into(),
                    text: "because".into(),
                },
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::FileChange {
                    id: "file_1".into(),
                    changes: vec![file_change.clone()],
                    status: FileChangeStatus::Completed,
                },
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: "tool_1".into(),
                    tool_name: "read".into(),
                    arguments: runtime_value(json!({ "path": "src/app.ts" })),
                    result: Some(runtime_value(json!({ "ok": true }))),
                    error: None,
                    status: ItemStatus::Completed,
                },
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::ToolCall {
                    id: "tool_2".into(),
                    tool_name: "bash".into(),
                    arguments: runtime_value(json!({ "command": "false" })),
                    result: None,
                    error: Some(ItemError {
                        message: "failed".into(),
                    }),
                    status: ItemStatus::Failed,
                },
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::Error {
                    id: "err_item_1".into(),
                    message: "visible item error".into(),
                },
            },
        ),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_1".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::Stdout {
                chunk: "out".into(),
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_1".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::Stderr {
                chunk: "err".into(),
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_1".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::Status {
                message: "running".into(),
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_1".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::FileChange {
                change: file_change.clone(),
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_2".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::AgentProgress {
                agent_id: runtime_value(json!("agent_child")),
                event: Box::new(klaus_agent_core::types::events::EngineEvent::TextDelta {
                    inference: None,
                    message_id: None,
                    phase: None,
                    text: "child delta".into(),
                }),
            },
        }),
        event(klaus_agent_core::types::events::EngineEvent::ToolEvent {
            tool_use_id: "tu_plan".into(),
            turn_id: "turn_1".into(),
            event: ToolDomainEvent::PlanModeChanged { enabled: true },
        }),
        event(
            klaus_agent_core::types::events::EngineEvent::ApprovalRequested {
                request: approval_request,
            },
        ),
        event(
            klaus_agent_core::types::events::EngineEvent::PermissionUpdate {
                destination: PermissionDestination::ProjectSettings,
                update: add_rules,
            },
        ),
        event(klaus_agent_core::types::events::EngineEvent::Compaction {
            mode: CompactionMode::Micro,
            freed_tokens: 128,
        }),
        event(klaus_agent_core::types::events::EngineEvent::Tombstone {
            uuid: "deadbeef".into(),
        }),
        event(klaus_agent_core::types::events::EngineEvent::Notification {
            level: NotificationLevel::Warn,
            message: "heads up".into(),
        }),
        event(klaus_agent_core::types::events::EngineEvent::TurnFailed {
            inference: None,
            error: EngineError {
                code: EngineErrorCode::ProviderError,
                message: "provider down".into(),
                cause: None,
                request_id: Some("req_123".into()),
                provider_kind: None,
                retry_after_ms: None,
            },
        }),
        event(
            klaus_agent_core::types::events::EngineEvent::TurnCompleted {
                turn_id: "turn_1".into(),
                usage: Some(usage.clone()),
                stop_reason: TerminalReason::Completed,
            },
        ),
    ];

    let installed_record = InstalledPluginRecord {
        name: "docs".into(),
        plugin_id: Some("docs@local".into()),
        marketplace: Some("local".into()),
        version: Some("1.0.0".into()),
        install_path: "/plugins/docs".into(),
        data_path: Some("/plugin-data/docs".into()),
        source: "marketplace:local".into(),
        scope: Some("user".into()),
        dependencies: vec!["base@local".into()],
        installed_at_ms: 10,
        enabled: true,
    };
    let installed_plugins = InstalledPlugins {
        version: 2,
        plugins: BTreeMap::from([("docs@local".into(), installed_record.clone())]),
    };
    let mut mutated_record = installed_record.clone();
    mutated_record.enabled = false;
    let mutated_plugins = InstalledPlugins {
        version: 2,
        plugins: BTreeMap::from([("docs@local".into(), mutated_record.clone())]),
    };
    let plugin_runtime = PluginRuntimeSnapshot {
        entries: vec![PluginRuntimeEntry {
            name: "docs".into(),
            version: Some("1.0.0".into()),
            display_name: Some("Docs".into()),
            description: Some("Search product docs".into()),
            plugin_namespace: "docs".into(),
            keywords: vec!["documentation".into()],
            dependencies: vec!["base@local".into()],
            user_config: Default::default(),
            settings: Default::default(),
            dir: "/plugins/docs".into(),
            enabled: true,
            permission_scope: PluginPermissionScope {
                mcp_servers: vec!["docs".into()],
                lsp_servers: Vec::new(),
            },
            has_skills: true,
            runtime_capabilities: vec!["browser".into()],
            interface_capabilities: vec!["panel".into()],
            app_connectors: vec!["connector-docs".into()],
        }],
        capability_summaries: vec![PluginCapabilitySummary {
            name: "docs".into(),
            display_name: "Docs".into(),
            description: Some("Search product docs".into()),
            has_skills: true,
            mcp_server_names: vec!["docs".into()],
            lsp_server_names: Vec::new(),
            runtime_capabilities: vec!["browser".into()],
            interface_capabilities: vec!["panel".into()],
            app_connectors: vec!["connector-docs".into()],
            dependencies: vec!["base@local".into()],
        }],
        disabled: vec![PluginDisabledEntry {
            name: Some("legacy".into()),
            dir: "/plugins/legacy".into(),
            reason: "disabled by installed plugin state".into(),
        }],
        errors: vec![PluginLoadIssue {
            kind: PluginLoadIssueKind::InvalidManifest,
            dir: "/plugins/broken".into(),
            message: "installed plugin has no loadable manifest".into(),
        }],
        warnings: vec![PluginLoadIssue {
            kind: PluginLoadIssueKind::DuplicateMcpServerSuppressed,
            dir: "/plugins/docs".into(),
            message: "duplicate MCP server suppressed".into(),
        }],
        needs_refresh: false,
        plugin_reconnect_key: 0,
    };

    let memory_run = AutoMemoryRun {
        run_id: "memory_run_1".into(),
        session_id: "session_1".into(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/workspace/.klaus/memory".into(),
        status: MemoryMaintenanceStatus::Failed,
        started_at: 10,
        ended_at: Some(20),
        reason: Some(AutoMemoryRunReason::ToolFailure),
        error: Some("write failed".into()),
        message_count: 2,
        cursor_message_uuid: None,
        files_touched: vec!["/workspace/.klaus/memory/MEMORY.md".into()],
        turn_count: Some(1),
    };
    let dream_run = AutoDreamRun {
        dream_run_id: "dream_run_1".into(),
        session_id: "session_1".into(),
        trigger: MemoryMaintenanceTrigger::Auto,
        status: MemoryMaintenanceStatus::Completed,
        started_at: 30,
        ended_at: Some(30),
        reason: None,
        error: None,
        last_consolidated_at: Some(20),
        sessions_reviewing: 5,
        files_touched: vec!["/workspace/.klaus/memory/MEMORY.md".into()],
        turns: vec![MemoryMaintenanceTurn {
            text: "finished\n<dream-summary>the dream sharpened: durable constraints emerged</dream-summary>".into(),
            tool_use_count: 1,
        }],
        turn_count: 2,
        summary: Some("the dream sharpened: durable constraints emerged".into()),
    };
    let agent_memory_snapshot_audit = AgentMemorySnapshotDecisionAudit {
        request: AgentMemorySnapshotDecisionRequest {
            request_id: "snapshot-decision-1".into(),
            agent_type: "planner".into(),
            scope: AgentMemoryScope::User,
            snapshot_timestamp: "2026-07-18T01:02:03Z".into(),
            snapshot_id: "snapshot-sha256-1".into(),
            local_state_id: "local-sha256-1".into(),
            diff: AgentMemorySnapshotDiff {
                added_count: 1,
                modified_count: 1,
                removed_count: 1,
                entries: vec![
                    AgentMemorySnapshotDiffEntry {
                        path: "MEMORY.md".into(),
                        kind: AgentMemorySnapshotDiffKind::Modified,
                    },
                    AgentMemorySnapshotDiffEntry {
                        path: "notes/added.md".into(),
                        kind: AgentMemorySnapshotDiffKind::Added,
                    },
                    AgentMemorySnapshotDiffEntry {
                        path: "notes/removed.md".into(),
                        kind: AgentMemorySnapshotDiffKind::Removed,
                    },
                ],
                truncated: false,
            },
            first_use: false,
            requested_at: 1_721_264_523_000,
            deadline_at: 1_721_264_583_000,
        },
        status: AgentMemorySnapshotDecisionStatus::Failed,
        decision: Some(AgentMemoryMergeChoice::Merge),
        source: Some(AgentMemorySnapshotDecisionSource::Host),
        decided_at: Some(1_721_264_530_000),
        ended_at: Some(1_721_264_531_000),
        error: Some("merge tool failed".into()),
    };

    let mut events = engine_events;
    events.extend([
        ProtocolEvent {
            submission_id: "sub_elicit".into(),
            elicitation: Some(Elicitation {
                request_id: "el_1".into(),
                request: ElicitRequestWire {
                    questions: None,
                    attachments_supported: None,
                    metadata: None,
                    prompt: "Pick one".into(),
                    options: Some(vec!["a".into(), "b".into()]),
                    multi_select: Some(true),
                },
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_elicit".to_string().into(),
            mcp_elicitation: Some(McpElicitationEvent {
                request_id: "mcp-elicit-public-1".into(),
                server_name: "computer-use".into(),
                request: runtime_codec::decode_value::<McpElicitationRequest>(&runtime_value(
                    json!({
                    "mode": "form",
                    "_meta": { "persist": true },
                    "message": "Allow access to Calendar?",
                    "requestedSchema": {
                        "type": "object",
                        "properties": {
                            "account": { "type": "string", "format": "email" },
                            "remember": { "type": "boolean", "default": true },
                            "scope": {
                                "type": "string",
                                "oneOf": [
                                    { "const": "read", "title": "Read only" },
                                    { "const": "write", "title": "Read and write" }
                                ]
                            }
                        },
                        "required": ["account", "scope"]
                    }
                    }),
                ))
                .expect("MCP elicitation fixture is typed"),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_elicit_resolve".to_string().into(),
            mcp_elicitation_resolved: Some(McpElicitationResolvedWire {
                request_id: "mcp-elicit-public-1".into(),
                action: McpElicitationAction::Accept,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: String::new().into(),
            mcp_authorization: Some(McpAuthorizationRequestWire {
                request_id: "mcp_auth_1".into(),
                server_name: "remote-tools".into(),
                authorization_url:
                    "https://auth.example/authorize?client_id=client-a&state=state-a".into(),
                redirect_uri: "http://127.0.0.1/callback".into(),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_auth_resolve".to_string().into(),
            mcp_authorization_resolved: Some(McpAuthorizationResolvedWire {
                request_id: "mcp_auth_1".into(),
                accepted: true,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_auth_status".to_string().into(),
            mcp_authorization_status: Some(McpAuthorizationStatusWire {
                server_name: "remote-tools".into(),
                auth_status: McpAuthStatus::OAuth,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_auth_revoke".to_string().into(),
            mcp_authorization_revoked: Some(McpAuthorizationRevokedWire {
                server_name: "remote-tools".into(),
                auth_status: McpAuthStatus::NotLoggedIn,
                revoked: true,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_mcp_runtime".to_string().into(),
            mcp_runtime: Some(McpRuntimeResult {
                runtime: McpRuntimeSnapshot {
                    servers: vec![
                        McpServerRuntimeSnapshot {
                            name: "remote-tools".into(),
                            state: McpConnectionState::Ready,
                            auth_status: McpAuthStatus::OAuth,
                            failure_reason: None,
                            error: None,
                            authorization_url: None,
                            instructions: Some("Use the indexed docs.".into()),
                            server_info: Some(McpServerInfo {
                                name: Some("remote-tools".into()),
                                version: Some("2.1.0".into()),
                                title: None,
                            }),
                            tools: vec![McpToolRuntimeSnapshot {
                                name: "mcp__remote-tools__lookup".into(),
                                exposure: ToolExposure::Deferred,
                                search_hint: Some("Search indexed docs".into()),
                                read_only: true,
                                destructive: Some(false),
                            }],
                            resources: vec![McpResourceRuntimeSnapshot {
                                uri: "docs://guide".into(),
                                name: Some("Guide".into()),
                                mime_type: Some("text/markdown".into()),
                                description: Some("Product guide".into()),
                            }],
                            resource_templates: vec![McpResourceTemplateDescriptor {
                                uri_template: "docs://{section}".into(),
                                name: Some("Section".into()),
                                mime_type: Some("text/markdown".into()),
                                description: Some("One documentation section".into()),
                            }],
                            prompts: vec![McpPromptRuntimeSnapshot {
                                name: "summarize".into(),
                                description: Some("Summarize a guide".into()),
                                arguments: Some(vec![McpPromptArgument {
                                    name: "uri".into(),
                                    description: Some("Guide URI".into()),
                                    required: Some(true),
                                }]),
                            }],
                        },
                        McpServerRuntimeSnapshot {
                            name: "unavailable".into(),
                            state: McpConnectionState::Failed,
                            auth_status: McpAuthStatus::NotLoggedIn,
                            failure_reason: Some(McpStartupFailureReason::ReauthenticationRequired),
                            error: Some("401 Unauthorized".into()),
                            authorization_url: None,
                            instructions: None,
                            server_info: None,
                            tools: Vec::new(),
                            resources: Vec::new(),
                            resource_templates: Vec::new(),
                            prompts: Vec::new(),
                        },
                    ],
                },
                activation_scope: McpRuntimeScope::ActiveSession,
            }),
            ..Default::default()
        },
        // The unsolicited lifecycle push. `submissionId` is empty on purpose: it
        // answers no op, and a host that read it as a turn reply would settle the
        // wrong waiter.
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            mcp_server_status: Some(McpServerStatusUpdate {
                server_name: "remote-tools".into(),
                state: McpConnectionState::Connecting,
                auth_status: McpAuthStatus::OAuth,
                failure_reason: None,
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            mcp_server_status: Some(McpServerStatusUpdate {
                server_name: "unavailable".into(),
                state: McpConnectionState::Failed,
                auth_status: McpAuthStatus::NotLoggedIn,
                failure_reason: Some(McpStartupFailureReason::ReauthenticationRequired),
                error: Some("401 Unauthorized".into()),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_runtime_capabilities".to_string().into(),
            runtime_capabilities: Some(RuntimeCapabilityResolution {
                capabilities: Default::default(),
                descriptors: vec![RuntimeCapabilityDescriptor {
                    name: RuntimeCapabilityName::Browser,
                    source: RuntimeCapabilitySource::plugin(
                        "calendar",
                        Some("2.0.0"),
                        "/plugins/calendar",
                    ),
                }],
                selected_roots: vec![
                    SelectedRuntimeCapabilityRoot::plugin("calendar"),
                    SelectedRuntimeCapabilityRoot::plugin("missing"),
                ],
                effective_roots: vec![ResolvedRuntimeCapabilityRoot {
                    selection: SelectedRuntimeCapabilityRoot::plugin("calendar"),
                    source: RuntimeCapabilitySource::plugin(
                        "calendar",
                        Some("2.0.0"),
                        "/plugins/calendar",
                    ),
                    capabilities: vec![RuntimeCapabilityName::Browser],
                }],
                missing_roots: vec![SelectedRuntimeCapabilityRoot::plugin("missing")],
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_extension_runtime".to_string().into(),
            extension_runtime: Some(Box::new(ExtensionRuntimeSnapshot {
                agents: vec![EffectiveExtension {
                    name: "desktop-agent".into(),
                    source: ExtensionSource::session(),
                }],
                skills: vec![EffectiveExtension {
                    name: "desktop-skill".into(),
                    source: ExtensionSource::session(),
                }],
                output_styles: vec![EffectiveExtension {
                    name: "formal".into(),
                    source: ExtensionSource::session(),
                }],
                selected_output_style: Some("formal".into()),
                hooks: vec![EffectiveHook {
                    event: HookEvent::PreToolUse,
                    matcher: Some("read".into()),
                    handler_kind: HookHandlerKind::Command,
                    is_async: false,
                    timeout_ms: Some(5_000),
                    source: ExtensionSource::session(),
                }],
                pricing: PricingRuntimeSnapshot {
                    model_ids: vec!["fake-model".into()],
                    source: ExtensionSource::session(),
                },
                suppressed: Vec::new(),
                plugin_runtime: PluginRuntimeSnapshot::default(),
            })),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_installed_plugins".to_string().into(),
            installed_plugins: Some(Box::new(installed_plugins)),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_plugin_runtime".to_string().into(),
            plugin_runtime: Some(Box::new(plugin_runtime.clone())),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_plugin_mutation".to_string().into(),
            plugin_mutation: Some(Box::new(InstalledPluginMutationResult {
                persisted: true,
                applied_to_current_session: false,
                activation_scope: PluginActivationScope::EndpointRestart,
                restart_required: true,
                plugin: mutated_record,
                installed_plugins: mutated_plugins,
                runtime: plugin_runtime,
            })),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_rewind".to_string().into(),
            rewound: Some(RewindOutcome {
                scope: RewindScope::Files,
                dropped_turns: 1,
                remaining_turns: 2,
                files_restored: true,
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_result".to_string().into(),
            result: Some(RunResult {
                stop_reason: TerminalReason::Completed,
                usage: Some(usage),
                final_text: "{\"answer\":42}".into(),
                final_message_id: Some("assistant-final".into()),
                structured_output: Some(runtime_value(json!({ "answer": 42 }))),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_history".to_string().into(),
            history: Some(vec![
                Message::User(UserMessage {
                    question_answer: None,
                    uuid: "u1".into(),
                    content: vec![text_block("hi")],
                    is_meta: None,
                }),
                Message::Assistant(AssistantMessage {
                    uuid: "a1".into(),
                    content: vec![text_block("hello")],
                    model: "fake-model".into(),
                    usage: None,
                }),
            ]),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_cost".to_string().into(),
            cost: Some(CostSnapshot {
                total_cost_usd: 0.01,
                cache_hit_rate: 0.25,
                models: vec![ModelUsageSnapshot {
                    totals: ModelUsageTotals {
                        model: "fake-model".into(),
                        input_tokens: 10,
                        output_tokens: 5,
                        cache_read_input_tokens: 3,
                        cache_creation_input_tokens: 2,
                        cost_usd: 0.01,
                        requests: 1,
                    },
                    cache_hit_rate: 0.2,
                }],
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_subagents".to_string().into(),
            subagents: Some(vec![BackgroundAgentSummary {
                id: "agent_1".into(),
                description: "research".into(),
                status: klaus_agent_core::types::tool::BackgroundTaskStatus::Completed,
                teammate: true,
                idle: false,
                transcript: "partial".into(),
                output_file: "/tmp/out.txt".into(),
            }]),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_subagent_lifecycle".to_string().into(),
            subagent_lifecycle: Some(SubagentSnapshot {
                agents: vec![SubagentLifecycleRecord {
                    agent_id: "agent_1".into(),
                    kind: SubagentKind::Workflow,
                    status: SubagentStatus::Failed,
                    context: SubagentContext {
                        session_id: "session_1".into(),
                        parent_agent_id: Some("parent_1".into()),
                        parent_thread_id: Some("thread_1".into()),
                        agent_type: Some("research".into()),
                        cwd: "/workspace".into(),
                        description: "research".into(),
                        depth: 2,
                        resume_from: None,
                        spawn_turn_id: None,
                    },
                    started_at_ms: 10,
                    updated_at_ms: 20,
                    completed_at_ms: Some(20),
                    last_result: None,
                    last_error: Some("failed".into()),
                    cancellation_requested: false,
                    model: None,
                    model_source: None,
                }],
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            memory_maintenance_event: Some(MemoryMaintenanceEvent::AutoMemory {
                run: memory_run.clone(),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            memory_maintenance_event: Some(MemoryMaintenanceEvent::AutoDream {
                run: dream_run.clone(),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_memory_maintenance".into(),
            memory_maintenance: Some(MemoryMaintenanceSnapshot {
                auto_memory_runs: vec![memory_run],
                auto_dream_runs: vec![dream_run],
                auto_memory_cursor_message_uuid: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_queued".to_string().into(),
            queued: Some(vec![QueuedMessage {
                trigger_turn: None,
                value: QueuedValue::Text("queued text".into()),
                mode: "prompt".into(),
                priority: Some(QueuePriority::Now),
                uuid: Some("q1".into()),
                session_id: Some("thread-queued".into()),
                is_meta: Some(true),
                origin: Some("host".into()),
                agent_id: None,
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: None,
                display_metadata: None,
            }]),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_pending".to_string().into(),
            pending: Some(vec![QueuedMessage {
                trigger_turn: None,
                value: QueuedValue::Blocks(vec![text_block("pending text")]),
                mode: "task-notification".into(),
                priority: Some(QueuePriority::Later),
                uuid: None,
                session_id: Some("thread-pending".into()),
                is_meta: None,
                origin: None,
                agent_id: Some("agent_1".into()),
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: None,
                display_metadata: None,
            }]),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            queue_snapshot: Some(QueueSnapshot {
                revision: 7,
                items: vec![QueueSnapshotItem {
                    message: QueuedMessage {
                        trigger_turn: None,
                        value: QueuedValue::Text("editable queued text".into()),
                        mode: "prompt".into(),
                        priority: Some(QueuePriority::Next),
                        uuid: Some("q-editable".into()),
                        session_id: Some("thread-queue".into()),
                        is_meta: None,
                        origin: Some("host".into()),
                        agent_id: None,
                        correlation_id: None,
                        pdf_bytes: 0,
                        pdf_documents: 0,
                        delivery: None,
                        display_metadata: None,
                    },
                    state: QueueItemState::Pending,
                    mutable: true,
                }],
                status: QueueSnapshotStatus::Authoritative,
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_queue_remove".into(),
            queue_mutation: Some(QueueMutationResult {
                mutation_id: "mutation-remove-1".into(),
                status: QueueMutationStatus::Applied,
                snapshot: QueueSnapshot {
                    revision: 8,
                    items: Vec::new(),
                    status: QueueSnapshotStatus::Authoritative,
                    error: None,
                },
                terminal_state: Some(QueueTerminalState::Removed),
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_queue_interrupted".into(),
            queue_mutation: Some(QueueMutationResult {
                mutation_id: "mutation-after-interrupt".into(),
                status: QueueMutationStatus::Missing,
                snapshot: QueueSnapshot {
                    revision: 9,
                    items: Vec::new(),
                    status: QueueSnapshotStatus::Authoritative,
                    error: None,
                },
                terminal_state: Some(QueueTerminalState::Interrupted),
                error: Some("queued continuation was interrupted".into()),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_unarchive".into(),
            unarchived: Some(UnarchiveSessionResult {
                thread_id: "thread-restored".into(),
                status: UnarchiveSessionStatus::Restored,
                thread: Some(ThreadSummary {
                    id: "thread-restored".into(),
                    created_at: 1_700_000_000,
                    updated_at: 1_700_000_100,
                    cwd: "/workspace".into(),
                    model: "fake-model".into(),
                    title: Some("Restored".into()),
                    role: klaus_agent_core::schemas::rollout::SessionRole::Main,
                    parent_thread_id: None,
                    forked_from_id: None,
                    archived: Some(false),
                    host_metadata: None,
                }),
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_setup".into(),
            setup: Some(SetupRunResult {
                request_id: "setup-init-1".into(),
                trigger: SetupTrigger::Init,
                status: SetupRunStatus::Completed,
                source_request_id: None,
                started_at: Some(1_700_000_100),
                ended_at: Some(1_700_000_101),
                hook_count: 1,
                additional_context: vec!["workspace initialized".into()],
                system_messages: Vec::new(),
                error: None,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_setup_lifecycle".into(),
            setup_lifecycle: Some(SetupLifecycleSnapshot {
                runs: vec![SetupRunRecord {
                    request_id: "setup-init-1".into(),
                    session_id: "thread-setup".into(),
                    trigger: SetupTrigger::Init,
                    status: SetupRunStatus::Completed,
                    started_at: 1_700_000_100,
                    ended_at: Some(1_700_000_101),
                    hook_count: 1,
                    additional_context: vec!["workspace initialized".into()],
                    system_messages: Vec::new(),
                    error: None,
                }],
                idempotency_results: BTreeMap::from([(
                    RuntimeString::from("setup-maintenance-evicted"),
                    SetupRunResult {
                        request_id: "setup-maintenance-evicted".into(),
                        trigger: SetupTrigger::Maintenance,
                        status: SetupRunStatus::Completed,
                        source_request_id: None,
                        started_at: Some(1_700_000_090),
                        ended_at: Some(1_700_000_091),
                        hook_count: 1,
                        additional_context: vec!["maintenance context".into()],
                        system_messages: Vec::new(),
                        error: None,
                    },
                )]),
                init_closed_at: Some(1_700_000_102),
                context_admission: Some(SetupContextAdmissionRecord {
                    admission_id: "setup-context-1".into(),
                    request_id: "setup-init-1".into(),
                    session_id: "thread-setup".into(),
                    status: SetupContextAdmissionStatus::Consumed,
                    started_at: 1_700_000_102,
                    ended_at: Some(1_700_000_102),
                    error: None,
                }),
                context_consumed_request_id: Some("setup-init-1".into()),
                context_consumed_at: Some(1_700_000_102),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_task_list".into(),
            task_list: Some(TaskListSnapshot {
                plan_id: None,
                plan_kind: "tasks".into(),
                session_id: "thread-tasks".into(),
                task_list_id: "thread-tasks".into(),
                generation: "task-generation-1".into(),
                revision: 4,
                fingerprint: "sha256:task-list".into(),
                tasks: vec![task_fixture()],
                next_cursor: Some("todo-v2-v1.4.1.cursor.identity".into()),
                total: 2,
                cursor_status: TaskListCursorStatus::Current,
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            task_list_change: Some(TaskListChange {
                plan_id: None,
                plan_kind: "tasks".into(),
                session_id: "thread-tasks".into(),
                task_list_id: "thread-tasks".into(),
                generation: "task-generation-1".into(),
                revision: 5,
                fingerprint: "sha256:task-list-next".into(),
                tasks: vec![task_fixture()],
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: RuntimeString::default(),
            agent_memory_snapshot_decision: Some(agent_memory_snapshot_audit.clone()),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_snapshot_decisions".into(),
            agent_memory_snapshot_decisions: Some(vec![agent_memory_snapshot_audit.clone()]),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_snapshot_decision_query".into(),
            agent_memory_snapshot_decision_query: Some(AgentMemorySnapshotDecisionQueryResult {
                request_id: "snapshot-decision-1".into(),
                audit: Some(agent_memory_snapshot_audit),
            }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_steer".to_string().into(),
            steered: Some(SteerResult { applied: true }),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_input_error".to_string().into(),
            error: Some("model text-only does not support native PDF input".into()),
            error_detail: Some(
                InputValidationErrorDetail {
                    code: InputValidationErrorCode::UnsupportedInput,
                    feature: Some("native_pdf".into()),
                    provider: Some("openai".into()),
                    model: Some("text-only".into()),
                    limit_bytes: None,
                    limit_count: None,
                }
                .into(),
            ),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_memory_maintenance_error".into(),
            error: Some("memory maintenance persistence is inconsistent".into()),
            error_detail: Some(
                MemoryMaintenanceQueryErrorDetail {
                    code: MemoryMaintenanceQueryErrorCode::PersistenceInconsistent,
                    run_kind: Some(MemoryMaintenanceRunKind::AutoDream),
                    run_id: Some("dream-terminal-failure".into()),
                }
                .into(),
            ),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_error".to_string().into(),
            error: Some("bad op".into()),
            ..Default::default()
        },
        ProtocolEvent {
            submission_id: "sub_shutdown".into(),
            shutdown_completed: Some(Box::new(ShutdownCompletion {
                ok: true,
                flush_completed: true,
                cleanup_completed: true,
                timed_out: false,
                error: None,
            })),
            ..Default::default()
        },
    ]);
    events
}

fn event(event: klaus_agent_core::types::events::EngineEvent) -> ProtocolEvent {
    ProtocolEvent {
        submission_id: "sub_run".to_string().into(),
        event: Some(event),
        ..Default::default()
    }
}

fn text_block(text: &str) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}

fn task_fixture() -> Task {
    Task {
        id: "1".into(),
        subject: "Wire task".into(),
        description: "Exercise TodoV2 protocol wire format".into(),
        active_form: Some("Testing TodoV2".into()),
        owner: None,
        status: TaskStatus::InProgress,
        blocks: Vec::new(),
        blocked_by: Vec::new(),
        metadata: None,
    }
}

fn op_validation_trace() -> Vec<Value> {
    op_cases()
        .into_iter()
        .map(|(name, value)| match parse_op(runtime_value(value)) {
            Ok(op) => json!({
                "name": name,
                "accepted": true,
                "normalized": runtime_encoded_value(&op),
            }),
            Err(_) => json!({
                "name": name,
                "accepted": false,
            }),
        })
        .collect()
}

fn runtime_value(value: Value) -> RuntimeJson {
    let text = serde_json::to_string(&value).expect("trace fixture JSON encodes");
    runtime_json::parse(&RuntimeString::from_utf8(&text)).expect("trace fixture JSON is exact")
}

fn runtime_encoded_value<T: RuntimeEncode + ?Sized>(value: &T) -> Value {
    let text = runtime_codec::encode_text(value).expect("runtime fixture encodes");
    let text = text
        .try_to_utf8()
        .expect("serialized JSON text contains only scalar characters");
    serde_json::from_str(&text).expect("runtime fixture emits JSON")
}

fn op_cases() -> Vec<(&'static str, Value)> {
    let add_rules = json!({
        "type": "addRules",
        "rules": [{ "toolName": "bash", "argumentPrefix": "git status" }],
        "behavior": "allow",
        "destination": "projectSettings"
    });
    vec![
        (
            "init-provider-oauth-token",
            json!({
                "id": "op_init_oauth_token",
                "op": {
                    "type": "init_session",
                    "init": {
                        "provider": { "type": "openai" },
                        "auth": {
                            "type": "o_auth_token",
                            "accessToken": "<redacted>",
                            "refreshToken": "<redacted>",
                            "credentialId": "desktop-account-a",
                            "replaceStored": true,
                            "oauth": { "clientId": "client-a" }
                        }
                    }
                }
            }),
        ),
        (
            "init-provider-oauth-stored",
            json!({
                "id": "op_init_oauth_stored",
                "op": {
                    "type": "init_session",
                    "init": {
                        "provider": { "type": "openai" },
                        "auth": {
                            "type": "oauth_stored",
                            "credentialId": "desktop-account-a",
                            "oauth": { "clientId": "client-a" }
                        }
                    }
                }
            }),
        ),
        (
            "init-openai-gateway-compatibility",
            json!({
                "id": "op_init_openai_gateway",
                "op": {
                    "type": "init_session",
                    "init": {
                        "provider": {
                            "type": "openai",
                            "model": "qwen3.7-max",
                            "api": "chat",
                            "baseUrl": "https://gateway.example/v1",
                            "compatibility": {
                                "requestIdentity": { "sessionId": "<redacted-session>" },
                                "extraHeaders": {
                                    "X-Endpoint": "<redacted-endpoint>",
                                    "X-Caller-App": "<redacted-caller>",
                                    "X-User-Id": "<redacted-user>",
                                    "X-Device-Id": "<redacted-device>"
                                }
                            }
                        },
                        "auth": { "type": "api_key", "key": "<redacted>" }
                    }
                }
            }),
        ),
        (
            "init-extensions",
            json!({
                "id": "op_init_extensions",
                "op": {
                    "type": "init_session",
                    "init": {
                        "agents": [{
                            "name": "desktop-agent",
                            "description": "Desktop specialist",
                            "prompt": "Work from the desktop context."
                        }],
                        "skills": [{
                            "name": "desktop-skill",
                            "description": "Desktop workflow",
                            "body": "Run the workflow.",
                            "path": "/desktop/skills/desktop-skill/SKILL.md"
                        }],
                        "outputStyles": [{
                            "name": "formal",
                            "description": "Formal prose",
                            "body": "Use formal prose."
                        }],
                        "selectedOutputStyle": "formal",
                        "commandHooks": [{
                            "event": "PreToolUse",
                            "matcher": "read",
                            "command": "validate-read",
                            "timeoutMs": 5000
                        }],
                        "pricing": [{
                            "model": "fake-model",
                            "inputPerMTok": 7.5,
                            "outputPerMTok": 0.25,
                            "cacheReadPerMTok": 0.1,
                            "cacheWritePerMTok": 0.2
                        }],
                        "agentMemorySnapshotPolicy": {
                            "timeoutMs": 45000,
                            "firstUse": "initialize",
                            "timeout": "keep",
                            "disconnect": "merge",
                            "noHandler": "replace"
                        }
                    }
                }
            }),
        ),
        (
            "run-string",
            json!({
                "id": "op_run_string",
                "op": {
                    "type": "run",
                    "input": "hello",
                    "options": {
                        "model": "model-a",
                        "permissionMode": "acceptEdits",
                        "outputSchema": { "type": "object" },
                        "systemPromptAppend": "extra",
                        "displayMetadata": {
                            "displayText": "/push-code",
                            "slashCommand": { "name": "push-code", "kind": "skill" }
                        }
                    }
                }
            }),
        ),
        // Per-turn reasoning effort rides the same `run` op as `model`. The two
        // `EffortValue` variants are untagged (a level string vs an integer), so
        // both decode paths are pinned: a host delivering into another
        // conversation must survive either spelling.
        (
            "run-effort-level",
            json!({
                "id": "op_run_effort_level",
                "op": {
                    "type": "run",
                    "input": "hello",
                    "options": { "effort": "high" }
                }
            }),
        ),
        (
            "run-effort-number",
            json!({
                "id": "op_run_effort_number",
                "op": {
                    "type": "run",
                    "input": "hello",
                    "options": { "effort": 42 }
                }
            }),
        ),
        (
            "run-parts",
            json!({
                "id": "op_run_parts",
                "op": {
                    "type": "run",
                    "input": [
                        { "type": "text", "text": "hi" },
                        { "type": "image", "base64": "AAAA", "mimeType": "image/png" }
                    ]
                }
            }),
        ),
        (
            "run-document",
            json!({
                "id": "op_run_document",
                "op": {
                    "type": "run",
                    "input": [{
                        "type": "document",
                        "base64": "JVBERi0xLjcK",
                        "mimeType": "application/pdf",
                        "filename": "report.pdf"
                    }]
                }
            }),
        ),
        (
            "approve-allow",
            json!({
                "id": "op_approve",
                "op": {
                    "type": "approve",
                    "requestId": "apr_1",
                    "response": {
                        "decision": "allow",
                        "updatedInput": { "path": "src/app.ts" },
                        "updatedPermissions": [add_rules]
                    }
                }
            }),
        ),
        (
            "resolve-elicitation",
            json!({
                "id": "op_elicit",
                "op": {
                    "type": "resolve_elicitation",
                    "requestId": "el_1",
                    "response": { "kind": "decline", "reason": "later" }
                }
            }),
        ),
        (
            "resolve-mcp-elicitation",
            json!({
                "id": "op_mcp_elicit",
                "op": {
                    "type": "resolve_mcp_elicitation",
                    "requestId": "mcp-elicit-public-1",
                    "response": {
                        "action": "accept",
                        "content": {
                            "account": "person@example.com",
                            "remember": true,
                            "scope": "read"
                        },
                        "_meta": { "persist": true }
                    }
                }
            }),
        ),
        (
            "resolve-mcp-authorization",
            json!({
                "id": "op_mcp_auth_resolve",
                "op": {
                    "type": "resolve_mcp_authorization",
                    "requestId": "mcp_auth_1",
                    "response": {
                        "kind": "code",
                        "code": "<redacted>",
                        "state": "state-a"
                    }
                }
            }),
        ),
        (
            "get-mcp-authorization-status",
            json!({
                "id": "op_mcp_auth_status",
                "op": {
                    "type": "get_mcp_authorization_status",
                    "serverName": "remote-tools"
                }
            }),
        ),
        (
            "revoke-mcp-authorization",
            json!({
                "id": "op_mcp_auth_revoke",
                "op": {
                    "type": "revoke_mcp_authorization",
                    "serverName": "remote-tools"
                }
            }),
        ),
        (
            "get-mcp-runtime",
            json!({ "id": "op_mcp_runtime", "op": { "type": "get_mcp_runtime" } }),
        ),
        (
            "reconnect-mcp-server",
            json!({
                "id": "op_mcp_reconnect",
                "op": { "type": "reconnect_mcp_server", "serverName": "remote-tools" }
            }),
        ),
        (
            "upsert-mcp-server",
            json!({
                "id": "op_mcp_upsert",
                "op": {
                    "type": "upsert_mcp_server",
                    "server": {
                        "transport": "http",
                        "name": "remote-tools",
                        "url": "https://mcp.example.test/mcp",
                        "oauth": true
                    }
                }
            }),
        ),
        (
            "remove-mcp-server",
            json!({
                "id": "op_mcp_remove",
                "op": { "type": "remove_mcp_server", "serverName": "remote-tools" }
            }),
        ),
        (
            "reload-mcp-servers",
            json!({
                "id": "op_mcp_reload",
                "op": {
                    "type": "reload_mcp_servers",
                    "servers": [{
                        "transport": "stdio",
                        "name": "local-tools",
                        "command": "local-mcp",
                        "args": ["--stdio"]
                    }]
                }
            }),
        ),
        (
            "get-runtime-capabilities",
            json!({
                "id": "op_runtime_capabilities",
                "op": { "type": "get_runtime_capabilities" }
            }),
        ),
        (
            "get-extension-runtime",
            json!({
                "id": "op_extension_runtime",
                "op": { "type": "get_extension_runtime" }
            }),
        ),
        (
            "list-installed-plugins",
            json!({
                "id": "op_plugins_list",
                "op": { "type": "list_installed_plugins" }
            }),
        ),
        (
            "get-plugin-runtime",
            json!({
                "id": "op_plugin_runtime",
                "op": { "type": "get_plugin_runtime" }
            }),
        ),
        (
            "disable-installed-plugin",
            json!({
                "id": "op_plugin_disable",
                "op": {
                    "type": "set_installed_plugin_enabled",
                    "name": "docs@local",
                    "enabled": false
                }
            }),
        ),
        (
            "uninstall-plugin",
            json!({
                "id": "op_plugin_uninstall",
                "op": {
                    "type": "uninstall_plugin",
                    "name": "docs@local",
                    "removeFiles": true
                }
            }),
        ),
        (
            "rewind-files",
            json!({ "id": "op_rewind", "op": { "type": "rewind", "numTurns": 2, "scope": "files" } }),
        ),
        (
            "steer-now",
            json!({ "id": "op_steer", "op": { "type": "steer", "input": "nudge", "priority": "now" } }),
        ),
        (
            "set-model",
            json!({ "id": "op_model", "op": { "type": "set_model", "model": "model-b" } }),
        ),
        (
            "set-permission-mode",
            json!({ "id": "op_perm", "op": { "type": "set_permission_mode", "mode": "dontAsk" } }),
        ),
        (
            "get-history",
            json!({ "id": "op_history", "op": { "type": "get_history" } }),
        ),
        (
            "get-cost",
            json!({ "id": "op_cost", "op": { "type": "get_cost" } }),
        ),
        (
            "get-subagents",
            json!({ "id": "op_subagents", "op": { "type": "get_subagents" } }),
        ),
        (
            "get-subagent-lifecycle",
            json!({ "id": "op_subagent_lifecycle", "op": { "type": "get_subagent_lifecycle" } }),
        ),
        (
            "get-memory-maintenance",
            json!({ "id": "op_memory_maintenance", "op": { "type": "get_memory_maintenance" } }),
        ),
        (
            "get-queued",
            json!({ "id": "op_queued", "op": { "type": "get_queued" } }),
        ),
        (
            "take-pending",
            json!({ "id": "op_pending", "op": { "type": "take_pending" } }),
        ),
        (
            "get-queue-snapshot",
            json!({ "id": "op_queue_snapshot", "op": { "type": "get_queue_snapshot" } }),
        ),
        (
            "edit-queued",
            json!({
                "id": "op_queue_edit",
                "op": {
                    "type": "edit_queued",
                    "mutationId": "mutation-edit-1",
                    "uuid": "q1",
                    "expectedRevision": 7,
                    "edit": { "value": "edited", "priority": "next" }
                }
            }),
        ),
        (
            "remove-queued",
            json!({
                "id": "op_queue_remove",
                "op": {
                    "type": "remove_queued",
                    "mutationId": "mutation-remove-1",
                    "uuid": "q1",
                    "expectedRevision": 8
                }
            }),
        ),
        (
            "reorder-queued",
            json!({
                "id": "op_queue_reorder",
                "op": {
                    "type": "reorder_queued",
                    "mutationId": "mutation-reorder-1",
                    "uuid": "q2",
                    "beforeUuid": "q1",
                    "expectedRevision": 9
                }
            }),
        ),
        (
            "unarchive-session",
            json!({
                "id": "op_unarchive",
                "op": { "type": "unarchive_session", "threadId": "thread-restored" }
            }),
        ),
        (
            "run-setup",
            json!({
                "id": "op_setup",
                "op": {
                    "type": "run_setup",
                    "request": { "requestId": "setup-init-1", "trigger": "init" }
                }
            }),
        ),
        (
            "get-setup-lifecycle",
            json!({ "id": "op_setup_lifecycle", "op": { "type": "get_setup_lifecycle" } }),
        ),
        (
            "get-task-list",
            json!({
                "id": "op_task_list",
                "op": {
                    "type": "get_task_list",
                    "options": { "cursor": "todo-v2-v1.4.1.cursor.identity", "limit": 25 }
                }
            }),
        ),
        (
            "resolve-agent-memory-snapshot-decision",
            json!({
                "id": "op_snapshot_resolve",
                "op": {
                    "type": "resolve_agent_memory_snapshot_decision",
                    "requestId": "snapshot-decision-1",
                    "decision": "merge"
                }
            }),
        ),
        (
            "settle-agent-memory-snapshot-unavailable",
            json!({
                "id": "op_snapshot_unavailable",
                "op": {
                    "type": "settle_agent_memory_snapshot_unavailable",
                    "requestId": "snapshot-decision-2",
                    "reason": "no-handler",
                    "error": "host has no snapshot decision handler"
                }
            }),
        ),
        (
            "get-agent-memory-snapshot-decision",
            json!({
                "id": "op_snapshot_query",
                "op": {
                    "type": "get_agent_memory_snapshot_decision",
                    "requestId": "snapshot-decision-1"
                }
            }),
        ),
        (
            "get-agent-memory-snapshot-decisions",
            json!({
                "id": "op_snapshot_queries",
                "op": { "type": "get_agent_memory_snapshot_decisions" }
            }),
        ),
        (
            "interrupt",
            json!({ "id": "op_interrupt", "op": { "type": "interrupt" } }),
        ),
        (
            "shutdown",
            json!({ "id": "op_shutdown", "op": { "type": "shutdown" } }),
        ),
        (
            "invalid-op",
            json!({ "id": "bad", "op": { "type": "bogus" } }),
        ),
        (
            "invalid-permission-mode",
            json!({ "id": "bad", "op": { "type": "set_permission_mode", "mode": "bogus" } }),
        ),
        (
            "invalid-rewind-zero",
            json!({ "id": "bad", "op": { "type": "rewind", "numTurns": 0 } }),
        ),
        (
            "invalid-run-input",
            json!({ "id": "bad", "op": { "type": "run", "input": [{ "type": "audio", "data": "nope" }] } }),
        ),
    ]
}
