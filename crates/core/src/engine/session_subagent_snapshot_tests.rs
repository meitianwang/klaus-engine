//! Capability and credential evidence tests use the real recovery metadata codec.

use super::*;

#[test]
fn cold_restore_keeps_the_original_child_model_directory_policy() {
    let original = ResolvedConfig {
        subagent_model_catalog: true,
        subagent_model_options: vec![crate::schemas::config::SubagentModelOption {
            value: "custom:child".into(),
            label: "Child model".into(),
        }],
        ..Default::default()
    };
    let (config, bindings) = config_metadata(&original).unwrap();
    let mut fields = RuntimeObject::new();
    fields.insert("config", config);
    fields.insert("credentialBindings", bindings);
    fields.insert("cwd", RuntimeJson::from("/tmp"));
    let restored =
        restore_config(&RuntimeJson::Object(fields), &ResolvedConfig::default()).unwrap();
    assert!(restored.subagent_model_catalog);
    assert_eq!(
        restored.subagent_model_options,
        original.subagent_model_options
    );
}

#[test]
fn boundary_review_floor_codec_requires_versioned_restrictive_evidence_or_explicit_clear() {
    use crate::types::permission::ApprovalPolicy;
    let parse = |text: &str| crate::runtime_values::runtime_json::parse(&text.into()).unwrap();
    assert_eq!(
        super::super::decode_boundary_review_floor(&parse(
            r#"{"version":1,"approvalPolicy":"untrusted"}"#
        ))
        .unwrap(),
        Some(ApprovalPolicy::UnlessTrusted)
    );
    assert_eq!(
        super::super::decode_boundary_review_floor(&parse(
            r#"{"version":1,"approvalPolicy":null}"#
        ))
        .unwrap(),
        None
    );
    for text in [
        "null",
        r#"{"version":2,"approvalPolicy":"untrusted"}"#,
        r#"{"approvalPolicy":"untrusted"}"#,
        r#"{"version":1}"#,
        r#"{"version":1,"approvalPolicy":"never"}"#,
        r#"{"version":1,"approvalPolicy":"on-request"}"#,
    ] {
        assert!(
            super::super::decode_boundary_review_floor(&parse(text)).is_err(),
            "{text}"
        );
    }
}

#[test]
fn creation_bindings_preserve_nested_depth_and_exact_turn_id() {
    let turn_id = RuntimeString::from_utf16(vec![0x0074, 0xd800, 0x0075]);
    let mut fields = RuntimeObject::new();
    fields.insert("depth", runtime_codec::encode_value(&3_u32).unwrap());
    fields.insert("parentAgentId", RuntimeJson::String("direct-parent".into()));
    fields.insert(
        "spawnTurnId",
        runtime_codec::encode_value(&Some(turn_id.clone())).unwrap(),
    );
    let serialized = crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(fields));
    let parsed = crate::runtime_values::runtime_json::parse(&serialized).unwrap();
    let restored = decode_bindings(&parsed).unwrap();
    assert_eq!(restored.depth, 3);
    assert_eq!(restored.parent_agent_id.as_deref(), Some("direct-parent"));
    assert_eq!(restored.spawn_turn_id, Some(turn_id));
}

#[test]
fn legacy_missing_and_explicit_null_creation_turns_remain_unbound() {
    let mut fields = RuntimeObject::new();
    fields.insert("depth", runtime_codec::encode_value(&2_u32).unwrap());
    fields.insert("parentAgentId", RuntimeJson::Null);
    let legacy = decode_bindings(&RuntimeJson::Object(fields.clone())).unwrap();
    assert_eq!(legacy.depth, 2);
    assert_eq!(legacy.parent_agent_id, None);
    assert_eq!(legacy.spawn_turn_id, None);
    fields.insert("spawnTurnId", RuntimeJson::Null);
    let explicit = decode_bindings(&RuntimeJson::Object(fields)).unwrap();
    assert_eq!(explicit.depth, 2);
    assert_eq!(explicit.spawn_turn_id, None);
}

#[test]
fn malformed_creation_bindings_do_not_fabricate_depth_or_turn_ownership() {
    let mut fields = RuntimeObject::new();
    fields.insert("depth", runtime_codec::encode_value(&2_u32).unwrap());
    fields.insert("parentAgentId", RuntimeJson::Null);
    fields.insert("spawnTurnId", RuntimeJson::Bool(true));
    assert!(decode_bindings(&RuntimeJson::Object(fields.clone())).is_err());
    fields.remove("depth");
    fields.insert("spawnTurnId", RuntimeJson::Null);
    assert!(decode_bindings(&RuntimeJson::Object(fields)).is_err());
}

#[test]
fn malformed_creation_parent_binding_does_not_redirect_results_to_root() {
    let mut fields = RuntimeObject::new();
    fields.insert("depth", runtime_codec::encode_value(&2_u32).unwrap());
    fields.insert("spawnTurnId", RuntimeJson::Null);
    assert!(decode_bindings(&RuntimeJson::Object(fields.clone())).is_err());
    fields.insert("parentAgentId", RuntimeJson::Bool(true));
    assert!(decode_bindings(&RuntimeJson::Object(fields)).is_err());
}

#[test]
fn stored_model_and_authority_roundtrip_through_the_runtime_document() {
    let config = crate::schemas::config::default_config();
    let (safe, bindings) = config_metadata(&config).unwrap();
    let mut fields = RuntimeObject::new();
    fields.insert("config", safe);
    fields.insert("credentialBindings", bindings);
    let serialized = crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(fields));
    let parsed = crate::runtime_values::runtime_json::parse(&serialized).unwrap();
    let restored = restore_config(&parsed, &config)
        .unwrap_or_else(|error| panic!("{error:?}; safe metadata={serialized:?}"));
    assert_eq!(restored.model, config.model);
    assert_eq!(restored.model_source, config.model_source);
    assert_eq!(restored.effort, config.effort);
    assert_eq!(restored.ultra_mode, config.ultra_mode);
    assert_eq!(restored.approval_policy, config.approval_policy);
    assert_eq!(restored.sandbox_mode, config.sandbox_mode);
    assert_eq!(
        restored.sandbox_workspace_write,
        config.sandbox_workspace_write
    );
}

#[test]
fn credential_projection_never_persists_sentinel_values() {
    let raw = crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#"{"model":"test","network_proxy":{"url":"https://user:SECRET_SENTINEL@proxy.example/path?access_token=SECRET_SENTINEL&scope=read","headers":{"Authorization":"Bearer SECRET_SENTINEL"},"env":{"API_KEY":"SECRET_SENTINEL"}},"max_output_tokens":4096}"#)).unwrap();
    let mut bindings = Vec::new();
    let safe = scrub(&raw, &mut Vec::new(), &mut bindings);
    let text = crate::runtime_values::runtime_json::stringify(&safe);
    assert!(!text.contains("SECRET_SENTINEL"));
    assert_eq!(
        safe.get("max_output_tokens").and_then(RuntimeJson::as_u64),
        Some(4096)
    );
    assert!(text.contains("proxy.example"));
    assert!(text.contains("scope=read"));
    assert!(!bindings.is_empty());
    for binding in bindings {
        assert!(
            !crate::runtime_values::runtime_json::stringify(&binding).contains("SECRET_SENTINEL")
        );
    }
}

#[test]
fn same_source_proxy_credentials_can_rotate_but_endpoint_cannot() {
    let old =
        RuntimeString::from("https://user:OLD_SECRET@proxy.example/route?access_token=OLD_SECRET");
    let new =
        RuntimeString::from("https://user:NEW_SECRET@proxy.example/route?access_token=NEW_SECRET");
    let other =
        RuntimeString::from("https://user:NEW_SECRET@other.example/route?access_token=NEW_SECRET");
    assert_eq!(safe_url(&old), safe_url(&new));
    assert_ne!(safe_url(&old), safe_url(&other));
}

fn prompt_part(name: &str, text: &str) -> SystemPromptPart {
    SystemPromptPart {
        name: name.into(),
        text: text.into(),
        cacheable: Some(true),
        cache_scope: None,
    }
}

#[test]
fn saved_base_instructions_preserve_text_and_metadata_without_freezing_role_parts() {
    let original = vec![
        prompt_part("base", "Saved instruction body."),
        prompt_part("env", "Saved environment."),
        prompt_part("agent:worker", "Old role."),
        prompt_part("agent:worker:critical", "Old reminder."),
        prompt_part("collaboration-mode", "Old mode."),
    ];
    let saved = base_instructions(&original);
    assert_eq!(saved, original[..2]);
    let mut metadata = RuntimeObject::new();
    metadata.insert("version", runtime_codec::encode_value(&5_u32).unwrap());
    metadata.insert(
        "baseInstructions",
        runtime_codec::encode_value(&saved).unwrap(),
    );
    metadata.insert(
        "collaborationOrigin",
        runtime_codec::encode_value(&SubagentCollaborationOrigin::capture(
            crate::planning::CollaborationMode::Plan,
        ))
        .unwrap(),
    );
    let current = vec![
        prompt_part("base", "Current template."),
        prompt_part("env", "Current environment."),
        prompt_part("agent:worker", "Current role."),
        prompt_part("agent:worker:critical", "Current reminder."),
    ];
    let encoded = crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(metadata));
    let value = crate::runtime_values::runtime_json::parse(&encoded).unwrap();
    let (origin, actual) = restore_instruction_parts(
        &value,
        &crate::schemas::config::default_config(),
        current.clone(),
    )
    .unwrap();
    assert!(origin.is_some());
    assert_eq!(&actual[..2], saved.as_slice());
    assert_eq!(&actual[2..4], &current[2..]);
    assert_eq!(actual[4].name, "collaboration-mode");
}

#[test]
fn historical_digest_without_instruction_body_uses_the_current_template() {
    for version in 2..=4 {
        let mut metadata = RuntimeObject::new();
        metadata.insert("version", runtime_codec::encode_value(&version).unwrap());
        metadata.insert(
            "systemPromptDigest",
            RuntimeJson::String("unrecoverable-old-digest".into()),
        );
        let current = vec![
            prompt_part("base", "Current model template."),
            prompt_part("agent:worker", "Current role."),
        ];
        let (origin, parts) = restore_instruction_parts(
            &RuntimeJson::Object(metadata),
            &crate::schemas::config::default_config(),
            current.clone(),
        )
        .unwrap();
        assert!(origin.is_none());
        assert_eq!(parts, current);
    }
}

#[test]
fn saved_instruction_body_cannot_reintroduce_stale_role_or_collaboration_parts() {
    for name in ["agent:worker", "agent:worker:memory", "collaboration-mode"] {
        let mut metadata = RuntimeObject::new();
        metadata.insert(
            "baseInstructions",
            runtime_codec::encode_value(&vec![prompt_part(name, "Stale body.")]).unwrap(),
        );
        assert!(restore_instruction_parts(
            &RuntimeJson::Object(metadata),
            &crate::schemas::config::default_config(),
            Vec::new()
        )
        .is_err());
    }
}

#[test]
fn stored_effort_is_overlaid_by_current_role_and_determines_default_mode() {
    use crate::schemas::effort::{EffortLevel, EffortValue};
    let mut metadata = RuntimeObject::new();
    metadata.insert("effort", RuntimeJson::String("low".into()));
    metadata.insert("ultraMode", RuntimeJson::Bool(false));
    let mut config = crate::schemas::config::default_config();
    config.effort = Some(EffortValue::Level(EffortLevel::Medium));
    config.ultra_mode = true;
    let definition = AgentDefinition {
        effort: Some(EffortValue::Level(EffortLevel::High)),
        ..Default::default()
    };
    assert_eq!(
        restore_effort(
            &RuntimeJson::Object(metadata.clone()),
            &config,
            Some(&definition)
        )
        .unwrap(),
        (definition.effort.clone(), false)
    );
    assert_eq!(
        restore_effort(&RuntimeJson::Object(metadata.clone()), &config, None).unwrap(),
        (Some(EffortValue::Level(EffortLevel::Low)), false)
    );
    metadata.insert("effort", RuntimeJson::String("ultra".into()));
    assert_eq!(
        restore_effort(&RuntimeJson::Object(metadata.clone()), &config, None).unwrap(),
        (Some(EffortValue::Level(EffortLevel::Ultra)), true)
    );
    metadata.remove("ultraMode");
    assert_eq!(
        restore_effort(&RuntimeJson::Object(metadata), &config, None).unwrap(),
        (Some(EffortValue::Level(EffortLevel::Ultra)), true)
    );
    let low_role = AgentDefinition {
        effort: Some(EffortValue::Level(EffortLevel::Low)),
        ..Default::default()
    };
    let value = crate::runtime_values::runtime_json::parse(
        &r#"{"effort":"ultra","ultraMode":true}"#.into(),
    )
    .unwrap();
    assert_eq!(
        restore_effort(&value, &config, Some(&low_role)).unwrap(),
        (Some(EffortValue::Level(EffortLevel::Low)), false)
    );
    let missing =
        crate::runtime_values::runtime_json::parse(&r#"{"effort":null,"ultraMode":true}"#.into())
            .unwrap();
    assert_eq!(
        restore_effort(&missing, &config, None).unwrap(),
        (None, false)
    );
}

#[test]
fn full_history_instruction_payload_survives_both_recovery_recipe_versions() {
    let frozen = vec![
        prompt_part("base", "Frozen base."),
        prompt_part("env", "Old worker environment."),
        prompt_part("agent:bounded", "Frozen inherited role."),
    ];
    for version in [4_u32, 5_u32] {
        let mut metadata = RuntimeObject::new();
        metadata.insert("version", runtime_codec::encode_value(&version).unwrap());
        metadata.insert("inForkChild", RuntimeJson::Bool(true));
        metadata.insert(
            "systemPromptParts",
            runtime_codec::encode_value(&frozen).unwrap(),
        );
        if version == 5 {
            metadata.insert(
                "baseInstructions",
                runtime_codec::encode_value(&base_instructions(&frozen)).unwrap(),
            );
        }
        let value = RuntimeJson::Object(metadata);
        let mut inherited = stored_fork_instructions(&value).unwrap().unwrap();
        inherited[1] = prompt_part("env", "Current sender environment.");
        let (_, actual) =
            restore_instruction_parts(&value, &crate::schemas::config::default_config(), inherited)
                .unwrap();
        assert_eq!(actual[0], frozen[0]);
        assert_eq!(actual[1].text, "Current sender environment.");
        assert_eq!(actual[2], frozen[2]);
    }
    let mut malformed = RuntimeObject::new();
    malformed.insert("inForkChild", RuntimeJson::Bool(false));
    malformed.insert(
        "systemPromptParts",
        runtime_codec::encode_value(&frozen).unwrap(),
    );
    assert!(stored_fork_instructions(&RuntimeJson::Object(malformed.clone())).is_err());
    malformed.insert("inForkChild", RuntimeJson::Bool(true));
    malformed.insert("systemPromptParts", RuntimeJson::String("invalid".into()));
    assert!(stored_fork_instructions(&RuntimeJson::Object(malformed)).is_err());
}

struct EvidenceTool {
    identity: Option<&'static str>,
    readonly: bool,
    schema_version: u32,
    server: Option<&'static str>,
}
#[async_trait::async_trait]
impl crate::types::tool::Tool for EvidenceTool {
    fn static_name(&self) -> &str {
        "same_name"
    }
    fn static_description(&self) -> &str {
        "Current adapter fixture."
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        self.identity.map(RuntimeString::from)
    }
    fn input_schema(&self) -> RuntimeJson {
        let mut fields = RuntimeObject::new();
        fields.insert(
            "version",
            runtime_codec::encode_value(&self.schema_version).unwrap(),
        );
        RuntimeJson::Object(fields)
    }
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.server.map(RuntimeString::from)
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &RuntimeJson) -> bool {
        true
    }
    fn is_read_only(&self, _: &RuntimeJson) -> bool {
        self.readonly
    }
    async fn call(
        &self,
        _: RuntimeJson,
        _: &crate::types::tool::ToolContext,
        _: Option<crate::types::tool::ToolProgress>,
    ) -> Result<crate::types::tool::ToolResult, crate::types::errors::ToolError> {
        unreachable!("declaration tests do not execute adapters")
    }
}

fn fixture_tool(
    identity: Option<&'static str>,
    readonly: bool,
    schema_version: u32,
    server: Option<&'static str>,
) -> AnyTool {
    Arc::new(EvidenceTool {
        identity,
        readonly,
        schema_version,
        server,
    })
}

#[test]
fn current_native_and_mcp_adapters_rebuild_without_old_binary_schema_or_readonly_hashes() {
    let legacy = crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#"[{"name":"same_name","identity":null,"version":"old-binary","schema":"old-schema","readonly":[true,true],"browserDependency":{"unknown":true}}]"#)).unwrap();
    for current in [
        fixture_tool(Some("new-native-source"), false, 2, None),
        fixture_tool(None, false, 3, Some("new-server")),
    ] {
        let tools = recover_tools(
            legacy.as_array().unwrap(),
            vec![current.clone()],
            None,
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].input_schema(), current.input_schema());
        assert!(!tools[0].is_read_only(&RuntimeJson::Null));
    }
    let declaration =
        tool_declaration(&fixture_tool(Some("old-native-source"), true, 1, None)).unwrap();
    for field in [
        "identity",
        "version",
        "schema",
        "readonly",
        "browserDependency",
    ] {
        assert!(declaration.get(field).is_none());
    }
    assert_eq!(
        declaration.get("source").and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("native"))
    );
}

#[test]
fn dynamic_declaration_survives_roundtrip_while_execution_authority_comes_from_current_adapter() {
    let original = fixture_tool(None, true, 1, None);
    let declaration = tool_declaration(&original).unwrap();
    let encoded = crate::runtime_values::runtime_json::stringify(&RuntimeJson::Array(vec![
        declaration.clone(),
    ]));
    let decoded = crate::runtime_values::runtime_json::parse(&encoded).unwrap();
    let current = fixture_tool(None, false, 2, None);
    let tools = recover_tools(decoded.as_array().unwrap(), vec![current], None, &[], &[]).unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].input_schema(), original.input_schema());
    assert!(!tools[0].is_read_only(&RuntimeJson::Null));
    assert_eq!(tool_declaration(&tools[0]).unwrap(), declaration);
    assert!(recover_tools(decoded.as_array().unwrap(), Vec::new(), None, &[], &[]).is_err());
    assert!(recover_tools(
        decoded.as_array().unwrap(),
        vec![fixture_tool(Some("native"), true, 1, None)],
        None,
        &[],
        &[]
    )
    .is_err());
}

#[test]
fn current_role_and_disabled_filters_apply_to_rebuilt_tools_and_dynamic_declarations() {
    let tool = fixture_tool(None, true, 1, None);
    let declaration = tool_declaration(&tool).unwrap();
    let saved = vec![declaration];
    assert!(
        recover_tools(&saved, vec![tool.clone()], Some(&[]), &[], &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        recover_tools(&saved, vec![tool.clone()], None, &["same_name".into()], &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        recover_tools(&saved, vec![tool.clone()], None, &[], &["same_name".into()])
            .unwrap()
            .is_empty()
    );
    let native = tool_declaration(&fixture_tool(Some("native"), true, 1, None)).unwrap();
    assert!(recover_tools(&[native], vec![tool], None, &[], &[])
        .unwrap()
        .is_empty());
}

#[test]
fn browser_dependencies_are_rebuilt_from_current_grants_and_mapping() {
    let mut mapping = crate::browser::BrowserHostToolMapping::for_mcp_server("selected");
    mapping.tabs = "same_name".into();
    let grants = crate::tools::builtins::browser::create_browser_tools()
        .into_iter()
        .filter(|tool| tool.name() == "browser_tabs")
        .collect::<Vec<_>>();
    let backend = fixture_tool(None, false, 2, Some("current-server"));
    let mut available = grants.clone();
    available.push(backend);
    let saved = grants
        .iter()
        .map(tool_declaration)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut tools = recover_tools(
        &saved,
        available.clone(),
        Some(&["browser_tabs".into()]),
        &[],
        &[],
    )
    .unwrap();
    crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
        &mut tools,
        available.clone(),
        Some(&mapping),
        &[],
        &[],
    );
    let dependency = tools
        .iter()
        .find(|tool| tool.name() == "same_name")
        .unwrap();
    assert_eq!(
        dependency.exposure(),
        crate::types::tool::ToolExposure::Hidden
    );
    assert_eq!(
        dependency.browser_dependency_owners(),
        vec![RuntimeString::from("browser_tabs")]
    );
    assert_eq!(
        dependency.browser_dependency_authorization(None, &RuntimeJson::Null),
        Some(false)
    );
    let mut tools = grants;
    crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
        &mut tools,
        available,
        Some(&mapping),
        &[],
        &["same_name".into()],
    );
    assert!(!tools.iter().any(|tool| tool.name() == "same_name"));
}

fn snapshot_runtime_state() -> crate::engine::session::RuntimePolicyState {
    crate::engine::session::RuntimePolicyState {
        approval_policy: Some(crate::types::permission::ApprovalPolicy::Never),
        sandbox_mode: Some(crate::types::permission::SandboxMode::WorkspaceWrite),
        approvals_reviewer: Some(crate::types::permission::ApprovalsReviewer::User),
        generation: 7,
        sandbox_workspace_write: crate::schemas::config::SandboxWorkspaceWrite {
            writable_roots: vec!["/workspace/retained".into()],
            network_access: true,
            exclude_tmpdir_env_var: true,
            exclude_slash_tmp: false,
        },
        network_allowed: true,
    }
}

#[test]
fn runtime_creation_metadata_round_trips_canonical_authority_without_retired_controls() {
    let state = snapshot_runtime_state();
    let mut fields = RuntimeObject::new();
    fields.insert("version", runtime_codec::encode_value(&5_u32).unwrap());
    encode_runtime_policy_state(&mut fields, &state).unwrap();
    for key in [
        "sandbox",
        "requireRealSandbox",
        "sandboxRetryOnDenial",
        "permissionMode",
        "runtimePermissionMode",
    ] {
        assert!(!fields.contains_key(key));
    }
    let encoded = crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(fields));
    let decoded = crate::runtime_values::runtime_json::parse(&encoded).unwrap();
    let restored =
        decode_runtime_policy_state(&decoded, &crate::schemas::config::default_config(), &state)
            .unwrap();
    assert_eq!(restored, state);
}

#[test]
fn legacy_runtime_metadata_migrates_original_roots_and_actual_network_ceiling() {
    let mut config = crate::schemas::config::default_config();
    config.sandbox_workspace_write = snapshot_runtime_state().sandbox_workspace_write;
    let current = snapshot_runtime_state();
    for (mode, recorded, expected) in [
        ("read-only", true, false),
        ("workspace-write", false, false),
        ("danger-full-access", false, true),
    ] {
        let value = crate::runtime_values::runtime_json::parse(&RuntimeString::from(format!(
            r#"{{"version":2,"sandbox":"seatbelt","sandboxMode":"{mode}","requireRealSandbox":true,"sandboxRetryOnDenial":true,"networkAllowed":{recorded}}}"#
        ))).unwrap();
        let captured = decode_runtime_policy_state(&value, &config, &current).unwrap();
        assert_eq!(captured.network_allowed, expected);
        assert_eq!(
            captured.sandbox_workspace_write,
            config.sandbox_workspace_write
        );
        assert_eq!(
            captured.sandbox_mode,
            Some(
                crate::types::permission::SandboxMode::decode_runtime(&RuntimeJson::String(
                    mode.into()
                ))
                .unwrap()
            )
        );
    }
}

#[test]
fn incomplete_or_invalid_runtime_migration_does_not_fabricate_authority() {
    let config = crate::schemas::config::default_config();
    let current = snapshot_runtime_state();
    for raw in [
        r#"{"version":2,"sandbox":"unknown","requireRealSandbox":true,"sandboxRetryOnDenial":false,"networkAllowed":true}"#,
        r#"{"version":2,"sandbox":"process","requireRealSandbox":true,"networkAllowed":true}"#,
        r#"{"version":3,"networkAllowed":true}"#,
        r#"{"version":4,"networkAllowed":true}"#,
    ] {
        let value = crate::runtime_values::runtime_json::parse(&raw.into()).unwrap();
        assert!(decode_runtime_policy_state(&value, &config, &current).is_err());
    }
}

#[test]
fn restoring_legacy_configuration_normalizes_retired_controls_and_preserves_limits() {
    let current = crate::schemas::config::default_config();
    let value = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"config":{"sandbox":"process","sandboxNetworkAllowed":true,"additionalWritableRoots":["/workspace/original"],"requireRealSandbox":false,"sandboxRetryOnDenial":true},"credentialBindings":[]}"#
    )).unwrap();
    let restored = restore_config(&value, &current).unwrap();
    assert_eq!(
        restored.sandbox_mode,
        Some(crate::types::permission::SandboxMode::DangerFullAccess)
    );
    assert_eq!(
        restored.sandbox_workspace_write.writable_roots,
        vec![RuntimeString::from("/workspace/original")]
    );
    assert!(restored.sandbox_workspace_write.network_access);
    let encoded = runtime_codec::encode_value(&restored).unwrap();
    for key in [
        "sandbox",
        "requireRealSandbox",
        "sandboxRetryOnDenial",
        "additionalWritableRoots",
    ] {
        assert!(encoded.get(key).is_none());
    }
}

#[test]
fn legacy_relative_workspace_roots_restore_against_the_saved_worker_directory() {
    let cwd = std::env::temp_dir().join("captured-worker-directory");
    let mut value = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"version":2,"sandbox":"seatbelt","sandboxMode":"workspace-write","requireRealSandbox":true,"sandboxRetryOnDenial":false,"networkAllowed":false,"config":{"sandbox":"seatbelt","additionalWritableRoots":["../shared",".","nested/../retained"]},"credentialBindings":[]}"#
    )).unwrap();
    value.as_object_mut().unwrap().insert(
        "cwd",
        runtime_codec::encode_value(&RuntimePath::from(cwd.to_str().unwrap())).unwrap(),
    );
    let restored = restore_config(&value, &crate::schemas::config::default_config()).unwrap();
    assert_eq!(
        restored.sandbox_workspace_write.writable_roots,
        vec![
            RuntimeString::from(cwd.parent().unwrap().join("shared").to_str().unwrap()),
            RuntimeString::from(cwd.to_str().unwrap()),
            RuntimeString::from(cwd.join("retained").to_str().unwrap()),
        ]
    );
    let mut current = snapshot_runtime_state();
    let retained = RuntimeString::from(cwd.join("retained/narrow").to_str().unwrap());
    current.sandbox_workspace_write.writable_roots = vec![
        retained.clone(),
        RuntimeString::from(
            cwd.parent()
                .unwrap()
                .join("fresh-parent-root")
                .to_str()
                .unwrap(),
        ),
    ];
    let captured = decode_runtime_policy_state(&value, &restored, &current).unwrap();
    let effective = crate::engine::session::intersect_subagent_runtime_state(&captured, &current);
    assert_eq!(
        effective.sandbox_workspace_write.writable_roots,
        vec![RuntimeString::from(
            std::fs::canonicalize(std::env::temp_dir())
                .unwrap()
                .join("captured-worker-directory/retained/narrow")
                .to_str()
                .unwrap()
        )]
    );
    value.as_object_mut().unwrap().remove("cwd");
    assert!(restore_config(&value, &crate::schemas::config::default_config()).is_err());
    value
        .as_object_mut()
        .unwrap()
        .insert("cwd", RuntimeJson::String("relative-worker".into()));
    assert!(restore_config(&value, &crate::schemas::config::default_config()).is_err());
}

#[test]
fn restoring_legacy_ultra_config_preserves_its_original_mode_and_explicit_false() {
    for (source, expected) in [
        (
            r#"{"config":{"effort":"ultra"},"credentialBindings":[]}"#,
            true,
        ),
        (
            r#"{"config":{"effort":"ultra","ultraMode":false},"credentialBindings":[]}"#,
            false,
        ),
    ] {
        let value =
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap();
        let current = crate::schemas::config::default_config();
        let restored = restore_config(&value, &current).unwrap();
        assert_eq!(
            restored.effort,
            Some(crate::schemas::effort::EffortValue::Level(
                crate::schemas::effort::EffortLevel::Ultra
            ))
        );
        assert_eq!(restored.ultra_mode, expected);
    }
}

#[test]
fn legacy_runtime_mode_is_validated_but_not_rewritten_as_authority() {
    let state = snapshot_runtime_state();
    let config = crate::schemas::config::default_config();
    for legacy in [
        "default",
        "plan",
        "acceptEdits",
        "auto",
        "dontAsk",
        "bypassPermissions",
    ] {
        let mut fields = RuntimeObject::new();
        fields.insert("version", runtime_codec::encode_value(&3_u32).unwrap());
        fields.insert("runtimePermissionMode", RuntimeJson::String(legacy.into()));
        encode_runtime_policy_state(&mut fields, &state).unwrap();
        let restored =
            decode_runtime_policy_state(&RuntimeJson::Object(fields), &config, &state).unwrap();
        assert_eq!(restored, state);
        let mut encoded = RuntimeObject::new();
        encode_runtime_policy_state(&mut encoded, &restored).unwrap();
        assert!(!encoded.contains_key("runtimePermissionMode"));
        assert!(!encoded.contains_key("permissionMode"));
    }
}

#[test]
fn historical_role_permission_recipe_remains_decodable_without_recovery_fingerprints() {
    let legacy = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"name":"legacy-reader","description":"Retained role","tools":["read"],"permissionMode":"dontAsk","effort":"high","prompt":"Inspect files."}"#,
    )).unwrap();
    let definition = AgentDefinition::decode_runtime(&legacy).unwrap();
    assert_eq!(definition.legacy_runtime_encodings().unwrap(), vec![legacy]);
    let canonical = runtime_codec::encode_value(&definition).unwrap();
    assert!(canonical.get("permissionMode").is_none());
    assert!(canonical.get("runtimePolicy").is_some());
}

#[test]
fn stale_proxy_credentials_and_unrelated_settings_do_not_block_stored_source_migration() {
    let value = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
        r#"{"config":{"model":"stored-model","modelSource":{"source":"custom","modelId":"stored-entry","selection":"custom:stored-entry"},"networkProxy":{"url":null,"headers":null},"maxOutputTokens":1},"credentialBindings":[{"path":["networkProxy","url"],"url":true,"source":"https://old-proxy.example/"},{"path":["networkProxy","headers"],"url":false}]}"#,
    )).unwrap();
    let current = crate::schemas::config::default_config();
    let restored = restore_config(&value, &current).unwrap();
    assert_eq!(restored.model, "stored-model");
    assert_eq!(
        restored.model_source.unwrap().selection.as_deref(),
        Some("custom:stored-entry")
    );
    assert_eq!(restored.max_output_tokens, current.max_output_tokens);
}

#[tokio::test]
async fn cold_full_history_reload_keeps_parent_role_pool_and_stored_model() {
    use crate::engine::agent::{Agent, AgentOptions, StartSessionOptions};
    use crate::engine::deps::EngineDepsOverrides;
    use crate::runtime_process::RuntimeProcessContext;
    use crate::schemas::effort::{EffortLevel, EffortValue};

    let process =
        RuntimeProcessContext::initialize(crate::testing::shared_test_process_supervisor())
            .unwrap();
    let directory =
        std::env::temp_dir().join(format!("klaus-fork-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let read = crate::tools::builtins::read::create_read_tool();
    let task = crate::tools::builtins::task::create_task_tool(Default::default());
    let root_tools = vec![
        read.clone(),
        task.clone(),
        crate::tools::builtins::write::create_write_tool(),
    ];
    let current_role = AgentDefinition {
        name: "bounded".into(),
        prompt: Some("CURRENT_EXPLICIT_ROLE".into()),
        effort: Some(EffortValue::Level(EffortLevel::High)),
        model: Some("role-model".into()),
        tools: Some(vec!["read".into(), "task".into()]),
        ..Default::default()
    };
    let agent = Agent::new(AgentOptions {
        process_context: process.clone(),
        provider: Arc::new(crate::testing::fake_provider()),
        config: Some(serde_json::json!({
            "model":"sender-model",
            "modelTiers":{"low":"stored-model", "high":"role-model"},
            "effort":"medium",
            "permissionMode":"bypassPermissions",
            "workflow":false,
            "maxSubAgentDepth":1
        })),
        tools: Some(root_tools),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(directory.to_string_lossy().into_owned()),
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: Some(vec![current_role]),
        include_default_agents: Some(false),
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    });
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    assert!(session
        .capture_tool_generation()
        .0
        .iter()
        .any(|tool| tool.name() == "write"));
    let resume = session.ordinary_resume_context(None).unwrap();
    assert_eq!(resume.bundle.config.model, "sender-model");
    let mut stored_config = resume.bundle.config.clone();
    stored_config.model = "stored-model".into();
    stored_config.effort = Some(EffortValue::Level(EffortLevel::Low));
    stored_config.ultra_mode = false;
    let (safe, bindings) = config_metadata(&stored_config).unwrap();
    let mut metadata = RuntimeObject::new();
    macro_rules! field {
        ($key:literal, $value:expr) => {
            metadata.insert($key, runtime_codec::encode_value(&$value).unwrap());
        };
    }
    field!("version", 5_u32);
    field!("agentType", "bounded");
    field!("providerName", resume.bundle.provider.info().name);
    metadata.insert("config", safe);
    metadata.insert("credentialBindings", bindings);
    field!(
        "tools",
        vec![
            tool_declaration(&read).unwrap(),
            tool_declaration(&task).unwrap()
        ]
    );
    field!("effort", EffortValue::Level(EffortLevel::Low));
    field!("ultraMode", false);
    field!("cwd", resume.cwd);
    field!("depth", 1_u32);
    field!("inForkChild", false);
    field!("forkSeedIds", Vec::<RuntimeString>::new());
    field!("taskListId", session.task_list_id());
    for key in [
        "name",
        "parentAgentId",
        "spawnTurnId",
        "maxTurns",
        "outputSchema",
        "webSearch",
    ] {
        metadata.insert(key, RuntimeJson::Null);
    }
    for key in ["allowRules", "denyRules", "askRules", "writableRoots"] {
        metadata.insert(key, RuntimeJson::Array(Vec::new()));
    }
    field!("avoidPrompts", false);
    encode_runtime_policy_state(&mut metadata, &resume.runtime_policy.state).unwrap();
    let mut parent = session
        .decode_ordinary_snapshot(&RuntimeJson::Object(metadata), &resume, None)
        .await
        .unwrap();
    assert!(!parent.tools.iter().any(|tool| tool.name() == "write"));
    parent.system_prompt = vec![
        prompt_part("base", "FROZEN_PARENT_BASE"),
        prompt_part("env", "OLD_WORKER_ENVIRONMENT"),
        prompt_part("agent:bounded", "FROZEN_PARENT_ROLE"),
    ];
    parent.effort = Some(EffortValue::Level(EffortLevel::Low));
    parent.config.effort = parent.effort.clone();
    parent.agent_type = Some(crate::engine::fork::FORK_AGENT_TYPE.into());
    parent.definition = None;
    parent.in_fork_child = true;
    parent.depth = 3;
    parent.parent_agent_id = Some("bounded-parent".into());
    parent.name = Some("grandchild".into());
    assert!(!parent.tools.iter().any(|tool| tool.name() == "write"));
    let frozen = parent.encode(&None).unwrap();
    assert!(stored_fork_instructions(&frozen).unwrap().is_some());
    let restored = session
        .decode_ordinary_snapshot(&frozen, &resume, None)
        .await
        .unwrap();
    assert_eq!(restored.config.model, "stored-model");
    assert_eq!(restored.effort, Some(EffortValue::Level(EffortLevel::Low)));
    assert!(!restored.ultra_mode);
    assert_eq!(restored.cwd, resume.cwd);
    for name in ["read", "task"] {
        assert!(restored.tools.iter().any(|tool| tool.name() == name));
    }
    assert!(!restored.tools.iter().any(|tool| tool.name() == "write"));
    assert!(restored
        .system_prompt
        .iter()
        .any(|part| part.text == "FROZEN_PARENT_ROLE"));
    assert!(!restored
        .system_prompt
        .iter()
        .any(|part| part.text == "CURRENT_EXPLICIT_ROLE"));
    assert!(!restored
        .system_prompt
        .iter()
        .any(|part| part.text == "OLD_WORKER_ENVIRONMENT"));

    let mut explicit = frozen;
    explicit
        .as_object_mut()
        .unwrap()
        .insert("agentType", RuntimeJson::String("bounded".into()));
    let refreshed_role = session
        .decode_ordinary_snapshot(&explicit, &resume, None)
        .await
        .unwrap();
    assert_eq!(refreshed_role.config.model, "stored-model");
    assert_eq!(
        refreshed_role.effort,
        Some(EffortValue::Level(EffortLevel::High))
    );
    assert!(refreshed_role
        .system_prompt
        .iter()
        .any(|part| part.text == "CURRENT_EXPLICIT_ROLE"));
    assert!(!refreshed_role
        .system_prompt
        .iter()
        .any(|part| part.text == "FROZEN_PARENT_ROLE"));
    assert!(!refreshed_role
        .tools
        .iter()
        .any(|tool| tool.name() == "write"));
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
