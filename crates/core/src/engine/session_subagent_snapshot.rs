//! Recover persistent worker metadata against the current query's runtime services.

#[cfg(test)]
#[path = "session_subagent_snapshot_tests.rs"]
mod tests;

use super::*;
fn credential_key(key: &RuntimeString) -> bool {
    let key = key
        .try_to_utf8()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .replace('-', "_");
    matches!(
        key.as_str(),
        "api_key"
            | "apikey"
            | "api_token"
            | "access_token"
            | "accesstoken"
            | "refresh_token"
            | "refreshtoken"
            | "password"
            | "passwd"
            | "authorization"
            | "cookie"
            | "client_secret"
            | "clientsecret"
            | "private_key"
            | "secret"
            | "credentials"
            | "headers"
            | "env"
            | "environment"
    ) || key.ends_with("_password")
        || key.ends_with("_secret")
}

fn safe_url(value: &RuntimeString) -> Option<RuntimeString> {
    let host = value.try_to_utf8().ok()?;
    let mut url = url::Url::parse(&host).ok()?;
    if !matches!(url.scheme(), "http" | "https" | "socks" | "socks5") {
        return None;
    }
    let mut changed = !url.username().is_empty() || url.password().is_some();
    if changed {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    let pairs = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if pairs
        .iter()
        .any(|(key, _)| credential_key(&RuntimeString::from(key.clone())))
    {
        changed = true;
        url.set_query(None);
        for (key, value) in pairs {
            if !credential_key(&RuntimeString::from(key.clone())) {
                url.query_pairs_mut().append_pair(&key, &value);
            }
        }
    }
    changed.then(|| url.to_string().into())
}

fn scrub(
    value: &RuntimeJson,
    path: &mut Vec<RuntimeString>,
    bindings: &mut Vec<RuntimeJson>,
) -> RuntimeJson {
    match value {
        RuntimeJson::Object(object) => {
            let mut safe = RuntimeObject::new();
            for (key, child) in object.entries() {
                path.push(key.clone());
                if credential_key(&key) {
                    let mut binding = RuntimeObject::new();
                    binding.insert(
                        "path",
                        RuntimeJson::Array(path.iter().cloned().map(RuntimeJson::String).collect()),
                    );
                    binding.insert("url", RuntimeJson::Bool(false));
                    bindings.push(RuntimeJson::Object(binding));
                    safe.insert(key.clone(), RuntimeJson::Null);
                } else {
                    safe.insert(key.clone(), scrub(child, path, bindings));
                }
                path.pop();
            }
            RuntimeJson::Object(safe)
        }
        RuntimeJson::Array(values) => RuntimeJson::Array(
            values
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    path.push(index.to_string().into());
                    let safe = scrub(child, path, bindings);
                    path.pop();
                    safe
                })
                .collect(),
        ),
        RuntimeJson::String(text) => {
            if let Some(safe) = safe_url(text) {
                let mut binding = RuntimeObject::new();
                binding.insert(
                    "path",
                    RuntimeJson::Array(path.iter().cloned().map(RuntimeJson::String).collect()),
                );
                binding.insert("url", RuntimeJson::Bool(true));
                binding.insert("source", RuntimeJson::String(safe.clone()));
                bindings.push(RuntimeJson::Object(binding));
                RuntimeJson::String(safe)
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    }
}

fn config_metadata(config: &ResolvedConfig) -> Result<(RuntimeJson, RuntimeJson), RuntimeString> {
    let value = runtime_codec::encode_value(config)
        .map_err(|error| RuntimeString::from(error.to_string()))?;
    let mut bindings = Vec::new();
    let safe = scrub(&value, &mut Vec::new(), &mut bindings);
    Ok((safe, RuntimeJson::Array(bindings)))
}

fn restore_config(
    value: &RuntimeJson,
    _current: &ResolvedConfig,
) -> Result<ResolvedConfig, RuntimeString> {
    let safe = value
        .get("config")
        .cloned()
        .ok_or_else(|| RuntimeString::from("Missing safe config metadata."))?;
    let bindings = value
        .get("credentialBindings")
        .and_then(RuntimeJson::as_array)
        .ok_or_else(|| RuntimeString::from("Missing credential binding metadata."))?;
    for binding in bindings {
        let path: Vec<RuntimeString> = required(binding, "path")?;
        if path.is_empty() {
            return Err("Invalid credential binding in creation metadata.".into());
        }
        if required::<bool>(binding, "url")? {
            let _: RuntimeString = required(binding, "source")?;
        }
    }
    // Configuration uses scalar serde integers; preserve their JSON number shape at this boundary.
    let serialized = crate::runtime_values::runtime_json::stringify(&safe)
        .try_to_utf8()
        .map_err(|error| RuntimeString::from(error.to_string()))?;
    let mut scalar: serde_json::Value = serde_json::from_str(&serialized)
        .map_err(|error| RuntimeString::from(error.to_string()))?;
    let object = scalar
        .as_object_mut()
        .ok_or_else(|| RuntimeString::from("Original configuration must be an object."))?;
    for warning in
        crate::schemas::config::normalize_sandbox_config(object).map_err(RuntimeString::from)?
    {
        tracing::warn!(code = %warning.code, keys = ?warning.keys, "{}", warning.message);
    }
    crate::schemas::config::normalize_legacy_ultra_fields(object);
    normalize_restored_workspace_roots(value, object)?;
    // Only model selection and historical authority feed cold recovery. Retired
    // proxy credentials and unrelated settings cannot override the current sender.
    object.retain(|key, _| {
        matches!(
            key.as_str(),
            "model"
                | "subagentModelCatalog"
                | "subagentModelOptions"
                | "modelSource"
                | "effort"
                | "ultraMode"
                | "approvalPolicy"
                | "approval_policy"
                | "sandboxMode"
                | "sandbox_mode"
                | "approvalsReviewer"
                | "approvals_reviewer"
                | "sandboxWorkspaceWrite"
                | "sandbox_workspace_write"
        )
    });
    crate::runtime_values::ConfigJson::validate_config_document(scalar)
        .map_err(|error| RuntimeString::from(error.to_string()))?
        .deserialize_config()
        .map_err(|error| RuntimeString::from(error.to_string()))
}

fn normalize_restored_workspace_roots(
    value: &RuntimeJson,
    config: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), RuntimeString> {
    let cwd: Option<RuntimePath> = optional(value, "cwd")?;
    let cwd = cwd
        .map(|cwd| {
            let text = cwd
                .as_runtime_string()
                .try_to_utf8()
                .map_err(|_| RuntimeString::from("Invalid original worker cwd."))?;
            let native = std::path::PathBuf::from(text);
            if !native.is_absolute() {
                return Err(RuntimeString::from("Original worker cwd must be absolute."));
            }
            Ok(native)
        })
        .transpose()?;
    crate::schemas::config::normalize_sandbox_workspace_roots(config, cwd.as_deref())
        .map_err(RuntimeString::from)
}

fn is_base_instruction(part: &SystemPromptPart) -> bool {
    !part.name.starts_with("agent:") && part.name != "collaboration-mode"
}

fn base_instructions(parts: &[SystemPromptPart]) -> Vec<SystemPromptPart> {
    parts
        .iter()
        .filter(|part| is_base_instruction(part))
        .cloned()
        .collect()
}

fn tool_source(tool: &AnyTool) -> &'static str {
    if tool.mcp_server().is_some() {
        "mcp"
    } else if tool.persistence_identity().is_some() && !tool.task_scope_host_capability() {
        "native"
    } else {
        "dynamic"
    }
}

fn tool_declaration(tool: &AnyTool) -> Result<RuntimeJson, RuntimeString> {
    let mut fields = RuntimeObject::new();
    fields.insert("name", RuntimeJson::String(tool.name()));
    fields.insert("source", RuntimeJson::from(tool_source(tool)));
    if tool_source(tool) == "dynamic" {
        fields.insert("description", RuntimeJson::String(tool.description()));
        fields.insert("inputSchema", tool.input_schema());
        fields.insert(
            "inputKind",
            runtime_codec::encode_value(&tool.input_kind())
                .map_err(|error| RuntimeString::from(error.to_string()))?,
        );
        fields.insert(
            "outputSchema",
            runtime_codec::encode_value(&tool.output_schema())
                .map_err(|error| RuntimeString::from(error.to_string()))?,
        );
        fields.insert(
            "exposure",
            runtime_codec::encode_value(&tool.exposure())
                .map_err(|error| RuntimeString::from(error.to_string()))?,
        );
    }
    Ok(RuntimeJson::Object(fields))
}

struct RestoredDynamicTool {
    current: AnyTool,
    description: RuntimeString,
    input_schema: RuntimeJson,
    input_kind: crate::types::tool::ToolInputKind,
    output_schema: Option<RuntimeJson>,
    exposure: crate::types::tool::ToolExposure,
}

#[async_trait::async_trait]
impl crate::types::tool::Tool for RestoredDynamicTool {
    fn name(&self) -> RuntimeString {
        self.current.name()
    }
    fn description(&self) -> RuntimeString {
        self.description.clone()
    }
    fn input_schema(&self) -> RuntimeJson {
        self.input_schema.clone()
    }
    fn input_kind(&self) -> crate::types::tool::ToolInputKind {
        self.input_kind.clone()
    }
    fn output_schema(&self) -> Option<RuntimeJson> {
        self.output_schema.clone()
    }
    fn exposure(&self) -> crate::types::tool::ToolExposure {
        self.exposure
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        None
    }
    fn task_scope_host_capability(&self) -> bool {
        self.current.task_scope_host_capability()
    }
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        self.current.approval_metadata()
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.current.search_hint()
    }
    fn max_result_size_chars(&self) -> Option<u64> {
        self.current.max_result_size_chars()
    }
    fn is_enabled(&self) -> bool {
        self.current.is_enabled()
    }
    fn is_concurrency_safe(&self, input: &RuntimeJson) -> bool {
        self.current.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &RuntimeJson) -> bool {
        self.current.is_read_only(input)
    }
    fn is_goal_state_control(&self) -> bool {
        self.current.is_goal_state_control()
    }
    fn planning_capability(
        &self,
        input: &RuntimeJson,
    ) -> crate::types::tool::PlanningToolCapability {
        self.current.planning_capability(input)
    }
    fn is_destructive(&self, input: &RuntimeJson) -> Option<bool> {
        self.current.is_destructive(input)
    }
    fn input_schema_property_order(&self, path: &[String]) -> Option<&'static [&'static str]> {
        self.current.input_schema_property_order(path)
    }
    fn preprocess_input(&self, input: &RuntimeJson) -> RuntimeJson {
        self.current.preprocess_input(input)
    }
    fn parse_input(
        &self,
        input: &RuntimeJson,
    ) -> Result<RuntimeJson, crate::types::errors::ToolError> {
        // The declaration is durable; parsing and execution still cross the current adapter.
        self.current.parse_input(input)
    }
    async fn validate_input(
        &self,
        input: &RuntimeJson,
        ctx: &crate::types::tool::ToolContext,
    ) -> Option<crate::types::tool::ValidationResult> {
        self.current.validate_input(input, ctx).await
    }
    async fn check_permissions(
        &self,
        input: &RuntimeJson,
        ctx: &crate::types::tool::ToolContext,
    ) -> Option<crate::types::permission::PermissionResult> {
        self.current.check_permissions(input, ctx).await
    }
    async fn call(
        &self,
        input: RuntimeJson,
        ctx: &crate::types::tool::ToolContext,
        progress: Option<crate::types::tool::ToolProgress>,
    ) -> Result<crate::types::tool::ToolResult, crate::types::errors::ToolError> {
        self.current.call(input, ctx, progress).await
    }
    async fn call_with_id(
        &self,
        id: &RuntimeString,
        input: RuntimeJson,
        ctx: &crate::types::tool::ToolContext,
        progress: Option<crate::types::tool::ToolProgress>,
    ) -> Result<crate::types::tool::ToolResult, crate::types::errors::ToolError> {
        self.current.call_with_id(id, input, ctx, progress).await
    }
    fn summarize(&self, input: &RuntimeJson) -> Option<RuntimeString> {
        self.current.summarize(input)
    }
    fn to_command_string(&self, input: &RuntimeJson) -> Option<RuntimeString> {
        self.current.to_command_string(input)
    }
    fn get_path(&self, input: &RuntimeJson) -> Option<crate::types::tool::PathTargets> {
        self.current.get_path(input)
    }
    fn get_raw_path(&self, input: &RuntimeJson) -> Option<RuntimeJson> {
        self.current.get_raw_path(input)
    }
    fn permission_path_kind(&self) -> Option<crate::types::tool::PermissionPathKind> {
        self.current.permission_path_kind()
    }
}

fn declared_tool_names(declarations: &[RuntimeJson]) -> Result<Vec<RuntimeString>, RuntimeString> {
    declarations
        .iter()
        .map(|value| {
            if let Some(name) = value.as_str() {
                Ok(name.clone())
            } else {
                required::<RuntimeString>(value, "name")
            }
        })
        .collect()
}

fn recover_tools(
    declarations: &[RuntimeJson],
    available: Vec<AnyTool>,
    allowed: Option<&[RuntimeString]>,
    disallowed: &[RuntimeString],
    disabled: &[RuntimeString],
) -> Result<Vec<AnyTool>, RuntimeString> {
    let names = declared_tool_names(declarations)?;
    let mut tools = Vec::new();
    for tool in available {
        let name = tool.name();
        if !tool.is_enabled()
            || name == "advisor"
            || allowed.is_some_and(|allowed| !allowed.contains(&name))
            || disallowed.contains(&name)
            || disabled.contains(&name)
        {
            continue;
        }
        if tool_source(&tool) == "dynamic"
            && !declarations
                .iter()
                .zip(&names)
                .any(|(declaration, declared_name)| {
                    declared_name == &name
                        && declaration.get("source").is_none_or(|source| {
                            source.as_str() == Some(&RuntimeString::from("dynamic"))
                        })
                })
        {
            continue;
        }
        if let Some(declaration) = declarations.iter().find(|value| {
            value.get("name").and_then(RuntimeJson::as_str) == Some(&name)
                && value.get("source").and_then(RuntimeJson::as_str)
                    == Some(&RuntimeString::from("dynamic"))
        }) {
            if tool_source(&tool) != "dynamic" {
                continue;
            }
            tools.push(Arc::new(RestoredDynamicTool {
                current: tool,
                description: required(declaration, "description")?,
                input_schema: required(declaration, "inputSchema")?,
                input_kind: required(declaration, "inputKind")?,
                output_schema: required(declaration, "outputSchema")?,
                exposure: required(declaration, "exposure")?,
            }) as AnyTool);
        } else {
            tools.push(tool);
        }
    }
    // A declaration never grants an executor. Only the current admitted adapters can run it.
    for declaration in declarations.iter().filter(|value| {
        value.get("source").and_then(RuntimeJson::as_str) == Some(&RuntimeString::from("dynamic"))
    }) {
        let name: RuntimeString = required(declaration, "name")?;
        if !disallowed.contains(&name)
            && !disabled.contains(&name)
            && allowed.is_none_or(|allowed| allowed.contains(&name))
            && tools.iter().filter(|tool| tool.name() == name).count() != 1
        {
            return Err(
                RuntimeString::from("The stored dynamic tool has no current adapter: ")
                    .concat(&name),
            );
        }
    }
    Ok(tools)
}

fn restore_effort(
    value: &RuntimeJson,
    _current: &ResolvedConfig,
    definition: Option<&AgentDefinition>,
) -> Result<(Option<crate::schemas::effort::EffortValue>, bool), RuntimeString> {
    let mut effort = required(value, "effort")?;
    let mut ultra_mode: Option<bool> = optional(value, "ultraMode")?;
    crate::schemas::effort::normalize_legacy_ultra(&mut effort, &mut ultra_mode);
    if let Some(role_effort) = definition.and_then(|definition| definition.effort.clone()) {
        effort = Some(role_effort);
    }
    let ultra_mode = effort
        .as_ref()
        .is_some_and(crate::schemas::effort::EffortValue::is_ultra);
    Ok((effort, ultra_mode))
}

fn stored_fork_instructions(
    value: &RuntimeJson,
) -> Result<Option<Vec<SystemPromptPart>>, RuntimeString> {
    let Some(parts) = value.get("systemPromptParts") else {
        return Ok(None);
    };
    if !required::<bool>(value, "inForkChild")? {
        return Err("Stored full-history instructions require a history worker.".into());
    }
    runtime_codec::decode_value(parts)
        .map(Some)
        .map_err(|error| {
            RuntimeString::from(format!("Invalid stored full-history instructions: {error}"))
        })
}

fn restore_instruction_parts(
    value: &RuntimeJson,
    config: &ResolvedConfig,
    current: Vec<SystemPromptPart>,
) -> Result<(Option<SubagentCollaborationOrigin>, Vec<SystemPromptPart>), RuntimeString> {
    let current_environment = value
        .get("systemPromptParts")
        .and_then(|_| current.iter().find(|part| part.name == "env").cloned());
    let mut parts = if let Some(saved) = value.get("baseInstructions") {
        let saved: Vec<SystemPromptPart> = runtime_codec::decode_value(saved).map_err(|error| {
            RuntimeString::from(format!("Invalid stored base instructions: {error}"))
        })?;
        if saved.iter().any(|part| !is_base_instruction(part)) {
            return Err(
                "Stored base instructions must not contain role or collaboration parts.".into(),
            );
        }
        let mut parts = saved;
        parts.extend(
            current
                .into_iter()
                .filter(|part| !is_base_instruction(part)),
        );
        parts
    } else {
        // Historical digests contain no instruction body. Rebuild from today's template.
        current
    };
    let origin = value
        .get("collaborationOrigin")
        .map(SubagentCollaborationOrigin::decode_runtime)
        .transpose()
        .map_err(|error| {
            RuntimeString::from(format!("Invalid original collaboration origin: {error}"))
        })?;
    if let Some(origin) = origin {
        parts = origin.apply(config, parts);
    }
    if let Some(environment) = current_environment {
        if let Some(part) = parts.iter_mut().find(|part| part.name == "env") {
            *part = environment;
        } else {
            parts.push(environment);
        }
    }
    Ok((origin, parts))
}

pub(super) fn decode_bindings(value: &RuntimeJson) -> Result<OrdinaryBindings, RuntimeString> {
    Ok(OrdinaryBindings {
        depth: required(value, "depth")?,
        parent_agent_id: required::<Option<RuntimeString>>(value, "parentAgentId")?
            .map(|id| {
                id.try_to_utf8()
                    .map_err(|error| RuntimeString::from(error.to_string()))
            })
            .transpose()?,
        // Older creation documents have no turn binding; absence must stay unbound.
        spawn_turn_id: if value.get("spawnTurnId").is_some() {
            required(value, "spawnTurnId")?
        } else {
            None
        },
    })
}

fn encode_runtime_policy_state(
    fields: &mut RuntimeObject,
    state: &crate::engine::session::RuntimePolicyState,
) -> Result<(), RuntimeString> {
    macro_rules! field {
        ($key:literal, $value:expr) => {
            fields.insert(
                $key,
                runtime_codec::encode_value(&$value)
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            );
        };
    }
    field!("approvalPolicy", state.approval_policy);
    field!("sandboxMode", state.sandbox_mode);
    field!("approvalsReviewer", state.approvals_reviewer);
    field!("sandboxWorkspaceWrite", state.sandbox_workspace_write);
    field!("networkAllowed", state.network_allowed);
    Ok(())
}

impl SubagentCreationSnapshot {
    pub(super) fn encode(
        &self,
        spawn_turn_id: &Option<RuntimeString>,
    ) -> Result<RuntimeJson, RuntimeString> {
        let mut fields = RuntimeObject::new();
        macro_rules! field {
            ($key:literal, $value:expr) => {
                fields.insert(
                    $key,
                    runtime_codec::encode_value(&$value)
                        .map_err(|error| RuntimeString::from(error.to_string()))?,
                );
            };
        }
        field!("version", 6_u32);
        field!("name", self.name);
        field!("agentType", self.agent_type);
        field!("baseInstructions", base_instructions(&self.system_prompt));
        fields.insert(
            "mcpInstructionSources",
            self.mcp_runtime.instruction_evidence(),
        );
        field!("providerName", self.bundle.provider.info().name);
        fields.insert(
            "tools",
            RuntimeJson::Array(
                self.tools
                    .iter()
                    .map(tool_declaration)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
        let (config, bindings) = config_metadata(&self.config)?;
        fields.insert("config", config);
        fields.insert("credentialBindings", bindings);
        if self.in_fork_child {
            field!("systemPromptParts", self.system_prompt);
        }
        if let Some(origin) = &self.collaboration_origin {
            field!("collaborationOrigin", origin);
        }
        field!("cwd", self.cwd);
        field!("maxTurns", self.max_turns);
        field!("effort", self.effort);
        field!("ultraMode", self.ultra_mode);
        field!("outputSchema", self.output_schema);
        field!("webSearch", self.web_search);
        field!("inForkChild", self.in_fork_child);
        // The child owns the exact inherited model context, including runtime
        // messages and placeholder results that never existed in the parent file.
        field!("forkHistory", self.fork_seed);
        field!(
            "forkSeedIds",
            self.fork_seed
                .iter()
                .map(crate::engine::session::message_uuid)
                .cloned()
                .collect::<Vec<_>>()
        );
        field!("depth", self.depth);
        field!("spawnTurnId", spawn_turn_id);
        field!("parentAgentId", self.parent_agent_id);
        field!("taskListId", self.task_list_id);
        field!(
            "builtinIdentity",
            self.definition
                .as_ref()
                .and_then(|definition| definition.builtin_identity.clone())
        );
        field!("runtimeCapabilityCapture", self.runtime_capabilities);
        if let Some(origin) = &self.permission_origin {
            field!("permissionOrigin", origin);
        }
        field!("allowRules", self.permission.allow_rules);
        field!("denyRules", self.permission.deny_rules);
        field!("askRules", self.permission.ask_rules);
        field!("writableRoots", self.permission.additional_writable_roots);
        field!("avoidPrompts", self.permission.should_avoid_prompts);
        encode_runtime_policy_state(&mut fields, &self.runtime_policy.state)?;
        if self.permission.pinned_memory_root.is_some()
            || !self.permission.internal_writable_roots.is_empty()
        {
            return Err(
                "Internal maintenance authority is not an ordinary worker capability.".into(),
            );
        }
        Ok(RuntimeJson::Object(fields))
    }
}

fn optional<T: RuntimeDecode>(value: &RuntimeJson, key: &str) -> Result<Option<T>, RuntimeString> {
    runtime_codec::decode_value(value.get(key).unwrap_or(&RuntimeJson::Null)).map_err(|error| {
        RuntimeString::from(format!(
            "Invalid sub-agent creation metadata {key}: {error}"
        ))
    })
}

fn decode_runtime_policy_state(
    value: &RuntimeJson,
    config: &ResolvedConfig,
    current: &crate::engine::session::RuntimePolicyState,
) -> Result<crate::engine::session::RuntimePolicyState, RuntimeString> {
    let version: u32 = required(value, "version")?;
    if !matches!(version, 2..=6) {
        return Err("Unsupported or incomplete runtime authority metadata.".into());
    }
    let legacy_keys = ["sandbox", "requireRealSandbox", "sandboxRetryOnDenial"];
    let has_legacy = legacy_keys.iter().any(|key| value.get(*key).is_some());
    if version == 2 || has_legacy {
        let backend: RuntimeString = required(value, "sandbox")?;
        if ![
            "process",
            "auto",
            "seatbelt",
            "bubblewrap",
            "native-helper",
            "windows-helper",
            "windows-restricted-token",
        ]
        .iter()
        .any(|name| backend == *name)
        {
            return Err("Invalid legacy sandbox backend in creation metadata.".into());
        }
        let _: bool = required(value, "requireRealSandbox")?;
        let _: bool = required(value, "sandboxRetryOnDenial")?;
        tracing::warn!(
            code = "legacySubagentSandboxMigrated",
            "Legacy worker sandbox metadata was migrated to canonical policy and intersected with current authority; retired backend and retry controls no longer select execution behavior."
        );
    }
    // Historical modes are validated at the boundary and never become runtime authority.
    if version == 3 {
        let _: crate::types::permission::ExternalPermissionMode =
            required(value, "runtimePermissionMode")?;
    }
    let captured_mode = optional(value, "sandboxMode")?;
    let recorded_network = required(value, "networkAllowed")?;
    let network_allowed = if version == 2 {
        match captured_mode {
            Some(crate::types::permission::SandboxMode::ReadOnly) => false,
            Some(crate::types::permission::SandboxMode::DangerFullAccess) => true,
            _ => recorded_network,
        }
    } else {
        recorded_network
    };
    Ok(crate::engine::session::RuntimePolicyState {
        approval_policy: optional(value, "approvalPolicy")?.or(config.approval_policy),
        sandbox_mode: captured_mode.or(config.sandbox_mode),
        approvals_reviewer: optional(value, "approvalsReviewer")?.or(config.approvals_reviewer),
        generation: current.generation,
        sandbox_workspace_write: if version >= 3 {
            required(value, "sandboxWorkspaceWrite")?
        } else {
            config.sandbox_workspace_write.clone()
        },
        network_allowed,
    })
}

fn restore_runtime_policy_backend(
    state: crate::engine::session::RuntimePolicyState,
    current: &crate::engine::session::RuntimePolicySnapshot,
    config: &crate::schemas::config::EngineConfig,
    config_home: &RuntimePath,
    required_network_sandbox: Option<&Arc<dyn crate::types::sandbox::SandboxBackend>>,
) -> Result<Arc<crate::engine::session::RuntimePolicySnapshot>, RuntimeString> {
    use crate::types::permission::SandboxMode;
    let mode = state.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    let current_mode = current.state.sandbox_mode.unwrap_or(SandboxMode::ReadOnly);
    // Both confined modes use the same native backend. Retain immutable task and
    // network decorators, while apply_to_context installs the captured ceiling.
    let sandbox_backend = if let Some(captured) = required_network_sandbox {
        captured.clone()
    } else if current_mode != SandboxMode::DangerFullAccess || mode == SandboxMode::DangerFullAccess
    {
        current.sandbox_backend.clone()
    } else {
        let mut backend = crate::sandbox::create_sandbox_for_mode_with_engine_config(
            mode,
            config,
            Some(config_home),
        )
        .map_err(RuntimeString::from)?;
        if let Some(scope) = current.sandbox_backend.task_resource_scope() {
            backend = Arc::new(
                crate::sandbox::task_scope::TaskScopedSandbox::new(backend, scope)
                    .map_err(RuntimeString::from)?,
            );
        }
        if current.sandbox_backend.enforces_planning_read_only() {
            backend = Arc::new(crate::sandbox::plan_readonly::PlanReadOnlySandbox::new(
                backend,
            ));
        }
        backend
    };
    Ok(Arc::new(crate::engine::session::RuntimePolicySnapshot {
        state,
        sandbox_backend,
    }))
}

impl Session {
    pub(crate) fn subagent_runtime_policy_override(
        &self,
        parent: &Arc<crate::engine::session::RuntimePolicySnapshot>,
        update: &crate::engine::session::RuntimePolicyUpdate,
    ) -> Result<Arc<crate::engine::session::RuntimePolicySnapshot>, RuntimeString> {
        let state =
            crate::engine::session::subagent_runtime_policy_state_for_update(&parent.state, update)
                .map_err(RuntimeString::from)?;
        let config_home = self
            .user_config_dir
            .clone()
            .unwrap_or_else(|| RuntimePath::from(crate::fs::paths::config_home()));
        restore_runtime_policy_backend(
            state,
            parent,
            &self.config,
            &config_home,
            self.required_network_sandbox.as_ref(),
        )
    }

    pub(super) async fn decode_ordinary_snapshot(
        self: &Arc<Self>,
        value: &RuntimeJson,
        resume: &SubagentResumeContext,
        evicted_capabilities: Option<&SubagentCapabilityCapture>,
    ) -> Result<SubagentCreationSnapshot, RuntimeString> {
        let version: u32 = required(value, "version")?;
        if !matches!(version, 2..=6) {
            return Err("Unsupported or incomplete creation authority metadata.".into());
        }
        if version < 4 {
            let _: crate::types::permission::PermissionMode = required(value, "permissionMode")?;
        }
        let bindings = decode_bindings(value)?;
        // Decode old configuration only for its stored model and authority migration.
        // All other settings start with the actual sender's current turn capture.
        let stored = restore_config(value, &resume.bundle.config)?;
        let agent_type: Option<RuntimeString> = required(value, "agentType")?;
        let in_fork_child: bool = required(value, "inForkChild")?;
        let definition = agent_type
            .as_ref()
            .map(|name| {
                if let Some(definition) = self.current_agent_registry().get(name).cloned() {
                    Ok(Some(definition))
                } else if in_fork_child && name == crate::engine::fork::FORK_AGENT_TYPE {
                    Ok(None)
                } else {
                    Err(RuntimeString::from("The stored agent role is unavailable: ").concat(name))
                }
            })
            .transpose()?
            .flatten();
        let (effort, ultra_mode) = restore_effort(value, &stored, definition.as_ref())?;
        let selection = stored.model_source.as_ref().and_then(|source| {
            source
                .selection
                .clone()
                .filter(|selection| selection.contains(':'))
                .or_else(|| {
                    Some(format!(
                        "{}:{}",
                        source.source.as_ref()?,
                        source.model_id.as_ref()?
                    ))
                })
        });
        if selection.is_none()
            && value.get("providerName").is_some()
            && required::<RuntimeString>(value, "providerName")?
                != resume.bundle.provider.info().name
        {
            return Err(
                "The stored model provider is unavailable in the current sender configuration."
                    .into(),
            );
        }
        let same_source = stored
            .model_source
            .as_ref()
            .zip(resume.bundle.config.model_source.as_ref())
            .is_some_and(|(saved, current)| {
                saved.source.is_some()
                    && saved.source == current.source
                    && (!matches!(saved.source.as_deref(), Some("custom" | "local"))
                        || saved.model_id == current.model_id)
            });
        let same_provider = match value.get("providerName") {
            Some(RuntimeJson::String(name)) => name == &resume.bundle.provider.info().name,
            Some(_) => false,
            None => {
                // Historical digests are positive identity evidence, never a recovery gate.
                if let Some(expected) = value.get("providerDigest").and_then(RuntimeJson::as_str) {
                    use sha2::{Digest, Sha256};
                    let info = runtime_codec::encode_value(resume.bundle.provider.info())
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                    let mut hash = Sha256::new();
                    for unit in crate::runtime_values::runtime_json::stringify(&info).as_utf16() {
                        hash.update(unit.to_le_bytes());
                    }
                    expected == &RuntimeString::from(format!("{:x}", hash.finalize()))
                } else {
                    false
                }
            }
        };
        // A declared raw model remains on its already bound provider. It does not
        // become a new source-qualified catalog selection during cold recovery.
        let reuse_bound_provider = same_source
            && same_provider
            && resume.bundle.provider.has_model_capabilities(&stored.model);
        let selected = if reuse_bound_provider {
            resume.bundle.for_selection(&stored.model)
        } else if let Some(selection) = selection {
            self.resolve_subagent_inference(
                &resume.bundle,
                &selection.into(),
                &crate::types::abort::AbortSignal::new(),
            )
            .await
        } else {
            resume.bundle.for_selection(&stored.model)
        }
        .map_err(|error| {
            RuntimeString::from(format!("Restoring the stored model source failed: {error}"))
        })?;
        // Source-qualified selectors can name a catalog entry; restore the stored transport model.
        let selected = selected
            .for_selection(&stored.model)
            .map_err(RuntimeString::from)?;
        let mut bundle = (*selected).clone();
        bundle.config.effort = effort.clone();
        bundle.config.ultra_mode = ultra_mode;
        bundle.config.subagent_model_catalog = stored.subagent_model_catalog;
        bundle.config.subagent_model_options = stored.subagent_model_options.clone();
        // The initial task review floor survives independently of the sender's
        // effective approval flow and of later per-turn boundary clear events.
        if stored.approval_policy == Some(crate::types::permission::ApprovalPolicy::UnlessTrusted) {
            bundle.config.approval_policy = stored.approval_policy;
        }
        let config = bundle.config.clone();
        let bundle = Arc::new(bundle).capture(&[]);

        let captured_state =
            decode_runtime_policy_state(value, &stored, &resume.runtime_policy.state)?;
        let state = crate::engine::session::intersect_subagent_runtime_state(
            &captured_state,
            &resume.runtime_policy.state,
        );
        let config_home = self
            .user_config_dir
            .clone()
            .unwrap_or_else(|| RuntimePath::from(crate::fs::paths::config_home()));
        let runtime_policy = restore_runtime_policy_backend(
            state,
            &resume.runtime_policy,
            &config,
            &config_home,
            self.required_network_sandbox.as_ref(),
        )?;
        let cwd = resume.cwd.clone();
        let native_cwd = cwd
            .as_runtime_string()
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("Invalid sender cwd."))?;
        if !std::path::Path::new(&native_cwd).is_absolute()
            || !std::path::Path::new(&native_cwd).is_dir()
        {
            return Err("The current sender cwd is unavailable.".into());
        }
        let permission_origin = value
            .get("permissionOrigin")
            .map(SubagentPermissionOrigin::decode_runtime)
            .transpose()
            .map_err(|error| {
                RuntimeString::from(format!("Invalid original permission origin: {error}"))
            })?;
        let admitted_permission = PermissionContext {
            allow_rules: required(value, "allowRules")?,
            deny_rules: required(value, "denyRules")?,
            ask_rules: required(value, "askRules")?,
            additional_writable_roots: required(value, "writableRoots")?,
            internal_writable_roots: Vec::new(),
            pinned_memory_root: None,
            should_avoid_prompts: required(value, "avoidPrompts")?,
        };
        let mut permission = crate::engine::session::project_subagent_permission(
            &admitted_permission,
            &resume.permission,
            permission_origin.as_ref(),
            true,
        );
        crate::engine::session::constrain_subagent_canonical_roots(
            &mut permission,
            &captured_state,
            &runtime_policy.state,
            permission_origin.as_ref(),
        );
        let runtime_capabilities = SubagentCapabilityCapture::restore(
            self,
            value.get("runtimeCapabilityCapture"),
            &resume.runtime_capabilities,
            evicted_capabilities,
        )?;
        let (mut available, _) = self.capture_tool_generation();
        available.extend(Self::mcp_tools_from_clients(
            &self.plugin_mcp_clients_snapshot(),
        ));
        self.prepare_subagent_tool_inventory(&mut available, &config);
        let role_mcp = definition
            .as_ref()
            .and_then(|definition| definition.mcp_servers.as_ref())
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mcp_runtime = self
            .connect_subagent_mcp(
                resume.mcp.clone(),
                role_mcp,
                cwd.clone(),
                crate::types::abort::AbortSignal::new(),
                config.tool_timeout_ms,
                runtime_capabilities
                    .capabilities()
                    .browser_tool_mapping
                    .clone(),
            )
            .await;
        available.retain(|tool| tool.native_mcp_binding().is_none());
        available.extend(resume.mcp.native_tools());
        available.extend(mcp_runtime.role_tools(role_mcp));
        let mcp_clients = mcp_runtime.clients.clone();
        let declarations = value
            .get("tools")
            .and_then(RuntimeJson::as_array)
            .ok_or_else(|| RuntimeString::from("Missing stored tool declarations."))?;
        let inherited_tool_names = (in_fork_child && definition.is_none())
            .then(|| declared_tool_names(declarations))
            .transpose()?;
        let disallowed = definition
            .as_ref()
            .and_then(|definition| definition.disallowed_tools.as_deref())
            .unwrap_or_default();
        let mut tools = recover_tools(
            declarations,
            available.clone(),
            inherited_tool_names.as_deref().or_else(|| {
                definition
                    .as_ref()
                    .and_then(|definition| definition.tools.as_deref())
            }),
            disallowed,
            &config.disabled_tools,
        )?;
        tools = mcp_runtime.rebind_tools(tools)?;
        available = mcp_runtime.rebind_available(
            available,
            &tools,
            runtime_capabilities
                .capabilities()
                .browser_tool_mapping
                .as_ref(),
        )?;
        let restricted = definition
            .as_ref()
            .and_then(|definition| definition.tools.as_ref())
            .is_some();
        let web_capable = bundle.provider.capabilities(&config.model).web_search;
        let has_search = tools
            .iter()
            .any(|tool| tool.name() == crate::engine::web_search_select::CLIENT_WEB_SEARCH_TOOL);
        let web = crate::engine::web_search_select::select_web_search(
            crate::tools::builtins::tool_search::ensure_tool_search(tools),
            Some(&config.web_search),
            web_capable && (!restricted || has_search),
        );
        tools = web.tools;
        tools.retain(|tool| {
            !disallowed.contains(&tool.name()) && !config.disabled_tools.contains(&tool.name())
        });
        let browser = runtime_capabilities.capabilities();
        if browser.browser_runtime.is_none() {
            crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
                &mut tools,
                available.iter().cloned(),
                browser.browser_tool_mapping.as_ref(),
                disallowed,
                &config.disabled_tools,
            );
            crate::engine::web_search_dependencies::retain_browser_search_dependency(
                &mut tools,
                available,
                browser.browser_tool_mapping.as_ref(),
                disallowed,
                &config.disabled_tools,
            );
        }
        if let Some(names) = &inherited_tool_names {
            tools.retain(|tool| names.contains(&tool.name()));
        }
        let role = definition.as_ref().and_then(|definition| {
            definition
                .prompt
                .clone()
                .or_else(|| definition.system_prompt.clone())
        });
        let reminder = definition
            .as_ref()
            .and_then(|definition| definition.critical_system_reminder.clone());
        let inherited_system = stored_fork_instructions(value)?;
        let rebuild_environment = self.system_prompt_is_default || inherited_system.is_some();
        let current_parts = self.build_sub_agent_system_prompt(
            &definition,
            &role,
            &reminder,
            &None,
            &None,
            rebuild_environment,
            &cwd,
            &config.model,
            &config,
            inherited_system,
        )()
        .await;
        let (collaboration_origin, parts) =
            restore_instruction_parts(value, &config, current_parts)?;
        mcp_runtime.restore_instruction_evidence(value.get("mcpInstructionSources"))?;
        let parts = mcp_runtime.rebind_instruction_parts(parts);
        let mut fork_seed = if version >= 6 {
            required(value, "forkHistory")?
        } else {
            Vec::new()
        };
        let ids: Vec<RuntimeString> = required(value, "forkSeedIds")?;
        if version < 6 && !ids.is_empty() {
            let store = self
                .persistence
                .store()
                .ok_or_else(|| RuntimeString::from("Original fork history is unavailable."))?;
            fork_seed = store
                .read_messages_by_ids(
                    &self
                        .id
                        .try_to_utf8()
                        .map_err(|_| RuntimeString::from("Invalid session identity."))?,
                    &ids,
                )
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            if fork_seed.len() != ids.len() {
                // Older seeds can name model-visible messages rendered from
                // typed events or a compaction checkpoint rather than message
                // rows. Reconstruct only the parent's captured prefix, never
                // its later live history, and require every requested identity.
                let parent = store
                    .read_thread(
                        &self
                            .id
                            .try_to_utf8()
                            .map_err(|_| RuntimeString::from("Invalid session identity."))?,
                    )
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?;
                if let Some(end) = parent.raw_items.iter().rposition(|item| {
                    matches!(item, RolloutItem::Message { message }
                        if ids.last() == Some(crate::engine::session::message_uuid(message)))
                }) {
                    let projected =
                        crate::store::projection::project_for_prompt(&parent.raw_items[..=end]);
                    let by_id = projected
                        .iter()
                        .map(|message| (crate::engine::session::message_uuid(message), message))
                        .collect::<HashMap<_, _>>();
                    fork_seed = ids
                        .iter()
                        .filter_map(|id| by_id.get(id).map(|message| (*message).clone()))
                        .collect();
                }
            }
            if fork_seed.len() != ids.len() {
                return Err("Original fork history is incomplete.".into());
            }
            fork_seed = crate::engine::resume_repair::normalize_tool_history(&fork_seed);
        }
        let builtin = definition
            .as_ref()
            .and_then(|definition| definition.builtin_identity.as_ref());
        Ok(SubagentCreationSnapshot {
            runtime_capabilities,
            tools,
            collaboration_origin,
            permission_origin,
            permission,
            config,
            one_shot: crate::engine::builtin_agents::is_one_shot_builtin_role(builtin),
            definition,
            agent_type,
            bundle,
            runtime_policy,
            boundary_approval_policy: None,
            system_prompt: parts,
            cwd,
            max_turns: required(value, "maxTurns")?,
            effort,
            ultra_mode,
            output_schema: required(value, "outputSchema")?,
            web_search: web.request,
            in_fork_child,
            fork_seed,
            depth: bindings.depth,
            name: required(value, "name")?,
            mcp_clients,
            mcp_runtime,
            parent_agent_id: bindings.parent_agent_id,
            task_plan: crate::tasks::task_store::TaskPlanBinding::capture(&required::<
                RuntimeString,
            >(
                value, "taskListId"
            )?),
            task_list_id: required(value, "taskListId")?,
        })
    }
}
