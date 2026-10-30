//! Child-owned native MCP connections captured from the actual initiating query.

use std::sync::{Arc, Mutex};

use crate::mcp::client::{connect_mcp_server, ConnectMcpOptions, ManagedClient};
use crate::mcp::elicitation::{McpElicitationQueryLease, McpElicitationRouter};
use crate::mcp::types::McpServerConfig;
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::types::abort::AbortSignal;
use crate::types::provider::SystemPromptPart;
use crate::types::tool::{McpResourceClient, Tool};

use super::{ManagedResourceClient, Session};

const NAMESPACE_UPDATE_NAME: &str = "mcp-runtime-namespace-update";
const NAMESPACE_UPDATE_SCOPE: &str = "These instructions replace earlier initialize instructions only for the listed native bindings of this current MCP declaration. An empty instructions value clears earlier initialize guidance for those bindings. Tool descriptions and input schemas are supplied separately through the current tool declarations; resource and prompt access remains limited to the admitted views. This update does not replace base instructions, agent-role instructions, permission constraints, or opaque/host-injected tools and guidance, including entries with the same display name.";
const LEGACY_NAMESPACE_UPDATE_SCOPE: &str = "The latest runtime definition below replaces earlier initialize instructions and tool/resource/prompt metadata only for the listed native bindings of this current MCP declaration. Namespace membership is exactly the listed tools, resources, resource templates and prompts. This update does not replace base instructions, agent-role instructions, permission constraints, or opaque/host-injected tools and guidance, including entries with the same display name.";

fn namespace_update_text(metadata: &RuntimeJson, scope: &str) -> Option<RuntimeString> {
    let namespace = metadata.get("namespace")?.as_str()?;
    Some(
        RuntimeString::from("Updated instructions for namespace ")
            .concat(&runtime_json::stringify(&RuntimeJson::String(
                namespace.clone(),
            )))
            .concat(&RuntimeString::from(":\n"))
            .concat(&RuntimeString::from(scope))
            .concat(&RuntimeString::from("\n\n"))
            .concat(&runtime_json::stringify_pretty(metadata)),
    )
}

fn is_runtime_namespace_update(part: &SystemPromptPart) -> bool {
    if part.name != NAMESPACE_UPDATE_NAME
        || part.cacheable != Some(true)
        || part.cache_scope.is_some()
    {
        return false;
    }
    let Ok(text) = part.text.try_to_utf8() else {
        return false;
    };
    let Some((_, payload)) = text.split_once("\n\n") else {
        return false;
    };
    let Ok(metadata) = runtime_json::parse(&RuntimeString::from(payload)) else {
        return false;
    };
    if metadata.as_object().is_none_or(|object| object.len() != 6)
        || metadata
            .get("instructions")
            .and_then(RuntimeJson::as_str)
            .is_none()
        || ["tools", "resources", "resourceTemplates", "prompts"]
            .iter()
            .any(|key| metadata.get(*key).and_then(RuntimeJson::as_array).is_none())
    {
        return false;
    }
    // Only replace the exact engine envelope, never a similarly named host part
    // or Markdown embedded inside opaque server guidance.
    [NAMESPACE_UPDATE_SCOPE, LEGACY_NAMESPACE_UPDATE_SCOPE]
        .iter()
        .any(|scope| namespace_update_text(&metadata, scope).as_ref() == Some(&part.text))
}

#[derive(Clone, Default)]
pub(crate) struct SubagentMcpCapture {
    declarations: Vec<McpServerConfig>,
    native_tools: Vec<Arc<dyn Tool>>,
    resource_scopes: Vec<NativeMcpResourceScope>,
    opaque_resources: Vec<Arc<dyn McpResourceClient>>,
    instruction_sources: Vec<McpInstructionGroup>,
    root_discovery: Option<RootMcpDiscovery>,
}

#[derive(Clone)]
struct RootMcpDiscovery {
    declarations: Vec<McpServerConfig>,
    tools: Vec<Arc<dyn Tool>>,
    role: Option<crate::engine::agent_definitions::AgentDefinition>,
    disabled: Vec<RuntimeString>,
    mapping: Option<crate::browser::BrowserHostToolMapping>,
}

impl RootMcpDiscovery {
    fn tools(&self, clients: &[ManagedClient]) -> Vec<Arc<dyn Tool>> {
        let native: Vec<_> = clients
            .iter()
            .filter(|client| {
                client
                    .declaration()
                    .is_some_and(|config| self.declarations.contains(&config))
            })
            .flat_map(ManagedClient::tools)
            .collect();
        let mut tools = self.tools.clone();
        tools.retain(|tool| {
            tool.native_mcp_binding()
                .is_none_or(|binding| !self.declarations.contains(binding.config.as_ref()))
        });
        tools.extend(native.clone());
        if let Some(role) = &self.role {
            role.retain_tools(&mut tools);
        }
        tools.retain(|tool| tool.is_enabled() && !self.disabled.contains(&tool.name()));
        let disallowed = self
            .role
            .as_ref()
            .and_then(|role| role.disallowed_tools.as_deref())
            .unwrap_or_default();
        crate::engine::browser_tool_dependencies::retain_browser_tool_dependencies(
            &mut tools,
            native.clone(),
            self.mapping.as_ref(),
            disallowed,
            &self.disabled,
        );
        crate::engine::web_search_dependencies::retain_browser_search_dependency(
            &mut tools,
            native,
            self.mapping.as_ref(),
            disallowed,
            &self.disabled,
        );
        if let Some(role) = &self.role {
            role.retain_tools(&mut tools);
        }
        tools
            .into_iter()
            .filter(|tool| tool.native_mcp_binding().is_some())
            .collect()
    }
}

#[derive(Clone)]
struct NativeMcpResourceScope {
    config: McpServerConfig,
    resources: Vec<RuntimeString>,
    templates: Vec<RuntimeString>,
    prompts: Vec<RuntimeString>,
}

#[derive(Clone, PartialEq, Eq)]
struct McpInstructionRow {
    server: RuntimeString,
    instructions: RuntimeString,
    native: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct McpInstructionGroup {
    name: RuntimeString,
    rows: Vec<McpInstructionRow>,
}

impl McpInstructionGroup {
    fn text(&self) -> RuntimeString {
        let mut text: RuntimeString = "# MCP Server Instructions\n\nThe following MCP servers have provided usage instructions:\n\n".into();
        for (index, row) in self.rows.iter().enumerate() {
            if index != 0 {
                text = text.concat(&RuntimeString::from("\n\n"));
            }
            text = text
                .concat(&RuntimeString::from("## "))
                .concat(&row.server)
                .concat(&RuntimeString::from("\n"))
                .concat(&row.instructions);
        }
        text
    }

    fn evidence(&self) -> RuntimeJson {
        let mut object = RuntimeObject::new();
        object.insert("name", RuntimeJson::String(self.name.clone()));
        object.insert(
            "rows",
            RuntimeJson::Array(
                self.rows
                    .iter()
                    .map(|row| {
                        let mut fields = RuntimeObject::new();
                        fields.insert("server", RuntimeJson::String(row.server.clone()));
                        fields.insert(
                            "instructions",
                            RuntimeJson::String(row.instructions.clone()),
                        );
                        fields.insert("native", RuntimeJson::Bool(row.native));
                        RuntimeJson::Object(fields)
                    })
                    .collect(),
            ),
        );
        RuntimeJson::Object(object)
    }
}

impl SubagentMcpCapture {
    pub(crate) fn capture(
        tools: &[Arc<dyn Tool>],
        resources: &[Arc<dyn McpResourceClient>],
    ) -> Self {
        let mut capture = Self::default();
        for resource in resources {
            if let Some(instructions) = resource.instructions() {
                let name = RuntimeString::from(resource.instructions_group());
                let index = capture
                    .instruction_sources
                    .iter()
                    .position(|group| group.name == name)
                    .unwrap_or_else(|| {
                        capture.instruction_sources.push(McpInstructionGroup {
                            name,
                            rows: Vec::new(),
                        });
                        capture.instruction_sources.len() - 1
                    });
                capture.instruction_sources[index]
                    .rows
                    .push(McpInstructionRow {
                        server: resource.name().clone(),
                        instructions: instructions.clone(),
                        native: resource.native_mcp_client().is_some(),
                    });
            }
            if let Some(client) = resource.native_mcp_client() {
                if let Some(config) = client.declaration() {
                    capture.include(config.clone());
                    capture.resource_scopes.push(NativeMcpResourceScope {
                        config,
                        resources: client
                            .resources
                            .iter()
                            .map(|resource| resource.uri.clone())
                            .collect(),
                        templates: client
                            .resource_templates
                            .iter()
                            .map(|template| template.uri_template.clone())
                            .collect(),
                        prompts: client
                            .prompts
                            .iter()
                            .map(|prompt| prompt.name.clone())
                            .collect(),
                    });
                }
            } else {
                capture.opaque_resources.push(resource.clone());
            }
        }
        capture.include_tools(tools);
        capture
    }

    pub(crate) fn include_tools(&mut self, tools: &[Arc<dyn Tool>]) {
        for tool in tools {
            if let Some(binding) = tool.native_mcp_binding() {
                self.include(binding.config.as_ref().clone());
                if !self
                    .native_tools
                    .iter()
                    .any(|current| Arc::ptr_eq(current, tool))
                {
                    self.native_tools.push(tool.clone());
                }
            }
        }
    }

    fn include(&mut self, config: McpServerConfig) {
        if config.is_enabled() && !self.declarations.contains(&config) {
            self.declarations.push(config);
        }
    }

    pub(crate) fn native_tools(&self) -> Vec<Arc<dyn Tool>> {
        self.native_tools.clone()
    }
}

pub(crate) struct SubagentMcpRuntime {
    pub(crate) clients: Vec<ManagedClient>,
    pub(crate) router: McpElicitationRouter,
    native_pool: Vec<Arc<dyn Tool>>,
    resource_clients: Vec<ManagedClient>,
    opaque_resources: Vec<Arc<dyn McpResourceClient>>,
    instruction_sources: Mutex<Vec<McpInstructionGroup>>,
    instruction_tools: Mutex<Vec<Arc<dyn Tool>>>,
    initialization: Mutex<Option<(McpElicitationQueryLease, AbortSignal)>>,
}

pub(crate) struct SubagentMcpQueryLease {
    _query: McpElicitationQueryLease,
    cancellation: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for SubagentMcpQueryLease {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.abort();
        }
    }
}

impl SubagentMcpRuntime {
    pub(crate) fn inherited_tools(&self) -> Vec<Arc<dyn Tool>> {
        self.native_pool.clone()
    }

    pub(crate) fn begin_query(&self, signal: AbortSignal) -> SubagentMcpQueryLease {
        if let Some((query, initialization_signal)) = self.initialization.lock().unwrap().take() {
            // Preserve initialization questions through the first actual query, including
            // cold restore where the worker's lifetime token is bound after decoding.
            let cancellation = tokio::spawn(async move {
                signal.cancelled().await;
                initialization_signal.cancel();
            });
            SubagentMcpQueryLease {
                _query: query,
                cancellation: Some(cancellation),
            }
        } else {
            SubagentMcpQueryLease {
                _query: self.router.begin_query(signal),
                cancellation: None,
            }
        }
    }

    pub(crate) fn resources(&self) -> Vec<Arc<dyn McpResourceClient>> {
        let mut resources: Vec<Arc<dyn McpResourceClient>> = Vec::new();
        let mut native = Vec::new();
        let mut opaque = Vec::new();
        // Preserve the trusted row grouping and order when the next query captures
        // these views. A full-history descendant must recognize its parent's
        // rebound prompt without interpreting Markdown inside server instructions.
        for source in self.instruction_sources.lock().unwrap().iter().rev() {
            let group = match source.name.try_to_utf8().ok().as_deref() {
                Some("mcp-instructions") => "mcp-instructions",
                Some("plugin-mcp-instructions") => "plugin-mcp-instructions",
                Some("child-mcp-instructions") => "child-mcp-instructions",
                _ => continue,
            };
            for row in &source.rows {
                if row.native {
                    for (index, client) in self.resource_clients.iter().enumerate() {
                        if !native.contains(&index)
                            && client.name == row.server
                            && client.instructions.as_ref() == Some(&row.instructions)
                        {
                            resources.push(Arc::new(ManagedResourceClient(
                                Arc::new(client.clone()),
                                group,
                            )));
                            native.push(index);
                        }
                    }
                } else {
                    for (index, client) in self.opaque_resources.iter().enumerate() {
                        if !opaque.contains(&index)
                            && client.name() == &row.server
                            && client.instructions() == Some(&row.instructions)
                        {
                            resources.push(client.clone());
                            opaque.push(index);
                        }
                    }
                }
            }
        }
        resources.extend(
            self.opaque_resources
                .iter()
                .enumerate()
                .filter(|(index, _)| !opaque.contains(index))
                .map(|(_, client)| client.clone()),
        );
        resources.extend(
            self.resource_clients
                .iter()
                .enumerate()
                .filter(|(index, _)| !native.contains(index))
                .map(|(_, client)| {
                    Arc::new(ManagedResourceClient(
                        Arc::new(client.clone()),
                        "child-mcp-instructions",
                    )) as Arc<dyn McpResourceClient>
                }),
        );
        resources
    }

    pub(crate) fn capture(&self, tools: &[Arc<dyn Tool>]) -> SubagentMcpCapture {
        *self.instruction_tools.lock().unwrap() = tools.to_vec();
        let mut capture = SubagentMcpCapture::capture(tools, &self.resources());
        capture.instruction_sources = self.instruction_sources.lock().unwrap().clone();
        capture
    }

    pub(crate) fn instruction_evidence(&self) -> RuntimeJson {
        RuntimeJson::Array(
            self.instruction_sources
                .lock()
                .unwrap()
                .iter()
                .map(McpInstructionGroup::evidence)
                .collect(),
        )
    }

    pub(crate) fn restore_instruction_evidence(
        &self,
        value: Option<&RuntimeJson>,
    ) -> Result<(), RuntimeString> {
        let Some(value) = value else {
            return Ok(());
        };
        let invalid = || RuntimeString::from("Invalid stored MCP instruction sources.");
        let mut sources = self.instruction_sources.lock().unwrap();
        for group in value.as_array().ok_or_else(invalid)? {
            let name = group
                .get("name")
                .and_then(RuntimeJson::as_str)
                .ok_or_else(invalid)?
                .clone();
            if ![
                "mcp-instructions",
                "plugin-mcp-instructions",
                "child-mcp-instructions",
            ]
            .iter()
            .any(|tag| name == *tag)
            {
                return Err(invalid());
            }
            let mut rows = Vec::new();
            for row in group
                .get("rows")
                .and_then(RuntimeJson::as_array)
                .ok_or_else(invalid)?
            {
                rows.push(McpInstructionRow {
                    server: row
                        .get("server")
                        .and_then(RuntimeJson::as_str)
                        .ok_or_else(invalid)?
                        .clone(),
                    instructions: row
                        .get("instructions")
                        .and_then(RuntimeJson::as_str)
                        .ok_or_else(invalid)?
                        .clone(),
                    native: row
                        .get("native")
                        .and_then(RuntimeJson::as_bool)
                        .ok_or_else(invalid)?,
                });
            }
            let group = McpInstructionGroup { name, rows };
            if !sources.contains(&group) {
                sources.push(group);
            }
        }
        Ok(())
    }

    pub(crate) fn rebind_instruction_parts(
        &self,
        mut parts: Vec<SystemPromptPart>,
    ) -> Vec<SystemPromptPart> {
        // Rebuild owned updates even when the new connection has no instructions
        // or no admitted bindings. Old snapshots must not retain schema dumps.
        parts.retain(|part| !is_runtime_namespace_update(part));
        let mut sources = self.instruction_sources.lock().unwrap();
        let mut represented = Vec::new();
        let mut rebound = Vec::new();
        let mut unmatched_mcp = false;
        parts.retain_mut(|part| {
            let Some(source) = sources
                .iter()
                .find(|source| source.name == part.name && source.text() == part.text)
            else {
                unmatched_mcp |= [
                    "mcp-instructions",
                    "plugin-mcp-instructions",
                    "child-mcp-instructions",
                ]
                .iter()
                .any(|name| part.name == *name);
                return true;
            };
            let mut rows = Vec::new();
            for row in &source.rows {
                if !row.native {
                    rows.push(row.clone());
                    continue;
                }
                let matches: Vec<_> = self
                    .clients
                    .iter()
                    .filter(|client| client.name == row.server)
                    .collect();
                // Duplicate display names cannot select an arbitrary native connection.
                if let [client] = matches.as_slice() {
                    if let Some(config) = client.declaration() {
                        represented.push(config);
                    }
                    if let Some(instructions) = &client.instructions {
                        rows.push(McpInstructionRow {
                            instructions: instructions.clone(),
                            ..row.clone()
                        });
                    }
                }
            }
            if rows.is_empty() {
                return false;
            }
            let group = McpInstructionGroup {
                name: part.name.clone(),
                rows,
            };
            part.text = group.text();
            rebound.push(group);
            true
        });
        for client in &self.clients {
            let Some(config) = client.declaration() else {
                continue;
            };
            if unmatched_mcp || !represented.contains(&config) {
                if let Some(update) = self.namespace_instruction_update(client, &config) {
                    if !parts.contains(&update) {
                        parts.push(update);
                    }
                }
            }
        }
        for group in rebound {
            if !sources.contains(&group) {
                sources.push(group);
            }
        }
        parts
    }

    fn namespace_instruction_update(
        &self,
        client: &ManagedClient,
        config: &McpServerConfig,
    ) -> Option<SystemPromptPart> {
        let instructions = client.instructions.clone().unwrap_or_default();
        let tools: Vec<_> = self
            .instruction_tools
            .lock()
            .unwrap()
            .iter()
            .filter_map(|tool| {
                let binding = tool.native_mcp_binding()?;
                if binding.config.as_ref() != config
                    || tool.exposure() == crate::types::tool::ToolExposure::Hidden
                {
                    return None;
                }
                let mut descriptor = RuntimeObject::new();
                descriptor.insert("name", RuntimeJson::String(tool.name()));
                descriptor.insert("nativeName", RuntimeJson::String(binding.tool_name));
                Some(RuntimeJson::Object(descriptor))
            })
            .collect();
        let views: Vec<_> = self
            .resource_clients
            .iter()
            .filter(|view| view.declaration().as_ref() == Some(config))
            .collect();
        let resources: Vec<_> = views
            .iter()
            .flat_map(|view| &view.resources)
            .map(|resource| RuntimeJson::String(resource.uri.clone()))
            .collect();
        let templates: Vec<_> = views
            .iter()
            .flat_map(|view| &view.resource_templates)
            .map(|template| RuntimeJson::String(template.uri_template.clone()))
            .collect();
        let prompts: Vec<_> = views
            .iter()
            .flat_map(|view| &view.prompts)
            .map(|prompt| RuntimeJson::String(prompt.name.clone()))
            .collect();
        if tools.is_empty() && resources.is_empty() && templates.is_empty() && prompts.is_empty() {
            return None;
        }
        let mut metadata = RuntimeObject::new();
        metadata.insert("namespace", RuntimeJson::String(client.name.clone()));
        metadata.insert("instructions", RuntimeJson::String(instructions));
        metadata.insert("tools", RuntimeJson::Array(tools));
        metadata.insert("resources", RuntimeJson::Array(resources));
        metadata.insert("resourceTemplates", RuntimeJson::Array(templates));
        metadata.insert("prompts", RuntimeJson::Array(prompts));
        // Binding identities scope the initialize guidance; full descriptions and
        // schemas belong exclusively to the executable tool declaration surface.
        Some(SystemPromptPart {
            name: NAMESPACE_UPDATE_NAME.into(),
            text: namespace_update_text(&RuntimeJson::Object(metadata), NAMESPACE_UPDATE_SCOPE)?,
            cacheable: Some(true),
            cache_scope: None,
        })
    }

    fn connected_tools(&self) -> Vec<Arc<dyn Tool>> {
        self.clients.iter().flat_map(ManagedClient::tools).collect()
    }

    pub(crate) fn role_tools(&self, declarations: &[McpServerConfig]) -> Vec<Arc<dyn Tool>> {
        self.clients
            .iter()
            .filter(|client| {
                client
                    .declaration()
                    .is_some_and(|config| declarations.contains(&config))
            })
            .flat_map(ManagedClient::tools)
            .collect()
    }

    pub(crate) fn rebind_tools(
        &self,
        tools: Vec<Arc<dyn Tool>>,
    ) -> Result<Vec<Arc<dyn Tool>>, RuntimeString> {
        let rebound: Vec<_> = tools
            .into_iter()
            .map(|tool| self.rebind_tool(tool))
            .collect::<Result<_, _>>()?;
        *self.instruction_tools.lock().unwrap() = rebound.clone();
        Ok(rebound)
    }

    fn rebind_tool(&self, tool: Arc<dyn Tool>) -> Result<Arc<dyn Tool>, RuntimeString> {
        let available = self.connected_tools();
        let Some(binding) = tool.native_mcp_binding() else {
            return Ok(tool);
        };
        let replacement = available
            .iter()
            .find(|candidate| {
                candidate.native_mcp_binding().is_some_and(|current| {
                    current.config == binding.config && current.tool_name == binding.tool_name
                })
            })
            .ok_or_else(|| {
                RuntimeString::from(
                    "The child MCP connection does not provide the authorized tool: ",
                )
                .concat(&tool.name())
            })?;
        // A missing or disabled native executor never falls back to its parent closure.
        Ok(tool.rebind_native_mcp(replacement.clone()))
    }

    pub(crate) fn rebind_available(
        &self,
        tools: Vec<Arc<dyn Tool>>,
        selected: &[Arc<dyn Tool>],
        mapping: Option<&crate::browser::BrowserHostToolMapping>,
    ) -> Result<Vec<Arc<dyn Tool>>, RuntimeString> {
        let mut required: Vec<_> = selected.iter().map(|tool| tool.name()).collect();
        if let Some(mapping) = mapping {
            for (owner, dependency) in crate::engine::browser_tool_dependencies::bindings(mapping) {
                if selected.iter().any(|tool| tool.name() == owner) {
                    required.push(dependency.into());
                }
            }
            if selected.iter().any(|tool| tool.name() == "web_search") {
                required.push(mapping.tabs_content.as_str().into());
            }
        }
        let mut available = Vec::new();
        for tool in tools {
            let needed = required.contains(&tool.name());
            match self.rebind_tool(tool) {
                Ok(tool) => available.push(tool),
                Err(error) if needed => return Err(error),
                Err(_) => {}
            }
        }
        Ok(available)
    }
}

impl Drop for SubagentMcpRuntime {
    fn drop(&mut self) {
        let clients = self.clients.clone();
        let close = async move {
            for client in clients {
                client.close().await;
            }
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(close);
        } else {
            std::thread::spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    runtime.block_on(close);
                }
            });
        }
    }
}

impl Session {
    pub(crate) fn current_subagent_mcp_capture(&self) -> SubagentMcpCapture {
        let (mut tools, _) = self.capture_tool_generation();
        tools.extend(Self::mcp_tools_from_clients(
            &self.plugin_mcp_clients_snapshot(),
        ));
        let mut capture =
            SubagentMcpCapture::capture(&tools, &self.all_mcp_resource_clients_snapshot());
        // A direct root API call can precede lazy connection initialization.
        let declarations = self.mcp.plugin_servers();
        for config in &declarations {
            capture.include(config.clone());
        }
        capture.root_discovery = Some(RootMcpDiscovery {
            declarations,
            tools,
            role: self.root_agent_definition.clone(),
            disabled: self.config.disabled_tools.clone(),
            mapping: None,
        });
        capture
    }

    pub(crate) async fn connect_subagent_mcp(
        &self,
        mut capture: SubagentMcpCapture,
        role: &[McpServerConfig],
        cwd: RuntimePath,
        signal: AbortSignal,
        call_timeout_ms: u64,
        browser_mapping: Option<crate::browser::BrowserHostToolMapping>,
    ) -> Arc<SubagentMcpRuntime> {
        if let Some(discovery) = &mut capture.root_discovery {
            discovery.mapping = browser_mapping;
        }
        for config in role {
            capture.include(config.clone());
        }
        let router = self.mcp_elicitation_router.fork_owner();
        let initialization = router.begin_query(signal.clone());
        let options = ConnectMcpOptions {
            credential_store: self.credential_store.clone(),
            mcp_authorize_handler: self.mcp_authorize_handler.clone(),
            oauth_redirect_uri: self.mcp_oauth_redirect_uri.clone(),
            cwd: Some(cwd),
            signal: Some(signal.clone()),
            call_timeout_ms: Some(call_timeout_ms),
            mcp_elicitation_router: Some(router.clone()),
            ..ConnectMcpOptions::new(self.process_context.clone())
        };
        let mut clients = Vec::new();
        for config in &capture.declarations {
            clients.push(connect_mcp_server(config, &options).await);
        }
        let resource_clients = clients
            .iter()
            .filter_map(|client| {
                let config = client.declaration()?;
                if role.contains(&config)
                    || capture
                        .root_discovery
                        .as_ref()
                        .is_some_and(|discovery| discovery.declarations.contains(&config))
                {
                    return Some(client.clone());
                }
                let scopes: Vec<_> = capture
                    .resource_scopes
                    .iter()
                    .filter(|scope| scope.config == config)
                    .collect();
                if scopes.is_empty() {
                    return None;
                }
                let mut scoped = client.clone();
                scoped.resources.retain(|resource| {
                    scopes
                        .iter()
                        .any(|scope| scope.resources.contains(&resource.uri))
                });
                scoped.resource_templates.retain(|template| {
                    scopes
                        .iter()
                        .any(|scope| scope.templates.contains(&template.uri_template))
                });
                scoped.prompts.retain(|prompt| {
                    scopes
                        .iter()
                        .any(|scope| scope.prompts.contains(&prompt.name))
                });
                Some(scoped)
            })
            .collect();
        let native_pool = match &capture.root_discovery {
            Some(discovery) => discovery.tools(&clients),
            None => capture.native_tools,
        };
        Arc::new(SubagentMcpRuntime {
            clients,
            router,
            native_pool,
            resource_clients,
            opaque_resources: capture.opaque_resources,
            instruction_sources: Mutex::new(capture.instruction_sources),
            instruction_tools: Mutex::new(Vec::new()),
            initialization: Mutex::new(Some((initialization, signal))),
        })
    }
}
