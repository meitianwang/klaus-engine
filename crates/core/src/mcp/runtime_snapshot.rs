//! Read-only MCP runtime exposure snapshot.
//!
//! This is an engine-facing status view over connected MCP clients. It does not
//! implement a public MCP server; hosts can use it to render or test what the
//! engine currently exposes from consumed MCP servers.

use crate::mcp::client::ManagedClient;
use crate::mcp::oauth_provider::McpAuthStatus;
use crate::mcp::types::{
    McpConnectionState, McpResourceTemplateDescriptor, McpServerInfo, McpStartupFailureReason,
};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};
use crate::types::json::Json;
use crate::types::tool::{McpPromptArgument, ToolExposure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRuntimeSnapshot {
    pub servers: Vec<McpServerRuntimeSnapshot>,
}

impl RuntimeEncode for McpRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("servers", &self.servers)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                servers: object.required("servers", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerRuntimeSnapshot {
    pub name: RuntimeString,
    pub state: McpConnectionState,
    /// How the connection authenticates. Separate from `state` on purpose: the
    /// host needs both to decide whether an authorize action belongs on the row.
    pub auth_status: McpAuthStatus,
    /// The actionable classification of a non-ready state, when there is one.
    pub failure_reason: Option<McpStartupFailureReason>,
    /// The failure text, verbatim, so a failed row can say more than "failed".
    pub error: Option<RuntimeString>,
    pub authorization_url: Option<RuntimeString>,
    pub instructions: Option<RuntimeString>,
    /// Which build answered `initialize`, when the server reported it.
    pub server_info: Option<McpServerInfo>,
    pub tools: Vec<McpToolRuntimeSnapshot>,
    pub resources: Vec<McpResourceRuntimeSnapshot>,
    /// URI patterns the caller fills in. Deliberately not merged into
    /// `resources`: a template cannot be read as-is.
    pub resource_templates: Vec<McpResourceTemplateDescriptor>,
    pub prompts: Vec<McpPromptRuntimeSnapshot>,
}

impl RuntimeEncode for McpServerRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("state", &self.state)?;
        object.field("authStatus", &self.auth_status)?;
        if let Some(value) = &self.failure_reason {
            object.field("failureReason", value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        if let Some(value) = &self.authorization_url {
            object.field("authorizationUrl", value)?;
        }
        if let Some(value) = &self.instructions {
            object.field("instructions", value)?;
        }
        if let Some(value) = &self.server_info {
            object.field("serverInfo", value)?;
        }
        object.field("tools", &self.tools)?;
        object.field("resources", &self.resources)?;
        object.field("resourceTemplates", &self.resource_templates)?;
        object.field("prompts", &self.prompts)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpServerRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                state: object.required("state", &[])?,
                auth_status: object.required("authStatus", &[])?,
                failure_reason: object.optional("failureReason", &[])?,
                error: object.optional("error", &[])?,
                authorization_url: object.optional("authorizationUrl", &[])?,
                instructions: object.optional("instructions", &[])?,
                server_info: object.optional("serverInfo", &[])?,
                tools: object.required("tools", &[])?,
                resources: object.required("resources", &[])?,
                resource_templates: object.required("resourceTemplates", &[])?,
                prompts: object.required("prompts", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpToolRuntimeSnapshot {
    pub name: RuntimeString,
    pub exposure: ToolExposure,
    pub search_hint: Option<RuntimeString>,
    pub read_only: bool,
    pub destructive: Option<bool>,
}

impl RuntimeEncode for McpToolRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("exposure", &self.exposure)?;
        if let Some(value) = &self.search_hint {
            object.field("searchHint", value)?;
        }
        object.field("readOnly", &self.read_only)?;
        if let Some(value) = &self.destructive {
            object.field("destructive", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpToolRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                exposure: object.required("exposure", &[])?,
                search_hint: object.optional("searchHint", &[])?,
                read_only: object.required("readOnly", &[])?,
                destructive: object.optional("destructive", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceRuntimeSnapshot {
    pub uri: RuntimeString,
    pub name: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
}

impl RuntimeEncode for McpResourceRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uri", &self.uri)?;
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpResourceRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri: object.required("uri", &[])?,
                name: object.optional("name", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                description: object.optional("description", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptRuntimeSnapshot {
    pub name: RuntimeString,
    pub description: Option<RuntimeString>,
    pub arguments: Option<Vec<McpPromptArgument>>,
}

impl RuntimeEncode for McpPromptRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.arguments {
            object.field("arguments", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpPromptRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.optional("description", &[])?,
                arguments: object.optional("arguments", &[])?,
            })
        })
    }
}

pub fn mcp_runtime_snapshot_from_clients(clients: &[ManagedClient]) -> McpRuntimeSnapshot {
    McpRuntimeSnapshot {
        servers: clients.iter().map(snapshot_client).collect(),
    }
}

fn snapshot_client(client: &ManagedClient) -> McpServerRuntimeSnapshot {
    let empty_input = Json::Object(Default::default());
    let tools = client
        .tools()
        .into_iter()
        .map(|tool| McpToolRuntimeSnapshot {
            name: tool.name(),
            exposure: tool.exposure(),
            search_hint: tool.search_hint(),
            read_only: tool.is_read_only(&empty_input),
            destructive: tool.is_destructive(&empty_input),
        })
        .collect();

    let resources = client
        .resources
        .iter()
        .map(|resource| McpResourceRuntimeSnapshot {
            uri: resource.uri.clone(),
            name: resource.name.clone(),
            mime_type: resource.mime_type.clone(),
            description: resource.description.clone(),
        })
        .collect();

    let prompts = client
        .prompts
        .iter()
        .map(|prompt| McpPromptRuntimeSnapshot {
            name: prompt.name.clone(),
            description: prompt.description.clone(),
            arguments: prompt.arguments.clone(),
        })
        .collect();

    McpServerRuntimeSnapshot {
        name: client.name.clone(),
        state: client.state,
        auth_status: client.auth_status,
        failure_reason: client.failure_reason,
        error: client.error.clone(),
        authorization_url: client.authorization_url.as_deref().map(RuntimeString::from),
        instructions: client.instructions.clone(),
        server_info: client.server_info.clone(),
        tools,
        resources,
        resource_templates: client.resource_templates.clone(),
        prompts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    #[test]
    fn runtime_snapshot_codec_preserves_external_surrogate_units() {
        let high = RuntimeString::from_utf16(vec![u16::from(b'n'), 0xd800]);
        let low = RuntimeString::from_utf16(vec![0xdc00, u16::from(b'v')]);
        let snapshot = McpRuntimeSnapshot {
            servers: vec![McpServerRuntimeSnapshot {
                name: high.clone(),
                state: McpConnectionState::Ready,
                auth_status: McpAuthStatus::OAuth,
                failure_reason: None,
                error: None,
                authorization_url: None,
                instructions: Some(low.clone()),
                server_info: Some(McpServerInfo {
                    name: Some(high.clone()),
                    version: None,
                    title: Some(low.clone()),
                }),
                tools: vec![McpToolRuntimeSnapshot {
                    name: low.clone(),
                    exposure: ToolExposure::Deferred,
                    search_hint: Some(high.clone()),
                    read_only: true,
                    destructive: Some(false),
                }],
                resources: vec![McpResourceRuntimeSnapshot {
                    uri: high.clone(),
                    name: Some(low.clone()),
                    mime_type: None,
                    description: Some(high.clone()),
                }],
                resource_templates: vec![McpResourceTemplateDescriptor {
                    uri_template: low.clone(),
                    name: Some(high.clone()),
                    mime_type: None,
                    description: None,
                }],
                prompts: vec![McpPromptRuntimeSnapshot {
                    name: low.clone(),
                    description: Some(high),
                    arguments: Some(vec![McpPromptArgument {
                        name: low,
                        description: None,
                        required: Some(true),
                    }]),
                }],
            }],
        };

        let encoded = runtime_codec::encode_value(&snapshot).unwrap();
        let wire = runtime_json::stringify(&encoded);
        assert!(wire.contains("\\ud800"));
        assert!(wire.contains("\\udc00"));
        let reparsed = runtime_json::parse(&wire).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<McpRuntimeSnapshot>(&reparsed).unwrap(),
            snapshot
        );
    }
}
