//! Bounded caller-owned history and the exact tool catalog for one model step.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

pub(super) const NAMESPACE: &str = "klaus_tools";
pub(super) const MAX_HISTORY: usize = 64 * 1024 * 1024;
// Leave room for the base64 envelope inside a 32 MiB transport frame.
pub(super) const MAX_BLOB: usize = 23 * 1024 * 1024;
pub(super) const MAX_HISTORY_ROOTS: usize = 4096;
const MAX_CALLER_BLOBS: usize = MAX_HISTORY_ROOTS + 3;
const MAX_UPSTREAM_BLOBS: usize = 512;
pub(super) const MAX_CHECKPOINT_ROOTS: usize = MAX_HISTORY_ROOTS + MAX_UPSTREAM_BLOBS;
// Bound calls within one step; the tool catalog has a separate byte budget.
pub(super) const MAX_TOOLS: usize = 256;
pub(super) const CONTEXT_FORMAT: &str = "cursor.model-step.v1";

#[derive(Clone)]
pub(super) struct Tool {
    pub name: String,
    pub alias: String,
    pub description: String,
    pub schema: Value,
}

pub(super) struct Catalog {
    pub tools: Vec<Tool>,
    pub definitions: Vec<Value>,
    pub choice: ToolChoice,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum ToolChoice {
    Auto,
    None,
    Required,
    Named(String),
}

impl ToolChoice {
    pub fn parse(value: Option<&Value>) -> Result<Self, String> {
        match value {
            None | Some(Value::Null) => Ok(Self::Auto),
            Some(Value::String(choice)) => match choice.as_str() {
                "auto" => Ok(Self::Auto),
                "none" => Ok(Self::None),
                "required" => Ok(Self::Required),
                _ => Err("cursor_tool_choice_invalid".into()),
            },
            Some(value) if value["type"] == "function" => {
                let map = value.as_object().ok_or("cursor_tool_choice_invalid")?;
                if map.len() != 2 || !map.contains_key("name") {
                    return Err("cursor_tool_choice_invalid".into());
                }
                let name = value["name"]
                    .as_str()
                    .filter(|name| !name.is_empty() && name.len() <= 256)
                    .ok_or("cursor_tool_choice_invalid")?;
                Ok(Self::Named(name.into()))
            }
            _ => Err("cursor_tool_choice_invalid".into()),
        }
    }

    pub fn requires_call(&self) -> bool {
        matches!(self, Self::Required | Self::Named(_))
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn safe_call_id(raw: &str) -> Result<String, String> {
    if raw.is_empty() || raw.len() > 2048 {
        return Err("cursor_tool_identity_invalid".into());
    }
    Ok(format!("cursor_call_{}", digest(raw.as_bytes())))
}

fn tool_alias(name: &str) -> String {
    if name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        && name.len() <= 96
    {
        name.to_string()
    } else {
        format!("tool_{}", digest(name.as_bytes()))
    }
}

pub(super) fn is_direct_mcp_outer(name: &str) -> bool {
    !name.trim().is_empty()
        && name.len() <= 256
        && !name.chars().any(char::is_control)
        && !matches!(
            name,
            "CallDynamicTool" | "CallMcpTool" | "Read" | "GetDynamicTools" | "GetMcpTools"
        )
}

fn sanitized_description(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

impl Catalog {
    fn binding_instruction(&self) -> String {
        let mappings=self.tools.iter().map(|tool|json!({"callerTool":tool.name,"toolName":tool.alias,"directMcpName":format!("{NAMESPACE}-{}",tool.alias)})).collect::<Vec<_>>();
        let selection = match &self.choice {
            ToolChoice::Auto => "Choose a tool when the task needs one.".to_string(),
            ToolChoice::None => "No tool may be called in this invocation. Return the final response directly.".to_string(),
            ToolChoice::Required => "This invocation requires at least one call to a declared caller tool before returning.".to_string(),
            ToolChoice::Named(name) => format!("This invocation requires a call to the declared caller tool {name}; no other caller tool is available."),
        };
        format!("{selection} Tool execution binding: each caller-declared execution tool in the mappings below belongs exclusively to the caller-owned MCP namespace {NAMESPACE}. Use CallDynamicTool with namespace {NAMESPACE}, or CallMcpTool with server {NAMESPACE}, the mapped toolName, and arguments matching the supplied input schema. Direct MCP names below are equivalent when exposed. For file reads, writes, terminal commands, searches, and other actions, use these mapped caller tools. Never substitute native tools such as Read, Shell, Grep, Write, or AskQuestion, and never substitute native mode-switching or native agent tools for a mapped caller tool. Local permissions and execution belong to the caller; do not request native approvals or authenticate other MCP servers. For metadata discovery, invoke GetDynamicTools or GetMcpTools, directly or through its corresponding namespace wrapper, scoped to klaus_tools; name patterns are resolved against this caller-owned catalog. These helpers return only metadata from the captured catalog and do not execute caller tools. An explicitly declared caller tool with one of these names keeps its declared execution semantics. If a required tool is absent, report that limitation. After returning tool calls, await actual caller results in the conversation. Treat existing native assistant/tool messages as replay history, not permission to expand the available tool set. Tool mappings: {}",json!(mappings))
    }
    pub fn new(body: &Value) -> Result<Self, String> {
        let choice = ToolChoice::parse(body.get("tool_choice"))?;
        let mut tools = Vec::new();
        let mut definitions = Vec::new();
        let mut names = HashSet::new();
        let mut aliases = HashSet::new();
        let mut bytes = 0;
        for value in body["tools"].as_array().into_iter().flatten() {
            let function = &value["function"];
            let name = function["name"]
                .as_str()
                .ok_or("cursor_tool_definition_invalid")?;
            if name.is_empty() || name.len() > 256 || !names.insert(name.to_string()) {
                return Err("cursor_tool_definition_invalid".into());
            }
            if choice == ToolChoice::None
                || matches!(&choice, ToolChoice::Named(selected) if selected != name)
            {
                continue;
            }
            let alias = tool_alias(name);
            if !aliases.insert(alias.clone()) {
                return Err("cursor_tool_alias_collision".into());
            }
            let description = function["description"].as_str().unwrap_or_default();
            let description = if alias == name {
                description.to_string()
            } else {
                format!("Tool {name}. {description}")
            };
            let schema = function["parameters"].clone();
            if !schema.is_object()
                || schema.to_string().len() > 256 * 1024
                || description.len() > 256 * 1024
            {
                return Err("cursor_tool_schema_invalid".into());
            }
            let definition = json!({"name":format!("{NAMESPACE}-{alias}"),"providerIdentifier":NAMESPACE,"toolName":alias,"description":description,"annotationsJson":"{\"readOnlyHint\":false}","inputSchema":schema});
            bytes += definition.to_string().len();
            if bytes > 2 * 1024 * 1024 {
                return Err("cursor_tool_catalog_too_large".into());
            }
            tools.push(Tool {
                name: name.into(),
                alias,
                description: sanitized_description(&description),
                schema,
            });
            definitions.push(definition);
        }
        if choice.requires_call() && tools.is_empty() {
            return Err("cursor_tool_choice_not_declared".into());
        }
        Ok(Self {
            tools,
            definitions,
            choice,
        })
    }

    pub fn by_alias(&self, alias: &str) -> Result<&Tool, String> {
        self.tools
            .iter()
            .find(|tool| tool.alias == alias)
            .ok_or_else(|| "cursor_tool_not_owned".into())
    }

    pub fn declared_read(&self) -> Result<&Tool, String> {
        let tool = self
            .tools
            .iter()
            .find(|tool| tool.name == "read")
            .ok_or("cursor_read_intent_not_declared")?;
        let mut schema = tool.schema.clone();
        if let Some(schema) = schema.as_object_mut() {
            schema.remove("description");
            if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
                for property in properties.values_mut().filter_map(Value::as_object_mut) {
                    property.remove("description");
                }
            }
        }
        let expected = json!({"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"integer","exclusiveMinimum":0,"maximum":9007199254740991_i64},"limit":{"type":"integer","exclusiveMinimum":0,"maximum":9007199254740991_i64},"pages":{"type":"string"}},"required":["path"],"additionalProperties":false});
        if schema != expected {
            return Err("cursor_read_intent_schema_unsupported".into());
        }
        Ok(tool)
    }

    pub fn request_context(&self) -> Value {
        json!({"tools":self.definitions,"webSearchEnabled":false,"webFetchEnabled":false,"mcpMetaToolOptions":{"enabled":!self.tools.is_empty(),"mcpDescriptors":self.descriptors()},"readLintsEnabled":false,"searchConversationsEnabled":false,"sendMessageEnabled":false,"rulesInfoComplete":true,"envInfoComplete":true,"repositoryInfoComplete":true,"customSubagentsInfoComplete":true,"agentSkillsInfoComplete":true,"mcpInfoComplete":true,"gitRepoInfoComplete":true,"mcpFileSystemInfoComplete":true,"gitStatusInfoComplete":true})
    }

    pub fn metadata_projection(&self, alias: Option<&str>, dynamic: bool) -> Result<Value, String> {
        let project = |tool: &Tool| {
            let mut value = json!({"tool":tool.alias,"inputSchema":tool.schema});
            if !tool.description.is_empty() {
                value["description"] = json!(tool.description);
            }
            value
        };
        if let Some(alias) = alias {
            let tool = project(self.by_alias(alias)?);
            return Ok(if dynamic {
                tool
            } else {
                json!({"mode":"single_tool","server":NAMESPACE,"serverStatus":"ready","tool":tool})
            });
        }
        let mut tools = self.tools.iter().collect::<Vec<_>>();
        tools.sort_by(|a, b| a.alias.cmp(&b.alias));
        let tools = tools.into_iter().map(project).collect::<Vec<_>>();
        Ok(if dynamic {
            json!({"mode":"namespace","namespace":NAMESPACE,"namespaceStatus":"ready","tools":tools})
        } else {
            json!({"mode":"server","server":NAMESPACE,"serverStatus":"ready","tools":tools})
        })
    }
}

pub(super) fn same_metadata(actual: &Value, expected: &Value) -> bool {
    fn normalize(value: &mut Value) {
        if let Some(tools) = value.get_mut("tools").and_then(Value::as_array_mut) {
            tools.sort_by(|a, b| a["tool"].as_str().cmp(&b["tool"].as_str()));
        }
        if let Some(matches) = value.get_mut("matches").and_then(Value::as_array_mut) {
            matches.sort_by(|a, b| {
                (
                    a["namespace"].as_str(),
                    a["server"].as_str(),
                    a["tool"].as_str(),
                )
                    .cmp(&(
                        b["namespace"].as_str(),
                        b["server"].as_str(),
                        b["tool"].as_str(),
                    ))
            });
        }
        for list in ["namespaces", "servers"] {
            if let Some(entries) = value.get_mut(list).and_then(Value::as_array_mut) {
                for entry in entries {
                    normalize(entry);
                }
            }
        }
    }
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    normalize(&mut actual);
    normalize(&mut expected);
    actual == expected
}

/// Project a deferred file-read intent without executing a provider operation.
pub(super) fn read_arguments(args: &Value, execution: bool) -> Result<Value, String> {
    let invalid = || "cursor_read_intent_arguments_unsupported".to_string();
    let map = args.as_object().ok_or_else(invalid)?;
    if map.keys().any(|key| {
        !(matches!(key.as_str(), "path" | "offset" | "limit") || execution && key == "toolCallId")
    }) {
        return Err(invalid());
    }
    let path = args["path"]
        .as_str()
        .filter(|path| !path.is_empty() && path.len() <= 256 * 1024)
        .ok_or_else(invalid)?;
    let mut result = json!({"path":path});
    for field in ["offset", "limit"] {
        if let Some(value) = args.get(field) {
            let number = value
                .as_u64()
                .filter(|number| *number > 0 && *number <= i32::MAX as u64)
                .ok_or_else(invalid)?;
            result[field] = json!(number);
        }
    }
    if execution {
        safe_call_id(args["toolCallId"].as_str().ok_or_else(invalid)?)?;
    }
    Ok(result)
}

pub(super) fn wrapper_arguments<'a>(
    args: &'a Value,
    owner: &str,
    alias: &str,
) -> Result<&'a Value, String> {
    if args[owner] != NAMESPACE
        || args["toolName"] != alias
        || args.as_object().is_none_or(|args| {
            args.keys().any(|key| {
                key != owner && !matches!(key.as_str(), "toolName" | "arguments" | "mcpDetails")
            })
        })
    {
        return Err(super::tool_mismatch(
            "wrapper_identity_or_shape",
            args,
            &json!({(owner):NAMESPACE,"toolName":alias,"arguments":{}}),
        ));
    }
    if let Some(details) = args.get("mcpDetails") {
        if details.as_object().is_none_or(|details| {
            details.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "description" | "requestSmartModeApproval" | "smartModeBlockReason"
                )
            })
        }) || details
            .get("requestSmartModeApproval")
            .is_some_and(|value| value != false)
            || ["description", "smartModeBlockReason"].iter().any(|field| {
                details
                    .get(field)
                    .is_some_and(|value| value.as_str().is_none_or(|text| text.len() > 256 * 1024))
            })
        {
            return Err(super::tool_mismatch(
                "wrapper_metadata",
                details,
                &json!({"requestSmartModeApproval":false}),
            ));
        }
    }
    args.get("arguments")
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            super::tool_mismatch("wrapper_arguments_shape", args, &json!({"arguments":{}}))
        })
}

#[derive(Default)]
pub(super) struct Blobs {
    values: BTreeMap<String, Vec<u8>>,
    bytes: usize,
    caller_entries: usize,
    upstream_entries: usize,
}

impl Blobs {
    pub fn insert(&mut self, key: String, bytes: Vec<u8>) -> Result<(), String> {
        self.store(key, bytes, false)
    }

    fn store(&mut self, key: String, bytes: Vec<u8>, caller_owned: bool) -> Result<(), String> {
        let limit_error = if caller_owned {
            "cursor_context_length_exceeded"
        } else {
            "cursor_blob_limit"
        };
        if key.len() > 128 || bytes.len() > MAX_BLOB {
            return Err(limit_error.into());
        }
        if STANDARD.encode(Sha256::digest(&bytes)) != key {
            return Err("cursor_blob_digest_mismatch".into());
        }
        if let Some(previous) = self.values.get(&key) {
            return if previous == &bytes {
                Ok(())
            } else {
                Err("cursor_blob_identity_changed".into())
            };
        }
        // Caller history and newly received data have independent entry budgets.
        // A long conversation must not consume the allowance for this response.
        if (caller_owned && self.caller_entries >= MAX_CALLER_BLOBS)
            || (!caller_owned && self.upstream_entries >= MAX_UPSTREAM_BLOBS)
            || self.bytes + bytes.len() > MAX_HISTORY
        {
            return Err(limit_error.into());
        }
        self.bytes += bytes.len();
        if caller_owned {
            self.caller_entries += 1;
        } else {
            self.upstream_entries += 1;
        }
        self.values.insert(key, bytes);
        Ok(())
    }
    pub fn seed(&mut self, bytes: Vec<u8>) -> Result<String, String> {
        let key = STANDARD.encode(Sha256::digest(&bytes));
        self.store(key.clone(), bytes, true)?;
        Ok(key)
    }
    pub fn get(&self, key: &str) -> Result<&[u8], String> {
        self.values
            .get(key)
            .map(Vec::as_slice)
            .ok_or_else(|| "cursor_unknown_blob".into())
    }
}

pub(super) fn proto_bytes(field: u8, bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![field * 8 + 2];
    let mut len = bytes.len();
    while len >= 128 {
        result.push((len as u8 & 127) | 128);
        len >>= 7;
    }
    result.push(len as u8);
    result.extend(bytes);
    result
}

pub(super) struct History {
    pub blobs: Blobs,
    pub state: Value,
    pub system: String,
}

fn content(value: &Value) -> Result<Value, String> {
    if let Some(text) = value.as_str() {
        return Ok(json!(text));
    }
    if value.is_null() {
        return Ok(json!(""));
    }
    let mut parts = Vec::new();
    for part in value.as_array().ok_or("cursor_content_invalid")? {
        match part["type"].as_str() {
            Some("text") => parts.push(json!({"type":"text","text":part["text"].as_str().ok_or("cursor_content_invalid")?})),
            Some("image_url") => {
                let url = part["image_url"]["url"].as_str().ok_or("cursor_image_invalid")?;
                let (metadata, data) = url.strip_prefix("data:").and_then(|url| url.split_once(',')).ok_or("cursor_image_requires_data_url")?;
                let mime = metadata.strip_suffix(";base64").filter(|mime| mime.starts_with("image/")).ok_or("cursor_image_invalid")?;
                STANDARD.decode(data).map_err(|_| "cursor_image_invalid")?;
                parts.push(json!({"type":"image","image":data,"mimeType":mime}));
            }
            _ => return Err("cursor_content_unsupported".into()),
        }
    }
    Ok(json!(parts))
}

fn projected_calls(message: &Value) -> Result<Vec<Value>, String> {
    message["tool_calls"].as_array().into_iter().flatten().map(|call| {
        Ok(json!({"toolCallId":call["id"].as_str().ok_or("cursor_tool_call_invalid")?,"toolName":call["function"]["name"].as_str().ok_or("cursor_tool_call_invalid")?,"args":serde_json::from_str::<Value>(call["function"]["arguments"].as_str().ok_or("cursor_tool_arguments_invalid")?).map_err(|_|"cursor_tool_arguments_invalid")?}))
    }).collect()
}

fn validate_native_context(context: &Value) -> Result<(), String> {
    let invalid = || "cursor_reasoning_context_history_mismatch".to_string();
    let mappings = context["callMap"].as_array().ok_or_else(invalid)?;
    let metadata = context["metadataProofs"].as_object().ok_or_else(invalid)?;
    let mut seen = HashSet::new();
    let mut helper_calls = BTreeMap::new();
    let mut helper_results = HashSet::new();
    let mut text = String::new();
    let mut projected = Vec::new();
    for message in context["nativeMessages"].as_array().ok_or_else(invalid)? {
        let parts = message["content"].as_array().ok_or_else(invalid)?;
        if message["role"] == "tool" {
            if parts.len() != 1 {
                return Err(invalid());
            }
            let part = &parts[0];
            let id = part["toolCallId"].as_str().ok_or_else(invalid)?;
            let outer = helper_calls.get(id).ok_or_else(invalid)?;
            let proof = metadata.get(id).ok_or_else(invalid)?;
            if part["type"] != "tool-result"
                || part["toolName"] != *outer
                || !helper_results.insert(id.to_string())
                || !super::metadata::history_result_matches(message, part, proof)
            {
                return Err(invalid());
            }
            continue;
        }
        if message["role"] != "assistant" {
            return Err(invalid());
        }
        for part in parts {
            match part["type"].as_str() {
                Some("text") => text.push_str(part["text"].as_str().ok_or_else(invalid)?),
                Some("reasoning" | "redacted-reasoning") => {}
                Some("tool-call") => {
                    let raw = part["toolCallId"].as_str().ok_or_else(invalid)?;
                    if !seen.insert(raw.to_string()) {
                        return Err(invalid());
                    }
                    let outer = part["toolName"].as_str().ok_or_else(invalid)?;
                    let args = &part["args"];
                    if let Some(proof) = metadata.get(raw) {
                        if !super::metadata::history_matches(args, outer, proof, raw) {
                            return Err(invalid());
                        }
                        helper_calls.insert(raw.to_string(), outer.to_string());
                        continue;
                    }
                    let mapping = mappings
                        .iter()
                        .find(|mapping| mapping["rawId"] == raw)
                        .ok_or_else(invalid)?;
                    let alias = mapping["alias"].as_str().ok_or_else(invalid)?;
                    if mapping["outerName"] != outer {
                        return Err(invalid());
                    }
                    let direct_mcp = mapping["intent"] == "direct_mcp";
                    let read;
                    let actual = match mapping.get("intent") {
                        Some(Value::String(intent)) if intent == "direct_mcp" => {
                            let name = mapping["name"].as_str().ok_or_else(invalid)?;
                            if !is_direct_mcp_outer(outer)
                                || name.is_empty()
                                || name.len() > 256
                                || alias != tool_alias(name)
                                || mapping["safeId"] != safe_call_id(raw)?
                                || !args.is_object()
                            {
                                return Err(invalid());
                            }
                            // The certified outer name is opaque. Its identity
                            // comes from the saved intent, not a guessed alias.
                            args
                        }
                        Some(Value::String(intent))
                            if intent == "read" && outer == "Read" && mapping["name"] == "read" =>
                        {
                            read = read_arguments(args, false)?;
                            &read
                        }
                        None => match outer {
                            "CallDynamicTool" => wrapper_arguments(args, "namespace", alias)?,
                            "CallMcpTool" => wrapper_arguments(args, "server", alias)?,
                            direct if direct == format!("{NAMESPACE}-{alias}") => args,
                            _ => return Err(invalid()),
                        },
                        _ => return Err(invalid()),
                    };
                    if actual != &mapping["args"] {
                        return Err(invalid());
                    }
                    let contract =
                        &message["providerOptions"]["cursor"]["pendingToolExecutionContracts"][raw];
                    if contract["toolCallId"] != raw
                        || contract["outerToolName"] != outer
                        || if mapping["intent"] == "read" {
                            contract["toolIdentifier"] != "READ" || contract["isDynamic"] != false
                        } else {
                            contract["toolIdentifier"] != "MCP"
                                || direct_mcp
                                    && contract
                                        .get("isDynamic")
                                        .is_some_and(|value| value != false)
                        }
                    {
                        return Err(invalid());
                    }
                    projected.push(json!({"toolCallId":safe_call_id(raw)?,"toolName":mapping["name"],"args":actual}));
                }
                _ => return Err(invalid()),
            }
        }
    }
    if context["text"] != text
        || context["toolCalls"] != json!(projected)
        || projected.len() != mappings.len()
        || helper_calls.len() != metadata.len()
        || helper_results.len() != metadata.len()
    {
        return Err(invalid());
    }
    Ok(())
}

impl History {
    pub fn bind_tools(&mut self, catalog: &Catalog) -> Result<(), String> {
        let instruction = catalog.binding_instruction();
        let roots = self.state["rootPromptMessagesJson"]
            .as_array_mut()
            .expect("history roots");
        let first = roots
            .first()
            .and_then(Value::as_str)
            .map(|key| self.blobs.get(key))
            .transpose()?;
        let mut message = first
            .map(serde_json::from_slice::<Value>)
            .transpose()
            .map_err(|_| "cursor_history_invalid")?;
        if message
            .as_ref()
            .is_some_and(|message| message["role"] == "system")
        {
            let message = message.as_mut().expect("system message");
            let original = message["content"]
                .as_str()
                .ok_or("cursor_system_content_invalid")?;
            message["content"] = json!(format!("{original}\n\n{instruction}"));
            roots[0] = json!(self
                .blobs
                .seed(serde_json::to_vec(message).map_err(|_| "cursor_history_invalid")?)?);
        } else {
            if roots.len() >= MAX_HISTORY_ROOTS {
                return Err("cursor_context_length_exceeded".into());
            }
            let key = self.blobs.seed(
                serde_json::to_vec(&json!({"role":"system","content":instruction}))
                    .map_err(|_| "cursor_history_invalid")?,
            )?;
            roots.insert(0, json!(key));
        }
        if !self.system.is_empty() {
            self.system.push_str("\n\n");
        }
        self.system.push_str(&instruction);
        Ok(())
    }

    pub fn new(body: &Value) -> Result<Self, String> {
        Self::with_mode(body, 1)
    }

    pub fn for_capability(body: &Value) -> Result<Self, String> {
        Self::with_mode(body, 2)
    }

    fn with_mode(body: &Value, mode: u8) -> Result<Self, String> {
        let mut native = Vec::new();
        let mut calls = BTreeMap::<String, (String, String)>::new();
        let mut system = String::new();
        let mut latest_user = None;
        for message in body["messages"]
            .as_array()
            .ok_or("cursor_messages_invalid")?
        {
            match message["role"].as_str() {
                Some("system" | "developer") => {
                    let text = message["content"]
                        .as_str()
                        .ok_or("cursor_system_content_invalid")?;
                    if !system.is_empty() {
                        system.push('\n');
                    }
                    system.push_str(text);
                    native.push(json!({"role":"system","content":text}));
                }
                Some("user") => {
                    let value = content(&message["content"])?;
                    latest_user = Some(if let Some(text) = value.as_str() {
                        text.to_string()
                    } else {
                        value
                            .as_array()
                            .expect("parts")
                            .iter()
                            .filter_map(|part| part["text"].as_str())
                            .collect::<String>()
                    });
                    native.push(json!({"role":"user","content":value}));
                }
                Some("assistant") => {
                    let visible = projected_calls(message)?;
                    let text = message["content"].as_str().unwrap_or_default();
                    if let Some(context) = message.get("nativeContext") {
                        if context["format"] != CONTEXT_FORMAT
                            || context["text"] != text
                            || context["toolCalls"] != json!(visible)
                        {
                            return Err("cursor_reasoning_context_history_mismatch".into());
                        }
                        validate_native_context(context)?;
                        let restored = context["nativeMessages"]
                            .as_array()
                            .ok_or("cursor_reasoning_context_invalid")?;
                        let mappings = context["callMap"]
                            .as_array()
                            .ok_or("cursor_reasoning_context_invalid")?;
                        if mappings.len() != visible.len() {
                            return Err("cursor_reasoning_context_history_mismatch".into());
                        }
                        for (mapping, call) in mappings.iter().zip(&visible) {
                            if mapping["safeId"] != call["toolCallId"]
                                || mapping["name"] != call["toolName"]
                                || mapping["args"] != call["args"]
                            {
                                return Err("cursor_reasoning_context_history_mismatch".into());
                            }
                            let safe = mapping["safeId"]
                                .as_str()
                                .ok_or("cursor_reasoning_context_invalid")?;
                            let raw = mapping["rawId"]
                                .as_str()
                                .ok_or("cursor_reasoning_context_invalid")?;
                            if safe_call_id(raw)? != safe
                                || calls
                                    .insert(
                                        safe.into(),
                                        (
                                            raw.into(),
                                            mapping["outerName"]
                                                .as_str()
                                                .ok_or("cursor_reasoning_context_invalid")?
                                                .into(),
                                        ),
                                    )
                                    .is_some()
                            {
                                return Err("cursor_tool_identity_invalid".into());
                            }
                        }
                        native.extend(restored.iter().cloned());
                    } else {
                        let mut parts = Vec::new();
                        if !text.is_empty() {
                            parts.push(json!({"type":"text","text":text}));
                        }
                        for call in visible {
                            let id = call["toolCallId"].as_str().expect("call id");
                            let name = call["toolName"].as_str().expect("tool name");
                            if calls.insert(id.into(), (id.into(), name.into())).is_some() {
                                return Err("cursor_tool_identity_invalid".into());
                            }
                            parts.push(json!({"type":"tool-call","toolCallId":id,"toolName":name,"args":call["args"]}));
                        }
                        native.push(json!({"role":"assistant","content":parts}));
                    }
                }
                Some("tool") => {
                    let id = message["tool_call_id"]
                        .as_str()
                        .ok_or("cursor_tool_result_invalid")?;
                    let (raw, name) = calls.remove(id).ok_or("cursor_tool_result_missing_call")?;
                    native.push(json!({"role":"tool","content":[{"type":"tool-result","toolCallId":raw,"toolName":name,"result":message["content"]}]}));
                }
                _ => return Err("cursor_message_role_unsupported".into()),
            }
        }
        if !calls.is_empty() {
            return Err("cursor_tool_result_missing".into());
        }
        let latest_user = latest_user.ok_or("cursor_latest_user_missing")?;
        if native.len() > MAX_HISTORY_ROOTS {
            return Err("cursor_context_length_exceeded".into());
        }
        let mut blobs = Blobs::default();
        let roots = native
            .iter()
            .map(|message| {
                blobs.seed(serde_json::to_vec(message).map_err(|_| "cursor_history_invalid")?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut user = proto_bytes(1, latest_user.as_bytes());
        user.extend(proto_bytes(2, uuid::Uuid::new_v4().to_string().as_bytes()));
        user.extend([32, mode]);
        let user_id = blobs.seed(user)?;
        let turn = proto_bytes(
            1,
            &proto_bytes(
                1,
                &STANDARD
                    .decode(user_id)
                    .map_err(|_| "cursor_history_invalid")?,
            ),
        );
        let turn_id = blobs.seed(turn)?;
        Ok(Self {
            blobs,
            state: json!({"mode":mode,"rootPromptMessagesJson":roots,"turns":[turn_id],"pendingToolCalls":[]}),
            system,
        })
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    fn tools() -> Value {
        json!({"tools":[
            {"function":{"name":"first","parameters":{"type":"object"}}},
            {"function":{"name":"second","parameters":{"type":"object"}}}
        ]})
    }

    #[test]
    fn tool_choice_filters_the_frozen_catalog_and_validates_required_names() {
        let mut body = tools();
        body["tool_choice"] = json!("none");
        let catalog = Catalog::new(&body).unwrap();
        assert!(catalog.tools.is_empty());
        assert!(catalog.definitions.is_empty());
        body["tool_choice"] = json!({"type":"function","name":"second"});
        let catalog = Catalog::new(&body).unwrap();
        assert!(catalog.choice.requires_call());
        assert_eq!(catalog.tools.len(), 1);
        assert_eq!(catalog.tools[0].name, "second");
        assert_eq!(catalog.descriptors().len(), 1);
        body["tool_choice"] = json!({"type":"function","name":"missing"});
        assert!(
            matches!(Catalog::new(&body), Err(error) if error == "cursor_tool_choice_not_declared")
        );
        for malformed in [
            json!("any"),
            json!({"type":"custom","name":"second"}),
            json!({"type":"function","name":"second","extra":true}),
        ] {
            body["tool_choice"] = malformed;
            assert!(Catalog::new(&body).is_err());
        }
        body["tool_choice"] = json!("required");
        assert_eq!(Catalog::new(&body).unwrap().tools.len(), 2);
    }

    #[test]
    fn catalog_capacity_is_independent_of_the_step_call_limit() {
        for count in [256, 257, 346] {
            let tools: Vec<Value> = (0..count)
                .map(|index| {
                    json!({"function":{"name":format!("tool_{index}"),"parameters":{"type":"object"}}})
                })
                .collect();
            let catalog = Catalog::new(&json!({"tools":tools})).unwrap();
            assert_eq!(catalog.definitions.len(), count);
            assert_eq!(
                catalog.tools.last().unwrap().name,
                format!("tool_{}", count - 1)
            );
            assert_eq!(
                catalog.request_context()["tools"],
                json!(catalog.definitions)
            );
            assert_eq!(
                catalog.descriptors()[0]["tools"].as_array().unwrap().len(),
                count
            );
        }
    }

    #[test]
    fn catalog_still_rejects_invalid_definitions_and_oversized_payloads() {
        for invalid in [
            json!({"function":{"name":"","parameters":{"type":"object"}}}),
            json!({"function":{"name":"x".repeat(257),"parameters":{"type":"object"}}}),
            json!({"function":{"parameters":{"type":"object"}}}),
        ] {
            assert!(matches!(
                Catalog::new(&json!({"tools":[invalid]})),
                Err(error) if error == "cursor_tool_definition_invalid"
            ));
        }
        let duplicate = tools()["tools"][0].clone();
        assert!(matches!(
            Catalog::new(&json!({"tools":[duplicate.clone(),duplicate]})),
            Err(error) if error == "cursor_tool_definition_invalid"
        ));
        for invalid in [
            json!({"name":"entry","parameters":null}),
            json!({"name":"entry","parameters":{"type":"object","description":"x".repeat(256 * 1024)}}),
            json!({"name":"entry","description":"x".repeat(256 * 1024 + 1),"parameters":{"type":"object"}}),
        ] {
            assert!(matches!(
                Catalog::new(&json!({"tools":[{"function":invalid}]})),
                Err(error) if error == "cursor_tool_schema_invalid"
            ));
        }
        let tools: Vec<Value> = (0..9)
            .map(|index| {
                json!({"function":{"name":format!("tool_{index}"),"description":"x".repeat(256 * 1024),"parameters":{"type":"object"}}})
            })
            .collect();
        assert!(matches!(
            Catalog::new(&json!({"tools":tools})),
            Err(error) if error == "cursor_tool_catalog_too_large"
        ));
    }

    #[test]
    fn image_history_admits_four_images_at_the_declared_base64_limit() {
        let data = STANDARD.encode(vec![0_u8; 3_932_160]);
        assert_eq!(data.len(), 5_242_880);
        let part =
            json!({"type":"image_url","image_url":{"url":format!("data:image/png;base64,{data}")}});
        let history = History::new(&json!({"messages":[{"role":"user","content":[part.clone(),part.clone(),part.clone(),part]}]})).unwrap();
        let root = history.state["rootPromptMessagesJson"][0].as_str().unwrap();
        let bytes = history.blobs.get(root).unwrap();
        assert!(bytes.len() > 20 * 1024 * 1024);
        assert!(bytes.len() <= MAX_BLOB);
        let frame = super::super::bridge::encode_frame(
            json!({"kvClientMessage":{"id":1,"getBlobResult":{"blobData":STANDARD.encode(bytes)}}}),
        )
        .unwrap();
        assert!(frame.len() < 32 * 1024 * 1024);
    }
}
