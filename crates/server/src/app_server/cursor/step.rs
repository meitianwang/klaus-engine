//! One bounded provider step. Execution requests are certified and returned to
//! the local session only after the upstream request has been closed.
use super::{
    bridge, changes, credential,
    step_history::{
        safe_call_id, Blobs, Catalog, History, CONTEXT_FORMAT, MAX_HISTORY, MAX_TOOLS, NAMESPACE,
    },
};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use futures::TryStreamExt;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use tokio::sync::mpsc;

pub(super) struct Certified {
    pub text: String,
    pub reasoning: String,
    pub calls: Vec<Value>,
    pub context: Value,
    pub usage: Value,
}

fn uint32_or_default(value: &Value, key: &str) -> Result<u32, String> {
    if !value.is_object() {
        return Err("cursor_native_scalar_invalid".into());
    }
    match value.get(key) {
        None => Ok(0),
        Some(value) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| "cursor_native_scalar_invalid".into()),
    }
}

pub(super) fn string_or_default<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    if !value.is_object() {
        return Err("cursor_native_scalar_invalid".into());
    }
    match value.get(key) {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .ok_or_else(|| "cursor_native_scalar_invalid".into()),
    }
}

fn safe_shape(value: &Value, known: &[&str]) -> Value {
    let kind = |value: &Value| match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    let fields = value
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| known.contains(&key.as_str()))
        .take(8)
        .map(|(key, value)| json!({"field":key,"type":kind(value)}))
        .collect::<Vec<_>>();
    let unknown = value
        .as_object()
        .map(|object| {
            object
                .keys()
                .filter(|key| !known.contains(&key.as_str()))
                .count()
        })
        .unwrap_or(0);
    json!({"type":kind(value),"fields":fields,"unknownFieldCount":unknown})
}

fn rejected_exec(exec: &Value) {
    let known = [
        "id",
        "execId",
        "machineId",
        "spanContext",
        "acceptHookAdditionalContexts",
        "shellArgs",
        "writeArgs",
        "deleteArgs",
        "grepArgs",
        "readArgs",
        "redactedReadArgs",
        "lsArgs",
        "diagnosticsArgs",
        "requestContextArgs",
        "mcpArgs",
        "shellStreamArgs",
        "backgroundShellSpawnArgs",
        "listMcpResourcesExecArgs",
        "readMcpResourceExecArgs",
        "mcpStateExecArgs",
        "fetchArgs",
        "recordScreenArgs",
        "computerUseArgs",
        "writeShellStdinArgs",
        "executeHookArgs",
        "subagentArgs",
        "forceBackgroundShellArgs",
        "forceBackgroundSubagentArgs",
        "subagentAwaitArgs",
        "smartModeClassifierArgs",
        "canvasDiagnosticsArgs",
        "shellAllowlistPrecheckArgs",
        "mcpAllowlistPrecheckArgs",
        "webFetchAllowlistPrecheckArgs",
        "gitDiffRequest",
        "piReadArgs",
        "piBashArgs",
        "piEditArgs",
        "piWriteArgs",
        "piGrepArgs",
        "piFindArgs",
        "piLsArgs",
        "miniSweAgentBashArgs",
        "conversationSearchArgs",
        "agentStoreConflictArgs",
    ];
    let cases = exec
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| {
            known.contains(&key.as_str())
                && (key.ends_with("Args") || key.as_str() == "gitDiffRequest")
        })
        .map(|(key, _)| key.as_str())
        .take(2)
        .collect::<Vec<_>>();
    let case = if cases.len() == 1 {
        cases[0]
    } else if cases.is_empty() {
        "unrecognized"
    } else {
        "multiple"
    };
    eprintln!(
        "[Cursor] disallowed native execution kind={case} shape={}",
        safe_shape(exec, &known)
    );
}

struct Batch {
    model_call_id: Value,
    started: BTreeMap<String, Value>,
    completed: HashSet<String>,
    count: Option<usize>,
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            model_call_id: Value::Null,
            started: BTreeMap::new(),
            completed: HashSet::new(),
            count: None,
        }
    }
}

struct Step {
    catalog: Catalog,
    blobs: Blobs,
    request_context: Value,
    held: BTreeMap<String, Value>,
    pending: Vec<Value>,
    text: String,
    batch: Batch,
    ended: bool,
    terminal_native: Vec<Value>,
    usage: Value,
    events: usize,
    initial_roots: Vec<Value>,
    latest_roots: Option<Vec<Value>>,
    metadata: BTreeMap<String, Value>,
    metadata_replies: BTreeMap<String, Value>,
}

fn own_args<'a>(
    catalog: &'a Catalog,
    args: &Value,
) -> Result<&'a super::step_history::Tool, String> {
    let allowed = [
        "name",
        "args",
        "toolCallId",
        "providerIdentifier",
        "toolName",
        "serverIdentifier",
        "smartModeApprovalOnly",
        "skipApproval",
        "symbolicPathArguments",
    ];
    let rejected = |reason: &str| {
        let alias = args["toolName"].as_str().unwrap_or_default();
        let diagnostic = json!({
            "reason":reason,
            "aliasKnown":catalog.tools.iter().any(|tool| tool.alias == alias),
            "callerNameKnown":catalog.tools.iter().any(|tool| tool.name == alias),
            "catalogSize":catalog.tools.len(),
            "providerMatches":args["providerIdentifier"] == NAMESPACE,
            "nameMatches":args["name"] == format!("{NAMESPACE}-{alias}"),
            "serverMatches":args.get("serverIdentifier").map(|server| server == NAMESPACE),
            "smartModeApprovalOnly":args["smartModeApprovalOnly"].as_bool(),
            "skipApproval":args["skipApproval"].as_bool(),
            "symbolicPathCount":args["symbolicPathArguments"].as_array().map(Vec::len),
            "shape":safe_shape(args, &allowed)
        });
        eprintln!("[Cursor] native tool mismatch intent={diagnostic}");
        "cursor_tool_not_owned".to_string()
    };
    if args
        .as_object()
        .is_none_or(|args| args.keys().any(|key| !allowed.contains(&key.as_str())))
        || args
            .get("smartModeApprovalOnly")
            .is_some_and(|value| value != false)
        || args.get("skipApproval").is_some_and(|value| value != false)
        || args
            .get("symbolicPathArguments")
            .is_some_and(|value| value.as_array().is_none_or(|array| !array.is_empty()))
    {
        return Err(rejected("metadata"));
    }
    let alias = args["toolName"]
        .as_str()
        .ok_or("cursor_tool_identity_invalid")?;
    let tool = catalog.by_alias(alias).map_err(|_| rejected("alias"))?;
    if args["providerIdentifier"] != NAMESPACE
        || args["name"] != format!("{NAMESPACE}-{alias}")
        || args
            .get("serverIdentifier")
            .is_some_and(|server| server != NAMESPACE)
        || !args["args"].is_object()
    {
        return Err(rejected("identity"));
    }
    safe_call_id(
        args["toolCallId"]
            .as_str()
            .ok_or("cursor_tool_identity_invalid")?,
    )?;
    Ok(tool)
}

impl Step {
    fn tool_kind<'a>(&self, call: &'a Value) -> Result<&'a str, String> {
        let map = call.as_object().ok_or("cursor_native_tool_not_allowed")?;
        let kinds = map
            .keys()
            .filter(|key| key.ends_with("ToolCall"))
            .collect::<Vec<_>>();
        if kinds.len() != 1
            || !matches!(
                kinds[0].as_str(),
                "mcpToolCall" | "getMcpToolsToolCall" | "readToolCall"
            )
            || map.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "mcpToolCall"
                        | "getMcpToolsToolCall"
                        | "readToolCall"
                        | "toolCallId"
                        | "startedAtMs"
                        | "completedAtMs"
                        | "hookAdditionalContexts"
                )
            })
            || call
                .get("hookAdditionalContexts")
                .is_some_and(|value| value.as_array().is_none_or(|array| !array.is_empty()))
        {
            let known = [
                "readToolCall",
                "shellToolCall",
                "editToolCall",
                "deleteToolCall",
                "globToolCall",
                "grepToolCall",
                "lsToolCall",
                "readLintsToolCall",
                "askQuestionToolCall",
                "switchModeToolCall",
                "generateImageToolCall",
                "generateVideoToolCall",
                "webSearchToolCall",
                "webFetchToolCall",
                "fetchToolCall",
                "taskToolCall",
                "getMcpToolsToolCall",
                "mcpToolCall",
                "mcpAuthToolCall",
                "updateTodosToolCall",
                "readTodosToolCall",
                "computerUseToolCall",
                "writeShellStdinToolCall",
            ];
            let kind = if kinds.len() == 1 {
                known
                    .iter()
                    .find(|known| **known == kinds[0])
                    .copied()
                    .unwrap_or("unrecognized")
            } else if kinds.is_empty() {
                "missing"
            } else {
                "multiple"
            };
            let mut fields = known.to_vec();
            fields.extend([
                "toolCallId",
                "startedAtMs",
                "completedAtMs",
                "hookAdditionalContexts",
            ]);
            eprintln!(
                "[Cursor] disallowed native tool kind={kind} shape={}",
                safe_shape(call, &fields)
            );
            return Err("cursor_native_tool_not_allowed".into());
        }
        Ok(kinds[0])
    }
    fn new(catalog: Catalog, history: History) -> Self {
        let request_context = catalog.request_context();
        Self {
            catalog,
            blobs: history.blobs,
            request_context,
            held: BTreeMap::new(),
            pending: Vec::new(),
            text: String::new(),
            batch: Batch::default(),
            ended: false,
            terminal_native: Vec::new(),
            usage: json!({}),
            events: 0,
            initial_roots: history.state["rootPromptMessagesJson"]
                .as_array()
                .expect("history roots")
                .clone(),
            latest_roots: None,
            metadata: BTreeMap::new(),
            metadata_replies: BTreeMap::new(),
        }
    }

    fn metadata_call<'a>(&self, call: &'a Value) -> Result<(&'a Value, &'a str), String> {
        let args = &call["getMcpToolsToolCall"]["args"];
        let id = args["toolCallId"]
            .as_str()
            .ok_or("cursor_metadata_call_invalid")?;
        safe_call_id(id)?;
        self.catalog.validate_metadata_args(args)?;
        Ok((args, id))
    }

    fn is_metadata_call(&self, call: &Value) -> bool {
        call.get("getMcpToolsToolCall").is_some()
            || call["mcpToolCall"]["args"]["toolName"]
                .as_str()
                .is_some_and(|alias| {
                    matches!(alias, "GetDynamicTools" | "GetMcpTools")
                        && self.catalog.by_alias(alias).is_err()
                })
    }

    fn started(&mut self, update: &Value) -> Result<(), String> {
        self.reset_completed_metadata_batch();
        let call = &update["toolCall"];
        self.tool_kind(call)?;
        let model_id = update
            .get("modelCallId")
            .filter(|id| {
                id.as_str()
                    .is_some_and(|id| !id.is_empty() && id.len() <= 2048)
            })
            .cloned()
            .ok_or("cursor_tool_batch_identity_missing")?;
        if self.batch.model_call_id != Value::Null && model_id != self.batch.model_call_id {
            return Err("cursor_tool_batch_mismatch".into());
        }
        self.batch.model_call_id = model_id;
        let id = if call.get("mcpToolCall").is_some() {
            let args = &call["mcpToolCall"]["args"];
            if self.catalog.mcp_metadata(args)?.is_none() {
                own_args(&self.catalog, args)?;
            }
            args["toolCallId"].as_str().expect("validated").to_string()
        } else if let Some(read) = call.get("readToolCall") {
            self.catalog.declared_read()?;
            if read
                .as_object()
                .is_none_or(|read| read.len() != 1 || !read.contains_key("args"))
            {
                return Err(super::tool_mismatch(
                    "read_started_shape",
                    read,
                    &json!({"args":{}}),
                ));
            }
            super::step_history::read_arguments(&read["args"], false)?;
            let id = call["toolCallId"]
                .as_str()
                .ok_or("cursor_tool_identity_invalid")?;
            safe_call_id(id)?;
            id.to_string()
        } else if call.get("getMcpToolsToolCall").is_some() {
            self.metadata_call(call)?.1.to_string()
        } else {
            return Err("cursor_native_tool_not_allowed".into());
        };
        if self.batch.started.len() >= MAX_TOOLS
            || self.batch.started.insert(id, call.clone()).is_some()
        {
            return Err("cursor_tool_batch_duplicate".into());
        }
        Ok(())
    }

    fn completed(&mut self, update: &Value) -> Result<(), String> {
        let call = &update["toolCall"];
        self.tool_kind(call)?;
        if update.get("modelCallId").cloned().unwrap_or(Value::Null) != self.batch.model_call_id {
            return Err("cursor_tool_batch_mismatch".into());
        }
        if let Some(mcp) = call.get("mcpToolCall") {
            let args = &mcp["args"];
            let generated = self
                .catalog
                .mcp_metadata(args)?
                .ok_or("cursor_tool_executed_upstream")?;
            let id = args["toolCallId"].as_str().expect("validated");
            let started = self
                .batch
                .started
                .get(id)
                .ok_or("cursor_metadata_call_not_started")?;
            let proof = self
                .metadata_replies
                .get(id)
                .ok_or("cursor_metadata_call_not_replied")?;
            if started["mcpToolCall"]["args"] != *args || proof != &generated {
                return Err("cursor_metadata_call_mismatch".into());
            }
            if mcp.as_object().is_none_or(|mcp| {
                mcp.len() != 2 || !mcp.contains_key("args") || !mcp.contains_key("result")
            }) || !super::metadata::mcp_result_matches(&mcp["result"], &proof["value"])
            {
                return Err("cursor_metadata_result_mismatch".into());
            }
            if !self.batch.completed.insert(id.to_string()) {
                return Err("cursor_tool_batch_duplicate".into());
            }
            if self.metadata.len() >= MAX_TOOLS || self.metadata.contains_key(id) {
                return Err("cursor_metadata_call_duplicate".into());
            }
            let proof = self.metadata_replies.remove(id).expect("validated reply");
            self.metadata.insert(id.to_string(), proof);
            return Ok(());
        }
        if call.get("readToolCall").is_some() {
            return Err("cursor_tool_executed_upstream".into());
        }
        let (args, id) = self.metadata_call(call)?;
        let started = self
            .batch
            .started
            .get(id)
            .ok_or("cursor_metadata_call_not_started")?;
        if started["getMcpToolsToolCall"]["args"] != *args {
            return Err("cursor_metadata_call_mismatch".into());
        }
        let result = &call["getMcpToolsToolCall"]["result"];
        if result.as_object().is_none_or(|result| result.len() != 1) {
            return Err("cursor_metadata_result_invalid".into());
        }
        let success = result
            .get("success")
            .ok_or("cursor_metadata_result_invalid")?;
        if success.get("outputFilePath").is_some()
            || success
                .as_object()
                .is_none_or(|map| map.keys().any(|key| key != "content"))
        {
            return Err("cursor_metadata_result_invalid".into());
        }
        let encoded = success["content"]
            .as_str()
            .ok_or("cursor_metadata_result_invalid")?;
        if encoded.len() > 2 * 1024 * 1024 {
            return Err("cursor_metadata_result_too_large".into());
        }
        let value: Value =
            serde_json::from_str(encoded).map_err(|_| "cursor_metadata_result_invalid")?;
        if !super::step_history::same_metadata(&value, &self.catalog.metadata_result(args, false)?)
            && !super::step_history::same_metadata(
                &value,
                &self.catalog.metadata_result(args, true)?,
            )
        {
            return Err("cursor_metadata_result_mismatch".into());
        }
        let id = id.to_string();
        if self.metadata.len() >= MAX_TOOLS
            || self
                .metadata
                .insert(id.clone(), json!({"args":args,"value":value}))
                .is_some()
        {
            return Err("cursor_metadata_call_duplicate".into());
        }
        if !self.batch.completed.insert(id) {
            return Err("cursor_tool_batch_duplicate".into());
        }
        Ok(())
    }

    fn checkpoint(&mut self, checkpoint: &Value) -> Result<(), String> {
        if let Some(roots) = checkpoint.get("rootPromptMessagesJson") {
            let roots = roots.as_array().ok_or("cursor_checkpoint_invalid")?;
            if roots.len() > super::step_history::MAX_CHECKPOINT_ROOTS
                || roots.len() < self.initial_roots.len()
            {
                return Err("cursor_checkpoint_history_changed".into());
            }
            if roots
                .iter()
                .any(|root| root.as_str().is_none_or(|key| key.len() > 128))
            {
                return Err("cursor_checkpoint_invalid".into());
            }
            self.latest_roots = Some(roots.clone());
        }
        self.pending.clear();
        if let Some(pending) = checkpoint.get("pendingToolCalls") {
            let pending = pending.as_array().ok_or("cursor_native_message_invalid")?;
            if pending.len() > MAX_TOOLS {
                return Err("cursor_native_message_limit".into());
            }
            self.pending = pending
                .iter()
                .map(|value| {
                    let text = value.as_str().ok_or("cursor_native_message_invalid")?;
                    if text.len() > 4 * 1024 * 1024 {
                        return Err("cursor_native_message_limit".into());
                    }
                    serde_json::from_str(text).map_err(|_| "cursor_native_message_invalid".into())
                })
                .collect::<Result<_, String>>()?;
        }
        Ok(())
    }

    fn certify(&self, native: &[Value], tools: bool) -> Result<Certified, String> {
        if !self.metadata_replies.is_empty() {
            return Err("cursor_metadata_completion_missing".into());
        }
        if native.is_empty() || native.len() > MAX_TOOLS {
            return Err("cursor_native_assistant_missing".into());
        }
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        let mut mappings = Vec::new();
        let mut ids = HashSet::new();
        let mut metadata_calls = BTreeMap::<String, String>::new();
        let mut metadata_results = HashSet::new();
        for message in native {
            if message["role"] == "tool" {
                let content = message["content"]
                    .as_array()
                    .ok_or("cursor_metadata_history_invalid")?;
                if content.len() != 1 {
                    return Err("cursor_metadata_history_invalid".into());
                }
                let part = &content[0];
                let id = part["toolCallId"]
                    .as_str()
                    .ok_or("cursor_metadata_history_invalid")?;
                let name = metadata_calls
                    .get(id)
                    .ok_or("cursor_metadata_history_invalid")?;
                let expected = self
                    .metadata
                    .get(id)
                    .ok_or("cursor_metadata_history_invalid")?;
                if part["type"] != "tool-result"
                    || part["toolName"] != *name
                    || !metadata_results.insert(id.to_string())
                    || !super::metadata::history_result_matches(message, part, expected)
                {
                    return Err("cursor_metadata_history_invalid".into());
                }
                continue;
            }
            if message["role"] != "assistant" {
                return Err("cursor_native_message_invalid".into());
            }
            for part in message["content"]
                .as_array()
                .ok_or("cursor_native_message_invalid")?
            {
                match part["type"].as_str() {
                    Some("text") => text.push_str(
                        part["text"]
                            .as_str()
                            .ok_or("cursor_native_message_invalid")?,
                    ),
                    Some("reasoning") => reasoning.push_str(
                        part["text"]
                            .as_str()
                            .ok_or("cursor_native_message_invalid")?,
                    ),
                    Some("redacted-reasoning") => {}
                    Some("tool-call") => {
                        let raw = part["toolCallId"]
                            .as_str()
                            .ok_or("cursor_tool_identity_invalid")?;
                        if !ids.insert(raw.to_string()) {
                            return Err("cursor_tool_batch_duplicate".into());
                        }
                        if let Some(metadata) = self.metadata.get(raw) {
                            let outer = part["toolName"]
                                .as_str()
                                .ok_or("cursor_metadata_history_invalid")?;
                            let args = &part["args"];
                            if !super::metadata::history_matches(args, outer, metadata, raw) {
                                return Err("cursor_metadata_history_invalid".into());
                            }
                            metadata_calls.insert(raw.to_string(), outer.into());
                            continue;
                        }
                        if !tools {
                            return Err("cursor_native_content_not_allowed".into());
                        }
                        let held = self.held.get(raw).ok_or_else(|| {
                            super::tool_mismatch("pending_held_missing", part, &Value::Null)
                        })?;
                        let read = held.get("readArgs");
                        let tool = if read.is_some() {
                            self.catalog.declared_read()?
                        } else {
                            own_args(&self.catalog, held)?
                        };
                        let outer = part["toolName"].as_str().ok_or_else(|| {
                            super::tool_mismatch("pending_outer_missing", part, &Value::Null)
                        })?;
                        let args = &part["args"];
                        let direct_mcp =
                            read.is_none() && super::step_history::is_direct_mcp_outer(outer);
                        let projected;
                        let actual = match outer {
                            "Read" if read.is_some() => {
                                projected = super::step_history::read_arguments(args, false)?;
                                &projected
                            }
                            "CallDynamicTool" if read.is_none() => {
                                super::step_history::wrapper_arguments(
                                    args,
                                    "namespace",
                                    &tool.alias,
                                )?
                            }
                            "CallMcpTool" if read.is_none() => {
                                super::step_history::wrapper_arguments(args, "server", &tool.alias)?
                            }
                            _ if direct_mcp => args,
                            _ => {
                                return Err(super::tool_mismatch(
                                    "pending_outer_unsupported",
                                    part,
                                    held,
                                ))
                            }
                        };
                        let expected = if let Some(read) = read {
                            super::step_history::read_arguments(read, true)?
                        } else {
                            held["args"].clone()
                        };
                        if actual != &expected || !actual.is_object() {
                            return Err(super::tool_mismatch(
                                "pending_arguments",
                                actual,
                                &expected,
                            ));
                        }
                        let contract = &message["providerOptions"]["cursor"]
                            ["pendingToolExecutionContracts"][raw];
                        if contract["toolCallId"] != raw
                            || contract["outerToolName"] != outer
                            || if read.is_some() {
                                contract["toolIdentifier"] != "READ"
                                    || contract["isDynamic"] != false
                            } else {
                                contract["toolIdentifier"] != "MCP"
                                    || direct_mcp
                                        && contract
                                            .get("isDynamic")
                                            .is_some_and(|value| value != false)
                            }
                        {
                            return Err("cursor_native_tool_contract_mismatch".into());
                        }
                        let safe = safe_call_id(raw)?;
                        calls.push(json!({"toolCallId":safe,"toolName":tool.name,"args":actual}));
                        let mut mapping = json!({"safeId":safe,"rawId":raw,"outerName":outer,"name":tool.name,"alias":tool.alias,"args":actual});
                        if read.is_some() {
                            mapping["intent"] = json!("read");
                        } else if direct_mcp {
                            mapping["intent"] = json!("direct_mcp");
                        }
                        mappings.push(mapping);
                    }
                    _ => return Err("cursor_native_content_not_allowed".into()),
                }
            }
        }
        if metadata_calls.len() != self.metadata.len()
            || metadata_results.len() != self.metadata.len()
        {
            return Err("cursor_metadata_history_missing".into());
        }
        if text != self.text || tools && calls.len() != self.held.len() {
            eprintln!(
                "[Cursor] native response mismatch site=checkpoint textMatches={} nativeTextBytes={} streamedTextBytes={} nativeCalls={} heldCalls={}",
                text == self.text, text.len(), self.text.len(), calls.len(), self.held.len()
            );
            return Err("cursor_native_response_mismatch".into());
        }
        if self.catalog.choice.requires_call() && calls.is_empty() {
            return Err("cursor_required_tool_missing".into());
        }
        let context = json!({"format":CONTEXT_FORMAT,"text":text,"toolCalls":calls,"nativeMessages":native,"callMap":mappings,"metadataProofs":self.metadata});
        if context.to_string().len() > 8 * 1024 * 1024 {
            return Err("cursor_reasoning_context_too_large".into());
        }
        Ok(Certified {
            text,
            reasoning,
            calls,
            context,
            usage: self.usage.clone(),
        })
    }

    fn reset_completed_metadata_batch(&mut self) {
        if self.batch.count.is_some_and(|count| {
            count > 0
                && self.batch.started.len() == count
                && self.batch.completed.len() == count
                && self
                    .batch
                    .started
                    .values()
                    .all(|call| self.is_metadata_call(call))
        }) {
            self.batch = Batch::default();
        }
    }

    fn complete_metadata_history(&self, native: &mut Vec<Value>) -> Result<(), String> {
        let mut calls = BTreeMap::new();
        let mut results = HashSet::new();
        for (index, message) in native.iter().enumerate() {
            for part in message["content"].as_array().into_iter().flatten() {
                let Some(raw) = part["toolCallId"].as_str() else {
                    continue;
                };
                let Some(proof) = self.metadata.get(raw) else {
                    continue;
                };
                if message["role"] == "assistant" && part["type"] == "tool-call" {
                    let outer = part["toolName"]
                        .as_str()
                        .ok_or("cursor_metadata_history_invalid")?;
                    if !super::metadata::history_matches(&part["args"], outer, proof, raw) {
                        return Err("cursor_metadata_history_invalid".into());
                    }
                    if calls
                        .insert(raw.to_string(), (index, outer.to_string()))
                        .is_some()
                    {
                        return Err("cursor_metadata_call_duplicate".into());
                    }
                } else if message["role"] == "tool" && part["type"] == "tool-result" {
                    results.insert(raw.to_string());
                }
            }
        }
        let pending_index = native.len().checked_sub(1);
        for (raw, (index, outer)) in calls {
            if !results.contains(&raw) {
                if Some(index) != pending_index {
                    return Err("cursor_metadata_history_missing".into());
                }
                // A parallel batch may checkpoint its signed assistant before
                // committing completed discovery results. Only the frozen,
                // already-verified local catalog result can be reconstructed.
                let value = &self.metadata[&raw]["value"];
                native.push(json!({"role":"tool","content":[{"type":"tool-result","toolCallId":raw,"toolName":outer,"result":value.to_string()}]}));
            }
        }
        Ok(())
    }

    fn ready(&mut self) -> Result<Option<Certified>, String> {
        if let Some(count) = self.batch.count {
            if self.batch.started.len() > count || self.held.len() > count {
                return Err("cursor_tool_batch_invalid".into());
            }
            if count != self.batch.started.len() {
                return Ok(None);
            }
            let helpers = self
                .batch
                .started
                .values()
                .filter(|call| self.is_metadata_call(call))
                .count();
            if helpers == count && count > 0 {
                if self.batch.completed.len() == count {
                    self.batch = Batch::default();
                }
                return Ok(None);
            }
            if count > helpers
                && self.held.len() == count - helpers
                && self.batch.completed.len() == helpers
                && !self.pending.is_empty()
            {
                for (id, call) in &self.batch.started {
                    if self.is_metadata_call(call) {
                        if !self.metadata.contains_key(id) {
                            return Err("cursor_metadata_history_missing".into());
                        }
                        continue;
                    }
                    let held = self.held.get(id).ok_or_else(|| {
                        super::tool_mismatch("batch_held_missing", call, &Value::Null)
                    })?;
                    if let Some(read) = held.get("readArgs") {
                        let started = &call["readToolCall"]["args"];
                        if call["toolCallId"] != *id
                            || super::step_history::read_arguments(started, false)?
                                != super::step_history::read_arguments(read, true)?
                        {
                            return Err(super::tool_mismatch(
                                "read_batch_arguments",
                                started,
                                read,
                            ));
                        }
                    } else if Some(held) != call["mcpToolCall"].get("args") {
                        return Err(super::tool_mismatch(
                            "mcp_batch_arguments",
                            &call["mcpToolCall"]["args"],
                            held,
                        ));
                    }
                }
                let Some(mut native) = self.new_roots()? else {
                    return Ok(None);
                };
                native.extend(self.pending.iter().cloned());
                let checkpoint_calls = native
                    .iter()
                    .filter(|message| message["role"] == "assistant")
                    .filter_map(|message| message["content"].as_array())
                    .flatten()
                    .filter(|part| part["type"] == "tool-call")
                    .map(|part| {
                        let id = part["toolCallId"]
                            .as_str()
                            .ok_or("cursor_tool_identity_invalid")?;
                        if !self.metadata.contains_key(id) && !self.held.contains_key(id) {
                            return Err(super::tool_mismatch(
                                "pending_held_missing",
                                part,
                                &Value::Null,
                            ));
                        }
                        Ok(id)
                    })
                    .collect::<Result<HashSet<_>, String>>()?;
                // Discovery snapshots can outlive their completed batch. Wait
                // for every held intent's certificate regardless of packet order.
                if self
                    .held
                    .keys()
                    .any(|id| !checkpoint_calls.contains(id.as_str()))
                {
                    return Ok(None);
                }
                self.complete_metadata_history(&mut native)?;
                return self.certify(&native, true).map(Some);
            }
        }
        Ok(None)
    }

    fn finish(&mut self) -> Result<Certified, String> {
        if !self.ended
            || !self.held.is_empty()
            || !self.pending.is_empty()
            || !self.metadata_replies.is_empty()
        {
            return Err("cursor_inference_incomplete".into());
        }
        self.latest_roots
            .as_ref()
            .ok_or("cursor_terminal_checkpoint_missing")?;
        self.terminal_native = self.new_roots()?.ok_or("cursor_checkpoint_unresolved")?;
        if self
            .terminal_native
            .last()
            .is_none_or(|message| message["role"] != "assistant")
        {
            return Err("cursor_terminal_native_history_mismatch".into());
        }
        self.certify(&self.terminal_native, false)
    }

    fn new_roots(&self) -> Result<Option<Vec<Value>>, String> {
        let Some(roots) = &self.latest_roots else {
            return Ok(None);
        };
        let mut native = Vec::new();
        for (index, root) in roots.iter().enumerate() {
            let key = root.as_str().ok_or("cursor_checkpoint_invalid")?;
            let bytes = match self.blobs.get(key) {
                Ok(bytes) => bytes,
                Err(error) if error == "cursor_unknown_blob" => return Ok(None),
                Err(error) => return Err(error),
            };
            if let Some(original) = self.initial_roots.get(index) {
                if original != root {
                    let original = self
                        .blobs
                        .get(original.as_str().ok_or("cursor_checkpoint_invalid")?)?;
                    let original: Value = serde_json::from_slice(original)
                        .map_err(|_| "cursor_native_message_invalid")?;
                    let updated: Value = serde_json::from_slice(bytes)
                        .map_err(|_| "cursor_native_message_invalid")?;
                    // Content hashes may change when identical JSON is encoded
                    // with different field ordering. The original caller-owned
                    // signed message remains the replay source.
                    if original != updated {
                        return Err("cursor_checkpoint_history_changed".into());
                    }
                }
            } else {
                native.push(
                    serde_json::from_slice(bytes).map_err(|_| "cursor_native_message_invalid")?,
                );
            }
        }
        Ok(Some(native))
    }

    fn receive(&mut self, value: &Value) -> Result<Vec<Value>, String> {
        self.events += 1;
        if self.events > 32768 {
            return Err("cursor_response_too_large".into());
        }
        let root = value.as_object().ok_or("cursor_native_packet_invalid")?;
        if root
            .keys()
            .filter(|key| key.as_str() != "ttftBreakdown")
            .count()
            != 1
            || root.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "interactionUpdate"
                        | "execServerMessage"
                        | "conversationCheckpointUpdate"
                        | "kvServerMessage"
                        | "ttftBreakdown"
                )
            })
        {
            return Err("cursor_native_packet_not_allowed".into());
        }
        // ttftBreakdown is upstream timing telemetry riding beside a native
        // packet. Klaus discards it after receipt, so its field set is not
        // validated: telemetry drift must not abort an in-flight turn.
        let mut replies = Vec::new();
        if let Some(kv) = value.get("kvServerMessage") {
            let map = kv.as_object().ok_or("cursor_blob_invalid")?;
            if map.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "id" | "getBlobArgs" | "setBlobArgs" | "spanContext"
                )
            }) || kv.get("setBlobArgs").is_some() == kv.get("getBlobArgs").is_some()
            {
                return Err("cursor_blob_invalid".into());
            }
            let id = uint32_or_default(kv, "id")?;
            let message = if let Some(set) = kv.get("setBlobArgs") {
                let key = set["blobId"].as_str().ok_or("cursor_blob_invalid")?;
                let data = string_or_default(set, "blobData")?;
                if data.len() > super::step_history::MAX_BLOB.div_ceil(3) * 4 {
                    return Err("cursor_blob_limit".into());
                }
                self.blobs.insert(
                    key.to_string(),
                    STANDARD.decode(data).map_err(|_| "cursor_blob_invalid")?,
                )?;
                json!({"setBlobResult":{}})
            } else if let Some(get) = kv.get("getBlobArgs") {
                json!({"getBlobResult":{"blobData":STANDARD.encode(self.blobs.get(get["blobId"].as_str().ok_or("cursor_blob_invalid")?)?)}})
            } else {
                return Err("cursor_blob_operation_not_allowed".into());
            };
            let mut reply = message;
            reply["id"] = json!(id);
            replies.push(json!({"kvClientMessage":reply}));
        }
        if let Some(exec) = value.get("execServerMessage") {
            let map = exec
                .as_object()
                .ok_or("cursor_native_execution_not_allowed")?;
            let args = map
                .keys()
                .filter(|key| key.ends_with("Args"))
                .collect::<Vec<_>>();
            if args.len() != 1
                || map.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "id" | "execId"
                            | "spanContext"
                            | "machineId"
                            | "requestContextArgs"
                            | "mcpStateExecArgs"
                            | "mcpArgs"
                            | "readArgs"
                            | "acceptHookAdditionalContexts"
                    )
                })
                || string_or_default(exec, "execId")?.len() > 2048
                || exec
                    .get("acceptHookAdditionalContexts")
                    .is_some_and(|value| !value.is_boolean())
                || exec.get("machineId").is_some()
            {
                rejected_exec(exec);
                return Err("cursor_native_execution_not_allowed".into());
            }
            let id = uint32_or_default(exec, "id")?;
            let exec_id = string_or_default(exec, "execId")?;
            if exec.get("requestContextArgs").is_some() {
                replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"requestContextResult":{"success":{"requestContext":self.request_context}}}}));
            } else if exec.get("mcpStateExecArgs").is_some() {
                // Discovery is a frozen projection of this step's caller tools.
                // It never enumerates or refreshes machine-wide MCP connections.
                let state = self.catalog.frozen_state(&exec["mcpStateExecArgs"])?;
                replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"mcpStateExecResult":{"success":{"servers":state}}}}));
            } else if let Some(args) = exec.get("readArgs") {
                self.catalog.declared_read()?;
                super::step_history::read_arguments(args, true)?;
                let id = args["toolCallId"].as_str().expect("validated");
                if self.held.len() >= MAX_TOOLS
                    || self
                        .held
                        .insert(id.into(), json!({"readArgs":args}))
                        .is_some()
                {
                    return Err("cursor_tool_batch_duplicate".into());
                }
                // This is a held intent only. No readResult is sent; the local
                // declared tool runs after certified stream closure.
            } else if let Some(args) = exec.get("mcpArgs") {
                let id = args["toolCallId"]
                    .as_str()
                    .ok_or("cursor_tool_identity_invalid")?;
                safe_call_id(id)?;
                if let Some(proof) = self.catalog.mcp_metadata(args)? {
                    let result = super::metadata::mcp_result(&proof["value"]);
                    if self.metadata.len() + self.metadata_replies.len() >= MAX_TOOLS
                        || self.metadata.contains_key(id)
                        || self.metadata_replies.insert(id.into(), proof).is_some()
                    {
                        return Err("cursor_metadata_call_duplicate".into());
                    }
                    replies.push(json!({"execClientMessage":{"id":uint32_or_default(exec, "id")?,"execId":exec_id,"mcpResult":result}}));
                } else {
                    own_args(&self.catalog, args)?;
                    if self.held.len() >= MAX_TOOLS
                        || self.held.insert(id.into(), args.clone()).is_some()
                    {
                        return Err("cursor_tool_batch_duplicate".into());
                    }
                    // Business actions remain held until certified stream closure.
                }
            } else {
                rejected_exec(exec);
                return Err("cursor_native_execution_not_allowed".into());
            }
        }
        if let Some(checkpoint) = value.get("conversationCheckpointUpdate") {
            self.checkpoint(checkpoint)?;
        }
        if let Some(update) = value.get("interactionUpdate") {
            let map = update
                .as_object()
                .ok_or("cursor_native_update_not_allowed")?;
            let cases = map
                .keys()
                .filter(|key| key.as_str() != "messageStartedAtMs")
                .collect::<Vec<_>>();
            if cases.len() == 1 && super::informational::is_informational(cases[0]) {
                if map.get("messageStartedAtMs").is_some_and(|value| {
                    value.as_u64().is_none()
                        && value
                            .as_str()
                            .is_none_or(|value| value.parse::<u64>().is_err())
                }) {
                    return Err("cursor_native_update_not_allowed".into());
                }
                super::informational::validate(cases[0], &map[cases[0]])?;
                // Presentation hints cannot become caller messages, tools,
                // answer text, usage, or a replacement conversation history.
                return Ok(replies);
            }
            if cases.len() == 1
                && matches!(
                    cases[0].as_str(),
                    "summaryStarted" | "summary" | "summaryCompleted"
                )
            {
                if !map[cases[0]].is_object() {
                    return Err("cursor_native_update_not_allowed".into());
                }
                // Context pressure returns to the local compaction controller.
                // Remote summaries never replace the caller-owned history.
                return Err("cursor_input_token_limit".into());
            }
            if cases.len() != 1
                || !matches!(
                    cases[0].as_str(),
                    "textDelta"
                        | "thinkingDelta"
                        | "thinkingCompleted"
                        | "partialToolCall"
                        | "toolCallStarted"
                        | "toolCallCompleted"
                        | "tokenDelta"
                        | "heartbeat"
                        | "turnEnded"
                        | "stepStarted"
                        | "stepCompleted"
                        | "toolRequestsListed"
                        | "routedModel"
                )
            {
                let known = [
                    "userMessageAppended",
                    "toolCallDelta",
                    "summary",
                    "summaryStarted",
                    "summaryCompleted",
                    "shellOutputDelta",
                    "promptSuggestion",
                    "postRequestPrompt",
                    "activeBranchChange",
                    "feedbackRequest",
                    "responseComparison",
                    "contextInjectionState",
                    "grokBotNudge",
                ];
                let kind = if cases.len() == 1 && known.contains(&cases[0].as_str()) {
                    cases[0].as_str()
                } else {
                    "unrecognized"
                };
                eprintln!(
                    "[Cursor] unsupported native update kind={kind} shape={}",
                    safe_shape(update, &known)
                );
                return Err("cursor_native_update_not_allowed".into());
            }
            if let Some(partial) = update.get("partialToolCall") {
                self.tool_kind(&partial["toolCall"])?;
            }
            if let Some(start) = update.get("toolCallStarted") {
                self.started(start)?;
            }
            if let Some(end) = update.get("toolCallCompleted") {
                self.completed(end)?;
            }
            if let Some(listed) = update.get("toolRequestsListed") {
                let count = uint32_or_default(listed, "callCount")? as usize;
                if count > MAX_TOOLS {
                    return Err("cursor_tool_batch_invalid".into());
                }
                if self.batch.count.replace(count).is_some() {
                    return Err("cursor_tool_batch_duplicate".into());
                }
            }
            if let Some(text) = update.get("textDelta") {
                let text = string_or_default(text, "text")?;
                if self.text.len() + text.len() > 8 * 1024 * 1024 {
                    return Err("cursor_response_too_large".into());
                }
                self.text.push_str(text);
            }
            if let Some(usage) = update.get("turnEnded") {
                self.ended = true;
                let token = |field: &str| {
                    usage
                        .get(field)
                        .and_then(|value| {
                            value.as_u64().or_else(|| {
                                value.as_str().and_then(|value| value.parse::<u64>().ok())
                            })
                        })
                        .map(|value| json!(value))
                };
                if let Some(input) = token("inputTokens") {
                    self.usage["input_tokens"] = input;
                }
                if let Some(output) = token("outputTokens") {
                    self.usage["output_tokens"] = output;
                }
                if let Some(cached) = token("cacheReadTokens") {
                    self.usage["input_tokens_details"] = json!({"cached_tokens":cached});
                }
                if let Some(created) = token("cacheWriteTokens") {
                    if self.usage.get("input_tokens_details").is_none() {
                        self.usage["input_tokens_details"] = json!({});
                    }
                    self.usage["input_tokens_details"]["cache_creation_tokens"] = created;
                }
                if let Some(reasoning) = token("reasoningTokens") {
                    self.usage["output_tokens_details"] = json!({"reasoning_tokens":reasoning});
                }
                if let (Some(input), Some(output)) = (
                    self.usage["input_tokens"].as_u64(),
                    self.usage["output_tokens"].as_u64(),
                ) {
                    self.usage["total_tokens"] = json!(input.saturating_add(output));
                }
            }
        }
        Ok(replies)
    }
}

pub(super) async fn invoke(
    body: &Value,
    model: &Value,
    credential: &credential::Credential,
    generation: u64,
    output: &mut super::responses::Output,
    downstream: &mpsc::Sender<Bytes>,
) -> Result<Certified, String> {
    for field in ["temperature", "top_p", "stop"] {
        if body.get(field).is_some_and(|value| !value.is_null()) {
            return Err(format!("cursor_{field}_unsupported"));
        }
    }
    if let Some(max) = body.get("max_tokens") {
        if max.as_u64().is_none_or(|max| max == 0) {
            return Err("cursor_output_token_limit_invalid".into());
        }
        eprintln!("[Cursor] max_output_tokens is deprecated for this provider: generation length is provider-controlled; the adapter enforces bounded output bytes");
    }
    let catalog = Catalog::new(body)?;
    let allowed_tools = if catalog.tools.is_empty() {
        ""
    } else {
        "mcp_tool_call,get_mcp_tools_tool_call"
    };
    let mut history = History::new(body)?;
    history.bind_tools(&catalog)?;
    let mut requested = model["requestedModel"].clone();
    requested["builtInModel"] = json!(true);
    let legacy = model["legacySlug"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("cursor_model_variant_unavailable")?;
    let request = json!({"runRequest":{"conversationId":uuid::Uuid::new_v4().to_string(),"conversationState":history.state,"requestedModel":requested,"modelDetails":{"modelId":legacy,"maxMode":requested["maxMode"]},"mcpTools":{"mcpTools":catalog.definitions},"canCreateCloudSubagents":false,"systemPromptSpec":{"replace":history.system},"action":{"resumeAction":{"requestContext":catalog.request_context()}}}});
    let mut step = Step::new(catalog, history);
    let mut change = changes().subscribe();
    credential::check_generation(generation)?;
    let (input_tx, input_rx) = mpsc::channel::<Bytes>(8);
    // Charge the initial request and every reply to one bounded HTTP stream,
    // including framing and base64 expansion of requested history blobs.
    let mut encoder = bridge::Encoder::with_max_bytes(MAX_HISTORY * 2)?;
    input_tx
        .send(encoder.frame(request)?)
        .await
        .map_err(|_| "cursor_request_cancelled")?;
    let input = futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), rx))
    });
    let client = credential::inference_client()?;
    let inference_origin = credential.inference_origin()?;
    let request = credential::sdk_authenticated(
        client.post(format!("{}/agent.v1.AgentService/Run", inference_origin)),
        credential,
    )
    .header("x-cursor-agent-allowed-tools", allowed_tools)
    .header("x-request-id", uuid::Uuid::new_v4().to_string())
    .header("x-original-request-id", uuid::Uuid::new_v4().to_string())
    .version(reqwest::Version::HTTP_2)
    .header("content-type", "application/connect+json")
    .header("x-cursor-streaming", "true")
    .body(reqwest::Body::wrap_stream(input))
    .send();
    let response = tokio::select! { _ = change.changed() => return Err("cursor_credential_changed".into()), result = request => result.map_err(|_|"cursor_network_error")? };
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let bytes = credential::bounded_body(response).await?;
        let error = credential::http_error(
            status,
            serde_json::from_slice::<Value>(&bytes).ok().as_ref(),
        );
        if error == credential::NEEDS_REIMPORT {
            return Err("cursor_model_authorization_failed".into());
        }
        return Err(error);
    }
    let mut source = response.bytes_stream();
    let mut decoder = bridge::Decoder::with_max_response(MAX_HISTORY * 2)?;
    let mut received = 0_usize;
    loop {
        let chunk = tokio::select! { _ = change.changed() => return Err("cursor_credential_changed".into()), result = source.try_next() => result.map_err(|_|"cursor_response_read_failed")? };
        let Some(chunk) = chunk else {
            if !decoder.is_empty() {
                return Err("cursor_frame_truncated".into());
            }
            return Err("cursor_terminal_trailer_missing".into());
        };
        received = received.saturating_add(chunk.len());
        if received > MAX_HISTORY * 2 {
            return Err("cursor_response_too_large".into());
        }
        let decoded = decoder.push(&chunk)?;
        let mut clean_terminal = false;
        for (index, (terminal, value)) in decoded.iter().enumerate() {
            credential::check_generation(generation)?;
            if *terminal || value.get("error").is_some() {
                if value.get("error").is_some() {
                    let error = credential::upstream_error(value);
                    if error == credential::NEEDS_REIMPORT {
                        return Err("cursor_model_authorization_failed".into());
                    }
                    return Err(error);
                }
                if !*terminal
                    || index + 1 != decoded.len()
                    || !decoder.is_empty()
                    || value
                        .as_object()
                        .is_none_or(|map| map.keys().any(|key| key != "metadata"))
                {
                    return Err("cursor_terminal_trailer_invalid".into());
                }
                clean_terminal = true;
                continue;
            }
            let replies = step.receive(value)?;
            if let Some(update) = value.get("interactionUpdate") {
                tokio::select! {
                    _ = change.changed() => return Err("cursor_credential_changed".into()),
                    result = output.delta(update,downstream) => result?,
                }
            }
            for reply in replies {
                tokio::select! {
                    _ = change.changed() => return Err("cursor_credential_changed".into()),
                    result = input_tx.send(encoder.frame(reply)?) => result.map_err(|_|"cursor_request_cancelled")?,
                }
            }
        }
        if clean_terminal {
            let certified = step.finish()?;
            drop(source);
            drop(input_tx);
            drop(client);
            credential::check_generation(generation)?;
            return Ok(certified);
        }
        if let Some(certified) = step.ready()? {
            // Dropping both halves closes the HTTP/2 stream before the caller
            // can observe calls or run any tool. No execution result is sent.
            drop(source);
            drop(input_tx);
            drop(client);
            credential::check_generation(generation)?;
            return Ok(certified);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn rejected_packet_diagnostics_contain_only_bounded_known_field_names_and_types() {
        let shape = safe_shape(
            &json!({"id":12345,"execId":"secret-identifier","readArgs":{"path":"/private/secret-path","contents":"secret-value"},"secret-unknown-key":"secret-value"}),
            &["id", "execId", "readArgs"],
        );
        let encoded = shape.to_string();
        for secret in [
            "12345",
            "secret-identifier",
            "/private/secret-path",
            "secret-value",
            "secret-unknown-key",
        ] {
            assert!(!encoded.contains(secret));
        }
        assert_eq!(shape["unknownFieldCount"], 1);
        assert_eq!(shape["fields"].as_array().unwrap().len(), 3);
        assert!(encoded.contains("readArgs"));
        assert!(encoded.contains("object"));
        let mut many = serde_json::Map::new();
        let names = (0..20)
            .map(|index| format!("field{index}"))
            .collect::<Vec<_>>();
        for name in &names {
            many.insert(name.clone(), json!("private"));
        }
        let known = names.iter().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(
            safe_shape(&Value::Object(many), &known)["fields"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
    }

    #[test]
    fn tool_bindings_reach_both_system_surfaces_and_preserve_caller_messages() {
        let body = body();
        let catalog = Catalog::new(&body).unwrap();
        let mut history = History::new(&body).unwrap();
        let roots = history.state["rootPromptMessagesJson"]
            .as_array()
            .unwrap()
            .clone();
        let original_user = history
            .blobs
            .get(roots[1].as_str().unwrap())
            .unwrap()
            .to_vec();
        history.bind_tools(&catalog).unwrap();
        let bound = history.state["rootPromptMessagesJson"].as_array().unwrap();
        assert_eq!(bound[1], roots[1]);
        assert_eq!(
            history.blobs.get(bound[1].as_str().unwrap()).unwrap(),
            original_user
        );
        let system: Value =
            serde_json::from_slice(history.blobs.get(bound[0].as_str().unwrap()).unwrap()).unwrap();
        let text = system["content"].as_str().unwrap();
        assert!(text.starts_with("Local instructions"));
        assert!(text.contains("Tool execution binding"));
        assert!(text.contains("\"callerTool\":\"echo\""));
        assert!(text.contains("\"toolName\":\"echo\""));
        assert!(history.system.contains("Tool execution binding"));
        assert_eq!(
            catalog.definitions[0]["annotationsJson"],
            "{\"readOnlyHint\":false}"
        );
    }

    #[test]
    fn timing_metadata_drift_rides_along_without_admitting_unknown_control_keys() {
        for value in [
            json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"serverFirstTokenMs":1.25,"clientClock":true}}),
            json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"serverFirstTokenMs":"1"}}),
            json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"clientClock":0}}),
            json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"unknown":1}}),
        ] {
            step().receive(&value).unwrap();
        }
        for value in [
            json!({"interactionUpdate":{"heartbeat":{}},"execServerMessage":{"requestContextArgs":{}},"ttftBreakdown":{}}),
            json!({"execServerControlMessage":{"abort":{}},"ttftBreakdown":{}}),
            json!({"interactionQuery":{},"ttftBreakdown":{}}),
        ] {
            assert!(step().receive(&value).is_err());
        }
    }

    #[test]
    fn omitted_protobuf_scalars_use_declared_defaults_and_present_malformed_values_fail() {
        let mut step = step();
        let empty = step.blobs.seed(Vec::new()).unwrap();
        let get = step
            .receive(&json!({"kvServerMessage":{"getBlobArgs":{"blobId":empty},"spanContext":{}}}))
            .unwrap();
        assert_eq!(get[0]["kvClientMessage"]["id"], 0);
        assert_eq!(get[0]["kvClientMessage"]["getBlobResult"]["blobData"], "");
        let set = step
            .receive(&json!({"kvServerMessage":{"setBlobArgs":{"blobId":empty}}}))
            .unwrap();
        assert_eq!(set[0]["kvClientMessage"]["id"], 0);
        let context = step
            .receive(&json!({"execServerMessage":{"requestContextArgs":{}}}))
            .unwrap();
        assert_eq!(context[0]["execClientMessage"]["id"], 0);
        assert_eq!(context[0]["execClientMessage"]["execId"], "");
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{}}}))
            .unwrap();
        assert_eq!(step.batch.count, Some(0));
        step.receive(&json!({"interactionUpdate":{"textDelta":{}}}))
            .unwrap();
        assert!(step.text.is_empty());
        step.pending = vec![native()];
        step.checkpoint(&json!({"rootPromptMessagesJson":step.initial_roots}))
            .unwrap();
        assert!(
            step.pending.is_empty(),
            "omitted repeated fields in a full checkpoint are empty"
        );
        for invalid in [
            Value::Null,
            json!("1"),
            json!(true),
            json!(-1),
            json!(u64::MAX),
        ] {
            assert!(uint32_or_default(&json!({"id":invalid}), "id").is_err());
        }
        assert!(string_or_default(&json!({"text":null}), "text").is_err());
        assert!(step
            .receive(&json!({"kvServerMessage":{"setBlobArgs":{"blobId":empty,"blobData":1}}}))
            .is_err());
    }

    fn body() -> Value {
        json!({"model":"fixture","messages":[{"role":"system","content":"Local instructions"},{"role":"user","content":"Call echo"}],"tools":[{"type":"function","function":{"name":"echo","description":"Echo\n value","parameters":{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}}}]})
    }

    fn step() -> Step {
        let body = body();
        Step::new(Catalog::new(&body).unwrap(), History::new(&body).unwrap())
    }

    fn held() -> Value {
        json!({"name":"klaus_tools-echo","providerIdentifier":NAMESPACE,"serverIdentifier":NAMESPACE,"toolName":"echo","toolCallId":"raw\ncall","args":{"value":"input"}})
    }

    fn native() -> Value {
        json!({"role":"assistant","id":"native","content":[{"type":"reasoning","text":"Reason","signature":"signed","providerOptions":{"cursor":{"modelName":"fixture"}}},{"type":"tool-call","toolCallId":"raw\ncall","toolName":"CallDynamicTool","args":{"namespace":NAMESPACE,"toolName":"echo","arguments":{"value":"input"}}}],"providerOptions":{"cursor":{"pendingToolExecutionContracts":{"raw\ncall":{"toolCallId":"raw\ncall","outerToolName":"CallDynamicTool","toolIdentifier":"MCP","isDynamic":true}}}}})
    }

    fn start(step: &mut Step) {
        step.receive(&json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"one","toolCall":{"mcpToolCall":{"args":held()}}}}})).unwrap();
    }

    #[test]
    fn execution_batch_keeps_its_call_limit_with_a_large_catalog() {
        let mut body = body();
        for index in 0..345 {
            body["tools"].as_array_mut().unwrap().push(
                json!({"type":"function","function":{"name":format!("extra_{index}"),"parameters":{"type":"object"}}}),
            );
        }
        let mut step = Step::new(Catalog::new(&body).unwrap(), History::new(&body).unwrap());
        for index in 0..=MAX_TOOLS {
            let mut args = held();
            args["toolCallId"] = json!(format!("call-{index}"));
            let update = json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}}}});
            let result = step.receive(&update);
            if index < MAX_TOOLS {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(error) if error == "cursor_tool_batch_duplicate"));
            }
        }
        assert_eq!(step.batch.started.len(), MAX_TOOLS);
    }

    #[test]
    fn calls_require_complete_owned_batch_and_native_certificate_without_execution_ack() {
        let mut step = step();
        start(&mut step);
        let replies = step
            .receive(&json!({"execServerMessage":{"id":7,"execId":"exec","mcpArgs":held()}}))
            .unwrap();
        assert!(
            replies.is_empty(),
            "no tool execution result is sent to the provider"
        );
        assert!(step.ready().unwrap().is_none());
        step.receive(
            &json!({"conversationCheckpointUpdate":{"pendingToolCalls":[native().to_string()]}}),
        )
        .unwrap();
        assert!(
            step.ready().unwrap().is_none(),
            "checkpoint alone is insufficient"
        );
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
            .unwrap();
        assert!(
            step.ready().unwrap().is_none(),
            "pending calls cannot publish before the complete history prefix is certified"
        );
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native().to_string()]}})).unwrap();
        let result = step.ready().unwrap().unwrap();
        assert_eq!(result.calls[0]["toolName"], "echo");
        assert!(!result.calls[0]["toolCallId"]
            .as_str()
            .unwrap()
            .contains('\n'));
        assert_eq!(result.context["nativeMessages"][0], native());
        assert_eq!(result.context["callMap"][0]["rawId"], "raw\ncall");
        assert_eq!(
            result.context["nativeMessages"][0]["content"][0]["signature"],
            "signed"
        );
    }

    #[test]
    fn terminal_requires_a_received_history_checkpoint_even_without_tools() {
        let mut step = step();
        step.ended = true;
        assert!(step.new_roots().unwrap().is_none());
        assert_eq!(
            step.finish().err().unwrap(),
            "cursor_terminal_checkpoint_missing"
        );
    }

    fn informational_updates() -> Vec<Value> {
        vec![
            json!({"promptSuggestion":{"suggestion":"Suggested follow-up"}}),
            json!({"postRequestPrompt":{"title":"Optional prompt","message":"Message","buttonLabel":"Continue","buttonUrl":"https://example.invalid/never-open"}}),
            json!({"feedbackRequest":{"requestId":"feedback","categories":[{"id":"quality","label":"Quality"}],"categoryGroups":[{"id":"group","prompt":"Feedback?","categories":[{"id":"helpful","label":"Helpful"}]}],"showFormImmediately":true}}),
            json!({"grokBotNudge":{"job":"job","description":"Optional nudge","showNumber":1}}),
            json!({"responseComparison":{"comparisonId":"comparison","started":{"displayOrder":"RESPONSE_COMPARISON_DISPLAY_ORDER_PARENT_FIRST","parentResponse":"An alternate parent answer","alternateModelId":"alternate"}}}),
            json!({"responseComparison":{"comparisonId":"comparison","textDelta":{"text":"An alternate answer that must stay private"}}}),
            json!({"responseComparison":{"comparisonId":"comparison","completed":{}}}),
            json!({"responseComparison":{"comparisonId":"skipped-comparison","skipped":{"reason":"RESPONSE_COMPARISON_SKIP_REASON_ALTERNATE_TOOL_CALL"}}}),
        ]
    }

    fn receive_informational_updates(step: &mut Step) {
        for mut update in informational_updates() {
            update["messageStartedAtMs"] = json!("123456789");
            assert!(step
                .receive(&json!({"interactionUpdate":update}))
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn informational_updates_preserve_the_certified_primary_answer_history_and_usage() {
        let mut step = step();
        let initial_roots = step.initial_roots.clone();
        receive_informational_updates(&mut step);
        assert!(step.ready().unwrap().is_none());
        assert_eq!(step.finish().err().unwrap(), "cursor_inference_incomplete");
        assert_eq!(step.initial_roots, initial_roots);
        assert!(step.latest_roots.is_none());
        assert!(step.held.is_empty());
        assert!(step.pending.is_empty());
        assert!(step.metadata.is_empty());
        assert!(step.text.is_empty());
        assert_eq!(step.usage, json!({}));

        step.receive(&json!({"interactionUpdate":{"textDelta":{"text":"Primary answer"}}}))
            .unwrap();
        let native = json!({"role":"assistant","content":[{"type":"reasoning","text":"Primary reasoning","signature":"signed"},{"type":"text","text":"Primary answer"}]});
        let blob = step
            .blobs
            .seed(serde_json::to_vec(&native).unwrap())
            .unwrap();
        let mut roots = step.initial_roots.clone();
        roots.push(json!(blob));
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":roots}}))
            .unwrap();
        receive_informational_updates(&mut step);
        assert_eq!(step.finish().err().unwrap(), "cursor_inference_incomplete");
        step.receive(&json!({"interactionUpdate":{"turnEnded":{"inputTokens":"7","outputTokens":"3","cacheReadTokens":"2","cacheWriteTokens":"1"}}}))
            .unwrap();
        receive_informational_updates(&mut step);
        let result = step.finish().unwrap();
        assert_eq!(result.text, "Primary answer");
        assert_eq!(result.reasoning, "Primary reasoning");
        assert!(result.calls.is_empty());
        assert_eq!(result.context["nativeMessages"], json!([native]));
        assert_eq!(result.context["text"], "Primary answer");
        assert_eq!(result.usage["input_tokens"], 7);
        assert_eq!(result.usage["output_tokens"], 3);
        assert_eq!(result.usage["input_tokens_details"]["cached_tokens"], 2);
        assert_eq!(
            result.usage["input_tokens_details"]["cache_creation_tokens"],
            1
        );
        assert_eq!(result.usage["total_tokens"], 10);
    }

    #[test]
    fn informational_updates_leave_local_tool_certification_and_required_choice_intact() {
        let mut body = body();
        body["tool_choice"] = json!("required");
        let mut step = read_step(&body);
        receive_informational_updates(&mut step);
        start(&mut step);
        step.receive(&json!({"interactionUpdate":{"textDelta":{"text":"Primary preamble"}}}))
            .unwrap();
        receive_informational_updates(&mut step);
        assert!(step
            .receive(&json!({"execServerMessage":{"mcpArgs":held()}}))
            .unwrap()
            .is_empty());
        let mut native = native();
        native["content"]
            .as_array_mut()
            .unwrap()
            .insert(1, json!({"type":"text","text":"Primary preamble"}));
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
            .unwrap();
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native.to_string()]}}))
            .unwrap();
        receive_informational_updates(&mut step);
        let certified = step.ready().unwrap().unwrap();
        assert_eq!(certified.text, "Primary preamble");
        assert_eq!(certified.reasoning, "Reason");
        assert_eq!(certified.calls.len(), 1);
        assert_eq!(certified.calls[0]["toolName"], "echo");
        assert_eq!(certified.calls[0]["args"], json!({"value":"input"}));
        assert_eq!(certified.context["nativeMessages"], json!([native]));

        let mut missing = read_step(&body);
        receive_informational_updates(&mut missing);
        let native = json!({"role":"assistant","content":[{"type":"text","text":""}]});
        assert_eq!(
            missing.certify(&[native], false).err().unwrap(),
            "cursor_required_tool_missing"
        );
        assert!(missing.ready().unwrap().is_none());
    }

    #[test]
    fn informational_updates_do_not_allow_state_mutation_native_execution_or_ambiguous_packets() {
        for update in [
            json!({"userMessageAppended":{"userMessage":{"text":"Injected"}}}),
            json!({"activeBranchChange":{"path":"/tmp","branchName":"other"}}),
            json!({"contextInjectionState":{"injectionId":"unknown","state":1}}),
            json!({"shellOutputDelta":{"stdout":{"data":"Native execution"}}}),
            json!({"toolCallDelta":{"toolCallDelta":{"shellToolCallDelta":{}}}}),
            json!({"toolCallDelta":{"toolCallDelta":{"taskToolCallDelta":{}}}}),
            json!({"toolCallDelta":{"toolCallDelta":{"editToolCallDelta":{}}}}),
            json!({"toolCallDelta":{"toolCallDelta":{"replaceEnvToolCallDelta":{}}}}),
            json!({"promptSuggestion":{},"turnEnded":{}}),
            json!({"promptSuggestion":{},"unknown":{}}),
            json!({"promptSuggestion":{},"messageStartedAtMs":false}),
            json!({"responseComparison":{"completed":{},"textDelta":{"text":"Alternate"}}}),
            json!({"feedbackRequest":{"categoryGroups":[{"categories":[{"command":"run"}]}]}}),
            json!({"futurePresentationHint":{}}),
        ] {
            let mut step = step();
            receive_informational_updates(&mut step);
            assert_eq!(
                step.receive(&json!({"interactionUpdate":update}))
                    .unwrap_err(),
                "cursor_native_update_not_allowed",
                "{update}"
            );
            assert!(!step.ended);
            assert!(step.text.is_empty());
            assert!(step.held.is_empty());
            assert!(step.pending.is_empty());
        }
        let mut step = step();
        step.text = "Primary answer".into();
        receive_informational_updates(&mut step);
        let altered =
            json!({"role":"assistant","content":[{"type":"text","text":"Alternate answer"}]});
        assert_eq!(
            step.certify(&[altered], false).err().unwrap(),
            "cursor_native_response_mismatch"
        );
    }

    #[test]
    fn foreign_tools_argument_changes_native_side_effects_and_duplicate_calls_fail_closed() {
        for mutation in ["provider", "name", "arguments", "contract"] {
            let mut step = step();
            start(&mut step);
            let mut call = held();
            let mut native = native();
            match mutation {
                "provider" => call["providerIdentifier"] = json!("foreign"),
                "name" => call["name"] = json!("foreign-echo"),
                "arguments" => {
                    native["content"][1]["args"]["arguments"] = json!({"value":"changed"})
                }
                _ => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["toolIdentifier"] = json!("SHELL")
                }
            }
            let execution =
                step.receive(&json!({"execServerMessage":{"id":7,"execId":"exec","mcpArgs":call}}));
            if execution.is_ok() {
                step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
                    .unwrap();
                step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native.to_string()]}})).unwrap();
                assert!(step.ready().is_err());
            } else {
                assert!(matches!(mutation, "provider" | "name"));
            }
        }
        for value in [
            json!({"interactionQuery":{"permission":{}}}),
            json!({"execServerMessage":{"shellArgs":{"command":"unsafe"}}}),
            json!({"interactionUpdate":{"toolCallStarted":{"toolCall":{"shellToolCall":{}}}}}),
        ] {
            assert!(step().receive(&value).is_err());
        }
        let mut step = step();
        start(&mut step);
        assert!(step
            .started(&json!({"modelCallId":"two","toolCall":{"mcpToolCall":{"args":held()}}}))
            .is_err());
    }

    fn read_body() -> Value {
        let mut value = body();
        value["tools"] = json!([{"type":"function","function":{"name":"read","parameters":{"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"integer","exclusiveMinimum":0,"maximum":9007199254740991_i64},"limit":{"type":"integer","exclusiveMinimum":0,"maximum":9007199254740991_i64},"pages":{"type":"string"}},"required":["path"],"additionalProperties":false}}}]);
        value
    }

    fn read_step(body: &Value) -> Step {
        Step::new(Catalog::new(body).unwrap(), History::new(body).unwrap())
    }

    fn read_native(args: Value) -> Value {
        json!({"role":"assistant","content":[{"type":"reasoning","text":"","signature":"read-signature"},{"type":"tool-call","toolCallId":"read\ncall","toolName":"Read","args":args}],"providerOptions":{"cursor":{"pendingToolExecutionContracts":{"read\ncall":{"toolCallId":"read\ncall","outerToolName":"Read","toolIdentifier":"READ","isDynamic":false}}}}})
    }

    fn prepare_read(
        step: &mut Step,
        args: Value,
        execution: Value,
        native: Value,
    ) -> Result<Option<Certified>, String> {
        step.receive(&json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"read-model","toolCall":{"readToolCall":{"args":args},"toolCallId":"read\ncall"}}}}))?;
        let replies = step.receive(&json!({"execServerMessage":{"readArgs":execution,"acceptHookAdditionalContexts":true}}))?;
        assert!(
            replies.is_empty(),
            "no native execution result is acknowledged"
        );
        assert!(
            step.ready()?.is_none(),
            "held read alone is not a local call"
        );
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))?;
        step.receive(
            &json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native.to_string()]}}),
        )?;
        step.ready()
    }

    #[test]
    fn declared_read_intent_is_certified_reversible_and_never_acknowledged_upstream() {
        let body = read_body();
        let args = json!({"path":"fixture.txt","offset":2,"limit":3});
        let mut execution = args.clone();
        execution["toolCallId"] = json!("read\ncall");
        let native = read_native(args.clone());
        let certified = prepare_read(
            &mut read_step(&body),
            args.clone(),
            execution,
            native.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(certified.calls[0]["toolName"], "read");
        assert_eq!(certified.calls[0]["args"], args);
        assert_eq!(certified.context["nativeMessages"][0], native);
        let call = &certified.calls[0];
        let mut replay = body;
        replay["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":"","nativeContext":certified.context,"tool_calls":[{"id":call["toolCallId"],"function":{"name":"read","arguments":args.to_string()}}]}),
            json!({"role":"tool","tool_call_id":call["toolCallId"],"content":"REAL_LOCAL_FILE"}),
        ]);
        let history = History::new(&replay).unwrap();
        let roots = history.state["rootPromptMessagesJson"].as_array().unwrap();
        let result: Value = serde_json::from_slice(
            history
                .blobs
                .get(roots.last().unwrap().as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["content"][0]["toolName"], "Read");
        assert_eq!(result["content"][0]["toolCallId"], "read\ncall");
        assert_eq!(result["content"][0]["result"], "REAL_LOCAL_FILE");
    }

    #[test]
    fn read_intent_rejects_schema_changes_non_equivalent_arguments_and_foreign_contracts() {
        for mutation in [
            "absent",
            "schema",
            "descriptionProperty",
            "offset",
            "limit",
            "includeLineNumbers",
            "path",
            "nativeArgs",
            "outer",
            "contract",
            "dynamic",
            "encodingHint",
            "writePrerequisite",
        ] {
            let mut body = read_body();
            let mut args = json!({"path":"fixture.txt"});
            let mut exec = json!({"path":"fixture.txt","toolCallId":"read\ncall"});
            let mut native = read_native(args.clone());
            match mutation {
                "absent" => body["tools"] = json!([]),
                "schema" => {
                    body["tools"][0]["function"]["parameters"]["additionalProperties"] = json!(true)
                }
                "descriptionProperty" => {
                    body["tools"][0]["function"]["parameters"]["properties"]["description"] =
                        json!({"type":"string"})
                }
                "offset" => args["offset"] = json!(0),
                "limit" => args["limit"] = json!(-1),
                "includeLineNumbers" => args["includeLineNumbers"] = json!(false),
                "path" => exec["path"] = json!("other.txt"),
                "nativeArgs" => native["content"][1]["args"]["path"] = json!("other.txt"),
                "outer" => native["content"][1]["toolName"] = json!("ReadFile"),
                "contract" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["read\ncall"]["toolIdentifier"] = json!("WRITE")
                }
                "dynamic" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["read\ncall"]["isDynamic"] = json!(true)
                }
                "encodingHint" => exec["encodingHint"] = json!("utf8"),
                "writePrerequisite" => {
                    native["content"][1]["toolName"] = json!("Write");
                    native["content"][1]["args"]["contents"] = json!("write");
                }
                _ => unreachable!(),
            }
            assert!(
                prepare_read(&mut read_step(&body), args, exec, native).is_err(),
                "{mutation}"
            );
        }
        for metadata in [Value::Null, json!("true"), json!(1)] {
            assert!(step().receive(&json!({"execServerMessage":{"requestContextArgs":{},"acceptHookAdditionalContexts":metadata}})).is_err());
        }
        assert!(read_step(&read_body()).receive(&json!({"execServerMessage":{"machineId":"other","readArgs":{"path":"fixture.txt","toolCallId":"read\ncall"}}})).is_err());
    }

    #[test]
    fn metadata_discovery_is_owned_exact_bounded_and_consumes_its_own_batch() {
        let mut step = step();
        let args = json!({"server":NAMESPACE,"toolName":"echo","toolCallId":"metadata"});
        let call = json!({"getMcpToolsToolCall":{"args":args}});
        step.started(&json!({"modelCallId":"metadata-call","toolCall":call}))
            .unwrap();
        step.batch.count = Some(1);
        let result = step
            .catalog
            .metadata_projection(Some("echo"), true)
            .unwrap();
        let complete = json!({"modelCallId":"metadata-call","toolCall":{"getMcpToolsToolCall":{"args":args,"result":{"success":{"content":result.to_string()}}}}});
        step.completed(&complete).unwrap();
        assert!(step.ready().unwrap().is_none());
        assert!(step.batch.started.is_empty());
        start(&mut step);
        let mut injected = result.clone();
        injected["tool"] = json!("mcp_auth");
        let mut invalid = complete.clone();
        invalid["toolCall"]["getMcpToolsToolCall"]["result"]["success"]["content"] =
            json!(injected.to_string());
        assert!(step.completed(&invalid).is_err());
        assert!(step
            .metadata_call(
                &json!({"getMcpToolsToolCall":{"args":{"server":"foreign","toolCallId":"m"}}})
            )
            .is_err());
    }

    #[test]
    fn pattern_results_reject_foreign_tools_and_reset_before_the_next_batch_in_one_chunk() {
        let args = json!({"pattern":"^echo$","toolCallId":"metadata"});
        let result = json!({"mode":"search","pattern":"^echo$","matches":[{"namespace":NAMESPACE,"tool":"echo","description":"Echo value"}]});
        for mutation in ["none", "namespace", "tool", "schema"] {
            let mut step = step();
            step.started(
                &json!({"modelCallId":"metadata","toolCall":{"getMcpToolsToolCall":{"args":args}}}),
            )
            .unwrap();
            step.batch.count = Some(1);
            let mut result = result.clone();
            match mutation {
                "namespace" => result["matches"][0]["namespace"] = json!("foreign"),
                "tool" => result["matches"][0]["tool"] = json!("shell"),
                "schema" => result["matches"][0]["inputSchema"] = json!({}),
                _ => {}
            }
            let complete = json!({"modelCallId":"metadata","toolCall":{"getMcpToolsToolCall":{"args":args,"result":{"success":{"content":result.to_string()}}}}});
            let completed = step.completed(&complete);
            if mutation == "none" {
                completed.unwrap();
                // No ready() call separates these two protocol batches.
                start(&mut step);
                assert_eq!(step.batch.model_call_id, "one");
            } else {
                assert_eq!(completed.unwrap_err(), "cursor_metadata_result_mismatch");
            }
        }
    }

    #[test]
    fn validated_metadata_roots_are_preserved_privately_with_the_signed_assistant() {
        let mut step = step();
        let args = json!({"server":NAMESPACE,"toolName":"echo","toolCallId":"metadata"});
        let projection = step
            .catalog
            .metadata_projection(Some("echo"), true)
            .unwrap();
        step.metadata
            .insert("metadata".into(), json!({"args":args,"value":projection}));
        let helpers = [
            json!({"role":"assistant","content":[{"type":"reasoning","text":"","signature":"signed-helper"},{"type":"tool-call","toolCallId":"metadata","toolName":"GetDynamicTools","args":{"namespace":NAMESPACE,"toolName":"echo"}}]}),
            json!({"role":"tool","content":[{"type":"tool-result","toolCallId":"metadata","toolName":"GetDynamicTools","result":projection.to_string(),"experimental_content":[{"type":"text","text":projection.to_string()}]}]}),
        ];
        let mut roots = step.initial_roots.clone();
        for helper in &helpers {
            roots.push(json!(step
                .blobs
                .seed(serde_json::to_vec(helper).unwrap())
                .unwrap()));
        }
        step.latest_roots = Some(roots);
        start(&mut step);
        step.held.insert("raw\ncall".into(), held());
        step.batch.count = Some(1);
        step.pending = vec![native()];
        let certified = step.ready().unwrap().unwrap();
        assert_eq!(certified.calls.len(), 1, "metadata calls stay private");
        assert_eq!(certified.context["nativeMessages"][0], helpers[0]);
        assert_eq!(certified.context["nativeMessages"][1], helpers[1]);
        assert_eq!(certified.context["nativeMessages"][2], native());
        let mut changed = helpers[1].clone();
        changed["content"][0]["experimental_content"][0]["text"] = json!("changed");
        let mut native_messages = helpers.to_vec();
        native_messages[1] = changed;
        native_messages.push(native());
        assert_eq!(
            step.certify(&native_messages, true).err().unwrap(),
            "cursor_metadata_history_invalid"
        );
    }

    #[test]
    fn parallel_discovery_and_local_calls_preserve_the_signed_batch_without_remote_execution() {
        let mut step = step();
        let args = json!({"server":NAMESPACE,"toolName":"echo","toolCallId":"metadata"});
        let projection = step
            .catalog
            .metadata_projection(Some("echo"), true)
            .unwrap();
        let call = json!({"getMcpToolsToolCall":{"args":args}});
        step.started(&json!({"modelCallId":"one","toolCall":call}))
            .unwrap();
        start(&mut step);
        step.completed(&json!({"modelCallId":"one","toolCall":{"getMcpToolsToolCall":{"args":args,"result":{"success":{"content":projection.to_string()}}}}})).unwrap();
        step.held.insert("raw\ncall".into(), held());
        step.batch.count = Some(2);
        let mut signed = native();
        signed["content"].as_array_mut().unwrap().insert(1, json!({"type":"tool-call","toolCallId":"metadata","toolName":"GetDynamicTools","args":{"namespace":NAMESPACE,"toolName":"echo"}}));
        step.latest_roots = Some(step.initial_roots.clone());
        step.pending = vec![signed.clone()];
        let result = step.ready().unwrap().unwrap();
        assert_eq!(result.calls.len(), 1);
        assert_eq!(result.calls[0]["toolName"], "echo");
        assert_eq!(result.context["nativeMessages"][0], signed);
        assert_eq!(result.context["nativeMessages"][1]["role"], "tool");
        assert_eq!(
            result.context["nativeMessages"][1]["content"][0]["toolCallId"],
            "metadata"
        );
        assert_eq!(
            result.context["nativeMessages"][1]["content"][0]["result"],
            projection.to_string()
        );
        let call = &result.calls[0];
        let mut replay = body();
        replay["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":"","nativeContext":result.context,"tool_calls":[{"id":call["toolCallId"],"function":{"name":"echo","arguments":"{\"value\":\"input\"}"}}]}),
            json!({"role":"tool","tool_call_id":call["toolCallId"],"content":"LOCAL_RESULT"}),
        ]);
        assert!(History::new(&replay).is_ok());
    }

    #[test]
    fn missing_discovery_results_cannot_move_across_later_assistant_messages() {
        let mut step = step();
        let args = json!({"server":NAMESPACE,"toolName":"echo","toolCallId":"metadata"});
        let projection = step
            .catalog
            .metadata_projection(Some("echo"), true)
            .unwrap();
        step.metadata
            .insert("metadata".into(), json!({"args":args,"value":projection}));
        let earlier = json!({"role":"assistant","content":[{"type":"tool-call","toolCallId":"metadata","toolName":"GetDynamicTools","args":{"namespace":NAMESPACE,"toolName":"echo"}}]});
        let mut messages = vec![earlier, native()];
        let original = messages.clone();
        assert_eq!(
            step.complete_metadata_history(&mut messages).unwrap_err(),
            "cursor_metadata_history_missing"
        );
        assert_eq!(messages, original);
    }

    fn metadata_mcp(helper: &str) -> Value {
        let owner = if helper == "GetDynamicTools" {
            "namespace"
        } else {
            "server"
        };
        json!({"name":format!("{NAMESPACE}-{helper}"),"providerIdentifier":NAMESPACE,"serverIdentifier":NAMESPACE,"toolName":helper,"toolCallId":"metadata","args":{(owner):NAMESPACE,"toolName":"echo"}})
    }

    fn metadata_native(helper: &str, outer: &str) -> Value {
        let request = metadata_mcp(helper);
        let args = match outer {
            "CallDynamicTool" => {
                json!({"namespace":NAMESPACE,"toolName":helper,"arguments":request["args"]})
            }
            "CallMcpTool" => {
                json!({"server":NAMESPACE,"toolName":helper,"arguments":request["args"]})
            }
            _ => request["args"].clone(),
        };
        json!({"type":"tool-call","toolCallId":"metadata","toolName":outer,"args":args})
    }

    fn reply_metadata(step: &mut Step, helper: &str) -> Value {
        let args = metadata_mcp(helper);
        step.started(&json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}}))
            .unwrap();
        let replies = step
            .receive(&json!({"execServerMessage":{"id":7,"execId":"metadata-exec","mcpArgs":args}}))
            .unwrap();
        assert!(
            step.held.is_empty(),
            "metadata never enters the execution queue"
        );
        assert_eq!(replies.len(), 1);
        let result = replies[0]["execClientMessage"]["mcpResult"].clone();
        assert_eq!(replies[0]["execClientMessage"]["id"], 7);
        assert_eq!(replies[0]["execClientMessage"]["execId"], "metadata-exec");
        step.completed(
            &json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args,"result":result}}}),
        )
        .unwrap();
        result
    }

    #[test]
    fn completed_discovery_checkpoint_waits_for_the_current_execution_batch() {
        let mut step = step();
        let preamble = "Checking the available tools.";
        step.receive(&json!({"interactionUpdate":{"textDelta":{"text":preamble}}}))
            .unwrap();
        reply_metadata(&mut step, "GetDynamicTools");
        step.batch.count = Some(1);
        let helper = json!({"role":"assistant","content":[{"type":"text","text":preamble},metadata_native("GetDynamicTools", "GetDynamicTools")]});
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[helper.to_string()]}})).unwrap();
        assert!(step.ready().unwrap().is_none());

        start(&mut step);
        step.receive(&json!({"execServerMessage":{"mcpArgs":held()}}))
            .unwrap();
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
            .unwrap();
        assert!(
            step.ready().unwrap().is_none(),
            "a completed discovery checkpoint is not the current execution certificate"
        );
        assert_eq!(step.held.len(), 1);

        let mut helpers = vec![helper];
        step.complete_metadata_history(&mut helpers).unwrap();
        let mut roots = step.initial_roots.clone();
        for message in &helpers {
            roots.push(json!(step
                .blobs
                .seed(serde_json::to_vec(message).unwrap())
                .unwrap()));
        }
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":roots,"pendingToolCalls":[native().to_string()]}})).unwrap();
        let certified = step.ready().unwrap().unwrap();
        assert_eq!(certified.text, preamble);
        assert_eq!(certified.calls.len(), 1);
        assert_eq!(certified.calls[0]["toolName"], "echo");
        assert_eq!(certified.context["nativeMessages"][0], helpers[0]);
        assert_eq!(certified.context["nativeMessages"][1], helpers[1]);
        assert_eq!(certified.context["nativeMessages"][2], native());

        step.pending[0]["content"][1]["args"]["arguments"]["value"] = json!("changed");
        assert_eq!(step.ready().err().unwrap(), "cursor_native_tool_mismatch");
    }

    #[test]
    fn unknown_checkpoint_calls_are_rejected_instead_of_waiting_for_another_snapshot() {
        for include_current_call in [false, true] {
            let mut step = step();
            start(&mut step);
            step.receive(&json!({"execServerMessage":{"mcpArgs":held()}}))
                .unwrap();
            step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
                .unwrap();
            let mut signed = native();
            let mut unknown = signed["content"][1].clone();
            unknown["toolCallId"] = json!("unknown-call");
            if include_current_call {
                signed["content"].as_array_mut().unwrap().push(unknown);
            } else {
                signed["content"][1] = unknown;
            }
            step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[signed.to_string()]}})).unwrap();
            assert_eq!(step.ready().err().unwrap(), "cursor_native_tool_mismatch");
        }
    }

    #[test]
    fn current_execution_checkpoint_can_arrive_before_its_intent() {
        let mut step = step();
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native().to_string()]}})).unwrap();
        assert!(step.ready().unwrap().is_none());
        start(&mut step);
        step.receive(&json!({"execServerMessage":{"mcpArgs":held()}}))
            .unwrap();
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
            .unwrap();
        let certified = step.ready().unwrap().unwrap();
        assert_eq!(certified.calls.len(), 1);
        assert_eq!(certified.context["nativeMessages"], json!([native()]));
    }

    #[test]
    fn terminal_without_the_current_execution_certificate_cannot_complete() {
        let mut step = step();
        start(&mut step);
        step.receive(&json!({"execServerMessage":{"mcpArgs":held()}}))
            .unwrap();
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))
            .unwrap();
        assert!(step.ready().unwrap().is_none());
        step.receive(&json!({"interactionUpdate":{"turnEnded":{}}}))
            .unwrap();
        assert_eq!(step.finish().err().unwrap(), "cursor_inference_incomplete");
    }

    #[test]
    fn metadata_mcp_encodings_preserve_signed_mixed_batches_and_next_step_replay() {
        for helper in ["GetDynamicTools", "GetMcpTools"] {
            for outer in [
                "CallDynamicTool",
                "CallMcpTool",
                helper,
                &format!("{NAMESPACE}-{helper}"),
            ] {
                let mut step = step();
                let reply = reply_metadata(&mut step, helper);
                assert!(super::super::metadata::mcp_result_matches(
                    &reply,
                    &step.metadata["metadata"]["value"]
                ));
                start(&mut step);
                step.held.insert("raw\ncall".into(), held());
                step.batch.count = Some(2);
                let mut signed = native();
                signed["content"]
                    .as_array_mut()
                    .unwrap()
                    .insert(1, metadata_native(helper, outer));
                step.latest_roots = Some(step.initial_roots.clone());
                step.pending = vec![signed.clone()];
                let result = step.ready().unwrap().unwrap();
                assert_eq!(result.calls.len(), 1);
                assert_eq!(result.calls[0]["toolName"], "echo");
                assert_eq!(
                    result.context["nativeMessages"][0], signed,
                    "signed arguments and wrapper remain unchanged"
                );
                assert_eq!(
                    result.context["metadataProofs"]["metadata"]["mcpArgs"],
                    metadata_mcp(helper)
                );
                assert_eq!(
                    result.context["nativeMessages"][1]["content"][0]["toolName"],
                    outer
                );
                let call = &result.calls[0];
                let mut replay = body();
                replay["messages"].as_array_mut().unwrap().extend([
                    json!({"role":"assistant","content":"","nativeContext":result.context,"tool_calls":[{"id":call["toolCallId"],"function":{"name":"echo","arguments":"{\"value\":\"input\"}"}}]}),
                    json!({"role":"tool","tool_call_id":call["toolCallId"],"content":"LOCAL_RESULT"}),
                ]);
                let history = History::new(&replay).unwrap();
                let roots = history.state["rootPromptMessagesJson"].as_array().unwrap();
                let messages = roots
                    .iter()
                    .map(|root| {
                        serde_json::from_slice::<Value>(
                            history.blobs.get(root.as_str().unwrap()).unwrap(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(messages[2], signed);
                assert_eq!(
                    messages.last().unwrap()["content"][0]["result"],
                    "LOCAL_RESULT"
                );
                for mutation in ["selector", "owner", "helper", "id", "value", "result"] {
                    let mut changed = replay.clone();
                    let context = &mut changed["messages"][2]["nativeContext"];
                    match mutation {
                        "selector" => {
                            context["metadataProofs"]["metadata"]["mcpArgs"]["args"]["pattern"] =
                                json!(".*")
                        }
                        "owner" => {
                            context["nativeMessages"][0]["content"][1]["args"]["namespace"] =
                                json!("foreign")
                        }
                        "helper" => {
                            context["nativeMessages"][0]["content"][1]["toolName"] =
                                json!("ExecuteTool")
                        }
                        "id" => {
                            context["nativeMessages"][0]["content"][1]["toolCallId"] =
                                json!("other")
                        }
                        "value" => {
                            context["metadataProofs"]["metadata"]["value"]["extra"] = json!(true)
                        }
                        "result" => {
                            context["nativeMessages"][1]["content"][0]["result"] = json!("{}")
                        }
                        _ => unreachable!(),
                    }
                    assert!(
                        History::new(&changed).is_err(),
                        "{helper}/{outer}/{mutation}"
                    );
                }
            }
        }
    }

    #[test]
    fn metadata_mcp_rejects_foreign_or_unbound_identity_and_selector_fields() {
        for mutation in [
            "provider",
            "server",
            "name",
            "alias",
            "id",
            "approval",
            "skip",
            "paths",
            "extra",
            "selectorExtra",
            "nestedId",
            "selectorOwner",
            "selectorWrongOwnerField",
            "tool",
            "patternType",
            "patternLength",
            "patternRegex",
            "args",
        ] {
            let mut args = metadata_mcp("GetDynamicTools");
            match mutation {
                "provider" => args["providerIdentifier"] = json!("foreign"),
                "server" => args["serverIdentifier"] = json!("foreign"),
                "name" => args["name"] = json!("foreign-GetDynamicTools"),
                "alias" => {
                    args["toolName"] = json!("unknown");
                    args["name"] = json!("klaus_tools-unknown");
                }
                "id" => args["toolCallId"] = json!(""),
                "approval" => args["smartModeApprovalOnly"] = json!(true),
                "skip" => args["skipApproval"] = json!(true),
                "paths" => args["symbolicPathArguments"] = json!(["path"]),
                "extra" => args["execute"] = json!(true),
                "selectorExtra" => args["args"]["execute"] = json!(true),
                "nestedId" => args["args"]["toolCallId"] = json!("metadata"),
                "selectorOwner" => args["args"]["namespace"] = json!("foreign"),
                "selectorWrongOwnerField" => args["args"]["server"] = json!(NAMESPACE),
                "tool" => args["args"]["toolName"] = json!("absent"),
                "patternType" => args["args"]["pattern"] = json!(null),
                "patternLength" => args["args"]["pattern"] = json!("x".repeat(257)),
                "patternRegex" => {
                    args["args"].as_object_mut().unwrap().remove("toolName");
                    args["args"]["pattern"] = json!("[");
                }
                "args" => args["args"] = json!([]),
                _ => unreachable!(),
            }
            let mut step = step();
            assert!(
                step.started(
                    &json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}})
                )
                .is_err(),
                "{mutation}"
            );
            assert!(
                step.receive(&json!({"execServerMessage":{"mcpArgs":args}}))
                    .is_err(),
                "{mutation}"
            );
            assert!(step.metadata.is_empty());
            assert!(step.held.is_empty());
        }
    }

    #[test]
    fn metadata_mcp_completion_requires_an_exact_previously_replied_projection() {
        for mutation in [
            "notReplied",
            "notStarted",
            "args",
            "id",
            "extra",
            "text",
            "image",
            "structured",
            "reminders",
            "output",
            "error",
            "twice",
        ] {
            let mut step = step();
            let args = metadata_mcp("GetDynamicTools");
            if mutation != "notStarted" {
                step.started(
                    &json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}}),
                )
                .unwrap();
            } else {
                step.batch.model_call_id = json!("one");
            }
            let result = super::super::metadata::mcp_result(
                &step.catalog.mcp_metadata(&args).unwrap().unwrap()["value"],
            );
            if mutation != "notReplied" {
                step.receive(&json!({"execServerMessage":{"mcpArgs":args}}))
                    .unwrap();
            }
            let mut complete = json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args,"result":result}}});
            let mcp = &mut complete["toolCall"]["mcpToolCall"];
            match mutation {
                "args" => mcp["args"]["args"]["pattern"] = json!(".*"),
                "id" => mcp["args"]["toolCallId"] = json!("other"),
                "extra" => mcp["extra"] = json!(true),
                "text" => mcp["result"]["success"]["content"][0]["text"]["text"] = json!("{}"),
                "image" => mcp["result"]["success"]["content"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"image":{"data":"AA==","mimeType":"image/png"}})),
                "structured" => mcp["result"]["success"]["structuredContent"] = json!({}),
                "reminders" => mcp["result"]["success"]["systemReminders"] = json!(["execute"]),
                "output" => mcp["result"]["success"]["outputLocation"] = json!("remote"),
                "error" => mcp["result"]["success"]["isError"] = json!(true),
                "twice" => step.completed(&complete).unwrap(),
                _ => {}
            }
            assert!(step.completed(&complete).is_err(), "{mutation}");
            assert!(step.held.is_empty());
        }
        let mut step = step();
        start(&mut step);
        assert_eq!(step.completed(&json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":held(),"result":{"success":{"content":[]}}}}})).unwrap_err(), "cursor_tool_executed_upstream");
    }

    #[test]
    fn a_metadata_reply_without_a_complete_lifecycle_never_certifies_terminal_history() {
        for started in [false, true] {
            let mut step = step();
            let args = metadata_mcp("GetDynamicTools");
            if started {
                step.started(
                    &json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}}),
                )
                .unwrap();
            }
            let reply = step
                .receive(&json!({"execServerMessage":{"mcpArgs":args}}))
                .unwrap();
            let encoded = reply[0]["execClientMessage"]["mcpResult"]["success"]["content"][0]
                ["text"]["text"]
                .as_str()
                .unwrap();
            let messages = vec![
                json!({"role":"assistant","content":[metadata_native("GetDynamicTools", "CallDynamicTool")]}),
                json!({"role":"tool","content":[{"type":"tool-result","toolCallId":"metadata","toolName":"CallDynamicTool","result":encoded}]}),
                json!({"role":"assistant","content":[{"type":"text","text":""}]}),
            ];
            let mut roots = step.initial_roots.clone();
            for message in &messages {
                roots.push(json!(step
                    .blobs
                    .seed(serde_json::to_vec(message).unwrap())
                    .unwrap()));
            }
            step.checkpoint(&json!({"rootPromptMessagesJson":roots,"pendingToolCalls":[]}))
                .unwrap();
            step.receive(&json!({"interactionUpdate":{"turnEnded":{}}}))
                .unwrap();
            assert!(
                step.metadata.is_empty(),
                "a reply is not a completion proof"
            );
            assert_eq!(step.metadata_replies.len(), 1);
            assert_eq!(step.finish().err().unwrap(), "cursor_inference_incomplete");
            assert_eq!(
                step.certify(&messages, false).err().unwrap(),
                "cursor_metadata_completion_missing"
            );
        }
    }

    #[test]
    fn metadata_mcp_alone_never_satisfies_required_choice_or_publishes_cancelled_work() {
        for choice in [
            json!("auto"),
            json!("required"),
            json!({"type":"function","name":"echo"}),
        ] {
            let mut request = body();
            request["tool_choice"] = choice.clone();
            let mut step = read_step(&request);
            reply_metadata(&mut step, "GetDynamicTools");
            step.batch.count = Some(1);
            assert!(step.ready().unwrap().is_none());
            assert_eq!(step.finish().err().unwrap(), "cursor_inference_incomplete");
            assert!(step.held.is_empty());
            let mut messages = vec![
                json!({"role":"assistant","content":[metadata_native("GetDynamicTools", "CallDynamicTool")]}),
            ];
            step.complete_metadata_history(&mut messages).unwrap();
            if choice == "auto" {
                assert!(step.certify(&messages, false).unwrap().calls.is_empty());
            } else {
                assert_eq!(
                    step.certify(&messages, false).err().unwrap(),
                    "cursor_required_tool_missing"
                );
            }
        }
        let mut request = body();
        request["tools"][0]["function"]["name"] = json!("GetDynamicTools");
        let mut step = read_step(&request);
        let args = metadata_mcp("GetDynamicTools");
        step.started(&json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args}}}))
            .unwrap();
        assert!(step
            .receive(&json!({"execServerMessage":{"mcpArgs":args}}))
            .unwrap()
            .is_empty());
        assert_eq!(
            step.held.len(),
            1,
            "a declared same-name execution tool keeps precedence"
        );
        assert!(step.metadata.is_empty());
        assert_eq!(step.completed(&json!({"modelCallId":"one","toolCall":{"mcpToolCall":{"args":args,"result":{"success":{"content":[]}}}}})).unwrap_err(), "cursor_tool_executed_upstream");
    }

    #[test]
    fn metadata_only_batches_reset_before_next_model_batch_without_changing_event_order() {
        let mut step = step();
        reply_metadata(&mut step, "GetMcpTools");
        step.batch.count = Some(1);
        let helper_call = metadata_native("GetMcpTools", "CallMcpTool");
        let proof = step.metadata["metadata"].clone();
        let result = json!({"role":"tool","content":[{"type":"tool-result","toolCallId":"metadata","toolName":"CallMcpTool","result":proof["value"].to_string(),"experimental_content":[{"type":"text","text":proof["value"].to_string()}]}],"providerOptions":{"cursor":{"highLevelToolCallResult":{"output":super::super::metadata::mcp_result(&proof["value"]),"isError":false}}}});
        let assistant = json!({"role":"assistant","content":[{"type":"reasoning","text":"","signature":"helper-signature"},helper_call]});
        let mut roots = step.initial_roots.clone();
        for message in [&assistant, &result] {
            roots.push(json!(step
                .blobs
                .seed(serde_json::to_vec(message).unwrap())
                .unwrap()));
        }
        step.latest_roots = Some(roots);
        start(&mut step);
        assert_eq!(step.batch.started.len(), 1);
        assert!(step.batch.completed.is_empty());
        step.held.insert("raw\ncall".into(), held());
        step.batch.count = Some(1);
        step.pending = vec![native()];
        let certified = step.ready().unwrap().unwrap();
        assert_eq!(
            certified.context["nativeMessages"],
            json!([assistant, result, native()])
        );
        assert_eq!(certified.calls.len(), 1);
    }

    #[test]
    fn upstream_summary_pressure_returns_to_local_compaction_without_replacing_history() {
        for update in [
            json!({"summaryStarted":{}}),
            json!({"summary":{"summary":"remote summary"}}),
            json!({"summaryCompleted":{"failed":false}}),
        ] {
            let mut step = step();
            let original = step.initial_roots.clone();
            assert_eq!(
                step.receive(&json!({"interactionUpdate":update}))
                    .unwrap_err(),
                "cursor_input_token_limit"
            );
            assert_eq!(step.initial_roots, original);
            assert!(step.held.is_empty());
            assert!(step.pending.is_empty());
        }
    }

    #[test]
    fn blob_hash_and_checkpoint_prefix_prevent_unowned_history_injection() {
        let mut step = step();
        assert!(step
            .blobs
            .insert(STANDARD.encode([0; 32]), b"other".to_vec())
            .is_err());
        assert!(step
            .checkpoint(&json!({"rootPromptMessagesJson":["foreign"]}))
            .is_err());
        assert!(
            step.finish().is_err(),
            "terminal marker and certified native root are required"
        );
    }

    #[test]
    fn checkpoint_prefix_accepts_only_semantically_identical_digest_verified_json() {
        let original = native();
        let mut step = step();
        let key = step
            .blobs
            .seed(serde_json::to_vec(&original).unwrap())
            .unwrap();
        step.initial_roots = vec![json!(key)];
        let encoded = serde_json::to_vec_pretty(&original).unwrap();
        let reordered = STANDARD.encode(sha2::Sha256::digest(&encoded));
        assert_ne!(key, reordered);
        step.checkpoint(&json!({"rootPromptMessagesJson":[reordered]}))
            .unwrap();
        assert!(
            step.new_roots().unwrap().is_none(),
            "checkpoint references may precede their KV blobs"
        );
        step.ended = true;
        assert_eq!(step.finish().err().unwrap(), "cursor_checkpoint_unresolved");
        step.blobs.insert(reordered, encoded).unwrap();
        assert_eq!(step.new_roots().unwrap(), Some(Vec::new()));
        assert_eq!(
            serde_json::from_slice::<Value>(step.blobs.get(&key).unwrap()).unwrap(),
            original,
            "original signed history is retained"
        );
        for mutation in ["signature", "text", "args", "id", "role"] {
            let mut changed = original.clone();
            match mutation {
                "signature" => changed["content"][0]["signature"] = json!("changed"),
                "text" => changed["content"][0]["text"] = json!("changed"),
                "args" => changed["content"][1]["args"]["arguments"]["value"] = json!("changed"),
                "id" => changed["content"][1]["toolCallId"] = json!("changed"),
                "role" => changed["role"] = json!("system"),
                _ => unreachable!(),
            }
            let key = step
                .blobs
                .seed(serde_json::to_vec(&changed).unwrap())
                .unwrap();
            step.checkpoint(&json!({"rootPromptMessagesJson":[key]}))
                .unwrap();
            assert_eq!(
                step.new_roots().err().unwrap(),
                "cursor_checkpoint_history_changed",
                "{mutation}"
            );
        }
        assert!(step
            .checkpoint(&json!({"rootPromptMessagesJson":[]}))
            .is_err());
        step.initial_roots.push(json!(step
            .blobs
            .seed(b"{\"role\":\"user\",\"content\":\"user\"}".to_vec())
            .unwrap()));
        let mut reversed = step.initial_roots.clone();
        reversed.reverse();
        step.checkpoint(&json!({"rootPromptMessagesJson":reversed}))
            .unwrap();
        assert_eq!(
            step.new_roots().err().unwrap(),
            "cursor_checkpoint_history_changed"
        );
    }

    #[test]
    fn same_variant_replay_preserves_original_signed_message_and_real_local_result() {
        let mut step = step();
        step.latest_roots = Some(step.initial_roots.clone());
        start(&mut step);
        step.held.insert("raw\ncall".into(), held());
        step.batch.count = Some(1);
        step.pending = vec![native()];
        let certified = step.ready().unwrap().unwrap();
        let call = &certified.calls[0];
        let mut replay = body();
        replay["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":"","nativeContext":certified.context,"tool_calls":[{"id":call["toolCallId"],"function":{"name":"echo","arguments":"{\"value\":\"input\"}"}}]}),
            json!({"role":"tool","tool_call_id":call["toolCallId"],"content":"LOCAL_RESULT"}),
        ]);
        let history = History::new(&replay).unwrap();
        let roots = history.state["rootPromptMessagesJson"].as_array().unwrap();
        let messages = roots
            .iter()
            .map(|root| {
                serde_json::from_slice::<Value>(history.blobs.get(root.as_str().unwrap()).unwrap())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(messages[2], native());
        assert_eq!(
            messages[3]["content"][0],
            json!({"type":"tool-result","toolCallId":"raw\ncall","toolName":"CallDynamicTool","result":"LOCAL_RESULT"})
        );
        for field in ["role", "arguments", "outerName"] {
            let mut corrupted = replay.clone();
            let context = &mut corrupted["messages"][2]["nativeContext"];
            match field {
                "role" => context["nativeMessages"][0]["role"] = json!("system"),
                "arguments" => {
                    context["nativeMessages"][0]["content"][1]["args"]["arguments"] =
                        json!({"value":"changed"})
                }
                _ => context["callMap"][0]["outerName"] = json!("Shell"),
            }
            assert!(
                History::new(&corrupted).is_err(),
                "corrupt {field} cannot change native replay independently of visible history"
            );
        }
    }

    fn direct_mcp_fixture(body: &Value, outer: &str) -> (Value, Value) {
        let catalog = Catalog::new(body).unwrap();
        let tool = &catalog.tools[0];
        let mut held = held();
        held["name"] = json!(format!("{NAMESPACE}-{}", tool.alias));
        held["toolName"] = json!(tool.alias);
        let mut native = native();
        native["content"][1]["toolName"] = json!(outer);
        native["content"][1]["args"] = held["args"].clone();
        let contract =
            &mut native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]["raw\ncall"];
        contract["outerToolName"] = json!(outer);
        contract["isDynamic"] = json!(false);
        (held, native)
    }

    fn prepare_direct_mcp(body: &Value, held: Value, native: Value) -> Result<Certified, String> {
        let mut step = read_step(body);
        step.receive(&json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"one","toolCall":{"mcpToolCall":{"args":held}}}}}))?;
        let replies = step.receive(&json!({"execServerMessage":{"mcpArgs":held}}))?;
        assert!(
            replies.is_empty(),
            "local execution is never acknowledged upstream"
        );
        step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))?;
        step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native.to_string()]}}))?;
        step.ready()?.ok_or_else(|| "fixture_not_ready".into())
    }

    fn direct_mcp_replay(body: &Value, certified: &Certified) -> Value {
        let call = &certified.calls[0];
        let mut replay = body.clone();
        replay["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":certified.text,"nativeContext":certified.context,"tool_calls":[{"id":call["toolCallId"],"function":{"name":call["toolName"],"arguments":call["args"].to_string()}}]}),
            json!({"role":"tool","tool_call_id":call["toolCallId"],"content":"LOCAL_RESULT"}),
        ]);
        replay
    }

    #[test]
    fn direct_mcp_outer_rewrites_preserve_frozen_identity_signed_history_and_local_replay() {
        let mut custom = body();
        custom["tools"][0]["function"]["name"] = json!("custom.namespace/tool:echo");
        for body in [body(), custom] {
            for outer in [
                "mcp_klaus_tools_echo".to_string(),
                "normalized_tool_6d31a42".to_string(),
                "a".repeat(256),
                "工具".repeat(42),
            ] {
                for omitted_default in [false, true] {
                    let (held, mut native) = direct_mcp_fixture(&body, &outer);
                    if omitted_default {
                        native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                            ["raw\ncall"]
                            .as_object_mut()
                            .unwrap()
                            .remove("isDynamic");
                    }
                    let certified =
                        prepare_direct_mcp(&body, held.clone(), native.clone()).unwrap();
                    assert_eq!(certified.calls.len(), 1);
                    assert_eq!(
                        certified.calls[0]["toolName"],
                        body["tools"][0]["function"]["name"]
                    );
                    assert_eq!(certified.calls[0]["args"], held["args"]);
                    assert_eq!(certified.context["nativeMessages"], json!([native.clone()]));
                    assert_eq!(certified.context["callMap"][0]["intent"], "direct_mcp");
                    assert_eq!(certified.context["callMap"][0]["outerName"], outer);
                    let replay = direct_mcp_replay(&body, &certified);
                    let history = History::new(&replay).unwrap();
                    let roots = history.state["rootPromptMessagesJson"].as_array().unwrap();
                    let assistant: Value = serde_json::from_slice(
                        history.blobs.get(roots[2].as_str().unwrap()).unwrap(),
                    )
                    .unwrap();
                    let result: Value = serde_json::from_slice(
                        history.blobs.get(roots[3].as_str().unwrap()).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(assistant, native);
                    assert_eq!(result["content"][0]["toolName"], outer);
                    assert_eq!(result["content"][0]["toolCallId"], "raw\ncall");
                    assert_eq!(result["content"][0]["result"], "LOCAL_RESULT");
                }
            }
        }
    }

    #[test]
    fn direct_mcp_outer_proof_rejects_reserved_wrappers_unbounded_names_and_changed_arguments() {
        for outer in [
            "".to_string(),
            " ".to_string(),
            "a".repeat(257),
            "界".repeat(86),
            "tool\nname".to_string(),
            "CallDynamicTool".to_string(),
            "CallMcpTool".to_string(),
            "Read".to_string(),
            "GetDynamicTools".to_string(),
            "GetMcpTools".to_string(),
        ] {
            let body = body();
            let (held, native) = direct_mcp_fixture(&body, &outer);
            assert!(
                prepare_direct_mcp(&body, held, native).is_err(),
                "{outer:?}"
            );
        }
        for mutation in [
            "args",
            "array",
            "contractKind",
            "contractOuter",
            "contractId",
            "rawId",
            "dynamic",
            "dynamicType",
        ] {
            let body = body();
            let (held, mut native) = direct_mcp_fixture(&body, "mcp_klaus_tools_echo");
            match mutation {
                "args" => native["content"][1]["args"]["value"] = json!("changed"),
                "array" => native["content"][1]["args"] = json!([]),
                "contractKind" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["toolIdentifier"] = json!("SHELL")
                }
                "contractOuter" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["outerToolName"] = json!("other")
                }
                "contractId" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["toolCallId"] = json!("other")
                }
                "rawId" => native["content"][1]["toolCallId"] = json!("other"),
                "dynamic" => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["isDynamic"] = json!(true)
                }
                _ => {
                    native["providerOptions"]["cursor"]["pendingToolExecutionContracts"]
                        ["raw\ncall"]["isDynamic"] = json!("false")
                }
            }
            assert!(
                prepare_direct_mcp(&body, held, native).is_err(),
                "{mutation}"
            );
        }
    }

    #[test]
    fn direct_mcp_outer_proof_requires_started_held_identity_and_frozen_catalog_ownership() {
        for mutation in [
            "startedArgs",
            "heldArgs",
            "name",
            "alias",
            "provider",
            "server",
            "undeclared",
        ] {
            let mut body = body();
            let (mut held, native) = direct_mcp_fixture(&body, "mcp_klaus_tools_echo");
            if mutation == "undeclared" {
                body["tools"] = json!([]);
            }
            let mut step = read_step(&body);
            let mut started = held.clone();
            match mutation {
                "startedArgs" => started["args"]["value"] = json!("different"),
                "heldArgs" => held["args"]["value"] = json!("different"),
                "name" => held["name"] = json!("rewritten_execution_name"),
                "alias" => held["toolName"] = json!("unknown"),
                "provider" => held["providerIdentifier"] = json!("foreign"),
                "server" => held["serverIdentifier"] = json!("foreign"),
                _ => {}
            }
            let result = (|| -> Result<Option<Certified>, String> {
                step.receive(&json!({"interactionUpdate":{"toolCallStarted":{"modelCallId":"one","toolCall":{"mcpToolCall":{"args":started}}}}}))?;
                step.receive(&json!({"execServerMessage":{"mcpArgs":held}}))?;
                step.receive(&json!({"interactionUpdate":{"toolRequestsListed":{"callCount":1}}}))?;
                step.receive(&json!({"conversationCheckpointUpdate":{"rootPromptMessagesJson":step.initial_roots,"pendingToolCalls":[native.to_string()]}}))?;
                step.ready()
            })();
            assert!(result.is_err(), "{mutation}");
        }
    }

    #[test]
    fn direct_mcp_replay_rejects_independent_mapping_contract_and_visible_history_changes() {
        let body = body();
        let (held, native) = direct_mcp_fixture(&body, "mcp_klaus_tools_echo");
        let certified = prepare_direct_mcp(&body, held, native).unwrap();
        let replay = direct_mcp_replay(&body, &certified);
        for mutation in [
            "mappingOuter",
            "nativeOuter",
            "contractOuter",
            "nativeArgs",
            "mappingArgs",
            "visibleName",
            "mappingName",
            "mappingAlias",
            "rawId",
            "safeId",
            "contractId",
            "contractKind",
            "dynamic",
            "dynamicType",
            "intent",
            "missingIntent",
            "reservedOuter",
        ] {
            let mut corrupted = replay.clone();
            let context = &mut corrupted["messages"][2]["nativeContext"];
            match mutation {
                "mappingOuter" => context["callMap"][0]["outerName"] = json!("other"),
                "nativeOuter" => {
                    context["nativeMessages"][0]["content"][1]["toolName"] = json!("other")
                }
                "contractOuter" => {
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["outerToolName"] =
                        json!("other")
                }
                "nativeArgs" => {
                    context["nativeMessages"][0]["content"][1]["args"]["value"] = json!("changed")
                }
                "mappingArgs" => context["callMap"][0]["args"]["value"] = json!("changed"),
                "visibleName" => {
                    corrupted["messages"][2]["tool_calls"][0]["function"]["name"] = json!("other")
                }
                "mappingName" => context["callMap"][0]["name"] = json!("other"),
                "mappingAlias" => context["callMap"][0]["alias"] = json!("other"),
                "rawId" => context["callMap"][0]["rawId"] = json!("other"),
                "safeId" => context["callMap"][0]["safeId"] = json!("other"),
                "contractId" => {
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["toolCallId"] =
                        json!("other")
                }
                "contractKind" => {
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["toolIdentifier"] =
                        json!("SHELL")
                }
                "dynamic" => {
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["isDynamic"] = json!(true)
                }
                "dynamicType" => {
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["isDynamic"] = json!("false")
                }
                "intent" => context["callMap"][0]["intent"] = json!("unknown"),
                "missingIntent" => {
                    context["callMap"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("intent");
                }
                _ => {
                    context["callMap"][0]["outerName"] = json!("CallDynamicTool");
                    context["nativeMessages"][0]["content"][1]["toolName"] =
                        json!("CallDynamicTool");
                    context["nativeMessages"][0]["providerOptions"]["cursor"]
                        ["pendingToolExecutionContracts"]["raw\ncall"]["outerToolName"] =
                        json!("CallDynamicTool");
                }
            }
            assert!(History::new(&corrupted).is_err(), "{mutation}");
        }
    }

    #[test]
    fn canonical_direct_mcp_context_without_an_intent_remains_replayable() {
        let body = body();
        let (held, native) = direct_mcp_fixture(&body, "klaus_tools-echo");
        let certified = prepare_direct_mcp(&body, held, native).unwrap();
        let mut replay = direct_mcp_replay(&body, &certified);
        replay["messages"][2]["nativeContext"]["callMap"][0]
            .as_object_mut()
            .unwrap()
            .remove("intent");
        assert!(History::new(&replay).is_ok());
    }

    #[test]
    fn model_or_source_switch_ignores_foreign_opaque_metadata_but_keeps_visible_history() {
        for opaque in ["foreign-provider-ciphertext".to_string(),json!({"kind":"klaus.provider-context.v1","scope":"cursor","model":"old","message":{"format":CONTEXT_FORMAT}}).to_string()] {
            let body=super::super::responses::normalize(&json!({"model":"new","input":[{"role":"user","content":"hello"},{"type":"reasoning","encrypted_content":opaque},{"role":"assistant","content":[{"type":"output_text","text":"Visible"}]},{"type":"function_call","call_id":"old-call","name":"echo","arguments":"{\"value\":\"x\"}"},{"type":"function_call_output","call_id":"old-call","output":"result"}]})).unwrap();
            let history=History::new(&body).unwrap();
            let messages=history.state["rootPromptMessagesJson"].as_array().unwrap().iter().map(|root|serde_json::from_slice::<Value>(history.blobs.get(root.as_str().unwrap()).unwrap()).unwrap()).collect::<Vec<_>>();
            assert_eq!(messages[1]["content"][0]["text"],"Visible");
            assert_eq!(messages[2]["content"][0]["result"],"result");
            assert!(!json!(messages).to_string().contains("nativeContext"));
        }
    }
}
