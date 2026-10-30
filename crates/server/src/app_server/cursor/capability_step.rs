//! One explicitly requested native operation. A held caller-owned barrier
//! prevents model continuation; no execution result is acknowledged upstream.
use super::{bridge, changes, credential, step_history::History};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use futures::TryStreamExt;
use serde_json::{json, Value};
use std::collections::HashSet;
use tokio::sync::mpsc;

const OWNER: &str = "klaus_capability";
const OUTER: &str = "CallDynamicTool";
const MAX_RESPONSE: usize = 8 * 1024 * 1024;
const MAX_EVENTS: usize = 512;

#[path = "capability_image.rs"]
mod image;
pub(super) use image::generate as generate_image;

fn invalid() -> String {
    "cursor_native_operation_response_invalid".into()
}

fn keys(value: &Value, allowed: &[&str]) -> Result<(), String> {
    if value
        .as_object()
        .is_none_or(|map| map.keys().any(|key| !allowed.contains(&key.as_str())))
    {
        return Err(invalid());
    }
    Ok(())
}

fn text<'a>(value: &'a Value, field: &str, max: usize) -> Result<&'a str, String> {
    match value.get(field) {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .filter(|s| s.len() <= max)
            .ok_or_else(invalid),
    }
}

fn id(value: &Value) -> Result<u32, String> {
    match value.get("id") {
        None => Ok(0),
        Some(value) => value
            .as_u64()
            .and_then(|id| u32::try_from(id).ok())
            .ok_or_else(invalid),
    }
}

fn action<'a>(packet: &'a Value, allowed: &[&str]) -> Result<&'a str, String> {
    keys(packet, allowed)?;
    let actions = packet
        .as_object()
        .ok_or_else(invalid)?
        .keys()
        .filter(|key| key.as_str() != "ttftBreakdown")
        .collect::<Vec<_>>();
    if actions.len() != 1 {
        return Err(invalid());
    }
    // ttftBreakdown is upstream timing telemetry riding beside a native
    // packet. Klaus discards it after receipt, so its field set is not
    // validated: telemetry drift must not abort an in-flight turn.
    Ok(actions[0].as_str())
}

struct SearchStep {
    history: History,
    query: String,
    native_id: String,
    barrier_id: String,
    context: Value,
    native_started: bool,
    barrier_started: bool,
    barrier_held: bool,
    approval: Option<Value>,
    approved: bool,
    result: Option<Value>,
    invocation: Option<String>,
    partials: usize,
    contexts: usize,
    events: usize,
}

impl SearchStep {
    fn new(query: &str) -> Result<Self, String> {
        let native_id = uuid::Uuid::new_v4().to_string();
        let barrier_id = uuid::Uuid::new_v4().to_string();
        let pending = json!({"role":"assistant","content":[
            {"type":"tool-call","toolCallId":native_id,"toolName":OUTER,"args":{"namespace":"cursor","toolName":"WebSearch","arguments":{"search_term":query,"explanation":"Find sources for the caller's exact search query."}}},
            {"type":"tool-call","toolCallId":barrier_id,"toolName":OUTER,"args":{"namespace":OWNER,"toolName":"barrier","arguments":{"hold":"pending"}}}
        ],"providerOptions":{"cursor":{"pendingToolExecutionContracts":{
            (native_id.clone()):{"toolCallId":native_id,"outerToolName":OUTER,"toolIdentifier":"WEB_SEARCH","isDynamic":true,"effectiveToolName":"WebSearch","effectiveToolIdentifier":"WEB_SEARCH","allowedToolNames":[OUTER,"GetDynamicTools"]},
            (barrier_id.clone()):{"toolCallId":barrier_id,"outerToolName":OUTER,"toolIdentifier":"MCP","isDynamic":true,"allowedToolNames":[OUTER,"GetDynamicTools"]}
        }}}});
        let definition = json!({"name":format!("{OWNER}-barrier"),"providerIdentifier":OWNER,"toolName":"barrier","description":"Caller-owned synchronization barrier. No execution result will be supplied.","inputSchema":{"type":"object","properties":{"hold":{"type":"string","enum":["pending"]}},"required":["hold"],"additionalProperties":false},"annotationsJson":"{\"readOnlyHint\":true}"});
        let descriptor = json!({"serverName":OWNER,"serverIdentifier":OWNER,"tools":[{"toolName":"barrier","description":definition["description"],"inputSchema":definition["inputSchema"],"inputSchemaJson":definition["inputSchema"].to_string(),"annotationsJson":definition["annotationsJson"]}]});
        let context = json!({"tools":[definition],"webSearchEnabled":true,"webFetchEnabled":false,"readLintsEnabled":false,"searchConversationsEnabled":false,"sendMessageEnabled":false,"mcpMetaToolOptions":{"enabled":true,"mcpDescriptors":[descriptor]},"rulesInfoComplete":true,"envInfoComplete":true,"repositoryInfoComplete":true,"customSubagentsInfoComplete":true,"agentSkillsInfoComplete":true,"mcpInfoComplete":true,"gitRepoInfoComplete":true,"mcpFileSystemInfoComplete":true,"gitStatusInfoComplete":true});
        let mut history = History::for_capability(&json!({"messages":[
            {"role":"system","content":"Execute only the explicitly pending native web search. Do not run a model continuation or any other tool."},
            {"role":"user","content":query}
        ]}))?;
        let root = history
            .blobs
            .seed(serde_json::to_vec(&pending).map_err(|_| invalid())?)?;
        history.state["rootPromptMessagesJson"]
            .as_array_mut()
            .ok_or_else(invalid)?
            .push(json!(root));
        history.state["pendingToolCalls"] = json!([pending.to_string()]);
        Ok(Self {
            history,
            query: query.into(),
            native_id,
            barrier_id,
            context,
            native_started: false,
            barrier_started: false,
            barrier_held: false,
            approval: None,
            approved: false,
            result: None,
            invocation: None,
            partials: 0,
            contexts: 0,
            events: 0,
        })
    }

    fn request(&self, model: &Value) -> Result<Value, String> {
        let legacy = model["legacySlug"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("cursor_model_variant_unavailable")?;
        if !model["requestedModel"].is_object() {
            return Err("cursor_model_variant_unavailable".into());
        }
        Ok(
            json!({"runRequest":{"conversationId":uuid::Uuid::new_v4().to_string(),"conversationState":self.history.state,"requestedModel":model["requestedModel"],"modelDetails":{"modelId":legacy,"maxMode":model["requestedModel"]["maxMode"]},"mcpTools":{"mcpTools":self.context["tools"]},"canCreateCloudSubagents":false,"clientSupportsSendToUser":false,"suggestNextPrompt":false,"action":{"resumeAction":{"requestContext":self.context}}}}),
        )
    }

    fn native_args(&self, args: &Value) -> Result<(), String> {
        keys(args, &["searchTerm", "toolCallId"])?;
        if args["searchTerm"] != self.query || args["toolCallId"] != self.native_id {
            return Err(invalid());
        }
        Ok(())
    }

    fn barrier_args(&self, args: &Value) -> Result<(), String> {
        keys(
            args,
            &[
                "name",
                "args",
                "toolCallId",
                "providerIdentifier",
                "toolName",
                "serverIdentifier",
                "smartModeApprovalOnly",
                "skipApproval",
                "symbolicPathArguments",
            ],
        )?;
        if args["toolCallId"] != self.barrier_id
            || args["providerIdentifier"] != OWNER
            || args["toolName"] != "barrier"
            || ![format!("{OWNER}-barrier"), format!("mcp_{OWNER}_barrier")]
                .iter()
                .any(|name| args["name"] == *name)
            || args["args"] != json!({"hold":"pending"})
            || args.get("serverIdentifier").is_some_and(|v| v != OWNER)
            || args
                .get("smartModeApprovalOnly")
                .is_some_and(|v| v != false)
            || args.get("skipApproval").is_some_and(|v| v != false)
            || args
                .get("symbolicPathArguments")
                .is_some_and(|v| v != &json!([]))
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn tool(
        &self,
        call: &Value,
        native: bool,
        partial: bool,
        completed: bool,
    ) -> Result<(), String> {
        let kind = if native {
            "webSearchToolCall"
        } else {
            "mcpToolCall"
        };
        keys(
            call,
            &[
                kind,
                "toolCallId",
                "hookAdditionalContexts",
                "startedAtMs",
                "completedAtMs",
            ],
        )?;
        let expected = if native {
            &self.native_id
        } else {
            &self.barrier_id
        };
        if call.get("toolCallId").is_some_and(|id| id != expected) {
            return Err(invalid());
        }
        if call
            .get("hookAdditionalContexts")
            .is_some_and(|value| value != &json!([]))
        {
            return Err(invalid());
        }
        let inner = call.get(kind).ok_or_else(invalid)?;
        keys(
            inner,
            if completed {
                &["args", "result"]
            } else if !native {
                &["args", "description"]
            } else {
                &["args"]
            },
        )?;
        if !native {
            text(inner, "description", 8192)?;
        }
        let args = inner.get("args");
        if partial && args.is_none_or(|value| value == &json!({})) {
            return Ok(());
        }
        if native {
            self.native_args(args.ok_or_else(invalid)?)
        } else {
            self.barrier_args(args.ok_or_else(invalid)?)
        }
    }

    fn invocation(&mut self, event: &Value) -> Result<(), String> {
        if let Some(value) = event.get("modelCallId") {
            let value = value
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 2048)
                .ok_or_else(invalid)?;
            if self.invocation.as_deref().is_some_and(|old| old != value) {
                return Err(invalid());
            }
            self.invocation = Some(value.into());
        }
        Ok(())
    }

    fn receive(&mut self, packet: &Value) -> Result<Vec<Value>, String> {
        self.events += 1;
        if self.events > MAX_EVENTS {
            return Err("cursor_response_too_large".into());
        }
        let action = action(
            packet,
            &[
                "kvServerMessage",
                "execServerMessage",
                "conversationCheckpointUpdate",
                "interactionQuery",
                "interactionUpdate",
                "ttftBreakdown",
            ],
        )?;
        let mut replies = Vec::new();
        match action {
            "kvServerMessage" => {
                let kv = &packet["kvServerMessage"];
                keys(kv, &["id", "spanContext", "getBlobArgs", "setBlobArgs"])?;
                let id = id(kv)?;
                if kv.get("getBlobArgs").is_some() == kv.get("setBlobArgs").is_some() {
                    return Err(invalid());
                }
                if let Some(get) = kv.get("getBlobArgs") {
                    keys(get, &["blobId"])?;
                    replies.push(json!({"kvClientMessage":{"id":id,"getBlobResult":{"blobData":STANDARD.encode(self.history.blobs.get(text(get,"blobId",128)?)?)}}}));
                } else {
                    let set = &kv["setBlobArgs"];
                    keys(set, &["blobId", "blobData"])?;
                    let bytes = STANDARD
                        .decode(text(set, "blobData", 6 * 1024 * 1024)?)
                        .map_err(|_| invalid())?;
                    self.history
                        .blobs
                        .insert(text(set, "blobId", 128)?.into(), bytes)?;
                    replies.push(json!({"kvClientMessage":{"id":id,"setBlobResult":{}}}));
                }
            }
            "execServerMessage" => {
                let exec = &packet["execServerMessage"];
                keys(
                    exec,
                    &[
                        "id",
                        "execId",
                        "spanContext",
                        "acceptHookAdditionalContexts",
                        "requestContextArgs",
                        "mcpArgs",
                    ],
                )?;
                let id = id(exec)?;
                let exec_id = text(exec, "execId", 2048)?;
                if exec
                    .get("acceptHookAdditionalContexts")
                    .is_some_and(|v| !v.is_boolean())
                    || exec.get("requestContextArgs").is_some() == exec.get("mcpArgs").is_some()
                {
                    return Err(invalid());
                }
                if let Some(args) = exec.get("mcpArgs") {
                    self.barrier_args(args)?;
                    if self.barrier_held {
                        return Err(invalid());
                    }
                    self.barrier_held = true;
                    // Deliberately no execution response. This unresolved call
                    // prevents any model continuation after the native result.
                } else {
                    keys(&exec["requestContextArgs"], &[])?;
                    self.contexts += 1;
                    if self.contexts > 4 {
                        return Err(invalid());
                    }
                    replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"requestContextResult":{"success":{"requestContext":self.context}}}}));
                }
            }
            "interactionQuery" => {
                let query = &packet["interactionQuery"];
                keys(query, &["id", "webSearchRequestQuery"])?;
                id(query)?;
                keys(&query["webSearchRequestQuery"], &["args"])?;
                self.native_args(&query["webSearchRequestQuery"]["args"])?;
                if self.approval.replace(query.clone()).is_some() {
                    return Err(invalid());
                }
            }
            "conversationCheckpointUpdate" => {
                // Checkpoints are never replayed or persisted by a capability.
                // Their bounded JSON has already passed frame/total limits.
                if !packet["conversationCheckpointUpdate"].is_object() {
                    return Err(invalid());
                }
            }
            "interactionUpdate" => {
                let update = &packet["interactionUpdate"];
                keys(
                    update,
                    &[
                        "heartbeat",
                        "partialToolCall",
                        "toolCallStarted",
                        "toolCallCompleted",
                        "messageStartedAtMs",
                    ],
                )?;
                let events = update
                    .as_object()
                    .ok_or_else(invalid)?
                    .keys()
                    .filter(|key| key.as_str() != "messageStartedAtMs")
                    .collect::<Vec<_>>();
                if events.len() != 1 {
                    return Err(invalid());
                }
                match events[0].as_str() {
                    "heartbeat" => {
                        keys(&update["heartbeat"], &[])?;
                    }
                    "partialToolCall" => {
                        let partial = &update["partialToolCall"];
                        keys(
                            partial,
                            &["callId", "toolCall", "argsTextDelta", "modelCallId"],
                        )?;
                        self.partials += 1;
                        if self.partials > 8 || !text(partial, "argsTextDelta", 1)?.is_empty() {
                            return Err(invalid());
                        }
                        let native = partial["callId"] == self.native_id;
                        if !native && partial["callId"] != self.barrier_id {
                            return Err(invalid());
                        }
                        self.invocation(partial)?;
                        self.tool(&partial["toolCall"], native, true, false)?;
                    }
                    "toolCallStarted" => {
                        let event = &update["toolCallStarted"];
                        keys(event, &["callId", "toolCall", "modelCallId"])?;
                        self.invocation(event)?;
                        let native = event["toolCall"].get("webSearchToolCall").is_some();
                        self.tool(&event["toolCall"], native, false, false)?;
                        if event["callId"].as_str()
                            != Some(if native {
                                self.native_id.as_str()
                            } else {
                                self.barrier_id.as_str()
                            })
                        {
                            return Err(invalid());
                        }
                        let started = if native {
                            &mut self.native_started
                        } else {
                            &mut self.barrier_started
                        };
                        if *started {
                            return Err(invalid());
                        }
                        *started = true;
                    }
                    "toolCallCompleted" => {
                        let event = &update["toolCallCompleted"];
                        keys(event, &["callId", "toolCall", "modelCallId"])?;
                        self.invocation(event)?;
                        if event["callId"] != self.native_id {
                            return Err(invalid());
                        }
                        if !self.approved
                            || !self.native_started
                            || !self.barrier_started
                            || !self.barrier_held
                            || self.result.is_some()
                        {
                            return Err(invalid());
                        }
                        self.tool(&event["toolCall"], true, false, true)?;
                        let result = &event["toolCall"]["webSearchToolCall"]["result"];
                        keys(result, &["success", "error", "rejected"])?;
                        if result.as_object().ok_or_else(invalid)?.len() != 1 {
                            return Err(invalid());
                        }
                        let success = result
                            .get("success")
                            .ok_or("cursor_native_service_failed")?;
                        self.result = Some(project_search(success)?);
                    }
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
        if !self.approved && self.barrier_held && self.barrier_started {
            if let Some(query) = &self.approval {
                replies.push(json!({"interactionResponse":{"id":id(query)?,"webSearchRequestResponse":{"approved":{}}}}));
                self.approved = true;
            }
        }
        Ok(replies)
    }
}

fn public_url(address: &str) -> Option<url::Url> {
    if address.is_empty() || address.len() > 8192 || address.chars().any(char::is_control) {
        return None;
    }
    let parsed = url::Url::parse(address).ok()?;
    (matches!(parsed.scheme(), "https" | "http")
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.host_str().is_some())
    .then_some(parsed)
}

fn snippet(text: &str) -> &str {
    let mut end = text.len().min(32768);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn project_search(success: &Value) -> Result<Value, String> {
    keys(success, &["references"])?;
    let empty = Vec::new();
    let references = match success.get("references") {
        None => &empty,
        Some(value) => value.as_array().ok_or_else(invalid)?,
    };
    if references.len() > 50 {
        return Err("cursor_search_response_invalid".into());
    }
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    let mut answer = String::new();
    for reference in references {
        keys(reference, &["title", "url", "chunk"])?;
        let title = text(reference, "title", 4096)?;
        let address = text(reference, "url", 8192)?;
        let chunk = text(reference, "chunk", 256 * 1024)?;
        if answer.len() + chunk.len() + 2 > 512 * 1024 {
            return Err("cursor_search_response_invalid".into());
        }
        if !answer.is_empty() {
            answer.push_str("\n\n");
        }
        answer.push_str(chunk);
        if !address.is_empty() {
            public_url(address).ok_or("cursor_search_response_invalid")?;
            if seen.insert(address.to_string()) {
                results.push(json!({"title":title,"url":address,"snippet":snippet(chunk)}));
            }
        } else {
            // Answer text may carry citations instead of separate documents.
            // Extract explicit links only; its prose/markup is not a schema.
            for line in chunk.lines().take(2048) {
                let Some((_, rest)) = line.split_once('[') else {
                    continue;
                };
                let Some((label, rest)) = rest.split_once("](") else {
                    continue;
                };
                let Some((url, _)) = rest.split_once(')') else {
                    continue;
                };
                if label.len() > 4096 || results.len() >= 50 {
                    continue;
                }
                if public_url(url).is_some() && seen.insert(url.to_string()) {
                    results.push(json!({"title":label,"url":url,"snippet":snippet(chunk)}));
                }
            }
        }
    }
    Ok(json!({"results":results,"answer":answer}))
}

trait NativeOperation {
    fn request(&self, model: &Value) -> Result<Value, String>;
    fn receive(&mut self, packet: &Value) -> Result<Vec<Value>, String>;
    fn take_result(&mut self) -> Option<Value>;
    fn limits(&self) -> (usize, usize);
}

fn certified_result(
    step: &mut impl NativeOperation,
    decoder: &bridge::Decoder,
) -> Result<Option<Value>, String> {
    let result = step.take_result();
    if result.is_some() && !decoder.is_empty() {
        return Err(invalid());
    }
    Ok(result)
}

impl NativeOperation for SearchStep {
    fn request(&self, model: &Value) -> Result<Value, String> {
        SearchStep::request(self, model)
    }
    fn receive(&mut self, packet: &Value) -> Result<Vec<Value>, String> {
        SearchStep::receive(self, packet)
    }
    fn take_result(&mut self) -> Option<Value> {
        self.result.take()
    }
    fn limits(&self) -> (usize, usize) {
        (MAX_RESPONSE, 1024 * 1024)
    }
}

async fn invoke(
    mut step: impl NativeOperation,
    credential: &credential::Credential,
    generation: u64,
    model: &Value,
) -> Result<Value, String> {
    let mut changed = changes().subscribe();
    credential::check_generation(generation)?;
    let (send, receive) = mpsc::channel::<Bytes>(4);
    send.send(bridge::encode_frame(step.request(model)?)?)
        .await
        .map_err(|_| "cursor_request_cancelled")?;
    let body = futures::stream::unfold(receive, |mut rx| async move {
        rx.recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), rx))
    });
    let client = credential::inference_client()?;
    let request = credential::authenticated(
        client.post(format!(
            "{}/agent.v1.AgentService/Run",
            credential.inference_origin()?
        )),
        credential,
    )
    .header(
        "x-cursor-checksum",
        credential.checksum(chrono::Utc::now().timestamp_millis())?,
    )
    .header("content-type", "application/connect+json")
    .header("x-cursor-streaming", "true")
    .version(reqwest::Version::HTTP_2)
    .body(reqwest::Body::wrap_stream(body))
    .send();
    let response = tokio::select! {biased;_=changed.changed()=>return Err("cursor_credential_changed".into()),result=request=>result.map_err(|_|"cursor_network_error")?};
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let bytes = tokio::select! {biased;_=changed.changed()=>return Err("cursor_credential_changed".into()),result=credential::bounded_body(response)=>result?};
        let error = credential::http_error(
            status,
            serde_json::from_slice::<Value>(&bytes).ok().as_ref(),
        );
        return Err(if error == credential::NEEDS_REIMPORT {
            credential::mark_rejected(generation)
        } else {
            error
        });
    }
    let (max_response, max_frame) = step.limits();
    let mut stream = response.bytes_stream();
    let mut decoder = bridge::Decoder::with_max_frame(max_frame)?;
    let mut total = 0usize;
    loop {
        let chunk = tokio::select! {biased;_=changed.changed()=>return Err("cursor_credential_changed".into()),result=stream.try_next()=>result.map_err(|_|"cursor_response_read_failed")?};
        let chunk = chunk.ok_or_else(invalid)?;
        total = total.saturating_add(chunk.len());
        if total > max_response {
            return Err("cursor_response_too_large".into());
        }
        for (terminal, packet) in decoder.push(&chunk)? {
            credential::check_generation(generation)?;
            if terminal || packet.get("error").is_some() {
                if packet.get("error").is_some() {
                    let error = credential::upstream_error(&packet);
                    return Err(if error == credential::NEEDS_REIMPORT {
                        credential::mark_rejected(generation)
                    } else {
                        error
                    });
                }
                return Err(invalid());
            }
            for reply in step.receive(&packet)? {
                tokio::select! {biased;_=changed.changed()=>return Err("cursor_credential_changed".into()),result=send.send(bridge::encode_frame(reply)?)=>result.map_err(|_|"cursor_request_cancelled")?}
            }
        }
        if let Some(result) = certified_result(&mut step, &decoder)? {
            // The barrier is still unresolved. Close both stream halves before
            // making the single native result observable to any caller.
            drop(stream);
            drop(send);
            drop(client);
            credential::check_generation(generation)?;
            return Ok(result);
        }
    }
}

pub(super) async fn search(
    credential: &credential::Credential,
    generation: u64,
    model: &Value,
    query: &str,
) -> Result<Value, String> {
    invoke(SearchStep::new(query)?, credential, generation, model).await
}

#[cfg(test)]
#[path = "capability_step_tests.rs"]
pub(crate) mod tests;
