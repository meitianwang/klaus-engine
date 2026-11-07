//! Native image bytes are captured from one exact held write. References are
//! served from caller-owned memory; this operation never accesses disk.
use super::{id, invalid, keys, text, NativeOperation};
use crate::app_server::cursor::{
    credential,
    step_history::{proto_bytes, Blobs, History},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

struct ImageStep {
    history: History,
    context: Value,
    native_id: String,
    prompt: String,
    filename: String,
    folder: std::path::PathBuf,
    ratio: Option<String>,
    references: BTreeMap<String, Vec<u8>>,
    reference_order: Vec<String>,
    reads: HashSet<String>,
    output_probes: usize,
    contexts: usize,
    events: usize,
    partials: usize,
    invocation: Option<String>,
    started: bool,
    approved: bool,
    result: Option<Value>,
}

impl ImageStep {
    fn new(prompt: &str, ratio: Option<&str>, references: &[Value]) -> Result<Self, String> {
        // This operation already requests generation. State that action
        // explicitly while preserving the user's description verbatim,
        // including languages without whitespace word boundaries.
        let description=format!("Create an image from the following user description. Treat the description as the requested visual content and preserve its stated subject, style, colors, and composition:\n\n{prompt}");
        let native_id = uuid::Uuid::new_v4().to_string();
        let folder =
            std::env::temp_dir().join(format!("klaus-capability-{}", uuid::Uuid::new_v4()));
        let filename = format!("image-{}.png", uuid::Uuid::new_v4());
        let mut stored = BTreeMap::new();
        let mut order = Vec::new();
        for (index, reference) in references.iter().enumerate() {
            let extension = match reference["mimeType"].as_str() {
                Some("image/png") => "png",
                Some("image/jpeg") => "jpg",
                Some("image/gif") => "gif",
                Some("image/webp") => "webp",
                _ => return Err("cursor_reference_images_invalid".into()),
            };
            let path = folder
                .join("references")
                .join(format!("reference-{index}.{extension}"))
                .to_string_lossy()
                .into_owned();
            let bytes = STANDARD
                .decode(reference["data"].as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 || stored.len() >= 4 {
                return Err("cursor_reference_images_invalid".into());
            }
            order.push(path.clone());
            stored.insert(path, bytes);
        }
        let mut args = json!({"description":description,"filename":filename});
        if let Some(ratio) = ratio {
            args["aspect_ratio"] = json!(ratio);
        }
        if !order.is_empty() {
            args["reference_image_paths"] = json!(order);
        }
        let pending = json!({"role":"assistant","content":[{"type":"tool-call","toolCallId":native_id,"toolName":"CallDynamicTool","args":{"namespace":"cursor","toolName":"GenerateImage","arguments":args}}],"providerOptions":{"cursor":{"pendingToolExecutionContracts":{
            (native_id.clone()):{"toolCallId":native_id,"outerToolName":"CallDynamicTool","toolIdentifier":"GENERATE_IMAGE","isDynamic":true,"effectiveToolName":"GenerateImage","effectiveToolIdentifier":"GENERATE_IMAGE","allowedToolNames":["CallDynamicTool","GetDynamicTools"]}
        }}}});
        let mut blobs = Blobs::default();
        let roots=[json!({"role":"system","content":"Execute only the explicitly pending image generation. Do not run a model continuation or any other tool."}),json!({"role":"user","content":prompt}),pending.clone()].iter().map(|message|blobs.seed(serde_json::to_vec(message).map_err(|_|invalid())?)).collect::<Result<Vec<_>,_>>()?;
        let mut user = proto_bytes(1, prompt.as_bytes());
        user.extend(proto_bytes(2, uuid::Uuid::new_v4().to_string().as_bytes()));
        user.extend([32, 1]);
        let user_id = blobs.seed(user)?;
        let turn = proto_bytes(
            1,
            &proto_bytes(1, &STANDARD.decode(user_id).map_err(|_| invalid())?),
        );
        let turn_id = blobs.seed(turn)?;
        let history = History {
            blobs,
            state: json!({"mode":1,"rootPromptMessagesJson":roots,"turns":[turn_id],"pendingToolCalls":[pending.to_string()]}),
            system: String::new(),
        };
        let context = json!({"env":{"osVersion":std::env::consts::OS,"workspacePaths":[folder],"projectFolder":folder,"artifactsFolder":folder},"tools":[],"webSearchEnabled":false,"webFetchEnabled":false,"readLintsEnabled":false,"searchConversationsEnabled":false,"sendMessageEnabled":false,"mcpMetaToolOptions":{"enabled":true,"mcpDescriptors":[]},"rulesInfoComplete":true,"envInfoComplete":true,"repositoryInfoComplete":true,"customSubagentsInfoComplete":true,"agentSkillsInfoComplete":true,"mcpInfoComplete":true,"gitRepoInfoComplete":true,"mcpFileSystemInfoComplete":true,"gitStatusInfoComplete":true});
        Ok(Self {
            history,
            context,
            native_id,
            prompt: description,
            filename,
            folder,
            ratio: ratio.map(str::to_owned),
            references: stored,
            reference_order: order,
            reads: HashSet::new(),
            output_probes: 0,
            contexts: 0,
            events: 0,
            partials: 0,
            invocation: None,
            started: false,
            approved: false,
            result: None,
        })
    }

    fn args(&self, args: &Value) -> Result<(), String> {
        keys(
            args,
            &[
                "description",
                "filePath",
                "referenceImagePaths",
                "aspectRatio",
            ],
        )?;
        if args["description"] != self.prompt
            || args["filePath"] != self.filename
            || text(args, "aspectRatio", 64)? != self.ratio.as_deref().unwrap_or("")
        {
            return Err(invalid());
        }
        let empty = Vec::new();
        let references = match args.get("referenceImagePaths") {
            None => &empty,
            Some(value) => value.as_array().ok_or_else(invalid)?,
        };
        if *references
            != self
                .reference_order
                .iter()
                .map(|path| json!(path))
                .collect::<Vec<_>>()
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn tool(&self, call: &Value, partial: bool, completed: bool) -> Result<(), String> {
        keys(
            call,
            &[
                "generateImageToolCall",
                "toolCallId",
                "hookAdditionalContexts",
                "startedAtMs",
                "completedAtMs",
            ],
        )?;
        if call["toolCallId"] != self.native_id
            || call
                .get("hookAdditionalContexts")
                .is_some_and(|v| v != &json!([]))
        {
            return Err(invalid());
        }
        let inner = call.get("generateImageToolCall").ok_or_else(invalid)?;
        keys(
            inner,
            if completed {
                &["args", "result"]
            } else {
                &["args"]
            },
        )?;
        if partial && inner.get("args").is_none_or(|value| value == &json!({})) {
            return Ok(());
        }
        self.args(&inner["args"])
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

    fn output_path(&self, path: &str) -> bool {
        let p = std::path::Path::new(path);
        let stem = std::path::Path::new(&self.filename).file_stem();
        p.parent() == Some(self.folder.join("assets").as_path())
            && p.file_stem() == stem
            && matches!(
                p.extension().and_then(|s| s.to_str()),
                Some("png" | "jpg" | "jpeg" | "gif" | "webp")
            )
    }
}

impl NativeOperation for ImageStep {
    fn request(&self, model: &Value) -> Result<Value, String> {
        let legacy = model["legacySlug"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("cursor_model_variant_unavailable")?;
        if !model["requestedModel"].is_object() {
            return Err("cursor_model_variant_unavailable".into());
        }
        Ok(
            json!({"runRequest":{"conversationId":uuid::Uuid::new_v4().to_string(),"conversationState":self.history.state,"requestedModel":model["requestedModel"],"modelDetails":{"modelId":legacy,"maxMode":model["requestedModel"]["maxMode"]},"mcpTools":{"mcpTools":[]},"canCreateCloudSubagents":false,"clientSupportsSendToUser":false,"suggestNextPrompt":false,"action":{"resumeAction":{"requestContext":self.context}}}}),
        )
    }

    fn receive(&mut self, packet: &Value) -> Result<Vec<Value>, String> {
        self.events += 1;
        if self.events > 512 {
            return Err("cursor_response_too_large".into());
        }
        let action = super::action(
            packet,
            &[
                "kvServerMessage",
                "execServerMessage",
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
                    self.history.blobs.insert(
                        text(set, "blobId", 128)?.into(),
                        STANDARD
                            .decode(text(set, "blobData", 6 * 1024 * 1024)?)
                            .map_err(|_| invalid())?,
                    )?;
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
                        "readArgs",
                        "writeArgs",
                    ],
                )?;
                if exec
                    .get("acceptHookAdditionalContexts")
                    .is_some_and(|v| !v.is_boolean())
                {
                    return Err(invalid());
                }
                let cases = ["requestContextArgs", "readArgs", "writeArgs"]
                    .iter()
                    .filter(|key| exec.get(**key).is_some())
                    .collect::<Vec<_>>();
                if cases.len() != 1 {
                    return Err(invalid());
                }
                let id = id(exec)?;
                let exec_id = text(exec, "execId", 2048)?;
                match **cases.first().ok_or_else(invalid)? {
                    "requestContextArgs" => {
                        keys(&exec["requestContextArgs"], &[])?;
                        self.contexts += 1;
                        if self.contexts > 4 {
                            return Err(invalid());
                        }
                        replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"requestContextResult":{"success":{"requestContext":self.context}}}}));
                    }
                    "readArgs" => {
                        if !self.started {
                            return Err(invalid());
                        }
                        let args = &exec["readArgs"];
                        keys(args, &["path", "toolCallId"])?;
                        if args["toolCallId"] != self.native_id {
                            return Err(invalid());
                        }
                        let path = text(args, "path", 8192)?;
                        if let Some(bytes) = self.references.get(path) {
                            if !self.reads.insert(path.into()) {
                                return Err(invalid());
                            }
                            replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"readResult":{"success":{"path":path,"data":STANDARD.encode(bytes),"fileSize":bytes.len().to_string()}}}}));
                        } else if self.output_path(path) && self.output_probes == 0 {
                            self.output_probes += 1;
                            replies.push(json!({"execClientMessage":{"id":id,"execId":exec_id,"readResult":{"fileNotFound":{"path":path}}}}));
                        } else {
                            return Err(invalid());
                        }
                    }
                    "writeArgs" => {
                        if !self.started
                            || self.result.is_some()
                            || self.reads.len() != self.references.len()
                        {
                            return Err(invalid());
                        }
                        let args = &exec["writeArgs"];
                        keys(
                            args,
                            &[
                                "path",
                                "fileText",
                                "toolCallId",
                                "returnFileContentAfterWrite",
                                "fileBytes",
                                "encodingHint",
                            ],
                        )?;
                        let path = text(args, "path", 8192)?;
                        if !self.output_path(path)
                            || !text(args, "fileText", 1)?.is_empty()
                            || !text(args, "encodingHint", 1)?.is_empty()
                            || args
                                .get("returnFileContentAfterWrite")
                                .is_some_and(|v| v != false)
                            || args
                                .get("toolCallId")
                                .is_some_and(|v| v != "" && v != &self.native_id)
                        {
                            return Err(invalid());
                        }
                        let bytes = super::super::capabilities::decode_base64(
                            text(args, "fileBytes", 24 * 1024 * 1024)?,
                            16 * 1024 * 1024,
                        )?;
                        let extension = std::path::Path::new(path)
                            .extension()
                            .and_then(|s| s.to_str())
                            .ok_or_else(invalid)?;
                        let mime = match extension {
                            "png" => "image/png",
                            "jpg" | "jpeg" => "image/jpeg",
                            "gif" => "image/gif",
                            "webp" => "image/webp",
                            _ => return Err(invalid()),
                        };
                        if !super::super::capabilities::valid_image(&bytes, mime) {
                            return Err("cursor_native_media_invalid".into());
                        }
                        self.result = Some(
                            json!({"imageBase64":STANDARD.encode(bytes),"mimeType":mime,"aspectRatio":self.ratio,"pixelSize":"provider-controlled"}),
                        );
                        // No write result is supplied. Its pending await blocks
                        // the server until the caller closes this request.
                    }
                    _ => return Err(invalid()),
                }
            }
            "interactionQuery" => {
                let query = &packet["interactionQuery"];
                keys(query, &["id", "generateImageRequestQuery"])?;
                let id = id(query)?;
                let image = &query["generateImageRequestQuery"];
                keys(image, &["args", "toolCallId"])?;
                if self.approved || image["toolCallId"] != self.native_id {
                    return Err(invalid());
                }
                self.args(&image["args"])?;
                self.approved = true;
                replies.push(json!({"interactionResponse":{"id":id,"generateImageRequestResponse":{"approved":{}}}}));
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
                        let event = &update["partialToolCall"];
                        keys(
                            event,
                            &["callId", "toolCall", "argsTextDelta", "modelCallId"],
                        )?;
                        self.partials += 1;
                        if self.partials > 4
                            || event["callId"] != self.native_id
                            || !text(event, "argsTextDelta", 1)?.is_empty()
                        {
                            return Err(invalid());
                        }
                        self.invocation(event)?;
                        self.tool(&event["toolCall"], true, false)?;
                    }
                    "toolCallStarted" => {
                        let event = &update["toolCallStarted"];
                        keys(event, &["callId", "toolCall", "modelCallId"])?;
                        if self.started || event["callId"] != self.native_id {
                            return Err(invalid());
                        }
                        self.invocation(event)?;
                        self.tool(&event["toolCall"], false, false)?;
                        self.started = true;
                    }
                    "toolCallCompleted" => {
                        let event = &update["toolCallCompleted"];
                        keys(event, &["callId", "toolCall", "modelCallId"])?;
                        if event["callId"] != self.native_id {
                            return Err(invalid());
                        }
                        self.invocation(event)?;
                        self.tool(&event["toolCall"], false, true)?;
                        return Err("cursor_native_service_failed".into());
                    }
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
        Ok(replies)
    }
    fn take_result(&mut self) -> Option<Value> {
        self.result.take()
    }
    fn limits(&self) -> (usize, usize) {
        (32 * 1024 * 1024, 24 * 1024 * 1024)
    }
}

pub(in crate::app_server::cursor) async fn generate(
    credential: &credential::Credential,
    generation: u64,
    model: &Value,
    prompt: &str,
    ratio: Option<&str>,
    references: &[Value],
) -> Result<Value, String> {
    super::invoke(
        ImageStep::new(prompt, ratio, references)?,
        credential,
        generation,
        model,
    )
    .await
}

#[cfg(test)]
#[path = "capability_image_tests.rs"]
mod tests;
