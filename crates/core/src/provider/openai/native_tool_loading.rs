//! Projection and bounded negotiation of the client-executed search protocol.
use super::*;

pub(super) fn apply_policy(body: &mut Value, request: &ModelRequest, native: bool) {
    fn reconcile(tools: &mut Vec<Value>, active: &[ToolSpec], loaded: &mut HashSet<RuntimeString>) {
        for tool in tools {
            // A namespace child and a flat declaration with the same short
            // name are different tools. Flat ToolSpec has no namespace field.
            if tool.get("tools").and_then(Value::as_array).is_some() {
                continue;
            }
            let Some(name) = tool.get("name").and_then(Value::as_str).cloned() else {
                continue;
            };
            if tool
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "function")
            {
                if let Some(current) = active
                    .iter()
                    .find(|spec| spec.name == name && spec.kind == ToolSpecKind::Function)
                {
                    *tool = to_responses_function_tool(current);
                    tool["defer_loading"] = Value::Bool(true);
                    loaded.insert(name);
                }
            }
        }
    }
    let mut loaded = HashSet::new();
    if let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) {
        for item in input {
            let kind = item.get("type").and_then(Value::as_str).cloned();
            if native {
                if kind
                    .as_ref()
                    .is_some_and(|kind| kind == "tool_search_output")
                {
                    if let Some(tools) = item.get_mut("tools").and_then(Value::as_array_mut) {
                        reconcile(tools, &request.tools, &mut loaded);
                    }
                }
            } else if kind.as_ref().is_some_and(|kind| kind == "tool_search_call") {
                *item = vobj([
                    ("type", vs("function_call")),
                    ("name", vs("tool_search")),
                    ("call_id", item["call_id"].clone()),
                    ("arguments", vs(runtime_json::stringify(&item["arguments"]))),
                ]);
            } else if kind
                .as_ref()
                .is_some_and(|kind| kind == "tool_search_output")
            {
                let call_id = item["call_id"].as_str();
                let text = request
                    .messages
                    .iter()
                    .find_map(|message| {
                        let Message::ToolResult(result) = message else {
                            return None;
                        };
                        (Some(&result.tool_use_id) == call_id)
                            .then(|| text_of_output_blocks(&result.content))
                    })
                    .unwrap_or_else(|| {
                        "Tool search completed; active definitions are declared in tools.".into()
                    });
                *item = vobj([
                    ("type", vs("function_call_output")),
                    ("call_id", item["call_id"].clone()),
                    ("output", vs(text)),
                ]);
            }
        }
    }
    if !native && body.get("tools").is_none() && !request.tools.is_empty() {
        body["tools"] = Value::Array(Vec::new());
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        if native {
            tools.retain(|tool| {
                !tool
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| loaded.contains(name))
            });
        } else {
            for tool in tools.iter_mut() {
                if tool
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "tool_search")
                {
                    *tool = vobj([
                        ("type", vs("function")),
                        ("name", vs("tool_search")),
                        ("description", tool["description"].clone()),
                        ("parameters", tool["parameters"].clone()),
                        ("strict", Value::Bool(false)),
                    ]);
                }
            }
            // Native replay may have removed these eager declarations. Restoring
            // only the already active request inventory preserves surface state.
            for spec in &request.tools {
                if !tools
                    .iter()
                    .any(|tool| tool.get("name").and_then(Value::as_str) == Some(&spec.name))
                {
                    tools.push(if spec.kind == ToolSpecKind::ToolSearch {
                        to_responses_function_tool(spec)
                    } else {
                        to_responses_tool(spec)
                    });
                }
            }
        }
    }
}

fn contains_native_search(body: &Value) -> bool {
    body.get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "tool_search")
            })
        })
        || body
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|input| {
                input.iter().any(|item| {
                    item.get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| {
                            kind == "tool_search_call" || kind == "tool_search_output"
                        })
                })
            })
}

pub(super) struct Transport {
    pub client: reqwest::Client,
    pub url: String,
    pub auth: Option<Arc<dyn AuthSource>>,
    pub api_key: Option<String>,
    pub extra_headers: HashMap<String, String>,
    pub retry: RetryOptions,
    pub signal: AbortSignal,
    pub interception: (Option<SharedRequestInterceptor>, String, String),
    pub role_state: (
        InstructionRoleCache,
        RuntimeString,
        Option<OpenAIInstructionRole>,
    ),
}

pub(super) async fn establish(
    transport: Transport,
    mut body: Value,
    request: &ModelRequest,
    learned: crate::provider::native_tools::NativeToolSupport,
) -> Result<ChatResponseBody, ProviderError> {
    if learned.disabled(&transport.url, &request.model) {
        apply_policy(&mut body, request, false);
    }
    let native = contains_native_search(&body);
    let result = establish_stream_with_instruction_role(
        transport.client.clone(),
        transport.url.clone(),
        body.clone(),
        transport.auth.clone(),
        transport.api_key.clone(),
        transport.extra_headers.clone(),
        transport.retry.clone(),
        transport.signal.clone(),
        transport.interception.clone(),
        transport.role_state.clone(),
    )
    .await;
    let error = match result {
        Ok(response) => return Ok(response),
        Err(error) => error,
    };
    if !native
        || transport.signal.is_cancelled()
        || !crate::provider::native_tools::unsupported_native_tools(
            error.status,
            &diagnostic_string(&error.message),
        )
    {
        return Err(error);
    }
    learned.disable(&transport.url, &request.model);
    apply_policy(&mut body, request, false);
    // This second attempt returns directly. Its refusal cannot recurse into
    // negotiation, and ordinary transport retries remain owned by the caller.
    establish_stream_with_instruction_role(
        transport.client,
        transport.url,
        body,
        transport.auth,
        transport.api_key,
        transport.extra_headers,
        transport.retry,
        transport.signal,
        transport.interception,
        transport.role_state,
    )
    .await
}
