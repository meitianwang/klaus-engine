//! Projection and bounded negotiation of the client-executed search protocol.
use super::*;

pub(super) fn apply_policy(body: &mut Value, request: &ModelRequest, native: bool) {
    fn reconcile(tools: &mut Vec<Value>, active: &[ToolSpec], loaded: &mut HashSet<RuntimeString>) {
        for tool in tools {
            // A namespace entry refreshes its children by their internal names;
            // the short wire names stay as declared.
            if tool.get("tools").and_then(Value::as_array).is_some() {
                let namespace = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(|name| name.try_to_utf8().ok());
                let Some(children) = tool.get_mut("tools").and_then(Value::as_array_mut) else {
                    continue;
                };
                for child in children.iter_mut() {
                    if !child
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| kind == "function" || kind == "custom")
                    {
                        continue;
                    }
                    let Some(child_name) = child.get("name").and_then(Value::as_str).cloned()
                    else {
                        continue;
                    };
                    let internal =
                        internal_tool_name(namespace.as_deref(), &diagnostic_string(&child_name));
                    if let Some(current) = active.iter().find(|spec| {
                        spec.name == internal
                            && matches!(spec.kind, ToolSpecKind::Function | ToolSpecKind::Freeform)
                    }) {
                        let mut refreshed = to_responses_tool(current);
                        refreshed["name"] = vs(child_name.clone());
                        refreshed["defer_loading"] = Value::Bool(true);
                        *child = refreshed;
                        loaded.insert(internal);
                    }
                }
                continue;
            }
            let Some(name) = tool.get("name").and_then(Value::as_str).cloned() else {
                continue;
            };
            if tool
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "function" || kind == "custom")
            {
                if let Some(current) = active.iter().find(|spec| {
                    spec.name == name
                        && matches!(spec.kind, ToolSpecKind::Function | ToolSpecKind::Freeform)
                }) {
                    *tool = to_responses_tool(current);
                    tool["defer_loading"] = Value::Bool(true);
                    loaded.insert(name);
                }
            }
        }
    }
    let mut loaded = HashSet::new();
    let batched = contains_tool_batches(body);
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
    if !batched && body.get("tools").is_none() && !request.tools.is_empty() {
        body["tools"] = Value::Array(Vec::new());
    }
    if batched {
        // Cumulative-batch mode owns every tool declaration inside `input`; the
        // top-level list must stay absent, so neither the eager-restore nor the
        // namespace reconcile below may run.
        return;
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        if native {
            tools.retain_mut(|tool| {
                if tool.get("tools").and_then(Value::as_array).is_some() {
                    let namespace = tool
                        .get("name")
                        .and_then(Value::as_str)
                        .and_then(|name| name.try_to_utf8().ok());
                    let Some(children) = tool.get_mut("tools").and_then(Value::as_array_mut) else {
                        return true;
                    };
                    children.retain_mut(|child| {
                        let Some(child_name) = child.get("name").and_then(Value::as_str).cloned()
                        else {
                            return true;
                        };
                        let internal = internal_tool_name(
                            namespace.as_deref(),
                            &diagnostic_string(&child_name),
                        );
                        !loaded.contains(&internal)
                    });
                    return !children.is_empty();
                }
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
                let declared = tools.iter().any(|tool| {
                    if let Some(children) = tool.get("tools").and_then(Value::as_array) {
                        let namespace = tool
                            .get("name")
                            .and_then(Value::as_str)
                            .and_then(|name| name.try_to_utf8().ok());
                        children.iter().any(|child| {
                            child
                                .get("name")
                                .and_then(Value::as_str)
                                .is_some_and(|child_name| {
                                    internal_tool_name(
                                        namespace.as_deref(),
                                        &diagnostic_string(child_name),
                                    ) == spec.name
                                })
                        })
                    } else {
                        tool.get("name").and_then(Value::as_str) == Some(&spec.name)
                    }
                });
                if !declared {
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

/// Whether the request declares tools through cumulative input batches.
pub(super) fn contains_tool_batches(body: &Value) -> bool {
    body.get("input")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "additional_tools")
            })
        })
}

/// Whether any declaration or call/choice in the body uses grouped namespace entries.
pub(super) fn contains_tool_namespaces(body: &Value) -> bool {
    fn list_has_namespace(tools: &Value) -> bool {
        tools.as_array().is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "namespace")
            })
        })
    }
    body.get("tools").is_some_and(list_has_namespace)
        || body
            .get("tool_choice")
            .and_then(|choice| choice.get("namespace"))
            .is_some()
        || body
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    let kind = item.get("type").and_then(Value::as_str);
                    (kind.is_some_and(|k| k == "additional_tools" || k == "tool_search_output")
                        && item.get("tools").is_some_and(list_has_namespace))
                        || (kind.is_some_and(|k| k == "function_call" || k == "custom_tool_call")
                            && item.get("namespace").is_some())
                })
            })
}

/// Collapse every namespace entry back into flat declarations (children are
/// renamed to their internal `{namespace}__{name}` form) and strip namespace
/// fields from replayed call items and `tool_choice`, so a refusal of the
/// grouped shape can be retried with the plain flat inventory.
pub(super) fn flatten_tool_namespaces(body: &mut Value) {
    fn flatten_list(tools: &mut Vec<Value>) {
        let mut out: Vec<Value> = Vec::with_capacity(tools.len());
        for tool in tools.drain(..) {
            if tool
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "namespace")
            {
                let namespace = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(|name| name.try_to_utf8().ok());
                if let Some(children) = tool.get("tools").and_then(Value::as_array) {
                    for child in children {
                        let mut flat = child.clone();
                        if let Some(child_name) = child.get("name").and_then(Value::as_str) {
                            let internal = internal_tool_name(
                                namespace.as_deref(),
                                &diagnostic_string(child_name),
                            );
                            flat["name"] = vs(internal);
                        }
                        out.push(flat);
                    }
                    continue;
                }
            }
            out.push(tool);
        }
        *tools = out;
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        flatten_list(tools);
    }
    if let Some(choice) = body.get_mut("tool_choice").and_then(Value::as_object_mut) {
        if let Some(namespace) = choice
            .remove("namespace")
            .and_then(|value| value.as_str().and_then(|name| name.try_to_utf8().ok()))
        {
            if let Some(name) = choice
                .get("name")
                .and_then(Value::as_str)
                .map(diagnostic_string)
            {
                choice.insert("name", vs(internal_tool_name(Some(&namespace), &name)));
            }
        }
    }
    if let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) {
        for item in items.iter_mut() {
            let kind = item
                .get("type")
                .and_then(Value::as_str)
                .and_then(|kind| kind.try_to_utf8().ok());
            match kind.as_deref() {
                Some("additional_tools") | Some("tool_search_output") => {
                    if let Some(tools) = item.get_mut("tools").and_then(Value::as_array_mut) {
                        flatten_list(tools);
                    }
                }
                Some("function_call") | Some("custom_tool_call") => {
                    if let Some(namespace) = item
                        .get("namespace")
                        .and_then(Value::as_str)
                        .and_then(|name| name.try_to_utf8().ok())
                    {
                        if let Some(name) = item.get("name").and_then(Value::as_str) {
                            let internal =
                                internal_tool_name(Some(&namespace), &diagnostic_string(name));
                            item["name"] = vs(internal);
                        }
                        if let Some(object) = item.as_object_mut() {
                            object.remove("namespace");
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// The plain text of a message item's content, whether it is a bare string or
/// a list of typed parts.
fn message_item_text(item: &Value) -> Option<String> {
    match item.get("content")? {
        Value::String(text) => Some(diagnostic_string(text)),
        content => content.as_array().map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .map(diagnostic_string)
                .collect::<Vec<_>>()
                .join("")
        }),
    }
}

/// Strip the cumulative-batch artifacts (batch items plus their notice and
/// withdrawal messages) so the caller can rebuild the plain top-level
/// inventory shape. The current declarations themselves come from the request,
/// which the caller's policy pass restores.
pub(super) fn rebuild_without_tool_batches(body: &mut Value) {
    if let Some(items) = body.get_mut("input").and_then(Value::as_array_mut) {
        items.retain(|item| {
            if item
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "additional_tools")
            {
                return false;
            }
            if let Some(text) = message_item_text(item) {
                if text.starts_with(TOOLS_UPDATE_NOTICE)
                    || text.starts_with(TOOLS_REMOVAL_NOTICE)
                    || text.starts_with(TOOLS_REMOVED_NAMESPACES_NOTICE)
                {
                    return false;
                }
            }
            true
        });
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
    let batched = contains_tool_batches(&body);
    let namespaced = contains_tool_namespaces(&body);
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
    if !(native || batched || namespaced)
        || transport.signal.is_cancelled()
        || !crate::provider::native_tools::unsupported_native_tools(
            error.status,
            &diagnostic_string(&error.message),
        )
    {
        return Err(error);
    }
    let lower = diagnostic_string(&error.message).to_ascii_lowercase();
    let mentions_namespace = lower.contains("namespace");
    let mentions_batches = lower.contains("additional_tools") || lower.contains("additional tools");
    let mentions_search = ["tool_search", "defer_loading", "execution"]
        .iter()
        .any(|needle| lower.contains(needle));
    if namespaced && mentions_namespace && !mentions_batches && !mentions_search {
        learned.disable_for(&transport.url, &request.model, TOOL_NAMESPACES_CAPABILITY);
        crate::observability::prefix_attribution::record_downgrade_rebuild(
            "tool-namespaces",
            &diagnostic_string(&request.model),
            error.status,
        );
        flatten_tool_namespaces(&mut body);
    } else if batched && mentions_batches && !mentions_namespace && !mentions_search {
        learned.disable_for(&transport.url, &request.model, ADDITIONAL_TOOLS_CAPABILITY);
        crate::observability::prefix_attribution::record_downgrade_rebuild(
            "additional-tools",
            &diagnostic_string(&request.model),
            error.status,
        );
        rebuild_without_tool_batches(&mut body);
        let grouping = if namespaced {
            ResponsesToolGrouping::TopLevel
        } else {
            ResponsesToolGrouping::Flat
        };
        let mut specs = request.tools.clone();
        if !native {
            for spec in &mut specs {
                if spec.kind == ToolSpecKind::ToolSearch {
                    spec.kind = ToolSpecKind::Function;
                }
            }
        }
        body["tools"] = Value::Array(grouped_responses_tools(&specs, grouping));
        apply_policy(&mut body, request, native);
    } else {
        if batched {
            learned.disable_for(&transport.url, &request.model, ADDITIONAL_TOOLS_CAPABILITY);
            crate::observability::prefix_attribution::record_downgrade_rebuild(
                "additional-tools",
                &diagnostic_string(&request.model),
                error.status,
            );
            rebuild_without_tool_batches(&mut body);
        } else if native {
            learned.disable(&transport.url, &request.model);
            crate::observability::prefix_attribution::record_downgrade_rebuild(
                "tool-search",
                &diagnostic_string(&request.model),
                error.status,
            );
        }
        if namespaced {
            learned.disable_for(&transport.url, &request.model, TOOL_NAMESPACES_CAPABILITY);
            crate::observability::prefix_attribution::record_downgrade_rebuild(
                "tool-namespaces",
                &diagnostic_string(&request.model),
                error.status,
            );
            flatten_tool_namespaces(&mut body);
        }
        apply_policy(&mut body, request, false);
    }
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
