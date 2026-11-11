//! The provider's stateless Responses format retains opaque replay context in
//! engine history, including signed reasoning and native assistant content.
use super::{bridge, credential};
use bytes::Bytes;
use serde_json::{json, Value};
use tokio::sync::mpsc;
const CONTEXT_KIND: &str = "klaus.provider-context.v1";
const MAX_CONTEXT: usize = 8 * 1024 * 1024;

fn flush_assistant(
    messages: &mut Vec<Value>,
    pending: &mut Option<Value>,
    native: &mut Option<Value>,
) -> Result<(), String> {
    let Some(mut assistant) = pending.take() else {
        if native.is_some() {
            return Err("cursor_orphaned_reasoning_context".into());
        }
        return Ok(());
    };
    if let Some(context) = native.take() {
        assistant["nativeContext"] = context;
    }
    messages.push(assistant);
    Ok(())
}

pub(super) fn normalize(body: &Value) -> Result<Value, String> {
    if body.get("text").and_then(|v| v.get("format")).is_some() {
        return Err("cursor_structured_output_unsupported".into());
    }
    super::step_history::ToolChoice::parse(body.get("tool_choice"))?;
    let mut messages = Vec::new();
    let mut pending = None;
    let mut native = None;
    if let Some(instructions) = body.get("instructions").and_then(Value::as_str) {
        messages.push(json!({"role":"system","content":instructions}));
    }
    for item in body["input"].as_array().ok_or("cursor_input_invalid")? {
        match item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message")
        {
            "reasoning" => {
                flush_assistant(&mut messages, &mut pending, &mut native)?;
                let encoded = item["encrypted_content"]
                    .as_str()
                    .ok_or("cursor_reasoning_context_missing")?;
                if encoded.len() > MAX_CONTEXT {
                    return Err("cursor_reasoning_context_too_large".into());
                }
                // Opaque context is replayable only by its original source and
                // exact model. Visible messages and tools remain portable.
                if let Ok(context) = serde_json::from_str::<Value>(encoded) {
                    if context["kind"] == CONTEXT_KIND
                        && context["scope"] == "cursor"
                        && context["model"] == body["model"]
                    {
                        native = Some(context["message"].clone());
                    }
                }
            }
            "function_call" => {
                let assistant = pending.get_or_insert_with(
                    || json!({"role":"assistant","content":"","tool_calls":[]}),
                );
                assistant["tool_calls"].as_array_mut().expect("calls").push(json!({"id":item["call_id"],"type":"function","function":{"name":item["name"],"arguments":item["arguments"]}}));
            }
            "function_call_output" => {
                flush_assistant(&mut messages, &mut pending, &mut native)?;
                messages.push(
                    json!({"role":"tool","tool_call_id":item["call_id"],"content":item["output"]}),
                );
            }
            "message" => {
                let role = item["role"]
                    .as_str()
                    .ok_or("cursor_message_role_unsupported")?;
                let content = if item["content"].is_array() {
                    let mut parts = Vec::new();
                    for part in item["content"].as_array().expect("array") {
                        match part["type"].as_str() {
                            Some("input_text" | "output_text" | "text") => {
                                parts.push(json!({"type":"text","text":part["text"]}))
                            }
                            Some("input_image") => parts.push(
                                json!({"type":"image_url","image_url":{"url":part["image_url"]}}),
                            ),
                            _ => parts.push(json!({"type":"text","text":"[content omitted: not replayable on this model]"})),
                        }
                    }
                    if parts.iter().all(|p| p["type"] == "text") {
                        json!(parts
                            .iter()
                            .filter_map(|p| p["text"].as_str())
                            .collect::<String>())
                    } else {
                        json!(parts)
                    }
                } else {
                    item["content"].clone()
                };
                if role == "assistant" {
                    let assistant = pending.get_or_insert_with(
                        || json!({"role":"assistant","content":"","tool_calls":[]}),
                    );
                    let text = content.as_str().ok_or("cursor_assistant_content_invalid")?;
                    let current = assistant["content"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    assistant["content"] = json!(format!("{current}{text}"));
                } else {
                    flush_assistant(&mut messages, &mut pending, &mut native)?;
                    messages.push(json!({"role":role,"content":content}));
                }
            }
            "compaction" => {
                flush_assistant(&mut messages, &mut pending, &mut native)?;
            }
            _ => return Err("cursor_input_item_unsupported".into()),
        }
    }
    flush_assistant(&mut messages, &mut pending, &mut native)?;
    let tools: Vec<Value> = body.get("tools").and_then(Value::as_array).into_iter().flatten().map(|tool| {
        if tool["type"] != "function" { return Err("cursor_tool_type_unsupported"); }
        Ok(json!({"type":"function","function":{"name":tool["name"],"description":tool.get("description").cloned().unwrap_or(json!("")),"parameters":tool["parameters"]}}))
    }).collect::<Result<_,_>>()?;
    let mut chat = json!({"model":body["model"],"messages":messages,"tools":tools});
    if let Some(choice) = body.get("tool_choice") {
        chat["tool_choice"] = choice.clone();
    }
    for (source, target) in [
        ("max_output_tokens", "max_tokens"),
        ("temperature", "temperature"),
        ("top_p", "top_p"),
        ("stop", "stop"),
    ] {
        if let Some(value) = body.get(source) {
            chat[target] = value.clone();
        }
    }
    if let Some(effort) = body.get("reasoning").and_then(|r| r.get("effort")) {
        chat["reasoning_effort"] = effort.clone();
    }
    Ok(chat)
}

pub(super) struct Output {
    id: String,
    model: String,
    text: String,
    reasoning: String,
}
impl Output {
    pub fn new(model: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            model: model.into(),
            text: String::new(),
            reasoning: String::new(),
        }
    }
    pub async fn start(&self, tx: &mpsc::Sender<Bytes>) -> Result<(), String> {
        bridge::emit(
            tx,
            json!({"type":"response.created","response":{"id":self.id,"model":self.model}}),
        )
        .await?;
        bridge::emit(tx,json!({"type":"response.reasoning_summary_text.delta","item_id":format!("{}-reasoning",self.id),"delta":""})).await
    }
    pub async fn delta(&mut self, update: &Value, tx: &mpsc::Sender<Bytes>) -> Result<(), String> {
        for (field, reasoning) in [("textDelta", false), ("thinkingDelta", true)] {
            if let Some(value) = update.get(field) {
                let text = super::step::string_or_default(value, "text")?;
                if text.is_empty() {
                    continue;
                }
                let target = if reasoning {
                    &mut self.reasoning
                } else {
                    &mut self.text
                };
                if target.len() + text.len() > MAX_CONTEXT {
                    return Err("cursor_response_too_large".into());
                }
                target.push_str(text);
                bridge::emit(tx,json!({"type":if reasoning {"response.reasoning_summary_text.delta"} else {"response.output_text.delta"},"item_id":format!("{}-{}",self.id,if reasoning {"reasoning"}else{"text"}),"delta":text})).await?;
            }
        }
        Ok(())
    }
    pub async fn finish(
        self,
        result: super::step::Certified,
        tx: &mpsc::Sender<Bytes>,
    ) -> Result<(), String> {
        if self.text != result.text {
            eprintln!(
                "[Cursor] native response mismatch site=output streamedTextBytes={} certifiedTextBytes={}",
                self.text.len(), result.text.len()
            );
            return Err("cursor_native_response_mismatch".into());
        }
        if self.reasoning.is_empty() && !result.reasoning.is_empty() {
            bridge::emit(tx,json!({"type":"response.reasoning_summary_text.delta","item_id":format!("{}-reasoning",self.id),"delta":result.reasoning})).await?;
        }
        let reasoning = if self.reasoning.is_empty() {
            &result.reasoning
        } else {
            &self.reasoning
        };
        let envelope=json!({"kind":CONTEXT_KIND,"scope":"cursor","model":self.model,"message":result.context}).to_string();
        if envelope.len() > MAX_CONTEXT {
            return Err("cursor_reasoning_context_too_large".into());
        }
        bridge::emit(tx,json!({"type":"response.output_item.done","item":{"type":"reasoning","id":format!("{}-reasoning",self.id),"summary":[{"type":"summary_text","text":reasoning}],"encrypted_content":envelope}})).await?;
        let mut output = Vec::new();
        for call in result.calls {
            let item = json!({"type":"function_call","id":call["toolCallId"],"call_id":call["toolCallId"],"name":call["toolName"],"arguments":call["args"].to_string()});
            bridge::emit(tx,json!({"type":"response.output_item.added","item":{"type":"function_call","id":call["toolCallId"],"call_id":call["toolCallId"],"name":call["toolName"],"arguments":""}})).await?;
            bridge::emit(tx,json!({"type":"response.function_call_arguments.delta","item_id":call["toolCallId"],"delta":call["args"].to_string()})).await?;
            bridge::emit(tx, json!({"type":"response.output_item.done","item":item})).await?;
            output.push(item);
        }
        if !result.text.is_empty() {
            output.push(json!({"type":"message","id":format!("{}-text",self.id),"role":"assistant","content":[{"type":"output_text","text":result.text}]}));
        }
        bridge::emit(tx,json!({"type":"response.completed","response":{"id":self.id,"model":self.model,"status":"completed","output":output,"usage":result.usage}})).await
    }
}

pub(super) fn public_error(error: &str) -> Value {
    // Malformed messages and rejected discovery selectors have not been
    // exposed as executable calls. Discard partial output and resample within
    // the existing retry budget; execution ownership and integrity failures
    // remain terminal.
    // Discovery drift has not been exposed as an executable call. Resample
    // it. Ownership, history structure, and certified-text mismatches stay
    // terminal.
    let recoverable_model_response = matches!(
        error,
        "cursor_native_message_invalid"
            | super::metadata::SELECTOR_INVALID
            | "cursor_metadata_result_mismatch"
            | "cursor_metadata_result_invalid"
    );
    let protocol_guard = error.starts_with("cursor_native_")
        || error.starts_with("cursor_read_intent_")
        || error.starts_with("cursor_tool_batch_")
        || error.starts_with("cursor_metadata_")
        || error.starts_with("cursor_blob_")
        || error.starts_with("cursor_checkpoint_")
        || error.starts_with("cursor_terminal_")
        || error.starts_with("cursor_frame_")
        || matches!(
            error,
            "cursor_unknown_blob"
                | "cursor_mixed_metadata_tool_batch"
                | "cursor_tool_not_owned"
                | "cursor_tool_executed_upstream"
        );
    let error = if protocol_guard {
        eprintln!("[Cursor] model response rejected by guard {error}");
        "cursor_model_response_unsupported"
    } else {
        error
    };
    let kind = if error == credential::NEEDS_REIMPORT
        || error == credential::NEEDS_LOGIN
        || error == credential::METADATA_MISSING
    {
        "authentication_error"
    } else if error == "cursor_inference_not_enabled" {
        "usage_not_included"
    } else if matches!(
        error,
        "cursor_permission_denied" | "cursor_model_blocked" | "cursor_max_mode_required"
    ) {
        "permission_error"
    } else if error == "cursor_rate_limit" {
        "rate_limit_error"
    } else if recoverable_model_response
        || matches!(
            error,
            "cursor_overloaded"
                | "cursor_network_error"
                | "cursor_response_read_failed"
                | "cursor_inference_incomplete"
        )
    {
        "overloaded_error"
    } else if matches!(
        error,
        "cursor_input_token_limit" | "cursor_context_length_exceeded"
    ) {
        "context_length_exceeded"
    } else if error == "cursor_token_limit_exceeded" {
        "token_limit_error"
    } else if matches!(
        error,
        "cursor_subscription_required" | "cursor_usage_pricing_required" | "cursor_usage_limit"
    ) {
        "usage_not_included"
    } else if error == "cursor_model_authorization_failed" {
        "permission_error"
    } else {
        "invalid_request_error"
    };
    json!({"type":"error","message":error,"error":{"code":kind,"type":kind,"message":error}})
}

#[cfg(test)]
mod tests {
    use super::normalize;
    use serde_json::json;

    #[test]
    fn foreign_checkpoint_and_file_parts_remain_portable() {
        let body = json!({
            "model": "cursor-model",
            "input": [
                {"type":"message","role":"user","content":[
                    {"type":"input_text","text":"hello from the previous model"},
                    {"type":"input_file","file_id":"not-replayable"}
                ]},
                {"type":"compaction","encrypted_content":"opaque-checkpoint"},
                {"type":"message","role":"user","content":"continue here"}
            ]
        });
        let chat = normalize(&body).expect("foreign history must not fail the switch");
        let rendered = chat.to_string();
        assert!(rendered.contains("hello from the previous model"));
        assert!(rendered.contains("continue here"));
        assert!(rendered.contains("not replayable on this model"));
        assert!(!rendered.contains("opaque-checkpoint"));
    }
}
