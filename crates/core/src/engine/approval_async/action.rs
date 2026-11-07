use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ActionRenderError {
    #[error("the complete action exceeds the {max_tokens}-token review allowance")]
    TooLarge { max_tokens: usize },
}

/// Keeps every argument, including untrusted keys, while replacing its tool identity.
/// Overflow defers the intact action to synchronous review instead of truncating it.
pub fn render_action(
    tool_name: RuntimeString,
    arguments: RuntimeJson,
    max_action_tokens: usize,
) -> Result<RuntimeString, ActionRenderError> {
    let mut action = match arguments.into_object() {
        Ok(object) => object,
        Err(arguments) => {
            let mut object = RuntimeObject::new();
            object.insert("arguments", arguments);
            object
        }
    };
    action.insert("tool", RuntimeJson::String(tool_name));
    let action = RuntimeJson::Object(action);
    render_review_action(&action, max_action_tokens)
}

/// Measures an already formatted review action without rewriting any field.
pub fn render_review_action(
    action: &RuntimeJson,
    max_action_tokens: usize,
) -> Result<RuntimeString, ActionRenderError> {
    let rendered = render_sorted(action);
    if rendered.to_node_utf8().len().saturating_add(1) > max_action_tokens.saturating_mul(4) {
        return Err(ActionRenderError::TooLarge {
            max_tokens: max_action_tokens,
        });
    }
    Ok(rendered)
}

enum RenderTask<'a> {
    Value(&'a RuntimeJson, usize),
    Text(RuntimeString),
    Ascii(&'static str),
    Indent(usize),
}

fn render_sorted(value: &RuntimeJson) -> RuntimeString {
    let mut output = Vec::new();
    let mut tasks = vec![RenderTask::Value(value, 0)];
    while let Some(task) = tasks.pop() {
        match task {
            RenderTask::Value(RuntimeJson::Object(object), depth) if !object.is_empty() => {
                let mut entries = object.entries().collect::<Vec<_>>();
                entries.sort_by_cached_key(|(key, _)| key.to_node_utf8());
                output.extend("{\n".encode_utf16());
                tasks.push(RenderTask::Ascii("}"));
                tasks.push(RenderTask::Indent(depth));
                let count = entries.len();
                for (index, (key, value)) in entries.into_iter().enumerate().rev() {
                    tasks.push(RenderTask::Ascii(if index + 1 == count {
                        "\n"
                    } else {
                        ",\n"
                    }));
                    tasks.push(RenderTask::Value(value, depth + 1));
                    tasks.push(RenderTask::Ascii(": "));
                    tasks.push(RenderTask::Text(runtime_json::stringify(
                        &RuntimeJson::String(key.clone()),
                    )));
                    tasks.push(RenderTask::Indent(depth + 1));
                }
            }
            RenderTask::Value(RuntimeJson::Array(values), depth) if !values.is_empty() => {
                output.extend("[\n".encode_utf16());
                tasks.push(RenderTask::Ascii("]"));
                tasks.push(RenderTask::Indent(depth));
                for (index, value) in values.iter().enumerate().rev() {
                    tasks.push(RenderTask::Ascii(if index + 1 == values.len() {
                        "\n"
                    } else {
                        ",\n"
                    }));
                    tasks.push(RenderTask::Value(value, depth + 1));
                    tasks.push(RenderTask::Indent(depth + 1));
                }
            }
            RenderTask::Value(RuntimeJson::Number(number), _)
                if number.as_f64() == 0.0 && number.as_f64().is_sign_negative() =>
            {
                output.extend("-0".encode_utf16());
            }
            RenderTask::Value(value, _) => output.extend(runtime_json::stringify(value).as_utf16()),
            RenderTask::Text(value) => output.extend(value.as_utf16()),
            RenderTask::Ascii(value) => output.extend(value.encode_utf16()),
            RenderTask::Indent(depth) => {
                for _ in 0..depth {
                    output.extend("  ".encode_utf16());
                }
            }
        }
    }
    RuntimeString::from_utf16(output)
}
