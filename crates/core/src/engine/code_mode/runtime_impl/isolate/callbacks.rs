use crate::engine::code_mode::protocol::OutputContentItem;
use crate::runtime_values::RuntimeString;

use super::timers;
use super::value::json_to_v8;
use super::value::normalize_output_image;
use super::value::runtime_string_to_v8;
use super::value::serialize_output_text;
use super::value::throw_type_error;
use super::value::v8_string_to_runtime;
use super::value::v8_value_to_json;
use super::value::v8_value_to_runtime_string;
use super::RuntimeEvent;
use super::RuntimeState;
use super::EXIT_SENTINEL;

pub(super) fn tool_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let tool_index = match v8_value_to_runtime_string(scope, args.data())
        .ok()
        .and_then(|value| value.try_to_utf8().ok())
        .and_then(|value| value.parse::<usize>().ok())
    {
        Some(tool_index) => tool_index,
        None => {
            throw_static_type_error(scope, "invalid tool callback data");
            return;
        }
    };
    let input = if args.length() == 0 {
        Ok(None)
    } else {
        v8_value_to_json(scope, args.get(0))
    };
    let input = match input {
        Ok(input) => input,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };

    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        throw_static_type_error(scope, "failed to create tool promise");
        return;
    };
    let promise = resolver.get_promise(scope);

    let resolver = v8::Global::new(scope, resolver);
    let (tool_name, tool_kind) = {
        let Some(state) = scope.get_slot::<RuntimeState>() else {
            throw_static_type_error(scope, "runtime state unavailable");
            return;
        };
        let Some(tool) = state.enabled_tools.get(tool_index) else {
            throw_static_type_error(scope, "tool callback data is out of range");
            return;
        };
        (tool.tool_name.clone(), tool.kind)
    };

    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        throw_static_type_error(scope, "runtime state unavailable");
        return;
    };
    let id = RuntimeString::from(format!("tool-{}", state.next_tool_call_id));
    state.next_tool_call_id = state.next_tool_call_id.saturating_add(1);
    let event_tx = state.event_tx.clone();
    state.pending_tool_calls.insert(id.clone(), resolver);
    let _ = event_tx.send(RuntimeEvent::ToolCall {
        id,
        name: tool_name,
        kind: tool_kind,
        input,
    });
    retval.set(promise.into());
}

pub(super) fn text_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let value = if args.length() == 0 {
        v8::undefined(scope).into()
    } else {
        args.get(0)
    };
    let text = match serialize_output_text(scope, value) {
        Ok(text) => text,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state
            .event_tx
            .send(RuntimeEvent::ContentItem(OutputContentItem::InputText {
                text,
            }));
    }
    retval.set(v8::undefined(scope).into());
}

pub(super) fn image_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let value = if args.length() == 0 {
        v8::undefined(scope).into()
    } else {
        args.get(0)
    };
    let detail_override = if args.length() < 2 {
        None
    } else {
        let detail = args.get(1);
        if detail.is_string() {
            match v8_value_to_runtime_string(scope, detail) {
                Ok(detail) => Some(detail),
                Err(error_text) => {
                    throw_type_error(scope, &error_text);
                    return;
                }
            }
        } else if detail.is_null() || detail.is_undefined() {
            None
        } else {
            throw_static_type_error(scope, "image detail must be a string when provided");
            return;
        }
    };
    let image_item = match normalize_output_image(scope, value, detail_override) {
        Ok(image_item) => image_item,
        Err(()) => return,
    };
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state.event_tx.send(RuntimeEvent::ContentItem(image_item));
    }
    retval.set(v8::undefined(scope).into());
}

pub(super) fn generated_image_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let value = if args.length() == 0 {
        v8::undefined(scope).into()
    } else {
        args.get(0)
    };
    let output_hint = match generated_image_output_hint(scope, value) {
        Ok(output_hint) => output_hint,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    let image_item = match normalize_output_image(scope, value, /*detail_override*/ None) {
        Ok(image_item) => image_item,
        Err(()) => return,
    };
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state.event_tx.send(RuntimeEvent::ContentItem(image_item));
        if let Some(text) = output_hint {
            let _ = state
                .event_tx
                .send(RuntimeEvent::ContentItem(OutputContentItem::InputText {
                    text,
                }));
        }
    }
    retval.set(v8::undefined(scope).into());
}

fn generated_image_output_hint(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<Option<RuntimeString>, RuntimeString> {
    let object = v8::Local::<v8::Object>::try_from(value).map_err(|_| {
        RuntimeString::from("generatedImage expects an image generation result object")
    })?;
    let key = v8::String::new(scope, "output_hint")
        .ok_or_else(|| RuntimeString::from("failed to allocate generatedImage helper keys"))?;
    let output_hint = object
        .get(scope, key.into())
        .ok_or_else(|| RuntimeString::from("failed to read generatedImage output_hint"))?;
    if output_hint.is_undefined() {
        return Ok(None);
    }
    if !output_hint.is_string() {
        return Err(RuntimeString::from(
            "generatedImage output_hint must be a string when provided",
        ));
    }
    Ok(Some(v8_value_to_runtime_string(scope, output_hint)?))
}

pub(super) fn store_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    let value = args.get(1);
    let value = if value.is_null_or_undefined() {
        v8::null(scope).into()
    } else {
        value
    };
    let serialized = match v8_value_to_json(scope, value) {
        Ok(Some(value)) => value,
        Ok(None) | Err(_) => {
            throw_store_value_error(scope);
            return;
        }
    };
    let Some(key) = js_string_coerce(scope, args.get(0)) else {
        return;
    };
    if let Some(state) = scope.get_slot_mut::<RuntimeState>() {
        state.stored_values.insert(key.clone(), serialized.clone());
        state.stored_value_writes.insert(key, serialized);
    }
}

const STORE_VALUE_ERROR: &str =
    "store() requires a JSON-serializable value (no BigInt, function, or circular reference)";

fn throw_store_value_error(scope: &mut v8::PinScope<'_, '_>) {
    if let Some(message) = runtime_string_to_v8(scope, &RuntimeString::from(STORE_VALUE_ERROR)) {
        let error = v8::Exception::error(scope, message);
        scope.throw_exception(error);
    }
}

/// `String(value)` matches `ToString(value)` except that primitive Symbols use
/// their descriptive form instead of throwing.
fn js_string_coerce(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Option<RuntimeString> {
    if value.is_symbol() {
        let symbol = v8::Local::<v8::Symbol>::try_from(value).ok()?;
        let description = symbol.description(scope);
        let description = if description.is_undefined() {
            RuntimeString::default()
        } else {
            let description = v8::Local::<v8::String>::try_from(description).ok()?;
            v8_string_to_runtime(scope, description).ok()?
        };
        return Some(
            RuntimeString::from("Symbol(")
                .concat(&description)
                .concat(&RuntimeString::from(")")),
        );
    }

    v8_value_to_runtime_string(scope, value).ok()
}

pub(super) fn load_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let Some(key) = js_string_coerce(scope, args.get(0)) else {
        return;
    };
    let value = scope
        .get_slot::<RuntimeState>()
        .and_then(|state| state.stored_values.get(&key))
        .cloned();
    let Some(value) = value else {
        retval.set(v8::undefined(scope).into());
        return;
    };
    let Some(value) = json_to_v8(scope, &value) else {
        throw_static_type_error(scope, "failed to load stored value");
        return;
    };
    retval.set(value);
}

pub(super) fn notify_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let value = if args.length() == 0 {
        v8::undefined(scope).into()
    } else {
        args.get(0)
    };
    let text = match serialize_output_text(scope, value) {
        Ok(text) => text,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    if text
        .as_utf16()
        .iter()
        .copied()
        .all(is_ecmascript_whitespace)
    {
        throw_static_type_error(scope, "notify expects non-empty text");
        return;
    }
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state.event_tx.send(RuntimeEvent::Notify {
            call_id: state.tool_call_id.clone(),
            text,
        });
    }
    retval.set(v8::undefined(scope).into());
}

pub(super) fn set_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let timeout_id = match timers::schedule_timeout(scope, args) {
        Ok(timeout_id) => timeout_id,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };

    retval.set(v8::Number::new(scope, timeout_id as f64).into());
}

pub(super) fn clear_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    if let Err(error_text) = timers::clear_timeout(scope, args) {
        throw_type_error(scope, &error_text);
        return;
    }

    retval.set(v8::undefined(scope).into());
}

fn throw_static_type_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    throw_type_error(scope, &RuntimeString::from(message));
}

fn is_ecmascript_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

pub(super) fn yield_control_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state.event_tx.send(RuntimeEvent::YieldRequested);
    }
    retval.set(v8::undefined(scope).into());
}

pub(super) fn exit_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot_mut::<RuntimeState>() {
        state.exit_requested = true;
    }
    if let Some(error) = v8::String::new(scope, EXIT_SENTINEL) {
        scope.throw_exception(error.into());
    }
}
