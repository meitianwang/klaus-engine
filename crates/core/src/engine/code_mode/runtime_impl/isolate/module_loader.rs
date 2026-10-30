use crate::runtime_values::{RuntimeJson, RuntimeString};
use crate::types::errors::CodeModeToolRejection;

use super::value::json_to_v8;
use super::value::runtime_string_to_v8;
use super::value::v8_string_to_runtime;
use super::value::value_to_error_text;
use super::CompletionState;
use super::RuntimeState;
use super::EXIT_SENTINEL;

pub(super) fn evaluate_main_module(
    scope: &mut v8::PinScope<'_, '_>,
    source_text: &RuntimeString,
) -> Result<Option<v8::Global<v8::Promise>>, RuntimeString> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let source = runtime_string_to_v8(&tc, source_text)
        .ok_or_else(|| RuntimeString::from("failed to allocate exec source"))?;
    let origin = script_origin(&mut tc, "exec_main.mjs")?;
    let mut source = v8::script_compiler::Source::new(source, Some(&origin));
    let module = v8::script_compiler::compile_module(&tc, &mut source).ok_or_else(|| {
        tc.exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| RuntimeString::from("unknown code mode exception"))
    })?;
    module
        .instantiate_module(&tc, resolve_module_callback)
        .ok_or_else(|| {
            tc.exception()
                .map(|exception| value_to_error_text(&mut tc, exception))
                .unwrap_or_else(|| RuntimeString::from("unknown code mode exception"))
        })?;
    let result = match module.evaluate(&tc) {
        Some(result) => result,
        None => {
            if let Some(exception) = tc.exception() {
                if is_exit_exception(&mut tc, exception) {
                    return Ok(None);
                }
                return Err(value_to_error_text(&mut tc, exception));
            }
            return Err(RuntimeString::from("unknown code mode exception"));
        }
    };
    tc.perform_microtask_checkpoint();

    if result.is_promise() {
        let promise = v8::Local::<v8::Promise>::try_from(result)
            .map_err(|_| RuntimeString::from("failed to read exec promise"))?;
        return Ok(Some(v8::Global::new(&tc, promise)));
    }

    Ok(None)
}

fn is_exit_exception(
    scope: &mut v8::PinScope<'_, '_>,
    exception: v8::Local<'_, v8::Value>,
) -> bool {
    scope
        .get_slot::<RuntimeState>()
        .map(|state| state.exit_requested)
        .unwrap_or(false)
        && exception.is_string()
        && v8::Local::<v8::String>::try_from(exception)
            .ok()
            .and_then(|exception| v8_string_to_runtime(scope, exception).ok())
            .is_some_and(|exception| exception == EXIT_SENTINEL)
}

pub(super) fn resolve_tool_response(
    scope: &mut v8::PinScope<'_, '_>,
    id: &RuntimeString,
    response: Result<RuntimeJson, CodeModeToolRejection>,
) -> Result<(), RuntimeString> {
    let resolver = {
        let state = scope
            .get_slot_mut::<RuntimeState>()
            .ok_or_else(|| RuntimeString::from("runtime state unavailable"))?;
        state.pending_tool_calls.remove(id)
    }
    .ok_or_else(|| {
        RuntimeString::from("unknown tool call `")
            .concat(id)
            .concat(&RuntimeString::from("`"))
    })?;

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let resolver = v8::Local::new(&tc, &resolver);
    match response {
        Ok(result) => {
            let value = json_to_v8(&mut tc, &result)
                .ok_or_else(|| RuntimeString::from("failed to serialize tool response"))?;
            resolver.resolve(&tc, value);
        }
        Err(rejection) => {
            let message = runtime_string_to_v8(&tc, &rejection.message)
                .ok_or_else(|| RuntimeString::from("failed to allocate tool error"))?;
            let value = v8::Exception::error(&tc, message);
            let error = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| RuntimeString::from("failed to allocate tool error object"))?;
            if rejection.name != "Error" {
                set_error_property(&tc, error, "name", rejection.name)?;
            }
            if let Some(kind) = rejection.kind_str() {
                set_error_property(&tc, error, "kind", kind)?;
            }
            resolver.reject(&tc, error.into());
        }
    }
    if tc.has_caught() {
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| RuntimeString::from("unknown code mode exception")));
    }
    Ok(())
}

fn set_error_property(
    scope: &v8::PinScope<'_, '_>,
    error: v8::Local<'_, v8::Object>,
    name: &str,
    value: &str,
) -> Result<(), RuntimeString> {
    let key = v8::String::new(scope, name)
        .ok_or_else(|| RuntimeString::from(format!("failed to allocate tool error {name} key")))?;
    let value = runtime_string_to_v8(scope, &RuntimeString::from(value)).ok_or_else(|| {
        RuntimeString::from(format!("failed to allocate tool error {name} value"))
    })?;
    error
        .set(scope, key.into(), value.into())
        .ok_or_else(|| RuntimeString::from(format!("failed to set tool error {name}")))?;
    Ok(())
}

pub(super) fn completion_state(
    scope: &mut v8::PinScope<'_, '_>,
    pending_promise: Option<&v8::Global<v8::Promise>>,
) -> CompletionState {
    let stored_value_writes = scope
        .get_slot::<RuntimeState>()
        .map(|state| state.stored_value_writes.clone())
        .unwrap_or_default();

    let Some(pending_promise) = pending_promise else {
        return CompletionState::Completed {
            stored_value_writes,
            error_text: None,
        };
    };

    let promise = v8::Local::new(scope, pending_promise);
    match promise.state() {
        v8::PromiseState::Pending => CompletionState::Pending,
        v8::PromiseState::Fulfilled => CompletionState::Completed {
            stored_value_writes,
            error_text: None,
        },
        v8::PromiseState::Rejected => {
            let result = promise.result(scope);
            let error_text = if is_exit_exception(scope, result) {
                None
            } else {
                Some(value_to_error_text(scope, result))
            };
            CompletionState::Completed {
                stored_value_writes,
                error_text,
            }
        }
    }
}

fn script_origin<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    resource_name_: &str,
) -> Result<v8::ScriptOrigin<'s>, RuntimeString> {
    let resource_name = v8::String::new(scope, resource_name_)
        .ok_or_else(|| RuntimeString::from("failed to allocate script origin"))?;
    let source_map_url = v8::String::new(scope, resource_name_)
        .ok_or_else(|| RuntimeString::from("failed to allocate source map url"))?;
    Ok(v8::ScriptOrigin::new(
        scope,
        resource_name.into(),
        0,
        0,
        true,
        0,
        Some(source_map_url.into()),
        true,
        false,
        true,
        None,
    ))
}

fn resolve_module_callback<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
    _referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    v8::callback_scope!(unsafe scope, context);
    let specifier = match v8_string_to_runtime(scope, specifier) {
        Ok(specifier) => specifier,
        Err(error_text) => {
            super::value::throw_type_error(scope, &error_text);
            return None;
        }
    };
    resolve_module(scope, &specifier)
}

pub(super) fn dynamic_import_callback<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    _resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let resolver = v8::PromiseResolver::new(scope)?;
    let specifier = match v8_string_to_runtime(scope, specifier) {
        Ok(specifier) => specifier,
        Err(error_text) => {
            let error = runtime_string_to_v8(scope, &error_text)
                .map(Into::into)
                .unwrap_or_else(|| v8::undefined(scope).into());
            resolver.reject(scope, error);
            return Some(resolver.get_promise(scope));
        }
    };

    match resolve_module(scope, &specifier) {
        Some(module) => {
            if module.get_status() == v8::ModuleStatus::Uninstantiated
                && module
                    .instantiate_module(scope, resolve_module_callback)
                    .is_none()
            {
                let error = v8::String::new(scope, "failed to instantiate module")
                    .map(Into::into)
                    .unwrap_or_else(|| v8::undefined(scope).into());
                resolver.reject(scope, error);
                return Some(resolver.get_promise(scope));
            }
            if matches!(
                module.get_status(),
                v8::ModuleStatus::Instantiated | v8::ModuleStatus::Evaluated
            ) && module.evaluate(scope).is_none()
            {
                let error = v8::String::new(scope, "failed to evaluate module")
                    .map(Into::into)
                    .unwrap_or_else(|| v8::undefined(scope).into());
                resolver.reject(scope, error);
                return Some(resolver.get_promise(scope));
            }
            let namespace = module.get_module_namespace();
            resolver.resolve(scope, namespace);
            Some(resolver.get_promise(scope))
        }
        None => {
            let error = v8::String::new(scope, "unsupported import in exec")
                .map(Into::into)
                .unwrap_or_else(|| v8::undefined(scope).into());
            resolver.reject(scope, error);
            Some(resolver.get_promise(scope))
        }
    }
}

fn resolve_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    specifier: &RuntimeString,
) -> Option<v8::Local<'s, v8::Module>> {
    let message = RuntimeString::from("Unsupported import in exec: ").concat(specifier);
    if let Some(message) = runtime_string_to_v8(scope, &message) {
        scope.throw_exception(message.into());
    } else {
        scope.throw_exception(v8::undefined(scope).into());
    }
    None
}
