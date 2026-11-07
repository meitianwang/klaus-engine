use std::time::Duration;

use crate::runtime_values::RuntimeString;
use tokio_util::sync::CancellationToken;

use super::value::value_to_error_text;
use super::RuntimeCommand;
use super::RuntimeState;

pub(super) struct ScheduledTimeout {
    callback: v8::Global<v8::Function>,
    cancellation_token: CancellationToken,
}

impl ScheduledTimeout {
    pub(super) fn cancel(self) {
        self.cancellation_token.cancel();
    }
}

pub(super) fn schedule_timeout(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<u64, RuntimeString> {
    let callback = args.get(0);
    if !callback.is_function() {
        return Err(RuntimeString::from(
            "setTimeout expects a function callback",
        ));
    }
    let callback = v8::Local::<v8::Function>::try_from(callback)
        .map_err(|_| RuntimeString::from("setTimeout expects a function callback"))?;

    let delay_ms = args
        .get(1)
        .number_value(scope)
        .map(normalize_delay_ms)
        .unwrap_or(0);

    let callback = v8::Global::new(scope, callback);
    let state = scope
        .get_slot_mut::<RuntimeState>()
        .ok_or_else(|| RuntimeString::from("runtime state unavailable"))?;
    let timeout_id = state.next_timeout_id;
    state.next_timeout_id = state.next_timeout_id.saturating_add(1);
    let runtime_command_tx = state.runtime_command_tx.clone();
    let cancellation_token = CancellationToken::new();
    state.pending_timeouts.insert(
        timeout_id,
        ScheduledTimeout {
            callback,
            cancellation_token: cancellation_token.clone(),
        },
    );
    state.timer_handle.spawn(async move {
        tokio::select! {
            _ = cancellation_token.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {
                let _ = runtime_command_tx.send(RuntimeCommand::TimeoutFired { id: timeout_id });
            }
        }
    });

    Ok(timeout_id)
}

pub(super) fn clear_timeout(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<(), RuntimeString> {
    let Some(timeout_id) = timeout_id_from_args(scope, args)? else {
        return Ok(());
    };

    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        return Err(RuntimeString::from("runtime state unavailable"));
    };
    if let Some(timeout) = state.pending_timeouts.remove(&timeout_id) {
        timeout.cancel();
    }
    Ok(())
}

pub(super) fn invoke_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    timeout_id: u64,
) -> Result<(), RuntimeString> {
    let callback = {
        let state = scope
            .get_slot_mut::<RuntimeState>()
            .ok_or_else(|| RuntimeString::from("runtime state unavailable"))?;
        state.pending_timeouts.remove(&timeout_id)
    };
    let Some(callback) = callback else {
        return Ok(());
    };
    callback.cancellation_token.cancel();

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let callback = v8::Local::new(&tc, &callback.callback);
    let receiver = v8::undefined(&tc).into();
    let _ = callback.call(&tc, receiver, &[]);
    if tc.has_caught() {
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| RuntimeString::from("unknown code mode exception")));
    }

    Ok(())
}
fn timeout_id_from_args(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<Option<u64>, RuntimeString> {
    if args.length() == 0 || args.get(0).is_null_or_undefined() {
        return Ok(None);
    }

    let Some(timeout_id) = args.get(0).number_value(scope) else {
        return Err(RuntimeString::from(
            "clearTimeout expects a numeric timeout id",
        ));
    };
    if !timeout_id.is_finite() || timeout_id <= 0.0 {
        return Ok(None);
    }

    Ok(Some(timeout_id.trunc().min(u64::MAX as f64) as u64))
}

fn normalize_delay_ms(delay_ms: f64) -> u64 {
    if !delay_ms.is_finite() || delay_ms <= 0.0 {
        0
    } else {
        delay_ms.trunc().min(u64::MAX as f64) as u64
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn timers_do_not_spawn_raw_os_threads() {
        let source = include_str!("timers.rs");
        let implementation = source.split("#[cfg(test)]").next().unwrap_or(source);
        assert!(
            !implementation.contains("thread::spawn"),
            "timer scheduling must not use raw OS threads"
        );
    }
}
