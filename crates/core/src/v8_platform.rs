//! Process-wide V8 and ICU initialization shared by every embedded runtime.
//!
//! Also owns the heap-limited isolate construction used by every embedded
//! script runtime, so a runaway script degrades into a clean per-isolate
//! error instead of growing until the engine process exhausts machine
//! memory.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::runtime_values::RuntimeString;

/// Hard V8 heap limit for long-lived model-authored script isolates (code
/// mode cells and REPL sessions).
pub(crate) const SCRIPT_HEAP_LIMIT_BYTES: usize = 512 * 1024 * 1024;

/// Hard V8 heap limit for short-lived auxiliary isolates that run
/// harness-authored transforms over bounded inputs (output filters, locale
/// sort).
pub(crate) const AUX_SCRIPT_HEAP_LIMIT_BYTES: usize = 128 * 1024 * 1024;

/// Headroom granted when the near-heap-limit callback fires so V8 can unwind
/// the terminated script instead of aborting the process.
const HEAP_LIMIT_UNWIND_HEADROOM_BYTES: usize = 32 * 1024 * 1024;

struct NearHeapLimitData {
    handle: v8::IsolateHandle,
    hit: Arc<AtomicBool>,
}

/// Near-heap-limit handler: records the breach, interrupts the running
/// script so the limit surfaces as a catchable termination instead of
/// `FatalProcessOutOfMemory`, and grants unwind headroom.
unsafe extern "C" fn near_heap_limit_callback(
    data: *mut c_void,
    current_heap_limit: usize,
    _initial_heap_limit: usize,
) -> usize {
    let data = &*(data as *const NearHeapLimitData);
    data.hit.store(true, Ordering::Relaxed);
    data.handle.terminate_execution();
    current_heap_limit.saturating_add(HEAP_LIMIT_UNWIND_HEADROOM_BYTES)
}

/// An isolate created with a hard heap limit and a near-heap-limit callback.
///
/// The callback data must outlive the isolate (V8 can invoke the callback
/// during teardown), so the manual `Drop` disposes the isolate before
/// reclaiming the boxed data.
pub(crate) struct HeapLimitedIsolate {
    isolate: Option<v8::OwnedIsolate>,
    callback_data: *mut NearHeapLimitData,
}

impl HeapLimitedIsolate {
    pub(crate) fn as_mut(&mut self) -> &mut v8::OwnedIsolate {
        self.isolate
            .as_mut()
            .expect("heap-limited isolate is present until drop")
    }
}

impl Drop for HeapLimitedIsolate {
    fn drop(&mut self) {
        drop(self.isolate.take());
        // SAFETY: the box was leaked in `heap_limited_isolate` and this guard
        // is the sole owner; the isolate is already disposed above.
        drop(unsafe { Box::from_raw(self.callback_data) });
    }
}

/// Cheap cloneable probe telling whether the isolate hit its heap limit,
/// with the user-facing explanation for that breach.
#[derive(Clone)]
pub(crate) struct HeapLimitWatch {
    hit: Arc<AtomicBool>,
    limit_bytes: usize,
}

impl HeapLimitWatch {
    pub(crate) fn hit(&self) -> bool {
        self.hit.load(Ordering::Relaxed)
    }

    /// Clear the breach flag. Persistent isolates (REPL sessions) reset it
    /// between evaluations so one breach does not relabel later
    /// interruptions.
    pub(crate) fn reset(&self) {
        self.hit.store(false, Ordering::Relaxed);
    }

    /// The heap-limit error text when the limit was hit, else `None`.
    pub(crate) fn error_text(&self) -> Option<RuntimeString> {
        self.hit().then(|| RuntimeString::from(self.message()))
    }

    /// The heap-limit error text when the limit was hit, else `None`.
    pub(crate) fn error_string(&self) -> Option<String> {
        self.hit().then(|| self.message())
    }

    fn message(&self) -> String {
        format!(
            "JavaScript heap out of memory: the script exceeded the {} MB heap limit and was stopped. Reduce memory usage: process large data in chunks instead of accumulating it in strings or arrays.",
            self.limit_bytes / (1024 * 1024)
        )
    }
}

/// Create an isolate whose heap is hard-capped at `limit_bytes`, together
/// with a watch that reports whether the cap was breached. Callers must run
/// [`initialize_v8`] first.
pub(crate) fn heap_limited_isolate(limit_bytes: usize) -> (HeapLimitedIsolate, HeapLimitWatch) {
    let mut isolate = v8::Isolate::new(v8::CreateParams::default().heap_limits(0, limit_bytes));
    let hit = Arc::new(AtomicBool::new(false));
    let data = Box::into_raw(Box::new(NearHeapLimitData {
        handle: isolate.thread_safe_handle(),
        hit: hit.clone(),
    }));
    isolate.add_near_heap_limit_callback(near_heap_limit_callback, data.cast::<c_void>());
    (
        HeapLimitedIsolate {
            isolate: Some(isolate),
            callback_data: data,
        },
        HeapLimitWatch { hit, limit_bytes },
    )
}

/// Initialize the single process-wide V8 platform with the bundled ICU 77
/// data. All isolates must call this boundary before construction.
pub(crate) fn initialize_v8() -> Result<(), String> {
    static PLATFORM: OnceLock<Result<v8::SharedRef<v8::Platform>, String>> = OnceLock::new();

    match PLATFORM.get_or_init(|| {
        v8::icu::set_common_data_77(klaus_agent_icu_data::common_data_77()?)
            .map_err(|error_code| format!("failed to initialize ICU data: {error_code}"))?;
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform.clone());
        v8::V8::initialize();
        Ok(platform)
    }) {
        Ok(_) => Ok(()),
        Err(error_text) => Err(error_text.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_icu_data_crate_supplies_the_v8_boundary() {
        let data = klaus_agent_icu_data::common_data_77().unwrap();
        assert_eq!(data.len(), 31_895_376);
        assert_eq!(data.as_ptr() as usize % 16, 0);
    }

    #[test]
    fn heap_limited_isolate_runs_normal_scripts_without_breaching() {
        initialize_v8().unwrap();
        let (mut heap_isolate, watch) = heap_limited_isolate(16 * 1024 * 1024);
        let isolate = heap_isolate.as_mut();
        v8::scope!(let scope, isolate);
        let context = v8::Context::new(scope, Default::default());
        let scope = &mut v8::ContextScope::new(scope, context);
        let source = v8::String::new(scope, "40 + 2").unwrap();
        let script = v8::Script::compile(scope, source, None).unwrap();
        let value = script.run(scope).expect("normal script runs");
        assert_eq!(value.int32_value(scope).unwrap(), 42);
        assert!(!watch.hit());
        assert!(watch.error_text().is_none());
    }

    #[test]
    fn heap_limited_isolate_stops_a_runaway_script_with_a_clean_error() {
        initialize_v8().unwrap();
        let (mut heap_isolate, watch) = heap_limited_isolate(16 * 1024 * 1024);
        let outcome = {
            let isolate = heap_isolate.as_mut();
            v8::scope!(let scope, isolate);
            let context = v8::Context::new(scope, Default::default());
            let scope = &mut v8::ContextScope::new(scope, context);
            let tc = std::pin::pin!(v8::TryCatch::new(scope));
            let tc = tc.init();
            let source = v8::String::new(
                &tc,
                "const chunks = []; for (;;) { chunks.push(new Array(200_000).fill('x'.repeat(64))); }",
            )
            .unwrap();
            let script = v8::Script::compile(&tc, source, None).unwrap();
            script.run(&tc).is_none()
        };
        // The runaway script was interrupted instead of running forever or
        // aborting the process, and the breach is reported with an
        // actionable message.
        assert!(outcome, "runaway script must not complete normally");
        assert!(watch.hit());
        let message = watch.error_string().expect("heap limit error text");
        assert!(message.contains("heap out of memory"), "{message}");
        assert!(message.contains("16 MB"), "{message}");
    }
}
