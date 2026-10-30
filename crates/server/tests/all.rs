//! Integration suite for this crate.
//!
//! One target instead of one per file. Cargo turns every tests/*.rs into its
//! own target and each target links the whole engine core, so every file here
//! cost another link of a large debug binary. Mounting them with `#[path]`
//! keeps each file where it is and leaves one link. Same convention as
//! `crates/core/tests/all.rs`.

/// The ONE process supervisor this binary may own.
///
/// `RuntimeProcessSupervisor` is a process-level singleton bound to the Tokio
/// runtime that initialized it. Two files each building their own runtime and
/// binding it are therefore a race, not two independent harnesses: whichever
/// binds first leaves the other failing with "already bound to a different Tokio
/// runtime" — which passes under `--test-threads=1` and fails under `cargo
/// test`'s default parallelism, i.e. exactly the shape that reads as flaky.
///
/// So the runtime, the supervisor and the context are owned here, where the
/// binary is composed, and every file that needs a process context borrows this
/// one. Nothing shuts it down: it is process state, and closing it mid-run would
/// pull the ground out from under whichever test happens to still be going.
pub mod shared_process {
    use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};
    use std::sync::{Arc, OnceLock};
    use tokio::runtime::{Handle, Runtime};

    struct Owner {
        runtime: Runtime,
        _supervisor: Arc<RuntimeProcessSupervisor>,
        context: RuntimeProcessContext,
    }

    fn owner() -> &'static Owner {
        static OWNER: OnceLock<Owner> = OnceLock::new();
        OWNER.get_or_init(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("the shared process runtime must build");
            let supervisor = RuntimeProcessSupervisor::initialize(runtime.handle().clone())
                .expect("the shared process supervisor must initialize");
            let context = RuntimeProcessContext::initialize(supervisor.clone())
                .expect("the shared process context must initialize");
            Owner {
                runtime,
                _supervisor: supervisor,
                context,
            }
        })
    }

    /// The runtime the supervisor is bound to. A test that spawns host processes
    /// runs its body here, so spawning and awaiting happen on one runtime.
    pub fn runtime() -> &'static Handle {
        owner().runtime.handle()
    }

    /// The shared context, as a host-launching engine would own.
    pub fn context() -> RuntimeProcessContext {
        owner().context.clone()
    }
}

#[path = "code_mode_host_process.rs"]
mod code_mode_host_process;

#[path = "regression_stdio_argv_never_hangs.rs"]
mod regression_stdio_argv_never_hangs;

#[path = "stdio_plugins.rs"]
mod stdio_plugins;
