//! Compatibility adapter for foreground shell execution.
//!
//! The lifecycle implementation lives in
//! [`crate::command_runtime::foreground`]. This module keeps the
//! historical tool-facing type names so existing built-ins do not change their
//! model-facing behavior.

pub use crate::command_runtime::foreground::{
    run_shell, OutputStream, RunShellOptions, RunShellResult,
};

// The single case here drives a POSIX shell command through the adapter, so the whole
// module is unix-only; on Windows its helpers would be types with no user.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
    use crate::runtime_values::{RuntimePath, RuntimeString};
    use crate::types::abort::AbortSignal;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::{Arc, Mutex};

    struct IdSandbox;

    impl SandboxBackend for IdSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }

        fn is_available(&self) -> bool {
            true
        }

        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req.argv.iter().skip(1).cloned().collect();
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    fn cwd() -> RuntimePath {
        RuntimePath::from(
            std::env::current_dir()
                .unwrap()
                .to_str()
                .expect("test cwd is a Unicode scalar path"),
        )
    }

    #[tokio::test]
    async fn exec_adapter_preserves_run_shell_api() {
        let mut env = RuntimeEnv::new();
        env.insert(
            RuntimeString::from("PATH"),
            RuntimeString::from(std::env::var("PATH").unwrap_or_default()),
        );
        let seen = Arc::new(Mutex::new(RuntimeString::default()));
        let sink = Arc::clone(&seen);

        let result = run_shell(RunShellOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            command: RuntimeString::from("printf adapter-output"),
            cwd: cwd(),
            sandbox: &IdSandbox,
            signal: AbortSignal::new(),
            timeout_ms: 30_000,
            max_output_bytes: 1024,
            env: Some(env),
            network_allowed: None,
            managed_network: None,
            writable_roots: None,
            unconfined_writes: false,
            on_chunk: Some(Box::new(move |stream, chunk| {
                if stream == OutputStream::Stdout {
                    let mut seen = sink.lock().unwrap();
                    *seen = seen.concat(chunk);
                }
                Box::pin(async {})
            })),
        })
        .await;

        assert_eq!(result.output, "adapter-output");
        assert_eq!(result.exit_code, Some(0));
        assert!(seen.lock().unwrap().contains("adapter-output"));
    }
}
