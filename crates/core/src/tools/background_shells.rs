//! Compatibility adapter for background shell tools.
//!
//! The lifecycle implementation lives in
//! [`crate::command_runtime::background`]. This module keeps the
//! historical tool-facing type names so existing built-ins keep their
//! model-facing behavior while the registry itself remains session-owned.

pub use crate::command_runtime::background::{
    BackgroundCommandComplete as CompleteResult, BackgroundCommandListEntry as ShellListEntry,
    BackgroundCommandOnComplete as OnComplete,
    BackgroundCommandRegistry as BackgroundShellRegistry,
    BackgroundCommandStatus as BackgroundShellStatus, ReadBackgroundCommandResult as ReadNewResult,
    StartBackgroundCommandOptions as StartShellOptions,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::runtime_values::{RuntimePath, RuntimeString};
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::sync::Arc;
    use std::time::Duration;

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
            let args = req
                .argv
                .get(1..)
                .map(<[RuntimeString]>::to_vec)
                .unwrap_or_default();
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
    async fn background_shells_adapter_preserves_start_and_read_api() {
        let registry = BackgroundShellRegistry::new();
        let id = registry
            .start(
                crate::runtime_process::test_runtime_process_context(),
                StartShellOptions {
                    command: RuntimeString::from("printf adapter-output"),
                    cwd: cwd(),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .unwrap();

        let mut output = RuntimeString::default();
        for _ in 0..20 {
            let read = registry.read_new(&id);
            output = output.concat(&read.output);
            if output.contains("adapter-output") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(output.contains("adapter-output"));
        registry.kill_all();
    }
}
