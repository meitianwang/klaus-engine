//! Read-only enforcement wrapper for the one class of plan-mode bash runs the
//! permission engine cannot prove read-only statically (interpreters, network
//! fetches piped into them, prefix-less globs). The engine auto-allows those
//! with an internal marker; the bash tool wraps the session sandbox in this
//! backend so the read-only promise is enforced by the OS instead of a
//! per-command prompt.

use std::sync::Arc;

use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};

/// Wraps a real isolation backend so ONE spawn gets a read-only filesystem and
/// no network: writable roots are dropped entirely and the backends' default
/// grants (the command's cwd, `/tmp`, `$TMPDIR`) are excluded, so no
/// `file-write*` survives beyond the base profile's `/dev/null` and stdio;
/// every network allowance is cleared. Reads keep the session policy, so
/// research commands still see the whole tree.
///
/// Only meaningful under a real isolation backend (seatbelt / bubblewrap /
/// native helper / Windows restricted token); callers skip wrapping under the
/// env-scrub-only process backend, which has no filesystem confinement to
/// tighten.
pub struct PlanReadOnlySandbox {
    inner: Arc<dyn SandboxBackend>,
}

impl PlanReadOnlySandbox {
    pub fn new(inner: Arc<dyn SandboxBackend>) -> Self {
        Self { inner }
    }
}

impl SandboxBackend for PlanReadOnlySandbox {
    fn enforces_planning_read_only(&self) -> bool {
        true
    }

    fn task_resource_scope(
        &self,
    ) -> Option<Arc<crate::permission::task_scope::CanonicalTaskResourceScope>> {
        self.inner.task_resource_scope()
    }

    fn task_runtime_program(
        &self,
        program: &crate::runtime_values::RuntimePath,
    ) -> Option<Arc<dyn SandboxBackend>> {
        self.inner
            .task_runtime_program(program)
            .map(|backend| Arc::new(Self::new(backend)) as Arc<dyn SandboxBackend>)
    }

    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }

    fn is_available(&self) -> bool {
        self.inner.is_available()
    }

    fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let mut enforced = policy.clone();
        enforced.enforce_planning_read_only();
        self.inner.transform(req, &enforced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimePath;
    use crate::types::sandbox::SpawnSpec;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CapturingBackend {
        captured: Mutex<Option<SandboxPolicy>>,
    }

    impl SandboxBackend for CapturingBackend {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, _req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.captured.lock().unwrap() = Some(policy.clone());
            SpawnSpec {
                file: crate::runtime_values::RuntimeString::from("/bin/sh"),
                args: Vec::new(),
                cwd: RuntimePath::from("/work"),
                env: crate::runtime_process::RuntimeEnv::new(),
            }
        }
    }

    #[test]
    fn wrapper_drops_writes_and_network_and_keeps_reads() {
        let inner = Arc::new(CapturingBackend::default());
        let wrapper = PlanReadOnlySandbox::new(inner.clone());
        let policy = SandboxPolicy {
            writable_roots: vec![crate::runtime_values::RuntimeString::from("/work")],
            network_allowed: true,
            allow_loopback_ports: Some(vec![8080]),
            ..Default::default()
        };
        let req = ExecRequest {
            argv: vec![crate::runtime_values::RuntimeString::from("true")],
            cwd: RuntimePath::from("/work"),
            env: None,
            timeout_ms: None,
        };
        wrapper.transform(&req, &policy);
        let captured = inner.captured.lock().unwrap().clone().unwrap();
        assert!(captured.writable_roots.is_empty());
        assert!(!captured.network_allowed);
        assert_eq!(captured.exclude_tmpdir_env_var, Some(true));
        assert_eq!(captured.exclude_slash_tmp, Some(true));
        assert_eq!(captured.exclude_cwd, Some(true));
        assert!(captured.allow_loopback_ports.is_none());
        // Reads are untouched: research still sees the tree.
        assert!(captured.readable_roots.is_none());
    }

    /// Real-backend proof on macOS: under the wrapper a write OUTSIDE the cwd
    /// fails at the OS layer, while the same write under the plain session
    /// backend succeeds. The cwd stays writable by design (it is the
    /// workspace), so the target lives in the scratch temp instead.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn enforced_policy_blocks_a_real_write_on_macos() {
        use crate::runtime_values::RuntimeString;
        use crate::sandbox::seatbelt::SeatbeltSandbox;
        use crate::tools::exec::{run_shell, RunShellOptions};
        use crate::types::abort::AbortSignal;

        let work = std::env::temp_dir().join(format!("klaus-plan-ro-cwd-{}", std::process::id()));
        std::fs::create_dir_all(&work).unwrap();
        let target = std::env::temp_dir().join(format!("klaus-plan-ro-{}.txt", std::process::id()));
        let _ = std::fs::remove_file(&target);
        let command = RuntimeString::from(format!("printf x > {}", target.display()));
        let cwd = RuntimePath::from(work.to_str().unwrap());

        let enforced = run_shell(RunShellOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            command: command.clone(),
            cwd: cwd.clone(),
            sandbox: &PlanReadOnlySandbox::new(Arc::new(SeatbeltSandbox::new())),
            signal: AbortSignal::new(),
            timeout_ms: 20_000,
            max_output_bytes: 4096,
            env: None,
            network_allowed: None,
            managed_network: None,
            writable_roots: Some(vec![RuntimeString::from(work.to_str().unwrap())]),
            unconfined_writes: false,
            on_chunk: None,
        })
        .await;
        assert_ne!(
            enforced.exit_code,
            Some(0),
            "the write must fail under enforcement"
        );
        assert!(!target.exists(), "no file may be created");

        // Control: the same write under the plain backend succeeds, proving the
        // block above comes from the wrapper and not from the command itself.
        let plain = run_shell(RunShellOptions {
            process_context: crate::runtime_process::test_runtime_process_context(),
            command,
            cwd,
            sandbox: &SeatbeltSandbox::new(),
            signal: AbortSignal::new(),
            timeout_ms: 20_000,
            max_output_bytes: 4096,
            env: None,
            network_allowed: None,
            managed_network: None,
            writable_roots: None,
            unconfined_writes: false,
            on_chunk: None,
        })
        .await;
        assert_eq!(plain.exit_code, Some(0), "the control write must succeed");
        assert!(target.exists());
        std::fs::remove_file(&target).ok();
        std::fs::remove_dir_all(&work).ok();
    }
}
