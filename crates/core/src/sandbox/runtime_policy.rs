//! Apply the captured filesystem ceiling before a shell reaches its backend.

use crate::runtime_values::RuntimeString;
use crate::types::permission::SandboxMode;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use std::sync::Arc;

pub(crate) struct RuntimePolicySandbox {
    inner: Arc<dyn SandboxBackend>,
    mode: SandboxMode,
    writable_roots: Vec<RuntimeString>,
    network_allowed: bool,
    exclude_tmpdir_env_var: bool,
    exclude_slash_tmp: bool,
}

impl RuntimePolicySandbox {
    pub(crate) fn new(
        inner: Arc<dyn SandboxBackend>,
        mode: SandboxMode,
        writable_roots: Vec<RuntimeString>,
        network_allowed: bool,
        exclude_tmpdir_env_var: bool,
        exclude_slash_tmp: bool,
    ) -> Self {
        Self {
            inner,
            mode,
            writable_roots,
            network_allowed,
            exclude_tmpdir_env_var,
            exclude_slash_tmp,
        }
    }
}

impl SandboxBackend for RuntimePolicySandbox {
    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }
    fn is_available(&self) -> bool {
        self.inner.is_available()
    }
    fn enforces_planning_read_only(&self) -> bool {
        self.inner.enforces_planning_read_only()
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
        self.inner.task_runtime_program(program).map(|inner| {
            Arc::new(Self::new(
                inner,
                self.mode,
                self.writable_roots.clone(),
                self.network_allowed,
                self.exclude_tmpdir_env_var,
                self.exclude_slash_tmp,
            )) as Arc<dyn SandboxBackend>
        })
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let planning_read_only = policy.planning_read_only;
        let filesystem_read_only = policy.filesystem_read_only;
        let network_ceiling = policy.runtime_network_ceiling;
        let mut policy = policy.clone();
        match self.mode {
            SandboxMode::ReadOnly => {
                policy.enforce_filesystem_read_only();
                policy.network_allowed = self.network_allowed;
                policy.runtime_network_ceiling = Some(self.network_allowed);
                if !self.network_allowed {
                    policy.enforce_no_network();
                }
            }
            SandboxMode::WorkspaceWrite => {
                policy.writable_roots = self.writable_roots.clone();
                policy.exclude_cwd = Some(true);
                policy.exclude_tmpdir_env_var = Some(self.exclude_tmpdir_env_var);
                policy.exclude_slash_tmp = Some(self.exclude_slash_tmp);
                policy.unconfined_writes = None;
                policy.network_allowed = self.network_allowed;
                policy.runtime_network_ceiling = Some(self.network_allowed);
                if !self.network_allowed {
                    policy.enforce_no_network();
                }
            }
            SandboxMode::DangerFullAccess => {
                policy.unconfined_writes = Some(true);
                policy.network_allowed = self.network_allowed;
                policy.runtime_network_ceiling = Some(self.network_allowed);
            }
        }
        if filesystem_read_only {
            policy.enforce_filesystem_read_only();
        }
        if planning_read_only || self.inner.enforces_planning_read_only() {
            policy.enforce_planning_read_only();
        }
        if network_ceiling == Some(false) {
            policy.runtime_network_ceiling = Some(false);
            policy.enforce_no_network();
        }
        self.inner.transform(request, &policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimePath;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Capture(Mutex<Option<SandboxPolicy>>);
    impl SandboxBackend for Capture {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            *self.0.lock().unwrap() = Some(policy.clone());
            SpawnSpec {
                file: "true".into(),
                args: Vec::new(),
                cwd: request.cwd.clone(),
                env: Default::default(),
            }
        }
    }
    fn request() -> ExecRequest {
        ExecRequest {
            argv: vec!["true".into()],
            cwd: RuntimePath::from("/outside"),
            env: None,
            timeout_ms: None,
        }
    }
    #[test]
    fn workspace_policy_does_not_grant_an_arbitrary_command_directory() {
        let capture = Arc::new(Capture::default());
        let wrapper = RuntimePolicySandbox::new(
            capture.clone(),
            SandboxMode::WorkspaceWrite,
            vec!["/workspace".into()],
            false,
            false,
            false,
        );
        wrapper.transform(
            &request(),
            &SandboxPolicy {
                writable_roots: vec!["/outside".into()],
                unconfined_writes: Some(true),
                network_allowed: true,
                ..Default::default()
            },
        );
        let captured = capture.0.lock().unwrap().clone().unwrap();
        assert_eq!(
            captured.writable_roots,
            vec![RuntimeString::from("/workspace")]
        );
        assert_eq!(captured.exclude_cwd, Some(true));
        assert_ne!(captured.unconfined_writes, Some(true));
        assert!(!captured.network_allowed);
    }
    #[test]
    fn read_only_policy_suppresses_all_default_write_and_network_grants() {
        let capture = Arc::new(Capture::default());
        let wrapper = RuntimePolicySandbox::new(
            capture.clone(),
            SandboxMode::ReadOnly,
            vec!["/workspace".into()],
            false,
            false,
            false,
        );
        wrapper.transform(
            &request(),
            &SandboxPolicy {
                writable_roots: vec!["/outside".into()],
                unconfined_writes: Some(true),
                network_allowed: true,
                allow_loopback_ports: Some(vec![80]),
                allow_all_unix_sockets: Some(true),
                ..Default::default()
            },
        );
        let captured = capture.0.lock().unwrap().clone().unwrap();
        assert!(captured.writable_roots.is_empty());
        assert_eq!(captured.exclude_cwd, Some(true));
        assert_eq!(captured.exclude_slash_tmp, Some(true));
        assert_eq!(captured.exclude_tmpdir_env_var, Some(true));
        assert_ne!(captured.unconfined_writes, Some(true));
        assert!(!captured.network_allowed);
        assert!(captured.allow_loopback_ports.is_none());
        assert!(captured.allow_all_unix_sockets.is_none());
    }
    #[test]
    fn read_only_filesystem_keeps_an_independent_network_grant() {
        let capture = Arc::new(Capture::default());
        let wrapper = RuntimePolicySandbox::new(
            capture.clone(),
            SandboxMode::ReadOnly,
            vec![],
            true,
            true,
            true,
        );
        wrapper.transform(&request(), &SandboxPolicy::default());
        let policy = capture.0.lock().unwrap().clone().unwrap();
        assert!(policy.filesystem_read_only);
        assert!(!policy.planning_read_only);
        assert!(policy.network_allowed);
        assert!(policy.writable_roots.is_empty());
    }

    #[test]
    fn workspace_temp_exclusions_survive_policy_projection() {
        let capture = Arc::new(Capture::default());
        let wrapper = RuntimePolicySandbox::new(
            capture.clone(),
            SandboxMode::WorkspaceWrite,
            vec!["/workspace".into()],
            true,
            true,
            true,
        );
        wrapper.transform(&request(), &SandboxPolicy::default());
        let policy = capture.0.lock().unwrap().clone().unwrap();
        assert_eq!(policy.exclude_tmpdir_env_var, Some(true));
        assert_eq!(policy.exclude_slash_tmp, Some(true));
        assert_eq!(policy.exclude_cwd, Some(true));
        assert!(policy.network_allowed);
    }
    #[test]
    fn a_captured_planning_or_network_ceiling_survives_runtime_projection() {
        let capture = Arc::new(Capture::default());
        let wrapper = RuntimePolicySandbox::new(
            capture.clone(),
            SandboxMode::WorkspaceWrite,
            vec!["/workspace".into()],
            true,
            false,
            false,
        );
        let mut planning = SandboxPolicy::default();
        planning.enforce_planning_read_only();
        wrapper.transform(&request(), &planning);
        let policy = capture.0.lock().unwrap().clone().unwrap();
        assert!(policy.writable_roots.is_empty());
        assert!(!policy.network_allowed);
        wrapper.transform(
            &request(),
            &SandboxPolicy {
                runtime_network_ceiling: Some(false),
                ..Default::default()
            },
        );
        let policy = capture.0.lock().unwrap().clone().unwrap();
        assert!(!policy.network_allowed);
        assert_eq!(policy.runtime_network_ceiling, Some(false));
    }
}
