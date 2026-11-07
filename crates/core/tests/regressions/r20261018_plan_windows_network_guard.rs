//! Defect: a disabled-network broker guard could reopen loopback ports during planning.
//! Root cause: guard-port projection survived the shell's read-only sandbox decorator.
//! Work item: 20261018 planning network ceilings across native broker guards.

#![cfg(windows)]

use klaus_agent_core::network_proxy::{
    TrustedWindowsSandboxContext, WindowsNetworkGuardSandboxBackend,
};
use klaus_agent_core::runtime_values::RuntimePath;
use klaus_agent_core::sandbox::plan_readonly::PlanReadOnlySandbox;
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Capture(Mutex<Option<SandboxPolicy>>);
impl SandboxBackend for Capture {
    fn kind(&self) -> SandboxKind {
        SandboxKind::WindowsRestrictedToken
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        *self.0.lock().unwrap() = Some(policy.clone());
        SpawnSpec {
            file: request.argv[0].clone(),
            args: Vec::new(),
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}

fn guard(inner: Arc<dyn SandboxBackend>) -> Arc<dyn SandboxBackend> {
    Arc::new(
        WindowsNetworkGuardSandboxBackend::try_new(
            inner,
            TrustedWindowsSandboxContext {
                route_sid: Default::default(),
                http_addr: ([127, 0, 0, 1], 3128).into(),
                socks_addr: Some(([127, 0, 0, 1], 8081).into()),
                broker_generation: "planning-network-fixture".into(),
                policy_digest: Default::default(),
            },
        )
        .unwrap(),
    )
}

#[test]
fn planning_removes_broker_guard_ports_in_both_decorator_orders() {
    let request = ExecRequest {
        argv: vec!["cmd.exe".into()],
        cwd: RuntimePath::from(r"C:\workspace"),
        env: None,
        timeout_ms: None,
    };
    let capture = Arc::new(Capture::default());
    guard(capture.clone()).transform(&request, &SandboxPolicy::default());
    assert_eq!(
        capture
            .0
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .windows_network_proxy_guard_ports,
        Some(vec![3128, 8081]),
        "ordinary broker guards retain their listener policy"
    );

    for plan_outer in [true, false] {
        let backend: Arc<dyn SandboxBackend> = if plan_outer {
            Arc::new(PlanReadOnlySandbox::new(guard(capture.clone())))
        } else {
            guard(Arc::new(PlanReadOnlySandbox::new(capture.clone())))
        };
        backend.transform(
            &request,
            &SandboxPolicy {
                network_allowed: true,
                writable_roots: vec!["C:\\workspace".into()],
                unconfined_writes: Some(true),
                ..Default::default()
            },
        );
        let policy = capture.0.lock().unwrap().clone().unwrap();
        assert!(policy.writable_roots.is_empty(), "{policy:?}");
        assert!(!policy.network_allowed, "{policy:?}");
        assert!(
            policy
                .windows_network_proxy_guard_ports
                .as_ref()
                .is_none_or(Vec::is_empty),
            "{policy:?}"
        );
        assert!(
            policy
                .allow_loopback_ports
                .as_ref()
                .is_none_or(Vec::is_empty),
            "{policy:?}"
        );
        assert!(
            policy.windows_network_proxy_restricting_sid.is_none(),
            "{policy:?}"
        );
        assert_ne!(
            policy.windows_network_allow_local_binding,
            Some(true),
            "{policy:?}"
        );
    }
}
