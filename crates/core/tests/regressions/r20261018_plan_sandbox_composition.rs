//! Defect: task and managed-network wrappers could reopen a Plan shell's writes and network.
//! Root cause: inner sandbox transforms replaced the read-only policy or appended late grants.
//! Fixed for the 2026-10-18 Plan sandbox composition work item.

#![cfg(target_os = "macos")]

use klaus_agent_core::network_proxy::{
    ManagedNetworkSandboxBackend, NetworkProxy, NetworkProxyConfig,
};
use klaus_agent_core::permission::task_scope::CanonicalTaskResourceScope;
use klaus_agent_core::runtime_process::{
    RuntimeEnv, RuntimeProcessContext, RuntimeProcessSupervisor,
};
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};
use klaus_agent_core::sandbox::plan_readonly::PlanReadOnlySandbox;
use klaus_agent_core::sandbox::task_scope::TaskScopedSandbox;
use klaus_agent_core::sandbox::SeatbeltSandbox;
use klaus_agent_core::schemas::config::{TaskResourceMode, TaskResourceScope};
use klaus_agent_core::tools::exec::{run_shell, RunShellOptions, RunShellResult};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-plan-sandbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(std::fs::canonicalize(path).unwrap())
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn runtime_path(path: &Path) -> RuntimePath {
    RuntimePath::from(path.to_str().expect("owned fixture path is scalar"))
}

fn runtime_string(path: &Path) -> RuntimeString {
    runtime_path(path).into_runtime_string()
}

struct Fixture {
    directory: Directory,
    scratch: PathBuf,
    read: PathBuf,
    write: PathBuf,
    scope: Arc<CanonicalTaskResourceScope>,
}

impl Fixture {
    fn new() -> Self {
        let directory = Directory::new();
        let scratch = directory.0.join("scratch");
        let read = directory.0.join("read");
        let write = directory.0.join("write");
        for path in [&scratch, &read, &write] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(read.join("material.txt"), "plan-sandbox-material").unwrap();
        let scope = CanonicalTaskResourceScope::bind(
            &TaskResourceScope {
                task_id: "plan-sandbox-composition".into(),
                mode: TaskResourceMode::ScopedAuto,
                network_allowed: true,
                read_roots: vec![runtime_string(&read)],
                write_roots: vec![runtime_string(&write)],
            },
            &runtime_path(&scratch),
        )
        .unwrap();
        Self {
            directory,
            scratch,
            read,
            write,
            scope,
        }
    }

    fn request(&self) -> ExecRequest {
        ExecRequest {
            argv: vec![
                "/bin/cat".into(),
                runtime_string(&self.read.join("material.txt")),
            ],
            cwd: runtime_path(&self.scratch),
            env: Some(RuntimeEnv::new()),
            timeout_ms: Some(20_000),
        }
    }

    fn requested_policy(&self) -> SandboxPolicy {
        SandboxPolicy {
            writable_roots: vec![runtime_string(&self.write), runtime_string(&self.scratch)],
            readable_roots: Some(vec![
                runtime_string(&self.read),
                runtime_string(&self.write),
                runtime_string(&self.scratch),
            ]),
            network_allowed: true,
            allow_loopback_ports: Some(vec![9]),
            allow_unix_sockets: Some(vec![runtime_string(
                &self.directory.0.join("requested.sock"),
            )]),
            allow_all_unix_sockets: Some(true),
            unconfined_writes: Some(true),
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct CaptureSandbox(Mutex<Option<SandboxPolicy>>);

impl SandboxBackend for CaptureSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Seatbelt
    }

    fn is_available(&self) -> bool {
        true
    }

    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        *self.0.lock().unwrap() = Some(policy.clone());
        SpawnSpec {
            file: "/usr/bin/sandbox-exec".into(),
            args: vec!["-p".into(), "BASE".into()],
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}

#[derive(Clone, Copy)]
enum Wrapper {
    Plan,
    Task,
    Managed,
}

#[tokio::test]
async fn plan_read_only_survives_task_and_managed_network_wrapper_orders() {
    use Wrapper::{Managed, Plan, Task};

    let fixture = Fixture::new();
    let proxy = NetworkProxy::start(NetworkProxyConfig {
        enabled: true,
        proxy_url: Some("http://127.0.0.1:0".into()),
        socks_url: Some("socks5://127.0.0.1:0".into()),
        allow_unix_sockets: Some(vec![runtime_string(
            &fixture.directory.0.join("proxy.sock"),
        )]),
        allow_all_unix_sockets: true,
        allow_local_binding: Some(true),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_ne!(proxy.http_addr().port(), 0);
    assert_ne!(proxy.socks_addr().unwrap().port(), 0);

    // Each order is listed from the leaf outward, including outer managed-network wrappers.
    let orders: &[(&str, &[Wrapper])] = &[
        ("Plan(Task(leaf))", &[Task, Plan]),
        ("Plan(Task(Managed(leaf)))", &[Managed, Task, Plan]),
        ("Plan(Managed(Task(leaf)))", &[Task, Managed, Plan]),
        ("Managed(Plan(Task(leaf)))", &[Task, Plan, Managed]),
        ("Task(Plan(Managed(leaf)))", &[Managed, Plan, Task]),
        ("Managed(Task(Plan(leaf)))", &[Plan, Task, Managed]),
        ("Task(Managed(Plan(leaf)))", &[Plan, Managed, Task]),
    ];
    for (label, order) in orders {
        let capture = Arc::new(CaptureSandbox::default());
        let mut backend: Arc<dyn SandboxBackend> = capture.clone();
        for wrapper in *order {
            backend = match wrapper {
                Plan => Arc::new(PlanReadOnlySandbox::new(backend)),
                Task => Arc::new(
                    TaskScopedSandbox::new(backend, fixture.scope.clone())
                        .expect("the real trusted system DNS socket is required on macOS"),
                ),
                Managed => Arc::new(ManagedNetworkSandboxBackend::new(backend, proxy.clone())),
            };
        }
        let spawn = backend.transform(&fixture.request(), &fixture.requested_policy());
        let policy = capture.locked_policy();
        assert!(policy.writable_roots.is_empty(), "{label}: {policy:?}");
        assert!(!policy.network_allowed, "{label}: {policy:?}");
        assert_ne!(policy.unconfined_writes, Some(true), "{label}: {policy:?}");
        assert_eq!(policy.exclude_cwd, Some(true), "{label}: {policy:?}");
        assert_eq!(policy.exclude_slash_tmp, Some(true), "{label}: {policy:?}");
        assert_eq!(
            policy.exclude_tmpdir_env_var,
            Some(true),
            "{label}: {policy:?}"
        );
        assert!(
            policy
                .allow_loopback_ports
                .as_ref()
                .is_none_or(Vec::is_empty),
            "{label}: {policy:?}"
        );
        assert!(
            policy.allow_unix_sockets.as_ref().is_none_or(Vec::is_empty),
            "{label}: {policy:?}"
        );
        assert_ne!(
            policy.allow_all_unix_sockets,
            Some(true),
            "{label}: {policy:?}"
        );
        assert!(
            policy.windows_network_proxy_restricting_sid.is_none(),
            "{label}: {policy:?}"
        );
        assert_ne!(
            policy.windows_network_allow_local_binding,
            Some(true),
            "{label}: {policy:?}"
        );
        let reads = policy
            .readable_roots
            .as_ref()
            .expect("task reads remain scoped");
        for root in [&fixture.read, &fixture.write, &fixture.scratch] {
            assert!(
                reads.contains(&runtime_string(root)),
                "{label}: lost read root {root:?}: {policy:?}"
            );
        }
        assert!(
            !reads.contains(&RuntimeString::from("/")),
            "{label}: {policy:?}"
        );
        assert_eq!(
            spawn.args.first(),
            Some(&RuntimeString::from("-p")),
            "{label}"
        );
        let profile = spawn.args.get(1).expect("Seatbelt profile");
        assert!(profile.starts_with("BASE"), "{label}: {profile:?}");
        assert!(
            profile.contains("file-map-executable"),
            "{label}: {profile:?}"
        );
        for forbidden in [
            "mDNSResponder",
            "(allow network",
            "proxy.sock",
            "requested.sock",
        ] {
            assert!(
                !profile.contains(forbidden),
                "{label}: late grant {forbidden}: {profile:?}"
            );
        }
    }
}

impl CaptureSandbox {
    fn locked_policy(&self) -> SandboxPolicy {
        self.0
            .lock()
            .unwrap()
            .clone()
            .expect("actual final leaf policy")
    }
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "'\"'\"'"))
}

async fn shell(
    context: &RuntimeProcessContext,
    fixture: &Fixture,
    sandbox: &dyn SandboxBackend,
    command: String,
) -> RunShellResult {
    run_shell(RunShellOptions {
        process_context: context.clone(),
        command: command.into(),
        cwd: runtime_path(&fixture.scratch),
        sandbox,
        signal: AbortSignal::new(),
        timeout_ms: 20_000,
        max_output_bytes: 4096,
        env: None,
        network_allowed: Some(true),
        managed_network: None,
        writable_roots: Some(vec![runtime_string(&fixture.write)]),
        unconfined_writes: true,
        on_chunk: None,
    })
    .await
}

#[tokio::test]
async fn plan_task_scope_real_seatbelt_reads_owned_material_but_denies_task_writes() {
    let fixture = Fixture::new();
    let native = Arc::new(SeatbeltSandbox::new());
    assert_eq!(native.kind(), SandboxKind::Seatbelt);
    assert!(
        native.is_available(),
        "macOS must provide the real Seatbelt backend"
    );
    let task = Arc::new(
        TaskScopedSandbox::new(native, fixture.scope.clone())
            .expect("the real trusted system DNS socket is required on macOS"),
    );
    let plan = PlanReadOnlySandbox::new(task.clone());
    let supervisor = RuntimeProcessSupervisor::initialize_current().unwrap();
    let context = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let control = fixture.write.join("ordinary-task-control.txt");
    let forbidden_write = fixture.write.join("plan-forbidden.txt");
    let forbidden_scratch = fixture.scratch.join("plan-forbidden.txt");
    let control_result = shell(
        &context,
        &fixture,
        task.as_ref(),
        format!("printf ordinary-task-write > {}", shell_quote(&control)),
    )
    .await;
    let read_result = shell(
        &context,
        &fixture,
        &plan,
        format!(
            "/bin/cat {}",
            shell_quote(&fixture.read.join("material.txt"))
        ),
    )
    .await;
    let mut denied = Vec::new();
    for path in [&forbidden_write, &forbidden_scratch] {
        denied.push(
            shell(
                &context,
                &fixture,
                &plan,
                format!("printf forbidden > {}", shell_quote(path)),
            )
            .await,
        );
    }
    context.shutdown().await.unwrap();
    drop(context);
    supervisor.shutdown().await.unwrap();

    assert_eq!(control_result.exit_code, Some(0), "{control_result:?}");
    assert_eq!(
        std::fs::read_to_string(control).unwrap(),
        "ordinary-task-write"
    );
    assert_eq!(read_result.exit_code, Some(0), "{read_result:?}");
    assert_eq!(read_result.output, "plan-sandbox-material");
    for result in denied {
        assert!(!result.aborted && !result.timed_out, "{result:?}");
        assert!(result.exit_code.is_some_and(|code| code != 0), "{result:?}");
    }
    assert!(!forbidden_write.exists());
    assert!(!forbidden_scratch.exists());
    assert_eq!(
        std::fs::read_to_string(fixture.read.join("material.txt")).unwrap(),
        "plan-sandbox-material"
    );
}

#[test]
fn owner_task_scope_intersects_the_captured_runtime_network_ceiling() {
    let fixture = Fixture::new();
    let capture = Arc::new(CaptureSandbox::default());
    let scoped = TaskScopedSandbox::new(capture.clone(), fixture.scope.clone()).unwrap();
    let mut policy = fixture.requested_policy();
    policy.runtime_network_ceiling = Some(false);
    scoped.transform(&fixture.request(), &policy);
    let effective = capture.0.lock().unwrap().clone().unwrap();
    assert!(
        !effective.network_allowed,
        "an owner scope cannot reopen runtime-disabled network access"
    );
    policy.runtime_network_ceiling = Some(true);
    scoped.transform(&fixture.request(), &policy);
    assert!(capture.0.lock().unwrap().as_ref().unwrap().network_allowed);
}
