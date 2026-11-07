//! Windows sandbox backend: translate a policy into a sandbox request, and launch
//! the engine's own binary as the wrapper that enforces it.
//!
//! The mechanics live in `klaus-sandbox-win`; this file is the translation layer
//! between Klaus's `SandboxPolicy` and that crate's boundary types, plus the
//! plumbing that gets the request to the wrapper process without putting it on a
//! command line.

use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this process image dispatches the sandbox wrapper argv.
static WRAPPER_ENTRY_POINT: AtomicBool = AtomicBool::new(false);

/// Declare that this process re-enters itself as the sandbox wrapper.
///
/// This backend enforces its boundaries by re-executing `current_exe` with
/// [`klaus_sandbox_win::WRAPPER_ARG`], so it only works inside a binary that inspects its
/// own argv for that marker before doing anything else — which is what
/// `klaus_agent_server::windows_process_mode::dispatch` does for every engine entry point.
/// A host that embeds the engine without that dispatch (a unit-test binary, for one) would
/// re-exec something that rejects the marker as an unknown option, and then every sandboxed
/// command fails with an argument-parsing error that describes nothing about the command.
pub fn declare_wrapper_entry_point() {
    WRAPPER_ENTRY_POINT.store(true, Ordering::SeqCst);
}

/// Whether a wrapper entry point has been declared for this process.
pub fn wrapper_entry_point_installed() -> bool {
    WRAPPER_ENTRY_POINT.load(Ordering::SeqCst)
}

#[derive(Debug, Clone, Default)]
pub struct WindowsSandbox {
    #[cfg(windows)]
    native: Option<WindowsNativeContext>,
}

#[cfg(windows)]
#[derive(Debug, Clone)]
struct WindowsNativeContext {
    level: crate::schemas::config::WindowsSandboxMode,
    config_home: String,
}

impl WindowsSandbox {
    /// An explicitly scoped container used by internal filesystem ceilings.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_native(
        level: crate::schemas::config::WindowsSandboxMode,
        config_home: &crate::runtime_values::RuntimePath,
    ) -> Result<Self, String> {
        if level == crate::schemas::config::WindowsSandboxMode::Mxc {
            return Err("MXC execution requires the native process-security backend".into());
        }
        let config_home = crate::fs::node_path::project_runtime_path(
            config_home,
            "Windows sandbox configuration home",
        )
        .map_err(|error| format!("invalid Windows sandbox configuration home: {error:?}"))?;
        #[cfg(windows)]
        {
            Ok(Self {
                native: Some(WindowsNativeContext {
                    level,
                    config_home: config_home
                        .to_str()
                        .ok_or("Windows sandbox configuration home is not valid Unicode")?
                        .to_owned(),
                }),
            })
        }
        #[cfg(not(windows))]
        {
            Err(format!("Windows native {level:?} execution is unavailable on this platform (configuration home: {})", config_home.display()))
        }
    }
}

impl SandboxBackend for WindowsSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::WindowsRestrictedToken
    }

    fn is_available(&self) -> bool {
        cfg!(target_os = "windows") && wrapper_entry_point_installed()
    }

    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        #[cfg(windows)]
        {
            imp::transform(request, policy, self.native.as_ref())
        }
        #[cfg(not(windows))]
        {
            let _ = (request, policy);
            panic!("WindowsSandbox cannot transform commands off Windows");
        }
    }
}

/// Derive native preparation input from the same policy projection used at
/// launch, without materializing metadata paths. The filtered environment is
/// retained for actual setup; no target command is accepted.
#[cfg(windows)]
pub fn preparation_request(
    request: &ExecRequest,
    policy: &SandboxPolicy,
) -> klaus_sandbox_win::protocol::SandboxRequest {
    assert!(
        request.argv.is_empty(),
        "preparation inspection cannot accept a target command"
    );
    imp::resolved_request(request, policy, false).0
}

/// Take the pending sandbox request that belongs to a set of spawn arguments.
///
/// The spawner calls this immediately before creating the process: the request is
/// held here rather than in the argument list because it carries the target's
/// environment, and a Windows command line is readable by any process on the
/// machine.
#[cfg(windows)]
pub(crate) fn take_request_from_spawn_args(
    args: &mut Vec<crate::runtime_values::RuntimeString>,
) -> Option<Vec<u8>> {
    imp::take_request_from_spawn_args(args)
}

#[cfg(windows)]
pub(crate) fn resolved_native_request(
    request: &ExecRequest,
    policy: &SandboxPolicy,
) -> (
    klaus_sandbox_win::protocol::SandboxRequest,
    crate::runtime_process::RuntimeEnv,
) {
    imp::resolved_request(request, policy, false)
}

#[cfg(windows)]
mod imp {
    use std::collections::{BTreeMap, HashMap};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock, PoisonError};
    use std::time::{Duration, Instant};

    use super::WindowsNativeContext;
    use klaus_sandbox_win::boundary::{AbsPath, FilesystemBoundary, NetworkBoundary};
    use klaus_sandbox_win::protocol::{
        NativeSandboxContext, NativeSandboxLevel, NativeSandboxRequest, SandboxRequest,
    };

    use crate::fs::node_path::project_runtime_path;
    use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
    use crate::runtime_values::{RuntimePath, RuntimeString};
    use crate::sandbox::policy::has_unconfined_writes;
    use crate::sandbox::process::scrub_env;
    use crate::types::sandbox::{ExecRequest, SandboxPolicy, SpawnSpec};

    /// Repository metadata a command must not rewrite while editing the tree.
    const DEFAULT_PROTECTED_METADATA_NAMES: &[&str] = &[".git", ".agents", ".klaus"];

    /// Argument that stands in for the request until the spawner has a pipe.
    ///
    /// `transform` produces arguments but does not create the process, so it cannot
    /// know the handle the wrapper will read from. It leaves this token instead, and
    /// the spawner swaps it for the real handle once the pipe exists.
    const REQUEST_TOKEN_ARG: &str = "--klaus-internal-sandbox-request-token";
    /// A token whose spawn never happened is dropped after this long, so a cancelled
    /// launch cannot pin a request (and the environment inside it) in memory.
    const REQUEST_MAX_AGE: Duration = Duration::from_secs(300);
    const REQUEST_MAX_PENDING: usize = 1024;

    struct PendingRequest {
        created_at: Instant,
        payload: Vec<u8>,
    }

    static PENDING_REQUESTS: OnceLock<Mutex<HashMap<String, PendingRequest>>> = OnceLock::new();

    pub(super) fn transform(
        request: &ExecRequest,
        policy: &SandboxPolicy,
        native: Option<&WindowsNativeContext>,
    ) -> SpawnSpec {
        assert!(!request.argv.is_empty(), "WindowsSandbox: empty argv");
        let (sandbox_request, inner_env) = resolved_request(request, policy, native.is_none());
        let unconfined_writes = has_unconfined_writes(policy);
        let payload = serde_json::to_vec(&NativeSandboxRequest {
            request: sandbox_request,
            native: native.map(|context| NativeSandboxContext {
                level: match context.level {
                    crate::schemas::config::WindowsSandboxMode::Unelevated => {
                        NativeSandboxLevel::Unelevated
                    }
                    crate::schemas::config::WindowsSandboxMode::Elevated => {
                        NativeSandboxLevel::Elevated
                    }
                    crate::schemas::config::WindowsSandboxMode::Mxc => {
                        unreachable!("experimental execution uses its own backend")
                    }
                },
                config_home: context.config_home.clone(),
                unconfined_writes,
            }),
        })
        .expect("serialize Windows sandbox request");
        let executable = std::env::current_exe().expect("resolve Klaus engine executable");
        let mut args = vec![
            klaus_sandbox_win::WRAPPER_ARG.to_string(),
            REQUEST_TOKEN_ARG.to_string(),
            stash_request(payload),
        ];
        args.shrink_to_fit();

        SpawnSpec {
            file: native_text(&executable),
            args: args
                .into_iter()
                .map(|arg| RuntimeString::from_utf8(&arg))
                .collect(),
            cwd: request.cwd.clone(),
            env: inner_env,
        }
    }

    pub(super) fn resolved_request(
        request: &ExecRequest,
        policy: &SandboxPolicy,
        materialize_metadata: bool,
    ) -> (SandboxRequest, RuntimeEnv) {
        let cwd = native_path(&request.cwd, "Windows sandbox cwd");
        let host_env = ParentEnvSnapshot::capture();
        let empty = RuntimeEnv::new();
        let inner_env = scrub_env(&host_env, request.env.as_ref().unwrap_or(&empty));
        let env_map = runtime_env_map(&inner_env);

        // Unconfined writes empty the protected-metadata list, so the derivations
        // below collapse on their own and only an explicit carve-out survives.
        let unconfined_writes = has_unconfined_writes(policy);

        let writable = absolute_paths(
            &crate::sandbox::policy::compute_writable_roots(
                policy,
                &request.cwd,
                "windows",
                &host_env,
            ),
            "Windows writable root",
        );
        let readable = policy
            .readable_roots
            .as_ref()
            .map(|roots| absolute_paths(roots, "Windows readable root"));

        let protected_names = protected_metadata_names(policy);
        // Created before the ACEs are written: a deny ACE can only be attached to a
        // path that exists, so a `.git` that is not there yet would be unprotected
        // for exactly as long as it takes the command to create it.
        if materialize_metadata {
            materialize_protected_metadata_paths(&protected_names, &writable);
        }

        let mut deny_write = absolute_paths(
            policy.read_only_subpaths.as_deref().unwrap_or_default(),
            "Windows read-only path",
        );
        let metadata_write_allow = metadata_write_overrides(policy, &writable, &deny_write);

        let mut deny_read = absolute_paths(
            policy.deny_read_subpaths.as_deref().unwrap_or_default(),
            "Windows deny-read path",
        );
        deny_write.sort();
        deny_write.dedup();
        deny_read.sort();
        deny_read.dedup();

        let filesystem = FilesystemBoundary {
            readable: readable.as_ref().map(|roots| to_abs_paths(roots)),
            writable: to_abs_paths(&writable),
            deny_write: to_abs_paths(&deny_write),
            deny_read: to_abs_paths(&deny_read),
            protected_metadata: if unconfined_writes {
                Vec::new()
            } else {
                protected_names.clone()
            },
            metadata_write_allow: to_abs_paths(&metadata_write_allow),
        };

        let network = network_boundary(policy);

        let sandbox_request = SandboxRequest {
            command: request.argv.iter().map(runtime_text).collect(),
            cwd: to_abs_path(&cwd),
            env: env_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            filesystem,
            network,
            // Only a derived custom capability belongs in SECURITY_CAPABILITIES.
            // Legacy restricting identities must never become capability grants.
            network_route_capability_sid: policy
                .windows_network_proxy_restricting_sid
                .as_ref()
                .map(runtime_text)
                .filter(|sid| {
                    klaus_sandbox_win::capability::validate_route_capability_sid(sid).is_ok()
                }),
            // Each execution owns one profile. Inspection borrows this serialized
            // identity; the eventual launch consumes it without generating another.
            container_name: format!("klaus-exec-{}", uuid::Uuid::new_v4().simple()),
            // Filled in by the spawner when the host has a terminal; `transform` does
            // not know whether one is attached.
            terminal: None,
        };

        (sandbox_request, inner_env)
    }

    /// How the policy's network settings map onto the sandbox's three shapes.
    ///
    /// A restricting SID in the policy means "traffic must go through the local
    /// proxy", which is the loopback-ports shape. Without one, the policy's plain
    /// allow/deny decides.
    fn network_boundary(policy: &SandboxPolicy) -> NetworkBoundary {
        if policy.planning_read_only {
            return NetworkBoundary::Denied;
        }
        let proxy_enforced = policy.windows_network_proxy_restricting_sid.is_some();
        if proxy_enforced {
            let ports = policy
                .allow_loopback_ports
                .as_deref()
                .unwrap_or_default()
                .to_vec();
            return NetworkBoundary::LoopbackPorts {
                ports,
                allow_local_binding: policy.windows_network_allow_local_binding.unwrap_or(false),
            };
        }
        if policy.network_allowed {
            return NetworkBoundary::Allowed;
        }
        // Guard ports are the shape a policy uses to keep a local port reachable
        // while the rest of the network is closed.
        let guard_ports = policy
            .windows_network_proxy_guard_ports
            .as_deref()
            .unwrap_or_default();
        if guard_ports.is_empty() {
            NetworkBoundary::Denied
        } else {
            NetworkBoundary::LoopbackPorts {
                ports: guard_ports.to_vec(),
                allow_local_binding: policy.windows_network_allow_local_binding.unwrap_or(false),
            }
        }
    }

    fn pending_requests() -> &'static Mutex<HashMap<String, PendingRequest>> {
        PENDING_REQUESTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Park a serialized request and return the token that stands for it.
    fn stash_request(payload: Vec<u8>) -> String {
        let now = Instant::now();
        let mut pending = pending_requests()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Requests whose spawn never happened are dropped: each one holds a copy of
        // the target's environment, so keeping them is keeping secrets alive.
        pending.retain(|_, entry| now.duration_since(entry.created_at) <= REQUEST_MAX_AGE);
        assert!(
            pending.len() < REQUEST_MAX_PENDING,
            "too many pending Windows sandbox requests"
        );
        let token = uuid::Uuid::new_v4().to_string();
        assert!(
            pending
                .insert(
                    token.clone(),
                    PendingRequest {
                        created_at: now,
                        payload,
                    },
                )
                .is_none(),
            "Windows sandbox request token collision"
        );
        token
    }

    /// Remove the token from `args` and return the request it stood for.
    pub(super) fn take_request_from_spawn_args(args: &mut Vec<RuntimeString>) -> Option<Vec<u8>> {
        let marker = args
            .iter()
            .position(|arg| arg.try_to_utf8().is_ok_and(|arg| arg == REQUEST_TOKEN_ARG))?;
        // The token must be followed by its value; a lone marker means the argument
        // list was built wrong, and launching without the request would run the
        // command with no boundaries at all.
        assert!(
            marker + 1 < args.len(),
            "Windows sandbox request token is missing its value"
        );
        let token = args
            .get(marker + 1)
            .and_then(|arg| arg.try_to_utf8().ok())?;
        args.drain(marker..=marker + 1);
        let mut pending = pending_requests()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pending.remove(&token).map(|entry| entry.payload)
    }

    #[cfg(test)]
    pub(super) fn inspect_request_from_spawn_args(args: &[RuntimeString]) -> Option<Vec<u8>> {
        let marker = args
            .iter()
            .position(|arg| arg.try_to_utf8().is_ok_and(|arg| arg == REQUEST_TOKEN_ARG))?;
        let token = args.get(marker + 1)?.try_to_utf8().ok()?;
        let pending = pending_requests()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pending.get(&token).map(|entry| entry.payload.clone())
    }

    #[cfg(test)]
    pub(super) fn discard_request_from_spawn_args(args: &[RuntimeString]) {
        let Some(marker) = args
            .iter()
            .position(|arg| arg.try_to_utf8().is_ok_and(|arg| arg == REQUEST_TOKEN_ARG))
        else {
            return;
        };
        let Some(token) = args.get(marker + 1).and_then(|arg| arg.try_to_utf8().ok()) else {
            return;
        };
        pending_requests()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&token);
    }

    fn protected_metadata_names(policy: &SandboxPolicy) -> Vec<String> {
        if has_unconfined_writes(policy) {
            return Vec::new();
        }
        policy
            .protected_metadata_names
            .as_ref()
            .map(|names| names.iter().map(protected_metadata_basename).collect())
            .unwrap_or_else(|| {
                DEFAULT_PROTECTED_METADATA_NAMES
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect()
            })
    }

    /// Create the protected metadata directories that do not exist yet.
    ///
    /// A deny ACE needs a path to attach to. Without this, a `.git` the command
    /// creates itself would be unprotected, because the deny was never written.
    fn materialize_protected_metadata_paths(names: &[String], writable: &[PathBuf]) {
        for root in writable {
            for name in names {
                let path = root.join(name);
                if !path.exists() {
                    std::fs::create_dir_all(&path).unwrap_or_else(|error| {
                        panic!(
                            "failed to materialize protected Windows metadata path {}: {error}",
                            path.display()
                        )
                    });
                }
            }
        }
    }

    /// Default-protected names the policy chose NOT to protect.
    ///
    /// A policy that names its own protected list is opting the others back in as
    /// writable, and those need an explicit exemption because the boundary's
    /// protected-name check is by name at any depth.
    fn metadata_write_overrides(
        policy: &SandboxPolicy,
        writable: &[PathBuf],
        deny_write: &[PathBuf],
    ) -> Vec<PathBuf> {
        let Some(configured) = policy.protected_metadata_names.as_ref() else {
            return Vec::new();
        };
        let configured = configured
            .iter()
            .map(protected_metadata_basename)
            .collect::<Vec<_>>();
        let mut paths = Vec::new();
        for root in writable {
            for name in DEFAULT_PROTECTED_METADATA_NAMES {
                let path = root.join(name);
                let explicitly_denied = deny_write.iter().any(|denied| {
                    denied
                        .as_os_str()
                        .to_string_lossy()
                        .eq_ignore_ascii_case(&path.as_os_str().to_string_lossy())
                });
                if !explicitly_denied
                    && !configured
                        .iter()
                        .any(|chosen| chosen.eq_ignore_ascii_case(name))
                {
                    push_unique(&mut paths, path);
                }
            }
        }
        paths
    }

    fn absolute_paths(values: &[RuntimeString], label: &str) -> Vec<PathBuf> {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let path = PathBuf::from(runtime_text(value));
            assert!(path.is_absolute(), "{label} must be absolute: {path:?}");
            push_unique(&mut out, path);
        }
        out
    }

    fn to_abs_paths(paths: &[PathBuf]) -> Vec<AbsPath> {
        paths.iter().map(|path| to_abs_path(path)).collect()
    }

    fn to_abs_path(path: &Path) -> AbsPath {
        AbsPath::new(path.to_string_lossy().as_ref())
            .unwrap_or_else(|error| panic!("Windows sandbox path {path:?} is unusable: {error}"))
    }

    fn native_path(path: &RuntimePath, label: &str) -> PathBuf {
        let native = project_runtime_path(path, label)
            .unwrap_or_else(|error| panic!("{label} is not a usable path: {error:?}"));
        assert!(native.is_absolute(), "{label} must be absolute: {native:?}");
        native
    }

    fn native_text(path: &Path) -> RuntimeString {
        RuntimeString::from_utf8(&path.to_string_lossy())
    }

    fn runtime_text(value: &RuntimeString) -> String {
        value
            .try_to_utf8()
            .unwrap_or_else(|_| panic!("Windows sandbox argument is not valid UTF-8"))
    }

    fn protected_metadata_basename(value: &RuntimeString) -> String {
        let text = runtime_text(value);
        let name = Path::new(&text)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or(text);
        assert!(
            !name.is_empty(),
            "protected metadata name must not be empty"
        );
        name
    }

    fn runtime_env_map(env: &RuntimeEnv) -> HashMap<String, String> {
        env.entries()
            .map(|(key, value)| (runtime_text(&key), runtime_text(value)))
            .collect()
    }

    fn push_unique(paths: &mut Vec<PathBuf>, path: PathBuf) {
        let already = paths.iter().any(|existing| {
            existing
                .as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case(&path.as_os_str().to_string_lossy())
        });
        if !already {
            paths.push(path);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_managed_route_capability_reaches_the_private_launch_request() {
            let capability = "S-1-15-3-1024-1-2-3-4-5-6-7-8";
            let request = ExecRequest {
                argv: vec!["DO_NOT_EXECUTE_SERIALIZATION_FIXTURE".into()],
                cwd: r"C:\klaus-route-serialization".into(),
                env: None,
                timeout_ms: None,
            };
            let mut policy = SandboxPolicy {
                // No filesystem path is materialized by this serialization case.
                protected_metadata_names: Some(Vec::new()),
                allow_loopback_ports: Some(vec![29433]),
                windows_network_proxy_restricting_sid: Some(capability.into()),
                ..Default::default()
            };
            for expected in [Some(capability), None] {
                let mut spawn = transform(&request, &policy, None);
                let payload = take_request_from_spawn_args(&mut spawn.args)
                    .expect("transform must stage the private request");
                let decoded: SandboxRequest = serde_json::from_slice(&payload).unwrap();
                assert_eq!(decoded.network_route_capability_sid.as_deref(), expected);
                klaus_sandbox_win::capability::validate_request_capabilities(&decoded)
                    .expect("launch capability and managed boundary must agree");
                // A legacy restricting SID is never promoted into capabilities.
                policy.windows_network_proxy_restricting_sid = Some("S-1-5-21-1-2-3-4".into());
            }
        }

        #[test]
        fn preparation_inspection_preserves_the_exact_pending_spawn_payload() {
            let payload = br#"{"containerName":"fixture","network":{"kind":"denied"}}"#.to_vec();
            let token = stash_request(payload.clone());
            let mut args = vec![
                RuntimeString::from(klaus_sandbox_win::WRAPPER_ARG),
                RuntimeString::from(REQUEST_TOKEN_ARG),
                RuntimeString::from(token),
            ];
            assert_eq!(
                inspect_request_from_spawn_args(&args),
                Some(payload.clone())
            );
            assert_eq!(
                inspect_request_from_spawn_args(&args),
                Some(payload.clone())
            );
            assert_eq!(take_request_from_spawn_args(&mut args), Some(payload));
            assert_eq!(
                args,
                vec![RuntimeString::from(klaus_sandbox_win::WRAPPER_ARG)]
            );
            assert!(inspect_request_from_spawn_args(&args).is_none());
            assert!(take_request_from_spawn_args(&mut args).is_none());
        }

        #[test]
        fn preparation_inspection_rejects_missing_or_unowned_pending_tokens() {
            assert!(inspect_request_from_spawn_args(&[]).is_none());
            assert!(
                inspect_request_from_spawn_args(&[RuntimeString::from(REQUEST_TOKEN_ARG)])
                    .is_none()
            );
            assert!(inspect_request_from_spawn_args(&[
                RuntimeString::from(REQUEST_TOKEN_ARG),
                RuntimeString::from(uuid::Uuid::new_v4().to_string()),
            ])
            .is_none());
        }

        #[test]
        fn cancelled_preparation_discards_only_its_valid_pending_request() {
            discard_request_from_spawn_args(&[RuntimeString::from(REQUEST_TOKEN_ARG)]);
            let payload = b"private pending fixture".to_vec();
            let token = stash_request(payload.clone());
            let args = vec![
                RuntimeString::from(REQUEST_TOKEN_ARG),
                RuntimeString::from(token),
            ];
            assert_eq!(inspect_request_from_spawn_args(&args), Some(payload));
            discard_request_from_spawn_args(&args);
            discard_request_from_spawn_args(&args);
            assert!(inspect_request_from_spawn_args(&args).is_none());
        }

        #[test]
        fn each_execution_keeps_one_fresh_identity_through_inspection_and_spawn() {
            let policy = SandboxPolicy {
                // This serialization fixture must not create metadata directories.
                protected_metadata_names: Some(Vec::new()),
                ..Default::default()
            };
            let mut identities = std::collections::HashSet::new();
            for cwd in [r"C:\ws\same", r"C:\ws\same", r"C:\ws\a-b", r"C:\ws\a b"] {
                let request = ExecRequest {
                    argv: vec!["DO_NOT_EXECUTE_IDENTITY_FIXTURE".into()],
                    cwd: cwd.into(),
                    env: None,
                    timeout_ms: None,
                };
                let mut spawn = transform(&request, &policy, None);
                let first = inspect_request_from_spawn_args(&spawn.args);
                let second = inspect_request_from_spawn_args(&spawn.args);
                // Consume before any assertion, including an invalid identity.
                let consumed = take_request_from_spawn_args(&mut spawn.args);
                assert_eq!(first, second);
                assert_eq!(first, consumed);
                let decoded: SandboxRequest =
                    serde_json::from_slice(&consumed.expect("staged request")).unwrap();
                let suffix = decoded.container_name.strip_prefix("klaus-exec-").unwrap();
                assert_eq!(suffix.len(), 32);
                assert!(suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
                assert!(identities.insert(decoded.container_name));
                assert!(inspect_request_from_spawn_args(&spawn.args).is_none());
            }
        }
    }
}

#[cfg(test)]
mod wrapper_entry_point_tests {
    use super::*;

    /// The gate that keeps a host without a wrapper entry point from selecting this
    /// backend.
    ///
    /// Only the negative direction is asserted, and deliberately: the flag is
    /// process-global, so declaring one here would make the backend available to every
    /// other case in this binary and put them back on a re-exec that rejects the wrapper
    /// marker. The positive direction is what `klaus doctor` exercises end to end in the
    /// real engine binary, which is the only place it can be true.
    #[test]
    fn a_host_without_a_declared_entry_point_cannot_use_the_backend() {
        assert!(
            !wrapper_entry_point_installed(),
            "a test binary never dispatches the wrapper argv, so nothing may declare it"
        );
        assert!(!WindowsSandbox::new().is_available());
    }
}
