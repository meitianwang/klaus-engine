//! Mandatory filesystem policy for an owner-approved task.

use crate::permission::task_scope::CanonicalTaskResourceScope;
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::RuntimeString;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use std::sync::Arc;

#[cfg(target_os = "macos")]
fn system_dns_socket_is_trusted() -> bool {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let socket = std::path::Path::new("/private/var/run/mDNSResponder");
    crate::fs::node_path::canonicalize_host(socket).is_ok_and(|path| path == socket)
        && std::fs::symlink_metadata(socket)
            .is_ok_and(|metadata| metadata.uid() == 0 && metadata.file_type().is_socket())
}

pub struct TaskScopedSandbox {
    inner: Arc<dyn SandboxBackend>,
    scope: Arc<CanonicalTaskResourceScope>,
    runtime_programs: Vec<crate::runtime_values::RuntimePath>,
}

impl TaskScopedSandbox {
    pub fn new(
        inner: Arc<dyn SandboxBackend>,
        scope: Arc<CanonicalTaskResourceScope>,
    ) -> Result<Self, String> {
        if inner.kind() == SandboxKind::Process || !inner.is_available() {
            return Err("Task resource scope requires a real filesystem sandbox".into());
        }
        if scope.network_allowed && inner.kind() != SandboxKind::Seatbelt {
            return Err("Task command networking without localhost or credential sockets is not supported by this sandbox backend".into());
        }
        #[cfg(target_os = "macos")]
        if scope.network_allowed && !system_dns_socket_is_trusted() {
            return Err("Task networking requires the trusted system DNS socket".into());
        }
        Ok(Self {
            inner,
            scope,
            runtime_programs: Vec::new(),
        })
    }

    pub fn policy(&self, requested: &SandboxPolicy) -> SandboxPolicy {
        let mut readable_roots = self
            .scope
            .readable_roots()
            .into_iter()
            .map(|path| path.into_runtime_string())
            .collect::<Vec<_>>();
        readable_roots.extend(
            self.runtime_programs
                .iter()
                .map(|path| path.as_runtime_string().clone()),
        );
        // Shell executables and their loader dependencies are read-only runtime
        // infrastructure. No user home, config, credential or temp tree is added.
        #[cfg(unix)]
        for path in [
            "/bin",
            "/usr/bin",
            "/usr/lib",
            "/lib",
            "/lib64",
            "/System/Library",
            "/dev/null",
            "/dev/zero",
            "/dev/urandom",
        ] {
            readable_roots.push(RuntimeString::from(path));
        }
        #[cfg(target_os = "macos")]
        readable_roots.extend(
            ["/private/etc/ssl/cert.pem", "/private/etc/ssl/openssl.cnf"].map(RuntimeString::from),
        );
        let mut effective = SandboxPolicy {
            network_allowed: self.scope.network_allowed
                && requested.runtime_network_ceiling.unwrap_or(true),
            external_network_only: Some(true),
            allow_loopback_ports: None,
            allow_unix_sockets: None,
            allow_all_unix_sockets: Some(false),
            writable_roots: self
                .scope
                .writable_roots()
                .into_iter()
                .map(|path| path.into_runtime_string())
                .collect(),
            readable_roots: Some(readable_roots),
            exclude_cwd: Some(true),
            exclude_slash_tmp: Some(true),
            exclude_tmpdir_env_var: Some(true),
            include_platform_defaults: Some(false),
            unconfined_writes: Some(false),
            ..requested.clone()
        };
        if requested.filesystem_read_only {
            effective.enforce_filesystem_read_only();
        }
        if requested.planning_read_only || self.inner.enforces_planning_read_only() {
            effective.enforce_planning_read_only();
        }
        effective
    }
}

impl SandboxBackend for TaskScopedSandbox {
    fn enforces_planning_read_only(&self) -> bool {
        self.inner.enforces_planning_read_only()
    }

    fn task_resource_scope(&self) -> Option<Arc<CanonicalTaskResourceScope>> {
        Some(self.scope.clone())
    }
    fn task_runtime_program(
        &self,
        program: &crate::runtime_values::RuntimePath,
    ) -> Option<Arc<dyn SandboxBackend>> {
        if !cfg!(target_os = "macos") {
            return None;
        }
        let mut programs = self.runtime_programs.clone();
        programs.push(program.clone());
        Some(Arc::new(Self {
            inner: self.inner.clone(),
            scope: self.scope.clone(),
            runtime_programs: programs,
        }))
    }
    fn kind(&self) -> SandboxKind {
        self.inner.kind()
    }
    fn is_available(&self) -> bool {
        self.inner.is_available()
    }
    fn transform(&self, request: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        let policy = self.policy(policy);
        let trusted_network_runtime = {
            #[cfg(target_os = "macos")]
            {
                !policy.network_allowed || system_dns_socket_is_trusted()
            }
            #[cfg(not(target_os = "macos"))]
            {
                true
            }
        };
        if !cfg!(target_os = "macos")
            || self.scope.validate_roots().is_err()
            || !trusted_network_runtime
        {
            // An empty executable cannot be spawned. Do not pass the requested
            // command to a backend that might resolve a replaced root alias.
            return SpawnSpec {
                file: RuntimeString::default(),
                args: Vec::new(),
                cwd: request.cwd.clone(),
                env: RuntimeEnv::new(),
            };
        }
        let mut spawn = self.inner.transform(request, &policy);
        if self.inner.kind() == SandboxKind::Seatbelt
            && spawn.args.first().is_some_and(|arg| arg == "-p")
        {
            // Mapping a runtime binary is separate from data reads in Seatbelt.
            // Keep it restricted to roots already allowed by this same policy.
            let roots = crate::sandbox::policy::compute_readable_roots(&policy);
            let mapping = roots
                .iter()
                .enumerate()
                .map(|(index, _)| format!("(subpath (param \"READABLE_ROOT_{index}\"))"))
                .collect::<Vec<_>>()
                .join(" ");
            if let Some(profile) = spawn.args.get_mut(1) {
                *profile = profile.concat(&RuntimeString::from(format!("\n(allow file-map-executable {mapping})\n; Directory identity and standard symlink resolution, never temp/home subtrees.\n(allow file-read* file-test-existence (literal \"/\") (literal \"/private/var/select/sh\"))\n(allow file-read-metadata file-test-existence (literal \"/var\") (literal \"/tmp\") (literal \"/etc\") (literal \"/private\"))\n(deny user-preference-read)\n(deny appleevent-send)\n(deny mach-lookup)\n(allow mach-lookup (global-name \"com.apple.system.opendirectoryd.libinfo\") (global-name \"com.apple.system.opendirectoryd.membership\") (global-name \"com.apple.trustd.agent\") (global-name \"com.apple.SystemConfiguration.DNSConfiguration\") (global-name \"com.apple.SystemConfiguration.configd\"))")));
            }
            if policy.network_allowed {
                if let Some(profile) = spawn.args.get_mut(1) {
                    // libc DNS uses this root-owned system IPC endpoint. The
                    // broad Unix deny remains in force for credential agents.
                    *profile = profile.concat(&RuntimeString::from("\n(allow network-outbound (remote unix-socket (path \"/private/var/run/mDNSResponder\")))\n(allow file-read* file-write* (literal \"/private/var/run/mDNSResponder\"))"));
                }
            }
        }
        let mut environment = RuntimeEnv::new();
        for (key, value) in spawn.env.entries() {
            if key.try_to_utf8().is_ok_and(|key| {
                matches!(
                    key.as_str(),
                    "LANG"
                        | "LC_ALL"
                        | "LC_CTYPE"
                        | "TZ"
                        | "TERM"
                        | "SystemRoot"
                        | "windir"
                        | "PATHEXT"
                        | "COMSPEC"
                        | "ComSpec"
                )
            }) {
                environment.insert(key, value.clone());
            }
        }
        let scratch = self.scope.scratch_root().into_runtime_string();
        for key in [
            "HOME",
            "TMPDIR",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
        ] {
            environment.insert(RuntimeString::from(key), scratch.clone());
        }
        environment.insert(
            RuntimeString::from("PWD"),
            request.cwd.as_runtime_string().clone(),
        );
        environment.insert(
            RuntimeString::from("OPENSSL_CONF"),
            RuntimeString::from("/dev/null"),
        );
        #[cfg(unix)]
        environment.insert(
            RuntimeString::from("PATH"),
            RuntimeString::from("/usr/bin:/bin:/usr/sbin:/sbin"),
        );
        #[cfg(windows)]
        if let Some(system) = environment.get(&RuntimeString::from("SystemRoot")).cloned() {
            environment.insert(
                RuntimeString::from("PATH"),
                system
                    .concat(&RuntimeString::from("\\System32;"))
                    .concat(&system),
            );
        }
        spawn.env = environment;
        spawn
    }
}
