//! Sandbox policy + backend contract.

use serde::{Deserialize, Serialize};

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath, RuntimeString,
};

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// What the sandbox is allowed to expose to a spawned command.
///
/// The three core fields (`writable_roots`, `readable_roots`,
/// `network_allowed`) are the stable contract consumed by `run_shell`. The
/// remaining fields are optional refinements that extend the core policy
/// model; backends apply sensible defaults when they are omitted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxPolicy {
    /// Filesystem subtrees the command may write to. Backends additionally grant
    /// the standard temp dirs and the cwd unless suppressed, following the
    /// workspace-write defaults.
    pub writable_roots: Vec<RuntimeString>,
    /// Filesystem subtrees the command may read. When `None`, backends grant
    /// read access to the whole filesystem (`/`); when present, reads are scoped
    /// to these roots (deny-by-default elsewhere).
    pub readable_roots: Option<Vec<RuntimeString>>,
    /// Whether the command may reach the network.
    pub network_allowed: bool,
    /// Native-only runtime ceiling retained across owner-scope projections.
    /// Public policy decoding cannot mint this capture.
    #[doc(hidden)]
    pub runtime_network_ceiling: Option<bool>,
    /// Outbound TCP and DNS only; localhost and Unix sockets remain denied.
    /// This does not distinguish other LAN addresses from Internet addresses.
    pub external_network_only: Option<bool>,
    /// Subpaths under a writable root that must stay read-only. Used to carve
    /// protected paths (e.g. `.git`) out of an otherwise-writable workspace.
    pub read_only_subpaths: Option<Vec<RuntimeString>>,
    /// Workspace metadata directory names kept read-only under EVERY writable
    /// root, by name — so they are protected even before they exist. Pass `[]`
    /// to disable. Omitting it takes a platform-dependent default: the posix
    /// backends use `['.git', '.agents']`, while the Windows backend also
    /// protects `'.klaus'`, because a writable root there can itself sit under
    /// a `.klaus/worktrees/` checkpoint directory.
    pub protected_metadata_names: Option<Vec<RuntimeString>>,
    /// Absolute paths whose reads are always denied, layered on top of the read
    /// allow as last-wins rules. This is the caller's own instruction: the engine
    /// adds no credential deny-list of its own, because subtracting `~/.ssh` from
    /// a broad read grant breaks the user's `git push` / `ssh` without stopping
    /// exfiltration (agent file tools do not go through this sandbox).
    pub deny_read_subpaths: Option<Vec<RuntimeString>>,
    /// Include the macOS read-only platform-defaults profile so exec still works
    /// when reads are constrained to `readable_roots`. Defaults to `false`.
    pub include_platform_defaults: Option<bool>,
    /// Drop the per-user `$TMPDIR` from the default writable set.
    pub exclude_tmpdir_env_var: Option<bool>,
    /// Drop `/tmp` from the default writable set.
    pub exclude_slash_tmp: Option<bool>,
    /// Drop the command's own cwd from the default writable set. The backends
    /// always grant the cwd unless suppressed; plan-mode read-only enforcement
    /// suppresses it so a shell command cannot write the workspace either
    /// ("read-only until the plan is approved" has no shell bypass).
    pub exclude_cwd: Option<bool>,
    /// When `network_allowed` is false, still permit loopback traffic on these
    /// TCP ports (used to reach a local proxy).
    pub allow_loopback_ports: Option<Vec<u16>>,
    /// Unix socket paths to permit while network policy is otherwise restricted.
    pub allow_unix_sockets: Option<Vec<RuntimeString>>,
    /// Permit every Unix socket. Intended for tightly controlled host setups.
    pub allow_all_unix_sockets: Option<bool>,
    /// Waive filesystem write confinement entirely: every path is writable and no
    /// metadata directory or read-only carve-out is protected. Set by the tool
    /// layer under `bypassPermissions`, whose product copy is "skip all permission
    /// prompts" — a sandbox refusal is not a prompt, but it breaks the same
    /// promise. Reads and the network keep their own policy.
    pub unconfined_writes: Option<bool>,
    /// Native-only planning ceiling propagated through policy decorators.
    /// It cannot be minted or persisted through the public policy codec.
    #[doc(hidden)]
    pub planning_read_only: bool,
    /// Native-only filesystem ceiling, independent of the network policy.
    #[doc(hidden)]
    pub filesystem_read_only: bool,
    /// Windows-only route SID supplied by the trusted broker. Session-owned
    /// AppContainer routes use a custom capability; the field name is retained
    /// for callers that borrow an external restricting-token route.
    ///
    /// This field is intentionally omitted from the public runtime codec. Only
    /// the in-process managed-network wrapper may populate it.
    #[doc(hidden)]
    pub windows_network_proxy_restricting_sid: Option<RuntimeString>,
    /// Windows-only trusted local-binding mode paired with the broker route.
    #[doc(hidden)]
    pub windows_network_allow_local_binding: Option<bool>,
    /// Windows-only listener ports held by the trusted Host broker while
    /// managed networking is disabled. These ports are retained in setup
    /// policy but no route SID or proxy environment is exposed to the child.
    #[doc(hidden)]
    pub windows_network_proxy_guard_ports: Option<Vec<u16>>,
}

impl SandboxPolicy {
    /// Apply after every grant projection so a decorator cannot reopen effects.
    pub(crate) fn enforce_planning_read_only(&mut self) {
        self.planning_read_only = true;
        self.enforce_filesystem_read_only();
        self.enforce_no_network();
    }

    pub(crate) fn enforce_filesystem_read_only(&mut self) {
        self.filesystem_read_only = true;
        self.writable_roots.clear();
        self.exclude_tmpdir_env_var = Some(true);
        self.exclude_slash_tmp = Some(true);
        self.exclude_cwd = Some(true);
        self.unconfined_writes = None;
    }

    pub(crate) fn enforce_no_network(&mut self) {
        self.network_allowed = false;
        self.allow_loopback_ports = None;
        self.allow_unix_sockets = None;
        self.allow_all_unix_sockets = None;
        self.windows_network_proxy_restricting_sid = None;
        self.windows_network_allow_local_binding = None;
        self.windows_network_proxy_guard_ports = None;
    }
}

impl RuntimeEncode for SandboxPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("writableRoots", &self.writable_roots)?;
        if let Some(value) = &self.readable_roots {
            object.field("readableRoots", value)?;
        }
        object.field("networkAllowed", &self.network_allowed)?;
        if let Some(value) = &self.external_network_only {
            object.field("externalNetworkOnly", value)?;
        }
        if let Some(value) = &self.read_only_subpaths {
            object.field("readOnlySubpaths", value)?;
        }
        if let Some(value) = &self.protected_metadata_names {
            object.field("protectedMetadataNames", value)?;
        }
        if let Some(value) = &self.deny_read_subpaths {
            object.field("denyReadSubpaths", value)?;
        }
        if let Some(value) = &self.include_platform_defaults {
            object.field("includePlatformDefaults", value)?;
        }
        if let Some(value) = &self.exclude_tmpdir_env_var {
            object.field("excludeTmpdirEnvVar", value)?;
        }
        if let Some(value) = &self.exclude_slash_tmp {
            object.field("excludeSlashTmp", value)?;
        }
        if let Some(value) = &self.exclude_cwd {
            object.field("excludeCwd", value)?;
        }
        if let Some(value) = &self.allow_loopback_ports {
            object.field("allowLoopbackPorts", value)?;
        }
        if let Some(value) = &self.allow_unix_sockets {
            object.field("allowUnixSockets", value)?;
        }
        if let Some(value) = &self.allow_all_unix_sockets {
            object.field("allowAllUnixSockets", value)?;
        }
        if let Some(value) = &self.unconfined_writes {
            object.field("unconfinedWrites", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SandboxPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                writable_roots: object.required("writableRoots", &[])?,
                readable_roots: object.optional("readableRoots", &[])?,
                network_allowed: object.required("networkAllowed", &[])?,
                runtime_network_ceiling: None,
                external_network_only: object.optional("externalNetworkOnly", &[])?,
                read_only_subpaths: object.optional("readOnlySubpaths", &[])?,
                protected_metadata_names: object.optional("protectedMetadataNames", &[])?,
                deny_read_subpaths: object.optional("denyReadSubpaths", &[])?,
                include_platform_defaults: object.optional("includePlatformDefaults", &[])?,
                exclude_tmpdir_env_var: object.optional("excludeTmpdirEnvVar", &[])?,
                exclude_slash_tmp: object.optional("excludeSlashTmp", &[])?,
                exclude_cwd: object.optional("excludeCwd", &[])?,
                allow_loopback_ports: object.optional("allowLoopbackPorts", &[])?,
                allow_unix_sockets: object.optional("allowUnixSockets", &[])?,
                allow_all_unix_sockets: object.optional("allowAllUnixSockets", &[])?,
                unconfined_writes: object.optional("unconfinedWrites", &[])?,
                planning_read_only: false,
                filesystem_read_only: false,
                windows_network_proxy_restricting_sid: None,
                windows_network_allow_local_binding: None,
                windows_network_proxy_guard_ports: None,
            })
        })
    }
}

/// A command to run, before sandbox transformation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecRequest {
    pub argv: Vec<RuntimeString>,
    pub cwd: RuntimePath,
    pub env: Option<RuntimeEnv>,
    pub timeout_ms: Option<u64>,
}

/// The concrete spawn instruction a backend produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnSpec {
    pub file: RuntimeString,
    pub args: Vec<RuntimeString>,
    pub cwd: RuntimePath,
    pub env: RuntimeEnv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxKind {
    Process,
    Seatbelt,
    Bubblewrap,
    NativeHelper,
    WindowsRestrictedToken,
    WindowsMxc,
}

impl RuntimeEncode for SandboxKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Process => "process",
            Self::Seatbelt => "seatbelt",
            Self::Bubblewrap => "bubblewrap",
            Self::NativeHelper => "native-helper",
            Self::WindowsRestrictedToken => "windows-restricted-token",
            Self::WindowsMxc => "windows-mxc",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SandboxKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("process", &[]),
                RuntimeEnumVariant::new("seatbelt", &[]),
                RuntimeEnumVariant::new("bubblewrap", &[]),
                RuntimeEnumVariant::new("native-helper", &[]),
                RuntimeEnumVariant::new("windows-restricted-token", &[]),
                RuntimeEnumVariant::new("windows-mxc", &[]),
            ],
            &[
                Self::Process,
                Self::Seatbelt,
                Self::Bubblewrap,
                Self::NativeHelper,
                Self::WindowsRestrictedToken,
                Self::WindowsMxc,
            ],
        )
    }
}

/// Wraps a command for execution under a platform isolation mechanism. The
/// `process` backend only scrubs env + manages the process group — it is NOT
/// filesystem isolation (see docs/security-model.md).
pub trait SandboxBackend: Send + Sync {
    fn task_resource_scope(
        &self,
    ) -> Option<std::sync::Arc<crate::permission::task_scope::CanonicalTaskResourceScope>> {
        None
    }
    fn task_runtime_program(
        &self,
        _program: &RuntimePath,
    ) -> Option<std::sync::Arc<dyn SandboxBackend>> {
        None
    }
    /// Native decorator identity survives projections regardless of nesting order.
    fn enforces_planning_read_only(&self) -> bool {
        false
    }
    fn kind(&self) -> SandboxKind;
    /// Runtime probe — e.g. whether `sandbox-exec`/`bwrap` exists.
    fn is_available(&self) -> bool;
    fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec;
}
