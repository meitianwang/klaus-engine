//! macOS `sandbox-exec` (Seatbelt) backend.
//!
//! Generates a complete, deny-by-default Seatbelt (`.sb`) profile from the
//! policy. The profile:
//!
//!   - starts closed (`(deny default)`),
//!   - allows `process-exec`/`process-fork` so the command and its children run,
//!   - grants `file-read*` across the whole filesystem (or the policy's
//!     `readable_roots`),
//!   - grants `file-write*` only under the writable roots (cwd + temp dirs +
//!     configured roots) via parameterized `(subpath (param "WRITABLE_ROOT_n"))`
//!     rules, with roots passed out-of-band as `-DWRITABLE_ROOT_n=<path>`,
//!   - denies the network unless `network_allowed`, adding localhost / loopback
//!     exceptions when only loopback ports are permitted.
//!
//! It is a pure-spawn transform — no persistent daemon — so it composes with the
//! executor's process-group handling.
//!
//! Note: `sandbox-exec` is deprecated by Apple but remains functional. We only
//! ever invoke the binary at the fixed `/usr/bin/sandbox-exec` location to avoid
//! PATH-injection of a malicious replacement.

use crate::fs::node_path::project_runtime_path;
use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::sandbox::policy::{
    canonicalize_root, compute_read_denies, compute_read_only_subpaths, compute_readable_roots,
    compute_writable_roots, has_full_disk_read_access, protected_metadata_names,
};
use crate::sandbox::process::scrub_env;
use crate::types::sandbox::{ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec};
use std::collections::HashSet;

const SANDBOX_EXEC_PATH: &str = "/usr/bin/sandbox-exec";

/// A `(key, path)` pair to be passed to sandbox-exec as `-Dkey=path`.
type DefineParam = (RuntimeString, RuntimeString);

#[derive(Debug, Clone, Copy, Default)]
pub struct SeatbeltSandbox;

impl SeatbeltSandbox {
    pub fn new() -> Self {
        SeatbeltSandbox
    }
}

impl SandboxBackend for SeatbeltSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Seatbelt
    }

    fn is_available(&self) -> bool {
        if std::env::consts::OS != "macos" {
            return false;
        }
        project_runtime_path(
            &RuntimePath::from(SANDBOX_EXEC_PATH),
            "sandbox-exec fixed path",
        )
        .map(|path| path.exists())
        .unwrap_or(false)
    }

    fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
        if req.argv.is_empty() {
            panic!("SeatbeltSandbox: empty argv");
        }
        let parent_env = ParentEnvSnapshot::capture();
        let (profile, params) = build_seatbelt_profile(policy, &req.cwd, &parent_env);

        // Roots are passed to sandbox-exec as `-Dkey=value` definitions and
        // referenced inside the profile via `(param "key")`. This keeps paths out
        // of the profile string (so they cannot break S-expression quoting).
        let define_args = params
            .iter()
            .map(|(key, value)| concat(&[rt("-D"), key.clone(), rt("="), value.clone()]));
        let file = rt(SANDBOX_EXEC_PATH);

        let mut args = Vec::with_capacity(3 + params.len() + req.argv.len());
        args.push(rt("-p"));
        args.push(profile);
        args.extend(define_args);
        args.push(rt("--"));
        args.extend(req.argv.iter().cloned());

        let empty = RuntimeEnv::new();
        let extra = req.env.as_ref().unwrap_or(&empty);

        SpawnSpec {
            file,
            args,
            cwd: req.cwd.clone(),
            env: scrub_env(&parent_env, extra),
        }
    }
}

/// Push a `-D` param, canonicalizing the value so rules match the real inode.
fn push_param(params: &mut Vec<DefineParam>, key: &str, path: &RuntimeString) {
    params.push((rt(key), canonicalize_root(path)));
}

/// Build the full Seatbelt profile string and the out-of-band `-D` parameters.
/// Exported for unit testing.
///
/// Section order: base, read allow, read denies (sensitive paths —
/// last-match-wins over the broad allow), write allow (with per-root metadata
/// carve-outs), network, and an optional read-only platform-defaults section for
/// constrained-read (`:minimal`) mode.
pub fn build_seatbelt_profile(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    env: &RuntimeEnv,
) -> (RuntimeString, Vec<DefineParam>) {
    let mut params: Vec<DefineParam> = Vec::new();

    let read_policy = build_read_policy(policy, &mut params);
    let write_policy = build_write_policy(policy, cwd, env, &mut params);
    let network_policy = build_network_policy(policy);
    let platform_defaults = if policy.include_platform_defaults.unwrap_or(false) {
        rt(MACOS_RESTRICTED_READ_ONLY_PLATFORM_DEFAULTS)
    } else {
        RuntimeString::default()
    };
    // Deny sections come LAST: Seatbelt is last-match-wins, so an explicit deny
    // overrides any earlier allow — including a broad writable root that nests
    // the workspace (e.g. a project under `$TMPDIR`) or the platform-defaults
    // scratch grants. This is what makes `.git`/credential protection robust.
    let deny_read_policy = build_deny_read_policy(policy, env, &mut params);
    let metadata_deny_policy = build_metadata_deny_policy(policy, cwd, env);
    let anchor_policy = build_root_anchor_policy(policy, cwd, env);

    let sections = [
        rt(SEATBELT_BASE_POLICY),
        read_policy,
        write_policy,
        network_policy,
        platform_defaults,
        deny_read_policy,
        metadata_deny_policy,
        anchor_policy,
    ]
    .into_iter()
    .filter(|section| !section.is_empty())
    .collect::<Vec<_>>();

    (join_runtime(&sections, "\n"), params)
}

/// Escape a string for use inside a Seatbelt `(regex #"…")` literal: escape
/// regex metacharacters, then escape embedded double-quotes for the
/// S-expression string.
fn escape_for_seatbelt_regex(value: &RuntimeString) -> RuntimeString {
    let mut output = Vec::with_capacity(value.len_utf16());
    for unit in value.as_utf16().iter().copied() {
        if matches!(
            unit,
            0x002e
                | 0x002a
                | 0x002b
                | 0x003f
                | 0x005e
                | 0x0024
                | 0x007b
                | 0x007d
                | 0x0028
                | 0x0029
                | 0x007c
                | 0x005b
                | 0x005d
                | 0x005c
                | 0x0022
        ) {
            output.push(u16::from(b'\\'));
        }
        output.push(unit);
    }
    RuntimeString::from_utf16(output)
}

/// Anchored regex protecting a metadata directory by NAME under a writable root,
/// covering the directory itself and its whole subtree even before it exists.
fn protected_metadata_name_regex(root: &RuntimeString, name: &RuntimeString) -> RuntimeString {
    let mut root = root.clone();
    while root.len_utf16() > 1 && root.ends_with("/") {
        root = root.slice_utf16(..root.len_utf16() - 1);
    }
    let er = escape_for_seatbelt_regex(&root);
    let en = escape_for_seatbelt_regex(name);
    if root == rt("/") {
        concat(&[rt("^/"), en, rt("(/.*)?$")])
    } else {
        concat(&[rt("^"), er, rt("/"), en, rt("(/.*)?$")])
    }
}

/// `file-read*` rules. Full-disk read collapses to a single `(allow file-read*)`;
/// a constrained policy emits one parameterized `(subpath (param ...))` per
/// readable root.
fn build_read_policy(policy: &SandboxPolicy, params: &mut Vec<DefineParam>) -> RuntimeString {
    if has_full_disk_read_access(policy) {
        return rt("; allow read-only file operations\n(allow file-read*)");
    }
    let roots = compute_readable_roots(policy);
    if roots.is_empty() {
        return RuntimeString::default();
    }
    let mut components: Vec<String> = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        let key = format!("READABLE_ROOT_{index}");
        push_param(params, &key, root);
        components.push(format!("(subpath (param \"{key}\"))"));
    }
    rt(&format!(
        "; allow read-only file operations\n(allow file-read*\n{}\n)",
        components.join(" ")
    ))
}

/// Read DENIES, emitted AFTER the read allow so Seatbelt's last-match-wins
/// semantics make them effective: a sandboxed command under a broad `(allow
/// file-read*)` still cannot read `~/.ssh`, `~/.aws`, etc. Also denies
/// `file-write-unlink` on those paths so they cannot be probed by deletion.
fn build_deny_read_policy(
    policy: &SandboxPolicy,
    env: &RuntimeEnv,
    params: &mut Vec<DefineParam>,
) -> RuntimeString {
    let denies = compute_read_denies(policy, env);
    if denies.is_empty() {
        return RuntimeString::default();
    }
    let mut components: Vec<String> = Vec::new();
    for (index, path) in denies.iter().enumerate() {
        let key = format!("DENY_READ_{index}");
        push_param(params, &key, path);
        components.push(format!("(subpath (param \"{key}\"))"));
    }
    let target = components.join(" ");
    rt(&format!(
        "; deny reads of sensitive paths (overrides the broad read allow)\n(deny file-read*\n{target}\n)\n(deny file-write-unlink\n{target}\n)"
    ))
}

/// `file-write*` rules, scoped to the writable roots (configured roots + cwd +
/// temp dirs). Each root becomes `(subpath (param "WRITABLE_ROOT_n"))`. Any
/// `read_only_subpaths` are carved back out with `(require-not ...)` so a
/// protected path under a writable root stays read-only.
fn build_write_policy(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    env: &RuntimeEnv,
    params: &mut Vec<DefineParam>,
) -> RuntimeString {
    let roots = compute_writable_roots(policy, cwd, "darwin", env);
    if roots.is_empty() {
        return RuntimeString::default();
    }

    // Explicit read-only carve-outs (configured + resolved git-worktree gitdir).
    // Metadata-name protection (.git/.agents) is handled separately as a trailing
    // deny section so it survives nested writable roots.
    let read_only = compute_read_only_subpaths(policy);
    let mut components: Vec<String> = Vec::new();

    for (index, root) in roots.iter().enumerate() {
        let root_key = format!("WRITABLE_ROOT_{index}");
        push_param(params, &root_key, root);

        // Read-only subpaths that live under this writable root are excluded.
        let excluded: Vec<&RuntimeString> = read_only
            .iter()
            .filter(|sub| is_under_root(sub, root))
            .collect();
        if excluded.is_empty() {
            components.push(format!("(subpath (param \"{root_key}\"))"));
            continue;
        }

        let mut require_parts = vec![format!("(subpath (param \"{root_key}\"))")];
        for (ex_index, sub) in excluded.iter().enumerate() {
            let ex_key = format!("{root_key}_EXCLUDED_{ex_index}");
            push_param(params, &ex_key, sub);
            // Exclude both the exact protected path and anything beneath it;
            // `subpath` alone leaves a gap for first-time creation of the
            // protected dir itself.
            require_parts.push(format!("(require-not (literal (param \"{ex_key}\")))"));
            require_parts.push(format!("(require-not (subpath (param \"{ex_key}\")))"));
        }
        components.push(format!("(require-all {} )", require_parts.join(" ")));
    }

    rt(&format!("(allow file-write*\n{}\n)", components.join(" ")))
}

/// Explicit DENY rules for protected workspace-metadata names (`.git`,`.agents`
/// by default) under every writable root, emitted at the END of the profile so
/// Seatbelt's last-match-wins semantics make them authoritative — even when a
/// broader writable root (e.g. `$TMPDIR` nesting the workspace, or the
/// platform-defaults `/tmp` scratch grant) would otherwise allow the write.
/// Denies both `file-write*` and `file-write-unlink` so the dir cannot be
/// created, modified, or deleted. Protects by NAME via regex so the rule holds
/// even for a not-yet-created `<root>/.git`.
fn build_metadata_deny_policy(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    env: &RuntimeEnv,
) -> RuntimeString {
    let names = protected_metadata_names(policy);
    if names.is_empty() {
        return RuntimeString::default();
    }
    let roots = compute_writable_roots(policy, cwd, "darwin", env);
    if roots.is_empty() {
        return RuntimeString::default();
    }

    // Dedupe regexes while preserving first-seen insertion order.
    let mut regexes = Vec::new();
    let mut seen = HashSet::new();
    for root in &roots {
        let canon_root = canonicalize_root(root);
        for name in &names {
            let re = protected_metadata_name_regex(&canon_root, name);
            if seen.insert(re.clone()) {
                regexes.push(re);
            }
        }
    }
    if regexes.is_empty() {
        return RuntimeString::default();
    }

    let mut lines = vec![rt(
        "; deny writes to protected workspace metadata (.git/.agents …) — last-wins",
    )];
    for regex in &regexes {
        lines.push(concat(&[
            rt("(deny file-write* (regex #\""),
            regex.clone(),
            rt("\"))"),
        ]));
        lines.push(concat(&[
            rt("(deny file-write-unlink (regex #\""),
            regex.clone(),
            rt("\"))"),
        ]));
    }
    join_runtime(&lines, "\n")
}

/// Keep reusable authority roots and the parents of read-only carveouts in
/// place. A rename must not move a protected descendant past its path rule.
fn build_root_anchor_policy(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    env: &RuntimeEnv,
) -> RuntimeString {
    use crate::runtime_process::{path_dirname, NodePathDialect};
    let roots = compute_writable_roots(policy, cwd, "darwin", env);
    let mut anchors = HashSet::new();
    for root in &roots {
        anchors.insert(canonicalize_root(root));
    }
    for path in compute_read_only_subpaths(policy) {
        for path in [path.clone(), canonicalize_root(&path)] {
            let mut ancestor = path_dirname(NodePathDialect::Posix, &RuntimePath::from(path))
                .into_runtime_string();
            loop {
                if !roots.iter().any(|root| is_under_root(&ancestor, root)) {
                    break;
                }
                anchors.insert(ancestor.clone());
                let parent =
                    path_dirname(NodePathDialect::Posix, &RuntimePath::from(ancestor.clone()))
                        .into_runtime_string();
                if parent == ancestor {
                    break;
                }
                ancestor = parent;
            }
        }
    }
    let mut anchors: Vec<_> = anchors.into_iter().collect();
    anchors.sort();
    join_runtime(
        &anchors
            .into_iter()
            .map(|anchor| {
                concat(&[
                    rt("(deny file-write-unlink (require-all (vnode-type DIRECTORY) (regex #\"^"),
                    escape_for_seatbelt_regex(&anchor),
                    rt("$\")))"),
                ])
            })
            .collect::<Vec<_>>(),
        "\n",
    )
}

/// Network rules. With full network access, allow inbound + outbound. Otherwise
/// deny by default; if loopback ports are explicitly permitted, re-allow only
/// localhost binding and outbound traffic to those ports.
fn build_network_policy(policy: &SandboxPolicy) -> RuntimeString {
    if policy.network_allowed {
        if policy.external_network_only == Some(true) {
            return rt(&format!(
                "; task command external networking; no listener, localhost or Unix sockets\n(allow network-outbound (remote tcp \"*:*\"))\n(allow network-outbound (remote udp \"*:53\"))\n(deny network-inbound)\n(deny network-bind)\n(deny network-outbound (remote ip \"localhost:*\"))\n(deny network-outbound (remote unix-socket))\n{}",
                SEATBELT_NETWORK_POLICY
            ));
        }
        return rt(&format!(
            "; allow outbound + inbound network\n(allow network-outbound)\n(allow network-inbound)\n{}",
            SEATBELT_NETWORK_POLICY
        ));
    }

    let ports = policy.allow_loopback_ports.clone().unwrap_or_default();
    let unix_policy = build_unix_socket_network_policy(policy);
    let allow_local_binding = policy.windows_network_allow_local_binding.unwrap_or(false);
    if ports.is_empty() && unix_policy.is_empty() && !allow_local_binding {
        // Fully closed: `(deny default)` from the base policy already denies the
        // network, but be explicit so the intent survives profile edits.
        return rt("; deny all network access\n(deny network*)");
    }

    let mut lines = Vec::new();
    if allow_local_binding {
        lines.push(rt("; allow local binding and loopback traffic"));
        lines.push(rt("(allow network-bind (local ip \"*:*\"))"));
        lines.push(rt("(allow network-inbound (local ip \"localhost:*\"))"));
        lines.push(rt("(allow network-outbound (remote ip \"localhost:*\"))"));
    }
    for port in &ports {
        lines.push(rt(&format!(
            "(allow network-outbound (remote ip \"localhost:{port}\"))"
        )));
    }
    if !unix_policy.is_empty() {
        lines.push(unix_policy);
    }
    lines.push(rt(SEATBELT_NETWORK_POLICY));
    join_runtime(&lines, "\n")
}

fn build_unix_socket_network_policy(policy: &SandboxPolicy) -> RuntimeString {
    if policy.allow_all_unix_sockets.unwrap_or(false) {
        return rt(&[
            "; allow all unix domain sockets for local IPC",
            "(allow system-socket (socket-domain AF_UNIX))",
            "(allow network-bind (local unix-socket))",
            "(allow network-outbound (remote unix-socket))",
        ]
        .join("\n"));
    }
    let sockets = policy.allow_unix_sockets.clone().unwrap_or_default();
    if sockets.is_empty() {
        return RuntimeString::default();
    }
    let mut lines = vec![
        rt("; allow selected unix domain sockets for local IPC"),
        rt("(allow system-socket (socket-domain AF_UNIX))"),
    ];
    for socket in sockets {
        let mut normalized = canonicalize_root(&socket);
        while normalized.len_utf16() > 1 && normalized.ends_with("/") {
            normalized = normalized.slice_utf16(..normalized.len_utf16() - 1);
        }
        let regex = escape_for_seatbelt_regex(&normalized);
        lines.push(concat(&[
            rt("(allow network-bind (local unix-socket (regex #\"^"),
            regex.clone(),
            rt("(/.*)?$\")))"),
        ]));
        lines.push(concat(&[
            rt("(allow network-outbound (remote unix-socket (regex #\"^"),
            regex,
            rt("(/.*)?$\")))"),
        ]));
    }
    join_runtime(&lines, "\n")
}

/// True when `candidate` is `root` or nested beneath it.
fn is_under_root(candidate: &RuntimeString, root: &RuntimeString) -> bool {
    if candidate == root {
        return true;
    }
    let prefix = if root.ends_with("/") {
        root.clone()
    } else {
        concat(&[root.clone(), rt("/")])
    };
    candidate.as_utf16().starts_with(prefix.as_utf16())
}

fn rt(value: &str) -> RuntimeString {
    RuntimeString::from_utf8(value)
}

fn concat(parts: &[RuntimeString]) -> RuntimeString {
    let length = parts.iter().map(RuntimeString::len_utf16).sum();
    let mut units = Vec::with_capacity(length);
    for part in parts {
        units.extend_from_slice(part.as_utf16());
    }
    RuntimeString::from_utf16(units)
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = rt(separator);
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(&separator);
        }
        output = output.concat(value);
    }
    output
}

/// The static Seatbelt base policy. Starts deny-by-default and re-allows the
/// minimum a typical developer command needs: exec/fork, signalling
/// same-sandbox peers, a curated set of read-only sysctls, `/dev/null` writes,
/// pty/tty access, and read-only user-preference lookups.
const SEATBELT_BASE_POLICY: &str = r##"(version 1)

; base sandbox policy (inspired by Chrome's macOS sandbox)

; start closed-by-default
(deny default)

; child processes inherit the policy of their parent
(allow process-exec)
(allow process-fork)
(allow signal (target same-sandbox))

; process-info
(allow process-info* (target same-sandbox))

(allow file-write-data
  (require-all
    (path "/dev/null")
    (vnode-type CHARACTER-DEVICE)))

; sysctls permitted (CPU/host metadata commonly probed at process startup)
(allow sysctl-read
  (sysctl-name "hw.activecpu")
  (sysctl-name "hw.busfrequency_compat")
  (sysctl-name "hw.byteorder")
  (sysctl-name "hw.cacheconfig")
  (sysctl-name "hw.cachelinesize_compat")
  (sysctl-name "hw.cpufamily")
  (sysctl-name "hw.cpufrequency_compat")
  (sysctl-name "hw.cputype")
  (sysctl-name "hw.l1dcachesize_compat")
  (sysctl-name "hw.l1icachesize_compat")
  (sysctl-name "hw.l2cachesize_compat")
  (sysctl-name "hw.l3cachesize_compat")
  (sysctl-name "hw.logicalcpu_max")
  (sysctl-name "hw.machine")
  (sysctl-name "hw.model")
  (sysctl-name "hw.memsize")
  (sysctl-name "hw.ncpu")
  (sysctl-name "hw.nperflevels")
  (sysctl-name-prefix "hw.optional.arm.")
  (sysctl-name-prefix "hw.optional.armv8_")
  (sysctl-name "hw.packages")
  (sysctl-name "hw.pagesize_compat")
  (sysctl-name "hw.pagesize")
  (sysctl-name "hw.physicalcpu")
  (sysctl-name "hw.physicalcpu_max")
  (sysctl-name "hw.logicalcpu")
  (sysctl-name "hw.cpufrequency")
  (sysctl-name "hw.tbfrequency_compat")
  (sysctl-name "hw.vectorunit")
  (sysctl-name "machdep.cpu.brand_string")
  (sysctl-name "kern.argmax")
  (sysctl-name "kern.hostname")
  (sysctl-name "kern.maxfilesperproc")
  (sysctl-name "kern.maxproc")
  (sysctl-name "kern.osproductversion")
  (sysctl-name "kern.osrelease")
  (sysctl-name "kern.ostype")
  (sysctl-name "kern.osvariant_status")
  (sysctl-name "kern.osversion")
  (sysctl-name "kern.secure_kernel")
  (sysctl-name "kern.usrstack64")
  (sysctl-name "kern.version")
  (sysctl-name "sysctl.proc_cputype")
  (sysctl-name "vm.loadavg")
  (sysctl-name-prefix "hw.perflevel")
  (sysctl-name-prefix "kern.proc.pgrp.")
  (sysctl-name-prefix "kern.proc.pid.")
  (sysctl-name-prefix "net.routetable.")
)

; Allow Java to read some CPU info (misclassified as a write by the kernel).
(allow sysctl-write
  (sysctl-name "kern.grade_cputype"))

; IOKit
(allow iokit-open
  (iokit-registry-entry-class "RootDomainUserClient")
)

; needed to look up user info
(allow mach-lookup
  (global-name "com.apple.system.opendirectoryd.libinfo")
)

; Needed for python multiprocessing on macOS for the SemLock
(allow ipc-posix-sem)

; Needed for PyTorch/libomp on macOS to register OpenMP runtimes.
(allow ipc-posix-shm-read-data
  ipc-posix-shm-write-create
  ipc-posix-shm-write-unlink
  (ipc-posix-name-regex #"^/__KMP_REGISTERED_LIB_[0-9]+$"))

(allow mach-lookup
  (global-name "com.apple.PowerManagement.control")
)

; allow openpty()
(allow pseudo-tty)
(allow file-read* file-write* file-ioctl (literal "/dev/ptmx"))
(allow file-read* file-write*
  (require-all
    (regex #"^/dev/ttys[0-9]+")
    (extension "com.apple.sandbox.pty")))
; PTYs created before entering seatbelt may lack the extension; allow ioctl on
; those slave ttys so interactive shells detect a TTY and remain functional.
(allow file-ioctl (regex #"^/dev/ttys[0-9]+"))

; allow readonly user preferences
(allow ipc-posix-shm-read* (ipc-posix-name-prefix "apple.cfprefs."))
(allow mach-lookup
  (global-name "com.apple.cfprefsd.daemon")
  (global-name "com.apple.cfprefsd.agent")
  (local-name "com.apple.cfprefsd.agent"))
(allow user-preference-read)"##;

/// Extra rules layered on when the network is enabled: safe AF_SYSTEM sockets,
/// the mach services required for DNS / TLS trust evaluation, and the network
/// route sysctls.
const SEATBELT_NETWORK_POLICY: &str = r##"; network platform services
(allow system-socket
  (require-all
    (socket-domain AF_SYSTEM)
    (socket-protocol 2)
  )
)

(allow mach-lookup
    (global-name "com.apple.bsd.dirhelper")
    (global-name "com.apple.system.opendirectoryd.membership")
    (global-name "com.apple.SecurityServer")
    (global-name "com.apple.networkd")
    (global-name "com.apple.ocspd")
    (global-name "com.apple.trustd.agent")
    (global-name "com.apple.SystemConfiguration.DNSConfiguration")
    (global-name "com.apple.SystemConfiguration.configd")
)

(allow sysctl-read
  (sysctl-name-regex #"^net.routetable")
)"##;

/// macOS read-only platform defaults, included only when
/// `include_platform_defaults` is set (constrained-read `:minimal` mode).
/// Grants the system framework / dylib loader paths, the standard exec runtime
/// (`/bin`, `/usr/bin`, …), device nodes and scratch temp so a process can
/// still launch when reads are otherwise scoped to `readable_roots`.
const MACOS_RESTRICTED_READ_ONLY_PLATFORM_DEFAULTS: &str = r##"; macOS platform defaults included when a split filesystem policy requests `:minimal`.

; Read access to standard system paths
(allow file-read* file-test-existence
  (subpath "/Library/Apple")
  (subpath "/Library/Filesystems/NetFSPlugins")
  (subpath "/Library/Preferences/Logging")
  (subpath "/private/var/db/DarwinDirectory/local/recordStore.data")
  (subpath "/private/var/db/timezone")
  (subpath "/usr/lib")
  (subpath "/usr/share")
  (subpath "/Library/Preferences")
  (subpath "/var/db")
  (subpath "/private/var/db"))

; Map system frameworks + dylibs for loader.
(allow file-map-executable
  (subpath "/Library/Apple/System/Library/Frameworks")
  (subpath "/Library/Apple/System/Library/PrivateFrameworks")
  (subpath "/Library/Apple/usr/lib")
  (subpath "/System/Library/Extensions")
  (subpath "/System/Library/Frameworks")
  (subpath "/System/Library/PrivateFrameworks")
  (subpath "/System/Library/SubFrameworks")
  (subpath "/System/iOSSupport/System/Library/Frameworks")
  (subpath "/System/iOSSupport/System/Library/PrivateFrameworks")
  (subpath "/System/iOSSupport/System/Library/SubFrameworks")
  (subpath "/usr/lib"))

; System Framework and AppKit resources
(allow file-read* file-test-existence
  (subpath "/Library/Apple/System/Library/Frameworks")
  (subpath "/Library/Apple/System/Library/PrivateFrameworks")
  (subpath "/Library/Apple/usr/lib")
  (subpath "/System/Library/Frameworks")
  (subpath "/System/Library/PrivateFrameworks")
  (subpath "/System/Library/SubFrameworks")
  (subpath "/System/iOSSupport/System/Library/Frameworks")
  (subpath "/System/iOSSupport/System/Library/PrivateFrameworks")
  (subpath "/System/iOSSupport/System/Library/SubFrameworks")
  (subpath "/usr/lib"))

; Allow guarded vnodes.
(allow system-mac-syscall (mac-policy-name "vnguard"))

; Determine whether a container is expected.
(allow system-mac-syscall
  (require-all
    (mac-policy-name "Sandbox")
    (mac-syscall-number 67)))

; Allow resolution of standard system symlinks.
(allow file-read-metadata file-test-existence
  (literal "/etc")
  (literal "/tmp")
  (literal "/var")
  (literal "/private/etc/localtime"))

; Allow stat'ing of firmlink parent path components.
(allow file-read-metadata file-test-existence
  (path-ancestors "/System/Volumes/Data/private"))

; Allow processes to get their current working directory.
(allow file-read* file-test-existence
  (literal "/"))

; Allow FSIOC_CAS_BSDFLAGS as alternate chflags.
(allow system-fsctl (fsctl-command FSIOC_CAS_BSDFLAGS))

; Allow access to standard special files.
(allow file-read* file-test-existence
  (literal "/dev/autofs_nowait")
  (literal "/dev/random")
  (literal "/dev/urandom")
  (literal "/private/etc/master.passwd")
  (literal "/private/etc/passwd")
  (literal "/private/etc/protocols")
  (literal "/private/etc/services"))

; Allow null/zero read/write.
(allow file-read* file-test-existence file-write-data
  (literal "/dev/null")
  (literal "/dev/zero"))

; Allow read/write access to the file descriptors.
(allow file-read-data file-test-existence file-write-data
  (subpath "/dev/fd"))

; Provide access to debugger helpers.
(allow file-read* file-test-existence file-write-data file-ioctl
  (literal "/dev/dtracehelper"))

; Scratch space so tools can create temp files.
(allow file-read* file-test-existence file-write* (subpath "/tmp"))
(allow file-read* file-write* (subpath "/private/tmp"))
(allow file-read* file-write* (subpath "/var/tmp"))
(allow file-read* file-write* (subpath "/private/var/tmp"))

; Allow reading standard config directories.
(allow file-read* (subpath "/etc"))
(allow file-read* (subpath "/private/etc"))

(allow file-read* file-test-existence
  (literal "/System/Library/CoreServices")
  (literal "/System/Library/CoreServices/.SystemVersionPlatform.plist")
  (literal "/System/Library/CoreServices/SystemVersion.plist"))

; Some processes read /var metadata during startup.
(allow file-read-metadata (subpath "/var"))
(allow file-read-metadata (subpath "/private/var"))

; IOKit access for root domain services.
(allow iokit-open
  (iokit-registry-entry-class "RootDomainUserClient"))

; macOS Standard library queries opendirectoryd at startup
(allow mach-lookup (global-name "com.apple.system.opendirectoryd.libinfo"))

; Allow IPC to analytics, logging, trust, and other system agents.
(allow mach-lookup
  (global-name "com.apple.analyticsd")
  (global-name "com.apple.analyticsd.messagetracer")
  (global-name "com.apple.appsleep")
  (global-name "com.apple.bsd.dirhelper")
  (global-name "com.apple.cfprefsd.agent")
  (global-name "com.apple.cfprefsd.daemon")
  (global-name "com.apple.diagnosticd")
  (global-name "com.apple.dt.automationmode.reader")
  (global-name "com.apple.espd")
  (global-name "com.apple.logd")
  (global-name "com.apple.logd.events")
  (global-name "com.apple.runningboard")
  (global-name "com.apple.secinitd")
  (global-name "com.apple.system.DirectoryService.libinfo_v1")
  (global-name "com.apple.system.logger")
  (global-name "com.apple.system.notification_center")
  (global-name "com.apple.system.opendirectoryd.membership")
  (global-name "com.apple.trustd")
  (global-name "com.apple.trustd.agent")
  (global-name "com.apple.xpc.activity.unmanaged")
  (local-name "com.apple.cfprefsd.agent"))

; Allow IPC to the syslog socket for logging.
(allow network-outbound (literal "/private/var/run/syslog"))

; macOS Notifications
(allow ipc-posix-shm-read*
  (ipc-posix-name "apple.shm.notification_center"))

; Regulatory domain support.
(allow file-read*
  (literal "/private/var/db/eligibilityd/eligibility.plist"))

; Audio and power management services.
(allow mach-lookup (global-name "com.apple.audio.audiohald"))
(allow mach-lookup (global-name "com.apple.audio.AudioComponentRegistrar"))
(allow mach-lookup (global-name "com.apple.PowerManagement.control"))

; Allow reading the minimum system runtime so exec works.
(allow file-read-data (subpath "/bin"))
(allow file-read-metadata (subpath "/bin"))
(allow file-read-data (subpath "/sbin"))
(allow file-read-metadata (subpath "/sbin"))
(allow file-read-data (subpath "/usr/bin"))
(allow file-read-metadata (subpath "/usr/bin"))
(allow file-read-data (subpath "/usr/sbin"))
(allow file-read-metadata (subpath "/usr/sbin"))
(allow file-read-data (subpath "/usr/libexec"))
(allow file-read-metadata (subpath "/usr/libexec"))

(allow file-read* (subpath "/Library/Preferences"))
(allow file-read* (subpath "/opt/homebrew/lib"))
(allow file-read* (subpath "/usr/local/lib"))
(allow file-read* (subpath "/Applications"))

; Terminal basics and device handles.
(allow file-read* (regex "^/dev/fd/(0|1|2)$"))
(allow file-write* (regex "^/dev/fd/(1|2)$"))
(allow file-read* file-write* (literal "/dev/null"))
(allow file-read* file-write* (literal "/dev/tty"))
(allow file-read-metadata (literal "/dev"))
(allow file-read-metadata (regex "^/dev/.*$"))
(allow file-read-metadata (literal "/dev/stdin"))
(allow file-read-metadata (literal "/dev/stdout"))
(allow file-read-metadata (literal "/dev/stderr"))
(allow file-read-metadata (regex "^/dev/tty[^/]*$"))
(allow file-read-metadata (regex "^/dev/pty[^/]*$"))
(allow file-read* file-write* (regex "^/dev/ttys[0-9]+$"))
(allow file-read* file-write* (literal "/dev/ptmx"))
(allow file-ioctl (regex "^/dev/ttys[0-9]+$"))

; Allow metadata traversal for firmlink parents.
(allow file-read-metadata (literal "/System/Volumes") (vnode-type DIRECTORY))
(allow file-read-metadata (literal "/System/Volumes/Data") (vnode-type DIRECTORY))
(allow file-read-metadata (literal "/System/Volumes/Data/Users") (vnode-type DIRECTORY))

; App sandbox extensions
(allow file-read* (extension "com.apple.app-sandbox.read"))
(allow file-read* file-write* (extension "com.apple.app-sandbox.read-write"))"##;

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ExecRequest {
        ExecRequest {
            argv: vec![rt("bash"), rt("-c"), rt("echo hi")],
            cwd: RuntimePath::from("/work/project"),
            env: None,
            timeout_ms: None,
        }
    }

    #[test]
    fn managed_proxy_ports_do_not_grant_other_loopback_destinations() {
        let policy = SandboxPolicy {
            network_allowed: false,
            allow_loopback_ports: Some(vec![41001]),
            windows_network_allow_local_binding: Some(false),
            ..SandboxPolicy::default()
        };
        let profile = build_network_policy(&policy);
        assert!(profile.contains("localhost:41001"));
        assert!(!profile.contains("localhost:*"));
        assert!(!profile.contains("network-bind"));
        assert!(!profile.contains("*:53"));
    }

    #[test]
    fn explicit_local_binding_grants_loopback_without_proxy_ports() {
        let policy = SandboxPolicy {
            network_allowed: false,
            windows_network_allow_local_binding: Some(true),
            ..SandboxPolicy::default()
        };
        let profile = build_network_policy(&policy);
        assert!(profile.contains("network-bind"));
        assert!(profile.contains("localhost:*"));
    }

    #[test]
    fn profile_and_define_args_preserve_lone_surrogate_paths() {
        let lone = RuntimeString::from_utf16(vec![u16::from(b'/'), 0xd800]);
        let policy = SandboxPolicy {
            writable_roots: vec![lone.clone()],
            readable_roots: Some(vec![lone.clone()]),
            protected_metadata_names: Some(vec![lone.clone()]),
            exclude_slash_tmp: Some(true),
            exclude_tmpdir_env_var: Some(true),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        let (profile, params) = build_seatbelt_profile(&policy, &request().cwd, &RuntimeEnv::new());
        assert!(params.iter().any(|(_, value)| value == &lone));
        assert!(profile.as_utf16().contains(&0xd800));
    }

    #[test]
    fn unix_socket_regex_escapes_without_scalar_projection() {
        let socket = RuntimeString::from_utf16(vec![
            u16::from(b'/'),
            u16::from(b't'),
            u16::from(b'.'),
            0xd800,
        ]);
        let policy = SandboxPolicy {
            allow_unix_sockets: Some(vec![socket]),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        let profile = build_unix_socket_network_policy(&policy);
        assert!(profile.as_utf16().contains(&0xd800));
        assert!(profile.contains(r"\."));
    }

    /// `unconfined_writes` (bypassPermissions) collapses the write policy to a
    /// single whole-filesystem grant and drops every metadata / read-only deny,
    /// while the sensitive-read denies stay in place.
    ///
    /// Unix-only, because "the whole filesystem" is spelled `/` there and the root the
    /// grant collapses to is derived through the host's path projection. On Windows the
    /// same policy yields `\\?\C:\`, and asserting either spelling on the other
    /// platform tests the projection rather than the profile — and seatbelt profiles
    /// are only ever used on macOS.
    #[test]
    #[cfg(unix)]
    fn unconfined_writes_grant_the_whole_filesystem_without_metadata_denies() {
        let mut env = RuntimeEnv::new();
        env.insert(rt("HOME"), rt("/Users/tester"));
        let policy = SandboxPolicy {
            writable_roots: vec![rt("/work/project")],
            unconfined_writes: Some(true),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        let (profile, params) = build_seatbelt_profile(&policy, &request().cwd, &env);

        assert!(profile.contains("(allow file-write*"));
        assert!(!profile.contains("deny file-write*"));
        assert!(!profile.contains(".git"));
        assert!(
            !profile.contains("deny file-read*"),
            "reads stay broadly allowed unless the caller names deny_read_subpaths"
        );
        let writable: Vec<&RuntimeString> = params
            .iter()
            .filter(|(key, _)| key.starts_with("WRITABLE_ROOT_"))
            .map(|(_, value)| value)
            .collect();
        assert_eq!(writable, vec![&rt("/")]);
    }
}
