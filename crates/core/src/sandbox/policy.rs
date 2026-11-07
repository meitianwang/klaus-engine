//! Policy derivation shared by the platform sandbox transforms. Logical paths remain UTF-16 until a named filesystem
//! probe; native results re-enter through the Node filesystem ingress.

use std::collections::HashSet;

use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_process::{
    path_is_absolute, path_join, path_normalize, NodePathDialect, RuntimeEnv,
};
use crate::runtime_values::{NodeBufferUtf8, RuntimePath, RuntimeString};
use crate::types::sandbox::SandboxPolicy;

pub const DEFAULT_PROTECTED_METADATA_NAMES: [&str; 3] = [".git", ".agents", ".klaus"];

fn is_absolute(path: &RuntimeString) -> bool {
    path_is_absolute(NodePathDialect::Posix, &RuntimePath::from(path.clone()))
}

fn join(parts: &[RuntimeString]) -> RuntimeString {
    path_join(
        NodePathDialect::Posix,
        &parts
            .iter()
            .cloned()
            .map(RuntimePath::from)
            .collect::<Vec<_>>(),
    )
    .into_runtime_string()
}

pub fn normalize_root(path: &RuntimeString) -> RuntimeString {
    if path.is_empty() {
        return RuntimeString::default();
    }
    let mut normalized = path_normalize(NodePathDialect::Posix, &RuntimePath::from(path.clone()))
        .into_runtime_string();
    while normalized.len_utf16() > 1 && normalized.ends_with("/") {
        normalized = normalized.slice_utf16(..normalized.len_utf16() - 1);
    }
    normalized
}

pub fn canonicalize_root(path: &RuntimeString) -> RuntimeString {
    if path.is_empty() || !is_absolute(path) {
        return normalize_root(path);
    }
    let logical = RuntimePath::from(path.clone());
    let Ok(native) = project_runtime_path(&logical, "sandbox root") else {
        return normalize_root(path);
    };
    match crate::fs::node_path::canonicalize_host(native) {
        Ok(canonical) => runtime_path_from_native(&canonical).into_runtime_string(),
        Err(_) => normalize_root(path),
    }
}

fn is_dir(path: &RuntimeString) -> bool {
    let Ok(native) =
        project_runtime_path(&RuntimePath::from(path.clone()), "sandbox directory probe")
    else {
        return false;
    };
    std::fs::metadata(native)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

pub fn resolve_gitdir_pointer(root: &RuntimeString) -> Option<RuntimeString> {
    let dot_git = join(&[root.clone(), RuntimeString::from_utf8(".git")]);
    let native = project_runtime_path(&RuntimePath::from(dot_git), "gitdir pointer").ok()?;
    if !std::fs::metadata(&native).ok()?.is_file() {
        return None;
    }
    let text = NodeBufferUtf8::decode(&std::fs::read(native).ok()?);
    let target = parse_gitdir_target(&text)?;
    let absolute = if is_absolute(&target) {
        target
    } else {
        join(&[root.clone(), target])
    };
    Some(normalize_root(&absolute))
}

fn parse_gitdir_target(text: &RuntimeString) -> Option<RuntimeString> {
    let prefix = "gitdir:".encode_utf16().collect::<Vec<_>>();
    for line in text.as_utf16().split(|unit| *unit == u16::from(b'\n')) {
        let line = line.strip_suffix(&[u16::from(b'\r')]).unwrap_or(line);
        if !line.starts_with(&prefix) {
            continue;
        }
        let mut start = prefix.len();
        let mut end = line.len();
        while start < end && is_regex_whitespace(line[start]) {
            start += 1;
        }
        while end > start && is_regex_whitespace(line[end - 1]) {
            end -= 1;
        }
        if start < end {
            return Some(RuntimeString::from_utf16(line[start..end].to_vec()));
        }
    }
    None
}

fn is_regex_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn add_normalized(
    output: &mut Vec<RuntimeString>,
    seen: &mut HashSet<RuntimeString>,
    path: &RuntimeString,
) {
    let normalized = normalize_root(path);
    if seen.insert(normalized.clone()) {
        output.push(normalized);
    }
}

fn push_unique_absolute(
    output: &mut Vec<RuntimeString>,
    seen: &mut HashSet<RuntimeString>,
    path: &RuntimeString,
) {
    if is_absolute(path) {
        add_normalized(output, seen, path);
    }
}

/// Whether the policy waives write confinement (see
/// [`SandboxPolicy::unconfined_writes`]).
pub fn has_unconfined_writes(policy: &SandboxPolicy) -> bool {
    policy.unconfined_writes == Some(true)
}

pub fn compute_read_only_subpaths(policy: &SandboxPolicy) -> Vec<RuntimeString> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    // An explicitly configured carve-out is the caller's own instruction and
    // survives the unconfined opt-out; only the engine-derived gitdir protection
    // below is dropped with it.
    for path in policy.read_only_subpaths.iter().flatten() {
        add_normalized(&mut output, &mut seen, path);
    }
    if has_unconfined_writes(policy) {
        return output;
    }
    for root in &policy.writable_roots {
        if let Some(gitdir) = resolve_gitdir_pointer(root) {
            add_normalized(&mut output, &mut seen, &gitdir);
        }
    }
    output
}

pub fn protected_metadata_names(policy: &SandboxPolicy) -> Vec<RuntimeString> {
    if has_unconfined_writes(policy) {
        return Vec::new();
    }
    policy.protected_metadata_names.clone().unwrap_or_else(|| {
        DEFAULT_PROTECTED_METADATA_NAMES
            .iter()
            .map(|name| RuntimeString::from_utf8(name))
            .collect()
    })
}

/// Read denies come only from the caller. There is no built-in credential
/// deny-list: a broad read grant is broad, and subtracting `~/.ssh` from it
/// broke every command that authenticates as the user (`git push`, `ssh`)
/// while stopping no real exfiltration — the agent reads files through its own
/// tools, which do not go through this sandbox at all. Callers that genuinely
/// want a narrower read surface should name `readable_roots`, which is an
/// allow-list, instead of poking holes in a global allow.
pub fn compute_read_denies(policy: &SandboxPolicy, _env: &RuntimeEnv) -> Vec<RuntimeString> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    for path in policy.deny_read_subpaths.iter().flatten() {
        push_unique_absolute(&mut output, &mut seen, path);
    }
    output
}

pub fn compute_writable_roots(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    platform: &str,
    env: &RuntimeEnv,
) -> Vec<RuntimeString> {
    if platform == "windows" {
        return compute_windows_writable_roots(policy, cwd, env);
    }
    if has_unconfined_writes(policy) {
        // One root that contains everything, so the backends emit a single
        // whole-filesystem write grant instead of enumerating roots.
        return vec![RuntimeString::from_utf8("/")];
    }
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    for root in &policy.writable_roots {
        push_unique_absolute(&mut output, &mut seen, root);
    }
    if policy.exclude_cwd != Some(true) {
        push_unique_absolute(&mut output, &mut seen, cwd.as_runtime_string());
    }

    let unix = platform == "darwin" || platform == "linux";
    let slash_tmp = RuntimeString::from_utf8("/tmp");
    if unix && policy.exclude_slash_tmp != Some(true) && is_dir(&slash_tmp) {
        push_unique_absolute(&mut output, &mut seen, &slash_tmp);
    }
    if policy.exclude_tmpdir_env_var != Some(true) {
        if let Some(tmpdir) = env.get(&RuntimeString::from_utf8("TMPDIR")) {
            if !tmpdir.is_empty() {
                push_unique_absolute(&mut output, &mut seen, tmpdir);
            }
        }
    }
    output
}

/// Derive Windows roots using Windows path semantics on every host.
fn compute_windows_writable_roots(
    policy: &SandboxPolicy,
    cwd: &RuntimePath,
    env: &RuntimeEnv,
) -> Vec<RuntimeString> {
    let mut roots = policy.writable_roots.clone();
    if policy.exclude_cwd != Some(true) {
        roots.push(cwd.as_runtime_string().clone());
    }
    if policy.exclude_tmpdir_env_var != Some(true) {
        if let Some(tmpdir) = env.get(&RuntimeString::from("TMPDIR")) {
            roots.push(tmpdir.clone());
        }
    }
    let mut seen = HashSet::new();
    roots
        .into_iter()
        .filter_map(|root| {
            let path = RuntimePath::from(root);
            if !path_is_absolute(NodePathDialect::Win32, &path) {
                return None;
            }
            let normalized = path_normalize(NodePathDialect::Win32, &path).into_runtime_string();
            seen.insert(normalized.clone()).then_some(normalized)
        })
        .collect()
}

pub fn compute_readable_roots(policy: &SandboxPolicy) -> Vec<RuntimeString> {
    let Some(roots) = &policy.readable_roots else {
        return vec![RuntimeString::from_utf8("/")];
    };
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    for root in roots {
        push_unique_absolute(&mut output, &mut seen, root);
    }
    output
}

pub fn has_full_disk_read_access(policy: &SandboxPolicy) -> bool {
    policy.readable_roots.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(entries: &[(&str, &str)]) -> RuntimeEnv {
        let mut environment = RuntimeEnv::new();
        for (key, value) in entries {
            environment.insert(RuntimeString::from(*key), RuntimeString::from(*value));
        }
        environment
    }

    #[test]
    fn normalization_preserves_lone_surrogate_identity() {
        let path = RuntimeString::from_utf16(vec![
            u16::from(b'/'),
            u16::from(b'a'),
            u16::from(b'/'),
            0xd800,
            u16::from(b'/'),
            u16::from(b'.'),
            u16::from(b'/'),
        ]);
        assert_eq!(
            normalize_root(&path).as_utf16(),
            &[u16::from(b'/'), u16::from(b'a'), u16::from(b'/'), 0xd800]
        );
    }

    #[test]
    fn writable_roots_preserve_first_seen_order() {
        let policy = SandboxPolicy {
            writable_roots: vec![
                RuntimeString::from("/first"),
                RuntimeString::from("/first/"),
            ],
            exclude_slash_tmp: Some(true),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        let roots = compute_writable_roots(
            &policy,
            &RuntimePath::from("/cwd"),
            "linux",
            &environment(&[("TMPDIR", "/tmp/user")]),
        );
        assert_eq!(
            roots,
            vec![
                RuntimeString::from("/first"),
                RuntimeString::from("/cwd"),
                RuntimeString::from("/tmp/user"),
            ]
        );
    }

    /// Under `unconfined_writes` the derivation every backend shares collapses to
    /// one root and no write-side carve-out.
    #[test]
    fn unconfined_writes_collapse_roots_and_drop_write_carveouts() {
        let policy = SandboxPolicy {
            writable_roots: vec![RuntimeString::from("/first")],
            read_only_subpaths: Some(vec![RuntimeString::from("/first/.git")]),
            unconfined_writes: Some(true),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        assert_eq!(
            compute_writable_roots(
                &policy,
                &RuntimePath::from("/cwd"),
                "linux",
                &environment(&[("TMPDIR", "/tmp/user")]),
            ),
            vec![RuntimeString::from("/")]
        );
        assert_eq!(
            compute_read_only_subpaths(&policy),
            vec![RuntimeString::from("/first/.git")],
            "an explicitly configured carve-out is the caller's own instruction"
        );
        assert!(protected_metadata_names(&policy).is_empty());
    }

    /// A broad read grant stays broad. The engine once subtracted a built-in
    /// `~/.ssh`-style deny-list here, which denied the user's own git/ssh
    /// credentials and made `git push` fail inside the sandbox.
    #[test]
    fn read_denies_come_only_from_the_caller() {
        let policy = SandboxPolicy {
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        assert!(
            compute_read_denies(&policy, &environment(&[("HOME", "/home/test")])).is_empty(),
            "a default policy must not invent read denials"
        );

        let explicit = SandboxPolicy {
            deny_read_subpaths: Some(vec![
                RuntimeString::from("/home/test/secrets"),
                RuntimeString::from("relative/ignored"),
            ]),
            network_allowed: false,
            ..SandboxPolicy::default()
        };
        assert_eq!(
            compute_read_denies(&explicit, &environment(&[("HOME", "/home/test")])),
            vec![RuntimeString::from("/home/test/secrets")],
            "caller-supplied denials are kept; relative paths are dropped"
        );
    }
}
