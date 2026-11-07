//! Filesystem policy translation for the embedded Linux namespace launcher.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::fs::node_path::{project_runtime_path, runtime_path_from_native};
use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::sandbox::{ExecRequest, SandboxPolicy};

use super::policy::{
    compute_read_denies, compute_read_only_subpaths, compute_readable_roots,
    compute_writable_roots, has_full_disk_read_access, protected_metadata_names,
};

pub struct BwrapPlan {
    pub arguments: Vec<RuntimeString>,
    pub missing_protected_paths: Vec<RuntimeString>,
    pub denied_read_files: Vec<RuntimeString>,
    pub synthetic_protected_paths: Vec<RuntimeString>,
}

fn native(path: &RuntimeString) -> Result<PathBuf, String> {
    project_runtime_path(&RuntimePath::from(path.clone()), "Linux sandbox path")
        .map_err(|error| format!("invalid Linux sandbox path: {error:?}"))
}

fn runtime(path: &Path) -> RuntimeString {
    runtime_path_from_native(path).into_runtime_string()
}

fn push(arguments: &mut Vec<RuntimeString>, operation: &str, path: &Path) {
    arguments.extend([RuntimeString::from(operation), runtime(path), runtime(path)]);
}

/// Produce a mount policy before the helper installs any namespace or filter.
/// Missing protected paths are supervised by the outer helper; existing paths
/// become read-only mounts after every writable grant has been applied.
pub fn build_bwrap_plan(
    request: &ExecRequest,
    policy: &SandboxPolicy,
    environment: &RuntimeEnv,
) -> Result<BwrapPlan, String> {
    if request.argv.is_empty() {
        return Err("sandbox request has no executable".into());
    }
    if !policy.network_allowed
        && policy.allow_all_unix_sockets != Some(true)
        && policy
            .allow_unix_sockets
            .as_ref()
            .is_some_and(|paths| !paths.is_empty())
    {
        return Err("This Linux executor cannot enforce a path-specific Unix-socket exception; the requested policy was not executed.".into());
    }
    if !policy.network_allowed
        && policy.allow_all_unix_sockets == Some(true)
        && policy
            .allow_loopback_ports
            .as_ref()
            .is_none_or(|ports| ports.is_empty())
    {
        return Err("Unix-socket exceptions require the managed routing scope on this Linux executor; the requested policy was not executed.".into());
    }
    if policy.external_network_only == Some(true) && policy.network_allowed {
        return Err("The task's external-only network scope requires a filtered routing bridge; unrestricted host networking cannot represent it.".into());
    }
    let mut arguments: Vec<RuntimeString> = [
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--cap-drop",
        "ALL",
    ]
    .into_iter()
    .map(RuntimeString::from)
    .collect();
    if !policy.network_allowed {
        arguments.push(RuntimeString::from("--unshare-net"));
    }
    if has_full_disk_read_access(policy) {
        push(&mut arguments, "--ro-bind", Path::new("/"));
    } else {
        arguments.extend([RuntimeString::from("--tmpfs"), RuntimeString::from("/")]);
        let mut readable = compute_readable_roots(policy);
        if policy.include_platform_defaults == Some(true) {
            readable.extend(
                [
                    "/bin",
                    "/sbin",
                    "/usr",
                    "/etc",
                    "/lib",
                    "/lib64",
                    "/nix/store",
                    "/run/current-system/sw",
                ]
                .into_iter()
                .map(RuntimeString::from),
            );
        }
        for root in readable {
            let root = native(&root)?;
            if root.exists() {
                let canonical = std::fs::canonicalize(root).map_err(|error| error.to_string())?;
                push(&mut arguments, "--ro-bind", &canonical);
            }
        }
    }
    arguments.extend(
        ["--dev", "/dev", "--proc", "/proc"]
            .into_iter()
            .map(RuntimeString::from),
    );

    let mut roots = Vec::new();
    for root in compute_writable_roots(policy, &request.cwd, "linux", environment) {
        let path = native(&root)?;
        if path.exists() {
            let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
            if !roots.contains(&canonical) {
                roots.push(canonical);
            }
        }
    }
    roots.sort_by_key(|path| path.components().count());
    for root in &roots {
        push(&mut arguments, "--bind", root);
    }
    let explicit_read_only_paths = compute_read_only_subpaths(policy);
    let mut protected: Vec<PathBuf> = explicit_read_only_paths
        .iter()
        .map(native)
        .collect::<Result<_, _>>()?;
    let names = protected_metadata_names(policy);
    for root in &roots {
        for name in &names {
            let name = native(name)?;
            if name.components().count() != 1 || name.is_absolute() {
                return Err("protected metadata name must be one path component".into());
            }
            protected.push(root.join(name));
        }
    }
    let mut seen = HashSet::new();
    let mut missing_protected_paths = Vec::new();
    let mut synthetic_protected_paths = Vec::new();
    for path in protected {
        if !seen.insert(path.clone()) {
            continue;
        }
        if let Some(symlink) = path.ancestors().find(|ancestor| {
            roots.iter().any(|root| ancestor.starts_with(root))
                && ancestor
                    .symlink_metadata()
                    .is_ok_and(|metadata| metadata.file_type().is_symlink())
        }) {
            return Err(format!(
                "protected metadata crosses a writable symlink: {}",
                symlink.display()
            ));
        }
        if let Ok(canonical) = std::fs::canonicalize(&path) {
            // A protected path reached through a mutable symlink can be replaced
            // after startup. Refuse the ambiguous policy instead of pinning only
            // the target that happened to exist during this transform.
            if path
                .symlink_metadata()
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return Err(format!(
                    "protected metadata path is a mutable symlink: {}",
                    path.display()
                ));
            }
            push(&mut arguments, "--ro-bind", &canonical);
        } else {
            let mut missing = path.as_path();
            while let Some(parent) = missing.parent() {
                if parent.exists() {
                    break;
                }
                missing = parent;
            }
            let missing = runtime(missing);
            let targets = if explicit_read_only_paths.iter().any(|path| {
                native(path).is_ok_and(|path| path.starts_with(native(&missing).unwrap()))
            }) {
                &mut synthetic_protected_paths
            } else {
                &mut missing_protected_paths
            };
            if !targets.contains(&missing) {
                targets.push(missing);
            }
        }
    }
    let mut denied_read_files = Vec::new();
    for path in compute_read_denies(policy, environment) {
        let path = native(&path)?;
        if !path.exists() {
            continue;
        }
        let path = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
        if path.is_dir() {
            arguments.extend([
                RuntimeString::from("--perms"),
                RuntimeString::from("000"),
                RuntimeString::from("--tmpfs"),
                runtime(&path),
                RuntimeString::from("--remount-ro"),
                runtime(&path),
            ]);
        } else {
            denied_read_files.push(runtime(&path));
        }
    }
    #[cfg(target_os = "linux")]
    {
        let alias = Path::new("/mnt/wslg/distro");
        if super::linux::wslg::is_duplicate_root(alias).map_err(|error| error.to_string())? {
            super::linux::wslg::ensure_supported_path(&native(request.cwd.as_runtime_string())?)?;
            for root in roots.iter().chain(
                policy
                    .readable_roots
                    .iter()
                    .flatten()
                    .map(native)
                    .collect::<Result<Vec<_>, _>>()?
                    .iter(),
            ) {
                super::linux::wslg::ensure_supported_path(root)?;
            }
            arguments.extend([RuntimeString::from("--tmpfs"), runtime(alias)]);
        }
        let interop = Path::new("/run/WSL");
        if interop.is_dir() {
            arguments.extend([RuntimeString::from("--tmpfs"), runtime(interop)]);
        }
    }
    arguments.extend([
        RuntimeString::from("--chdir"),
        request.cwd.as_runtime_string().clone(),
    ]);
    Ok(BwrapPlan {
        arguments,
        missing_protected_paths,
        denied_read_files,
        synthetic_protected_paths,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_has_no_writable_temp_or_command_directory_mount() {
        let request = ExecRequest {
            argv: vec!["true".into()],
            cwd: "/tmp".into(),
            env: None,
            timeout_ms: None,
        };
        let mut policy = SandboxPolicy::default();
        policy.enforce_filesystem_read_only();
        let plan = build_bwrap_plan(&request, &policy, &RuntimeEnv::new()).unwrap();
        assert!(!plan.arguments.iter().any(|argument| argument == "--bind"));
        assert!(!plan
            .arguments
            .windows(2)
            .any(|pair| pair[0] == "--tmpfs" && pair[1] == "/tmp"));
        assert!(plan
            .arguments
            .iter()
            .any(|argument| argument == "--unshare-net"));
    }

    #[test]
    fn network_grant_preserves_filesystem_read_only_mounts() {
        let request = ExecRequest {
            argv: vec!["true".into()],
            cwd: "/tmp".into(),
            env: None,
            timeout_ms: None,
        };
        let mut policy = SandboxPolicy {
            network_allowed: true,
            ..Default::default()
        };
        policy.enforce_filesystem_read_only();
        let plan = build_bwrap_plan(&request, &policy, &RuntimeEnv::new()).unwrap();
        assert!(!plan
            .arguments
            .iter()
            .any(|argument| argument == "--unshare-net"));
        assert!(!plan.arguments.iter().any(|argument| argument == "--bind"));
        assert!(plan
            .arguments
            .windows(3)
            .any(|part| part[0] == "--ro-bind" && part[1] == "/" && part[2] == "/"));
    }
}
