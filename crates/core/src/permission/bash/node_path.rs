//! `path.resolve` / `isAbsolute` / `dirname` / `basename` over plain strings, for the
//! bash permission gates (which run on already-absolute, cwd-relative operands) and the
//! engine's on-disk layout helpers.
//!
//! The dialect follows the host. A hand-written POSIX-only version treated `C:\work` as
//! relative, so every gate pasted it onto the process cwd and then compared two paths
//! that could never be equal — on Windows the containment checks silently stopped
//! containing anything. The semantics come from the pinned Node implementation in
//! `runtime_process::path` rather than a second hand-written copy.

use crate::fs::node_path::{host_path_dialect, process_cwd_snapshot};
use crate::runtime_process::{
    path_basename, path_dirname, path_is_absolute, path_join, path_relative, path_resolve,
    NodePathDialect,
};
use crate::runtime_values::RuntimePath;

/// The separator this host's dialect emits, for callers that compare a resolved path
/// against a `<root><separator>` prefix.
pub const SEPARATOR: &str = if cfg!(windows) { "\\" } else { "/" };

/// Node `path.isAbsolute` for the host dialect.
pub fn is_absolute(p: &str) -> bool {
    path_is_absolute(host_path_dialect(), &RuntimePath::from(p))
}

/// Node `path.resolve(...segments)` for the host dialect, against the process cwd.
pub fn resolve(segments: &[&str]) -> String {
    let paths = segments
        .iter()
        .copied()
        .map(RuntimePath::from)
        .collect::<Vec<_>>();
    let resolved = path_resolve(host_path_dialect(), &paths, &mut process_cwd_snapshot())
        .expect("a snapshot with the process cwd and every drive cwd resolves exactly");
    plain(&resolved)
}

/// Node `path.join` for the host dialect. Empty segments are dropped, as Node does.
pub fn join(parts: &[&str]) -> String {
    let paths = parts
        .iter()
        .copied()
        .filter(|part| !part.is_empty())
        .map(RuntimePath::from)
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return ".".to_string();
    }
    plain(&path_join(host_path_dialect(), &paths))
}

/// Node `path.dirname` for the host dialect.
pub fn dirname(s: &str) -> String {
    plain(&path_dirname(host_path_dialect(), &RuntimePath::from(s)))
}

/// Node `path.relative(from, to)` for the host dialect.
pub fn relative(from: &str, to: &str) -> String {
    let relative = path_relative(
        host_path_dialect(),
        &RuntimePath::from(from),
        &RuntimePath::from(to),
        &mut process_cwd_snapshot(),
    )
    .expect("a snapshot with the process cwd and every drive cwd resolves exactly");
    plain(&relative)
}

/// Node `path.basename` for the host dialect (no suffix stripping).
pub fn basename(s: &str) -> String {
    plain(&path_basename(
        host_path_dialect(),
        &RuntimePath::from(s),
        None,
    ))
}

/// Node `path.posix.*` semantics regardless of host, for the operands that are POSIX by
/// construction (a `/`-normalized bash word, a rule pattern) rather than by host.
pub fn posix_dirname(s: &str) -> String {
    plain(&path_dirname(NodePathDialect::Posix, &RuntimePath::from(s)))
}

/// Render a host path for model-facing text: separators become `/`, nothing else changes.
///
/// The engine tells the model on Windows to use forward slashes in paths (see
/// `context::env_info::shell_line`), and every other path it hands the model already
/// follows that. A directory printed straight into a prompt with `\` contradicted that
/// instruction on one platform only.
pub fn posix_rendered(s: &str) -> String {
    if cfg!(windows) {
        s.replace('\\', "/")
    } else {
        s.to_string()
    }
}

/// Render a resolved path back as a plain string. Every input here arrives as `&str`, and
/// neither resolve nor dirname invents unpaired surrogates, so the projection is exact;
/// the lossy arm exists only to keep the signature infallible.
fn plain(path: &RuntimePath) -> String {
    let value = path.as_runtime_string();
    value
        .try_to_utf8()
        .unwrap_or_else(|_| String::from_utf8_lossy(&value.to_node_utf8()).into_owned())
}
