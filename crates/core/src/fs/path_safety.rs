//! Write/edit path-safety guards: reject protected paths, resolve symlinks on
//! the existing parent to defeat symlink escape, keep the path within an allowed
//! root, and refuse to overwrite a symlink. (B7)
//!
//! Rather than threading a full tool-context slice, here we
//! take a lightweight [`PathContext`] (cwd + writable roots + the
//! unconfined-writes opt-out) so the guards don't depend on constructing a full
//! tool context.

#[cfg(any(windows, test))]
use crate::fs::node_path::win32_is_absolute_runtime;
use crate::fs::node_path::{
    basename_runtime, components_runtime, dirname_runtime, project_runtime_path, relative_runtime,
    resolve_runtime, runtime_path_from_native,
};
use crate::runtime_values::{RuntimePath, RuntimeString};
use crate::types::permission::SandboxMode;

const PROTECTED_SEGMENTS: [&str; 1] = [".git"];
const PROTECTED_BASENAMES: [&str; 1] = [".env"];

/// Whether the canonical filesystem policy allows unrestricted write targets.
pub fn writes_are_unconfined(mode: Option<SandboxMode>) -> bool {
    mode == Some(SandboxMode::DangerFullAccess)
}

/// The subset of tool context the path guards need.
pub struct PathContext<'a> {
    pub cwd: &'a RuntimePath,
    pub additional_writable_roots: &'a [RuntimePath],
    /// Full filesystem access waives protected-path and root confinement checks.
    /// Symlink overwrite protection still applies to the named target.
    pub unconfined_writes: bool,
}

pub fn writable_roots(ctx: &PathContext) -> Vec<RuntimePath> {
    std::iter::once(ctx.cwd.clone())
        .chain(ctx.additional_writable_roots.iter().cloned())
        .map(|root| {
            resolve_runtime(ctx.cwd, &[root])
                .expect("an explicit exact cwd resolves permission roots")
        })
        .collect()
}

/// macOS aliases `/private/var` and `/private/tmp` as `/var` and `/tmp`; fold
/// both so containment treats them as equal. No-op on Linux/Windows.
fn deprivate(path: &RuntimePath) -> RuntimePath {
    let value = path.as_runtime_string();
    if value.starts_with("/private/var/") {
        return RuntimePath::from(RuntimeString::from("/var/").concat(&value.slice_utf16(13..)));
    }
    if value == "/private/tmp" {
        return RuntimePath::from("/tmp");
    }
    if value.starts_with("/private/tmp/") {
        return RuntimePath::from(RuntimeString::from("/tmp/").concat(&value.slice_utf16(13..)));
    }
    path.clone()
}

/// POSIX containment predicate: production on unix hosts, exercised by tests
/// everywhere.
#[cfg(any(not(windows), test))]
fn is_posix_contained_relative(relative: &RuntimePath) -> bool {
    let relative = relative.as_runtime_string();
    relative.is_empty()
        || (relative != ".." && !relative.starts_with("../") && !relative.starts_with("/"))
}

/// Win32 containment predicate: production on Windows hosts, exercised by
/// tests everywhere.
#[cfg(any(windows, test))]
fn is_win32_contained_relative(relative: &RuntimePath) -> bool {
    let value = relative.as_runtime_string();
    value.is_empty()
        || (value != ".."
            && !value.starts_with("../")
            && !value.starts_with("..\\")
            && !win32_is_absolute_runtime(relative))
}

fn is_contained_relative(relative: &RuntimePath) -> bool {
    #[cfg(not(windows))]
    {
        is_posix_contained_relative(relative)
    }
    #[cfg(windows)]
    {
        is_win32_contained_relative(relative)
    }
}

pub fn is_within_roots(path: &RuntimePath, roots: &[RuntimePath]) -> bool {
    let Some(cwd) = roots.first() else {
        return false;
    };
    let abs = deprivate(
        &resolve_runtime(cwd, std::slice::from_ref(path))
            .expect("an explicit permission root resolves a candidate path"),
    );
    roots.iter().any(|root| {
        let root = deprivate(
            &resolve_runtime(cwd, std::slice::from_ref(root))
                .expect("an explicit permission root resolves itself"),
        );
        let rel = relative_runtime(cwd, &root, &abs)
            .expect("absolute permission paths compute an exact relative path");
        is_contained_relative(&rel)
    })
}

pub fn is_protected_path(path: &RuntimePath) -> bool {
    let abs = resolve_runtime(path, std::slice::from_ref(path))
        .expect("an explicit path resolves without reading process cwd");
    let segments = components_runtime(&abs);
    if segments
        .iter()
        .any(|segment| PROTECTED_SEGMENTS.iter().any(|value| segment == value))
    {
        return true;
    }
    matches!(segments.last(), Some(base) if PROTECTED_BASENAMES.iter().any(|value| base == value))
}

fn basename_of(path: &RuntimePath) -> RuntimePath {
    basename_runtime(path)
}

/// Resolve a core-owned descendant against a canonical anchor without allowing
/// an existing symlink below that anchor to redefine the authorized scope.
///
/// The anchor itself may be a deliberate host alias (for example a symlinked
/// config home), so it is canonicalized first. Every existing descendant
/// component must then be a non-symlink and must resolve to the path obtained by
/// appending the lexical relative suffix to that canonical anchor. Missing
/// descendants are supported so a cold project-memory directory can be
/// authorized before it is created; unexpected I/O failures are fail-closed.
pub fn canonical_descendant_scope(
    anchor: &RuntimePath,
    descendant: &RuntimePath,
) -> Option<RuntimePath> {
    let anchor = resolve_runtime(anchor, std::slice::from_ref(anchor)).ok()?;
    let descendant = resolve_runtime(&anchor, std::slice::from_ref(descendant)).ok()?;
    let relative = relative_runtime(&anchor, &anchor, &descendant).ok()?;
    if !is_contained_relative(&relative) {
        return None;
    }

    let mut current = anchor.clone();
    for component in components_runtime(&relative) {
        current = resolve_runtime(&current, &[RuntimePath::from(component)]).ok()?;
        let projected = project_runtime_path(&current, "canonical scope descendant").ok()?;
        match std::fs::symlink_metadata(projected) {
            Ok(metadata) if metadata.file_type().is_symlink() => return None,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => return None,
        }
    }

    let canonical_anchor = real_parent_resolve_sync(&anchor)?;
    let expected = resolve_runtime(&canonical_anchor, &[relative]).ok()?;
    let actual = real_parent_resolve_sync(&descendant)?;
    (actual == expected).then_some(expected)
}

/// Synchronous counterpart of [`real_parent_resolve`] for construction-time
/// scope authorization. Unlike the write-time fallback, non-NotFound I/O
/// errors are rejected because an internal permission exemption must be
/// established fail-closed.
fn real_parent_resolve_sync(abs: &RuntimePath) -> Option<RuntimePath> {
    let projected = project_runtime_path(abs, "canonical scope").ok()?;
    match crate::fs::node_path::canonicalize_host(projected) {
        Ok(real) => return Some(runtime_path_from_native(&real)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return None,
    }

    let mut tail: Vec<RuntimePath> = Vec::new();
    let mut dir = abs.clone();
    while dir != dirname_runtime(&dir) {
        tail.insert(0, basename_of(&dir));
        dir = dirname_runtime(&dir);
        let projected = project_runtime_path(&dir, "canonical scope ancestor").ok()?;
        match crate::fs::node_path::canonicalize_host(projected) {
            Ok(real_dir) => {
                let real_dir = runtime_path_from_native(&real_dir);
                let mut parts = Vec::with_capacity(tail.len() + 1);
                parts.push(real_dir.clone());
                parts.extend(tail.iter().cloned());
                return resolve_runtime(&real_dir, &parts).ok();
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    None
}

/// The path forms a permission rule should match against: the lexical absolute
/// path plus, when it differs, the symlink-resolved real path (resolving the
/// nearest existing ancestor when the leaf doesn't exist yet).
pub async fn get_paths_for_permission_check(path: &RuntimePath) -> Vec<RuntimePath> {
    let abs = resolve_runtime(path, std::slice::from_ref(path))
        .expect("an explicit path resolves without reading process cwd");
    let mut out = vec![abs.clone()];
    let projected = match project_runtime_path(&abs, "path") {
        Ok(path) => path,
        Err(_) => return out,
    };
    match tokio::fs::canonicalize(projected)
        .await
        .map(crate::fs::node_path::simplify_host)
    {
        Ok(real) => {
            let real = runtime_path_from_native(&real);
            if real != abs {
                out.push(real);
            }
        }
        Err(_) => {
            if let Some(resolved) = real_parent_resolve(&abs).await {
                if resolved != abs {
                    out.push(resolved);
                }
            }
        }
    }
    out
}

/// Resolve the nearest existing ancestor of `abs` via realpath, then re-append
/// the missing tail.
async fn real_parent_resolve(abs: &RuntimePath) -> Option<RuntimePath> {
    let mut tail: Vec<RuntimePath> = Vec::new();
    let mut dir = abs.clone();
    while dir != dirname_runtime(&dir) {
        tail.insert(0, basename_of(&dir));
        dir = dirname_runtime(&dir);
        let projected = project_runtime_path(&dir, "path").ok()?;
        if let Ok(real_dir) = tokio::fs::canonicalize(projected)
            .await
            .map(crate::fs::node_path::simplify_host)
        {
            let real_dir = runtime_path_from_native(&real_dir);
            let mut parts = Vec::with_capacity(tail.len() + 1);
            parts.push(real_dir.clone());
            parts.extend(tail.iter().cloned());
            return resolve_runtime(&real_dir, &parts).ok();
        }
    }
    None
}

/// Guard a write/edit target (see module docs). Under `unconfined_writes` only the
/// symlink-overwrite refusal remains: that one protects the file the user did not
/// name, so it is not part of what a permission mode waives.
pub async fn assert_writable(
    path: &RuntimePath,
    ctx: &PathContext<'_>,
) -> Result<(), RuntimeString> {
    let abs = resolve_runtime(ctx.cwd, std::slice::from_ref(path))
        .map_err(|error| error.message().clone())?;
    if !ctx.unconfined_writes {
        if is_protected_path(&abs) {
            return Err(RuntimeString::from("Refusing to write protected path: ")
                .concat(path.as_runtime_string()));
        }

        // Realpath the roots too (macOS /var → /private/var symlinks).
        let mut roots: Vec<RuntimePath> = Vec::new();
        for r in writable_roots(ctx) {
            let projected =
                project_runtime_path(&r, "path").map_err(|error| error.message().clone())?;
            match tokio::fs::canonicalize(projected)
                .await
                .map(crate::fs::node_path::simplify_host)
            {
                Ok(p) => roots.push(runtime_path_from_native(&p)),
                Err(_) => roots.push(real_parent_resolve(&r).await.unwrap_or(r)),
            }
        }

        // Resolve the nearest existing ancestor, then re-append the missing tail.
        // Canonicalizing only the immediate parent is insufficient for paths such
        // as `root/escape/new/file` where `escape` is a symlink and `new` does not
        // exist yet (write tools create it recursively).
        let real_target = real_parent_resolve(&abs)
            .await
            .unwrap_or_else(|| abs.clone());
        if is_protected_path(&real_target) {
            return Err(RuntimeString::from("Refusing to write protected path: ")
                .concat(path.as_runtime_string()));
        }
        if !is_within_roots(&real_target, &roots) {
            return Err(RuntimeString::from("Path is outside the writable roots: ")
                .concat(path.as_runtime_string()));
        }
    }

    // Refuse to overwrite an existing symlink.
    let projected_abs =
        project_runtime_path(&abs, "path").map_err(|error| error.message().clone())?;
    if let Ok(md) = tokio::fs::symlink_metadata(projected_abs).await {
        if md.file_type().is_symlink() {
            return Err(RuntimeString::from("Refusing to overwrite a symlink: ")
                .concat(path.as_runtime_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use std::path::PathBuf;

    fn make_temp_dir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let uniq = format!(
            "ae-sec-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(uniq);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ctx_of(dir: &RuntimePath) -> PathContext<'_> {
        PathContext {
            cwd: dir,
            additional_writable_roots: &[],
            unconfined_writes: false,
        }
    }

    fn unconfined_ctx_of(dir: &RuntimePath) -> PathContext<'_> {
        PathContext {
            cwd: dir,
            additional_writable_roots: &[],
            unconfined_writes: true,
        }
    }

    #[test]
    fn protected_and_within_roots_pure() {
        assert!(is_protected_path(&RuntimePath::from("/repo/.git/config")));
        assert!(is_protected_path(&RuntimePath::from("/repo/.env")));
        assert!(!is_protected_path(&RuntimePath::from("/repo/src/main.rs")));
        assert!(is_within_roots(
            &RuntimePath::from("/repo/src/a.rs"),
            &[RuntimePath::from("/repo")]
        ));
        assert!(!is_within_roots(
            &RuntimePath::from("/etc/passwd"),
            &[RuntimePath::from("/repo")]
        ));
    }

    #[test]
    fn containment_relative_shape_is_platform_native() {
        for value in ["", "src/main.rs", r"..\escape", r"C:\absolute"] {
            assert!(is_posix_contained_relative(&RuntimePath::from(value)));
        }
        for value in ["..", "../escape", "/absolute"] {
            assert!(!is_posix_contained_relative(&RuntimePath::from(value)));
        }

        assert!(is_win32_contained_relative(&RuntimePath::from(
            r"src\main.rs"
        )));
        for value in [
            "..",
            r"..\escape",
            r"C:\absolute",
            r"\\server\share\absolute",
        ] {
            assert!(!is_win32_contained_relative(&RuntimePath::from(value)));
        }
    }

    #[tokio::test]
    async fn allows_normal_path_inside_root() {
        let dir = make_temp_dir("ok");
        let cwd = runtime_path_from_native(&dir);
        let target = runtime_path_from_native(&dir.join("ok.txt"));
        assert!(assert_writable(&target, &ctx_of(&cwd)).await.is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn permission_paths_keep_js_identity_but_project_for_realpath_io() {
        let dir = make_temp_dir("node-utf8");
        let cwd = runtime_path_from_native(&dir);
        let actual = dir.join("\u{fffd}.txt");
        std::fs::write(&actual, "x").unwrap();
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800.txt""#)).unwrap();
        let logical =
            resolve_runtime(&cwd, &[RuntimePath::from(parsed.as_str().unwrap().clone())]).unwrap();

        let paths = get_paths_for_permission_check(&logical).await;
        assert_eq!(paths.first(), Some(&logical));
        let actual_lexical = runtime_path_from_native(&actual);
        let actual_real =
            runtime_path_from_native(&crate::fs::node_path::canonicalize_host(&actual).unwrap());
        assert_eq!(paths.get(1), Some(&actual_real));
        assert!(assert_writable(&logical, &ctx_of(&cwd)).await.is_ok());

        let replacement = get_paths_for_permission_check(&actual_lexical).await;
        let mut expected_replacement = vec![actual_lexical.clone()];
        if actual_real != actual_lexical {
            expected_replacement.push(actual_real);
        }
        assert_eq!(replacement, expected_replacement);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn canonical_scope_rejects_a_symlinked_descendant_but_supports_a_cold_root() {
        let dir = make_temp_dir("canonical-scope");
        let anchor = dir.join("config");
        let outside = dir.join("outside");
        std::fs::create_dir_all(anchor.join("projects/project-a")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let linked_root = anchor.join("projects/project-a/memory");
        std::os::unix::fs::symlink(&outside, &linked_root).unwrap();

        assert_eq!(
            canonical_descendant_scope(
                &runtime_path_from_native(&anchor),
                &runtime_path_from_native(&linked_root),
            ),
            None
        );

        let linked_parent = anchor.join("projects/project-linked");
        std::os::unix::fs::symlink(&outside, &linked_parent).unwrap();
        assert_eq!(
            canonical_descendant_scope(
                &runtime_path_from_native(&anchor),
                &runtime_path_from_native(&linked_parent.join("memory")),
            ),
            None,
            "an intermediate descendant symlink must not redefine the canonical scope"
        );

        let cold_root = anchor.join("projects/project-b/memory");
        let expected = crate::fs::node_path::canonicalize_host(&anchor)
            .unwrap()
            .join("projects/project-b/memory");
        assert_eq!(
            canonical_descendant_scope(
                &runtime_path_from_native(&anchor),
                &runtime_path_from_native(&cold_root),
            ),
            Some(runtime_path_from_native(&expected))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_outside_root() {
        let dir = make_temp_dir("out");
        let cwd = runtime_path_from_native(&dir);
        let err = assert_writable(&RuntimePath::from("/etc/passwd"), &ctx_of(&cwd))
            .await
            .unwrap_err();
        assert!(err.contains("outside"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_protected_path() {
        let dir = make_temp_dir("prot");
        let cwd = runtime_path_from_native(&dir);
        let target = runtime_path_from_native(&dir.join(".git").join("config"));
        let err = assert_writable(&target, &ctx_of(&cwd)).await.unwrap_err();
        assert!(err.contains("protected"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_to_overwrite_symlink() {
        let dir = make_temp_dir("link");
        let cwd = runtime_path_from_native(&dir);
        let target = dir.join("real.txt");
        std::fs::write(&target, "x").unwrap();
        let link = dir.join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let link = runtime_path_from_native(&link);
        let err = assert_writable(&link, &ctx_of(&cwd)).await.unwrap_err();
        assert!(err.contains("symlink"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_escape_through_a_missing_descendant() {
        let dir = make_temp_dir("missing-symlink-parent");
        let outside = make_temp_dir("missing-symlink-outside");
        std::os::unix::fs::symlink(&outside, dir.join("escape")).unwrap();
        let cwd = runtime_path_from_native(&dir);
        let target = runtime_path_from_native(&dir.join("escape/new/note.md"));

        let err = assert_writable(&target, &ctx_of(&cwd)).await.unwrap_err();
        assert!(err.contains("outside"), "{err:?}");
        assert!(!outside.join("new").exists());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// Full filesystem access waives the confinement guards: a target outside every
    /// writable root, a `.git` path and a `.env` path all pass.
    #[tokio::test]
    async fn unconfined_writes_waive_root_and_protected_path_refusals() {
        let dir = make_temp_dir("unconfined");
        let outside = make_temp_dir("unconfined-outside");
        let cwd = runtime_path_from_native(&dir);
        for target in [
            runtime_path_from_native(&outside.join("note.md")),
            runtime_path_from_native(&dir.join(".git").join("config")),
            runtime_path_from_native(&dir.join(".env")),
        ] {
            assert!(
                assert_writable(&target, &unconfined_ctx_of(&cwd))
                    .await
                    .is_ok(),
                "expected {target:?} to be writable when writes are unconfined"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// The symlink-overwrite refusal is about the file the caller did not name, so
    /// it survives the unconfined opt-out.
    #[cfg(unix)]
    #[tokio::test]
    async fn unconfined_writes_still_refuse_to_overwrite_a_symlink() {
        let dir = make_temp_dir("unconfined-link");
        let cwd = runtime_path_from_native(&dir);
        let target = dir.join("real.txt");
        std::fs::write(&target, "x").unwrap();
        let link = dir.join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let link = runtime_path_from_native(&link);
        let err = assert_writable(&link, &unconfined_ctx_of(&cwd))
            .await
            .unwrap_err();
        assert!(err.contains("symlink"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
