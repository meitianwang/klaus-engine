//! The subset of Node's platform-native `node:path` contract used by filesystem
//! helpers. The production surface works on exact [`RuntimePath`] values and
//! delegates per host dialect. The string-based win32 helpers further down are
//! test-only: they pin Node's drive/UNC/backslash semantics so they can be
//! verified on any host. The string-based POSIX path handling the bash
//! permission gates actually run in production lives in
//! `crate::permission::bash::node_path`.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

use crate::runtime_process::fs_path_projection::{
    Node20FsPathProjection, Node20FsPathProjectionError,
};
use crate::runtime_process::{
    path_basename, path_dirname, path_is_absolute, path_relative, path_resolve, NodePathDialect,
    NodePathHostPlatform, RuntimePathCwdError, RuntimePathCwdSnapshot,
};
#[cfg(unix)]
use crate::runtime_values::NodeBufferUtf8;
use crate::runtime_values::{RuntimePath, RuntimeString};

pub(crate) fn host_path_dialect() -> NodePathDialect {
    if cfg!(windows) {
        NodePathDialect::Win32
    } else {
        NodePathDialect::Posix
    }
}

fn host_path_platform() -> NodePathHostPlatform {
    if cfg!(windows) {
        NodePathHostPlatform::Windows
    } else {
        NodePathHostPlatform::Posix
    }
}

fn cwd_snapshot(cwd: &RuntimePath) -> RuntimePathCwdSnapshot {
    #[allow(unused_mut)] // only Windows fills the per-drive entries
    let mut snapshot = RuntimePathCwdSnapshot::new(host_path_platform(), Ok(cwd.clone()));
    #[cfg(windows)]
    populate_windows_drive_cwds(&mut snapshot);
    snapshot
}

#[cfg(windows)]
const fn host_root() -> &'static str {
    r"\"
}

#[cfg(not(windows))]
const fn host_root() -> &'static str {
    "/"
}

/// A snapshot seeded with the real process cwd, for the string-based helpers that
/// resolve without an explicit session cwd.
pub(crate) fn process_cwd_snapshot() -> RuntimePathCwdSnapshot {
    let cwd = std::env::current_dir()
        .map(|path| runtime_path_from_native(&path))
        .unwrap_or_else(|_| RuntimePath::from(host_root()));
    cwd_snapshot(&cwd)
}

/// Record every drive's remembered cwd (`=D:` in the environment) in `snapshot`.
///
/// Not optional on Windows: a drive-relative operand like `D:file` makes Node consult
/// that variable, and the snapshot answers an unrecorded drive with an error rather than
/// a fallback — so skipping this turns every caller's `expect` into a panic on a path
/// shape a command line can contain. Drives with no variable are recorded as absent,
/// which is what makes the resolution infallible.
#[cfg(windows)]
pub(crate) fn populate_windows_drive_cwds(snapshot: &mut RuntimePathCwdSnapshot) {
    use std::os::windows::ffi::OsStrExt;

    for drive in b'A'..=b'Z' {
        snapshot.insert_drive_missing(RuntimeString::from(format!("={}:", char::from(drive))));
    }
    for (key, value) in std::env::vars_os() {
        let key = RuntimeString::from_utf16(key.encode_wide().collect::<Vec<_>>());
        let units = key.as_utf16();
        if units.len() == 3
            && units[0] == u16::from(b'=')
            && units[2] == u16::from(b':')
            && (u16::from(b'A')..=u16::from(b'Z')).contains(&units[1])
        {
            snapshot.insert_drive_value(
                key,
                RuntimeString::from_utf16(value.encode_wide().collect::<Vec<_>>()),
            );
        }
    }
}

/// Resolve exact logical paths with the pinned host `node:path` dialect.
pub(crate) fn resolve_runtime(
    cwd: &RuntimePath,
    paths: &[RuntimePath],
) -> Result<RuntimePath, RuntimePathCwdError> {
    path_resolve(host_path_dialect(), paths, &mut cwd_snapshot(cwd))
}

/// Compute one exact logical relative path without host projection.
pub(crate) fn relative_runtime(
    cwd: &RuntimePath,
    from: &RuntimePath,
    to: &RuntimePath,
) -> Result<RuntimePath, RuntimePathCwdError> {
    path_relative(host_path_dialect(), from, to, &mut cwd_snapshot(cwd))
}

/// A relative path for model-facing output and stored index keys: compared in the host
/// dialect, rendered with `/`.
///
/// The separator is not cosmetic here. The engine tells the model on Windows to use
/// forward slashes (see `context::env_info::shell_line`) and its committed traces pin that
/// shape, so emitting `\` would change the wire on one platform only. The comparison
/// itself still has to run in the host dialect, or a Windows path never resolves against
/// its own root.
pub(crate) fn relative_posix_runtime(
    cwd: &RuntimePath,
    from: &RuntimePath,
    to: &RuntimePath,
) -> Result<RuntimePath, RuntimePathCwdError> {
    Ok(posix_rendered_runtime(&relative_runtime(cwd, from, to)?))
}

/// Render a resolved host path for model-facing output: separators become `/`, nothing else
/// changes.
///
/// Same contract as `relative_posix_runtime`, for the paths a tool echoes back whole instead
/// of relativizing. Leaving the host separator in place made the write tool report
/// `C:\repo\a.txt` on Windows while every other path the model saw used `/`, and the
/// committed provider-behavior contract holds the `/` shape on every host.
pub(crate) fn posix_rendered_runtime(path: &RuntimePath) -> RuntimePath {
    if !cfg!(windows) {
        return path.clone();
    }
    const BACKSLASH: u16 = b'\\' as u16;
    const SLASH: u16 = b'/' as u16;
    let units = path
        .as_runtime_string()
        .as_utf16()
        .iter()
        .map(|unit| if *unit == BACKSLASH { SLASH } else { *unit })
        .collect::<Vec<_>>();
    RuntimePath::from(RuntimeString::from_utf16(units))
}

pub(crate) fn dirname_runtime(path: &RuntimePath) -> RuntimePath {
    path_dirname(host_path_dialect(), path)
}

pub(crate) fn basename_runtime(path: &RuntimePath) -> RuntimePath {
    path_basename(host_path_dialect(), path, None)
}

pub(crate) fn is_absolute_runtime(path: &RuntimePath) -> bool {
    path_is_absolute(host_path_dialect(), path)
}

/// Node's `path.win32.isAbsolute` on exact logical paths. Consumed in
/// production only by the Windows containment check in `fs::path_safety` and
/// pinned by tests on every host.
#[cfg(any(windows, test))]
pub(crate) fn win32_is_absolute_runtime(path: &RuntimePath) -> bool {
    path_is_absolute(NodePathDialect::Win32, path)
}

pub(crate) fn components_runtime(path: &RuntimePath) -> Vec<RuntimeString> {
    let is_separator = |unit| match host_path_dialect() {
        NodePathDialect::Posix => unit == u16::from(b'/'),
        NodePathDialect::Win32 => unit == u16::from(b'/') || unit == u16::from(b'\\'),
    };
    let units = path.as_runtime_string().as_utf16();
    let mut output = Vec::new();
    let mut start = 0;
    for index in 0..=units.len() {
        if index != units.len() && !is_separator(units[index]) {
            continue;
        }
        if start < index {
            output.push(RuntimeString::from_utf16(units[start..index].to_vec()));
        }
        start = index.saturating_add(1);
    }
    output
}

/// Select the sole logical-path to native-path terminal immediately before a
/// real filesystem syscall.
pub(crate) fn project_runtime_path(
    path: &RuntimePath,
    argument_label: &str,
) -> Result<PathBuf, Node20FsPathProjectionError> {
    let native = Node20FsPathProjection::project(path, argument_label)?;
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(PathBuf::from(OsString::from_vec(
            native.as_bytes().to_vec(),
        )))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        Ok(PathBuf::from(OsString::from_wide(native.as_wide())))
    }
}

/// Decode one native filesystem result according to Node's platform ingress.
pub(crate) fn runtime_string_from_native(value: &OsStr) -> RuntimeString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        NodeBufferUtf8::decode(value.as_bytes())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        RuntimeString::from_utf16(value.encode_wide().collect::<Vec<_>>())
    }
}

pub(crate) fn runtime_path_from_native(value: &Path) -> RuntimePath {
    RuntimePath::from(runtime_string_from_native(value.as_os_str()))
}

/// Canonicalize `path`, without the Windows verbatim prefix.
///
/// `std::fs::canonicalize` answers `\\?\C:\…` on Windows. Nothing in the engine wants that
/// form: it fails every comparison against a path the engine built itself, and it splits
/// into the segments `?` and `C:`, which the sensitive-path classifier reads as an
/// alternate-data-stream colon and refuses outright — a plan file under the user's own
/// config directory came back "suspicious path pattern". The prefix is dropped here so no
/// caller has to remember to.
pub(crate) fn canonicalize_host<P: AsRef<Path>>(path: P) -> std::io::Result<PathBuf> {
    dunce::canonicalize(path)
}

/// Drop the Windows verbatim prefix from an already-canonicalized path, for the async
/// call sites that go through `tokio::fs::canonicalize`.
pub(crate) fn simplify_host(path: PathBuf) -> PathBuf {
    dunce::simplified(&path).to_path_buf()
}

// ---------------------------------------------------------------------------
// Test-only string-based win32 path helpers. They pin Node's `path.win32`
// semantics (drive roots, UNC shares, case folding) so they can be verified on
// any host; no production code path consumes them — the bash permission gates'
// string-based POSIX implementation lives in `permission::bash::node_path`,
// and everything else works on the exact `RuntimePath` API above.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[derive(Debug, Default)]
struct WinRoot {
    device: String,
    root_end: usize,
    is_absolute: bool,
}

#[cfg(test)]
fn is_win_separator(byte: u8) -> bool {
    matches!(byte, b'/' | b'\\')
}

#[cfg(test)]
fn is_win_device_root(byte: u8) -> bool {
    byte.is_ascii_alphabetic()
}

#[cfg(test)]
fn win_case_eq(left: &str, right: &str) -> bool {
    left.to_lowercase() == right.to_lowercase()
}

#[cfg(test)]
fn parse_win_root(path: &str) -> WinRoot {
    let bytes = path.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return WinRoot::default();
    }

    if is_win_separator(bytes[0]) {
        let mut root = WinRoot {
            is_absolute: true,
            ..WinRoot::default()
        };
        if len == 1 {
            root.root_end = 1;
            return root;
        }
        if !is_win_separator(bytes[1]) {
            root.root_end = 1;
            return root;
        }

        let mut cursor = 2;
        let server_start = cursor;
        while cursor < len && !is_win_separator(bytes[cursor]) {
            cursor += 1;
        }
        if cursor >= len || cursor == server_start {
            return root;
        }
        let server = &path[server_start..cursor];
        let separators_start = cursor;
        while cursor < len && is_win_separator(bytes[cursor]) {
            cursor += 1;
        }
        if cursor >= len || cursor == separators_start {
            return root;
        }
        let share_start = cursor;
        while cursor < len && !is_win_separator(bytes[cursor]) {
            cursor += 1;
        }
        if cursor == share_start {
            return root;
        }
        root.device = format!(r"\\{server}\{}", &path[share_start..cursor]);
        root.root_end = cursor;
        return root;
    }

    if len >= 2 && is_win_device_root(bytes[0]) && bytes[1] == b':' {
        let is_absolute = len > 2 && is_win_separator(bytes[2]);
        return WinRoot {
            device: path[..2].to_string(),
            root_end: if is_absolute { 3 } else { 2 },
            is_absolute,
        };
    }

    WinRoot::default()
}

#[cfg(test)]
fn normalize_win_tail(path: &str, allow_above_root: bool) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => {
                if matches!(parts.last(), Some(last) if *last != "..") {
                    parts.pop();
                } else if allow_above_root {
                    parts.push(part);
                }
            }
            _ => parts.push(part),
        }
    }
    parts.join("\\")
}

#[cfg(test)]
pub(crate) fn win32_is_absolute(path: &str) -> bool {
    let bytes = path.as_bytes();
    !bytes.is_empty()
        && (is_win_separator(bytes[0])
            || (bytes.len() > 2
                && is_win_device_root(bytes[0])
                && bytes[1] == b':'
                && is_win_separator(bytes[2])))
}

#[cfg(test)]
pub(crate) fn win32_resolve_with_cwd(segments: &[&str], cwd: &str) -> String {
    let mut resolved_device = String::new();
    let mut resolved_tail = String::new();
    let mut resolved_absolute = false;
    let mut index = segments.len() as isize - 1;

    while index >= -1 {
        let owned;
        let path = if index >= 0 {
            segments[index as usize]
        } else if resolved_device.is_empty() {
            cwd
        } else {
            let cwd_root = parse_win_root(cwd);
            if !cwd_root.device.is_empty() && win_case_eq(&cwd_root.device, &resolved_device) {
                cwd
            } else {
                owned = format!("{resolved_device}\\");
                &owned
            }
        };
        index -= 1;

        if path.is_empty() {
            continue;
        }
        let root = parse_win_root(path);
        if !root.device.is_empty() {
            if !resolved_device.is_empty() && !win_case_eq(&root.device, &resolved_device) {
                continue;
            }
            if resolved_device.is_empty() {
                resolved_device = root.device.clone();
            }
        }

        if resolved_absolute {
            if !resolved_device.is_empty() {
                break;
            }
        } else {
            let tail = &path[root.root_end..];
            resolved_tail = if resolved_tail.is_empty() {
                format!("{tail}\\")
            } else {
                format!("{tail}\\{resolved_tail}")
            };
            resolved_absolute = root.is_absolute;
            if root.is_absolute && !resolved_device.is_empty() {
                break;
            }
        }
    }

    let tail = normalize_win_tail(&resolved_tail, !resolved_absolute);
    if resolved_absolute {
        format!("{resolved_device}\\{tail}")
    } else {
        let resolved = format!("{resolved_device}{tail}");
        if resolved.is_empty() {
            ".".to_string()
        } else {
            resolved
        }
    }
}

#[cfg(test)]
pub(crate) fn win32_relative_with_cwd(from: &str, to: &str, cwd: &str) -> String {
    if from == to {
        return String::new();
    }
    let from_resolved = win32_resolve_with_cwd(&[from], cwd);
    let to_resolved = win32_resolve_with_cwd(&[to], cwd);
    if win_case_eq(&from_resolved, &to_resolved) {
        return String::new();
    }

    let from_parts: Vec<&str> = from_resolved
        .split('\\')
        .filter(|part| !part.is_empty())
        .collect();
    let to_parts: Vec<&str> = to_resolved
        .split('\\')
        .filter(|part| !part.is_empty())
        .collect();
    let mut common = 0;
    while common < from_parts.len()
        && common < to_parts.len()
        && win_case_eq(from_parts[common], to_parts[common])
    {
        common += 1;
    }
    if common == 0 {
        return to_resolved;
    }

    let mut result = vec![".."; from_parts.len() - common];
    result.extend_from_slice(&to_parts[common..]);
    result.join("\\")
}

#[cfg(test)]
pub(crate) fn win32_dirname(path: &str) -> String {
    let bytes = path.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return ".".to_string();
    }
    if len == 1 {
        return if is_win_separator(bytes[0]) {
            path.to_string()
        } else {
            ".".to_string()
        };
    }

    let mut root_end: Option<usize> = None;
    let mut offset = 0;
    if is_win_separator(bytes[0]) {
        root_end = Some(1);
        offset = 1;
        if is_win_separator(bytes[1]) {
            let mut cursor = 2;
            let server_start = cursor;
            while cursor < len && !is_win_separator(bytes[cursor]) {
                cursor += 1;
            }
            if cursor < len && cursor != server_start {
                let separators_start = cursor;
                while cursor < len && is_win_separator(bytes[cursor]) {
                    cursor += 1;
                }
                if cursor < len && cursor != separators_start {
                    let share_start = cursor;
                    while cursor < len && !is_win_separator(bytes[cursor]) {
                        cursor += 1;
                    }
                    if cursor == len {
                        return path.to_string();
                    }
                    if cursor != share_start {
                        root_end = Some(cursor + 1);
                        offset = cursor + 1;
                    }
                }
            }
        }
    } else if is_win_device_root(bytes[0]) && bytes[1] == b':' {
        let end = if len > 2 && is_win_separator(bytes[2]) {
            3
        } else {
            2
        };
        root_end = Some(end);
        offset = end;
    }

    let mut end = None;
    let mut matched_separator = true;
    for index in (offset..len).rev() {
        if is_win_separator(bytes[index]) {
            if !matched_separator {
                end = Some(index);
                break;
            }
        } else {
            matched_separator = false;
        }
    }
    match end.or(root_end) {
        Some(end) => path[..end].to_string(),
        None => ".".to_string(),
    }
}

#[cfg(test)]
pub(crate) fn win32_basename(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut start = if bytes.len() >= 2 && is_win_device_root(bytes[0]) && bytes[1] == b':' {
        2
    } else {
        0
    };
    let mut end = None;
    let mut matched_separator = true;
    for index in (start..bytes.len()).rev() {
        if is_win_separator(bytes[index]) {
            if !matched_separator {
                start = index + 1;
                break;
            }
        } else {
            if end.is_none() {
                end = Some(index + 1);
            }
            matched_separator = false;
        }
    }
    end.map_or_else(String::new, |end| path[start..end].to_string())
}

#[cfg(test)]
pub(crate) fn win32_components(path: &str) -> Vec<&str> {
    path.split(['/', '\\'])
        .filter(|part| !part.is_empty())
        .collect()
}

#[cfg(all(test, not(windows)))]
pub(crate) fn is_absolute(path: &str) -> bool {
    crate::permission::bash::node_path::is_absolute(path)
}

#[cfg(all(test, not(windows)))]
pub(crate) fn resolve(segments: &[&str]) -> String {
    crate::permission::bash::node_path::resolve(segments)
}

#[cfg(all(test, not(windows)))]
pub(crate) fn dirname(path: &str) -> String {
    crate::permission::bash::node_path::dirname(path)
}

#[cfg(all(test, not(windows)))]
pub(crate) fn basename(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    trimmed.rsplit('/').next().unwrap_or_default().to_string()
}

#[cfg(all(test, not(windows)))]
pub(crate) fn relative(from: &str, to: &str) -> String {
    let from = resolve(&[from]);
    let to = resolve(&[to]);
    if from == to {
        return String::new();
    }
    let from_parts: Vec<&str> = from.split('/').filter(|part| !part.is_empty()).collect();
    let to_parts: Vec<&str> = to.split('/').filter(|part| !part.is_empty()).collect();
    let mut common = 0;
    while common < from_parts.len()
        && common < to_parts.len()
        && from_parts[common] == to_parts[common]
    {
        common += 1;
    }
    let mut result = vec![".."; from_parts.len() - common];
    result.extend_from_slice(&to_parts[common..]);
    result.join("/")
}

#[cfg(test)]
mod tests {
    use super::{
        win32_basename, win32_components, win32_dirname, win32_is_absolute,
        win32_relative_with_cwd, win32_resolve_with_cwd,
    };

    #[test]
    fn win32_absolute_detection_matches_node() {
        assert!(win32_is_absolute(r"C:\repo"));
        assert!(win32_is_absolute(r"\repo"));
        assert!(win32_is_absolute(r"\\server\share\dir"));
        assert!(!win32_is_absolute(r"C:repo"));
        assert!(!win32_is_absolute("repo\\file"));
    }

    #[test]
    fn win32_resolve_matches_node_for_drive_unc_and_relative_paths() {
        let cwd = r"C:\repo";
        assert_eq!(
            win32_resolve_with_cwd(&[cwd, r"src\..\a"], cwd),
            r"C:\repo\a"
        );
        assert_eq!(win32_resolve_with_cwd(&[cwd, r"\tmp\x"], cwd), r"C:\tmp\x");
        assert_eq!(win32_resolve_with_cwd(&[cwd, r"D:\x"], cwd), r"D:\x");
        assert_eq!(
            win32_resolve_with_cwd(&[cwd, r"\\server\share\dir\..\x"], cwd),
            r"\\server\share\x"
        );
        assert_eq!(
            win32_resolve_with_cwd(&[cwd, r"C:child\file"], cwd),
            r"C:\repo\child\file"
        );
        assert_eq!(
            win32_resolve_with_cwd(&[r"\\server\share\repo", r"..\x"], cwd),
            r"\\server\share\x"
        );
    }

    #[test]
    fn win32_relative_matches_node_across_roots_and_case() {
        let cwd = r"C:\repo";
        assert_eq!(
            win32_relative_with_cwd(r"C:\repo", r"C:\repo\a\b", cwd),
            r"a\b"
        );
        assert_eq!(win32_relative_with_cwd(r"C:\Repo", r"c:\repo\A", cwd), "A");
        assert_eq!(win32_relative_with_cwd(r"C:\Ä", r"c:\ä\X", cwd), "X");
        assert_eq!(win32_relative_with_cwd(r"C:\repo", r"D:\x", cwd), r"D:\x");
        assert_eq!(
            win32_relative_with_cwd(r"\\server\share\repo", r"\\server\share\repo\a", cwd,),
            "a"
        );
        assert_eq!(
            win32_relative_with_cwd(r"\\server\share1\a", r"\\server\share2\x", cwd,),
            r"..\..\share2\x"
        );
    }

    #[test]
    fn win32_dirname_and_basename_match_node_roots() {
        assert_eq!(win32_dirname(r"C:\repo\a.txt"), r"C:\repo");
        assert_eq!(win32_dirname(r"C:\"), r"C:\");
        assert_eq!(win32_dirname(r"C:repo"), "C:");
        assert_eq!(
            win32_dirname(r"\\server\share\dir\file"),
            r"\\server\share\dir"
        );
        assert_eq!(win32_dirname(r"\\server\share\"), r"\\server\share\");
        assert_eq!(win32_basename(r"C:\repo\a.txt"), "a.txt");
        assert_eq!(win32_basename(r"\\server\share\"), "share");
        assert_eq!(
            win32_components(r"C:\repo/.git\config"),
            vec!["C:", "repo", ".git", "config"]
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn native_helpers_preserve_posix_behavior() {
        use super::{basename, dirname, is_absolute, relative, resolve};

        assert!(is_absolute("/repo"));
        assert!(!is_absolute("repo"));
        assert_eq!(resolve(&["/repo", "src/../a"]), "/repo/a");
        assert_eq!(relative("/repo", "/repo/a/b"), "a/b");
        assert_eq!(dirname("/repo/a.txt"), "/repo");
        assert_eq!(basename("/repo/a.txt"), "a.txt");
    }
}
