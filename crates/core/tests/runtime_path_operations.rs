use klaus_agent_core::runtime_process::{
    path_basename, path_dirname, path_extname, path_format, path_is_absolute, path_join,
    path_normalize, path_parse, path_relative, path_resolve, path_to_namespaced_path,
    NodePathDialect, NodePathHostPlatform, RuntimePathCwdError, RuntimePathCwdRead,
    RuntimePathCwdSnapshot, RuntimePathFormatInput, RuntimePathParts,
};
use klaus_agent_core::runtime_values::{RuntimePath, RuntimeString};

fn path(units: impl Into<Vec<u16>>) -> RuntimePath {
    RuntimePath::from_runtime_string(RuntimeString::from_utf16(units))
}

fn scalar(value: &str) -> RuntimePath {
    RuntimePath::from(value)
}

fn units(value: &RuntimePath) -> &[u16] {
    value.as_runtime_string().as_utf16()
}

#[test]
fn resolve_reads_exact_context_lazily_in_node_order() {
    let cwd_error = RuntimePathCwdError::new(scalar_string("ENOENT"), scalar_string("cwd gone"));
    let mut absolute_snapshot =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Err(cwd_error));
    assert_eq!(
        path_resolve(
            NodePathDialect::Win32,
            &[
                scalar("ignored"),
                path(vec![b'D' as u16, b':' as u16, b'\\' as u16, 0xd800])
            ],
            &mut absolute_snapshot,
        )
        .unwrap(),
        path(vec![b'D' as u16, b':' as u16, b'\\' as u16, 0xd800]),
    );
    assert!(absolute_snapshot.reads().is_empty());

    let drive_key = scalar_string("=D:");
    let mut hit =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
    hit.insert_drive_value(drive_key.clone(), scalar_string(r"D:\drivecwd"));
    assert_eq!(
        path_resolve(NodePathDialect::Win32, &[scalar("D:child")], &mut hit).unwrap(),
        scalar(r"D:\drivecwd\child"),
    );
    assert_eq!(
        hit.reads(),
        &[RuntimePathCwdRead::DriveEnv(drive_key.clone())]
    );

    for present_empty in [false, true] {
        let mut miss =
            RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
        if present_empty {
            miss.insert_drive_value(drive_key.clone(), RuntimeString::default());
        } else {
            miss.insert_drive_missing(drive_key.clone());
        }
        assert_eq!(
            path_resolve(NodePathDialect::Win32, &[scalar("D:child")], &mut miss).unwrap(),
            scalar(r"D:\child"),
        );
        assert_eq!(
            miss.reads(),
            &[
                RuntimePathCwdRead::DriveEnv(drive_key.clone()),
                RuntimePathCwdRead::ProcessCwd,
            ],
        );
    }

    let mut wrong_drive =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
    wrong_drive.insert_drive_value(drive_key.clone(), scalar_string(r"C:\wrong"));
    assert_eq!(
        path_resolve(
            NodePathDialect::Win32,
            &[scalar("D:child")],
            &mut wrong_drive,
        )
        .unwrap(),
        scalar(r"D:\child"),
    );
    assert_eq!(
        wrong_drive.reads(),
        &[RuntimePathCwdRead::DriveEnv(drive_key)]
    );
}

#[test]
fn resolve_propagates_the_original_drive_or_cwd_error() {
    let drive_error = RuntimePathCwdError::new(scalar_string("EIO"), scalar_string("drive read"));
    let cwd_error = RuntimePathCwdError::new(scalar_string("ENOENT"), scalar_string("cwd read"));
    let key = scalar_string("=D:");
    let mut drive =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Err(cwd_error.clone()));
    drive.insert_drive_error(key.clone(), drive_error.clone());
    assert_eq!(
        path_resolve(NodePathDialect::Win32, &[scalar("D:child")], &mut drive),
        Err(drive_error),
    );
    assert_eq!(drive.reads(), &[RuntimePathCwdRead::DriveEnv(key.clone())]);

    let mut cwd =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Err(cwd_error.clone()));
    cwd.insert_drive_missing(key.clone());
    assert_eq!(
        path_resolve(NodePathDialect::Win32, &[scalar("D:child")], &mut cwd),
        Err(cwd_error),
    );
    assert_eq!(
        cwd.reads(),
        &[
            RuntimePathCwdRead::DriveEnv(key),
            RuntimePathCwdRead::ProcessCwd,
        ],
    );
}

#[test]
fn posix_resolve_tests_transformed_windows_cwd_before_adding_the_join_separator() {
    for (cwd, input, expected) in [
        ("", "a", "a"),
        ("", "..", ".."),
        (r"C:\proc", "a", "/proc/a"),
    ] {
        let mut snapshot =
            RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(cwd)));
        assert_eq!(
            path_resolve(NodePathDialect::Posix, &[scalar(input)], &mut snapshot).unwrap(),
            scalar(expected),
        );
        assert_eq!(snapshot.reads(), &[RuntimePathCwdRead::ProcessCwd]);
    }
}

#[test]
fn relative_uses_node_20_unicode_lowercase_and_preserves_original_output_units() {
    let mut snapshot =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
    assert_eq!(
        path_relative(
            NodePathDialect::Win32,
            &scalar("C:\\İ"),
            &scalar("c:\\i\u{307}\\X"),
            &mut snapshot,
        )
        .unwrap(),
        scalar("X"),
    );
    assert_eq!(
        path_relative(
            NodePathDialect::Win32,
            &scalar("C:\\ΟΣ"),
            &scalar("c:\\ος\\X"),
            &mut snapshot,
        )
        .unwrap(),
        scalar("X"),
    );
    assert_eq!(
        path_relative(
            NodePathDialect::Win32,
            &path(vec![b'C' as u16, b':' as u16, b'\\' as u16, 0xd800]),
            &path(vec![b'c' as u16, b':' as u16, b'\\' as u16, 0xdc00]),
            &mut snapshot,
        )
        .unwrap(),
        path(vec![b'.' as u16, b'.' as u16, b'\\' as u16, 0xdc00]),
    );
    assert!(snapshot.reads().is_empty());
}

/// A bare root as `from` leaves no segment to climb out of. Node reads one code unit
/// past the end there and gets NaN, which its loop treats as the terminator; the values
/// below come from `path.win32.relative` on node v20.19.4.
#[test]
fn relative_from_a_bare_root_walks_no_parents() {
    let mut snapshot =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
    for (from, to, expected) in [
        (r"\", r"\etc\passwd", r"etc\passwd"),
        (r"\\", r"\etc\passwd", r"etc\passwd"),
        ("/", "/etc/passwd", r"etc\passwd"),
        ("//", "/etc/passwd", r"etc\passwd"),
        (r"\", r"\", ""),
        (r"\", r"\a", "a"),
    ] {
        assert_eq!(
            path_relative(
                NodePathDialect::Win32,
                &scalar(from),
                &scalar(to),
                &mut snapshot,
            )
            .unwrap(),
            scalar(expected),
            "win32 relative({from:?}, {to:?})",
        );
    }
}

#[test]
fn relative_and_namespaced_paths_match_posix_unc_and_drive_contracts() {
    let mut posix = RuntimePathCwdSnapshot::new(NodePathHostPlatform::Posix, Ok(scalar("/cwd")));
    assert_eq!(
        path_relative(
            NodePathDialect::Posix,
            &scalar("a"),
            &path(vec![b'a' as u16, b'/' as u16, 0xd800, 0xdc00]),
            &mut posix,
        )
        .unwrap(),
        path(vec![0xd800, 0xdc00]),
    );
    assert_eq!(
        posix.reads(),
        &[
            RuntimePathCwdRead::ProcessCwd,
            RuntimePathCwdRead::ProcessCwd
        ]
    );

    let mut win =
        RuntimePathCwdSnapshot::new(NodePathHostPlatform::Windows, Ok(scalar(r"C:\proc")));
    assert_eq!(
        path_to_namespaced_path(
            NodePathDialect::Win32,
            &path(vec![
                b'C' as u16,
                b':' as u16,
                b'\\' as u16,
                b'a' as u16,
                b'\\' as u16,
                0xd800
            ]),
            &mut win,
        )
        .unwrap(),
        path(vec![
            b'\\' as u16,
            b'\\' as u16,
            b'?' as u16,
            b'\\' as u16,
            b'C' as u16,
            b':' as u16,
            b'\\' as u16,
            b'a' as u16,
            b'\\' as u16,
            0xd800
        ]),
    );
    assert_eq!(
        path_to_namespaced_path(
            NodePathDialect::Win32,
            &path(vec![
                b'/' as u16,
                b'/' as u16,
                b's' as u16,
                b'r' as u16,
                b'v' as u16,
                b'/' as u16,
                b's' as u16,
                b'h' as u16,
                b'a' as u16,
                b'r' as u16,
                b'e' as u16,
                b'/' as u16,
                0xd800
            ]),
            &mut win,
        )
        .unwrap(),
        path(vec![
            b'\\' as u16,
            b'\\' as u16,
            b'?' as u16,
            b'\\' as u16,
            b'U' as u16,
            b'N' as u16,
            b'C' as u16,
            b'\\' as u16,
            b's' as u16,
            b'r' as u16,
            b'v' as u16,
            b'\\' as u16,
            b's' as u16,
            b'h' as u16,
            b'a' as u16,
            b'r' as u16,
            b'e' as u16,
            b'\\' as u16,
            0xd800
        ]),
    );
    assert!(win.reads().is_empty());
}

fn scalar_string(value: &str) -> RuntimeString {
    RuntimeString::from(value)
}

#[test]
fn normalize_matches_node_20_roots_colons_and_exact_trailing_separators() {
    let high = 0xd800;
    let low = 0xdc00;
    for (dialect, input, expected) in [
        (NodePathDialect::Posix, scalar(""), scalar(".")),
        (NodePathDialect::Posix, scalar("a/./"), scalar("a/")),
        (NodePathDialect::Posix, scalar("//a///b/../"), scalar("/a/")),
        (
            NodePathDialect::Posix,
            path(vec![
                b'/' as u16,
                high,
                b'/' as u16,
                b'.' as u16,
                b'/' as u16,
                low,
            ]),
            path(vec![b'/' as u16, high, b'/' as u16, low]),
        ),
        (NodePathDialect::Win32, scalar(""), scalar(".")),
        (
            NodePathDialect::Win32,
            scalar("//server/share/a/../"),
            scalar("\\\\server\\share\\"),
        ),
        (
            NodePathDialect::Win32,
            scalar("foo:bar/../C:evil"),
            scalar(".\\C:evil"),
        ),
        (NodePathDialect::Win32, scalar("CON:"), scalar(".\\CON:.")),
        (
            NodePathDialect::Win32,
            path(vec![
                b'C' as u16,
                b':' as u16,
                b'/' as u16,
                high,
                b'\\' as u16,
                b'.' as u16,
                b'/' as u16,
                low,
                b'/' as u16,
            ]),
            path(vec![
                b'C' as u16,
                b':' as u16,
                b'\\' as u16,
                high,
                b'\\' as u16,
                low,
                b'\\' as u16,
            ]),
        ),
    ] {
        assert_eq!(path_normalize(dialect, &input), expected);
    }
}

#[test]
fn is_absolute_matches_node_20_without_projecting_surrogates() {
    let high = 0xd800;
    for (dialect, input, expected) in [
        (NodePathDialect::Posix, scalar(""), false),
        (NodePathDialect::Posix, path(vec![high, b'/' as u16]), false),
        (NodePathDialect::Posix, path(vec![b'/' as u16, high]), true),
        (
            NodePathDialect::Win32,
            path(vec![b'C' as u16, b':' as u16, high]),
            false,
        ),
        (
            NodePathDialect::Win32,
            path(vec![b'C' as u16, b':' as u16, b'/' as u16, high]),
            true,
        ),
        (NodePathDialect::Win32, path(vec![b'\\' as u16, high]), true),
    ] {
        assert_eq!(path_is_absolute(dialect, &input), expected);
    }
}

#[test]
fn join_matches_node_20_empty_unc_and_mixed_separator_rules() {
    let high = path(vec![0xd800]);
    assert_eq!(path_join(NodePathDialect::Posix, &[]), scalar("."));
    assert_eq!(
        path_join(
            NodePathDialect::Posix,
            &[scalar(""), scalar("//a"), high.clone(), scalar("..")],
        ),
        scalar("/a"),
    );
    assert_eq!(
        path_join(
            NodePathDialect::Win32,
            &[scalar("///"), scalar("server"), high.clone()],
        ),
        path(vec![
            b'\\' as u16,
            b's' as u16,
            b'e' as u16,
            b'r' as u16,
            b'v' as u16,
            b'e' as u16,
            b'r' as u16,
            b'\\' as u16,
            0xd800
        ]),
    );
    assert_eq!(
        path_join(
            NodePathDialect::Win32,
            &[scalar("//server"), scalar("share"), high],
        ),
        path(vec![
            b'\\' as u16,
            b'\\' as u16,
            b's' as u16,
            b'e' as u16,
            b'r' as u16,
            b'v' as u16,
            b'e' as u16,
            b'r' as u16,
            b'\\' as u16,
            b's' as u16,
            b'h' as u16,
            b'a' as u16,
            b'r' as u16,
            b'e' as u16,
            b'\\' as u16,
            0xd800
        ]),
    );
}

#[test]
fn dirname_matches_node_20_code_unit_scans() {
    let high = 0xd800;

    for (dialect, input, expected) in [
        (NodePathDialect::Posix, scalar(""), scalar(".")),
        (NodePathDialect::Posix, scalar("//a"), scalar("//")),
        (
            NodePathDialect::Posix,
            path(vec![
                b'/' as u16,
                b'a' as u16,
                b'/' as u16,
                high,
                b'/' as u16,
                b'/' as u16,
            ]),
            scalar("/a"),
        ),
        (NodePathDialect::Win32, scalar(""), scalar(".")),
        (NodePathDialect::Win32, scalar(r"C:child"), scalar("C:")),
        (
            NodePathDialect::Win32,
            scalar("\\\\server\\share\\"),
            scalar("\\\\server\\share\\"),
        ),
        (
            NodePathDialect::Win32,
            path(vec![
                b'C' as u16,
                b':' as u16,
                b'\\' as u16,
                b'a' as u16,
                b'\\' as u16,
                high,
                b'\\' as u16,
            ]),
            scalar(r"C:\a"),
        ),
    ] {
        assert_eq!(path_dirname(dialect, &input), expected);
    }
}

#[test]
fn basename_suffix_is_exact_even_when_it_splits_a_surrogate_pair() {
    let high = 0xd800;
    let low = 0xdc00;
    let component = path(vec![b'/' as u16, b'a' as u16, b'/' as u16, high, low]);

    assert_eq!(
        units(&path_basename(
            NodePathDialect::Posix,
            &component,
            Some(&path(vec![low])),
        )),
        &[high],
    );
    assert_eq!(
        path_basename(
            NodePathDialect::Posix,
            &path(vec![b'/' as u16, b'a' as u16, b'/' as u16, high, low, high]),
            Some(&path(vec![high])),
        ),
        path(vec![high, low]),
    );
    assert_eq!(
        path_basename(
            NodePathDialect::Win32,
            &path(vec![
                b'C' as u16,
                b':' as u16,
                b'\\' as u16,
                high,
                low,
                high
            ]),
            Some(&path(vec![high])),
        ),
        path(vec![high, low]),
    );
    assert_eq!(
        path_basename(
            NodePathDialect::Win32,
            &scalar(r"C:\dir\name.txt\\"),
            Some(&scalar(".js")),
        ),
        scalar("name.txt"),
    );
    assert_eq!(
        path_basename(
            NodePathDialect::Posix,
            &scalar("same"),
            Some(&scalar("same")),
        ),
        scalar(""),
    );
}

#[test]
fn extname_matches_node_dot_and_surrogate_rules() {
    let high = 0xd800;
    let low = 0xdc00;
    let surrogate_ext = path(vec![
        b'/' as u16,
        b'a' as u16,
        b'/' as u16,
        high,
        b'.' as u16,
        b'x' as u16,
        low,
    ]);

    for dialect in [NodePathDialect::Posix, NodePathDialect::Win32] {
        assert_eq!(path_extname(dialect, &scalar(".profile")), scalar(""));
        assert_eq!(path_extname(dialect, &scalar("..")), scalar(""));
        assert_eq!(
            path_extname(dialect, &surrogate_ext),
            path(vec![b'.' as u16, b'x' as u16, low]),
        );
    }
}

#[test]
fn parse_matches_node_20_field_order_and_exact_slices() {
    let high = 0xd800;
    let low = 0xdc00;
    for (input, base, ext, name) in [
        ("/", "", "", ""),
        ("/foo", "foo", "", "foo"),
        ("/foo.txt", "foo.txt", ".txt", "foo"),
    ] {
        let parsed = path_parse(NodePathDialect::Posix, &scalar(input));
        assert_parts(
            &parsed,
            &scalar("/"),
            &scalar("/"),
            &scalar(base),
            &scalar(ext),
            &scalar(name),
        );
    }

    for input in ["/..", "/../", "/..//", "/..///"] {
        let parsed = path_parse(NodePathDialect::Posix, &scalar(input));
        assert_parts(
            &parsed,
            &scalar("/"),
            &scalar("/"),
            &scalar(".."),
            &scalar("."),
            &scalar("."),
        );
    }

    let parsed = path_parse(
        NodePathDialect::Posix,
        &path(vec![
            b'/' as u16,
            b'/' as u16,
            b'a' as u16,
            b'/' as u16,
            high,
            b'.' as u16,
            b'x' as u16,
            low,
        ]),
    );
    assert_parts(
        &parsed,
        &scalar("/"),
        &scalar("//a"),
        &path(vec![high, b'.' as u16, b'x' as u16, low]),
        &path(vec![b'.' as u16, b'x' as u16, low]),
        &path(vec![high]),
    );

    let parsed = path_parse(
        NodePathDialect::Win32,
        &path(vec![
            b'\\' as u16,
            b'\\' as u16,
            b's' as u16,
            b'r' as u16,
            b'v' as u16,
            b'\\' as u16,
            b's' as u16,
            b'h' as u16,
            b'a' as u16,
            b'r' as u16,
            b'e' as u16,
            b'\\' as u16,
            high,
            b'.' as u16,
            b'x' as u16,
            low,
        ]),
    );
    let root = scalar("\\\\srv\\share\\");
    assert_parts(
        &parsed,
        &root,
        &root,
        &path(vec![high, b'.' as u16, b'x' as u16, low]),
        &path(vec![b'.' as u16, b'x' as u16, low]),
        &path(vec![high]),
    );
}

#[test]
fn format_obeys_dir_base_precedence_and_node_20_ext_dot_rule() {
    let high = path(vec![0xd800]);
    let with_dot = path_format(
        NodePathDialect::Posix,
        &RuntimePathFormatInput::new()
            .with_root(scalar("/"))
            .with_name(high.clone())
            .with_ext(scalar("txt")),
    );
    assert_eq!(
        with_dot,
        path(vec![
            b'/' as u16,
            0xd800,
            b'.' as u16,
            b't' as u16,
            b'x' as u16,
            b't' as u16
        ]),
    );

    let precedence = path_format(
        NodePathDialect::Win32,
        &RuntimePathFormatInput::new()
            .with_root(scalar("C:\\"))
            .with_dir(scalar("C:\\a"))
            .with_base(path(vec![0xd800, b'.' as u16, b'x' as u16]))
            .with_name(scalar("ignored"))
            .with_ext(scalar("ignored")),
    );
    assert_eq!(
        precedence,
        path(vec![
            b'C' as u16,
            b':' as u16,
            b'\\' as u16,
            b'a' as u16,
            b'\\' as u16,
            0xd800,
            b'.' as u16,
            b'x' as u16
        ]),
    );
}

fn assert_parts(
    actual: &RuntimePathParts,
    root: &RuntimePath,
    dir: &RuntimePath,
    base: &RuntimePath,
    ext: &RuntimePath,
    name: &RuntimePath,
) {
    assert_eq!(actual.root(), root);
    assert_eq!(actual.dir(), dir);
    assert_eq!(actual.base(), base);
    assert_eq!(actual.ext(), ext);
    assert_eq!(actual.name(), name);
}
