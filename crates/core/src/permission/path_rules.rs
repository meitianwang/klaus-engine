//! Path-scoped permission rules for FILE tools — `Read(secrets/**)`, `Edit(.env)`,
//! `Write(dist/**)`. These are matched by tool CLASS (Read covers read/glob/grep/ls; Edit/Write
//! cover write/edit/multi_edit) and by gitignore-style PATH patterns against the tool's target,
//! not by bash-prefix. The engine's bash rule matcher is prefix-only, so file rules were inert;
//! this module makes them live. Patterns are matched with gitignore semantics (via the `ignore`
//! crate) plus case-insensitive matching.

use crate::fs::node_path::{relative_runtime, resolve_runtime, runtime_path_from_native};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimePath, RuntimeString,
};
use crate::types::permission::PermissionRule;
use ignore::gitignore::GitignoreBuilder;
use serde::{Deserialize, Serialize};

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

/// The file-tool class a path rule can address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PathRuleKind {
    Read,
    Edit,
}

impl RuntimeEncode for PathRuleKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Read => "read",
            Self::Edit => "edit",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PathRuleKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("read", &[]),
                RuntimeEnumVariant::new("edit", &[]),
            ],
            &[Self::Read, Self::Edit],
        )
    }
}

/// Whether a rule's tool name addresses the given file-tool class (the by-class mapping).
fn rule_addresses_kind(rule_tool_name: &RuntimeString, kind: PathRuleKind) -> bool {
    let is = |expected: &str| ascii_case_insensitive_eq(rule_tool_name, expected);
    if kind == PathRuleKind::Read {
        return is("read");
    }
    // `Write` is accepted as an alias for the edit class (ergonomic; a strictly-wider
    // addition so a Write(dist/**) rule actually applies).
    is("edit") || is("write")
}

fn ascii_case_insensitive_eq(value: &RuntimeString, expected: &str) -> bool {
    value
        .as_utf16()
        .iter()
        .copied()
        .map(ascii_lower)
        .eq(expected.encode_utf16().map(ascii_lower))
}

fn ascii_lower(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

/// The user's home directory: `$HOME` when set, else the passwd entry for the
/// effective uid (mirroring the platform home-directory lookup).
fn home_dir() -> RuntimePath {
    if let Some(home) = std::env::var_os("HOME") {
        return runtime_path_from_native(std::path::Path::new(&home));
    }
    #[cfg(unix)]
    unsafe {
        let pw = libc::getpwuid(libc::getuid());
        if !pw.is_null() {
            let dir = (*pw).pw_dir;
            if !dir.is_null() {
                let bytes = std::ffi::CStr::from_ptr(dir).to_bytes();
                return RuntimePath::from(crate::runtime_values::NodeBufferUtf8::decode(bytes));
            }
        }
    }
    RuntimePath::from("")
}

/// Resolve a pattern's anchor root and the relative pattern body.
fn pattern_root(pattern: &RuntimeString) -> (Option<RuntimePath>, RuntimeString) {
    if pattern.starts_with("//") {
        return (Some(RuntimePath::from("/")), pattern.slice_utf16(2..));
    }
    if pattern.starts_with("~/") {
        return (Some(home_dir()), pattern.slice_utf16(2..));
    }
    if pattern.starts_with("/") {
        return (None, pattern.slice_utf16(1..));
    }
    (None, pattern.clone())
}

/// Whether `abs_path` matches gitignore `pattern`, anchored relative to `source_root` (the cwd in
/// headless). Standard gitignore semantics: a pattern with a slash is anchored to the root
/// (`secrets/**` matches `secrets/x` but not `sub/secrets/x`), a bare name matches at any depth
/// (`.env`). The user controls anchoring with the slash. Returns false when the path falls
/// outside the anchor root.
pub fn path_matches_pattern(
    pattern: &RuntimeString,
    abs_path: &RuntimePath,
    source_root: &RuntimePath,
) -> bool {
    let (root, body) = pattern_root(pattern);
    if body.is_empty() {
        return true; // an anchor-only pattern matches everything under it
    }
    let anchor = match root {
        Some(root) => resolve_runtime(source_root, &[root])
            .expect("an explicit permission anchor resolves exactly"),
        None => resolve_runtime(source_root, std::slice::from_ref(source_root))
            .expect("an explicit permission source root resolves exactly"),
    };
    let absolute = resolve_runtime(source_root, std::slice::from_ref(abs_path))
        .expect("an explicit permission path resolves exactly");
    let rel = relative_runtime(source_root, &anchor, &absolute)
        .expect("absolute permission paths compute an exact relative path");
    if rel.as_runtime_string().is_empty()
        || rel.as_runtime_string().starts_with("../")
        || rel.as_runtime_string() == ".."
    {
        return false; // at/outside the anchor
    }
    // Matching is case-insensitive; any pattern that
    // fails to compile simply doesn't match.
    let mut builder = GitignoreBuilder::new("");
    if builder.case_insensitive(false).is_err() {
        return false;
    }
    let encoded_body = matcher_pattern(&body);
    if builder.add_line(None, &encoded_body).is_err() {
        return false;
    }
    // `prefix/**` matches the CONTENTS of prefix; also match the prefix DIRECTORY itself
    // (so a deny on `secrets/**` covers a glob/grep whose search root IS `secrets`), keeping
    // the same anchoring (`/prefix` is anchored to the rule's root, not any depth).
    if body.ends_with("/**") {
        let dir = body.slice_utf16(..body.len_utf16().saturating_sub(3));
        if !dir.is_empty()
            && builder
                .add_line(None, &format!("/{}", matcher_pattern(&dir)))
                .is_err()
        {
            return false;
        }
    }
    let matcher = match builder.build() {
        Ok(m) => m,
        Err(_) => return false,
    };
    // A path under an ignored directory is ignored too, so check the parents as
    // well as the path itself (gitignore's directory-containment semantics).
    matcher
        .matched_path_or_any_parents(matcher_path(rel.as_runtime_string()), false)
        .is_ignore()
}

/// Map every literal UTF-16 code unit to one scalar in a private plane. This
/// keeps lone surrogates collision-free while leaving gitignore syntax and path
/// separators available to the matcher. Literal case folding happens before
/// the mapping so case-insensitive rules remain deterministic.
fn matcher_pattern(value: &RuntimeString) -> String {
    matcher_string(value, true)
}

fn matcher_path(value: &RuntimeString) -> String {
    matcher_string(value, false)
}

fn matcher_string(value: &RuntimeString, pattern: bool) -> String {
    let mut output = String::with_capacity(value.len_utf16());
    for point in value.code_points() {
        let syntax = pattern
            && matches!(
                point,
                0x0021 | 0x002a | 0x003f | 0x005b | 0x005d | 0x005c | 0x002d | 0x005e
            );
        let separator = matches!(point, 0x002f | 0x005c);
        if syntax || separator {
            output.push(char::from_u32(point).expect("ASCII matcher syntax"));
        } else if let Some(scalar) = char::from_u32(point) {
            for lowercase in scalar.to_lowercase() {
                let mut encoded = [0; 2];
                for unit in lowercase.encode_utf16(&mut encoded).iter().copied() {
                    output.push(
                        char::from_u32(0x0f0000 + u32::from(unit))
                            .expect("one private scalar exists per UTF-16 code unit"),
                    );
                }
            }
        } else {
            output.push(
                char::from_u32(0x0f0000 + point)
                    .expect("one private scalar exists per UTF-16 code unit"),
            );
        }
    }
    output
}

/// First rule that addresses `kind` and matches any of `paths` (gitignore), or `None`. A rule
/// with no path pattern is tool-wide (matches any path of the class). `source_root` anchors
/// relative patterns (the cwd).
pub fn match_path_rule<'a>(
    rules: &'a [PermissionRule],
    kind: PathRuleKind,
    paths: &[RuntimePath],
    source_root: &RuntimePath,
) -> Option<&'a PermissionRule> {
    for rule in rules {
        if !rule_addresses_kind(&rule.tool_name, kind) {
            continue;
        }
        let pattern = match &rule.argument_prefix {
            None => return Some(rule),                    // tool-wide
            Some(p) if p.is_empty() => return Some(rule), // tool-wide
            Some(p) => p,
        };
        if paths
            .iter()
            .any(|p| path_matches_pattern(pattern, p, source_root))
        {
            return Some(rule);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::rules::parse_rule_string;
    use crate::types::permission::PermissionRule;

    const CWD: &str = "/work";

    fn rule(s: &str) -> PermissionRule {
        parse_rule_string(s).unwrap()
    }

    fn path_matches_pattern(pattern: &str, path: &str, source_root: &str) -> bool {
        super::path_matches_pattern(
            &RuntimeString::from(pattern),
            &RuntimePath::from(path),
            &RuntimePath::from(source_root),
        )
    }

    fn match_path_rule<'a>(
        rules: &'a [PermissionRule],
        kind: PathRuleKind,
        paths: &[String],
        source_root: &str,
    ) -> Option<&'a PermissionRule> {
        let paths = paths
            .iter()
            .cloned()
            .map(RuntimePath::from)
            .collect::<Vec<_>>();
        super::match_path_rule(rules, kind, &paths, &RuntimePath::from(source_root))
    }

    // pathMatchesPattern

    #[test]
    fn matches_a_globstar_pattern_against_contents_and_the_directory_itself_anchored() {
        assert!(path_matches_pattern(
            "secrets/**",
            "/work/secrets/key.txt",
            CWD
        ));
        assert!(path_matches_pattern("secrets/**", "/work/secrets", CWD)); // the dir itself
        assert!(!path_matches_pattern(
            "secrets/**",
            "/work/sub/secrets/x",
            CWD
        )); // anchored
    }

    #[test]
    fn matches_a_bare_filename_at_any_depth() {
        assert!(path_matches_pattern(".env", "/work/.env", CWD));
        assert!(path_matches_pattern(".env", "/work/a/b/.env", CWD));
    }

    #[test]
    fn does_not_match_unrelated_paths() {
        assert!(!path_matches_pattern(
            "secrets/**",
            "/work/src/index.ts",
            CWD
        ));
        assert!(!path_matches_pattern("dist/**", "/work/build/x", CWD));
    }

    #[test]
    fn a_leading_single_slash_selects_the_source_root_and_anchors_there() {
        assert!(path_matches_pattern("/dist/**", "/work/dist/a.js", CWD));
        assert!(!path_matches_pattern(
            "/dist/**",
            "/work/sub/dist/a.js",
            CWD
        )); // anchored
    }

    #[test]
    fn a_double_slash_anchors_to_the_filesystem_root() {
        assert!(path_matches_pattern("//etc/**", "/etc/passwd", CWD));
        assert!(!path_matches_pattern("//etc/**", "/work/etc/x", CWD));
    }

    #[test]
    fn returns_false_for_a_path_outside_the_cwd_anchor() {
        assert!(!path_matches_pattern("secrets/**", "/etc/passwd", CWD));
    }

    // matchPathRule

    #[test]
    fn matches_a_read_rule_against_a_read_class_path() {
        let rules = [rule("Read(secrets/**)")];
        let r = match_path_rule(
            &rules,
            PathRuleKind::Read,
            &["/work/secrets/k".to_string()],
            CWD,
        );
        assert!(r.is_some());
    }

    #[test]
    fn does_not_match_a_read_rule_for_an_edit_class_call() {
        let rules = [rule("Read(secrets/**)")];
        let r = match_path_rule(
            &rules,
            PathRuleKind::Edit,
            &["/work/secrets/k".to_string()],
            CWD,
        );
        assert!(r.is_none());
    }

    #[test]
    fn maps_edit_and_write_both_to_the_edit_class() {
        let edit_rules = [rule("Edit(.env)")];
        assert!(match_path_rule(
            &edit_rules,
            PathRuleKind::Edit,
            &["/work/.env".to_string()],
            CWD
        )
        .is_some());
        let write_rules = [rule("Write(dist/**)")];
        assert!(match_path_rule(
            &write_rules,
            PathRuleKind::Edit,
            &["/work/dist/a".to_string()],
            CWD
        )
        .is_some());
    }

    #[test]
    fn a_tool_wide_rule_no_path_matches_any_path_of_the_class() {
        let rules = [rule("Read")];
        assert!(match_path_rule(
            &rules,
            PathRuleKind::Read,
            &["/work/anything".to_string()],
            CWD
        )
        .is_some());
        assert!(match_path_rule(
            &rules,
            PathRuleKind::Edit,
            &["/work/anything".to_string()],
            CWD
        )
        .is_none());
    }
}
