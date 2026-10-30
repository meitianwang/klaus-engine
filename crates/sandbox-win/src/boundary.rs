//! Absolute Windows paths and the boundary comparisons every access decision
//! rests on.
//!
//! This is the security core. Two deliberate choices shape it:
//!
//! **Windows path syntax is parsed here, not by `std::path`.** `std::path` follows
//! the host's rules, so on a non-Windows machine `C:\ws` is a *relative* path with
//! one funny-looking component and `\\srv\share` is something else again. A
//! sandbox whose containment logic only means what it says on one operating
//! system cannot be tested anywhere else, and this logic is exactly the part that
//! must be tested. Parsing the syntax directly makes every rule below decidable
//! on any host, which is why the test suite can cover drive letters, UNC shares
//! and case folding without a Windows box.
//!
//! **Ambiguous spellings are refused, not guessed at.** `\ws` (root of whichever
//! drive is current), `C:ws` (relative to whichever directory is current on C:)
//! and `\\.\PhysicalDrive0` (device namespace, not a file) each resolve
//! differently depending on state this code cannot see. A sandbox that accepts
//! them decides containment against one interpretation while the target process
//! acts on another.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The root a path hangs from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Root {
    /// `C:` — a drive letter, stored uppercased so comparison is trivial.
    Drive(char),
    /// `\\server\share` — both parts stored uppercased for the same reason.
    Unc { server: String, share: String },
}

impl Root {
    fn render(&self) -> String {
        match self {
            Self::Drive(letter) => format!("{letter}:"),
            Self::Unc { server, share } => format!("\\\\{server}\\{share}"),
        }
    }
}

/// An absolute, normalized Windows path: a known root plus resolved components,
/// with no `.`, no `..`, no verbatim prefix and no trailing separator.
///
/// Construction is the only place that has to be careful about syntax; every
/// comparison downstream is a plain component walk.
///
/// Equality and hashing are case-insensitive, matching [`Self::is_within`] and
/// matching Windows itself. Keeping them case-sensitive while containment ignored
/// case would mean `Vec::contains`, `HashSet` and `==` disagreed with the
/// containment check about whether two spellings name one file, and a policy
/// assembled with one and enforced with the other has a hole in it. Original case
/// is still carried for display, because an error message should echo the path the
/// caller wrote.
#[derive(Debug, Clone)]
pub struct AbsPath {
    root: Root,
    /// Path components below the root, original case preserved for display.
    components: Vec<String>,
}

impl PartialEq for AbsPath {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
            && self.components.len() == other.components.len()
            && self
                .components
                .iter()
                .zip(&other.components)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}

impl Eq for AbsPath {}

impl std::hash::Hash for AbsPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the folded form so `Hash` agrees with `Eq`; a hash over the
        // original case would put two equal paths in different buckets.
        self.root.hash(state);
        for component in &self.components {
            component.to_ascii_uppercase().hash(state);
        }
    }
}

/// Why a path could not become an [`AbsPath`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    #[error("path is not absolute (needs a drive or UNC root): {0}")]
    NotAbsolute(String),
    #[error("path escapes its root with `..`: {0}")]
    EscapesRoot(String),
    #[error("device-namespace paths are not files: {0}")]
    DeviceNamespace(String),
    #[error("UNC path names no share: {0}")]
    IncompleteUnc(String),
}

fn is_separator(ch: char) -> bool {
    // Windows accepts both, and so must this: a policy written with `/` has to
    // compare equal to a target the OS reported with `\`.
    ch == '\\' || ch == '/'
}

impl AbsPath {
    /// Parse and normalize an absolute Windows path. Never touches the filesystem.
    ///
    /// Lexical on purpose: a boundary check has to answer for paths that do not
    /// exist yet (a file about to be created) or no longer exist (one just
    /// deleted), and any resolution that consults the disk fails on both.
    pub fn new(path: impl AsRef<str>) -> Result<Self, PathError> {
        let raw = path.as_ref();
        let text = raw.trim();
        if text.is_empty() {
            return Err(PathError::NotAbsolute(raw.to_string()));
        }
        let normalized: String = text
            .chars()
            .map(|ch| if is_separator(ch) { '\\' } else { ch })
            .collect();

        // `\\?\` only means "hand the rest to the filesystem unparsed"; it names
        // the same file as the plain form, so it must not compare differently.
        let body = normalized
            .strip_prefix("\\\\?\\")
            .or_else(|| normalized.strip_prefix("\\??\\"))
            .unwrap_or(&normalized);

        if body.starts_with("\\\\.\\") {
            return Err(PathError::DeviceNamespace(raw.to_string()));
        }

        let (root, rest) = if let Some(unc) = body.strip_prefix("\\\\") {
            let mut parts = unc.splitn(3, '\\');
            let server = parts.next().unwrap_or_default();
            let share = parts.next().unwrap_or_default();
            if server.is_empty() || share.is_empty() {
                return Err(PathError::IncompleteUnc(raw.to_string()));
            }
            let root = Root::Unc {
                server: server.to_ascii_uppercase(),
                share: share.to_ascii_uppercase(),
            };
            (root, parts.next().unwrap_or_default().to_string())
        } else {
            let mut chars = body.chars();
            let letter = chars.next().unwrap_or_default();
            if !letter.is_ascii_alphabetic() || chars.next() != Some(':') {
                // Covers `\ws` and any bare relative path: both depend on state
                // this code cannot observe.
                return Err(PathError::NotAbsolute(raw.to_string()));
            }
            let after_colon: String = chars.collect();
            match after_colon.strip_prefix('\\') {
                Some(rest) => (Root::Drive(letter.to_ascii_uppercase()), rest.to_string()),
                // `C:ws` is relative to the current directory *on that drive*.
                None if after_colon.is_empty() => {
                    (Root::Drive(letter.to_ascii_uppercase()), String::new())
                }
                None => return Err(PathError::NotAbsolute(raw.to_string())),
            }
        };

        let mut components: Vec<String> = Vec::new();
        for part in rest.split('\\') {
            match part {
                "" | "." => {}
                ".." => {
                    // Refused rather than clamped: clamping turns
                    // `C:\ws\..\..\Windows` into `C:\Windows` and lets it pass a
                    // check whose entire purpose was to keep it out.
                    if components.pop().is_none() {
                        return Err(PathError::EscapesRoot(raw.to_string()));
                    }
                }
                name => components.push(name.to_string()),
            }
        }
        Ok(Self { root, components })
    }

    /// The normalized text form, using `\` separators.
    pub fn as_str(&self) -> String {
        let mut out = self.root.render();
        out.push('\\');
        out.push_str(&self.components.join("\\"));
        out
    }

    /// Append a relative path, keeping the result normalized.
    pub fn join(&self, relative: impl AsRef<str>) -> Result<Self, PathError> {
        let mut text = self.as_str();
        if !text.ends_with('\\') {
            text.push('\\');
        }
        text.push_str(relative.as_ref());
        Self::new(text)
    }

    /// Whether `self` is `root` or sits underneath it.
    ///
    /// Per component and case-insensitive, which is what makes it safe here. A
    /// byte-prefix test reports `C:\workspace-old` as inside `C:\workspace`; a
    /// case-sensitive one reports `C:\WS\a` as outside `C:\ws`. Both are
    /// exploitable, in opposite directions.
    pub fn is_within(&self, root: &AbsPath) -> bool {
        if self.root != root.root {
            return false;
        }
        if self.components.len() < root.components.len() {
            return false;
        }
        root.components
            .iter()
            .zip(&self.components)
            .all(|(expected, actual)| expected.eq_ignore_ascii_case(actual))
    }

    /// The component names between `root` and `self`, if `self` is inside it.
    pub fn relative_components(&self, root: &AbsPath) -> Option<&[String]> {
        if !self.is_within(root) {
            return None;
        }
        Some(&self.components[root.components.len()..])
    }

    /// Whether any component of `self` is named `name` (case-insensitive).
    pub fn has_component_named(&self, name: &str) -> bool {
        self.components
            .iter()
            .any(|component| component.eq_ignore_ascii_case(name))
    }

    /// Every directory above `self`, outermost first, ending with its parent.
    ///
    /// The drive or share root comes first and `self` is not included. Used to grant
    /// a sandboxed process the right to *walk* to a permitted root: a container that
    /// can open a file inside the root but cannot list the directories above it looks
    /// like a working sandbox until a shell built-in runs, because `cmd.exe`
    /// canonicalizes an absolute path by descending from the drive root and reports
    /// "access denied" for the whole command when any step of that descent fails.
    pub fn ancestors(&self) -> Vec<AbsPath> {
        (0..self.components.len())
            .map(|depth| AbsPath {
                root: self.root.clone(),
                components: self.components[..depth].to_vec(),
            })
            .collect()
    }
}

impl fmt::Display for AbsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}

// Serialized as its text form: the wrapper receives this as JSON on a command
// line, and a struct-shaped encoding would be one more thing to keep in step.
impl Serialize for AbsPath {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_str())
    }
}

impl<'de> Deserialize<'de> for AbsPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::new(&text).map_err(serde::de::Error::custom)
    }
}

/// What a sandboxed process may reach on disk.
///
/// Deny always wins over allow. That ordering is the point: a policy grows by
/// adding allowed roots, and if allow could win, one broad root would silently
/// re-expose everything a deny rule was added to protect.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilesystemBoundary {
    /// Roots the process may read. `None` means "no read restriction beyond the
    /// deny list", which is what a policy that only confines writes wants.
    pub readable: Option<Vec<AbsPath>>,
    /// Roots the process may write. Empty means the process writes nothing.
    pub writable: Vec<AbsPath>,
    /// Paths that may never be written, even inside a writable root.
    pub deny_write: Vec<AbsPath>,
    /// Paths that may never be read, even inside a readable root.
    pub deny_read: Vec<AbsPath>,
    /// Directory names protected anywhere below a writable root: the repository
    /// metadata a command must not rewrite while it edits the tree around it.
    ///
    /// Enforced at the top level of each writable root, because that is where the ACL
    /// layer can name a path. A predicate that also answered for deeper occurrences
    /// used to live here and was removed: nothing consulted it, and a rule stated in
    /// one place and enforced differently in another is worse than an honest gap.
    pub protected_metadata: Vec<String>,
    /// Specific paths exempted from `protected_metadata`, for when a command
    /// legitimately has to touch one of those directories.
    pub metadata_write_allow: Vec<AbsPath>,
}

/// What a sandboxed process may reach on the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum NetworkBoundary {
    /// No outbound connections at all.
    Denied,
    /// Unrestricted, for a policy that opted into network access.
    Allowed,
    /// Loopback only, and only to these ports: the shape Klaus uses when traffic
    /// has to go through a local proxy it controls.
    #[serde(rename_all = "camelCase")]
    LoopbackPorts {
        ports: Vec<u16>,
        /// Whether the process may itself listen on a loopback port. Servers a
        /// command starts for its own tests need this; a policy that only allows
        /// outbound proxy traffic does not.
        allow_local_binding: bool,
    },
}

impl NetworkBoundary {
    /// Whether an outbound loopback connection to `port` is permitted.
    pub fn loopback_connect_allowed(&self, port: u16) -> bool {
        match self {
            Self::Denied => false,
            Self::Allowed => true,
            Self::LoopbackPorts { ports, .. } => ports.contains(&port),
        }
    }

    /// Whether any non-loopback destination is reachable.
    pub fn external_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abs(path: &str) -> AbsPath {
        AbsPath::new(path).unwrap_or_else(|error| panic!("{path} should parse: {error}"))
    }

    #[test]
    fn the_ancestors_of_a_path_run_from_the_root_down_to_its_parent() {
        let ancestors: Vec<String> = abs("C:\\users\\me\\ws")
            .ancestors()
            .iter()
            .map(AbsPath::as_str)
            .collect();
        assert_eq!(ancestors, vec!["C:\\", "C:\\users", "C:\\users\\me"]);
    }

    #[test]
    fn a_path_is_not_one_of_its_own_ancestors() {
        // If it were, the ancestor walk grant would be applied to the granted root as
        // well and would downgrade it.
        let path = abs("C:\\ws");
        assert!(!path.ancestors().contains(&path));
        assert_eq!(path.ancestors().len(), 1);
        assert_eq!(path.ancestors()[0].as_str(), "C:\\");
    }

    #[test]
    fn a_drive_root_has_no_ancestors() {
        assert!(abs("C:\\").ancestors().is_empty());
    }

    #[test]
    fn a_unc_walk_stops_at_the_share() {
        // Above the share there is nothing to grant: `\\server` is not a directory
        // whose ACL could be written.
        let ancestors: Vec<String> = abs("\\\\server\\share\\team\\ws")
            .ancestors()
            .iter()
            .map(AbsPath::as_str)
            .collect();
        assert_eq!(
            ancestors,
            // Server and share are folded to one case by normalization; the components
            // below them keep theirs.
            vec!["\\\\SERVER\\SHARE\\", "\\\\SERVER\\SHARE\\team"]
        );
    }

    #[test]
    fn normalization_folds_the_spellings_that_name_one_file() {
        let target = abs("C:\\ws\\a.txt");
        for spelling in [
            "C:\\ws\\a.txt",
            "C:/ws/a.txt",
            "C:\\ws\\.\\a.txt",
            "C:\\ws\\sub\\..\\a.txt",
            "\\\\?\\C:\\ws\\a.txt",
            "c:\\WS\\a.txt",
            "C:\\ws\\\\a.txt",
            "  C:\\ws\\a.txt  ",
        ] {
            assert_eq!(abs(spelling), target, "{spelling:?} should equal target");
        }
    }

    #[test]
    fn the_text_form_is_stable_and_uses_backslashes() {
        assert_eq!(abs("c:/ws/sub/a.txt").as_str(), "C:\\ws\\sub\\a.txt");
        assert_eq!(
            abs("//SRV/share/dir/f.txt").as_str(),
            "\\\\SRV\\SHARE\\dir\\f.txt"
        );
    }

    #[test]
    fn ambiguous_and_relative_spellings_are_refused() {
        // Each of these resolves against state this code cannot see.
        assert!(matches!(
            AbsPath::new("ws\\a.txt"),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(
            AbsPath::new("\\ws\\a.txt"),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(
            AbsPath::new("C:ws\\a.txt"),
            Err(PathError::NotAbsolute(_))
        ));
        assert!(matches!(AbsPath::new(""), Err(PathError::NotAbsolute(_))));
        assert!(matches!(
            AbsPath::new("\\\\.\\PhysicalDrive0"),
            Err(PathError::DeviceNamespace(_))
        ));
        assert!(matches!(
            AbsPath::new("\\\\server"),
            Err(PathError::IncompleteUnc(_))
        ));
    }

    #[test]
    fn dotdot_above_the_root_is_refused_not_clamped() {
        assert!(matches!(
            AbsPath::new("C:\\ws\\..\\..\\Windows"),
            Err(PathError::EscapesRoot(_))
        ));
        // One level up inside the tree is fine and resolves.
        assert_eq!(abs("C:\\ws\\sub\\..\\other"), abs("C:\\ws\\other"));
    }

    #[test]
    fn containment_compares_whole_components_not_string_prefixes() {
        let root = abs("C:\\workspace");
        assert!(abs("C:\\workspace").is_within(&root));
        assert!(abs("C:\\workspace\\src\\main.rs").is_within(&root));
        // The bug this guards: a byte-prefix test calls this one "inside".
        assert!(!abs("C:\\workspace-old\\secret").is_within(&root));
        assert!(!abs("C:\\other").is_within(&root));
        // A shorter path is never inside a longer root.
        assert!(!abs("C:\\").is_within(&root));
    }

    #[test]
    fn containment_ignores_case_including_the_root() {
        let root = abs("C:\\Workspace");
        assert!(abs("c:\\workspace\\SRC\\main.rs").is_within(&root));
        assert!(abs("C:\\WORKSPACE").is_within(&root));

        let unc = abs("\\\\srv\\share\\ws");
        assert!(abs("\\\\SRV\\SHARE\\ws\\a.txt").is_within(&unc));
    }

    #[test]
    fn different_roots_never_contain_each_other() {
        assert!(!abs("D:\\ws\\a.txt").is_within(&abs("C:\\ws")));
        assert!(!abs("\\\\srv\\share\\ws\\a.txt").is_within(&abs("C:\\ws")));
        assert!(!abs("C:\\ws\\a.txt").is_within(&abs("\\\\srv\\share\\ws")));
    }

    #[test]
    fn network_denied_by_default_and_ports_are_exact() {
        assert!(!NetworkBoundary::Denied.loopback_connect_allowed(8080));
        assert!(!NetworkBoundary::Denied.external_allowed());

        let proxied = NetworkBoundary::LoopbackPorts {
            ports: vec![8080, 8443],
            allow_local_binding: false,
        };
        assert!(proxied.loopback_connect_allowed(8080));
        assert!(proxied.loopback_connect_allowed(8443));
        assert!(!proxied.loopback_connect_allowed(8081));
        // Proxy scoping is loopback only: it must not imply general egress.
        assert!(!proxied.external_allowed());

        assert!(NetworkBoundary::Allowed.external_allowed());
        assert!(NetworkBoundary::Allowed.loopback_connect_allowed(1));
    }

    #[test]
    fn a_boundary_survives_the_json_round_trip_the_wrapper_uses() {
        let boundary = FilesystemBoundary {
            readable: Some(vec![abs("C:\\ws")]),
            writable: vec![abs("C:\\ws")],
            deny_write: vec![abs("C:\\ws\\vendor")],
            deny_read: vec![abs("C:\\users\\me\\.ssh")],
            protected_metadata: vec![".git".into()],
            metadata_write_allow: vec![abs("C:\\ws\\.git\\info\\exclude")],
        };
        let json = serde_json::to_string(&boundary).expect("serialize boundary");
        let restored: FilesystemBoundary = serde_json::from_str(&json).expect("restore boundary");
        assert_eq!(restored, boundary);

        let network = NetworkBoundary::LoopbackPorts {
            ports: vec![8080],
            allow_local_binding: true,
        };
        let json = serde_json::to_string(&network).expect("serialize network");
        let restored: NetworkBoundary = serde_json::from_str(&json).expect("restore network");
        assert_eq!(restored, network);
    }

    #[test]
    fn a_malformed_path_in_json_is_rejected_rather_than_silently_accepted() {
        // The wrapper receives this JSON from a command line. A path that does not
        // parse must fail loudly there, not become a boundary that matches nothing.
        let json = r#"{"readable":null,"writable":["ws\\relative"],"denyWrite":[],"denyRead":[],"protectedMetadata":[],"metadataWriteAllow":[]}"#;
        let restored: Result<FilesystemBoundary, _> = serde_json::from_str(json);
        assert!(restored.is_err(), "relative path must not deserialize");
    }

    #[test]
    fn equality_hashing_and_containment_agree_about_case() {
        // The invariant: if two spellings name one file, every comparison in this
        // type says so. A mismatch here is how a path lands in a deny list under
        // one spelling and slips past the check under another.
        use std::collections::HashSet;

        let lower = abs("C:\\ws\\a.txt");
        let upper = abs("c:\\WS\\A.TXT");
        assert_eq!(lower, upper);
        assert!(lower.is_within(&upper));
        assert!(upper.is_within(&lower));

        let mut set = HashSet::new();
        set.insert(lower.clone());
        assert!(set.contains(&upper), "Hash must agree with Eq");
        assert!([lower].contains(&upper), "slice contains must agree too");

        // And display keeps what the caller wrote.
        assert_eq!(abs("C:\\Ws\\A.txt").as_str(), "C:\\Ws\\A.txt");
    }

    #[test]
    fn join_stays_normalized() {
        let root = abs("C:\\ws");
        assert_eq!(
            root.join("src\\main.rs").expect("join"),
            abs("C:\\ws\\src\\main.rs")
        );
        assert_eq!(
            root.join("sub\\..\\a.txt").expect("join"),
            abs("C:\\ws\\a.txt")
        );
        assert!(matches!(
            root.join("..\\..\\Windows"),
            Err(PathError::EscapesRoot(_))
        ));
    }
}
