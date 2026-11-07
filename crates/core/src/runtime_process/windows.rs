//! Pure Windows process-image construction.
//!
//! This module does not create a process, duplicate a handle, or install a
//! wait. It closes every pure file/argv/env/cwd observation into owned UTF-16
//! storage for a later project-owned transaction. The image alone is not
//! syscall-ready: checked flags, stdio handles, and transaction state still
//! have to be sealed around it. Node/libuv and Rust 1.93 remain separate
//! builders because their quoting and environment laws are observably
//! different.

use std::cmp::Ordering;
use std::fmt;

use crate::runtime_values::RuntimeString;

use super::node20_path_case_data::UPPER_MAPPINGS;
use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::{
    NativeOsString, NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan,
    ProcessImageDialect, ProcessPlanError, Rust193EnvMutation,
};

const NODE_OPTIONS: &[u16] = &[
    b'N' as u16,
    b'O' as u16,
    b'D' as u16,
    b'E' as u16,
    b'_' as u16,
    b'O' as u16,
    b'P' as u16,
    b'T' as u16,
    b'I' as u16,
    b'O' as u16,
    b'N' as u16,
    b'S' as u16,
];

const PATH: &[u16] = &[b'P' as u16, b'A' as u16, b'T' as u16, b'H' as u16];

const REQUIRED_NODE_ENVIRONMENT: [&str; 11] = [
    "HOMEDRIVE",
    "HOMEPATH",
    "LOGONSERVER",
    "PATH",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "WINDIR",
];

/// A closed pure process image with no unresolved file, cwd, PATH, or
/// environment observation. The later transaction combines it with checked
/// flags and stdio handles; only that sealed transaction is syscall-ready.
pub(super) struct WindowsExecImage {
    image_dialect: ProcessImageDialect,
    application: Box<[u16]>,
    command_line: Box<[u16]>,
    cwd: Option<WindowsCwdImage>,
    environment: WindowsEnvironmentImage,
}

/// Node always retains the exact cwd snapshot used by libuv search, including
/// when the logical option omitted cwd. Rust additionally retains the full
/// allocation plus the exact pointer offset selected by Rust 1.93 `make_dirp`.
/// The two type states cannot be confused by the later transaction.
pub(super) enum WindowsCwdImage {
    Node20LibuvSnapshot(Box<[u16]>),
    Rust193PlatformPrepared {
        storage: Box<[u16]>,
        argument_offset: usize,
    },
}

/// The closed result of pinned libuv executable search.
///
/// Construction classifies `.bat`/`.cmd` from the searched path itself, so a
/// caller cannot relabel a batch file as a binary to bypass Node/libuv's
/// direct-execution rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WindowsNode20ResolvedExecutable {
    application: Vec<u16>,
    is_batch: bool,
}

/// The two legal `lpEnvironment` states.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WindowsEnvironmentImage {
    /// Pass `lpEnvironment = NULL` without observing or rebuilding the parent.
    NativeParentInherit,
    /// Pass this explicit `CREATE_UNICODE_ENVIRONMENT` block.
    Explicit(Box<[u16]>),
}

/// A filesystem-resolved Rust 1.93 executable returned by its platform
/// resolver. The Node/libuv builder cannot accept this type.
///
/// For batch resolution the application is the attested system-directory
/// `cmd.exe`; `script` is the resolved `.bat`/`.cmd` path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WindowsResolvedExecutable {
    Binary {
        application: Vec<u16>,
    },
    Batch {
        application: Vec<u16>,
        script: Vec<u16>,
    },
}

/// One exact native-wide environment property before block serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WindowsEnvironmentEntry {
    key: Vec<u16>,
    value: Vec<u16>,
}

/// One raw record from `GetEnvironmentStringsW`, without its terminating NUL.
///
/// Unlike [`WindowsEnvironmentEntry`], this carrier deliberately does not
/// interpret `=`. Windows permits drive-current-directory records such as
/// `=C:=C:\\work`, and a native block can also contain duplicate aliases or
/// malformed/no-`=` records that a sealed pass must preserve byte-for-byte.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WindowsRawEnvironmentRecord {
    units: Vec<u16>,
}

/// OS-dependent operations whose results are frozen by the Windows platform
/// fixtures. Keeping them explicit lets macOS test every pure UTF-16 step
/// without substituting Rust Unicode or sorting behavior for Win32/UCRT.
pub(super) trait WindowsEnvironmentPlatform {
    /// Pinned libuv `GetCurrentDirectoryW` observation used when Node's cwd is
    /// omitted. The returned units exclude the terminating NUL.
    fn node_current_directory(&self) -> Result<Vec<u16>, String>;

    /// Pinned libuv `search_path` plus final executable classification.
    /// `cwd` is the exact snapshot retained by the image; `child_path` comes
    /// from the final serialized child block, or `None` when the resolver must
    /// perform libuv's parent `GetEnvironmentVariableW(PATH)` fallback.
    fn resolve_node20_executable(
        &self,
        requested: &[u16],
        cwd: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsNode20ResolvedExecutable, String>;

    /// Pinned libuv required-variable lookup. Empty and missing both return
    /// `None`, matching `GetEnvironmentVariableW(..., NULL, 0) == 0`.
    fn node_required_value(&self, name: &[u16]) -> Result<Option<Vec<u16>>, String>;

    /// Sort complete projected entries through libuv's UCRT `qsort` and
    /// `LCMapStringW(LOCALE_INVARIANT, LCMAP_UPPERCASE)` key comparator.
    fn sort_libuv_environment(&self, entries: &mut [WindowsEnvironmentEntry])
        -> Result<(), String>;

    /// One key-only libuv invariant-uppercase comparison, used while merging
    /// the already-sorted required-variable list into the UCRT-sorted input.
    fn compare_libuv_environment_key(
        &self,
        left: &[u16],
        right: &[u16],
    ) -> Result<Ordering, String>;

    /// Rust 1.93 `EnvKey` comparison through `CompareStringOrdinal(..., TRUE)`.
    fn compare_ordinal_ignore_case(&self, left: &[u16], right: &[u16]) -> Result<Ordering, String>;

    /// Rust 1.93 `env::vars_os()` source block in native enumeration order.
    fn parent_environment(&self) -> Result<Vec<WindowsEnvironmentEntry>, String>;

    /// One raw `GetEnvironmentStringsW` snapshot in native record order.
    /// This is used only when the project guard must filter an otherwise
    /// inherited block without applying Rust `CommandEnv` map semantics.
    fn raw_parent_environment(&self) -> Result<Vec<WindowsRawEnvironmentRecord>, String>;

    /// Rust 1.93 executable resolution, including batch detection and
    /// `cmd.exe` selection. `requested` is intentionally not prevalidated:
    /// the resolver owns the source-ordered program-path validation boundary.
    /// When `child_path` is present, it borrows the PATH value from the exact
    /// captured-and-folded child environment that will later be serialized;
    /// the platform resolver still owns Rust's later live parent-PATH fallback.
    fn resolve_rust193_executable(
        &self,
        requested: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsResolvedExecutable, String>;

    /// Apply Rust 1.93 `make_dirp`: conditionally rewrite/strip `\\?\UNC` or
    /// `\\?\` only when Win32 `GetFullPathNameW` proves the candidate path is
    /// absolute and byte-for-byte unchanged. Returns the pointer offset into
    /// the same mutable, NUL-terminated allocation.
    fn prepare_rust193_cwd(&self, cwd: &mut [u16]) -> Result<usize, String>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsImageField {
    RequestedApplication,
    ResolvedApplication,
    ResolvedBatchScript,
    Cwd,
    Argument(usize),
    EnvironmentKey(usize),
    EnvironmentValue(usize),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum WindowsExecImageBuildError {
    InvalidPlan(ProcessPlanError),
    ContainsNul {
        field: WindowsImageField,
        unit_index: usize,
    },
    EmptyResolvedApplication,
    NodeBatchExecutable,
    GuardMismatch,
    NodeEnvironmentRequired,
    InvalidBatchScript,
    InvalidBatchArgument,
    NativeAtomUnavailable,
    Platform(String),
    ForbiddenNodeOptionsMutation,
}

impl fmt::Display for WindowsExecImageBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(error) => error.fmt(formatter),
            Self::ContainsNul { field, unit_index } => {
                write!(
                    formatter,
                    "Windows process image contains NUL at {field:?}[{unit_index}]"
                )
            }
            Self::EmptyResolvedApplication => {
                formatter.write_str("resolved Windows application is empty")
            }
            Self::NodeBatchExecutable => {
                formatter.write_str("Node/libuv cannot execute a batch image directly")
            }
            Self::GuardMismatch => {
                formatter.write_str("Windows process environment guard mismatch")
            }
            Self::NodeEnvironmentRequired => {
                formatter.write_str("Node/libuv requires a logical RuntimeEnv")
            }
            Self::InvalidBatchScript => {
                formatter.write_str("Windows file names may not contain `\"` or end with `\\`")
            }
            Self::InvalidBatchArgument => formatter.write_str("batch file arguments are invalid"),
            Self::NativeAtomUnavailable => {
                formatter.write_str("native Windows wide atom is unavailable on this target")
            }
            Self::Platform(error) => write!(formatter, "Windows platform image failure: {error}"),
            Self::ForbiddenNodeOptionsMutation => formatter.write_str(
                "Custom browser environment may not set NODE_OPTIONS in the exact runtime.",
            ),
        }
    }
}

impl std::error::Error for WindowsExecImageBuildError {}

impl fmt::Debug for WindowsExecImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WindowsExecImage")
            .field("image_dialect", &self.image_dialect)
            .field("application", &"<redacted>")
            .field("command_line_units", &self.command_line.len())
            .field("cwd_present", &self.cwd.is_some())
            .field("environment", &self.environment)
            .finish()
    }
}

impl WindowsResolvedExecutable {
    pub(super) fn binary(application: Vec<u16>) -> Result<Self, WindowsExecImageBuildError> {
        validate_resolved_units(&application, WindowsImageField::ResolvedApplication)?;
        Ok(Self::Binary { application })
    }

    pub(super) fn batch(
        application: Vec<u16>,
        script: Vec<u16>,
    ) -> Result<Self, WindowsExecImageBuildError> {
        validate_resolved_units(&application, WindowsImageField::ResolvedApplication)?;
        validate_units(&script, WindowsImageField::ResolvedBatchScript)?;
        if script.contains(&(b'"' as u16)) || script.last() == Some(&(b'\\' as u16)) {
            return Err(WindowsExecImageBuildError::InvalidBatchScript);
        }
        Ok(Self::Batch {
            application,
            script,
        })
    }

    fn application(&self) -> &[u16] {
        match self {
            Self::Binary { application } | Self::Batch { application, .. } => application,
        }
    }
}

impl WindowsNode20ResolvedExecutable {
    /// Classify one exact path returned by pinned libuv `search_path`.
    pub(super) fn from_search_result(
        application: Vec<u16>,
    ) -> Result<Self, WindowsExecImageBuildError> {
        validate_resolved_units(&application, WindowsImageField::ResolvedApplication)?;
        let is_batch = has_ascii_case_insensitive_suffix(&application, b".bat")
            || has_ascii_case_insensitive_suffix(&application, b".cmd");
        Ok(Self {
            application,
            is_batch,
        })
    }

    fn application(&self) -> &[u16] {
        &self.application
    }

    fn is_batch(&self) -> bool {
        self.is_batch
    }
}

impl WindowsEnvironmentEntry {
    pub(super) fn new(key: Vec<u16>, value: Vec<u16>) -> Result<Self, WindowsExecImageBuildError> {
        validate_units(&key, WindowsImageField::EnvironmentKey(0))?;
        validate_units(&value, WindowsImageField::EnvironmentValue(0))?;
        Ok(Self { key, value })
    }

    pub(super) fn key(&self) -> &[u16] {
        &self.key
    }

    fn libuv_key(&self) -> &[u16] {
        let end = self
            .key
            .iter()
            .position(|unit| *unit == b'=' as u16)
            .unwrap_or(self.key.len());
        &self.key[..end]
    }
}

impl WindowsRawEnvironmentRecord {
    pub(super) fn new(units: Vec<u16>) -> Result<Self, WindowsExecImageBuildError> {
        validate_units(&units, WindowsImageField::EnvironmentKey(0))?;
        Ok(Self { units })
    }

    pub(super) fn units(&self) -> &[u16] {
        &self.units
    }
}

impl WindowsExecImage {
    pub(super) const NODE_VERSION: &'static str = "20.19.4";
    pub(super) const LIBUV_VERSION: &'static str = "1.46.0";
    pub(super) const LIBUV_WINDOWS_PROCESS_C_SHA256: &'static str =
        "0e0adb69bdf79afd76b073d99026de525f4ce892e58384863588e52700d1d9c2";
    pub(super) const NODE_CHILD_PROCESS_SHA256: &'static str =
        "c571886cf0afcc81be7aafe99a0e3cab85c76099ad1dce4938f2a76322ad28c5";
    pub(super) const RUST_VERSION: &'static str = "1.93.0";
    pub(super) const RUSTC_COMMIT: &'static str = "254b59607d4417e9dffbc307138ae5c86280fe4c";
    pub(super) const RUST_WINDOWS_PROCESS_RS_SHA256: &'static str =
        "badf23ba0957c21dd280c705e392f3cd247390ec72a94a6095bad96544432b7c";
    pub(super) const RUST_WINDOWS_ARGS_RS_SHA256: &'static str =
        "69ffe5aedea4a6194365f11904329ebfa7c51d0f52960628f0a66fed05523d6d";

    /// Build a closed Node 20/libuv 1.46 image in pinned `uv_spawn` order.
    pub(super) fn build_node<P: WindowsEnvironmentPlatform>(
        plan: &NodeSpawnPlan,
        guard: ProcessEnvironmentGuard,
        platform: &P,
    ) -> Result<Self, WindowsExecImageBuildError> {
        if guard != ProcessEnvironmentGuard::None {
            return Err(WindowsExecImageBuildError::GuardMismatch);
        }
        let ProcessEnvironmentPlan::NodeRuntime(environment) = plan.environment() else {
            return Err(WindowsExecImageBuildError::NodeEnvironmentRequired);
        };

        // Pinned libuv `uv_spawn`: file projection, args, env, explicit or
        // observed cwd, child-PATH/parent-PATH search, final application.
        let requested = project_atom(plan.file(), WindowsImageField::RequestedApplication)?;
        let command_line =
            build_node_command_line(plan.argv0(), plan.args(), plan.windows_verbatim_arguments())?;
        let environment = build_node_environment(environment, platform)?;
        let cwd = match plan.cwd() {
            Some(value) => project_atom(value, WindowsImageField::Cwd)?,
            None => {
                let cwd = platform
                    .node_current_directory()
                    .map_err(WindowsExecImageBuildError::Platform)?;
                if cwd.is_empty() {
                    return Err(WindowsExecImageBuildError::Platform(
                        "libuv current-directory observation was empty".to_string(),
                    ));
                }
                validate_units(&cwd, WindowsImageField::Cwd)?;
                cwd
            }
        };
        let child_path = find_node20_path(&environment);
        let resolved = platform
            .resolve_node20_executable(&requested, &cwd, child_path)
            .map_err(WindowsExecImageBuildError::Platform)?;
        if resolved.is_batch() {
            return Err(WindowsExecImageBuildError::NodeBatchExecutable);
        }
        let application = terminated(resolved.application());
        let cwd = Some(WindowsCwdImage::Node20LibuvSnapshot(
            terminated(&cwd).into_boxed_slice(),
        ));
        let environment = WindowsEnvironmentImage::Explicit(environment.into_boxed_slice());
        Ok(Self {
            image_dialect: ProcessImageDialect::Node20Libuv146,
            application: application.into_boxed_slice(),
            command_line: command_line.into_boxed_slice(),
            cwd,
            environment,
        })
    }

    /// Build a Rust 1.93 Windows image in the pinned std source order:
    /// capture/fold environment, resolve the program from that snapshot, build
    /// argv, serialize the final environment, then prepare cwd.
    pub(super) fn build_rust193<P: WindowsEnvironmentPlatform>(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
        platform: &P,
    ) -> Result<Self, WindowsExecImageBuildError> {
        validate_rust193_guard(preimage.environment(), guard, platform)?;

        let captured_environment =
            CapturedRust193Environment::capture(preimage.environment(), guard, platform)?;
        let requested = project_atom_unchecked(preimage.file())?;
        let child_path = captured_environment.child_path(platform)?;
        let resolved = platform
            .resolve_rust193_executable(&requested, child_path)
            .map_err(WindowsExecImageBuildError::Platform)?;
        let application = terminated(resolved.application());
        let command_line = match &resolved {
            WindowsResolvedExecutable::Binary { .. } => {
                build_rust193_command_line(preimage.argv0(), preimage.args(), false)?
            }
            WindowsResolvedExecutable::Batch { script, .. } => {
                build_rust193_batch_command_line(script, preimage.args(), false)?
            }
        };

        let environment = captured_environment.into_image(guard, platform)?;
        let cwd = preimage
            .cwd()
            .map(|value| project_atom(value, WindowsImageField::Cwd).map(|wide| terminated(&wide)))
            .transpose()?
            .map(|mut wide| {
                let argument_offset = platform
                    .prepare_rust193_cwd(&mut wide)
                    .map_err(WindowsExecImageBuildError::Platform)?;
                if argument_offset >= wide.len()
                    || wide.last() != Some(&0)
                    || !wide[argument_offset..].ends_with(&[0])
                {
                    return Err(WindowsExecImageBuildError::Platform(
                        "Rust 1.93 cwd platform returned an invalid pointer offset".to_string(),
                    ));
                }
                Ok(WindowsCwdImage::Rust193PlatformPrepared {
                    storage: wide.into_boxed_slice(),
                    argument_offset,
                })
            })
            .transpose()?;
        Ok(Self {
            image_dialect: ProcessImageDialect::Rust193,
            application: application.into_boxed_slice(),
            command_line: command_line.into_boxed_slice(),
            cwd,
            environment,
        })
    }

    pub(super) fn application(&self) -> &[u16] {
        &self.application
    }

    pub(super) fn command_line(&self) -> &[u16] {
        &self.command_line
    }

    pub(super) fn environment(&self) -> &WindowsEnvironmentImage {
        &self.environment
    }

    pub(super) fn cwd(&self) -> Option<&WindowsCwdImage> {
        self.cwd.as_ref()
    }
}

impl WindowsCwdImage {
    pub(super) fn argument(&self) -> &[u16] {
        match self {
            Self::Node20LibuvSnapshot(storage) => storage,
            Self::Rust193PlatformPrepared {
                storage,
                argument_offset,
            } => &storage[*argument_offset..],
        }
    }

    pub(super) fn storage(&self) -> &[u16] {
        match self {
            Self::Node20LibuvSnapshot(storage) | Self::Rust193PlatformPrepared { storage, .. } => {
                storage
            }
        }
    }
}

fn build_node_command_line(
    argv0: &ProcessAtom,
    args: &[ProcessAtom],
    verbatim: bool,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let mut command = Vec::new();
    for (index, argument) in std::iter::once(argv0).chain(args).enumerate() {
        if index != 0 {
            command.push(b' ' as u16);
        }
        let wide = project_atom(argument, WindowsImageField::Argument(index))?;
        if verbatim {
            command.extend_from_slice(&wide);
        } else {
            quote_libuv_argument(&wide, &mut command);
        }
    }
    command.push(0);
    Ok(command)
}

fn quote_libuv_argument(source: &[u16], target: &mut Vec<u16>) {
    if source.is_empty() {
        target.extend_from_slice(&[b'"' as u16, b'"' as u16]);
        return;
    }
    if !source
        .iter()
        .any(|unit| matches!(*unit, 0x20 | 0x09 | 0x22))
    {
        target.extend_from_slice(source);
        return;
    }
    if !source.iter().any(|unit| matches!(*unit, 0x22 | 0x5c)) {
        target.push(b'"' as u16);
        target.extend_from_slice(source);
        target.push(b'"' as u16);
        return;
    }

    target.push(b'"' as u16);
    let mut body = Vec::with_capacity(source.len().saturating_mul(2));
    let mut quote_hit = true;
    for unit in source.iter().rev().copied() {
        body.push(unit);
        if quote_hit && unit == b'\\' as u16 {
            body.push(b'\\' as u16);
        } else if unit == b'"' as u16 {
            quote_hit = true;
            body.push(b'\\' as u16);
        } else {
            quote_hit = false;
        }
    }
    body.reverse();
    target.extend(body);
    target.push(b'"' as u16);
}

fn build_rust193_command_line(
    argv0: &ProcessAtom,
    args: &[ProcessAtom],
    force_quotes: bool,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let argv0 = project_atom(argv0, WindowsImageField::Argument(0))?;
    let mut command = Vec::new();
    command.push(b'"' as u16);
    command.extend_from_slice(&argv0);
    command.push(b'"' as u16);
    for (index, argument) in args.iter().enumerate() {
        command.push(b' ' as u16);
        let argument = project_atom(argument, WindowsImageField::Argument(index + 1))?;
        append_rust193_argument(&mut command, &argument, force_quotes);
    }
    command.push(0);
    Ok(command)
}

fn append_rust193_argument(command: &mut Vec<u16>, argument: &[u16], force_quotes: bool) {
    let quote = force_quotes
        || argument.is_empty()
        || argument.iter().any(|unit| matches!(*unit, 0x20 | 0x09));
    if quote {
        command.push(b'"' as u16);
    }

    let mut backslashes = 0_usize;
    for unit in argument.iter().copied() {
        if unit == b'\\' as u16 {
            backslashes += 1;
        } else {
            if unit == b'"' as u16 {
                command.extend(std::iter::repeat_n(b'\\' as u16, backslashes + 1));
            }
            backslashes = 0;
        }
        command.push(unit);
    }
    if quote {
        command.extend(std::iter::repeat_n(b'\\' as u16, backslashes));
        command.push(b'"' as u16);
    }
}

fn build_rust193_batch_command_line(
    script: &[u16],
    args: &[ProcessAtom],
    force_quotes: bool,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    if script.contains(&(b'"' as u16)) || script.last() == Some(&(b'\\' as u16)) {
        return Err(WindowsExecImageBuildError::InvalidBatchScript);
    }
    let mut command: Vec<u16> = "cmd.exe /e:ON /v:OFF /d /c \"".encode_utf16().collect();
    command.push(b'"' as u16);
    command.extend_from_slice(script);
    command.push(b'"' as u16);
    for (index, argument) in args.iter().enumerate() {
        command.push(b' ' as u16);
        let argument = project_atom(argument, WindowsImageField::Argument(index + 1))?;
        if argument.iter().any(|unit| matches!(*unit, 0x0a | 0x0d)) {
            return Err(WindowsExecImageBuildError::InvalidBatchArgument);
        }
        append_rust193_batch_argument(&mut command, &argument, force_quotes);
    }
    command.push(b'"' as u16);
    command.push(0);
    Ok(command)
}

fn append_rust193_batch_argument(command: &mut Vec<u16>, argument: &[u16], mut quote: bool) {
    if argument.is_empty() || argument.last() == Some(&(b'\\' as u16)) {
        quote = true;
    }
    for point in runtime_code_points(argument) {
        if let Some(character) = char::from_u32(point) {
            const UNQUOTED: &str = r"#$*+-./:?@\_";
            let ascii_needs_quotes = character.is_ascii()
                && !(character.is_ascii_alphanumeric() || UNQUOTED.contains(character));
            if ascii_needs_quotes || character.is_control() {
                quote = true;
            }
        }
    }
    if quote {
        command.push(b'"' as u16);
    }

    let mut backslashes = 0_usize;
    for unit in argument.iter().copied() {
        if unit == b'\\' as u16 {
            backslashes += 1;
        } else {
            if unit == b'"' as u16 {
                command.extend(std::iter::repeat_n(b'\\' as u16, backslashes));
                command.push(b'"' as u16);
            } else if unit == b'%' as u16 || unit == b'\r' as u16 {
                command.extend_from_slice(&[
                    b'%' as u16,
                    b'%' as u16,
                    b'c' as u16,
                    b'd' as u16,
                    b':' as u16,
                    b'~' as u16,
                    b',' as u16,
                ]);
            }
            backslashes = 0;
        }
        command.push(unit);
    }
    if quote {
        command.extend(std::iter::repeat_n(b'\\' as u16, backslashes));
        command.push(b'"' as u16);
    }
}

fn build_node_environment<P: WindowsEnvironmentPlatform>(
    environment: &super::RuntimeEnv,
    platform: &P,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let mut logical: Vec<_> = environment.entries().collect();
    logical.sort_by(|(left, _), (right, _)| left.as_utf16().cmp(right.as_utf16()));

    let mut seen_uppercase = Vec::<Vec<u16>>::new();
    let mut projected = Vec::new();
    for (key, value) in logical {
        let uppercase = javascript_uppercase(&key);
        if seen_uppercase.contains(&uppercase) {
            continue;
        }
        seen_uppercase.push(uppercase);
        projected.push(WindowsEnvironmentEntry {
            key: project_runtime_string(&key),
            value: project_runtime_string(value),
        });
    }
    platform
        .sort_libuv_environment(&mut projected)
        .map_err(WindowsExecImageBuildError::Platform)?;

    let mut merged = Vec::with_capacity(projected.len() + REQUIRED_NODE_ENVIRONMENT.len());
    let mut entry_index = 0_usize;
    let mut required_index = 0_usize;
    while entry_index < projected.len() || required_index < REQUIRED_NODE_ENVIRONMENT.len() {
        if required_index == REQUIRED_NODE_ENVIRONMENT.len() {
            merged.push(projected[entry_index].clone());
            entry_index += 1;
            continue;
        }
        let required_name: Vec<u16> = REQUIRED_NODE_ENVIRONMENT[required_index]
            .encode_utf16()
            .collect();
        let comparison = if entry_index == projected.len() {
            Ordering::Less
        } else {
            platform
                .compare_libuv_environment_key(&required_name, projected[entry_index].libuv_key())
                .map_err(WindowsExecImageBuildError::Platform)?
        };
        if comparison == Ordering::Less {
            if let Some(value) = platform
                .node_required_value(&required_name)
                .map_err(WindowsExecImageBuildError::Platform)?
                .filter(|value| !value.is_empty())
            {
                validate_units(&value, WindowsImageField::EnvironmentValue(merged.len()))?;
                merged.push(WindowsEnvironmentEntry {
                    key: required_name,
                    value,
                });
            }
            required_index += 1;
        } else {
            merged.push(projected[entry_index].clone());
            entry_index += 1;
            if comparison == Ordering::Equal {
                required_index += 1;
            }
        }
    }
    serialize_environment(&merged)
}

/// Port of libuv's `find_path`: only ASCII case aliases of the literal PATH
/// key match, and the first matching record in the final child block wins.
fn find_node20_path(environment: &[u16]) -> Option<&[u16]> {
    let mut start = 0_usize;
    while start < environment.len() && environment[start] != 0 {
        let end = environment[start..]
            .iter()
            .position(|unit| *unit == 0)
            .map(|offset| start + offset)?;
        let entry = &environment[start..end];
        if entry.len() >= 5
            && matches!(entry[0], 0x50 | 0x70)
            && matches!(entry[1], 0x41 | 0x61)
            && matches!(entry[2], 0x54 | 0x74)
            && matches!(entry[3], 0x48 | 0x68)
            && entry[4] == b'=' as u16
        {
            return Some(&entry[5..]);
        }
        start = end + 1;
    }
    None
}

fn javascript_uppercase(value: &RuntimeString) -> Vec<u16> {
    let mut output = Vec::new();
    for point in value.code_points() {
        if (0xd800..=0xdfff).contains(&point) {
            output.push(point as u16);
            continue;
        }
        if let Ok(index) = UPPER_MAPPINGS.binary_search_by_key(&point, |(source, _)| *source) {
            for mapped in UPPER_MAPPINGS[index].1.iter().copied() {
                push_code_point(&mut output, mapped);
            }
        } else {
            push_code_point(&mut output, point);
        }
    }
    output
}

fn validate_rust193_guard<P: WindowsEnvironmentPlatform>(
    environment: &Rust193EnvironmentPlan,
    guard: ProcessEnvironmentGuard,
    platform: &P,
) -> Result<(), WindowsExecImageBuildError> {
    if guard != ProcessEnvironmentGuard::SealedNodeNoPreloadV1 {
        return Ok(());
    }
    let Rust193EnvironmentPlan::Overlay { mutations } = environment else {
        return Ok(());
    };
    for mutation in mutations {
        let key = match mutation {
            Rust193EnvMutation::RuntimeSet { key, .. } => Some(project_runtime_string(key)),
            Rust193EnvMutation::NativeHostSet(overlay) => {
                Some(project_runtime_string(overlay.key()))
            }
            Rust193EnvMutation::RuntimeRemove { .. } => None,
        };
        if let Some(key) = key {
            if platform
                .compare_ordinal_ignore_case(&key, NODE_OPTIONS)
                .map_err(WindowsExecImageBuildError::Platform)?
                == Ordering::Equal
            {
                return Err(WindowsExecImageBuildError::ForbiddenNodeOptionsMutation);
            }
        }
    }
    Ok(())
}

enum CapturedRust193Environment {
    NativeInherit,
    GuardedNative(Vec<WindowsRawEnvironmentRecord>),
    Changed {
        environment: WindowsEnvironmentMap,
        saw_path: bool,
    },
}

impl CapturedRust193Environment {
    fn capture<P: WindowsEnvironmentPlatform>(
        plan: &Rust193EnvironmentPlan,
        guard: ProcessEnvironmentGuard,
        platform: &P,
    ) -> Result<Self, WindowsExecImageBuildError> {
        match plan {
            Rust193EnvironmentPlan::NativeParentInherit
                if guard == ProcessEnvironmentGuard::None =>
            {
                Ok(Self::NativeInherit)
            }
            Rust193EnvironmentPlan::NativeParentInherit => platform
                .raw_parent_environment()
                .map(Self::GuardedNative)
                .map_err(WindowsExecImageBuildError::Platform),
            Rust193EnvironmentPlan::Overlay { mutations } => {
                // `CommandEnv::vars` is already a target-comparator BTreeMap
                // when spawn begins. Recreate that fold before observing the
                // parent; overwritten/removed invalid values never reach
                // `make_envp` validation.
                let mutations = WindowsMutationMap::fold(mutations, platform)?;
                let parent = platform
                    .parent_environment()
                    .map_err(WindowsExecImageBuildError::Platform)?;
                let mut environment = WindowsEnvironmentMap::capture(parent, platform)?;
                let saw_path = mutations.saw_path;
                mutations.apply(&mut environment, platform)?;
                Ok(Self::Changed {
                    environment,
                    saw_path,
                })
            }
        }
    }

    fn child_path<P: WindowsEnvironmentPlatform>(
        &self,
        platform: &P,
    ) -> Result<Option<&[u16]>, WindowsExecImageBuildError> {
        match self {
            Self::Changed {
                environment,
                saw_path: true,
            } => environment
                .get(PATH, platform)
                .map(|entry| entry.map(|entry| entry.value.as_slice())),
            Self::NativeInherit
            | Self::GuardedNative(_)
            | Self::Changed {
                saw_path: false, ..
            } => Ok(None),
        }
    }

    fn into_image<P: WindowsEnvironmentPlatform>(
        self,
        guard: ProcessEnvironmentGuard,
        platform: &P,
    ) -> Result<WindowsEnvironmentImage, WindowsExecImageBuildError> {
        match self {
            Self::NativeInherit => Ok(WindowsEnvironmentImage::NativeParentInherit),
            Self::GuardedNative(records) => Ok(WindowsEnvironmentImage::Explicit(
                serialize_guarded_native_parent(records, platform)?.into_boxed_slice(),
            )),
            Self::Changed {
                mut environment, ..
            } => {
                if guard == ProcessEnvironmentGuard::SealedNodeNoPreloadV1 {
                    environment.remove(NODE_OPTIONS, platform)?;
                }
                Ok(WindowsEnvironmentImage::Explicit(
                    serialize_environment(&environment.entries)?.into_boxed_slice(),
                ))
            }
        }
    }
}

struct WindowsMutationEntry {
    key: Vec<u16>,
    value: Option<Vec<u16>>,
}

struct WindowsMutationMap {
    entries: Vec<WindowsMutationEntry>,
    saw_path: bool,
}

impl WindowsMutationMap {
    fn fold<P: WindowsEnvironmentPlatform>(
        mutations: &[Rust193EnvMutation],
        platform: &P,
    ) -> Result<Self, WindowsExecImageBuildError> {
        let mut map = Self {
            entries: Vec::with_capacity(mutations.len()),
            saw_path: false,
        };
        for mutation in mutations {
            let (key, value) = match mutation {
                Rust193EnvMutation::RuntimeSet { key, value } => (
                    project_runtime_string(key),
                    Some(project_runtime_string(value)),
                ),
                Rust193EnvMutation::RuntimeRemove { key } => (project_runtime_string(key), None),
                Rust193EnvMutation::NativeHostSet(overlay) => (
                    project_runtime_string(overlay.key()),
                    Some(native_windows_wide(overlay.value())?),
                ),
            };
            if !map.saw_path
                && platform
                    .compare_ordinal_ignore_case(&key, PATH)
                    .map_err(WindowsExecImageBuildError::Platform)?
                    == Ordering::Equal
            {
                map.saw_path = true;
            }
            map.insert(WindowsMutationEntry { key, value }, platform)?;
        }
        Ok(map)
    }

    fn insert<P: WindowsEnvironmentPlatform>(
        &mut self,
        entry: WindowsMutationEntry,
        platform: &P,
    ) -> Result<(), WindowsExecImageBuildError> {
        for index in 0..self.entries.len() {
            match platform
                .compare_ordinal_ignore_case(&self.entries[index].key, &entry.key)
                .map_err(WindowsExecImageBuildError::Platform)?
            {
                Ordering::Less => {}
                Ordering::Equal => {
                    // `BTreeMap::insert` replaces only the value, retaining
                    // the first key object and therefore its original casing.
                    self.entries[index].value = entry.value;
                    return Ok(());
                }
                Ordering::Greater => {
                    self.entries.insert(index, entry);
                    return Ok(());
                }
            }
        }
        self.entries.push(entry);
        Ok(())
    }

    fn apply<P: WindowsEnvironmentPlatform>(
        self,
        environment: &mut WindowsEnvironmentMap,
        platform: &P,
    ) -> Result<(), WindowsExecImageBuildError> {
        for entry in self.entries {
            match entry.value {
                Some(value) => environment.insert(
                    WindowsEnvironmentEntry {
                        key: entry.key,
                        value,
                    },
                    platform,
                )?,
                None => environment.remove(&entry.key, platform)?,
            }
        }
        Ok(())
    }
}

fn serialize_guarded_native_parent<P: WindowsEnvironmentPlatform>(
    records: Vec<WindowsRawEnvironmentRecord>,
    platform: &P,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let mut block = Vec::new();
    for (index, record) in records.into_iter().enumerate() {
        validate_units(&record.units, WindowsImageField::EnvironmentKey(index))?;
        let ordinary_key = if record.units.first() == Some(&(b'=' as u16)) {
            None
        } else {
            record
                .units
                .iter()
                .position(|unit| *unit == b'=' as u16)
                .map(|separator| &record.units[..separator])
        };
        let remove = match ordinary_key {
            Some(key) => {
                platform
                    .compare_ordinal_ignore_case(key, NODE_OPTIONS)
                    .map_err(WindowsExecImageBuildError::Platform)?
                    == Ordering::Equal
            }
            None => false,
        };
        if remove {
            continue;
        }
        block.extend_from_slice(&record.units);
        block.push(0);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

struct WindowsEnvironmentMap {
    entries: Vec<WindowsEnvironmentEntry>,
}

impl WindowsEnvironmentMap {
    fn capture<P: WindowsEnvironmentPlatform>(
        parent: Vec<WindowsEnvironmentEntry>,
        platform: &P,
    ) -> Result<Self, WindowsExecImageBuildError> {
        let mut map = Self {
            entries: Vec::with_capacity(parent.len()),
        };
        for entry in parent {
            map.insert(entry, platform)?;
        }
        Ok(map)
    }

    fn insert<P: WindowsEnvironmentPlatform>(
        &mut self,
        entry: WindowsEnvironmentEntry,
        platform: &P,
    ) -> Result<(), WindowsExecImageBuildError> {
        for index in 0..self.entries.len() {
            match platform
                .compare_ordinal_ignore_case(self.entries[index].key(), entry.key())
                .map_err(WindowsExecImageBuildError::Platform)?
            {
                Ordering::Less => {}
                Ordering::Equal => {
                    // `BTreeMap::insert` retains the first stored key object.
                    self.entries[index].value = entry.value;
                    return Ok(());
                }
                Ordering::Greater => {
                    self.entries.insert(index, entry);
                    return Ok(());
                }
            }
        }
        self.entries.push(entry);
        Ok(())
    }

    fn remove<P: WindowsEnvironmentPlatform>(
        &mut self,
        key: &[u16],
        platform: &P,
    ) -> Result<(), WindowsExecImageBuildError> {
        for index in 0..self.entries.len() {
            match platform
                .compare_ordinal_ignore_case(self.entries[index].key(), key)
                .map_err(WindowsExecImageBuildError::Platform)?
            {
                Ordering::Less => {}
                Ordering::Equal => {
                    self.entries.remove(index);
                    return Ok(());
                }
                Ordering::Greater => return Ok(()),
            }
        }
        Ok(())
    }

    fn get<P: WindowsEnvironmentPlatform>(
        &self,
        key: &[u16],
        platform: &P,
    ) -> Result<Option<&WindowsEnvironmentEntry>, WindowsExecImageBuildError> {
        for entry in &self.entries {
            match platform
                .compare_ordinal_ignore_case(entry.key(), key)
                .map_err(WindowsExecImageBuildError::Platform)?
            {
                Ordering::Less => {}
                Ordering::Equal => return Ok(Some(entry)),
                Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }
}

fn serialize_environment(
    entries: &[WindowsEnvironmentEntry],
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let mut block = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        validate_units(&entry.key, WindowsImageField::EnvironmentKey(index))?;
        validate_units(&entry.value, WindowsImageField::EnvironmentValue(index))?;
        block.extend_from_slice(&entry.key);
        block.push(b'=' as u16);
        block.extend_from_slice(&entry.value);
        block.push(0);
    }
    if entries.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn project_atom(
    atom: &ProcessAtom,
    field: WindowsImageField,
) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    let wide = project_atom_unchecked(atom)?;
    validate_units(&wide, field)?;
    Ok(wide)
}

fn project_atom_unchecked(atom: &ProcessAtom) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    match atom {
        ProcessAtom::Runtime(value) => Ok(project_runtime_string(value)),
        ProcessAtom::Native(value) => native_windows_wide(value),
    }
}

fn project_runtime_string(value: &RuntimeString) -> Vec<u16> {
    let mut output = Vec::with_capacity(value.len_utf16());
    for point in value.code_points() {
        push_code_point(
            &mut output,
            char::from_u32(point)
                .map(u32::from)
                .unwrap_or(u32::from(char::REPLACEMENT_CHARACTER)),
        );
    }
    output
}

#[cfg(windows)]
fn native_windows_wide(value: &NativeOsString) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    Ok(value.as_windows_wide().to_vec())
}

#[cfg(not(windows))]
fn native_windows_wide(_value: &NativeOsString) -> Result<Vec<u16>, WindowsExecImageBuildError> {
    Err(WindowsExecImageBuildError::NativeAtomUnavailable)
}

fn push_code_point(output: &mut Vec<u16>, point: u32) {
    let character = char::from_u32(point).expect("generated case mapping is scalar");
    let mut units = [0_u16; 2];
    output.extend_from_slice(character.encode_utf16(&mut units));
}

fn runtime_code_points(units: &[u16]) -> impl Iterator<Item = u32> + '_ {
    struct Points<'a> {
        units: &'a [u16],
        index: usize,
    }
    impl Iterator for Points<'_> {
        type Item = u32;

        fn next(&mut self) -> Option<Self::Item> {
            let first = *self.units.get(self.index)?;
            self.index += 1;
            if (0xd800..=0xdbff).contains(&first) {
                if let Some(second) = self
                    .units
                    .get(self.index)
                    .copied()
                    .filter(|second| (0xdc00..=0xdfff).contains(second))
                {
                    self.index += 1;
                    return Some(
                        0x10000
                            + ((u32::from(first) - 0xd800) << 10)
                            + (u32::from(second) - 0xdc00),
                    );
                }
            }
            Some(u32::from(first))
        }
    }
    Points { units, index: 0 }
}

fn validate_resolved_units(
    units: &[u16],
    field: WindowsImageField,
) -> Result<(), WindowsExecImageBuildError> {
    if units.is_empty() {
        return Err(WindowsExecImageBuildError::EmptyResolvedApplication);
    }
    validate_units(units, field)
}

fn has_ascii_case_insensitive_suffix(units: &[u16], suffix: &[u8]) -> bool {
    units.len() >= suffix.len()
        && units[units.len() - suffix.len()..]
            .iter()
            .zip(suffix)
            .all(|(unit, expected)| {
                let folded = if (b'A' as u16..=b'Z' as u16).contains(unit) {
                    unit + u16::from(b'a' - b'A')
                } else {
                    *unit
                };
                folded == u16::from(*expected)
            })
}

fn validate_units(
    units: &[u16],
    field: WindowsImageField,
) -> Result<(), WindowsExecImageBuildError> {
    if let Some(unit_index) = units.iter().position(|unit| *unit == 0) {
        return Err(WindowsExecImageBuildError::ContainsNul { field, unit_index });
    }
    Ok(())
}

fn terminated(value: &[u16]) -> Vec<u16> {
    let mut terminated = Vec::with_capacity(value.len() + 1);
    terminated.extend_from_slice(value);
    terminated.push(0);
    terminated
}
