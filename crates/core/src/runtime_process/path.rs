use crate::runtime_values::{RuntimePath, RuntimeString};

use super::node20_path_case_data::{CASED_RANGES, CASE_IGNORABLE_RANGES, LOWER_MAPPINGS};

const CHAR_BACKWARD_SLASH: u16 = b'\\' as u16;
const CHAR_COLON: u16 = b':' as u16;
const CHAR_DOT: u16 = b'.' as u16;
const CHAR_FORWARD_SLASH: u16 = b'/' as u16;

/// One pinned Node 20.19.4 `node:path` lexical dialect.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NodePathDialect {
    /// `path.posix` semantics.
    Posix,
    /// `path.win32` semantics.
    Win32,
}

/// Host family that controls Node's `path.posix` cwd adaptation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NodePathHostPlatform {
    /// A host whose `process.cwd()` is already POSIX-shaped.
    Posix,
    /// A Windows host, where `path.posix` replaces backslashes and drops the drive.
    Windows,
}

/// Exact error captured while reading `process.cwd()` or `process.env["=X:"]`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RuntimePathCwdError {
    code: RuntimeString,
    message: RuntimeString,
}

impl RuntimePathCwdError {
    /// Preserve an exact host error code and message for later propagation.
    pub fn new(code: RuntimeString, message: RuntimeString) -> Self {
        Self { code, message }
    }

    /// Exact error code.
    pub fn code(&self) -> &RuntimeString {
        &self.code
    }

    /// Exact error message.
    pub fn message(&self) -> &RuntimeString {
        &self.message
    }

    fn uncaptured_drive(key: &RuntimeString) -> Self {
        let mut message = RuntimeString::from("runtime cwd snapshot has no getter result for ")
            .as_utf16()
            .to_vec();
        message.extend_from_slice(key.as_utf16());
        Self {
            code: RuntimeString::from("KLAUS_RUNTIME_PATH_SNAPSHOT_MISSING"),
            message: RuntimeString::from_utf16(message),
        }
    }
}

/// One observable lazy read made while resolving a path.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum RuntimePathCwdRead {
    /// `process.env["=X:"]` using the exact generated key.
    DriveEnv(RuntimeString),
    /// `process.cwd()`.
    ProcessCwd,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DriveCwdResult {
    Missing,
    Value(RuntimeString),
    Error(RuntimePathCwdError),
}

/// Per-operation exact cwd and Windows drive-getter snapshot.
///
/// Values are captured before pure path execution, but reads remain lazy and
/// ordered exactly like Node's `lib/path.js`. This means an absolute operand
/// observes neither a stored cwd error nor a stored drive-getter error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePathCwdSnapshot {
    host_platform: NodePathHostPlatform,
    process_cwd: Result<RuntimePath, RuntimePathCwdError>,
    drive_results: Vec<(RuntimeString, DriveCwdResult)>,
    reads: Vec<RuntimePathCwdRead>,
}

impl RuntimePathCwdSnapshot {
    /// Construct a snapshot with an exact cwd result and no captured drives.
    pub fn new(
        host_platform: NodePathHostPlatform,
        process_cwd: Result<RuntimePath, RuntimePathCwdError>,
    ) -> Self {
        Self {
            host_platform,
            process_cwd,
            drive_results: Vec::new(),
            reads: Vec::new(),
        }
    }

    /// Capture a getter miss for one exact `=X:` key.
    pub fn insert_drive_missing(&mut self, key: RuntimeString) {
        self.insert_drive_result(key, DriveCwdResult::Missing);
    }

    /// Capture a present getter value, including a present empty string.
    pub fn insert_drive_value(&mut self, key: RuntimeString, value: RuntimeString) {
        self.insert_drive_result(key, DriveCwdResult::Value(value));
    }

    /// Capture an exact drive-getter error for later propagation.
    pub fn insert_drive_error(&mut self, key: RuntimeString, error: RuntimePathCwdError) {
        self.insert_drive_result(key, DriveCwdResult::Error(error));
    }

    /// Ordered reads performed so far.
    pub fn reads(&self) -> &[RuntimePathCwdRead] {
        &self.reads
    }

    /// Clear only the observation log, retaining all captured values.
    pub fn clear_reads(&mut self) {
        self.reads.clear();
    }

    fn insert_drive_result(&mut self, key: RuntimeString, result: DriveCwdResult) {
        if let Some((_, existing)) = self
            .drive_results
            .iter_mut()
            .find(|(existing, _)| existing == &key)
        {
            *existing = result;
        } else {
            self.drive_results.push((key, result));
        }
    }

    fn read_process_cwd(&mut self) -> Result<RuntimePath, RuntimePathCwdError> {
        self.reads.push(RuntimePathCwdRead::ProcessCwd);
        self.process_cwd.clone()
    }

    fn read_drive(
        &mut self,
        key: &RuntimeString,
    ) -> Result<Option<RuntimeString>, RuntimePathCwdError> {
        self.reads.push(RuntimePathCwdRead::DriveEnv(key.clone()));
        match self
            .drive_results
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, result)| result)
        {
            Some(DriveCwdResult::Missing) => Ok(None),
            Some(DriveCwdResult::Value(value)) => Ok(Some(value.clone())),
            Some(DriveCwdResult::Error(error)) => Err(error.clone()),
            None => Err(RuntimePathCwdError::uncaptured_drive(key)),
        }
    }
}

/// Exact five-field result returned by Node's `path.parse`.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct RuntimePathParts {
    root: RuntimePath,
    dir: RuntimePath,
    base: RuntimePath,
    ext: RuntimePath,
    name: RuntimePath,
}

impl RuntimePathParts {
    /// Parsed root.
    pub fn root(&self) -> &RuntimePath {
        &self.root
    }

    /// Parsed directory.
    pub fn dir(&self) -> &RuntimePath {
        &self.dir
    }

    /// Parsed basename.
    pub fn base(&self) -> &RuntimePath {
        &self.base
    }

    /// Parsed extension.
    pub fn ext(&self) -> &RuntimePath {
        &self.ext
    }

    /// Parsed name without the extension.
    pub fn name(&self) -> &RuntimePath {
        &self.name
    }
}

/// Exact string-only input accepted by the frozen `path.format` surface.
///
/// `None` represents a missing property. An empty path represents JavaScript's
/// empty-string falsey value, which intentionally falls through to the next
/// field with the same precedence as Node 20.19.4.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct RuntimePathFormatInput {
    root: Option<RuntimePath>,
    dir: Option<RuntimePath>,
    base: Option<RuntimePath>,
    ext: Option<RuntimePath>,
    name: Option<RuntimePath>,
}

impl RuntimePathFormatInput {
    /// Construct an input whose five properties are missing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign `root`.
    pub fn with_root(mut self, value: RuntimePath) -> Self {
        self.root = Some(value);
        self
    }

    /// Assign `dir`.
    pub fn with_dir(mut self, value: RuntimePath) -> Self {
        self.dir = Some(value);
        self
    }

    /// Assign `base`.
    pub fn with_base(mut self, value: RuntimePath) -> Self {
        self.base = Some(value);
        self
    }

    /// Assign `ext`.
    pub fn with_ext(mut self, value: RuntimePath) -> Self {
        self.ext = Some(value);
        self
    }

    /// Assign `name`.
    pub fn with_name(mut self, value: RuntimePath) -> Self {
        self.name = Some(value);
        self
    }
}

/// Apply Node 20.19.4 `path.normalize` without host projection.
pub fn path_normalize(dialect: NodePathDialect, path: &RuntimePath) -> RuntimePath {
    match dialect {
        NodePathDialect::Posix => posix_normalize(path),
        NodePathDialect::Win32 => win32_normalize(path),
    }
}

/// Apply Node 20.19.4 `path.isAbsolute` to exact UTF-16 code units.
pub fn path_is_absolute(dialect: NodePathDialect, path: &RuntimePath) -> bool {
    let units = path_units(path);
    match dialect {
        NodePathDialect::Posix => units.first().copied() == Some(CHAR_FORWARD_SLASH),
        NodePathDialect::Win32 => {
            units.first().copied().is_some_and(is_windows_separator)
                || (units.len() > 2
                    && is_windows_device_root(units[0])
                    && units[1] == CHAR_COLON
                    && is_windows_separator(units[2]))
        }
    }
}

/// Apply Node 20.19.4 variadic `path.join` to exact logical paths.
pub fn path_join(dialect: NodePathDialect, paths: &[RuntimePath]) -> RuntimePath {
    match dialect {
        NodePathDialect::Posix => posix_join(paths),
        NodePathDialect::Win32 => win32_join(paths),
    }
}

/// Apply Node 20.19.4 `path.resolve` against a lazy exact cwd snapshot.
pub fn path_resolve(
    dialect: NodePathDialect,
    paths: &[RuntimePath],
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    match dialect {
        NodePathDialect::Posix => posix_resolve(paths, snapshot),
        NodePathDialect::Win32 => win32_resolve(paths, snapshot),
    }
}

/// Apply Node 20.19.4 `path.relative` against a lazy exact cwd snapshot.
pub fn path_relative(
    dialect: NodePathDialect,
    from: &RuntimePath,
    to: &RuntimePath,
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    match dialect {
        NodePathDialect::Posix => posix_relative(from, to, snapshot),
        NodePathDialect::Win32 => win32_relative(from, to, snapshot),
    }
}

/// Apply Node 20.19.4 `path.toNamespacedPath` without host projection.
pub fn path_to_namespaced_path(
    dialect: NodePathDialect,
    path: &RuntimePath,
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    match dialect {
        NodePathDialect::Posix => Ok(path.clone()),
        NodePathDialect::Win32 => win32_to_namespaced_path(path, snapshot),
    }
}

/// Apply Node 20.19.4 `path.dirname` without projecting the logical path.
pub fn path_dirname(dialect: NodePathDialect, path: &RuntimePath) -> RuntimePath {
    match dialect {
        NodePathDialect::Posix => posix_dirname(path),
        NodePathDialect::Win32 => win32_dirname(path),
    }
}

fn posix_normalize(path: &RuntimePath) -> RuntimePath {
    let units = path_units(path);
    if units.is_empty() {
        return dot();
    }
    let absolute = units[0] == CHAR_FORWARD_SLASH;
    let trailing_separator = units.last().copied() == Some(CHAR_FORWARD_SLASH);
    let mut normalized = normalize_string(units, !absolute, CHAR_FORWARD_SLASH, |unit| {
        unit == CHAR_FORWARD_SLASH
    });
    if normalized.is_empty() {
        if absolute {
            return slash();
        }
        return if trailing_separator {
            from_units(vec![CHAR_DOT, CHAR_FORWARD_SLASH])
        } else {
            dot()
        };
    }
    if trailing_separator {
        normalized.push(CHAR_FORWARD_SLASH);
    }
    if absolute {
        normalized.insert(0, CHAR_FORWARD_SLASH);
    }
    from_units(normalized)
}

fn win32_normalize(path: &RuntimePath) -> RuntimePath {
    let units = path_units(path);
    let len = units.len();
    if len == 0 {
        return dot();
    }
    if len == 1 {
        return if units[0] == CHAR_FORWARD_SLASH {
            from_units(vec![CHAR_BACKWARD_SLASH])
        } else {
            path.clone()
        };
    }

    let mut root_end = 0;
    let mut device: Option<Vec<u16>> = None;
    let mut absolute = false;
    if is_windows_separator(units[0]) {
        absolute = true;
        if is_windows_separator(units[1]) {
            let mut cursor = 2;
            let mut last = cursor;
            while cursor < len && !is_windows_separator(units[cursor]) {
                cursor += 1;
            }
            if cursor < len && cursor != last {
                let first_part = &units[last..cursor];
                last = cursor;
                while cursor < len && is_windows_separator(units[cursor]) {
                    cursor += 1;
                }
                if cursor < len && cursor != last {
                    last = cursor;
                    while cursor < len && !is_windows_separator(units[cursor]) {
                        cursor += 1;
                    }
                    if cursor == len {
                        let mut root = vec![CHAR_BACKWARD_SLASH, CHAR_BACKWARD_SLASH];
                        root.extend_from_slice(first_part);
                        root.push(CHAR_BACKWARD_SLASH);
                        root.extend_from_slice(&units[last..]);
                        root.push(CHAR_BACKWARD_SLASH);
                        return from_units(root);
                    }
                    if cursor != last {
                        let mut parsed = vec![CHAR_BACKWARD_SLASH, CHAR_BACKWARD_SLASH];
                        parsed.extend_from_slice(first_part);
                        parsed.push(CHAR_BACKWARD_SLASH);
                        parsed.extend_from_slice(&units[last..cursor]);
                        device = Some(parsed);
                        root_end = cursor;
                    }
                }
            }
        } else {
            root_end = 1;
        }
    } else if let Some(colon_index) = index_of(units, CHAR_COLON, 0) {
        if colon_index > 0 {
            if is_windows_device_root(units[0]) && colon_index == 1 {
                device = Some(units[..2].to_vec());
                root_end = 2;
                if len > 2 && is_windows_separator(units[2]) {
                    absolute = true;
                    root_end = 3;
                }
            } else if is_windows_reserved_name(units, Some(colon_index)) {
                device = Some(units[..=colon_index].to_vec());
                root_end = colon_index + 1;
            }
        }
    }

    let mut tail = if root_end < len {
        normalize_string(
            &units[root_end..],
            !absolute,
            CHAR_BACKWARD_SLASH,
            is_windows_separator,
        )
    } else {
        Vec::new()
    };
    if tail.is_empty() && !absolute {
        tail.push(CHAR_DOT);
    }
    if !tail.is_empty() && units.last().copied().is_some_and(is_windows_separator) {
        tail.push(CHAR_BACKWARD_SLASH);
    }

    if !absolute && device.is_none() && units.contains(&CHAR_COLON) {
        if tail.len() >= 2 && is_windows_device_root(tail[0]) && tail[1] == CHAR_COLON {
            return dot_backslash_prefixed(tail);
        }
        let mut colon_index = index_of(units, CHAR_COLON, 0);
        while let Some(index) = colon_index {
            if index == len - 1 || is_windows_separator(units[index + 1]) {
                return dot_backslash_prefixed(tail);
            }
            colon_index = index_of(units, CHAR_COLON, index + 1);
        }
    }

    let colon_index = index_of(units, CHAR_COLON, 0);
    if is_windows_reserved_name(units, colon_index) {
        let mut output = vec![CHAR_DOT, CHAR_BACKWARD_SLASH];
        if let Some(device) = device {
            output.extend_from_slice(&device);
        }
        output.extend_from_slice(&tail);
        return from_units(output);
    }

    let Some(device) = device else {
        if absolute {
            tail.insert(0, CHAR_BACKWARD_SLASH);
        }
        return from_units(tail);
    };
    let mut output = device;
    if absolute {
        output.push(CHAR_BACKWARD_SLASH);
    }
    output.extend_from_slice(&tail);
    from_units(output)
}

fn posix_join(paths: &[RuntimePath]) -> RuntimePath {
    if paths.is_empty() {
        return dot();
    }
    let mut joined = Vec::new();
    for path in paths {
        let units = path_units(path);
        if units.is_empty() {
            continue;
        }
        if !joined.is_empty() {
            joined.push(CHAR_FORWARD_SLASH);
        }
        joined.extend_from_slice(units);
    }
    if joined.is_empty() {
        dot()
    } else {
        posix_normalize(&from_units(joined))
    }
}

fn win32_join(paths: &[RuntimePath]) -> RuntimePath {
    if paths.is_empty() {
        return dot();
    }
    let mut joined = Vec::new();
    let mut first_part: Option<Vec<u16>> = None;
    for path in paths {
        let units = path_units(path);
        if units.is_empty() {
            continue;
        }
        if first_part.is_none() {
            first_part = Some(units.to_vec());
        } else {
            joined.push(CHAR_BACKWARD_SLASH);
        }
        joined.extend_from_slice(units);
    }
    let Some(first_part) = first_part else {
        return dot();
    };

    let mut needs_replace = true;
    let mut slash_count = 0;
    if first_part
        .first()
        .copied()
        .is_some_and(is_windows_separator)
    {
        slash_count += 1;
        if first_part.len() > 1 && is_windows_separator(first_part[1]) {
            slash_count += 1;
            if first_part.len() > 2 {
                if is_windows_separator(first_part[2]) {
                    slash_count += 1;
                } else {
                    needs_replace = false;
                }
            }
        }
    }
    if needs_replace {
        while slash_count < joined.len() && is_windows_separator(joined[slash_count]) {
            slash_count += 1;
        }
        if slash_count >= 2 {
            let mut replaced = vec![CHAR_BACKWARD_SLASH];
            replaced.extend_from_slice(&joined[slash_count..]);
            joined = replaced;
        }
    }
    win32_normalize(&from_units(joined))
}

fn posix_resolve(
    paths: &[RuntimePath],
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    let mut resolved = Vec::new();
    let mut absolute = false;
    for path in paths.iter().rev() {
        let units = path_units(path);
        if units.is_empty() {
            continue;
        }
        let mut next = units.to_vec();
        next.push(CHAR_FORWARD_SLASH);
        next.extend_from_slice(&resolved);
        resolved = next;
        absolute = units[0] == CHAR_FORWARD_SLASH;
        if absolute {
            break;
        }
    }
    if !absolute {
        let cwd = snapshot.read_process_cwd()?;
        let cwd = posix_cwd(path_units(&cwd), snapshot.host_platform);
        absolute = cwd.first().copied() == Some(CHAR_FORWARD_SLASH);
        let mut next = cwd;
        next.push(CHAR_FORWARD_SLASH);
        next.extend_from_slice(&resolved);
        resolved = next;
    }
    let mut normalized = normalize_string(&resolved, !absolute, CHAR_FORWARD_SLASH, |unit| {
        unit == CHAR_FORWARD_SLASH
    });
    if absolute {
        normalized.insert(0, CHAR_FORWARD_SLASH);
    } else if normalized.is_empty() {
        normalized.push(CHAR_DOT);
    }
    Ok(from_units(normalized))
}

fn win32_resolve(
    paths: &[RuntimePath],
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    let mut resolved_device = Vec::new();
    let mut resolved_tail = Vec::new();
    let mut resolved_absolute = false;
    let mut index = paths.len() as isize - 1;
    while index >= -1 {
        let path = if index >= 0 {
            path_units(&paths[index as usize]).to_vec()
        } else if resolved_device.is_empty() {
            path_units(&snapshot.read_process_cwd()?).to_vec()
        } else {
            let mut key = vec![b'=' as u16];
            key.extend_from_slice(&resolved_device);
            let key = RuntimeString::from_utf16(key);
            let drive_value = snapshot.read_drive(&key)?;
            let mut path = match drive_value {
                Some(value) if !value.is_empty() => value.as_utf16().to_vec(),
                Some(_) | None => path_units(&snapshot.read_process_cwd()?).to_vec(),
            };
            let drive_mismatch =
                node20_lowercase(&path[..path.len().min(2)]) != node20_lowercase(&resolved_device);
            if drive_mismatch && path.get(2).copied() == Some(CHAR_BACKWARD_SLASH) {
                path = resolved_device.clone();
                path.push(CHAR_BACKWARD_SLASH);
            }
            path
        };
        index -= 1;
        if path.is_empty() {
            continue;
        }

        let (root_end, device, absolute) = win32_resolve_root(&path);
        if !device.is_empty() {
            if !resolved_device.is_empty()
                && node20_lowercase(&device) != node20_lowercase(&resolved_device)
            {
                continue;
            }
            if resolved_device.is_empty() {
                resolved_device = device;
            }
        }

        if resolved_absolute {
            if !resolved_device.is_empty() {
                break;
            }
        } else {
            let mut next = path[root_end..].to_vec();
            next.push(CHAR_BACKWARD_SLASH);
            next.extend_from_slice(&resolved_tail);
            resolved_tail = next;
            resolved_absolute = absolute;
            if absolute && !resolved_device.is_empty() {
                break;
            }
        }
    }

    resolved_tail = normalize_string(
        &resolved_tail,
        !resolved_absolute,
        CHAR_BACKWARD_SLASH,
        is_windows_separator,
    );
    let mut output = resolved_device;
    if resolved_absolute {
        output.push(CHAR_BACKWARD_SLASH);
    }
    output.extend_from_slice(&resolved_tail);
    if output.is_empty() {
        output.push(CHAR_DOT);
    }
    Ok(from_units(output))
}

fn win32_resolve_root(path: &[u16]) -> (usize, Vec<u16>, bool) {
    let len = path.len();
    let mut root_end = 0;
    let mut device = Vec::new();
    let mut absolute = false;
    if len == 1 {
        if is_windows_separator(path[0]) {
            root_end = 1;
            absolute = true;
        }
    } else if is_windows_separator(path[0]) {
        absolute = true;
        if is_windows_separator(path[1]) {
            let mut cursor = 2;
            let mut last = cursor;
            while cursor < len && !is_windows_separator(path[cursor]) {
                cursor += 1;
            }
            if cursor < len && cursor != last {
                let first_part = &path[last..cursor];
                last = cursor;
                while cursor < len && is_windows_separator(path[cursor]) {
                    cursor += 1;
                }
                if cursor < len && cursor != last {
                    last = cursor;
                    while cursor < len && !is_windows_separator(path[cursor]) {
                        cursor += 1;
                    }
                    if cursor == len || cursor != last {
                        device.extend_from_slice(&[CHAR_BACKWARD_SLASH, CHAR_BACKWARD_SLASH]);
                        device.extend_from_slice(first_part);
                        device.push(CHAR_BACKWARD_SLASH);
                        device.extend_from_slice(&path[last..cursor]);
                        root_end = cursor;
                    }
                }
            }
        } else {
            root_end = 1;
        }
    } else if is_windows_device_root(path[0]) && path[1] == CHAR_COLON {
        device.extend_from_slice(&path[..2]);
        root_end = 2;
        if len > 2 && is_windows_separator(path[2]) {
            absolute = true;
            root_end = 3;
        }
    }
    (root_end, device, absolute)
}

fn posix_relative(
    from: &RuntimePath,
    to: &RuntimePath,
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    if from == to {
        return Ok(empty());
    }
    let from = path_resolve(NodePathDialect::Posix, std::slice::from_ref(from), snapshot)?;
    let to = path_resolve(NodePathDialect::Posix, std::slice::from_ref(to), snapshot)?;
    if from == to {
        return Ok(empty());
    }
    let from = path_units(&from);
    let to = path_units(&to);
    let from_start = 1;
    let from_len = from.len() - from_start;
    let to_start = 1;
    let to_len = to.len() - to_start;
    let length = from_len.min(to_len);
    let mut last_common_separator = None;
    let mut matched = 0;
    while matched < length && from[from_start + matched] == to[to_start + matched] {
        if from[from_start + matched] == CHAR_FORWARD_SLASH {
            last_common_separator = Some(matched);
        }
        matched += 1;
    }
    if matched == length {
        if to_len > length {
            if to[to_start + matched] == CHAR_FORWARD_SLASH {
                return Ok(slice(to, to_start + matched + 1, to.len()));
            }
            if matched == 0 {
                return Ok(slice(to, to_start, to.len()));
            }
        } else if from_len > length {
            if from[from_start + matched] == CHAR_FORWARD_SLASH {
                last_common_separator = Some(matched);
            } else if matched == 0 {
                last_common_separator = Some(0);
            }
        }
    }
    let common = last_common_separator.map_or(0, |value| value + 1);
    let mut output = Vec::new();
    for code in from[from_start + common..]
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        if code.is_none() || code == Some(CHAR_FORWARD_SLASH) {
            if !output.is_empty() {
                output.push(CHAR_FORWARD_SLASH);
            }
            output.extend_from_slice(&[CHAR_DOT, CHAR_DOT]);
        }
    }
    let slice_start = last_common_separator.map_or(to_start - 1, |value| to_start + value);
    output.extend_from_slice(&to[slice_start..]);
    Ok(from_units(output))
}

fn win32_relative(
    from: &RuntimePath,
    to: &RuntimePath,
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    if from == to {
        return Ok(empty());
    }
    let from_original = path_resolve(NodePathDialect::Win32, std::slice::from_ref(from), snapshot)?;
    let to_original = path_resolve(NodePathDialect::Win32, std::slice::from_ref(to), snapshot)?;
    if from_original == to_original {
        return Ok(empty());
    }
    let from_original_units = path_units(&from_original);
    let to_original_units = path_units(&to_original);
    let from_lower = node20_lowercase(from_original_units);
    let to_lower = node20_lowercase(to_original_units);
    if from_lower == to_lower {
        return Ok(empty());
    }

    if from_original_units.len() != from_lower.len() || to_original_units.len() != to_lower.len() {
        return Ok(win32_relative_expanding_case(
            from_original_units,
            to_original_units,
        ));
    }

    let mut from_start = 0;
    while from_start < from_lower.len() && from_lower[from_start] == CHAR_BACKWARD_SLASH {
        from_start += 1;
    }
    let mut from_end = from_lower.len();
    while from_end.saturating_sub(1) > from_start && from_lower[from_end - 1] == CHAR_BACKWARD_SLASH
    {
        from_end -= 1;
    }
    let from_len = from_end - from_start;

    let mut to_start = 0;
    while to_start < to_lower.len() && to_lower[to_start] == CHAR_BACKWARD_SLASH {
        to_start += 1;
    }
    let mut to_end = to_lower.len();
    while to_end.saturating_sub(1) > to_start && to_lower[to_end - 1] == CHAR_BACKWARD_SLASH {
        to_end -= 1;
    }
    let to_len = to_end - to_start;
    let length = from_len.min(to_len);
    let mut last_common_separator = None;
    let mut matched = 0;
    while matched < length && from_lower[from_start + matched] == to_lower[to_start + matched] {
        if from_lower[from_start + matched] == CHAR_BACKWARD_SLASH {
            last_common_separator = Some(matched);
        }
        matched += 1;
    }
    if matched != length {
        if last_common_separator.is_none() {
            return Ok(to_original);
        }
    } else {
        if to_len > length {
            if to_lower[to_start + matched] == CHAR_BACKWARD_SLASH {
                return Ok(slice(to_original_units, to_start + matched + 1, to_end));
            }
            if matched == 2 {
                return Ok(slice(to_original_units, to_start + matched, to_end));
            }
        }
        if from_len > length {
            if from_lower[from_start + matched] == CHAR_BACKWARD_SLASH {
                last_common_separator = Some(matched);
            } else if matched == 2 {
                last_common_separator = Some(3);
            }
        }
        if last_common_separator.is_none() {
            last_common_separator = Some(0);
        }
    }

    let common = last_common_separator.expect("set above");
    let mut output = Vec::new();
    // Node walks `i` from `fromStart + lastCommonSep + 1` up to and INCLUDING `fromEnd`,
    // where `charCodeAt(fromEnd)` reads past the end and yields NaN — the loop body's
    // "emit one `..`" sentinel. When the start already exceeds `fromEnd` (a bare root as
    // `from`, e.g. relative("\\", "\\etc\\passwd")) JavaScript simply never enters the loop.
    // Expressing that as a slice made the same input panic with an inverted range, so the
    // bounds stay on the index instead.
    let mut index = from_start + common + 1;
    while index <= from_end {
        if index == from_end || from_lower[index] == CHAR_BACKWARD_SLASH {
            if !output.is_empty() {
                output.push(CHAR_BACKWARD_SLASH);
            }
            output.extend_from_slice(&[CHAR_DOT, CHAR_DOT]);
        }
        index += 1;
    }
    to_start += common;
    if !output.is_empty() {
        output.extend_from_slice(&to_original_units[to_start..to_end]);
        return Ok(from_units(output));
    }
    if to_original_units.get(to_start).copied() == Some(CHAR_BACKWARD_SLASH) {
        to_start += 1;
    }
    Ok(slice(to_original_units, to_start, to_end))
}

fn win32_relative_expanding_case(from: &[u16], to: &[u16]) -> RuntimePath {
    let mut from_parts = split_units(from, CHAR_BACKWARD_SLASH);
    let mut to_parts = split_units(to, CHAR_BACKWARD_SLASH);
    if from_parts.last().is_some_and(|part| part.is_empty()) {
        from_parts.pop();
    }
    if to_parts.last().is_some_and(|part| part.is_empty()) {
        to_parts.pop();
    }
    let length = from_parts.len().min(to_parts.len());
    let mut matched = 0;
    while matched < length
        && node20_lowercase(from_parts[matched]) == node20_lowercase(to_parts[matched])
    {
        matched += 1;
    }
    if matched == 0 {
        return from_units(to.to_vec());
    }
    if matched == length {
        if to_parts.len() > length {
            return join_unit_parts(&to_parts[matched..], CHAR_BACKWARD_SLASH);
        }
        if from_parts.len() > length {
            return repeated_parent(from_parts.len() - matched, CHAR_BACKWARD_SLASH);
        }
        return empty();
    }
    let mut output = repeated_parent(from_parts.len() - matched, CHAR_BACKWARD_SLASH)
        .into_runtime_string()
        .as_utf16()
        .to_vec();
    if !output.is_empty() && matched < to_parts.len() {
        output.push(CHAR_BACKWARD_SLASH);
    }
    output.extend_from_slice(path_units(&join_unit_parts(
        &to_parts[matched..],
        CHAR_BACKWARD_SLASH,
    )));
    from_units(output)
}

fn win32_to_namespaced_path(
    path: &RuntimePath,
    snapshot: &mut RuntimePathCwdSnapshot,
) -> Result<RuntimePath, RuntimePathCwdError> {
    if path_units(path).is_empty() {
        return Ok(path.clone());
    }
    let resolved = path_resolve(NodePathDialect::Win32, std::slice::from_ref(path), snapshot)?;
    let units = path_units(&resolved);
    if units.len() <= 2 {
        return Ok(path.clone());
    }
    if units[0] == CHAR_BACKWARD_SLASH && units[1] == CHAR_BACKWARD_SLASH {
        let third = units[2];
        if third != b'?' as u16 && third != CHAR_DOT {
            let mut output = ascii_units(r"\\?\UNC\");
            output.extend_from_slice(&units[2..]);
            return Ok(from_units(output));
        }
    } else if is_windows_device_root(units[0])
        && units[1] == CHAR_COLON
        && units[2] == CHAR_BACKWARD_SLASH
    {
        let mut output = ascii_units(r"\\?\");
        output.extend_from_slice(units);
        return Ok(from_units(output));
    }
    Ok(resolved)
}

/// Apply Node 20.19.4 `path.basename`, including its optional suffix rule.
pub fn path_basename(
    dialect: NodePathDialect,
    path: &RuntimePath,
    suffix: Option<&RuntimePath>,
) -> RuntimePath {
    let units = path_units(path);
    let start = match dialect {
        NodePathDialect::Posix => 0,
        NodePathDialect::Win32
            if units.len() >= 2 && is_windows_device_root(units[0]) && units[1] == CHAR_COLON =>
        {
            2
        }
        NodePathDialect::Win32 => 0,
    };
    basename_from(units, start, suffix.map(path_units), |unit| {
        is_separator(dialect, unit)
    })
}

/// Apply Node 20.19.4 `path.extname` without scalar conversion.
pub fn path_extname(dialect: NodePathDialect, path: &RuntimePath) -> RuntimePath {
    let units = path_units(path);
    let mut start = 0;
    let mut start_part = 0;
    if matches!(dialect, NodePathDialect::Win32)
        && units.len() >= 2
        && units[1] == CHAR_COLON
        && is_windows_device_root(units[0])
    {
        start = 2;
        start_part = 2;
    }
    extname_from(units, start, start_part, |unit| is_separator(dialect, unit))
}

/// Apply the pinned Node 20.19.4 `String.prototype.toLowerCase` mapping without
/// projecting through a Rust scalar string.
pub(crate) fn path_lowercase(value: &RuntimeString) -> RuntimeString {
    RuntimeString::from_utf16(node20_lowercase(value.as_utf16()))
}

/// Apply Node 20.19.4 `path.parse` and retain all field slices exactly.
pub fn path_parse(dialect: NodePathDialect, path: &RuntimePath) -> RuntimePathParts {
    match dialect {
        NodePathDialect::Posix => posix_parse(path),
        NodePathDialect::Win32 => win32_parse(path),
    }
}

/// Apply Node 20.19.4 string-only `path.format` semantics.
pub fn path_format(dialect: NodePathDialect, input: &RuntimePathFormatInput) -> RuntimePath {
    let dir = truthy(input.dir.as_ref()).or_else(|| truthy(input.root.as_ref()));
    let base = if let Some(base) = truthy(input.base.as_ref()) {
        base.clone()
    } else {
        let name =
            truthy(input.name.as_ref()).map_or_else(Vec::new, |value| path_units(value).to_vec());
        let ext = truthy(input.ext.as_ref());
        let mut units = name;
        if let Some(ext) = ext {
            let ext = path_units(ext);
            if ext.first().copied() != Some(CHAR_DOT) {
                units.push(CHAR_DOT);
            }
            units.extend_from_slice(ext);
        }
        from_units(units)
    };

    let Some(dir) = dir else {
        return base;
    };
    let mut units = path_units(dir).to_vec();
    if input.root.as_ref() != Some(dir) {
        units.push(match dialect {
            NodePathDialect::Posix => CHAR_FORWARD_SLASH,
            NodePathDialect::Win32 => CHAR_BACKWARD_SLASH,
        });
    }
    units.extend_from_slice(path_units(&base));
    from_units(units)
}

fn posix_dirname(path: &RuntimePath) -> RuntimePath {
    let units = path_units(path);
    if units.is_empty() {
        return dot();
    }
    let has_root = units[0] == CHAR_FORWARD_SLASH;
    let mut end = None;
    let mut matched_slash = true;
    for index in (1..units.len()).rev() {
        if units[index] == CHAR_FORWARD_SLASH {
            if !matched_slash {
                end = Some(index);
                break;
            }
        } else {
            matched_slash = false;
        }
    }
    let Some(end) = end else {
        return if has_root { slash() } else { dot() };
    };
    if has_root && end == 1 {
        return from_units(vec![CHAR_FORWARD_SLASH, CHAR_FORWARD_SLASH]);
    }
    slice(units, 0, end)
}

fn win32_dirname(path: &RuntimePath) -> RuntimePath {
    let units = path_units(path);
    let len = units.len();
    if len == 0 {
        return dot();
    }
    if len == 1 {
        return if is_windows_separator(units[0]) {
            path.clone()
        } else {
            dot()
        };
    }

    let mut root_end = None;
    let mut offset = 0;
    if is_windows_separator(units[0]) {
        root_end = Some(1);
        offset = 1;
        if is_windows_separator(units[1]) {
            let mut cursor = 2;
            let mut last = cursor;
            while cursor < len && !is_windows_separator(units[cursor]) {
                cursor += 1;
            }
            if cursor < len && cursor != last {
                last = cursor;
                while cursor < len && is_windows_separator(units[cursor]) {
                    cursor += 1;
                }
                if cursor < len && cursor != last {
                    last = cursor;
                    while cursor < len && !is_windows_separator(units[cursor]) {
                        cursor += 1;
                    }
                    if cursor == len {
                        return path.clone();
                    }
                    if cursor != last {
                        root_end = Some(cursor + 1);
                        offset = cursor + 1;
                    }
                }
            }
        }
    } else if is_windows_device_root(units[0]) && units[1] == CHAR_COLON {
        let end = if len > 2 && is_windows_separator(units[2]) {
            3
        } else {
            2
        };
        root_end = Some(end);
        offset = end;
    }

    let mut end = None;
    let mut matched_slash = true;
    for index in (offset..len).rev() {
        if is_windows_separator(units[index]) {
            if !matched_slash {
                end = Some(index);
                break;
            }
        } else {
            matched_slash = false;
        }
    }
    match end.or(root_end) {
        Some(end) => slice(units, 0, end),
        None => dot(),
    }
}

fn basename_from(
    path: &[u16],
    mut start: usize,
    suffix: Option<&[u16]>,
    separator: impl Fn(u16) -> bool,
) -> RuntimePath {
    let len = path.len();
    if let Some(suffix) = suffix.filter(|suffix| !suffix.is_empty() && suffix.len() <= len) {
        if suffix == path {
            return empty();
        }
        let mut ext_index = suffix.len() as isize - 1;
        let mut first_non_slash_end = None;
        let mut end = None;
        let mut matched_slash = true;
        for index in (start..len).rev() {
            let unit = path[index];
            if separator(unit) {
                if !matched_slash {
                    start = index + 1;
                    break;
                }
            } else {
                if first_non_slash_end.is_none() {
                    matched_slash = false;
                    first_non_slash_end = Some(index + 1);
                }
                if ext_index >= 0 {
                    if unit == suffix[ext_index as usize] {
                        ext_index -= 1;
                        if ext_index == -1 {
                            end = Some(index);
                        }
                    } else {
                        ext_index = -1;
                        end = first_non_slash_end;
                    }
                }
            }
        }
        let mut end = end.unwrap_or(len);
        if start == end {
            end = first_non_slash_end.unwrap_or(end);
        }
        return slice(path, start, end);
    }

    let mut end = None;
    let mut matched_slash = true;
    for index in (start..len).rev() {
        if separator(path[index]) {
            if !matched_slash {
                start = index + 1;
                break;
            }
        } else if end.is_none() {
            matched_slash = false;
            end = Some(index + 1);
        }
    }
    end.map_or_else(empty, |end| slice(path, start, end))
}

fn extname_from(
    path: &[u16],
    start: usize,
    mut start_part: usize,
    separator: impl Fn(u16) -> bool,
) -> RuntimePath {
    let mut start_dot = None;
    let mut end = None;
    let mut matched_slash = true;
    let mut pre_dot_state = 0;
    for index in (start..path.len()).rev() {
        let unit = path[index];
        if separator(unit) {
            if !matched_slash {
                start_part = index + 1;
                break;
            }
            continue;
        }
        if end.is_none() {
            matched_slash = false;
            end = Some(index + 1);
        }
        if unit == CHAR_DOT {
            if start_dot.is_none() {
                start_dot = Some(index);
            } else if pre_dot_state != 1 {
                pre_dot_state = 1;
            }
        } else if start_dot.is_some() {
            pre_dot_state = -1;
        }
    }
    let (Some(start_dot), Some(end)) = (start_dot, end) else {
        return empty();
    };
    if pre_dot_state == 0
        || (pre_dot_state == 1 && start_dot == end - 1 && start_dot == start_part + 1)
    {
        return empty();
    }
    slice(path, start_dot, end)
}

fn posix_parse(path: &RuntimePath) -> RuntimePathParts {
    let units = path_units(path);
    if units.is_empty() {
        return RuntimePathParts::default();
    }
    let absolute = units[0] == CHAR_FORWARD_SLASH;
    let root = if absolute { slash() } else { empty() };
    let scan_start = usize::from(absolute);
    let (start_part, end, start_dot, pre_dot_state) =
        scan_final_component(units, scan_start, 0, |unit| unit == CHAR_FORWARD_SLASH);
    let component_start = if absolute && start_part == 0 {
        1
    } else {
        start_part
    };
    let (base, ext, name) = parsed_component(
        units,
        component_start,
        start_part,
        end,
        start_dot,
        pre_dot_state,
    );
    let dir = if start_part > 0 {
        slice(units, 0, start_part - 1)
    } else {
        root.clone()
    };
    RuntimePathParts {
        root,
        dir,
        base,
        ext,
        name,
    }
}

fn win32_parse(path: &RuntimePath) -> RuntimePathParts {
    let units = path_units(path);
    let len = units.len();
    if len == 0 {
        return RuntimePathParts::default();
    }
    if len == 1 {
        if is_windows_separator(units[0]) {
            return RuntimePathParts {
                root: path.clone(),
                dir: path.clone(),
                ..RuntimePathParts::default()
            };
        }
        return RuntimePathParts {
            base: path.clone(),
            name: path.clone(),
            ..RuntimePathParts::default()
        };
    }

    let mut root_end = 0;
    if is_windows_separator(units[0]) {
        root_end = 1;
        if is_windows_separator(units[1]) {
            let mut cursor = 2;
            let mut last = cursor;
            while cursor < len && !is_windows_separator(units[cursor]) {
                cursor += 1;
            }
            if cursor < len && cursor != last {
                last = cursor;
                while cursor < len && is_windows_separator(units[cursor]) {
                    cursor += 1;
                }
                if cursor < len && cursor != last {
                    last = cursor;
                    while cursor < len && !is_windows_separator(units[cursor]) {
                        cursor += 1;
                    }
                    if cursor == len {
                        root_end = cursor;
                    } else if cursor != last {
                        root_end = cursor + 1;
                    }
                }
            }
        }
    } else if is_windows_device_root(units[0]) && units[1] == CHAR_COLON {
        if len <= 2 {
            return RuntimePathParts {
                root: path.clone(),
                dir: path.clone(),
                ..RuntimePathParts::default()
            };
        }
        root_end = 2;
        if is_windows_separator(units[2]) {
            if len == 3 {
                return RuntimePathParts {
                    root: path.clone(),
                    dir: path.clone(),
                    ..RuntimePathParts::default()
                };
            }
            root_end = 3;
        }
    }

    let root = if root_end > 0 {
        slice(units, 0, root_end)
    } else {
        empty()
    };
    let (start_part, end, start_dot, pre_dot_state) =
        scan_final_component(units, root_end, root_end, is_windows_separator);
    let (base, ext, name) =
        parsed_component(units, start_part, start_part, end, start_dot, pre_dot_state);
    let dir = if start_part > 0 && start_part != root_end {
        slice(units, 0, start_part - 1)
    } else {
        root.clone()
    };
    RuntimePathParts {
        root,
        dir,
        base,
        ext,
        name,
    }
}

fn scan_final_component(
    path: &[u16],
    scan_start: usize,
    initial_start_part: usize,
    separator: impl Fn(u16) -> bool,
) -> (usize, Option<usize>, Option<usize>, i8) {
    let mut start_dot = None;
    let mut start_part = initial_start_part;
    let mut end = None;
    let mut matched_slash = true;
    let mut pre_dot_state = 0;
    for index in (scan_start..path.len()).rev() {
        let unit = path[index];
        if separator(unit) {
            if !matched_slash {
                start_part = index + 1;
                break;
            }
            continue;
        }
        if end.is_none() {
            matched_slash = false;
            end = Some(index + 1);
        }
        if unit == CHAR_DOT {
            if start_dot.is_none() {
                start_dot = Some(index);
            } else if pre_dot_state != 1 {
                pre_dot_state = 1;
            }
        } else if start_dot.is_some() {
            pre_dot_state = -1;
        }
    }
    (start_part, end, start_dot, pre_dot_state)
}

fn parsed_component(
    path: &[u16],
    component_start: usize,
    dot_rule_start_part: usize,
    end: Option<usize>,
    start_dot: Option<usize>,
    pre_dot_state: i8,
) -> (RuntimePath, RuntimePath, RuntimePath) {
    let Some(end) = end else {
        return (empty(), empty(), empty());
    };
    let start_dot_is_extension = start_dot.is_some_and(|start_dot| {
        pre_dot_state != 0
            && !(pre_dot_state == 1 && start_dot == end - 1 && start_dot == dot_rule_start_part + 1)
    });
    if start_dot_is_extension {
        let start_dot = start_dot.expect("checked above");
        (
            slice(path, component_start, end),
            slice(path, start_dot, end),
            slice(path, component_start, start_dot),
        )
    } else {
        let component = slice(path, component_start, end);
        (component.clone(), empty(), component)
    }
}

fn normalize_string(
    path: &[u16],
    allow_above_root: bool,
    separator: u16,
    is_path_separator: impl Fn(u16) -> bool,
) -> Vec<u16> {
    let mut result = Vec::new();
    let mut last_segment_length = 0;
    let mut last_slash = -1_isize;
    let mut dots = 0_isize;
    let mut code = 0;
    for index in 0..=path.len() {
        if index < path.len() {
            code = path[index];
        } else if is_path_separator(code) {
            break;
        } else {
            code = CHAR_FORWARD_SLASH;
        }

        if is_path_separator(code) {
            if last_slash == index as isize - 1 || dots == 1 {
                // Empty segments and single-dot segments disappear.
            } else if dots == 2 {
                if result.len() < 2
                    || last_segment_length != 2
                    || result[result.len() - 1] != CHAR_DOT
                    || result[result.len() - 2] != CHAR_DOT
                {
                    if result.len() > 2 {
                        if let Some(last_separator) =
                            result.iter().rposition(|unit| *unit == separator)
                        {
                            result.truncate(last_separator);
                            last_segment_length = result
                                .iter()
                                .rposition(|unit| *unit == separator)
                                .map_or(result.len(), |previous| result.len() - 1 - previous);
                        } else {
                            result.clear();
                            last_segment_length = 0;
                        }
                        last_slash = index as isize;
                        dots = 0;
                        continue;
                    } else if !result.is_empty() {
                        result.clear();
                        last_segment_length = 0;
                        last_slash = index as isize;
                        dots = 0;
                        continue;
                    }
                }
                if allow_above_root {
                    if !result.is_empty() {
                        result.push(separator);
                    }
                    result.extend_from_slice(&[CHAR_DOT, CHAR_DOT]);
                    last_segment_length = 2;
                }
            } else {
                if !result.is_empty() {
                    result.push(separator);
                }
                let start = (last_slash + 1) as usize;
                result.extend_from_slice(&path[start..index]);
                last_segment_length = index - start;
            }
            last_slash = index as isize;
            dots = 0;
        } else if code == CHAR_DOT && dots != -1 {
            dots += 1;
        } else {
            dots = -1;
        }
    }
    result
}

fn is_windows_reserved_name(path: &[u16], colon_index: Option<usize>) -> bool {
    let end = colon_index.unwrap_or_else(|| path.len().saturating_sub(1));
    let name = &path[..end];
    const RESERVED: &[&[u8]] = &[
        b"CON", b"PRN", b"AUX", b"NUL", b"COM1", b"COM2", b"COM3", b"COM4", b"COM5", b"COM6",
        b"COM7", b"COM8", b"COM9", b"LPT1", b"LPT2", b"LPT3", b"LPT4", b"LPT5", b"LPT6", b"LPT7",
        b"LPT8", b"LPT9",
    ];
    RESERVED.iter().any(|candidate| {
        name.len() == candidate.len()
            && name.iter().zip(*candidate).all(|(unit, expected)| {
                let upper = if (b'a' as u16..=b'z' as u16).contains(unit) {
                    unit - u16::from(b'a' - b'A')
                } else {
                    *unit
                };
                upper == u16::from(*expected)
            })
    })
}

fn index_of(path: &[u16], needle: u16, start: usize) -> Option<usize> {
    path.get(start..)?
        .iter()
        .position(|unit| *unit == needle)
        .map(|index| start + index)
}

fn dot_backslash_prefixed(tail: Vec<u16>) -> RuntimePath {
    let mut output = vec![CHAR_DOT, CHAR_BACKWARD_SLASH];
    output.extend_from_slice(&tail);
    from_units(output)
}

fn posix_cwd(cwd: &[u16], host_platform: NodePathHostPlatform) -> Vec<u16> {
    match host_platform {
        NodePathHostPlatform::Posix => cwd.to_vec(),
        NodePathHostPlatform::Windows => {
            let replaced: Vec<u16> = cwd
                .iter()
                .map(|unit| {
                    if *unit == CHAR_BACKWARD_SLASH {
                        CHAR_FORWARD_SLASH
                    } else {
                        *unit
                    }
                })
                .collect();
            let start = replaced
                .iter()
                .position(|unit| *unit == CHAR_FORWARD_SLASH)
                .unwrap_or_else(|| replaced.len().saturating_sub(1));
            replaced[start..].to_vec()
        }
    }
}

fn node20_lowercase(units: &[u16]) -> Vec<u16> {
    let code_points = decode_utf16_code_points(units);
    let mut lowered = Vec::with_capacity(units.len());
    for (index, code_point) in code_points.iter().copied().enumerate() {
        if code_point == 0x03a3 && is_final_sigma(&code_points, index) {
            push_code_point(&mut lowered, 0x03c2);
            continue;
        }
        match LOWER_MAPPINGS.binary_search_by_key(&code_point, |(source, _)| *source) {
            Ok(mapping_index) => {
                for mapped in LOWER_MAPPINGS[mapping_index].1 {
                    push_code_point(&mut lowered, *mapped);
                }
            }
            Err(_) => push_code_point(&mut lowered, code_point),
        }
    }
    lowered
}

fn decode_utf16_code_points(units: &[u16]) -> Vec<u32> {
    let mut output = Vec::with_capacity(units.len());
    let mut index = 0;
    while index < units.len() {
        let lead = units[index];
        if (0xd800..=0xdbff).contains(&lead) && index + 1 < units.len() {
            let trail = units[index + 1];
            if (0xdc00..=0xdfff).contains(&trail) {
                output.push(
                    0x10000 + ((u32::from(lead) - 0xd800) << 10) + (u32::from(trail) - 0xdc00),
                );
                index += 2;
                continue;
            }
        }
        output.push(u32::from(lead));
        index += 1;
    }
    output
}

fn is_final_sigma(code_points: &[u32], index: usize) -> bool {
    let preceded_by_cased = code_points[..index]
        .iter()
        .rev()
        .copied()
        .find(|code_point| !in_code_point_ranges(*code_point, CASE_IGNORABLE_RANGES))
        .is_some_and(|code_point| in_code_point_ranges(code_point, CASED_RANGES));
    if !preceded_by_cased {
        return false;
    }
    !code_points[index + 1..]
        .iter()
        .copied()
        .find(|code_point| !in_code_point_ranges(*code_point, CASE_IGNORABLE_RANGES))
        .is_some_and(|code_point| in_code_point_ranges(code_point, CASED_RANGES))
}

fn in_code_point_ranges(code_point: u32, ranges: &[(u32, u32)]) -> bool {
    ranges
        .binary_search_by(|(start, end)| {
            if code_point < *start {
                std::cmp::Ordering::Greater
            } else if code_point > *end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

fn push_code_point(output: &mut Vec<u16>, code_point: u32) {
    if code_point <= 0xffff {
        output.push(code_point as u16);
    } else {
        let value = code_point - 0x10000;
        output.push(0xd800 + ((value >> 10) as u16));
        output.push(0xdc00 + ((value & 0x3ff) as u16));
    }
}

fn split_units(units: &[u16], separator: u16) -> Vec<&[u16]> {
    let mut parts = Vec::new();
    let mut start = 0;
    for (index, unit) in units.iter().copied().enumerate() {
        if unit == separator {
            parts.push(&units[start..index]);
            start = index + 1;
        }
    }
    parts.push(&units[start..]);
    parts
}

fn join_unit_parts(parts: &[&[u16]], separator: u16) -> RuntimePath {
    let mut output = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if index != 0 {
            output.push(separator);
        }
        output.extend_from_slice(part);
    }
    from_units(output)
}

fn repeated_parent(count: usize, separator: u16) -> RuntimePath {
    let mut output = Vec::with_capacity(count.saturating_mul(3).saturating_sub(1));
    for index in 0..count {
        if index != 0 {
            output.push(separator);
        }
        output.extend_from_slice(&[CHAR_DOT, CHAR_DOT]);
    }
    from_units(output)
}

fn ascii_units(value: &str) -> Vec<u16> {
    value.bytes().map(u16::from).collect()
}

fn truthy(value: Option<&RuntimePath>) -> Option<&RuntimePath> {
    value.filter(|value| !path_units(value).is_empty())
}

fn is_separator(dialect: NodePathDialect, unit: u16) -> bool {
    match dialect {
        NodePathDialect::Posix => unit == CHAR_FORWARD_SLASH,
        NodePathDialect::Win32 => is_windows_separator(unit),
    }
}

fn is_windows_separator(unit: u16) -> bool {
    matches!(unit, CHAR_FORWARD_SLASH | CHAR_BACKWARD_SLASH)
}

fn is_windows_device_root(unit: u16) -> bool {
    (b'A' as u16..=b'Z' as u16).contains(&unit) || (b'a' as u16..=b'z' as u16).contains(&unit)
}

fn path_units(path: &RuntimePath) -> &[u16] {
    path.as_runtime_string().as_utf16()
}

fn slice(units: &[u16], start: usize, end: usize) -> RuntimePath {
    from_units(units[start..end].to_vec())
}

fn from_units(units: Vec<u16>) -> RuntimePath {
    RuntimePath::from_runtime_string(RuntimeString::from_utf16(units))
}

fn empty() -> RuntimePath {
    RuntimePath::default()
}

fn dot() -> RuntimePath {
    from_units(vec![CHAR_DOT])
}

fn slash() -> RuntimePath {
    from_units(vec![CHAR_FORWARD_SLASH])
}
