//! Production Win32 observations for the exact Windows process-image builder.
//!
//! This module is compiled only for the Windows target. It deliberately keeps
//! executable search, environment enumeration/comparison, and cwd preparation
//! outside `std::process::Command`, so the project-owned suspended transaction
//! remains the sole process backend.

use std::cell::Cell;
use std::cmp::Ordering;
use std::ffi::{c_void, OsString};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::ptr;

use super::windows::{
    WindowsEnvironmentEntry, WindowsEnvironmentPlatform, WindowsNode20ResolvedExecutable,
    WindowsRawEnvironmentRecord, WindowsResolvedExecutable,
};

pub(super) struct ProductionWindowsEnvironmentPlatform;

impl ProductionWindowsEnvironmentPlatform {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) fn canonical_taskkill_path() -> Result<Vec<u16>, String> {
        let path = join_windows(
            &system_directory()?,
            &"taskkill.exe".encode_utf16().collect::<Vec<_>>(),
        );
        if !node_file_exists(&path)? {
            return Err("canonical Windows taskkill.exe is missing".to_string());
        }
        Ok(path)
    }
}

impl WindowsEnvironmentPlatform for ProductionWindowsEnvironmentPlatform {
    fn node_current_directory(&self) -> Result<Vec<u16>, String> {
        current_directory()
    }

    fn resolve_node20_executable(
        &self,
        requested: &[u16],
        cwd: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsNode20ResolvedExecutable, String> {
        resolve_node20(requested, cwd, child_path)
    }

    fn node_required_value(&self, name: &[u16]) -> Result<Option<Vec<u16>>, String> {
        environment_variable(name)
    }

    fn sort_libuv_environment(
        &self,
        entries: &mut [WindowsEnvironmentEntry],
    ) -> Result<(), String> {
        let keys = entries
            .iter()
            .map(|entry| invariant_uppercase(libuv_key(entry.key())))
            .collect::<Result<Vec<_>, _>>()?;
        let mut order = (0..entries.len()).collect::<Vec<usize>>();
        if order.len() > 1 {
            LIBUV_SORT_KEYS.with(|slot| {
                assert!(
                    slot.replace(&keys).is_null(),
                    "nested libuv environment qsort"
                );
                // SAFETY: UCRT qsort operates on the initialized pointer-sized
                // index array. The thread-local key pointer remains live and
                // unchanged for the complete synchronous call.
                unsafe {
                    qsort(
                        order.as_mut_ptr().cast(),
                        order.len(),
                        std::mem::size_of::<usize>(),
                        Some(libuv_qsort_compare),
                    );
                }
                slot.set(ptr::null());
            });
        }
        let sorted = order
            .into_iter()
            .map(|index| entries[index].clone())
            .collect::<Vec<_>>();
        entries.clone_from_slice(&sorted);
        Ok(())
    }

    fn compare_libuv_environment_key(
        &self,
        left: &[u16],
        right: &[u16],
    ) -> Result<Ordering, String> {
        Ok(invariant_uppercase(libuv_key(left))?.cmp(&invariant_uppercase(libuv_key(right))?))
    }

    fn compare_ordinal_ignore_case(&self, left: &[u16], right: &[u16]) -> Result<Ordering, String> {
        compare_ordinal_ignore_case(left, right)
    }

    fn parent_environment(&self) -> Result<Vec<WindowsEnvironmentEntry>, String> {
        let records = raw_environment_records()?;
        let mut entries = Vec::new();
        for record in records {
            let units = record.units();
            let Some(separator) = units
                .get(1..)
                .and_then(|tail| tail.iter().position(|unit| *unit == b'=' as u16))
                .map(|offset| offset + 1)
            else {
                continue;
            };
            entries.push(
                WindowsEnvironmentEntry::new(
                    units[..separator].to_vec(),
                    units[separator + 1..].to_vec(),
                )
                .map_err(|error| error.to_string())?,
            );
        }
        Ok(entries)
    }

    fn raw_parent_environment(&self) -> Result<Vec<WindowsRawEnvironmentRecord>, String> {
        raw_environment_records()
    }

    fn resolve_rust193_executable(
        &self,
        requested: &[u16],
        child_path: Option<&[u16]>,
    ) -> Result<WindowsResolvedExecutable, String> {
        resolve_rust193(requested, child_path)
    }

    fn prepare_rust193_cwd(&self, cwd: &mut [u16]) -> Result<usize, String> {
        prepare_rust193_cwd(cwd)
    }
}

thread_local! {
    static LIBUV_SORT_KEYS: Cell<*const Vec<Vec<u16>>> = const { Cell::new(ptr::null()) };
}

unsafe extern "C" fn libuv_qsort_compare(left: *const c_void, right: *const c_void) -> i32 {
    // SAFETY: qsort passes pointers to initialized `usize` elements.
    let left = unsafe { *(left.cast::<usize>()) };
    let right = unsafe { *(right.cast::<usize>()) };
    LIBUV_SORT_KEYS.with(|slot| {
        let keys = slot.get();
        assert!(!keys.is_null(), "libuv qsort comparator lost key storage");
        // SAFETY: the pointer is installed from a live immutable vector for
        // exactly the duration of qsort on this thread.
        match unsafe { (&*keys)[left].cmp(&(&*keys)[right]) } {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    })
}

fn invariant_uppercase(value: &[u16]) -> Result<Vec<u16>, String> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let length =
        i32::try_from(value.len()).map_err(|_| "Windows string is too long".to_string())?;
    let mut mapped = vec![0_u16; value.len()];
    // SAFETY: both slices are initialized and have the exact explicit length.
    let written = unsafe {
        LCMapStringW(
            LOCALE_INVARIANT,
            LCMAP_UPPERCASE,
            value.as_ptr(),
            length,
            mapped.as_mut_ptr(),
            length,
        )
    };
    if written != length {
        return Err(last_error("LCMapStringW invariant uppercase"));
    }
    Ok(mapped)
}

fn compare_ordinal_ignore_case(left: &[u16], right: &[u16]) -> Result<Ordering, String> {
    let left_len =
        i32::try_from(left.len()).map_err(|_| "Windows string is too long".to_string())?;
    let right_len =
        i32::try_from(right.len()).map_err(|_| "Windows string is too long".to_string())?;
    // SAFETY: explicit lengths make empty/non-NUL-terminated slices legal.
    let result =
        unsafe { CompareStringOrdinal(left.as_ptr(), left_len, right.as_ptr(), right_len, 1) };
    match result {
        CSTR_LESS_THAN => Ok(Ordering::Less),
        CSTR_EQUAL => Ok(Ordering::Equal),
        CSTR_GREATER_THAN => Ok(Ordering::Greater),
        _ => Err(last_error("CompareStringOrdinal")),
    }
}

fn libuv_key(value: &[u16]) -> &[u16] {
    let end = value
        .iter()
        .position(|unit| *unit == b'=' as u16)
        .unwrap_or(value.len());
    &value[..end]
}

fn current_directory() -> Result<Vec<u16>, String> {
    let mut size = 256_u32;
    loop {
        let mut buffer = vec![0_u16; size as usize];
        // SAFETY: the buffer has exactly `size` writable UTF-16 units.
        let written = unsafe { GetCurrentDirectoryW(size, buffer.as_mut_ptr()) };
        if written == 0 {
            return Err(last_error("GetCurrentDirectoryW"));
        }
        if written < size {
            buffer.truncate(written as usize);
            return Ok(buffer);
        }
        size = written
            .checked_add(1)
            .ok_or_else(|| "GetCurrentDirectoryW size overflow".to_string())?;
    }
}

fn environment_variable(name: &[u16]) -> Result<Option<Vec<u16>>, String> {
    let name = terminated(name, "environment variable name")?;
    // libuv treats both an absent and an empty value as zero length here.
    // SAFETY: `name` is NUL-terminated.
    let required = unsafe { GetEnvironmentVariableW(name.as_ptr(), ptr::null_mut(), 0) };
    if required == 0 {
        return Ok(None);
    }
    let mut size = required;
    loop {
        let mut value = vec![0_u16; size as usize];
        // SAFETY: both buffers satisfy GetEnvironmentVariableW's contract.
        let written = unsafe { GetEnvironmentVariableW(name.as_ptr(), value.as_mut_ptr(), size) };
        if written == 0 {
            return Ok(None);
        }
        if written < size {
            value.truncate(written as usize);
            return Ok(Some(value));
        }
        size = written
            .checked_add(1)
            .ok_or_else(|| "GetEnvironmentVariableW size overflow".to_string())?;
    }
}

fn raw_environment_records() -> Result<Vec<WindowsRawEnvironmentRecord>, String> {
    // SAFETY: the returned block is released by the guard on every path.
    let block = unsafe { GetEnvironmentStringsW() };
    if block.is_null() {
        return Err(last_error("GetEnvironmentStringsW"));
    }
    let _guard = EnvironmentBlock(block);
    let mut records = Vec::new();
    let mut cursor = block;
    // SAFETY: GetEnvironmentStringsW returns a double-NUL-terminated block.
    unsafe {
        while *cursor != 0 {
            let start = cursor;
            while *cursor != 0 {
                cursor = cursor.add(1);
            }
            let length = cursor.offset_from(start) as usize;
            records.push(
                WindowsRawEnvironmentRecord::new(
                    std::slice::from_raw_parts(start, length).to_vec(),
                )
                .map_err(|error| error.to_string())?,
            );
            cursor = cursor.add(1);
        }
    }
    Ok(records)
}

struct EnvironmentBlock(*mut u16);

impl Drop for EnvironmentBlock {
    fn drop(&mut self) {
        // SAFETY: this guard is constructed only from GetEnvironmentStringsW.
        unsafe {
            FreeEnvironmentStringsW(self.0);
        }
    }
}

fn resolve_node20(
    requested: &[u16],
    cwd: &[u16],
    child_path: Option<&[u16]>,
) -> Result<WindowsNode20ResolvedExecutable, String> {
    reject_nul(requested, "Node executable")?;
    reject_nul(cwd, "Node cwd")?;
    if requested.is_empty() || requested == [b'.' as u16] {
        return Err("ENOENT: Node/libuv executable is empty or dot".to_string());
    }

    let file_start = requested
        .iter()
        .rposition(|unit| matches!(*unit, 0x2f | 0x5c | 0x3a))
        .map_or(0, |index| index + 1);
    if file_start == requested.len() {
        return Err("ENOENT: Node/libuv executable has no file name".to_string());
    }
    let file_name = &requested[file_start..];
    let name_has_ext = file_name
        .iter()
        .enumerate()
        .any(|(index, unit)| *unit == b'.' as u16 && index + 1 < file_name.len());

    if file_start != 0 {
        let base = resolve_against_cwd(cwd, requested);
        if let Some(found) = node_try_extensions(&base, name_has_ext)? {
            return WindowsNode20ResolvedExecutable::from_search_result(found)
                .map_err(|error| error.to_string());
        }
        return Err("ENOENT: Node/libuv executable was not found".to_string());
    }

    if let Some(found) = node_try_in_directory(cwd, file_name, name_has_ext)? {
        return WindowsNode20ResolvedExecutable::from_search_result(found)
            .map_err(|error| error.to_string());
    }
    let path = match child_path {
        Some(path) => Some(path.to_vec()),
        None => environment_variable(&[b'P' as u16, b'A' as u16, b'T' as u16, b'H' as u16])?,
    };
    if let Some(path) = path {
        for segment in split_libuv_path(&path) {
            if segment.is_empty() {
                continue;
            }
            let directory = resolve_against_cwd(cwd, &segment);
            if let Some(found) = node_try_in_directory(&directory, file_name, name_has_ext)? {
                return WindowsNode20ResolvedExecutable::from_search_result(found)
                    .map_err(|error| error.to_string());
            }
        }
    }
    Err("ENOENT: Node/libuv executable was not found".to_string())
}

fn node_try_in_directory(
    directory: &[u16],
    file_name: &[u16],
    name_has_ext: bool,
) -> Result<Option<Vec<u16>>, String> {
    node_try_extensions(&join_windows(directory, file_name), name_has_ext)
}

fn node_try_extensions(base: &[u16], name_has_ext: bool) -> Result<Option<Vec<u16>>, String> {
    if name_has_ext && node_file_exists(base)? {
        return Ok(Some(base.to_vec()));
    }
    for extension in [b".com".as_slice(), b".exe".as_slice()] {
        let mut candidate = base.to_vec();
        candidate.extend(extension.iter().map(|byte| u16::from(*byte)));
        if node_file_exists(&candidate)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn node_file_exists(path: &[u16]) -> Result<bool, String> {
    let path = terminated(path, "Node executable candidate")?;
    // SAFETY: path is a valid NUL-terminated wide string.
    let attributes = unsafe { GetFileAttributesW(path.as_ptr()) };
    Ok(attributes != INVALID_FILE_ATTRIBUTES && attributes & FILE_ATTRIBUTE_DIRECTORY == 0)
}

fn split_libuv_path(path: &[u16]) -> Vec<Vec<u16>> {
    let mut segments = Vec::new();
    let mut segment = Vec::new();
    let mut quoted = false;
    for unit in path.iter().copied() {
        if unit == b'"' as u16 {
            quoted = !quoted;
        } else if unit == b';' as u16 && !quoted {
            segments.push(std::mem::take(&mut segment));
        } else {
            segment.push(unit);
        }
    }
    segments.push(segment);
    segments
}

fn resolve_against_cwd(cwd: &[u16], path: &[u16]) -> Vec<u16> {
    if is_unc(path) || is_drive_absolute(path) {
        return path.to_vec();
    }
    if path
        .first()
        .is_some_and(|unit| matches!(*unit, 0x2f | 0x5c))
    {
        if cwd.get(1) == Some(&(b':' as u16)) {
            let mut resolved = cwd[..2].to_vec();
            resolved.extend_from_slice(path);
            return resolved;
        }
        return path.to_vec();
    }
    if path.get(1) == Some(&(b':' as u16)) {
        if cwd.get(1) == Some(&(b':' as u16))
            && ascii_eq_ignore_case(cwd[0], path[0])
            && path.len() > 2
        {
            return join_windows(cwd, &path[2..]);
        }
        return path.to_vec();
    }
    join_windows(cwd, path)
}

fn join_windows(directory: &[u16], tail: &[u16]) -> Vec<u16> {
    if directory.is_empty() {
        return tail.to_vec();
    }
    let mut joined = directory.to_vec();
    if !joined
        .last()
        .is_some_and(|unit| matches!(*unit, 0x2f | 0x5c | 0x3a))
    {
        joined.push(b'\\' as u16);
    }
    joined.extend_from_slice(tail);
    joined
}

fn resolve_rust193(
    requested: &[u16],
    child_path: Option<&[u16]>,
) -> Result<WindowsResolvedExecutable, String> {
    reject_nul(requested, "Rust 1.93 program")?;
    if requested.is_empty() || has_trailing_slash(requested) {
        return Err("program path has no file name".to_string());
    }
    let requested_os = OsString::from_wide(requested);
    let requested_path = Path::new(&requested_os);
    let has_exe_suffix = has_ascii_suffix(requested, b".exe");

    let resolved = if !rust_is_file_name(requested) {
        if has_exe_suffix {
            to_user_path(requested)?
        } else {
            let mut with_exe = requested.to_vec();
            with_exe.extend(".exe".encode_utf16());
            match rust_program_exists(&with_exe)? {
                Some(path) => path,
                None => to_user_path(requested)?,
            }
        }
    } else {
        let has_extension = requested.contains(&(b'.' as u16));
        let mut found = None;
        if let Some(child_path) = child_path {
            found = search_rust_paths(child_path, requested_path, has_extension)?;
        }
        if found.is_none() {
            if let Ok(executable) = std::env::current_exe() {
                if let Some(parent) = executable.parent() {
                    found = rust_candidate(parent.to_path_buf(), requested_path, has_extension)?;
                }
            }
        }
        if found.is_none() {
            found = rust_candidate(
                PathBuf::from(OsString::from_wide(&system_directory()?)),
                requested_path,
                has_extension,
            )?;
        }
        if found.is_none() {
            found = rust_candidate(
                PathBuf::from(OsString::from_wide(&windows_directory()?)),
                requested_path,
                has_extension,
            )?;
        }
        if found.is_none() {
            if let Some(parent_path) =
                environment_variable(&[b'P' as u16, b'A' as u16, b'T' as u16, b'H' as u16])?
            {
                found = search_rust_paths(&parent_path, requested_path, has_extension)?;
            }
        }
        found.ok_or_else(|| "program not found".to_string())?
    };

    let classification = if is_verbatim(&resolved) {
        resolved.clone()
    } else {
        get_full_path(&resolved)?
    };
    if has_ascii_suffix(&classification, b".bat") || has_ascii_suffix(&classification, b".cmd") {
        let application = join_windows(
            &system_directory()?,
            &"cmd.exe".encode_utf16().collect::<Vec<_>>(),
        );
        WindowsResolvedExecutable::batch(application, resolved).map_err(|error| error.to_string())
    } else {
        WindowsResolvedExecutable::binary(resolved).map_err(|error| error.to_string())
    }
}

fn search_rust_paths(
    paths: &[u16],
    requested: &Path,
    has_extension: bool,
) -> Result<Option<Vec<u16>>, String> {
    let paths = OsString::from_wide(paths);
    for directory in std::env::split_paths(&paths).filter(|path| !path.as_os_str().is_empty()) {
        if let Some(found) = rust_candidate(directory, requested, has_extension)? {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

fn rust_candidate(
    mut directory: PathBuf,
    requested: &Path,
    has_extension: bool,
) -> Result<Option<Vec<u16>>, String> {
    directory.push(requested);
    if !has_extension {
        directory.set_extension("exe");
    }
    rust_program_exists(&directory.as_os_str().encode_wide().collect::<Vec<_>>())
}

fn rust_program_exists(path: &[u16]) -> Result<Option<Vec<u16>>, String> {
    let user_path = to_user_path(path)?;
    let terminated = terminated(&user_path, "Rust executable candidate")?;
    // SAFETY: candidate is NUL-terminated.
    let attributes = unsafe { GetFileAttributesW(terminated.as_ptr()) };
    Ok((attributes != INVALID_FILE_ATTRIBUTES).then_some(user_path))
}

fn to_user_path(path: &[u16]) -> Result<Vec<u16>, String> {
    reject_nul(path, "Windows path")?;
    if path.len() < LEGACY_MAX_PATH {
        if path.len() >= 7
            && path[..4] == [0x5c, 0x5c, 0x3f, 0x5c]
            && path[5] == b':' as u16
            && path[6] == b'\\' as u16
        {
            let candidate = &path[4..];
            if get_full_path(candidate).is_ok_and(|full| full == candidate) {
                return Ok(candidate.to_vec());
            }
        }
        if path.len() >= 8 && path[..8] == [0x5c, 0x5c, 0x3f, 0x5c, 0x55, 0x4e, 0x43, 0x5c] {
            let mut candidate = vec![b'\\' as u16, b'\\' as u16];
            candidate.extend_from_slice(&path[8..]);
            if get_full_path(&candidate).is_ok_and(|full| full == candidate) {
                return Ok(candidate);
            }
        }
    }
    get_long_path(path)
}

fn get_long_path(path: &[u16]) -> Result<Vec<u16>, String> {
    if is_verbatim(path) || path.starts_with(&[0x5c, 0x3f, 0x3f, 0x5c]) || path.is_empty() {
        return Ok(path.to_vec());
    }
    if path.len() + 1 < LONG_PATH_THRESHOLD && (is_drive_absolute(path) || is_unc(path)) {
        return Ok(path.to_vec());
    }
    let mut absolute = get_full_path(path)?;
    if absolute.len() + 1 < LONG_PATH_THRESHOLD {
        return Ok(absolute);
    }
    if is_drive_absolute(&absolute) {
        let mut prefixed = vec![0x5c, 0x5c, 0x3f, 0x5c];
        prefixed.append(&mut absolute);
        Ok(prefixed)
    } else if is_unc(&absolute) {
        let mut prefixed = vec![0x5c, 0x5c, 0x3f, 0x5c, 0x55, 0x4e, 0x43, 0x5c];
        prefixed.extend_from_slice(&absolute[2..]);
        Ok(prefixed)
    } else {
        Ok(absolute)
    }
}

fn prepare_rust193_cwd(cwd: &mut [u16]) -> Result<usize, String> {
    if cwd.last() != Some(&0) {
        return Err("Rust 1.93 cwd is not NUL-terminated".to_string());
    }
    const UNC_PREFIX: &[u16] = &[0x5c, 0x5c, 0x3f, 0x5c, 0x55, 0x4e, 0x43];
    const VERBATIM_PREFIX: &[u16] = &[0x5c, 0x5c, 0x3f, 0x5c];
    if cwd.starts_with(UNC_PREFIX) {
        let offset = 6;
        cwd[offset] = b'\\' as u16;
        if is_absolute_exact(&cwd[offset..]) {
            return Ok(offset);
        }
        cwd[offset] = b'C' as u16;
    } else if cwd.starts_with(VERBATIM_PREFIX) {
        let offset = 4;
        if is_absolute_exact(&cwd[offset..]) {
            return Ok(offset);
        }
    }
    Ok(0)
}

fn is_absolute_exact(terminated_path: &[u16]) -> bool {
    if terminated_path.is_empty() || terminated_path.last() != Some(&0) {
        return false;
    }
    get_full_path(&terminated_path[..terminated_path.len() - 1])
        .is_ok_and(|full| full == terminated_path[..terminated_path.len() - 1])
}

fn get_full_path(path: &[u16]) -> Result<Vec<u16>, String> {
    let path = terminated(path, "GetFullPathNameW path")?;
    // SAFETY: input is NUL-terminated; null output obtains required capacity.
    let mut size = unsafe { GetFullPathNameW(path.as_ptr(), 0, ptr::null_mut(), ptr::null_mut()) };
    if size == 0 {
        return Err(last_error("GetFullPathNameW size"));
    }
    loop {
        let mut full = vec![0_u16; size as usize];
        // SAFETY: output has exactly the advertised writable capacity.
        let written =
            unsafe { GetFullPathNameW(path.as_ptr(), size, full.as_mut_ptr(), ptr::null_mut()) };
        if written == 0 {
            return Err(last_error("GetFullPathNameW"));
        }
        if written < size {
            full.truncate(written as usize);
            return Ok(full);
        }
        size = written
            .checked_add(1)
            .ok_or_else(|| "GetFullPathNameW size overflow".to_string())?;
    }
}

fn system_directory() -> Result<Vec<u16>, String> {
    fill_directory(GetSystemDirectoryW, "GetSystemDirectoryW")
}

fn windows_directory() -> Result<Vec<u16>, String> {
    fill_directory(GetWindowsDirectoryW, "GetWindowsDirectoryW")
}

fn fill_directory(
    function: unsafe extern "system" fn(*mut u16, u32) -> u32,
    label: &str,
) -> Result<Vec<u16>, String> {
    let mut size = 260_u32;
    loop {
        let mut directory = vec![0_u16; size as usize];
        // SAFETY: caller supplies one Win32 directory getter with this ABI.
        let written = unsafe { function(directory.as_mut_ptr(), size) };
        if written == 0 {
            return Err(last_error(label));
        }
        if written < size {
            directory.truncate(written as usize);
            return Ok(directory);
        }
        size = written
            .checked_add(1)
            .ok_or_else(|| format!("{label} size overflow"))?;
    }
}

fn rust_is_file_name(path: &[u16]) -> bool {
    !path.iter().any(|unit| matches!(*unit, 0x2f | 0x5c))
}

fn has_trailing_slash(path: &[u16]) -> bool {
    let verbatim = is_verbatim(path);
    path.last()
        .is_some_and(|unit| *unit == b'\\' as u16 || (!verbatim && *unit == b'/' as u16))
}

fn is_verbatim(path: &[u16]) -> bool {
    path.starts_with(&[0x5c, 0x5c, 0x3f, 0x5c]) || path.starts_with(&[0x5c, 0x3f, 0x3f, 0x5c])
}

fn is_unc(path: &[u16]) -> bool {
    path.len() >= 2 && matches!(path[0], 0x2f | 0x5c) && matches!(path[1], 0x2f | 0x5c)
}

fn is_drive_absolute(path: &[u16]) -> bool {
    path.len() >= 3
        && path[1] == b':' as u16
        && matches!(path[2], 0x2f | 0x5c)
        && !matches!(path[0], 0x2f | 0x5c)
}

fn has_ascii_suffix(value: &[u16], suffix: &[u8]) -> bool {
    value.len() >= suffix.len()
        && value[value.len() - suffix.len()..]
            .iter()
            .zip(suffix)
            .all(|(unit, byte)| ascii_eq_ignore_case(*unit, u16::from(*byte)))
}

fn ascii_eq_ignore_case(left: u16, right: u16) -> bool {
    let fold = |unit: u16| {
        if (b'A' as u16..=b'Z' as u16).contains(&unit) {
            unit + u16::from(b'a' - b'A')
        } else {
            unit
        }
    };
    fold(left) == fold(right)
}

fn reject_nul(value: &[u16], label: &str) -> Result<(), String> {
    if let Some(index) = value.iter().position(|unit| *unit == 0) {
        Err(format!("{label} contains NUL at UTF-16 unit {index}"))
    } else {
        Ok(())
    }
}

fn terminated(value: &[u16], label: &str) -> Result<Vec<u16>, String> {
    reject_nul(value, label)?;
    let mut terminated = Vec::with_capacity(value.len() + 1);
    terminated.extend_from_slice(value);
    terminated.push(0);
    Ok(terminated)
}

fn last_error(operation: &str) -> String {
    format!("{operation} failed: {}", std::io::Error::last_os_error())
}

const INVALID_FILE_ATTRIBUTES: u32 = u32::MAX;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
const LOCALE_INVARIANT: u32 = 0x007f;
const LCMAP_UPPERCASE: u32 = 0x0000_0200;
const CSTR_LESS_THAN: i32 = 1;
const CSTR_EQUAL: i32 = 2;
const CSTR_GREATER_THAN: i32 = 3;
const LEGACY_MAX_PATH: usize = 260;
const LONG_PATH_THRESHOLD: usize = 248;

#[link(name = "ucrt")]
unsafe extern "C" {
    fn qsort(
        base: *mut c_void,
        count: usize,
        width: usize,
        compare: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> i32>,
    );
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetCurrentDirectoryW(size: u32, buffer: *mut u16) -> u32;
    fn GetEnvironmentVariableW(name: *const u16, buffer: *mut u16, size: u32) -> u32;
    fn GetEnvironmentStringsW() -> *mut u16;
    fn FreeEnvironmentStringsW(environment_block: *mut u16) -> i32;
    fn GetFileAttributesW(path: *const u16) -> u32;
    fn GetFullPathNameW(
        path: *const u16,
        size: u32,
        buffer: *mut u16,
        file_part: *mut *mut u16,
    ) -> u32;
    fn GetSystemDirectoryW(buffer: *mut u16, size: u32) -> u32;
    fn GetWindowsDirectoryW(buffer: *mut u16, size: u32) -> u32;
    fn LCMapStringW(
        locale: u32,
        flags: u32,
        source: *const u16,
        source_length: i32,
        destination: *mut u16,
        destination_length: i32,
    ) -> i32;
    fn CompareStringOrdinal(
        left: *const u16,
        left_length: i32,
        right: *const u16,
        right_length: i32,
        ignore_case: i32,
    ) -> i32;
}
