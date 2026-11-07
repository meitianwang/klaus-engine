use crate::runtime_values::{NodeBufferUtf8, RuntimeString};

use super::env::RuntimeEnv;
use super::process_environment::with_process_environment_lock;

const MAX_ARRAY_INDEX: u32 = u32::MAX - 1;

/// Exact parent `Object.entries(process.env)` snapshot for Node 20.19.4.
///
/// This is a production shadow foundation. Capturing it does not mutate the
/// parent and it is not wired into any legacy spawn path.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct ParentEnvSnapshot;

impl ParentEnvSnapshot {
    /// Pinned JavaScript runtime whose environment boundary is reproduced.
    pub(crate) const NODE_VERSION: &'static str = "20.19.4";
    /// Pinned V8 runtime for UTF-8 ingress and `Utf8Value` projection.
    pub(crate) const V8_VERSION: &'static str = "11.3.244.8-node.29";
    /// Pinned libuv runtime for enumeration and getter behavior.
    pub(crate) const LIBUV_VERSION: &'static str = "1.46.0";
    /// SHA-256 of the official `node-v20.19.4.tar.xz` source archive.
    pub(crate) const NODE_SOURCE_ARCHIVE_SHA256: &'static str =
        "b87fd7106013d3906706913ffc63a4403715fbb272c4f83ff4338527353eec0f";
    /// SHA-256 of Node's `src/node_env_var.cc`.
    pub(crate) const NODE_ENV_VAR_CC_SHA256: &'static str =
        "7d873c1c1ab15a19a143332a0c78654bdf3097f3b4913b0376985fc384fd3b14";
    /// SHA-256 of Node's vendored `deps/uv/src/unix/core.c`.
    pub(crate) const NODE_LIBUV_UNIX_CORE_SHA256: &'static str =
        "fc3fee4aef98750b9f9d7d1287d1fe9d232e09c9d280642b1c29b9b84179d9e1";
    /// SHA-256 of Node's vendored `deps/uv/src/win/util.c`.
    pub(crate) const NODE_LIBUV_WINDOWS_UTIL_SHA256: &'static str =
        "91e06faa7c9001a1b7de26d76ad5216936f65860ee8676165886cfdcb27c46ff";
    /// Exact Node source declaration for the process-environment mutex.
    pub(crate) const NODE_ENV_MUTEX_DECLARATION: &'static str = "Mutex env_var_mutex;";
    /// Exact lock statement used by Node's Get/Set/Query/Delete/Enumerate paths.
    pub(crate) const NODE_ENV_MUTEX_SCOPED_LOCK: &'static str =
        "Mutex::ScopedLock lock(per_process::env_var_mutex);";
    /// Exact fatal enumeration check in Node's `RealEnvStore::Enumerate`.
    pub(crate) const NODE_ENV_ENUMERATE_CHECK: &'static str =
        "CHECK_EQ(uv_os_environ(&items, &count), 0);";
    /// Exact V8-to-UTF-8 environment key projection in Node's named getter.
    pub(crate) const NODE_ENV_UTF8_KEY: &'static str = "node::Utf8Value key(isolate, property);";
    /// Exact Unix libuv enumeration loop anchor.
    pub(crate) const LIBUV_UNIX_ENUMERATE: &'static str = "for (i = 0; environ[i] != NULL; i++);";
    /// Exact Unix libuv name/value split anchor.
    pub(crate) const LIBUV_UNIX_SPLIT: &'static str = "ptr = strchr(buf, '=');";
    /// Exact Unix libuv getter anchor.
    pub(crate) const LIBUV_UNIX_GETTER: &'static str = "var = getenv(name);";
    /// Exact Windows environment-block acquisition anchor.
    pub(crate) const LIBUV_WINDOWS_ENUMERATE: &'static str = "env = GetEnvironmentStringsW();";
    /// Exact Windows hidden-drive-safe name/value split anchor.
    pub(crate) const LIBUV_WINDOWS_SPLIT: &'static str = "ptr = strchr(buf + 1, '=');";
    /// Exact Windows environment-block release anchor.
    pub(crate) const LIBUV_WINDOWS_FREE: &'static str = "FreeEnvironmentStringsW(env);";
    /// Exact Windows environment getter anchor.
    pub(crate) const LIBUV_WINDOWS_GETTER: &'static str =
        "len = GetEnvironmentVariableW(name_w, var, varlen);";

    /// Capture the actual parent environment through the current host's
    /// Node/libuv enumeration and getter boundaries.
    pub(crate) fn capture() -> RuntimeEnv {
        with_process_environment_lock(Self::capture_unlocked)
    }

    /// Capture while the caller already owns the process-environment mutex.
    /// The Windows image adapter uses this to keep Node enumeration, required
    /// variable reads, PATH resolution, and block construction in one atomic
    /// observation without recursively acquiring the non-reentrant mutex.
    pub(crate) fn capture_unlocked() -> RuntimeEnv {
        #[cfg(unix)]
        {
            return Self::from_unix_raw_entries(unix_raw_environ_entries(), unix_getenv);
        }
        #[cfg(windows)]
        {
            return Self::from_windows_libuv_utf8_entries(
                windows_environment_entries_utf8(),
                windows_getenv_utf8,
            );
        }
        #[allow(unreachable_code)]
        RuntimeEnv::new()
    }

    /// Deterministic seam at Unix `environ` plus `getenv` boundaries.
    #[cfg(any(test, unix))]
    pub(crate) fn from_unix_raw_entries<I, E, G>(entries: I, mut getter: G) -> RuntimeEnv
    where
        I: IntoIterator<Item = E>,
        E: AsRef<[u8]>,
        G: FnMut(&[u8]) -> Option<Vec<u8>>,
    {
        let names = entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref().to_vec();
            let equals = entry.iter().position(|byte| *byte == b'=')?;
            Some(entry[..equals].to_vec())
        });
        snapshot_from_enumerated_names(names, &mut getter)
    }

    /// Deterministic seam after libuv 1.46 converted each Windows wide entry
    /// with `WideCharToMultiByte(CP_UTF8, 0)`.
    #[cfg(any(test, windows))]
    pub(crate) fn from_windows_libuv_utf8_entries<I, E, G>(entries: I, mut getter: G) -> RuntimeEnv
    where
        I: IntoIterator<Item = E>,
        E: AsRef<[u8]>,
        G: FnMut(&[u8]) -> Option<Vec<u8>>,
    {
        let names = entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref().to_vec();
            let equals = entry.get(1..)?.iter().position(|byte| *byte == b'=')? + 1;
            let name = &entry[..equals];
            (!name.starts_with(b"=")).then(|| name.to_vec())
        });
        snapshot_from_enumerated_names(names, &mut getter)
    }
}

fn snapshot_from_enumerated_names<I, G>(names: I, getter: &mut G) -> RuntimeEnv
where
    I: IntoIterator<Item = Vec<u8>>,
    G: FnMut(&[u8]) -> Option<Vec<u8>>,
{
    let mut environment = RuntimeEnv::new();
    let mut seen = Vec::<RuntimeString>::new();
    for raw_name in names {
        // `String::NewFromUtf8` uses the same malformed-sequence replacement
        // profile already pinned by `NodeBufferUtf8`.
        let logical_key = NodeBufferUtf8::decode(&raw_name);
        if seen.iter().any(|seen| seen == &logical_key) {
            continue;
        }
        seen.push(logical_key.clone());

        // Node's named interceptor enumerates canonical indices, but
        // `Object.entries` performs an indexed property Get for them. With no
        // indexed interceptor installed the Get misses, so the entry is absent.
        if is_canonical_array_index(&logical_key) {
            continue;
        }

        let projected_key = logical_key.to_node_utf8();
        let Some(raw_value) = getter(&projected_key) else {
            continue;
        };
        let logical_value = NodeBufferUtf8::decode(&raw_value);
        environment.insert(logical_key, logical_value);
    }
    environment
}

fn is_canonical_array_index(key: &RuntimeString) -> bool {
    let units = key.as_utf16();
    if units.is_empty() || (units.len() > 1 && units[0] == u16::from(b'0')) {
        return false;
    }
    let mut value = 0_u32;
    for unit in units.iter().copied() {
        if !(u16::from(b'0')..=u16::from(b'9')).contains(&unit) {
            return false;
        }
        let Some(next) = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(unit - u16::from(b'0'))))
        else {
            return false;
        };
        value = next;
        if value > MAX_ARRAY_INDEX {
            return false;
        }
    }
    true
}

#[cfg(unix)]
fn unix_raw_environ_entries() -> Vec<Vec<u8>> {
    use std::ffi::CStr;

    let mut entries = Vec::new();
    // SAFETY: Node/libuv reads the same process-global, null-terminated
    // `environ` vector. Each entry is copied before the next pointer read.
    unsafe {
        let mut cursor = unix_environ();
        while !cursor.is_null() && !(*cursor).is_null() {
            entries.push(CStr::from_ptr(*cursor).to_bytes().to_vec());
            cursor = cursor.add(1);
        }
    }
    entries
}

#[cfg(all(unix, target_os = "macos"))]
unsafe fn unix_environ() -> *mut *mut libc::c_char {
    unsafe extern "C" {
        fn _NSGetEnviron() -> *mut *mut *mut libc::c_char;
    }
    // SAFETY: Darwin exposes the live `environ` slot through this function.
    unsafe { *_NSGetEnviron() }
}

#[cfg(all(unix, not(target_os = "macos")))]
unsafe fn unix_environ() -> *mut *mut libc::c_char {
    unsafe extern "C" {
        static mut environ: *mut *mut libc::c_char;
    }
    // SAFETY: reading the exported pointer value does not retain a Rust
    // reference to the mutable static.
    unsafe { environ }
}

#[cfg(unix)]
fn unix_getenv(key: &[u8]) -> Option<Vec<u8>> {
    use std::ffi::CStr;

    if key.contains(&0) {
        return None;
    }
    let mut terminated = Vec::with_capacity(key.len() + 1);
    terminated.extend_from_slice(key);
    terminated.push(0);
    // SAFETY: the key is explicitly NUL-terminated and the returned pointer,
    // when non-null, is a host-owned NUL-terminated value exactly like libuv's
    // `getenv` path. It is copied immediately.
    unsafe {
        let value = libc::getenv(terminated.as_ptr().cast());
        (!value.is_null()).then(|| CStr::from_ptr(value).to_bytes().to_vec())
    }
}

#[cfg(all(test, unix))]
fn unix_getenv_for_test(key: &[u8]) -> Option<Vec<u8>> {
    with_process_environment_lock(|| unix_getenv(key))
}

#[cfg(windows)]
fn windows_environment_entries_utf8() -> Vec<Vec<u8>> {
    let mut entries = Vec::new();
    // SAFETY: the Windows API returns a double-NUL-terminated block owned by
    // the caller. `WindowsEnvironmentBlock` releases it on every exit path.
    unsafe {
        let block = require_windows_environment_block(GetEnvironmentStringsW()).as_ptr();
        let _guard = WindowsEnvironmentBlock(block);
        let mut cursor = block;
        while *cursor != 0 {
            entries.push(windows_wide_cstr_to_utf8(cursor).unwrap_or_else(|| {
                panic!(
                    "Node vendored libuv Windows environment enumeration conversion failed: {}",
                    GetLastError()
                )
            }));
            while *cursor != 0 {
                cursor = cursor.add(1);
            }
            cursor = cursor.add(1);
        }
    }
    entries
}

#[cfg(any(test, windows))]
fn require_windows_environment_block(block: *mut u16) -> std::ptr::NonNull<u16> {
    std::ptr::NonNull::new(block).unwrap_or_else(|| {
        panic!(
            "Node fatal environment invariant failed: CHECK_EQ(uv_os_environ(&items, &count), 0)"
        )
    })
}

#[cfg(windows)]
fn windows_getenv_utf8(key: &[u8]) -> Option<Vec<u8>> {
    let name = windows_utf8_to_wide_cstr(key)?;
    let mut value = vec![0_u16; 512];
    loop {
        // SAFETY: both buffers are terminated/sized as required by
        // `GetEnvironmentVariableW`; `SetLastError(0)` distinguishes an empty
        // value from a miss exactly as libuv 1.46 does.
        let length = unsafe {
            SetLastError(0);
            GetEnvironmentVariableW(name.as_ptr(), value.as_mut_ptr(), value.len() as u32)
        };
        if length < value.len() as u32 {
            if length == 0 && unsafe { GetLastError() } != 0 {
                return None;
            }
            return unsafe { windows_wide_cstr_to_utf8(value.as_ptr()) };
        }
        value.resize(length as usize + 1, 0);
    }
}

#[cfg(windows)]
fn windows_utf8_to_wide_cstr(value: &[u8]) -> Option<Vec<u16>> {
    if value.contains(&0) {
        return None;
    }
    let mut terminated = Vec::with_capacity(value.len() + 1);
    terminated.extend_from_slice(value);
    terminated.push(0);
    // SAFETY: `terminated` is a valid NUL-terminated byte buffer. CP_UTF8 and
    // flags zero reproduce libuv 1.46's conversion.
    unsafe {
        let required = MultiByteToWideChar(
            CP_UTF8,
            0,
            terminated.as_ptr().cast(),
            -1,
            std::ptr::null_mut(),
            0,
        );
        if required <= 0 {
            return None;
        }
        let mut wide = vec![0_u16; required as usize];
        let written = MultiByteToWideChar(
            CP_UTF8,
            0,
            terminated.as_ptr().cast(),
            -1,
            wide.as_mut_ptr(),
            required,
        );
        (written > 0).then_some(wide)
    }
}

#[cfg(windows)]
unsafe fn windows_wide_cstr_to_utf8(value: *const u16) -> Option<Vec<u8>> {
    // SAFETY: callers pass a Windows-owned or local NUL-terminated wide string.
    let required = unsafe {
        WideCharToMultiByte(
            CP_UTF8,
            0,
            value,
            -1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
            std::ptr::null_mut(),
        )
    };
    if required <= 0 {
        return None;
    }
    let mut utf8 = vec![0_u8; required as usize];
    // SAFETY: the allocated output has exactly the size returned above.
    let written = unsafe {
        WideCharToMultiByte(
            CP_UTF8,
            0,
            value,
            -1,
            utf8.as_mut_ptr().cast(),
            required,
            std::ptr::null(),
            std::ptr::null_mut(),
        )
    };
    if written <= 0 {
        return None;
    }
    assert_eq!(utf8.pop(), Some(0));
    Some(utf8)
}

#[cfg(windows)]
struct WindowsEnvironmentBlock(*mut u16);

#[cfg(windows)]
impl Drop for WindowsEnvironmentBlock {
    fn drop(&mut self) {
        // SAFETY: this guard is constructed only from GetEnvironmentStringsW.
        unsafe {
            FreeEnvironmentStringsW(self.0);
        }
    }
}

#[cfg(windows)]
const CP_UTF8: u32 = 65_001;

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetEnvironmentStringsW() -> *mut u16;
    fn FreeEnvironmentStringsW(environment_block: *mut u16) -> i32;
    fn GetEnvironmentVariableW(name: *const u16, buffer: *mut u16, size: u32) -> u32;
    fn WideCharToMultiByte(
        code_page: u32,
        flags: u32,
        wide: *const u16,
        wide_length: i32,
        utf8: *mut i8,
        utf8_length: i32,
        default_char: *const i8,
        used_default_char: *mut i32,
    ) -> i32;
    fn MultiByteToWideChar(
        code_page: u32,
        flags: u32,
        utf8: *const i8,
        utf8_length: i32,
        wide: *mut u16,
        wide_length: i32,
    ) -> i32;
    fn GetLastError() -> u32;
    fn SetLastError(error: u32);
}

#[cfg(test)]
#[path = "parent_env_snapshot_tests.rs"]
mod tests;
